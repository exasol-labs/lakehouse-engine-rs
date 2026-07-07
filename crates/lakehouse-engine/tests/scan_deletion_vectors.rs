//! Scan-level no-container tests for Iceberg v3 **deletion vectors** (task 2.D.4).
//!
//! Each test writes a data Parquet and a hand-built `deletion-vector-v1` Puffin
//! file to a local temp directory (no S3 / MinIO, no Docker), builds a `ScanSpec`
//! whose `FileEntry` carries a `ResolvedDelete::deletion_vector` reference, drives
//! the production raw-scan pipeline ([`run_raw_scan_with_session`] →
//! `PositionalDeleteScanTable` → `scan::puffin` → `scan::deletion_vectors`), and
//! asserts the deleted rows are gone. Mirrors `scan_positional_deletes.rs`.
//!
//! The Puffin file is written with iceberg-rust's `PuffinWriter` and read back
//! with `PuffinReader` to discover the blob's real offset/length — the same
//! coordinates the adapter resolves from the manifest at plan time.
//!
//! Host-runnable: everything lives under `file://`.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
use iceberg::puffin::{Blob, CompressionCodec, DELETION_VECTOR_V1, PuffinReader, PuffinWriter};
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    FileEntry, ProjectionItem, ResolvedDelete, ScanSpec, StorageProps,
};
use lakehouse_engine::scan::{run_raw_scan_with_session, session_config_for_spec};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use roaring::RoaringBitmap;
use url::Url;

const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

// ---------------------------------------------------------------------------
// Test harness (mirrors scan_positional_deletes.rs)
// ---------------------------------------------------------------------------

struct FakeCtx {
    served: bool,
    emitted: Vec<RecordBatch>,
}

impl FakeCtx {
    fn new() -> Self {
        Self {
            served: false,
            emitted: Vec::new(),
        }
    }
}

impl UdfContext for FakeCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&Value, UdfError> {
        Err(UdfError::User("FakeCtx has no input columns".into()))
    }
    fn get_string(&self, _col: usize) -> Result<Option<&str>, UdfError> {
        Ok(None)
    }
    fn emit(&mut self, _values: &[Value]) -> Result<(), UdfError> {
        Err(UdfError::User("raw path must use emit_batch".into()))
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        if self.served {
            Ok(false)
        } else {
            self.served = true;
            Ok(true)
        }
    }
    fn debug_level(&self) -> tracing::Level {
        tracing::Level::INFO
    }
    fn emit_record_batch_ipc(&mut self, ipc: &[u8]) -> Result<(), UdfError> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;
        let reader = StreamReader::try_new(Cursor::new(ipc), None)
            .map_err(|e| UdfError::User(format!("ipc decode: {e}")))?;
        for batch in reader {
            let batch = batch.map_err(|e| UdfError::User(format!("ipc batch: {e}")))?;
            self.emitted.push(batch);
        }
        Ok(())
    }
}

fn dummy_storage() -> StorageProps {
    // Multi-character placeholder credentials: value-based error redaction strips
    // any literal secret substring, so single-character creds would corrupt
    // English words in error messages (a test artifact, not a production concern).
    StorageProps {
        endpoint: "http://localhost:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        session_token: None,
        allow_http: true,
        path_style: true,
    }
}

fn local_file_size(file_url: &str) -> u64 {
    let path = Url::parse(file_url)
        .expect("valid file URL")
        .to_file_path()
        .expect("file:// URL");
    std::fs::metadata(path).expect("stat local file").len()
}

fn write_data_parquet(dir: &Path, relative: &str, ids: &[i64], row_group: usize) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let names: Vec<String> = ids.iter().map(|id| format!("row-{id}")).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn write_delete_parquet(dir: &Path, relative: &str, entries: &[(&str, i64)]) -> String {
    let field_id_meta =
        |id: i32| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false)
            .with_metadata(field_id_meta(FIELD_ID_POSITIONAL_DELETE_FILE_PATH)),
        Field::new("pos", DataType::Int64, false)
            .with_metadata(field_id_meta(FIELD_ID_POSITIONAL_DELETE_POS)),
    ]));
    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let paths: Vec<&str> = entries.iter().map(|(p, _)| *p).collect();
    let positions: Vec<i64> = entries.iter().map(|(_, pos)| *pos).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(paths)),
            Arc::new(Int64Array::from(positions)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

/// Encode a set of 64-bit positions into a spec-conformant `deletion-vector-v1`
/// blob payload (4-byte BE length | magic | portable Roaring vector | 4-byte BE
/// CRC-32), mirroring the layout `scan::deletion_vectors` decodes.
fn encode_dv(positions: &[u64]) -> Vec<u8> {
    const MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
    let mut buckets: BTreeMap<u32, RoaringBitmap> = BTreeMap::new();
    for &p in positions {
        buckets
            .entry((p >> 32) as u32)
            .or_default()
            .insert(p as u32);
    }
    let mut vector = Vec::new();
    vector.extend_from_slice(&(buckets.len() as u64).to_le_bytes());
    for (key, bitmap) in &buckets {
        vector.extend_from_slice(&key.to_le_bytes());
        bitmap.serialize_into(&mut vector).unwrap();
    }
    let mut magic_and_vector = Vec::with_capacity(4 + vector.len());
    magic_and_vector.extend_from_slice(&MAGIC);
    magic_and_vector.extend_from_slice(&vector);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&magic_and_vector);
    let crc = hasher.finalize();
    let mut blob = Vec::new();
    blob.extend_from_slice(&(magic_and_vector.len() as u32).to_be_bytes());
    blob.extend_from_slice(&magic_and_vector);
    blob.extend_from_slice(&crc.to_be_bytes());
    blob
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

/// Write a `deletion-vector-v1` Puffin file marking `positions` of
/// `referenced_data_file` deleted, then read it back to discover the blob's real
/// `(offset, length)`. Returns `(puffin_url, offset, length)`.
fn write_deletion_vector_puffin(
    dir: &Path,
    relative: &str,
    referenced_data_file: &str,
    positions: &[u64],
) -> (String, u64, u64) {
    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let puffin_url = Url::from_file_path(&path)
        .expect("absolute path")
        .to_string();

    let payload = encode_dv(positions);
    let mut properties = HashMap::new();
    properties.insert("cardinality".to_string(), positions.len().to_string());
    properties.insert(
        "referenced-data-file".to_string(),
        referenced_data_file.to_string(),
    );
    let blob = Blob::builder()
        .r#type(DELETION_VECTOR_V1.to_string())
        .fields(vec![])
        .snapshot_id(0)
        .sequence_number(0)
        .data(payload)
        .properties(properties)
        .build();

    block_on(async {
        let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
        let output = file_io.new_output(&puffin_url).expect("new_output");
        let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
            .await
            .expect("puffin writer");
        // deletion-vector-v1 blobs are never Puffin-compressed.
        writer
            .add(blob, CompressionCodec::None)
            .await
            .expect("add blob");
        writer.close().await.expect("close puffin");

        // Read back to discover the blob's real offset/length.
        let input = file_io.new_input(&puffin_url).expect("new_input");
        let reader = PuffinReader::new(input);
        let meta = reader.file_metadata().await.expect("file metadata");
        let blob_meta = meta.blobs().first().expect("one blob");
        (puffin_url.clone(), blob_meta.offset(), blob_meta.length())
    })
}

fn scan_spec(files: Vec<FileEntry>, filter: Option<String>, limit: Option<u64>) -> ScanSpec {
    ScanSpec {
        table_root: String::new(),
        files,
        projection: vec!["ID".into(), "NAME".into()],
        filter,
        limit,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: Vec::new(),
        logical_schema: Vec::new(),
        join: None,
        storage: dummy_storage(),
        df_target_partitions: 1,
        df_batch_size: 64,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    }
}

async fn try_run_scan(spec: &ScanSpec, register_url: &str) -> Result<Vec<RecordBatch>, UdfError> {
    let session = SessionContext::new_with_config(session_config_for_spec(spec));
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    session
        .runtime_env()
        .register_object_store(&Url::parse(register_url).expect("register url"), store);
    let mut ctx = FakeCtx::new();
    assert!(ctx.next().expect("next"), "one input row");
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, spec, &mut timers).await?;
    Ok(ctx.emitted)
}

fn run_scan(spec: &ScanSpec, register_url: &str) -> Vec<RecordBatch> {
    block_on(try_run_scan(spec, register_url)).expect("raw scan must succeed")
}

fn ids_of(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id col");
        for i in 0..b.num_rows() {
            out.push(ids.value(i));
        }
    }
    out.sort_unstable();
    out
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("lh_dv_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/// Scenario: a deletion vector removes exactly its flagged row positions.
#[test]
fn dv_removes_flagged_rows() {
    let dir = temp_dir("removes");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..20).collect::<Vec<_>>(), 8);
    // Delete positions 3 and 7 of the data file.
    let (puffin_url, offset, length) =
        write_deletion_vector_puffin(&dir, "dv.puffin", &data_url, &[3, 7]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![ResolvedDelete::deletion_vector(
            puffin_url,
            local_file_size(&data_url), // puffin size is resolved by the FileIO; any value works here
            offset,
            length,
        )],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    assert_eq!(total_rows(&rows), 18, "18 rows survive after 2 DV deletes");
    let ids = ids_of(&rows);
    assert_eq!(
        ids,
        (0..20).filter(|i| *i != 3 && *i != 7).collect::<Vec<_>>(),
        "positions 3 and 7 must be removed by the deletion vector"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a fully deleted data file yields no rows (and no error).
#[test]
fn dv_fully_deleted_file_empty() {
    let dir = temp_dir("fully_deleted");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..8).collect::<Vec<_>>(), 4);
    let all: Vec<u64> = (0..8).collect();
    let (puffin_url, offset, length) =
        write_deletion_vector_puffin(&dir, "dv.puffin", &data_url, &all);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![ResolvedDelete::deletion_vector(
            puffin_url, 4096, offset, length,
        )],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    assert_eq!(
        total_rows(&rows),
        0,
        "a fully-deleted file must emit no rows"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: deletion vectors compose with projection/filter pushdown +
/// row-group pruning, and separately with LIMIT pushdown, rather than disabling
/// either — the DV-derived base access plan and the opener's own pruning/limit
/// intersect to the correct final row set in both cases.
///
/// Filter and LIMIT are exercised in SEPARATE sub-scans (not combined in one
/// query): combining a WHERE predicate with LIMIT mis-orders results even with
/// NO deletes at all — a pre-existing scan-execution gap unrelated to delete
/// application (documented in `scan_positional_deletes.rs`).
#[test]
fn dv_composes_with_pushdown() {
    let dir = temp_dir("pushdown");
    // Small row groups (16 rows) so the predicate prunes whole row groups while
    // the DV-derived base access plan still carries the deletes.
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..100).collect::<Vec<_>>(), 16);
    // Delete positions 5, 50, 95 (ids 5, 50, 95 — position == id for this file).
    let (puffin_url, offset, length) =
        write_deletion_vector_puffin(&dir, "dv.puffin", &data_url, &[5, 50, 95]);
    let make_entry = || {
        FileEntry::with_deletes(
            data_url.clone(),
            local_file_size(&data_url),
            vec![ResolvedDelete::deletion_vector(
                puffin_url.clone(),
                4096,
                offset,
                length,
            )],
        )
    };

    // Filter pushdown + row-group pruning: keep ids >= 60 (prunes several whole
    // row groups); only id 95 is an in-range deleted position.
    let mut filter_spec = scan_spec(vec![make_entry()], Some("\"ID\" >= 60".to_string()), None);
    filter_spec.projection = vec![ProjectionItem::Column("ID".into())];
    let filter_rows = run_scan(&filter_spec, &data_url);
    let expected_filtered: Vec<i64> = (60..100).filter(|id| *id != 95).collect();
    assert_eq!(
        ids_of(&filter_rows),
        expected_filtered,
        "filter + row-group pruning must compose with the DV (only 95 was in-range)"
    );

    // LIMIT pushdown: the first rows the limit observes are already post-delete,
    // so position 5 (id 5) is removed BEFORE the limit counts rows.
    let mut limit_spec = scan_spec(vec![make_entry()], None, Some(10));
    limit_spec.projection = vec![ProjectionItem::Column("ID".into())];
    let limit_rows = run_scan(&limit_spec, &data_url);
    let limit_ids = ids_of(&limit_rows);
    assert_eq!(limit_ids.len(), 10, "LIMIT 10 over post-delete rows");
    assert!(
        !limit_ids.contains(&5),
        "the deleted position 5 must not appear even under LIMIT: {limit_ids:?}"
    );
    // The first 10 post-delete rows are ids {0,1,2,3,4,6,7,8,9,10}.
    assert_eq!(
        limit_ids,
        vec![0, 1, 2, 3, 4, 6, 7, 8, 9, 10],
        "LIMIT must observe post-delete rows in order"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a mixed shard — one data file backed by a Parquet positional delete
/// and another backed by a deletion vector — resolves each mechanism per file.
#[test]
fn mixed_mechanisms_resolve_per_file() {
    let dir = temp_dir("mixed");
    let f_pos = write_data_parquet(&dir, "pos/data.parquet", &(0..10).collect::<Vec<_>>(), 4);
    let f_dv = write_data_parquet(&dir, "dv/data.parquet", &(100..110).collect::<Vec<_>>(), 4);

    // Positional delete removes positions 2 and 5 of f_pos (ids 2 and 5).
    let delete_url = write_delete_parquet(&dir, "pos/deletes.parquet", &[(&f_pos, 2), (&f_pos, 5)]);
    // Deletion vector removes positions 1 and 8 of f_dv (ids 101 and 108).
    let (puffin_url, offset, length) =
        write_deletion_vector_puffin(&dir, "dv/dv.puffin", &f_dv, &[1, 8]);

    let entries = vec![
        FileEntry::with_deletes(
            f_pos.clone(),
            local_file_size(&f_pos),
            vec![ResolvedDelete::position(
                delete_url.clone(),
                local_file_size(&delete_url),
            )],
        ),
        FileEntry::with_deletes(
            f_dv.clone(),
            local_file_size(&f_dv),
            vec![ResolvedDelete::deletion_vector(
                puffin_url, 4096, offset, length,
            )],
        ),
    ];
    let spec = scan_spec(entries, None, None);
    let rows = run_scan(&spec, &f_pos);

    let ids = ids_of(&rows);
    let mut expected: Vec<i64> = (0..10).filter(|i| *i != 2 && *i != 5).collect();
    expected.extend((100..110).filter(|i| *i != 101 && *i != 108));
    expected.sort_unstable();
    assert_eq!(
        ids, expected,
        "each file's own mechanism must apply independently"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a referenced-data-file mismatch fails loud (the blob names a
/// different data file than the one it is applied to).
#[test]
fn dv_referenced_data_file_mismatch_errors() {
    let dir = temp_dir("mismatch");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 4);
    let other_url = write_data_parquet(&dir, "other.parquet", &(0..10).collect::<Vec<_>>(), 4);
    // The blob's referenced-data-file is `other_url`, but we apply it to `data_url`.
    let (puffin_url, offset, length) =
        write_deletion_vector_puffin(&dir, "dv.puffin", &other_url, &[1, 2]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![ResolvedDelete::deletion_vector(
            puffin_url, 4096, offset, length,
        )],
    );
    let spec = scan_spec(vec![entry], None, None);

    let err = block_on(try_run_scan(&spec, &data_url))
        .expect_err("a referenced-data-file mismatch must fail loud");
    let msg = err.to_string();
    assert!(
        msg.contains("referenced-data-file mismatch"),
        "error must name the mismatch: {msg}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

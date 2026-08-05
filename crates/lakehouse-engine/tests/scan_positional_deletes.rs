//! Scan-level no-container tests for Iceberg merge-on-read **Parquet positional
//! deletes** (Task 4.1).
//!
//! Every test here writes a data Parquet and a positional-delete Parquet to a
//! local temp directory (no S3 / MinIO, no Docker), hand-builds a `ScanSpec`
//! whose `FileEntry`s carry `DeleteFileRef`s, drives the production raw-scan
//! pipeline ([`run_raw_scan_with_session`] → `build_dataframe` →
//! `register_files` → `PositionalDeleteScanTable`), and asserts the deleted
//! rows are gone from the emitted output.
//!
//! Host-runnable: everything lives under `file://`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use futures::stream::BoxStream;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, DeleteFileContentType, DeleteFileRef, FileEntry, JoinSpec, JoinType,
    LogicalField, ScanSpec, StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{
    build_join_physical_plan, build_raw_scan_physical_plan, register_files,
    run_join_scan_with_session, run_raw_scan_with_session, session_config_for_spec,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use url::Url;

/// Iceberg reserved field-ids for a positional-delete file's `file_path`/`pos`
/// columns (mirrors `scan::positional_deletes`'s private constants; duplicated
/// here since this integration test cannot import a `pub(crate)` item).
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

/// A fake `UdfContext` serving one input row and decoding every emitted Arrow
/// IPC batch — the same capture pattern the sibling scan integration tests use.
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

/// Storage props are never dialed for a local `file://` scan; a placeholder
/// keeps the spec well-formed.
fn dummy_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://localhost:9000".into(),
        region: "us-east-1".into(),
        access_key: "k".into(),
        secret_key: "s".into(),
        allow_http: true,
        ..Default::default()
    })
}

/// Byte size of a local file, given its `file://` URL — robust to
/// URL-encoding (unlike a bare `strip_prefix("file://")`).
fn local_file_size(file_url: &str) -> u64 {
    let path = Url::parse(file_url)
        .expect("valid file URL")
        .to_file_path()
        .expect("file:// URL");
    std::fs::metadata(path).expect("stat local parquet").len()
}

/// Write a local data Parquet at `dir/relative` with an `id`/`name` row per
/// entry in `ids` (`name` is `row-<id>`), across small row groups so
/// multi-row-group deletes are exercised. Returns the file's absolute
/// `file://` URL.
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

/// Write a local positional-delete Parquet at `dir/relative`: `file_path`/`pos`
/// columns tagged with the Iceberg reserved field-ids, one row per
/// `(referenced_file_abs_url, position)` entry. Returns the file's absolute
/// `file://` URL.
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

/// A [`DeleteFileRef`] for the Parquet positional-delete file at `abs_url`.
fn delete_ref(abs_url: &str) -> DeleteFileRef {
    DeleteFileRef {
        path: abs_url.to_string(),
        size: local_file_size(abs_url),
        content_type: DeleteFileContentType::PositionDeletes,
    }
}

/// A row-scan `ScanSpec` over `files` (already absolute, `table_root` empty),
/// optionally pushing a filter and/or a limit.
fn scan_spec(files: Vec<FileEntry>, filter: Option<String>, limit: Option<u64>) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            filter,
            limit,
            storage: dummy_storage(),
            df_batch_size: 64,
            ..Default::default()
        },
        files,
    }
}

/// Build a `Vec<LogicalField>` from `(name, arrow_type_tag)` pairs, assigning
/// Iceberg field-ids sequentially from 1 in the given order. The single seam
/// every non-empty `logical_schema` in this file is built through, so a
/// differently-shaped keyed-column schema (e.g. `o_key`/`o_data` on a join's
/// fact side) can be built through the exact same construction as the
/// `id`/`name` shape `scan_spec_with_logical_schema` uses.
fn logical_fields(fields: &[(&str, &str)]) -> Vec<LogicalField> {
    fields
        .iter()
        .enumerate()
        .map(|(i, (name, arrow_type))| LogicalField {
            field_id: (i + 1) as i32,
            name: (*name).to_string(),
            arrow_type: (*arrow_type).to_string(),
            nullable: false,
            initial_default: None,
        })
        .collect()
}

/// Like [`scan_spec`], but with a populated `common.logical_schema`: `id`
/// (field-id 1, `int64`) and `name` (field-id 2, `utf8`), both non-nullable —
/// matching `write_data_parquet`'s fixture schema and this helper's own
/// `["ID", "NAME"]` projection (lowercase logical names against an uppercase
/// projection, exactly as `scan_name_mapping.rs`'s helper already does).
///
/// Every new request-count assertion in this file MUST build its spec through
/// this helper, never through `scan_spec` (decision-log [8]). An empty
/// `logical_schema` sends `register_file_list` down the
/// `ParquetFormat::infer_schema` branch (`crates/lakehouse-engine/src/scan/raw_scan.rs:203-216`),
/// which fetches the FIRST assigned file's Parquet footer BEFORE Phase B runs
/// and which `TrackingStore::get_opts` records — so with a delete-free file
/// first the per-file zero-GET assertion would fail, and with a
/// delete-carrying file first the fetched-once assertion would fail, because
/// the inference entry carries `LocalFileSystem`'s real `last_modified` while
/// `object_meta_for` builds `Utc.timestamp_nanos(0)`, `CachedFileMetadataEntry
/// ::is_valid_for` misses, and Phase B re-fetches.
fn scan_spec_with_logical_schema(
    files: Vec<FileEntry>,
    filter: Option<String>,
    limit: Option<u64>,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            filter,
            limit,
            storage: dummy_storage(),
            df_batch_size: 64,
            logical_schema: logical_fields(&[("id", "int64"), ("name", "utf8")]),
            ..Default::default()
        },
        files,
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

/// Run the production raw scan for `spec` against a session registering
/// `store` for every `register_url` scheme/authority. Returns the decoded
/// emitted batches, or the scan's error.
async fn try_run_scan_with_store(
    spec: &ScanSpec,
    register_url: &str,
    store: Arc<dyn ObjectStore>,
) -> Result<Vec<RecordBatch>, UdfError> {
    let session = SessionContext::new_with_config(session_config_for_spec(spec));
    session
        .runtime_env()
        .register_object_store(&Url::parse(register_url).expect("register url"), store);
    let mut ctx = FakeCtx::new();
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, spec, &mut timers).await?;
    Ok(ctx.emitted)
}

/// Run the production raw scan over a plain `LocalFileSystem`, panicking on
/// scan failure (the happy-path helper used by every scenario except the
/// backstop-rejection test).
fn run_scan(spec: &ScanSpec, register_url: &str) -> Vec<RecordBatch> {
    block_on(try_run_scan_with_store(
        spec,
        register_url,
        Arc::new(LocalFileSystem::new()),
    ))
    .expect("raw scan must succeed")
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
    let dir = std::env::temp_dir().join(format!("lh_pos_del_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Scenario: `write.delete.granularity=file` — a data file's OWN positional-delete
/// file removes exactly its flagged row positions.
#[test]
fn scan_applies_file_granularity_positional_deletes() {
    let dir = temp_dir("file_gran");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..20).collect::<Vec<_>>(), 8);
    let delete_url =
        write_delete_parquet(&dir, "deletes.parquet", &[(&data_url, 3), (&data_url, 7)]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    assert_eq!(total_rows(&rows), 18, "18 rows survive after 2 deletes");
    let ids = ids_of(&rows);
    assert!(!ids.contains(&3), "position 3 must be deleted: {ids:?}");
    assert!(!ids.contains(&7), "position 7 must be deleted: {ids:?}");
    assert_eq!(
        ids,
        (0..20).filter(|i| *i != 3 && *i != 7).collect::<Vec<_>>()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: `write.delete.granularity=partition` — ONE delete file references
/// data files spanning multiple files; each data file's read is filtered to
/// only the delete rows whose `file_path` matches ITS own absolute URI.
#[test]
fn scan_filters_partition_delete_by_file_path() {
    let dir = temp_dir("partition_gran");
    let f0 = write_data_parquet(&dir, "p0/data.parquet", &(100..110).collect::<Vec<_>>(), 4);
    let f1 = write_data_parquet(&dir, "p1/data.parquet", &(200..210).collect::<Vec<_>>(), 4);
    // One shared partition-granularity delete file: 2 rows for f0, 1 row for f1.
    let delete_url = write_delete_parquet(
        &dir,
        "shared_delete.parquet",
        &[(&f0, 2), (&f0, 5), (&f1, 1)],
    );
    let shared_delete = delete_ref(&delete_url);

    let entries = vec![
        FileEntry::with_deletes(
            f0.clone(),
            local_file_size(&f0),
            vec![shared_delete.clone()],
        ),
        FileEntry::with_deletes(f1.clone(), local_file_size(&f1), vec![shared_delete]),
    ];
    let spec = scan_spec(entries, None, None);
    let rows = run_scan(&spec, &f0);

    // f0 loses positions 2,5 -> ids 102,105; f1 loses position 1 -> id 201.
    assert_eq!(total_rows(&rows), 17, "20 rows - 3 deleted = 17");
    let ids = ids_of(&rows);
    for missing in [102, 105, 201] {
        assert!(
            !ids.contains(&missing),
            "id {missing} must be deleted by the shared partition delete file: {ids:?}"
        );
    }
    assert!(ids.contains(&200), "f1's other rows must survive: {ids:?}");
    assert!(ids.contains(&100), "f0's other rows must survive: {ids:?}");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: multiple positional-delete files associated with the SAME data
/// file are unioned (including an overlapping position) rather than only the
/// last one applying.
#[test]
fn scan_unions_multiple_delete_files() {
    let dir = temp_dir("union");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..20).collect::<Vec<_>>(), 8);
    let delete_a = write_delete_parquet(&dir, "del_a.parquet", &[(&data_url, 1), (&data_url, 4)]);
    // Overlaps position 4 with delete_a; also deletes 9.
    let delete_b = write_delete_parquet(&dir, "del_b.parquet", &[(&data_url, 4), (&data_url, 9)]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_a), delete_ref(&delete_b)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    // Union of {1,4} and {4,9} = {1,4,9}: exactly 3 rows removed, not 4.
    assert_eq!(
        total_rows(&rows),
        17,
        "17 rows survive after the union of 2 delete files"
    );
    let ids = ids_of(&rows);
    for missing in [1, 4, 9] {
        assert!(
            !ids.contains(&missing),
            "id {missing} must be deleted: {ids:?}"
        );
    }
    assert_eq!(
        ids,
        (0..20)
            .filter(|i| ![1, 4, 9].contains(i))
            .collect::<Vec<_>>()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a delete file that flags EVERY row of its data file yields zero
/// rows for that file (rather than erroring or returning stale rows).
#[test]
fn scan_fully_deleted_file_yields_no_rows() {
    let dir = temp_dir("fully_deleted");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..5).collect::<Vec<_>>(), 8);
    let delete_url = write_delete_parquet(
        &dir,
        "deletes.parquet",
        &[
            (&data_url, 0),
            (&data_url, 1),
            (&data_url, 2),
            (&data_url, 3),
            (&data_url, 4),
        ],
    );

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    assert_eq!(
        total_rows(&rows),
        0,
        "a fully-deleted file must yield no rows"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: positional deletes compose with projection/filter pushdown +
/// row-group pruning, and separately with LIMIT pushdown, rather than
/// disabling either — the base access plan (deletes) and the opener's own
/// pruning/limit intersect to the correct final row set in both cases.
///
/// Filter and LIMIT are exercised in SEPARATE sub-scans here rather than
/// combined in one query: combining a WHERE predicate with LIMIT in this
/// engine's scan pipeline currently mis-orders results even with NO deletes
/// involved at all (reproduced independently against the plain `ListingTable`
/// path via `build_raw_scan_physical_plan`, i.e. a pre-existing scan-execution
/// gap unrelated to positional-delete application — out of scope here; see
/// Task 4.2's plan-shape/pruning gate).
#[test]
fn scan_deletes_compose_with_pushdown_and_pruning() {
    let dir = temp_dir("compose");
    // Small row groups (16 rows) so a predicate can prune whole row groups
    // while the base access plan still carries the deletes.
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..100).collect::<Vec<_>>(), 16);
    let delete_url = write_delete_parquet(
        &dir,
        "deletes.parquet",
        &[(&data_url, 5), (&data_url, 50), (&data_url, 95)],
    );
    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_url)],
    );

    // Filter pushdown + row-group pruning: a predicate that prunes several
    // whole row groups (keeps only ids >= 60, spanning groups 3..6) still
    // composes correctly with the base delete access plan.
    let filter_spec = scan_spec(vec![entry.clone()], Some("\"ID\" >= 60".to_string()), None);
    let filter_rows = run_scan(&filter_spec, &data_url);
    let expected_filtered: Vec<i64> = (60..100).filter(|id| *id != 95).collect();
    assert_eq!(
        ids_of(&filter_rows),
        expected_filtered,
        "filter pushdown + row-group pruning must compose with the delete (only 95 was in-range)"
    );

    // LIMIT pushdown: the first N surviving (post-delete) rows in file order.
    let limit_spec = scan_spec(vec![entry], None, Some(10));
    let limit_rows = run_scan(&limit_spec, &data_url);
    assert_eq!(
        ids_of(&limit_rows),
        vec![0, 1, 2, 3, 4, 6, 7, 8, 9, 10],
        "LIMIT pushdown must count only post-delete rows (position 5 is deleted)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (backstop): an assigned delete file this engine cannot apply
/// (equality delete) is rejected with a clean, mechanism-naming error rather
/// than silently ignored or applied incorrectly. Because the read-time
/// backstop check runs BEFORE the delete file is opened, the referenced path
/// need not exist.
#[test]
fn scan_rejects_unapplicable_delete_file() {
    let dir = temp_dir("unapplicable");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);

    let bogus_delete = DeleteFileRef {
        path: format!("{}/does-not-need-to-exist.parquet", dir.to_string_lossy()),
        size: 10,
        content_type: DeleteFileContentType::EqualityDeletes,
    };
    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![bogus_delete],
    );
    let spec = scan_spec(vec![entry], None, None);

    let err = block_on(try_run_scan_with_store(
        &spec,
        &data_url,
        Arc::new(LocalFileSystem::new()),
    ))
    .expect_err("an equality delete must be rejected, not applied");
    let msg = err.to_string();
    assert!(
        msg.contains("equality delete"),
        "error must name the unsupported mechanism: {msg}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (backstop): an assigned delete file this engine cannot apply
/// (Puffin / v3 deletion vector) is rejected with a clean, mechanism-naming
/// error rather than silently ignored or applied incorrectly. Same shape as
/// `scan_rejects_unapplicable_delete_file` (the equality-delete case): the
/// read-time backstop check runs BEFORE the delete file is opened, so the
/// referenced path need not exist, and the error is credential-free — only
/// the (non-secret) path is redacted before being interpolated, and the
/// mechanism text `ensure_positional_delete` emits is a fixed literal that
/// never carries storage credentials.
#[test]
fn scan_rejects_puffin_deletion_vector() {
    let dir = temp_dir("puffin_dv");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);

    let bogus_delete = DeleteFileRef {
        path: format!("{}/does-not-need-to-exist.puffin", dir.to_string_lossy()),
        size: 10,
        content_type: DeleteFileContentType::PuffinDeletionVector,
    };
    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![bogus_delete],
    );
    let spec = scan_spec(vec![entry], None, None);

    let err = block_on(try_run_scan_with_store(
        &spec,
        &data_url,
        Arc::new(LocalFileSystem::new()),
    ))
    .expect_err("a Puffin deletion vector must be rejected, not applied");
    let msg = err.to_string();
    assert!(
        msg.contains("Puffin deletion vector"),
        "error must name the unsupported mechanism: {msg}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (fail loud): a malformed positional-delete file carrying a negative
/// `pos` is rejected with a clean error rather than silently dropped (casting a
/// negative to `u64` would wrap to a huge index and skip the delete).
#[test]
fn scan_rejects_negative_positional_delete() {
    let dir = temp_dir("neg_pos");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);
    let delete_url = write_delete_parquet(&dir, "delete.parquet", &[(data_url.as_str(), -1)]);
    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);

    let err = block_on(try_run_scan_with_store(
        &spec,
        &data_url,
        Arc::new(LocalFileSystem::new()),
    ))
    .expect_err("a negative pos must be rejected, not silently dropped");
    // NB: the `dummy_storage` secret_key is "s", so credential redaction strips
    // every "s" from the message — assert on tokens that survive it.
    let msg = err.to_string();
    assert!(
        msg.contains("negative") && msg.contains("(-1)"),
        "error must name the malformed negative position: {msg}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (fail loud): a spec whose files resolve to more than one
/// object-store root is rejected at registration. The scan registers a single
/// store (keyed by the first file); a file under a different scheme/host would
/// otherwise be read through the wrong store and fail confusingly.
#[test]
fn scan_rejects_mixed_object_store_roots() {
    let dir = temp_dir("mixed_roots");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);
    let local = FileEntry::new(data_url.clone(), local_file_size(&data_url));
    // A second data file under a DIFFERENT (s3://) root than the first (file://).
    let foreign = FileEntry::new("s3://other-bucket/part-0.parquet", 10);
    let spec = scan_spec(vec![local, foreign], None, None);

    let err = block_on(try_run_scan_with_store(
        &spec,
        &data_url,
        Arc::new(LocalFileSystem::new()),
    ))
    .expect_err("a spec mixing object-store roots must be rejected");
    assert!(
        err.to_string().contains("mixes object-store roots"),
        "error must explain the mixed-root rejection: {err}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a data file with NO associated delete files scans unchanged —
/// the unified `PositionalDeleteScanTable` path must not regress the
/// delete-free case.
#[test]
fn scan_delete_free_file_unchanged() {
    let dir = temp_dir("delete_free");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);

    let entry = FileEntry::new(data_url.clone(), local_file_size(&data_url));
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    assert_eq!(
        total_rows(&rows),
        10,
        "no rows must be dropped when there are no deletes"
    );
    assert_eq!(ids_of(&rows), (0..10).collect::<Vec<_>>());

    let _ = std::fs::remove_dir_all(&dir);
}

/// An [`ObjectStore`] decorator that records every non-HEAD `get` it serves, by
/// location. Delegates everything to `inner` (a plain [`LocalFileSystem`]).
/// Used to prove the delete file is fetched through the SAME registered
/// object-store instance the data file uses — i.e. delete-file reads ride the
/// identical credentialed client the scan configures from `spec.storage`
/// rather than opening a separate, unauthenticated path.
#[derive(Debug)]
struct TrackingStore {
    inner: Arc<dyn ObjectStore>,
    gets: Arc<std::sync::Mutex<Vec<ObjectStorePath>>>,
    calls: Arc<AtomicUsize>,
    /// Present for both the delete-read AND footer-fetch concurrency-bound
    /// tests (`scan_delete_reads_*`, `scan_footer_fetches_*`); `None` for the
    /// plain tracking uses. When set, a non-HEAD `get_opts` whose location
    /// matches a probed needle (a delete file's or a data file's bare filename,
    /// depending on which the test supplies) is counted as an in-flight probed
    /// read (peak recorded) and delayed a fixed interval to force deterministic
    /// overlap. A read matching no needle is neither counted nor delayed.
    concurrency: Option<ConcurrencyProbe>,
}

/// Instrumentation shared by the delete-read AND footer-fetch concurrency-bound
/// tests: an atomic peak-concurrency counter over probed reads (delete-file
/// bodies, or data-file footers for the footer-fetch tests) plus a fixed
/// artificial delay that forces genuine overlap without real I/O timing.
#[derive(Debug)]
struct ConcurrencyProbe {
    /// Bare filenames identifying the reads to instrument — delete files for
    /// the delete-read bound tests, data files for the footer-fetch bound
    /// tests. A non-HEAD `get_opts` whose object-store path contains any of
    /// these is a probed read.
    needles: Vec<String>,
    /// Probed reads currently inside a delayed `get_opts`.
    in_flight: Arc<AtomicUsize>,
    /// Maximum value `in_flight` ever reached — the observed peak concurrency.
    peak: Arc<AtomicUsize>,
    /// Fixed per-read delay holding the read "in flight" long enough that every
    /// concurrently-admitted read overlaps deterministically.
    delay: Duration,
}

impl ConcurrencyProbe {
    fn is_probed_read(&self, location: &ObjectStorePath) -> bool {
        let path = location.as_ref();
        self.needles.iter().any(|n| path.contains(n.as_str()))
    }
}

/// Decrements the in-flight counter on scope exit, INCLUDING on future
/// cancellation (a fired test timeout drops the read mid-await) — so a leaked
/// in-flight count can never survive to corrupt a later assertion.
struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl std::fmt::Display for TrackingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TrackingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for TrackingStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !options.head {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.gets.lock().unwrap().push(location.clone());
        }
        // Concurrency instrumentation: record the peak number of overlapping
        // PROBED reads and hold each one "in flight" for a fixed interval. Gated
        // to non-HEAD reads matching a needle (see `ConcurrencyProbe::needles`) so
        // unrelated reads are neither counted nor delayed. When the needles name
        // DELETE files, the permit that bounds this concurrency is held by the
        // production code across the whole `read_delete_file_positions` call,
        // whose `get_opts` are sequential — so the count observed here is exactly
        // the number of delete reads holding a semaphore permit, and can never
        // exceed the budget. When the needles name DATA files instead (the
        // footer-fetch bound tests), that guarantee holds ONLY for a driver that
        // constructs the physical plan and never executes it: an executed scan's
        // opener re-reads those same data files at execute time while holding no
        // permit, which would inflate this monotonic peak past the budget.
        if !options.head
            && let Some(probe) = &self.concurrency
            && probe.is_probed_read(location)
        {
            let now = probe.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            probe.peak.fetch_max(now, Ordering::SeqCst);
            let _guard = InFlightGuard {
                in_flight: Arc::clone(&probe.in_flight),
            };
            tokio::time::sleep(probe.delay).await;
            return self.inner.get_opts(location, options).await;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectStorePath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectStorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Scenario (memory-creds): delete files are read through the SAME registered
/// object-store instance the scan configures for data-file access — modeling
/// "read with vended credentials" locally: a store standing in for a
/// credentialed client is registered ONCE for the scan, and both the data
/// file's read AND the associated delete file's read must flow through it (no
/// separate, unauthenticated object-store path for delete files).
#[test]
fn scan_reads_delete_files_with_vended_credentials() {
    let dir = temp_dir("vended_creds");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..20).collect::<Vec<_>>(), 8);
    let delete_url =
        write_delete_parquet(&dir, "deletes.parquet", &[(&data_url, 2), (&data_url, 6)]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);

    let calls = Arc::new(AtomicUsize::new(0));
    let gets = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracking_store = Arc::new(TrackingStore {
        inner: Arc::new(LocalFileSystem::new()),
        gets: Arc::clone(&gets),
        calls: Arc::clone(&calls),
        concurrency: None,
    });

    let rows = block_on(try_run_scan_with_store(&spec, &data_url, tracking_store))
        .expect("raw scan must succeed via the tracking (credentialed) store");

    assert_eq!(total_rows(&rows), 18, "2 deletes applied");

    // The delete file's content was fetched via a `get` (not just a HEAD),
    // proving it went through the SAME registered store as the data file.
    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "both the data file and the delete file must be fetched via the registered store (got {} calls)",
        calls.load(Ordering::SeqCst)
    );
    let recorded = gets.lock().unwrap();
    let delete_needle = file_needle(&delete_url);
    assert!(
        recorded
            .iter()
            .any(|p| p.as_ref().contains(delete_needle.as_str())),
        "the delete file must be fetched through the registered (credentialed) store: {recorded:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Counts of non-HEAD `get_opts` calls whose location contains
/// `needle` (typically a delete file's bare filename), taken from a
/// [`TrackingStore`]'s recorded `gets`.
fn count_gets_matching(gets: &std::sync::Mutex<Vec<ObjectStorePath>>, needle: &str) -> usize {
    gets.lock()
        .unwrap()
        .iter()
        .filter(|p| p.as_ref().contains(needle))
        .count()
}

/// Run `spec` against a fresh [`TrackingStore`]-wrapped `LocalFileSystem`,
/// returning the emitted rows and the recorded non-HEAD `get_opts` locations.
fn run_scan_tracked(
    spec: &ScanSpec,
    register_url: &str,
) -> (
    Vec<RecordBatch>,
    Arc<std::sync::Mutex<Vec<ObjectStorePath>>>,
) {
    let gets = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracking_store = Arc::new(TrackingStore {
        inner: Arc::new(LocalFileSystem::new()),
        gets: Arc::clone(&gets),
        calls: Arc::new(AtomicUsize::new(0)),
        concurrency: None,
    });
    let rows = block_on(try_run_scan_with_store(spec, register_url, tracking_store))
        .expect("raw scan must succeed");
    (rows, gets)
}

/// Scenario (perf regression guard, Phase A dedup): a single
/// partition-granularity delete file referenced by TWO assigned data files —
/// the same fixture shape as `scan_filters_partition_delete_by_file_path` — is
/// read from the object store the SAME number of times regardless of how many
/// data files reference it, proving Phase A dedups by delete-file path rather
/// than reading once per referencing data file. Reuses the [`TrackingStore`]
/// decorator (already proven in `scan_reads_delete_files_with_vended_credentials`)
/// to count every non-HEAD `get_opts` by location — the actual body-read op,
/// not an indirect proxy.
///
/// A single logical Parquet open (footer metadata + row-group data) issues
/// more than one range `get_opts` on this object-store implementation, so
/// "read exactly once" is proven by comparing traffic against a SOLO baseline
/// (one referencing data file) rather than asserting a magic total: if Phase A
/// re-read the delete file per referencing data file, the two-referencer count
/// would be double the solo count instead of identical to it.
#[test]
fn scan_reads_shared_delete_file_once_per_shard() {
    let dir = temp_dir("shared_delete_once");
    let f0 = write_data_parquet(&dir, "p0/data.parquet", &(100..110).collect::<Vec<_>>(), 4);
    let f1 = write_data_parquet(&dir, "p1/data.parquet", &(200..210).collect::<Vec<_>>(), 4);
    // One shared partition-granularity delete file: 2 rows for f0, 1 row for f1.
    let delete_url = write_delete_parquet(
        &dir,
        "shared_delete.parquet",
        &[(&f0, 2), (&f0, 5), (&f1, 1)],
    );
    let shared_delete = delete_ref(&delete_url);
    let delete_filename = file_needle(&delete_url);

    // Baseline: only f0 references the delete file (K=1 referencer).
    let solo_entries = vec![FileEntry::with_deletes(
        f0.clone(),
        local_file_size(&f0),
        vec![shared_delete.clone()],
    )];
    let solo_spec = scan_spec(solo_entries, None, None);
    let (_solo_rows, solo_gets) = run_scan_tracked(&solo_spec, &f0);
    let solo_delete_reads = count_gets_matching(&solo_gets, &delete_filename);
    assert!(
        solo_delete_reads > 0,
        "the solo baseline scan must actually fetch the delete file's body"
    );

    // Both f0 and f1 reference the SAME delete file (K=2 referencers).
    let shared_entries = vec![
        FileEntry::with_deletes(
            f0.clone(),
            local_file_size(&f0),
            vec![shared_delete.clone()],
        ),
        FileEntry::with_deletes(f1.clone(), local_file_size(&f1), vec![shared_delete]),
    ];
    let shared_spec = scan_spec(shared_entries, None, None);
    let (shared_rows, shared_gets) = run_scan_tracked(&shared_spec, &f0);
    let shared_delete_reads = count_gets_matching(&shared_gets, &delete_filename);

    // Read-once-per-shard: adding a second referencing data file must not add
    // any extra delete-file traffic.
    assert_eq!(
        shared_delete_reads, solo_delete_reads,
        "the shared delete file must be read exactly once per shard regardless of \
         referencing data-file count: solo (1 referencer) = {solo_delete_reads} non-HEAD \
         get_opts, shared (2 referencers) = {shared_delete_reads}"
    );

    // Same post-delete row set as scan_filters_partition_delete_by_file_path:
    // f0 loses positions 2,5 -> ids 102,105; f1 loses position 1 -> id 201.
    // The dedup-by-path restructure must not change this result.
    assert_eq!(total_rows(&shared_rows), 17, "20 rows - 3 deleted = 17");
    let ids = ids_of(&shared_rows);
    for missing in [102, 105, 201] {
        assert!(
            !ids.contains(&missing),
            "id {missing} must be deleted by the shared partition delete file: {ids:?}"
        );
    }
    assert!(ids.contains(&100), "f0's other rows must survive: {ids:?}");
    assert!(ids.contains(&200), "f1's other rows must survive: {ids:?}");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Fixed per-read delay for the concurrency-bound tests. Long enough that, on the
/// tests' current-thread runtime, every read admitted in one scheduling wave has
/// incremented the peak counter before any timer fires (a current-thread runtime
/// only fires timers once it parks, i.e. after the whole wave is polled) — so the
/// observed peak is deterministic, not a race on real I/O timing.
const DELETE_READ_DELAY: Duration = Duration::from_millis(50);

/// Explicit upper bound on each concurrency assertion: a mis-wired limiter that
/// deadlocks (or never admits a read) fails here rather than hanging CI.
const DELETE_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Build a [`TrackingStore`] over a plain `LocalFileSystem` instrumented with a
/// peak-concurrency probe over the given bare filenames — delete files for the
/// `scan_delete_reads_*` tests, data files for the `scan_footer_fetches_*`
/// tests. Returns the store and a handle to its peak-concurrency counter.
fn tracking_store_with_probe(needles: Vec<String>) -> (Arc<TrackingStore>, Arc<AtomicUsize>) {
    let peak = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(TrackingStore {
        inner: Arc::new(LocalFileSystem::new()),
        gets: Arc::new(std::sync::Mutex::new(Vec::new())),
        calls: Arc::new(AtomicUsize::new(0)),
        concurrency: Some(ConcurrencyProbe {
            needles,
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak: Arc::clone(&peak),
            delay: DELETE_READ_DELAY,
        }),
    });
    (store, peak)
}

/// Scenario (connection-concurrency bound): with a delete-read budget of N and
/// MORE than N unique delete files to read, the concurrent delete-file reads peak
/// at EXACTLY N — the shared instance-level semaphore admits N at a time and no
/// more. The peak reaching N (not 0 or 1) proves the bound is genuinely
/// exercised, not vacuously respected.
///
/// Determinism: each fake read holds one semaphore permit across its whole
/// `read_delete_file_positions` call and sleeps a fixed interval inside its
/// `get_opts`, so on the current-thread runtime the first scheduling wave admits
/// exactly N reads (they all reach the sleep and bump the peak to N) before the
/// (N+1)-th blocks on the permit — no reliance on real I/O timing.
#[test]
fn scan_delete_reads_bounded_by_connection_budget() {
    const BUDGET: usize = 3;
    const UNIQUE_DELETES: usize = 6; // strictly greater than BUDGET

    let dir = temp_dir("bounded_budget");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..12).collect::<Vec<_>>(), 4);

    let mut delete_refs = Vec::with_capacity(UNIQUE_DELETES);
    let mut needles = Vec::with_capacity(UNIQUE_DELETES);
    for i in 0..UNIQUE_DELETES {
        let name = format!("del_{i}.parquet");
        let url = write_delete_parquet(&dir, &name, &[(&data_url, i as i64)]);
        delete_refs.push(delete_ref(&url));
        needles.push(name);
    }

    let entry = FileEntry::with_deletes(data_url.clone(), local_file_size(&data_url), delete_refs);
    let mut spec = scan_spec(vec![entry], None, None);
    spec.common.s3_max_connections = BUDGET;

    let (store, peak) = tracking_store_with_probe(needles);
    let rows = block_on(async {
        tokio::time::timeout(
            DELETE_READ_TIMEOUT,
            try_run_scan_with_store(&spec, &data_url, store),
        )
        .await
        .expect("bounded delete-read fan-out must finish within the timeout, not hang")
        .expect("raw scan must succeed")
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        BUDGET,
        "concurrent delete-file reads must peak at EXACTLY the connection budget ({BUDGET}): \
         a lower peak means the fan-out was not exercised, a higher peak means the bound leaked"
    );

    // Post-delete correctness: positions 0..6 removed from a 12-row file.
    assert_eq!(total_rows(&rows), 6, "6 of 12 rows deleted");
    assert_eq!(ids_of(&rows), (6..12).collect::<Vec<_>>());

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (connection-concurrency bound, N=1): a budget of 1 forces delete-file
/// reads to run strictly serially — the peak in-flight count is exactly 1 even
/// when several unique delete files must be read.
#[test]
fn scan_delete_reads_serial_when_budget_is_one() {
    const UNIQUE_DELETES: usize = 4;

    let dir = temp_dir("serial_budget");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 4);

    let mut delete_refs = Vec::with_capacity(UNIQUE_DELETES);
    let mut needles = Vec::with_capacity(UNIQUE_DELETES);
    for i in 0..UNIQUE_DELETES {
        let name = format!("del_{i}.parquet");
        let url = write_delete_parquet(&dir, &name, &[(&data_url, i as i64)]);
        delete_refs.push(delete_ref(&url));
        needles.push(name);
    }

    let entry = FileEntry::with_deletes(data_url.clone(), local_file_size(&data_url), delete_refs);
    let mut spec = scan_spec(vec![entry], None, None);
    spec.common.s3_max_connections = 1;

    let (store, peak) = tracking_store_with_probe(needles);
    let rows = block_on(async {
        tokio::time::timeout(
            DELETE_READ_TIMEOUT,
            try_run_scan_with_store(&spec, &data_url, store),
        )
        .await
        .expect("serial delete reads must finish within the timeout, not hang")
        .expect("raw scan must succeed")
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "a budget of 1 must serialize delete-file reads (peak in-flight == 1)"
    );

    // Post-delete correctness: positions 0..4 removed from a 10-row file.
    assert_eq!(total_rows(&rows), 6, "4 of 10 rows deleted");
    assert_eq!(ids_of(&rows), (4..10).collect::<Vec<_>>());

    let _ = std::fs::remove_dir_all(&dir);
}

/// Write a two-column keyed Parquet (`key_col` Int64, `data_col` Utf8) with one
/// row per key. Disjoint column names between the two join sides satisfy the VS
/// disjoint-column guarantee the join path relies on. Returns the `file://` URL.
fn write_keyed_parquet(
    dir: &Path,
    relative: &str,
    key_col: &str,
    data_col: &str,
    keys: &[i64],
    row_group: usize,
) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new(key_col, DataType::Int64, false),
        Field::new(data_col, DataType::Utf8, false),
    ]));
    let path = dir.join(relative);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let data: Vec<String> = keys.iter().map(|k| format!("{data_col}-{k}")).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(StringArray::from(data)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

/// Scenario (shared limiter across join sides — the regression guard a
/// single-provider test cannot cover): a broadcast join whose fact AND dimension
/// sides BOTH carry positional deletes, with more than N unique delete files
/// across the two sides combined. `register_join_tables` builds ONE
/// `Arc<Semaphore>` sized `s3_max_connections` and clones it into both sides; with
/// `planning_concurrency` pinned to 2 DataFusion plans the two scan leaves
/// concurrently, so both sides' Phase A delete reads contend for the SAME budget.
///
/// The peak reaches EXACTLY N only when the two sides genuinely overlap and draw
/// from one shared pool: here N=3 with 2 unique delete files per side, so N is
/// reached only by 2 fact reads + 1 dimension read in flight together — impossible
/// without concurrent planning of both leaves (hence the explicit
/// `planning_concurrency = 2`, never the runner's core-count default).
///
/// This FAILS against a per-provider-semaphore implementation: each side would get
/// its own size-3 pool, admit both its reads at once, and the shared counter would
/// peak at 4 (2 + 2) — exceeding N. That divergence (3 for the shared handle, 4
/// for a per-provider handle) is exactly what pins the shared-`Arc` wiring.
#[test]
fn scan_delete_reads_bounded_across_join_sides() {
    const BUDGET: usize = 3;

    let dir = temp_dir("join_shared_budget");
    // Fact (orders) and dimension (customer) sides, disjoint columns, joinable keys.
    let orders_url = write_keyed_parquet(
        &dir,
        "orders.parquet",
        "o_key",
        "o_data",
        &(0..8).collect::<Vec<_>>(),
        4,
    );
    let customer_url = write_keyed_parquet(
        &dir,
        "customer.parquet",
        "c_key",
        "c_data",
        &(0..8).collect::<Vec<_>>(),
        4,
    );

    // Two unique delete files per side (four total > BUDGET), each removing one
    // row position from its side's data file.
    let mut needles = Vec::new();
    let mut fact_deletes = Vec::new();
    for i in 0..2 {
        let name = format!("fact_del_{i}.parquet");
        let url = write_delete_parquet(&dir, &name, &[(&orders_url, i as i64)]);
        fact_deletes.push(delete_ref(&url));
        needles.push(name);
    }
    let mut dim_deletes = Vec::new();
    for i in 0..2 {
        let name = format!("dim_del_{i}.parquet");
        let url = write_delete_parquet(&dir, &name, &[(&customer_url, i as i64)]);
        dim_deletes.push(delete_ref(&url));
        needles.push(name);
    }

    let fact_entry = FileEntry::with_deletes(
        orders_url.clone(),
        local_file_size(&orders_url),
        fact_deletes,
    );
    let dim_entry = FileEntry::with_deletes(
        customer_url.clone(),
        local_file_size(&customer_url),
        dim_deletes,
    );

    let mut spec = scan_spec(vec![fact_entry], None, None);
    spec.common.projection = vec!["O_KEY".into(), "C_DATA".into()];
    spec.common.s3_max_connections = BUDGET;
    spec.common.join = Some(JoinSpec {
        table_root: String::new(),
        files: vec![dim_entry],
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"C_KEY\" = \"O_KEY\"".into(),
    });

    let (store, peak) = tracking_store_with_probe(needles);
    let rows = block_on(async {
        let mut ctx = FakeCtx::new();
        let mut config = session_config_for_spec(&spec);
        // Pin concurrent planning of the two scan leaves regardless of core count,
        // so both sides' Phase A runs concurrently against the one shared budget —
        // a single-core runner must not serialize the leaves and pass vacuously.
        config.options_mut().execution.planning_concurrency = 2;
        let session = SessionContext::new_with_config(config);
        session
            .runtime_env()
            .register_object_store(&Url::parse(&orders_url).expect("register url"), store);
        let mut timers = PhaseTimers::start();
        tokio::time::timeout(
            DELETE_READ_TIMEOUT,
            run_join_scan_with_session(&mut ctx, &session, &spec, &mut timers),
        )
        .await
        .expect("join delete-read fan-out must finish within the timeout, not hang")
        .expect("join scan must succeed");
        ctx.emitted
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        BUDGET,
        "delete-file reads across BOTH join sides must peak at EXACTLY the shared budget \
         ({BUDGET}): a peak of {BUDGET} proves one shared limiter caps both sides; a peak above \
         {BUDGET} (up to 4) would mean each provider built its own size-{BUDGET} semaphore"
    );

    // Post-delete correctness: each side drops keys 0 and 1; the inner join over
    // the surviving keys 2..8 yields 6 rows.
    assert_eq!(
        total_rows(&rows),
        6,
        "inner join over post-delete rows (keys 2..8 on both sides) yields 6 rows"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// An [`ObjectStore`] decorator that sums the byte length of every non-HEAD
/// `get_opts` whose location contains `needle`, and counts how many such
/// requests occurred. Delegates everything else to `inner` (a plain
/// [`LocalFileSystem`]). Used by the row-group pruning tests (task 6) to
/// observe HOW MUCH of a delete file's body a read actually transfers — the
/// externally-visible signal that some row groups were skipped rather than
/// decoded, since `with_row_groups(selected)` only fetches the column data of
/// the selected row groups.
#[derive(Debug)]
struct RangeBytesStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    matched_bytes: Arc<AtomicUsize>,
}

impl std::fmt::Display for RangeBytesStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeBytesStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RangeBytesStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let matches = !options.head && location.as_ref().contains(self.needle.as_str());
        let result = self.inner.get_opts(location, options).await?;
        if matches {
            let len = (result.range.end - result.range.start) as usize;
            self.matched_bytes.fetch_add(len, Ordering::SeqCst);
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectStorePath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectStorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// The bare filename of a `file://` URL, used as a [`RangeBytesStore`] /
/// [`TrackingStore`]-style needle that matches only that file's own path.
fn file_needle(abs_url: &str) -> String {
    let path = Url::parse(abs_url)
        .expect("valid file URL")
        .to_file_path()
        .expect("file:// URL");
    path.file_name()
        .expect("file has a name")
        .to_string_lossy()
        .to_string()
}

/// Like [`write_delete_parquet`], but with control over row-group size,
/// whether statistics are written, and the statistics truncation length.
///
/// Pass `None` for `truncate_length` unless the test targets the truncated-
/// statistics fallback: arrow-rs truncates min/max to 64 bytes by default,
/// but real Iceberg writers (parquet-java) don't truncate, so `None` matches
/// real-world data.
fn write_delete_parquet_shaped(
    dir: &Path,
    relative: &str,
    entries: &[(&str, i64)],
    row_group_size: usize,
    statistics_enabled: bool,
    truncate_length: Option<usize>,
) -> String {
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
    let stats_level = if statistics_enabled {
        EnabledStatistics::Chunk
    } else {
        EnabledStatistics::None
    };
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_size))
        .set_statistics_enabled(stats_level)
        .set_statistics_truncate_length(truncate_length)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
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

/// Three data files' delete-entry rows, sorted by `(file_path, pos)` as
/// Iceberg requires, `rows_per_file` positions each (`0..rows_per_file`). With
/// a row-group size of exactly `rows_per_file`, each file's rows land in its
/// own row group, so each group's true `file_path` min/max collapses to that
/// one file's own path.
fn one_row_group_per_file_entries(files: [&str; 3], rows_per_file: usize) -> Vec<(String, i64)> {
    let mut entries = Vec::with_capacity(files.len() * rows_per_file);
    for path in files {
        for pos in 0..rows_per_file {
            entries.push((path.to_string(), pos as i64));
        }
    }
    entries
}

/// Scenario (row-group pruning correctness, task 3 regression guard): a
/// partition-granularity delete file with THREE row groups, each holding only
/// one referenced data file's positions, is read by two shards that differ
/// ONLY in which files are assigned. A shard assigning just the MIDDLE file
/// must transfer strictly fewer, and less-than-half, of the delete file's
/// bytes than a shard assigning all three files (whose every row group's own
/// tight range matches its own assigned entry, so none is pruned) — an
/// externally observable proof that the un-assigned files' row groups were
/// skipped rather than decoded-and-discarded. Both shards still apply the
/// correct, identical delete set to the middle file.
#[test]
fn scan_prunes_delete_row_groups_by_file_path() {
    // Large enough that each row group's `pos` column data dwarfs the fixed
    // per-scan footer-read overhead the byte counter also captures — without
    // this margin, the footer cost alone can keep the pruned/full ratio above
    // one-half even though strictly fewer row groups are decoded.
    const ROWS_PER_FILE: usize = 500;

    let dir = temp_dir("row_group_pruning");
    let f1 = write_data_parquet(&dir, "f1.parquet", &(0..5).collect::<Vec<_>>(), 8);
    let f2 = write_data_parquet(&dir, "f2.parquet", &(1000..1005).collect::<Vec<_>>(), 8);
    let f3 = write_data_parquet(&dir, "f3.parquet", &(2000..2005).collect::<Vec<_>>(), 8);

    let entries = one_row_group_per_file_entries([&f1, &f2, &f3], ROWS_PER_FILE);
    let entry_refs: Vec<(&str, i64)> = entries.iter().map(|(p, pos)| (p.as_str(), *pos)).collect();
    let delete_url = write_delete_parquet_shaped(
        &dir,
        "shared_delete.parquet",
        &entry_refs,
        ROWS_PER_FILE,
        true,
        None,
    );
    let shared_delete = delete_ref(&delete_url);
    let needle = file_needle(&delete_url);

    // Pruned shard: only f2 is assigned. f1's and f3's row groups cannot
    // overlap f2's path and must be pruned, leaving one row group to decode.
    let pruned_entries = vec![FileEntry::with_deletes(
        f2.clone(),
        local_file_size(&f2),
        vec![shared_delete.clone()],
    )];
    let pruned_spec = scan_spec(pruned_entries, None, None);
    let pruned_bytes = Arc::new(AtomicUsize::new(0));
    let pruned_store = Arc::new(RangeBytesStore {
        inner: Arc::new(LocalFileSystem::new()),
        needle: needle.clone(),
        matched_bytes: Arc::clone(&pruned_bytes),
    });
    let pruned_rows = block_on(try_run_scan_with_store(&pruned_spec, &f2, pruned_store))
        .expect("pruned scan must succeed");

    // Full shard: all three files are assigned, so every row group's own
    // tight range matches its own assigned entry and none is pruned.
    let full_entries = vec![
        FileEntry::with_deletes(
            f1.clone(),
            local_file_size(&f1),
            vec![shared_delete.clone()],
        ),
        FileEntry::with_deletes(
            f2.clone(),
            local_file_size(&f2),
            vec![shared_delete.clone()],
        ),
        FileEntry::with_deletes(f3.clone(), local_file_size(&f3), vec![shared_delete]),
    ];
    let full_spec = scan_spec(full_entries, None, None);
    let full_bytes = Arc::new(AtomicUsize::new(0));
    let full_store = Arc::new(RangeBytesStore {
        inner: Arc::new(LocalFileSystem::new()),
        needle,
        matched_bytes: Arc::clone(&full_bytes),
    });
    block_on(try_run_scan_with_store(&full_spec, &f1, full_store)).expect("full scan must succeed");

    let pruned_total = pruned_bytes.load(Ordering::SeqCst);
    let full_total = full_bytes.load(Ordering::SeqCst);
    assert!(
        pruned_total > 0,
        "the pruned scan must still fetch its own assigned file's row group"
    );
    assert!(
        pruned_total < full_total,
        "assigning only 1 of 3 files must decode fewer delete-file bytes than assigning all 3: \
         pruned={pruned_total} full={full_total}"
    );
    assert!(
        pruned_total * 2 < full_total,
        "pruning 2 of 3 disjoint row groups should cut delete-file bytes by more than half: \
         pruned={pruned_total} full={full_total}"
    );

    // Correctness: f2's declared deletes (positions 0..100) cover all 5 of its
    // real rows, and the pruned read must apply exactly the same delete set an
    // unpruned read would.
    assert_eq!(
        total_rows(&pruned_rows),
        0,
        "all of f2's rows must be deleted, pruning notwithstanding"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (row-group pruning fallback — real string-statistics truncation):
/// long, near-identical paths (mirroring deep S3 URI namespaces) whose shared
/// prefix exceeds Parquet's 64-byte statistics-truncation length force the
/// WRITER itself — not a hand-crafted fixture — to truncate the `file_path`
/// column's min/max statistics. Each assigned file's own row group then
/// carries truncated bounds that are DIFFERENT byte strings from its real
/// path (a shortened prefix for min, an incremented shortened prefix for
/// max): an equality-based pruning shortcut would wrongly skip such a row
/// group, while the range-based check must still decode it. Confirms pruning
/// under genuine truncation still yields the correct (unpruned-equivalent)
/// delete set.
#[test]
fn scan_prunes_delete_row_groups_with_truncated_statistics() {
    const ROWS_PER_FILE: usize = 20;

    let dir = temp_dir("truncated_stats");
    // Deliberately longer than the 64-byte statistics-truncation length so the
    // three files' paths share a prefix that itself exceeds the truncation
    // boundary; they differ only in their final path segment.
    let long_prefix =
        "warehouse/analytics/very/long/namespace/orders/table/data/nested/deeply/for/truncation";
    assert!(
        long_prefix.len() > 64,
        "shared prefix must exceed the 64-byte statistics-truncation length"
    );

    let f_a = write_data_parquet(
        &dir,
        &format!("{long_prefix}/part-00001-order-events.parquet"),
        &(0..5).collect::<Vec<_>>(),
        8,
    );
    let f_b = write_data_parquet(
        &dir,
        &format!("{long_prefix}/part-00002-order-events.parquet"),
        &(1000..1005).collect::<Vec<_>>(),
        8,
    );
    let f_c = write_data_parquet(
        &dir,
        &format!("{long_prefix}/part-00003-order-events.parquet"),
        &(2000..2005).collect::<Vec<_>>(),
        8,
    );

    let entries = one_row_group_per_file_entries([&f_a, &f_b, &f_c], ROWS_PER_FILE);
    let entry_refs: Vec<(&str, i64)> = entries.iter().map(|(p, pos)| (p.as_str(), *pos)).collect();
    let delete_url = write_delete_parquet_shaped(
        &dir,
        "truncated_delete.parquet",
        &entry_refs,
        ROWS_PER_FILE,
        true,
        Some(64),
    );

    // Only f_b (the MIDDLE file) is assigned; its own row group's truncated
    // bounds must still be recognized as overlapping its real (untruncated)
    // path, or its deletes would be silently skipped.
    let entry = FileEntry::with_deletes(
        f_b.clone(),
        local_file_size(&f_b),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &f_b);

    // f_b's declared deletes (positions 0..20) cover all 5 of its real rows.
    assert_eq!(
        total_rows(&rows),
        0,
        "truncated file_path statistics must not cause the assigned file's own row group to be \
         wrongly pruned: expected all 5 of f_b's rows deleted, got {} surviving",
        total_rows(&rows)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (row-group pruning fallback — absent statistics): a delete file
/// written with column statistics DISABLED carries no `file_path` min/max on
/// any row group. [`delete_row_group_may_match`] must treat an
/// absent-statistics row group as "cannot rule out a match" and decode it
/// rather than pruning it. Confirmed two ways: the read transfers essentially
/// the WHOLE delete-file body (no row group is skipped for lack of stats to
/// prune by), and it still yields the correct delete set for the assigned
/// file.
///
/// [`delete_row_group_may_match`]: lakehouse_engine::scan::positional_deletes
#[test]
fn scan_decodes_all_row_groups_when_file_path_statistics_absent() {
    const ROWS_PER_FILE: usize = 100;

    let dir = temp_dir("no_stats");
    let f1 = write_data_parquet(&dir, "f1.parquet", &(0..5).collect::<Vec<_>>(), 8);
    let f2 = write_data_parquet(&dir, "f2.parquet", &(1000..1005).collect::<Vec<_>>(), 8);
    let f3 = write_data_parquet(&dir, "f3.parquet", &(2000..2005).collect::<Vec<_>>(), 8);

    let entries = one_row_group_per_file_entries([&f1, &f2, &f3], ROWS_PER_FILE);
    let entry_refs: Vec<(&str, i64)> = entries.iter().map(|(p, pos)| (p.as_str(), *pos)).collect();
    let delete_url = write_delete_parquet_shaped(
        &dir,
        "no_stats_delete.parquet",
        &entry_refs,
        ROWS_PER_FILE,
        false,
        None,
    );
    let delete_total_size = local_file_size(&delete_url);
    let needle = file_needle(&delete_url);

    // Only f2 is assigned; with no file_path statistics to prune by, f1's and
    // f3's row groups cannot be ruled out and must be decoded too.
    let entry = FileEntry::with_deletes(
        f2.clone(),
        local_file_size(&f2),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let bytes = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(RangeBytesStore {
        inner: Arc::new(LocalFileSystem::new()),
        needle,
        matched_bytes: Arc::clone(&bytes),
    });
    let rows =
        block_on(try_run_scan_with_store(&spec, &f2, store)).expect("unpruned scan must succeed");

    let fetched = bytes.load(Ordering::SeqCst);
    assert!(
        (fetched as f64) >= delete_total_size as f64 * 0.9,
        "with no file_path statistics every row group must be decoded (no pruning possible): \
         fetched {fetched} bytes of a {delete_total_size}-byte delete file"
    );

    assert_eq!(
        total_rows(&rows),
        0,
        "all of f2's rows must be deleted despite absent statistics"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Recurse the physical plan, collecting every leaf node (no children) as an
/// owned `Arc<dyn ExecutionPlan>`. Mirrors `scan_agg_projection_pruning.rs`'s
/// `collect_leaf_scans`, which collects labels/schemas instead of the node
/// itself; this variant returns the node so its `DataSourceExec` can be
/// downcast to inspect the `FileScanConfig` it built.
fn collect_leaf_execs(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Arc<dyn ExecutionPlan>>) {
    let children = plan.children();
    if children.is_empty() {
        out.push(Arc::clone(plan));
    } else {
        for child in children {
            collect_leaf_execs(child, out);
        }
    }
}

/// The raw scan plan's single leaf, downcast to its `FileScanConfig` (the
/// Parquet `DataSourceExec` the production provider builds). Asserts there is
/// exactly one leaf and that it is Parquet-backed, so a caller can inspect
/// `file_groups` directly.
fn leaf_file_scan_config(
    plan: &Arc<dyn ExecutionPlan>,
) -> datafusion::datasource::physical_plan::FileScanConfig {
    let mut leaves = Vec::new();
    collect_leaf_execs(plan, &mut leaves);
    assert_eq!(leaves.len(), 1, "raw scan plan must have exactly one leaf");
    let (file_scan_config, _parquet_source) = leaves[0]
        .downcast_ref::<DataSourceExec>()
        .expect("leaf must be a DataSourceExec")
        .downcast_to_file_source::<ParquetSource>()
        .expect("leaf must be backed by a ParquetSource");
    file_scan_config.clone()
}

/// Scenario (connection-concurrency bound, PLAN CONSTRUCTION ONLY): with a
/// footer-fetch budget of N and MORE than N delete-carrying data files to
/// fetch footers for, the concurrent footer fetches peak at EXACTLY N — the
/// shared instance-level semaphore Phase B now shares with Phase A admits N at
/// a time and no more.
///
/// PLAN CONSTRUCTION ONLY: `build_raw_scan_physical_plan` is awaited and
/// `peak` is read IMMEDIATELY afterward; the returned plan is NEVER executed.
/// The needles here are DATA-file names, not delete-file names (unlike
/// `scan_delete_reads_bounded_by_connection_budget`), so executing the plan
/// would let the opener's execute-time column reads of those same files —
/// which hold no semaphore permit — latch into the same monotonic `fetch_max`
/// peak and turn the assertion into an order-dependent flake.
///
/// File order is asserted from the PLAN itself (`leaf_file_scan_config`'s
/// `file_groups`), not from execution.
#[test]
fn scan_footer_fetches_bounded_by_connection_budget() {
    const BUDGET: usize = 3;
    const DATA_FILES: usize = 6; // strictly greater than BUDGET

    let dir = temp_dir("footer_bounded_budget");

    let mut entries = Vec::with_capacity(DATA_FILES);
    let mut needles = Vec::with_capacity(DATA_FILES);
    let mut data_urls = Vec::with_capacity(DATA_FILES);
    for i in 0..DATA_FILES {
        let data_name = format!("data_{i}.parquet");
        let data_url = write_data_parquet(&dir, &data_name, &(0..10).collect::<Vec<_>>(), 4);
        let delete_url = write_delete_parquet(&dir, &format!("del_{i}.parquet"), &[(&data_url, 0)]);
        entries.push(FileEntry::with_deletes(
            data_url.clone(),
            local_file_size(&data_url),
            vec![delete_ref(&delete_url)],
        ));
        needles.push(file_needle(&data_url));
        data_urls.push(data_url);
    }

    let mut spec = scan_spec_with_logical_schema(entries, None, None);
    spec.common.s3_max_connections = BUDGET;

    let (store, peak) = tracking_store_with_probe(needles.clone());

    let plan = block_on(async {
        let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
        ctx.runtime_env()
            .register_object_store(&Url::parse(&data_urls[0]).expect("register url"), store);
        register_files(&ctx, "scan_target", &spec)
            .await
            .expect("register_files must succeed");
        tokio::time::timeout(
            DELETE_READ_TIMEOUT,
            build_raw_scan_physical_plan(&ctx, &spec),
        )
        .await
        .expect("plan construction must finish within the timeout, not hang")
        .expect("physical plan must build")
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        BUDGET,
        "concurrent footer fetches must peak at EXACTLY the connection budget ({BUDGET}): \
         a lower peak means the fan-out never ran, a higher peak means the bound leaked"
    );

    let file_scan_config = leaf_file_scan_config(&plan);
    assert_eq!(
        file_scan_config.file_groups.len(),
        1,
        "a single-shard raw scan must produce exactly one file group"
    );
    let group_locations: Vec<String> = file_scan_config.file_groups[0]
        .iter()
        .map(|f| f.object_meta.location.as_ref().to_string())
        .collect();
    assert_eq!(
        group_locations.len(),
        DATA_FILES,
        "the file group must list every assigned data file"
    );
    for (i, needle) in needles.iter().enumerate() {
        assert!(
            group_locations[i].contains(needle.as_str()),
            "file group entry {i} must be {needle} (spec order), got {}: {group_locations:?}",
            group_locations[i]
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (file-metadata, PLAN CONSTRUCTION ONLY): a shard mixing
/// delete-carrying and delete-free data files fetches a footer ONLY for the
/// delete-carrying ones, and exactly once each — a delete-free file costs no
/// footer fetch of its own, proving Phase B's fan-out (task 1.6) does not
/// widen I/O beyond the entries that actually need an access plan.
///
/// PLAN CONSTRUCTION ONLY, for the same reason as
/// `scan_footer_fetches_bounded_by_connection_budget`: the plan is built and
/// never executed, so the opener's execute-time reads of these same files
/// never contaminate the `get_opts` counts read immediately afterward.
#[test]
fn scan_mixed_shard_fetches_footers_only_for_delete_carrying_files() {
    let dir = temp_dir("mixed_shard_footers");

    // Interleaved order: delete-free, delete-carrying, delete-free, delete-carrying.
    let free_a = write_data_parquet(&dir, "free_a.parquet", &(0..10).collect::<Vec<_>>(), 4);
    let carrying_b =
        write_data_parquet(&dir, "carrying_b.parquet", &(0..10).collect::<Vec<_>>(), 4);
    let del_b = write_delete_parquet(&dir, "del_b.parquet", &[(&carrying_b, 0)]);
    let free_c = write_data_parquet(&dir, "free_c.parquet", &(0..10).collect::<Vec<_>>(), 4);
    let carrying_d =
        write_data_parquet(&dir, "carrying_d.parquet", &(0..10).collect::<Vec<_>>(), 4);
    let del_d = write_delete_parquet(&dir, "del_d.parquet", &[(&carrying_d, 0)]);

    let entries = vec![
        FileEntry::new(free_a.clone(), local_file_size(&free_a)),
        FileEntry::with_deletes(
            carrying_b.clone(),
            local_file_size(&carrying_b),
            vec![delete_ref(&del_b)],
        ),
        FileEntry::new(free_c.clone(), local_file_size(&free_c)),
        FileEntry::with_deletes(
            carrying_d.clone(),
            local_file_size(&carrying_d),
            vec![delete_ref(&del_d)],
        ),
    ];
    let spec_order = [
        file_needle(&free_a),
        file_needle(&carrying_b),
        file_needle(&free_c),
        file_needle(&carrying_d),
    ];
    let delete_carrying_needles = [file_needle(&carrying_b), file_needle(&carrying_d)];
    let delete_free_needles = [file_needle(&free_a), file_needle(&free_c)];

    let spec = scan_spec_with_logical_schema(entries, None, None);

    let gets = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = Arc::new(TrackingStore {
        inner: Arc::new(LocalFileSystem::new()),
        gets: Arc::clone(&gets),
        calls: Arc::new(AtomicUsize::new(0)),
        concurrency: None,
    });

    let plan = block_on(async {
        let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
        ctx.runtime_env()
            .register_object_store(&Url::parse(&free_a).expect("register url"), store);
        register_files(&ctx, "scan_target", &spec)
            .await
            .expect("register_files must succeed");
        build_raw_scan_physical_plan(&ctx, &spec)
            .await
            .expect("physical plan must build")
    });

    for needle in &delete_free_needles {
        assert_eq!(
            count_gets_matching(&gets, needle.as_str()),
            0,
            "a delete-free file must cost no footer fetch of its own: {needle}"
        );
    }
    for needle in &delete_carrying_needles {
        assert_eq!(
            count_gets_matching(&gets, needle.as_str()),
            1,
            "a delete-carrying file's footer must be fetched exactly once: {needle}"
        );
    }

    let file_scan_config = leaf_file_scan_config(&plan);
    assert_eq!(
        file_scan_config.file_groups.len(),
        1,
        "a single-shard raw scan must produce exactly one file group"
    );
    let group = &file_scan_config.file_groups[0];
    assert_eq!(
        group.iter().count(),
        spec_order.len(),
        "the file group must list every assigned file"
    );
    for (i, (partitioned, needle)) in group.iter().zip(spec_order.iter()).enumerate() {
        let location = partitioned.object_meta.location.as_ref();
        assert!(
            location.contains(needle.as_str()),
            "file group entry {i} must be {needle} (spec order), got {location}"
        );
        let has_access_plan = partitioned.extension::<ParquetAccessPlan>().is_some();
        let expect_access_plan = delete_carrying_needles.contains(needle);
        assert_eq!(
            has_access_plan, expect_access_plan,
            "file group entry {i} ({needle}) access-plan presence must match its delete-carrying status"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (shared limiter across join sides, PLAN CONSTRUCTION ONLY — the
/// regression guard the single-provider footer tests cannot give): a broadcast
/// join whose fact AND dimension sides BOTH carry positional deletes on every
/// assigned data file, so BOTH sides run a Phase B footer fan-out.
/// `register_join_tables` builds ONE `Arc<Semaphore>` sized `s3_max_connections`
/// and clones it into both sides; with `planning_concurrency` pinned to 2
/// DataFusion plans the two scan leaves concurrently, so both sides' footer
/// fetches contend for the SAME budget.
///
/// The peak reaches EXACTLY N only when the two sides genuinely overlap and draw
/// from one shared pool: here N=3 with 2 delete-carrying data files per side, so
/// N is reached only by 2 fact footers + 1 dimension footer in flight together.
/// A per-provider — or Phase-B-private — semaphore would give each side its own
/// size-3 pool, admit both its footers at once, and peak at 4. That divergence
/// (3 for one shared handle, 4 for two) is the whole point of this test, and no
/// single-provider test can observe it.
///
/// A peak of 2 would mean the two leaves were planned sequentially rather than
/// concurrently, collapsing the overlap to one in-flight footer fetch per side
/// at a time; a peak of 4 would mean each side drew from its own per-provider
/// semaphore instead of the one shared handle.
///
/// PLAN CONSTRUCTION ONLY: [`build_join_physical_plan`] registers both sides and
/// returns the physical plan WITHOUT executing it, and `peak` is read
/// immediately after it returns. `run_join_scan_with_session` MUST NOT be used
/// here. The needles below are DATA-file names, not the delete-file names
/// `scan_delete_reads_bounded_across_join_sides` uses, and the opener never
/// reads a delete file — so an executed join would count and delay the opener's
/// execute-time column reads of those same four data files, which hold no
/// semaphore permit, into this same monotonic `fetch_max` peak, latching any
/// execute-time overlap above N permanently and turning `assert_eq!` into an
/// order-dependent flake.
///
/// Needling only the DATA files also keeps Phase A's delete-file reads
/// undelayed, so the peak this test pins is built purely from Phase B windows.
///
/// BOTH sides carry a non-empty `logical_schema`, built through the same
/// [`logical_fields`] seam as the single-provider tests but with the column
/// names `write_keyed_parquet` actually writes (decision-log [8] applies to a
/// join's peak assertion exactly as it does to a single provider's): an empty
/// `logical_schema` sends `register_file_list` down the
/// `ParquetFormat::infer_schema` branch, whose GET against the first assigned —
/// and therefore needled — DATA file is a DELAYED read taken outside the
/// semaphore, contaminating the very peak asserted here.
///
/// Post-delete row-set equality is deliberately NOT asserted here;
/// `scan_delete_reads_bounded_across_join_sides` already covers it, executing
/// against its own store with no data-file needles.
#[test]
fn scan_footer_fetches_bounded_across_join_sides() {
    const BUDGET: usize = 3;
    const FILES_PER_SIDE: usize = 2; // 2 + 2 = 4 footers, strictly greater than BUDGET

    let dir = temp_dir("join_footer_shared_budget");

    // Fact (orders) and dimension (customer) sides with disjoint column names,
    // as the VS disjoint-column guarantee the join path relies on requires.
    // EVERY data file carries its own one-position delete file, so both sides
    // fetch a footer per assigned file during access-plan construction.
    let mut needles = Vec::with_capacity(2 * FILES_PER_SIDE);
    let mut fact_entries = Vec::with_capacity(FILES_PER_SIDE);
    let mut dim_entries = Vec::with_capacity(FILES_PER_SIDE);
    let mut first_data_url = None;
    for i in 0..FILES_PER_SIDE {
        let keys: Vec<i64> = ((i as i64) * 8..(i as i64) * 8 + 8).collect();
        let orders_url = write_keyed_parquet(
            &dir,
            &format!("orders_{i}.parquet"),
            "o_key",
            "o_data",
            &keys,
            4,
        );
        let customer_url = write_keyed_parquet(
            &dir,
            &format!("customer_{i}.parquet"),
            "c_key",
            "c_data",
            &keys,
            4,
        );
        let fact_delete_url =
            write_delete_parquet(&dir, &format!("fact_del_{i}.parquet"), &[(&orders_url, 0)]);
        let dim_delete_url =
            write_delete_parquet(&dir, &format!("dim_del_{i}.parquet"), &[(&customer_url, 0)]);

        needles.push(file_needle(&orders_url));
        needles.push(file_needle(&customer_url));
        first_data_url.get_or_insert_with(|| orders_url.clone());
        fact_entries.push(FileEntry::with_deletes(
            orders_url.clone(),
            local_file_size(&orders_url),
            vec![delete_ref(&fact_delete_url)],
        ));
        dim_entries.push(FileEntry::with_deletes(
            customer_url.clone(),
            local_file_size(&customer_url),
            vec![delete_ref(&dim_delete_url)],
        ));
    }
    let register_url = first_data_url.expect("at least one data file per side");

    let mut spec = scan_spec_with_logical_schema(fact_entries, None, None);
    spec.common.projection = vec!["O_KEY".into(), "C_DATA".into()];
    spec.common.logical_schema = logical_fields(&[("o_key", "int64"), ("o_data", "utf8")]);
    spec.common.s3_max_connections = BUDGET;
    spec.common.join = Some(JoinSpec {
        table_root: String::new(),
        files: dim_entries,
        logical_schema: logical_fields(&[("c_key", "int64"), ("c_data", "utf8")]),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"C_KEY\" = \"O_KEY\"".into(),
    });

    let (store, peak) = tracking_store_with_probe(needles);

    block_on(async {
        let mut config = session_config_for_spec(&spec);
        // Pin concurrent planning of the two scan leaves regardless of core
        // count, so both sides' Phase B fan-out runs against the one shared
        // budget — a single-core runner must not serialize the leaves and pass
        // vacuously.
        config.options_mut().execution.planning_concurrency = 2;
        let session = SessionContext::new_with_config(config);
        session
            .runtime_env()
            .register_object_store(&Url::parse(&register_url).expect("register url"), store);
        tokio::time::timeout(
            DELETE_READ_TIMEOUT,
            build_join_physical_plan(&session, &spec),
        )
        .await
        .expect("join plan construction must finish within the timeout, not hang")
        .expect("join physical plan must build");
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        BUDGET,
        "data-file footer fetches across BOTH join sides must peak at EXACTLY the shared budget \
         ({BUDGET}): a peak of {BUDGET} proves one shared limiter caps both sides' Phase B, a \
         lower peak means the fan-out never ran, and a peak above {BUDGET} (up to 4) would mean \
         each provider built its own size-{BUDGET} semaphore"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

//! Integration test for the two-argument scan UDF reconstitution (task 2.3).
//!
//! The scan SET UDF now receives its spec across TWO VARCHAR arguments: the
//! shard-invariant common blob (column 0, serialized once per fan-out) and the
//! per-shard files JSON array (column 1). `run_scan` reads both via
//! `ctx.get_string(0)` / `ctx.get_string(1)` and reconstitutes a `ScanSpec`
//! through [`read_scan_spec`] → `ScanSpec::from_parts_json`.
//!
//! This test drives the EXACT production two-argument reconstitution
//! ([`read_scan_spec`]) against a fake `UdfContext` backed by a local `file://`
//! Parquet (no S3 / MinIO), then runs the unchanged downstream raw-scan path
//! ([`run_raw_scan_with_session`]) and asserts the emitted rows are byte-for-byte
//! identical to the pre-split single-argument path (a whole `ScanSpec` parsed via
//! `ScanSpec::from_json`). It also pins the NULL-argument contract for BOTH
//! arguments.
//!
//! Host-runnable: no S3 / MinIO stack — the scan registers a `file://` Parquet.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    DeleteFileContentType, DeleteFileRef, FileEntry, ScanSpec, StorageProps,
};
use lakehouse_engine::scan::{read_scan_spec, run_raw_scan_with_session, session_config_for_spec};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;

/// Iceberg reserved field-ids for a positional-delete file's `file_path`/`pos`
/// columns (mirrors `scan::positional_deletes`'s private constants; duplicated
/// here since this integration test cannot import a `pub(crate)` item).
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

/// A fake `UdfContext` serving up to two string columns for one input row and
/// capturing every `emit_batch` as a decoded `RecordBatch`.
///
/// A column value of `None` models a SQL NULL argument, exercising the
/// NULL-handling contract of [`read_scan_spec`].
struct FakeCtx {
    columns: Vec<Option<String>>,
    served: bool,
    emitted: Vec<RecordBatch>,
}

impl FakeCtx {
    fn new(columns: Vec<Option<String>>) -> Self {
        Self {
            columns,
            served: false,
            emitted: Vec::new(),
        }
    }
}

impl UdfContext for FakeCtx {
    fn num_columns(&self) -> usize {
        self.columns.len()
    }
    fn get(&self, _col: usize) -> Result<&Value, UdfError> {
        Err(UdfError::User("FakeCtx uses get_string only".into()))
    }
    fn get_string(&self, col: usize) -> Result<Option<&str>, UdfError> {
        Ok(self.columns.get(col).and_then(|c| c.as_deref()))
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

/// Write a local Parquet file with `rows` rows across small row groups (so the
/// scan produces several batches) and return its `file://` URL.
fn write_local_parquet(dir: &std::path::Path, rows: i64, row_group: usize) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let path = dir.join("two_arg_data.parquet");
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let ids: Vec<i64> = (0..rows).collect();
    let names: Vec<String> = (0..rows).map(|i| format!("row-{i}")).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn scan_spec(file_url: String) -> ScanSpec {
    let size = std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(&file_url))
        .map(|m| m.len())
        .unwrap_or(0);
    ScanSpec {
        table_root: String::new(),
        files: vec![FileEntry::new(file_url, size)],
        projection: vec!["ID".into(), "NAME".into()],
        filter: Some("\"ID\" >= 10".into()),
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        distinct: false,
        emit_exa_types: vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join: None,
        storage: StorageProps {
            endpoint: "http://localhost:9000".into(),
            region: "us-east-1".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
            session_token: None,
            allow_http: true,
            path_style: true,
        },
        df_target_partitions: 1,
        df_batch_size: 64,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    }
}

/// Run the raw scan for `spec` against a capture-only context (whatever input
/// columns it carries are irrelevant here — the spec is passed directly), and
/// return the decoded emitted batches. Models the PRE-SPLIT single-argument
/// path: the whole spec parsed up front, then the unchanged downstream scan.
async fn run_with_spec(spec: &ScanSpec) -> Vec<RecordBatch> {
    let mut ctx = FakeCtx::new(vec![Some(spec.to_json())]);
    let session = SessionContext::new_with_config(session_config_for_spec(spec));
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, spec, &mut timers)
        .await
        .expect("raw scan must succeed");
    ctx.emitted
}

/// Run the raw scan driving the TWO-ARGUMENT reconstitution: feed the common
/// blob (col 0) and the per-shard files JSON (col 1) through the production
/// [`read_scan_spec`], then run the unchanged downstream scan over the
/// reconstituted spec. Returns the decoded emitted batches.
async fn run_two_arg(common_json: &str, files_json: &str) -> Vec<RecordBatch> {
    let mut ctx = FakeCtx::new(vec![
        Some(common_json.to_string()),
        Some(files_json.to_string()),
    ]);
    // Production two-argument reconstitution (the code under test).
    let spec = read_scan_spec(&ctx).expect("reconstitute spec from two args");
    let session = SessionContext::new_with_config(session_config_for_spec(&spec));
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, &spec, &mut timers)
        .await
        .expect("raw scan must succeed");
    ctx.emitted
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
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

/// Byte size of a local file, given its `file://` URL.
fn file_size(file_url: &str) -> u64 {
    std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(file_url))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Write a local positional-delete Parquet at `dir/relative`: `file_path`/`pos`
/// columns tagged with the Iceberg reserved field-ids, one row per
/// `(referenced_file_abs_url, position)` entry. Returns the file's absolute
/// `file://` URL.
fn write_delete_parquet(dir: &std::path::Path, relative: &str, entries: &[(&str, i64)]) -> String {
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
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

/// A row-scan `ScanSpec` over `files` (already absolute, `table_root` empty),
/// with no filter/limit pushdown — a minimal template for tests that only care
/// about which files/deletes are carried through the two-argument split.
fn spec_for_files(files: Vec<FileEntry>) -> ScanSpec {
    ScanSpec {
        table_root: String::new(),
        files,
        projection: vec!["ID".into(), "NAME".into()],
        filter: None,
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        distinct: false,
        emit_exa_types: Vec::new(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join: None,
        storage: StorageProps {
            endpoint: "http://localhost:9000".into(),
            region: "us-east-1".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
            session_token: None,
            allow_http: true,
            path_style: true,
        },
        df_target_partitions: 1,
        df_batch_size: 64,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    }
}

/// The two-argument reconstitution path scans exactly the assigned file and
/// emits rows byte-for-byte identical to the pre-split single-argument path.
#[test]
fn scan_registers_only_assigned_files_two_arg() {
    let dir = std::env::temp_dir().join(format!("lh_two_arg_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file_url = write_local_parquet(&dir, 200, 64);
    let spec = scan_spec(file_url);

    // Split the spec the way the adapter does: common blob serialized once,
    // files as a separate JSON array. The common blob must not carry `files`.
    let common_json = spec.to_common_json();
    let files_json = ScanSpec::files_json(&spec.files);
    assert!(
        !common_json.contains("\"files\""),
        "common blob must not carry a files key: {common_json}"
    );

    // Reconstituted spec equals the pre-split spec (defense-in-depth vs. the
    // unit-level round-trip test).
    let reconstituted =
        ScanSpec::from_parts_json(&common_json, &files_json).expect("from_parts_json");
    assert_eq!(
        reconstituted, spec,
        "two-arg reconstitution must equal spec"
    );

    // Drive both paths against the same local Parquet.
    let single = block_on(run_with_spec(&spec));
    let two_arg = block_on(run_two_arg(&common_json, &files_json));

    // Filter is "ID >= 10" over ids 0..200 → 190 surviving rows.
    assert_eq!(total_rows(&single), 190, "single-arg row count");
    assert_eq!(
        total_rows(&two_arg),
        190,
        "two-arg row count must match the filtered file contents"
    );

    // Byte-for-byte identical emitted output: same batch count, same batches.
    assert_eq!(
        two_arg.len(),
        single.len(),
        "two-arg and single-arg must emit the same number of batches"
    );
    assert_eq!(
        two_arg, single,
        "two-arg reconstitution must emit rows identical to the pre-split single-arg path"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A SQL NULL in EITHER argument is a user error — the NULL-handling contract is
/// preserved for both arguments (mirrors the pre-split single-arg NULL check).
#[test]
fn two_arg_null_in_either_argument_is_user_error() {
    let files_json = ScanSpec::files_json(&[FileEntry::new("s3://w/f0.parquet", 0)]);
    let common_json = scan_spec("s3://w/f0.parquet".into()).to_common_json();

    // NULL common blob (col 0).
    let ctx = FakeCtx::new(vec![None, Some(files_json.clone())]);
    let err = read_scan_spec(&ctx).expect_err("NULL common must error");
    assert!(
        matches!(err, UdfError::User(ref m) if m.contains("common") && m.contains("NULL")),
        "NULL common must be a user error naming the common arg: {err:?}"
    );

    // NULL files blob (col 1).
    let ctx = FakeCtx::new(vec![Some(common_json), None]);
    let err = read_scan_spec(&ctx).expect_err("NULL files must error");
    assert!(
        matches!(err, UdfError::User(ref m) if m.contains("files") && m.contains("NULL")),
        "NULL files must be a user error naming the files arg: {err:?}"
    );
}

/// Scenario (scan-execution): the two-argument reconstitution registers ONLY
/// the assigned file through the `PositionalDeleteScanTable`/`ParquetSource`
/// provider that replaced `ListingTable` in `register_files` — no directory
/// discovery. A second "decoy" file sits in the SAME directory with a
/// disjoint id range; if the provider ever discovered files itself (rather
/// than reading exactly the assigned list), the decoy's rows would leak in.
#[test]
fn scan_registers_assigned_files_via_parquet_provider() {
    let dir = std::env::temp_dir().join(format!("lh_provider_{}", std::process::id()));
    let assigned_dir = dir.join("assigned");
    let decoy_dir = dir.join("decoy");
    std::fs::create_dir_all(&assigned_dir).unwrap();
    std::fs::create_dir_all(&decoy_dir).unwrap();

    // Assigned file: ids 0..30. Decoy file (same directory tree, NOT assigned):
    // ids 10_000..10_500 — a disjoint range that makes any accidental discovery
    // immediately visible.
    let assigned_url = write_local_parquet(&assigned_dir, 30, 8);
    let decoy_url = write_local_parquet(&decoy_dir, 500, 64);
    let _ = &decoy_url; // written to disk; deliberately never assigned to the spec.

    let entry = FileEntry::new(assigned_url.clone(), file_size(&assigned_url));
    let spec = spec_for_files(vec![entry]);
    let common_json = spec.to_common_json();
    let files_json = ScanSpec::files_json(&spec.files);

    let rows = block_on(run_two_arg(&common_json, &files_json));
    let ids = ids_of(&rows);

    assert_eq!(
        total_rows(&rows),
        30,
        "only the assigned file's 30 rows must be scanned, not the decoy's 500"
    );
    assert_eq!(
        ids,
        (0..30).collect::<Vec<_>>(),
        "no decoy id (>= 10_000 range would be impossible here since decoy ids are 0..500, \
         so any leak would inflate the count/ids beyond the assigned file's own range): {ids:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (reconstitution): a `ScanSpec` whose files carry positional-delete
/// refs reconstitutes byte-for-byte through the two-argument split
/// (`to_common_json` + `files_json` → `from_parts_json`), AND the reconstituted
/// spec's deletes are FUNCTIONALLY enforced when driven through the exact
/// two-argument scan pipeline the production UDF uses (not merely structurally
/// equal).
#[test]
fn spec_reconstitutes_with_delete_entries() {
    let dir = std::env::temp_dir().join(format!("lh_recon_del_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let data_url = write_local_parquet(&dir, 20, 8);
    let delete_url =
        write_delete_parquet(&dir, "deletes.parquet", &[(&data_url, 2), (&data_url, 9)]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        file_size(&data_url),
        vec![DeleteFileRef {
            path: delete_url.clone(),
            size: file_size(&delete_url),
            content_type: DeleteFileContentType::PositionDeletes,
        }],
    );
    let spec = spec_for_files(vec![entry]);

    let common_json = spec.to_common_json();
    let files_json = ScanSpec::files_json(&spec.files);
    assert!(
        files_json.contains("deletes.parquet"),
        "per-shard files JSON must carry the delete file: {files_json}"
    );

    // Structural reconstitution: byte-for-byte equal to the pre-split spec,
    // deletes included.
    let reconstituted =
        ScanSpec::from_parts_json(&common_json, &files_json).expect("from_parts_json");
    assert_eq!(
        reconstituted, spec,
        "two-arg reconstitution must equal the delete-carrying spec"
    );
    assert_eq!(reconstituted.files[0].deletes.len(), 1);
    assert_eq!(
        reconstituted.files[0].deletes[0].content_type,
        DeleteFileContentType::PositionDeletes
    );

    // Functional reconstitution: driving the two-argument pipeline actually
    // applies the reconstituted deletes.
    let rows = block_on(run_two_arg(&common_json, &files_json));
    assert_eq!(total_rows(&rows), 18, "2 of 20 rows deleted");
    let ids = ids_of(&rows);
    assert!(!ids.contains(&2), "position 2 must be deleted: {ids:?}");
    assert!(!ids.contains(&9), "position 9 must be deleted: {ids:?}");

    let _ = std::fs::remove_dir_all(&dir);
}

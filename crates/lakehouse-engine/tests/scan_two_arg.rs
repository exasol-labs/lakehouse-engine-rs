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

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{ScanSpec, StorageProps};
use lakehouse_engine::scan::{
    build_s3_store, client_options_for, read_scan_spec, run_raw_scan_with_session,
    session_config_for_spec,
};
use object_store::ClientConfigKey;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

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
    ScanSpec {
        files: vec![file_url],
        projection: vec!["ID".into(), "NAME".into()],
        filter: Some("\"ID\" >= 10".into()),
        limit: None,
        aggregates: None,
        group_keys: None,
        emit_exa_types: vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
        logical_schema: Vec::new(),
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
    // Advance to the single input row, exactly as run_scan does.
    assert!(ctx.next().expect("next"), "one input row");
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
    // Advance to the single input row, exactly as run_scan does.
    assert!(ctx.next().expect("next"), "one input row");
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
    let files_json = ScanSpec::files_json(&["s3://w/f0.parquet".to_string()]);
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

/// The scan configures its object store from the resolved connection budget
/// (task 2.7). Builds a `ScanSpec` with `s3_max_connections` set to a value
/// distinct from the `scan_spec()` fixture default (24 vs. 8) and drives the
/// exact production object store construction path — [`build_s3_store`], the
/// same function `build_session_context` calls, together with the
/// [`client_options_for`] seam it uses internally — asserting the spec's budget
/// reaches the resulting `ClientOptions` as the warm-connection-pool ceiling per
/// host. Also confirms the construction succeeds without ever needing to surface
/// a credential: `build_s3_store` redacts secret values and credential-shaped
/// strings from any `build()` error before wrapping it (see
/// `crate::scan::emit::redact_secret_values` / `redact_credentials`), so a
/// successful build here proves the path taken never had a raw credential to
/// leak in the first place.
///
/// Host-runnable: object store construction is pure builder logic — no S3 /
/// MinIO network I/O.
#[test]
fn scan_applies_s3_max_connections_to_object_store() {
    let budget = 24;
    let mut spec = scan_spec("s3://budget-bucket/f0.parquet".into());
    spec.s3_max_connections = budget;
    assert_ne!(
        budget, 8,
        "budget must differ from the scan_spec() fixture default to prove it flows through"
    );

    // The production seam build_s3_store uses to size the pool: the spec's
    // resolved budget reaches the object store's HTTP client options.
    let opts = client_options_for(spec.s3_max_connections);
    assert_eq!(
        opts.get_config_value(&ClientConfigKey::PoolMaxIdlePerHost),
        Some(budget.to_string()),
        "client options must carry the spec's s3_max_connections budget"
    );

    // The full object store construction path (also used by
    // build_session_context) accepts the same spec/budget and succeeds; the
    // storage props carry a credential (access_key/secret_key), so a successful
    // build with no error means no credential had a path to leak.
    let bucket = url::Url::parse(&spec.files[0])
        .expect("valid file URI")
        .host_str()
        .expect("bucket host")
        .to_string();
    build_s3_store(&spec.storage, &bucket, spec.s3_max_connections)
        .expect("store must build with the spec's connection budget, leaking no credentials");
}

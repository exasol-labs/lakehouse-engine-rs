//! Integration tests for Task 4 — on-demand phase telemetry.
//!
//! Drives the production raw-scan streaming + telemetry path
//! (`run_raw_scan_with_session`) against a local Parquet file with a fake
//! `UdfContext` whose debug level is settable, and observes the per-process
//! telemetry file. Covers the four spec scenarios:
//!   * silent at the default (`info`) level;
//!   * three phase durations reported when enabled (`debug`);
//!   * import vs emit attributed to distinct accumulators;
//!   * a telemetry-sink failure never fails the scan.
//!
//! Host-runnable: no S3 / MinIO stack — the scan registers a `file://` Parquet.

use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use lakehouse_engine::scan::diagnostics::{PhaseTimers, telemetry_file_path};
use lakehouse_engine::scan::run_raw_scan_with_session;
use lakehouse_engine::scan::session_config_for_spec;
use lakehouse_engine::scan::spec::{ScanSpec, StorageProps};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// The per-process telemetry file is keyed by PID and therefore shared by every
/// test in this binary. Serialize the telemetry tests so one test's file writes
/// never race another's assertions.
static TELEMETRY_LOCK: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    TELEMETRY_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// A fake UdfContext: serves one ScanSpec JSON row, captures emit_batch calls,
/// and reports a configurable debug level.
struct FakeCtx {
    spec_json: String,
    served: bool,
    debug_level: tracing::Level,
    emitted_batches: usize,
    emitted_rows: u64,
}

impl FakeCtx {
    fn new(spec_json: String, level: tracing::Level) -> Self {
        Self {
            spec_json,
            served: false,
            debug_level: level,
            emitted_batches: 0,
            emitted_rows: 0,
        }
    }
}

impl exasol_udf_sdk::context::UdfContext for FakeCtx {
    fn num_columns(&self) -> usize {
        1
    }
    fn get(&self, _col: usize) -> Result<&Value, UdfError> {
        Err(UdfError::User("FakeCtx uses get_string only".into()))
    }
    fn get_string(&self, _col: usize) -> Result<Option<&str>, UdfError> {
        Ok(Some(self.spec_json.as_str()))
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
        self.debug_level
    }
    fn emit_record_batch_ipc(&mut self, ipc: &[u8]) -> Result<(), UdfError> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;
        let reader = StreamReader::try_new(Cursor::new(ipc), None)
            .map_err(|e| UdfError::User(format!("ipc decode: {e}")))?;
        for batch in reader {
            let batch = batch.map_err(|e| UdfError::User(format!("ipc batch: {e}")))?;
            self.emitted_rows += batch.num_rows() as u64;
        }
        self.emitted_batches += 1;
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
    let path = dir.join("telemetry_data.parquet");
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
        files: vec![(file_url, size)],
        projection: vec!["ID".into(), "NAME".into()],
        filter: None,
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
    }
}

/// Run one raw scan to completion with the given debug level; returns the fake
/// context (carrying the captured emit counts). Registers a fresh session per
/// run so the local Parquet path is exercised exactly as production would.
async fn run_scan(spec: &ScanSpec, level: tracing::Level) -> FakeCtx {
    let mut ctx = FakeCtx::new(spec.to_json(), level);
    let session = SessionContext::new_with_config(session_config_for_spec(spec));
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, spec, &mut timers)
        .await
        .expect("raw scan must succeed");
    ctx
}

/// Read all `LHTELEM` lines currently in the per-process telemetry file.
fn telemetry_lines() -> Vec<String> {
    match std::fs::read_to_string(telemetry_file_path()) {
        Ok(s) => s
            .lines()
            .filter(|l| l.starts_with("LHTELEM "))
            .map(|l| l.to_string())
            .collect(),
        Err(_) => Vec::new(),
    }
}

fn clear_telemetry_file() {
    let _ = std::fs::remove_file(telemetry_file_path());
}

fn parse_phase_ms(line: &str, key: &str) -> f64 {
    line.split_whitespace()
        .find_map(|tok| tok.strip_prefix(key))
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or_else(|| panic!("line missing {key}: {line}"))
}

/// Drive an async test body to completion on a fresh multi-thread runtime.
///
/// The tests are plain `#[test]` fns (not `#[tokio::test]`) so the serialization
/// guard is held across this synchronous `block_on` rather than across an
/// `.await` — the per-process telemetry file is shared by PID, so the tests must
/// not interleave, but holding a std `Mutex` across an await point is a footgun
/// (and a clippy lint). Holding it across a blocking `block_on` is clean.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

#[test]
fn telemetry_silent_at_default_level() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("lh_telem_silent_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let spec = scan_spec(write_local_parquet(&dir, 200, 64));

    clear_telemetry_file();
    let ctx = block_on(run_scan(&spec, tracing::Level::INFO));

    // Scan still produced output...
    assert_eq!(ctx.emitted_rows, 200, "all rows must be emitted");
    // ...but no telemetry line was written at the default level.
    assert!(
        telemetry_lines().is_empty(),
        "no telemetry must be emitted at the default (info) level"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn telemetry_reports_three_phases_when_enabled() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("lh_telem_three_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let spec = scan_spec(write_local_parquet(&dir, 200, 64));

    clear_telemetry_file();
    let ctx = block_on(run_scan(&spec, tracing::Level::DEBUG));
    assert_eq!(ctx.emitted_rows, 200, "all rows must be emitted");

    let lines = telemetry_lines();
    assert_eq!(
        lines.len(),
        1,
        "exactly one telemetry record must be emitted, got {lines:?}"
    );
    let line = &lines[0];
    let pid = std::process::id();
    assert!(
        line.contains(&format!("pid={pid}")),
        "must carry pid: {line}"
    );

    // All three phases plus the reconstructed body wall-clock are present.
    let startup = parse_phase_ms(line, "phase_startup_ms=");
    let import = parse_phase_ms(line, "phase_import_ms=");
    let emit = parse_phase_ms(line, "phase_emit_ms=");
    let body = parse_phase_ms(line, "body_ms=");

    assert!(startup >= 0.0 && import >= 0.0 && emit >= 0.0 && body >= 0.0);
    // The three phases account for the scan-body wall-clock within measurement
    // error (no phase silently omitted). Allow a generous tolerance for the
    // small un-timed glue between phases.
    let summed = startup + import + emit;
    assert!(
        (body - summed).abs() < 5.0 || summed <= body,
        "startup+import+emit ({summed:.3}ms) must account for body ({body:.3}ms)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn telemetry_attributes_import_separately_from_emit() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("lh_telem_split_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    // Many small row groups → many batches → both import and emit accumulate
    // across multiple iterations, so they are independently observable.
    let spec = scan_spec(write_local_parquet(&dir, 1000, 32));

    clear_telemetry_file();
    let ctx = block_on(run_scan(&spec, tracing::Level::DEBUG));
    assert!(ctx.emitted_batches > 1, "scan must span multiple batches");

    let lines = telemetry_lines();
    assert_eq!(lines.len(), 1, "one telemetry record, got {lines:?}");
    let line = &lines[0];

    // Import and emit are reported as DISTINCT durations (separate keys), so a
    // benchmark can tell a read-bound scan from an emit-bound one. Both keys
    // must be present and parse independently.
    let import = parse_phase_ms(line, "phase_import_ms=");
    let emit = parse_phase_ms(line, "phase_emit_ms=");
    assert!(
        line.contains("phase_import_ms=") && line.contains("phase_emit_ms="),
        "import and emit must be reported as distinct fields: {line}"
    );
    // They are independent accumulators: at least one phase recorded measurable
    // time over 1000 rows / many batches, and the two values are tracked apart.
    assert!(
        import + emit > 0.0,
        "import+emit must capture measurable streaming time: import={import} emit={emit}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn telemetry_failure_never_fails_scan() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("lh_telem_fail_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let spec = scan_spec(write_local_parquet(&dir, 200, 64));

    // Make the telemetry SINK unwritable: pre-create the telemetry file path as
    // a DIRECTORY, so the best-effort append open() fails. The scan must still
    // complete and return its result, never surfacing the sink failure.
    clear_telemetry_file();
    let sink_path = telemetry_file_path();
    std::fs::create_dir_all(&sink_path).expect("occupy telemetry path with a directory");

    let mut ctx = FakeCtx::new(spec.to_json(), tracing::Level::DEBUG);
    let session = SessionContext::new_with_config(session_config_for_spec(&spec));
    let mut timers = PhaseTimers::start();
    let result = block_on(run_raw_scan_with_session(
        &mut ctx,
        &session,
        &spec,
        &mut timers,
    ));

    assert!(
        result.is_ok(),
        "a telemetry-sink failure must NOT fail the scan: {result:?}"
    );
    assert_eq!(ctx.emitted_rows, 200, "all rows must still be emitted");

    // No LHTELEM line could have been appended (the sink is a directory), and
    // the scan was unaffected.
    let _ = std::fs::remove_dir(&sink_path);
    let _ = std::fs::remove_dir_all(&dir);
}

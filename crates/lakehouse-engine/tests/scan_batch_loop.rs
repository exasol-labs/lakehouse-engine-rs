//! Batch-loop coverage for the SCALAR-EMIT scan (task 1.2).
//!
//! Exasol batches multiple input rows into ONE scalar `run()` call. `run_scan`
//! must loop over EVERY row and scan each row's assigned file list; reading only
//! the first row silently drops every later shard (the spike observed 108M of
//! 210M rows returned). These tests drive the production batch loop
//! ([`run_scan_batch`]) against a multi-row fake `UdfContext` backed by local
//! `file://` Parquet, injecting a local-file `SessionContext` builder so the loop
//! is exercised without an S3 / MinIO stack — the same seam
//! [`run_raw_scan_with_session`] already exposes for host tests.
//!
//! - A multi-row batch scans every row: the emitted rows are the UNION of all
//!   rows' file contents, not just the first row's.
//! - A single-row batch is byte-for-byte identical to the pre-batching output
//!   (the unchanged downstream [`run_raw_scan_with_session`] path over one spec).

use std::sync::Arc;

use arrow::array::{Array, Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{FileEntry, ScanSpec, StorageProps};
use lakehouse_engine::scan::{
    read_scan_spec, run_raw_scan_with_session, run_scan_batch, session_config_for_spec,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// A fake `UdfContext` serving several input rows, each with its own column
/// values, and capturing every `emit_batch` as a decoded `RecordBatch`.
///
/// Models the SCALAR batch: `next()` advances across rows, `get_string(col)`
/// reads the CURRENT row's column. Before the first `next()` the cursor is unset;
/// once exhausted it stays past the end (no current row).
struct BatchCtx {
    rows: Vec<Vec<Option<String>>>,
    cursor: Option<usize>,
    emitted: Vec<RecordBatch>,
}

impl BatchCtx {
    fn new(rows: Vec<Vec<Option<String>>>) -> Self {
        Self {
            rows,
            cursor: None,
            emitted: Vec::new(),
        }
    }

    fn current(&self) -> Option<&Vec<Option<String>>> {
        self.cursor.and_then(|i| self.rows.get(i))
    }
}

impl UdfContext for BatchCtx {
    fn num_columns(&self) -> usize {
        self.current().map(|r| r.len()).unwrap_or(0)
    }
    fn get(&self, _col: usize) -> Result<&Value, UdfError> {
        Err(UdfError::User("BatchCtx uses get_string only".into()))
    }
    fn get_string(&self, col: usize) -> Result<Option<&str>, UdfError> {
        Ok(self
            .current()
            .and_then(|r| r.get(col))
            .and_then(|c| c.as_deref()))
    }
    fn emit(&mut self, _values: &[Value]) -> Result<(), UdfError> {
        Err(UdfError::User("raw path must use emit_batch".into()))
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        let next_idx = self.cursor.map(|i| i + 1).unwrap_or(0);
        self.cursor = Some(next_idx);
        Ok(next_idx < self.rows.len())
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

/// Write a local Parquet at `dir/name` with `count` rows whose ids run
/// `start..start+count` (so files carry disjoint id ranges), and return its
/// `file://` URL.
fn write_parquet_ids(dir: &std::path::Path, name: &str, start: i64, count: i64) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(8))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let ids: Vec<i64> = (start..start + count).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();
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

fn file_size(file_url: &str) -> u64 {
    std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(file_url))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// A minimal raw-scan `ScanSpec` over one file (absolute `file://` URL, empty
/// `table_root`, no filter/limit), projecting ID/NAME.
fn spec_for_file(file_url: String) -> ScanSpec {
    let size = file_size(&file_url);
    ScanSpec {
        table_root: String::new(),
        files: vec![FileEntry::new(file_url, size)],
        projection: vec!["ID".into(), "NAME".into()],
        filter: None,
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
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

/// Split a spec into one batch row: `[common blob, files JSON]`, exactly as the
/// adapter splices the scalar scan's two arguments.
fn row_for_spec(spec: &ScanSpec) -> Vec<Option<String>> {
    vec![
        Some(spec.to_common_json()),
        Some(ScanSpec::files_json(&spec.files)),
    ]
}

/// A local-file `SessionContext` builder injected in place of the production
/// `build_session_context` (which requires an S3 bucket host). `file://` URLs
/// resolve through DataFusion's default LocalFileSystem store — no S3 needed.
fn local_session(spec: &ScanSpec, _memory_limit_bytes: u64) -> Result<SessionContext, UdfError> {
    Ok(SessionContext::new_with_config(session_config_for_spec(
        spec,
    )))
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

/// Collect the ID column across all emitted batches as `i64`. The raw emit path
/// coerces column 0 to the Arrow type its declared `DECIMAL(20,0)` EMITS type
/// accepts — `Decimal128(20,0)` (p>18) — so the id arrives as a scale-0
/// `Decimal128Array`; a spec with no declared type would keep the source
/// `Int64Array`. Handle both so the assertion tracks the real coercion.
fn ids_of(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0);
        if let Some(dec) = col.as_any().downcast_ref::<Decimal128Array>() {
            for i in 0..b.num_rows() {
                out.push(dec.value(i) as i64);
            }
        } else if let Some(ints) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..b.num_rows() {
                out.push(ints.value(i));
            }
        } else {
            panic!("unexpected id column type: {:?}", col.data_type());
        }
    }
    out.sort_unstable();
    out
}

/// Drive the production batch loop over `specs` (one batch row per spec) and
/// return the decoded emitted batches. Replicates `run_scan`'s prologue (advance
/// to the first row, read its spec, build the runtime) but injects a local-file
/// session so the loop runs without S3.
fn drive_batch(specs: &[ScanSpec]) -> Vec<RecordBatch> {
    let rows: Vec<Vec<Option<String>>> = specs.iter().map(row_for_spec).collect();
    let mut ctx = BatchCtx::new(rows);
    assert!(ctx.next().expect("first next"), "at least one input row");
    let first_spec = read_scan_spec(&ctx).expect("reconstitute first spec");
    block_on(run_scan_batch(&mut ctx, first_spec, local_session)).expect("batch scan");
    ctx.emitted
}

/// A multi-row batch scans EVERY row: the emitted rows are the union of all three
/// files' disjoint id ranges — not just the first row's (the drop-past-first bug).
#[test]
fn multi_row_batch_scans_every_shard_row() {
    let dir = std::env::temp_dir().join(format!("lh_batch_multi_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // Three shards, disjoint id ranges: 0..10, 100..110, 200..210.
    let specs = vec![
        spec_for_file(write_parquet_ids(&dir, "f0.parquet", 0, 10)),
        spec_for_file(write_parquet_ids(&dir, "f1.parquet", 100, 10)),
        spec_for_file(write_parquet_ids(&dir, "f2.parquet", 200, 10)),
    ];

    let emitted = drive_batch(&specs);

    assert_eq!(
        total_rows(&emitted),
        30,
        "all three shard rows must be scanned (10 each); dropping rows past the first \
         would yield only 10"
    );
    let mut expected: Vec<i64> = (0..10).chain(100..110).chain(200..210).collect();
    expected.sort_unstable();
    assert_eq!(
        ids_of(&emitted),
        expected,
        "emitted ids must be the union of every shard's file contents"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A single-row batch is byte-for-byte identical to the pre-batching output: the
/// unchanged downstream `run_raw_scan_with_session` path over the same one spec.
#[test]
fn single_row_batch_is_byte_identical_to_pre_batching_output() {
    let dir = std::env::temp_dir().join(format!("lh_batch_single_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let spec = spec_for_file(write_parquet_ids(&dir, "only.parquet", 0, 200));

    // Batched path: one input row through the production batch loop.
    let batched = drive_batch(std::slice::from_ref(&spec));

    // Pre-batching reference: drive the unchanged downstream raw-scan path over
    // the same spec, with an equivalent local session.
    let reference = block_on(async {
        let mut ctx = BatchCtx::new(vec![row_for_spec(&spec)]);
        assert!(ctx.next().expect("next"), "one row");
        let session = local_session(&spec, 0).expect("session");
        let mut timers = PhaseTimers::start();
        run_raw_scan_with_session(&mut ctx, &session, &spec, &mut timers)
            .await
            .expect("reference raw scan");
        ctx.emitted
    });

    assert_eq!(total_rows(&batched), 200, "single-row batch scans all rows");
    assert_eq!(
        batched, reference,
        "a single-row batch must emit rows byte-for-byte identical to the pre-batching path"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

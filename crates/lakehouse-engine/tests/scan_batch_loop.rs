//! Per-row scalar-dispatch coverage for the SCALAR-EMIT scan.
//!
//! Under SDK 0.21.0 Exasol drives a scalar `run()` once PER ROW — it does NOT
//! hand a whole multi-row batch to one call, and `ctx.next()` in scalar context
//! is now a runtime error rather than a loop advance. So the scan reconstitutes
//! exactly one row's `ScanSpec` (via [`read_scan_spec`], no `ctx.next()`), scans
//! that row's assigned file list, and returns; Exasol invokes it again for the
//! next row. The "no dropped rows" guarantee is therefore an emergent property of
//! the fan-out: the UNION of every per-row [`run_scan_one`] call must cover every
//! shard. (An earlier batch-loop bug silently dropped every shard past the first,
//! returning 108M of 210M rows — the exact regression these tests guard.)
//!
//! These tests drive [`run_scan_one`] once per row against a single-row fake
//! `UdfContext` backed by local `file://` Parquet, injecting a local-file
//! `SessionContext` builder so the path runs without an S3 / MinIO stack — the
//! same seam [`run_raw_scan_with_session`] already exposes for host tests. This
//! harness mirrors `run_scan`'s structure (reconstitute spec, build runtime,
//! run, tear down) but calls [`run_scan_one`] and [`build_scan_runtime`]
//! directly rather than through `run_scan` itself, so it checks the harness's
//! own call discipline rather than exercising `run_scan` end to end.
//!
//! - `per_row_calls_emit_union_of_all_shards`: N independent per-row calls emit
//!   the UNION of all shards' disjoint file contents — not just the first row's.
//! - `run_scan_one_builds_and_tears_down_runtime_per_call`: this harness builds
//!   and tears down its own fresh Tokio runtime per row (mirroring `run_scan`'s
//!   structure); it does not exercise `run_scan` itself, which calls
//!   `build_scan_runtime` directly rather than through an injected seam.
//! - `single_row_call_is_byte_identical_to_direct_raw_scan`: one row through the
//!   per-row seam is byte-for-byte identical to the unchanged downstream
//!   [`run_raw_scan_with_session`] path over the same spec.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, Decimal128Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{FileEntry, ScanSpec, StorageProps};
use lakehouse_engine::scan::{
    build_scan_runtime, read_scan_spec, run_raw_scan_with_session, run_scan_one,
    session_config_for_spec,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// A fake `UdfContext` serving exactly ONE input row (its two column values) and
/// capturing every `emit_batch` as a decoded `RecordBatch`.
///
/// Models SDK 0.21.0 scalar dispatch: one `run()` call sees one row, so
/// `get_string(col)` reads that row's column directly with no cursor. `next()` is
/// a hard error — a scalar `run()` iterating with `ctx.next()` is exactly the
/// illegal batch-loop behavior these tests guard against, so any call to it fails
/// the test loudly rather than silently masking a regression.
struct RowCtx {
    row: Vec<Option<String>>,
    emitted: Vec<RecordBatch>,
}

impl RowCtx {
    fn new(row: Vec<Option<String>>) -> Self {
        Self {
            row,
            emitted: Vec::new(),
        }
    }
}

impl UdfContext for RowCtx {
    fn num_columns(&self) -> usize {
        self.row.len()
    }
    fn get(&self, _col: usize) -> Result<&Value, UdfError> {
        Err(UdfError::User("RowCtx uses get_string only".into()))
    }
    fn get_string(&self, col: usize) -> Result<Option<&str>, UdfError> {
        Ok(self.row.get(col).and_then(|c| c.as_deref()))
    }
    fn emit(&mut self, _values: &[Value]) -> Result<(), UdfError> {
        Err(UdfError::User("raw path must use emit_batch".into()))
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        Err(UdfError::User(
            "scalar run() handles exactly one row; ctx.next() must never be called".into(),
        ))
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

/// Write a local Parquet at `dir/name` with a single Utf8 `category` column
/// holding `values` verbatim (duplicates included), and return its `file://` URL.
fn write_parquet_categories(dir: &std::path::Path, name: &str, values: &[&str]) -> String {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "category",
        DataType::Utf8,
        false,
    )]));
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))])
        .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

/// A DISTINCT row-scan `ScanSpec` over one file: single-column `CATEGORY`
/// projection with `distinct: true` and no LIMIT/ORDER BY — the same fan-out
/// shape the single-group `COUNT(DISTINCT col)` adapter path builds
/// (`single_group_agg.rs`), minus the NULL-excluding filter (not needed here
/// since the fixture carries no NULLs).
fn distinct_spec_for_file(file_url: String) -> ScanSpec {
    let size = file_size(&file_url);
    ScanSpec {
        table_root: String::new(),
        files: vec![FileEntry::new(file_url, size)],
        projection: vec!["CATEGORY".into()],
        filter: None,
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        distinct: true,
        emit_exa_types: vec!["VARCHAR(2000000)".into()],
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

/// Build one scalar-input row for `spec`: `[common blob, files JSON]`, exactly as
/// the adapter splices the scalar scan's two arguments for a single fan-out row.
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

/// Collect the CATEGORY column across all emitted batches as `String`, sorted.
/// DataFusion's Parquet reader may return the raw scan plan's string column as
/// either `Utf8` or `Utf8View`; the emit path coerces it before it crosses the
/// UDF boundary, so `StringArray` is expected here, but both are accepted per
/// the repo's established downcast pattern (`scan_parquet_pruning.rs`,
/// `scan_plan_shape.rs`).
fn categories_of(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0);
        if let Some(v) = col.as_any().downcast_ref::<StringViewArray>() {
            for i in 0..b.num_rows() {
                out.push(v.value(i).to_string());
            }
        } else if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
            for i in 0..b.num_rows() {
                out.push(s.value(i).to_string());
            }
        } else {
            panic!("unexpected category column type: {:?}", col.data_type());
        }
    }
    out.sort_unstable();
    out
}

/// Build a fresh Tokio runtime for one per-row scan by calling the real
/// runtime builder [`build_scan_runtime`], recording the construction in `built`.
/// This harness calls the builder once per row by construction, so `built`
/// counts this test's own call discipline — it does NOT exercise `run_scan`
/// (the actual UDF entry point), which calls `build_scan_runtime` directly
/// rather than through an injected seam. A future regression that cached a
/// runtime inside `run_scan` itself would not be caught by this counter.
/// `threads` comes from the row's `df_threads_per_udf`, so the runtime kind
/// matches what production would size for this row.
fn counting_build_runtime(threads: usize, built: &AtomicUsize) -> tokio::runtime::Runtime {
    built.fetch_add(1, Ordering::SeqCst);
    build_scan_runtime(threads).expect("build per-row runtime")
}

/// Drive ONE scalar `run()` call for a single shard: reconstitute the row's spec
/// (proving the read-one-row-no-`next()` contract), build a fresh runtime, run
/// [`run_scan_one`] to completion on it, then tear that runtime down explicitly —
/// mirroring production `run_scan` for a single row. Returns the emitted batches.
fn run_one_row(spec: &ScanSpec, built: &AtomicUsize) -> Vec<RecordBatch> {
    let mut ctx = RowCtx::new(row_for_spec(spec));
    // Reconstitute this row's spec from the two scalar arguments, exactly as
    // production does — reading only columns 0 and 1, never calling ctx.next().
    let reconstituted = read_scan_spec(&ctx).expect("reconstitute row spec");
    let rt = counting_build_runtime(reconstituted.df_threads_per_udf, built);
    let result = rt.block_on(run_scan_one(&mut ctx, reconstituted, local_session));
    result.expect("per-row scan");
    // Explicit, deterministic teardown of THIS call's runtime — the runtime is a
    // call-local value consumed here, never hoisted out of the per-row loop.
    rt.shutdown_timeout(std::time::Duration::from_secs(5));
    ctx.emitted
}

/// Drive one independent scalar `run()` call per shard spec and concatenate the
/// emitted batches across all N calls. This concatenation IS the fan-out UNION the
/// regression guard asserts against.
fn run_all_rows(specs: &[ScanSpec], built: &AtomicUsize) -> Vec<RecordBatch> {
    specs.iter().flat_map(|s| run_one_row(s, built)).collect()
}

/// N independent per-row `run()` calls emit the UNION of every shard: the three
/// files' disjoint id ranges, all present — not just the first row's (the
/// drop-past-first bug that returned 108M of 210M rows).
#[test]
fn per_row_calls_emit_union_of_all_shards() {
    let dir = std::env::temp_dir().join(format!("lh_perrow_multi_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // Three shards, disjoint id ranges: 0..10, 100..110, 200..210.
    let specs = vec![
        spec_for_file(write_parquet_ids(&dir, "f0.parquet", 0, 10)),
        spec_for_file(write_parquet_ids(&dir, "f1.parquet", 100, 10)),
        spec_for_file(write_parquet_ids(&dir, "f2.parquet", 200, 10)),
    ];

    let built = AtomicUsize::new(0);
    let emitted = run_all_rows(&specs, &built);

    assert_eq!(
        total_rows(&emitted),
        30,
        "every shard is scanned by its own run() call (10 rows each); dropping any \
         shard's call would leave fewer than 30 rows"
    );
    let mut expected: Vec<i64> = (0..10).chain(100..110).chain(200..210).collect();
    expected.sort_unstable();
    assert_eq!(
        ids_of(&emitted),
        expected,
        "emitted ids must be the UNION of every per-row call's file contents"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// This harness's `run_one_row` builds and tears down its own fresh Tokio
/// runtime per row (mirroring `run_scan`'s structure). Driving N shards
/// constructs exactly N runtimes in the harness — this does not exercise
/// `run_scan` itself; see [`counting_build_runtime`].
#[test]
fn run_scan_one_builds_and_tears_down_runtime_per_call() {
    let dir = std::env::temp_dir().join(format!("lh_perrow_rt_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let specs = vec![
        spec_for_file(write_parquet_ids(&dir, "r0.parquet", 0, 10)),
        spec_for_file(write_parquet_ids(&dir, "r1.parquet", 50, 10)),
        spec_for_file(write_parquet_ids(&dir, "r2.parquet", 100, 10)),
    ];

    let built = AtomicUsize::new(0);
    let emitted = run_all_rows(&specs, &built);

    assert_eq!(
        built.load(Ordering::SeqCst),
        specs.len(),
        "harness must build one fresh runtime per row (this checks the harness's own \
         call discipline, not run_scan's)"
    );
    // Sanity: with one fresh runtime per call, every shard still emits its rows —
    // teardown of each call-local runtime does not drop the next call's output.
    assert_eq!(
        total_rows(&emitted),
        30,
        "every per-row call scans its shard even though its runtime is torn down \
         before the next call builds a fresh one"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A single row through the per-row seam is byte-for-byte identical to the
/// unchanged downstream `run_raw_scan_with_session` path over the same one spec.
#[test]
fn single_row_call_is_byte_identical_to_direct_raw_scan() {
    let dir = std::env::temp_dir().join(format!("lh_perrow_single_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let spec = spec_for_file(write_parquet_ids(&dir, "only.parquet", 0, 200));

    // Per-row path: one scalar run() call through the production per-row seam.
    let built = AtomicUsize::new(0);
    let per_row = run_one_row(&spec, &built);

    // Reference: drive the unchanged downstream raw-scan path over the same spec,
    // with an equivalent local session.
    let reference = block_on(async {
        let mut ctx = RowCtx::new(row_for_spec(&spec));
        let session = local_session(&spec, 0).expect("session");
        let mut timers = PhaseTimers::start();
        run_raw_scan_with_session(&mut ctx, &session, &spec, &mut timers)
            .await
            .expect("reference raw scan");
        ctx.emitted
    });

    assert_eq!(total_rows(&per_row), 200, "single-row call scans all rows");
    assert_eq!(
        per_row, reference,
        "a single per-row call must emit rows byte-for-byte identical to the direct \
         raw-scan path"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A `distinct: true` scan spec streams exactly one row per shard-local distinct
/// projected value through the same `emit_batch`/batch-loop mechanism ordinary
/// row-scans use — the single-group `COUNT(DISTINCT col)` fan-out shape
/// (`vs-adapter/pushdown-planning-count-distinct`; see `scan/mod.rs`
/// `build_dataframe`'s `if spec.distinct { df.distinct() }`). Ten rows over a
/// three-value column collapse to exactly those three distinct values, each
/// appearing once, proving `.distinct()` is actually applied at the DataFusion/
/// batch level rather than merely accepted at the SQL-generation level (that
/// contract is covered separately by the `support.rs` SQL-shape unit tests).
#[test]
fn distinct_row_scan_streams_one_row_per_distinct_value() {
    let dir = std::env::temp_dir().join(format!("lh_distinct_row_scan_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // Ten rows, three distinct values, each duplicated at least once.
    let values = ["a", "b", "a", "c", "b", "a", "c", "b", "a", "c"];
    let file_url = write_parquet_categories(&dir, "categories.parquet", &values);
    let spec = distinct_spec_for_file(file_url);

    let built = AtomicUsize::new(0);
    let emitted = run_one_row(&spec, &built);

    assert_eq!(
        total_rows(&emitted),
        3,
        "distinct: true must collapse 10 duplicate-laden rows down to the 3 \
         distinct values, not stream all 10"
    );
    assert_eq!(
        categories_of(&emitted),
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        "emitted rows must be exactly the distinct value set, no duplicates"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

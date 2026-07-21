//! Synthetic micro-benchmarks (plan task 5.1 / 5.2 / 5.3) — NO spec.
//!
//! Two paths are isolated so end-to-end throughput can later be attributed:
//!
//! - **5.1 emit-only** — the work the scan does BEFORE the SDK boundary on every
//!   batch: `coerce_batch_to_exa_types` (the real coercion the emit loop runs) +
//!   Arrow IPC serialization via `StreamWriter`. This is exactly what
//!   `ctx.emit_batch(&batch)` does internally (`record_batch_to_ipc`, SDK
//!   `context.rs`) before any bytes cross the `.so`. It does NOT include the ZMQ
//!   `MT_EMIT` round-trip to the engine — `emit_batch` only exists inside the UDF
//!   runtime and cannot be called from a host benchmark. So 5.1 measures
//!   build+coerce+serialize, the pre-SDK emit cost; the round-trip is measured
//!   end-to-end on the cluster (tasks 6/7).
//!
//! - **5.2 scan-only** — Iceberg/Parquet → DataFusion stream, drained WITHOUT
//!   emitting. Reuses the production seams `session_config_for_spec` +
//!   `build_raw_scan_physical_plan` against a self-contained local Parquet file
//!   (no MinIO/S3 dependency), then `execute_stream` + drain. Isolates
//!   read+decode throughput from send-back.
//!
//! Run (host debug/bench build — never rebuilds the cdylib `.so`):
//!
//! ```bash
//! # full numbers (ignored by default so `cargo test` stays fast):
//! cargo test -p lakehouse-engine --test micro_bench -- --ignored --nocapture
//! # release-opt numbers (goes to target/release/deps, NOT the cdylib):
//! cargo test -p lakehouse-engine --test micro_bench --release -- --ignored --nocapture
//! # smoke checks only (CI): assert each path yields a positive GB/sec
//! cargo test -p lakehouse-engine --test micro_bench
//! ```

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use futures::StreamExt;
use lakehouse_engine::scan::emit::coerce_batch_to_exa_types;
use lakehouse_engine::scan::spec::{FileEntry, ScanSpec, StorageProps};
use lakehouse_engine::scan::{build_raw_scan_physical_plan, session_config_for_spec};
use parquet::arrow::ArrowWriter;

const GB: f64 = 1_000_000_000.0;

/// Resident set size in bytes from `/proc/self/statm` (best-effort, 0 if unreadable).
/// Mirrors `scan::diagnostics::current_rss_bytes` (which is private); 3 lines is
/// lazier than widening the crate's public surface for a benchmark.
fn rss_bytes() -> u64 {
    const PAGE: u64 = 4096;
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| {
            s.split_ascii_whitespace()
                .nth(1)
                .and_then(|f| f.parse().ok())
        })
        .map(|pages: u64| pages * PAGE)
        .unwrap_or(0)
}

// ------------------------------------------------------------------ 5.1 emit -

/// Serialize one RecordBatch to Arrow IPC stream bytes — byte-for-byte what the
/// SDK's `record_batch_to_ipc` (exasol-udf-sdk `context.rs`) does inside
/// `emit_batch` before the bytes cross the `.so` boundary: a fresh `StreamWriter`,
/// one `write`, then `finish`.
fn record_batch_to_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
        .expect("ipc writer init");
    w.write(batch).expect("ipc write");
    w.finish().expect("ipc finish");
    buf
}

/// One emit-path measurement: coerce + IPC-serialize `batch` `iters` times.
/// Returns (rows/sec, in-memory GB/sec, ipc-out GB/sec, ipc bytes).
fn bench_emit_path(name: &str, batch: &RecordBatch, exa_types: &[String], iters: usize) {
    let rows = batch.num_rows();
    let in_mem_bytes = batch.get_array_memory_size();

    // Warmup (also fills the JIT/branch predictors and validates coercion).
    let warm = coerce_batch_to_exa_types(batch.clone(), exa_types).expect("coerce");
    let ipc_bytes = record_batch_to_ipc(&warm).len();

    let rss_before = rss_bytes();
    let start = Instant::now();
    let mut sink: u64 = 0;
    for _ in 0..iters {
        let coerced = coerce_batch_to_exa_types(batch.clone(), exa_types).expect("coerce");
        let ipc = record_batch_to_ipc(&coerced);
        sink = sink.wrapping_add(ipc.len() as u64);
        std::hint::black_box(&ipc);
        drop(ipc);
        drop(coerced);
    }
    let elapsed = start.elapsed().as_secs_f64();
    let rss_after = rss_bytes();
    std::hint::black_box(sink);

    let total_rows = (rows * iters) as f64;
    let total_in_mem = (in_mem_bytes * iters) as f64;
    let total_ipc = (ipc_bytes * iters) as f64;
    println!(
        "5.1 emit   {name:<12} rows/s={:>14.0}  in-mem GB/s={:>7.3}  ipc-out GB/s={:>7.3}  \
         (rows={rows} in-mem={in_mem_bytes}B ipc={ipc_bytes}B iters={iters} t={elapsed:.3}s \
         rss_delta={}MB)",
        total_rows / elapsed,
        total_in_mem / GB / elapsed,
        total_ipc / GB / elapsed,
        (rss_after.saturating_sub(rss_before)) / (1024 * 1024),
    );
}

/// Build a single-column batch of `n` rows for the named primitive schema, with
/// the Exasol EMITS type string the production emit loop would coerce against.
fn primitive_batch(kind: &str, n: usize) -> (RecordBatch, Vec<String>) {
    let (field, col, exa): (Field, ArrayRef, &str) = match kind {
        "BIGINT" => (
            Field::new("c", DataType::Int64, false),
            Arc::new(Int64Array::from_iter_values((0..n).map(|i| i as i64))),
            "DECIMAL(20,0)",
        ),
        "DOUBLE" => (
            Field::new("c", DataType::Float64, false),
            Arc::new(Float64Array::from_iter_values(
                (0..n).map(|i| i as f64 * 1.5),
            )),
            "DOUBLE PRECISION",
        ),
        "TIMESTAMP" => (
            Field::new("c", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                (0..n).map(|i| 1_700_000_000_000_000 + i as i64),
            )),
            "TIMESTAMP",
        ),
        "DECIMAL" => (
            Field::new("c", DataType::Decimal128(20, 4), false),
            Arc::new(
                Decimal128Array::from_iter_values((0..n).map(|i| i as i128 * 12345))
                    .with_precision_and_scale(20, 4)
                    .expect("decimal"),
            ),
            "DECIMAL(20,4)",
        ),
        "VARCHAR" => (
            Field::new("c", DataType::Utf8, false),
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!("string-value-row-{i:08}")),
            )),
            "VARCHAR(2000000)",
        ),
        other => panic!("unknown primitive schema {other}"),
    };
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema, vec![col]).expect("batch");
    (batch, vec![exa.to_string()])
}

/// A TPC-H `lineitem`-shaped mixed batch (the "production" schema): the column
/// types a real lineitem scan emits, so 5.1 measures a realistic mixed row.
fn lineitem_batch(n: usize) -> (RecordBatch, Vec<String>) {
    let fields = vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Decimal128(15, 2), false),
        Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new(
            "l_committs",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ];
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values((0..n).map(|i| i as i64))),
        Arc::new(Int64Array::from_iter_values((0..n).map(|i| (i * 7) as i64))),
        Arc::new(
            Decimal128Array::from_iter_values((0..n).map(|i| (i as i128 % 50 + 1) * 100))
                .with_precision_and_scale(15, 2)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from_iter_values((0..n).map(|i| (i as i128 % 100000) * 137))
                .with_precision_and_scale(15, 2)
                .unwrap(),
        ),
        Arc::new(Float64Array::from_iter_values(
            (0..n).map(|i| (i % 11) as f64 / 100.0),
        )),
        Arc::new(StringArray::from_iter_values(
            (0..n).map(|i| if i % 2 == 0 { "N" } else { "R" }),
        )),
        Arc::new(Date32Array::from_iter_values(
            (0..n).map(|i| 19_000 + (i % 2000) as i32),
        )),
        Arc::new(TimestampMicrosecondArray::from_iter_values(
            (0..n).map(|i| 1_700_000_000_000_000 + i as i64 * 1000),
        )),
        Arc::new(StringArray::from_iter_values(
            (0..n).map(|_| "DELIVER IN PERSON"),
        )),
        Arc::new(StringArray::from_iter_values(
            (0..n).map(|i| format!("comment text for line item number {i}")),
        )),
    ];
    let exa = vec![
        "DECIMAL(20,0)".to_string(),
        "DECIMAL(20,0)".to_string(),
        "DECIMAL(15,2)".to_string(),
        "DECIMAL(15,2)".to_string(),
        "DOUBLE PRECISION".to_string(),
        "VARCHAR(2000000)".to_string(),
        "DATE".to_string(),
        "TIMESTAMP".to_string(),
        "VARCHAR(2000000)".to_string(),
        "VARCHAR(2000000)".to_string(),
    ];
    let schema = Arc::new(Schema::new(fields));
    (
        RecordBatch::try_new(schema, cols).expect("lineitem batch"),
        exa,
    )
}

#[test]
#[ignore = "micro-benchmark; run with --ignored --nocapture"]
fn bench_emit_only() {
    let rows = 200_000usize;
    let iters = 50usize;
    println!(
        "\n=== 5.1 emit-only (coerce + Arrow IPC StreamWriter; pre-SDK, no ZMQ round-trip) ==="
    );
    for kind in ["BIGINT", "DOUBLE", "TIMESTAMP", "DECIMAL", "VARCHAR"] {
        let (batch, exa) = primitive_batch(kind, rows);
        bench_emit_path(kind, &batch, &exa, iters);
    }
    let (li, exa) = lineitem_batch(rows);
    bench_emit_path("lineitem", &li, &exa, iters);
}

/// Smoke check (runnable in CI): the emit path produces a positive GB/sec on a
/// tiny input. Fails if coercion or IPC serialization regresses to zero/panic.
#[test]
fn emit_path_smoke_positive_throughput() {
    let (batch, exa) = lineitem_batch(1000);
    let in_mem = batch.get_array_memory_size();
    let start = Instant::now();
    let coerced = coerce_batch_to_exa_types(batch, &exa).expect("coerce");
    let ipc = record_batch_to_ipc(&coerced);
    let secs = start.elapsed().as_secs_f64();
    assert!(!ipc.is_empty(), "IPC serialization produced bytes");
    let gb_per_s = (in_mem as f64 / GB) / secs.max(1e-9);
    assert!(gb_per_s > 0.0, "emit path GB/sec must be positive");
}

// ------------------------------------------------------------------ 5.2 scan -

/// Write a TPC-H lineitem-shaped local Parquet file of `n` rows; return its
/// file:// URL and on-disk byte size.
fn write_lineitem_parquet(path: &std::path::Path, n: usize) -> (String, u64) {
    let (batch, _exa) = lineitem_batch(n);
    let file = std::fs::File::create(path).expect("create parquet");
    // Default writer props: real-world compression so bytes-on-disk is realistic.
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    // Write in batch-sized chunks so the file has multiple row groups.
    let chunk = 50_000usize;
    let mut offset = 0;
    while offset < n {
        let len = chunk.min(n - offset);
        writer
            .write(&batch.slice(offset, len))
            .expect("write chunk");
        offset += len;
    }
    writer.close().expect("close writer");
    let size = std::fs::metadata(path).expect("stat").len();
    let url = url::Url::from_file_path(path)
        .expect("abs path")
        .to_string();
    (url, size)
}

fn scan_spec(file_url: String) -> ScanSpec {
    let size = std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(&file_url))
        .map(|m| m.len())
        .unwrap_or(0);
    ScanSpec {
        table_root: String::new(),
        files: vec![FileEntry::new(file_url, size)],
        projection: Vec::new(),
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
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    }
}

/// Build the production raw-scan plan over the registered local Parquet and drain
/// the stream WITHOUT emitting. Returns (rows, decoded in-memory bytes).
async fn drain_scan(file_url: &str) -> (u64, u64) {
    let spec = scan_spec(file_url.to_string());
    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    ctx.register_parquet("scan_target", &spec.files[0].path, Default::default())
        .await
        .expect("register parquet");
    let plan = build_raw_scan_physical_plan(&ctx, &spec)
        .await
        .expect("physical plan");
    let mut stream =
        datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).expect("execute_stream");
    let mut rows = 0u64;
    let mut decoded = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch");
        rows += batch.num_rows() as u64;
        decoded += batch.get_array_memory_size() as u64;
        std::hint::black_box(&batch);
        drop(batch);
    }
    (rows, decoded)
}

#[test]
#[ignore = "micro-benchmark; run with --ignored --nocapture"]
fn bench_scan_only() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let n = 2_000_000usize;
    let iters = 5usize;
    let dir = std::env::temp_dir().join(format!("lh_scan_bench_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("lineitem.parquet");
    let (url, disk_bytes) = write_lineitem_parquet(&path, n);

    println!("\n=== 5.2 scan-only (Parquet read+decode → DataFusion stream, drained, NO emit) ===");
    println!(
        "file: {} rows, {:.2} MB on disk ({} row-group chunks)",
        n,
        disk_bytes as f64 / (1024.0 * 1024.0),
        n.div_ceil(50_000),
    );

    // Warmup (page cache + plan build).
    let _ = rt.block_on(drain_scan(&url));

    let rss_before = rss_bytes();
    let start = Instant::now();
    let mut total_rows = 0u64;
    let mut total_decoded = 0u64;
    for _ in 0..iters {
        let (rows, decoded) = rt.block_on(drain_scan(&url));
        total_rows += rows;
        total_decoded += decoded;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let rss_after = rss_bytes();

    let total_disk = (disk_bytes as f64) * iters as f64;
    println!(
        "5.2 scan   lineitem     rows/s={:>14.0}  disk GB/s={:>7.3}  decoded GB/s={:>7.3}  \
         (rows={total_rows} t={elapsed:.3}s iters={iters} rss_delta={}MB)",
        total_rows as f64 / elapsed,
        total_disk / GB / elapsed,
        total_decoded as f64 / GB / elapsed,
        (rss_after.saturating_sub(rss_before)) / (1024 * 1024),
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Smoke check (runnable in CI): the scan path reads+decodes a tiny local Parquet
/// and yields positive throughput. Fails if the plan-build or drain regresses.
#[test]
fn scan_path_smoke_positive_throughput() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = std::env::temp_dir().join(format!("lh_scan_smoke_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("lineitem.parquet");
    let (url, disk_bytes) = write_lineitem_parquet(&path, 5000);
    let start = Instant::now();
    let (rows, _decoded) = rt.block_on(drain_scan(&url));
    let secs = start.elapsed().as_secs_f64();
    assert_eq!(rows, 5000, "drained every row");
    let gb_per_s = (disk_bytes as f64 / GB) / secs.max(1e-9);
    assert!(gb_per_s > 0.0, "scan path GB/sec must be positive");
    let _ = std::fs::remove_dir_all(&dir);
}

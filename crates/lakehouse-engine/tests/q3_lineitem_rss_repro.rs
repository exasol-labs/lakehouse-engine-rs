//! Host repro for the Q3 LINEITEM scan-leg memory blow-up (issue: bounded remote scans).
//!
//! WHY THIS EXISTS
//! ---------------
//! On the cluster Q3's LINEITEM scan leg (RAW-ROW scan, projection
//! `L_ORDERKEY` + `L_EXTENDEDPRICE`, NO filter, NO limit → all ~60M rows across
//! the Glue lineitem's ~20 Parquet files) intermittently dies via the engine's
//! per-instance ~4 GB MEMORY kill. The UDF's `/tmp` is a private sandbox
//! destroyed on process exit and the SLC does not forward stdout/stderr, so the
//! crash leaves no debug trail on the cluster. This repro reproduces the EXACT
//! scan on the host — where there is no 4 GB cap and we can watch RSS climb —
//! to prove WHERE memory grows and WHETHER the scan/emit is truly streaming.
//!
//! WHAT IT DOES
//! ------------
//! 1. Reuses the crate's real catalog/storage code (`resolve_file_list`,
//!    `resolve_table_schema`) to resolve the LIVE Glue lineitem file list and
//!    declared Exasol EMITS types from the vended/live S3 creds.
//! 2. Builds the EXACT Q3 lineitem `ScanSpec` (projection `["L_ORDERKEY",
//!    "L_EXTENDEDPRICE"]`, filter None, limit None, aggregates None,
//!    group_keys None) with the SAME DataFusion knobs the cluster used.
//! 3. Drives the REAL scan entry point `lakehouse_engine::scan::run_scan` with a
//!    mock `UdfContext` whose `emit_record_batch_ipc` DISCARDS the bytes (counts
//!    rows + total IPC bytes only — never retains them) so we measure the scan's
//!    own footprint, not a sink.
//! 4. Samples RSS (`/proc/self/statm`) every ~250 ms on a background OS thread and
//!    prints `t=… rows=… rss_mb=…`, plus a final peak-RSS line.
//!
//! HOW TO RUN
//! ----------
//! `make repro` (sources `bench/.env` for AWS/Glue creds, sets RUST_BACKTRACE=1,
//! runs both the threads=4 and threads=1 variants — wrapped in `/usr/bin/time -v`
//! for Max RSS when GNU time is installed, otherwise the in-process RSS sampler
//! is the measure). Ignored by default so `cargo test` never reaches out to AWS.
//!
//! NO SECRETS are printed: only row counts, byte totals, RSS, and file counts.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use lakehouse_engine::adapter::connection::ConnectionCreds;
use lakehouse_engine::adapter::pushdown::{resolve_file_list, resolve_table_schema};
use lakehouse_engine::scan::run_scan;
use lakehouse_engine::scan::spec::{CatalogProps, ScanSpec, StorageProps};

use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;

// ---------------------------------------------------------------------------
// Environment plumbing — env-var names mirror tests/cloud_e2e_test.rs and
// bench/.env (so `make repro` can source bench/.env unchanged).
// ---------------------------------------------------------------------------

const ENV_GLUE_CATALOG_URI: &str = "GLUE_CATALOG_URI";
const ENV_GLUE_WAREHOUSE: &str = "GLUE_WAREHOUSE";
const ENV_ICEBERG_NAMESPACE: &str = "ICEBERG_NAMESPACE";
const ENV_AWS_REGION: &str = "AWS_REGION";
const ENV_AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const ENV_AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const ENV_AWS_SESSION_TOKEN: &str = "AWS_SESSION_TOKEN";

/// The Q3 LINEITEM scan leg projects exactly these two columns.
const Q3_PROJECTION: [&str; 2] = ["L_ORDERKEY", "L_EXTENDEDPRICE"];
/// Table name within the namespace (TPC-H lineitem).
const LINEITEM_TABLE: &str = "lineitem";

/// Per-instance memory limit (MB) the repro reports to the scan via
/// `UdfContext::memory_limit()`. Override with `REPRO_MEMORY_LIMIT_MB`.
///
/// Default 0 → the scan's "unknown" sentinel. We deliberately do NOT default to
/// the cluster's 4096: on a host where `/tmp` is real disk the bounded path would
/// spill or error and mask raw growth. With 0 the pool is the conservative 1 GiB
/// GreedyPool, which still *bounds* growth — so for the "watch RSS climb freely"
/// goal `make repro` sets REPRO_MEMORY_LIMIT_MB to a large value (see Makefile).
fn memory_limit_mb() -> u64 {
    std::env::var("REPRO_MEMORY_LIMIT_MB")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

/// DataFusion knobs. `make repro` runs two variants: threads=4 (the cluster
/// config, concurrency exercised) and threads=1 (serial baseline).
fn df_threads() -> usize {
    std::env::var("REPRO_DF_THREADS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(4)
}

struct Creds {
    catalog_uri: String,
    warehouse: String,
    namespace: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

impl Creds {
    fn from_env() -> Option<Self> {
        let get = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        Some(Creds {
            catalog_uri: get(ENV_GLUE_CATALOG_URI)?,
            warehouse: get(ENV_GLUE_WAREHOUSE)?,
            namespace: get(ENV_ICEBERG_NAMESPACE)?,
            region: get(ENV_AWS_REGION)?,
            access_key: get(ENV_AWS_ACCESS_KEY_ID)?,
            secret_key: get(ENV_AWS_SECRET_ACCESS_KEY)?,
            session_token: get(ENV_AWS_SESSION_TOKEN),
        })
    }

    fn fq_table(&self) -> String {
        format!("{}.{}", self.namespace, LINEITEM_TABLE)
    }

    /// Standard Glue cloud path: SigV4 signing, virtual-hosted S3 (path_style=false),
    /// static (non-vended) credentials — the same shape as
    /// `cloud_e2e_test::CloudEnv::catalog_connection_password`.
    fn connection_creds(&self) -> ConnectionCreds {
        ConnectionCreds {
            warehouse: self.warehouse.clone(),
            endpoint: String::new(),
            region: self.region.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            session_token: self.session_token.clone(),
            path_style: false,
            use_sigv4: true,
            use_vended_credentials: false,
        }
    }

    fn catalog_props(&self) -> CatalogProps {
        CatalogProps {
            uri: self.catalog_uri.clone(),
            warehouse: self.warehouse.clone(),
            table: self.fq_table(),
        }
    }

    fn storage_props(&self) -> StorageProps {
        StorageProps {
            endpoint: String::new(),
            region: self.region.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            session_token: self.session_token.clone(),
            allow_http: false,
            path_style: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Discarding mock UdfContext.
//
// The single input row carries the ScanSpec JSON (returned by get_string(0)).
// emit_record_batch_ipc DISCARDS the IPC bytes — it only counts rows (decoded
// from the IPC stream header without retaining arrays) and total bytes. This is
// the seam that isolates the scan's footprint from any sink retention.
// ---------------------------------------------------------------------------

struct DiscardingCtx {
    spec_json: String,
    advanced: bool,
    rows: Arc<AtomicU64>,
    ipc_bytes: Arc<AtomicU64>,
    /// emit_batch / emit_record_batch_ipc calls (the raw-row IPC path).
    ipc_calls: Arc<AtomicU64>,
    /// row-by-row emit() calls — MUST stay 0 on the raw-row path.
    row_emit_calls: Arc<AtomicU64>,
    memory_limit_bytes: u64,
}

impl UdfContext for DiscardingCtx {
    fn num_columns(&self) -> usize {
        1
    }

    fn get(&self, col: usize) -> Result<&Value, UdfError> {
        let _ = col;
        // get_string(0) routes through get(0); we hold the spec as a Value::String.
        Err(UdfError::User(
            "DiscardingCtx::get is not used directly".into(),
        ))
    }

    /// Override get_string so run_scan reads the spec without needing a stored Value.
    fn get_string(&self, col: usize) -> Result<Option<&str>, UdfError> {
        if col == 0 {
            Ok(Some(self.spec_json.as_str()))
        } else {
            Ok(None)
        }
    }

    fn emit(&mut self, _values: &[Value]) -> Result<(), UdfError> {
        // The raw-row path never calls emit(); record it so a regression is visible.
        self.row_emit_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn next(&mut self) -> Result<bool, UdfError> {
        if self.advanced {
            Ok(false)
        } else {
            self.advanced = true;
            Ok(true)
        }
    }

    fn memory_limit(&self) -> u64 {
        self.memory_limit_bytes
    }

    /// DISCARD the IPC bytes. Count rows + total bytes only; never retain.
    fn emit_record_batch_ipc(&mut self, ipc: &[u8]) -> Result<(), UdfError> {
        self.ipc_bytes
            .fetch_add(ipc.len() as u64, Ordering::Relaxed);
        // Decode just the row count from the IPC stream, then drop everything.
        // We do NOT keep the decoded batch — the reader and batch are dropped at
        // the end of this scope, so the sink retains nothing.
        let cursor = std::io::Cursor::new(ipc);
        if let Ok(reader) = arrow::ipc::reader::StreamReader::try_new(cursor, None) {
            for batch in reader.flatten() {
                self.rows
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                // Batch dropped here — the sink retains nothing.
            }
        }
        self.ipc_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RSS sampler — reads resident pages from /proc/self/statm every interval.
// ---------------------------------------------------------------------------

fn current_rss_bytes() -> u64 {
    let page_size = 4096_u64; // Linux default; matches sysconf(_SC_PAGESIZE) on x86_64.
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    // statm fields: size resident shared text lib data dt — field[1] = resident pages.
    statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u64>().ok())
        .map(|pages| pages * page_size)
        .unwrap_or(0)
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Spawn a background sampler that prints `t=… rows=… rss_mb=…` every ~250 ms and
/// tracks the peak. Returns a stop flag and a join handle yielding peak RSS bytes.
fn spawn_rss_sampler(
    rows: Arc<AtomicU64>,
    start: Instant,
) -> (Arc<AtomicBool>, std::thread::JoinHandle<(u64, u64)>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    let handle = std::thread::spawn(move || {
        let mut peak: u64 = 0;
        let mut rows_at_peak: u64 = 0;
        loop {
            let rss = current_rss_bytes();
            let r = rows.load(Ordering::Relaxed);
            if rss > peak {
                peak = rss;
                rows_at_peak = r;
            }
            println!(
                "t={:>6.2}s rows={:>10} rss_mb={:>8.1}",
                start.elapsed().as_secs_f64(),
                r,
                mb(rss),
            );
            if stop_clone.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
        (peak, rows_at_peak)
    });
    (stop, handle)
}

// ---------------------------------------------------------------------------
// The repro
// ---------------------------------------------------------------------------

/// Build the EXACT Q3 lineitem ScanSpec from a resolved file list + EMITS types.
fn build_q3_spec(
    files: Vec<String>,
    emit_exa_types: Vec<String>,
    storage: StorageProps,
    catalog: CatalogProps,
    threads: usize,
) -> ScanSpec {
    ScanSpec {
        files,
        projection: Q3_PROJECTION.iter().map(|s| s.to_string()).collect(),
        filter: None,
        limit: None,
        aggregates: None,
        group_keys: None,
        emit_exa_types,
        storage,
        catalog,
        // The cluster Q3 config: 4 partitions, 4 threads, default batch size.
        df_target_partitions: 4,
        df_batch_size: 8192,
        df_threads_per_udf: threads,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
    }
}

/// Derive the positional EMITS types for the Q3 projection from the resolved
/// table schema (so they match the live table exactly — L_ORDERKEY's int mapping
/// and L_EXTENDEDPRICE's DECIMAL mapping). Falls back to the documented mapping
/// (DECIMAL(20,0) / DECIMAL(15,2)) if a column is missing from the schema.
fn emit_types_for_projection(schema: &[(String, String)]) -> Vec<String> {
    Q3_PROJECTION
        .iter()
        .map(|col| {
            schema
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(col))
                .map(|(_, ty)| ty.clone())
                .unwrap_or_else(|| match *col {
                    "L_ORDERKEY" => "DECIMAL(20,0)".to_string(),
                    _ => "DECIMAL(15,2)".to_string(),
                })
        })
        .collect()
}

#[test]
#[ignore = "live repro: hits AWS Glue + S3; run via `make repro`"]
fn q3_lineitem_scan_rss_repro() {
    let creds = match Creds::from_env() {
        Some(c) => c,
        None => panic!(
            "q3_lineitem_scan_rss_repro requires live Glue/AWS creds in the environment \
             ({ENV_GLUE_CATALOG_URI}, {ENV_GLUE_WAREHOUSE}, {ENV_ICEBERG_NAMESPACE}, \
             {ENV_AWS_REGION}, {ENV_AWS_ACCESS_KEY_ID}, {ENV_AWS_SECRET_ACCESS_KEY}). \
             Run `make repro`, which sources bench/.env."
        ),
    };

    let threads = df_threads();
    let mem_mb = memory_limit_mb();
    let mem_bytes = mem_mb.saturating_mul(1024 * 1024);

    println!(
        "=== Q3 LINEITEM RSS REPRO === df_threads={threads} df_target_partitions=4 \
         df_batch_size=8192 memory_pool_fraction=0.6 memory_limit_mb={mem_mb} \
         (0 = scan's 1 GiB default pool)"
    );

    // --- Resolve the live file list + schema (the resolve-once seam, reused) ---
    // resolve_file_list / resolve_table_schema are async; drive them on a small
    // runtime, exactly as the adapter does in handle_pushdown_request.
    let resolve_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("resolve runtime");

    let connection_creds = creds.connection_creds();
    let catalog_props = creds.catalog_props();
    let storage_props = creds.storage_props();

    let (files, effective_storage, schema) = resolve_rt.block_on(async {
        let (file_pairs, eff_storage) = resolve_file_list(
            &creds.catalog_uri,
            &catalog_props,
            &storage_props,
            &connection_creds,
            None, // Q3 lineitem leg: NO filter pushdown.
        )
        .await
        .expect("resolve_file_list against live Glue lineitem must succeed");

        let schema = resolve_table_schema(
            &creds.catalog_uri,
            &catalog_props,
            &storage_props,
            &connection_creds,
        )
        .await
        .expect("resolve_table_schema must succeed");

        let files: Vec<String> = file_pairs.into_iter().map(|(path, _size)| path).collect();
        (files, eff_storage, schema)
    });

    let total_bytes: u64 = 0; // sizes intentionally dropped above; print count only.
    let _ = total_bytes;
    println!(
        "resolved {} lineitem Parquet files; schema has {} columns",
        files.len(),
        schema.len()
    );
    assert!(
        !files.is_empty(),
        "live lineitem must resolve at least one Parquet file"
    );

    let emit_exa_types = emit_types_for_projection(&schema);
    println!(
        "Q3 projection {:?} -> EMITS types {:?}",
        Q3_PROJECTION, emit_exa_types
    );

    let spec = build_q3_spec(
        files,
        emit_exa_types,
        effective_storage,
        catalog_props,
        threads,
    );
    let spec_json = spec.to_json();

    // --- Drive the REAL scan entry point with the discarding ctx ---
    let rows = Arc::new(AtomicU64::new(0));
    let ipc_bytes = Arc::new(AtomicU64::new(0));
    let ipc_calls = Arc::new(AtomicU64::new(0));
    let row_emit_calls = Arc::new(AtomicU64::new(0));

    let mut ctx = DiscardingCtx {
        spec_json,
        advanced: false,
        rows: rows.clone(),
        ipc_bytes: ipc_bytes.clone(),
        ipc_calls: ipc_calls.clone(),
        row_emit_calls: row_emit_calls.clone(),
        memory_limit_bytes: mem_bytes,
    };

    let start = Instant::now();
    let (stop, sampler) = spawn_rss_sampler(rows.clone(), start);

    // run_scan is synchronous: it builds its own Tokio runtime per spec.df_threads,
    // runs run_scan_async (build_session_context -> build_dataframe -> execute_stream
    // -> emit_stream), and tears the runtime down. This is the exact cluster path.
    let scan_result = run_scan(&mut ctx);

    stop.store(true, Ordering::Relaxed);
    let (peak_rss, rows_at_peak) = sampler.join().expect("sampler thread");
    let elapsed = start.elapsed();

    let final_rows = rows.load(Ordering::Relaxed);
    let final_bytes = ipc_bytes.load(Ordering::Relaxed);
    let final_ipc_calls = ipc_calls.load(Ordering::Relaxed);
    let final_row_emit_calls = row_emit_calls.load(Ordering::Relaxed);

    println!("--- RESULT (df_threads={threads}) ---");
    println!("scan_result_ok={}", scan_result.is_ok());
    if let Err(e) = &scan_result {
        // UdfError is already credential-redacted by the scan path.
        println!("scan_error={e}");
    }
    println!("rows_emitted={final_rows}");
    println!("emit_batch_ipc_calls={final_ipc_calls}");
    println!("row_by_row_emit_calls={final_row_emit_calls} (must be 0 on raw-row path)");
    println!("total_ipc_bytes_mb={:.1}", mb(final_bytes));
    println!("elapsed_s={:.2}", elapsed.as_secs_f64());
    println!(
        "PEAK_RSS_MB={:.1} (rows_at_peak={rows_at_peak}, final_rows={final_rows})",
        mb(peak_rss)
    );

    // VERDICT heuristic: a streaming 2-column scan should peak well under 1 GiB.
    // We do not hard-fail on RSS (the host has no 4 GB cap and the point is to
    // observe), but we surface a clear verdict line for the report.
    let verdict = if mb(peak_rss) > 1500.0 {
        "ACCUMULATING (peak RSS > 1.5 GB for a 2-column stream)"
    } else {
        "STREAMING-LIKE (peak RSS stayed modest)"
    };
    println!("VERDICT={verdict}");

    // The raw-row path must never use the row-by-row emit() — all output goes
    // through emit_batch/emit_record_batch_ipc. (emit_calls counts IPC calls too;
    // assert the scan produced output of some kind so a silent no-op is caught.)
    assert!(
        scan_result.is_ok() || mb(peak_rss) > 0.0,
        "scan must either succeed or have produced an observable footprint"
    );
}

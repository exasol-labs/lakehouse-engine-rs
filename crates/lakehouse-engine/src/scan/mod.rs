/// DataFusion scan SCALAR EMIT UDF — reconstitutes a ScanSpec from its TWO input
/// arguments (the shard-invariant common blob at column 0, serialized once per
/// fan-out, and the per-shard files JSON array at column 1), builds a DataFusion
/// SessionContext, registers ONLY the assigned files over MinIO, applies
/// projection/filter/limit, and streams rows back via ctx.emit.
pub mod convert;
pub mod diagnostics;
pub mod emit;
pub mod positional_deletes;
pub mod runtime;
pub mod spec;

use crate::scan::spec::ScanSpec;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::udf_log;

mod field_id_projection;
pub(crate) use field_id_projection::{
    FieldIdExprAdapterFactory, FieldIdResolution, PARQUET_FIELD_ID_META_KEY,
};

mod sql_support;
pub use sql_support::build_alias_items;

mod object_store;
use object_store::build_session_context;
pub(crate) use object_store::reconstruct_abs_uri;

mod raw_scan;
pub use raw_scan::{
    build_raw_scan_physical_plan, int96_coerced_parquet_format, register_files,
    run_raw_scan_with_session,
};

mod join_scan;
pub use join_scan::{build_join_physical_plan, run_join_scan_with_session};

mod partial_agg;
#[cfg(test)]
pub use partial_agg::build_partial_agg_sql;
use partial_agg::run_partial_aggregate;
pub use partial_agg::{build_grouped_partial_agg_sql, build_partial_agg_sql_filtered};

#[cfg(test)]
mod test_support;

/// Bounded grace period for draining background async work at runtime teardown.
///
/// After the scan future returns, object_store's S3 client (hyper) may still hold
/// detached connection-pool tasks and open sockets. Dropping the Tokio runtime
/// while those are mid-flight is a non-deterministic teardown race: a detached
/// task's `Drop` can touch the reactor after it has been torn down, aborting the
/// VM process *after* the final emit/flush — outside the entry-point's
/// `catch_unwind`, so it surfaces as an `err_zombie` VM crash with no Rust panic
/// text. `shutdown_timeout` instead drives the runtime down deterministically:
/// it drives pending tasks for up to this window, then cancels what remains in a
/// defined order. The value is a teardown bound, not a query timeout — the scan
/// future has already completed before it applies.
const RUNTIME_SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// Run `future` to completion on `rt`, then shut the runtime down deterministically.
///
/// This is the abort-free teardown seam for the scan UDF. The future MUST resolve
/// to a value that owns no async resources (no DataFusion `SessionContext`, no
/// `RecordBatch` stream, no object-store handle): everything async-borrowing must
/// be dropped *inside* the future, before it returns. Given that, the result is
/// fully materialized while the runtime is still live, and the runtime is then
/// torn down via `shutdown_timeout` from this synchronous (non-async) context —
/// never by an implicit `Drop` that could race hyper's detached connection tasks.
fn run_on_runtime<T>(
    rt: tokio::runtime::Runtime,
    future: impl std::future::Future<Output = T>,
) -> T {
    let result = rt.block_on(future);
    // Explicit, bounded teardown from the synchronous context. Replaces the
    // implicit `drop(rt)` whose internal task/IO teardown raced hyper background
    // work and intermittently aborted the VM at end-of-life.
    rt.shutdown_timeout(RUNTIME_SHUTDOWN_GRACE);
    result
}

/// Build the Tokio runtime for the scan UDF.
///
/// When `threads` is 1 (the default), a current-thread runtime is created —
/// one OS thread, matching Exasol's per-instance model and the
/// `NR_OF_CORES`-bound VM pool. When `threads` exceeds 1, a multi-thread
/// runtime is created with exactly `threads` worker threads, which is only
/// correct when the operator has explicitly widened the thread budget via the
/// `DATAFUSION_THREADS_PER_UDF` VS property.
///
/// Exposed publicly so a host integration test can wrap this builder to count
/// how many runtimes its own per-row harness constructs. Note: since `run_scan`
/// calls this function directly (not through an injected seam), such a test
/// exercises the harness's own call discipline, not `run_scan` itself — it
/// cannot catch a future regression that caches a runtime inside `run_scan`.
pub fn build_scan_runtime(threads: usize) -> Result<tokio::runtime::Runtime, String> {
    if threads <= 1 {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("failed to build current-thread tokio runtime: {e}"))
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .map_err(|e| format!("failed to build multi-thread tokio runtime: {e}"))
    }
}

/// Build the DataFusion `SessionConfig` for the given scan spec.
///
/// Sets `target_partitions` from `spec.df_target_partitions` (clamped to ≥1)
/// and `batch_size` from `spec.df_batch_size` (clamped to ≥1).
/// With the default of 1 partition and a current-thread Tokio runtime, each UDF
/// instance uses exactly one core; cluster-level shard fan-out provides all parallelism.
///
/// Parquet pruning is enabled explicitly rather than left to the DataFusion
/// defaults: row-group statistics pruning (`pruning`) and page-index pruning
/// (`enable_page_index`) default on, but predicate pushdown into the decode
/// (`pushdown_filters`) defaults OFF — so a pushed-down filter would not skip
/// rows during decode unless we turn it on. These compose with the Iceberg
/// file-level pruning the planning layer already applies: Iceberg drops whole
/// files, the Parquet reader then drops row groups and pages within survivors.
/// Pruning narrows what is read; it never changes the result set.
pub fn session_config_for_spec(spec: &ScanSpec) -> SessionConfig {
    let config = SessionConfig::new()
        .with_information_schema(false)
        .with_target_partitions(spec.df_target_partitions.max(1))
        .with_batch_size(spec.df_batch_size.max(1))
        .with_parquet_pruning(true)
        .with_parquet_page_index_pruning(true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true);

    if spec.join.is_some() {
        // Broadcast-join build-side determinism. The scan places the bounded
        // dimension on the LEFT of the join (`build_join_sql`), and `HashJoinExec`
        // always builds its hash table from the left child. DataFusion's
        // `JoinSelection` would otherwise swap inputs based on table statistics —
        // but the scan disables statistics collection (`with_collect_stat(false)`),
        // so a swap decision would be non-deterministic. Turning join reordering
        // off pins the dimension as the build side regardless of statistics, which
        // is exactly the bounded, memory-safe build side the broadcast contract
        // guarantees fits in the pool.
        config.set_bool("datafusion.optimizer.join_reordering", false)
    } else {
        config
    }
}

/// Reconstitute the full `ScanSpec` from the two scan-UDF input arguments.
///
/// Column 0 is the shard-invariant common blob JSON (serialized ONCE per
/// fan-out); column 1 is the per-shard files JSON array. Either argument being
/// SQL NULL is a user error — the adapter always supplies both. Reconstitution
/// goes through [`ScanSpec::from_parts_json`], whose errors NEVER echo the raw
/// inputs (the common blob carries credentials).
///
/// Exposed so a host integration test can drive the exact production two-argument
/// reconstitution against a fake `UdfContext` (no S3), then feed the resulting
/// spec to [`run_raw_scan_with_session`].
pub fn read_scan_spec(ctx: &dyn UdfContext) -> Result<ScanSpec, UdfError> {
    let common_json = ctx
        .get_string(0)?
        .ok_or_else(|| UdfError::User("scan common input is NULL".into()))?;
    let files_json = ctx
        .get_string(1)?
        .ok_or_else(|| UdfError::User("scan files input is NULL".into()))?;
    ScanSpec::from_parts_json(common_json, files_json).map_err(UdfError::User)
}

/// Entry point for the LAKEHOUSE_SCAN SCALAR EMIT UDF — handles exactly ONE row.
///
/// Under SDK 0.21.0 Exasol drives a scalar `run()` once per input row, and
/// `ctx.next()` in scalar context is now a runtime-enforced error rather than a
/// loop advance. So `run_scan` reconstitutes exactly one row's `ScanSpec` (via
/// [`read_scan_spec`], with no `ctx.next()` call), scans that row's assigned
/// files, and returns; Exasol invokes it again for the next row. Each row carries
/// the shard-invariant common blob (column 0) and that row's per-shard files JSON
/// (column 1). The "no dropped rows" guarantee is preserved by construction: the
/// UNION of every per-row `run()` call covers every shard, so no batch loop is
/// needed — and none is legal.
///
/// The Tokio runtime is built fresh from THIS call's `df_threads_per_udf` and
/// torn down before returning; it is NEVER cached at `static`/process scope. Its
/// sizing derives from a per-call input parameter, so a runtime cached on a
/// pooled VM process (reused across queries) would silently apply stale sizing to
/// a later query with a different value — the exact reason the runtime is rebuilt
/// per call. Teardown goes through [`run_on_runtime`],
/// whose abort-free contract holds because [`run_scan_one`]'s future drops its
/// `SessionContext` and every stream before it resolves, owning no async resource
/// at return. Production builds the session via `build_session_context`.
pub fn run_scan(ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    // One run() call = one row (SDK 0.21.0 scalar dispatch — no ctx.next()).
    // Reconstitute this row's spec BEFORE building the runtime: the runtime kind
    // depends on spec.df_threads_per_udf. NULL in either argument is a user error.
    let spec = read_scan_spec(ctx)?;

    let rt = build_scan_runtime(spec.df_threads_per_udf).map_err(UdfError::User)?;

    // Run this row's scan on the runtime and tear it down deterministically. The
    // implicit `drop(rt)` path raced object_store's detached hyper tasks at
    // end-of-life and aborted the VM after the final flush (err_zombie, no panic
    // text); `run_on_runtime` drives shutdown via `shutdown_timeout` from this
    // synchronous context instead. Production builds the session via
    // `build_session_context`.
    run_on_runtime(rt, run_scan_one(ctx, spec, build_session_context))
}

/// Scan exactly ONE reconstituted spec: build its session, dispatch, drop it.
///
/// One `run()` call handles one row (SDK 0.21.0 scalar dispatch), so there is no
/// loop: this builds a [`SessionContext`] for the row via `build_session`,
/// dispatches to the join / partial-aggregate / raw-row path through
/// [`run_scan_dispatch`], and drops the session (with its object store and any
/// residual handles) before returning. Because the session and every stream are
/// dropped inside this future before it resolves, no async resource outlives the
/// future — preserving [`run_on_runtime`]'s abort-free teardown contract.
///
/// `build_session` is injected so a host test can supply a local-file session
/// (no S3), exactly as [`run_raw_scan_with_session`] is exposed for host tests;
/// production passes [`build_session_context`].
pub async fn run_scan_one(
    ctx: &mut dyn UdfContext,
    spec: ScanSpec,
    build_session: impl Fn(&ScanSpec, u64) -> Result<SessionContext, UdfError>,
) -> Result<(), UdfError> {
    let memory_limit_bytes = ctx.memory_limit();
    let session_ctx = build_session(&spec, memory_limit_bytes)?;
    run_scan_dispatch(ctx, &session_ctx, &spec).await?;
    // Drop this row's session (and its object store / any residual handles)
    // before returning — no async resource may outlive run_on_runtime's future.
    drop(session_ctx);
    Ok(())
}

/// Scan one reconstituted spec over an already-built session, dispatching to the
/// join / partial-aggregate / raw-row path. All streams are drained and dropped
/// before it returns, so the caller's future owns no async resources.
async fn run_scan_dispatch(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<(), UdfError> {
    // Phase telemetry: the startup clock starts at scan-body entry and is sealed
    // at the first batch fetch inside emit_stream. Always measured (monotonic-clock
    // arithmetic is cheap); emission is gated on the debug level so production at
    // the default `info` level stays silent.
    let mut timers = diagnostics::PhaseTimers::start();

    if spec.join.is_some() {
        // A join spec drives the two-table broadcast inner equi-join path: register
        // the sharded fact side and the full dimension side in one session, join
        // node-locally, and stream joined batches. Takes precedence over the
        // aggregate/raw dispatch (the VS never combines a join with aggregates).
        run_join_scan_with_session(ctx, session_ctx, spec, &mut timers).await
    } else if spec.aggregates.is_some() {
        // Partial-aggregate paths emit a single summary row; phase telemetry
        // targets the raw-row streaming path where startup / import / send-back
        // are the throughput question. Leave the aggregate path unchanged.
        run_partial_aggregate(ctx, session_ctx, spec).await
    } else {
        run_raw_scan_with_session(ctx, session_ctx, spec, &mut timers).await
    }
}

/// Emit the per-VM phase-telemetry record, gated on the debug level.
///
/// Best-effort: at the production default level (`info`) this is a no-op; at
/// `debug` it writes one `LHTELEM` line to stderr (auto-tagged per-VM by the SLC
/// fd-redirect) and appends it to the per-process telemetry file. Every write is
/// swallowed — a telemetry failure must never surface as a scan error.
fn emit_phase_telemetry(ctx: &dyn UdfContext, timers: &diagnostics::PhaseTimers) {
    if !diagnostics::telemetry_enabled(ctx.debug_level()) {
        return;
    }
    let record = diagnostics::telemetry_record(timers);
    udf_log!(ctx, debug, "{}", record);
    diagnostics::write_telemetry_file(&record);
}

#[cfg(test)]
mod tests {
    use super::test_support::minimal_spec;
    use super::*;

    /// Task 4.3: session_config_for_spec applies df_batch_size and clamps sub-1 values to 1.
    ///
    /// Verifies that:
    /// 1. An explicit batch size flows through to SessionConfig::batch_size().
    /// 2. A zero batch size is clamped to 1 (sub-1 values must not reach DataFusion as-is).
    #[test]
    fn session_config_applies_batch_size_and_clamps_floor() {
        // 1. Explicit batch size is applied.
        let mut spec = minimal_spec();
        spec.df_batch_size = 4096;
        let config = session_config_for_spec(&spec);
        assert_eq!(
            config.batch_size(),
            4096,
            "SessionConfig must use df_batch_size from spec"
        );

        // 2. Zero (sub-1) batch size is clamped to 1.
        spec.df_batch_size = 0;
        let config_clamped = session_config_for_spec(&spec);
        assert_eq!(
            config_clamped.batch_size(),
            1,
            "df_batch_size of 0 must be clamped to 1"
        );
    }

    /// Parquet row-group statistics pruning, page-index pruning, and predicate
    /// pushdown are all enabled on the session config — not left to the
    /// DataFusion defaults (`pushdown_filters` defaults to `false`).
    ///
    /// Scenario: Scan enables Parquet row-group and page pruning.
    #[test]
    fn session_config_enables_parquet_pruning_flags() {
        let config = session_config_for_spec(&minimal_spec());
        let parquet = &config.options().execution.parquet;
        assert!(
            parquet.pruning,
            "row-group statistics pruning must be enabled"
        );
        assert!(
            parquet.enable_page_index,
            "page-index pruning must be enabled"
        );
        assert!(
            parquet.pushdown_filters,
            "predicate pushdown into the Parquet decode must be enabled (DataFusion defaults it off)"
        );
    }

    /// SessionConfig applies target_partitions from the spec.
    ///
    /// Scenario: session_config_uses_spec_target_partitions
    #[test]
    fn session_config_uses_spec_target_partitions() {
        let mut spec = minimal_spec();
        spec.df_target_partitions = 4;
        let config = session_config_for_spec(&spec);
        assert_eq!(
            config.target_partitions(),
            4,
            "SessionConfig must use df_target_partitions from spec"
        );
    }

    /// A spec with df_threads_per_udf == 1 selects the current-thread runtime.
    ///
    /// Scenario: runtime_is_current_thread_when_threads_is_one
    #[test]
    fn runtime_is_current_thread_when_threads_is_one() {
        let rt = build_scan_runtime(1).expect("runtime must build");
        assert_eq!(
            rt.handle().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread,
            "df_threads_per_udf == 1 must yield a current-thread runtime"
        );
    }

    /// A spec with df_threads_per_udf > 1 selects the multi-thread runtime.
    ///
    /// Scenario: runtime_is_multi_thread_when_threads_exceeds_one
    #[test]
    fn runtime_is_multi_thread_when_threads_exceeds_one() {
        let rt = build_scan_runtime(4).expect("runtime must build");
        assert_eq!(
            rt.handle().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread,
            "df_threads_per_udf > 1 must yield a multi-thread runtime"
        );
    }

    /// Teardown regression: a multi-thread runtime that still has detached
    /// background tasks live when `block_on` returns must be torn down
    /// deterministically, not by an implicit `Drop` that races those tasks.
    ///
    /// This reproduces the *mechanism* of the live `err_zombie` VM abort: the scan
    /// future completes and returns its result while object_store's hyper client
    /// (modeled here by a spawned task parked past the future's return) is still
    /// alive. The fix is `run_on_runtime`, which drives the runtime down via
    /// `shutdown_timeout` from the synchronous context. The invariants:
    ///   1. the future's result is returned intact (work completed before teardown);
    ///   2. `run_on_runtime` returns within the grace window (it drains/cancels the
    ///      detached task rather than blocking forever or aborting);
    ///   3. control reaches the assertions — a raced teardown abort would have
    ///      killed the test process before this point.
    ///
    /// A real VM-process abort cannot be reproduced on the host (it needs Exasol's
    /// VM teardown), so this asserts the deterministic-shutdown seam the abort-free
    /// fix depends on; the live bench is the end-to-end arbiter.
    #[test]
    fn run_on_runtime_tears_down_multi_thread_runtime_with_live_background_task() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let rt = build_scan_runtime(2).expect("multi-thread runtime must build");
        let parked_started = Arc::new(AtomicBool::new(false));
        let started_in_future = parked_started.clone();

        let started_in_outer = parked_started.clone();
        let before = std::time::Instant::now();
        let result = run_on_runtime(rt, async move {
            // Spawn a detached task that outlives the future's return — the
            // host analog of hyper's connection-pool/reaper tasks that object_store
            // keeps alive past the last poll of the scan stream.
            tokio::spawn(async move {
                started_in_future.store(true, Ordering::SeqCst);
                // Park far longer than the grace window; shutdown must cancel it.
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            });
            // Yield until the detached task has actually reached its body (set the
            // flag) so the runtime genuinely has live background work at teardown
            // time. A single `yield_now()` is not enough under a loaded scheduler:
            // the detached task may not be polled before this future returns,
            // which made the "task started" assertion flaky. The bounded yield
            // loop is deterministic without changing what is tested — the task is
            // still parked on its 3600s sleep when teardown runs.
            while !started_in_outer.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            42_u32
        });
        let elapsed = before.elapsed();

        // 1. The future's result survives the explicit teardown.
        assert_eq!(result, 42, "future result must be returned before teardown");
        // 2. Teardown is bounded — it returns near-immediately (the detached task
        //    is cancelled), never blocking on the 3600s park.
        assert!(
            elapsed < RUNTIME_SHUTDOWN_GRACE + std::time::Duration::from_secs(2),
            "run_on_runtime must return within the bounded grace window, took {elapsed:?}"
        );
        // 3. The detached task was genuinely live (otherwise the test proves nothing).
        assert!(
            parked_started.load(Ordering::SeqCst),
            "the detached background task must have started before teardown"
        );
        // Reaching here without a process abort is the core assertion: deterministic
        // shutdown replaced the racy implicit drop.
    }
}

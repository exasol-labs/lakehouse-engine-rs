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
pub(crate) use self::object_store::build_table_root_store;
use object_store::build_session_context;
pub(crate) use spec::reconstruct_abs_uri;

mod store_router;

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
#[path = "test_support_tests.rs"]
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
/// Sets `target_partitions` from `spec.common.df_target_partitions` (clamped to ≥1)
/// and `batch_size` from `spec.common.df_batch_size` (clamped to ≥1).
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
        .with_target_partitions(spec.common.df_target_partitions.max(1))
        .with_batch_size(spec.common.df_batch_size.max(1))
        .with_parquet_pruning(true)
        .with_parquet_page_index_pruning(true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true);

    if spec.common.join.is_some() {
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
    // depends on spec.common.df_threads_per_udf. NULL in either argument is a user error.
    let spec = read_scan_spec(ctx)?;

    let rt = build_scan_runtime(spec.common.df_threads_per_udf).map_err(UdfError::User)?;

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

    // Footer re-fetch observable (task 1.7b): reset the process-global record of
    // access-plan-cached footer paths at the start of every invocation. A pooled
    // UDF process serves many invocations in sequence off the same fixed
    // per-node VM (see CLAUDE.md § UDF parallelization); without this reset a
    // later invocation would report an earlier invocation's recorded paths.
    diagnostics::reset_access_plan_cached_footers();

    let result = if spec.common.join.is_some() {
        // A join spec drives the two-table broadcast inner equi-join path: register
        // the sharded fact side and the full dimension side in one session, join
        // node-locally, and stream joined batches. Takes precedence over the
        // aggregate/raw dispatch (the VS never combines a join with aggregates).
        run_join_scan_with_session(ctx, session_ctx, spec, &mut timers).await
    } else if spec.common.aggregates.is_some() {
        // Partial-aggregate paths emit a single summary row; phase telemetry
        // targets the raw-row streaming path where startup / import / send-back
        // are the throughput question. Leave the aggregate path unchanged.
        run_partial_aggregate(ctx, session_ctx, spec).await
    } else {
        run_raw_scan_with_session(ctx, session_ctx, spec, &mut timers).await
    };

    if result.is_ok() {
        // A pushed LIMIT can end the stream before the opener opens every
        // assigned file, and a join whose build side is empty never polls the
        // probe side — either leaves an access-plan-cached footer at `hits == 0`
        // without anything having been re-fetched. Only this site knows the
        // shape, so it decides how the counter may read `hits == 0`.
        let coverage = if spec.common.limit.is_none() && spec.common.join.is_none() {
            diagnostics::OpenerCoverage::EveryAssignedFile
        } else {
            diagnostics::OpenerCoverage::MayStopEarly
        };
        emit_footer_refetch_diagnostic(ctx, session_ctx, coverage);
    }
    result
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

/// Report a positional-delete footer re-fetch, gated on the debug level and
/// on the count being non-zero (task 1.7b).
///
/// Reads the session's [`FileMetadataCache`] entry snapshot — the same cache
/// `PositionalDeleteScanTable::partitioned_files` reaches through
/// `state.runtime_env()` — and passes it to
/// [`diagnostics::footer_refetch_count`]. When that count is non-zero, emits
/// ONE `udf_log!` debug line naming the count, so an operator capturing
/// `SCRIPT_OUTPUT_ADDRESS` at debug level sees a metadata-cache eviction that
/// cost the opener a second footer fetch. The line carries the count only,
/// never a recorded path, so no credential a path's URI can carry ever reaches
/// it.
///
/// Inert at the production default `info` level: the leading
/// [`diagnostics::telemetry_enabled`] check — the same gate
/// [`emit_phase_telemetry`] uses — returns before the cache is ever snapshotted,
/// so a production scan pays no `list_entries()` traversal. At `debug`, a
/// fully-cached scan still writes nothing once snapshotted, since the count is
/// zero.
///
/// `coverage` tells the counter whether this scan shape guarantees the opener
/// opens every assigned file; a cached footer the opener never opened is not a
/// re-fetch, and only the caller knows the shape (see
/// [`diagnostics::footer_refetch_count`]).
///
/// [`FileMetadataCache`]: datafusion::execution::cache::cache_manager::FileMetadataCache
fn emit_footer_refetch_diagnostic(
    ctx: &dyn UdfContext,
    session_ctx: &SessionContext,
    coverage: diagnostics::OpenerCoverage,
) {
    if !diagnostics::telemetry_enabled(ctx.debug_level()) {
        return;
    }
    let entries = session_ctx
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache()
        .list_entries();
    let count = diagnostics::footer_refetch_count(&entries, coverage);
    if count > 0 {
        udf_log!(
            ctx,
            debug,
            "positional-delete footer re-fetch: {count} footer(s) cached during access-plan \
             construction were not retained by the metadata cache before the opener read them"
        );
    }
}

#[cfg(test)]
#[path = "scan_tests.rs"]
mod tests;

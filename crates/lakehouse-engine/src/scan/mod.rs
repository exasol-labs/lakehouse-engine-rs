pub mod convert;
pub mod diagnostics;
pub mod emit;
pub mod positional_deletes;
pub mod runtime;
pub(crate) mod sealed;
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

mod json_render;
pub(crate) use json_render::render_nested_column_as_json;

mod sql_support;
pub use sql_support::build_alias_items;

mod checked_div;

mod object_store;
pub(crate) use self::object_store::{
    build_admission_limited_store, build_table_root_store, store_root_url,
};
use object_store::build_session_context;
pub(crate) use spec::{encode_file_path, reconstruct_abs_uri};

mod storage_ref;
pub use storage_ref::ResolvedScanStorage;
use storage_ref::resolve_scan_storage;

mod store_router;

mod deletion_vectors;

mod partition_values;

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

#[cfg(test)]
#[path = "type_relaxation_tests.rs"]
pub(crate) mod type_relaxation;

/// After the scan future returns, hyper may still hold detached tasks; dropping the runtime then
/// races the reactor teardown and aborts the VM outside `catch_unwind` (`err_zombie`, no panic
/// text). A teardown bound, not a query timeout.
const RUNTIME_SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// The future MUST resolve to a value owning no async resources (no `SessionContext`, stream, or
/// object-store handle), so the runtime can be shut down via `shutdown_timeout` rather than an
/// implicit `Drop` racing hyper's detached tasks.
fn run_on_runtime<T>(
    rt: tokio::runtime::Runtime,
    future: impl std::future::Future<Output = T>,
) -> T {
    let result = rt.block_on(future);
    rt.shutdown_timeout(RUNTIME_SHUTDOWN_GRACE);
    result
}

/// `threads == 1` (the default) yields a current-thread runtime, matching Exasol's per-instance
/// model; more threads are only correct when `DATAFUSION_THREADS_PER_UDF` was widened explicitly.
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

/// `pushdown_filters` defaults OFF in DataFusion, so it is enabled explicitly, except for a scan
/// rendering a nested column to JSON: `try_pushdown_filters` ORs the session flag with the
/// table's own, so a session-level `true` would re-enable it for the table that must not have it
/// (see `raw_scan::nested_json_parquet_format`). Each other table opts back in per table.
pub fn session_config_for_spec(spec: &ScanSpec) -> SessionConfig {
    let config = SessionConfig::new()
        .with_information_schema(false)
        .with_target_partitions(spec.common.df_target_partitions.max(1))
        .with_batch_size(spec.common.df_batch_size.max(1))
        .with_parquet_pruning(true)
        .with_parquet_page_index_pruning(true)
        .set_bool(
            "datafusion.execution.parquet.pushdown_filters",
            !raw_scan::scan_renders_nested_json(spec),
        );

    if spec.common.join.is_some() {
        // `HashJoinExec` builds from the left child, where the dimension sits. Statistics
        // collection is disabled, so `JoinSelection` swaps would be non-deterministic; turning
        // reordering off pins the bounded dimension as the build side.
        config.set_bool("datafusion.optimizer.join_reordering", false)
    } else {
        config
    }
}

/// Errors from [`ScanSpec::from_parts_json`] never echo the raw inputs (the common blob carries
/// credentials).
pub fn read_scan_spec(ctx: &dyn UdfContext) -> Result<ScanSpec, UdfError> {
    let common_json = ctx
        .get_string(0)?
        .ok_or_else(|| UdfError::User("scan common input is NULL".into()))?;
    let files_json = ctx
        .get_string(1)?
        .ok_or_else(|| UdfError::User("scan files input is NULL".into()))?;
    ScanSpec::from_parts_json(common_json, files_json).map_err(UdfError::User)
}

/// Handles exactly ONE row: Exasol drives a scalar `run()` once per input row and `ctx.next()`
/// in scalar context is an error, so the union of per-row calls covers every shard.
///
/// The Tokio runtime is built per call and NEVER cached: its sizing comes from this call's
/// `df_threads_per_udf`, and a runtime cached on a pooled VM would apply stale sizing to a later
/// query. [`run_on_runtime`]'s contract holds because [`run_scan_one`] drops every async resource
/// before resolving.
pub fn run_scan(ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    // The runtime kind depends on the spec, so read it first.
    let spec = read_scan_spec(ctx)?;

    let storage = resolve_scan_storage(&spec.common, ctx)?;

    let rt = build_scan_runtime(spec.common.df_threads_per_udf).map_err(UdfError::User)?;

    run_on_runtime(rt, run_scan_one(ctx, spec, &storage, build_session_context))
}

/// `build_session` is injected so a host test can supply a local-file session.
pub async fn run_scan_one(
    ctx: &mut dyn UdfContext,
    spec: ScanSpec,
    storage: &ResolvedScanStorage,
    build_session: impl Fn(&ScanSpec, &ResolvedScanStorage, u64) -> Result<SessionContext, UdfError>,
) -> Result<(), UdfError> {
    let memory_limit_bytes = ctx.memory_limit();
    let session_ctx = build_session(&spec, storage, memory_limit_bytes)?;
    run_scan_dispatch(ctx, &session_ctx, &spec, storage).await?;
    // No async resource may outlive run_on_runtime's future.
    drop(session_ctx);
    Ok(())
}

/// All streams are drained and dropped before it returns.
async fn run_scan_dispatch(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
) -> Result<(), UdfError> {
    let mut timers = diagnostics::PhaseTimers::start();

    // A pooled UDF process serves many invocations in sequence; without this reset a later
    // invocation would report an earlier one's recorded paths.
    diagnostics::reset_access_plan_cached_footers();

    let result = if spec.common.join.is_some() {
        // The VS never combines a join with aggregates.
        run_join_scan_with_session(ctx, session_ctx, spec, storage, &mut timers).await
    } else if spec.common.aggregates.is_some() {
        // Phase telemetry targets only the raw-row and join streaming paths.
        run_partial_aggregate(ctx, session_ctx, spec, storage).await
    } else {
        run_raw_scan_with_session(ctx, session_ctx, spec, storage, &mut timers).await
    };

    if result.is_ok() {
        // A pushed LIMIT or a join with an empty build side can leave a cached footer at
        // `hits == 0` without any re-fetch; only this site knows the shape.
        let coverage = if spec.common.limit.is_none() && spec.common.join.is_none() {
            diagnostics::OpenerCoverage::EveryAssignedFile
        } else {
            diagnostics::OpenerCoverage::MayStopEarly
        };
        emit_footer_refetch_diagnostic(ctx, session_ctx, coverage);
    }
    // A checked division raised inside a pushed filter loses its type at the Parquet row-filter
    // boundary; every run path funnels through here, and the session still holds it typed.
    result.map_err(|e| emit::reframe_checked_division(session_ctx, e, &storage.all_secret_values()))
}

/// A no-op at the production `info` level; a telemetry failure never surfaces as a scan error.
fn emit_phase_telemetry(ctx: &dyn UdfContext, timers: &diagnostics::PhaseTimers) {
    if !diagnostics::telemetry_enabled(ctx.debug_level()) {
        return;
    }
    let record = diagnostics::telemetry_record(timers);
    udf_log!(ctx, debug, "{}", record);
    diagnostics::write_telemetry_file(&record);
}

/// Logs only the count, never a path, so no credential in a URI can leak. The
/// [`diagnostics::telemetry_enabled`] gate runs first, so a production scan pays no
/// `list_entries()` traversal.
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

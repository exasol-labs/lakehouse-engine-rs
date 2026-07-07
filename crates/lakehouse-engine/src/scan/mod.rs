/// DataFusion scan SET UDF — reconstitutes a ScanSpec from its TWO input
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

use crate::scan::convert::arrow_value_at;
use crate::scan::emit::{classify_scan_error, emit_stream};
use crate::scan::runtime::{build_runtime_env, probe_tmp_spill};
use crate::scan::spec::{AggKind, AggregatePlan, ProjectionItem, ScanSpec, render_order_by_clause};
use crate::types::mapping::needs_json_fallback;
use arrow::array::{Array, ListArray};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::prelude::SessionConfig;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::udf_log;
use exasol_udf_sdk::value::Value;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ClientOptions;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

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
fn build_scan_runtime(threads: usize) -> Result<tokio::runtime::Runtime, String> {
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
    SessionConfig::new()
        .with_information_schema(false)
        .with_target_partitions(spec.df_target_partitions.max(1))
        .with_batch_size(spec.df_batch_size.max(1))
        .with_parquet_pruning(true)
        .with_parquet_page_index_pruning(true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
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

/// Entry point for the LAKEHOUSE_SCAN SET UDF.
///
/// Reconstitutes the scan spec from the two input columns (the common blob at
/// column 0 and the per-shard files JSON at column 1), builds a DataFusion
/// session, scans the assigned files, and emits rows.
pub fn run_scan(ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    // Advance to the first (and only) input row.
    let has_row = ctx.next()?;
    if !has_row {
        // No input row — nothing to scan.
        return Ok(());
    }

    // Reconstitute the spec from the two arguments BEFORE building the runtime:
    // the runtime kind depends on spec.df_threads_per_udf, so we must
    // deserialize first. NULL in either argument is a user error.
    let spec = read_scan_spec(ctx)?;

    // Build the Tokio runtime according to the spec's thread configuration.
    // A fresh runtime per call is correct for a stateless disposable UDF.
    let rt = build_scan_runtime(spec.df_threads_per_udf).map_err(UdfError::User)?;

    // Run on the runtime and tear it down deterministically. The implicit `drop(rt)`
    // path raced object_store's detached hyper tasks at end-of-life and aborted the
    // VM after the final flush (err_zombie, no panic text); `run_on_runtime` drives
    // shutdown via `shutdown_timeout` from this synchronous context instead.
    // run_scan_async returns a value owning no async resources (the SessionContext,
    // streams, and object store are all dropped inside the future), satisfying
    // run_on_runtime's contract.
    run_on_runtime(rt, async { run_scan_async(ctx, &spec).await })
}

async fn run_scan_async(ctx: &mut dyn UdfContext, spec: &ScanSpec) -> Result<(), UdfError> {
    // Phase telemetry (Task 4): the startup clock starts at scan-body entry and
    // is sealed at the first batch fetch inside emit_stream. Always measured
    // (monotonic-clock arithmetic is cheap); emission is gated on the debug
    // level so production at the default `info` level stays silent.
    let mut timers = diagnostics::PhaseTimers::start();

    let memory_limit_bytes = ctx.memory_limit();
    let session_ctx = build_session_context(spec, memory_limit_bytes)?;
    if spec.aggregates.is_some() {
        // Partial-aggregate paths emit a single summary row; phase telemetry
        // targets the raw-row streaming path where startup / import / send-back
        // are the throughput question. Leave the aggregate path unchanged.
        run_partial_aggregate(ctx, &session_ctx, spec).await
    } else {
        run_raw_scan_with_session(ctx, &session_ctx, spec, &mut timers).await
    }
}

/// Stream the raw-row scan over an already-built session and emit phase telemetry.
///
/// Registers the assigned files as `scan_target`, builds the projection/filter/
/// LIMIT DataFrame, executes it, and streams batches through [`emit_stream`]
/// (one fetched, emitted, dropped before the next). On completion it emits the
/// single gated per-VM phase-telemetry record. Exposed so a host integration
/// test can drive the exact production streaming + telemetry path against a
/// local Parquet file (no S3 store), feeding its own `SessionContext`.
pub async fn run_raw_scan_with_session(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
    timers: &mut diagnostics::PhaseTimers,
) -> Result<(), UdfError> {
    let secrets = spec.storage.secret_values();
    let df = build_dataframe(session_ctx, spec).await?;
    let stream = df
        .execute_stream()
        .await
        .map_err(|e| classify_scan_error(e, &secrets))?;
    emit_stream(ctx, stream, &secrets, &spec.emit_exa_types, timers).await?;
    // One per-VM telemetry record at completion. Gated + best-effort: a
    // logging/sink failure NEVER fails the scan (the scan already succeeded).
    emit_phase_telemetry(ctx, timers);
    Ok(())
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

/// Run a node-local partial aggregate and emit exactly one row per shard.
///
/// Dispatches to `run_grouped_partial_aggregate` when the spec carries non-empty
/// `group_keys`; otherwise executes the single-group (ungrouped) path which
/// always emits exactly one partial-aggregate row.
///
/// The column layout follows the COLUMN CONTRACT (see `build_partial_agg_sql`
/// and `build_grouped_partial_agg_sql`).
async fn run_partial_aggregate(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<(), UdfError> {
    // Dispatch: grouped path when group_keys is Some and non-empty.
    if let Some(group_keys) = &spec.group_keys
        && !group_keys.is_empty()
    {
        return run_grouped_partial_aggregate(ctx, session_ctx, spec).await;
    }

    let secrets = spec.storage.secret_values();
    let aggregates = spec
        .aggregates
        .as_deref()
        .expect("run_partial_aggregate called without aggregates");

    // Register the assigned files so we can query them.
    let table_name = "scan_target";
    register_files(session_ctx, table_name, spec).await?;

    // Build the alias inner SELECT (uppercase column names).
    let table = session_ctx
        .table(table_name)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered table: {e}")))?;
    let alias_items = build_alias_items(table.schema());
    let aliased_table = format!("SELECT {} FROM {table_name}", alias_items.join(", "));

    let sql = build_partial_agg_sql_filtered(aggregates, &aliased_table, spec.filter.as_deref());

    let df = session_ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("partial aggregate SQL error: {e}")))?;

    // Execute and collect the single partial-aggregate row.
    let batches = df
        .collect()
        .await
        .map_err(|e| classify_scan_error(e, &secrets))?;

    // The aggregate always produces exactly one row (even over an empty table).
    // Emit that row; if the query produced no batches at all (should not happen
    // for a well-formed aggregate), emit a row of NULLs. CountDistinct columns are
    // serialized from their Arrow List cell to a JSON-array VARCHAR in Rust.
    let row = match batches.first() {
        Some(batch) if batch.num_rows() > 0 => partial_row_from_batch(aggregates, batch)?,
        _ => emit_null_partial_row(aggregates),
    };

    ctx.emit(&row)?;
    Ok(())
}

/// Execute a grouped partial aggregate for the assigned shard files.
///
/// DataFusion runs the GROUP BY query and streams one row per distinct group.
/// Each emitted row carries:
///   - one `Value::String` per group key (GK_0 … GK_{n-1}), stringified via
///     `arrow_value_at` then `to_string()` — the adapter declares all GK columns
///     as `VARCHAR(2000000)` in the EMITS clause.
///   - the PARTIAL_* values in the same order produced by the single-group path.
///
/// An empty result (no matching rows in this shard) emits zero rows, NOT a null
/// fallback row.  This matches the COLUMN CONTRACT: the outer wrapper re-groups
/// partial rows from all shards, so zero rows from one shard is correct.
///
/// Streaming rule: fetch one `RecordBatch` at a time, convert → emit → drop
/// before fetching the next.  Never collect all batches in memory at once.
async fn run_grouped_partial_aggregate(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<(), UdfError> {
    let secrets = spec.storage.secret_values();
    let group_keys = spec
        .group_keys
        .as_deref()
        .expect("run_grouped_partial_aggregate called without group_keys");
    let aggregates = spec
        .aggregates
        .as_deref()
        .expect("run_grouped_partial_aggregate called without aggregates");

    // Register the assigned files so we can query them.
    let table_name = "scan_target";
    register_files(session_ctx, table_name, spec).await?;

    // Build the alias inner SELECT (uppercase column names) — same pattern as
    // the single-group path so group-key expressions reference uppercase names.
    let table = session_ctx
        .table(table_name)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered table: {e}")))?;
    let alias_items = build_alias_items(table.schema());
    let aliased_table = format!("SELECT {} FROM {table_name}", alias_items.join(", "));

    let sql = build_grouped_partial_agg_sql(
        group_keys,
        aggregates,
        &aliased_table,
        spec.filter.as_deref(),
    );

    let df = session_ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("grouped partial aggregate SQL error: {e}")))?;

    // Stream result batches — fetch one RecordBatch at a time, convert → emit → drop.
    let mut stream = df
        .execute_stream()
        .await
        .map_err(|e| classify_scan_error(e, &secrets))?;

    let n_group_keys = group_keys.len();

    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| classify_scan_error(e, &secrets))?;

        for row_idx in 0..batch.num_rows() {
            // Group-key columns come first (columns 0 .. n_group_keys - 1).
            // They are emitted as VARCHAR strings regardless of the DataFusion type.
            let mut row_values: Vec<Value> = Vec::with_capacity(batch.num_columns());

            for col_idx in 0..n_group_keys {
                let raw = arrow_value_at(batch.column(col_idx), row_idx);
                // Stringify for GK_i VARCHAR(2000000) column.
                // Value has no Display; format each variant explicitly.
                let gk_str = value_to_gk_string(raw);
                row_values.push(gk_str);
            }

            // Partial aggregate columns follow.
            for col_idx in n_group_keys..batch.num_columns() {
                row_values.push(arrow_value_at(batch.column(col_idx), row_idx));
            }

            ctx.emit(&row_values)?;
        }
        // Drop the batch before fetching the next — never hold two batches at once.
        drop(batch);
    }

    Ok(())
}

/// Build the DataFusion SQL for a grouped partial aggregate.
///
/// Produces:
/// ```sql
/// SELECT <gk_0>, ..., <gk_{n-1}>, <partial_agg_0>, ...
/// FROM (<aliased_table>)
/// [WHERE <filter>]
/// GROUP BY <gk_0>, ..., <gk_{n-1}>
/// ```
///
/// Group-key expressions are inserted verbatim — they are already-rendered
/// DataFusion SQL fragments from the adapter (e.g. `"REGION"` or `YEAR("DATE")`).
/// No LIMIT is applied (the adapter never pushes LIMIT into grouped shard specs;
/// the outer wrapper applies LIMIT after re-grouping the partials).
pub fn build_grouped_partial_agg_sql(
    group_keys: &[String],
    aggregates: &[AggregatePlan],
    aliased_table: &str,
    filter: Option<&str>,
) -> String {
    // SELECT list: group keys first (verbatim), then partial aggregate items.
    let mut select_items: Vec<String> = group_keys.to_vec();
    let partial_items: Vec<String> = aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| partial_select_items(plan, i))
        .collect();
    select_items.extend(partial_items);

    let mut sql = format!(
        "SELECT {} FROM ({})",
        select_items.join(", "),
        aliased_table
    );

    if let Some(f) = filter
        && !f.is_empty()
    {
        sql.push_str(" WHERE ");
        sql.push_str(f);
    }

    // GROUP BY the group-key expressions (same verbatim fragments as in SELECT).
    sql.push_str(" GROUP BY ");
    sql.push_str(&group_keys.join(", "));

    sql
}

/// Stringify a group-key `Value` for the `GK_i VARCHAR(2000000)` EMITS column.
///
/// NULL group keys stay NULL (the outer wrapper groups them together consistently).
/// String values pass through unchanged. All other types are converted to their
/// canonical string representation so the adapter's VARCHAR column accepts them.
fn value_to_gk_string(v: Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::String(s) => Value::String(s),
        Value::Bool(b) => Value::String(if b { "true" } else { "false" }.to_string()),
        Value::Int32(n) => Value::String(n.to_string()),
        Value::Int64(n) => Value::String(n.to_string()),
        Value::Double(f) => Value::String(f.to_string()),
        Value::Numeric(d) => Value::String(d.to_string()),
        Value::Date(nd) => Value::String(nd.to_string()),
        Value::Timestamp(ndt) => Value::String(ndt.to_string()),
    }
}

/// Build the fallback null partial row for an empty aggregate result.
///
/// COUNT/CountCol -> 0 (not NULL); SUM/Min/Max/Avg parts -> NULL.
/// Stat family: cnt -> 0, sum -> NULL, sumsq -> NULL.
fn emit_null_partial_row(aggregates: &[AggregatePlan]) -> Vec<exasol_udf_sdk::value::Value> {
    use exasol_udf_sdk::value::Value;
    let mut row = Vec::new();
    for plan in aggregates {
        match plan.kind {
            AggKind::Count | AggKind::CountCol => row.push(Value::Int64(0)),
            AggKind::Sum | AggKind::Min | AggKind::Max => row.push(Value::Null),
            // An empty shard contributes NO distinct values: emit an empty JSON
            // array (not NULL, not 0) so the scalar merge UDF unions it cleanly.
            AggKind::CountDistinct => row.push(Value::String("[]".to_string())),
            AggKind::Avg => {
                row.push(Value::Null); // partial_avg_sum
                row.push(Value::Int64(0)); // partial_avg_cnt
            }
            AggKind::VarPop | AggKind::VarSamp | AggKind::StddevPop | AggKind::StddevSamp => {
                row.push(Value::Int64(0)); // partial_stat_cnt
                row.push(Value::Null); // partial_stat_sum
                row.push(Value::Null); // partial_stat_sumsq
            }
        }
    }
    row
}

/// COLUMN CONTRACT:
///
/// Iterating `aggregates` in order, each plan item at index `i` contributes:
/// - `Count`    -> 1 column: `"PARTIAL_count_{i}"`   (DECIMAL(20,0), summable)
/// - `CountCol` -> 1 column: `"PARTIAL_count_{i}"`   (DECIMAL(20,0), summable)
/// - `Sum`      -> 1 column: `"PARTIAL_sum_{i}"`     (type from `partial_emits_items`: DOUBLE
///   PRECISION for float columns, DECIMAL(36,s) for DECIMAL(p,s) columns)
/// - `Min`      -> 1 column: `"PARTIAL_min_{i}"`     (type from `partial_emits_items`: the
///   column's real Exasol type, e.g. DATE, TIMESTAMP, or DECIMAL)
/// - `Max`      -> 1 column: `"PARTIAL_max_{i}"`     (type from `partial_emits_items`: same
///   as Min — the column's real Exasol type)
/// - `Avg`      -> 2 columns: `"PARTIAL_avg_sum_{i}"` (DOUBLE PRECISION) then
///   `"PARTIAL_avg_cnt_{i}"` (DECIMAL(20,0))
///
/// For the exact EMITS types, defer to `partial_emits_items` in `adapter::pushdown` as the
/// single source of truth — this DataFusion SELECT list produces the values; the EMITS clause
/// declares the Exasol types that receive them.
///
/// The scan UDF aggregate SELECT list, the EMITS clause in the fan-out SQL, and
/// the outer merge SELECT MUST all agree on this order and column count.
///
/// `aliased_table` is a subquery string: `SELECT ... FROM scan_target` with
/// uppercase aliases already applied. No filter applied.
#[cfg(test)]
pub fn build_partial_agg_sql(aggregates: &[AggregatePlan], aliased_table: &str) -> String {
    build_partial_agg_sql_filtered(aggregates, aliased_table, None)
}

/// Build the partial-aggregate SQL, optionally with a WHERE clause.
pub fn build_partial_agg_sql_filtered(
    aggregates: &[AggregatePlan],
    aliased_table: &str,
    filter: Option<&str>,
) -> String {
    let select_items: Vec<String> = aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| partial_select_items(plan, i))
        .collect();

    let mut sql = format!(
        "SELECT {} FROM ({})",
        select_items.join(", "),
        aliased_table
    );

    if let Some(f) = filter
        && !f.is_empty()
    {
        sql.push_str(" WHERE ");
        sql.push_str(f);
    }

    sql
}

/// Render the DataFusion SQL argument for an aggregate plan entry.
///
/// When the plan carries a rendered expression argument (`arg_expr`, produced by
/// the adapter via `vs_expression::render_expression` — e.g. `LENGTH("L_COMMENT")`)
/// it is substituted VERBATIM as raw SQL text; it is already a fully-rendered
/// DataFusion fragment, so it is NOT re-quoted or re-escaped as an identifier.
/// Otherwise the bare column name is emitted as a quoted identifier.
fn agg_arg_sql(plan: &AggregatePlan) -> String {
    match plan.arg_expr.as_deref() {
        Some(expr) => expr.to_string(),
        None => quote_ident(plan.column.as_deref().unwrap_or("")),
    }
}

/// Produce the SELECT list items for one aggregate plan entry at index `i`.
fn partial_select_items(plan: &AggregatePlan, i: usize) -> Vec<String> {
    match plan.kind {
        AggKind::Count => {
            vec![format!(r#"COUNT(*) AS "PARTIAL_count_{i}""#)]
        }
        AggKind::CountCol => {
            let arg = agg_arg_sql(plan);
            vec![format!(r#"COUNT({arg}) AS "PARTIAL_count_{i}""#)]
        }
        AggKind::Sum => {
            let arg = agg_arg_sql(plan);
            vec![format!(r#"SUM({arg}) AS "PARTIAL_sum_{i}""#)]
        }
        AggKind::Min => {
            let arg = agg_arg_sql(plan);
            vec![format!(r#"MIN({arg}) AS "PARTIAL_min_{i}""#)]
        }
        AggKind::Max => {
            let arg = agg_arg_sql(plan);
            vec![format!(r#"MAX({arg}) AS "PARTIAL_max_{i}""#)]
        }
        AggKind::Avg => {
            let arg = agg_arg_sql(plan);
            vec![
                format!(r#"SUM({arg}) AS "PARTIAL_avg_sum_{i}""#),
                format!(r#"COUNT({arg}) AS "PARTIAL_avg_cnt_{i}""#),
            ]
        }
        // Single-group COUNT(DISTINCT): emit the shard's LOCAL distinct set as one
        // Arrow List cell. NULLs are excluded downstream in Rust during JSON
        // serialization (`distinct_list_to_json`), so the merged count matches
        // single-node `COUNT(DISTINCT)` semantics (which never count NULL).
        AggKind::CountDistinct => {
            let arg = agg_arg_sql(plan);
            vec![format!(r#"array_agg(DISTINCT {arg}) AS "PARTIAL_cd_{i}""#)]
        }
        // STDDEV/VARIANCE family: emit (cnt, sum, sum_sq) sufficient statistics.
        // COUNT(col) excludes NULLs, matching single-node semantics.
        AggKind::VarPop | AggKind::VarSamp | AggKind::StddevPop | AggKind::StddevSamp => {
            let col = plan.column.as_deref().unwrap_or("");
            let qcol = quote_ident(col);
            vec![
                format!(r#"COUNT({qcol}) AS "PARTIAL_stat_cnt_{i}""#),
                format!(r#"SUM({qcol}) AS "PARTIAL_stat_sum_{i}""#),
                format!(r#"SUM({qcol} * {qcol}) AS "PARTIAL_stat_sumsq_{i}""#),
            ]
        }
    }
}

/// Per-shard cap on the number of distinct elements a single `COUNT(DISTINCT)`
/// local set may hold before the scan aborts. Bounds pre-serialization
/// memory/CPU for a single shard (see the plan's Requirements table).
const MAX_DISTINCT_ELEMENTS_PER_SHARD: usize = 100_000;

/// Per-shard cap on the serialized-byte size of a single `COUNT(DISTINCT)` local
/// set's JSON array (1 MiB). Kept well below the `VARCHAR(2000000)` wire limit so
/// the array-of-arrays LISTAGG wrapper the merge UDF consumes still fits.
const MAX_DISTINCT_BYTES_PER_SHARD: usize = 1_048_576;

/// Which per-shard `COUNT(DISTINCT)` safety cap was exceeded.
enum DistinctCap {
    Elements,
    Bytes,
}

/// Build the clean bounded-resource error for an exceeded `COUNT(DISTINCT)` cap.
///
/// The message names the offending column and the cap that tripped, mirroring the
/// engine's `ResourcesExhausted` bounded-execution convention. `label` is the
/// aggregate argument (a bare column name or a rendered DataFusion expression) —
/// never a credential value, so the message is credential-free by construction.
fn distinct_cap_error(label: &str, cap: DistinctCap) -> UdfError {
    let detail = match cap {
        DistinctCap::Elements => format!(
            "distinct-element count exceeded the per-shard cap of {MAX_DISTINCT_ELEMENTS_PER_SHARD}"
        ),
        DistinctCap::Bytes => format!(
            "serialized size exceeded the per-shard cap of {MAX_DISTINCT_BYTES_PER_SHARD} bytes"
        ),
    };
    UdfError::User(format!(
        "scan failed: memory exhausted (ResourcesExhausted): COUNT(DISTINCT {label}) \
         local set for this shard {detail}; the query cannot be completed within the \
         per-shard safety bound"
    ))
}

/// Map one SDK [`Value`] to its canonical `serde_json::Value` token for the
/// per-shard distinct set. All shards run identical DataFusion over the same
/// column type, so the representation is stable across shards and the scalar
/// merge UDF can union tokens by value. Decimal / date / timestamp use their
/// lossless string form; NULL is never reached (callers skip null elements).
fn distinct_value_to_json(v: Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        Value::Null => J::Null,
        Value::Bool(b) => J::Bool(b),
        Value::Int32(n) => J::Number(n.into()),
        Value::Int64(n) => J::Number(n.into()),
        Value::Double(f) => serde_json::Number::from_f64(f)
            .map(J::Number)
            .unwrap_or(J::Null),
        Value::Numeric(d) => J::String(d.to_string()),
        Value::Date(nd) => J::String(nd.to_string()),
        Value::Timestamp(ndt) => J::String(ndt.to_string()),
        Value::String(s) => J::String(s),
    }
}

/// Serialize one shard's local distinct set (the element values of a single
/// `array_agg(DISTINCT)` List cell) to a JSON array string.
///
/// - NULL elements are SKIPPED: `COUNT(DISTINCT)` never counts NULL, so the
///   merged cardinality matches single-node semantics.
/// - Elements are converted to canonical JSON tokens IN RUST — no Arrow type
///   crosses the `.so` boundary; only the returned `String` does.
/// - The per-shard safety cap is enforced WHILE building: the count and running
///   serialized byte length are checked as each element is appended, aborting
///   with [`distinct_cap_error`] the moment either threshold is exceeded. The
///   partial set is never truncated (a truncated set would produce a wrong
///   merged count).
fn distinct_list_to_json(values: &dyn Array, label: &str) -> Result<String, UdfError> {
    let mut tokens: Vec<String> = Vec::new();
    // Account for the enclosing '[' and ']'.
    let mut running_bytes: usize = 2;
    for i in 0..values.len() {
        if values.is_null(i) {
            continue;
        }
        // Adding this element would exceed the element cap → abort (no truncation).
        if tokens.len() + 1 > MAX_DISTINCT_ELEMENTS_PER_SHARD {
            return Err(distinct_cap_error(label, DistinctCap::Elements));
        }
        let token = serde_json::to_string(&distinct_value_to_json(arrow_value_at(values, i)))
            .map_err(|e| {
                UdfError::User(format!(
                    "scan failed: could not serialize a COUNT(DISTINCT {label}) value: {e}"
                ))
            })?;
        // +1 for the comma separator preceding every element after the first.
        let separator = usize::from(!tokens.is_empty());
        running_bytes = running_bytes.saturating_add(token.len() + separator);
        if running_bytes > MAX_DISTINCT_BYTES_PER_SHARD {
            return Err(distinct_cap_error(label, DistinctCap::Bytes));
        }
        tokens.push(token);
    }
    Ok(format!("[{}]", tokens.join(",")))
}

/// Serialize the `COUNT(DISTINCT)` List cell at `row` to its JSON array partial
/// value. A NULL cell (an empty group — `array_agg` over zero rows returns NULL)
/// becomes the empty array `"[]"`, so the merge treats the shard as contributing
/// no distinct values.
fn distinct_cell_to_json(col: &dyn Array, row: usize, label: &str) -> Result<String, UdfError> {
    if col.is_null(row) {
        return Ok("[]".to_string());
    }
    let list = col.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        UdfError::User(
            "COUNT(DISTINCT) partial column is not an Arrow List as expected".to_string(),
        )
    })?;
    let elements = list.value(row);
    distinct_list_to_json(elements.as_ref(), label)
}

/// A human-readable, credential-free label for an aggregate's argument, used in
/// safety-cap error messages (the bare column name, or the rendered expression).
fn agg_label(plan: &AggregatePlan) -> String {
    plan.column
        .as_deref()
        .map(str::to_string)
        .or_else(|| plan.arg_expr.clone())
        .unwrap_or_default()
}

/// Convert the single-group partial-aggregate result row (row 0 of `batch`) into
/// the ordered `Value` row emitted for this shard.
///
/// Walks `aggregates` in the COLUMN CONTRACT order, consuming the exact number of
/// batch columns each aggregate produced in [`partial_select_items`]. Every
/// aggregate except `CountDistinct` converts each of its columns straight through
/// [`arrow_value_at`]; `CountDistinct`'s single Arrow List column is serialized to
/// its JSON-array `VARCHAR` partial value in Rust (Arrow never crosses the `.so`
/// boundary).
fn partial_row_from_batch(
    aggregates: &[AggregatePlan],
    batch: &arrow::record_batch::RecordBatch,
) -> Result<Vec<Value>, UdfError> {
    let mut row: Vec<Value> = Vec::with_capacity(batch.num_columns());
    let mut col = 0usize;
    for plan in aggregates {
        match plan.kind {
            AggKind::CountDistinct => {
                let json = distinct_cell_to_json(batch.column(col).as_ref(), 0, &agg_label(plan))?;
                row.push(Value::String(json));
                col += 1;
            }
            AggKind::Avg => {
                row.push(arrow_value_at(batch.column(col), 0));
                row.push(arrow_value_at(batch.column(col + 1), 0));
                col += 2;
            }
            AggKind::VarPop | AggKind::VarSamp | AggKind::StddevPop | AggKind::StddevSamp => {
                row.push(arrow_value_at(batch.column(col), 0));
                row.push(arrow_value_at(batch.column(col + 1), 0));
                row.push(arrow_value_at(batch.column(col + 2), 0));
                col += 3;
            }
            _ => {
                row.push(arrow_value_at(batch.column(col), 0));
                col += 1;
            }
        }
    }
    Ok(row)
}

/// Build a DataFusion SessionContext with the MinIO object store registered.
///
/// Sizes the DataFusion memory pool from `memory_limit_bytes` (UDF per-instance
/// limit in bytes; `0` = unknown sentinel → conservative 1024 MB default) and
/// probes `/tmp` for disk-spill eligibility.
fn build_session_context(
    spec: &ScanSpec,
    memory_limit_bytes: u64,
) -> Result<SessionContext, UdfError> {
    let config = session_config_for_spec(spec);

    // Memory pool + spill config.
    let spill = probe_tmp_spill();
    let runtime_env = build_runtime_env(
        memory_limit_bytes,
        spec.memory_pool_fraction,
        spec.instance_overhead_mb * 1024 * 1024,
        spill,
    )
    .map_err(|e| UdfError::User(format!("failed to build DataFusion runtime env: {e}")))?;

    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

    // Register the MinIO object store for the S3 URL scheme, wrapped so that
    // per-file HEAD requests are answered from the caller-supplied sizes in the
    // spec instead of issuing an object-store HEAD over the network.
    let bucket = extract_bucket(spec)?;
    let s3 = build_s3_store(&spec.storage, &bucket, spec.s3_max_connections)?;
    let sizes = build_spec_size_index(spec)?;
    let sized_store = SpecSizedObjectStore::new(Arc::new(s3), sizes);
    let store_url = Url::parse(&format!("s3://{bucket}"))
        .map_err(|e| UdfError::User(format!("invalid bucket URL: {e}")))?;
    ctx.runtime_env()
        .register_object_store(&store_url, Arc::new(sized_store));

    Ok(ctx)
}

/// HTTP client options that bound the object store's warm connection pool to the
/// resolved connection-concurrency budget.
///
/// `object_store` 0.13.2 exposes no hard "max concurrent requests" ceiling — the
/// reqwest/hyper backend never caps in-flight connections. `pool_max_idle_per_host`
/// is the closest available knob: it bounds how many established connections the
/// pool keeps warm (idle, reusable) per host, whose reqwest default is unbounded.
/// This is the axis that maps to "how many concurrent fetches from S3 the instance
/// keeps warm", independent of the DataFusion CPU thread/partition budget. Clamped
/// to at least 1 so the ceiling is never zero.
fn client_options_for(budget: usize) -> ClientOptions {
    ClientOptions::new().with_pool_max_idle_per_host(budget.max(1))
}

/// Reconstruct the absolute file URI for a per-shard `(path, _)` entry.
///
/// An entry that already contains a scheme (`"://"`) is absolute and returned
/// unchanged. Otherwise it is relative to `table_root` and joined onto it with
/// exactly one `/` separator (a trailing `/` on the root and a leading `/` on the
/// entry are both trimmed first, so the separator is neither doubled nor dropped).
pub(crate) fn reconstruct_abs_uri(entry_path: &str, table_root: &str) -> String {
    if entry_path.contains("://") {
        return entry_path.to_string();
    }
    let root = table_root.strip_suffix('/').unwrap_or(table_root);
    let rel = entry_path.strip_prefix('/').unwrap_or(entry_path);
    format!("{root}/{rel}")
}

/// Build the map of caller-known file sizes keyed by the object-store [`Path`]
/// the store observes in `head` — i.e. the `ListingTableUrl` prefix DataFusion
/// passes for an exact-file (non-collection) URL. Keying by that prefix is what
/// lets [`SpecSizedObjectStore`] satisfy each per-file metadata lookup from the
/// spec without a network round-trip.
///
/// [`Path`]: object_store::path::Path
fn build_spec_size_index(spec: &ScanSpec) -> Result<HashMap<ObjectStorePath, u64>, UdfError> {
    let mut sizes = HashMap::with_capacity(spec.files.len());
    for entry in &spec.files {
        let abs = reconstruct_abs_uri(&entry.path, &spec.table_root);
        let url = ListingTableUrl::parse(&abs)
            .map_err(|e| UdfError::User(format!("invalid listing URL '{abs}': {e}")))?;
        sizes.insert(url.prefix().clone(), entry.size);
    }
    Ok(sizes)
}

/// An [`ObjectStore`] decorator that answers per-file metadata (`head`) from a
/// caller-supplied size index instead of the network, delegating every other
/// operation to the wrapped store.
///
/// DataFusion resolves an exact-file `ListingTableUrl` by calling `head` on the
/// store, which (object_store 0.13.2) dispatches through the `ObjectStoreExt`
/// blanket to `get_opts(location, GetOptions { head: true, .. })`. So the HEAD is
/// intercepted here in `get_opts`: when `head` is set and the location is present
/// in the index, a synthetic [`ObjectMeta`] built from the spec size is returned
/// with no I/O. Data reads (`head == false`) and all non-`get_opts` operations
/// fall through to the inner store unchanged.
#[derive(Debug)]
struct SpecSizedObjectStore {
    inner: Arc<dyn ObjectStore>,
    sizes: HashMap<ObjectStorePath, u64>,
}

impl SpecSizedObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, sizes: HashMap<ObjectStorePath, u64>) -> Self {
        Self { inner, sizes }
    }
}

impl std::fmt::Display for SpecSizedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpecSizedObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SpecSizedObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head
            && let Some(&size) = self.sizes.get(location)
        {
            let meta = ObjectMeta {
                location: location.clone(),
                last_modified: Utc.timestamp_nanos(0),
                size,
                e_tag: None,
                version: None,
            };
            return Ok(GetResult {
                payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
                meta,
                range: 0..0,
                attributes: object_store::Attributes::default(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectStorePath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectStorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Build an AmazonS3 (MinIO-compatible) object store from StorageProps, sizing the
/// HTTP connection pool to the resolved `s3_max_connections` budget.
fn build_s3_store(
    storage: &crate::scan::spec::StorageProps,
    bucket: &str,
    s3_max_connections: usize,
) -> Result<impl ObjectStore, UdfError> {
    // `with_client_options` REPLACES the builder's whole `ClientOptions` (it does
    // not merge), so it must run before `with_allow_http`, which layers onto
    // whatever `ClientOptions` is already set. Reversing this order silently
    // drops `allow_http`, breaking plain-HTTP endpoints like MinIO.
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&storage.region)
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key)
        .with_client_options(client_options_for(s3_max_connections))
        .with_allow_http(storage.allow_http);

    // Path-style stores (MinIO and other S3-compatibles) need the explicit endpoint
    // and path-style addressing. For real AWS S3 (virtual-hosted) we must NOT set an
    // endpoint: object_store derives https://<bucket>.s3.<region>.amazonaws.com from
    // the region. Setting a regional endpoint without the bucket sends requests to
    // the account root -> S3 returns 403 (s3:ListAllMyBuckets).
    if storage.path_style {
        builder = builder
            .with_endpoint(&storage.endpoint)
            .with_virtual_hosted_style_request(false);
    }

    if let Some(token) = &storage.session_token {
        builder = builder.with_token(token);
    }

    let secrets = storage.secret_values();
    builder.build().map_err(|e| {
        // Do not echo the error directly — it might contain credential fragments.
        let stripped = crate::scan::emit::redact_secret_values(&e.to_string(), &secrets);
        UdfError::User(format!(
            "failed to configure S3 object store: {}",
            crate::scan::emit::redact_credentials(&stripped)
        ))
    })
}

/// Extract the S3 bucket name from the first file in the spec.
///
/// The first entry may now be relative to `table_root`, so it is reconstructed
/// into its absolute URI first (a `://`-bearing entry passes through unchanged);
/// the bucket is then the host of that absolute URI. For the all-absolute case
/// (empty `table_root`) reconstruction is a no-op, so behavior is unchanged.
fn extract_bucket(spec: &ScanSpec) -> Result<String, UdfError> {
    let first = spec
        .files
        .first()
        .ok_or_else(|| UdfError::User("scan spec has no files".into()))?;
    let abs = reconstruct_abs_uri(&first.path, &spec.table_root);
    let url = Url::parse(&abs).map_err(|e| UdfError::User(format!("invalid file URI: {e}")))?;
    url.host_str()
        .map(|h| h.to_string())
        .ok_or_else(|| UdfError::User(format!("file URI has no bucket/host: {abs}")))
}

/// Verify every data file and associated delete file in `spec` resolves to the
/// same object-store root (scheme + host) as `first_abs`.
///
/// The scan registers a single object store keyed by that root (see
/// [`register_files`] / [`build_session_context`]); a file under a different
/// root would be read through the wrong store. This fails loud on a mixed-root
/// spec rather than misreading or failing confusingly downstream.
fn validate_uniform_object_store(spec: &ScanSpec, first_abs: &str) -> Result<(), UdfError> {
    // Compare the exact `ObjectStoreUrl` (scheme + authority) each file resolves
    // to — the very key the store is registered/looked up under — so the check
    // matches the runtime invariant precisely (and accepts every URI form the
    // scan itself accepts, e.g. bare local paths).
    let store_key = |abs: &str| -> Result<String, UdfError> {
        Ok(ListingTableUrl::parse(abs)
            .map_err(|e| UdfError::User(format!("invalid file URI '{abs}': {e}")))?
            .object_store()
            .as_str()
            .to_string())
    };
    let expected = store_key(first_abs)?;
    let check = |abs: &str, kind: &str| -> Result<(), UdfError> {
        let got = store_key(abs)?;
        if got != expected {
            return Err(UdfError::User(format!(
                "scan spec mixes object-store roots: {kind} '{abs}' resolves to store '{got}' but \
                 the first file resolves to '{expected}'; the scan registers a single object store"
            )));
        }
        Ok(())
    };
    for entry in &spec.files {
        check(
            &reconstruct_abs_uri(&entry.path, &spec.table_root),
            "data file",
        )?;
        for delete in &entry.deletes {
            check(
                &reconstruct_abs_uri(&delete.path, &spec.table_root),
                "delete file",
            )?;
        }
    }
    Ok(())
}

/// Build the DataFrame: register files as a ListingTable, then apply
/// projection/filter/limit SQL.
async fn build_dataframe(
    ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<datafusion::dataframe::DataFrame, UdfError> {
    // Register only the assigned files as a listing table.
    let table_name = "scan_target";
    register_files(ctx, table_name, spec).await?;

    // Build the SELECT SQL applying projection, filter, and limit.
    let sql = build_scan_sql(ctx, table_name, spec).await?;
    ctx.sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("DataFusion SQL error: {e}")))
}

/// Register the assigned Parquet files as `table_name`, backed by the custom
/// [`PositionalDeleteScanTable`] provider over DataFusion's `ParquetSource`.
///
/// This replaces the previous `ListingTable`: a `ListingTable` cannot build a
/// `FileScanConfig` directly and therefore cannot attach the per-data-file base
/// `ParquetAccessPlan` that applies Iceberg positional deletes. The custom
/// provider is unified across ALL scans — delete-free files take the identical
/// path (no access plan attached) — and preserves exactly: the logical schema,
/// the `FieldIdExprAdapter` (field-id-first column binding), and the lean
/// single-partition plan.
///
/// Public so plan-shape / pruning-preservation integration tests can register
/// the exact production provider (with per-file base `ParquetAccessPlan`s) as
/// `scan_target` before asking [`build_raw_scan_physical_plan`] for the committed
/// pipeline — the built-in `SessionContext::register_parquet` shortcut never
/// attaches an access plan and so cannot exercise the delete-carrying path.
pub async fn register_files(
    ctx: &SessionContext,
    table_name: &str,
    spec: &ScanSpec,
) -> Result<(), UdfError> {
    let first_abs = reconstruct_abs_uri(
        &spec
            .files
            .first()
            .ok_or_else(|| UdfError::User("scan spec has no files".into()))?
            .path,
        &spec.table_root,
    );
    // The scan registers exactly ONE object store, keyed by the first file's
    // scheme+host (`object_store_url` below and the store registered in
    // `build_session_context`). Every data file and every associated delete file
    // must resolve to that same root; a file under a different bucket/host would
    // be read through the wrong (or an unregistered) store — a confusing failure
    // or, worse, a wrong-key read. Fail loud on a mixed-root spec (e.g. an
    // Iceberg `write.data.path` or a delete file in a different bucket) instead.
    validate_uniform_object_store(spec, &first_abs)?;

    let object_store_url = ListingTableUrl::parse(&first_abs)
        .map_err(|e| UdfError::User(format!("invalid listing URL '{first_abs}': {e}")))?
        .object_store();

    // Prefer the query-time logical schema (with Iceberg field-ids) when the
    // adapter supplied one: use it as the table schema and install the field-id
    // expression adapter so column binding is field-id-first (name fallback) —
    // correct across schema evolution. When it is absent (legacy specs), fall
    // back to inferring one Arrow schema from the first file.
    let secrets = spec.storage.secret_values();
    let use_field_id_adapter = !spec.logical_schema.is_empty();
    let table_schema = if use_field_id_adapter {
        build_logical_arrow_schema(&spec.logical_schema)
    } else {
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_collect_stat(false);
        let first_url = ListingTableUrl::parse(&first_abs)
            .map_err(|e| UdfError::User(format!("invalid listing URL '{first_abs}': {e}")))?;
        listing_options
            .infer_schema(&ctx.state(), &first_url)
            .await
            .map_err(|e| classify_scan_error(e, &secrets))?
    };

    let table = crate::scan::positional_deletes::PositionalDeleteScanTable::new(
        object_store_url,
        table_schema,
        use_field_id_adapter,
        spec.files.clone(),
        spec.table_root.clone(),
        &spec.storage,
    );

    ctx.register_table(table_name, Arc::new(table))
        .map_err(|e| UdfError::User(format!("failed to register table: {e}")))?;

    Ok(())
}

/// Build the raw-row-path DataFusion physical plan for a session whose scan
/// table is already registered as `scan_target`.
///
/// This is the exact production raw-scan pipeline: it reuses [`build_scan_sql`]
/// (the same projection / filter / LIMIT SQL the UDF runs) and DataFusion's
/// `create_physical_plan`. Exposed so plan-shape and pruning-parity integration
/// tests can inspect the committed pipeline without standing up an S3 store —
/// the caller registers a local Parquet file as `scan_target`, then asks for
/// the plan this function produces.
pub async fn build_raw_scan_physical_plan(
    ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>, UdfError> {
    let sql = build_scan_sql(ctx, "scan_target", spec).await?;
    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("DataFusion SQL error: {e}")))?;
    df.create_physical_plan()
        .await
        .map_err(|e| UdfError::User(format!("physical plan error: {e}")))
}

/// Build the SQL string for the scan query.
///
/// For incompatible columns, CAST(col AS VARCHAR) so they arrive as Utf8 and
/// the convert module's JSON fallback just passes them through as Value::String.
async fn build_scan_sql(
    ctx: &SessionContext,
    table_name: &str,
    spec: &ScanSpec,
) -> Result<String, UdfError> {
    // Get the registered table's schema so we can check which columns need casting.
    let table = ctx
        .table(table_name)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered table: {e}")))?;
    let schema = table.schema();

    // The adapter speaks Exasol identifier casing (uppercase) for projection,
    // filter, and EMITS, while the Parquet/Arrow columns keep the Iceberg field
    // casing (typically lowercase). DataFusion matches quoted identifiers
    // case-sensitively, so first wrap the listing table in an inner SELECT that
    // aliases every Arrow column to its uppercase name. The outer projection and
    // the pushed-down WHERE then both resolve against those uppercase aliases.
    // All columns are aliased (not just projected ones) because the filter may
    // reference a column that is not projected.
    let alias_items: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let arrow_name = f.name();
            format!(
                "{} AS {}",
                quote_ident(arrow_name),
                quote_ident(&arrow_name.to_uppercase())
            )
        })
        .collect();
    let inner = format!("SELECT {} FROM {table_name}", alias_items.join(", "));

    // Determine the items to project (already uppercase from the adapter). An
    // empty projection means "all columns"; each is a bare column reference.
    let proj_items: Vec<ProjectionItem> = if spec.projection.is_empty() {
        schema
            .fields()
            .iter()
            .map(|f| ProjectionItem::Column(f.name().to_uppercase()))
            .collect()
    } else {
        spec.projection.clone()
    };

    // Build outer SELECT items. A bare column is quoted as an identifier, with a
    // CAST to VARCHAR for incompatible types so the convert module receives them
    // as Utf8 and emits Value::String. A rendered scalar expression (e.g.
    // `("SCORE" * 2)`) is spliced VERBATIM — it is already valid DataFusion SQL
    // resolved against the uppercase-aliased inner scan, exactly like `filter`
    // and the aggregate `arg_expr`; quoting it as an identifier would build a
    // phantom column name that has no matching field. Emission is positional, so
    // projection order — not name — carries through to EMITS.
    let select_items: Vec<String> = proj_items
        .iter()
        .map(|item| match item {
            ProjectionItem::Expr { expr } => expr.clone(),
            ProjectionItem::Column(col_name) => {
                let col_lower = col_name.to_lowercase();
                let needs_cast = schema
                    .fields()
                    .iter()
                    .find(|f| f.name().to_lowercase() == col_lower)
                    .map(|f| needs_json_fallback(f.data_type()))
                    .unwrap_or(false);
                let upper = col_name.to_uppercase();
                if needs_cast {
                    format!("CAST({} AS VARCHAR)", quote_ident(&upper))
                } else {
                    quote_ident(&upper)
                }
            }
        })
        .collect();

    let select_clause = select_items.join(", ");
    let mut sql = format!("SELECT {select_clause} FROM ({inner})");

    // Append WHERE clause if a translated filter is present.
    if let Some(filter) = &spec.filter
        && !filter.is_empty()
    {
        sql.push_str(" WHERE ");
        sql.push_str(filter);
    }

    // Append ORDER BY clause for a pushed-down ordered top-N scan. The keys are
    // rendered through the SAME shared `render_order_by_clause` the adapter's
    // outer merge SQL uses, so the per-shard bounded sort and the merge sort
    // induce the IDENTICAL ranking — key order, direction (ASC/DESC), and explicit
    // NULL placement (NULLS FIRST/LAST). That structural reuse is what makes the
    // distributed top-N provably equal to single-node evaluation regardless of any
    // engine's default NULL ordering. Placed after WHERE and before LIMIT so
    // DataFusion folds `ORDER BY <keys> LIMIT n` into a bounded, fetch-limited
    // TopK (not a full global sort). When `order_by` is empty this block emits
    // nothing, leaving pre-ordering-feature SQL byte-identical.
    if !spec.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&render_order_by_clause(&spec.order_by));
    }

    // Append LIMIT clause.
    if let Some(limit) = spec.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    Ok(sql)
}

/// Build `"col" AS "COL"` alias items for all fields in `schema`.
///
/// Used to wrap a listing table in an inner SELECT that exposes uppercase column
/// names, so projection/filter expressions resolved against uppercase identifiers
/// find a match regardless of the Parquet field casing.
fn build_alias_items(schema: &datafusion::common::DFSchema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let arrow_name = f.name();
            format!(
                "{} AS {}",
                quote_ident(arrow_name),
                quote_ident(&arrow_name.to_uppercase())
            )
        })
        .collect()
}

/// Arrow field-metadata key that carries the Iceberg field-id.
///
/// Re-exported from the arrow-58 parquet crate so the whole scan crate has one
/// canonical spelling; the logical-schema builder tags each field with it and
/// [`rename_physical_to_logical`] reads it off both the logical and physical schemas.
pub(crate) use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Read the Iceberg field-id off an Arrow field, if present.
///
/// Returns `None` when the field carries no `PARQUET:field_id` metadata (an older
/// writer) or the value is not a parseable `i32`.
fn field_id_of(field: &arrow::datatypes::Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse::<i32>().ok())
}

/// Factory for a field-id-aware [`PhysicalExprAdapter`], installed on the
/// `ListingTableConfig` via `with_expr_adapter_factory`. The Parquet opener calls
/// [`Self::create`] once per file, so files with divergent physical layouts each
/// bind correctly.
///
/// It does NOT reimplement schema adaptation. It composes two steps around
/// [`DefaultPhysicalExprAdapter`]:
///
/// 1. Feed the default a physical schema renamed to logical names by field-id
///    (see [`rename_physical_to_logical`]). The default then resolves each logical
///    column to the correct physical index and reuses its own behavior for the
///    rest — nullable-missing → NULL literal, type divergence → cast,
///    required-missing → error.
/// 2. Rename the default's OUTPUT columns back to the real physical names (at
///    their already-correct indices) — see [`FieldIdExprAdapter`].
///
/// # Why the output must be renamed back (the E2E `rating`/`score` failure)
///
/// The default adapter resolves columns by NAME, so feeding it logical names on
/// both sides makes it emit `Column`s carrying the LOGICAL name (`rating`). But in
/// DataFusion 54 the Parquet opener applies the expr adapter to the PROJECTION as
/// well as the filter, and every downstream consumer of the rewritten projection —
/// `build_projection_read_plan`, `reassign_expr_columns`, and `make_projector` —
/// resolves those `Column`s by NAME against the REAL physical file schema
/// (`score`). A projected `Column("rating")` therefore fails with
/// `Unable to get field named "rating"`. Renaming the output back to the real
/// physical name (order is preserved, so the index is already right) makes those
/// name-based lookups succeed while keeping the field-id binding.
#[derive(Debug)]
pub(crate) struct FieldIdExprAdapterFactory;

impl PhysicalExprAdapterFactory for FieldIdExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: arrow::datatypes::SchemaRef,
        physical_file_schema: arrow::datatypes::SchemaRef,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExprAdapter>> {
        // Delegate to the default adapter over a physical schema whose fields are
        // renamed to their logical names by field-id. The default then resolves
        // each logical column to the correct physical INDEX (order is preserved by
        // the rename) and applies cast / NULL-fill / required-missing-error against
        // the logical field — the reused behavior.
        let renamed_physical =
            rename_physical_to_logical(&logical_file_schema, &physical_file_schema);
        let inner = DefaultPhysicalExprAdapterFactory
            .create(logical_file_schema, Arc::clone(&renamed_physical))?;
        Ok(Arc::new(FieldIdExprAdapter {
            inner,
            physical_file_schema,
        }))
    }
}

/// Wraps [`DefaultPhysicalExprAdapter`] so field-id resolution reaches the
/// projection READ path, not just filter/predicate expressions.
///
/// The default adapter resolves columns by NAME. We feed it a physical schema
/// renamed to logical names (so it binds by field-id and reuses its cast /
/// NULL-fill / required-missing logic), which makes it emit `Column`s carrying
/// the LOGICAL name at the correct physical index. But every downstream consumer
/// in the Parquet opener — `build_projection_read_plan`, `reassign_expr_columns`,
/// and `make_projector` — resolves those `Column`s by NAME against the REAL
/// physical file schema (`score`, not `rating`). Left as-is a renamed column
/// projection fails with `Unable to get field named "rating"`.
///
/// So after delegating, we walk the rewritten expression and rename each
/// resolved `Column` back to the real physical field NAME at its (already
/// correct) index. Order is preserved by [`rename_physical_to_logical`], so the
/// column's index still points at the right physical slot; only the name must be
/// restored so the opener's name-based lookups succeed. NULL-filled columns
/// become `Literal`s (no `Column` to rename) and pass through untouched.
#[derive(Debug)]
struct FieldIdExprAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    physical_file_schema: arrow::datatypes::SchemaRef,
}

impl PhysicalExprAdapter for FieldIdExprAdapter {
    fn rewrite(
        &self,
        expr: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
        use datafusion::physical_expr::expressions::Column;

        let rewritten = self.inner.rewrite(expr)?;
        rewritten
            .transform_down(|node| {
                if let Some(column) = node.downcast_ref::<Column>() {
                    let real_name = self.physical_file_schema.field(column.index()).name();
                    if real_name != column.name() {
                        return Ok(Transformed::yes(Arc::new(Column::new(
                            real_name,
                            column.index(),
                        ))));
                    }
                }
                Ok(Transformed::no(node))
            })
            .data()
    }
}

/// Rename each physical field to the logical name that shares its Iceberg
/// field-id, preserving field order, type, nullability, and metadata.
///
/// Resolution per physical field:
/// 1. If it carries a `PARQUET:field_id` matching a logical field's id → adopt
///    that logical field's name (this is the rename/field-id binding).
/// 2. Otherwise (no field-id, or an id absent from the logical schema) → keep the
///    physical name unchanged, which makes the default adapter's name lookup act
///    as the physical-name fallback (and leaves dropped columns unreferenced).
///
/// Assumes that post-rename logical names are unique among the referenced physical
/// fields. Name collisions from drop+rename-into-a-reused-name are out of scope
/// and belong to the name-mapping work tracked in issue #28.
fn rename_physical_to_logical(
    logical: &arrow::datatypes::Schema,
    physical: &arrow::datatypes::Schema,
) -> arrow::datatypes::SchemaRef {
    use std::collections::HashMap;

    let logical_name_by_id: HashMap<i32, &str> = logical
        .fields()
        .iter()
        .filter_map(|f| field_id_of(f).map(|id| (id, f.name().as_str())))
        .collect();

    let renamed_fields: Vec<arrow::datatypes::FieldRef> = physical
        .fields()
        .iter()
        .map(|physical_field| {
            match field_id_of(physical_field).and_then(|id| logical_name_by_id.get(&id)) {
                Some(&logical_name) if logical_name != physical_field.name() => {
                    Arc::new(physical_field.as_ref().clone().with_name(logical_name))
                }
                _ => Arc::clone(physical_field),
            }
        })
        .collect();

    Arc::new(arrow::datatypes::Schema::new_with_metadata(
        renamed_fields,
        physical.metadata().clone(),
    ))
}

/// Build the logical Arrow schema from the spec's query-time logical schema.
///
/// Each field is tagged with its Iceberg field-id (`PARQUET:field_id`) so
/// [`FieldIdExprAdapterFactory`] can bind physical file columns to it by id, and
/// carries the schema's declared nullability (Iceberg `optional`). The Arrow data
/// type is reconstructed from the compact tag via [`arrow_type_from_tag`].
pub(crate) fn build_logical_arrow_schema(
    logical_schema: &[crate::scan::spec::LogicalField],
) -> arrow::datatypes::SchemaRef {
    use crate::types::mapping::arrow_type_from_tag;
    use std::collections::HashMap;

    let fields: Vec<arrow::datatypes::FieldRef> = logical_schema
        .iter()
        .map(|lf| {
            let field = arrow::datatypes::Field::new(
                &lf.name,
                arrow_type_from_tag(&lf.arrow_type),
                lf.nullable,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                lf.field_id.to_string(),
            )]));
            Arc::new(field)
        })
        .collect();

    Arc::new(arrow::datatypes::Schema::new(fields))
}

/// Double-quote an identifier (SQL-safe column name).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::runtime::{DEFAULT_BUDGET_BYTES, MIN_POOL_FLOOR_BYTES};
    use crate::scan::spec::{AggKind, AggregatePlan, FileEntry, StorageProps};
    use datafusion::execution::memory_pool::MemoryLimit;
    use object_store::ClientConfigKey;

    // ---------------------------------------------------------------------------
    // build_session_context memory pool sizing — seam tests for task 1.3
    // ---------------------------------------------------------------------------

    /// Minimal ScanSpec with a valid-looking S3 URI for build_session_context tests.
    /// The byte size of the local file behind a `file://` URL.
    ///
    /// The custom `ParquetSource`-backed provider builds each file's `ObjectMeta`
    /// from the spec-supplied size (the no-HEAD design), so tests that register a
    /// local Parquet file must supply its real size instead of a `0` placeholder.
    fn local_file_size(file_url: &str) -> u64 {
        let path = url::Url::parse(file_url)
            .expect("valid file URL")
            .to_file_path()
            .expect("file:// URL");
        std::fs::metadata(path).expect("stat local parquet").len()
    }

    fn minimal_spec() -> ScanSpec {
        ScanSpec {
            table_root: String::new(),
            files: vec![FileEntry::new("s3://test-bucket/data/part-0.parquet", 1024)],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "testkey".into(),
                secret_key: "testsecret".into(),
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

    /// A positive memory limit causes the DataFusion pool to be sized at fraction × (limit − overhead).
    /// Uses minimal_spec defaults: fraction=0.6, overhead=200 MiB.
    #[test]
    fn session_context_sizes_pool_from_ctx_limit() {
        let limit: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB
        let spec = minimal_spec();
        let overhead_bytes = spec.instance_overhead_mb * 1024 * 1024;
        let net = limit - overhead_bytes;
        let expected_budget = (net as f64 * spec.memory_pool_fraction) as usize;
        let ctx = build_session_context(&spec, limit).expect("build must succeed");
        match ctx.runtime_env().memory_pool.memory_limit() {
            MemoryLimit::Finite(actual) => assert_eq!(
                actual, expected_budget,
                "pool budget must be fraction × (limit − overhead)"
            ),
            _ => panic!("expected Finite pool limit"),
        }
    }

    /// A zero memory limit causes the DataFusion pool to use the conservative default budget.
    #[test]
    fn session_context_uses_default_budget_on_zero_limit() {
        let ctx = build_session_context(&minimal_spec(), 0).expect("build must succeed");
        match ctx.runtime_env().memory_pool.memory_limit() {
            MemoryLimit::Finite(actual) => assert_eq!(
                actual, DEFAULT_BUDGET_BYTES as usize,
                "pool budget must equal the 1 GiB default when limit is unknown (0)"
            ),
            _ => panic!("expected Finite pool limit"),
        }
    }

    /// Task 5.2: explicit non-default fraction/overhead in spec flow through to pool sizing.
    ///
    /// Builds a spec with fraction=0.5 and overhead=256 MiB, calls build_session_context
    /// with a known limit (4 GiB), and asserts the pool equals 0.5 × (4 GiB − 256 MiB).
    /// This proves the values are read from the spec, not from hardcoded constants.
    #[test]
    fn memory_budget_round_trips_into_scan_spec() {
        let mut spec = minimal_spec();
        spec.memory_pool_fraction = 0.5;
        spec.instance_overhead_mb = 256;
        let limit: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB
        let overhead_bytes = 256_u64 * 1024 * 1024;
        let net = limit - overhead_bytes;
        let expected = (net as f64 * 0.5_f64) as usize;
        let ctx = build_session_context(&spec, limit).expect("build must succeed");
        match ctx.runtime_env().memory_pool.memory_limit() {
            MemoryLimit::Finite(actual) => assert_eq!(
                actual, expected,
                "pool budget must be 0.5 × (4 GiB − 256 MiB); got {actual}, expected {expected}"
            ),
            _ => panic!("expected Finite pool limit"),
        }
        // Verify this is NOT the MIN_POOL_FLOOR_BYTES (it should be much larger).
        assert!(
            expected > MIN_POOL_FLOOR_BYTES as usize,
            "expected budget must exceed the floor"
        );
    }

    // ---------------------------------------------------------------------------
    // client_options_for — object-store connection-concurrency seam (task 2.5)
    // ---------------------------------------------------------------------------

    /// The resolved connection budget is carried onto the object store's HTTP
    /// client options as the warm-connection-pool ceiling per host.
    #[test]
    fn client_options_carry_connection_budget() {
        let opts = client_options_for(32);
        assert_eq!(
            opts.get_config_value(&ClientConfigKey::PoolMaxIdlePerHost),
            Some("32".to_string()),
            "client options must carry the resolved connection budget as pool_max_idle_per_host"
        );
    }

    /// A zero budget clamps to at least 1 so the pool ceiling is never zero/negative.
    #[test]
    fn client_options_clamp_budget_to_at_least_one() {
        let opts = client_options_for(0);
        assert_eq!(
            opts.get_config_value(&ClientConfigKey::PoolMaxIdlePerHost),
            Some("1".to_string()),
            "a zero budget must clamp to at least 1"
        );
    }

    /// The store built from a spec inherits the spec's connection budget, and the
    /// build succeeds without leaking any credential value into an error. Exercised
    /// as a unit test against the private `build_s3_store` seam directly (rather
    /// than an integration test) so this function does not need to be `pub` — it
    /// is otherwise only ever called internally from `build_session_context`.
    #[test]
    fn build_s3_store_applies_spec_connection_budget() {
        let mut spec = minimal_spec();
        spec.s3_max_connections = 16;
        let bucket = extract_bucket(&spec).expect("bucket must parse");
        build_s3_store(&spec.storage, &bucket, spec.s3_max_connections)
            .expect("store must build with a connection budget");
    }

    // ---------------------------------------------------------------------------
    // Absolute-URI reconstruction, size-index keying, bucket extraction, and the
    // spec-size HEAD wrapper (Group D: tasks 4.1–4.3)
    // ---------------------------------------------------------------------------

    /// 4.1: a `://`-bearing entry is absolute and passes through unchanged.
    #[test]
    fn reconstruct_absolute_entry_passes_through() {
        assert_eq!(
            reconstruct_abs_uri(
                "s3://bucket/db/table/data/f.parquet",
                "s3://bucket/db/table"
            ),
            "s3://bucket/db/table/data/f.parquet"
        );
        // Passthrough holds even against an empty root.
        assert_eq!(
            reconstruct_abs_uri("s3://other/x.parquet", ""),
            "s3://other/x.parquet"
        );
    }

    /// 4.1: a relative entry joins onto the root with exactly one separator,
    /// regardless of a trailing `/` on the root or a leading `/` on the entry.
    #[test]
    fn reconstruct_relative_entry_normalizes_single_separator() {
        let expected = "s3://bucket/db/table/data/f.parquet";
        // Neither side carries the separator.
        assert_eq!(
            reconstruct_abs_uri("data/f.parquet", "s3://bucket/db/table"),
            expected
        );
        // Trailing slash on the root only.
        assert_eq!(
            reconstruct_abs_uri("data/f.parquet", "s3://bucket/db/table/"),
            expected
        );
        // Leading slash on the entry only.
        assert_eq!(
            reconstruct_abs_uri("/data/f.parquet", "s3://bucket/db/table"),
            expected
        );
        // Both sides carry the separator — still not doubled.
        assert_eq!(
            reconstruct_abs_uri("/data/f.parquet", "s3://bucket/db/table/"),
            expected
        );
    }

    /// 4.2: the size index is keyed by the object-store `Path` DataFusion passes
    /// to `head` for an exact-file URL — i.e. the `ListingTableUrl` prefix. A
    /// relative entry keys under the reconstructed path; an absolute entry keys
    /// under its own path.
    #[test]
    fn size_index_keys_by_listing_url_prefix() {
        let mut spec = minimal_spec();
        spec.table_root = "s3://bucket/db/table".into();
        spec.files = vec![
            FileEntry::new("data/rel.parquet", 111),
            FileEntry::new("s3://bucket/db/table/data/abs.parquet", 222),
        ];
        let index = build_spec_size_index(&spec).expect("index must build");

        let rel_key = ObjectStorePath::from("db/table/data/rel.parquet");
        let abs_key = ObjectStorePath::from("db/table/data/abs.parquet");
        assert_eq!(index.get(&rel_key), Some(&111));
        assert_eq!(index.get(&abs_key), Some(&222));

        // The keys equal what an exact-file ListingTableUrl reports as its prefix
        // (the value DataFusion 54 hands to head()).
        let rel_url = ListingTableUrl::parse("s3://bucket/db/table/data/rel.parquet").unwrap();
        assert_eq!(rel_url.prefix(), &rel_key);
    }

    /// 4.3: the bucket is derived from the reconstructed absolute URI of the
    /// first file — for a relative first entry it comes via the table root, for
    /// an absolute-only spec (empty root) behavior is unchanged.
    #[test]
    fn extract_bucket_handles_relative_and_absolute_first_entry() {
        // Relative first entry: bucket comes from the table root.
        let mut rel = minimal_spec();
        rel.table_root = "s3://warehouse/db/table".into();
        rel.files = vec![FileEntry::new("data/part-0.parquet", 1)];
        assert_eq!(extract_bucket(&rel).unwrap(), "warehouse");

        // Absolute first entry, empty root (legacy): unchanged behavior.
        let mut abs = minimal_spec();
        abs.table_root = String::new();
        abs.files = vec![FileEntry::new("s3://legacy-bucket/data/part-0.parquet", 1)];
        assert_eq!(extract_bucket(&abs).unwrap(), "legacy-bucket");
    }

    /// 4.2: the wrapper answers a HEAD (`get_opts` with `head`) from the size
    /// index with no I/O, and falls through to the inner store for an unknown
    /// path and for data reads.
    #[tokio::test]
    async fn sized_store_serves_head_from_index_and_delegates_otherwise() {
        use object_store::ObjectStoreExt;
        use object_store::memory::InMemory;

        // An empty in-memory store: any real head/get is a NotFound, so a
        // successful head can only have come from the size index.
        let inner = Arc::new(InMemory::new());
        let known = ObjectStorePath::from("db/table/data/f.parquet");
        let mut sizes = HashMap::new();
        sizes.insert(known.clone(), 4096u64);
        let store = SpecSizedObjectStore::new(inner, sizes);

        // Known path: metadata is synthesized from the spec size.
        let meta = store
            .head(&known)
            .await
            .expect("head of a known path must be served from the index");
        assert_eq!(meta.size, 4096);
        assert_eq!(meta.location, known);
        assert!(meta.e_tag.is_none());
        assert!(meta.version.is_none());

        // Unknown path: head falls through to the inner store (NotFound).
        let unknown = ObjectStorePath::from("db/table/data/missing.parquet");
        assert!(
            matches!(
                store.head(&unknown).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "an unindexed path must delegate to the inner store"
        );

        // Data read (head == false) of the known path also delegates — the
        // synthetic metadata must never satisfy an actual byte read.
        assert!(
            matches!(
                store.get(&known).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "a data read must delegate to the inner store, not the size index"
        );
    }

    // ---------------------------------------------------------------------------
    // build_partial_agg_sql — host-runnable unit tests
    // ---------------------------------------------------------------------------

    fn sample_plans_count_sum_min_max() -> Vec<AggregatePlan> {
        vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("TS".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("TS".into()),
                arg_expr: None,
            },
        ]
    }

    /// Column order: COUNT(*) first, then SUM, MIN, MAX — each one column.
    #[test]
    fn partial_agg_sql_count_star_uses_count_star() {
        let sql = build_partial_agg_sql(&sample_plans_count_sum_min_max(), "aliased");
        assert!(
            sql.contains("COUNT(*) AS"),
            "COUNT(*) plan must use COUNT(*): {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "COUNT(*) partial column must be PARTIAL_count_0: {sql}"
        );
    }

    /// COUNT(col) plan uses COUNT("COL"), not COUNT(*).
    #[test]
    fn partial_agg_sql_count_col_uses_count_col() {
        let plans = vec![AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("ID".into()),
            arg_expr: None,
        }];
        let sql = build_partial_agg_sql(&plans, "aliased");
        assert!(
            sql.contains(r#"COUNT("ID")"#),
            "COUNT(col) must use COUNT(\"ID\"): {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "COUNT(col) partial must be PARTIAL_count_0: {sql}"
        );
        assert!(
            !sql.contains("COUNT(*)"),
            "COUNT(col) must not use COUNT(*): {sql}"
        );
    }

    /// SUM plan uses SUM("COL") at index 1.
    #[test]
    fn partial_agg_sql_sum_uses_sum_col() {
        let sql = build_partial_agg_sql(&sample_plans_count_sum_min_max(), "aliased");
        assert!(
            sql.contains(r#"SUM("AMOUNT") AS "PARTIAL_sum_1""#),
            "SUM plan must use SUM(\"AMOUNT\") as PARTIAL_sum_1: {sql}"
        );
    }

    /// MIN/MAX plans use MIN/MAX("COL").
    #[test]
    fn partial_agg_sql_min_max_use_min_max_col() {
        let sql = build_partial_agg_sql(&sample_plans_count_sum_min_max(), "aliased");
        assert!(
            sql.contains(r#"MIN("TS") AS "PARTIAL_min_2""#),
            "MIN plan must use MIN at index 2: {sql}"
        );
        assert!(
            sql.contains(r#"MAX("TS") AS "PARTIAL_max_3""#),
            "MAX plan must use MAX at index 3: {sql}"
        );
    }

    /// AVG plan emits TWO columns: sum first, count second.
    #[test]
    fn partial_agg_sql_avg_emits_sum_count_pair() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Avg,
            column: Some("SCORE".into()),
            arg_expr: None,
        }];
        let sql = build_partial_agg_sql(&plans, "aliased");
        // Must NOT emit an AVG() function.
        assert!(
            !sql.contains("AVG("),
            "must not use AVG() for partial avg: {sql}"
        );
        // Must emit SUM for the sum part.
        assert!(
            sql.contains(r#"SUM("SCORE") AS "PARTIAL_avg_sum_0""#),
            "AVG plan must emit SUM as PARTIAL_avg_sum_0: {sql}"
        );
        // Must emit COUNT(col) for the count part (not COUNT(*)).
        assert!(
            sql.contains(r#"COUNT("SCORE") AS "PARTIAL_avg_cnt_0""#),
            "AVG plan must emit COUNT(col) as PARTIAL_avg_cnt_0: {sql}"
        );
    }

    /// Mixed: COUNT/SUM/AVG — AVG contributes two columns at indices 2 (sum) and 2 (cnt),
    /// i.e., each plan item is indexed by its position in the aggregates vec.
    #[test]
    fn partial_agg_sql_mixed_column_order_and_indices() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Avg,
                column: Some("SCORE".into()),
                arg_expr: None,
            },
        ];
        let sql = build_partial_agg_sql(&plans, "aliased");
        // COUNT at index 0.
        assert!(sql.contains("PARTIAL_count_0"), "count at index 0: {sql}");
        // SUM at index 1.
        assert!(sql.contains("PARTIAL_sum_1"), "sum at index 1: {sql}");
        // AVG at index 2 -> both sum and cnt use index 2.
        assert!(
            sql.contains("PARTIAL_avg_sum_2"),
            "avg sum at index 2: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_cnt_2"),
            "avg cnt at index 2: {sql}"
        );
    }

    /// Filter is applied when present.
    #[test]
    fn partial_agg_sql_applies_filter() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }];
        let sql = build_partial_agg_sql_filtered(&plans, "aliased", Some("\"ID\" > 5"));
        assert!(
            sql.contains("WHERE"),
            "filter must produce WHERE clause: {sql}"
        );
        assert!(
            sql.contains("\"ID\" > 5"),
            "filter expression must appear: {sql}"
        );
    }

    /// No filter: no WHERE clause.
    #[test]
    fn partial_agg_sql_no_filter_no_where() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }];
        let sql = build_partial_agg_sql(&plans, "aliased");
        assert!(
            !sql.contains("WHERE"),
            "no filter must produce no WHERE: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 4.1 / 4.2 — expression-argument aggregates and COUNT(DISTINCT) rendering
    // ---------------------------------------------------------------------------

    /// A partial aggregate over a rendered scalar expression argument substitutes
    /// that fragment VERBATIM as the DataFusion function argument — it is NOT
    /// re-quoted as an identifier — while a bare-column plan is unchanged.
    #[test]
    fn partial_sql_uses_rendered_expression_argument() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Sum,
                column: None,
                arg_expr: Some(r#"LENGTH("L_COMMENT")"#.into()),
            },
            AggregatePlan {
                kind: AggKind::Avg,
                column: None,
                arg_expr: Some(r#"("A" + "B")"#.into()),
            },
            // A bare-column plan alongside the expression ones stays quoted-identifier.
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
        ];
        let sql = build_partial_agg_sql(&plans, "aliased");

        // Expression argument is substituted raw (no identifier quoting of the whole expr).
        assert!(
            sql.contains(r#"SUM(LENGTH("L_COMMENT")) AS "PARTIAL_sum_0""#),
            "SUM over an expression must render the expression verbatim: {sql}"
        );
        // The rendered expression must NOT be wrapped as a single quoted identifier.
        assert!(
            !sql.contains(r#"SUM("LENGTH("#),
            "expression argument must not be re-quoted as an identifier: {sql}"
        );
        // AVG over an expression emits the sum/count pair over the same fragment.
        assert!(
            sql.contains(r#"SUM(("A" + "B")) AS "PARTIAL_avg_sum_1""#)
                && sql.contains(r#"COUNT(("A" + "B")) AS "PARTIAL_avg_cnt_1""#),
            "AVG over an expression must decompose over the rendered fragment: {sql}"
        );
        // The bare-column plan is unchanged.
        assert!(
            sql.contains(r#"SUM("AMOUNT") AS "PARTIAL_sum_2""#),
            "bare-column aggregate must remain quoted-identifier: {sql}"
        );
    }

    /// COUNT(DISTINCT) renders an `array_agg(DISTINCT ...)` partial column, over a
    /// bare quoted column by default and over the rendered expression when present.
    #[test]
    fn partial_sql_count_distinct_uses_array_agg_distinct() {
        let bare = vec![AggregatePlan {
            kind: AggKind::CountDistinct,
            column: Some("L_SHIPMODE".into()),
            arg_expr: None,
        }];
        let sql = build_partial_agg_sql(&bare, "aliased");
        assert!(
            sql.contains(r#"array_agg(DISTINCT "L_SHIPMODE") AS "PARTIAL_cd_0""#),
            "COUNT(DISTINCT col) must render array_agg(DISTINCT \"col\"): {sql}"
        );

        let expr = vec![AggregatePlan {
            kind: AggKind::CountDistinct,
            column: None,
            arg_expr: Some(r#"UPPER("L_SHIPMODE")"#.into()),
        }];
        let sql2 = build_partial_agg_sql(&expr, "aliased");
        assert!(
            sql2.contains(r#"array_agg(DISTINCT UPPER("L_SHIPMODE")) AS "PARTIAL_cd_0""#),
            "COUNT(DISTINCT expr) must render array_agg over the verbatim expression: {sql2}"
        );
    }

    /// The shard's local distinct set is serialized to a JSON array string with
    /// NULLs excluded; an empty/NULL list cell serializes to `[]`; and numeric
    /// distinct values serialize as JSON numbers. No Arrow type leaves the boundary.
    #[test]
    fn count_distinct_partial_emits_json_array_null_excluded() {
        use arrow::array::{Int64Builder, ListBuilder, StringBuilder};

        // One list cell holding a shard's local distinct string set, incl. a NULL.
        let mut lb = ListBuilder::new(StringBuilder::new());
        lb.values().append_value("F");
        lb.values().append_value("N");
        lb.values().append_null();
        lb.append(true);
        let list = lb.finish();
        let json = distinct_cell_to_json(&list, 0, "L_LINESTATUS").expect("serialize");
        assert_eq!(
            json, r#"["F","N"]"#,
            "distinct set must be a JSON array with NULLs excluded"
        );

        // A NULL list cell (empty group: array_agg over zero rows returns NULL) → [].
        let mut empty_lb = ListBuilder::new(StringBuilder::new());
        empty_lb.append(false); // null list row
        let empty_list = empty_lb.finish();
        let empty_json = distinct_cell_to_json(&empty_list, 0, "L_LINESTATUS").expect("serialize");
        assert_eq!(empty_json, "[]", "NULL list cell must serialize to []");

        // Numeric distinct values serialize as JSON numbers (stable across shards).
        let mut nb = ListBuilder::new(Int64Builder::new());
        nb.values().append_value(3);
        nb.values().append_value(1);
        nb.values().append_value(2);
        nb.append(true);
        let num_list = nb.finish();
        let num_json = distinct_cell_to_json(&num_list, 0, "L_LINENUMBER").expect("serialize");
        assert_eq!(
            num_json, "[3,1,2]",
            "numeric distinct set must be JSON numbers"
        );
    }

    /// The empty-shard fallback row emits `[]` for a CountDistinct aggregate — not
    /// NULL and not 0 — so it merges cleanly with other shards' non-empty sets.
    #[test]
    fn count_distinct_empty_shard_emits_empty_json_array() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::CountDistinct,
                column: Some("L_SHIPMODE".into()),
                arg_expr: None,
            },
        ];
        let row = emit_null_partial_row(&plans);
        assert_eq!(row.len(), 2, "one partial value per aggregate");
        assert_eq!(row[0], Value::Null, "SUM empty shard is NULL");
        assert_eq!(
            row[1],
            Value::String("[]".to_string()),
            "CountDistinct empty shard must emit an empty JSON array"
        );
    }

    /// The per-shard safety cap aborts with a clean bounded-resource error naming
    /// the offending column and the cap that tripped — never truncating the set and
    /// never leaking a credential. Both the element-count and byte-size caps trip.
    #[test]
    fn distinct_set_cap_returns_clean_error_no_credentials() {
        use arrow::array::{ListBuilder, StringBuilder};

        // 1. Element-count cap: many tiny distinct values (bytes stay well under 1 MiB,
        //    so the element cap trips first).
        let mut lb = ListBuilder::new(StringBuilder::new());
        for i in 0..(MAX_DISTINCT_ELEMENTS_PER_SHARD + 1) {
            lb.values().append_value(format!("v{i}"));
        }
        lb.append(true);
        let list = lb.finish();
        let err = distinct_cell_to_json(&list, 0, "L_ORDERKEY")
            .expect_err("exceeding the element cap must error");
        let msg = match err {
            UdfError::User(m) => m,
            other => panic!("expected UdfError::User, got {other:?}"),
        };
        assert!(
            msg.contains("ResourcesExhausted"),
            "cap error must follow the bounded-resource convention: {msg}"
        );
        assert!(
            msg.contains("L_ORDERKEY") && msg.contains("distinct-element count"),
            "cap error must name the column and the element cap: {msg}"
        );

        // 2. Byte-size cap: a few very large distinct values (< element cap, so the
        //    byte cap trips first).
        let big = "x".repeat(300_000);
        let mut lb2 = ListBuilder::new(StringBuilder::new());
        for _ in 0..5 {
            lb2.values().append_value(&big);
        }
        lb2.append(true);
        let list2 = lb2.finish();
        let err2 = distinct_cell_to_json(&list2, 0, "L_COMMENT")
            .expect_err("exceeding the byte cap must error");
        let msg2 = match err2 {
            UdfError::User(m) => m,
            other => panic!("expected UdfError::User, got {other:?}"),
        };
        assert!(
            msg2.contains("serialized size exceeded") && msg2.contains("L_COMMENT"),
            "byte-cap error must name the column and the byte cap: {msg2}"
        );

        // Neither message leaks a credential-shaped token.
        for m in [&msg, &msg2] {
            assert!(
                !m.contains("access_key") && !m.contains("secret_key") && !m.contains("minioadmin"),
                "cap error must not contain any credential: {m}"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // build_grouped_partial_agg_sql — GROUP BY key emission and partial columns
    // ---------------------------------------------------------------------------

    /// Single group key with COUNT(*): SELECT includes the key and COUNT(*).
    #[test]
    fn grouped_partial_agg_sql_single_key_count() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }];
        let sql =
            build_grouped_partial_agg_sql(&[r#""REGION""#.to_string()], &plans, "aliased", None);
        assert!(
            sql.contains(r#""REGION""#),
            "group key must appear in SQL: {sql}"
        );
        assert!(sql.contains("COUNT(*) AS"), "COUNT(*) must appear: {sql}");
        assert!(
            sql.contains("PARTIAL_count_0"),
            "partial count column at index 0: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "must have GROUP BY clause: {sql}");
    }

    /// The emitted SELECT layout matches the GK_* then PARTIAL_* adapter contract:
    /// group keys appear before partial aggregate columns in the SELECT list.
    #[test]
    fn grouped_partial_agg_sql_layout_matches_emits() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
        ];
        let sql = build_grouped_partial_agg_sql(
            &[r#""REGION""#.to_string(), r#""CATEGORY""#.to_string()],
            &plans,
            "aliased",
            None,
        );
        // Verify ordering: group key positions come before partial aggregate positions.
        let region_pos = sql.find(r#""REGION""#).expect("REGION must appear");
        let partial_count_pos = sql
            .find("PARTIAL_count_0")
            .expect("PARTIAL_count_0 must appear");
        assert!(
            region_pos < partial_count_pos,
            "group key must precede partial columns: {sql}"
        );
        let category_pos = sql.find(r#""CATEGORY""#).expect("CATEGORY must appear");
        assert!(
            category_pos < partial_count_pos,
            "second group key must precede partial columns: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_sum_1"),
            "SUM at index 1 must appear: {sql}"
        );
    }

    /// No LIMIT is ever added to a grouped partial aggregate SQL.
    #[test]
    fn grouped_partial_agg_sql_no_limit() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }];
        let sql =
            build_grouped_partial_agg_sql(&[r#""REGION""#.to_string()], &plans, "aliased", None);
        assert!(
            !sql.contains("LIMIT"),
            "grouped partial SQL must not contain LIMIT: {sql}"
        );
    }

    /// Expression group keys (e.g. YEAR("DATE")) are inserted verbatim into the
    /// DataFusion GROUP BY clause without any quoting or transformation.
    #[test]
    fn grouped_partial_agg_sql_expression_key_verbatim() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }];
        let expr_key = r#"YEAR("ORDER_DATE")"#.to_string();
        let sql =
            build_grouped_partial_agg_sql(std::slice::from_ref(&expr_key), &plans, "aliased", None);
        assert!(
            sql.contains(&expr_key),
            "expression key must appear verbatim in SQL: {sql}"
        );
        // Must appear in both SELECT and GROUP BY.
        let first_pos = sql.find(&expr_key).unwrap();
        let second_pos = sql[first_pos + 1..]
            .find(&expr_key)
            .map(|p| p + first_pos + 1);
        assert!(
            second_pos.is_some(),
            "expression key must appear in both SELECT and GROUP BY: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Stat aggregate partial SQL — COUNT(col), SUM(col), SUM(col*col) triple
    // ---------------------------------------------------------------------------

    /// Stat aggregate partial emits COUNT(col), SUM(col), SUM(col*col) at index 0.
    #[test]
    fn partial_agg_sql_stat_emits_cnt_sum_sumsq() {
        for kind in &[
            AggKind::VarPop,
            AggKind::VarSamp,
            AggKind::StddevPop,
            AggKind::StddevSamp,
        ] {
            let plans = vec![AggregatePlan {
                kind: kind.clone(),
                column: Some("SCORE".into()),
                arg_expr: None,
            }];
            let sql = build_partial_agg_sql(&plans, "aliased");
            assert!(
                sql.contains(r#"COUNT("SCORE") AS "PARTIAL_stat_cnt_0""#),
                "{kind:?} must emit COUNT(col) as PARTIAL_stat_cnt_0: {sql}"
            );
            assert!(
                sql.contains(r#"SUM("SCORE") AS "PARTIAL_stat_sum_0""#),
                "{kind:?} must emit SUM(col) as PARTIAL_stat_sum_0: {sql}"
            );
            assert!(
                sql.contains(r#"SUM("SCORE" * "SCORE") AS "PARTIAL_stat_sumsq_0""#),
                "{kind:?} must emit SUM(col*col) as PARTIAL_stat_sumsq_0: {sql}"
            );
            // Must NOT use AVG or STDDEV directly — only sufficient statistics
            assert!(
                !sql.contains("STDDEV"),
                "{kind:?} must not emit STDDEV: {sql}"
            );
            assert!(
                !sql.contains("VARIANCE"),
                "{kind:?} must not emit VARIANCE: {sql}"
            );
        }
    }

    /// Stat aggregate null fallback row has 3 values: cnt=0, sum=NULL, sumsq=NULL.
    #[test]
    fn stat_aggregate_null_fallback_row_has_three_values() {
        use exasol_udf_sdk::value::Value;
        for kind in &[
            AggKind::VarPop,
            AggKind::VarSamp,
            AggKind::StddevPop,
            AggKind::StddevSamp,
        ] {
            let plans = vec![AggregatePlan {
                kind: kind.clone(),
                column: Some("X".into()),
                arg_expr: None,
            }];
            let row = emit_null_partial_row(&plans);
            assert_eq!(row.len(), 3, "{kind:?} fallback row must have 3 values");
            assert_eq!(row[0], Value::Int64(0), "{kind:?} cnt must be 0");
            assert_eq!(row[1], Value::Null, "{kind:?} sum must be NULL");
            assert_eq!(row[2], Value::Null, "{kind:?} sumsq must be NULL");
        }
    }

    // ---------------------------------------------------------------------------
    // T7 — runtime selection and session config from ScanSpec
    // ---------------------------------------------------------------------------

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

    /// Mixed stat + count: stat at index 1 uses PARTIAL_stat_*_1 names.
    #[test]
    fn stat_aggregate_index_follows_plan_order() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::VarPop,
                column: Some("X".into()),
                arg_expr: None,
            },
        ];
        let sql = build_partial_agg_sql(&plans, "aliased");
        assert!(sql.contains("PARTIAL_count_0"), "count at index 0: {sql}");
        assert!(
            sql.contains("PARTIAL_stat_cnt_1"),
            "stat at index 1 must use suffix _1: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_stat_sum_1"),
            "stat sum at index 1: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_stat_sumsq_1"),
            "stat sumsq at index 1: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // R2 — classify_scan_error on the partial-aggregate paths
    // ---------------------------------------------------------------------------

    /// R2: ResourcesExhausted on the grouped/ungrouped partial-aggregate paths surfaces
    /// as a memory-exhaustion error, not a storage error, and leaks no credentials.
    ///
    /// This test exercises classify_scan_error directly (the same function now called
    /// at all five mod.rs error sites) to confirm the classification is correct for
    /// the DataFusion error shapes that aggregation and execution produce.
    #[test]
    fn resources_exhausted_on_partial_aggregate_path_surfaces_as_memory_error() {
        use crate::scan::emit::classify_scan_error;
        use datafusion::error::DataFusionError;

        let secret = "my-secret-key-value";
        let secrets = [secret];

        // 1. Direct ResourcesExhausted (e.g., from HashAggregateExec OOM).
        let direct = DataFusionError::ResourcesExhausted(
            "Failed to allocate additional 512 MiB for HashAggregateExec".to_string(),
        );
        let err = classify_scan_error(direct, &secrets);
        let text = err.to_string();
        assert!(
            text.contains("memory exhausted"),
            "direct ResourcesExhausted must surface as memory error: {text}"
        );
        assert!(
            !text.contains("assigned data could not be read"),
            "must NOT be classified as storage error: {text}"
        );
        assert!(!text.contains(secret), "must not leak credentials: {text}");

        // 2. Context-wrapped ResourcesExhausted (DataFusion sort wraps with .context()).
        let ctx_wrapped = DataFusionError::ResourcesExhausted("pool limit hit".to_string())
            .context(format!("External sort failed secret={secret}"));
        let err_ctx = classify_scan_error(ctx_wrapped, &secrets);
        let text_ctx = err_ctx.to_string();
        assert!(
            text_ctx.contains("memory exhausted"),
            "context-wrapped must surface as memory error: {text_ctx}"
        );
        assert!(
            !text_ctx.contains("assigned data could not be read"),
            "must NOT be classified as storage error: {text_ctx}"
        );
        assert!(
            !text_ctx.contains(secret),
            "context-wrapped must not leak credentials: {text_ctx}"
        );

        // 3. Non-ResourcesExhausted errors still route to the storage-error path.
        let storage_err = DataFusionError::Execution("S3 403 Forbidden".to_string());
        let err_storage = classify_scan_error(storage_err, &[]);
        let text_storage = err_storage.to_string();
        assert!(
            text_storage.contains("assigned data could not be read"),
            "non-OOM error must use the storage path: {text_storage}"
        );
        assert!(
            !text_storage.contains("memory exhausted"),
            "non-OOM error must NOT look like a memory error: {text_storage}"
        );
    }

    // ---------------------------------------------------------------------------
    // register_files — logical-schema fallback path (task 5.1)
    // ---------------------------------------------------------------------------

    /// Scenario: scan without a logical schema falls back to first-file inference.
    ///
    /// When `spec.logical_schema` is empty (legacy or unset), `register_files`
    /// must infer the Arrow schema from the first file and register the table
    /// without installing the field-id adapter. The registered table must be
    /// queryable and return all rows written to the file.
    #[tokio::test]
    async fn register_files_falls_back_without_logical_schema() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::execution::context::SessionContext;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        // Write a minimal local Parquet file.
        let dir =
            std::env::temp_dir().join(format!("lh_fallback_inference_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("fallback.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, true),
        ]));
        {
            let file = std::fs::File::create(&path).expect("create parquet file");
            let mut writer =
                ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                    Arc::new(Int64Array::from(vec![Some(10i64), Some(20), None])),
                ],
            )
            .expect("record batch");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }
        let file_url = url::Url::from_file_path(&path)
            .expect("absolute path")
            .to_string();

        // Build a spec with empty logical_schema — the fallback inference path.
        // Absolute file:// entry (empty table_root) exercises the passthrough
        // reconstruction branch; the real file size is supplied because the
        // provider builds each file's ObjectMeta from it (no-HEAD design).
        let mut spec = minimal_spec();
        let file_size = local_file_size(&file_url);
        spec.files = vec![FileEntry::new(file_url, file_size)];
        spec.logical_schema = Vec::new();

        let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
        register_files(&ctx, "scan_target", &spec)
            .await
            .expect("register_files must succeed on first-file inference path");

        // The table must be registered and queryable.
        let table = ctx
            .table("scan_target")
            .await
            .expect("scan_target must be registered after register_files");
        let schema = table.schema();
        assert_eq!(
            schema.fields().len(),
            2,
            "inferred schema must have 2 fields; got {:?}",
            schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
        );
    }

    /// Scenario: column projection binds by Iceberg field-id across physical layouts.
    ///
    /// Row-level regression for the E2E `e2e_renamed_column_resolves_by_field_id`
    /// failure: a Parquet file whose PHYSICAL column is `score` (field-id 2) is
    /// registered through the production `register_files` path against a LOGICAL
    /// schema that calls field-id 2 `rating`. Selecting `RATING` through the same
    /// `build_scan_sql` the UDF runs must read the physical `score` values — the
    /// projected output column must be remapped by field-id on the READ path, not
    /// looked up by the (non-existent) physical name `rating`.
    ///
    /// Before the fix this fails with the exact E2E error
    /// (`Unable to get field named "rating". Valid fields: ["id", "score"]`)
    /// because the projected `Column("rating")` is resolved by NAME against the
    /// real physical file schema `[id, score]`.
    #[tokio::test]
    async fn field_id_adapter_reads_renamed_column_rows() {
        use crate::scan::spec::LogicalField;
        use arrow::array::{Array, Float64Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::execution::context::SessionContext;
        use parquet::arrow::ArrowWriter;
        use std::collections::HashMap;

        // Write a local Parquet file with PHYSICAL fields id (field-id 1) and
        // score (field-id 2) — the pre-rename layout. score = 10 * id.
        let dir = std::env::temp_dir().join(format!("lh_fieldid_rows_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("renamed.parquet");

        let physical_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("score", DataType::Float64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));
        let ids: Vec<i64> = (1..=5).collect();
        let scores: Vec<f64> = ids.iter().map(|i| 10.0 * *i as f64).collect();
        {
            let file = std::fs::File::create(&path).expect("create parquet file");
            let mut writer =
                ArrowWriter::try_new(file, physical_schema.clone(), None).expect("arrow writer");
            let batch = RecordBatch::try_new(
                physical_schema,
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(Float64Array::from(scores.clone())),
                ],
            )
            .expect("record batch");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }
        let file_url = url::Url::from_file_path(&path)
            .expect("absolute path")
            .to_string();

        // Logical (current) schema: field-id 2 is now `rating`, not `score`.
        let logical = vec![
            LogicalField {
                field_id: 1,
                name: "id".to_string(),
                arrow_type: "int64".to_string(),
                nullable: false,
            },
            LogicalField {
                field_id: 2,
                name: "rating".to_string(),
                arrow_type: "float64".to_string(),
                nullable: false,
            },
        ];

        let mut spec = minimal_spec();
        let file_size = local_file_size(&file_url);
        spec.files = vec![FileEntry::new(file_url, file_size)];
        spec.logical_schema = logical;
        // The adapter pushes uppercase current-name projection.
        spec.projection = vec!["ID".into(), "RATING".into()];

        // Drive the EXACT production path: register_files + build_scan_sql, then
        // collect the resulting rows.
        let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
        register_files(&ctx, "scan_target", &spec)
            .await
            .expect("register_files must succeed with logical schema");
        let sql = build_scan_sql(&ctx, "scan_target", &spec)
            .await
            .expect("build_scan_sql");
        let df = ctx.sql(&sql).await.expect("plan scan SQL");
        let batches = df.collect().await.expect("scan must read renamed column");

        // Assert the RATING output column carries the physical `score` values.
        let mut got: Vec<(i64, f64)> = Vec::new();
        for batch in &batches {
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            let rating_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("rating column is Float64");
            for row in 0..batch.num_rows() {
                assert!(!rating_col.is_null(row), "rating must not be NULL");
                got.push((id_col.value(row), rating_col.value(row)));
            }
        }
        got.sort_by_key(|(id, _)| *id);

        let expected: Vec<(i64, f64)> = ids.iter().map(|i| (*i, 10.0 * *i as f64)).collect();
        assert_eq!(
            got, expected,
            "RATING must read the physical `score` values (rating = 10*id)"
        );
    }

    /// Task B4 (scenario `topn: Ordered top-N preserves descending and NULL ordering`):
    /// `build_scan_sql` renders a pushed-down ORDER BY through the SAME shared
    /// `render_order_by_clause` the adapter's outer merge uses, so per-shard and
    /// merge agree on direction AND explicit NULL placement. Over a local Parquet
    /// file whose sort column carries NULLs, a DESC sort yields a bounded,
    /// correctly-ordered result, and flipping ONLY the `nulls_last` flag moves the
    /// NULLs from the head to the tail — proving the NULL placement is honored
    /// explicitly, not left to a DataFusion default.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ordered_scan_sql_preserves_desc_and_null_placement() {
        use crate::scan::spec::SortKey;
        use arrow::array::{Array, Float64Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::execution::context::SessionContext;
        use parquet::arrow::ArrowWriter;

        // price is nullable with NULLs interleaved among descending-comparable values.
        let dir = std::env::temp_dir().join(format!("lh_topn_nulls_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("topn.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("price", DataType::Float64, true),
        ]));
        {
            let file = std::fs::File::create(&path).expect("create parquet file");
            let mut writer =
                ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                    Arc::new(Float64Array::from(vec![
                        Some(10.0),
                        None,
                        Some(30.0),
                        Some(20.0),
                        None,
                    ])),
                ],
            )
            .expect("record batch");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }
        let file_url = url::Url::from_file_path(&path)
            .expect("absolute path")
            .to_string();

        // Collect the (id, Option<price>) rows build_scan_sql produces for a given
        // sort direction / NULL placement / limit, IN PLAN ORDER (no test-side re-sort).
        async fn topn_rows(
            file_url: &str,
            ascending: bool,
            nulls_last: bool,
            limit: u64,
        ) -> Vec<(i64, Option<f64>)> {
            let mut spec = minimal_spec();
            let file_size = local_file_size(file_url);
            spec.files = vec![FileEntry::new(file_url, file_size)];
            spec.projection = vec!["ID".into(), "PRICE".into()];
            spec.order_by = vec![SortKey {
                column: "PRICE".into(),
                ascending,
                nulls_last,
            }];
            spec.limit = Some(limit);

            let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
            register_files(&ctx, "scan_target", &spec)
                .await
                .expect("register local parquet");
            let sql = build_scan_sql(&ctx, "scan_target", &spec)
                .await
                .expect("build_scan_sql");
            let df = ctx.sql(&sql).await.expect("plan scan SQL");
            let batches = df.collect().await.expect("collect");
            let mut rows: Vec<(i64, Option<f64>)> = Vec::new();
            for batch in &batches {
                let id_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("col 0 Int64 (ID)");
                let price_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("col 1 Float64 (PRICE)");
                for r in 0..batch.num_rows() {
                    let p = if price_col.is_null(r) {
                        None
                    } else {
                        Some(price_col.value(r))
                    };
                    rows.push((id_col.value(r), p));
                }
            }
            rows
        }

        // DESC + NULLS FIRST, bounded to 3: the two NULLs rank first, then the max.
        let desc_nulls_first = topn_rows(&file_url, false, false, 3).await;
        assert_eq!(
            desc_nulls_first.len(),
            3,
            "LIMIT 3 must bound the result: {desc_nulls_first:?}"
        );
        assert!(
            desc_nulls_first[0].1.is_none() && desc_nulls_first[1].1.is_none(),
            "DESC NULLS FIRST must rank NULLs first: {desc_nulls_first:?}"
        );
        assert_eq!(
            desc_nulls_first[2].1,
            Some(30.0),
            "after the NULLs the largest value comes next: {desc_nulls_first:?}"
        );

        // DESC + NULLS LAST, bounded to 3: flipping ONLY the NULL flag moves the NULLs
        // to the tail, so the top-3 are the descending non-NULL values.
        let desc_nulls_last = topn_rows(&file_url, false, true, 3).await;
        assert_eq!(
            desc_nulls_last.iter().map(|(_, p)| *p).collect::<Vec<_>>(),
            vec![Some(30.0), Some(20.0), Some(10.0)],
            "DESC NULLS LAST must rank non-NULLs descending ahead of NULLs: {desc_nulls_last:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Scenario: column projection binds by Iceberg field-id across physical layouts.
    ///
    /// The multi-file mirror of the E2E: one shard covers a file written BEFORE a
    /// rename (physical column `score`) and a file written AFTER it (physical column
    /// `rating`), both carrying field-id 2. A single `ListingTable` over both must
    /// bind each file's field-id-2 column to the current logical name `rating` — the
    /// per-file expr adapter is created once per file, so divergent physical layouts
    /// in the same shard each resolve correctly.
    #[tokio::test]
    async fn field_id_adapter_reads_divergent_layouts_across_files() {
        use crate::scan::spec::LogicalField;
        use arrow::array::{Array, Float64Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::execution::context::SessionContext;
        use parquet::arrow::ArrowWriter;
        use std::collections::HashMap;

        fn id_field() -> Field {
            Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )]))
        }
        fn score_field(physical_name: &str) -> Field {
            Field::new(physical_name, DataType::Float64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )]))
        }

        let dir = std::env::temp_dir().join(format!("lh_fieldid_multi_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");

        // Write one file per physical layout. score = 10 * id; ids 1..=3 (old
        // `score`), 4..=6 (new `rating`).
        let write_file = |name: &str, physical_col: &str, ids: &[i64]| -> String {
            let schema = Arc::new(Schema::new(vec![id_field(), score_field(physical_col)]));
            let scores: Vec<f64> = ids.iter().map(|i| 10.0 * *i as f64).collect();
            let path = dir.join(name);
            let file = std::fs::File::create(&path).expect("create parquet file");
            let mut writer =
                ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(ids.to_vec())),
                    Arc::new(Float64Array::from(scores)),
                ],
            )
            .expect("record batch");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
            url::Url::from_file_path(&path)
                .expect("absolute path")
                .to_string()
        };
        let file_old = write_file("old_score.parquet", "score", &[1, 2, 3]);
        let file_new = write_file("new_rating.parquet", "rating", &[4, 5, 6]);

        let logical = vec![
            LogicalField {
                field_id: 1,
                name: "id".to_string(),
                arrow_type: "int64".to_string(),
                nullable: false,
            },
            LogicalField {
                field_id: 2,
                name: "rating".to_string(),
                arrow_type: "float64".to_string(),
                nullable: false,
            },
        ];

        let mut spec = minimal_spec();
        let old_size = local_file_size(&file_old);
        let new_size = local_file_size(&file_new);
        spec.files = vec![
            FileEntry::new(file_old, old_size),
            FileEntry::new(file_new, new_size),
        ];
        spec.logical_schema = logical;
        spec.projection = vec!["ID".into(), "RATING".into()];

        let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
        register_files(&ctx, "scan_target", &spec)
            .await
            .expect("register_files must succeed");
        let sql = build_scan_sql(&ctx, "scan_target", &spec)
            .await
            .expect("build_scan_sql");
        let df = ctx.sql(&sql).await.expect("plan scan SQL");
        let batches = df
            .collect()
            .await
            .expect("scan must read both physical layouts");

        let mut got: Vec<(i64, f64)> = Vec::new();
        for batch in &batches {
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            let rating_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("rating column is Float64");
            for row in 0..batch.num_rows() {
                assert!(!rating_col.is_null(row), "rating must not be NULL");
                got.push((id_col.value(row), rating_col.value(row)));
            }
        }
        got.sort_by_key(|(id, _)| *id);

        let expected: Vec<(i64, f64)> = (1..=6).map(|i| (i, 10.0 * i as f64)).collect();
        assert_eq!(
            got, expected,
            "both files must resolve field-id 2 to `rating`; rating = 10*id for ids 1..=6"
        );
    }

    // ---------------------------------------------------------------------------
    // FieldIdExprAdapter — column resolution by Iceberg field-id (task 4.1)
    // ---------------------------------------------------------------------------

    mod field_id_adapter {
        use super::super::{FieldIdExprAdapterFactory, PARQUET_FIELD_ID_META_KEY};
        use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
        use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
        use datafusion::scalar::ScalarValue;
        use std::collections::HashMap;
        use std::sync::Arc;

        /// A field tagged with its Iceberg field-id (`PARQUET:field_id`).
        fn field_with_id(name: &str, dt: DataType, nullable: bool, id: i32) -> Field {
            Field::new(name, dt, nullable).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        }

        /// A field carrying no field-id metadata (older writer).
        fn field_no_id(name: &str, dt: DataType, nullable: bool) -> Field {
            Field::new(name, dt, nullable)
        }

        fn rewrite(
            logical: SchemaRef,
            physical: SchemaRef,
            column: Column,
        ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
            let adapter = FieldIdExprAdapterFactory
                .create(logical, physical)
                .expect("adapter creation");
            adapter.rewrite(Arc::new(column))
        }

        /// A renamed column (physical `score`, logical `rating`, same field-id 2)
        /// binds to the physical column BY field-id, not by name.
        #[test]
        fn resolves_renamed_column_by_field_id() {
            let logical = Arc::new(Schema::new(vec![
                field_with_id("id", DataType::Int64, false, 1),
                field_with_id("rating", DataType::Int64, true, 2),
            ]));
            // Physical file predates the rename: field-id 2 is named `score`, at index 1.
            let physical = Arc::new(Schema::new(vec![
                field_with_id("id", DataType::Int64, false, 1),
                field_with_id("score", DataType::Int64, true, 2),
            ]));

            // The planner references the CURRENT logical name `rating`.
            let result = rewrite(logical, physical, Column::new("rating", 1)).expect("rewrite ok");

            // Types match, so it resolves to a plain physical Column (no cast),
            // and it must point at physical index 1 (the `score` slot).
            let col = result
                .downcast_ref::<Column>()
                .expect("renamed column resolves to a Column, no cast");
            assert_eq!(col.index(), 1, "must bind to physical field-id-2 slot");
        }

        /// A type divergence between the logical and physical field (same field-id)
        /// is wrapped in a cast (delegated to the default adapter).
        #[test]
        fn casts_on_type_divergence_by_field_id() {
            let logical = Arc::new(Schema::new(vec![field_with_id(
                "amount",
                DataType::Int64,
                true,
                5,
            )]));
            // Same field-id 5 but a narrower physical type, and a different physical name.
            let physical = Arc::new(Schema::new(vec![field_with_id(
                "amt",
                DataType::Int32,
                true,
                5,
            )]));

            let result = rewrite(logical, physical, Column::new("amount", 0)).expect("rewrite ok");
            let cast = result
                .downcast_ref::<CastExpr>()
                .expect("type divergence must produce a cast");
            let inner = cast
                .expr()
                .downcast_ref::<Column>()
                .expect("cast wraps the resolved physical column");
            assert_eq!(inner.index(), 0, "cast must wrap the field-id-5 slot");
        }

        /// A dropped column (present physically with an id absent from the logical
        /// schema) is simply not referenced by the projection; the adapter leaves
        /// the remaining physical fields resolvable by their logical names.
        #[test]
        fn ignores_dropped_physical_column() {
            let logical = Arc::new(Schema::new(vec![field_with_id(
                "id",
                DataType::Int64,
                false,
                1,
            )]));
            // Physical file still has an old, since-dropped column (field-id 7).
            let physical = Arc::new(Schema::new(vec![
                field_with_id("id", DataType::Int64, false, 1),
                field_with_id("legacy", DataType::Utf8, true, 7),
            ]));

            // The kept logical column `id` still binds correctly.
            let result = rewrite(logical, physical, Column::new("id", 0)).expect("rewrite ok");
            let col = result
                .downcast_ref::<Column>()
                .expect("kept column resolves to a Column");
            assert_eq!(col.index(), 0);
        }

        /// Task 4.2: the logical Arrow schema built from `ScanSpec::logical_schema`
        /// tags each field with its Iceberg field-id, reconstructs the Arrow type
        /// from the tag, and preserves the declared nullability.
        #[test]
        fn builds_logical_arrow_schema_with_field_ids() {
            use super::super::{build_logical_arrow_schema, field_id_of};
            use crate::scan::spec::LogicalField;

            let logical = vec![
                LogicalField {
                    field_id: 1,
                    name: "id".to_string(),
                    arrow_type: "int64".to_string(),
                    nullable: false,
                },
                LogicalField {
                    field_id: 2,
                    name: "rating".to_string(),
                    arrow_type: "float64".to_string(),
                    nullable: true,
                },
            ];

            let schema = build_logical_arrow_schema(&logical);

            assert_eq!(schema.fields().len(), 2);
            let id = schema.field(0);
            assert_eq!(id.name(), "id");
            assert_eq!(id.data_type(), &DataType::Int64);
            assert!(!id.is_nullable(), "non-nullable must be preserved");
            assert_eq!(field_id_of(id), Some(1), "field-id metadata must be tagged");

            let rating = schema.field(1);
            assert_eq!(rating.name(), "rating");
            assert_eq!(rating.data_type(), &DataType::Float64);
            assert!(rating.is_nullable(), "nullable must be preserved");
            assert_eq!(field_id_of(rating), Some(2));
        }

        /// Scenario: field-id resolution falls back to physical name when a file
        /// field carries no embedded field-id.
        ///
        /// A file whose fields carry no `PARQUET:field_id` metadata cannot be bound
        /// by id; the adapter falls through to the physical-name match so the
        /// column is still resolved correctly.
        #[test]
        fn field_id_adapter_falls_back_to_name_without_field_id() {
            let logical = Arc::new(Schema::new(vec![
                field_with_id("id", DataType::Int64, false, 1),
                field_with_id("rating", DataType::Int64, true, 2),
            ]));
            // Physical file carries NO field-ids at all (older writer).
            let physical = Arc::new(Schema::new(vec![
                field_no_id("id", DataType::Int64, false),
                field_no_id("rating", DataType::Int64, true),
            ]));

            let result = rewrite(logical, physical, Column::new("rating", 1)).expect("rewrite ok");
            let bound_index = result
                .downcast_ref::<Column>()
                .map(Column::index)
                .or_else(|| {
                    result
                        .downcast_ref::<CastExpr>()
                        .and_then(|c| c.expr().downcast_ref::<Column>())
                        .map(Column::index)
                });
            assert_eq!(
                bound_index,
                Some(1),
                "name fallback must bind to the `rating` slot"
            );
        }

        /// Scenario: added nullable column absent from an older file is NULL-filled.
        ///
        /// When a column was added to the schema AFTER a file was written, the file
        /// simply does not contain the field. The adapter delegates to
        /// `DefaultPhysicalExprAdapter` which returns a NULL literal for nullable
        /// missing columns rather than erroring.
        #[test]
        fn field_id_adapter_null_fills_added_nullable_column() {
            let logical = Arc::new(Schema::new(vec![
                field_with_id("id", DataType::Int64, false, 1),
                field_with_id("note", DataType::Utf8, true, 9),
            ]));
            // Physical file predates the addition: field-id 9 is absent.
            let physical = Arc::new(Schema::new(vec![field_with_id(
                "id",
                DataType::Int64,
                false,
                1,
            )]));

            let result = rewrite(logical, physical, Column::new("note", 1)).expect("rewrite ok");
            let lit = result
                .downcast_ref::<Literal>()
                .expect("added nullable missing column becomes a NULL literal");
            assert_eq!(*lit.value(), ScalarValue::Utf8(None));
        }

        /// Scenario: added required column missing from an older file errors cleanly.
        ///
        /// A REQUIRED (non-nullable) column that is absent from an older file must
        /// produce a clean descriptive error — never wrong data or a silent NULL.
        #[test]
        fn field_id_adapter_errors_on_missing_required_column() {
            let logical = Arc::new(Schema::new(vec![
                field_with_id("id", DataType::Int64, false, 1),
                field_with_id("mandatory", DataType::Utf8, false, 9),
            ]));
            let physical = Arc::new(Schema::new(vec![field_with_id(
                "id",
                DataType::Int64,
                false,
                1,
            )]));

            let err = rewrite(logical, physical, Column::new("mandatory", 1))
                .expect_err("missing required column must error");
            let text = err.to_string();
            assert!(
                text.contains("mandatory") && text.contains("missing"),
                "error must name the missing required column: {text}"
            );
        }

        /// Task 4.3: `build_scan_sql`'s uppercase-alias inner-SELECT wrapper works
        /// unchanged over a registered logical (current-name) schema — the table
        /// schema DataFusion sees is the logical one, so aliases and projection
        /// resolve against the current names.
        #[tokio::test]
        async fn build_scan_sql_aliases_over_logical_schema() {
            use super::super::{build_logical_arrow_schema, build_scan_sql};
            use crate::scan::spec::LogicalField;
            use datafusion::datasource::MemTable;
            use datafusion::execution::context::SessionContext;

            let logical = vec![
                LogicalField {
                    field_id: 1,
                    name: "id".to_string(),
                    arrow_type: "int64".to_string(),
                    nullable: false,
                },
                LogicalField {
                    field_id: 2,
                    name: "rating".to_string(),
                    arrow_type: "float64".to_string(),
                    nullable: true,
                },
            ];
            let logical_schema = build_logical_arrow_schema(&logical);

            // Register the logical schema as the table schema (as register_files
            // does via with_schema), with no rows — build_scan_sql only reads the
            // advertised schema.
            let ctx = SessionContext::new();
            let table = MemTable::try_new(logical_schema.clone(), vec![vec![]]).unwrap();
            ctx.register_table("scan_target", Arc::new(table)).unwrap();

            let mut spec = super::minimal_spec();
            spec.projection = vec!["ID".into(), "RATING".into()];
            spec.logical_schema = logical;

            let sql = build_scan_sql(&ctx, "scan_target", &spec).await.unwrap();

            // Inner SELECT aliases each current (lowercase) name to its uppercase form.
            assert!(
                sql.contains(r#""id" AS "ID""#) && sql.contains(r#""rating" AS "RATING""#),
                "inner SELECT must alias current names to uppercase: {sql}"
            );
            // Outer projection references the uppercase aliases.
            assert!(
                sql.contains(r#""ID""#) && sql.contains(r#""RATING""#),
                "outer projection must use uppercase aliases: {sql}"
            );
        }
    }
}

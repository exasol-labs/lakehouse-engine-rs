//! Node-local partial-aggregate scan paths: register the shard's assigned files,
//! run a single-group or grouped partial aggregate in DataFusion, and emit the
//! per-shard partial rows the Exasol outer wrapper re-aggregates.
//!
//! Both paths share the uppercase-aliased inner-SELECT seam
//! (`register_aliased_scan_target`) and the COLUMN CONTRACT SQL builders
//! (`build_partial_agg_sql_filtered` / `build_grouped_partial_agg_sql`).

use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use futures::StreamExt;

use crate::scan::convert::arrow_value_at;
use crate::scan::emit::classify_scan_error;
use crate::scan::spec::{AggregatePlan, PartialAggColumn, ScanSpec, partial_column_name};

use super::raw_scan::register_files;
use super::sql_support::{build_alias_items, quote_ident};

/// Register the shard's assigned files as `scan_target` and build the
/// uppercase-aliased inner-SELECT subquery both partial-aggregate paths wrap.
///
/// Wrapping the registered table in `SELECT <uppercase aliases> FROM scan_target`
/// makes every aggregate argument and group-key expression reference the
/// uppercase, Exasol-facing column names — the same seam the single-table and
/// join paths use. The returned string is spliced verbatim as the inner table of
/// the partial-aggregate SQL, so its exact text is part of the emitted SQL.
async fn register_aliased_scan_target(
    session_ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<String, UdfError> {
    // Register the assigned files so we can query them.
    let table_name = "scan_target";
    register_files(session_ctx, table_name, spec).await?;

    // Build the alias inner SELECT (uppercase column names).
    let table = session_ctx
        .table(table_name)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered table: {e}")))?;
    let alias_items = build_alias_items(table.schema());
    Ok(format!(
        "SELECT {} FROM {table_name}",
        alias_items.join(", ")
    ))
}

/// Run a node-local partial aggregate and emit exactly one row per shard.
///
/// Dispatches to `run_grouped_partial_aggregate` when the spec carries non-empty
/// `group_keys`; otherwise executes the single-group (ungrouped) path which
/// always emits exactly one partial-aggregate row.
///
/// The column layout follows the COLUMN CONTRACT (see `build_partial_agg_sql`
/// and `build_grouped_partial_agg_sql`).
pub(super) async fn run_partial_aggregate(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<(), UdfError> {
    // Dispatch: grouped path when group_keys is Some and non-empty.
    if let Some(group_keys) = &spec.common.group_keys
        && !group_keys.is_empty()
    {
        return run_grouped_partial_aggregate(ctx, session_ctx, spec).await;
    }

    let secrets = spec.common.storage.secret_values();
    let aggregates = spec
        .common
        .aggregates
        .as_deref()
        .expect("run_partial_aggregate called without aggregates");

    let aliased_table = register_aliased_scan_target(session_ctx, spec).await?;

    let sql =
        build_partial_agg_sql_filtered(aggregates, &aliased_table, spec.common.filter.as_deref());

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
    // for a well-formed aggregate), emit a row of NULLs.
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
    let secrets = spec.common.storage.secret_values();
    let group_keys = spec
        .common
        .group_keys
        .as_deref()
        .expect("run_grouped_partial_aggregate called without group_keys");
    let aggregates = spec
        .common
        .aggregates
        .as_deref()
        .expect("run_grouped_partial_aggregate called without aggregates");

    let aliased_table = register_aliased_scan_target(session_ctx, spec).await?;

    let sql = build_grouped_partial_agg_sql(
        group_keys,
        aggregates,
        &aliased_table,
        spec.common.filter.as_deref(),
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
                let raw = arrow_value_at(batch.column(col_idx), row_idx)?;
                // Stringify for GK_i VARCHAR(2000000) column.
                // Value has no Display; format each variant explicitly.
                let gk_str = value_to_gk_string(raw);
                row_values.push(gk_str);
            }

            // Partial aggregate columns follow.
            for col_idx in n_group_keys..batch.num_columns() {
                row_values.push(arrow_value_at(batch.column(col_idx), row_idx)?);
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
/// A counter column contributes `0` — a shard that matched no rows legitimately
/// counted none — and a value column contributes NULL, because it has no value at
/// all. [`PartialAggColumn::is_counter`] owns which is which, and
/// [`crate::scan::spec::AggKind::partial_columns`] owns the row's length and order:
/// that ordering IS the row's whole contract, since the Exasol outer wrapper
/// addresses these values positionally.
fn emit_null_partial_row(aggregates: &[AggregatePlan]) -> Vec<exasol_udf_sdk::value::Value> {
    use exasol_udf_sdk::value::Value;
    aggregates
        .iter()
        .flat_map(|plan| plan.kind.partial_columns())
        .map(|col| {
            if col.is_counter() {
                Value::Int64(0)
            } else {
                Value::Null
            }
        })
        .collect()
}

/// Test-only no-filter wrapper over `build_partial_agg_sql_filtered`
/// (`filter = None`); used by the partial-aggregate SQL-builder unit tests.
#[cfg(test)]
pub fn build_partial_agg_sql(aggregates: &[AggregatePlan], aliased_table: &str) -> String {
    build_partial_agg_sql_filtered(aggregates, aliased_table, None)
}

/// Build the partial-aggregate SQL, optionally with a WHERE clause.
///
/// COLUMN CONTRACT: iterating `aggregates` in order, each plan item at index `i`
/// contributes the columns [`crate::scan::spec::AggKind::partial_columns`] lists for
/// its kind, named by [`partial_column_name`] at that index — the single owner of
/// both the column count and the column name. For the Exasol type each column is
/// received as, defer to `partial_emits_items` in `adapter::pushdown`: this
/// DataFusion SELECT list produces the values, the EMITS clause declares the types.
///
/// The scan UDF aggregate SELECT list, the EMITS clause in the fan-out SQL, and
/// the outer merge SELECT MUST all agree on this order and column count.
///
/// `aliased_table` is a subquery string: `SELECT ... FROM scan_target` with
/// uppercase aliases already applied.
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
///
/// A plan reaching this helper with neither `arg_expr` nor `column` set is a
/// malformed non-COUNT aggregate (COUNT(*) never calls here). Rather than emit
/// an empty `""` identifier — which DataFusion rejects with an opaque
/// `column "" not found` — fall back to a self-describing sentinel so the error
/// names the actual defect.
fn agg_arg_sql(plan: &AggregatePlan) -> String {
    /// Sentinel identifier for a malformed aggregate plan missing both its
    /// rendered expression and its bare column name.
    const MISSING_AGG_ARG: &str = "__MISSING_AGG_ARGUMENT__";
    match plan.arg_expr.as_deref() {
        Some(expr) => expr.to_string(),
        None => quote_ident(plan.column.as_deref().unwrap_or(MISSING_AGG_ARG)),
    }
}

/// Produce the SELECT list items for one aggregate plan entry at index `i`.
///
/// [`crate::scan::spec::AggKind::partial_columns`] owns which columns exist and in
/// what order, and [`partial_column_name`] owns what each is called; this function
/// owns only each column's DataFusion aggregate expression. Every argument comes
/// from [`agg_arg_sql`], so a rendered expression argument is substituted verbatim
/// wherever a bare column would be. The counting columns use `COUNT(<arg>)` rather
/// than `COUNT(*)` so NULLs are excluded, matching single-node AVG and
/// STDDEV/VARIANCE semantics.
fn partial_select_items(plan: &AggregatePlan, i: usize) -> Vec<String> {
    plan.kind
        .partial_columns()
        .iter()
        .map(|col| {
            let expr = match col {
                PartialAggColumn::CountStar => "COUNT(*)".to_string(),
                PartialAggColumn::CountArg
                | PartialAggColumn::AvgCnt
                | PartialAggColumn::StatCnt => format!("COUNT({})", agg_arg_sql(plan)),
                PartialAggColumn::Sum | PartialAggColumn::AvgSum | PartialAggColumn::StatSum => {
                    format!("SUM({})", agg_arg_sql(plan))
                }
                PartialAggColumn::Min => format!("MIN({})", agg_arg_sql(plan)),
                PartialAggColumn::Max => format!("MAX({})", agg_arg_sql(plan)),
                PartialAggColumn::StatSumSq => {
                    let arg = agg_arg_sql(plan);
                    format!("SUM({arg} * {arg})")
                }
            };
            let name = partial_column_name(*col, i);
            format!(r#"{expr} AS "{name}""#)
        })
        .collect()
}

/// Convert the single-group partial-aggregate result row (row 0 of `batch`) into
/// the ordered `Value` row emitted for this shard.
///
/// Walks `aggregates` in the COLUMN CONTRACT order, consuming exactly
/// `partial_columns().len()` batch columns per aggregate — the count read from the
/// one owner rather than re-derived here, because [`partial_select_items`] produced
/// the batch from that same owner and a divergence would silently shift every later
/// aggregate's value. Each column converts straight through [`arrow_value_at`].
fn partial_row_from_batch(
    aggregates: &[AggregatePlan],
    batch: &arrow::record_batch::RecordBatch,
) -> Result<Vec<Value>, UdfError> {
    let mut row: Vec<Value> = Vec::with_capacity(batch.num_columns());
    let mut col = 0usize;
    for plan in aggregates {
        let width = plan.kind.partial_columns().len();
        for c in col..col + width {
            row.push(arrow_value_at(batch.column(c), 0)?);
        }
        col += width;
    }
    Ok(row)
}

#[cfg(test)]
#[path = "partial_agg_tests.rs"]
mod tests;

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
use crate::scan::spec::{AggKind, AggregatePlan, ScanSpec};

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

/// Test-only no-filter wrapper over `build_partial_agg_sql_filtered`
/// (`filter = None`); used by the partial-aggregate SQL-builder unit tests.
#[cfg(test)]
pub fn build_partial_agg_sql(aggregates: &[AggregatePlan], aliased_table: &str) -> String {
    build_partial_agg_sql_filtered(aggregates, aliased_table, None)
}

/// Build the partial-aggregate SQL, optionally with a WHERE clause.
///
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

/// Convert the single-group partial-aggregate result row (row 0 of `batch`) into
/// the ordered `Value` row emitted for this shard.
///
/// Walks `aggregates` in the COLUMN CONTRACT order, consuming the exact number of
/// batch columns each aggregate produced in [`partial_select_items`], converting
/// each column straight through [`arrow_value_at`].
fn partial_row_from_batch(
    aggregates: &[AggregatePlan],
    batch: &arrow::record_batch::RecordBatch,
) -> Result<Vec<Value>, UdfError> {
    let mut row: Vec<Value> = Vec::with_capacity(batch.num_columns());
    let mut col = 0usize;
    for plan in aggregates {
        match plan.kind {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}

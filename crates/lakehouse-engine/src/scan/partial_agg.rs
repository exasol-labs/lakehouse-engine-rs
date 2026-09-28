use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{ColumnInfo, Decimal, Value};
use futures::StreamExt;

use crate::scan::convert::arrow_value_at;
use crate::scan::emit::{
    check_declared_arity, classify_scan_error, coerce_column, declared_output_columns,
    target_arrow_type,
};
use crate::scan::spec::{AggregatePlan, PartialAggColumn, ScanSpec, partial_column_name};
use crate::scan::storage_ref::ResolvedScanStorage;

use super::raw_scan::register_files;
use super::sql_support::{build_alias_items, quote_ident};

/// The returned string is spliced verbatim as the inner table, so its text is part of the emitted SQL.
async fn register_aliased_scan_target(
    session_ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
) -> Result<String, UdfError> {
    let table_name = "scan_target";
    register_files(session_ctx, table_name, spec, storage).await?;

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

/// Emits exactly one row per shard on the single-group path.
pub(super) async fn run_partial_aggregate(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
) -> Result<(), UdfError> {
    if let Some(group_keys) = &spec.common.group_keys
        && !group_keys.is_empty()
    {
        return run_grouped_partial_aggregate(ctx, session_ctx, spec, storage).await;
    }

    let secrets = storage.all_secret_values();
    let aggregates = spec
        .common
        .aggregates
        .as_deref()
        .expect("run_partial_aggregate called without aggregates");

    let aliased_table = register_aliased_scan_target(session_ctx, spec, storage).await?;

    let sql =
        build_partial_agg_sql_filtered(aggregates, &aliased_table, spec.common.filter.as_deref());

    let df = session_ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("partial aggregate SQL error: {e}")))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| classify_scan_error(e, &secrets))?;

    // Both arms emit into the same EMITS clause, so the declaration is read once.
    let declared = declared_output_columns(ctx)?;

    // An aggregate always yields one row, even over an empty table; the NULL row is a backstop.
    let row = match batches.first() {
        Some(batch) if batch.num_rows() > 0 => {
            partial_row_from_batch(aggregates, batch, &declared)?
        }
        _ => emit_null_partial_row(aggregates, &declared)?,
    };

    ctx.emit(row)?;
    Ok(())
}

/// Group keys are emitted as `Value::String`, since the adapter declares every GK column
/// `VARCHAR(2000000)`. An empty shard emits zero rows, not a null fallback row: the outer
/// wrapper re-groups partials from all shards.
async fn run_grouped_partial_aggregate(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
) -> Result<(), UdfError> {
    let secrets = storage.all_secret_values();
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

    let aliased_table = register_aliased_scan_target(session_ctx, spec, storage).await?;

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

    let mut stream = df
        .execute_stream()
        .await
        .map_err(|e| classify_scan_error(e, &secrets))?;

    let n_group_keys = group_keys.len();
    let declared = declared_output_columns(ctx)?;

    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| classify_scan_error(e, &secrets))?;
        // Group keys pass through uncoerced so their merge identity is unchanged.
        let columns = coerce_partial_agg_columns(&batch, &declared, n_group_keys)?;

        for row_idx in 0..batch.num_rows() {
            let mut row_values: Vec<Value> = Vec::with_capacity(columns.len());

            for column in columns.iter().take(n_group_keys) {
                let raw = arrow_value_at(column.as_ref(), row_idx)?;
                let gk_str = value_to_gk_string(raw);
                row_values.push(gk_str);
            }

            for column in columns.iter().skip(n_group_keys) {
                row_values.push(arrow_value_at(column.as_ref(), row_idx)?);
            }

            ctx.emit(row_values)?;
        }
        // Never hold two batches at once.
        drop(columns);
        drop(batch);
    }

    Ok(())
}

/// Group-key expressions are already-rendered DataFusion fragments, inserted verbatim. No LIMIT:
/// the outer wrapper applies it after re-grouping.
pub fn build_grouped_partial_agg_sql(
    group_keys: &[String],
    aggregates: &[AggregatePlan],
    aliased_table: &str,
    filter: Option<&str>,
) -> String {
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

    sql.push_str(" GROUP BY ");
    sql.push_str(&group_keys.join(", "));

    sql
}

/// NULL group keys stay NULL so the outer wrapper groups them together.
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

/// A counter column contributes `0` (the shard counted none) and a value column NULL. The order
/// from [`crate::scan::spec::AggKind::partial_columns`] is the whole contract, since the outer
/// wrapper addresses values positionally. `declared` is shared with the populated arm so both
/// emit the same `Value` variants.
fn emit_null_partial_row(
    aggregates: &[AggregatePlan],
    declared: &[ColumnInfo],
) -> Result<Vec<Value>, UdfError> {
    let columns: Vec<PartialAggColumn> = aggregates
        .iter()
        .flat_map(|plan| plan.kind.partial_columns())
        .copied()
        .collect();
    check_declared_arity(declared, columns.len())?;
    columns
        .iter()
        .zip(declared)
        .map(|(col, declared)| {
            if col.is_counter() {
                counter_zero(declared)
            } else {
                Ok(Value::Null)
            }
        })
        .collect()
}

/// Uses the same [`target_arrow_type`] resolution as the populated arm, so a zero and a real
/// count cannot reach the wire as different variants of one column.
fn counter_zero(declared: &ColumnInfo) -> Result<Value, UdfError> {
    let target = target_arrow_type(declared)?;
    match target {
        DataType::Int32 => Ok(Value::Int32(0)),
        DataType::Int64 => Ok(Value::Int64(0)),
        DataType::Float64 => Ok(Value::Double(0.0)),
        DataType::Decimal128(_, scale) => {
            let scale = u8::try_from(scale).map_err(|_| {
                UdfError::User(format!(
                    "emit failed: counter column {} declares scale {scale}, which is not a decimal scale",
                    declared.name
                ))
            })?;
            Ok(Value::Numeric(Decimal { unscaled: 0, scale }))
        }
        other => Err(UdfError::User(format!(
            "emit failed: counter column {} is declared {other:?}, which cannot carry a row count",
            declared.name
        ))),
    }
}

/// COLUMN CONTRACT: each plan at index `i` contributes the columns
/// [`crate::scan::spec::AggKind::partial_columns`] lists, named by [`partial_column_name`]. The
/// scan SELECT list, the fan-out EMITS clause (`partial_emits_items` in `adapter::pushdown`), and
/// the outer merge SELECT MUST agree on this order and count.
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

/// A rendered `arg_expr` is substituted VERBATIM, never re-quoted. A plan with neither
/// `arg_expr` nor `column` gets a self-describing sentinel instead of `""`, which DataFusion
/// would reject with an opaque `column "" not found`.
fn agg_arg_sql(plan: &AggregatePlan) -> String {
    const MISSING_AGG_ARG: &str = "__MISSING_AGG_ARGUMENT__";
    match plan.arg_expr.as_deref() {
        Some(expr) => expr.to_string(),
        None => quote_ident(plan.column.as_deref().unwrap_or(MISSING_AGG_ARG)),
    }
}

/// Counting columns use `COUNT(<arg>)`, not `COUNT(*)`, so NULLs are excluded as in single-node
/// AVG and STDDEV/VARIANCE.
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

/// Column widths come from `partial_columns()`, the same owner [`partial_select_items`] used; a
/// re-derived count could silently shift every later aggregate's value.
fn partial_row_from_batch(
    aggregates: &[AggregatePlan],
    batch: &arrow::record_batch::RecordBatch,
    declared: &[ColumnInfo],
) -> Result<Vec<Value>, UdfError> {
    let columns = coerce_partial_agg_columns(batch, declared, 0)?;
    let mut row: Vec<Value> = Vec::with_capacity(columns.len());
    let mut col = 0usize;
    for plan in aggregates {
        let width = plan.kind.partial_columns().len();
        for column in columns.iter().skip(col).take(width) {
            row.push(arrow_value_at(column.as_ref(), 0)?);
        }
        col += width;
    }
    Ok(row)
}

/// Columns before `first_agg_column` (grouped-path keys) are returned untouched: an Arrow cast
/// to `Utf8` formats differently from [`value_to_gk_string`] and would change a group's merge
/// identity across shards.
fn coerce_partial_agg_columns(
    batch: &arrow::record_batch::RecordBatch,
    declared: &[ColumnInfo],
    first_agg_column: usize,
) -> Result<Vec<ArrayRef>, UdfError> {
    check_declared_arity(declared, batch.num_columns())?;
    batch
        .columns()
        .iter()
        .zip(declared)
        .enumerate()
        .map(|(idx, (column, declared))| {
            if idx < first_agg_column {
                Ok(column.clone())
            } else {
                let target = target_arrow_type(declared)?;
                coerce_column(column, declared, &target)
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "partial_agg_tests.rs"]
mod tests;

#[cfg(test)]
pub use tests::build_partial_agg_sql;

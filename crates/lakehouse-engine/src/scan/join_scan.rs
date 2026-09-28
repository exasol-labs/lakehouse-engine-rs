use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;

use crate::scan::emit::{classify_scan_error, emit_stream};
use crate::scan::spec::{ProjectionItem, ScanSpec};
use crate::scan::storage_ref::ResolvedScanStorage;
use crate::scan::{diagnostics, emit_phase_telemetry};
use crate::types::mapping::{
    ExaTypeClass, arrow_to_exasol_type, classify_exa_type, needs_json_fallback,
    needs_nested_json_rendering,
};

use super::raw_scan::{NESTED_JSON_RENDER_UDF_NAME, delete_path_read_limiter, register_file_list};
use super::sql_support::{build_alias_items, quote_ident};

const JOIN_FACT_TABLE: &str = "fact_scan";
const JOIN_DIM_TABLE: &str = "dim_scan";

/// The dimension side sits on the LEFT with join reordering disabled (see
/// [`session_config_for_spec`]), so it is deterministically the hash-join build side.
pub async fn run_join_scan_with_session(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
    timers: &mut diagnostics::PhaseTimers,
) -> Result<(), UdfError> {
    let secrets = storage.all_secret_values();
    register_join_tables(session_ctx, spec, storage).await?;
    let sql = build_join_sql(session_ctx, JOIN_FACT_TABLE, JOIN_DIM_TABLE, spec).await?;
    let df = session_ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("join SQL error: {e}")))?;
    let stream = df
        .execute_stream()
        .await
        .map_err(|e| classify_scan_error(e, &secrets))?;
    emit_stream(ctx, stream, &secrets, timers).await?;
    emit_phase_telemetry(ctx, timers);
    Ok(())
}

async fn register_join_tables(
    ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
) -> Result<(), UdfError> {
    let join = spec
        .common
        .join
        .as_ref()
        .expect("register_join_tables called without a join block");

    if spec.common.aggregates.is_some() || spec.common.group_keys.is_some() {
        return Err(UdfError::User(
            "join pushdown does not support aggregate or GROUP BY in the same scan spec".into(),
        ));
    }

    let dimension_backend = storage.join().ok_or_else(|| {
        UdfError::User(
            "the resolved scan storage carries no dimension-side backend for this join spec".into(),
        )
    })?;

    // The dimension side registers against `join.storage`, never `common.storage`: a vended
    // credential is scoped to its own table, and its reads must redact against its own secrets.
    //
    // One delete-path read semaphore is shared by BOTH sides: DataFusion plans the two scan
    // leaves concurrently, so a per-side semaphore would allow 2N concurrent reads instead of N.
    let delete_path_read_limiter = delete_path_read_limiter(spec);
    register_file_list(
        ctx,
        JOIN_FACT_TABLE,
        &spec.files,
        &spec.common.table_root,
        &spec.common.logical_schema,
        &spec.common.name_mapping,
        &spec.common.partition_columns,
        storage.primary(),
        Arc::clone(&delete_path_read_limiter),
    )
    .await?;
    register_file_list(
        ctx,
        JOIN_DIM_TABLE,
        &join.files,
        &join.table_root,
        &join.logical_schema,
        &join.name_mapping,
        &join.partition_columns,
        dimension_backend,
        delete_path_read_limiter,
    )
    .await?;
    Ok(())
}

/// Uppercase aliased sub-SELECTs make the pushed projection, `condition`, and filter resolve
/// unambiguously against the combined schema. The dimension side is LEFT (the build side).
/// [`JoinSpec::post_join_order_by`](crate::scan::spec::JoinSpec::post_join_order_by) and
/// [`JoinSpec::post_join_limit`](crate::scan::spec::JoinSpec::post_join_limit) are applied
/// here, after the join and its `WHERE`, never to either side's scan.
async fn build_join_sql(
    ctx: &SessionContext,
    fact_table: &str,
    dim_table: &str,
    spec: &ScanSpec,
) -> Result<String, UdfError> {
    let join = spec
        .common
        .join
        .as_ref()
        .expect("build_join_sql called without a join block");

    let fact = ctx
        .table(fact_table)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered fact table: {e}")))?;
    let dim = ctx
        .table(dim_table)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered dimension table: {e}")))?;
    let fact_schema = fact.schema();
    let dim_schema = dim.schema();

    let fact_aliased = format!(
        "SELECT {} FROM {fact_table}",
        build_alias_items(fact_schema).join(", ")
    );
    let dim_aliased = format!(
        "SELECT {} FROM {dim_table}",
        build_alias_items(dim_schema).join(", ")
    );

    // Columns are disjoint across the two tables (VS guarantee), so a bare name resolves once.
    let combined = combined_upper_fields(dim_schema, fact_schema);

    let proj_items: Vec<ProjectionItem> = if spec.common.projection.is_empty() {
        combined
            .iter()
            .map(|(name, _)| ProjectionItem::Column(name.clone()))
            .collect()
    } else {
        spec.common.projection.clone()
    };

    let select_items: Vec<String> = proj_items
        .iter()
        .map(|item| render_join_select_item(item, &combined))
        .collect();

    let mut sql = format!(
        "SELECT {} FROM ({dim_aliased}) INNER JOIN ({fact_aliased}) ON {}",
        select_items.join(", "),
        join.condition
    );

    if let Some(filter) = &spec.common.filter
        && !filter.is_empty()
    {
        sql.push_str(" WHERE ");
        sql.push_str(filter);
    }

    if !join.post_join_order_by.is_empty() {
        let elements: Vec<String> = join
            .post_join_order_by
            .iter()
            .map(|key| key.render_ordered(&render_join_sort_target(&key.column, &combined)))
            .collect();
        sql.push_str(" ORDER BY ");
        sql.push_str(&elements.join(", "));
    }

    if let Some(limit) = join.post_join_limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    Ok(sql)
}

/// Dimension columns first, matching the left/build side.
fn combined_upper_fields(
    dim_schema: &datafusion::common::DFSchema,
    fact_schema: &datafusion::common::DFSchema,
) -> Vec<(String, arrow::datatypes::DataType)> {
    dim_schema
        .fields()
        .iter()
        .chain(fact_schema.fields().iter())
        .map(|f| (f.name().to_uppercase(), f.data_type().clone()))
        .collect()
}

/// Same column rendering rule as the single-table `build_scan_sql`.
fn render_join_select_item(
    item: &ProjectionItem,
    combined: &[(String, arrow::datatypes::DataType)],
) -> String {
    match item {
        ProjectionItem::Expr { expr } => expr.clone(),
        ProjectionItem::Column(col_name) => {
            let ident = quote_ident(&col_name.to_uppercase());
            match combined_type(col_name, combined) {
                Some(dt) if needs_nested_json_rendering(dt) => {
                    format!("{NESTED_JSON_RENDER_UDF_NAME}({ident})")
                }
                Some(dt) if needs_json_fallback(dt) => format!("CAST({ident} AS VARCHAR)"),
                _ => ident,
            }
        }
    }
}

/// Ranks by the value the shard emits, since that is what the Exasol-side wrapper merges and
/// ranks; ranking anything else can cut a row the wrapper's global top-`n` needs. Exasol's
/// VARCHAR has no empty string (`''` arrives as NULL), and `emit_batch` emits `NaN` as NULL (#246).
fn render_join_sort_target(
    column: &str,
    combined: &[(String, arrow::datatypes::DataType)],
) -> String {
    use arrow::datatypes::DataType;

    let emitted = render_join_select_item(&ProjectionItem::Column(column.to_string()), combined);
    match combined_type(column, combined) {
        Some(DataType::Float32 | DataType::Float64) => {
            format!("CASE WHEN isnan({emitted}) THEN NULL ELSE {emitted} END")
        }
        Some(dt) if classify_exa_type(&arrow_to_exasol_type(dt)) == ExaTypeClass::Character => {
            format!("nullif({emitted}, '')")
        }
        _ => emitted,
    }
}

fn combined_type<'a>(
    column: &str,
    combined: &'a [(String, arrow::datatypes::DataType)],
) -> Option<&'a arrow::datatypes::DataType> {
    let upper = column.to_uppercase();
    combined
        .iter()
        .find(|(name, _)| *name == upper)
        .map(|(_, dt)| dt)
}

/// Exposed so a host test can assert the dimension side is the hash-join build side.
pub async fn build_join_physical_plan(
    ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>, UdfError> {
    register_join_tables(ctx, spec, storage).await?;
    let sql = build_join_sql(ctx, JOIN_FACT_TABLE, JOIN_DIM_TABLE, spec).await?;
    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("join SQL error: {e}")))?;
    df.create_physical_plan()
        .await
        .map_err(|e| UdfError::User(format!("physical plan error: {e}")))
}

#[cfg(test)]
#[path = "join_scan_tests.rs"]
mod tests;

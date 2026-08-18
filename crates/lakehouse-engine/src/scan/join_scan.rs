//! Two-table broadcast inner equi-join scan path: registers the sharded fact
//! file list and the full, shard-invariant dimension file list into ONE
//! session, builds the joined SQL exposing Exasol-facing uppercase column
//! names, and streams the joined batches back through `ctx.emit`.

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;

use crate::scan::emit::{classify_scan_error, emit_stream};
use crate::scan::spec::{ProjectionItem, ScanSpec};
use crate::scan::{diagnostics, emit_phase_telemetry};
use crate::types::mapping::{needs_json_fallback, needs_nested_json_rendering};

use super::raw_scan::{NESTED_JSON_RENDER_UDF_NAME, delete_path_read_limiter, register_file_list};
use super::sql_support::{build_alias_items, quote_ident};

/// Registered table name for the sharded fact (large) side of a broadcast join.
const JOIN_FACT_TABLE: &str = "fact_scan";
/// Registered table name for the full dimension (small/build) side of a broadcast join.
const JOIN_DIM_TABLE: &str = "dim_scan";

/// Stream a two-table inner equi-join over an already-built session.
///
/// Registers the sharded fact file list (`spec.files`) and the full, shard-invariant
/// dimension file list (`spec.common.join.files`) as two tables in the SAME session, each
/// wrapped in an aliased sub-SELECT exposing Exasol-facing uppercase column names,
/// then executes `SELECT <projection> FROM (dim) INNER JOIN (fact) ON <condition>
/// [WHERE <filter>] [LIMIT n]` and streams the joined batches through [`emit_stream`]
/// (one fetched, emitted, dropped before the next — never collect-all).
///
/// The bounded dimension side is placed on the LEFT of the join and join reordering
/// is disabled (see [`session_config_for_spec`]), so the dimension is deterministically
/// the hash-join build side regardless of table statistics. Read/deserialization
/// errors for EITHER side route through [`classify_scan_error`] against the UNION of
/// both sides' secret values ([`crate::scan::spec::CommonScanSpec::all_secret_values`]) — a fact-side-only
/// set would leak the dimension side's own credential, since `join.storage` holds a
/// genuinely different one. Exposed so a host integration test can drive this exact
/// path over local Parquet (no S3 store).
pub async fn run_join_scan_with_session(
    ctx: &mut dyn UdfContext,
    session_ctx: &SessionContext,
    spec: &ScanSpec,
    timers: &mut diagnostics::PhaseTimers,
) -> Result<(), UdfError> {
    let secrets = spec.common.all_secret_values();
    register_join_tables(session_ctx, spec).await?;
    let sql = build_join_sql(session_ctx, JOIN_FACT_TABLE, JOIN_DIM_TABLE, spec).await?;
    let df = session_ctx
        .sql(&sql)
        .await
        .map_err(|e| UdfError::User(format!("join SQL error: {e}")))?;
    let stream = df
        .execute_stream()
        .await
        .map_err(|e| classify_scan_error(e, &secrets))?;
    emit_stream(ctx, stream, &secrets, &spec.common.emit_exa_types, timers).await?;
    emit_phase_telemetry(ctx, timers);
    Ok(())
}

/// Register both sides of a broadcast join into one session: the sharded fact file
/// list and the full dimension file list, each via [`register_file_list`].
///
/// Aggregates or GROUP BY alongside a join are out of scope for this phase (the VS
/// never emits that combination); such a spec is rejected with a clear error rather
/// than silently producing a wrong-shaped result.
async fn register_join_tables(ctx: &SessionContext, spec: &ScanSpec) -> Result<(), UdfError> {
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

    // Each side carries its OWN storage backend (StorageBackend) alongside its own
    // table_root, file list, logical schema, and per-file positional deletes, all
    // of which register_file_list applies to the dimension registration exactly as
    // it does for the fact side. A vended credential is scoped to the table it was
    // resolved for, so the dimension side is registered against join.storage and
    // never common.storage: that is what makes its reads — data files and delete
    // files alike — redact against ITS own secret values rather than the fact
    // side's.
    //
    // ONE shared delete-path read semaphore for this invocation, cloned into BOTH
    // sides' registration: DataFusion plans a broadcast join's two scan leaves
    // concurrently, so a per-side semaphore would allow up to 2N concurrent
    // delete-path reads (Phase A delete-file bodies and Phase B data-file footers
    // alike) instead of the intended N. This is deliberately NOT per side, unlike
    // the object store above: the semaphore bounds in-flight reads for the whole
    // instance, whereas each side needs its own store to read through its own
    // credential.
    let delete_path_read_limiter = delete_path_read_limiter(spec);
    register_file_list(
        ctx,
        JOIN_FACT_TABLE,
        &spec.files,
        &spec.common.table_root,
        &spec.common.logical_schema,
        &spec.common.name_mapping,
        &spec.common.partition_columns,
        &spec.common.storage,
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
        &join.storage,
        delete_path_read_limiter,
    )
    .await?;
    Ok(())
}

/// Build the DataFusion SQL for a two-table inner equi-join.
///
/// Both registered tables are wrapped in an aliased sub-SELECT exposing uppercase,
/// Exasol-facing column names (the same seam the single-table and partial-aggregate
/// paths use), so the pushed projection, the rendered join `condition`, and the
/// WHERE filter — all uppercase and disjoint across the two tables — resolve
/// unambiguously against the join's combined schema.
///
/// The dimension side is placed on the LEFT so it is the hash-join build side (see
/// [`run_join_scan_with_session`]). Output column order follows `spec.common.projection`
/// (positionally aligned with `emit_exa_types`); an empty projection expands to
/// every column, dimension columns first. The row cap comes from
/// [`JoinSpec::post_join_limit`](crate::scan::spec::JoinSpec::post_join_limit)
/// and is applied HERE — after the join and its
/// `WHERE` — never to either side's registered scan; see that field's doc.
///
/// The JSON-render scalar function is registered here so `render_join_select_item`
/// can name it in the generated select list.
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

    // Uppercase output column names paired with their Arrow type, dimension side
    // first (matching the left/build side). Columns are disjoint across the two
    // tables (VS guarantee), so a bare uppercase name resolves in exactly one side.
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

    // Dimension on the LEFT = hash-join build side (reordering is disabled).
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

    if let Some(limit) = join.post_join_limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    Ok(sql)
}

/// Build an ordered `(UPPERCASE_NAME, DataType)` list for every column across both
/// join inputs, dimension columns first (matching the left/build side). Column
/// names are disjoint across the two tables (VS guarantee), so the flattened list
/// carries no duplicate names.
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

/// Render one projection item for the join SELECT list. A rendered scalar
/// expression is spliced verbatim; a bare column is quoted as an uppercase
/// identifier, routed through [`NESTED_JSON_RENDER_UDF_NAME`] when its Arrow
/// type is one of the five `needs_nested_json_rendering` owns, or wrapped in
/// `CAST(... AS VARCHAR)` for every other type the JSON fallback covers — the
/// same rule the single-table scan applies in `build_scan_sql`.
fn render_join_select_item(
    item: &ProjectionItem,
    combined: &[(String, arrow::datatypes::DataType)],
) -> String {
    match item {
        ProjectionItem::Expr { expr } => expr.clone(),
        ProjectionItem::Column(col_name) => {
            let upper = col_name.to_uppercase();
            let data_type = combined
                .iter()
                .find(|(name, _)| *name == upper)
                .map(|(_, dt)| dt.clone());
            match data_type {
                Some(dt) if needs_nested_json_rendering(&dt) => {
                    format!("{NESTED_JSON_RENDER_UDF_NAME}({})", quote_ident(&upper))
                }
                Some(dt) if needs_json_fallback(&dt) => {
                    format!("CAST({} AS VARCHAR)", quote_ident(&upper))
                }
                _ => quote_ident(&upper),
            }
        }
    }
}

/// Build the physical plan for the two-table inner equi-join, registering both
/// sides into `ctx`. Exposed so a host test can assert the bounded dimension side
/// is the hash-join build (left) side without standing up an S3 store — the caller
/// registers local Parquet files, then inspects the plan this function produces.
pub async fn build_join_physical_plan(
    ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>, UdfError> {
    register_join_tables(ctx, spec).await?;
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

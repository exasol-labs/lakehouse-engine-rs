use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use exasol_udf_sdk::error::UdfError;

use crate::scan::spec::{JoinSpec, ProjectionItem, ScanSpec};
use crate::scan::storage_ref::ResolvedScanStorage;

use super::raw_scan::{
    build_dataframe, delete_path_read_limiter, physical_plan, register_file_list,
};
use super::source::{ScanSource, relation_columns};
use super::sql_support::{aliased_select, quote_ident};

const JOIN_FACT_TABLE: &str = "fact_scan";
const JOIN_DIM_TABLE: &str = "dim_scan";

pub(super) async fn register_join_tables(
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

/// The broadcast join of the two registered tables. Uppercase aliased sub-SELECTs, each wrapped
/// as its own named relation, make the pushed projection, `condition`, and filter resolve
/// unambiguously: every column is qualified by its relation alias. The dimension side sits on
/// the LEFT with join reordering disabled (see [`session_config_for_spec`]), so it is
/// deterministically the hash-join build side.
///
/// [`session_config_for_spec`]: crate::scan::session_config_for_spec
pub(super) async fn join_source(
    ctx: &SessionContext,
    spec: &ScanSpec,
) -> Result<ScanSource, UdfError> {
    let join = spec
        .common
        .join
        .as_ref()
        .expect("join_source called without a join block");
    ensure_join_columns_are_qualified(spec, join)?;

    let fact = ctx
        .table(JOIN_FACT_TABLE)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered fact table: {e}")))?;
    let dim = ctx
        .table(JOIN_DIM_TABLE)
        .await
        .map_err(|e| UdfError::User(format!("cannot resolve registered dimension table: {e}")))?;

    let binary_as_string = spec.common.binary_as_string;
    let from = format!(
        "({}) AS {} INNER JOIN ({}) AS {} ON {}",
        aliased_select(JOIN_DIM_TABLE, dim.schema(), binary_as_string),
        quote_ident(&join.dimension_alias),
        aliased_select(JOIN_FACT_TABLE, fact.schema(), binary_as_string),
        quote_ident(&join.fact_alias),
        join.condition
    );
    let mut columns = relation_columns(Some(&join.dimension_alias), dim.schema());
    columns.extend(relation_columns(Some(&join.fact_alias), fact.schema()));
    Ok(ScanSource::new(from, columns))
}

/// `project_columns` returns at least one item for every join select list, so an empty
/// projection is a malformed spec, not a request for the full row. A column with no relation
/// alias cannot be bound to one side, and a bare-name guess would pick the wrong relation of a
/// shared column name.
fn ensure_join_columns_are_qualified(spec: &ScanSpec, join: &JoinSpec) -> Result<(), UdfError> {
    if spec.common.projection.is_empty() {
        return Err(UdfError::User(
            "join scan spec carries an empty projection; it must name at least one column".into(),
        ));
    }
    let projected = spec.common.projection.iter().filter_map(|item| match item {
        ProjectionItem::Column(column) => Some(column),
        ProjectionItem::Expr { .. } => None,
    });
    let sort_keys = join.post_join_order_by.iter().map(|key| &key.column);
    match projected.chain(sort_keys).find(|c| c.relation.is_none()) {
        Some(column) => Err(UdfError::User(format!(
            "join scan spec column {} carries no relation alias",
            column.name
        ))),
        None => Ok(()),
    }
}

/// Registers the spec's tables first. Exposed so a host test can assert the dimension side is
/// the hash-join build side.
pub async fn build_join_physical_plan(
    ctx: &SessionContext,
    spec: &ScanSpec,
    storage: &ResolvedScanStorage,
) -> Result<Arc<dyn ExecutionPlan>, UdfError> {
    physical_plan(build_dataframe(ctx, spec, storage).await?).await
}

#[cfg(test)]
#[path = "join_scan_tests.rs"]
mod tests;

use crate::adapter::permission::PermissionCheck;
use crate::adapter::properties::resolve_binary_as_string;
use crate::adapter::statement_state::StatementCatalog;
#[cfg(test)]
use crate::scan::spec::StorageBackend;
use exasol_udf_sdk::error::UdfError;
use futures::future::try_join_all;
use serde_json::Value as Json;

use super::empty_result::empty_result_sql;
use super::refused_columns::ensure_no_touched_column_is_refused;
use super::scan_resolution::TableScanResolver;
use super::support::ScanTranslator;
use super::support::{
    ColumnTypes, DISTRIBUTE_FILES_UDF_NAME, InvolvedColumns, PushdownRequest, SCAN_UDF_NAME,
    project_columns, quote_ident,
};

mod attribution;
mod planning;
mod rendering;
mod sql_builders;

pub(crate) use planning::{
    DetectedJoin, JoinLeaf, JoinShape, JoinSides, ResolvedJoinSide, detect_join,
};
pub(crate) use sql_builders::{RenderedJoinPushdown, render_broadcast_join};

pub(super) use sql_builders::{EmptyRelation, qualified_single_table_fallback_pushdown};
#[cfg(test)]
pub(super) use sql_builders::{
    FanOutProjection, build_qualified_single_table_fallback_sql, referenced_columns,
};

pub(super) use planning::{JoinWindowPlan, involved_table};
use planning::{classify_join_window, resolve_one_join_side, select_broadcast_sides};
use rendering::{has_no_explicit_select_list, leg_local_filter, possible_side_column_names};
// `pub(super)` so the `#[cfg(test)]` dispatch-golden sibling module can drive both builders.
pub(super) use sql_builders::{
    JoinScanRequestConfig, build_broadcast_join_sql, build_n_scan_join_sql,
};

/// Fails the request unless every `column` node of `pushdown` binds one table of the query, a
/// leg of `join` or the request's one table, and that table declares it (#615). `declared` holds
/// each leg's columns in leg order, as the caller already read them from `involvedTables`. Both
/// planners run it before any catalog request.
pub(crate) fn ensure_columns_declared(
    request: &Json,
    pushdown: &Json,
    join: Option<&DetectedJoin>,
    declared: &[&InvolvedColumns<'_>],
) -> Result<(), UdfError> {
    let legs = match join {
        Some(join) => join.legs(),
        None => attribution::JoinLegs::for_single_scan(request),
    };
    legs.ensure_columns_declared(pushdown, declared)
}

/// The generated SQL runs outside the adapter script's schema, so an unqualified name would
/// not resolve.
pub(super) fn qualify_udf(scan_schema: Option<&str>, udf: &str) -> String {
    match scan_schema {
        Some(schema) if !schema.is_empty() => format!("{}.{}", quote_ident(schema), udf),
        _ => udf.to_string(),
    }
}

/// Attribution is per side: a name refused on one side must not refuse a query reading only
/// the other side's same-named column. With no explicit select list, each side's whole
/// declared row is charged.
///
/// Lives here rather than in `planning`, which must not depend on `rendering`.
fn ensure_no_side_refuses_a_referenced_column(
    request: &Json,
    pushdown_req: &Json,
    sides: &[ResolvedJoinSide],
) -> Result<(), UdfError> {
    if sides.iter().all(|side| side.refused_columns.is_empty()) {
        return Ok(());
    }
    let emits_unnamed_row = has_no_explicit_select_list(pushdown_req);
    for side in sides {
        let mut touched = possible_side_column_names(request, &side.table_name);
        if emits_unnamed_row {
            touched.extend(
                side.column_types
                    .iter()
                    .map(|(column, _)| column.name.clone()),
            );
        }
        ensure_no_touched_column_is_refused(&touched, &side.refused_columns)?;
    }
    Ok(())
}

/// Resolves each table once (never per shard), pruned by its leg-local WHERE conjuncts. Any
/// empty side short-circuits to the shape-correct empty result, since an inner join is then
/// empty.
///
/// Broadcast is an optimization inside this one path: taken only for an N = 2 equi-join
/// whose smaller side fits `join_broadcast_max_bytes`, whose window the broadcast path can
/// serve, and whose leg-qualified render succeeds. The legs may share column names or be one
/// table joined to itself. Everything else takes the sole fallback,
/// [`build_n_scan_join_sql`], which scans each table via its own fan-out and joins in Exasol.
/// A column that binds no single leg, or that its leg does not declare, fails the request
/// before any catalog call ([`ensure_columns_declared`]).
///
/// No plan-time check compares the sides' storage backends: each side reads through its own
/// store. The one unservable collapse (two sides on one DataFusion registry key needing
/// different stores) is owned by the scan's `validate_sides_share_one_store`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn plan_join(
    request: &Json,
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    statement: &StatementCatalog,
    permission: &PermissionCheck,
    props: &Json,
    scan_schema: Option<&str>,
    cluster_nodes: usize,
    parallelism_factor: usize,
    df_target_partitions: usize,
    df_batch_size: usize,
    df_threads_per_udf: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    s3_max_connections: usize,
    join_broadcast_max_bytes: u64,
) -> Result<Json, UdfError> {
    let pushdown_req = pushdown.json();
    let filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    // Built once for every leg, so every leg's identifier passes its rule before any catalog
    // request.
    let identifiers: Vec<&str> = join
        .tables
        .iter()
        .map(|leaf| leaf.table_identifier.as_str())
        .collect();
    // Legs are indexed positionally, not by tableName, which a self-join's occurrences share.
    let legs = join.legs();
    let involved = join
        .tables
        .iter()
        .map(|leaf| involved_table(request, &leaf.table_name))
        .collect::<Result<Vec<_>, _>>()?;
    ensure_columns_declared(
        request,
        pushdown_req,
        Some(join),
        &involved.iter().collect::<Vec<_>>(),
    )?;
    let resolver =
        TableScanResolver::for_request(statement, permission, &identifiers, props).await?;
    let side_filters: Vec<Option<Json>> = (0..join.tables.len())
        .map(|leg| filter.and_then(|f| leg_local_filter(f, &legs, leg)))
        .collect();
    // `try_join_all` preserves leg order, which later steps index by.
    let sides: Vec<ResolvedJoinSide> =
        try_join_all(join.tables.iter().zip(&side_filters).zip(&involved).map(
            |((leaf, side_filter), involved)| {
                resolve_one_join_side(
                    &leaf.table_name,
                    &leaf.table_identifier,
                    &resolver,
                    side_filter.as_ref(),
                    involved,
                )
            },
        ))
        .await?;

    ensure_no_side_refuses_a_referenced_column(request, pushdown_req, &sides)?;

    if sides.iter().any(|s| s.files.is_empty()) {
        let combined = ColumnTypes::concat(sides.iter().map(|side| &side.column_types));
        let scan = ScanTranslator::over(&combined, &[pushdown_req])?;
        let (projection, widened) = project_columns(pushdown, scan)?;
        let cols_per_leg = sides.iter().map(|side| side.column_types.clone()).collect();
        return empty_result_sql(
            pushdown,
            &projection,
            widened,
            scan,
            &EmptyRelation::join(join, cols_per_leg),
        );
    }

    let udf_name = qualify_udf(scan_schema, SCAN_UDF_NAME);
    let distribute_udf_name = qualify_udf(scan_schema, DISTRIBUTE_FILES_UDF_NAME);
    let inputs = JoinScanRequestConfig {
        cluster_nodes,
        parallelism_factor,
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
        binary_as_string: resolve_binary_as_string(props),
        connection: statement.connection(),
    };

    // The window is classified before sizing and rendering, so a request Exasol must
    // post-process never reaches `render_broadcast_join`. A decline falls through to the N-scan
    // fallback.
    let is_equi =
        join.conditions[0].get("type").and_then(|t| t.as_str()) == Some("predicate_equal");
    let window = classify_join_window(pushdown_req);
    if join.tables.len() == 2 && is_equi && !matches!(window, JoinWindowPlan::ExasolPostProcessed) {
        let candidate =
            select_broadcast_sides(sides[0].clone(), sides[1].clone(), join_broadcast_max_bytes);
        if candidate.broadcast_eligible
            && let Some(rendered) = render_broadcast_join(pushdown, join, &sides)?
            && let Some(sql) = build_broadcast_join_sql(
                &candidate,
                &rendered,
                &inputs,
                &udf_name,
                &distribute_udf_name,
            )?
        {
            return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
        }
    }

    let sql = build_n_scan_join_sql(
        pushdown,
        join,
        &sides,
        &inputs,
        &udf_name,
        &distribute_udf_name,
    )?;
    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

#[cfg(test)]
#[path = "joins_tests.rs"]
mod tests;

use crate::adapter::ResolvedConnectionConfig;
#[cfg(test)]
use crate::scan::spec::StorageBackend;
use exasol_udf_sdk::error::UdfError;
use futures::future::try_join_all;
use serde_json::Value as Json;

use super::ConnectionStorage;
use super::empty_result::empty_result_sql;
use super::refused_columns::ensure_no_touched_column_is_refused;
use super::scan_resolution::TableScanResolver;
use super::support::{DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, project_columns, quote_ident};

mod attribution;
mod planning;
mod rendering;
mod sql_builders;

pub(crate) use planning::{
    DetectedJoin, IneligibleJoinReason, JoinLeaf, JoinShape, JoinSides, ResolvedJoinSide,
    detect_join,
};
pub(crate) use sql_builders::{RenderedJoinPushdown, render_broadcast_join};

pub(super) use sql_builders::qualified_single_table_fallback_pushdown;
#[cfg(test)]
pub(super) use sql_builders::{
    FanOutProjection, build_qualified_single_table_fallback_sql, referenced_column_projection,
};

pub(super) use planning::JoinWindowPlan;
use planning::{
    classify_join_window, involved_table_columns, resolve_one_join_side, select_broadcast_sides,
};
use rendering::{has_no_explicit_select_list, leg_local_filter, possible_side_column_names};
// `pub(super)` so the `#[cfg(test)]` dispatch-golden sibling module can drive both builders.
pub(super) use sql_builders::{
    JoinScanRequestConfig, build_broadcast_join_sql, build_n_scan_join_sql,
};

/// The generated SQL runs outside the adapter script's schema, so an unqualified name would
/// not resolve.
pub(super) fn qualify_udf(scan_schema: Option<&str>, udf: &str) -> String {
    match scan_schema {
        Some(schema) if !schema.is_empty() => format!("{}.{}", quote_ident(schema), udf),
        _ => udf.to_string(),
    }
}

/// The last-resort decline for a join the adapter cannot render at all (a non-inner join
/// node or malformed shape). Falling through to the single-table path would scan only the
/// first table and silently drop the join, so this must be a hard `User` error with no
/// native re-plan.
pub(super) fn ineligible_join_decline(reason: IneligibleJoinReason) -> UdfError {
    let detail = match reason {
        IneligibleJoinReason::NotInnerJoinType => "the join is not an inner join",
        IneligibleJoinReason::UnsupportedShape => "the join `from` clause has an unsupported shape",
    };
    UdfError::User(format!(
        "join pushdown declined: {detail}; the adapter cannot render this join shape, \
         so this is a hard error, not a native re-plan"
    ))
}

/// Attribution is per side: a name refused on one side must not refuse a query reading only
/// the other side's same-named column. A `column` with no `tableName` is charged to every
/// side. With no explicit select list, each side's whole declared row is charged.
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
                involved_table_columns(request, &side.table_name)
                    .into_iter()
                    .map(|(name, _)| name),
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
/// serve, and whose bare-name render succeeds. Everything else takes the sole fallback,
/// [`build_n_scan_join_sql`], which scans each table via its own fan-out and joins in Exasol.
///
/// No plan-time check compares the sides' storage backends: each side reads through its own
/// store. The one unservable collapse (two sides on one DataFusion registry key needing
/// different stores) is owned by the scan's `validate_sides_share_one_store`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn plan_join(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
    conn: &ResolvedConnectionConfig,
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
    let filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    let connection = ConnectionStorage {
        storage: &conn.storage,
        creds: &conn.creds,
        allow_http: conn.allow_http,
    };
    // Built once and shared by every leg, so identifiers are validated before any catalog
    // HTTP and a per-leg rebuild is inexpressible.
    let identifiers: Vec<&str> = join
        .tables
        .iter()
        .map(|leaf| leaf.table_identifier.as_str())
        .collect();
    let resolver = TableScanResolver::for_request(
        conn.catalog_kind,
        &conn.catalog_uri,
        connection,
        &identifiers,
        props,
    )
    .await?;
    // Legs are indexed positionally, not by tableName, which a self-join's occurrences share.
    let legs = join.legs();
    let side_filters: Vec<Option<Json>> = (0..join.tables.len())
        .map(|leg| filter.and_then(|f| leg_local_filter(f, &legs, leg)))
        .collect();
    let side_columns: Vec<Vec<(String, String)>> = join
        .tables
        .iter()
        .map(|leaf| involved_table_columns(request, &leaf.table_name))
        .collect();
    // `try_join_all` preserves leg order, which later steps index by.
    let sides: Vec<ResolvedJoinSide> = try_join_all(
        join.tables
            .iter()
            .zip(&side_filters)
            .zip(&side_columns)
            .map(|((leaf, side_filter), columns)| {
                resolve_one_join_side(
                    &leaf.table_name,
                    &leaf.table_identifier,
                    &resolver,
                    side_filter.as_ref(),
                    columns,
                )
            }),
    )
    .await?;

    ensure_no_side_refuses_a_referenced_column(request, pushdown_req, &sides)?;

    if sides.iter().any(|s| s.files.is_empty()) {
        let combined = side_columns.concat();
        let (proj_cols, proj_types, widened) = project_columns(pushdown_req, combined.clone())?;
        return empty_result_sql(pushdown_req, &proj_cols, &proj_types, widened, &combined);
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
        connection: conn,
    };

    // The window is classified before sizing and rendering, so a request Exasol must
    // post-process never reaches `render_broadcast_join`, keeping its `Err` arm unreachable.
    // Any miss falls through to the N-scan fallback, never an error.
    let is_equi =
        join.conditions[0].get("type").and_then(|t| t.as_str()) == Some("predicate_equal");
    let window = classify_join_window(pushdown_req);
    if join.tables.len() == 2 && is_equi && !matches!(window, JoinWindowPlan::ExasolPostProcessed) {
        let candidate =
            select_broadcast_sides(sides[0].clone(), sides[1].clone(), join_broadcast_max_bytes);
        if candidate.broadcast_eligible
            && let Some(rendered) = render_broadcast_join(request, pushdown_req, join)?
            && let Some(sql) = build_broadcast_join_sql(
                &candidate,
                &rendered,
                window,
                &inputs,
                &udf_name,
                &distribute_udf_name,
            )?
        {
            return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
        }
    }

    let sql = build_n_scan_join_sql(
        request,
        pushdown_req,
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

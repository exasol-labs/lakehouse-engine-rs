use crate::adapter::ResolvedConnectionConfig;
use crate::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, ProjectionItem, ScanSpec, ScanStorage,
    StorageBackend, render_ordered,
};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::{render_df_filter_safe, render_expression_safe};

use super::super::shard_paths::relativize_shards_to_root;
use super::super::support::{
    build_scan_driving_sql, classify_where_filter, collect_all_column_names, extract_limit,
    extract_offset, quote_ident, render_limit_offset, scan_storage_for, shard_count,
    strip_table_alias,
};
use super::super::topn::{ParsedSortKey, parse_sort_flags, wrap_declined_order_by};
use super::attribution::{JoinLegs, UnattributableColumn};
use super::planning::{
    DetectedJoin, JoinSides, JoinWindowPlan, ResolvedJoinSide, disjoint_schema_guard,
    involved_table_columns,
};
use super::rendering::{
    conjoin_filters, cross_leg_residual_filter, declined_only, extract_join_projection,
    join_col_types, leg_local_filter, projection_item_select_sql, referenced_clause_values,
    referenced_leg_columns, render_df_filter_qualified, render_expression_qualified,
    renderable_only, type_screened_leg_filter,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RenderedJoinPushdown {
    pub condition: String,
    /// Never a declined filter: a decline forfeits the broadcast plan entirely.
    pub filter: Option<String>,
    pub projection: Vec<ProjectionItem>,
    /// Positionally aligned with `projection`.
    pub projection_types: Vec<String>,
}

/// `Ok(None)` is a clean decline to the N-scan fallback when the sides share a column name,
/// the condition won't render, the projection widened to the full row (#196), or the WHERE
/// filter declines through [`classify_where_filter`]: broadcast has no outer WHERE to apply
/// a declined predicate. `Err` only for a request with no column metadata at all.
///
/// The disjoint-schema guard must run first: only then does the union built by
/// [`join_col_types`] map each bare name to one type, and only then is stripping
/// `tableAlias` safe. Stripping is required because `build_join_sql` wraps each side in an
/// unaliased sub-SELECT (`No field named "O"."O_ORDERDATE"` otherwise).
pub(crate) fn render_broadcast_join(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
) -> Result<Option<RenderedJoinPushdown>, UdfError> {
    let left_cols = involved_table_columns(request, &join.tables[0].table_name);
    let right_cols = involved_table_columns(request, &join.tables[1].table_name);
    if !disjoint_schema_guard(&left_cols, &right_cols) {
        return Ok(None);
    }

    let bare_condition = strip_table_alias(&join.conditions[0]);
    // Not the filter renderer, which would suppress a trivially-true boolean.
    let condition = match render_expression_safe(&bare_condition) {
        Some(condition) => condition,
        None => return Ok(None),
    };

    let col_types = join_col_types(request, join);
    let bare_pushdown_req = strip_table_alias(pushdown_req);
    let bare_filter_json = bare_pushdown_req.get("filter").filter(|f| !f.is_null());
    let (filter, declined) = classify_where_filter(bare_filter_json, &col_types);
    if declined.is_some() {
        return Ok(None);
    }

    let (projection, projection_types, widened) =
        extract_join_projection(request, &bare_pushdown_req, join)?;
    // A widened projection is the full two-table row, the wrong shape for a broadcast fan-out
    // (#196).
    if widened {
        return Ok(None);
    }

    Ok(Some(RenderedJoinPushdown {
        condition,
        filter,
        projection,
        projection_types,
    }))
}

/// Each condition attaches greedily to the earliest join point with every referenced leg
/// in scope, as decided by [`JoinLegs::attachment_leg`] (never by table or column name). A
/// join point with none renders `ON 1=1`. Unattachable conditions are returned as outer
/// WHERE residuals, which for an inner join is result-equivalent to `ON`.
fn build_n_scan_join_from(
    fan_outs: &[String],
    legs: &JoinLegs,
    raw_conditions: &[Json],
    conditions: &[String],
) -> (String, Vec<String>) {
    let last_join_point = fan_outs.len().saturating_sub(1);

    let mut on_at: Vec<Vec<String>> = vec![Vec::new(); fan_outs.len()];
    let mut residual: Vec<String> = Vec::new();
    for (raw, rendered) in raw_conditions.iter().zip(conditions) {
        // With a single leg there is no join point (and `clamp(1, 0)` would panic).
        match legs.attachment_leg(raw) {
            Some(m) if last_join_point >= 1 => {
                on_at[m.clamp(1, last_join_point)].push(rendered.clone())
            }
            _ => residual.push(rendered.clone()),
        }
    }

    let mut from = format!("({}) AS {}", fan_outs[0], quote_ident(&legs.leg_alias(0)));
    for (k, fan_out) in fan_outs.iter().enumerate().skip(1) {
        let on = if on_at[k].is_empty() {
            "1=1".to_string()
        } else {
            on_at[k]
                .iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        from.push_str(&format!(
            " INNER JOIN ({fan_out}) AS {} ON {on}",
            quote_ident(&legs.leg_alias(k))
        ));
    }
    (from, residual)
}

fn n_full_row_qualified_items(
    legs: &JoinLegs,
    cols_per_leg: &[Vec<(String, String)>],
) -> Vec<ProjectionItem> {
    cols_per_leg
        .iter()
        .enumerate()
        .flat_map(|(leg, cols)| {
            let alias = quote_ident(&legs.leg_alias(leg));
            cols.iter().map(move |(name, _)| ProjectionItem::Expr {
                expr: format!("{alias}.{}", quote_ident(name)),
            })
        })
        .collect()
}

/// Takes the whole side so files and table root cannot be mismatched.
fn shard_side(side: &ResolvedJoinSide, inputs: &JoinScanRequestConfig<'_>) -> Vec<Vec<FileEntry>> {
    let g = shard_count(
        inputs.cluster_nodes,
        inputs.parallelism_factor,
        side.files.len(),
    );
    let shards = crate::adapter::sharding::partition_files_by_bytes(side.files.clone(), g);
    relativize_shards_to_root(shards, &side.table_root)
}

/// Not merged with [`super::ineligible_join_decline`], whose sentence differs.
fn join_render_decline(clause: &str) -> UdfError {
    UdfError::User(format!(
        "join pushdown declined: {clause}; this is a hard error, not a native re-plan"
    ))
}

/// Picking a leg arbitrarily instead would return silently wrong rows.
fn unattributable_decline(column: UnattributableColumn) -> UdfError {
    join_render_decline(&format!(
        "{column} could not be attributed to a join leg, so no correct qualified \
         reference exists"
    ))
}

/// `Ok(None)` when trivially true; `Err` when neither dialect renders it (see
/// `_decision/045`) or a reference cannot be placed on a leg.
fn render_self_applied_where(
    tree: &Json,
    legs: &JoinLegs,
    subject: &str,
) -> Result<Option<String>, UdfError> {
    if let Some(sql) = render_df_filter_qualified(tree, legs).map_err(unattributable_decline)? {
        return Ok(Some(sql));
    }
    if render_expression_qualified(tree, legs)
        .map_err(unattributable_decline)?
        .is_some()
    {
        return Ok(None);
    }
    let tree_json = serde_json::to_string(tree).unwrap_or_default();
    Err(join_render_decline(&format!(
        "{subject} could be rendered by neither dialect, so it could be applied nowhere: {tree_json}"
    )))
}

/// An absent/empty select list projects every column of every leg.
fn n_scan_join_select_items(
    pushdown_req: &Json,
    legs: &JoinLegs,
    cols_per_leg: &[Vec<(String, String)>],
) -> Result<Vec<ProjectionItem>, UdfError> {
    match pushdown_req.get("selectList") {
        Some(Json::Array(list)) if !list.is_empty() => {
            let mut items = Vec::with_capacity(list.len());
            for item in list {
                let sql = render_expression_qualified(item, legs)
                    .map_err(unattributable_decline)?
                    .ok_or_else(|| {
                        join_render_decline(
                            "a select-list item could not be rendered for the qualified N-scan join",
                        )
                    })?;
                items.push(ProjectionItem::Expr { expr: sql });
            }
            Ok(items)
        }
        _ => Ok(n_full_row_qualified_items(legs, cols_per_leg)),
    }
}

/// Shared by the N-scan wrapper and the grouped single-table fallback. Clauses are computed
/// in SELECT, GROUP BY, HAVING, ORDER BY order so the first unrenderable one surfaces its
/// error; the window renders last so it applies after the sort.
struct OuterWrapperClauses {
    select: String,
    trailing: String,
}

fn outer_wrapper_clauses(
    pushdown_req: &Json,
    legs: &JoinLegs,
    cols_per_leg: &[Vec<(String, String)>],
) -> Result<OuterWrapperClauses, UdfError> {
    let select_items = n_scan_join_select_items(pushdown_req, legs, cols_per_leg)?;
    let group_by = qualified_join_group_by(pushdown_req, legs)?;
    let having = qualified_join_having(pushdown_req, legs)?;
    let order_by = qualified_join_order_by(pushdown_req, legs)?;
    let limit = extract_limit(pushdown_req);
    let offset = extract_offset(pushdown_req);
    // Exasol withholds `limit` when it cannot delegate an ordering, so an offset never arrives
    // without `orderBy` (#191, verified live). Pinned rather than declined: no request reaches it.
    debug_assert!(
        offset == 0 || order_by.is_some(),
        "fact 5: Exasol withholds `limit` entirely when it cannot delegate an ordering, \
         so a non-zero offset must never arrive without a non-empty orderBy"
    );

    let select = if select_items.is_empty() {
        "*".to_string()
    } else {
        select_items
            .iter()
            .map(projection_item_select_sql)
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut trailing = String::new();
    if let Some(clause) = group_by {
        trailing.push_str(&format!(" GROUP BY {clause}"));
    }
    if let Some(clause) = having {
        trailing.push_str(&format!(" HAVING {clause}"));
    }
    if let Some(clause) = order_by {
        trailing.push_str(&format!(" ORDER BY {clause}"));
    }
    trailing.push_str(&render_limit_offset(limit, offset));

    Ok(OuterWrapperClauses { select, trailing })
}

/// The sole unaccelerated fallback: each table scans through its own sharded fan-out, and
/// Exasol rebuilds the join via a left-to-right `INNER JOIN … ON` chain. Leg identity is
/// owned by [`JoinLegs`], so self-join occurrences stay distinct legs (#361). All references
/// render leg-qualified (`"LHS_T{i}"."COL"`), so shared column or table names are safe.
///
/// Each leg receives only leg-local conjuncts passing [`renderable_only`] and
/// [`type_screened_leg_filter`] (which also rewrites them). Everything else (cross-leg,
/// OR-spanning, untagged, declined, type-declined, unattachable conditions) goes to the
/// outer WHERE, each parenthesized; nothing is omitted. The fan-out loop must run before the
/// residual is assembled, since the type screen can hand conjuncts back.
///
/// `Err` (hard, no native re-plan) only when the wrapper cannot be built, including a
/// residual neither dialect renders: a predicate applicable nowhere must fail the query.
#[allow(clippy::too_many_arguments)]
pub(in super::super) fn build_n_scan_join_sql(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    inputs: &JoinScanRequestConfig<'_>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    let cols_per_side: Vec<Vec<(String, String)>> = sides
        .iter()
        .map(|s| involved_table_columns(request, &s.table_name))
        .collect();
    if cols_per_side.iter().any(|c| c.is_empty()) {
        return Err(join_render_decline(
            "an involved table carries no column metadata, so the unaccelerated N-scan \
             fallback cannot be built",
        ));
    }

    let legs = join.legs();
    // A real guard, not a debug assertion: a mismatch would panic out of bounds in a release UDF.
    if legs.leg_count() != sides.len() {
        return Err(join_render_decline(&format!(
            "leg count ({}) and resolved-side count ({}) disagree, so a leg index cannot \
             index the resolved sides",
            legs.leg_count(),
            sides.len()
        )));
    }

    // A condition has no lower fallback: if it cannot render qualified, no correct SQL exists.
    let mut conditions = Vec::with_capacity(join.conditions.len());
    for cond in &join.conditions {
        let rendered = render_expression_qualified(cond, &legs)
            .map_err(unattributable_decline)?
            .ok_or_else(|| {
                join_render_decline(
                    "a join condition could not be rendered against the qualified N-scan schema",
                )
            })?;
        conditions.push(rendered);
    }

    let where_filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    let leg_eligible = where_filter.and_then(renderable_only);

    // All conditions go in as one array so `referenced_leg_columns` keeps any side column a
    // condition references.
    let all_conditions = Json::Array(join.conditions.clone());
    let mut fan_outs = Vec::with_capacity(sides.len());
    let mut type_declined: Option<Json> = None;
    for (i, side) in sides.iter().enumerate() {
        let narrowed =
            referenced_leg_columns(pushdown_req, &all_conditions, &legs, i, &cols_per_side[i]);
        let (side_filter, side_declined) = match leg_eligible
            .as_ref()
            .and_then(|f| leg_local_filter(f, &legs, i))
        {
            Some(side_local) => type_screened_leg_filter(&side_local, &cols_per_side[i]),
            None => (None, None),
        };
        // Disjoint by attribution, so no conjunct is double-applied.
        type_declined = conjoin_filters(type_declined, side_declined);
        fan_outs.push(build_side_fan_out_sql(
            side,
            &narrowed,
            side_filter.as_ref(),
            inputs,
            udf_name,
            distribute_udf_name,
        )?);
    }

    // Three disjoint sets that, with the per-side leg filters, partition the request's filter.
    let residual = conjoin_filters(
        conjoin_filters(
            leg_eligible
                .as_ref()
                .and_then(|f| cross_leg_residual_filter(f, &legs)),
            where_filter.and_then(declined_only),
        ),
        type_declined,
    );
    let filter = match &residual {
        None => None,
        Some(tree) => render_self_applied_where(tree, &legs, "a residual WHERE conjunct")?,
    };

    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown_req, &legs, &cols_per_side)?;

    let (from, residual_conditions) =
        build_n_scan_join_from(&fan_outs, &legs, &join.conditions, &conditions);

    let mut where_parts: Vec<String> = residual_conditions
        .iter()
        .map(|c| format!("({c})"))
        .collect();
    if let Some(f) = &filter {
        where_parts.push(format!("({f})"));
    }

    let mut sql = format!("SELECT {select} FROM {from}");
    if !where_parts.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
    }
    sql.push_str(&trailing);
    Ok(sql)
}

pub(in super::super) struct JoinScanRequestConfig<'a> {
    pub(in super::super) cluster_nodes: usize,
    pub(in super::super) parallelism_factor: usize,
    pub(in super::super) df_target_partitions: usize,
    pub(in super::super) df_batch_size: usize,
    pub(in super::super) df_threads_per_udf: usize,
    pub(in super::super) memory_pool_fraction: f64,
    pub(in super::super) instance_overhead_mb: u64,
    pub(in super::super) s3_max_connections: usize,
    pub(in super::super) connection: &'a ResolvedConnectionConfig,
}

fn relativize_files_to_root(files: Vec<FileEntry>, table_root: &str) -> Vec<FileEntry> {
    relativize_shards_to_root(vec![files], table_root)
        .pop()
        .unwrap_or_default()
}

/// Carries only `primary`'s own storage; the broadcast dimension's storage rides in
/// `join.storage`. `common.limit` is always `None`: a post-join cap rides in
/// [`JoinSpec::post_join_limit`] instead.
fn join_fan_out_scan_spec(
    primary: &ResolvedJoinSide,
    projection: Vec<ProjectionItem>,
    filter: Option<String>,
    join: Option<JoinSpec>,
    inputs: &JoinScanRequestConfig<'_>,
) -> Result<ScanSpec, UdfError> {
    let storage = scan_storage_for_side(&primary.effective_storage, inputs)?;
    Ok(ScanSpec {
        common: CommonScanSpec {
            table_root: primary.table_root.clone(),
            projection,
            filter,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: false,
            logical_schema: primary.logical_schema.clone(),
            name_mapping: primary.name_mapping.clone(),
            join,
            partition_columns: primary.partition_columns.clone(),
            storage,
            df_target_partitions: inputs.df_target_partitions,
            df_batch_size: inputs.df_batch_size,
            df_threads_per_udf: inputs.df_threads_per_udf,
            memory_pool_fraction: inputs.memory_pool_fraction,
            instance_overhead_mb: inputs.instance_overhead_mb,
            s3_max_connections: inputs.s3_max_connections,
        },
        files: vec![],
    })
}

fn scan_storage_for_side(
    effective: &StorageBackend,
    inputs: &JoinScanRequestConfig<'_>,
) -> Result<ScanStorage, UdfError> {
    let conn = inputs.connection;
    scan_storage_for(
        &conn.creds,
        &conn.connection_name,
        conn.allow_http,
        effective,
        conn.sealed_storage_key.as_ref(),
    )
}

/// `side_filter` arrives pre-screened and pre-rewritten by the caller: this function cannot
/// route a decline to the outer wrapper, so a decline swallowed here would be applied
/// nowhere. `columns` must expose every column any outer clause references.
pub(super) fn build_side_fan_out_sql(
    side: &ResolvedJoinSide,
    columns: &[(String, String)],
    side_filter: Option<&Json>,
    inputs: &JoinScanRequestConfig<'_>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    let proj_cols: Vec<ProjectionItem> = columns
        .iter()
        .map(|(name, _)| ProjectionItem::Column(name.clone()))
        .collect();
    let proj_types: Vec<String> = columns.iter().map(|(_, ty)| ty.clone()).collect();

    let shards = shard_side(side, inputs);

    // Bare: the fan-out relation exposes bare uppercase names, so an alias would not resolve.
    let filter = side_filter
        .map(strip_table_alias)
        .and_then(|f| render_df_filter_safe(&f));
    let spec = join_fan_out_scan_spec(side, proj_cols.clone(), filter, None, inputs)?;
    Ok(build_scan_driving_sql(
        &spec,
        &shards,
        &proj_cols,
        &proj_types,
        None,
        &[],
        None,
        udf_name,
        distribute_udf_name,
    ))
}

fn binds_to_projection(key: &ParsedSortKey, projection: &[ProjectionItem]) -> bool {
    let ParsedSortKey::Column(key) = key else {
        return false;
    };
    projection
        .iter()
        .any(|item| matches!(item, ProjectionItem::Column(name) if *name == key.column))
}

/// `None` falls through to the N-scan wrapper, never an error.
///
/// The fact side is sharded; the dimension's full file list rides once in the common blob's
/// [`JoinSpec`], so every shard joins node-locally with no exchange. Each side carries its
/// own storage because a vended credential is scoped to its table. The window only ever
/// applies after the join ([`JoinSpec::post_join_limit`]): an unordered cap composes per
/// shard, an ordered window rides on an outer wrapper.
pub(in super::super) fn build_broadcast_join_sql(
    sides: &JoinSides,
    rendered: &RenderedJoinPushdown,
    window: JoinWindowPlan,
    inputs: &JoinScanRequestConfig<'_>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<Option<String>, UdfError> {
    let (shard_cap, ordering) = match window {
        JoinWindowPlan::Unbounded => (None, None),
        JoinWindowPlan::BareLimit(n) => (Some(n), None),
        JoinWindowPlan::Ordered {
            keys,
            limit,
            offset,
        } => {
            // Only now is there a projection to check: the wrapper's ORDER BY binds against emitted
            // columns and this path appends no hidden ones.
            if !keys
                .iter()
                .all(|key| binds_to_projection(key, &rendered.projection))
            {
                return Ok(None);
            }
            (None, Some((keys, limit, offset)))
        }
        JoinWindowPlan::ExasolPostProcessed => return Ok(None),
    };

    let fact = &sides.fact;
    let dimension = &sides.dimension;

    let shards = shard_side(fact, inputs);

    let join = JoinSpec {
        table_root: dimension.table_root.clone(),
        files: relativize_files_to_root(dimension.files.clone(), &dimension.table_root),
        logical_schema: dimension.logical_schema.clone(),
        name_mapping: dimension.name_mapping.clone(),
        join_type: JoinType::Inner,
        condition: rendered.condition.clone(),
        post_join_limit: shard_cap,
        partition_columns: dimension.partition_columns.clone(),
        storage: scan_storage_for_side(&dimension.effective_storage, inputs)?,
    };

    let spec = join_fan_out_scan_spec(
        fact,
        rendered.projection.clone(),
        rendered.filter.clone(),
        Some(join),
        inputs,
    )?;

    let fan_out = build_scan_driving_sql(
        &spec,
        &shards,
        &rendered.projection,
        &rendered.projection_types,
        shard_cap,
        &[],
        None,
        udf_name,
        distribute_udf_name,
    );

    let Some((keys, limit, offset)) = ordering else {
        return Ok(Some(fan_out));
    };
    let wrapped = wrap_declined_order_by(
        &fan_out,
        &rendered.projection,
        rendered.projection.len(),
        &keys,
        limit,
        offset,
    );
    // The wrapper returns its input unchanged when no key rendered; emitting the bare fan-out
    // would answer an advertised ORDER_BY_COLUMN with unordered rows, so fall back instead.
    debug_assert_ne!(
        wrapped, fan_out,
        "an Ordered window must render an ORDER BY"
    );
    Ok((wrapped != fan_out).then_some(wrapped))
}

/// A group key that cannot be rendered is a hard error.
fn qualified_join_group_by(
    pushdown_req: &Json,
    legs: &JoinLegs,
) -> Result<Option<String>, UdfError> {
    let keys = match pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
    {
        Some(keys) => keys,
        None => return Ok(None),
    };
    let mut parts = Vec::with_capacity(keys.len());
    for key in keys {
        parts.push(
            render_expression_qualified(key, legs)
                .map_err(unattributable_decline)?
                .ok_or_else(|| {
                    join_render_decline(
                        "a GROUP BY key could not be rendered for the qualified N-scan join",
                    )
                })?,
        );
    }
    Ok(Some(parts.join(", ")))
}

/// Unrenderable is a hard error: dropping it would return wrong rows.
fn qualified_join_having(pushdown_req: &Json, legs: &JoinLegs) -> Result<Option<String>, UdfError> {
    match pushdown_req.get("having").filter(|h| !h.is_null()) {
        Some(having) => Ok(Some(
            render_expression_qualified(having, legs)
                .map_err(unattributable_decline)?
                .ok_or_else(|| {
                    join_render_decline(
                        "HAVING could not be rendered for the qualified N-scan join",
                    )
                })?,
        )),
        None => Ok(None),
    }
}

/// An unrenderable element (or missing sort flags) is a hard error: Exasol does not
/// re-sort a delegated ordering.
fn qualified_join_order_by(
    pushdown_req: &Json,
    legs: &JoinLegs,
) -> Result<Option<String>, UdfError> {
    let elements = match pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
    {
        Some(elements) => elements,
        None => return Ok(None),
    };
    let decline = || {
        join_render_decline("an ORDER BY key could not be rendered for the qualified N-scan join")
    };
    let mut parts = Vec::with_capacity(elements.len());
    for element in elements {
        let (ascending, nulls_last) = parse_sort_flags(element).ok_or_else(decline)?;
        let expr = element.get("expression").ok_or_else(decline)?;
        let rendered = render_expression_qualified(expr, legs)
            .map_err(unattributable_decline)?
            .ok_or_else(decline)?;
        parts.push(render_ordered(&rendered, ascending, nulls_last));
    }
    Ok(Some(parts.join(", ")))
}

/// The shared inner-scan projection for both decline wrappers (#160).
///
/// With no select list it must not narrow: a genuine `SELECT *` is validated positionally
/// against the full base row (`04000` otherwise). Live Exasol sends an absent `selectList`
/// (captured via `EXPLAIN VIRTUAL`) while the protocol documents an empty one, so absent,
/// `null`, `[]`, and non-arrays are all accepted.
///
/// Otherwise walks every clause via [`referenced_clause_values`] with the Unicode fold.
/// Falls back to the first column when nothing is referenced (an empty EMITS is invalid),
/// unlike [`referenced_leg_columns`], which falls back to all columns.
pub(in super::super) fn referenced_column_projection(
    pushdown_req: &Json,
    all_cols: &[(String, String)],
) -> (Vec<ProjectionItem>, Vec<String>) {
    if !matches!(pushdown_req.get("selectList"), Some(Json::Array(list)) if !list.is_empty()) {
        return (
            all_cols
                .iter()
                .map(|(name, _)| ProjectionItem::Column(name.clone()))
                .collect(),
            all_cols.iter().map(|(_, ty)| ty.clone()).collect(),
        );
    }

    let mut names = std::collections::HashSet::new();
    referenced_clause_values(pushdown_req, |v| collect_all_column_names(v, &mut names));

    let mut cols = Vec::new();
    let mut types = Vec::new();
    for (name, ty) in all_cols {
        if names.contains(name) {
            cols.push(ProjectionItem::Column(name.clone()));
            types.push(ty.clone());
        }
    }
    if cols.is_empty()
        && let Some((name, ty)) = all_cols.first()
    {
        cols.push(ProjectionItem::Column(name.clone()));
        types.push(ty.clone());
    }
    (cols, types)
}

/// The pair is constructed together because its positional alignment is load-bearing: both
/// feed the wrapper universe and the `EMITS (...)` clause, and a shorter list truncates both.
#[derive(Debug)]
pub(in super::super) struct FanOutProjection<'a> {
    pub spec: &'a ScanSpec,
    pub proj_types: &'a [String],
}

impl<'a> FanOutProjection<'a> {
    pub(in super::super) fn new(
        spec: &'a ScanSpec,
        proj_types: &'a [String],
    ) -> Result<Self, UdfError> {
        if spec.common.projection.len() != proj_types.len() {
            return Err(UdfError::User(format!(
                "the fan-out projection carries {} item(s) but {} declared type(s); the two \
                 must stay positionally aligned",
                spec.common.projection.len(),
                proj_types.len()
            )));
        }
        Ok(Self { spec, proj_types })
    }
}

/// The N-scan fallback at N = 1 for an aggregate request that could not be decomposed
/// (grouped, or single-group multi/mixed `COUNT(DISTINCT)`): Exasol computes the aggregate
/// over the raw fan-out aliased `LHS_T0`.
///
/// `declined_filter` is a predicate DataFusion cannot render, applied as the wrapper's own
/// Exasol-dialect `WHERE`: since the fan-out is aggregate-, sort-, and LIMIT-free, it
/// restricts the rows the outer clauses consume. Callers must leave `fan_out_spec.filter`
/// `None` alongside it so it applies exactly once.
pub(in super::super) fn build_qualified_single_table_fallback_sql<E: Clone + Into<FileEntry>>(
    request: &Json,
    pushdown_req: &Json,
    fan_out: &FanOutProjection<'_>,
    shards: &[Vec<E>],
    udf_name: &str,
    distribute_udf_name: &str,
    declined_filter: Option<&Json>,
) -> Result<String, UdfError> {
    let fan_out_spec = fan_out.spec;
    let proj_types = fan_out.proj_types;

    let legs = JoinLegs::for_single_scan(request);
    let alias = legs.leg_alias(0);

    let all_cols: Vec<(String, String)> = fan_out_spec
        .common
        .projection
        .iter()
        .zip(proj_types.iter())
        .filter_map(|(item, ty)| match item {
            ProjectionItem::Column(name) => Some((name.clone(), ty.clone())),
            ProjectionItem::Expr { .. } => None,
        })
        .collect();
    let cols_per_leg = vec![all_cols];

    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown_req, &legs, &cols_per_leg)?;

    let proj_cols = fan_out_spec.common.projection.clone();
    let fan_out = build_scan_driving_sql(
        fan_out_spec,
        shards,
        &proj_cols,
        proj_types,
        None,
        &[],
        None,
        udf_name,
        distribute_udf_name,
    );

    let where_clause = match declined_filter {
        None => None,
        Some(tree) => render_self_applied_where(tree, &legs, "a declined WHERE predicate")?,
    };

    let mut sql = format!(
        "SELECT {select} FROM ({fan_out}) AS {}",
        quote_ident(&alias)
    );
    if let Some(clause) = &where_clause {
        sql.push_str(&format!(" WHERE {clause}"));
    }
    sql.push_str(&trailing);
    Ok(sql)
}

/// `filter` must be `None` when `declined_filter` is set. Projection is decided by the select
/// list, not the decline, so a declined filter over a real select list keeps the narrowing
/// (#160), which matters here since the fan-out ships every row.
#[allow(clippy::too_many_arguments)]
pub(in super::super) fn qualified_single_table_fallback_pushdown(
    request: &Json,
    pushdown_req: &Json,
    base: &CommonScanSpec,
    filter: Option<String>,
    shards: &[Vec<FileEntry>],
    col_types: &[(String, String)],
    udf_name: &str,
    distribute_udf_name: &str,
    declined_filter: Option<&Json>,
) -> Result<Json, UdfError> {
    let (fb_proj_cols, fb_proj_types) = referenced_column_projection(pushdown_req, col_types);
    let fan_out_spec = ScanSpec {
        common: CommonScanSpec {
            projection: fb_proj_cols,
            filter,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: false,
            ..base.clone()
        },
        files: vec![],
    };
    let fan_out = FanOutProjection::new(&fan_out_spec, &fb_proj_types)?;
    let sql = build_qualified_single_table_fallback_sql(
        request,
        pushdown_req,
        &fan_out,
        shards,
        udf_name,
        distribute_udf_name,
        declined_filter,
    )?;
    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

#[cfg(test)]
#[path = "sql_builders_tests.rs"]
mod tests;

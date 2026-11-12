use crate::adapter::ResolvedConnectionConfig;
use crate::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, ProjectionItem, ScanSpec, ScanStorage, SortKey,
    StorageBackend, relation_alias, render_ordered,
};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::SqlFragment;

use super::super::shard_paths::relativize_shards_to_root;
use super::super::support::{
    ColumnTypes, ProjectedColumn, PushdownRequest, ScanTranslator, build_scan_driving_sql,
    collect_all_column_names, extract_limit, extract_offset, output_select_list, project_columns,
    projection_items, quote_ident, render_limit_offset, scan_storage_for, shard_count,
};
use super::super::topn::{ParsedSortKey, sort_flags, wrap_declined_order_by};
use super::attribution::JoinLegs;
use super::planning::{
    DetectedJoin, JoinSides, JoinWindowPlan, ResolvedJoinSide, classify_join_window,
};
use super::rendering::{
    conjoin_filters, cross_leg_residual_filter, leg_local_filter, projection_item_select_sql,
    referenced_clause_values, referenced_leg_columns, render_expression_qualified,
    screen_leg_filter, select_item_type,
};

/// Exasol rejects a longer identifier (`maximum length of identifier exceeded`, verified live
/// with `SELECT 1 AS "<129 chars>"`); the limit counts characters, not bytes.
const EXASOL_MAX_IDENTIFIER_CHARS: usize = 128;

/// The request rendered against the leg binding: each column of the condition, filter,
/// projection, and window is a (relation alias, column) pair.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RenderedJoinPushdown {
    pub condition: String,
    /// Never a declined filter: a decline forfeits the broadcast plan entirely.
    pub filter: Option<String>,
    pub projection: Vec<ProjectedColumn>,
    /// Classified from the qualified ORDER BY, so each key carries its own leg's pair.
    pub(in super::super) window: JoinWindowPlan,
}

/// `Ok(None)` is a clean decline to the N-scan fallback when the condition won't render, the
/// projection widened to the full row (#196), a projected output name exceeds Exasol's
/// identifier limit, or the WHERE filter declines through [`ScanTranslator::split_where`]: broadcast
/// has no outer WHERE to apply a declined predicate.
///
/// Every column of the condition, filter, select list, and ORDER BY is qualified through
/// [`JoinLegs`] with its leg's alias before anything reads it, and the column-type map is the
/// resolved `sides`' maps keyed by the same pairs, so legs may share column names: the scan
/// aliases each relation with the alias the join block carries for it.
pub(crate) fn render_broadcast_join(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
) -> Result<Option<RenderedJoinPushdown>, UdfError> {
    let legs = join.legs();
    let condition_tree = legs.qualify(&join.conditions[0]);
    let qualified = pushdown.with_json(qualify_broadcast_clauses(pushdown.json(), &legs));
    let col_types = ColumnTypes::per_relation(
        sides
            .iter()
            .enumerate()
            .map(|(leg, side)| (legs.leg_alias(leg), &side.column_types)),
    );
    let rendered_clauses = BROADCAST_CLAUSES
        .into_iter()
        .filter_map(|clause| qualified.json().get(clause));
    let scan = ScanTranslator::over(
        &col_types,
        &std::iter::once(&condition_tree)
            .chain(rendered_clauses)
            .collect::<Vec<_>>(),
    )?;
    // Not the filter renderer, which would suppress a trivially-true boolean. A declined condition
    // loses broadcast eligibility and takes the N-scan fallback (#587).
    let Some(condition) = scan.expression(&condition_tree).map(|f| f.sql) else {
        return Ok(None);
    };
    let filter_json = qualified.json().get("filter").filter(|f| !f.is_null());
    let (filter, declined) = scan.split_where(filter_json);
    if declined.is_some() {
        return Ok(None);
    }

    let (projection, widened) = project_columns(&qualified, scan)?;
    // A widened projection is the full two-table row, the wrong shape for a broadcast fan-out
    // (#196).
    if widened
        || projection
            .iter()
            .any(|column| exceeds_identifier_limit(&column.item))
    {
        return Ok(None);
    }

    Ok(Some(RenderedJoinPushdown {
        condition,
        filter,
        projection,
        window: classify_join_window(qualified.json()),
    }))
}

/// The request clauses the broadcast plan renders, so the ones it qualifies and type-checks.
const BROADCAST_CLAUSES: [&str; 3] = ["selectList", "filter", "orderBy"];

fn qualify_broadcast_clauses(pushdown_req: &Json, legs: &JoinLegs) -> Json {
    let mut qualified = pushdown_req.clone();
    for clause in BROADCAST_CLAUSES {
        if let Some(value) = pushdown_req.get(clause) {
            qualified[clause] = legs.qualify(value);
        }
    }
    qualified
}

fn exceeds_identifier_limit(item: &ProjectionItem) -> bool {
    match item {
        ProjectionItem::Column(column) => {
            column.output_name().chars().count() > EXASOL_MAX_IDENTIFIER_CHARS
        }
        ProjectionItem::Expr { .. } => false,
    }
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
    cols_per_leg: &[&ColumnTypes],
) -> Vec<ProjectionItem> {
    cols_per_leg
        .iter()
        .enumerate()
        .flat_map(|(leg, cols)| {
            let alias = quote_ident(&legs.leg_alias(leg));
            cols.iter().map(move |(column, _)| ProjectionItem::Expr {
                expr: format!("{alias}.{}", quote_ident(&column.name)),
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

fn join_render_decline(clause: &str) -> UdfError {
    UdfError::User(format!(
        "join pushdown declined: {clause}; this is a hard error, not a native re-plan"
    ))
}

/// An absent/empty select list projects every column of every leg. An item without a stated type
/// is cast to its declared type: Exasol sends `''` and `NULL` alike as `literal_null` (#587).
fn n_scan_join_select_items(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
    cols_per_leg: &[&ColumnTypes],
) -> Result<Vec<ProjectionItem>, UdfError> {
    match pushdown.json().get("selectList") {
        Some(Json::Array(list)) if !list.is_empty() => {
            let mut fragments = Vec::with_capacity(list.len());
            for item in list {
                let sql = render_expression_qualified(item, legs);
                fragments.push(SqlFragment::new(
                    sql,
                    select_item_type(item, legs, cols_per_leg),
                ));
            }
            let items =
                output_select_list(fragments, pushdown.select_types()).ok_or_else(|| {
                    join_render_decline("the select list and its declared types differ in length")
                })?;
            Ok(items
                .into_iter()
                .map(|expr| ProjectionItem::Expr { expr })
                .collect())
        }
        _ => Ok(n_full_row_qualified_items(legs, cols_per_leg)),
    }
}

/// Shared by the N-scan wrapper and the grouped single-table fallback. The window renders
/// last so it applies after the sort.
struct OuterWrapperClauses {
    select: String,
    trailing: String,
}

fn outer_wrapper_clauses(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
    cols_per_leg: &[&ColumnTypes],
) -> Result<OuterWrapperClauses, UdfError> {
    let pushdown_req = pushdown.json();
    let select_items = n_scan_join_select_items(pushdown, legs, cols_per_leg)?;
    let group_by = qualified_join_group_by(pushdown_req, legs);
    let having = qualified_join_having(pushdown_req, legs);
    let order_by = qualified_join_order_by(pushdown_req, legs);
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
/// Each leg receives the leg-local conjuncts [`screen_leg_filter`] renders; every other conjunct,
/// declined ones included, goes to the outer WHERE, so the fan-out loop runs first.
///
#[allow(clippy::too_many_arguments)]
pub(in super::super) fn build_n_scan_join_sql(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    inputs: &JoinScanRequestConfig<'_>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    let cols_per_side: Vec<&ColumnTypes> = sides.iter().map(|side| &side.column_types).collect();

    let legs = join.legs();

    let conditions: Vec<String> = join
        .conditions
        .iter()
        .map(|cond| render_expression_qualified(cond, &legs))
        .collect();

    let pushdown_req = pushdown.json();
    let where_filter = pushdown_req.get("filter").filter(|f| !f.is_null());

    // All conditions go in as one array so `referenced_leg_columns` keeps any side column a
    // condition references.
    let all_conditions = Json::Array(join.conditions.clone());
    let mut fan_outs = Vec::with_capacity(sides.len());
    let mut declined: Option<Json> = None;
    for (i, side) in sides.iter().enumerate() {
        let narrowed =
            referenced_leg_columns(pushdown_req, &all_conditions, &legs, i, cols_per_side[i]);
        let (side_filter, side_declined) =
            match where_filter.and_then(|f| leg_local_filter(f, &legs, i)) {
                Some(side_local) => screen_leg_filter(&side_local, cols_per_side[i])?,
                None => (None, None),
            };
        // Disjoint by attribution, so no conjunct is double-applied.
        declined = conjoin_filters(declined, side_declined);
        fan_outs.push(build_side_fan_out_sql(
            side,
            &narrowed,
            side_filter,
            inputs,
            udf_name,
            distribute_udf_name,
        )?);
    }

    // With the per-side leg filters, these partition the request's filter.
    let residual = conjoin_filters(
        where_filter.and_then(|f| cross_leg_residual_filter(f, &legs)),
        declined,
    );
    let filter = residual.map(|tree| render_expression_qualified(&tree, &legs));

    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown, &legs, &cols_per_side)?;

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
    pub(in super::super) binary_as_string: bool,
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
            binary_as_string: inputs.binary_as_string,
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

/// `filter` is the leg filter [`screen_leg_filter`] rendered. `columns` must expose every
/// column any outer clause references.
pub(super) fn build_side_fan_out_sql(
    side: &ResolvedJoinSide,
    columns: &ColumnTypes,
    filter: Option<String>,
    inputs: &JoinScanRequestConfig<'_>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    let projection = columns.projection();

    let shards = shard_side(side, inputs);
    let spec = join_fan_out_scan_spec(side, projection_items(&projection), filter, None, inputs)?;
    Ok(build_scan_driving_sql(
        &spec,
        &shards,
        &projection,
        None,
        None,
        udf_name,
        distribute_udf_name,
    ))
}

/// Binds by the (relation alias, column) pair: `ORDER BY b.ID` never binds to a projected
/// `a.ID`, which carries the other leg's values.
fn bound_sort_key(key: &ParsedSortKey, projection: &[ProjectedColumn]) -> Option<SortKey> {
    let ParsedSortKey::Column(key) = key else {
        return None;
    };
    projection
        .iter()
        .any(|projected| matches!(&projected.item, ProjectionItem::Column(column) if *column == key.column))
        .then(|| key.clone())
}

struct BroadcastWindowPlacement {
    shard_cap: Option<u64>,
    shard_order_by: Vec<SortKey>,
    merge_cap: Option<u64>,
    wrapper: Option<(Vec<ParsedSortKey>, Option<u64>, u64)>,
}

/// `None` falls through to the N-scan wrapper: the window itself requires it, or an
/// `Ordered` key is absent from `projection`.
fn place_broadcast_window(
    window: JoinWindowPlan,
    projection: &[ProjectedColumn],
) -> Option<BroadcastWindowPlacement> {
    match window {
        JoinWindowPlan::Unbounded => Some(BroadcastWindowPlacement {
            shard_cap: None,
            shard_order_by: Vec::new(),
            merge_cap: None,
            wrapper: None,
        }),
        JoinWindowPlan::BareLimit(n) => Some(BroadcastWindowPlacement {
            shard_cap: Some(n),
            shard_order_by: Vec::new(),
            merge_cap: Some(n),
            wrapper: None,
        }),
        JoinWindowPlan::Ordered {
            keys,
            limit,
            offset,
        } => {
            // Only now is there a projection to check: the wrapper's ORDER BY binds against
            // emitted columns and this path appends no hidden ones.
            let bound_keys = keys
                .iter()
                .map(|key| bound_sort_key(key, projection))
                .collect::<Option<Vec<SortKey>>>()?;
            let (shard_cap, shard_order_by) = match (offset, limit) {
                (0, Some(n)) => (Some(n), bound_keys),
                _ => (None, Vec::new()),
            };
            Some(BroadcastWindowPlacement {
                shard_cap,
                shard_order_by,
                merge_cap: None,
                wrapper: Some((keys, limit, offset)),
            })
        }
        JoinWindowPlan::ExasolPostProcessed => None,
    }
}

/// `None` falls through to the N-scan wrapper, never an error.
///
/// The fact side is sharded; the dimension's full file list rides once in the common blob's
/// [`JoinSpec`], so every shard joins node-locally with no exchange. Each side carries its
/// own storage because a vended credential is scoped to its table. The window always applies
/// after the join; [`place_broadcast_window`] decides where it lands. The join block names each
/// relation by the alias its leg was rendered with, so the scan binds every qualified column to
/// the side that owns it.
pub(in super::super) fn build_broadcast_join_sql(
    sides: &JoinSides,
    rendered: &RenderedJoinPushdown,
    inputs: &JoinScanRequestConfig<'_>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<Option<String>, UdfError> {
    let Some(placement) = place_broadcast_window(rendered.window.clone(), &rendered.projection)
    else {
        return Ok(None);
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
        fact_alias: relation_alias(sides.fact_leg()),
        dimension_alias: relation_alias(sides.dimension_leg),
        condition: rendered.condition.clone(),
        post_join_limit: placement.shard_cap,
        post_join_order_by: placement.shard_order_by,
        partition_columns: dimension.partition_columns.clone(),
        storage: scan_storage_for_side(&dimension.effective_storage, inputs)?,
    };

    let spec = join_fan_out_scan_spec(
        fact,
        projection_items(&rendered.projection),
        rendered.filter.clone(),
        Some(join),
        inputs,
    )?;

    let fan_out = build_scan_driving_sql(
        &spec,
        &shards,
        &rendered.projection,
        placement.merge_cap,
        None,
        udf_name,
        distribute_udf_name,
    );

    let Some((keys, limit, offset)) = placement.wrapper else {
        return Ok(Some(fan_out));
    };
    Ok(Some(wrap_declined_order_by(
        &fan_out,
        &spec.common.projection,
        spec.common.projection.len(),
        &keys,
        limit,
        offset,
    )))
}

fn qualified_join_group_by(pushdown_req: &Json, legs: &JoinLegs) -> Option<String> {
    let keys = pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;
    let parts: Vec<String> = keys
        .iter()
        .map(|key| render_expression_qualified(key, legs))
        .collect();
    Some(parts.join(", "))
}

fn qualified_join_having(pushdown_req: &Json, legs: &JoinLegs) -> Option<String> {
    pushdown_req
        .get("having")
        .filter(|h| !h.is_null())
        .map(|having| render_expression_qualified(having, legs))
}

fn qualified_join_order_by(pushdown_req: &Json, legs: &JoinLegs) -> Option<String> {
    let elements = pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;
    let parts: Vec<String> = elements
        .iter()
        .map(|element| {
            let (ascending, nulls_last) = sort_flags(element);
            let rendered = render_expression_qualified(&element["expression"], legs);
            render_ordered(&rendered, ascending, nulls_last)
        })
        .collect();
    Some(parts.join(", "))
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
pub(in super::super) fn referenced_columns(
    pushdown_req: &Json,
    all_cols: &ColumnTypes,
) -> ColumnTypes {
    if !matches!(pushdown_req.get("selectList"), Some(Json::Array(list)) if !list.is_empty()) {
        return all_cols.clone();
    }

    let mut names = std::collections::HashSet::new();
    referenced_clause_values(pushdown_req, |v| collect_all_column_names(v, &mut names));

    let referenced = all_cols.narrowed(|column| names.contains(&column.name));
    if referenced.is_empty() {
        return all_cols.first_only();
    }
    referenced
}

/// `columns` is the spec's projection, the `EMITS (...)` clause, and the wrapper's full row.
/// The constructor derives the projection from it, so the three stay aligned by construction.
#[derive(Debug)]
pub(in super::super) struct FanOutProjection<'a> {
    spec: ScanSpec,
    columns: &'a ColumnTypes,
}

impl<'a> FanOutProjection<'a> {
    /// `common`'s own projection is replaced.
    pub(in super::super) fn new(common: CommonScanSpec, columns: &'a ColumnTypes) -> Self {
        let spec = ScanSpec {
            common: CommonScanSpec {
                projection: projection_items(&columns.projection()),
                ..common
            },
            files: vec![],
        };
        Self { spec, columns }
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
    pushdown: &PushdownRequest,
    fan_out: &FanOutProjection<'_>,
    shards: &[Vec<E>],
    udf_name: &str,
    distribute_udf_name: &str,
    declined_filter: Option<&Json>,
) -> Result<String, UdfError> {
    let fan_out_spec = &fan_out.spec;

    let legs = JoinLegs::for_single_scan(request);
    let alias = legs.leg_alias(0);

    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown, &legs, &[fan_out.columns])?;

    let fan_out = build_scan_driving_sql(
        fan_out_spec,
        shards,
        &fan_out.columns.projection(),
        None,
        None,
        udf_name,
        distribute_udf_name,
    );

    let where_clause = declined_filter.map(|tree| render_expression_qualified(tree, &legs));

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
    pushdown: &PushdownRequest,
    base: &CommonScanSpec,
    filter: Option<String>,
    shards: &[Vec<FileEntry>],
    col_types: &ColumnTypes,
    udf_name: &str,
    distribute_udf_name: &str,
    declined_filter: Option<&Json>,
) -> Result<Json, UdfError> {
    let referenced = referenced_columns(pushdown.json(), col_types);
    let fan_out = FanOutProjection::new(
        CommonScanSpec {
            filter,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: false,
            ..base.clone()
        },
        &referenced,
    );
    let sql = build_qualified_single_table_fallback_sql(
        request,
        pushdown,
        &fan_out,
        shards,
        udf_name,
        distribute_udf_name,
        declined_filter,
    )?;
    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

pub(in super::super) struct EmptyRelation {
    legs: JoinLegs,
    cols_per_leg: Vec<ColumnTypes>,
}

impl EmptyRelation {
    pub(in super::super) fn single_scan(request: &Json, col_types: &ColumnTypes) -> Self {
        Self {
            legs: JoinLegs::for_single_scan(request),
            cols_per_leg: vec![col_types.clone()],
        }
    }

    pub(super) fn join(join: &DetectedJoin, cols_per_leg: Vec<ColumnTypes>) -> Self {
        Self {
            legs: join.legs(),
            cols_per_leg,
        }
    }

    pub(in super::super) fn pushdown(&self, pushdown: &PushdownRequest) -> Result<Json, UdfError> {
        let OuterWrapperClauses { select, trailing } = outer_wrapper_clauses(
            pushdown,
            &self.legs,
            &self.cols_per_leg.iter().collect::<Vec<_>>(),
        )?;
        let from = self
            .cols_per_leg
            .iter()
            .enumerate()
            .map(|(leg, cols)| {
                let nulls = cols
                    .iter()
                    .map(|(column, column_type)| {
                        format!(
                            "CAST(NULL AS {}) AS {}",
                            column_type.exa_type,
                            quote_ident(&column.name)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "(SELECT {nulls} FROM DUAL WHERE 1=0) AS {}",
                    quote_ident(&self.legs.leg_alias(leg))
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {select} FROM {from}{trailing}");
        Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
    }
}

#[cfg(test)]
#[path = "sql_builders_tests.rs"]
mod tests;

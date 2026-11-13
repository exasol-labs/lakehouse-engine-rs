use crate::adapter::ResolvedConnectionConfig;
use crate::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, ProjectionItem, ScanSpec, ScanStorage, SortKey,
    StorageBackend, relation_alias, render_ordered,
};
use crate::types::exasol_type::exasol_type_to_json;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::{ColumnNode, SqlFragment};

use super::super::shard_paths::relativize_shards_to_root;
use super::super::support::{
    AggregationType, ColumnTypes, Emitted, OutputContract, ProjectedColumn, PushdownRequest,
    RowProjection, ScanTranslator, aggregation_type, build_scan_driving_sql, extract_limit,
    extract_offset, output_select_list, project_columns, projection_items, pushdown_response,
    quote_ident, render_limit_offset, scan_storage_for, shard_count,
};
use super::super::topn::{ParsedSortKey, sort_flags, wrap_declined_order_by};
use super::attribution::JoinLegs;
use super::planning::{
    DetectedJoin, JoinSides, JoinWindowPlan, ResolvedJoinSide, classify_join_window,
};
use super::rendering::{
    LegColumns, broadcast_width_bounded, conjoin_filters, cross_leg_residual_filter,
    fan_out_emission, leg_columns_named_in, leg_emission, leg_local_filter, named_emission,
    projection_item_select_sql, referenced_clause_values, render_expression_qualified,
    screen_leg_filter, select_item_type, with_placeholder, wrapper_clause_values,
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

/// `Ok(None)` is a clean decline to the N-scan fallback when the condition won't render, a
/// select-list item is one only Exasol computes (#196), a projected output name exceeds Exasol's
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
    // A declined condition loses broadcast eligibility and takes the N-scan fallback (#587).
    let Some(condition) = scan.expression(&condition_tree).map(|f| f.sql) else {
        return Ok(None);
    };
    let filter_json = qualified.json().get("filter").filter(|f| !f.is_null());
    let (filter, declined) = scan.split_where(filter_json);
    if declined.is_some() {
        return Ok(None);
    }

    let emitted = broadcast_emission(pushdown, join, sides)?;
    let RowProjection::Direct(projection) = project_columns(&qualified, scan, &emitted)? else {
        return Ok(None);
    };
    if projection
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

/// What the broadcast fan-out emits, each column qualified with its leg's alias: the columns the
/// select list names. The scan reads a join key anyway, so one serves a list that names none.
fn broadcast_emission(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
) -> Result<Emitted, UdfError> {
    let legs = join.legs();
    let above: Vec<&Json> = pushdown.json().get("selectList").into_iter().collect();
    let scanned = scanned_legs(sides);
    let keys = leg_columns_named_in(&join.conditions, &legs, 0, &sides[0].column_types);
    let per_leg = fan_out_emission(pushdown.output(), &above, &legs, &scanned, &keys)?;
    Ok(qualified_emission(&legs, &per_leg))
}

fn scanned_legs(sides: &[ResolvedJoinSide]) -> Vec<(usize, &ColumnTypes)> {
    sides
        .iter()
        .map(|side| &side.column_types)
        .enumerate()
        .collect()
}

/// `per_leg` as one emission, each column qualified with its leg's alias.
pub(super) fn qualified_emission(legs: &JoinLegs, per_leg: &[Emitted]) -> Emitted {
    Emitted::per_relation(
        per_leg
            .iter()
            .enumerate()
            .map(|(leg, emitted)| (legs.leg_alias(leg), emitted)),
    )
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
/// join point with none renders `ON 1=1`.
fn build_n_scan_join_from(
    fan_outs: &[String],
    legs: &JoinLegs,
    raw_conditions: &[Json],
    conditions: &[String],
) -> String {
    let mut on_at: Vec<Vec<String>> = vec![Vec::new(); fan_outs.len()];
    for (raw, rendered) in raw_conditions.iter().zip(conditions) {
        on_at[legs.attachment_leg(raw)].push(rendered.clone());
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
    from
}

fn n_full_row_items(
    legs: &JoinLegs,
    emitted: &[Emitted],
    naming: LegColumns,
) -> Vec<ProjectionItem> {
    emitted
        .iter()
        .enumerate()
        .flat_map(|(leg, cols)| {
            cols.columns()
                .iter()
                .map(move |(column, _)| ProjectionItem::Expr {
                    expr: naming.column(legs, leg, &column.name),
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

/// The wrapper's select list over the columns the fan-outs emit. An item without a stated type
/// is cast to its declared type: Exasol sends `''` and `NULL` alike as `literal_null` (#587).
fn n_scan_join_select_items(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
    emitted: &[Emitted],
    naming: LegColumns,
) -> Result<Vec<ProjectionItem>, UdfError> {
    let list = match pushdown.output() {
        OutputContract::WholeRow => {
            return pushdown.whole_row(n_full_row_items(legs, emitted, naming));
        }
        // Exasol counts the rows and rejects any width but one column.
        OutputContract::AnyOneColumn => {
            let row = n_full_row_items(legs, emitted, naming);
            return Ok(row.into_iter().take(1).collect());
        }
        OutputContract::Items => pushdown.select_items().unwrap_or_default(),
    };
    let cols_per_leg: Vec<&ColumnTypes> = emitted.iter().map(Emitted::columns).collect();
    let fragments = list.iter().map(|item| {
        SqlFragment::new(
            naming.render(item, legs),
            select_item_type(item, legs, &cols_per_leg),
        )
    });
    let items = output_select_list(fragments, pushdown.select_types()).ok_or_else(|| {
        join_render_decline("the select list and its declared types differ in length")
    })?;
    Ok(items
        .into_iter()
        .map(|expr| ProjectionItem::Expr { expr })
        .collect())
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
    emitted: &[Emitted],
    naming: LegColumns,
) -> Result<OuterWrapperClauses, UdfError> {
    let pushdown_req = pushdown.json();
    let select_items = n_scan_join_select_items(pushdown, legs, emitted, naming)?;
    if select_items.is_empty() {
        return Err(UdfError::User(
            "internal adapter error: the pushdown wrapper selects no column".into(),
        ));
    }
    let group_by = join_group_by(pushdown_req, legs, naming);
    let having = join_having(pushdown_req, legs, naming);
    let order_by = join_order_by(pushdown_req, legs, naming);
    let limit = extract_limit(pushdown_req);
    let offset = extract_offset(pushdown_req);
    // Exasol withholds `limit` when it cannot delegate an ordering, so an offset never arrives
    // without `orderBy` (#191, verified live). Pinned rather than declined: no request reaches it.
    debug_assert!(
        offset == 0 || order_by.is_some(),
        "fact 5: Exasol withholds `limit` entirely when it cannot delegate an ordering, \
         so a non-zero offset must never arrive without a non-empty orderBy"
    );

    let select = select_items
        .iter()
        .map(projection_item_select_sql)
        .collect::<Vec<_>>()
        .join(", ");

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

/// What each leg's fan-out of the N-scan wrapper emits: its columns that a join condition or a
/// request clause names. The wrapper renders the whole WHERE, so the filter counts.
pub(in super::super) fn n_scan_emission(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
) -> Result<Vec<Emitted>, UdfError> {
    let legs = join.legs();
    let above: Vec<&Json> = join
        .conditions
        .iter()
        .chain(referenced_clause_values(pushdown.json()))
        .collect();
    sides
        .iter()
        .enumerate()
        .map(|(leg, side)| leg_emission(pushdown.output(), &above, &legs, leg, &side.column_types))
        .collect()
}

/// What the qualified single-table wrapper's fan-out emits: the columns a request clause names.
pub(in super::super) fn single_table_emission(
    request: &Json,
    pushdown: &PushdownRequest,
    col_types: &ColumnTypes,
) -> Result<Emitted, UdfError> {
    let legs = JoinLegs::for_single_scan(request);
    let above: Vec<&Json> = referenced_clause_values(pushdown.json()).collect();
    leg_emission(pushdown.output(), &above, &legs, 0, col_types)
}

/// The sole unaccelerated fallback: Exasol rebuilds the join over one fan-out per leg. A declined
/// or cross-leg conjunct goes to the outer WHERE, so it applies once, after the fan-outs.
#[allow(clippy::too_many_arguments)]
pub(in super::super) fn build_n_scan_join_sql(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    emitted: &[Emitted],
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

    let mut fan_outs = Vec::with_capacity(sides.len());
    let mut declined: Option<Json> = None;
    for (i, side) in sides.iter().enumerate() {
        let (side_filter, side_declined) =
            match where_filter.and_then(|f| leg_local_filter(f, &legs, i)) {
                Some(side_local) => screen_leg_filter(&side_local, cols_per_side[i])?,
                None => (None, None),
            };
        // Disjoint by attribution, so no conjunct is double-applied.
        declined = conjoin_filters(declined, side_declined);
        fan_outs.push(build_side_fan_out_sql(
            side,
            &emitted[i],
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
        outer_wrapper_clauses(pushdown, &legs, emitted, LegColumns::Relations)?;

    let from = build_n_scan_join_from(&fan_outs, &legs, &join.conditions, &conditions);

    let mut sql = format!("SELECT {select} FROM {from}");
    if let Some(f) = &filter {
        sql.push_str(&format!(" WHERE ({f})"));
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
/// `join.storage`. A post-join cap rides in [`JoinSpec::post_join_limit`], never `common.limit`.
fn join_fan_out_scan_spec(
    primary: &ResolvedJoinSide,
    filter: Option<String>,
    join: Option<JoinSpec>,
    inputs: &JoinScanRequestConfig<'_>,
) -> Result<ScanSpec, UdfError> {
    let storage = scan_storage_for_side(&primary.effective_storage, inputs)?;
    Ok(ScanSpec {
        common: CommonScanSpec {
            table_root: primary.table_root.clone(),
            // The fan-out builder writes the projection beside its `EMITS`.
            projection: Vec::new(),
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

/// `filter` is the leg filter [`screen_leg_filter`] rendered.
pub(super) fn build_side_fan_out_sql(
    side: &ResolvedJoinSide,
    columns: &Emitted,
    filter: Option<String>,
    inputs: &JoinScanRequestConfig<'_>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    let projection = columns.projection();

    let shards = shard_side(side, inputs);
    let spec = join_fan_out_scan_spec(side, filter, None, inputs)?;
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
        JoinWindowPlan::BroadcastUnservable => None,
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

    let spec = join_fan_out_scan_spec(fact, rendered.filter.clone(), Some(join), inputs)?;

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
        &projection_items(&rendered.projection),
        rendered.projection.len(),
        &keys,
        limit,
        offset,
    )))
}

/// The broadcast plan of an eligible two-leg join. An aggregating request is aggregated, sorted,
/// and limited only by an outer Exasol SELECT, never per shard. `None` falls through to the N-scan.
pub(in super::super) fn build_broadcast_plan_sql(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    broadcast: &JoinSides,
    inputs: &JoinScanRequestConfig<'_>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<Option<String>, UdfError> {
    let aggregation_input = match aggregation_type(pushdown.json()) {
        Some(_) => {
            match aggregation_input_request(pushdown, join, sides, broadcast.dimension_leg)? {
                Some(input) => Some(input),
                None => return Ok(None),
            }
        }
        None => None,
    };
    let fan_out_request = aggregation_input
        .as_ref()
        .map_or(pushdown, |(request, _)| request);
    let Some(rendered) = render_broadcast_join(fan_out_request, join, sides)? else {
        return Ok(None);
    };
    let Some(sql) =
        build_broadcast_join_sql(broadcast, &rendered, inputs, udf_name, distribute_udf_name)?
    else {
        return Ok(None);
    };
    let Some((_, emitted)) = aggregation_input else {
        return Ok(Some(sql));
    };
    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown, &join.legs(), &emitted, LegColumns::FanOutOutput)?;
    Ok(Some(format!("SELECT {select} FROM ({sql}){trailing}")))
}

/// The fan-out request of an aggregating broadcast beside the columns it emits per leg. `None`
/// when a dimension column the aggregation reads is not [`broadcast_width_bounded`].
fn aggregation_input_request(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    dimension_leg: usize,
) -> Result<Option<(PushdownRequest, Vec<Emitted>)>, UdfError> {
    let pushdown_req = pushdown.json();
    let legs = join.legs();
    let above: Vec<&Json> = wrapper_clause_values(pushdown_req).collect();
    let scanned = scanned_legs(sides);
    let read = named_emission(pushdown.output(), &above, &legs, &scanned)?;
    let filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    if !broadcast_width_bounded(filter, &legs, dimension_leg, read[dimension_leg].columns()) {
        return Ok(None);
    }
    // After the width rule, which judges only what the aggregation reads. The scan reads a
    // join key anyway, so one serves an aggregation that reads no column.
    let keys = leg_columns_named_in(&join.conditions, &legs, 0, &sides[0].column_types);
    let read = with_placeholder(read, &scanned, &keys)?;
    let (select_list, select_types): (Vec<Json>, Vec<Json>) = join
        .tables
        .iter()
        .zip(&read)
        .flat_map(|(leaf, emitted)| {
            emitted.columns().iter().map(|(column, column_type)| {
                let node = ColumnNode::to_json(
                    &column.name,
                    Some(&leaf.table_name),
                    leaf.table_alias.as_deref(),
                );
                (node, exasol_type_to_json(&column_type.exa_type))
            })
        })
        .unzip();
    let mut json = serde_json::json!({
        "selectList": select_list,
        "selectListDataTypes": select_types,
    });
    if let Some(filter) = pushdown_req.get("filter") {
        json["filter"] = filter.clone();
    }
    Ok(Some((PushdownRequest::parse(json)?, read)))
}

fn join_group_by(pushdown_req: &Json, legs: &JoinLegs, naming: LegColumns) -> Option<String> {
    let keys = match pushdown_req.get("groupBy").and_then(|v| v.as_array()) {
        Some(keys) if !keys.is_empty() => keys,
        // Exasol may strip every aggregate from a single-group select list (#627).
        _ if aggregation_type(pushdown_req) == Some(AggregationType::SingleGroup) => {
            return Some("()".to_string());
        }
        _ => return None,
    };
    let parts: Vec<String> = keys.iter().map(|key| naming.render(key, legs)).collect();
    Some(parts.join(", "))
}

fn join_having(pushdown_req: &Json, legs: &JoinLegs, naming: LegColumns) -> Option<String> {
    pushdown_req
        .get("having")
        .filter(|h| !h.is_null())
        .map(|having| naming.render(having, legs))
}

fn join_order_by(pushdown_req: &Json, legs: &JoinLegs, naming: LegColumns) -> Option<String> {
    let elements = pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;
    let parts: Vec<String> = elements
        .iter()
        .map(|element| {
            let (ascending, nulls_last) = sort_flags(element);
            let rendered = naming.render(&element["expression"], legs);
            render_ordered(&rendered, ascending, nulls_last)
        })
        .collect();
    Some(parts.join(", "))
}

/// The N-scan fallback at N = 1: Exasol computes the request over the raw fan-out, which emits
/// `emitted`. `declined_filter` is the wrapper's WHERE, so `fan_out.filter` must then be `None`.
#[allow(clippy::too_many_arguments)]
pub(in super::super) fn build_qualified_single_table_fallback_sql<E: Clone + Into<FileEntry>>(
    request: &Json,
    pushdown: &PushdownRequest,
    fan_out: CommonScanSpec,
    emitted: &Emitted,
    shards: &[Vec<E>],
    udf_name: &str,
    distribute_udf_name: &str,
    declined_filter: Option<&Json>,
) -> Result<String, UdfError> {
    let legs = JoinLegs::for_single_scan(request);
    let alias = legs.leg_alias(0);

    let OuterWrapperClauses { select, trailing } = outer_wrapper_clauses(
        pushdown,
        &legs,
        std::slice::from_ref(emitted),
        LegColumns::Relations,
    )?;

    let fan_out = build_scan_driving_sql(
        &ScanSpec {
            common: fan_out,
            files: vec![],
        },
        shards,
        &emitted.projection(),
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

/// `filter` must be `None` when `declined_filter` is set. The emission is decided by the request's
/// clauses, not the decline, so a declined filter over a real select list keeps the narrowing
/// (#160), which matters here since the fan-out ships every row.
#[allow(clippy::too_many_arguments)]
pub(in super::super) fn qualified_single_table_fallback_pushdown(
    request: &Json,
    pushdown: &PushdownRequest,
    base: &CommonScanSpec,
    filter: Option<String>,
    shards: &[Vec<FileEntry>],
    emitted: &Emitted,
    udf_name: &str,
    distribute_udf_name: &str,
    declined_filter: Option<&Json>,
) -> Result<Json, UdfError> {
    let fan_out = CommonScanSpec {
        filter,
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        distinct: false,
        ..base.clone()
    };
    let sql = build_qualified_single_table_fallback_sql(
        request,
        pushdown,
        fan_out,
        emitted,
        shards,
        udf_name,
        distribute_udf_name,
        declined_filter,
    )?;
    Ok(pushdown_response(sql))
}

/// The empty relation of a request whose scan has no file: one empty subquery per leg, exposing
/// the columns the leg's fan-out would emit, so the wrapper over it is the non-empty wrapper.
pub(in super::super) struct EmptyRelation {
    legs: JoinLegs,
    emitted: Vec<Emitted>,
}

impl EmptyRelation {
    pub(in super::super) fn single_scan(request: &Json, emitted: Emitted) -> Self {
        Self {
            legs: JoinLegs::for_single_scan(request),
            emitted: vec![emitted],
        }
    }

    pub(super) fn join(join: &DetectedJoin, emitted: Vec<Emitted>) -> Self {
        Self {
            legs: join.legs(),
            emitted,
        }
    }

    pub(in super::super) fn pushdown(
        &self,
        pushdown: &PushdownRequest,
    ) -> Result<String, UdfError> {
        let OuterWrapperClauses { select, trailing } =
            outer_wrapper_clauses(pushdown, &self.legs, &self.emitted, LegColumns::Relations)?;
        let from = self
            .emitted
            .iter()
            .enumerate()
            .map(|(leg, cols)| {
                let nulls = cols
                    .columns()
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
        Ok(format!("SELECT {select} FROM {from}{trailing}"))
    }
}

#[cfg(test)]
#[path = "sql_builders_tests.rs"]
mod tests;

use crate::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, ProjectionItem, ScanSpec, SortKey,
    relation_alias, render_ordered,
};
use crate::types::exasol_type::ExasolType;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::{ColumnNode, SqlFragment};

use super::super::context::{PushdownContext, Shards};
use super::super::request::{AggregationType, Clause, OutputContract, PushdownRequest};
use super::super::shard_paths::relativize_shards_to_root;
use super::super::support::{
    ColumnTypes, Emitted, ProjectedColumn, RowProjection, ScanTranslator, build_scan_driving_sql,
    output_select_list, project_columns, projection_items, pushdown_response, quote_ident,
    render_limit_offset,
};
use super::super::topn::{ParsedSortKey, sort_flags, wrap_declined_order_by};
use super::attribution::JoinLegs;
use super::planning::{
    DetectedJoin, JoinSides, JoinWindowPlan, ResolvedJoinSide, classify_join_window,
};
use super::rendering::{
    LegColumns, fan_out_emission, leg_columns_named_in, leg_emission, named_emission,
    projection_item_select_sql, render_expression_qualified, select_item_type, with_placeholder,
};
use super::where_placement::WherePlacement;

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
/// identifier limit, or a conjunct of the WHERE filter declines through
/// [`WherePlacement::single_scan`]: broadcast has no outer WHERE to apply a declined predicate.
///
/// Every column of the condition, filter, select list, and ORDER BY is qualified through
/// [`JoinLegs`] with its leg's alias before anything reads it, and the column-type map is the
/// resolved `sides`' maps keyed by the same pairs, so legs may share column names: the scan
/// aliases each relation with the alias the join block carries for it.
pub(crate) fn render_broadcast_join(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    context: &PushdownContext<'_>,
) -> Result<Option<RenderedJoinPushdown>, UdfError> {
    let emitted = broadcast_emission(pushdown, join, sides)?;
    render_broadcast_join_emitting(pushdown, join, sides, &emitted, context)
}

/// [`render_broadcast_join`] of a fan-out that emits `emitted` per leg.
fn render_broadcast_join_emitting(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    emitted: &[Emitted],
    context: &PushdownContext<'_>,
) -> Result<Option<RenderedJoinPushdown>, UdfError> {
    let legs = join.legs();
    let condition_tree = legs.qualify(&join.conditions[0]);
    let qualified = pushdown.map_clauses(&BROADCAST_CLAUSES, |clause| legs.qualify(clause));
    let col_types = ColumnTypes::per_relation(
        sides
            .iter()
            .enumerate()
            .map(|(leg, side)| (legs.leg_alias(leg), &side.column_types)),
    );
    let rendered_clauses = qualified.clauses(&BROADCAST_CLAUSES);
    let scan = ScanTranslator::over(
        &col_types,
        context.version,
        &std::iter::once(&condition_tree)
            .chain(rendered_clauses)
            .collect::<Vec<_>>(),
    )?;
    // A declined condition loses broadcast eligibility and takes the N-scan fallback (#587). It
    // renders as a filter does, so its string keys stay bare columns the hash join reads.
    let Some(condition) = scan.filter(&condition_tree) else {
        return Ok(None);
    };
    let placement = WherePlacement::single_scan(scan, &qualified);
    if placement.wrapper().is_some() {
        return Ok(None);
    }
    let filter = placement.scan_filter(0);

    let emitted = qualified_emission(&legs, emitted);
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
        window: classify_join_window(&qualified),
    }))
}

/// What the broadcast fan-out emits per leg: the columns the select list names.
fn broadcast_emission(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
) -> Result<Vec<Emitted>, UdfError> {
    let above: Vec<&Json> = pushdown.clauses(&[Clause::SelectList]).collect();
    fan_out_emission(
        pushdown.output(),
        &above,
        &join.legs(),
        &scanned_legs(sides),
        &first_leg_join_keys(join, sides),
    )
}

/// The scan reads a join key anyway, so one serves a fan-out whose clauses name no column.
fn first_leg_join_keys(join: &DetectedJoin, sides: &[ResolvedJoinSide]) -> ColumnTypes {
    leg_columns_named_in(&join.conditions, &join.legs(), 0, &sides[0].column_types)
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
const BROADCAST_CLAUSES: [Clause; 3] = [Clause::SelectList, Clause::Filter, Clause::OrderBy];

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
fn shard_side(side: &ResolvedJoinSide, context: &PushdownContext<'_>) -> Shards {
    context.shards(side.scan.files.clone(), &side.scan.table_root)
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
    context: &PushdownContext<'_>,
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
            select_item_type(item, legs, &cols_per_leg, context),
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
    context: &PushdownContext<'_>,
) -> Result<OuterWrapperClauses, UdfError> {
    let select_items = n_scan_join_select_items(pushdown, legs, emitted, naming, context)?;
    if select_items.is_empty() {
        return Err(UdfError::User(
            "internal adapter error: the pushdown wrapper selects no column".into(),
        ));
    }
    let group_by = join_group_by(pushdown, legs, naming);
    let having = join_having(pushdown, legs, naming);
    let order_by = join_order_by(pushdown, legs, naming);
    let limit = pushdown.limit();
    let offset = pushdown.offset();
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

/// What each leg's fan-out emits under a wrapper that applies `placement`'s part of the WHERE:
/// its columns that a join condition, a wrapper clause, or that part names. A conjunct a leg's
/// scan applies adds no column (#680).
pub(in super::super) fn n_scan_emission(
    pushdown: &PushdownRequest,
    placement: &WherePlacement,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
) -> Result<Vec<Emitted>, UdfError> {
    let legs = join.legs();
    let above: Vec<&Json> = join
        .conditions
        .iter()
        .chain(wrapper_reads(pushdown, placement))
        .collect();
    sides
        .iter()
        .enumerate()
        .map(|(leg, side)| leg_emission(pushdown.output(), &above, &legs, leg, &side.column_types))
        .collect()
}

/// What the qualified single-table wrapper's fan-out emits under a wrapper that applies
/// `placement`'s part of the WHERE: the columns a wrapper clause or that part names.
pub(in super::super) fn single_table_emission(
    request: &Json,
    pushdown: &PushdownRequest,
    placement: &WherePlacement,
    col_types: &ColumnTypes,
) -> Result<Emitted, UdfError> {
    let legs = JoinLegs::for_single_scan(request);
    let above: Vec<&Json> = wrapper_reads(pushdown, placement).collect();
    leg_emission(pushdown.output(), &above, &legs, 0, col_types)
}

/// The single owner of which parts of a request an Exasol wrapper reads.
fn wrapper_reads<'a>(
    pushdown: &'a PushdownRequest,
    placement: &'a WherePlacement,
) -> impl Iterator<Item = &'a Json> {
    pushdown
        .clauses(&Clause::WRAPPER)
        .chain(placement.wrapper())
}

/// The sole unaccelerated fallback: Exasol rebuilds the join over one fan-out per leg. A declined
/// or cross-leg conjunct goes to the outer WHERE, so it applies once, after the fan-outs.
pub(in super::super) fn build_n_scan_join_sql(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    context: &PushdownContext<'_>,
) -> Result<String, UdfError> {
    let legs = join.legs();

    let conditions: Vec<String> = join
        .conditions
        .iter()
        .map(|cond| render_expression_qualified(cond, &legs))
        .collect();

    let placement = WherePlacement::n_scan(pushdown, &legs, sides, context)?;
    let filter = placement
        .wrapper()
        .map(|tree| render_expression_qualified(tree, &legs));
    let emitted = n_scan_emission(pushdown, &placement, join, sides)?;

    let fan_outs = sides
        .iter()
        .zip(&emitted)
        .enumerate()
        .map(|(leg, (side, emitted))| {
            build_side_fan_out_sql(side, emitted, placement.scan_filter(leg), context)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown, &legs, &emitted, LegColumns::Relations, context)?;

    let from = build_n_scan_join_from(&fan_outs, &legs, &join.conditions, &conditions);

    let mut sql = format!("SELECT {select} FROM {from}");
    if let Some(f) = &filter {
        sql.push_str(&format!(" WHERE ({f})"));
    }
    sql.push_str(&trailing);
    Ok(sql)
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
    context: &PushdownContext<'_>,
) -> Result<ScanSpec, UdfError> {
    let common = context.common_spec(&primary.scan)?;
    Ok(ScanSpec {
        common: CommonScanSpec {
            filter,
            join,
            ..common
        },
        files: vec![],
    })
}

/// `filter` is the leg filter [`WherePlacement::n_scan`] placed in this leg.
pub(super) fn build_side_fan_out_sql(
    side: &ResolvedJoinSide,
    columns: &Emitted,
    filter: Option<String>,
    context: &PushdownContext<'_>,
) -> Result<String, UdfError> {
    let projection = columns.projection();

    let shards = shard_side(side, context);
    let spec = join_fan_out_scan_spec(side, filter, None, context)?;
    Ok(build_scan_driving_sql(
        &spec,
        &shards,
        &projection,
        None,
        None,
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
    context: &PushdownContext<'_>,
) -> Result<Option<String>, UdfError> {
    let Some(placement) = place_broadcast_window(rendered.window.clone(), &rendered.projection)
    else {
        return Ok(None);
    };

    let fact = &sides.fact;
    let dimension = &sides.dimension;

    let shards = shard_side(fact, context);

    let join = JoinSpec {
        table_root: dimension.scan.table_root.clone(),
        files: relativize_files_to_root(dimension.scan.files.clone(), &dimension.scan.table_root),
        logical_schema: dimension.scan.logical_schema.clone(),
        name_mapping: dimension.scan.name_mapping.clone(),
        join_type: JoinType::Inner,
        fact_alias: relation_alias(sides.fact_leg()),
        dimension_alias: relation_alias(sides.dimension_leg),
        condition: rendered.condition.clone(),
        post_join_limit: placement.shard_cap,
        post_join_order_by: placement.shard_order_by,
        partition_columns: dimension.scan.partition_columns.clone(),
        storage: context.scan_storage(&dimension.scan.effective_storage)?,
    };

    let spec = join_fan_out_scan_spec(fact, rendered.filter.clone(), Some(join), context)?;

    let fan_out = build_scan_driving_sql(
        &spec,
        &shards,
        &rendered.projection,
        placement.merge_cap,
        None,
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

/// The broadcast plan of a two-leg join. `aggregation_reads` is [`aggregation_reads`] of the
/// request: an aggregating request is aggregated, sorted, and limited only by an outer Exasol
/// SELECT, never per shard. `None` is a render decline and falls through to the N-scan.
pub(in super::super) fn build_broadcast_plan_sql(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    broadcast: &JoinSides,
    aggregation_reads: Option<Vec<Emitted>>,
    context: &PushdownContext<'_>,
) -> Result<Option<String>, UdfError> {
    let aggregation_input = aggregation_reads
        .map(|read| aggregation_input_request(pushdown, join, sides, read))
        .transpose()?;
    let rendered = match &aggregation_input {
        Some((request, emitted)) => {
            render_broadcast_join_emitting(request, join, sides, emitted, context)?
        }
        None => render_broadcast_join(pushdown, join, sides, context)?,
    };
    let Some(rendered) = rendered else {
        return Ok(None);
    };
    let Some(sql) = build_broadcast_join_sql(broadcast, &rendered, context)? else {
        return Ok(None);
    };
    let Some((_, emitted)) = aggregation_input else {
        return Ok(Some(sql));
    };
    let OuterWrapperClauses { select, trailing } = outer_wrapper_clauses(
        pushdown,
        &join.legs(),
        &emitted,
        LegColumns::FanOutOutput,
        context,
    )?;
    Ok(Some(format!("SELECT {select} FROM ({sql}){trailing}")))
}

/// Per leg, the columns the select list, `GROUP BY`, `HAVING`, and `ORDER BY` of an aggregating
/// request read. `None` when the request does not aggregate.
pub(in super::super) fn aggregation_reads(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
) -> Result<Option<Vec<Emitted>>, UdfError> {
    if pushdown.aggregation_type().is_none() {
        return Ok(None);
    }
    let above: Vec<&Json> = pushdown.clauses(&Clause::WRAPPER).collect();
    named_emission(
        pushdown.output(),
        &above,
        &join.legs(),
        &scanned_legs(sides),
    )
    .map(Some)
}

/// The fan-out request of an aggregating broadcast beside the columns it emits per leg, from the
/// columns the aggregation `read`s.
fn aggregation_input_request(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    read: Vec<Emitted>,
) -> Result<(PushdownRequest, Vec<Emitted>), UdfError> {
    let read = with_placeholder(
        read,
        &scanned_legs(sides),
        &first_leg_join_keys(join, sides),
    )?;
    let (select_list, select_types): (Vec<Json>, Vec<ExasolType>) = join
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
                (node, column_type.exa_type)
            })
        })
        .unzip();
    Ok((pushdown.selecting(select_list, &select_types)?, read))
}

fn join_group_by(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
    naming: LegColumns,
) -> Option<String> {
    let keys = match pushdown.group_by() {
        keys if !keys.is_empty() => keys,
        // Exasol may strip every aggregate from a single-group select list (#627).
        _ if pushdown.aggregation_type() == Some(AggregationType::SingleGroup) => {
            return Some("()".to_string());
        }
        _ => return None,
    };
    let parts: Vec<String> = keys.iter().map(|key| naming.render(key, legs)).collect();
    Some(parts.join(", "))
}

fn join_having(pushdown: &PushdownRequest, legs: &JoinLegs, naming: LegColumns) -> Option<String> {
    pushdown.having().map(|having| naming.render(having, legs))
}

fn join_order_by(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
    naming: LegColumns,
) -> Option<String> {
    let elements = pushdown.order_by();
    if elements.is_empty() {
        return None;
    }
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

/// The N-scan fallback at N = 1: Exasol computes the request over the raw fan-out. The wrapper's
/// WHERE is `placement`'s wrapper part, and the fan-out emits only the columns the wrapper reads,
/// so a conjunct the scan applies adds none.
pub(in super::super) fn build_qualified_single_table_fallback_sql<E: Clone + Into<FileEntry>>(
    request: &Json,
    pushdown: &PushdownRequest,
    fan_out: CommonScanSpec,
    columns: &ColumnTypes,
    shards: &Shards<E>,
    placement: &WherePlacement,
    context: &PushdownContext<'_>,
) -> Result<String, UdfError> {
    let legs = JoinLegs::for_single_scan(request);
    let alias = legs.leg_alias(0);
    let emitted = single_table_emission(request, pushdown, placement, columns)?;

    let OuterWrapperClauses { select, trailing } = outer_wrapper_clauses(
        pushdown,
        &legs,
        std::slice::from_ref(&emitted),
        LegColumns::Relations,
        context,
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
    );

    let where_clause = placement
        .wrapper()
        .map(|tree| render_expression_qualified(tree, &legs));

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

/// The qualified single-table wrapper over a fan-out that applies the pushed part of the WHERE.
/// The fan-out ships every row, so its columns narrow to what the wrapper reads (#160).
pub(in super::super) fn qualified_single_table_fallback_pushdown(
    request: &Json,
    pushdown: &PushdownRequest,
    base: &CommonScanSpec,
    placement: &WherePlacement,
    shards: &Shards,
    columns: &ColumnTypes,
    context: &PushdownContext<'_>,
) -> Result<Json, UdfError> {
    let fan_out = CommonScanSpec {
        filter: placement.scan_filter(0),
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        distinct: false,
        ..base.clone()
    };
    let sql = build_qualified_single_table_fallback_sql(
        request, pushdown, fan_out, columns, shards, placement, context,
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
        context: &PushdownContext<'_>,
    ) -> Result<String, UdfError> {
        let OuterWrapperClauses { select, trailing } = outer_wrapper_clauses(
            pushdown,
            &self.legs,
            &self.emitted,
            LegColumns::Relations,
            context,
        )?;
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

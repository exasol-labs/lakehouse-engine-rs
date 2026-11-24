use crate::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, ProjectionItem, ScanSpec, SortKey,
    relation_alias, render_ordered,
};
use crate::types::exasol_type::exasol_type_to_json;
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
use super::plan::{JoinPlan, LegGroup, tail_runs_in_shards};
use super::planning::{DetectedJoin, JoinWindowPlan, ResolvedJoinSide, classify_join_window};
use super::rendering::{
    ConditionPlacement, LegNaming, fan_out_emission, leg_column_types, leg_columns_named_in,
    leg_emission, place_conditions, projection_item_select_sql, render_expression_qualified,
    screen_pair_filter, select_item_type,
};
use super::where_placement::WherePlacement;

/// Exasol rejects a longer identifier (`maximum length of identifier exceeded`, verified live
/// with `SELECT 1 AS "<129 chars>"`); the limit counts characters, not bytes.
const EXASOL_MAX_IDENTIFIER_CHARS: usize = 128;

/// A broadcast pair's scan, rendered against the leg binding: each column of the condition,
/// filter, projection, and window is a (relation alias, column) pair.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RenderedJoinPushdown {
    pub condition: String,
    /// The conjuncts the scan renders. A declined one runs in Exasol's WHERE.
    pub filter: Option<String>,
    pub projection: Vec<ProjectedColumn>,
    /// Classified from the qualified ORDER BY, so each key carries its own leg's pair.
    pub(in super::super) window: JoinWindowPlan,
}

/// The two-leg broadcast of `pair` with its window in the shards: Exasol joins and filters
/// nothing over it. `Ok(None)` when the scan declines the condition, a select-list item (#196), or a conjunct
/// of the WHERE, or a projected output name exceeds Exasol's identifier limit.
///
/// Every column of the condition, filter, select list, and ORDER BY is qualified through
/// [`JoinLegs`] with its leg's alias before anything reads it, and the column-type map is the
/// resolved `sides`' maps keyed by the same pairs, so legs may share column names: the scan
/// aliases each relation with the alias the join block carries for it.
pub(in super::super) fn render_broadcast_join(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    pair: &LegGroup,
    context: &PushdownContext<'_>,
) -> Result<Option<RenderedJoinPushdown>, UdfError> {
    let legs = join.legs();
    let plan = JoinPlan::broadcasting(2, pair);
    let filter = pushdown.filter();
    let placement = place_conditions(filter, &join.conditions, &legs, &plan);
    let (Some(keys), None) = (&placement.pair_keys, &placement.residual) else {
        return Ok(None);
    };
    let filter = match filter {
        None => None,
        Some(filter) => match screen_pair_filter(filter, &legs, sides, context)? {
            (sql, None) => sql,
            (_, Some(_)) => return Ok(None),
        },
    };
    let qualified = legs.qualify_request(pushdown);
    let above: Vec<&Json> = pushdown.clauses(&[Clause::SelectList]).collect();
    let emitted = pair_emission(
        pushdown.output(),
        &above,
        &legs,
        sides,
        &plan.fan_outs()[0],
        keys,
    )?;
    let Some((condition, RowProjection::Direct(projection))) =
        render_pair(&qualified, &legs, sides, keys, &emitted, context)?
    else {
        return Ok(None);
    };
    Ok(Some(RenderedJoinPushdown {
        condition,
        filter,
        projection,
        window: classify_join_window(&qualified),
    }))
}

/// The SQL of `plan`: one fan-out per entry of the plan, joined by Exasol's `INNER JOIN … ON`
/// chain, with each conjunct a fan-out's scan cannot run in Exasol's WHERE. `Ok(None)` when the
/// plan's broadcast pair does not render ([`render_pair`]); the N-scan always renders.
///
/// The request's window and aggregation run in the shards only for a two-leg broadcast that
/// leaves Exasol nothing to join or filter ([`render_broadcast_join`]); otherwise Exasol runs
/// them over the fan-outs.
pub(in super::super) fn render_join_plan(
    pushdown: &PushdownRequest,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    plan: &JoinPlan,
    context: &PushdownContext<'_>,
) -> Result<Option<String>, UdfError> {
    let legs = join.legs();
    let conditions = place_conditions(pushdown.filter(), &join.conditions, &legs, plan);
    let placement = WherePlacement::of_plan(&conditions, plan, &legs, sides, context)?;
    let ConditionPlacement { pair_keys, on, .. } = conditions;

    if tail_runs_in_shards(plan, placement.wrapper(), pushdown)
        && let Some(rendered) =
            render_broadcast_join(pushdown, join, sides, &plan.fan_outs()[0], context)?
    {
        return build_pair_fan_out_sql(sides, &plan.fan_outs()[0], &rendered, context);
    }

    // A conjunct a fan-out's scan applies adds no column to it (#680).
    let n_scan_above: Vec<&Json> = join
        .conditions
        .iter()
        .chain(wrapper_reads(pushdown, &placement))
        .collect();
    let pair_above: Vec<&Json> = wrapper_reads(pushdown, &placement)
        .chain(on.iter().flatten())
        .collect();
    let mut emitted: Vec<Option<Emitted>> = vec![None; sides.len()];
    let mut fan_out_sqls = Vec::with_capacity(plan.fan_outs().len());
    for (position, fan_out) in plan.fan_outs().iter().enumerate() {
        let scan_filter = placement.scan_filter(position);
        if fan_out.is_plain() {
            let leg = fan_out.sharded;
            let columns = leg_emission(
                pushdown.output(),
                &n_scan_above,
                &legs,
                leg,
                &sides[leg].column_types,
            )?;
            fan_out_sqls.push(build_side_fan_out_sql(
                &sides[leg],
                &columns,
                scan_filter,
                context,
            )?);
            emitted[leg] = Some(columns);
            continue;
        }
        let keys = pair_keys
            .as_ref()
            .expect("a plan with a broadcast pair places its join keys");
        let pair_emitted =
            pair_emission(pushdown.output(), &pair_above, &legs, sides, fan_out, keys)?;
        let Some(fan_out_sql) = render_pair_fan_out(
            join,
            &legs,
            sides,
            fan_out,
            keys,
            scan_filter,
            &pair_emitted,
            context,
        )?
        else {
            return Ok(None);
        };
        fan_out_sqls.push(fan_out_sql);
        for leg in fan_out.legs() {
            emitted[leg] = Some(pair_emitted[leg].clone());
        }
    }
    let emitted: Vec<Emitted> = emitted
        .into_iter()
        .map(|columns| columns.expect("every leg lies in one fan-out"))
        .collect();

    let naming = LegNaming::of_plan(plan, sides.len());
    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown, &legs, &emitted, &naming, context)?;
    let from = match fan_out_sqls.as_slice() {
        [single] => format!("({single})"),
        _ => join_chain(plan, &fan_out_sqls, &on, &legs, &naming),
    };
    let mut sql = format!("SELECT {select} FROM {from}");
    if let Some(residual) = placement.wrapper() {
        sql.push_str(&format!(" WHERE ({})", naming.render(residual, &legs)));
    }
    sql.push_str(&trailing);
    Ok(Some(sql))
}

/// Whether the scan renders `conjunct`, one top-level WHERE conjunct, against its legs' own
/// column types.
pub(super) fn renders_in_scan(
    conjunct: &Json,
    legs: &JoinLegs,
    sides: &[ResolvedJoinSide],
    context: &PushdownContext<'_>,
) -> Result<bool, UdfError> {
    let col_types = leg_column_types(legs, sides);
    let qualified = legs.qualify(conjunct);
    let scan = ScanTranslator::over(&col_types, context.version, &[&qualified])?;
    let (_, declined) = scan.screen_conjuncts(conjunct, |conjunct| legs.qualify(conjunct));
    Ok(declined.is_none())
}

/// The pair's join keys and the projection of `qualified`'s select list over the columns the
/// pair emits per leg. `Ok(None)` when the scan declines a key, which keeps the pair off the
/// broadcast (#587), or a projected output name exceeds Exasol's identifier limit.
fn render_pair(
    qualified: &PushdownRequest,
    legs: &JoinLegs,
    sides: &[ResolvedJoinSide],
    keys: &Json,
    emitted: &[Emitted],
    context: &PushdownContext<'_>,
) -> Result<Option<(String, RowProjection)>, UdfError> {
    let condition_tree = legs.qualify(keys);
    let col_types = leg_column_types(legs, sides);
    let rendered_clauses = qualified.clauses(&[Clause::SelectList, Clause::OrderBy]);
    let scan = ScanTranslator::over(
        &col_types,
        context.version,
        &std::iter::once(&condition_tree)
            .chain(rendered_clauses)
            .collect::<Vec<_>>(),
    )?;
    // It renders as a filter does, so its string keys stay bare columns the hash join reads.
    let Some(condition) = scan.filter(&condition_tree) else {
        return Ok(None);
    };
    let projection = project_columns(qualified, scan, &qualified_emission(legs, emitted))?;
    if let RowProjection::Direct(projection) = &projection
        && projection
            .iter()
            .any(|column| exceeds_identifier_limit(&column.item))
    {
        return Ok(None);
    }
    Ok(Some((condition, projection)))
}

/// What the pair's fan-out emits per leg, empty for a leg outside it: the columns `above` names,
/// or one join key of its first leg in FROM-tree order when they name none.
fn pair_emission(
    output: OutputContract,
    above: &[&Json],
    legs: &JoinLegs,
    sides: &[ResolvedJoinSide],
    pair: &LegGroup,
    keys: &Json,
) -> Result<Vec<Emitted>, UdfError> {
    let pair_legs = pair.legs();
    let scanned: Vec<(usize, &ColumnTypes)> = pair_legs
        .iter()
        .map(|&leg| (leg, &sides[leg].column_types))
        .collect();
    let first = pair_legs[0];
    let prefer = leg_columns_named_in([keys], legs, first, &sides[first].column_types);
    let named = fan_out_emission(output, above, legs, &scanned, &prefer)?;
    let mut per_leg = sides
        .iter()
        .map(|side| side.column_types.named(|_| false))
        .collect::<Result<Vec<_>, _>>()?;
    for (leg, columns) in pair_legs.into_iter().zip(named) {
        per_leg[leg] = columns;
    }
    Ok(per_leg)
}

/// The pair's fan-out when Exasol runs the request over it: it emits plain columns and carries no
/// window. `Ok(None)` as [`render_pair`].
#[allow(clippy::too_many_arguments)]
fn render_pair_fan_out(
    join: &DetectedJoin,
    legs: &JoinLegs,
    sides: &[ResolvedJoinSide],
    pair: &LegGroup,
    keys: &Json,
    filter: Option<String>,
    emitted: &[Emitted],
    context: &PushdownContext<'_>,
) -> Result<Option<String>, UdfError> {
    let request = plain_columns_request(join, emitted)?;
    let qualified = legs.qualify_request(&request);
    let Some((condition, RowProjection::Direct(projection))) =
        render_pair(&qualified, legs, sides, keys, emitted, context)?
    else {
        return Ok(None);
    };
    let rendered = RenderedJoinPushdown {
        condition,
        filter,
        projection,
        window: JoinWindowPlan::Unbounded,
    };
    build_pair_fan_out_sql(sides, pair, &rendered, context)
}

/// A request selecting each column of `emitted`, under its own leg, so the pair projects it.
fn plain_columns_request(
    join: &DetectedJoin,
    emitted: &[Emitted],
) -> Result<PushdownRequest, UdfError> {
    let (select_list, select_types): (Vec<Json>, Vec<Json>) = join
        .tables
        .iter()
        .zip(emitted)
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
    PushdownRequest::parse(serde_json::json!({
        "selectList": select_list,
        "selectListDataTypes": select_types,
    }))
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

fn exceeds_identifier_limit(item: &ProjectionItem) -> bool {
    match item {
        ProjectionItem::Column(column) => {
            column.output_name().chars().count() > EXASOL_MAX_IDENTIFIER_CHARS
        }
        ProjectionItem::Expr { .. } => false,
    }
}

/// Each fan-out is a relation aliased by its sharded leg, joined at its chain position with the
/// conditions placed there. A join point with none renders `ON 1=1`.
fn join_chain(
    plan: &JoinPlan,
    fan_out_sqls: &[String],
    on: &[Vec<Json>],
    legs: &JoinLegs,
    naming: &LegNaming,
) -> String {
    let alias = |position: usize| quote_ident(&relation_alias(plan.fan_outs()[position].sharded));
    let mut from = format!("({}) AS {}", fan_out_sqls[0], alias(0));
    for (position, fan_out_sql) in fan_out_sqls.iter().enumerate().skip(1) {
        let conditions = if on[position].is_empty() {
            "1=1".to_string()
        } else {
            on[position]
                .iter()
                .map(|condition| format!("({})", naming.render(condition, legs)))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        from.push_str(&format!(
            " INNER JOIN ({fan_out_sql}) AS {} ON {conditions}",
            alias(position)
        ));
    }
    from
}

fn n_full_row_items(emitted: &[Emitted], naming: &LegNaming) -> Vec<ProjectionItem> {
    emitted
        .iter()
        .enumerate()
        .flat_map(|(leg, cols)| {
            cols.columns()
                .iter()
                .map(move |(column, _)| ProjectionItem::Expr {
                    expr: naming.column(leg, &column.name),
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
    naming: &LegNaming,
    context: &PushdownContext<'_>,
) -> Result<Vec<ProjectionItem>, UdfError> {
    let list = match pushdown.output() {
        OutputContract::WholeRow => {
            return pushdown.whole_row(n_full_row_items(emitted, naming));
        }
        // Exasol counts the rows and rejects any width but one column.
        OutputContract::AnyOneColumn => {
            let row = n_full_row_items(emitted, naming);
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

/// Shared by the join wrapper and the grouped single-table fallback. The window renders last so
/// it applies after the sort.
struct OuterWrapperClauses {
    select: String,
    trailing: String,
}

fn outer_wrapper_clauses(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
    emitted: &[Emitted],
    naming: &LegNaming,
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

/// `filter` is the filter [`WherePlacement::of_plan`] placed in this leg's fan-out.
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

/// `None` declines the pair: the window itself requires the N-scan, or an `Ordered` key is absent
/// from `projection`.
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

/// The fan-out of a broadcast `pair`, `Ok(None)` when [`place_broadcast_window`] declines.
///
/// The fact leg is sharded; the dimension's full file list rides once in the common blob's
/// [`JoinSpec`], so every shard joins node-locally with no exchange. Each leg carries its own
/// storage because a vended credential is scoped to its table. The window always applies after
/// the join; [`place_broadcast_window`] decides where it lands. The join block names each
/// relation by the alias its leg was rendered with, so the scan binds every qualified column to
/// the leg that owns it.
pub(in super::super) fn build_pair_fan_out_sql(
    sides: &[ResolvedJoinSide],
    pair: &LegGroup,
    rendered: &RenderedJoinPushdown,
    context: &PushdownContext<'_>,
) -> Result<Option<String>, UdfError> {
    let Some(placement) = place_broadcast_window(rendered.window.clone(), &rendered.projection)
    else {
        return Ok(None);
    };

    let (fact_leg, dimension_leg) = (pair.sharded, pair.broadcast[0]);
    let fact = &sides[fact_leg];
    let dimension = &sides[dimension_leg];

    let shards = shard_side(fact, context);

    let join = JoinSpec {
        table_root: dimension.scan.table_root.clone(),
        files: relativize_files_to_root(dimension.scan.files.clone(), &dimension.scan.table_root),
        logical_schema: dimension.scan.logical_schema.clone(),
        name_mapping: dimension.scan.name_mapping.clone(),
        join_type: JoinType::Inner,
        fact_alias: relation_alias(fact_leg),
        dimension_alias: relation_alias(dimension_leg),
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

fn join_group_by(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
    naming: &LegNaming,
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

fn join_having(pushdown: &PushdownRequest, legs: &JoinLegs, naming: &LegNaming) -> Option<String> {
    pushdown.having().map(|having| naming.render(having, legs))
}

fn join_order_by(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
    naming: &LegNaming,
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
        &LegNaming::relations(1),
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
            &LegNaming::relations(self.emitted.len()),
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

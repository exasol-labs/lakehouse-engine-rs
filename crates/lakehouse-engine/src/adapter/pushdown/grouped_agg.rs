use crate::scan::spec::{AggregatePlan, FileEntry, ScanSpec, partial_column_name, render_ordered};
use crate::types::exasol_type::ExasolType;
use serde_json::Value as Json;
use vs_expression::{ColumnNode, SqlFragment, render_exasol_fragment};

use super::scalar_over_agg::{
    TypedPlans, classify_scalar_over_aggregate, fold_aggregate_plan, fold_nested_plans,
    is_aggregate_node, parse_agg_item, partial_column_type, render_scalar_over_merge,
    substitute_subtrees, with_stat_grand_means,
};
use super::support::{
    AggregationType, ColumnTypes, PushdownRequest, ScanTranslator, aggregation_type,
    build_fan_out_inner, output_select_list, quote_ident, render_limit_offset,
};
use super::topn::sort_flags;

/// Each variant carries its original `selectList` ordinal: Exasol validates the
/// outer wrapper SELECT positionally against `selectListDataTypes`.
// No `Eq`: `ScalarOverAggregate` holds a `serde_json::Value`, which can hold floats.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupedSelectItem {
    /// `group_key_slot` also indexes the scan-side `GK_{slot}` EMITS column.
    GroupKey {
        group_key_slot: usize,
        select_index: usize,
    },
    Aggregate {
        plan_slot: usize,
        select_index: usize,
    },
    /// Exasol's "count the groups" rewrite (a `literal_null`-only select list). It
    /// contributes no aggregate plan; `projection` is the literal in the Exasol dialect, never
    /// reused as a column identifier.
    Constant {
        select_index: usize,
        projection: SqlFragment,
    },
    /// Nested aggregates fold into the shared `plans`; the wrapper renders over the
    /// merged partials only.
    ScalarOverAggregate { select_index: usize, node: Json },
}

fn select_item_index(item: &GroupedSelectItem) -> usize {
    match item {
        GroupedSelectItem::GroupKey { select_index, .. }
        | GroupedSelectItem::Aggregate { select_index, .. }
        | GroupedSelectItem::Constant { select_index, .. }
        | GroupedSelectItem::ScalarOverAggregate { select_index, .. } => *select_index,
    }
}

#[derive(Debug, Clone)]
pub struct GroupedAggregateDetection {
    pub group_keys: Vec<String>,
    /// Aligned 1:1 with `group_keys`: the type each `GK_i` is emitted, merged, and output in.
    pub group_key_types: Vec<ExasolType>,
    /// Deduplicated by `AggregatePlan` equality across top-level and nested occurrences, and
    /// aligned with `plan_slot`, not with the `selectList`.
    pub aggregates: TypedPlans,
    pub select_items: Vec<GroupedSelectItem>,
}

/// Shared by `detect_group_by_aggregates` and `project_columns` so the two
/// lists cannot drift apart (#52).
const LITERAL_SELECTLIST_TYPES: &[&str] = &[
    "literal_null",
    "literal_bool",
    "literal_string",
    "literal_exactnumeric",
    "literal_double",
    "literal_date",
    "literal_timestamp",
    "literal_timestamp_utc",
    "literal_timestamputc",
];

pub(super) fn is_literal_selectlist_item(item_type: &str) -> bool {
    LITERAL_SELECTLIST_TYPES.contains(&item_type)
}

/// `None` on any unsupported shape; the caller falls back to single-group detection
/// or a row scan.
pub fn detect_group_by_aggregates(
    pushdown: &PushdownRequest,
    scan: ScanTranslator<'_>,
) -> Option<GroupedAggregateDetection> {
    let pushdown_req = pushdown.json();
    if aggregation_type(pushdown_req) != Some(AggregationType::GroupBy) {
        return None;
    }

    let group_by = pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;

    // Any group-key render failure collapses the whole result.
    let mut key_fragments = Vec::with_capacity(group_by.len());
    for node in group_by {
        key_fragments.push(scan.expression(node)?);
    }
    let group_keys: Vec<String> = key_fragments.iter().map(|key| key.sql.clone()).collect();

    let list = pushdown_req.get("selectList").and_then(|v| v.as_array())?;
    if list.is_empty() {
        return None;
    }

    let mut plans = Vec::new();
    let mut plan_types = Vec::new();
    let mut select_items = Vec::with_capacity(list.len());
    for (select_index, item) in list.iter().enumerate() {
        let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match item_type {
            "function_aggregate" => {
                let plan = parse_agg_item(item, scan)?;
                let declared = pushdown.emittable_select_type(select_index)?;
                let plan_slot =
                    fold_aggregate_plan(&mut plans, &mut plan_types, plan, Some(declared));
                select_items.push(GroupedSelectItem::Aggregate {
                    plan_slot,
                    select_index,
                });
            }
            // Exasol dialect: the constant is projected by the Exasol-parsed merge (#550).
            t if is_literal_selectlist_item(t) => {
                let projection = render_exasol_fragment(item, scan.columns());
                select_items.push(GroupedSelectItem::Constant {
                    select_index,
                    projection,
                });
            }
            _ => {
                // A group-key projection: a column or an expression rendering to a group key.
                if let Some(group_key_slot) = scan
                    .expression(item)
                    .and_then(|key| group_keys.iter().position(|gk| *gk == key.sql))
                {
                    pushdown.emittable_select_type(select_index)?;
                    select_items.push(GroupedSelectItem::GroupKey {
                        group_key_slot,
                        select_index,
                    });
                    continue;
                }
                // Otherwise a scalar wrapping aggregates; `None` declines the whole grouped
                // detection, routing to the qualified wrapper (never a bare row scan).
                let nested = classify_scalar_over_aggregate(item, scan)?;
                fold_nested_plans(&mut plans, &mut plan_types, &nested, scan);
                select_items.push(GroupedSelectItem::ScalarOverAggregate {
                    select_index,
                    node: item.clone(),
                });
            }
        }
    }

    // Typed here, before the shape is chosen, so a key or nested aggregate without an exact
    // type declines the split, and HAVING and ORDER BY can reference typed `GK_*` keys.
    let group_key_types = group_key_exasol_types(pushdown, &key_fragments, &select_items)?;
    let aggregates = TypedPlans::new(plans, plan_types, scan.columns())?;
    Some(GroupedAggregateDetection {
        group_keys,
        group_key_types,
        aggregates,
        select_items,
    })
}

/// A projected key takes its `selectListDataTypes` entry, an unprojected one its exact type
/// (`exact_expression_type`), a constant [`CONSTANT_KEY_EMIT_TYPE`]. `None` for any other key
/// (#476, #551).
fn group_key_exasol_types(
    pushdown: &PushdownRequest,
    group_keys: &[SqlFragment],
    select_items: &[GroupedSelectItem],
) -> Option<Vec<ExasolType>> {
    let mut types: Vec<Option<ExasolType>> = group_keys.iter().map(|key| key.ty).collect();
    for item in select_items {
        if let GroupedSelectItem::GroupKey {
            group_key_slot,
            select_index,
        } = item
            && let Some(ty) = pushdown.select_type(*select_index)
            && let Some(slot) = types.get_mut(*group_key_slot)
        {
            *slot = Some(ty);
        }
    }
    let group_by = pushdown.json().get("groupBy").and_then(|v| v.as_array());
    types
        .into_iter()
        .enumerate()
        .map(|(slot, typed)| {
            typed.or_else(|| {
                let node = group_by?.get(slot)?;
                let constant = node
                    .get("type")
                    .and_then(|t| t.as_str())
                    .is_some_and(is_literal_selectlist_item);
                constant.then_some(CONSTANT_KEY_EMIT_TYPE)
            })
        })
        .collect()
}

/// What HAVING and ORDER BY over the merge can reference: the merged aggregates and the `GK_*`
/// keys, which travel typed.
pub(super) struct MergeScope<'a> {
    /// Each merge cast to its aggregate's native type, so an outer scalar or comparison sees the
    /// type Exasol would (#476).
    pub aggregates: &'a TypedPlans,
    /// Rendered scan fragments, aligned with `GK_*`; empty for a single-group aggregate.
    pub group_keys: &'a [String],
}

fn group_key_name(slot: usize) -> String {
    format!("GK_{slot}")
}

fn group_key_column(slot: usize) -> String {
    quote_ident(&group_key_name(slot))
}

/// Exasol does not re-sort a delegated `ORDER BY`, so the merge sorts itself, on the keys a HAVING
/// operand reads, with no hidden output column. `None` without an `orderBy` (#198).
pub(super) fn build_grouped_order_by_clause(
    pushdown_req: &Json,
    scope: &MergeScope<'_>,
    scan: ScanTranslator<'_>,
) -> Option<GroupedOrderBy> {
    let elements = pushdown_req.get("orderBy").and_then(|v| v.as_array())?;
    if elements.is_empty() {
        return None;
    }
    let mut parts = Vec::with_capacity(elements.len());
    for element in elements {
        // Flags only: an aggregate sort key yields no `SortKey`.
        let (ascending, nulls_last) = sort_flags(element);
        let Some(ordering) = render_merge_operand(Some(&element["expression"]), scope, scan) else {
            return Some(GroupedOrderBy::Unresolvable);
        };
        parts.push(render_ordered(&ordering, ascending, nulls_last));
    }
    Some(GroupedOrderBy::Clause(parts.join(", ")))
}

/// The `GK_*` slot of an expression that renders to a group key.
fn group_key_slot(expr: &Json, scope: &MergeScope<'_>, scan: ScanTranslator<'_>) -> Option<usize> {
    if scope.group_keys.is_empty() {
        return None;
    }
    let rendered = scan.expression(expr)?.sql;
    scope.group_keys.iter().position(|gk| *gk == rendered)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum GroupedOrderBy {
    Clause(String),
    Unresolvable,
}

// Every argument is a distinct resolved plan input.
#[allow(clippy::too_many_arguments)]
pub fn build_grouped_aggregate_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    group_key_types: &[ExasolType],
    aggregates: &TypedPlans,
    select_items: &[GroupedSelectItem],
    select_types: &[ExasolType],
    limit: Option<u64>,
    offset: u64,
    scan: ScanTranslator<'_>,
    udf_name: &str,
    distribute_udf_name: &str,
    having: Option<&str>,
    order_by: Option<&str>,
) -> Option<String> {
    // Keys travel typed, so the merge groups values without reading text by NLS settings.
    let gk_emits: Vec<String> = group_key_types
        .iter()
        .enumerate()
        .map(|(i, ty)| format!("{} {ty}", group_key_column(i)))
        .collect();
    let all_emits: Vec<String> = gk_emits
        .iter()
        .chain(aggregates.emits.iter())
        .cloned()
        .collect();
    let emits = all_emits.join(", ");

    // Exasol validates this SELECT positionally, so it follows `selectList` order,
    // not the inner keys-first fan-out order.
    let mut ordered = select_items.to_vec();
    ordered.sort_by_key(select_item_index);
    let fragments = ordered
        .iter()
        .map(|item| match item {
            GroupedSelectItem::GroupKey { group_key_slot, .. } => Some(SqlFragment::new(
                group_key_column(*group_key_slot),
                group_key_types.get(*group_key_slot).copied(),
            )),
            GroupedSelectItem::Aggregate { plan_slot, .. } => aggregates.fragment(*plan_slot),
            GroupedSelectItem::Constant { projection, .. } => Some(projection.clone()),
            GroupedSelectItem::ScalarOverAggregate { node, .. } => {
                render_scalar_over_merge(node, &aggregates.plans, &aggregates.merged, scan)
            }
        })
        .collect::<Option<Vec<_>>>()?;
    let outer_select_str = output_select_list(fragments, select_types)?.join(", ");

    let outer_group_by: Vec<String> = (0..group_key_types.len()).map(group_key_column).collect();
    let outer_group_by_str = outer_group_by.join(", ");

    // The common blob is shared by all shards, so building it with `limit = None`
    // structurally keeps LIMIT out of every per-shard partial.
    let mut common_template = spec_template.clone();
    common_template.common.limit = None;
    let fan_out = build_fan_out_inner(
        &common_template,
        shards,
        &emits,
        udf_name,
        distribute_udf_name,
    );

    let from = with_stat_grand_means(&fan_out, &aggregates.plans, &outer_group_by);

    let mut sql = format!("SELECT {outer_select_str} FROM ({from}) GROUP BY {outer_group_by_str}");

    // HAVING only in the outer wrapper: per shard it would discard groups that clear
    // the threshold only after merging.
    if let Some(h) = having.filter(|h| !h.is_empty()) {
        sql.push_str(" HAVING ");
        sql.push_str(h);
    }

    if let Some(ob) = order_by.filter(|s| !s.is_empty()) {
        sql.push_str(" ORDER BY ");
        sql.push_str(ob);
    }

    sql.push_str(&render_limit_offset(limit, offset));
    Some(sql)
}

/// An unprojected constant key only groups, so any type holding its text works.
const CONSTANT_KEY_EMIT_TYPE: ExasolType = ExasolType::WIDEST_VARCHAR;

/// `None` when a partial column has no exact type.
pub(super) fn partial_emits_items(
    aggregates: &[AggregatePlan],
    col_types: &ColumnTypes,
    aggregate_types: &[ExasolType],
) -> Option<Vec<String>> {
    aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| {
            let declared = aggregate_types.get(i).copied();
            plan.kind.partial_columns().iter().map(move |col| {
                let ty = partial_column_type(plan, *col, col_types, declared)?;
                let name = partial_column_name(*col, i);
                Some(format!(r#""{name}" {ty}"#))
            })
        })
        .collect()
}

/// Rewrites each aggregate to its merged expression and each group key to its `GK_*` column.
/// `None` for an aggregate not among the plans or an unsupported node, never a dropped HAVING.
pub(super) fn render_having_over_merge(
    node: &Json,
    scope: &MergeScope<'_>,
    scan: ScanTranslator<'_>,
) -> Option<String> {
    if !node.is_object() {
        return None;
    }
    let kind = node.get("type").and_then(|t| t.as_str())?;
    let child = |key: &str| node.get(key);

    if kind == "function_aggregate" {
        let plan = parse_agg_item(node, scan)?;
        let slot = scope.aggregates.slot_of(&plan)?;
        return scope.aggregates.merged.get(slot).cloned();
    }

    match kind {
        "predicate_and" => render_having_junction(child("expressions"), scope, " AND ", scan),
        "predicate_or" => render_having_junction(child("expressions"), scope, " OR ", scan),
        "predicate_not" => {
            let inner = render_merge_operand(child("expression"), scope, scan)?;
            Some(format!("(NOT {inner})"))
        }
        "predicate_equal"
        | "predicate_notequal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal" => {
            let op = match kind {
                "predicate_equal" => "=",
                "predicate_notequal" => "<>",
                "predicate_less" => "<",
                "predicate_lessequal" => "<=",
                "predicate_greater" => ">",
                "predicate_greaterequal" => ">=",
                _ => unreachable!(),
            };
            let left = render_merge_operand(child("left"), scope, scan)?;
            let right = render_merge_operand(child("right"), scope, scan)?;
            Some(format!("({left} {op} {right})"))
        }
        "predicate_between" => {
            let target = render_merge_operand(child("expression"), scope, scan)?;
            let low = render_merge_operand(child("left"), scope, scan)?;
            let high = render_merge_operand(child("right"), scope, scan)?;
            Some(format!("({target} BETWEEN {low} AND {high})"))
        }
        "predicate_is_null" => {
            let inner = render_merge_operand(child("expression"), scope, scan)?;
            Some(format!("({inner} IS NULL)"))
        }
        "predicate_is_not_null" => {
            let inner = render_merge_operand(child("expression"), scope, scan)?;
            Some(format!("({inner} IS NOT NULL)"))
        }
        _ => None,
    }
}

/// A HAVING operand or ORDER BY key over the merge, which has only `GK_*` and `PARTIAL_*` columns.
fn render_merge_operand(
    node: Option<&Json>,
    scope: &MergeScope<'_>,
    scan: ScanTranslator<'_>,
) -> Option<String> {
    let node = node.filter(|n| !n.is_null())?;
    let kind = node.get("type").and_then(|t| t.as_str())?;
    // A group key operand reads its typed `GK_*` column; the merge has no source columns (#551).
    if kind != "function_aggregate"
        && let Some(slot) = group_key_slot(node, scope, scan)
    {
        return Some(group_key_column(slot));
    }
    match kind {
        "function_aggregate"
        | "predicate_and"
        | "predicate_or"
        | "predicate_not"
        | "predicate_equal"
        | "predicate_notequal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal"
        | "predicate_between"
        | "predicate_is_null"
        | "predicate_is_not_null" => render_having_over_merge(node, scope, scan),
        // Rewrites every nested aggregate, so a scalar wrapping an aggregate never renders
        // over absent source columns (#82).
        _ => render_scalar_over_merge_with_keys(node, scope, scan),
    }
}

/// A scalar over group keys and aggregates, such as `C_QTY + SUM(ID)`, with each key read from its
/// `GK_*` column. `None` while another source column remains, because the merge has none (#551).
fn render_scalar_over_merge_with_keys(
    node: &Json,
    scope: &MergeScope<'_>,
    scan: ScanTranslator<'_>,
) -> Option<String> {
    let mut residual = false;
    // Stops at an aggregate: its argument runs in the scan, not over the merge.
    let substituted = substitute_subtrees(node, &mut |subtree| {
        if is_aggregate_node(subtree) {
            return Some(subtree.clone());
        }
        ColumnNode::parse(subtree)?;
        Some(match group_key_slot(subtree, scope, scan) {
            Some(slot) => ColumnNode::to_json(&group_key_name(slot), None, None),
            None => {
                residual = true;
                subtree.clone()
            }
        })
    });
    if residual {
        return None;
    }
    let aggregates = scope.aggregates;
    render_scalar_over_merge(&substituted, &aggregates.plans, &aggregates.merged, scan)
        .map(|fragment| fragment.sql)
}

fn render_having_junction(
    expressions: Option<&Json>,
    scope: &MergeScope<'_>,
    op: &str,
    scan: ScanTranslator<'_>,
) -> Option<String> {
    let items = expressions?.as_array()?;
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        parts.push(render_having_over_merge(item, scope, scan)?);
    }
    match parts.len() {
        0 => None,
        1 => parts.into_iter().next(),
        _ => Some(format!("({})", parts.join(op))),
    }
}

#[cfg(test)]
#[path = "grouped_agg_tests.rs"]
mod tests;

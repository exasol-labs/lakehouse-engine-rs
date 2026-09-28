use crate::scan::spec::{
    AggKind, AggregatePlan, FileEntry, PartialAggColumn, ScanSpec, partial_column_name,
    render_ordered,
};
use crate::types::mapping::{exasol_type_from_json, parse_decimal_args};
use serde_json::Value as Json;
use vs_expression::render_expression;

use super::scalar_over_agg::{
    cast_merge_items, classify_scalar_over_aggregate, fold_aggregate_plan, merge_select_items,
    parse_agg_item, render_scalar_over_merge,
};
use super::support::{
    build_fan_out_inner, cast_to_declared_type, declared_select_type, render_limit_offset,
};
use super::topn::parse_sort_flags;

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
    /// contributes no aggregate plan; `projection` is the literal already cast to its
    /// declared type, never reused as a column identifier.
    Constant {
        select_index: usize,
        projection: String,
    },
    /// Nested aggregates fold into the shared `plans`; the wrapper renders over the
    /// merged partials only, cast to `declared_type` for Exasol's positional type check.
    ScalarOverAggregate {
        select_index: usize,
        node: Json,
        declared_type: String,
    },
}

pub(super) fn select_item_index(item: &GroupedSelectItem) -> usize {
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
    /// Deduplicated by `AggregatePlan` equality across top-level and nested occurrences.
    pub plans: Vec<AggregatePlan>,
    /// Aligned 1:1 with `plans`, not with the `selectList`.
    pub plan_types: Vec<String>,
    pub select_items: Vec<GroupedSelectItem>,
}

fn constant_projection_sql(pushdown_req: &Json, select_index: usize, rendered: &str) -> String {
    let declared = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array())
        .and_then(|d| d.get(select_index))
        .map(exasol_type_from_json);
    cast_to_declared_type(rendered, declared.as_deref())
}

/// Shared by `detect_group_by_aggregates` and `extract_projection` so the two
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
pub fn detect_group_by_aggregates(pushdown_req: &Json) -> Option<GroupedAggregateDetection> {
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) != Some("group_by") {
        return None;
    }

    let group_by = pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;

    // Any group-key render failure collapses the whole result.
    let mut group_keys = Vec::with_capacity(group_by.len());
    for node in group_by {
        match render_expression(node) {
            Ok(sql) => group_keys.push(sql),
            Err(_) => return None,
        }
    }

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
                let plan = parse_agg_item(item)?;
                let plan_slot = fold_aggregate_plan(
                    &mut plans,
                    &mut plan_types,
                    plan,
                    Some(declared_select_type(pushdown_req, select_index)),
                );
                select_items.push(GroupedSelectItem::Aggregate {
                    plan_slot,
                    select_index,
                });
            }
            t if is_literal_selectlist_item(t) => {
                let rendered = render_expression(item).ok()?;
                let projection = constant_projection_sql(pushdown_req, select_index, &rendered);
                select_items.push(GroupedSelectItem::Constant {
                    select_index,
                    projection,
                });
            }
            _ => {
                // A group-key projection: a column or an expression rendering to a group key.
                if let Some(group_key_slot) = render_expression(item)
                    .ok()
                    .and_then(|sql| group_keys.iter().position(|gk| *gk == sql))
                {
                    select_items.push(GroupedSelectItem::GroupKey {
                        group_key_slot,
                        select_index,
                    });
                    continue;
                }
                // Otherwise a scalar wrapping aggregates; `None` declines the whole grouped
                // detection, routing to the qualified wrapper (never a bare row scan).
                let nested = classify_scalar_over_aggregate(item)?;
                for plan in nested {
                    fold_aggregate_plan(&mut plans, &mut plan_types, plan, None);
                }
                select_items.push(GroupedSelectItem::ScalarOverAggregate {
                    select_index,
                    node: item.clone(),
                    declared_type: declared_select_type(pushdown_req, select_index),
                });
            }
        }
    }

    Some(GroupedAggregateDetection {
        group_keys,
        plans,
        plan_types,
        select_items,
    })
}

/// A projected key takes `selectListDataTypes` (matched by index, not rendered SQL,
/// and authoritative since Exasol type-checks it). An unprojected key takes its
/// `groupBy` node's own `dataType` when present (e.g. a CAST): without it a
/// `CHAR(n)` key reached DataFusion unpadded and trailing-blank variants stayed
/// separate groups (#192). Falls back to `VARCHAR(2000000)`.
pub(super) fn group_key_exasol_types(
    pushdown_req: &Json,
    group_keys: &[String],
    select_items: &[GroupedSelectItem],
) -> Vec<String> {
    let declared_types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array());
    let mut types: Vec<Option<String>> = vec![None; group_keys.len()];
    for item in select_items {
        if let GroupedSelectItem::GroupKey {
            group_key_slot,
            select_index,
        } = item
            && let Some(ty) = declared_types
                .and_then(|d| d.get(*select_index))
                .map(exasol_type_from_json)
            && let Some(slot) = types.get_mut(*group_key_slot)
        {
            *slot = Some(ty);
        }
    }
    let group_by = pushdown_req.get("groupBy").and_then(|v| v.as_array());
    for (slot, resolved) in types.iter_mut().enumerate() {
        if resolved.is_none() {
            *resolved = group_by
                .and_then(|nodes| nodes.get(slot))
                .and_then(|node| node.get("dataType"))
                .map(exasol_type_from_json);
        }
    }
    types
        .into_iter()
        .map(|ty| ty.unwrap_or_else(|| "VARCHAR(2000000)".to_string()))
        .collect()
}

/// Exasol's `CAST(x AS CHAR(n))` blank-pads, so trailing-blank variants are one
/// group natively; DataFusion does not pad, so the merge would return a row per
/// variant (#192).
///
/// Guarded by a length test rather than a bare `rpad`, which truncates: an
/// over-length value passes through so the outer `CAST("GK_i" AS CHAR(n))` still
/// raises Exasol's 22001 error. Only this copy is padded; the unpadded fragments
/// stay the match keys for [`build_grouped_order_by_clause`].
///
/// The one DataFusion SQL fragment the adapter synthesises directly instead of via
/// `vs-expression`; DataFusion-executing tests pin it.
pub(super) fn blank_pad_char_group_keys(
    group_keys: &[String],
    group_key_types: &[String],
) -> Vec<String> {
    group_keys
        .iter()
        .enumerate()
        .map(
            |(slot, fragment)| match group_key_types.get(slot).and_then(|ty| char_width(ty)) {
                Some(width) => format!(
                    "CASE WHEN character_length({fragment}) < {width} \
                     THEN rpad({fragment}, {width}) ELSE {fragment} END"
                ),
                None => fragment.clone(),
            },
        )
        .collect()
}

/// Reads the digits between the parentheses so `CHAR(3) ASCII` still yields a
/// width; the anchored `CHAR(` prefix never matches `VARCHAR(n)`.
fn char_width(declared_type: &str) -> Option<u32> {
    declared_type
        .strip_prefix("CHAR(")?
        .split_once(')')?
        .0
        .parse()
        .ok()
}

/// Exasol does not re-sort a delegated `ORDER BY`, so the merge must sort itself.
/// Group keys render as positional output ordinals so they sort the type-cast
/// output, not the lexicographic VARCHAR `GK_*` staging column (`1,10,2,…`).
/// Aggregate keys render as their merged expression; no hidden output column is
/// added, so Exasol's positional validation is unaffected.
///
/// `None` without an `orderBy`. Anything unexpressible over the merge is
/// [`GroupedOrderBy::Unresolvable`], routing to the qualified wrapper (#198).
pub(super) fn build_grouped_order_by_clause(
    pushdown_req: &Json,
    detection: &GroupedAggregateDetection,
) -> Option<GroupedOrderBy> {
    let elements = pushdown_req.get("orderBy").and_then(|v| v.as_array())?;
    if elements.is_empty() {
        return None;
    }
    let mut parts = Vec::with_capacity(elements.len());
    for element in elements {
        // Flags only: an aggregate sort key yields no `SortKey`.
        let Some((ascending, nulls_last)) = parse_sort_flags(element) else {
            return Some(GroupedOrderBy::Unresolvable);
        };
        let Some(expr) = element.get("expression") else {
            return Some(GroupedOrderBy::Unresolvable);
        };
        let ordering = match group_key_output_ordinal(expr, detection) {
            Some(ordinal) => ordinal.to_string(),
            None => match render_having_over_merge(expr, &detection.plans) {
                Some(merged) => merged,
                None => return Some(GroupedOrderBy::Unresolvable),
            },
        };
        parts.push(render_ordered(&ordering, ascending, nulls_last));
    }
    Some(GroupedOrderBy::Clause(parts.join(", ")))
}

/// 1-based merge-output ordinal of a sort key that is a projected group key.
fn group_key_output_ordinal(expr: &Json, detection: &GroupedAggregateDetection) -> Option<usize> {
    let rendered = render_expression(expr).ok()?;
    let slot = detection.group_keys.iter().position(|gk| *gk == rendered)?;
    detection.select_items.iter().find_map(|it| match it {
        GroupedSelectItem::GroupKey {
            group_key_slot,
            select_index,
        } if *group_key_slot == slot => Some(select_index + 1),
        _ => None,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum GroupedOrderBy {
    Clause(String),
    Unresolvable,
}

// Two call sites and every argument is a distinct resolved plan input.
#[allow(clippy::too_many_arguments)]
pub fn build_grouped_aggregate_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    group_keys: &[String],
    group_key_types: &[String],
    aggregates: &[AggregatePlan],
    aggregate_types: &[String],
    select_items: &[GroupedSelectItem],
    limit: Option<u64>,
    offset: u64,
    col_types: &[(String, String)],
    udf_name: &str,
    distribute_udf_name: &str,
    having: Option<&str>,
    order_by: Option<&str>,
) -> String {
    let gk_emits: Vec<String> = (0..group_keys.len())
        .map(|i| format!(r#""GK_{i}" VARCHAR(2000000)"#))
        .collect();
    let partial_items = partial_emits_items(aggregates, col_types, aggregate_types);
    let all_emits: Vec<String> = gk_emits
        .iter()
        .chain(partial_items.iter())
        .cloned()
        .collect();
    let emits = all_emits.join(", ");

    // The scan stringifies group keys; cast back so result types match Exasol's.
    let gk_select: Vec<String> = (0..group_keys.len())
        .map(|i| {
            cast_to_declared_type(
                &format!(r#""GK_{i}""#),
                group_key_types.get(i).map(String::as_str),
            )
        })
        .collect();
    let merge_items = cast_merge_items(aggregates, aggregate_types);
    let merged_partials = merge_select_items(aggregates);

    // Exasol validates this SELECT positionally, so it follows `selectList` order,
    // not the inner keys-first fan-out order.
    let mut ordered = select_items.to_vec();
    ordered.sort_by_key(select_item_index);
    let outer_select: Vec<String> = ordered
        .iter()
        .filter_map(|item| match item {
            GroupedSelectItem::GroupKey { group_key_slot, .. } => {
                gk_select.get(*group_key_slot).cloned()
            }
            GroupedSelectItem::Aggregate { plan_slot, .. } => merge_items.get(*plan_slot).cloned(),
            GroupedSelectItem::Constant { projection, .. } => Some(projection.clone()),
            GroupedSelectItem::ScalarOverAggregate {
                node,
                declared_type,
                ..
            } => render_scalar_over_merge(node, aggregates, &merged_partials)
                .map(|expr| cast_to_declared_type(&expr, Some(declared_type))),
        })
        .collect();
    let outer_select_str = outer_select.join(", ");

    let outer_group_by: Vec<String> = (0..group_keys.len())
        .map(|i| format!(r#""GK_{i}""#))
        .collect();
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

    let mut sql =
        format!("SELECT {outer_select_str} FROM ({fan_out}) GROUP BY {outer_group_by_str}");

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
    sql
}

/// Counts are DECIMAL(20,0); SUM widens DECIMAL(p,s) to DECIMAL(36,s) against
/// overflow; AVG and stat sums are DOUBLE PRECISION (reconstructed in floating
/// point); MIN/MAX use the argument's exact type.
pub(super) fn partial_emits_items(
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
    aggregate_types: &[String],
) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| {
            // The sole type source for an expression-argument aggregate (no source column).
            let declared = aggregate_types.get(i).map(String::as_str);
            plan.kind.partial_columns().iter().map(move |col| {
                let ty = match col {
                    PartialAggColumn::CountStar
                    | PartialAggColumn::CountArg
                    | PartialAggColumn::AvgCnt
                    | PartialAggColumn::StatCnt => "DECIMAL(20,0)".to_string(),
                    PartialAggColumn::AvgSum
                    | PartialAggColumn::StatSum
                    | PartialAggColumn::StatSumSq => "DOUBLE PRECISION".to_string(),
                    PartialAggColumn::Sum => sum_emit_type(&col_type_for(
                        plan.column.as_deref(),
                        plan.arg_expr.as_deref(),
                        col_types,
                        declared,
                    )),
                    PartialAggColumn::Min | PartialAggColumn::Max => col_type_for(
                        plan.column.as_deref(),
                        plan.arg_expr.as_deref(),
                        col_types,
                        declared,
                    ),
                };
                let name = partial_column_name(*col, i);
                format!(r#""{name}" {ty}"#)
            })
        })
        .collect()
}

/// An expression-argument aggregate has no source column, so it takes `declared`.
pub(super) fn col_type_for(
    column: Option<&str>,
    arg_expr: Option<&str>,
    col_types: &[(String, String)],
    declared: Option<&str>,
) -> String {
    if column.is_none()
        && arg_expr.is_some()
        && let Some(ty) = declared
    {
        return ty.to_string();
    }
    column
        .and_then(|col| {
            col_types
                .iter()
                .find(|(n, _)| n == col)
                .map(|(_, t)| t.clone())
        })
        .unwrap_or_else(|| "DOUBLE PRECISION".to_string())
}

/// Absent DECIMAL scale defaults to 0 per `parse_decimal_args`.
fn sum_emit_type(col_ty: &str) -> String {
    if col_ty == "DOUBLE PRECISION" {
        return "DOUBLE PRECISION".to_string();
    }
    // No uppercasing: producers already emit uppercase, and adding one would change
    // the answer for a lowercase input.
    if let Some((_p, s)) = parse_decimal_args(col_ty) {
        return format!("DECIMAL(36,{s})");
    }
    "DOUBLE PRECISION".to_string()
}

/// SUM and STDDEV/VARIANCE need a numeric target; `false` routes to a row scan. An
/// expression-argument aggregate resolves to the numeric `DOUBLE PRECISION` default.
pub fn validate_agg_col_types(
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
) -> bool {
    for plan in aggregates {
        let needs_numeric = matches!(
            plan.kind,
            AggKind::Sum
                | AggKind::VarPop
                | AggKind::VarSamp
                | AggKind::StddevPop
                | AggKind::StddevSamp
        );
        if needs_numeric {
            let ty = col_type_for(
                plan.column.as_deref(),
                plan.arg_expr.as_deref(),
                col_types,
                None,
            );
            if !is_numeric_exasol_type(&ty) {
                return false;
            }
        }
    }
    true
}

fn is_numeric_exasol_type(ty: &str) -> bool {
    ty == "DOUBLE PRECISION" || ty.starts_with("DECIMAL(")
}

/// The merge wrapper has only `GK_*`/`PARTIAL_*` columns, so each aggregate is
/// rewritten to its merged expression. `None` for an aggregate not among `plans` or
/// an unsupported node, so the caller never emits a wrong or dropped HAVING.
pub(super) fn render_having_over_merge(node: &Json, plans: &[AggregatePlan]) -> Option<String> {
    if !node.is_object() {
        return None;
    }
    let kind = node.get("type").and_then(|t| t.as_str())?;
    let child = |key: &str| node.get(key);

    // Uncast: the comparison is against the merged value; `cast_merge_items`'s CAST is
    // only for output-column typing.
    if kind == "function_aggregate" {
        let plan = parse_agg_item(node)?;
        let idx = plans.iter().position(|p| *p == plan)?;
        return merge_select_items(plans).into_iter().nth(idx);
    }

    match kind {
        "predicate_and" => render_having_junction(child("expressions"), plans, " AND "),
        "predicate_or" => render_having_junction(child("expressions"), plans, " OR "),
        "predicate_not" => {
            let inner = render_having_operand(child("expression"), plans)?;
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
            let left = render_having_operand(child("left"), plans)?;
            let right = render_having_operand(child("right"), plans)?;
            Some(format!("({left} {op} {right})"))
        }
        "predicate_between" => {
            let target = render_having_operand(child("expression"), plans)?;
            let low = render_having_operand(child("left"), plans)?;
            let high = render_having_operand(child("right"), plans)?;
            Some(format!("({target} BETWEEN {low} AND {high})"))
        }
        "predicate_is_null" => {
            let inner = render_having_operand(child("expression"), plans)?;
            Some(format!("({inner} IS NULL)"))
        }
        "predicate_is_not_null" => {
            let inner = render_having_operand(child("expression"), plans)?;
            Some(format!("({inner} IS NOT NULL)"))
        }
        _ => None,
    }
}

fn render_having_operand(node: Option<&Json>, plans: &[AggregatePlan]) -> Option<String> {
    let node = node.filter(|n| !n.is_null())?;
    let kind = node.get("type").and_then(|t| t.as_str())?;
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
        | "predicate_is_not_null" => render_having_over_merge(node, plans),
        // Rewrites every nested aggregate, so a scalar wrapping an aggregate never renders
        // over absent source columns (#82).
        _ => render_scalar_over_merge(node, plans, &merge_select_items(plans)),
    }
}

fn render_having_junction(
    expressions: Option<&Json>,
    plans: &[AggregatePlan],
    op: &str,
) -> Option<String> {
    let items = expressions?.as_array()?;
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        parts.push(render_having_over_merge(item, plans)?);
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

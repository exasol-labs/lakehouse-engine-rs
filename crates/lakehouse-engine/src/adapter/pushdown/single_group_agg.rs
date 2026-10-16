use crate::scan::spec::AggregatePlan;
use serde_json::Value as Json;

use super::scalar_over_agg::{
    arg_column_or_expr, cast_merge_items, classify_scalar_over_aggregate, fold_aggregate_plan,
    nested_aggregate_type, parse_agg_item, render_scalar_over_merge,
};
use super::support::{cast_to_declared_type, declared_select_type};

/// A `COUNT(DISTINCT ...)` is not an aggregate partial: it is its own DISTINCT
/// row-scan fan-out counted by an outer Exasol-native `COUNT(DISTINCT "V")`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingleGroupItem {
    Aggregate(AggregatePlan),
    Distinct(DistinctCount),
    /// `node` is kept verbatim because the merge rewrite re-walks it to substitute
    /// each nested aggregate's merged expression.
    ScalarOverAggregate {
        node: Json,
        declared_type: String,
    },
}

/// Exactly one of `column` or `arg_expr` is populated, mirroring [`AggregatePlan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctCount {
    pub column: Option<String>,
    pub arg_expr: Option<String>,
}

/// `None` (row-scan fallback) when any item declines; one declining item declines
/// the whole detection.
pub fn detect_aggregates(pushdown_req: &Json) -> Option<Vec<SingleGroupItem>> {
    if pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return None;
    }

    let list = pushdown_req.get("selectList").and_then(|v| v.as_array())?;

    if list.is_empty() {
        return None;
    }

    let mut items = Vec::with_capacity(list.len());
    for (select_index, item) in list.iter().enumerate() {
        let resolved = if item.get("type").and_then(|t| t.as_str()) == Some("function_aggregate") {
            match parse_count_distinct(item) {
                Some(distinct) => SingleGroupItem::Distinct(distinct),
                None => SingleGroupItem::Aggregate(parse_agg_item(item)?),
            }
        } else {
            // Classified for the decline only; `ordinary_plans` re-derives the nested plans.
            classify_scalar_over_aggregate(item)?;
            SingleGroupItem::ScalarOverAggregate {
                node: item.clone(),
                declared_type: declared_select_type(pushdown_req, select_index),
            }
        };
        items.push(resolved);
    }

    Some(items)
}

pub fn ordinary_plans(items: &[SingleGroupItem]) -> Vec<AggregatePlan> {
    let mut plans = Vec::new();
    // The fold's accumulated types are discarded; see `single_group_plan_types`.
    let mut plan_types = Vec::new();
    for item in items {
        match item {
            SingleGroupItem::Aggregate(plan) => {
                fold_aggregate_plan(&mut plans, &mut plan_types, plan.clone(), None);
            }
            SingleGroupItem::Distinct(_) => {}
            SingleGroupItem::ScalarOverAggregate { node, .. } => {
                for (plan, _) in classify_scalar_over_aggregate(node).into_iter().flatten() {
                    fold_aggregate_plan(&mut plans, &mut plan_types, plan, None);
                }
            }
        }
    }
    plans
}

/// Aligned 1:1 with `ordinary_plans(items)`. A top-level item's declared type wins; a
/// slot reached only through a nested aggregate takes [`nested_aggregate_type`], and
/// `None` when that is unknown.
pub fn single_group_plan_types(
    pushdown_req: &Json,
    items: &[SingleGroupItem],
    col_types: &[(String, String)],
) -> Option<Vec<String>> {
    let plans = ordinary_plans(items);
    let mut plan_types: Vec<Option<String>> = vec![None; plans.len()];
    let slot_of = |plan: &AggregatePlan| {
        plans
            .iter()
            .position(|p| p == plan)
            .expect("ordinary_plans folds every aggregate plan in")
    };

    for (select_index, item) in items.iter().enumerate() {
        match item {
            SingleGroupItem::Aggregate(plan) => {
                plan_types[slot_of(plan)] = Some(declared_select_type(pushdown_req, select_index));
            }
            SingleGroupItem::Distinct(_) => {}
            SingleGroupItem::ScalarOverAggregate { node, .. } => {
                for (plan, agg) in classify_scalar_over_aggregate(node).into_iter().flatten() {
                    let slot = &mut plan_types[slot_of(&plan)];
                    if slot.is_none() {
                        *slot = nested_aggregate_type(&agg, &plan, col_types);
                    }
                }
            }
        }
    }

    plan_types.into_iter().collect()
}

/// `None` when any item has no merge expression (a `COUNT(DISTINCT)` or an
/// unrenderable scalar); the caller must then use the qualified wrapper, since a
/// narrower select list fails Exasol's positional validation.
pub(super) fn single_group_merge_select(
    items: &[SingleGroupItem],
    plans: &[AggregatePlan],
    plan_types: &[String],
) -> Option<Vec<String>> {
    let cast_merged = cast_merge_items(plans, plan_types);
    items
        .iter()
        .map(|item| match item {
            SingleGroupItem::Aggregate(plan) => {
                let slot = plans.iter().position(|p| p == plan)?;
                cast_merged.get(slot).cloned()
            }
            SingleGroupItem::Distinct(_) => None,
            SingleGroupItem::ScalarOverAggregate {
                node,
                declared_type,
                ..
            } => render_scalar_over_merge(node, plans, &cast_merged)
                .map(|expr| cast_to_declared_type(&expr, Some(declared_type))),
        })
        .collect()
}

pub(super) fn has_distinct(items: &[SingleGroupItem]) -> bool {
    items
        .iter()
        .any(|item| matches!(item, SingleGroupItem::Distinct(_)))
}

/// Only a bare-column argument qualifies: its exact Exasol type can be declared for
/// the per-shard `"V"` column. An expression would need `VARCHAR(2000000)`, and a
/// non-injective cast to text can silently undercount. Every other distinct shape
/// routes to the qualified wrapper, where several emitting-UDF scalar subqueries in
/// one select list would not compile anyway (`sqlCode 04000`).
pub(super) fn is_lone_count_distinct(items: &[SingleGroupItem]) -> bool {
    matches!(items, [SingleGroupItem::Distinct(dc)] if dc.column.is_some())
}

/// `None` when the item is not a distinct `COUNT` or its argument cannot be resolved;
/// the caller then defers to [`parse_agg_item`], which declines it.
fn parse_count_distinct(item: &Json) -> Option<DistinctCount> {
    if item.get("distinct").and_then(|d| d.as_bool()) != Some(true) {
        return None;
    }
    let fn_name = item
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_uppercase();
    if fn_name != "COUNT" {
        return None;
    }
    let args = item.get("arguments").and_then(|a| a.as_array());
    let (column, arg_expr) = arg_column_or_expr(args)?;
    Some(DistinctCount { column, arg_expr })
}

#[cfg(test)]
#[path = "single_group_agg_tests.rs"]
mod tests;

use crate::scan::spec::AggregatePlan;
use vs_expression::Expr;

use super::grouped_agg::is_literal_selectlist_item;
use super::request::PushdownRequest;
use super::scalar_over_agg::{
    TypedPlans, arg_column_or_expr, classify_scalar_over_aggregate, fold_aggregate_plan,
    fold_nested_plans, parse_agg_item, render_scalar_over_merge,
};
use super::support::{ScanTranslator, output_select_list};
use crate::types::exasol_type::ExasolType;

/// A `COUNT(DISTINCT ...)` is not an aggregate partial: it is its own DISTINCT
/// row-scan fan-out counted by an outer Exasol-native `COUNT(DISTINCT "V")`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingleGroupItem {
    Aggregate(AggregatePlan),
    Distinct(DistinctCount),
    /// `node` is kept verbatim because the merge rewrite re-walks it to substitute
    /// each nested aggregate's merged expression.
    ScalarOverAggregate {
        node: Expr,
        nested: Vec<(AggregatePlan, Expr)>,
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
pub fn detect_aggregates(
    pushdown: &PushdownRequest,
    scan: ScanTranslator<'_>,
) -> Option<Vec<SingleGroupItem>> {
    if !pushdown.group_by().is_empty() {
        return None;
    }

    let list = pushdown.select_items()?;

    let mut items = Vec::with_capacity(list.len());
    for (select_index, item) in list.iter().enumerate() {
        let resolved = if item.is_aggregate() {
            pushdown.emittable_select_type(select_index)?;
            match parse_count_distinct(item, scan) {
                Some(distinct) => SingleGroupItem::Distinct(distinct),
                None => SingleGroupItem::Aggregate(parse_agg_item(item, scan)?),
            }
        } else {
            // A constant, such as a folded clock function, carries no aggregate to merge.
            let nested = if is_literal_selectlist_item(item) {
                Vec::new()
            } else {
                classify_scalar_over_aggregate(item, scan)?
            };
            SingleGroupItem::ScalarOverAggregate {
                node: item.clone(),
                nested,
            }
        };
        items.push(resolved);
    }

    // A select list of constants alone has nothing to aggregate per shard.
    let aggregates = items.iter().any(|item| {
        !matches!(item, SingleGroupItem::ScalarOverAggregate { nested, .. } if nested.is_empty())
    });
    aggregates.then_some(items)
}

/// The partial plans of a single-group select list, typed and merged once per request.
pub fn single_group_plans(
    items: &[SingleGroupItem],
    select_types: &[ExasolType],
    scan: ScanTranslator<'_>,
) -> Option<TypedPlans> {
    let (plans, types) = fold_single_group_plans(items, select_types, scan);
    TypedPlans::new(plans, types, scan.columns())
}

/// Dedup is a correctness requirement: the merge rewrite binds each nested aggregate to the
/// first structurally-equal slot, so duplicates would misalign with the `EMITS` columns.
pub(super) fn fold_single_group_plans(
    items: &[SingleGroupItem],
    select_types: &[ExasolType],
    scan: ScanTranslator<'_>,
) -> (Vec<AggregatePlan>, Vec<Option<ExasolType>>) {
    let mut plans = Vec::new();
    let mut plan_types = Vec::new();
    for (select_index, item) in items.iter().enumerate() {
        match item {
            SingleGroupItem::Aggregate(plan) => {
                let declared = select_types.get(select_index).copied();
                fold_aggregate_plan(&mut plans, &mut plan_types, plan.clone(), declared);
            }
            SingleGroupItem::Distinct(_) => {}
            SingleGroupItem::ScalarOverAggregate { nested, .. } => {
                fold_nested_plans(&mut plans, &mut plan_types, nested, scan);
            }
        }
    }
    (plans, plan_types)
}

/// `None` when any item has no merge expression (a `COUNT(DISTINCT)`, or a scalar over an
/// aggregate the merge does not carry); the caller must then use the qualified wrapper, since a
/// narrower select list fails Exasol's positional validation.
pub(super) fn single_group_merge_select(
    items: &[SingleGroupItem],
    plans: &TypedPlans,
    select_types: &[ExasolType],
    scan: ScanTranslator<'_>,
) -> Option<Vec<String>> {
    let fragments = items
        .iter()
        .map(|item| match item {
            SingleGroupItem::Aggregate(plan) => plans.fragment(plans.slot_of(plan)?),
            SingleGroupItem::Distinct(_) => None,
            SingleGroupItem::ScalarOverAggregate { node, .. } => {
                render_scalar_over_merge(node, &plans.plans, &plans.merged, scan)
            }
        })
        .collect::<Option<Vec<_>>>()?;
    output_select_list(fragments, select_types)
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
fn parse_count_distinct(item: &Expr, scan: ScanTranslator<'_>) -> Option<DistinctCount> {
    let Expr::Aggregate(call) = item else {
        return None;
    };
    if !call.distinct || call.name != "COUNT" {
        return None;
    }
    let (column, arg_expr) = arg_column_or_expr(&call.arguments, scan)?;
    Some(DistinctCount { column, arg_expr })
}

#[cfg(test)]
#[path = "single_group_agg_tests.rs"]
mod tests;

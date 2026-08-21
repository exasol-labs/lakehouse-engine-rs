//! Single-group (ungrouped) aggregate detection and merge-SELECT assembly.
//!
//! `detect_aggregates` is the single-group entry point. The aggregate primitives
//! both planners share — item parsing, the per-`AggKind` merge formulas, and
//! scalar-over-aggregate rewriting — live in
//! [`scalar_over_agg`](super::scalar_over_agg), so this module never names the GROUP
//! BY planner and vice versa.

use crate::scan::spec::AggregatePlan;
use serde_json::Value as Json;

use super::scalar_over_agg::{
    NESTED_AGGREGATE_PLAN_TYPE, arg_column_or_expr, cast_merge_items,
    classify_scalar_over_aggregate, fold_aggregate_plan, merge_select_items, parse_agg_item,
    render_scalar_over_merge,
};
use super::support::{cast_to_declared_type, declared_select_type};

/// One resolved single-group select-list item, in select-list order.
///
/// An ordinary aggregate ([`SingleGroupItem::Aggregate`]) is computed node-locally
/// as a partial result and merged by the outer wrapper's aggregate expression. A
/// `COUNT(DISTINCT ...)` ([`SingleGroupItem::Distinct`]) is NOT an aggregate
/// partial: it is decomposed into its own DISTINCT row-scan fan-out counted by an
/// outer Exasol-native `COUNT(DISTINCT "V")` — see
/// `vs-adapter/pushdown-planning-count-distinct`. A scalar function wrapping
/// aggregates ([`SingleGroupItem::ScalarOverAggregate`]) reuses the ordinary
/// partial columns and renders its own structure over the merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingleGroupItem {
    /// An ordinary aggregate served by the shared per-shard partial-aggregate scan.
    Aggregate(AggregatePlan),
    /// A `COUNT(DISTINCT col|expr)` served by its own DISTINCT row-scan fan-out.
    Distinct(DistinctCount),
    /// A scalar/arithmetic node WRAPPING one or more aggregates (e.g.
    /// `ROUND(SUM(q) / COUNT(*), 2)`). Its nested aggregates become per-shard
    /// `PARTIAL_*` columns like any other single-group aggregate; the surrounding
    /// structure is rendered ONCE over the outer merge wrapper. The `node` is kept
    /// verbatim rather than pre-rendered because the merge rewrite re-walks it to
    /// substitute each nested aggregate's merged expression — see
    /// [`scalar_over_agg`](super::scalar_over_agg).
    ScalarOverAggregate { node: Json, declared_type: String },
}

/// A single-group `COUNT(DISTINCT col)` / `COUNT(DISTINCT expr)` descriptor.
///
/// Exactly one of `column` (bare-column fast path) or `arg_expr` (a rendered
/// DataFusion SQL fragment) is populated, mirroring [`AggregatePlan`]. It carries
/// no `AggKind`: the distinct count is realized as a DISTINCT row-scan fan-out over
/// this argument (`CommonScanSpec::distinct`), not as an aggregate partial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctCount {
    pub column: Option<String>,
    pub arg_expr: Option<String>,
}

/// Inspect the pushdown request's `selectList` and return one resolved
/// [`SingleGroupItem`] per item, in order, if every select-list item is a
/// supported single-group aggregate or a decomposable scalar-over-aggregate.
///
/// Returns `None` (fall back to row scan) when any of the following hold:
/// - `groupBy` is present and non-empty (GROUP BY not supported)
/// - any select item has `distinct: true` OTHER than a `COUNT(DISTINCT ...)`
///   (single-group `COUNT(DISTINCT col)` / `COUNT(DISTINCT expr)` is accepted as a
///   [`SingleGroupItem::Distinct`] fan-out; DISTINCT SUM/AVG/etc. still decline)
/// - any `function_aggregate` item is not one of COUNT(*), COUNT(col)/COUNT(expr),
///   SUM/MIN/MAX/AVG (bare column or renderable expression), or the
///   STDDEV/VARIANCE family
/// - any other item is not a decomposable scalar-over-aggregate per
///   [`classify_scalar_over_aggregate`] — which declines a plain column or scalar
///   projection with no nested aggregate, a nested aggregate the merge cannot
///   express, a bare source column outside the aggregates, and an unrenderable
///   residual structure. One declining item declines the WHOLE detection, so the
///   request routes to the qualified single-table wrapper via `project_columns`'s
///   widening signal rather than being evaluated per shard.
/// - the select list is absent or empty
pub fn detect_aggregates(pushdown_req: &Json) -> Option<Vec<SingleGroupItem>> {
    // Reject GROUP BY.
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
            // A single-group COUNT(DISTINCT ...) becomes a DISTINCT row-scan fan-out;
            // every OTHER distinct aggregate declines via parse_agg_item.
            match parse_count_distinct(item) {
                Some(distinct) => SingleGroupItem::Distinct(distinct),
                None => SingleGroupItem::Aggregate(parse_agg_item(item)?),
            }
        } else {
            // Classified for the decline only — `ordinary_plans` re-derives and folds
            // the nested plans, which keeps this function's return type unchanged.
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

/// The ordinary (non-distinct) aggregate plans among `items`, in encounter order,
/// deduplicated by [`AggregatePlan`] equality.
///
/// Every aggregate the select list needs a per-shard `PARTIAL_*` column for is
/// folded in: a top-level [`SingleGroupItem::Aggregate`], and every aggregate
/// nested inside a [`SingleGroupItem::ScalarOverAggregate`]. Dedup is a
/// correctness requirement, not an optimization — the merge rewrite resolves each
/// nested aggregate to the FIRST structurally-equal slot, so an un-deduplicated
/// list would bind a repeated aggregate to a slot other than the one its `EMITS`
/// column was declared at (decision-log [6]).
///
/// These drive the shared per-shard partial-aggregate scan and
/// [`validate_agg_col_types`](super::validate_agg_col_types); the distinct items
/// are handled separately as DISTINCT row-scan fan-outs.
///
/// `pub` (not `pub(super)`): this is the boundary that lets callers outside the
/// `pushdown` module (e.g. integration tests) obtain a nameable `Vec<AggregatePlan>`
/// from the opaque [`SingleGroupItem`] returned by [`detect_aggregates`] without
/// ever naming `SingleGroupItem` itself.
pub fn ordinary_plans(items: &[SingleGroupItem]) -> Vec<AggregatePlan> {
    let mut plans = Vec::new();
    // The per-plan declared types are derived separately from the folded list, so
    // the types this fold accumulates are discarded here.
    let mut plan_types = Vec::new();
    for item in items {
        match item {
            SingleGroupItem::Aggregate(plan) => {
                fold_aggregate_plan(&mut plans, &mut plan_types, plan.clone(), None);
            }
            SingleGroupItem::Distinct(_) => {}
            SingleGroupItem::ScalarOverAggregate { node, .. } => {
                for plan in classify_scalar_over_aggregate(node).into_iter().flatten() {
                    fold_aggregate_plan(&mut plans, &mut plan_types, plan, None);
                }
            }
        }
    }
    plans
}

/// The declared Exasol output type for each slot of `ordinary_plans(items)`,
/// aligned 1:1 with its slot order.
///
/// The two defaults are deliberately different, because this list is not only the
/// outer merge's CAST source — `build_aggregate_scan_sql` also types the scan's
/// `EMITS` clause from it:
///
/// - A slot reached by a top-level [`SingleGroupItem::Aggregate`] takes that item's
///   own declared type through [`declared_select_type`], which owns the
///   `"VARCHAR(2000000)"` answer for an item Exasol declared no usable type for.
/// - A slot reached ONLY through a nested [`SingleGroupItem::ScalarOverAggregate`]
///   has no `selectListDataTypes` entry naming it, so it takes
///   [`NESTED_AGGREGATE_PLAN_TYPE`] — the same answer the grouped planner's
///   `fold_aggregate_plan` gives such a slot. Its outer cast comes from the
///   `ScalarOverAggregate` item's own `declared_type` instead.
pub fn single_group_plan_types(pushdown_req: &Json, items: &[SingleGroupItem]) -> Vec<String> {
    let plans = ordinary_plans(items);
    let mut plan_types = vec![NESTED_AGGREGATE_PLAN_TYPE.to_string(); plans.len()];

    for (select_index, item) in items.iter().enumerate() {
        if let SingleGroupItem::Aggregate(plan) = item {
            let slot = plans
                .iter()
                .position(|p| p == plan)
                .expect("ordinary_plans folds every top-level Aggregate item's plan in");
            plan_types[slot] = declared_select_type(pushdown_req, select_index);
        }
    }

    plan_types
}

/// The outer merge SELECT items for a single-group aggregate request, one per
/// `items` entry in `selectList` order, each cast to the type Exasol declared for
/// that select-list item.
///
/// A bare [`SingleGroupItem::Aggregate`] takes the merge expression of its own
/// plan slot; a [`SingleGroupItem::ScalarOverAggregate`] renders its scalar
/// structure ONCE over the merged partials, with every nested aggregate rewritten
/// to that aggregate's merge expression. `plans` and `plan_types` are
/// [`ordinary_plans`] and [`single_group_plan_types`] over the same `items`.
///
/// Returns `None` when any item has no merge expression — a `COUNT(DISTINCT)`
/// (served by its own DISTINCT row-scan fan-out instead) or a scalar structure the
/// merge cannot render. The caller must then route the request to the qualified
/// single-table wrapper: emitting the renderable items alone would return a select
/// list narrower than the one Exasol validates positionally against.
pub(super) fn single_group_merge_select(
    items: &[SingleGroupItem],
    plans: &[AggregatePlan],
    plan_types: &[String],
) -> Option<Vec<String>> {
    let merged = merge_select_items(plans);
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
            } => render_scalar_over_merge(node, plans, &merged)
                .map(|expr| cast_to_declared_type(&expr, Some(declared_type))),
        })
        .collect()
}

/// Whether any item is a `COUNT(DISTINCT ...)` needing its own row-scan fan-out.
pub(super) fn has_distinct(items: &[SingleGroupItem]) -> bool {
    items
        .iter()
        .any(|item| matches!(item, SingleGroupItem::Distinct(_)))
}

/// Whether the select list is EXACTLY one `COUNT(DISTINCT <bare column>)` and nothing
/// else — the ONLY shape that fans out to a dedicated DISTINCT row-scan counted by a
/// native `COUNT(DISTINCT "V")` (Case 1). The lone distinct must have a bare-column
/// argument (`dc.column.is_some()`): only a source column has a known exact Exasol
/// type to declare for the per-shard `"V"` value column, so cross-shard dedup stays
/// exact. A lone `COUNT(DISTINCT <expression>)` (`dc.column` is `None`, `dc.arg_expr`
/// set) is deliberately EXCLUDED — it would otherwise have to declare `"V"` as
/// `VARCHAR(2000000)` and rely on the expression's native→`Utf8` cast being injective,
/// which can silently undercount (e.g. two distinct timestamps that print identically
/// after string truncation). Any non-lone distinct shape (more than one distinct, or a
/// distinct alongside an ordinary SUM/MIN/MAX/COUNT/AVG aggregate — Case 2/3) is
/// likewise excluded. Every excluded shape declines the fan-out and routes to the
/// qualified single-table wrapper (`has_distinct && !is_lone_count_distinct` in
/// `mod.rs`), where Exasol evaluates any expression and the DISTINCT natively over
/// exact-typed base columns — and where no composition of several scalar-subquery
/// emitting-UDF calls in one select list would compile anyway (`sqlCode 04000`,
/// "emitting function in expression").
pub(super) fn is_lone_count_distinct(items: &[SingleGroupItem]) -> bool {
    matches!(items, [SingleGroupItem::Distinct(dc)] if dc.column.is_some())
}

/// Parse a single-group `COUNT(DISTINCT ...)` select-list item into a
/// [`DistinctCount`] fan-out descriptor.
///
/// Handles both `COUNT(DISTINCT col)` (bare-column fast path) and
/// `COUNT(DISTINCT expr)` (rendered argument), mirroring how `COUNT(col)` /
/// `COUNT(expr)` are resolved. Returns `None` when the item is not a distinct
/// `COUNT`, or when its argument cannot be resolved to a column or rendered
/// expression — the single-group caller then defers to [`parse_agg_item`]
/// (which declines every other `distinct: true` item), so grouped
/// `COUNT(DISTINCT)` and other distinct aggregates still fall back to row scan.
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

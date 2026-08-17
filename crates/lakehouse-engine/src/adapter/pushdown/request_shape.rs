//! Shared request-shape classifier for the pushdown dispatcher.
//!
//! [`classify_request_shape`] owns the single routing decision that BOTH the
//! non-empty dispatcher ([`build_dispatch_sql`](super::build_dispatch_sql)) and
//! the empty-result path ([`empty_result_sql`](super::empty_result)) consume,
//! so the empty and non-empty output shapes are identical by construction rather
//! than by two independently-maintained routing trees that merely agree today
//! (issue #175 / plan `refactor-scan-spec-dispatch-dedup`).
//!
//! The classifier owns exactly the routing concerns:
//! - the 3-tier detection priority — GROUP BY aggregate → single-group aggregate
//!   → row scan;
//! - the [`validate_agg_col_types`] numeric-type gate on BOTH aggregate tiers (a
//!   non-numeric aggregate demotes to the next shape);
//! - whether a grouped HAVING renders over the partial/merge decomposition, and
//!   whether a grouped ORDER BY resolves over it. Expressibility IS a routing
//!   predicate, so [`render_having_over_merge`] and
//!   [`build_grouped_order_by_clause`] are called HERE and their decline routes to
//!   [`RequestShape::GroupByWrapper`] — which renders both natively over the
//!   materialized rows — rather than dropping them (issue #195) or erroring
//!   (issue #198).
//!
//! The grouped tier raises NO hard error, and no rendering-level decline is left
//! behind it. Every grouped decline — numeric-gate failure, an unrenderable HAVING,
//! or an unresolvable merge ORDER BY, with or without either present — takes the
//! same fall-through to `GroupByWrapper`.
//!
//! Each consumer renders only its own SQL from the returned shape; neither
//! re-derives any part of this priority or these gates.

use serde_json::Value as Json;

use super::grouped_agg::{GroupedOrderBy, build_grouped_order_by_clause, render_having_over_merge};
use super::single_group_agg::SingleGroupItem;
use super::{
    GroupedAggregateDetection, detect_aggregates, detect_group_by_aggregates, ordinary_plans,
    validate_agg_col_types,
};

/// The routing shape of a resolved pushdown request, decided once and consumed by
/// both the non-empty dispatcher and the empty-result path.
#[derive(Debug)]
pub(super) enum RequestShape {
    /// A GROUP BY aggregate that decomposes into the partial/merge grouped scan.
    /// Produced ONLY when the aggregate column types pass [`validate_agg_col_types`].
    ///
    /// `detection` is the ordered group-key / aggregate / select-item
    /// classification. `having` is the HAVING ALREADY RENDERED over the merge
    /// (`SUM(score)` → `SUM("PARTIAL_sum_0")`), not the raw node: the non-empty
    /// grouped renderer splices the fragment verbatim, while the empty path ignores
    /// it (a zero-row result already satisfies any HAVING). A HAVING that does not
    /// render over the merge never reaches this variant — it routes to
    /// [`RequestShape::GroupByWrapper`].
    ///
    /// `order_by` is the merge `ORDER BY` element list ALREADY RESOLVED over the
    /// same decomposition (a group key as its positional output ordinal, an
    /// aggregate as its merged `PARTIAL_*` expression), `None` when the request
    /// carries no `orderBy`. Like `having`, an ordering that does not resolve never
    /// reaches this variant.
    Grouped {
        detection: GroupedAggregateDetection,
        having: Option<String>,
        order_by: Option<String>,
    },
    /// A GROUP BY request that did NOT decompose — an undecomposable select item, a
    /// non-numeric aggregate that fell through the gate, a HAVING that does not
    /// render over the merge, or an ORDER BY that does not resolve over it. Reached
    /// with or without a HAVING or an ORDER BY present; the wrapper renders both
    /// natively over the materialized rows, so nothing is dropped and nothing
    /// errors. It routes to the qualified single-table wrapper whose output columns
    /// are the `selectList` items, NEVER a bare row scan (which would trip Exasol's
    /// positional column-count validation with SQL state `04000`).
    GroupByWrapper,
    /// A single-group aggregate (no GROUP BY) whose ordinary aggregate column types
    /// pass [`validate_agg_col_types`]. `items` preserves the select-list order; the
    /// non-empty renderer performs the lone-`COUNT(DISTINCT)` / multi-or-mixed-
    /// distinct / ordinary-aggregate sub-split, while the empty path collapses all
    /// three into one aggregate empty shape.
    SingleGroupAgg { items: Vec<SingleGroupItem> },
    /// A plain row scan: no decomposable aggregate, or an aggregate the numeric gate
    /// demoted.
    RowScan,
}

/// Decide the routing shape for a resolved pushdown request.
///
/// Applies the 3-tier priority (grouped aggregate → single-group aggregate → row
/// scan) with the [`validate_agg_col_types`] numeric gate on both aggregate tiers,
/// and resolves a grouped HAVING and a grouped ORDER BY over the merge decomposition
/// here, because whether they can be expressed decides the shape.
///
/// Every request resolves to a shape; all three grouped declines — the numeric gate
/// failing, a HAVING that does not render over the merge, and an ORDER BY that does
/// not resolve over it — fall through to [`RequestShape::GroupByWrapper`], which
/// renders both natively rather than dropping a predicate the adapter advertised
/// AGGREGATE_HAVING for or an ordering it advertised ORDER_BY_* for.
pub(super) fn classify_request_shape(
    pushdown_req: &Json,
    col_types: &[(String, String)],
) -> RequestShape {
    // Tier 1: GROUP BY aggregate (partial/merge decomposition).
    if let Some(detection) = detect_group_by_aggregates(pushdown_req) {
        // Same numeric gate the single-group tier applies below: a SUM over a
        // non-numeric column (VARCHAR, DATE, …) would produce an opaque UDF error,
        // so it must demote rather than push down.
        if validate_agg_col_types(&detection.plans, col_types) {
            // Resolve the merge ORDER BY HERE for the same reason as the HAVING
            // below: the outer merge wrapper's only columns are `GK_*` and
            // `PARTIAL_*`, so an aggregate absent from the select list has no
            // partial to sort on and the adapter will not fabricate one (issue
            // #198). `detect_group_by_aggregates` returns `Some` only for an
            // `aggregationType` of `group_by`, so declining here is exactly the
            // Tier 1b fall-through below.
            let order_by = match build_grouped_order_by_clause(pushdown_req, &detection) {
                Some(GroupedOrderBy::Clause(clause)) => Some(clause),
                Some(GroupedOrderBy::Unresolvable) => return RequestShape::GroupByWrapper,
                None => None,
            };
            match pushdown_req.get("having").filter(|h| !h.is_null()) {
                None => {
                    return RequestShape::Grouped {
                        detection,
                        having: None,
                        order_by,
                    };
                }
                // Rewrite the HAVING over the merge HERE: the outer merge wrapper's
                // only columns are `GK_*` and `PARTIAL_*`, so an aggregate absent
                // from the select list, a junction poisoned by one, or a DISTINCT
                // aggregate cannot be expressed there — and that is a decision about
                // the SHAPE, not about rendering within an already-chosen shape.
                Some(node) => {
                    if let Some(sql) = render_having_over_merge(node, &detection.plans) {
                        return RequestShape::Grouped {
                            detection,
                            having: Some(sql),
                            order_by,
                        };
                    }
                }
            }
        }
        // Every grouped decline — numeric-gate failure, an unrenderable HAVING, or an
        // unresolvable merge ORDER BY, with or without either present — falls through
        // to the GroupByWrapper tier below, which renders both natively over the
        // materialized rows rather than dropping them (issue #195) or erroring
        // (issue #198).
    }

    // Tier 1b: a GROUP BY request that did not decompose above routes to the
    // qualified single-table wrapper (its output columns are the `selectList`
    // items), never the bare row scan below.
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by") {
        return RequestShape::GroupByWrapper;
    }

    // Tier 2: single-group aggregate (validated against the ORDINARY plans only — a
    // distinct item is a row-scan fan-out, not an aggregate partial). Tier 3: row
    // scan when nothing decomposes or the gate demotes the aggregate.
    match detect_aggregates(pushdown_req)
        .filter(|it| validate_agg_col_types(&ordinary_plans(it), col_types))
    {
        Some(items) => RequestShape::SingleGroupAgg { items },
        None => RequestShape::RowScan,
    }
}

#[cfg(test)]
#[path = "request_shape_tests.rs"]
mod tests;

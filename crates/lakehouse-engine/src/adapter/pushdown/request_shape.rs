//! Shared request-shape classifier for the pushdown dispatcher.
//!
//! [`classify_request_shape`] owns the single routing decision that BOTH the
//! non-empty dispatcher ([`build_dispatch_sql`](super::build_dispatch_sql)) and
//! the empty-result path ([`empty_result_sql`](super::file_resolution)) consume,
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
mod tests {
    use super::super::test_support::*;
    use super::*;

    /// The fixed column universe every classifier case validates against: one
    /// numeric DECIMAL, one non-numeric VARCHAR, one DECIMAL id.
    fn col_types() -> Vec<(String, String)> {
        vec![
            ("AMOUNT".to_string(), "DECIMAL(18,2)".to_string()),
            ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
            ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ]
    }

    /// A GROUP BY over a NUMERIC aggregate (`SUM(AMOUNT)`) classifies as the
    /// decomposable grouped shape, carrying no HAVING.
    #[test]
    fn grouped_numeric_aggregate_classifies_as_grouped() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("AMOUNT"), false),
            ],
            "selectListDataTypes": [
                {"type": "varchar", "size": 2000000},
                {"type": "decimal", "precision": 36, "scale": 2},
            ],
        });
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::Grouped { having: None, .. }),
            "numeric grouped aggregate must decompose: {shape:?}"
        );
    }

    /// A GROUP BY over a NON-numeric aggregate (`SUM(NAME)`, VARCHAR) with NO HAVING
    /// fails the numeric gate and falls through to the qualified wrapper — never a
    /// grouped decomposition, never a bare row scan.
    #[test]
    fn grouped_non_numeric_without_having_falls_through_to_wrapper() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("NAME"), false),
            ],
        });
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::GroupByWrapper),
            "non-numeric grouped aggregate with no HAVING routes to the wrapper: {shape:?}"
        );
    }

    /// A GROUP BY over a NON-numeric aggregate that ALSO carries a HAVING no
    /// longer hard-errors: the gate failure falls through to the qualified
    /// wrapper exactly like its no-HAVING sibling above, because the wrapper
    /// renders the HAVING natively rather than dropping it.
    #[test]
    fn grouped_non_numeric_with_having_falls_through_to_wrapper() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("NAME"), false),
            ],
            "having": {"type": "predicate_greater"},
        });
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::GroupByWrapper),
            "non-numeric grouped aggregate with a HAVING routes to the wrapper: {shape:?}"
        );
    }

    /// A HAVING referencing an aggregate absent from the select list
    /// (`SUM(AMOUNT)` when only `COUNT(*)` was projected) cannot render over
    /// the merge, so the classifier falls through to the wrapper rather than
    /// erroring or committing to `Grouped` (issue #195).
    #[test]
    fn grouped_having_unmatched_aggregate_falls_through_to_wrapper() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("COUNT", None, false),
            ],
            "having": {
                "type": "predicate_greater",
                "left": agg_item("SUM", Some("AMOUNT"), false),
                "right": {"type": "literal_exactnumeric", "value": 10},
            },
        });
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::GroupByWrapper),
            "an unmatched HAVING aggregate must fall through to the wrapper, not Grouped or Err: {shape:?}"
        );
    }

    /// A mixed AND junction where one child matches a select-list plan
    /// (`COUNT(*) > 0`) and one does not (`SUM(AMOUNT) > 10`) must route to
    /// the wrapper as a whole — a partially-matching junction never renders a
    /// partial HAVING.
    #[test]
    fn grouped_having_mixed_junction_falls_through_to_wrapper() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("COUNT", None, false),
            ],
            "having": {
                "type": "predicate_and",
                "expressions": [
                    {
                        "type": "predicate_greater",
                        "left": agg_item("COUNT", None, false),
                        "right": {"type": "literal_exactnumeric", "value": 0},
                    },
                    {
                        "type": "predicate_greater",
                        "left": agg_item("SUM", Some("AMOUNT"), false),
                        "right": {"type": "literal_exactnumeric", "value": 10},
                    },
                ],
            },
        });
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::GroupByWrapper),
            "a partially-matching AND junction must fall through to the wrapper as a whole: {shape:?}"
        );
    }

    /// A HAVING whose aggregate IS present in the select list still decomposes:
    /// the classifier returns `Grouped` with the ALREADY-RENDERED merge SQL
    /// (`PARTIAL_sum_0`), not the raw source-column reference. This must fail
    /// on an implementation that routes every HAVING-carrying grouped request
    /// to the wrapper — a bare `matches!(shape, Grouped { .. })` would not
    /// catch that regression, since the field type changed to `Option<String>`.
    #[test]
    fn grouped_having_fully_matched_stays_grouped() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("AMOUNT"), false),
            ],
            "having": {
                "type": "predicate_greater",
                "left": agg_item("SUM", Some("AMOUNT"), false),
                "right": {"type": "literal_exactnumeric", "value": 10},
            },
        });
        let shape = classify_request_shape(&req, &col_types());
        match shape {
            RequestShape::Grouped {
                having: Some(sql), ..
            } => {
                assert!(
                    sql.contains("PARTIAL_sum_0"),
                    "rendered HAVING must reference the merged partial: {sql}"
                );
                assert!(
                    !sql.contains("AMOUNT"),
                    "rendered HAVING must NOT reference the source column AMOUNT: {sql}"
                );
            }
            other => panic!("expected Grouped {{ having: Some(sql), .. }}, got {other:?}"),
        }
    }

    /// A `COUNT(DISTINCT ID)` in the HAVING is the third route to the same
    /// `None`: `parse_agg_item` rejects `distinct: true` unconditionally, so
    /// `render_having_over_merge`'s internal `parse_agg_item(node)?`
    /// short-circuits before any plan lookup. Falls through to the wrapper.
    #[test]
    fn grouped_having_distinct_aggregate_falls_through_to_wrapper() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("COUNT", None, false),
            ],
            "having": {
                "type": "predicate_greater",
                "left": agg_item("COUNT", Some("ID"), true),
                "right": {"type": "literal_exactnumeric", "value": 1},
            },
        });
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::GroupByWrapper),
            "a DISTINCT aggregate in the HAVING must fall through to the wrapper: {shape:?}"
        );
    }

    /// Issue #198's own grouped repro — a GROUP-KEY-ONLY select list ordered by an
    /// aggregate absent from it, plus a `LIMIT` (`SELECT c_nationkey FROM CUSTOMER
    /// GROUP BY c_nationkey ORDER BY SUM(c_acctbal) DESC LIMIT 5`) — reaches the
    /// wrapper through an EMPTY aggregate-plan list: its lone select item classifies
    /// as a group key, so grouped detection succeeds with zero plans, the numeric
    /// gate passes vacuously, and the sort key then resolves against zero plans. It
    /// is NOT filtered out ahead of detection, and it MUST NOT error.
    #[test]
    fn unresolvable_grouped_order_by_classifies_group_by_wrapper_incl_group_key_only() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [{"type": "column", "name": "REGION"}],
            "orderBy": [{
                "type": "order_by_element",
                "expression": agg_item("SUM", Some("AMOUNT"), false),
                "isAscending": false,
                "nullsLast": true,
            }],
            "limit": 5,
        });
        assert!(
            detect_group_by_aggregates(&req)
                .expect("a group-key-only select list still detects as grouped")
                .plans
                .is_empty(),
            "the group-key-only shape must reach the classifier with an EMPTY plan list"
        );
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::GroupByWrapper),
            "an aggregate sort key resolvable against no plan must route to the wrapper, not Err: {shape:?}"
        );
    }

    /// The same unresolvable outcome from the OTHER direction: the select list
    /// carries a DIFFERENT aggregate (`COUNT(*)`), so the plan list is NON-empty and
    /// the sort key simply matches none of it. Same route, different plan-list state.
    #[test]
    fn unresolvable_grouped_order_by_with_nonempty_plans_classifies_group_by_wrapper() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("COUNT", None, false),
            ],
            "orderBy": [{
                "type": "order_by_element",
                "expression": agg_item("SUM", Some("AMOUNT"), false),
                "isAscending": false,
                "nullsLast": true,
            }],
        });
        assert_eq!(
            detect_group_by_aggregates(&req)
                .expect("grouped aggregate")
                .plans
                .len(),
            1,
            "this shape must reach the classifier with a NON-empty plan list"
        );
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::GroupByWrapper),
            "a sort key matching none of the non-empty plans must route to the wrapper: {shape:?}"
        );
    }

    /// A RESOLVABLE grouped `ORDER BY` still decomposes, and the classifier carries
    /// the already-rendered clause on the shape — the dispatcher no longer resolves
    /// it, so a classifier that always returned `None` here would silently drop
    /// every grouped ordering.
    #[test]
    fn grouped_order_by_group_key_classifies_grouped_with_resolved_clause() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("AMOUNT"), false),
            ],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "name": "REGION"},
                "isAscending": true,
                "nullsLast": true,
            }],
        });
        match classify_request_shape(&req, &col_types()) {
            RequestShape::Grouped { order_by, .. } => assert_eq!(
                order_by.as_deref(),
                Some("1 ASC NULLS LAST"),
                "the classifier must carry the resolved merge ORDER BY"
            ),
            other => panic!("expected Grouped, got {other:?}"),
        }
    }

    /// A single-group NUMERIC aggregate (no GROUP BY) classifies as single-group,
    /// carrying its resolved items in select-list order.
    #[test]
    fn single_group_numeric_aggregate_classifies_as_single_group() {
        let req = serde_json::json!({
            "selectList": [agg_item("SUM", Some("AMOUNT"), false)],
        });
        let shape = classify_request_shape(&req, &col_types());
        match shape {
            RequestShape::SingleGroupAgg { items } => assert_eq!(items.len(), 1),
            other => panic!("expected SingleGroupAgg, got {other:?}"),
        }
    }

    /// A single-group `COUNT(DISTINCT ID)` classifies as single-group (the non-empty
    /// renderer decides the lone-fan-out vs wrapper sub-split, not the classifier).
    #[test]
    fn single_group_count_distinct_classifies_as_single_group() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("ID"), true)],
        });
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::SingleGroupAgg { .. }),
            "a single-group COUNT(DISTINCT) is a single-group shape: {shape:?}"
        );
    }

    /// A plain projection (no aggregate, no GROUP BY) classifies as a row scan.
    #[test]
    fn plain_projection_classifies_as_row_scan() {
        let req = serde_json::json!({
            "selectList": [
                {"type": "column", "name": "REGION"},
                {"type": "column", "name": "AMOUNT"},
            ],
        });
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::RowScan),
            "a plain projection is a row scan: {shape:?}"
        );
    }

    /// A NON-numeric single-group aggregate (`SUM(NAME)`, VARCHAR, no GROUP BY) fails
    /// the numeric gate and demotes to a row scan (same gate as the grouped tier).
    #[test]
    fn non_numeric_single_group_aggregate_demotes_to_row_scan() {
        let req = serde_json::json!({
            "selectList": [agg_item("SUM", Some("NAME"), false)],
        });
        let shape = classify_request_shape(&req, &col_types());
        assert!(
            matches!(shape, RequestShape::RowScan),
            "a non-numeric single-group aggregate demotes to a row scan: {shape:?}"
        );
    }
}

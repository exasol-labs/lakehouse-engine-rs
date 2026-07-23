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
//! - the one routing-level hard-error decline — a non-numeric grouped aggregate
//!   that carries a HAVING (the adapter advertised AGGREGATE_HAVING, so dropping a
//!   HAVING it claims to handle would yield wrong results).
//!
//! Each consumer renders only its own SQL from the returned shape; neither
//! re-derives any part of this priority or these gates. Rendering-level declines
//! (a HAVING that cannot be merged, an unresolvable grouped ORDER BY) stay in the
//! non-empty grouped rendering arm — they are rendering, not routing.

use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::single_group_agg::SingleGroupItem;
use super::{
    GroupedAggregateDetection, detect_aggregates, detect_group_by_aggregates, ordinary_plans,
    validate_agg_col_types,
};

/// The routing shape of a resolved pushdown request, decided once and consumed by
/// both the non-empty dispatcher and the empty-result path.
#[derive(Debug)]
pub(super) enum RequestShape<'a> {
    /// A GROUP BY aggregate that decomposes into the partial/merge grouped scan.
    /// Produced ONLY when the aggregate column types pass [`validate_agg_col_types`].
    ///
    /// `detection` is the ordered group-key / aggregate / select-item
    /// classification. `having` is the raw HAVING node, resolved once here: the
    /// non-empty grouped renderer rewrites it over the merge, while the empty path
    /// ignores it (a zero-row result already satisfies any HAVING).
    Grouped {
        detection: GroupedAggregateDetection,
        having: Option<&'a Json>,
    },
    /// A GROUP BY request that did NOT decompose — an undecomposable select item,
    /// or a non-numeric aggregate with no HAVING that fell through the gate. It
    /// routes to the qualified single-table wrapper whose output columns are the
    /// `selectList` items, NEVER a bare row scan (which would trip Exasol's
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
/// scan) with the [`validate_agg_col_types`] numeric gate on both aggregate tiers.
/// Returns `Err` only for the single routing-level decline: a non-numeric grouped
/// aggregate that carries a HAVING (the adapter advertised AGGREGATE_HAVING, so
/// Exasol will not re-apply a dropped HAVING — a hard error, not a native re-plan).
pub(super) fn classify_request_shape<'a>(
    pushdown_req: &'a Json,
    col_types: &[(String, String)],
) -> Result<RequestShape<'a>, UdfError> {
    // Tier 1: GROUP BY aggregate (partial/merge decomposition).
    if let Some(detection) = detect_group_by_aggregates(pushdown_req) {
        // Same numeric gate the single-group tier applies below: a SUM over a
        // non-numeric column (VARCHAR, DATE, …) would produce an opaque UDF error,
        // so it must demote rather than push down.
        if validate_agg_col_types(&detection.plans, col_types) {
            let having = pushdown_req.get("having").filter(|h| !h.is_null());
            return Ok(RequestShape::Grouped { detection, having });
        }
        // Gate failed. A HAVING we advertised AGGREGATE_HAVING for cannot be
        // silently dropped — Exasol would not re-apply it, yielding wrong results.
        // Raise the routing-level hard error. Without a HAVING it is safe to fall
        // through to the group_by-wrapper / single-group / row-scan tiers below.
        if pushdown_req
            .get("having")
            .filter(|h| !h.is_null())
            .is_some()
        {
            return Err(UdfError::User(
                "grouped aggregate pushdown declined: HAVING present but aggregate \
                 column type is non-numeric; this is a hard error, not a native re-plan"
                    .into(),
            ));
        }
    }

    // Tier 1b: a GROUP BY request that did not decompose above routes to the
    // qualified single-table wrapper (its output columns are the `selectList`
    // items), never the bare row scan below.
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by") {
        return Ok(RequestShape::GroupByWrapper);
    }

    // Tier 2: single-group aggregate (validated against the ORDINARY plans only — a
    // distinct item is a row-scan fan-out, not an aggregate partial). Tier 3: row
    // scan when nothing decomposes or the gate demotes the aggregate.
    match detect_aggregates(pushdown_req)
        .filter(|it| validate_agg_col_types(&ordinary_plans(it), col_types))
    {
        Some(items) => Ok(RequestShape::SingleGroupAgg { items }),
        None => Ok(RequestShape::RowScan),
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
        let shape = classify_request_shape(&req, &col_types()).expect("must classify");
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
        let shape = classify_request_shape(&req, &col_types()).expect("must classify");
        assert!(
            matches!(shape, RequestShape::GroupByWrapper),
            "non-numeric grouped aggregate with no HAVING routes to the wrapper: {shape:?}"
        );
    }

    /// A GROUP BY over a NON-numeric aggregate that ALSO carries a HAVING cannot
    /// silently demote (AGGREGATE_HAVING is advertised): the classifier declines
    /// with the verbatim hard-error message.
    #[test]
    fn grouped_non_numeric_with_having_declines_hard() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("NAME"), false),
            ],
            "having": {"type": "predicate_greater"},
        });
        let err = classify_request_shape(&req, &col_types()).unwrap_err();
        match err {
            UdfError::User(msg) => assert!(
                msg.contains("HAVING present but aggregate column type is non-numeric"),
                "decline message must name the HAVING conflict verbatim: {msg}"
            ),
            other => panic!("expected UdfError::User, got {other:?}"),
        }
    }

    /// A single-group NUMERIC aggregate (no GROUP BY) classifies as single-group,
    /// carrying its resolved items in select-list order.
    #[test]
    fn single_group_numeric_aggregate_classifies_as_single_group() {
        let req = serde_json::json!({
            "selectList": [agg_item("SUM", Some("AMOUNT"), false)],
        });
        let shape = classify_request_shape(&req, &col_types()).expect("must classify");
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
        let shape = classify_request_shape(&req, &col_types()).expect("must classify");
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
        let shape = classify_request_shape(&req, &col_types()).expect("must classify");
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
        let shape = classify_request_shape(&req, &col_types()).expect("must classify");
        assert!(
            matches!(shape, RequestShape::RowScan),
            "a non-numeric single-group aggregate demotes to a row scan: {shape:?}"
        );
    }
}

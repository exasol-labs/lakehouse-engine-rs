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

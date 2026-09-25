use super::super::test_support::*;
use super::*;

fn col_types() -> Vec<(String, String)> {
    vec![
        ("AMOUNT".to_string(), "DECIMAL(18,2)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
    ]
}

/// Scenario: a GROUP BY over a numeric aggregate classifies as Grouped with no HAVING
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

/// Scenario: a non-numeric grouped aggregate without HAVING falls through to the wrapper
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

/// Scenario: a non-numeric grouped aggregate with HAVING falls through to the wrapper
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

/// Scenario: a HAVING on an aggregate absent from the select list routes to the wrapper (#195)
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

/// Scenario: a partially-matching HAVING AND junction routes to the wrapper as a whole
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

/// Scenario: a fully-matched HAVING stays Grouped with SQL rendered over the merged partial
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

/// Scenario: a COUNT(DISTINCT) in the HAVING routes to the wrapper
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

/// Scenario: a group-key-only select list ordered by an absent aggregate routes to the wrapper (#198)
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

/// Scenario: a sort key matching none of a non-empty plan list routes to the wrapper
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

/// Scenario: a resolvable grouped ORDER BY stays Grouped carrying the resolved clause
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

/// Scenario: a numeric single-group aggregate classifies as SingleGroupAgg
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

/// Scenario: a single-group COUNT(DISTINCT) classifies as SingleGroupAgg
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

/// Scenario: a plain projection classifies as a row scan
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

/// Scenario: a non-numeric single-group aggregate demotes to a row scan
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

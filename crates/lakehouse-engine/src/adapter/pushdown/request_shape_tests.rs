use super::super::test_support::*;
use super::*;
use crate::types::exasol_type::ExasolType;
use serde_json::Value as Json;

const T_COLUMNS: [(&str, ExasolType); 4] = [
    ("REGION", ExasolType::WIDEST_VARCHAR),
    ("AMOUNT", ExasolType::decimal(18, 2)),
    ("NAME", ExasolType::WIDEST_VARCHAR),
    ("ID", ExasolType::decimal(20, 0)),
];

#[test]
fn grouped_numeric_aggregate_classifies_as_grouped() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .group_by(vec![col("REGION")])
        .column(col("REGION"))
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .build_scanned();
    let shape = classify_request_shape(&req, scan);
    assert!(
        matches!(shape, RequestShape::Grouped { having: None, .. }),
        "numeric grouped aggregate must decompose: {shape:?}"
    );
}

#[test]
fn grouped_non_numeric_without_having_falls_through_to_wrapper() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .group_by(vec![col("REGION")])
        .column(col("REGION"))
        .item(
            agg_item("SUM", Some("NAME"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_scanned();
    let shape = classify_request_shape(&req, scan);
    assert!(
        matches!(shape, RequestShape::GroupByWrapper),
        "non-numeric grouped aggregate with no HAVING routes to the wrapper: {shape:?}"
    );
}

#[test]
fn grouped_non_numeric_with_having_falls_through_to_wrapper() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .group_by(vec![col("REGION")])
        .field(
            "having",
            compare(
                "predicate_greater",
                agg_item("SUM", Some("NAME"), false),
                num_lit(1),
            ),
        )
        .column(col("REGION"))
        .item(
            agg_item("SUM", Some("NAME"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_scanned();
    let shape = classify_request_shape(&req, scan);
    assert!(
        matches!(shape, RequestShape::GroupByWrapper),
        "non-numeric grouped aggregate with a HAVING routes to the wrapper: {shape:?}"
    );
}

#[test]
fn grouped_having_unmatched_aggregate_falls_through_to_wrapper() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .group_by(vec![col("REGION")])
        .field(
            "having",
            compare(
                "predicate_greater",
                agg_item("SUM", Some("AMOUNT"), false),
                num_lit(10),
            ),
        )
        .column(col("REGION"))
        .count_star()
        .build_scanned();
    let shape = classify_request_shape(&req, scan);
    assert!(
        matches!(shape, RequestShape::GroupByWrapper),
        "an unmatched HAVING aggregate must fall through to the wrapper, not Grouped or Err: {shape:?}"
    );
}

#[test]
fn grouped_having_mixed_junction_falls_through_to_wrapper() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .group_by(vec![col("REGION")])
        .field(
            "having",
            and([
                compare("predicate_greater", count_star(), num_lit(0)),
                compare(
                    "predicate_greater",
                    agg_item("SUM", Some("AMOUNT"), false),
                    num_lit(10),
                ),
            ]),
        )
        .column(col("REGION"))
        .count_star()
        .build_scanned();
    let shape = classify_request_shape(&req, scan);
    assert!(
        matches!(shape, RequestShape::GroupByWrapper),
        "a partially-matching AND junction must fall through to the wrapper as a whole: {shape:?}"
    );
}

#[test]
fn grouped_having_fully_matched_stays_grouped() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .group_by(vec![col("REGION")])
        .field(
            "having",
            compare(
                "predicate_greater",
                agg_item("SUM", Some("AMOUNT"), false),
                num_lit(10),
            ),
        )
        .column(col("REGION"))
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_scanned();
    let shape = classify_request_shape(&req, scan);
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

#[test]
fn grouped_having_distinct_aggregate_falls_through_to_wrapper() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .group_by(vec![col("REGION")])
        .field(
            "having",
            compare(
                "predicate_greater",
                agg_item("COUNT", Some("ID"), true),
                num_lit(1),
            ),
        )
        .column(col("REGION"))
        .count_star()
        .build_scanned();
    let shape = classify_request_shape(&req, scan);
    assert!(
        matches!(shape, RequestShape::GroupByWrapper),
        "a DISTINCT aggregate in the HAVING must fall through to the wrapper: {shape:?}"
    );
}

#[test]
fn unresolvable_grouped_order_by_classifies_group_by_wrapper_incl_group_key_only() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .group_by(vec![col("REGION")])
        .order_by(serde_json::json!([order_by_element(
            agg_item("SUM", Some("AMOUNT"), false),
            false,
            true
        )]))
        .field("limit", serde_json::json!(5))
        .column(col("REGION"))
        .build_scanned();
    assert!(
        detect_group_by_aggregates(&req, scan)
            .expect("a group-key-only select list still detects as grouped")
            .aggregates
            .plans
            .is_empty(),
        "the group-key-only shape must reach the classifier with an EMPTY plan list"
    );
    let shape = classify_request_shape(&req, scan);
    assert!(
        matches!(shape, RequestShape::GroupByWrapper),
        "an aggregate sort key resolvable against no plan must route to the wrapper, not Err: {shape:?}"
    );
}

#[test]
fn unresolvable_grouped_order_by_with_nonempty_plans_classifies_group_by_wrapper() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .group_by(vec![col("REGION")])
        .order_by(serde_json::json!([order_by_element(
            agg_item("SUM", Some("AMOUNT"), false),
            false,
            true
        )]))
        .column(col("REGION"))
        .count_star()
        .build_scanned();
    assert_eq!(
        detect_group_by_aggregates(&req, scan)
            .expect("grouped aggregate")
            .aggregates
            .plans
            .len(),
        1,
        "this shape must reach the classifier with a NON-empty plan list"
    );
    let shape = classify_request_shape(&req, scan);
    assert!(
        matches!(shape, RequestShape::GroupByWrapper),
        "a sort key matching none of the non-empty plans must route to the wrapper: {shape:?}"
    );
}

#[test]
fn grouped_order_by_group_key_classifies_grouped_with_resolved_clause() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .group_by(vec![col("REGION")])
        .order_by(serde_json::json!([order_by_element(
            col("REGION"),
            true,
            true
        )]))
        .column(col("REGION"))
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_scanned();
    match classify_request_shape(&req, scan) {
        RequestShape::Grouped { order_by, .. } => assert_eq!(
            order_by.as_deref(),
            Some(r#""GK_0" ASC NULLS LAST"#),
            "the classifier must carry the resolved merge ORDER BY"
        ),
        other => panic!("expected Grouped, got {other:?}"),
    }
}

#[test]
fn single_group_numeric_aggregate_classifies_as_single_group() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_scanned();
    let shape = classify_request_shape(&req, scan);
    match shape {
        RequestShape::SingleGroupAgg {
            items,
            having: None,
            ..
        } => assert_eq!(items.len(), 1),
        other => panic!("expected SingleGroupAgg, got {other:?}"),
    }
}

#[test]
fn single_group_count_distinct_classifies_as_single_group() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .item(
            agg_item("COUNT", Some("ID"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_scanned();
    let shape = classify_request_shape(&req, scan);
    assert!(
        matches!(shape, RequestShape::SingleGroupAgg { .. }),
        "a single-group COUNT(DISTINCT) is a single-group shape: {shape:?}"
    );
}

#[test]
fn plain_projection_classifies_as_row_scan() {
    let (req, scan) = pushdown_request("T", &T_COLUMNS)
        .column(col("REGION"))
        .column(col("AMOUNT"))
        .build_scanned();
    let shape = classify_request_shape(&req, scan);
    assert!(
        matches!(shape, RequestShape::RowScan),
        "a plain projection is a row scan: {shape:?}"
    );
}

/// Every item declared `VARCHAR(2000000)`: the classification reads no item type.
fn single_group(
    select_list: Json,
    having: Option<Json>,
) -> (PushdownRequest, ScanTranslator<'static>) {
    let items = select_list.as_array().expect("a select-list array").clone();
    let request = items
        .into_iter()
        .fold(pushdown_request("T", &T_COLUMNS), |request, item| {
            request.item(item, ExasolType::WIDEST_VARCHAR)
        })
        .single_group();
    let request = match having {
        Some(having) => request.field("having", having),
        None => request,
    };
    request.build_scanned()
}

fn count_greater_than(n: i64) -> Json {
    compare("predicate_greater", count_star(), num_lit(n))
}

#[test]
fn single_group_having_over_a_split_aggregate_renders_over_the_merge() {
    let (req, scan) = single_group(
        serde_json::json!([count_star()]),
        Some(count_greater_than(100)),
    );
    match classify_request_shape(&req, scan) {
        RequestShape::SingleGroupAgg {
            having: Some(having),
            ..
        } => assert!(having.contains("> 100"), "{having}"),
        other => panic!("expected SingleGroupAgg with HAVING, got {other:?}"),
    }
}

#[test]
fn single_group_that_does_not_split_never_classifies_as_row_scan() {
    let literal = str_lit("X");
    for (req, scan) in [
        single_group(
            serde_json::json!([agg_item("SUM", Some("NAME"), false)]),
            None,
        ),
        single_group(serde_json::json!([literal.clone(), count_star()]), None),
        single_group(serde_json::json!([literal]), Some(count_greater_than(0))),
        single_group(
            serde_json::json!([count_star()]),
            Some(compare(
                "predicate_greater",
                agg_item("MAX", Some("AMOUNT"), false),
                num_lit(0),
            )),
        ),
        single_group(
            serde_json::json!([agg_item("COUNT", Some("ID"), true)]),
            Some(predicate_equal(num_lit(1), num_lit(1))),
        ),
    ] {
        let shape = classify_request_shape(&req, scan);
        assert!(
            matches!(shape, RequestShape::SingleGroupWrapper),
            "{}: {shape:?}",
            req.json()
        );
    }
}

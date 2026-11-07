use super::super::grouped_agg::partial_emits_items;
use super::super::scalar_over_agg::cast_merge_items;
use super::super::support::ColumnTypes;
use super::super::test_support::*;
use super::super::validate_agg_col_types;
use super::*;
use crate::scan::spec::AggKind;

fn agg_of(item: &SingleGroupItem) -> &AggregatePlan {
    match item {
        SingleGroupItem::Aggregate(plan) => plan,
        SingleGroupItem::Distinct(_) | SingleGroupItem::ScalarOverAggregate { .. } => {
            panic!("expected an ordinary aggregate item")
        }
    }
}

fn distinct_of(item: &SingleGroupItem) -> &DistinctCount {
    match item {
        SingleGroupItem::Distinct(dc) => dc,
        SingleGroupItem::Aggregate(_) | SingleGroupItem::ScalarOverAggregate { .. } => {
            panic!("expected a COUNT(DISTINCT) item")
        }
    }
}

fn length_expr(column: &str) -> serde_json::Value {
    scalar_fn("LENGTH", [col(column)])
}

/// Exasol's `MULT` node, pushed once `FN_MULT` is advertised (decision-log [7]).
fn mult_expr(a: &str, b: &str) -> serde_json::Value {
    scalar_fn("MULT", [col(a), col(b)])
}

#[test]
fn detect_aggregates_accepts_scalar_over_aggregate_and_still_declines_undecomposable() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            round_of(agg_item("SUM", Some("L_QUANTITY"), false), 2),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("ROUND(SUM(col), 2) must decompose");
    assert_eq!(items.len(), 1, "one select item, one resolved item");
    assert!(
        !has_distinct(&items),
        "a scalar-over-aggregate is not a distinct fan-out"
    );

    let variance_req = pushdown_request("T", NO_COLUMNS)
        .item(
            round_of(agg_item("VARIANCE", Some("C_ACCTBAL"), false), 4),
            ExasolType::decimal(36, 4),
        )
        .build_pushdown();
    assert!(
        detect_aggregates(&variance_req, &ColumnTypes::default()).is_some(),
        "ROUND(VARIANCE(col), 4) must decompose rather than reach DataFusion by name"
    );

    let interleaved = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("L_QUANTITY"), false),
            ExasolType::decimal(36, 2),
        )
        .item(
            round_of(
                float_div(agg_item("SUM", Some("L_QUANTITY"), false), count_star()),
                2,
            ),
            ExasolType::decimal(36, 4),
        )
        .build_pushdown();
    assert_eq!(
        detect_aggregates(&interleaved, &ColumnTypes::default())
            .expect("an interleaved list must decompose")
            .len(),
        2
    );

    let distinct_req = pushdown_request("T", NO_COLUMNS)
        .item(
            round_of(agg_item("COUNT", Some("L_ORDERKEY"), true), 2),
            ExasolType::decimal(18, 0),
        )
        .build_pushdown();
    assert!(
        detect_aggregates(&distinct_req, &ColumnTypes::default()).is_none(),
        "ROUND(COUNT(DISTINCT col), 2) must decline the whole detection"
    );

    let median_req = pushdown_request("T", NO_COLUMNS)
        .item(
            round_of(agg_item("MEDIAN", Some("L_QUANTITY"), false), 2),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();
    assert!(
        detect_aggregates(&median_req, &ColumnTypes::default()).is_none(),
        "ROUND(MEDIAN(col), 2) must decline the whole detection"
    );

    let residual_req = pushdown_request("T", NO_COLUMNS)
        .item(
            scalar_fn(
                "MULT",
                [
                    agg_item("SUM", Some("L_QUANTITY"), false),
                    col("L_QUANTITY"),
                ],
            ),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();
    assert!(
        detect_aggregates(&residual_req, &ColumnTypes::default()).is_none(),
        "a residual bare column must decline the whole detection"
    );

    let scalar_only = pushdown_request("T", NO_COLUMNS)
        .item(length_expr("L_COMMENT"), ExasolType::decimal(18, 0))
        .build_pushdown();
    assert!(
        detect_aggregates(&scalar_only, &ColumnTypes::default()).is_none(),
        "a scalar item with no nested aggregate must still decline"
    );
}

#[test]
fn single_group_scalar_over_aggregate_preserves_selectlist_order_and_item_types() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("L_QUANTITY"), false),
            ExasolType::decimal(36, 2),
        )
        .item(
            round_of(
                float_div(agg_item("SUM", Some("L_QUANTITY"), false), count_star()),
                2,
            ),
            ExasolType::decimal(9, 4),
        )
        .count_star()
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("an interleaved list must decompose");
    assert_eq!(items.len(), 3);
    assert_eq!(agg_of(&items[0]).kind, AggKind::Sum);
    assert!(
        matches!(
            &items[1],
            SingleGroupItem::ScalarOverAggregate {
                declared_type,
                node,
            } if *declared_type == ExasolType::decimal(9, 4) && node == &req.json()["selectList"][1]
        ),
        "item 1 must be a ScalarOverAggregate at ordinal 1 carrying its own \
         declared type and the verbatim node: {:?}",
        items[1]
    );
    assert_eq!(agg_of(&items[2]).kind, AggKind::Count);

    assert_eq!(ordinary_plans(&items, &ColumnTypes::default()).len(), 2);
}

#[test]
fn single_group_scalar_over_aggregate_dedups_shared_inner_aggregates() {
    let req = pushdown_request("T", NO_COLUMNS)
        .count_star()
        .item(
            round_of(
                float_div(agg_item("SUM", Some("L_QUANTITY"), false), count_star()),
                2,
            ),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("COUNT(*) + ROUND(SUM/COUNT) must decompose");
    let plans = ordinary_plans(&items, &ColumnTypes::default());
    assert_eq!(
        plans.len(),
        2,
        "the nested COUNT(*) must dedup against the bare COUNT(*): {plans:?}"
    );
    assert_eq!(
        plans[0],
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None
        },
        "slot 0 is the bare COUNT(*), which the nested occurrence reuses"
    );
    assert_eq!(plans[1].kind, AggKind::Sum);
    assert_eq!(plans[1].column.as_deref(), Some("L_QUANTITY"));
}

#[test]
fn single_group_scalar_over_aggregate_folds_nested_plans_in_encounter_order() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            round_of(
                float_div(agg_item("SUM", Some("L_QUANTITY"), false), count_star()),
                2,
            ),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();
    let items =
        detect_aggregates(&req, &ColumnTypes::default()).expect("ROUND(SUM/COUNT) must decompose");
    let plans = ordinary_plans(&items, &ColumnTypes::default());
    assert_eq!(plans.len(), 2, "{plans:?}");
    assert_eq!(plans[0].kind, AggKind::Sum);
    assert_eq!(plans[1].kind, AggKind::Count);
}

#[test]
fn ordinary_plans_unchanged_for_bare_aggregate_select_lists() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .count_star()
        .item(
            agg_item("MIN", Some("TS"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("a bare aggregate list must decompose");
    let plans = ordinary_plans(&items, &ColumnTypes::default());
    assert_eq!(plans.len(), 3);
    assert_eq!(plans[0].kind, AggKind::Sum);
    assert_eq!(plans[1].kind, AggKind::Count);
    assert_eq!(plans[2].kind, AggKind::Min);
}

#[test]
fn detect_count_star_produces_count_no_column() {
    let req = pushdown_request("T", NO_COLUMNS)
        .count_star()
        .build_pushdown();
    let plans = detect_aggregates(&req, &ColumnTypes::default()).expect("should detect COUNT(*)");
    assert_eq!(plans.len(), 1);
    assert_eq!(agg_of(&plans[0]).kind, AggKind::Count);
    assert!(agg_of(&plans[0]).column.is_none());
}

#[test]
fn detect_count_col_produces_count_col() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("COUNT", Some("amount"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let plans = detect_aggregates(&req, &ColumnTypes::default()).expect("should detect COUNT(col)");
    assert_eq!(agg_of(&plans[0]).kind, AggKind::CountCol);
    assert_eq!(agg_of(&plans[0]).column.as_deref(), Some("AMOUNT"));
}

#[test]
fn detect_sum_min_max_avg_produce_correct_plans() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("amount"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("MIN", Some("ts"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("MAX", Some("ts"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("AVG", Some("score"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let plans = detect_aggregates(&req, &ColumnTypes::default()).expect("should detect all four");
    assert_eq!(agg_of(&plans[0]).kind, AggKind::Sum);
    assert_eq!(agg_of(&plans[0]).column.as_deref(), Some("AMOUNT"));
    assert_eq!(agg_of(&plans[1]).kind, AggKind::Min);
    assert_eq!(agg_of(&plans[1]).column.as_deref(), Some("TS"));
    assert_eq!(agg_of(&plans[2]).kind, AggKind::Max);
    assert_eq!(agg_of(&plans[2]).column.as_deref(), Some("TS"));
    assert_eq!(agg_of(&plans[3]).kind, AggKind::Avg);
    assert_eq!(agg_of(&plans[3]).column.as_deref(), Some("SCORE"));
}

#[test]
fn detect_aggregates_falls_back_on_group_by() {
    let req = pushdown_request("T", NO_COLUMNS)
        .field("groupBy", serde_json::json!([col("region")]))
        .item(
            agg_item("SUM", Some("amount"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    assert!(
        detect_aggregates(&req, &ColumnTypes::default()).is_none(),
        "must fall back when GROUP BY is present"
    );
}

#[test]
fn detect_aggregates_falls_back_on_distinct() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("amount"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    assert!(
        detect_aggregates(&req, &ColumnTypes::default()).is_none(),
        "must fall back when a non-COUNT DISTINCT is present"
    );
}

#[test]
fn detect_aggregates_falls_back_on_unsupported_function() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("amount"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("MEDIAN", Some("amount"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    assert!(
        detect_aggregates(&req, &ColumnTypes::default()).is_none(),
        "must fall back when any item is unsupported"
    );
}

#[test]
fn detect_aggregates_falls_back_on_column_select() {
    let req = pushdown_request("T", &[("region", ExasolType::WIDEST_VARCHAR)])
        .column(col("region"))
        .build_pushdown();
    assert!(
        detect_aggregates(&req, &ColumnTypes::default()).is_none(),
        "must fall back when select list contains non-aggregate"
    );
}

#[test]
fn detect_aggregates_returns_none_for_empty_select_list() {
    let req = pushdown_request("T", NO_COLUMNS)
        .empty_select_list()
        .build_pushdown();
    assert!(detect_aggregates(&req, &ColumnTypes::default()).is_none());
}

#[test]
fn bare_column_aggregates_unchanged_regression() {
    let req = pushdown_request("T", NO_COLUMNS)
        .count_star()
        .item(
            agg_item("COUNT", Some("id"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("SUM", Some("amount"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("MIN", Some("ts"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("MAX", Some("ts"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("AVG", Some("score"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("STDDEV", Some("score"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let plans = detect_aggregates(&req, &ColumnTypes::default())
        .expect("bare-column aggregates must decompose");
    assert!(
        plans.iter().all(|p| agg_of(p).arg_expr.is_none()),
        "bare-column aggregates must never populate arg_expr: {plans:?}"
    );
    assert_eq!(agg_of(&plans[0]).kind, AggKind::Count);
    assert!(agg_of(&plans[0]).column.is_none());
    assert_eq!(agg_of(&plans[1]).kind, AggKind::CountCol);
    assert_eq!(agg_of(&plans[1]).column.as_deref(), Some("ID"));
    assert_eq!(agg_of(&plans[2]).kind, AggKind::Sum);
    assert_eq!(agg_of(&plans[2]).column.as_deref(), Some("AMOUNT"));
    assert_eq!(agg_of(&plans[5]).kind, AggKind::Avg);
    assert_eq!(agg_of(&plans[6]).kind, AggKind::Stddev);

    let col_types = vec![
        ("AMOUNT".to_string(), ExasolType::decimal(20, 0)),
        ("SCORE".to_string(), ExasolType::DoublePrecision),
        ("TS".to_string(), ExasolType::BARE_TIMESTAMP),
    ];
    let sum_only = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("AMOUNT".into()),
        arg_expr: None,
    }];
    let emits = partial_emits_items(
        &sum_only,
        &column_types(&col_types),
        &[ExasolType::WIDEST_VARCHAR],
    );
    assert_eq!(emits, vec![r#""PARTIAL_sum_0" DECIMAL(36,0)"#.to_string()]);
}

#[test]
fn expression_arg_partial_and_merge_types_from_declared_type() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item_expr("SUM", length_expr("L_COMMENT"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let plans = detect_aggregates(&req, &ColumnTypes::default())
        .expect("expression-argument SUM must decompose");
    assert_eq!(agg_of(&plans[0]).kind, AggKind::Sum);
    assert!(
        agg_of(&plans[0]).column.is_none(),
        "expression argument must not populate column"
    );
    assert_eq!(
        agg_of(&plans[0]).arg_expr.as_deref(),
        Some(r#"character_length("L_COMMENT")"#),
        "the rendered DataFusion fragment must be carried in arg_expr"
    );

    // Deliberately no matching entry in col_types: the type must come from the declared type.
    let col_types: Vec<(String, ExasolType)> = vec![];

    let sum_expr = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: None,
        arg_expr: Some(r#"character_length("L_COMMENT")"#.into()),
    }];
    let emits = partial_emits_items(
        &sum_expr,
        &column_types(&col_types),
        &[ExasolType::decimal(36, 4)],
    );
    assert_eq!(emits, vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()]);
    let emits = partial_emits_items(
        &sum_expr,
        &column_types(&col_types),
        &[ExasolType::DoublePrecision],
    );
    assert_eq!(
        emits,
        vec![r#""PARTIAL_sum_0" DOUBLE PRECISION"#.to_string()]
    );

    let min_expr = vec![AggregatePlan {
        kind: AggKind::Min,
        column: None,
        arg_expr: Some(r#"("A" + "B")"#.into()),
    }];
    let emits = partial_emits_items(&min_expr, &column_types(&col_types), &[ExasolType::Date]);
    assert_eq!(emits, vec![r#""PARTIAL_min_0" DATE"#.to_string()]);

    let count_expr = vec![AggregatePlan {
        kind: AggKind::CountCol,
        column: None,
        arg_expr: Some(r#"character_length("L_COMMENT")"#.into()),
    }];
    let emits = partial_emits_items(
        &count_expr,
        &column_types(&col_types),
        &[ExasolType::decimal(18, 0)],
    );
    assert_eq!(
        emits,
        vec![r#""PARTIAL_count_0" DECIMAL(20,0)"#.to_string()]
    );

    assert!(
        validate_agg_col_types(&sum_expr, &column_types(&col_types)),
        "expression-argument SUM must pass validation, not force a row scan"
    );
}

#[test]
fn decimal_product_sum_partial_widens_to_decimal_36() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item_expr("SUM", mult_expr("L_EXTENDEDPRICE", "L_DISCOUNT"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("SUM(col * col) must decompose, not fall back to scan");
    assert_eq!(items.len(), 1);
    let plans = ordinary_plans(&items, &ColumnTypes::default());
    assert_eq!(plans[0].kind, AggKind::Sum);
    assert!(
        plans[0].column.is_none(),
        "a two-column product has no single source column"
    );
    assert_eq!(
        plans[0].arg_expr.as_deref(),
        Some(r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#),
        "the rendered product must be carried in arg_expr"
    );

    // No operand column in col_types: the declared type is authoritative.
    let col_types: Vec<(String, ExasolType)> = vec![];
    let declared = [ExasolType::decimal(36, 4)];

    let emits = partial_emits_items(&plans, &column_types(&col_types), &declared);
    assert_eq!(
        emits,
        vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()],
        "partial SUM column must widen to the declared DECIMAL(36,4)"
    );

    let merge = cast_merge_items(&plans, &declared, &ColumnTypes::default());
    assert_eq!(
        merge,
        vec![r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,4))"#.to_string()],
        "merge must cast the summed partial to the declared DECIMAL(36,4)"
    );

    assert!(validate_agg_col_types(&plans, &column_types(&col_types)));
}

#[test]
fn unrenderable_agg_arg_falls_back_to_row_scan() {
    let unknown = scalar_fn("TOTALLY_UNKNOWN_FN", [col("id")]);
    for name in &["SUM", "MIN", "MAX", "AVG", "COUNT"] {
        let req = pushdown_request("T", NO_COLUMNS)
            .item(
                agg_item_expr(name, unknown.clone(), false),
                ExasolType::WIDEST_VARCHAR,
            )
            .build_pushdown();
        assert!(
            detect_aggregates(&req, &ColumnTypes::default()).is_none(),
            "{name} over an unrenderable argument must fall back to row scan"
        );
    }
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item_expr("COUNT", unknown.clone(), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    assert!(
        detect_aggregates(&req, &ColumnTypes::default()).is_none(),
        "COUNT(DISTINCT unrenderable) must fall back to row scan"
    );
}

#[test]
fn count_distinct_builds_distinct_row_scan_spec() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("COUNT", Some("L_SHIPMODE"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("single-group COUNT(DISTINCT) must decompose");
    assert_eq!(items.len(), 1);
    assert!(has_distinct(&items), "the item must be a distinct fan-out");
    let dc = distinct_of(&items[0]);
    assert_eq!(dc.column.as_deref(), Some("L_SHIPMODE"));
    assert!(dc.arg_expr.is_none());
    assert!(
        ordinary_plans(&items, &ColumnTypes::default()).is_empty(),
        "a COUNT(DISTINCT) item must not appear among the ordinary aggregate plans"
    );

    let req_expr = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item_expr("COUNT", length_expr("L_COMMENT"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let items_expr = detect_aggregates(&req_expr, &ColumnTypes::default())
        .expect("COUNT(DISTINCT expr) must decompose");
    let dc_expr = distinct_of(&items_expr[0]);
    assert!(dc_expr.column.is_none());
    assert_eq!(
        dc_expr.arg_expr.as_deref(),
        Some(r#"character_length("L_COMMENT")"#)
    );
}

#[test]
fn multi_count_distinct_declines_to_qualified_wrapper() {
    let lone = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("COUNT", Some("CATEGORY"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let items = detect_aggregates(&lone, &ColumnTypes::default())
        .expect("a lone COUNT(DISTINCT) decomposes");
    assert!(
        has_distinct(&items) && is_lone_count_distinct(&items),
        "a lone COUNT(DISTINCT) is the only shape that fans out (Case 1)"
    );

    let multi = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("COUNT", Some("CATEGORY"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("COUNT", Some("REGION"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let items = detect_aggregates(&multi, &ColumnTypes::default())
        .expect("multiple distinct items still detect");
    assert!(
        has_distinct(&items) && !is_lone_count_distinct(&items),
        "more than one COUNT(DISTINCT) must decline the fan-out (Case 2)"
    );

    let mixed = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("COUNT", Some("CATEGORY"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let items = detect_aggregates(&mixed, &ColumnTypes::default())
        .expect("distinct-plus-ordinary still detects");
    assert!(
        has_distinct(&items) && !is_lone_count_distinct(&items),
        "a distinct mixed with an ordinary aggregate must decline the fan-out (Case 3)"
    );
}

#[test]
fn lone_expression_count_distinct_declines_fan_out_to_wrapper() {
    let expr = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item_expr("COUNT", length_expr("NAME"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let items = detect_aggregates(&expr, &ColumnTypes::default())
        .expect("a lone COUNT(DISTINCT expr) still decomposes to a distinct item");
    assert_eq!(items.len(), 1);
    let dc = distinct_of(&items[0]);
    assert!(
        dc.column.is_none() && dc.arg_expr.is_some(),
        "the argument is an expression, so the distinct carries a rendered arg_expr, \
         not a bare column"
    );
    assert!(
        has_distinct(&items),
        "a lone expression COUNT(DISTINCT) is still a distinct request"
    );
    assert!(
        !is_lone_count_distinct(&items),
        "an EXPRESSION-argument lone COUNT(DISTINCT) must NOT fan out — it declines to \
         the qualified single-table wrapper exactly like a Case 2/3 request (this is \
         the regression: before the dispatch narrowing it wrongly fanned out with a \
         VARCHAR-typed \"V\")"
    );

    let bare = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("COUNT", Some("NAME"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let bare_items = detect_aggregates(&bare, &ColumnTypes::default())
        .expect("a lone bare-column COUNT(DISTINCT) decomposes");
    assert!(
        is_lone_count_distinct(&bare_items),
        "a lone BARE-COLUMN COUNT(DISTINCT) is unaffected — it still fans out (Case 1)"
    );
}

#[test]
fn non_decomposable_aggregate_falls_back_to_row_scan() {
    for name in &[
        "MEDIAN",
        "APPROXIMATE_COUNT_DISTINCT",
        "LISTAGG",
        "GROUP_CONCAT",
    ] {
        let req = pushdown_request("T", NO_COLUMNS)
            .item(
                agg_item(name, Some("AMOUNT"), false),
                ExasolType::WIDEST_VARCHAR,
            )
            .build_pushdown();
        assert!(
            detect_aggregates(&req, &ColumnTypes::default()).is_none(),
            "{name} must fall back to row scan"
        );
    }
    let req_distinct = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("AMOUNT"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    assert!(
        detect_aggregates(&req_distinct, &ColumnTypes::default()).is_none(),
        "SUM(DISTINCT) must fall back to row scan"
    );
}

#[test]
fn parse_agg_item_recognises_stat_functions() {
    for (name, expected_kind) in &[
        ("STDDEV", AggKind::Stddev),
        ("STDDEV_SAMP", AggKind::StddevSamp),
        ("STDDEV_POP", AggKind::StddevPop),
        ("VARIANCE", AggKind::Variance),
        ("VAR_SAMP", AggKind::VarSamp),
        ("VAR_POP", AggKind::VarPop),
    ] {
        let item = agg_item(name, Some("AMOUNT"), false);
        let plan = parse_agg_item(&item, &ColumnTypes::default())
            .unwrap_or_else(|| panic!("{name} must parse to a stat plan"));
        assert_eq!(
            plan.kind, *expected_kind,
            "{name} must map to {:?}",
            expected_kind
        );
        assert_eq!(plan.column.as_deref(), Some("AMOUNT"));
    }
}

#[test]
fn stat_aggregate_over_expression_argument_declines() {
    for arg in [length_expr("SCORE"), mult_expr("SCORE", "ID")] {
        let item = agg_item_expr("STDDEV", arg.clone(), false);
        assert!(
            parse_agg_item(&item, &ColumnTypes::default()).is_none(),
            "STDDEV over a non-column argument must decline: {arg}"
        );
        let req = pushdown_request("T", NO_COLUMNS)
            .item(item, ExasolType::WIDEST_VARCHAR)
            .build_pushdown();
        assert!(
            detect_aggregates(&req, &ColumnTypes::default()).is_none(),
            "one declining stat aggregate must decline the whole select list: {arg}"
        );
    }
}

#[test]
fn stat_aggregate_over_bare_column_still_parses() {
    let plan = parse_agg_item(
        &agg_item("STDDEV", Some("SCORE"), false),
        &ColumnTypes::default(),
    )
    .expect("STDDEV over a bare column must still parse");
    assert_eq!(plan.kind, AggKind::Stddev);
    assert_eq!(plan.column.as_deref(), Some("SCORE"));
    assert_eq!(plan.arg_expr, None);
}

#[test]
fn single_group_plan_types_returns_empty_vec_for_no_items() {
    assert_eq!(
        single_group_plan_types(&[], &[], &ColumnTypes::default()),
        Some(Vec::<ExasolType>::new())
    );
}

#[test]
fn single_group_plan_types_aligns_with_bare_aggregate_select_list() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .count_star()
        .item(
            agg_item("MIN", Some("TS"), false),
            ExasolType::decimal(9, 4),
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("a bare aggregate list must decompose");
    assert_eq!(
        single_group_plan_types(req.select_types(), &items, &ColumnTypes::default()),
        Some(vec![
            ExasolType::decimal(36, 2),
            ExasolType::decimal(18, 0),
            ExasolType::decimal(9, 4),
        ])
    );
}

#[test]
fn single_group_plan_types_types_a_slot_reached_only_through_scalar_wrapper() {
    let col_types = column_types(&[("L_QUANTITY", ExasolType::decimal(15, 2))]);
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            round_of(
                float_div(agg_item("SUM", Some("L_QUANTITY"), false), count_star()),
                2,
            ),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &col_types).expect("ROUND(SUM/COUNT) must decompose");
    assert_eq!(
        single_group_plan_types(req.select_types(), &items, &col_types),
        Some(vec![ExasolType::decimal(26, 2), ExasolType::decimal(18, 0)]),
        "neither nested aggregate has a top-level select-list entry of its own, so \
         both slots take Exasol's result type of the aggregate"
    );
}

#[test]
fn single_group_plan_types_prefers_top_level_declared_type_for_shared_slot() {
    let col_types = column_types(&[("L_QUANTITY", ExasolType::decimal(15, 2))]);
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            round_of(
                float_div(agg_item("SUM", Some("L_QUANTITY"), false), count_star()),
                2,
            ),
            ExasolType::decimal(9, 4),
        )
        .item(
            agg_item("SUM", Some("L_QUANTITY"), false),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &col_types).expect("ROUND(SUM/COUNT) + SUM must decompose");
    assert_eq!(
        single_group_plan_types(req.select_types(), &items, &col_types),
        Some(vec![ExasolType::decimal(36, 2), ExasolType::decimal(18, 0)]),
        "slot 0 (SUM) takes its top-level declared type, not the nested DECIMAL(26,2); \
         slot 1 (COUNT) has no top-level occurrence and takes its nested type"
    );
}

#[test]
fn single_group_plan_types_resolves_both_ends_of_an_interleaved_list() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("L_QUANTITY"), false),
            ExasolType::decimal(36, 2),
        )
        .item(
            round_of(
                float_div(agg_item("SUM", Some("L_QUANTITY"), false), count_star()),
                2,
            ),
            ExasolType::decimal(9, 4),
        )
        .count_star()
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("an interleaved list must decompose");
    assert_eq!(
        single_group_plan_types(req.select_types(), &items, &ColumnTypes::default()),
        Some(vec![ExasolType::decimal(36, 2), ExasolType::decimal(18, 0)]),
        "slot 0 (SUM) takes ordinal 0's type; slot 1 (COUNT) takes ordinal 2's type, \
         never ordinal 1's scalar-item type"
    );
}

#[test]
fn single_group_plan_types_skips_distinct_items() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .item(
            agg_item("COUNT", Some("L_SHIPMODE"), true),
            ExasolType::decimal(18, 0),
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("SUM + COUNT(DISTINCT) must decompose");
    assert_eq!(
        single_group_plan_types(req.select_types(), &items, &ColumnTypes::default()),
        Some(vec![ExasolType::decimal(36, 2)]),
        "the COUNT(DISTINCT) item contributes no ordinary-aggregate slot"
    );
}

#[test]
fn single_group_plan_types_declines_a_nested_only_aggregate_over_an_untyped_expression() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            round_of(agg_item_expr("MIN", mult_expr("A", "B"), false), 2),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("ROUND(MIN(A * B), 2) must decompose");
    assert_eq!(
        single_group_plan_types(req.select_types(), &items, &ColumnTypes::default()),
        None,
        "a guessed partial type would break the merge, so an untyped MIN(A * B) declines (#476)"
    );
}

#[test]
fn merge_select_wraps_scalar_structure_around_the_merged_partial() {
    let col_types = column_types(&[("L_QUANTITY", ExasolType::decimal(15, 2))]);
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            round_of(agg_item("SUM", Some("L_QUANTITY"), false), 2),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &col_types).expect("ROUND(SUM(col), 2) must decompose");
    let plans = ordinary_plans(&items, &col_types);
    let plan_types = single_group_plan_types(req.select_types(), &items, &col_types)
        .expect("SUM over a typed column is exactly typed");

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types, &col_types),
        Some(vec![
            r#"CAST(ROUND(CAST(SUM("PARTIAL_sum_0") AS DECIMAL(26,2)), 2) AS DECIMAL(36,2))"#
                .to_string()
        ]),
        "the merged SUM is cast to Exasol's SUM type before ROUND applies (#476)"
    );
}

#[test]
fn merge_select_interleaves_items_in_selectlist_order_with_per_item_casts() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("L_QUANTITY"), false),
            ExasolType::decimal(36, 2),
        )
        .item(
            round_of(
                float_div(agg_item("SUM", Some("L_QUANTITY"), false), count_star()),
                2,
            ),
            ExasolType::decimal(9, 4),
        )
        .count_star()
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("an interleaved list must decompose");
    let plans = ordinary_plans(&items, &ColumnTypes::default());
    let plan_types = single_group_plan_types(req.select_types(), &items, &ColumnTypes::default())
        .expect("every slot is exactly typed");

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types, &ColumnTypes::default()),
        Some(vec![
            r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,2))"#.to_string(),
            r#"CAST(ROUND((CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,2)) / CAST(SUM("PARTIAL_count_1") AS DECIMAL(18,0))), 2) AS DECIMAL(9,4))"#
                .to_string(),
            r#"CAST(SUM("PARTIAL_count_1") AS DECIMAL(18,0))"#.to_string(),
        ])
    );
}

#[test]
fn merge_select_declines_a_list_holding_a_distinct_item() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .item(
            agg_item("COUNT", Some("L_SHIPMODE"), true),
            ExasolType::decimal(18, 0),
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("SUM + COUNT(DISTINCT) must decompose");
    let plans = ordinary_plans(&items, &ColumnTypes::default());
    let plan_types = single_group_plan_types(req.select_types(), &items, &ColumnTypes::default())
        .expect("every slot is exactly typed");

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types, &ColumnTypes::default()),
        None,
        "a distinct item carries no merge expression, so the whole assembly declines"
    );
}

#[test]
fn merge_select_declines_when_the_scalar_structure_fails_to_render() {
    let node = serde_json::json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [count_star()]
    });
    let items = vec![SingleGroupItem::ScalarOverAggregate {
        node,
        declared_type: ExasolType::decimal(18, 0),
    }];
    let plans = ordinary_plans(&items, &ColumnTypes::default());
    let plan_types = single_group_plan_types(&[], &items, &ColumnTypes::default())
        .expect("an unclassifiable scalar contributes no slot to type");

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types, &ColumnTypes::default()),
        None,
        "an unrenderable scalar structure carries no merge expression, so the \
         whole assembly declines"
    );
}

#[test]
fn merge_select_emits_one_item_per_selectlist_item_for_duplicate_aggregates() {
    let req = pushdown_request("T", NO_COLUMNS)
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .build_pushdown();
    let items = detect_aggregates(&req, &ColumnTypes::default())
        .expect("a duplicated aggregate must decompose");
    let plans = ordinary_plans(&items, &ColumnTypes::default());
    let plan_types = single_group_plan_types(req.select_types(), &items, &ColumnTypes::default())
        .expect("every slot is exactly typed");
    assert_eq!(plans.len(), 1, "one partial column for both occurrences");

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types, &ColumnTypes::default()),
        Some(vec![
            r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,2))"#.to_string(),
            r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,2))"#.to_string(),
        ])
    );
}

use super::super::grouped_agg::partial_emits_items;
use super::super::scalar_over_agg::cast_merge_items;
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

fn length_expr(col: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "LENGTH",
        "arguments": [{"type": "column", "name": col}],
    })
}

/// Exasol's `MULT` node, pushed once `FN_MULT` is advertised (decision-log [7]).
fn mult_expr(a: &str, b: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "MULT",
        "arguments": [
            {"type": "column", "name": a},
            {"type": "column", "name": b},
        ],
    })
}

fn round_expr(inner: serde_json::Value, digits: i64) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "ROUND",
        "arguments": [inner, {"type": "literal_exactnumeric", "value": digits}],
    })
}

fn float_div(a: serde_json::Value, b: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [a, b],
    })
}

fn decimal_type(precision: u32, scale: u32) -> serde_json::Value {
    serde_json::json!({"type": "decimal", "precision": precision, "scale": scale})
}

#[test]
fn detect_aggregates_accepts_scalar_over_aggregate_and_still_declines_undecomposable() {
    let req = serde_json::json!({
        "selectList": [round_expr(agg_item("SUM", Some("L_QUANTITY"), false), 2)],
        "selectListDataTypes": [decimal_type(36, 2)],
    });
    let items = detect_aggregates(&req).expect("ROUND(SUM(col), 2) must decompose");
    assert_eq!(items.len(), 1, "one select item, one resolved item");
    assert!(
        !has_distinct(&items),
        "a scalar-over-aggregate is not a distinct fan-out"
    );

    let variance_req = serde_json::json!({
        "selectList": [round_expr(agg_item("VARIANCE", Some("C_ACCTBAL"), false), 4)],
        "selectListDataTypes": [decimal_type(36, 4)],
    });
    assert!(
        detect_aggregates(&variance_req).is_some(),
        "ROUND(VARIANCE(col), 4) must decompose rather than reach DataFusion by name"
    );

    let interleaved = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("L_QUANTITY"), false),
            round_expr(
                float_div(
                    agg_item("SUM", Some("L_QUANTITY"), false),
                    agg_item("COUNT", None, false),
                ),
                2,
            ),
        ],
        "selectListDataTypes": [decimal_type(36, 2), decimal_type(36, 4)],
    });
    assert_eq!(
        detect_aggregates(&interleaved)
            .expect("an interleaved list must decompose")
            .len(),
        2
    );

    let distinct_req = serde_json::json!({
        "selectList": [round_expr(agg_item("COUNT", Some("L_ORDERKEY"), true), 2)],
        "selectListDataTypes": [decimal_type(18, 0)],
    });
    assert!(
        detect_aggregates(&distinct_req).is_none(),
        "ROUND(COUNT(DISTINCT col), 2) must decline the whole detection"
    );

    let median_req = serde_json::json!({
        "selectList": [round_expr(agg_item("MEDIAN", Some("L_QUANTITY"), false), 2)],
        "selectListDataTypes": [decimal_type(36, 2)],
    });
    assert!(
        detect_aggregates(&median_req).is_none(),
        "ROUND(MEDIAN(col), 2) must decline the whole detection"
    );

    let residual_req = serde_json::json!({
        "selectList": [serde_json::json!({
            "type": "function_scalar",
            "name": "MULT",
            "arguments": [
                agg_item("SUM", Some("L_QUANTITY"), false),
                {"type": "column", "name": "L_QUANTITY"},
            ],
        })],
        "selectListDataTypes": [decimal_type(36, 2)],
    });
    assert!(
        detect_aggregates(&residual_req).is_none(),
        "a residual bare column must decline the whole detection"
    );

    let scalar_only = serde_json::json!({
        "selectList": [length_expr("L_COMMENT")],
        "selectListDataTypes": [decimal_type(18, 0)],
    });
    assert!(
        detect_aggregates(&scalar_only).is_none(),
        "a scalar item with no nested aggregate must still decline"
    );
}

#[test]
fn single_group_scalar_over_aggregate_preserves_selectlist_order_and_item_types() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("L_QUANTITY"), false),
            round_expr(
                float_div(
                    agg_item("SUM", Some("L_QUANTITY"), false),
                    agg_item("COUNT", None, false),
                ),
                2,
            ),
            agg_item("COUNT", None, false),
        ],
        "selectListDataTypes": [decimal_type(36, 2), decimal_type(9, 4), decimal_type(18, 0)],
    });
    let items = detect_aggregates(&req).expect("an interleaved list must decompose");
    assert_eq!(items.len(), 3);
    assert_eq!(agg_of(&items[0]).kind, AggKind::Sum);
    assert!(
        matches!(
            &items[1],
            SingleGroupItem::ScalarOverAggregate {
                declared_type,
                node,
            } if declared_type == "DECIMAL(9,4)" && node == &req["selectList"][1]
        ),
        "item 1 must be a ScalarOverAggregate at ordinal 1 carrying its own \
         declared type and the verbatim node: {:?}",
        items[1]
    );
    assert_eq!(agg_of(&items[2]).kind, AggKind::Count);

    assert_eq!(ordinary_plans(&items).len(), 2);
}

#[test]
fn single_group_scalar_over_aggregate_defaults_declared_type_when_absent() {
    let req = serde_json::json!({
        "selectList": [round_expr(agg_item("SUM", Some("L_QUANTITY"), false), 2)],
    });
    let items = detect_aggregates(&req).expect("must decompose without declared types");
    assert!(matches!(
        &items[0],
        SingleGroupItem::ScalarOverAggregate { declared_type, .. }
            if declared_type == "VARCHAR(2000000)"
    ));
}

#[test]
fn single_group_scalar_over_aggregate_dedups_shared_inner_aggregates() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("COUNT", None, false),
            round_expr(
                float_div(
                    agg_item("SUM", Some("L_QUANTITY"), false),
                    agg_item("COUNT", None, false),
                ),
                2,
            ),
        ],
        "selectListDataTypes": [decimal_type(18, 0), decimal_type(36, 2)],
    });
    let items = detect_aggregates(&req).expect("COUNT(*) + ROUND(SUM/COUNT) must decompose");
    let plans = ordinary_plans(&items);
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
    let req = serde_json::json!({
        "selectList": [round_expr(
            float_div(
                agg_item("SUM", Some("L_QUANTITY"), false),
                agg_item("COUNT", None, false),
            ),
            2,
        )],
        "selectListDataTypes": [decimal_type(36, 2)],
    });
    let items = detect_aggregates(&req).expect("ROUND(SUM/COUNT) must decompose");
    let plans = ordinary_plans(&items);
    assert_eq!(plans.len(), 2, "{plans:?}");
    assert_eq!(plans[0].kind, AggKind::Sum);
    assert_eq!(plans[1].kind, AggKind::Count);
}

#[test]
fn ordinary_plans_unchanged_for_bare_aggregate_select_lists() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("AMOUNT"), false),
            agg_item("COUNT", None, false),
            agg_item("MIN", Some("TS"), false),
        ]
    });
    let items = detect_aggregates(&req).expect("a bare aggregate list must decompose");
    let plans = ordinary_plans(&items);
    assert_eq!(plans.len(), 3);
    assert_eq!(plans[0].kind, AggKind::Sum);
    assert_eq!(plans[1].kind, AggKind::Count);
    assert_eq!(plans[2].kind, AggKind::Min);
}

#[test]
fn detect_count_star_produces_count_no_column() {
    let req = serde_json::json!({
        "selectList": [agg_item("COUNT", None, false)]
    });
    let plans = detect_aggregates(&req).expect("should detect COUNT(*)");
    assert_eq!(plans.len(), 1);
    assert_eq!(agg_of(&plans[0]).kind, AggKind::Count);
    assert!(agg_of(&plans[0]).column.is_none());
}

#[test]
fn detect_count_col_produces_count_col() {
    let req = serde_json::json!({
        "selectList": [agg_item("COUNT", Some("amount"), false)]
    });
    let plans = detect_aggregates(&req).expect("should detect COUNT(col)");
    assert_eq!(agg_of(&plans[0]).kind, AggKind::CountCol);
    assert_eq!(agg_of(&plans[0]).column.as_deref(), Some("AMOUNT"));
}

#[test]
fn detect_sum_min_max_avg_produce_correct_plans() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("amount"), false),
            agg_item("MIN", Some("ts"), false),
            agg_item("MAX", Some("ts"), false),
            agg_item("AVG", Some("score"), false),
        ]
    });
    let plans = detect_aggregates(&req).expect("should detect all four");
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
    let req = serde_json::json!({
        "selectList": [agg_item("SUM", Some("amount"), false)],
        "groupBy": [{"type": "column", "name": "region"}],
    });
    assert!(
        detect_aggregates(&req).is_none(),
        "must fall back when GROUP BY is present"
    );
}

#[test]
fn detect_aggregates_falls_back_on_distinct() {
    let req = serde_json::json!({
        "selectList": [agg_item("SUM", Some("amount"), true)]
    });
    assert!(
        detect_aggregates(&req).is_none(),
        "must fall back when a non-COUNT DISTINCT is present"
    );
}

#[test]
fn detect_aggregates_falls_back_on_unsupported_function() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("amount"), false),
            agg_item("MEDIAN", Some("amount"), false),
        ]
    });
    assert!(
        detect_aggregates(&req).is_none(),
        "must fall back when any item is unsupported"
    );
}

#[test]
fn detect_aggregates_falls_back_on_column_select() {
    let req = serde_json::json!({
        "selectList": [
            {"type": "column", "name": "region"},
        ]
    });
    assert!(
        detect_aggregates(&req).is_none(),
        "must fall back when select list contains non-aggregate"
    );
}

#[test]
fn detect_aggregates_returns_none_for_empty_select_list() {
    let req = serde_json::json!({ "selectList": [] });
    assert!(detect_aggregates(&req).is_none());
}

#[test]
fn bare_column_aggregates_unchanged_regression() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("COUNT", None, false),
            agg_item("COUNT", Some("id"), false),
            agg_item("SUM", Some("amount"), false),
            agg_item("MIN", Some("ts"), false),
            agg_item("MAX", Some("ts"), false),
            agg_item("AVG", Some("score"), false),
            agg_item("STDDEV", Some("score"), false),
        ]
    });
    let plans = detect_aggregates(&req).expect("bare-column aggregates must decompose");
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
    assert_eq!(agg_of(&plans[6]).kind, AggKind::StddevSamp);

    let col_types = vec![
        ("AMOUNT".to_string(), "DECIMAL(20,0)".to_string()),
        ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
        ("TS".to_string(), "TIMESTAMP".to_string()),
    ];
    let sum_only = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("AMOUNT".into()),
        arg_expr: None,
    }];
    let emits = partial_emits_items(&sum_only, &col_types, &["VARCHAR(2000000)".to_string()]);
    assert_eq!(emits, vec![r#""PARTIAL_sum_0" DECIMAL(36,0)"#.to_string()]);
}

#[test]
fn expression_arg_partial_and_merge_types_from_declared_type() {
    let req = serde_json::json!({
        "selectList": [agg_item_expr("SUM", length_expr("L_COMMENT"), false)]
    });
    let plans = detect_aggregates(&req).expect("expression-argument SUM must decompose");
    assert_eq!(agg_of(&plans[0]).kind, AggKind::Sum);
    assert!(
        agg_of(&plans[0]).column.is_none(),
        "expression argument must not populate column"
    );
    assert_eq!(
        agg_of(&plans[0]).arg_expr.as_deref(),
        Some(r#"character_length(exa_to_varchar("L_COMMENT"))"#),
        "the rendered DataFusion fragment must be carried in arg_expr"
    );

    // Deliberately no matching entry in col_types: the type must come from the declared type.
    let col_types: Vec<(String, String)> = vec![];

    let sum_expr = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: None,
        arg_expr: Some(r#"character_length("L_COMMENT")"#.into()),
    }];
    let emits = partial_emits_items(&sum_expr, &col_types, &["DECIMAL(38,4)".to_string()]);
    assert_eq!(emits, vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()]);
    let emits = partial_emits_items(&sum_expr, &col_types, &["DOUBLE PRECISION".to_string()]);
    assert_eq!(
        emits,
        vec![r#""PARTIAL_sum_0" DOUBLE PRECISION"#.to_string()]
    );

    let min_expr = vec![AggregatePlan {
        kind: AggKind::Min,
        column: None,
        arg_expr: Some(r#"("A" + "B")"#.into()),
    }];
    let emits = partial_emits_items(&min_expr, &col_types, &["DATE".to_string()]);
    assert_eq!(emits, vec![r#""PARTIAL_min_0" DATE"#.to_string()]);

    let count_expr = vec![AggregatePlan {
        kind: AggKind::CountCol,
        column: None,
        arg_expr: Some(r#"character_length("L_COMMENT")"#.into()),
    }];
    let emits = partial_emits_items(&count_expr, &col_types, &["DECIMAL(18,0)".to_string()]);
    assert_eq!(
        emits,
        vec![r#""PARTIAL_count_0" DECIMAL(20,0)"#.to_string()]
    );

    assert!(
        validate_agg_col_types(&sum_expr, &col_types),
        "expression-argument SUM must pass validation, not force a row scan"
    );
}

#[test]
fn decimal_product_sum_partial_widens_to_decimal_36() {
    let req = serde_json::json!({
        "selectList": [
            agg_item_expr("SUM", mult_expr("L_EXTENDEDPRICE", "L_DISCOUNT"), false)
        ]
    });
    let items =
        detect_aggregates(&req).expect("SUM(col * col) must decompose, not fall back to scan");
    assert_eq!(items.len(), 1);
    let plans = ordinary_plans(&items);
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
    let col_types: Vec<(String, String)> = vec![];
    let declared = ["DECIMAL(36,4)".to_string()];

    let emits = partial_emits_items(&plans, &col_types, &declared);
    assert_eq!(
        emits,
        vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()],
        "partial SUM column must widen to the declared DECIMAL(36,4)"
    );

    let merge = cast_merge_items(&plans, &declared);
    assert_eq!(
        merge,
        vec![r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,4))"#.to_string()],
        "merge must cast the summed partial to the declared DECIMAL(36,4)"
    );

    assert!(validate_agg_col_types(&plans, &col_types));
}

#[test]
fn unrenderable_agg_arg_falls_back_to_row_scan() {
    let unknown = serde_json::json!({
        "type": "function_scalar",
        "name": "TOTALLY_UNKNOWN_FN",
        "arguments": [{"type": "column", "name": "id"}],
    });
    for name in &["SUM", "MIN", "MAX", "AVG", "COUNT"] {
        let req = serde_json::json!({
            "selectList": [agg_item_expr(name, unknown.clone(), false)]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "{name} over an unrenderable argument must fall back to row scan"
        );
    }
    let req = serde_json::json!({
        "selectList": [agg_item_expr("COUNT", unknown.clone(), true)]
    });
    assert!(
        detect_aggregates(&req).is_none(),
        "COUNT(DISTINCT unrenderable) must fall back to row scan"
    );
}

#[test]
fn count_distinct_builds_distinct_row_scan_spec() {
    let req = serde_json::json!({
        "selectList": [agg_item("COUNT", Some("L_SHIPMODE"), true)]
    });
    let items = detect_aggregates(&req).expect("single-group COUNT(DISTINCT) must decompose");
    assert_eq!(items.len(), 1);
    assert!(has_distinct(&items), "the item must be a distinct fan-out");
    let dc = distinct_of(&items[0]);
    assert_eq!(dc.column.as_deref(), Some("L_SHIPMODE"));
    assert!(dc.arg_expr.is_none());
    assert!(
        ordinary_plans(&items).is_empty(),
        "a COUNT(DISTINCT) item must not appear among the ordinary aggregate plans"
    );

    let req_expr = serde_json::json!({
        "selectList": [agg_item_expr("COUNT", length_expr("L_COMMENT"), true)]
    });
    let items_expr = detect_aggregates(&req_expr).expect("COUNT(DISTINCT expr) must decompose");
    let dc_expr = distinct_of(&items_expr[0]);
    assert!(dc_expr.column.is_none());
    assert_eq!(
        dc_expr.arg_expr.as_deref(),
        Some(r#"character_length(exa_to_varchar("L_COMMENT"))"#)
    );
}

#[test]
fn multi_count_distinct_declines_to_qualified_wrapper() {
    let lone = serde_json::json!({
        "selectList": [agg_item("COUNT", Some("CATEGORY"), true)],
    });
    let items = detect_aggregates(&lone).expect("a lone COUNT(DISTINCT) decomposes");
    assert!(
        has_distinct(&items) && is_lone_count_distinct(&items),
        "a lone COUNT(DISTINCT) is the only shape that fans out (Case 1)"
    );

    let multi = serde_json::json!({
        "selectList": [
            agg_item("COUNT", Some("CATEGORY"), true),
            agg_item("COUNT", Some("REGION"), true),
        ],
    });
    let items = detect_aggregates(&multi).expect("multiple distinct items still detect");
    assert!(
        has_distinct(&items) && !is_lone_count_distinct(&items),
        "more than one COUNT(DISTINCT) must decline the fan-out (Case 2)"
    );

    let mixed = serde_json::json!({
        "selectList": [
            agg_item("COUNT", Some("CATEGORY"), true),
            agg_item("SUM", Some("AMOUNT"), false),
        ],
    });
    let items = detect_aggregates(&mixed).expect("distinct-plus-ordinary still detects");
    assert!(
        has_distinct(&items) && !is_lone_count_distinct(&items),
        "a distinct mixed with an ordinary aggregate must decline the fan-out (Case 3)"
    );
}

#[test]
fn lone_expression_count_distinct_declines_fan_out_to_wrapper() {
    let expr = serde_json::json!({
        "selectList": [agg_item_expr("COUNT", length_expr("NAME"), true)],
    });
    let items = detect_aggregates(&expr)
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

    let bare = serde_json::json!({
        "selectList": [agg_item("COUNT", Some("NAME"), true)],
    });
    let bare_items =
        detect_aggregates(&bare).expect("a lone bare-column COUNT(DISTINCT) decomposes");
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
        let req = serde_json::json!({
            "selectList": [agg_item(name, Some("AMOUNT"), false)],
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "{name} must fall back to row scan"
        );
    }
    let req_distinct = serde_json::json!({
        "selectList": [agg_item("SUM", Some("AMOUNT"), true)],
    });
    assert!(
        detect_aggregates(&req_distinct).is_none(),
        "SUM(DISTINCT) must fall back to row scan"
    );
}

#[test]
fn parse_agg_item_recognises_stat_functions() {
    for (name, expected_kind) in &[
        ("STDDEV", AggKind::StddevSamp),
        ("STDDEV_SAMP", AggKind::StddevSamp),
        ("STDDEV_POP", AggKind::StddevPop),
        ("VARIANCE", AggKind::VarSamp),
        ("VAR_SAMP", AggKind::VarSamp),
        ("VAR_POP", AggKind::VarPop),
    ] {
        let item = agg_item(name, Some("AMOUNT"), false);
        let plan =
            parse_agg_item(&item).unwrap_or_else(|| panic!("{name} must parse to a stat plan"));
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
            parse_agg_item(&item).is_none(),
            "STDDEV over a non-column argument must decline: {arg}"
        );
        let req = serde_json::json!({"selectList": [item]});
        assert!(
            detect_aggregates(&req).is_none(),
            "one declining stat aggregate must decline the whole select list: {arg}"
        );
    }
}

#[test]
fn stat_aggregate_over_bare_column_still_parses() {
    let plan = parse_agg_item(&agg_item("STDDEV", Some("SCORE"), false))
        .expect("STDDEV over a bare column must still parse");
    assert_eq!(plan.kind, AggKind::StddevSamp);
    assert_eq!(plan.column.as_deref(), Some("SCORE"));
    assert_eq!(plan.arg_expr, None);
}

#[test]
fn single_group_plan_types_returns_empty_vec_for_no_items() {
    let req = serde_json::json!({});
    assert_eq!(
        single_group_plan_types(&req, &[], &[]),
        Vec::<String>::new()
    );
}

#[test]
fn single_group_plan_types_aligns_with_bare_aggregate_select_list() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("AMOUNT"), false),
            agg_item("COUNT", None, false),
            agg_item("MIN", Some("TS"), false),
        ],
        "selectListDataTypes": [decimal_type(36, 2), decimal_type(18, 0), decimal_type(9, 4)],
    });
    let items = detect_aggregates(&req).expect("a bare aggregate list must decompose");
    assert_eq!(
        single_group_plan_types(&req, &items, &[]),
        vec![
            "DECIMAL(36,2)".to_string(),
            "DECIMAL(18,0)".to_string(),
            "DECIMAL(9,4)".to_string(),
        ]
    );
}

#[test]
fn single_group_plan_types_defaults_when_reached_only_through_scalar_wrapper() {
    let req = serde_json::json!({
        "selectList": [round_expr(
            float_div(
                agg_item("SUM", Some("L_QUANTITY"), false),
                agg_item("COUNT", None, false),
            ),
            2,
        )],
        "selectListDataTypes": [decimal_type(36, 2)],
    });
    let items = detect_aggregates(&req).expect("ROUND(SUM/COUNT) must decompose");
    assert_eq!(
        single_group_plan_types(&req, &items, &[]),
        vec![
            "DOUBLE PRECISION".to_string(),
            "DOUBLE PRECISION".to_string()
        ],
        "neither nested aggregate has a top-level select-list entry of its own, so \
         both slots take the nested-only numeric default"
    );
}

#[test]
fn single_group_plan_types_prefers_top_level_declared_type_for_shared_slot() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("COUNT", None, false),
            round_expr(
                float_div(
                    agg_item("SUM", Some("L_QUANTITY"), false),
                    agg_item("COUNT", None, false),
                ),
                2,
            ),
        ],
        "selectListDataTypes": [decimal_type(18, 0), decimal_type(36, 2)],
    });
    let items = detect_aggregates(&req).expect("COUNT(*) + ROUND(SUM/COUNT) must decompose");
    assert_eq!(
        single_group_plan_types(&req, &items, &[]),
        vec!["DECIMAL(18,0)".to_string(), "DOUBLE PRECISION".to_string()],
        "slot 0 (COUNT) takes its bare declared type; slot 1 (SUM) has no top-level \
         occurrence and keeps the nested-only numeric default"
    );
}

#[test]
fn single_group_plan_types_resolves_both_ends_of_an_interleaved_list() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("L_QUANTITY"), false),
            round_expr(
                float_div(
                    agg_item("SUM", Some("L_QUANTITY"), false),
                    agg_item("COUNT", None, false),
                ),
                2,
            ),
            agg_item("COUNT", None, false),
        ],
        "selectListDataTypes": [decimal_type(36, 2), decimal_type(9, 4), decimal_type(18, 0)],
    });
    let items = detect_aggregates(&req).expect("an interleaved list must decompose");
    assert_eq!(
        single_group_plan_types(&req, &items, &[]),
        vec!["DECIMAL(36,2)".to_string(), "DECIMAL(18,0)".to_string()],
        "slot 0 (SUM) takes ordinal 0's type; slot 1 (COUNT) takes ordinal 2's type, \
         never ordinal 1's scalar-item type"
    );
}

#[test]
fn single_group_plan_types_skips_distinct_items() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("AMOUNT"), false),
            agg_item("COUNT", Some("L_SHIPMODE"), true),
        ],
        "selectListDataTypes": [decimal_type(36, 2), decimal_type(18, 0)],
    });
    let items = detect_aggregates(&req).expect("SUM + COUNT(DISTINCT) must decompose");
    assert_eq!(
        single_group_plan_types(&req, &items, &[]),
        vec!["DECIMAL(36,2)".to_string()],
        "the COUNT(DISTINCT) item contributes no ordinary-aggregate slot"
    );
}

#[test]
fn nested_only_expression_argument_min_emits_a_numeric_partial_column() {
    let req = serde_json::json!({
        "selectList": [round_expr(
            serde_json::json!({
                "type": "function_aggregate",
                "name": "MIN",
                "arguments": [mult_expr("A", "B")],
            }),
            2,
        )],
        "selectListDataTypes": [decimal_type(36, 2)],
    });
    let items = detect_aggregates(&req).expect("ROUND(MIN(A * B), 2) must decompose");
    let plans = ordinary_plans(&items);
    let plan_types = single_group_plan_types(&req, &items, &[]);
    assert_eq!(
        partial_emits_items(&plans, &[], &plan_types),
        vec![r#""PARTIAL_min_0" DOUBLE PRECISION"#.to_string()],
        "a nested-only MIN over an expression must emit a numeric partial column"
    );
}

#[test]
fn merge_select_wraps_scalar_structure_around_the_merged_partial() {
    let req = serde_json::json!({
        "selectList": [round_expr(agg_item("SUM", Some("L_QUANTITY"), false), 2)],
        "selectListDataTypes": [decimal_type(36, 2)],
    });
    let items = detect_aggregates(&req).expect("ROUND(SUM(col), 2) must decompose");
    let plans = ordinary_plans(&items);
    let plan_types = single_group_plan_types(&req, &items, &[]);

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        Some(vec![
            r#"CAST(ROUND(SUM("PARTIAL_sum_0"), 2) AS DECIMAL(36,2))"#.to_string()
        ])
    );
}

#[test]
fn merge_select_interleaves_items_in_selectlist_order_with_per_item_casts() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("L_QUANTITY"), false),
            round_expr(
                float_div(
                    agg_item("SUM", Some("L_QUANTITY"), false),
                    agg_item("COUNT", None, false),
                ),
                2,
            ),
            agg_item("COUNT", None, false),
        ],
        "selectListDataTypes": [decimal_type(36, 2), decimal_type(9, 4), decimal_type(18, 0)],
    });
    let items = detect_aggregates(&req).expect("an interleaved list must decompose");
    let plans = ordinary_plans(&items);
    let plan_types = single_group_plan_types(&req, &items, &[]);

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        Some(vec![
            r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,2))"#.to_string(),
            r#"CAST(ROUND((SUM("PARTIAL_sum_0") / SUM("PARTIAL_count_1")), 2) AS DECIMAL(9,4))"#
                .to_string(),
            r#"CAST(SUM("PARTIAL_count_1") AS DECIMAL(18,0))"#.to_string(),
        ])
    );
}

#[test]
fn merge_select_leaves_items_uncast_without_a_declared_type() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("COUNT", None, false),
            round_expr(agg_item("SUM", Some("L_QUANTITY"), false), 2),
        ],
    });
    let items = detect_aggregates(&req).expect("must decompose without declared types");
    let plans = ordinary_plans(&items);
    let plan_types = single_group_plan_types(&req, &items, &[]);

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        Some(vec![
            r#"SUM("PARTIAL_count_0")"#.to_string(),
            r#"ROUND(SUM("PARTIAL_sum_1"), 2)"#.to_string(),
        ])
    );
}

#[test]
fn merge_select_declines_a_list_holding_a_distinct_item() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("AMOUNT"), false),
            agg_item("COUNT", Some("L_SHIPMODE"), true),
        ],
        "selectListDataTypes": [decimal_type(36, 2), decimal_type(18, 0)],
    });
    let items = detect_aggregates(&req).expect("SUM + COUNT(DISTINCT) must decompose");
    let plans = ordinary_plans(&items);
    let plan_types = single_group_plan_types(&req, &items, &[]);

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        None,
        "a distinct item carries no merge expression, so the whole assembly declines"
    );
}

#[test]
fn merge_select_declines_when_the_scalar_structure_fails_to_render() {
    let node = serde_json::json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [agg_item("COUNT", None, false)],
        "dataType": {"type": "UNSUPPORTED_TARGET"}
    });
    let items = vec![SingleGroupItem::ScalarOverAggregate {
        node,
        declared_type: "DECIMAL(18,0)".to_string(),
    }];
    let plans = ordinary_plans(&items);
    let plan_types = single_group_plan_types(&serde_json::json!({}), &items, &[]);

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        None,
        "an unrenderable scalar structure carries no merge expression, so the \
         whole assembly declines"
    );
}

#[test]
fn merge_select_emits_one_item_per_selectlist_item_for_duplicate_aggregates() {
    let req = serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("AMOUNT"), false),
            agg_item("SUM", Some("AMOUNT"), false),
        ],
        "selectListDataTypes": [decimal_type(36, 2), decimal_type(36, 2)],
    });
    let items = detect_aggregates(&req).expect("a duplicated aggregate must decompose");
    let plans = ordinary_plans(&items);
    let plan_types = single_group_plan_types(&req, &items, &[]);
    assert_eq!(plans.len(), 1, "one partial column for both occurrences");

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        Some(vec![
            r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,2))"#.to_string(),
            r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,2))"#.to_string(),
        ])
    );
}

fn single_group_over_customer(select_list: serde_json::Value) -> serde_json::Value {
    let declared: Vec<serde_json::Value> = select_list
        .as_array()
        .expect("select list must be an array")
        .iter()
        .map(|_| serde_json::json!({"type": "DECIMAL", "precision": 18, "scale": 0}))
        .collect();
    customer_request(serde_json::json!({
        "aggregationType": "single_group",
        "selectList": select_list,
        "selectListDataTypes": declared,
    }))
}

fn length_of(node: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"type": "function_scalar", "name": "LENGTH", "arguments": [node]})
}

fn is_single_group_shape(request: &serde_json::Value) -> bool {
    matches!(
        super::super::request_shape::classify_request_shape(
            &pd(request),
            &super::super::support::extract_all_column_types(request),
        ),
        super::super::request_shape::RequestShape::SingleGroupAgg { .. }
    )
}

/// Scenario: One node renders one text on every DataFusion surface
#[test]
fn aggregate_over_string_fn_of_integer_decomposes() {
    let request = single_group_over_customer(serde_json::json!([
        agg_item_expr("MAX", upper_c_custkey(), false),
        agg_item_expr("COUNT", upper_c_custkey(), false),
    ]));

    let items = detect_aggregates(&pd(&request)).expect("the string-fn arguments must detect");
    let converted = Some(UPPER_C_CUSTKEY_SQL.to_string());
    assert_eq!(
        items,
        vec![
            SingleGroupItem::Aggregate(AggregatePlan {
                kind: AggKind::Max,
                column: None,
                arg_expr: converted.clone(),
            }),
            SingleGroupItem::Aggregate(AggregatePlan {
                kind: AggKind::CountCol,
                column: None,
                arg_expr: converted,
            }),
        ]
    );
    assert!(
        is_single_group_shape(&request),
        "the request must decompose into the single-group partial/merge scan"
    );
    let sql = dispatch_sql_with_files(&request);
    let aggregates = &scan_spec_json(&sql)["aggregates"];
    for slot in 0..2 {
        assert_eq!(
            aggregates[slot]["arg_expr"],
            serde_json::json!(UPPER_C_CUSTKEY_SQL),
            "aggregate {slot} must convert inside the scan: {sql}"
        );
    }
}

/// Scenario: One node renders one text on every DataFusion surface
#[test]
fn count_distinct_over_string_fn_carries_wrapped_arg() {
    let request = single_group_over_customer(serde_json::json!([agg_item_expr(
        "COUNT",
        upper_c_custkey(),
        true
    )]));

    let items = detect_aggregates(&pd(&request)).expect("the distinct count must detect");
    assert_eq!(
        items,
        vec![SingleGroupItem::Distinct(DistinctCount {
            column: None,
            arg_expr: Some(UPPER_C_CUSTKEY_SQL.to_string()),
        })]
    );
    assert!(is_single_group_shape(&request));
    // An expression argument is not a lone bare-column distinct, so Exasol counts it natively.
    let sql = dispatch_sql_with_files(&request);
    assert_eq!(
        outer_select_list(&sql),
        r#"COUNT(DISTINCT UPPER("LHS_T0"."C_CUSTKEY"))"#,
        "the qualified wrapper must count the Exasol-dialect expression: {sql}"
    );
    assert!(
        !sql.contains("exa_to_varchar"),
        "neither the wrapper nor its bare-column fan-out converts: {sql}"
    );
}

/// Scenario: One node renders one text on every DataFusion surface
#[test]
fn single_group_scalar_over_string_fn_aggregate_decomposes() {
    let request = single_group_over_customer(serde_json::json!([length_of(agg_item_expr(
        "MAX",
        upper_c_custkey(),
        false
    ))]));

    let items = detect_aggregates(&pd(&request)).expect("the scalar over MAX must detect");
    assert!(matches!(
        items.as_slice(),
        [SingleGroupItem::ScalarOverAggregate { .. }]
    ));
    assert_eq!(
        ordinary_plans(&items),
        vec![AggregatePlan {
            kind: AggKind::Max,
            column: None,
            arg_expr: Some(UPPER_C_CUSTKEY_SQL.to_string()),
        }],
        "the nested aggregate's plan must carry the converted argument"
    );
    assert!(is_single_group_shape(&request));
    let sql = dispatch_sql_with_files(&request);
    assert_eq!(
        scan_spec_json(&sql)["aggregates"][0]["arg_expr"],
        serde_json::json!(UPPER_C_CUSTKEY_SQL),
        "the conversion must stay inside the scan: {sql}"
    );
    assert_eq!(
        outer_select_list(&sql),
        r#"CAST(LENGTH(MAX("PARTIAL_max_0")) AS DECIMAL(18,0))"#,
        "the scalar must render over the merged partial in the Exasol dialect: {sql}"
    );
}

/// Scenario: One node renders one text on every DataFusion surface
#[test]
fn single_group_merge_wrapper_sql_for_string_fn_aggregate_has_no_exa_to_varchar() {
    let request = single_group_over_customer(serde_json::json!([
        agg_item_expr("MAX", upper_c_custkey(), false),
        length_of(agg_item_expr("MIN", upper_c_custkey(), false)),
    ]));

    let sql = dispatch_sql_with_files(&request);
    assert!(
        common_arg_literal(&sql).contains("exa_to_varchar"),
        "the fixture must carry the conversion inside the scan spec: {sql}"
    );
    assert!(
        !sql_outside_string_literals(&sql).contains("exa_to_varchar"),
        "the merge wrapper Exasol parses must not name exa_to_varchar: {sql}"
    );
}

#[test]
fn single_group_scalar_over_min_max_of_string_fn_emits_character_partials() {
    let request = customer_request(serde_json::json!({
        "aggregationType": "single_group",
        "selectList": [concat_max_dash_min_of_upper_c_custkey()],
        "selectListDataTypes": [{"type": "VARCHAR", "size": 43, "characterSet": "UTF8"}],
    }));

    let sql = dispatch_sql_with_files(&request);

    assert_eq!(
        emits_clause(&sql),
        r#""PARTIAL_max_0" VARCHAR(2000000), "PARTIAL_min_1" VARCHAR(2000000)"#,
        "text partials declared numeric would merge as numbers across shards: {sql}"
    );
}

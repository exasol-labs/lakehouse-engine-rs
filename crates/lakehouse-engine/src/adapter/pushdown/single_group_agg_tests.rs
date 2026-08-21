use super::super::grouped_agg::partial_emits_items;
use super::super::scalar_over_agg::cast_merge_items;
use super::super::test_support::*;
use super::super::validate_agg_col_types;
use super::*;
use crate::scan::spec::AggKind;

/// Extract the [`AggregatePlan`] from an ordinary-aggregate item, panicking on a
/// `COUNT(DISTINCT)` item — the detection tests assert the ordinary shape.
fn agg_of(item: &SingleGroupItem) -> &AggregatePlan {
    match item {
        SingleGroupItem::Aggregate(plan) => plan,
        SingleGroupItem::Distinct(_) | SingleGroupItem::ScalarOverAggregate { .. } => {
            panic!("expected an ordinary aggregate item")
        }
    }
}

/// Extract the [`DistinctCount`] from a `COUNT(DISTINCT)` item.
fn distinct_of(item: &SingleGroupItem) -> &DistinctCount {
    match item {
        SingleGroupItem::Distinct(dc) => dc,
        SingleGroupItem::Aggregate(_) | SingleGroupItem::ScalarOverAggregate { .. } => {
            panic!("expected a COUNT(DISTINCT) item")
        }
    }
}

/// `LENGTH(<col>)` scalar-expression node — renders to `character_length("<COL>")`.
fn length_expr(col: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "LENGTH",
        "arguments": [{"type": "column", "name": col}],
    })
}

/// `<a> * <b>` two-column product node, as Exasol pushes it once `FN_MULT` is
/// advertised (node name `MULT`; see decision-log entry [7]). Renders to
/// `("<A>" * "<B>")` via the vs-expression translator.
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

/// `ROUND(<inner>, <digits>)` scalar node, as Exasol pushes it once `FN_ROUND`
/// is advertised.
fn round_expr(inner: serde_json::Value, digits: i64) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "ROUND",
        "arguments": [inner, {"type": "literal_exactnumeric", "value": digits}],
    })
}

/// `<a> / <b>` division node.
fn float_div(a: serde_json::Value, b: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [a, b],
    })
}

/// A `DECIMAL(p,s)` `selectListDataTypes` entry.
fn decimal_type(precision: u32, scale: u32) -> serde_json::Value {
    serde_json::json!({"type": "decimal", "precision": precision, "scale": scale})
}

/// Scenario (`pushdown-planning-single-group-agg-scalar-over-aggregate`): an
/// ungrouped select item that wraps aggregates in scalar/arithmetic structure is
/// classified as a single-group item instead of declining the whole detection —
/// while every shape the decomposition cannot express still declines, so the
/// projection guard routes it to the qualified wrapper.
#[test]
fn detect_aggregates_accepts_scalar_over_aggregate_and_still_declines_undecomposable() {
    // Issue #194's shape: ROUND(SUM(L_QUANTITY), 2).
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

    // Issue #188's shape: a scalar-wrapped statistical aggregate resolves through
    // the shared AggKind tables, so no aggregate name reaches DataFusion.
    let variance_req = serde_json::json!({
        "selectList": [round_expr(agg_item("VARIANCE", Some("C_ACCTBAL"), false), 4)],
        "selectListDataTypes": [decimal_type(36, 4)],
    });
    assert!(
        detect_aggregates(&variance_req).is_some(),
        "ROUND(VARIANCE(col), 4) must decompose rather than reach DataFusion by name"
    );

    // An interleaved list of a bare aggregate and a scalar-over-aggregate keeps
    // one resolved item per select-list item, in order.
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

    // A DISTINCT inner aggregate is not decomposable: the WHOLE detection declines.
    let distinct_req = serde_json::json!({
        "selectList": [round_expr(agg_item("COUNT", Some("L_ORDERKEY"), true), 2)],
        "selectListDataTypes": [decimal_type(18, 0)],
    });
    assert!(
        detect_aggregates(&distinct_req).is_none(),
        "ROUND(COUNT(DISTINCT col), 2) must decline the whole detection"
    );

    // An unsupported inner aggregate declines.
    let median_req = serde_json::json!({
        "selectList": [round_expr(agg_item("MEDIAN", Some("L_QUANTITY"), false), 2)],
        "selectListDataTypes": [decimal_type(36, 2)],
    });
    assert!(
        detect_aggregates(&median_req).is_none(),
        "ROUND(MEDIAN(col), 2) must decline the whole detection"
    );

    // A bare source column OUTSIDE the aggregate cannot be referenced by the
    // outer merge wrapper (which exposes only PARTIAL_* columns) → decline.
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

    // A scalar item with NO nested aggregate is not a scalar-over-aggregate — the
    // pre-existing decline for a plain scalar projection is unchanged.
    let scalar_only = serde_json::json!({
        "selectList": [length_expr("L_COMMENT")],
        "selectListDataTypes": [decimal_type(18, 0)],
    });
    assert!(
        detect_aggregates(&scalar_only).is_none(),
        "a scalar item with no nested aggregate must still decline"
    );
}

/// Scenario (`pushdown-planning-single-group-agg-scalar-over-aggregate`): a
/// scalar-over-aggregate item carries its OWN select-list ordinal and its OWN
/// declared type, so an interleaved list can be reassembled in `selectList` order
/// with per-item output casts.
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

    // Only two partial columns: the nested SUM and COUNT dedup against the bare
    // items at ordinals 0 and 2.
    assert_eq!(ordinary_plans(&items).len(), 2);
}

/// A scalar-over-aggregate item whose ordinal has no `selectListDataTypes` entry
/// falls back to the same `VARCHAR(2000000)` default the grouped planner uses —
/// the sentinel `cast_to_declared_type` reads as "no cast".
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

/// Scenario (`pushdown-planning-single-group-agg-scalar-over-aggregate`): inner
/// aggregates shared across the select list collapse into ONE partial column.
/// Dedup is a correctness requirement, not an optimization: the merge rewrite
/// resolves each nested aggregate to the FIRST structurally-equal slot, so an
/// un-deduplicated `[Count, Sum, Count]` would bind the nested `COUNT(*)` to slot
/// 0 while its `EMITS` column was declared at slot 2 (decision-log [6]).
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

/// The nested aggregates of a lone scalar-over-aggregate item are folded in
/// encounter order, so the item contributes every partial column it needs.
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

/// Regression: widening `ordinary_plans` into a folding walk leaves every select
/// list WITHOUT a nested aggregate folding exactly as before — one plan per
/// ordinary aggregate item, in select-list order.
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

/// COUNT(*) translates to Count with column=None.
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

/// COUNT(col) translates to CountCol with the column name.
#[test]
fn detect_count_col_produces_count_col() {
    let req = serde_json::json!({
        "selectList": [agg_item("COUNT", Some("amount"), false)]
    });
    let plans = detect_aggregates(&req).expect("should detect COUNT(col)");
    assert_eq!(agg_of(&plans[0]).kind, AggKind::CountCol);
    assert_eq!(agg_of(&plans[0]).column.as_deref(), Some("AMOUNT"));
}

/// SUM/MIN/MAX/AVG each translate to the right kind + column.
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

/// GROUP BY present and non-empty => fall back (None).
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

/// A non-COUNT DISTINCT aggregate (e.g. SUM DISTINCT) => fall back.
/// (Single-group COUNT(DISTINCT) is now decomposed — see
/// `count_distinct_builds_distinct_row_scan_spec`.)
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

/// Unsupported aggregate function (e.g., MEDIAN) => fall back to row scan.
/// Note: STDDEV is a supported decomposable aggregate via sufficient-statistics.
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

/// Non-aggregate select item (e.g., plain column) => fall back.
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

/// Empty select list => None.
#[test]
fn detect_aggregates_returns_none_for_empty_select_list() {
    let req = serde_json::json!({ "selectList": [] });
    assert!(detect_aggregates(&req).is_none());
}

/// Scenario (bare-column regression): COUNT(*), COUNT(col), SUM/MIN/MAX/AVG(col)
/// and the STDDEV family keep the bare-column fast path — `column` populated,
/// `arg_expr` None — so the pre-existing decomposition is byte-for-byte unchanged.
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
    // Every plan takes the fast path: no rendered expression argument.
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

    // The partial EMITS clause is identical to the pre-change output: bare-column
    // SUM over DECIMAL widens to DECIMAL(36,s) from the COLUMN type (aggregate_types
    // is ignored for bare columns), independent of any declared aggregate type.
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
    // A misleading declared type must NOT override the bare-column source type.
    let emits = partial_emits_items(&sum_only, &col_types, &["VARCHAR(2000000)".to_string()]);
    assert_eq!(emits, vec![r#""PARTIAL_sum_0" DECIMAL(36,0)"#.to_string()]);
}

/// Scenario: a renderable scalar-expression argument is carried in `arg_expr`
/// (not `column`), and the partial/merge column TYPE is derived from the
/// aggregate item's declared type — SUM(expr)::DECIMAL widens to DECIMAL(36,s),
/// SUM(expr)::DOUBLE stays DOUBLE, MIN/MAX(expr) take the declared type verbatim,
/// and COUNT(expr) stays DECIMAL(20,0).
#[test]
fn expression_arg_partial_and_merge_types_from_declared_type() {
    // Detection: SUM(LENGTH(L_COMMENT)) renders the argument into arg_expr.
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
        Some(r#"character_length("L_COMMENT")"#),
        "the rendered DataFusion fragment must be carried in arg_expr"
    );

    // Typing: no source column exists, so the type comes from the declared type.
    // There is deliberately NO matching entry in col_types.
    let col_types: Vec<(String, String)> = vec![];

    let sum_expr = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: None,
        arg_expr: Some(r#"character_length("L_COMMENT")"#.into()),
    }];
    // SUM(expr) declared DECIMAL(38,4) → partial widens to DECIMAL(36,4).
    let emits = partial_emits_items(&sum_expr, &col_types, &["DECIMAL(38,4)".to_string()]);
    assert_eq!(emits, vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()]);
    // SUM(expr) declared DOUBLE → partial stays DOUBLE PRECISION.
    let emits = partial_emits_items(&sum_expr, &col_types, &["DOUBLE PRECISION".to_string()]);
    assert_eq!(
        emits,
        vec![r#""PARTIAL_sum_0" DOUBLE PRECISION"#.to_string()]
    );

    // MIN(expr) takes the declared type verbatim.
    let min_expr = vec![AggregatePlan {
        kind: AggKind::Min,
        column: None,
        arg_expr: Some(r#"("A" + "B")"#.into()),
    }];
    let emits = partial_emits_items(&min_expr, &col_types, &["DATE".to_string()]);
    assert_eq!(emits, vec![r#""PARTIAL_min_0" DATE"#.to_string()]);

    // COUNT(expr) is a plain count → DECIMAL(20,0), declared type irrelevant.
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

    // An expression SUM/MIN/MAX validates (its declared type is numeric in
    // practice; the missing column resolves to the numeric DOUBLE fallback).
    assert!(
        validate_agg_col_types(&sum_expr, &col_types),
        "expression-argument SUM must pass validation, not force a row scan"
    );
}

/// Scenario (NQ1 / TPC-H Q6 shape): `SUM(L_EXTENDEDPRICE * L_DISCOUNT)` over two
/// DECIMAL(15,2) columns. Exasol declares the SUM result as DECIMAL(36,4) (it
/// widens the DECIMAL(30,4) product's precision to its max 36, holding the
/// natural scale 4 — verified live, decision-log entry [7]). The partial column
/// must be sized from that declared type — NOT recomputed from the operands'
/// own DECIMAL(15,2) types — so it widens to DECIMAL(36,4), and the merge casts
/// to the same declared DECIMAL(36,4). This exercises the DECIMAL-with-nonzero-
/// scale declared-type path for a two-column product argument.
#[test]
fn decimal_product_sum_partial_widens_to_decimal_36() {
    // Detection: SUM(L_EXTENDEDPRICE * L_DISCOUNT) carries the product in
    // arg_expr (no bare source column) — proving the aggregate is decomposed,
    // not declined into a raw two-column row scan.
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

    // Typing is driven purely by Exasol's declared result type; there is
    // deliberately NO operand column in col_types (the product has none), so a
    // type recomputed from operands would have to reimplement Exasol's widening
    // rules. The declared DECIMAL(36,4) is authoritative and read verbatim.
    let col_types: Vec<(String, String)> = vec![];
    let declared = ["DECIMAL(36,4)".to_string()];

    let emits = partial_emits_items(&plans, &col_types, &declared);
    assert_eq!(
        emits,
        vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()],
        "partial SUM column must widen to the declared DECIMAL(36,4)"
    );

    // The merge wrapper casts the summed partial back to the declared type so
    // it matches Exasol's positional selectListDataTypes validation.
    let merge = cast_merge_items(&plans, &declared);
    assert_eq!(
        merge,
        vec![r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,4))"#.to_string()],
        "merge must cast the summed partial to the declared DECIMAL(36,4)"
    );

    // The expression-argument SUM validates (no operand column → numeric
    // DOUBLE fallback), so it is never forced into a row scan.
    assert!(validate_agg_col_types(&plans, &col_types));
}

/// Scenario: an aggregate whose argument the VS translator cannot render
/// declines the whole aggregate pushdown (row-scan fallback), rather than
/// emitting a plan referencing an argument it could not render soundly.
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
    // A distinct COUNT over an unrenderable argument also falls back.
    let req = serde_json::json!({
        "selectList": [agg_item_expr("COUNT", unknown.clone(), true)]
    });
    assert!(
        detect_aggregates(&req).is_none(),
        "COUNT(DISTINCT unrenderable) must fall back to row scan"
    );
}

/// Scenario: single-group COUNT(DISTINCT col) is decomposed into a DISTINCT
/// row-scan fan-out descriptor ([`SingleGroupItem::Distinct`], bare column
/// populated); COUNT(DISTINCT expr) carries the rendered argument; and neither
/// contributes an ordinary aggregate plan (so no partial-aggregate column is
/// emitted for it — the count is a native `COUNT(DISTINCT "V")` over the fan-out).
#[test]
fn count_distinct_builds_distinct_row_scan_spec() {
    // COUNT(DISTINCT L_SHIPMODE) — bare column fast path.
    let req = serde_json::json!({
        "selectList": [agg_item("COUNT", Some("L_SHIPMODE"), true)]
    });
    let items = detect_aggregates(&req).expect("single-group COUNT(DISTINCT) must decompose");
    assert_eq!(items.len(), 1);
    assert!(has_distinct(&items), "the item must be a distinct fan-out");
    let dc = distinct_of(&items[0]);
    assert_eq!(dc.column.as_deref(), Some("L_SHIPMODE"));
    assert!(dc.arg_expr.is_none());
    // A distinct item is NOT an ordinary aggregate: it drives no partial column.
    assert!(
        ordinary_plans(&items).is_empty(),
        "a COUNT(DISTINCT) item must not appear among the ordinary aggregate plans"
    );

    // COUNT(DISTINCT LENGTH(col)) — rendered expression argument.
    let req_expr = serde_json::json!({
        "selectList": [agg_item_expr("COUNT", length_expr("L_COMMENT"), true)]
    });
    let items_expr = detect_aggregates(&req_expr).expect("COUNT(DISTINCT expr) must decompose");
    let dc_expr = distinct_of(&items_expr[0]);
    assert!(dc_expr.column.is_none());
    assert_eq!(
        dc_expr.arg_expr.as_deref(),
        Some(r#"character_length("L_COMMENT")"#)
    );
}

/// Scenario (task 6.5): single-group `COUNT(DISTINCT)` detection fans out ONLY a
/// lone distinct (Case 1 — `is_lone_count_distinct` true), and declines every
/// multi-distinct or distinct-plus-ordinary-aggregate shape (Case 2/3 —
/// `is_lone_count_distinct` false while `has_distinct` stays true), which is the
/// dispatch condition the `mod.rs` Case 2/3 guard uses to route the request to the
/// qualified single-table wrapper instead of the fan-out.
#[test]
fn multi_count_distinct_declines_to_qualified_wrapper() {
    // Case 1: exactly one COUNT(DISTINCT), nothing else → fans out.
    let lone = serde_json::json!({
        "selectList": [agg_item("COUNT", Some("CATEGORY"), true)],
    });
    let items = detect_aggregates(&lone).expect("a lone COUNT(DISTINCT) decomposes");
    assert!(
        has_distinct(&items) && is_lone_count_distinct(&items),
        "a lone COUNT(DISTINCT) is the only shape that fans out (Case 1)"
    );

    // Case 2: more than one COUNT(DISTINCT) → declines the fan-out.
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

    // Case 3: a COUNT(DISTINCT) mixed with an ordinary aggregate → declines.
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

/// Scenario (PR #163 review finding, task 1.4): a LONE
/// `COUNT(DISTINCT <expression>)` (e.g. `COUNT(DISTINCT LENGTH(name))`, nothing
/// else in the select list) must NOT take the per-shard fan-out. After narrowing
/// `is_lone_count_distinct` to bare-column arguments, an expression argument makes
/// `is_lone_count_distinct` false while `has_distinct` stays true — which is the
/// EXACT dispatch condition (`has_distinct && !is_lone_count_distinct`) the `mod.rs`
/// Case 2/3 guard uses to decline the fan-out and route to the qualified
/// single-table wrapper, where Exasol evaluates the expression and DISTINCT natively
/// over exact-typed base columns (no `arrow::compute::cast(.., Utf8)` injectivity
/// dependency, which could silently undercount). A lone BARE-COLUMN distinct is
/// unaffected — it still fans out (Case 1).
///
/// This is the direct regression test for the dispatch narrowing: before it, a lone
/// expression distinct wrongly matched "lone" and fanned out with a VARCHAR-typed
/// `"V"`; it must now route exactly like a genuine multi-distinct/mixed Case 2/3.
#[test]
fn lone_expression_count_distinct_declines_fan_out_to_wrapper() {
    // Lone COUNT(DISTINCT LENGTH(NAME)) — a single expression-argument distinct.
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

    // Contrast: a lone BARE-COLUMN COUNT(DISTINCT) IS a lone distinct — still fans out.
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

/// MEDIAN, *_DISTINCT, APPROX_COUNT_DISTINCT, LISTAGG, GROUP_CONCAT all cause
/// parse_agg_item / detect_aggregates to return None (row-scan fallback).
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
    // A non-COUNT DISTINCT (SUM DISTINCT) is not decomposable — falls back.
    // (Single-group COUNT(DISTINCT) IS decomposed; see
    // `count_distinct_builds_distinct_row_scan_spec`.)
    let req_distinct = serde_json::json!({
        "selectList": [agg_item("SUM", Some("AMOUNT"), true)],
    });
    assert!(
        detect_aggregates(&req_distinct).is_none(),
        "SUM(DISTINCT) must fall back to row scan"
    );
}

/// parse_agg_item returns a stat plan for STDDEV/VARIANCE family names.
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

/// A statistical aggregate whose first argument is not a bare `column` node
/// declines, so the whole single-group select list declines and Exasol computes
/// the statistic natively over the Tier 3 row scan.
///
/// Before this decline such an item parsed to a plan carrying NEITHER `column`
/// nor `arg_expr`, passed type validation on the declared-type default, was
/// given three `EMITS` columns, and then failed inside the scan on an argument
/// naming no field. Measured 2026-07-31 against the Docker Exasol container:
/// `SELECT STDDEV(score + id) FROM MY_LAKEHOUSE.EVENTS` is PUSHED by Exasol and
/// fails with `sqlCode 22002`, `partial aggregate SQL error: Schema error: No
/// field named .`
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

/// The bare-column form is untouched by the expression-argument decline:
/// `STDDEV(SCORE)` still decomposes into the (cnt, sum, sum_sq) triple over the
/// source column, with no rendered argument.
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
    assert_eq!(single_group_plan_types(&req, &[]), Vec::<String>::new());
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
        single_group_plan_types(&req, &items),
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
        single_group_plan_types(&req, &items),
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
        single_group_plan_types(&req, &items),
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
        single_group_plan_types(&req, &items),
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
        single_group_plan_types(&req, &items),
        vec!["DECIMAL(36,2)".to_string()],
        "the COUNT(DISTINCT) item contributes no ordinary-aggregate slot"
    );
}

/// A plan slot reached ONLY through a nested scalar-over-aggregate is typed
/// `DOUBLE PRECISION`, not `VARCHAR(2000000)`: `plan_types` also types the scan's
/// `EMITS` clause, and an expression-argument MIN/MAX has no source column to fall
/// back to — a character partial column would make the merge's `MIN(...)` a
/// lexicographic minimum of a numeric expression.
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
    let plan_types = single_group_plan_types(&req, &items);
    assert_eq!(
        partial_emits_items(&plans, &[], &plan_types),
        vec![r#""PARTIAL_min_0" DOUBLE PRECISION"#.to_string()],
        "a nested-only MIN over an expression must emit a numeric partial column"
    );
}

/// Scenario (`pushdown-planning-single-group-agg-scalar-over-aggregate`): issue
/// #194's shape. The merge SELECT wraps the scalar structure around the MERGED
/// partial column, so the query answers one merged row — never one unwrapped
/// per-shard partial per shard.
#[test]
fn merge_select_wraps_scalar_structure_around_the_merged_partial() {
    let req = serde_json::json!({
        "selectList": [round_expr(agg_item("SUM", Some("L_QUANTITY"), false), 2)],
        "selectListDataTypes": [decimal_type(36, 2)],
    });
    let items = detect_aggregates(&req).expect("ROUND(SUM(col), 2) must decompose");
    let plans = ordinary_plans(&items);
    let plan_types = single_group_plan_types(&req, &items);

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        Some(vec![
            r#"CAST(ROUND(SUM("PARTIAL_sum_0"), 2) AS DECIMAL(36,2))"#.to_string()
        ])
    );
}

/// Scenario (`pushdown-planning-single-group-agg-scalar-over-aggregate`): an
/// interleaved list keeps `selectList` order and casts each item to ITS OWN
/// declared type — the bare aggregates to their plan slots' types, the scalar
/// item to the type Exasol declared for the scalar item itself. Exasol validates
/// the pushdown output columns positionally, so a transposed or mistyped item is
/// a hard `04000`.
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
    let plan_types = single_group_plan_types(&req, &items);

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

/// A slot with no usable declared type emits the bare uncast merge expression —
/// the same `VARCHAR(2000000)`-means-no-cast rule the grouped merge follows.
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
    let plan_types = single_group_plan_types(&req, &items);

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        Some(vec![
            r#"SUM("PARTIAL_count_0")"#.to_string(),
            r#"ROUND(SUM("PARTIAL_sum_1"), 2)"#.to_string(),
        ])
    );
}

/// A `COUNT(DISTINCT)` item has no merge expression at all — it is served by its
/// own DISTINCT row-scan fan-out. Assembling a merge SELECT over a list holding
/// one must DECLINE, not silently emit a shorter select list: a dropped column
/// is a positional `04000` at best and a wrong answer at worst. The dispatcher
/// routes such a list to the qualified wrapper before reaching here.
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
    let plan_types = single_group_plan_types(&req, &items);

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        None,
        "a distinct item carries no merge expression, so the whole assembly declines"
    );
}

/// A scalar structure the Exasol-dialect renderer cannot render (an unsupported
/// CAST target, here) carries no merge expression, so the merge assembly must
/// DECLINE — the reachability boundary the dispatcher's `else` arm in `mod.rs`
/// routes to the qualified wrapper. No current node type actually reaches this
/// path through `detect_aggregates` (an unrenderable structure declines at
/// `classify_scalar_over_aggregate` time, before a `ScalarOverAggregate` item is
/// ever produced), so this constructs the item directly to pin the function's own
/// contract at this boundary.
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
    let plan_types = single_group_plan_types(&serde_json::json!({}), &items);

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        None,
        "an unrenderable scalar structure carries no merge expression, so the \
         whole assembly declines"
    );
}

/// A literal duplicate bare aggregate collapses to ONE partial slot, yet the merge
/// SELECT still carries ONE item per select-list item: Exasol validates the
/// returned column count positionally against the select list it sent, so a
/// deduplicated merge would be an arity mismatch, not an optimization (#190).
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
    let plan_types = single_group_plan_types(&req, &items);
    assert_eq!(plans.len(), 1, "one partial column for both occurrences");

    assert_eq!(
        single_group_merge_select(&items, &plans, &plan_types),
        Some(vec![
            r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,2))"#.to_string(),
            r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,2))"#.to_string(),
        ])
    );
}

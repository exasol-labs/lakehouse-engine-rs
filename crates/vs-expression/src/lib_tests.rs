use super::*;
use serde_json::json;

#[test]
fn renders_column_as_quoted_uppercase_ident() {
    let expr = json!({"type": "column", "name": "region"});
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""REGION""#);
}

#[test]
fn renders_column_with_embedded_quotes() {
    let expr = json!({"type": "column", "name": r#"my"col"#});
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""MY""COL""#);
}

#[test]
fn renders_table_qualified_column_when_alias_present() {
    let expr = json!({"type": "column", "name": "id", "tableAlias": "LHS_FACT"});
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""LHS_FACT"."ID""#);
}

#[test]
fn empty_table_alias_falls_back_to_bare_column() {
    let expr = json!({"type": "column", "name": "id", "tableAlias": ""});
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""ID""#);
}

#[test]
fn renders_string_literal() {
    let expr = json!({"type": "literal_string", "value": "hello"});
    assert_eq!(render_expression(&expr).unwrap(), "'hello'");
}

#[test]
fn renders_string_literal_with_single_quote_escaped() {
    let expr = json!({"type": "literal_string", "value": "it's"});
    assert_eq!(render_expression(&expr).unwrap(), "'it''s'");
}

#[test]
fn renders_null_literal() {
    let expr = json!({"type": "literal_null"});
    assert_eq!(render_expression(&expr).unwrap(), "NULL");
}

#[test]
fn renders_bool_literal() {
    let t = json!({"type": "literal_bool", "value": true});
    let f = json!({"type": "literal_bool", "value": false});
    assert_eq!(render_expression(&t).unwrap(), "TRUE");
    assert_eq!(render_expression(&f).unwrap(), "FALSE");
}

#[test]
fn renders_numeric_literal() {
    let expr = json!({"type": "literal_exactnumeric", "value": 42});
    assert_eq!(render_expression(&expr).unwrap(), "42");
}

#[test]
fn renders_date_literal() {
    let expr = json!({"type": "literal_date", "value": "2024-01-15"});
    assert_eq!(render_expression(&expr).unwrap(), "DATE '2024-01-15'");
}

#[test]
fn renders_timestamp_literal() {
    let expr = json!({"type": "literal_timestamp", "value": "2024-01-15 12:00:00"});
    assert_eq!(
        render_expression(&expr).unwrap(),
        "arrow_cast('2024-01-15 12:00:00', 'Timestamp(Microsecond, None)')"
    );
}

#[test]
fn renders_far_future_timestamp_literal() {
    let expr = json!({"type": "literal_timestamp", "value": "9999-12-31 23:59:59"});
    assert_eq!(
        render_expression(&expr).unwrap(),
        "arrow_cast('9999-12-31 23:59:59', 'Timestamp(Microsecond, None)')"
    );
}

#[test]
fn renders_simple_equality() {
    let expr = json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "id"},
        "right": {"type": "literal_exactnumeric", "value": 10}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("ID" = 10)"#);
}

#[test]
fn renders_and_predicate() {
    let expr = json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_greater", "left": {"type": "column", "name": "age"}, "right": {"type": "literal_exactnumeric", "value": 18}},
            {"type": "predicate_less", "left": {"type": "column", "name": "age"}, "right": {"type": "literal_exactnumeric", "value": 65}}
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("AND"), "AND not found in: {sql}");
}

#[test]
fn renders_or_predicate() {
    let expr = json!({
        "type": "predicate_or",
        "expressions": [
            {"type": "predicate_equal", "left": {"type": "column", "name": "status"}, "right": {"type": "literal_string", "value": "A"}},
            {"type": "predicate_equal", "left": {"type": "column", "name": "status"}, "right": {"type": "literal_string", "value": "B"}}
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("OR"), "OR not found in: {sql}");
}

#[test]
fn renders_not_predicate() {
    let expr = json!({
        "type": "predicate_not",
        "expression": {"type": "predicate_equal", "left": {"type": "column", "name": "active"}, "right": {"type": "literal_bool", "value": true}}
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("NOT"), "NOT not found in: {sql}");
}

#[test]
fn renders_empty_and_as_true() {
    let expr = json!({"type": "predicate_and", "expressions": []});
    assert_eq!(render_expression(&expr).unwrap(), "TRUE");
}

#[test]
fn renders_empty_or_as_false() {
    let expr = json!({"type": "predicate_or", "expressions": []});
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

#[test]
fn renders_is_null() {
    let expr = json!({"type": "predicate_is_null", "expression": {"type": "column", "name": "x"}});
    assert_eq!(render_expression(&expr).unwrap(), r#"("X" IS NULL)"#);
}

#[test]
fn renders_is_not_null() {
    let expr =
        json!({"type": "predicate_is_not_null", "expression": {"type": "column", "name": "x"}});
    assert_eq!(render_expression(&expr).unwrap(), r#"("X" IS NOT NULL)"#);
}

#[test]
fn renders_in_constlist() {
    let expr = json!({
        "type": "predicate_in_constlist",
        "expression": {"type": "column", "name": "status"},
        "arguments": [
            {"type": "literal_string", "value": "A"},
            {"type": "literal_string", "value": "B"}
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("IN"), "IN not found: {sql}");
    assert!(sql.contains("'A'"), "'A' not found: {sql}");
    assert!(sql.contains("'B'"), "'B' not found: {sql}");
}

#[test]
fn renders_empty_in_as_false() {
    let expr = json!({
        "type": "predicate_in_constlist",
        "expression": {"type": "column", "name": "x"},
        "arguments": []
    });
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

#[test]
fn renders_in_constlist_strips_null() {
    let expr = json!({
        "type": "predicate_in_constlist",
        "expression": {"type": "column", "name": "status"},
        "arguments": [
            {"type": "literal_string", "value": "A"},
            {"type": "literal_null"},
            {"type": "literal_date", "value": null}
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("'A'"), "'A' not found: {sql}");
    assert!(!sql.contains("NULL"), "NULL should not survive: {sql}");
}

#[test]
fn renders_all_null_in_as_false() {
    let expr = json!({
        "type": "predicate_in_constlist",
        "expression": {"type": "column", "name": "x"},
        "arguments": [
            {"type": "literal_null"},
            {"type": "literal_date", "value": null}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

#[test]
fn renders_not_in_constlist_strips_null() {
    let expr = json!({
        "type": "predicate_not",
        "expression": {
            "type": "predicate_in_constlist",
            "expression": {"type": "column", "name": "status"},
            "arguments": [
                {"type": "literal_string", "value": "A"},
                {"type": "literal_null"},
                {"type": "literal_date", "value": null}
            ]
        }
    });
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#"(NOT ("STATUS" IN ('A')))"#);
}

#[test]
fn renders_between() {
    let expr = json!({
        "type": "predicate_between",
        "expression": {"type": "column", "name": "age"},
        "left": {"type": "literal_exactnumeric", "value": 18},
        "right": {"type": "literal_exactnumeric", "value": 65}
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("BETWEEN"), "BETWEEN not found: {sql}");
    assert!(sql.contains("18"), "low bound not found: {sql}");
    assert!(sql.contains("65"), "high bound not found: {sql}");
}

#[test]
fn renders_like_without_escape() {
    let expr = json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "A%"}
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("LIKE"), "LIKE not found: {sql}");
    assert!(!sql.contains("ESCAPE"), "ESCAPE should be absent: {sql}");
}

#[test]
fn renders_like_with_escape() {
    let expr = json!({
        "type": "predicate_like",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "A!%"},
        "escape_char": "!"
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("LIKE"), "LIKE not found: {sql}");
    assert!(sql.contains("ESCAPE"), "ESCAPE not found: {sql}");
    assert!(sql.contains("'!'"), "escape char not found: {sql}");
}

#[test]
fn renders_arithmetic_add() {
    let expr = json!({
        "type": "function_scalar",
        "name": "ADD",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" + 1)"#);
}

#[test]
fn renders_arithmetic_sub() {
    let expr = json!({
        "type": "function_scalar",
        "name": "SUB",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" - 1)"#);
}

#[test]
fn renders_arithmetic_mul() {
    let expr = json!({
        "type": "function_scalar",
        "name": "MULT",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" * 2)"#);
}

/// Scenario: the legacy node name `MUL` is not recognized; Exasol only emits `MULT`.
#[test]
fn legacy_mul_name_is_not_recognized() {
    let expr = json!({
        "type": "function_scalar",
        "name": "MUL",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert!(
        render_expression_safe(&expr).is_none(),
        "the obsolete \"MUL\" node name must not translate; Exasol emits \"MULT\""
    );
}

/// Scenario: the two-column product `L_EXTENDEDPRICE * L_DISCOUNT` renders.
#[test]
fn renders_two_column_arithmetic_product() {
    let expr = json!({
        "type": "function_scalar",
        "name": "MULT",
        "arguments": [
            {"type": "column", "name": "l_extendedprice"},
            {"type": "column", "name": "l_discount"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#
    );
}

/// Scenario: arithmetic node names equal the advertised `FN_` capability names minus the prefix.
#[test]
fn arithmetic_operator_set_matches_advertised_capabilities() {
    let arithmetic = [
        (
            "FN_ADD",
            "ADD",
            r#"("L_EXTENDEDPRICE" + "L_DISCOUNT")"#.to_string(),
        ),
        (
            "FN_SUB",
            "SUB",
            r#"("L_EXTENDEDPRICE" - "L_DISCOUNT")"#.to_string(),
        ),
        (
            "FN_MULT",
            "MULT",
            r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#.to_string(),
        ),
        (
            "FN_FLOAT_DIV",
            "FLOAT_DIV",
            format!(r#"{CHECKED_FLOAT_DIV_FN}("L_EXTENDEDPRICE", "L_DISCOUNT")"#),
        ),
    ];
    for (cap, node, expected) in arithmetic {
        assert_eq!(
            node,
            cap.strip_prefix("FN_").unwrap(),
            "node name must equal capability {cap} minus FN_ prefix"
        );
        let expr = json!({
            "type": "function_scalar",
            "name": node,
            "arguments": [
                {"type": "column", "name": "l_extendedprice"},
                {"type": "column", "name": "l_discount"}
            ]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            expected,
            "translator must render advertised capability {cap} (node {node})"
        );
    }
}

#[test]
fn float_div_calls_checked_division_for_column_left_operand_and_literal_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}("A", 2)"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_column_left_operand_and_column_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}("A", "B")"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_literal_left_operand_and_column_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "literal_exactnumeric", "value": 10},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(10, "B")"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_literal_left_operand_and_literal_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "literal_exactnumeric", "value": 10},
            {"type": "literal_exactnumeric", "value": 4}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(10, 4)"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_nested_expression_left_and_column_right() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "ADD",
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "column", "name": "b"}
                ]
            },
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(("A" + "B"), "C")"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_nested_expression_left_and_literal_right() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "ADD",
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "column", "name": "b"}
                ]
            },
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(("A" + "B"), 2)"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_aggregate_left_operand_and_column_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {
                "type": "function_aggregate",
                "name": "SUM",
                "arguments": [{"type": "column", "name": "amount"}],
                "distinct": false
            },
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(SUM("AMOUNT"), "C")"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_aggregate_left_operand_and_literal_right_operand() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {
                "type": "function_aggregate",
                "name": "SUM",
                "arguments": [{"type": "column", "name": "amount"}],
                "distinct": false
            },
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(SUM("AMOUNT"), 2)"#)
    );
}

#[test]
fn float_div_with_null_left_operand_passes_the_null_literal_to_checked_division() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "literal_null"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(NULL, "B")"#)
    );
}

#[test]
fn float_div_with_null_right_operand_passes_null_to_checked_division() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_null"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}("A", NULL)"#)
    );
}

#[test]
fn float_div_null_over_zero_passes_both_literals_to_checked_division() {
    let expr = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "literal_null"},
            {"type": "literal_exactnumeric", "value": 0}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(NULL, 0)"#)
    );
}

#[test]
fn renders_arithmetic_neg() {
    let expr = json!({
        "type": "function_scalar",
        "name": "NEG",
        "arguments": [
            {"type": "column", "name": "a"}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"(-"A")"#);
}

#[test]
fn neg_composes_with_aggregate_decomposition() {
    let sum_neg = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{
            "type": "function_scalar",
            "name": "NEG",
            "arguments": [{"type": "column", "name": "col"}]
        }],
        "distinct": false
    });
    assert_eq!(render_expression(&sum_neg).unwrap(), r#"SUM((-"COL"))"#);
}

#[test]
fn renders_cast_varchar() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "VARCHAR", "size": 100}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
}

#[test]
fn renders_cast_decimal() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "DECIMAL", "precision": 10, "scale": 2}
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("X" AS DECIMAL(10,2))"#
    );
}

#[test]
fn renders_cast_double() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "DOUBLE"}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS DOUBLE)"#);
}

#[test]
fn renders_cast_date() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "DATE"}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS DATE)"#);
}

#[test]
fn renders_cast_char_as_datafusion_varchar() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "CHAR", "size": 3, "characterSet": "ASCII"}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
}

#[test]
fn renders_cast_char_as_exasol_char() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "CHAR", "size": 3, "characterSet": "ASCII"}
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("X" AS CHAR(3) ASCII)"#
    );
}

#[test]
fn renders_cast_bool_to_varchar_as_exasol_case_uppercase() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{
            "type": "predicate_greater",
            "left": {"type": "column", "name": "c_acctbal"},
            "right": {"type": "literal_exactnumeric", "value": 0}
        }],
        "dataType": {"type": "VARCHAR", "size": 10}
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE ("C_ACCTBAL" > 0) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)"#
    );
}

#[test]
fn renders_cast_bool_to_varchar_uses_case_for_any_predicate_source() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{
            "type": "predicate_is_null",
            "expression": {"type": "column", "name": "x"}
        }],
        "dataType": {"type": "VARCHAR", "size": 10}
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE ("X" IS NULL) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)"#
    );
}

#[test]
fn renders_cast_boolean() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "BOOLEAN"}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS BOOLEAN)"#);
}

#[test]
fn renders_cast_timestamp_without_local_time_zone() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "TIMESTAMP", "withLocalTimeZone": false, "fractionalSecondsPrecision": 3}
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("X" AS TIMESTAMP(3))"#
    );
}

#[test]
fn renders_cast_timestamp_precision_per_dialect() {
    fn cast(data_type: Json) -> Json {
        json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": data_type
        })
    }

    for p in 0u64..=9 {
        let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": p}));
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"CAST("X" AS TIMESTAMP({p}))"#),
            "Exasol dialect must render TIMESTAMP({p}) verbatim"
        );
    }

    for p in [0u64, 3, 6, 9] {
        let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": p}));
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!(r#"CAST("X" AS TIMESTAMP({p}))"#),
            "DataFusion dialect must render TIMESTAMP({p}) verbatim"
        );
        assert_eq!(
            render_expression_safe(&expr),
            Some(format!(r#"CAST("X" AS TIMESTAMP({p}))"#)),
            "DataFusion dialect safe variant must render TIMESTAMP({p}) verbatim"
        );
    }

    for p in [1u64, 2, 4, 5, 7, 8, 10, 99] {
        let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": p}));
        assert!(
            render_expression(&expr).is_err(),
            "DataFusion dialect must decline TIMESTAMP({p})"
        );
        assert_eq!(
            render_expression_safe(&expr),
            None,
            "DataFusion dialect safe variant must decline TIMESTAMP({p})"
        );
    }

    for data_type in [
        json!({"type": "TIMESTAMP"}),
        json!({"type": "TIMESTAMP", "withLocalTimeZone": false}),
    ] {
        let expr = cast(data_type.clone());
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP)"#,
            "DataFusion dialect: absent precision must render bare TIMESTAMP for {data_type}"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP)"#,
            "Exasol dialect: absent precision must render bare TIMESTAMP for {data_type}"
        );
    }
}

#[test]
fn cast_to_unsupported_target_declines() {
    let unsupported = [
        json!({"type": "INTERVAL", "fromTo": "YEAR TO MONTH", "precision": 2}),
        json!({"type": "INTERVAL", "fromTo": "DAY TO SECONDS", "precision": 2, "fraction": 2}),
        json!({"type": "GEOMETRY", "srid": 4326}),
        json!({"type": "HASHTYPE", "bytesize": 16}),
        json!({"type": "TIMESTAMP", "withLocalTimeZone": true, "fractionalSecondsPrecision": 9}),
    ];
    for data_type in unsupported {
        let expr = json!({
            "type": "function_scalar_cast",
            "name": "CAST",
            "arguments": [{"type": "column", "name": "x"}],
            "dataType": data_type.clone()
        });
        assert!(
            render_expression(&expr).is_err(),
            "CAST to {data_type} must raise in raising mode"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "CAST to {data_type} must be None in safe mode"
        );
    }
}

#[test]
fn renders_cast_nested_function_scalar_defensive() {
    let expr = json!({
        "type": "function_scalar",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "VARCHAR", "size": 100}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
}

#[test]
fn renders_decimal_to_varchar_exasol() {
    let expr = json!({
        "type": "decimal_to_varchar_exasol",
        "arguments": [{"type": "column", "name": "c_decimal_a"}]
    });
    let expected = format_decimal_exasol_style(r#""C_DECIMAL_A""#);
    assert_eq!(render_expression(&expr).unwrap(), expected);
    assert_eq!(render_expression_safe(&expr).unwrap(), expected);
}

#[test]
fn decimal_to_varchar_exasol_wrong_arity_errors() {
    let no_args = json!({
        "type": "decimal_to_varchar_exasol",
        "arguments": []
    });
    let two_args = json!({
        "type": "decimal_to_varchar_exasol",
        "arguments": [
            {"type": "column", "name": "c_decimal_a"},
            {"type": "column", "name": "c_decimal_b"}
        ]
    });
    for expr in [&no_args, &two_args] {
        assert!(
            render_expression(expr).is_err(),
            "decimal_to_varchar_exasol with non-unary arguments must raise: {expr}"
        );
        assert!(
            render_expression_safe(expr).is_none(),
            "decimal_to_varchar_exasol with non-unary arguments must be None in safe mode: {expr}"
        );
    }
}

#[test]
fn format_decimal_exasol_style_renders_exact_regex_sql() {
    assert_eq!(
        format_decimal_exasol_style("some_col"),
        r#"regexp_replace(regexp_replace(CAST(some_col AS VARCHAR), '(\.[0-9]*[1-9])0+$', '\1'), '\.0+$', '')"#
    );
}

#[test]
fn unsupported_node_returns_error() {
    let expr = json!({"type": "fn_sum", "operands": []});
    let err = render_expression(&expr).unwrap_err();
    assert!(
        err.to_string().contains("fn_sum"),
        "error must name the unsupported type: {err}"
    );
}

#[test]
fn unsupported_node_returns_none_in_safe_mode() {
    let expr = json!({"type": "fn_sum", "operands": []});
    assert!(render_expression_safe(&expr).is_none());
}

#[test]
fn true_filter_returns_none_in_safe_mode() {
    let expr = json!({"type": "literal_bool", "value": true});
    assert!(render_df_filter_safe(&expr).is_none());
}

#[test]
fn null_filter_returns_none_in_safe_mode() {
    let expr = json!({"type": "literal_null"});
    assert!(render_df_filter_safe(&expr).is_none());
}

#[test]
fn renders_timestamp_utc_literal() {
    let expr = json!({"type": "literal_timestamp_utc", "value": "2024-03-01 10:00:00"});
    let sql = render_expression(&expr).unwrap();
    assert_eq!(
        sql,
        "arrow_cast('2024-03-01 10:00:00+00:00', 'Timestamp(Microsecond, Some(\"UTC\"))')"
    );
}

#[test]
fn renders_timestamp_literals_as_bare_timestamp_in_exasol_dialect() {
    let ts = json!({"type": "literal_timestamp", "value": "2024-01-15 12:00:00"});
    let ts_utc = json!({"type": "literal_timestamp_utc", "value": "2024-03-01 10:00:00"});

    let ts_exasol = render_expression_exasol(&ts).unwrap();
    assert_eq!(ts_exasol, "TIMESTAMP '2024-01-15 12:00:00'");
    assert!(
        !ts_exasol.contains("arrow_cast"),
        "Exasol rejects arrow_cast with sqlCode 42000: {ts_exasol}"
    );

    assert_eq!(
        render_expression_exasol(&json!({
            "type": "literal_timestamp",
            "value": "2024-01-15 12:00:00' OR '1'='1"
        }))
        .unwrap(),
        "TIMESTAMP '2024-01-15 12:00:00'' OR ''1''=''1'"
    );

    assert_eq!(
        render_expression(&ts).unwrap(),
        "arrow_cast('2024-01-15 12:00:00', 'Timestamp(Microsecond, None)')"
    );
    assert_eq!(
        render_expression(&ts_utc).unwrap(),
        "arrow_cast('2024-03-01 10:00:00+00:00', 'Timestamp(Microsecond, Some(\"UTC\"))')"
    );
}

#[test]
fn renders_timestamp_utc_literal_via_convert_tz_in_exasol_dialect() {
    let value = "2024-03-01 10:00:00";
    let ts_utc = json!({"type": "literal_timestamp_utc", "value": value});

    let exasol = render_expression_exasol(&ts_utc).unwrap();
    assert_eq!(
        exasol,
        "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 10:00:00', 'UTC', SESSIONTIMEZONE) \
         AS TIMESTAMP WITH LOCAL TIME ZONE)"
    );
    assert!(
        !exasol.contains("+00:00"),
        "Exasol rejects an offset in a TIMESTAMP literal with sqlCode 22018: {exasol}"
    );

    assert!(
        render_expression(&ts_utc).unwrap().contains("+00:00"),
        "the DataFusion dialect keeps the offset that types the literal UTC"
    );
}

#[test]
fn literal_timestamputc_wire_name_renders_exasol_only() {
    let value = "2024-03-01 09:00:00";
    let real_wire_name = json!({"type": "literal_timestamputc", "value": value});
    assert_eq!(
        render_expression_exasol(&real_wire_name).unwrap(),
        "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) \
         AS TIMESTAMP WITH LOCAL TIME ZONE)"
    );

    assert!(render_expression_safe(&real_wire_name).is_none());
}

#[test]
fn renders_null_valued_tstz_literal_bare_in_exasol_dialect() {
    for node_type in ["literal_timestamp_utc", "literal_timestamputc"] {
        for node in [
            json!({"type": node_type, "value": null}),
            json!({"type": node_type}),
        ] {
            assert_eq!(
                render_expression_exasol(&node).unwrap(),
                "NULL",
                "{node_type} with a null/absent value must render bare NULL"
            );
        }
    }
}

#[test]
fn renders_null_valued_timestamp_literal_per_dialect() {
    let cases = [
        (
            "literal_timestamp",
            "arrow_cast(NULL, 'Timestamp(Microsecond, None)')",
        ),
        ("literal_timestamp_utc", "NULL"),
    ];
    for (node_type, expected_datafusion) in cases {
        for (variant, node) in [
            (
                "carrying a JSON-null value",
                json!({"type": node_type, "value": null}),
            ),
            ("with no value key at all", json!({"type": node_type})),
        ] {
            assert_eq!(
                render_expression_exasol(&node).unwrap(),
                "NULL",
                "{node_type} {variant}, Exasol dialect"
            );
            assert_eq!(
                render_expression(&node).unwrap(),
                expected_datafusion,
                "{node_type} {variant}, DataFusion dialect"
            );
        }
    }
}

#[test]
fn renders_regexp_like() {
    let expr = json!({
        "type": "predicate_like_regexp",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "^A.*"}
    });
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#"regexp_like("NAME", '^A.*')"#);

    let expr2 = json!({
        "type": "function_scalar",
        "name": "REGEXP_LIKE",
        "arguments": [
            {"type": "column", "name": "name"},
            {"type": "literal_string", "value": "^B.*"}
        ]
    });
    let sql2 = render_expression(&expr2).unwrap();
    assert_eq!(sql2, r#"regexp_like("NAME", '^B.*')"#);
}

#[test]
fn renders_regexp_like_as_infix_predicate_in_exasol_dialect() {
    let predicate = json!({
        "type": "predicate_like_regexp",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "^A.*"}
    });
    let scalar = json!({
        "type": "function_scalar",
        "name": "REGEXP_LIKE",
        "arguments": [
            {"type": "column", "name": "name"},
            {"type": "literal_string", "value": "^A.*"}
        ]
    });

    let predicate_exasol = render_expression_exasol(&predicate).unwrap();
    let scalar_exasol = render_expression_exasol(&scalar).unwrap();
    assert_eq!(predicate_exasol, r#"("NAME" REGEXP_LIKE '^A.*')"#);
    assert_eq!(
        scalar_exasol, predicate_exasol,
        "the two REGEXP_LIKE encodings must render byte-identically in the Exasol dialect"
    );

    assert_eq!(
        render_expression(&predicate).unwrap(),
        r#"regexp_like("NAME", '^A.*')"#
    );
    assert_eq!(
        render_expression(&scalar).unwrap(),
        r#"regexp_like("NAME", '^A.*')"#
    );
}

#[test]
fn regexp_like_predicate_missing_operand_errors_in_both_dialects() {
    let missing_pattern = json!({
        "type": "predicate_like_regexp",
        "expression": {"type": "column", "name": "name"}
    });
    assert!(render_expression(&missing_pattern).is_err());
    assert!(render_expression_exasol(&missing_pattern).is_err());

    let missing_expression = json!({
        "type": "predicate_like_regexp",
        "pattern": {"type": "literal_string", "value": "^A.*"}
    });
    assert!(render_expression(&missing_expression).is_err());
    assert!(render_expression_exasol(&missing_expression).is_err());
}

#[test]
fn regexp_like_scalar_arity_errors_in_both_dialects() {
    let one_arg = json!({
        "type": "function_scalar",
        "name": "REGEXP_LIKE",
        "arguments": [{"type": "column", "name": "name"}]
    });
    assert!(render_expression(&one_arg).is_err());
    assert!(render_expression_exasol(&one_arg).is_err());
}

#[test]
fn renders_math_scalar_functions() {
    let cases_1arg = [
        ("ABS", "abs"),
        ("FLOOR", "floor"),
        ("CEIL", "ceil"),
        ("SQRT", "sqrt"),
        ("EXP", "exp"),
        ("LN", "ln"),
        ("SIGN", "signum"),
        ("DEGREES", "degrees"),
        ("RADIANS", "radians"),
        ("SIN", "sin"),
        ("COS", "cos"),
        ("TAN", "tan"),
        ("ASIN", "asin"),
        ("ACOS", "acos"),
        ("ATAN", "atan"),
        ("SINH", "sinh"),
        ("COSH", "cosh"),
        ("TANH", "tanh"),
        ("COT", "cot"),
    ];
    for (exasol, df) in cases_1arg {
        let expr = json!({
            "type": "function_scalar",
            "name": exasol,
            "arguments": [{"type": "column", "name": "x"}]
        });
        let sql = render_expression(&expr).unwrap();
        assert_eq!(sql, format!(r#"{df}("X")"#), "failed for {exasol}");
    }

    let expr = json!({
        "type": "function_scalar",
        "name": "POWER",
        "arguments": [
            {"type": "column", "name": "x"},
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"power("X", 2)"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "ATAN2",
        "arguments": [
            {"type": "column", "name": "y"},
            {"type": "column", "name": "x"}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"atan2("Y", "X")"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "ROUND",
        "arguments": [{"type": "column", "name": "v"}, {"type": "literal_exactnumeric", "value": 2}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"round("V", 2)"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "TRUNC",
        "arguments": [{"type": "column", "name": "v"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"trunc("V")"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "ABS",
        "arguments": [
            {"type": "column", "name": "x"},
            {"type": "column", "name": "y"}
        ]
    });
    assert!(render_expression_safe(&expr).is_none());
}

#[test]
fn renders_mod_as_operator() {
    let expr = json!({
        "type": "function_scalar",
        "name": "MOD",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" % 3)"#);
}

#[test]
fn renders_mod_as_function_call_in_exasol_dialect() {
    let expr = json!({
        "type": "function_scalar",
        "name": "MOD",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(render_expression_exasol(&expr).unwrap(), r#"MOD("A", 3)"#);
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" % 3)"#);
}

#[test]
fn renders_string_scalar_functions() {
    let cases_lower = [
        "LOWER",
        "UPPER",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "REPLACE",
        "REPEAT",
        "REVERSE",
        "LPAD",
        "RPAD",
        "ASCII",
        "CHR",
        "INITCAP",
        "LEFT",
        "RIGHT",
        "TRANSLATE",
    ];
    for name in cases_lower {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [{"type": "column", "name": "s"}]
        });
        let sql = render_expression(&expr).unwrap();
        assert_eq!(
            sql,
            format!(r#"{}("S")"#, name.to_lowercase()),
            "failed for {name}"
        );
    }

    let expr = json!({
        "type": "function_scalar",
        "name": "LENGTH",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"character_length("S")"#
    );

    let expr = json!({
        "type": "function_scalar",
        "name": "OCTET_LENGTH",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"octet_length("S")"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "UNICODE",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"ascii("S")"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "UNICODECHR",
        "arguments": [{"type": "column", "name": "n"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"chr("N")"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "SUBSTR",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_exactnumeric", "value": 1},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"substr("S", 1, 3)"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "INSTR",
        "arguments": [
            {"type": "literal_string", "value": "hello"},
            {"type": "literal_string", "value": "ll"}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), "strpos('hello', 'll')");

    let expr = json!({
        "type": "function_scalar",
        "name": "LOCATE",
        "arguments": [
            {"type": "literal_string", "value": "ll"},
            {"type": "literal_string", "value": "hello"}
        ]
    });
    assert_eq!(render_expression(&expr).unwrap(), "strpos('hello', 'll')");
}

#[test]
fn renders_concat_as_nullif_wrapped_concat_call() {
    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_string", "value": ""}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat("S", ''), '')"#
    );

    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"},
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat("A", "B", "C"), '')"#
    );
}

#[test]
fn renders_concat_bool_operand_as_exasol_case() {
    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "active"},
             "right": {"type": "literal_bool", "value": true}},
            {"type": "literal_string", "value": ""}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat((CASE ("ACTIVE" = TRUE) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END), ''), '')"#
    );
}

#[test]
fn renders_concat_as_chained_pipe_operator_in_exasol_dialect() {
    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_string", "value": ""}
        ]
    });
    assert_eq!(render_expression_exasol(&expr).unwrap(), r#"("S" || '')"#);

    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"},
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"("A" || "B" || "C")"#
    );

    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "active"},
             "right": {"type": "literal_bool", "value": true}},
            {"type": "literal_string", "value": ""}
        ]
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"((CASE ("ACTIVE" = TRUE) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END) || '')"#
    );
}

#[test]
fn renders_nested_concat_wrapper_per_level() {
    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "function_scalar",
             "name": "CONCAT",
             "arguments": [
                 {"type": "column", "name": "a"},
                 {"type": "column", "name": "b"}
             ]},
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat(nullif(concat("A", "B"), ''), "C"), '')"#
    );
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"(("A" || "B") || "C")"#
    );
}

#[test]
fn renders_concat_single_argument() {
    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat("S"), '')"#
    );
    assert_eq!(render_expression_exasol(&expr).unwrap(), r#"("S")"#);
}

#[test]
fn concat_empty_argument_list_errors_in_both_dialects() {
    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": []
    });
    let expected = "function_scalar CONCAT requires at least 1 argument, got 0";
    assert_eq!(render_expression(&expr).unwrap_err().to_string(), expected);
    assert_eq!(
        render_expression_exasol(&expr).unwrap_err().to_string(),
        expected
    );
    assert!(render_expression_safe(&expr).is_none());
    assert!(render_expression_exasol_safe(&expr).is_none());
}

#[test]
fn concat_missing_arguments_or_null_argument_errors_in_both_dialects() {
    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT"
    });
    let expected = "function_scalar CONCAT missing 'arguments'";
    assert_eq!(render_expression(&expr).unwrap_err().to_string(), expected);
    assert_eq!(
        render_expression_exasol(&expr).unwrap_err().to_string(),
        expected
    );
    assert!(render_expression_safe(&expr).is_none());
    assert!(render_expression_exasol_safe(&expr).is_none());

    let expr = json!({
        "type": "function_scalar",
        "name": "CONCAT",
        "arguments": [
            {"type": "column", "name": "a"},
            null
        ]
    });
    let expected = "CONCAT argument rendered to null";
    assert_eq!(render_expression(&expr).unwrap_err().to_string(), expected);
    assert_eq!(
        render_expression_exasol(&expr).unwrap_err().to_string(),
        expected
    );
    assert!(render_expression_safe(&expr).is_none());
    assert!(render_expression_exasol_safe(&expr).is_none());
}

#[test]
fn renders_case_when() {
    let expr = json!({
        "type": "function_scalar",
        "name": "CASE",
        "arguments": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "status"},
             "right": {"type": "literal_string", "value": "A"}},
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#"CASE WHEN ("STATUS" = 'A') THEN 1 END"#);

    let expr2 = json!({
        "type": "function_scalar",
        "name": "CASE",
        "arguments": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "x"},
             "right": {"type": "literal_exactnumeric", "value": 1}},
            {"type": "literal_string", "value": "one"},
            {"type": "predicate_equal",
             "left": {"type": "column", "name": "x"},
             "right": {"type": "literal_exactnumeric", "value": 2}},
            {"type": "literal_string", "value": "two"},
            {"type": "literal_string", "value": "other"}
        ]
    });
    let sql2 = render_expression(&expr2).unwrap();
    assert_eq!(
        sql2,
        r#"CASE WHEN ("X" = 1) THEN 'one' WHEN ("X" = 2) THEN 'two' ELSE 'other' END"#
    );

    let expr3 = json!({
        "type": "function_scalar",
        "name": "CASE",
        "arguments": []
    });
    assert!(render_expression_safe(&expr3).is_none());
}

#[test]
fn renders_greatest_least() {
    let expr = json!({
        "type": "function_scalar",
        "name": "GREATEST",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"},
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN "A" IS NULL OR "B" IS NULL OR "C" IS NULL THEN NULL ELSE greatest("A", "B", "C") END"#
    );

    let expr2 = json!({
        "type": "function_scalar",
        "name": "LEAST",
        "arguments": [
            {"type": "column", "name": "x"},
            {"type": "literal_exactnumeric", "value": 0}
        ]
    });
    assert_eq!(
        render_expression(&expr2).unwrap(),
        r#"CASE WHEN "X" IS NULL OR 0 IS NULL THEN NULL ELSE least("X", 0) END"#
    );
}

#[test]
fn renders_greatest_least_single_argument_guard() {
    let greatest = json!({
        "type": "function_scalar",
        "name": "GREATEST",
        "arguments": [{"type": "column", "name": "a"}]
    });
    assert_eq!(
        render_expression(&greatest).unwrap(),
        r#"CASE WHEN "A" IS NULL THEN NULL ELSE greatest("A") END"#
    );

    let least = json!({
        "type": "function_scalar",
        "name": "LEAST",
        "arguments": [{"type": "column", "name": "a"}]
    });
    assert_eq!(
        render_expression(&least).unwrap(),
        r#"CASE WHEN "A" IS NULL THEN NULL ELSE least("A") END"#
    );
}

#[test]
fn renders_greatest_least_with_literal_null_argument() {
    let expr = json!({
        "type": "function_scalar",
        "name": "LEAST",
        "arguments": [
            {"type": "column", "name": "x"},
            {"type": "column", "name": "y"},
            {"type": "literal_null"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN "X" IS NULL OR "Y" IS NULL OR NULL IS NULL THEN NULL ELSE least("X", "Y", NULL) END"#
    );
}

#[test]
fn renders_greatest_least_nested_argument_once_referenced_twice() {
    let expr = json!({
        "type": "function_scalar",
        "name": "GREATEST",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "ABS",
                "arguments": [{"type": "column", "name": "y"}]
            },
            {"type": "column", "name": "z"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN abs("Y") IS NULL OR "Z" IS NULL THEN NULL ELSE greatest(abs("Y"), "Z") END"#
    );
}

#[test]
fn renders_nested_greatest_guard_referencing_the_inner_case_twice() {
    let expr = json!({
        "type": "function_scalar",
        "name": "GREATEST",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "GREATEST",
                "arguments": [
                    {"type": "column", "name": "a"},
                    {"type": "column", "name": "b"}
                ]
            },
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B") END IS NULL OR "C" IS NULL THEN NULL ELSE greatest(CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B") END, "C") END"#
    );
}

#[test]
fn renders_greatest_least_empty_argument_list_errors() {
    for name in ["GREATEST", "LEAST"] {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": []
        });
        assert!(
            render_expression(&expr).is_err(),
            "{name} empty args must raise"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} empty args must be None in safe mode"
        );
    }
}

#[test]
fn greatest_least_without_arguments_key_errors() {
    for name in ["GREATEST", "LEAST"] {
        let expr = json!({
            "type": "function_scalar",
            "name": name
        });
        let err = render_expression(&expr)
            .expect_err(&format!("{name} without arguments key must raise"));
        assert!(
            err.to_string().contains("missing 'arguments'"),
            "{name} error should mention missing 'arguments', got: {err}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} without arguments key must be None in safe mode"
        );
    }
}

#[test]
fn renders_nullifzero_zeroifnull() {
    let expr = json!({
        "type": "function_scalar",
        "name": "NULLIFZERO",
        "arguments": [{"type": "column", "name": "v"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"nullif("V", 0)"#);

    let expr2 = json!({
        "type": "function_scalar",
        "name": "ZEROIFNULL",
        "arguments": [{"type": "column", "name": "v"}]
    });
    assert_eq!(render_expression(&expr2).unwrap(), r#"coalesce("V", 0)"#);
}

/// Scenario: the NULLIF(MOD(id,5),0) group key renders, so the grouped-aggregate path handles it.
#[test]
fn renders_nullif_of_mod() {
    let expr = json!({
        "type": "function_scalar",
        "name": "NULLIF",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "MOD",
                "arguments": [
                    {"type": "column", "name": "id"},
                    {"type": "literal_exactnumeric", "value": "5"}
                ]
            },
            {"type": "literal_exactnumeric", "value": "0"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(("ID" % 5), 0)"#
    );
}

/// Scenario: Exasol's simple-CASE expansion of NULLIF(MOD(id,5),0) renders as a group key.
#[test]
fn renders_simple_case_from_nullif_expansion() {
    let mod_node = json!({
        "type": "function_scalar",
        "name": "MOD",
        "arguments": [
            {"type": "column", "name": "ID"},
            {"type": "literal_exactnumeric", "value": "5"}
        ]
    });
    let expr = json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "basis": mod_node,
        "arguments": [{"type": "literal_exactnumeric", "value": "0"}],
        "results": [
            {"type": "literal_null"},
            mod_node
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE ("ID" % 5) WHEN 0 THEN NULL ELSE ("ID" % 5) END)"#
    );
}

/// Scenario: a searched CASE (no `basis`) renders its WHEN arguments as predicates.
#[test]
fn renders_searched_case_without_basis() {
    let expr = json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {"type": "predicate_less",
             "left": {"type": "column", "name": "SCORE"},
             "right": {"type": "literal_exactnumeric", "value": "50"}}
        ],
        "results": [
            {"type": "literal_string", "value": "low"},
            {"type": "literal_string", "value": "high"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE WHEN ("SCORE" < 50) THEN 'low' ELSE 'high' END)"#
    );
}

/// Scenario: a CASE without ELSE has exactly one result per WHEN.
#[test]
fn renders_case_without_else() {
    let expr = json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "basis": {"type": "column", "name": "ID"},
        "arguments": [{"type": "literal_exactnumeric", "value": "1"}],
        "results": [{"type": "literal_string", "value": "one"}]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE "ID" WHEN 1 THEN 'one' END)"#
    );
}

#[test]
fn renders_extract() {
    let expr = json!({
        "type": "function_scalar_extract",
        "name": "EXTRACT",
        "toExtract": "YEAR",
        "arguments": [{"type": "column", "name": "ts"}]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('YEAR', "TS")"#
    );

    let expr2 = json!({
        "type": "function_scalar_extract",
        "name": "EXTRACT",
        "toExtract": "MONTH",
        "arguments": [{"type": "column", "name": "ts"}]
    });
    assert_eq!(
        render_expression(&expr2).unwrap(),
        r#"date_part('MONTH', "TS")"#
    );
}

#[test]
fn renders_extract_as_exasol_extract_from_in_exasol_dialect() {
    let expr = json!({
        "type": "function_scalar_extract",
        "name": "EXTRACT",
        "toExtract": "DAY",
        "arguments": [{"type": "column", "name": "ts"}]
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"EXTRACT(DAY FROM "TS")"#
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('DAY', "TS")"#
    );
}

#[test]
fn renders_year_month_day_extract() {
    let shortcuts = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"];
    for field in shortcuts {
        let expr = json!({
            "type": "function_scalar",
            "name": field,
            "arguments": [{"type": "column", "name": "ts"}]
        });
        let sql = render_expression(&expr).unwrap();
        assert_eq!(
            sql,
            format!(r#"date_part('{field}', "TS")"#),
            "failed for {field}"
        );
    }
}

#[test]
fn renders_date_trunc() {
    let expr = json!({
        "type": "function_scalar",
        "name": "DATE_TRUNC",
        "arguments": [
            {"type": "literal_string", "value": "month"},
            {"type": "column", "name": "ts"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_trunc('month', "TS")"#
    );
}

#[test]
fn now_family_falls_through() {
    // Pinned to the generic decline text: an arity-checking arm would also name the function.
    for name in [
        "CURRENT_DATE",
        "SYSDATE",
        "CURRENT_TIMESTAMP",
        "SYSTIMESTAMP",
    ] {
        let expr = json!({"type": "function_scalar", "name": name, "arguments": []});
        for (dialect, rendered) in [
            ("DataFusion", render_expression(&expr)),
            ("Exasol", render_expression_exasol(&expr)),
        ] {
            let err = rendered.unwrap_err().to_string();
            assert!(
                err.contains("unsupported scalar function"),
                "{name} must fall through the generic unsupported-scalar-function \
                 path in the {dialect} dialect: {err}"
            );
            assert!(
                err.contains(name),
                "the {dialect}-dialect error must name '{name}': {err}"
            );
        }
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} must be None in the DataFusion safe variant"
        );
        assert!(
            render_expression_exasol_safe(&expr).is_none(),
            "{name} must be None in the Exasol safe variant"
        );
    }
}

#[test]
fn renders_to_date_to_timestamp() {
    let expr = json!({
        "type": "function_scalar",
        "name": "TO_DATE",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"to_date("S")"#);

    let expr2 = json!({
        "type": "function_scalar",
        "name": "TO_DATE",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_string", "value": "%Y-%m-%d"}
        ]
    });
    assert_eq!(
        render_expression(&expr2).unwrap(),
        r#"to_date("S", '%Y-%m-%d')"#
    );

    let expr3 = json!({
        "type": "function_scalar",
        "name": "TO_TIMESTAMP",
        "arguments": [{"type": "column", "name": "s"}]
    });
    assert_eq!(render_expression(&expr3).unwrap(), r#"to_timestamp("S")"#);

    let expr4 = json!({
        "type": "function_scalar",
        "name": "TO_TIMESTAMP",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_string", "value": "%Y-%m-%d %H:%M:%S"}
        ]
    });
    assert_eq!(
        render_expression(&expr4).unwrap(),
        r#"to_timestamp("S", '%Y-%m-%d %H:%M:%S')"#
    );
}

#[test]
fn renders_week_as_iso_date_part() {
    let expr = json!({
        "type": "function_scalar",
        "name": "WEEK",
        "arguments": [{"type": "column", "name": "d"}]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('week', "D")"#
    );
}

#[test]
fn renders_week_at_year_boundary_dates() {
    // ISO-8601 boundary weeks, verified in DataFusion: 2021-01-01 -> 53, 2020-12-31 -> 53,
    // 2019-12-30 -> 1, 2023-01-01 -> 52.
    let boundary_dates = ["2021-01-01", "2020-12-31", "2019-12-30", "2023-01-01"];
    for date in boundary_dates {
        let expr = json!({
            "type": "function_scalar",
            "name": "WEEK",
            "arguments": [{"type": "literal_date", "value": date}]
        });
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!("date_part('week', DATE '{date}')"),
            "failed for boundary date {date}"
        );
    }
}

#[test]
fn week_with_wrong_arity_falls_back() {
    let expr = json!({
        "type": "function_scalar",
        "name": "WEEK",
        "arguments": [
            {"type": "column", "name": "d"},
            {"type": "column", "name": "e"}
        ]
    });
    assert!(render_expression(&expr).is_err());
    assert!(render_expression_safe(&expr).is_none());
}

#[test]
fn renders_days_between_as_date_difference() {
    let expr = json!({
        "type": "function_scalar",
        "name": "DAYS_BETWEEN",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST("A" AS DATE) - CAST("B" AS DATE))"#
    );
}

#[test]
fn renders_time_between_as_epoch_difference() {
    let hours = json!({
        "type": "function_scalar",
        "name": "HOURS_BETWEEN",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&hours).unwrap(),
        r#"((date_part('epoch', "A") - date_part('epoch', "B")) / 3600)"#
    );

    let minutes = json!({
        "type": "function_scalar",
        "name": "MINUTES_BETWEEN",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&minutes).unwrap(),
        r#"((date_part('epoch', "A") - date_part('epoch', "B")) / 60)"#
    );

    let seconds = json!({
        "type": "function_scalar",
        "name": "SECONDS_BETWEEN",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression(&seconds).unwrap(),
        r#"(date_part('epoch', "A") - date_part('epoch', "B"))"#
    );
}

#[test]
fn between_fns_reject_wrong_arity() {
    for name in [
        "DAYS_BETWEEN",
        "HOURS_BETWEEN",
        "MINUTES_BETWEEN",
        "SECONDS_BETWEEN",
    ] {
        let one_arg = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [{"type": "column", "name": "a"}]
        });
        assert!(
            render_expression(&one_arg).is_err(),
            "{name} 1-arg must raise"
        );
        assert!(
            render_expression_safe(&one_arg).is_none(),
            "{name} 1-arg must be None in safe mode"
        );

        let three_args = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"},
                {"type": "column", "name": "c"}
            ]
        });
        assert!(
            render_expression(&three_args).is_err(),
            "{name} 3-arg must raise"
        );
        assert!(
            render_expression_safe(&three_args).is_none(),
            "{name} 3-arg must be None in safe mode"
        );
    }
}

/// Scenario: a 2-argument `SECOND(ts, 3)` declines in DataFusion but renders verbatim in Exasol.
#[test]
fn second_with_precision_declines_for_datafusion_renders_for_exasol() {
    let expr = json!({
        "type": "function_scalar",
        "name": "SECOND",
        "arguments": [
            {"type": "column", "name": "ts"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });

    assert!(
        render_expression_safe(&expr).is_none(),
        "SECOND(ts, 3) must decline under the DataFusion dialect"
    );
    assert!(
        render_expression_exasol_safe(&expr).is_some(),
        "SECOND(ts, 3) must still render under the Exasol dialect"
    );
}

#[test]
fn div_falls_through_as_unsupported() {
    // DataFusion has no `div`, and a TRUNC(m/n) emulation diverges from Exasol on DOUBLE
    // division by zero (22012 vs infinity); operand types are not carried in the node.
    let expr = json!({
        "type": "function_scalar",
        "name": "DIV",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    let err = render_expression(&expr).unwrap_err();
    assert!(
        err.to_string().contains("DIV"),
        "error must name DIV as unsupported: {err}"
    );
    assert!(
        render_expression_safe(&expr).is_none(),
        "DIV must be None in safe mode without panicking"
    );
}

#[test]
fn to_char_and_to_number_fall_through_as_unsupported() {
    // DataFusion's `to_char` uses strftime masks, not Exasol format models, and it has no
    // `to_number`.
    let unsupported = ["TO_CHAR", "TO_NUMBER"];
    for name in unsupported {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_string", "value": "999.99"}
            ]
        });
        let err = render_expression(&expr).unwrap_err();
        assert!(
            err.to_string().contains(name),
            "error must name the unsupported function '{name}': {err}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} must be None in safe mode without panicking"
        );
    }
}

#[test]
fn regexp_scalar_functions_decline_in_both_dialects() {
    // The `regex` crate lacks backreferences, lookaround, and `regexp_substr`, and the
    // argument shapes differ from Exasol's (#106).
    let unsupported = [
        "REGEXP_REPLACE",
        "REGEXP_SUBSTR",
        "REGEXP_INSTR",
        "REGEXP_COUNT",
    ];
    for name in unsupported {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "s"},
                {"type": "literal_string", "value": "a+"}
            ]
        });
        let expected = format!("unsupported scalar function: {name}");
        assert_eq!(
            render_expression(&expr).unwrap_err().to_string(),
            expected,
            "the DataFusion dialect must decline {name}"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap_err().to_string(),
            expected,
            "the Exasol dialect must decline {name}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} must be None in the DataFusion safe variant"
        );
        assert!(
            render_expression_exasol_safe(&expr).is_none(),
            "{name} must be None in the Exasol safe variant"
        );
    }
}

#[test]
fn bitwise_operator_functions_fall_through() {
    // Exasol's bit functions use an unsigned 64-bit domain: DataFusion's bitwise operators
    // are signed (`>>` sign-extends) and the rest have no builtin (#108).
    let unsupported = [
        "BIT_AND",
        "BIT_OR",
        "BIT_XOR",
        "BIT_NOT",
        "BIT_LSHIFT",
        "BIT_RSHIFT",
        "BIT_LROTATE",
        "BIT_RROTATE",
        "BIT_CHECK",
        "BIT_SET",
        "BIT_TO_NUM",
    ];
    for name in unsupported {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        let err = render_expression(&expr).unwrap_err();
        let err_string = err.to_string();
        // Pinned to the generic text: an arity-checking arm would also name the function.
        assert!(
            err_string.contains("unsupported scalar function"),
            "{name} must fall through the generic unsupported-scalar-function path: {err}"
        );
        assert!(
            err_string.contains(name),
            "error must name the unsupported function '{name}': {err}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} must be None in safe mode without panicking"
        );
    }
}

#[test]
fn undeclared_scalar_function_declines_in_both_dialects() {
    // Beyond SUBSTRING/SOUNDEX, the rows pin the gate's edges: case-insensitive lookup,
    // declaration checked before `arguments`, and a missing `name`.
    let arg = json!([{"type": "column", "name": "a"}]);
    let cases = [
        (
            "SUBSTRING",
            json!({"type": "function_scalar", "name": "SUBSTRING", "arguments": arg.clone()}),
        ),
        (
            "SOUNDEX",
            json!({"type": "function_scalar", "name": "SOUNDEX", "arguments": arg.clone()}),
        ),
        (
            "SUBSTRING",
            json!({"type": "function_scalar", "name": "substring", "arguments": arg.clone()}),
        ),
        (
            "SUBSTRING",
            json!({"type": "function_scalar", "name": "SUBSTRING"}),
        ),
        ("", json!({"type": "function_scalar", "arguments": arg})),
    ];
    for (declined_name, expr) in cases {
        let expected = format!("unsupported scalar function: {declined_name}");
        assert_eq!(
            render_expression(&expr).unwrap_err().to_string(),
            expected,
            "DataFusion dialect must decline the undeclared node {expr}"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap_err().to_string(),
            expected,
            "Exasol dialect must decline the undeclared node {expr}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "DataFusion safe variant must be None for {expr}"
        );
        assert!(
            render_expression_exasol_safe(&expr).is_none(),
            "Exasol safe variant must be None for {expr}"
        );
    }
}

#[test]
fn regexp_scalar_exclusion_leaves_regexp_like_untouched() {
    let predicate = json!({
        "type": "predicate_like_regexp",
        "expression": {"type": "column", "name": "name"},
        "pattern": {"type": "literal_string", "value": "^A.*"}
    });
    assert_eq!(
        render_expression(&predicate).unwrap(),
        r#"regexp_like("NAME", '^A.*')"#
    );
    let scalar = json!({
        "type": "function_scalar",
        "name": "REGEXP_LIKE",
        "arguments": [
            {"type": "column", "name": "name"},
            {"type": "literal_string", "value": "^B.*"}
        ]
    });
    assert_eq!(
        render_expression(&scalar).unwrap(),
        r#"regexp_like("NAME", '^B.*')"#
    );
}

#[test]
fn unsupported_date_functions_decline_in_both_dialects() {
    // Every name exists in Exasol, so the Exasol-dialect assertion guards against the
    // verbatim rule re-admitting them.
    let unsupported = [
        "ADD_HOURS",
        "ADD_MINUTES",
        "ADD_DAYS",
        "ADD_SECONDS",
        "ADD_WEEKS",
        "ADD_MONTHS",
        "ADD_YEARS",
        "MONTHS_BETWEEN",
        "YEARS_BETWEEN",
        "DAYOFWEEK",
        "LAST_DAY",
        "CONVERT_TZ",
        "POSIX_TIME",
    ];
    for name in unsupported {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [{"type": "column", "name": "x"}]
        });
        let expected = format!("unsupported scalar function: {name}");
        assert_eq!(
            render_expression(&expr).unwrap_err().to_string(),
            expected,
            "the DataFusion dialect must decline {name}"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap_err().to_string(),
            expected,
            "the Exasol dialect must decline {name}"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "{name} must be None in the DataFusion safe variant"
        );
        assert!(
            render_expression_exasol_safe(&expr).is_none(),
            "{name} must be None in the Exasol safe variant"
        );
    }
}

#[test]
fn render_expression_renders_aggregate_nodes() {
    let sum = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{"type": "column", "name": "col"}],
        "distinct": false
    });
    assert_eq!(render_expression(&sum).unwrap(), r#"SUM("COL")"#);

    let count_star = json!({
        "type": "function_aggregate",
        "name": "COUNT",
        "arguments": [],
        "distinct": false
    });
    assert_eq!(render_expression(&count_star).unwrap(), "COUNT(*)");

    let count_distinct = json!({
        "type": "function_aggregate",
        "name": "COUNT",
        "arguments": [{"type": "column", "name": "col"}],
        "distinct": true
    });
    assert_eq!(
        render_expression(&count_distinct).unwrap(),
        r#"COUNT(DISTINCT "COL")"#
    );

    let avg = json!({
        "type": "function_aggregate",
        "name": "AVG",
        "arguments": [{"type": "column", "name": "col"}],
        "distinct": false
    });
    assert_eq!(render_expression(&avg).unwrap(), r#"AVG("COL")"#);

    let sum_qualified = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{"type": "column", "name": "amount", "tableAlias": "O"}],
        "distinct": false
    });
    assert_eq!(
        render_expression(&sum_qualified).unwrap(),
        r#"SUM("O"."AMOUNT")"#
    );
}

#[test]
fn render_expression_renders_scalar_wrapping_aggregates() {
    let sum_case = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{
            "type": "function_scalar",
            "name": "CASE",
            "arguments": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "l_returnflag"},
                 "right": {"type": "literal_string", "value": "R"}},
                {"type": "literal_exactnumeric", "value": 1},
                {"type": "literal_exactnumeric", "value": 0}
            ]
        }],
        "distinct": false
    });
    let count_star = json!({
        "type": "function_aggregate",
        "name": "COUNT",
        "arguments": [],
        "distinct": false
    });
    let round = json!({
        "type": "function_scalar",
        "name": "ROUND",
        "arguments": [
            {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                {"type": "function_scalar", "name": "MULT", "arguments": [
                    {"type": "literal_double", "value": 100.0},
                    sum_case
                ]},
                count_star
            ]},
            {"type": "literal_exactnumeric", "value": 2}
        ]
    });

    let sql = render_expression_safe(&round).expect("scalar-over-aggregate must render");
    assert!(
        sql.contains(r#"SUM(CASE WHEN ("L_RETURNFLAG" = 'R') THEN 1 ELSE 0 END)"#),
        "nested SUM(CASE ...) must be spliced verbatim: {sql}"
    );
    assert!(
        sql.contains("COUNT(*)"),
        "nested COUNT(*) must render as the star case: {sql}"
    );
}

#[test]
fn aggregate_with_unrenderable_argument_declines() {
    let bad = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [{"type": "totally_unknown_node"}],
        "distinct": false
    });
    assert!(
        render_expression(&bad).is_err(),
        "an unrenderable argument must raise in raising mode"
    );
    assert!(
        render_expression_safe(&bad).is_none(),
        "an unrenderable argument must be None in safe mode"
    );
}

#[test]
fn renders_cast_varchar_exasol_dialect_includes_length() {
    let expr = json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "VARCHAR", "size": 100}
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("X" AS VARCHAR(100))"#
    );
}

#[test]
fn renders_cast_char_exasol_dialect_includes_length() {
    let expr = json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "CHAR", "size": 3, "characterSet": "ASCII"}
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("X" AS CHAR(3) ASCII)"#
    );
}

/// Scenario: the same CHAR node renders bare `VARCHAR` in DataFusion and `CHAR(n)` in Exasol (#192).
#[test]
fn cast_char_target_diverges_between_dialects() {
    let expr = json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [{"type": "column", "name": "c_varchar"}],
        "dataType": {"type": "CHAR", "size": 20, "characterSet": "ASCII"}
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("C_VARCHAR" AS VARCHAR)"#
    );
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("C_VARCHAR" AS CHAR(20) ASCII)"#
    );
}

/// Scenario: a size-less VARCHAR target renders `VARCHAR(2000000)` in the Exasol dialect.
#[test]
fn renders_cast_varchar_exasol_dialect_without_size_falls_back() {
    let expr = json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "VARCHAR"}
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("X" AS VARCHAR(2000000))"#
    );
}

/// Scenario: a size-less CHAR target renders `VARCHAR(2000000)` rather than inventing a CHAR width.
#[test]
fn renders_cast_char_exasol_dialect_without_size_falls_back_to_varchar_default() {
    let expr = json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [{"type": "column", "name": "x"}],
        "dataType": {"type": "CHAR", "characterSet": "ASCII"}
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("X" AS VARCHAR(2000000))"#
    );
}

#[test]
fn renders_math_family_verbatim_in_exasol_dialect() {
    let one_arg = [
        ("ABS", "abs"),
        ("FLOOR", "floor"),
        ("CEIL", "ceil"),
        ("SQRT", "sqrt"),
        ("EXP", "exp"),
        ("LN", "ln"),
        ("SIGN", "signum"),
        ("DEGREES", "degrees"),
        ("RADIANS", "radians"),
        ("SIN", "sin"),
        ("COS", "cos"),
        ("TAN", "tan"),
        ("ASIN", "asin"),
        ("ACOS", "acos"),
        ("ATAN", "atan"),
        ("SINH", "sinh"),
        ("COSH", "cosh"),
        ("TANH", "tanh"),
        ("COT", "cot"),
    ];
    for (exasol_name, df_name) in one_arg {
        let expr = json!({
            "type": "function_scalar",
            "name": exasol_name,
            "arguments": [{"type": "column", "name": "x"}]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"{exasol_name}("X")"#),
            "the Exasol dialect must render {exasol_name} verbatim"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!(r#"{df_name}("X")"#),
            "the DataFusion dialect must stay unchanged for {exasol_name}"
        );
    }

    let two_arg = [
        ("ROUND", "round"),
        ("TRUNC", "trunc"),
        ("LOG", "log"),
        ("POWER", "power"),
        ("ATAN2", "atan2"),
    ];
    for (exasol_name, df_name) in two_arg {
        let expr = json!({
            "type": "function_scalar",
            "name": exasol_name,
            "arguments": [
                {"type": "column", "name": "v"},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"{exasol_name}("V", 2)"#),
            "the Exasol dialect must render {exasol_name} verbatim"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!(r#"{df_name}("V", 2)"#),
            "the DataFusion dialect must stay unchanged for {exasol_name}"
        );
    }
}

#[test]
fn renders_sign_as_native_sign_in_exasol_dialect() {
    let expr = json!({
        "type": "function_scalar",
        "name": "SIGN",
        "arguments": [{
            "type": "function_scalar",
            "name": "SUB",
            "arguments": [
                {"type": "function_aggregate", "name": "SUM",
                 "arguments": [{"type": "column", "name": "l_discount"}]},
                {"type": "literal_double", "value": 0.5}
            ]
        }]
    });
    let exasol = render_expression_exasol(&expr).unwrap();
    assert_eq!(exasol, r#"SIGN((SUM("L_DISCOUNT") - 0.5))"#);
    assert!(
        !exasol.contains("signum"),
        "the Exasol dialect must not emit DataFusion's signum: {exasol}"
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"signum((SUM("L_DISCOUNT") - 0.5))"#
    );
}

#[test]
fn renders_string_family_verbatim_in_exasol_dialect() {
    let one_arg = [
        ("LOWER", "lower"),
        ("UPPER", "upper"),
        ("TRIM", "trim"),
        ("LTRIM", "ltrim"),
        ("RTRIM", "rtrim"),
        ("REPLACE", "replace"),
        ("REPEAT", "repeat"),
        ("REVERSE", "reverse"),
        ("LPAD", "lpad"),
        ("RPAD", "rpad"),
        ("ASCII", "ascii"),
        ("CHR", "chr"),
        ("INITCAP", "initcap"),
        ("LEFT", "left"),
        ("RIGHT", "right"),
        ("TRANSLATE", "translate"),
        ("LENGTH", "character_length"),
        ("OCTET_LENGTH", "octet_length"),
        ("UNICODE", "ascii"),
        ("UNICODECHR", "chr"),
    ];
    for (exasol_name, df_name) in one_arg {
        let expr = json!({
            "type": "function_scalar",
            "name": exasol_name,
            "arguments": [{"type": "column", "name": "s"}]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"{exasol_name}("S")"#),
            "the Exasol dialect must render {exasol_name} verbatim"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!(r#"{df_name}("S")"#),
            "the DataFusion dialect must stay unchanged for {exasol_name}"
        );
    }

    let substr = json!({
        "type": "function_scalar",
        "name": "SUBSTR",
        "arguments": [
            {"type": "column", "name": "s"},
            {"type": "literal_exactnumeric", "value": 1},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(
        render_expression_exasol(&substr).unwrap(),
        r#"SUBSTR("S", 1, 3)"#
    );
    assert_eq!(render_expression(&substr).unwrap(), r#"substr("S", 1, 3)"#);
}

#[test]
fn renders_instr_locate_verbatim_with_start_arg_in_exasol_dialect() {
    // The DataFusion dialect's strpos drops a third (start) argument, a known limitation of
    // that rendering.
    let instr = json!({
        "type": "function_scalar",
        "name": "INSTR",
        "arguments": [
            {"type": "literal_string", "value": "hello"},
            {"type": "literal_string", "value": "l"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(
        render_expression_exasol(&instr).unwrap(),
        "INSTR('hello', 'l', 3)"
    );
    assert_eq!(render_expression(&instr).unwrap(), "strpos('hello', 'l')");

    let locate = json!({
        "type": "function_scalar",
        "name": "LOCATE",
        "arguments": [
            {"type": "literal_string", "value": "l"},
            {"type": "literal_string", "value": "hello"},
            {"type": "literal_exactnumeric", "value": 3}
        ]
    });
    assert_eq!(
        render_expression_exasol(&locate).unwrap(),
        "LOCATE('l', 'hello', 3)"
    );
    assert_eq!(render_expression(&locate).unwrap(), "strpos('hello', 'l')");
}

#[test]
fn renders_greatest_least_verbatim_in_exasol_dialect() {
    let greatest = json!({
        "type": "function_scalar",
        "name": "GREATEST",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"},
            {"type": "column", "name": "c"}
        ]
    });
    assert_eq!(
        render_expression_exasol(&greatest).unwrap(),
        r#"GREATEST("A", "B", "C")"#
    );
    assert_eq!(
        render_expression(&greatest).unwrap(),
        r#"CASE WHEN "A" IS NULL OR "B" IS NULL OR "C" IS NULL THEN NULL ELSE greatest("A", "B", "C") END"#
    );

    let least = json!({
        "type": "function_scalar",
        "name": "LEAST",
        "arguments": [
            {"type": "column", "name": "x"},
            {"type": "literal_exactnumeric", "value": 0}
        ]
    });
    assert_eq!(
        render_expression_exasol(&least).unwrap(),
        r#"LEAST("X", 0)"#
    );
    assert_eq!(
        render_expression(&least).unwrap(),
        r#"CASE WHEN "X" IS NULL OR 0 IS NULL THEN NULL ELSE least("X", 0) END"#
    );
}

#[test]
fn renders_nullifzero_zeroifnull_verbatim_in_exasol_dialect() {
    let nullifzero = json!({
        "type": "function_scalar",
        "name": "NULLIFZERO",
        "arguments": [{"type": "column", "name": "v"}]
    });
    assert_eq!(
        render_expression_exasol(&nullifzero).unwrap(),
        r#"NULLIFZERO("V")"#
    );
    assert_eq!(render_expression(&nullifzero).unwrap(), r#"nullif("V", 0)"#);

    let zeroifnull = json!({
        "type": "function_scalar",
        "name": "ZEROIFNULL",
        "arguments": [{"type": "column", "name": "v"}]
    });
    assert_eq!(
        render_expression_exasol(&zeroifnull).unwrap(),
        r#"ZEROIFNULL("V")"#
    );
    assert_eq!(
        render_expression(&zeroifnull).unwrap(),
        r#"coalesce("V", 0)"#
    );
}

#[test]
fn renders_nullif_verbatim_in_exasol_dialect() {
    // The verbatim gate renders arguments in its own dialect, so the nested MOD must stay
    // `MOD(a, b)`, not `%`.
    let expr = json!({
        "type": "function_scalar",
        "name": "NULLIF",
        "arguments": [
            {
                "type": "function_scalar",
                "name": "MOD",
                "arguments": [
                    {"type": "column", "name": "id"},
                    {"type": "literal_exactnumeric", "value": "5"}
                ]
            },
            {"type": "literal_exactnumeric", "value": "0"}
        ]
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"NULLIF(MOD("ID", 5), 0)"#
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(("ID" % 5), 0)"#
    );
}

#[test]
fn renders_date_field_shortcuts_verbatim_in_exasol_dialect() {
    for field in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"] {
        let expr = json!({
            "type": "function_scalar",
            "name": field,
            "arguments": [{"type": "column", "name": "ts"}]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"{field}("TS")"#),
            "the Exasol dialect must render {field} verbatim"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!(r#"date_part('{field}', "TS")"#),
            "the DataFusion dialect must stay unchanged for {field}"
        );
    }
}

#[test]
fn renders_week_as_native_week_in_exasol_dialect() {
    let expr = json!({
        "type": "function_scalar",
        "name": "WEEK",
        "arguments": [{"type": "column", "name": "d"}]
    });
    assert_eq!(render_expression_exasol(&expr).unwrap(), r#"WEEK("D")"#);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('week', "D")"#
    );
}

#[test]
fn renders_date_trunc_verbatim_in_exasol_dialect() {
    let expr = json!({
        "type": "function_scalar",
        "name": "DATE_TRUNC",
        "arguments": [
            {"type": "literal_string", "value": "month"},
            {"type": "column", "name": "ts"}
        ]
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"DATE_TRUNC('month', "TS")"#
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_trunc('month', "TS")"#
    );
}

#[test]
fn renders_to_date_to_timestamp_verbatim_in_exasol_dialect() {
    for (exasol_name, df_name) in [("TO_DATE", "to_date"), ("TO_TIMESTAMP", "to_timestamp")] {
        let bare = json!({
            "type": "function_scalar",
            "name": exasol_name,
            "arguments": [{"type": "column", "name": "s"}]
        });
        assert_eq!(
            render_expression_exasol(&bare).unwrap(),
            format!(r#"{exasol_name}("S")"#),
            "the Exasol dialect must render {exasol_name} verbatim"
        );
        assert_eq!(
            render_expression(&bare).unwrap(),
            format!(r#"{df_name}("S")"#),
            "the DataFusion dialect must stay unchanged for {exasol_name}"
        );

        let formatted = json!({
            "type": "function_scalar",
            "name": exasol_name,
            "arguments": [
                {"type": "column", "name": "s"},
                {"type": "literal_string", "value": "YYYY-MM-DD"}
            ]
        });
        assert_eq!(
            render_expression_exasol(&formatted).unwrap(),
            format!(r#"{exasol_name}("S", 'YYYY-MM-DD')"#),
            "the Exasol dialect must forward {exasol_name}'s format model verbatim"
        );
        assert_eq!(
            render_expression(&formatted).unwrap(),
            format!(r#"{df_name}("S", 'YYYY-MM-DD')"#),
            "the DataFusion dialect must stay unchanged for {exasol_name} with a format"
        );
    }
}

#[test]
fn renders_days_between_verbatim_in_exasol_dialect() {
    let expr = json!({
        "type": "function_scalar",
        "name": "DAYS_BETWEEN",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "column", "name": "b"}
        ]
    });
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"DAYS_BETWEEN("A", "B")"#
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST("A" AS DATE) - CAST("B" AS DATE))"#
    );
}

#[test]
fn renders_between_family_verbatim_in_exasol_dialect() {
    let df_epoch = r#"(date_part('epoch', "A") - date_part('epoch', "B"))"#;
    let cases = [
        ("HOURS_BETWEEN", format!("({df_epoch} / 3600)")),
        ("MINUTES_BETWEEN", format!("({df_epoch} / 60)")),
        ("SECONDS_BETWEEN", df_epoch.to_string()),
    ];
    for (name, df_expected) in cases {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "column", "name": "b"}
            ]
        });
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"{name}("A", "B")"#),
            "the Exasol dialect must render {name} verbatim"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            df_expected,
            "the DataFusion dialect must stay unchanged for {name}"
        );
    }
}

/// Scenario: every declared name has a fixture, and each `VerbatimCall` renders its derived `<NAME>(<args>)` form over dialect-invariant arguments.
#[test]
fn exasol_dialect_renders_declared_verbatim_surface() {
    struct ScalarFixture {
        name: &'static str,
        node: Json,
        shaped_exasol: Option<&'static str>,
    }

    fn col(name: &str) -> Json {
        json!({"type": "column", "name": name})
    }
    fn num(value: i64) -> Json {
        json!({"type": "literal_exactnumeric", "value": value})
    }
    fn text(value: &str) -> Json {
        json!({"type": "literal_string", "value": value})
    }
    fn scalar(name: &str, args: Vec<Json>) -> Json {
        json!({"type": "function_scalar", "name": name, "arguments": args})
    }
    fn verbatim(name: &'static str, args: Vec<Json>) -> ScalarFixture {
        ScalarFixture {
            name,
            node: scalar(name, args),
            shaped_exasol: None,
        }
    }
    fn shaped(name: &'static str, node: Json, exasol: &'static str) -> ScalarFixture {
        ScalarFixture {
            name,
            node,
            shaped_exasol: Some(exasol),
        }
    }

    let mut fixtures: Vec<ScalarFixture> = Vec::new();

    for name in [
        "ABS", "FLOOR", "CEIL", "SQRT", "EXP", "LN", "SIGN", "DEGREES", "RADIANS", "SIN", "COS",
        "TAN", "ASIN", "ACOS", "ATAN", "SINH", "COSH", "TANH", "COT",
    ] {
        fixtures.push(verbatim(name, vec![col("x")]));
    }
    for name in ["ROUND", "TRUNC", "LOG", "POWER", "ATAN2"] {
        fixtures.push(verbatim(name, vec![col("v"), num(2)]));
    }

    for name in [
        "LOWER",
        "UPPER",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "REVERSE",
        "ASCII",
        "INITCAP",
        "LENGTH",
        "OCTET_LENGTH",
        "UNICODE",
    ] {
        fixtures.push(verbatim(name, vec![col("s")]));
    }
    for name in ["CHR", "UNICODECHR"] {
        fixtures.push(verbatim(name, vec![num(65)]));
    }
    for name in ["SUBSTR", "LEFT", "RIGHT", "REPEAT", "LPAD", "RPAD"] {
        fixtures.push(verbatim(name, vec![col("s"), num(3)]));
    }
    fixtures.push(verbatim("REPLACE", vec![col("s"), text("a")]));
    fixtures.push(verbatim(
        "TRANSLATE",
        vec![col("s"), text("ab"), text("xy")],
    ));
    fixtures.push(verbatim("INSTR", vec![col("s"), text("a")]));
    fixtures.push(verbatim("LOCATE", vec![text("a"), col("s")]));

    for name in ["GREATEST", "LEAST", "NULLIF"] {
        fixtures.push(verbatim(name, vec![col("a"), col("b")]));
    }
    for name in ["NULLIFZERO", "ZEROIFNULL"] {
        fixtures.push(verbatim(name, vec![col("v")]));
    }

    for name in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "WEEK"] {
        fixtures.push(verbatim(name, vec![col("ts")]));
    }
    fixtures.push(verbatim("DATE_TRUNC", vec![text("month"), col("ts")]));
    fixtures.push(verbatim("TO_DATE", vec![col("s"), text("YYYY-MM-DD")]));
    fixtures.push(verbatim(
        "TO_TIMESTAMP",
        vec![col("s"), text("YYYY-MM-DD HH24:MI:SS")],
    ));

    for name in [
        "DAYS_BETWEEN",
        "HOURS_BETWEEN",
        "MINUTES_BETWEEN",
        "SECONDS_BETWEEN",
    ] {
        fixtures.push(verbatim(name, vec![col("a"), col("b")]));
    }

    fixtures.extend([
        shaped(
            "ADD",
            scalar("ADD", vec![col("a"), col("b")]),
            r#"("A" + "B")"#,
        ),
        shaped(
            "SUB",
            scalar("SUB", vec![col("a"), col("b")]),
            r#"("A" - "B")"#,
        ),
        shaped(
            "MULT",
            scalar("MULT", vec![col("a"), col("b")]),
            r#"("A" * "B")"#,
        ),
        shaped(
            "FLOAT_DIV",
            scalar("FLOAT_DIV", vec![col("a"), col("b")]),
            r#"("A" / "B")"#,
        ),
        shaped("NEG", scalar("NEG", vec![col("a")]), r#"(-"A")"#),
        shaped(
            "MOD",
            scalar("MOD", vec![col("a"), col("b")]),
            r#"MOD("A", "B")"#,
        ),
        shaped(
            "CONCAT",
            scalar("CONCAT", vec![col("a"), col("b")]),
            r#"("A" || "B")"#,
        ),
        shaped(
            "CAST",
            json!({
                "type": "function_scalar", "name": "CAST",
                "arguments": [col("v")],
                "dataType": {"type": "VARCHAR", "size": 50}
            }),
            r#"CAST("V" AS VARCHAR(50))"#,
        ),
        shaped(
            "REGEXP_LIKE",
            scalar("REGEXP_LIKE", vec![col("s"), text("^a")]),
            r#"("S" REGEXP_LIKE '^a')"#,
        ),
        shaped(
            "CASE",
            scalar(
                "CASE",
                vec![
                    json!({"type": "predicate_greater", "left": col("x"), "right": num(0)}),
                    num(1),
                    num(0),
                ],
            ),
            r#"CASE WHEN ("X" > 0) THEN 1 ELSE 0 END"#,
        ),
    ]);

    let missing: Vec<&str> = TRANSLATED_SCALAR_FNS
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| !fixtures.iter().any(|f| f.name == *name))
        .collect();
    assert!(
        missing.is_empty(),
        "every name declared in TRANSLATED_SCALAR_FNS needs a sweep fixture; missing: \
         {missing:?}"
    );
    let undeclared: Vec<&str> = fixtures
        .iter()
        .map(|f| f.name)
        .filter(|name| declared_scalar_fn(name).is_none())
        .collect();
    assert!(
        undeclared.is_empty(),
        "every sweep fixture must name a declared function; undeclared: {undeclared:?}"
    );
    assert_eq!(
        fixtures.len(),
        TRANSLATED_SCALAR_FNS.len(),
        "the fixture map and the declaration must line up one to one; a duplicated row on \
         either side is the only way both subset checks above can pass at different sizes"
    );

    let mut swept: Vec<String> = Vec::new();

    for (declared_name, form) in TRANSLATED_SCALAR_FNS {
        let fixture = fixtures
            .iter()
            .find(|f| f.name == *declared_name)
            .expect("fixture completeness is asserted above");
        // The only call that reaches the per-name arms: Exasol `VerbatimCall`s return
        // ahead of them.
        if let Err(err) = render_expression(&fixture.node) {
            panic!(
                "{declared_name} is declared in TRANSLATED_SCALAR_FNS, which gates BOTH \
                 dialects from one lookup, so it MUST render in the DataFusion dialect too; \
                 it declined with {err:?}. A declared name that has lost its per-name arm \
                 still renders in the Exasol dialect through the verbatim gate, so this is \
                 the only assertion that catches it."
            );
        }
        let rendered = render_expression_exasol(&fixture.node)
            .unwrap_or_else(|err| panic!("{declared_name} failed to render: {err:?}"));
        match form {
            ExasolForm::VerbatimCall => {
                assert!(
                    fixture.shaped_exasol.is_none(),
                    "{declared_name} is declared VerbatimCall, so its expectation MUST be \
                     derived from the node, never hand-written"
                );
                let node_name = fixture
                    .node
                    .get("name")
                    .and_then(|n| n.as_str())
                    .expect("fixture node carries a name")
                    .to_uppercase();
                let rendered_args: Vec<String> = fixture
                    .node
                    .get("arguments")
                    .and_then(|a| a.as_array())
                    .expect("fixture node carries arguments")
                    .iter()
                    .map(|arg| render_expression_exasol(arg).expect("argument renders"))
                    .collect();
                assert_eq!(
                    rendered,
                    format!("{node_name}({})", rendered_args.join(", ")),
                    "the Exasol dialect must re-emit {declared_name} as the call Exasol sent"
                );
            }
            ExasolForm::Shaped => {
                let expected = fixture.shaped_exasol.unwrap_or_else(|| {
                    panic!(
                        "{declared_name} is declared Shaped, so its fixture MUST declare the \
                         expected Exasol string"
                    )
                });
                assert_eq!(
                    rendered, expected,
                    "{declared_name} is outside the <NAME>(<args>) shape and must render its \
                     own declared form"
                );
            }
        }
        swept.push(rendered);
    }

    let node_type_rows = [
        (
            json!({
                "type": "function_scalar_extract", "name": "EXTRACT",
                "toExtract": "YEAR", "arguments": [col("ts")]
            }),
            r#"EXTRACT(YEAR FROM "TS")"#,
            r#"date_part('YEAR', "TS")"#,
        ),
        (
            json!({
                "type": "function_scalar_cast", "name": "CAST",
                "arguments": [col("v")],
                "dataType": {"type": "VARCHAR", "size": 50}
            }),
            r#"CAST("V" AS VARCHAR(50))"#,
            r#"CAST("V" AS VARCHAR)"#,
        ),
        (
            json!({
                "type": "predicate_like_regexp",
                "expression": col("s"), "pattern": text("^a")
            }),
            r#"("S" REGEXP_LIKE '^a')"#,
            r#"regexp_like("S", '^a')"#,
        ),
        (
            json!({"type": "literal_timestamp", "value": "2024-03-01 12:34:56.789"}),
            "TIMESTAMP '2024-03-01 12:34:56.789'",
            "arrow_cast('2024-03-01 12:34:56.789', 'Timestamp(Microsecond, None)')",
        ),
        (
            json!({"type": "literal_timestamp_utc", "value": "2024-03-01 12:34:56.789"}),
            "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 12:34:56.789', 'UTC', SESSIONTIMEZONE) \
             AS TIMESTAMP WITH LOCAL TIME ZONE)",
            r#"arrow_cast('2024-03-01 12:34:56.789+00:00', 'Timestamp(Microsecond, Some("UTC"))')"#,
        ),
    ];
    for (node, expected_exasol, expected_datafusion) in node_type_rows {
        let node_type = node["type"]
            .as_str()
            .expect("row carries a node type")
            .to_string();
        let rendered = render_expression_exasol(&node)
            .unwrap_or_else(|err| panic!("{node_type} failed to render: {err:?}"));
        assert_eq!(
            rendered, expected_exasol,
            "{node_type} in the Exasol dialect"
        );
        assert_eq!(
            render_expression(&node).unwrap(),
            expected_datafusion,
            "{node_type} in the DataFusion dialect"
        );
        swept.push(rendered);
    }

    // Case-sensitive on purpose: uppercase names are correct Exasol; only the lowercase
    // DataFusion twins must not appear.
    for rendered in &swept {
        for token in [
            "signum",
            "date_part",
            "strpos",
            "arrow_cast",
            "character_length",
            "octet_length",
            "regexp_like(",
            "current_date()",
            "now()",
            "nullif(",
            "coalesce(",
            CHECKED_FLOAT_DIV_FN,
        ] {
            assert!(
                !rendered.contains(token),
                "Exasol-dialect output must not contain the DataFusion-only token `{token}`, \
                 but rendered: {rendered}"
            );
        }
    }
}

#[test]
fn arithmetic_operators_render_identically_in_both_dialects() {
    let binary = [("ADD", "+"), ("SUB", "-"), ("MULT", "*")];
    for (name, op) in binary {
        let expr = json!({
            "type": "function_scalar",
            "name": name,
            "arguments": [
                {"type": "column", "name": "a"},
                {"type": "literal_exactnumeric", "value": 1}
            ]
        });
        let expected = format!(r#"("A" {op} 1)"#);
        assert_eq!(
            render_expression(&expr).unwrap(),
            expected,
            "{name} DataFusion dialect"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            expected,
            "{name} Exasol dialect"
        );
    }

    let neg = json!({
        "type": "function_scalar",
        "name": "NEG",
        "arguments": [{"type": "column", "name": "a"}]
    });
    let expected_neg = r#"(-"A")"#;
    assert_eq!(
        render_expression(&neg).unwrap(),
        expected_neg,
        "NEG DataFusion dialect"
    );
    assert_eq!(
        render_expression_exasol(&neg).unwrap(),
        expected_neg,
        "NEG Exasol dialect"
    );
}

/// Scenario: FLOAT_DIV renders the checked-division call only in the DataFusion dialect.
#[test]
fn float_div_renders_checked_division_call_only_in_the_datafusion_dialect() {
    let float_div = json!({
        "type": "function_scalar",
        "name": "FLOAT_DIV",
        "arguments": [
            {"type": "column", "name": "a"},
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });
    assert_eq!(
        render_expression(&float_div).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}("A", 1)"#),
        "FLOAT_DIV DataFusion dialect"
    );
    assert_eq!(
        render_expression_exasol(&float_div).unwrap(),
        r#"("A" / 1)"#,
        "FLOAT_DIV Exasol dialect"
    );
}

#[test]
fn non_timestamp_literals_render_identically_in_both_dialects() {
    let cases: [(Json, &str); 7] = [
        (json!({"type": "literal_null"}), "NULL"),
        (json!({"type": "literal_bool", "value": true}), "TRUE"),
        (json!({"type": "literal_bool", "value": false}), "FALSE"),
        (
            json!({"type": "literal_string", "value": "it's"}),
            "'it''s'",
        ),
        (json!({"type": "literal_exactnumeric", "value": 42}), "42"),
        (json!({"type": "literal_double", "value": 0.5}), "0.5"),
        (
            json!({"type": "literal_date", "value": "2024-01-15"}),
            "DATE '2024-01-15'",
        ),
    ];
    for (node, expected) in cases {
        let node_type = node["type"].as_str().unwrap();
        assert_eq!(
            render_expression(&node).unwrap(),
            expected,
            "{node_type} DataFusion dialect"
        );
        assert_eq!(
            render_expression_exasol(&node).unwrap(),
            expected,
            "{node_type} Exasol dialect"
        );
    }
}

#[test]
fn exasol_df_filter_suppresses_trivially_true() {
    let true_filter = json!({"type": "literal_bool", "value": true});
    assert!(render_df_filter_exasol_safe(&true_filter).is_none());

    let null_filter = json!({"type": "literal_null"});
    assert!(render_df_filter_exasol_safe(&null_filter).is_none());
}

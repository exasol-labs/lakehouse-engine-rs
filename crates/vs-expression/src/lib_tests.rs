use super::*;
use crate::test_support::*;
use serde_json::json;

/// Scenario: Bare column reference translates to quoted identifier
#[test]
fn renders_column_as_quoted_uppercase_ident() {
    for (name, expected) in [("region", r#""REGION""#), (r#"my"col"#, r#""MY""COL""#)] {
        assert_eq!(
            render_expression(&col(name)).unwrap(),
            expected,
            "column {name}"
        );
    }
}

/// Scenario: Bare column reference translates to quoted identifier
#[test]
fn renders_table_qualified_column_when_alias_present() {
    let expr = json!({"type": "column", "name": "id", "tableAlias": "LHS_FACT"});
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""LHS_FACT"."ID""#);
}

#[test]
fn empty_table_alias_falls_back_to_bare_column() {
    let sql = render_expression(&alias_col("", "id")).unwrap();
    assert_eq!(sql, r#""ID""#);
}

#[test]
fn renders_timestamp_literal() {
    for (value, expected) in [
        (
            "2024-01-15 12:00:00",
            "arrow_cast('2024-01-15 12:00:00', 'Timestamp(Microsecond, None)')",
        ),
        (
            "9999-12-31 23:59:59",
            "arrow_cast('9999-12-31 23:59:59', 'Timestamp(Microsecond, None)')",
        ),
    ] {
        assert_eq!(
            render_expression(&timestamp_lit(value)).unwrap(),
            expected,
            "literal {value}"
        );
    }
}

#[test]
fn timestamp_literal_with_sub_microsecond_digits_renders_nanoseconds() {
    for (value, unit) in [
        ("2025-06-01 00:00:00.123456789", "Nanosecond"),
        ("2025-06-01 00:00:00.1234567", "Nanosecond"),
        ("2025-06-01 00:00:00.123456000", "Microsecond"),
        ("2025-06-01 00:00:00.123456", "Microsecond"),
    ] {
        assert_eq!(
            render_expression(&timestamp_lit(value)).unwrap(),
            format!("arrow_cast('{value}', 'Timestamp({unit}, None)')"),
            "literal {value}"
        );
    }
}

/// Scenario: Comparison predicates translate to binary operator expressions
#[test]
fn renders_simple_equality() {
    let expr = predicate_equal(col("id"), num_lit(10));
    assert_eq!(render_expression(&expr).unwrap(), r#"("ID" = 10)"#);
}

/// Scenario: Logical connectives translate to AND/OR/NOT with parentheses
#[test]
fn renders_and_predicate() {
    let expr = json!({
        "type": "predicate_and",
        "expressions": [
            predicate_greater(col("age"), num_lit(18)),
            predicate_less(col("age"), num_lit(65))
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("AND"), "AND not found in: {sql}");
}

/// Scenario: Logical connectives translate to AND/OR/NOT with parentheses
#[test]
fn renders_or_predicate() {
    let expr = json!({
        "type": "predicate_or",
        "expressions": [
            predicate_equal(col("status"), str_lit("A")),
            predicate_equal(col("status"), str_lit("B"))
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("OR"), "OR not found in: {sql}");
}

/// Scenario: Logical connectives translate to AND/OR/NOT with parentheses
#[test]
fn renders_not_predicate() {
    let expr = json!({
        "type": "predicate_not",
        "expression": predicate_equal(col("active"), bool_lit(true))
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("NOT"), "NOT not found in: {sql}");
}

/// Scenario: Logical connectives translate to AND/OR/NOT with parentheses
#[test]
fn renders_empty_junction_as_its_identity() {
    for (expr, expected) in [(predicate_and([]), "TRUE"), (predicate_or([]), "FALSE")] {
        assert_eq!(render_expression(&expr).unwrap(), expected, "{expr}");
    }
}

/// Scenario: IS NULL and IS NOT NULL predicates translate correctly
#[test]
fn renders_is_null() {
    let expr = json!({"type": "predicate_is_null", "expression": col("x")});
    assert_eq!(render_expression(&expr).unwrap(), r#"("X" IS NULL)"#);
}

/// Scenario: IS NULL and IS NOT NULL predicates translate correctly
#[test]
fn renders_is_not_null() {
    let expr = json!({"type": "predicate_is_not_null", "expression": col("x")});
    assert_eq!(render_expression(&expr).unwrap(), r#"("X" IS NOT NULL)"#);
}

/// Scenario: IN constant list translates to SQL IN expression
#[test]
fn renders_in_constlist() {
    let expr = json!({
        "type": "predicate_in_constlist",
        "expression": col("status"),
        "arguments": [
            str_lit("A"),
            str_lit("B")
        ]
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("IN"), "IN not found: {sql}");
    assert!(sql.contains("'A'"), "'A' not found: {sql}");
    assert!(sql.contains("'B'"), "'B' not found: {sql}");
}

/// Scenario: IN constant list translates to SQL IN expression
#[test]
fn renders_empty_in_as_false() {
    let expr = predicate_in(col("x"), []);
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

/// Scenario: IN constant list translates to SQL IN expression
#[test]
fn renders_in_constlist_strips_null() {
    let expr = predicate_in(
        col("status"),
        [str_lit("A"), null_lit(), date_lit(Json::Null)],
    );
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("'A'"), "'A' not found: {sql}");
    assert!(!sql.contains("NULL"), "NULL should not survive: {sql}");
}

/// Scenario: IN constant list translates to SQL IN expression
#[test]
fn renders_all_null_in_as_false() {
    let expr = predicate_in(col("x"), [null_lit(), date_lit(Json::Null)]);
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

/// Scenario: IN constant list translates to SQL IN expression
#[test]
fn renders_not_in_constlist_strips_null() {
    let expr = predicate_not(predicate_in(
        col("status"),
        [str_lit("A"), null_lit(), date_lit(Json::Null)],
    ));
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#"(NOT ("STATUS" IN ('A')))"#);
}

/// Scenario: BETWEEN predicate translates correctly
#[test]
fn renders_between() {
    let expr = json!({
        "type": "predicate_between",
        "expression": col("age"),
        "left": num_lit(18),
        "right": num_lit(65)
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("BETWEEN"), "BETWEEN not found: {sql}");
    assert!(sql.contains("18"), "low bound not found: {sql}");
    assert!(sql.contains("65"), "high bound not found: {sql}");
}

/// Scenario: LIKE predicate translates with optional escape character
#[test]
fn renders_like_without_escape() {
    let expr = predicate_like(col("name"), str_lit("A%"));
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("LIKE"), "LIKE not found: {sql}");
    assert!(!sql.contains("ESCAPE"), "ESCAPE should be absent: {sql}");
}

/// Scenario: LIKE predicate translates with optional escape character
#[test]
fn renders_like_with_escape() {
    let expr = json!({
        "type": "predicate_like",
        "expression": col("name"),
        "pattern": str_lit("A!%"),
        "escape_char": "!"
    });
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("LIKE"), "LIKE not found: {sql}");
    assert!(sql.contains("ESCAPE"), "ESCAPE not found: {sql}");
    assert!(sql.contains("'!'"), "escape char not found: {sql}");
}

#[test]
fn renders_binary_arithmetic_operators() {
    for (name, operand, expected) in [
        ("ADD", 1, r#"("A" + 1)"#),
        ("SUB", 1, r#"("A" - 1)"#),
        ("MULT", 2, r#"("A" * 2)"#),
    ] {
        let expr = scalar_fn(name, [col("a"), num_lit(operand)]);
        assert_eq!(render_expression(&expr).unwrap(), expected, "{name}");
    }
}

#[test]
fn legacy_mul_name_is_not_recognized() {
    let expr = scalar_fn("MUL", [col("a"), col("b")]);
    assert!(
        render_expression_safe(&expr).is_none(),
        "the obsolete \"MUL\" node name must not translate; Exasol emits \"MULT\""
    );
}

/// Scenario: Arithmetic operators translate to binary SQL expressions
#[test]
fn renders_two_column_arithmetic_product() {
    let expr = json!({
        "type": "function_scalar",
        "name": "MULT",
        "arguments": [
            col("l_extendedprice"),
            col("l_discount")
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#
    );
}

/// Scenario: Arithmetic operators translate to binary SQL expressions
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
        let expr = scalar_fn(node, [col("l_extendedprice"), col("l_discount")]);
        assert_eq!(
            render_expression(&expr).unwrap(),
            expected,
            "translator must render advertised capability {cap} (node {node})"
        );
    }
}

#[test]
fn float_div_calls_checked_division_for_every_operand_kind() {
    let a_plus_b = || scalar_fn("ADD", [col("a"), col("b")]);
    let sum_amount = || aggregate("SUM", [col("amount")]);
    let cases = [
        ("column over literal", col("a"), num_lit(2), r#"("A", 2)"#),
        ("column over column", col("a"), col("b"), r#"("A", "B")"#),
        ("literal over column", num_lit(10), col("b"), r#"(10, "B")"#),
        ("literal over literal", num_lit(10), num_lit(4), "(10, 4)"),
        (
            "nested expression over column",
            a_plus_b(),
            col("c"),
            r#"(("A" + "B"), "C")"#,
        ),
        (
            "nested expression over literal",
            a_plus_b(),
            num_lit(2),
            r#"(("A" + "B"), 2)"#,
        ),
        (
            "aggregate over column",
            sum_amount(),
            col("c"),
            r#"(SUM("AMOUNT"), "C")"#,
        ),
        (
            "aggregate over literal",
            sum_amount(),
            num_lit(2),
            r#"(SUM("AMOUNT"), 2)"#,
        ),
        ("NULL over column", null_lit(), col("b"), r#"(NULL, "B")"#),
        ("column over NULL", col("a"), null_lit(), r#"("A", NULL)"#),
        ("NULL over zero", null_lit(), num_lit(0), "(NULL, 0)"),
    ];
    for (operands, left, right, arguments) in cases {
        assert_eq!(
            render_expression(&scalar_fn("FLOAT_DIV", [left, right])).unwrap(),
            format!("{CHECKED_FLOAT_DIV_FN}{arguments}"),
            "{operands}"
        );
    }
}

#[test]
fn renders_arithmetic_neg() {
    let expr = scalar_fn("NEG", [col("a")]);
    assert_eq!(render_expression(&expr).unwrap(), r#"(-"A")"#);
}

/// Scenario: Arithmetic operators translate to binary SQL expressions
#[test]
fn neg_composes_with_aggregate_decomposition() {
    let sum_neg = aggregate("SUM", [scalar_fn("NEG", [col("col")])]);
    assert_eq!(render_expression(&sum_neg).unwrap(), r#"SUM((-"COL"))"#);
}

#[test]
fn renders_cast_varchar() {
    let expr = json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [col("x")],
        "dataType": {"type": "VARCHAR", "size": 100}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
}

#[test]
fn renders_cast_targets_in_the_datafusion_dialect() {
    for (target, expected) in [
        (decimal_type(10, 2), r#"CAST("X" AS DECIMAL(10,2))"#),
        (data_type("DOUBLE"), r#"CAST("X" AS DOUBLE)"#),
        (data_type("DATE"), r#"CAST("X" AS DATE)"#),
        (char_type(3), r#"CAST("X" AS VARCHAR)"#),
        (data_type("BOOLEAN"), r#"CAST("X" AS BOOLEAN)"#),
    ] {
        let label = target.to_string();
        assert_eq!(
            render_expression(&cast(col("x"), target)).unwrap(),
            expected,
            "CAST to {label}"
        );
    }
}

#[test]
fn renders_cast_bool_to_varchar_as_exasol_case_for_any_predicate_source() {
    for (source, expected) in [
        (
            predicate_greater(col("c_acctbal"), num_lit(0)),
            r#"(CASE ("C_ACCTBAL" > 0) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)"#,
        ),
        (
            predicate_is_null(col("x")),
            r#"(CASE ("X" IS NULL) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END)"#,
        ),
    ] {
        let expr = cast(source, varchar_type(10));
        assert_eq!(render_expression(&expr).unwrap(), expected);
    }
}

#[test]
fn renders_cast_timestamp_without_local_time_zone() {
    let expr = cast(
        col("x"),
        json!({"type": "TIMESTAMP", "withLocalTimeZone": false, "fractionalSecondsPrecision": 3}),
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("X" AS TIMESTAMP(3))"#
    );
}

#[test]
fn renders_cast_timestamp_precision_per_dialect() {
    for p in 0u64..=9 {
        let expr = cast(col("x"), timestamp_type(p));
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            format!(r#"CAST("X" AS TIMESTAMP({p}))"#),
            "Exasol dialect must render TIMESTAMP({p}) verbatim"
        );
    }

    for p in [0u64, 3, 6, 9] {
        let expr = cast(col("x"), timestamp_type(p));
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
        let expr = cast(col("x"), timestamp_type(p));
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

    for target in [
        data_type("TIMESTAMP"),
        json!({"type": "TIMESTAMP", "withLocalTimeZone": false}),
    ] {
        let expr = cast(col("x"), target.clone());
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP)"#,
            "DataFusion dialect: absent precision must render bare TIMESTAMP for {target}"
        );
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP)"#,
            "Exasol dialect: absent precision must render bare TIMESTAMP for {target}"
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
    for target in unsupported {
        let expr = cast(col("x"), target.clone());
        assert!(
            render_expression(&expr).is_err(),
            "CAST to {target} must raise in raising mode"
        );
        assert!(
            render_expression_safe(&expr).is_none(),
            "CAST to {target} must be None in safe mode"
        );
    }
}

#[test]
fn renders_cast_nested_function_scalar_defensive() {
    let expr = json!({
        "type": "function_scalar",
        "name": "CAST",
        "arguments": [col("x")],
        "dataType": {"type": "VARCHAR", "size": 100}
    });
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
}

/// Scenario: Decimal-to-VARCHAR node renders Exasol-trimmed string
#[test]
fn renders_decimal_to_varchar_exasol() {
    let expr = json!({
        "type": "decimal_to_varchar_exasol",
        "arguments": [col("c_decimal_a")]
    });
    let expected = format_decimal_exasol_style(r#""C_DECIMAL_A""#);
    assert_eq!(render_expression(&expr).unwrap(), expected);
    assert_eq!(render_expression_safe(&expr).unwrap(), expected);
}

/// Scenario: Decimal-to-VARCHAR node renders Exasol-trimmed string
#[test]
fn decimal_to_varchar_exasol_wrong_arity_errors() {
    let no_args = decimal_to_varchar([]);
    let two_args = decimal_to_varchar([col("c_decimal_a"), col("c_decimal_b")]);
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

/// Scenario: An unsupported expression node fails a required translation with an error naming its type
#[test]
fn unsupported_node_returns_error() {
    let expr = json!({"type": "fn_sum", "operands": []});
    let err = render_expression(&expr).unwrap_err();
    assert!(
        err.to_string().contains("fn_sum"),
        "error must name the unsupported type: {err}"
    );
}

/// Scenario: An unsupported expression node declines translation without failing
#[test]
fn unsupported_node_returns_none_in_safe_mode() {
    let expr = json!({"type": "fn_sum", "operands": []});
    assert!(render_expression_safe(&expr).is_none());
}

/// Scenario: A trivially-true filter is omitted from the pushed-down scan
#[test]
fn trivially_true_filter_returns_none_in_safe_mode_in_both_dialects() {
    for filter in [bool_lit(true), null_lit()] {
        assert!(render_df_filter_safe(&filter).is_none(), "{filter}");
        assert!(render_df_filter_exasol_safe(&filter).is_none(), "{filter}");
    }
}

#[test]
fn renders_timestamp_utc_literal() {
    let expr = timestamp_utc_lit("2024-03-01 10:00:00");
    let sql = render_expression(&expr).unwrap();
    assert_eq!(
        sql,
        "arrow_cast('2024-03-01 10:00:00+00:00', 'Timestamp(Microsecond, Some(\"UTC\"))')"
    );
}

/// Scenario: Plain timestamp literals render as bare Exasol TIMESTAMP literals in the Exasol dialect
#[test]
fn renders_timestamp_literals_as_bare_timestamp_in_exasol_dialect() {
    let ts = timestamp_lit("2024-01-15 12:00:00");
    let ts_utc = timestamp_utc_lit("2024-03-01 10:00:00");

    let ts_exasol = render_expression_exasol(&ts).unwrap();
    assert_eq!(ts_exasol, "TIMESTAMP '2024-01-15 12:00:00'");
    assert!(
        !ts_exasol.contains("arrow_cast"),
        "Exasol rejects arrow_cast with sqlCode 42000: {ts_exasol}"
    );

    assert_eq!(
        render_expression_exasol(&timestamp_lit("2024-01-15 12:00:00' OR '1'='1")).unwrap(),
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
    let ts_utc = timestamp_utc_lit("2024-03-01 10:00:00");

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
    let real_wire_name = lit("literal_timestamputc", "2024-03-01 09:00:00");
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
        for node in [lit(node_type, Json::Null), json!({"type": node_type})] {
            assert_eq!(
                render_expression_exasol(&node).unwrap(),
                "NULL",
                "{node_type} with a null/absent value must render bare NULL"
            );
        }
    }
}

/// Scenario: Plain timestamp literals render as bare Exasol TIMESTAMP literals in the Exasol dialect
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
            ("carrying a JSON-null value", lit(node_type, Json::Null)),
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

/// Scenario: Math scalar functions translate to DataFusion math calls
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
        let sql = render_expression(&scalar_fn(exasol, [col("x")])).unwrap();
        assert_eq!(sql, format!(r#"{df}("X")"#), "failed for {exasol}");
    }

    for (expr, expected) in [
        (
            scalar_fn("POWER", [col("x"), num_lit(2)]),
            r#"power("X", 2)"#,
        ),
        (
            scalar_fn("ATAN2", [col("y"), col("x")]),
            r#"atan2("Y", "X")"#,
        ),
        (
            scalar_fn("ROUND", [col("v"), num_lit(2)]),
            r#"round("V", 2)"#,
        ),
        (scalar_fn("TRUNC", [col("v")]), r#"trunc("V")"#),
    ] {
        assert_eq!(render_expression(&expr).unwrap(), expected, "{expr}");
    }

    let expr = scalar_fn("ABS", [col("x"), col("y")]);
    assert!(render_expression_safe(&expr).is_none());
}

#[test]
fn renders_mod_as_operator() {
    let expr = scalar_fn("MOD", [col("a"), num_lit(3)]);
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" % 3)"#);
}

/// Scenario: MOD translates to the modulo operator
#[test]
fn renders_mod_as_function_call_in_exasol_dialect() {
    let expr = scalar_fn("MOD", [col("a"), num_lit(3)]);
    assert_eq!(render_expression_exasol(&expr).unwrap(), r#"MOD("A", 3)"#);
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" % 3)"#);
}

/// Scenario: String scalar functions translate to DataFusion string calls
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
        let sql = render_expression(&scalar_fn(name, [col("s")])).unwrap();
        assert_eq!(
            sql,
            format!(r#"{}("S")"#, name.to_lowercase()),
            "failed for {name}"
        );
    }

    for (expr, expected) in [
        (scalar_fn("LENGTH", [col("s")]), r#"character_length("S")"#),
        (
            scalar_fn("OCTET_LENGTH", [col("s")]),
            r#"octet_length("S")"#,
        ),
        (scalar_fn("UNICODE", [col("s")]), r#"ascii("S")"#),
        (scalar_fn("UNICODECHR", [col("n")]), r#"chr("N")"#),
        (
            scalar_fn("SUBSTR", [col("s"), num_lit(1), num_lit(3)]),
            r#"substr("S", 1, 3)"#,
        ),
        (
            scalar_fn("INSTR", [str_lit("hello"), str_lit("ll")]),
            "strpos('hello', 'll')",
        ),
        (
            scalar_fn("LOCATE", [str_lit("ll"), str_lit("hello")]),
            "strpos('hello', 'll')",
        ),
    ] {
        assert_eq!(render_expression(&expr).unwrap(), expected, "{expr}");
    }
}

/// Scenario: CONCAT translates to a NULL-skipping DataFusion concat call
#[test]
fn renders_concat_as_nullif_wrapped_concat_call() {
    for (expr, expected) in [
        (
            scalar_fn("CONCAT", [col("s"), str_lit("")]),
            r#"nullif(concat("S", ''), '')"#,
        ),
        (
            scalar_fn("CONCAT", [col("a"), col("b"), col("c")]),
            r#"nullif(concat("A", "B", "C"), '')"#,
        ),
    ] {
        assert_eq!(render_expression(&expr).unwrap(), expected, "{expr}");
    }
}

/// Scenario: CONCAT translates to a NULL-skipping DataFusion concat call
#[test]
fn renders_concat_bool_operand_as_exasol_case() {
    let expr = scalar_fn(
        "CONCAT",
        [predicate_equal(col("active"), bool_lit(true)), str_lit("")],
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat((CASE ("ACTIVE" = TRUE) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END), ''), '')"#
    );
}

/// Scenario: CONCAT translates to a NULL-skipping DataFusion concat call
#[test]
fn renders_concat_as_chained_pipe_operator_in_exasol_dialect() {
    for (expr, expected) in [
        (
            scalar_fn("CONCAT", [col("s"), str_lit("")]),
            r#"("S" || '')"#,
        ),
        (
            scalar_fn("CONCAT", [col("a"), col("b"), col("c")]),
            r#"("A" || "B" || "C")"#,
        ),
        (
            scalar_fn(
                "CONCAT",
                [predicate_equal(col("active"), bool_lit(true)), str_lit("")],
            ),
            r#"((CASE ("ACTIVE" = TRUE) WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END) || '')"#,
        ),
    ] {
        assert_eq!(render_expression_exasol(&expr).unwrap(), expected, "{expr}");
    }
}

/// Scenario: CONCAT translates to a NULL-skipping DataFusion concat call
#[test]
fn renders_nested_concat_wrapper_per_level() {
    let expr = scalar_fn(
        "CONCAT",
        [scalar_fn("CONCAT", [col("a"), col("b")]), col("c")],
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat(nullif(concat("A", "B"), ''), "C"), '')"#
    );
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"(("A" || "B") || "C")"#
    );
}

/// Scenario: CONCAT translates to a NULL-skipping DataFusion concat call
#[test]
fn renders_concat_single_argument() {
    let expr = scalar_fn("CONCAT", [col("s")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat("S"), '')"#
    );
    assert_eq!(render_expression_exasol(&expr).unwrap(), r#"("S")"#);
}

/// Scenario: CONCAT translates to a NULL-skipping DataFusion concat call
#[test]
fn concat_empty_argument_list_errors_in_both_dialects() {
    let expr = scalar_fn("CONCAT", []);
    let expected = "function_scalar CONCAT requires at least 1 argument, got 0";
    assert_eq!(render_expression(&expr).unwrap_err().to_string(), expected);
    assert_eq!(
        render_expression_exasol(&expr).unwrap_err().to_string(),
        expected
    );
    assert!(render_expression_safe(&expr).is_none());
    assert!(render_expression_exasol_safe(&expr).is_none());
}

/// Scenario: CONCAT translates to a NULL-skipping DataFusion concat call
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

    let expr = scalar_fn("CONCAT", [col("a"), Json::Null]);
    let expected = "CONCAT argument rendered to null";
    assert_eq!(render_expression(&expr).unwrap_err().to_string(), expected);
    assert_eq!(
        render_expression_exasol(&expr).unwrap_err().to_string(),
        expected
    );
    assert!(render_expression_safe(&expr).is_none());
    assert!(render_expression_exasol_safe(&expr).is_none());
}

/// Scenario: CASE expression translates to SQL CASE WHEN
#[test]
fn renders_case_when() {
    for (expr, expected) in [
        (
            scalar_fn(
                "CASE",
                [predicate_equal(col("status"), str_lit("A")), num_lit(1)],
            ),
            r#"CASE WHEN ("STATUS" = 'A') THEN 1 END"#,
        ),
        (
            scalar_fn(
                "CASE",
                [
                    predicate_equal(col("x"), num_lit(1)),
                    str_lit("one"),
                    predicate_equal(col("x"), num_lit(2)),
                    str_lit("two"),
                    str_lit("other"),
                ],
            ),
            r#"CASE WHEN ("X" = 1) THEN 'one' WHEN ("X" = 2) THEN 'two' ELSE 'other' END"#,
        ),
    ] {
        assert_eq!(render_expression(&expr).unwrap(), expected, "{expr}");
    }

    assert!(render_expression_safe(&scalar_fn("CASE", [])).is_none());
}

/// Scenario: GREATEST and LEAST translate to DataFusion greatest/least
#[test]
fn renders_greatest_least_behind_a_null_guard() {
    let cases = [
        (
            "three columns",
            scalar_fn("GREATEST", [col("a"), col("b"), col("c")]),
            r#"CASE WHEN "A" IS NULL OR "B" IS NULL OR "C" IS NULL THEN NULL ELSE greatest("A", "B", "C") END"#,
        ),
        (
            "a column and a literal",
            scalar_fn("LEAST", [col("x"), num_lit(0)]),
            r#"CASE WHEN "X" IS NULL OR 0 IS NULL THEN NULL ELSE least("X", 0) END"#,
        ),
        (
            "a single argument",
            scalar_fn("GREATEST", [col("a")]),
            r#"CASE WHEN "A" IS NULL THEN NULL ELSE greatest("A") END"#,
        ),
        (
            "a single argument",
            scalar_fn("LEAST", [col("a")]),
            r#"CASE WHEN "A" IS NULL THEN NULL ELSE least("A") END"#,
        ),
        (
            "a literal NULL argument",
            scalar_fn("LEAST", [col("x"), col("y"), null_lit()]),
            r#"CASE WHEN "X" IS NULL OR "Y" IS NULL OR NULL IS NULL THEN NULL ELSE least("X", "Y", NULL) END"#,
        ),
        (
            "a nested argument referenced twice",
            scalar_fn("GREATEST", [scalar_fn("ABS", [col("y")]), col("z")]),
            r#"CASE WHEN abs("Y") IS NULL OR "Z" IS NULL THEN NULL ELSE greatest(abs("Y"), "Z") END"#,
        ),
        (
            "a nested GREATEST whose guard is referenced twice",
            scalar_fn(
                "GREATEST",
                [scalar_fn("GREATEST", [col("a"), col("b")]), col("c")],
            ),
            r#"CASE WHEN CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B") END IS NULL OR "C" IS NULL THEN NULL ELSE greatest(CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B") END, "C") END"#,
        ),
    ];
    for (arguments, expr, expected) in cases {
        assert_eq!(
            render_expression(&expr).unwrap(),
            expected,
            "{arguments}: {expr}"
        );
    }
}

/// Scenario: GREATEST and LEAST translate to DataFusion greatest/least
#[test]
fn renders_greatest_least_empty_argument_list_errors() {
    for name in ["GREATEST", "LEAST"] {
        let expr = scalar_fn(name, []);
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

/// Scenario: GREATEST and LEAST translate to DataFusion greatest/least
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

/// Scenario: NULLIFZERO and ZEROIFNULL translate to NULLIF and COALESCE
#[test]
fn renders_nullifzero_zeroifnull() {
    for (name, expected) in [
        ("NULLIFZERO", r#"nullif("V", 0)"#),
        ("ZEROIFNULL", r#"coalesce("V", 0)"#),
    ] {
        let expr = scalar_fn(name, [col("v")]);
        assert_eq!(render_expression(&expr).unwrap(), expected, "{name}");
    }
}

/// Scenario: NULLIF translates to the DataFusion nullif call
#[test]
fn renders_nullif_of_mod() {
    let expr = scalar_fn(
        "NULLIF",
        [scalar_fn("MOD", [col("id"), num_lit("5")]), num_lit("0")],
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(("ID" % 5), 0)"#
    );
}

#[test]
fn renders_simple_case_from_nullif_expansion() {
    let mod_node = scalar_fn("MOD", [col("ID"), num_lit("5")]);
    let expr = json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "basis": mod_node,
        "arguments": [num_lit("0")],
        "results": [
            null_lit(),
            mod_node
        ]
    });
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE ("ID" % 5) WHEN 0 THEN NULL ELSE ("ID" % 5) END)"#
    );
}

#[test]
fn renders_searched_case_without_basis() {
    let expr = searched_case(
        [predicate_less(col("SCORE"), num_lit("50"))],
        [str_lit("low"), str_lit("high")],
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE WHEN ("SCORE" < 50) THEN 'low' ELSE 'high' END)"#
    );
}

#[test]
fn renders_case_without_else() {
    let expr = simple_case(col("ID"), [num_lit("1")], [str_lit("one")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE "ID" WHEN 1 THEN 'one' END)"#
    );
}

/// Scenario: EXTRACT translates to the DataFusion date_part call
#[test]
fn renders_extract() {
    for (field, expected) in [
        ("YEAR", r#"date_part('YEAR', "TS")"#),
        ("MONTH", r#"date_part('MONTH', "TS")"#),
    ] {
        let expr = extract(field, col("ts"));
        assert_eq!(render_expression(&expr).unwrap(), expected, "{field}");
    }
}

/// Scenario: EXTRACT renders Exasol's EXTRACT FROM form in the Exasol dialect
#[test]
fn renders_extract_as_exasol_extract_from_in_exasol_dialect() {
    let expr = json!({
        "type": "function_scalar_extract",
        "name": "EXTRACT",
        "toExtract": "DAY",
        "arguments": [col("ts")]
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

/// Scenario: Field-shortcut date functions translate to date_part of the matching field
#[test]
fn renders_year_month_day_extract() {
    let shortcuts = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"];
    for field in shortcuts {
        let sql = render_expression(&scalar_fn(field, [col("ts")])).unwrap();
        assert_eq!(
            sql,
            format!(r#"date_part('{field}', "TS")"#),
            "failed for {field}"
        );
    }
}

/// Scenario: DATE_TRUNC translates to the DataFusion date_trunc call
#[test]
fn renders_date_trunc() {
    let expr = scalar_fn("DATE_TRUNC", [str_lit("month"), col("ts")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_trunc('month', "TS")"#
    );
}

/// Scenario: The now-family is not translated in either dialect
#[test]
fn now_family_falls_through() {
    // Pinned to the generic decline text: an arity-checking arm would also name the function.
    for name in [
        "CURRENT_DATE",
        "SYSDATE",
        "CURRENT_TIMESTAMP",
        "SYSTIMESTAMP",
    ] {
        let expr = scalar_fn(name, []);
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
    for (expr, expected) in [
        (scalar_fn("TO_DATE", [col("s")]), r#"to_date("S")"#),
        (
            scalar_fn("TO_DATE", [col("s"), str_lit("YYYY-MM-DD")]),
            r#"to_date(nullif("S", ''), '%Y-%m-%d')"#,
        ),
        (
            scalar_fn("TO_TIMESTAMP", [col("s")]),
            r#"to_timestamp("S")"#,
        ),
        (
            scalar_fn("TO_TIMESTAMP", [col("s"), str_lit("YYYY-MM-DD HH24:MI:SS")]),
            r#"to_timestamp(nullif("S", ''), '%Y-%m-%d %H:%M:%S')"#,
        ),
    ] {
        assert_eq!(render_expression(&expr).unwrap(), expected, "{expr}");
    }
}

#[test]
fn renders_week_as_iso_date_part() {
    let expr = scalar_fn("WEEK", [col("d")]);
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
        let expr = scalar_fn("WEEK", [date_lit(date)]);
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!("date_part('week', DATE '{date}')"),
            "failed for boundary date {date}"
        );
    }
}

#[test]
fn week_with_wrong_arity_falls_back() {
    let expr = scalar_fn("WEEK", [col("d"), col("e")]);
    assert!(render_expression(&expr).is_err());
    assert!(render_expression_safe(&expr).is_none());
}

#[test]
fn renders_days_between_as_date_difference() {
    let expr = scalar_fn("DAYS_BETWEEN", [col("a"), col("b")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST("A" AS DATE) - CAST("B" AS DATE))"#
    );
}

#[test]
fn renders_time_between_as_epoch_difference() {
    for (name, expected) in [
        (
            "HOURS_BETWEEN",
            r#"((date_part('epoch', "A") - date_part('epoch', "B")) / 3600)"#,
        ),
        (
            "MINUTES_BETWEEN",
            r#"((date_part('epoch', "A") - date_part('epoch', "B")) / 60)"#,
        ),
        (
            "SECONDS_BETWEEN",
            r#"(date_part('epoch', "A") - date_part('epoch', "B"))"#,
        ),
    ] {
        let expr = scalar_fn(name, [col("a"), col("b")]);
        assert_eq!(render_expression(&expr).unwrap(), expected, "{name}");
    }
}

#[test]
fn between_fns_reject_wrong_arity() {
    for name in [
        "DAYS_BETWEEN",
        "HOURS_BETWEEN",
        "MINUTES_BETWEEN",
        "SECONDS_BETWEEN",
    ] {
        let one_arg = scalar_fn(name, [col("a")]);
        assert!(
            render_expression(&one_arg).is_err(),
            "{name} 1-arg must raise"
        );
        assert!(
            render_expression_safe(&one_arg).is_none(),
            "{name} 1-arg must be None in safe mode"
        );

        let three_args = scalar_fn(name, [col("a"), col("b"), col("c")]);
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

#[test]
fn second_with_precision_declines_for_datafusion_renders_for_exasol() {
    let expr = scalar_fn("SECOND", [col("ts"), num_lit(3)]);

    assert!(
        render_expression_safe(&expr).is_none(),
        "SECOND(ts, 3) must decline under the DataFusion dialect"
    );
    assert!(
        render_expression_exasol_safe(&expr).is_some(),
        "SECOND(ts, 3) must still render under the Exasol dialect"
    );
}

/// Scenario: Integer division DIV is deliberately not translated
#[test]
fn div_falls_through_as_unsupported() {
    // DataFusion has no `div`, and a TRUNC(m/n) emulation diverges from Exasol on DOUBLE
    // division by zero (22012 vs infinity); operand types are not carried in the node.
    let expr = scalar_fn("DIV", [col("a"), col("b")]);
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

/// Scenario: Conversion format functions TO_CHAR and TO_NUMBER are deliberately not translated
#[test]
fn to_char_and_to_number_fall_through_as_unsupported() {
    // DataFusion's `to_char` uses strftime masks, not Exasol format models, and it has no
    // `to_number`.
    let unsupported = ["TO_CHAR", "TO_NUMBER"];
    for name in unsupported {
        let expr = scalar_fn(name, [col("a"), str_lit("999.99")]);
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
    // DataFusion's regexp_like matches anywhere in the value, Exasol's PCRE only
    // when the match spans the whole value; `regex` also lacks look-around.
    let unsupported = [
        "REGEXP_LIKE",
        "REGEXP_REPLACE",
        "REGEXP_SUBSTR",
        "REGEXP_INSTR",
        "REGEXP_COUNT",
    ];
    for name in unsupported {
        let expr = scalar_fn(name, [col("s"), str_lit("a+")]);
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
fn regexp_like_predicate_declines_in_both_dialects() {
    let expr = json!({
        "type": "predicate_like_regexp",
        "expression": col("name"),
        "pattern": str_lit("^A.*")
    });
    let expected = "unsupported expression node type: predicate_like_regexp";
    assert_eq!(render_expression(&expr).unwrap_err().to_string(), expected);
    assert_eq!(
        render_expression_exasol(&expr).unwrap_err().to_string(),
        expected
    );
    assert!(render_expression_safe(&expr).is_none());
    assert!(render_df_filter_safe(&expr).is_none());
    assert!(render_expression_exasol_safe(&expr).is_none());
    assert!(render_df_filter_exasol_safe(&expr).is_none());
}

/// Scenario: Bitwise operator functions are deliberately not translated
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
        let expr = scalar_fn(name, [col("a"), col("b")]);
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

/// Scenario: An undeclared scalar function name is not translated in either dialect
#[test]
fn undeclared_scalar_function_declines_in_both_dialects() {
    // Beyond SUBSTRING/SOUNDEX, the rows pin the gate's edges: case-insensitive lookup,
    // declaration checked before `arguments`, and a missing `name`.
    let cases = [
        ("SUBSTRING", scalar_fn("SUBSTRING", [col("a")])),
        ("SOUNDEX", scalar_fn("SOUNDEX", [col("a")])),
        ("SUBSTRING", scalar_fn("substring", [col("a")])),
        (
            "SUBSTRING",
            json!({"type": "function_scalar", "name": "SUBSTRING"}),
        ),
        (
            "",
            json!({"type": "function_scalar", "arguments": [col("a")]}),
        ),
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

/// Scenario: Unsupported date functions fall through as unsupported nodes
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
        let expr = scalar_fn(name, [col("x")]);
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

/// Scenario: Aggregate function nodes render with the aggregate name spliced verbatim
#[test]
fn render_expression_renders_aggregate_nodes() {
    let sum = json!({
        "type": "function_aggregate",
        "name": "SUM",
        "arguments": [col("col")],
        "distinct": false
    });
    assert_eq!(render_expression(&sum).unwrap(), r#"SUM("COL")"#);

    for (expr, expected) in [
        (aggregate("COUNT", []), "COUNT(*)"),
        (
            distinct_aggregate("COUNT", [col("col")]),
            r#"COUNT(DISTINCT "COL")"#,
        ),
        (aggregate("AVG", [col("col")]), r#"AVG("COL")"#),
        (
            aggregate("SUM", [alias_col("O", "amount")]),
            r#"SUM("O"."AMOUNT")"#,
        ),
    ] {
        assert_eq!(render_expression(&expr).unwrap(), expected, "{expr}");
    }
}

/// Scenario: Aggregate function nodes render with the aggregate name spliced verbatim
#[test]
fn render_expression_renders_scalar_wrapping_aggregates() {
    let sum_case = aggregate(
        "SUM",
        [scalar_fn(
            "CASE",
            [
                predicate_equal(col("l_returnflag"), str_lit("R")),
                num_lit(1),
                num_lit(0),
            ],
        )],
    );
    let count_star = aggregate("COUNT", []);
    let round = scalar_fn(
        "ROUND",
        [
            scalar_fn(
                "FLOAT_DIV",
                [scalar_fn("MULT", [double_lit(100.0), sum_case]), count_star],
            ),
            num_lit(2),
        ],
    );

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

/// Scenario: Aggregate function nodes render with the aggregate name spliced verbatim
#[test]
fn aggregate_with_unrenderable_argument_declines() {
    let bad = aggregate("SUM", [json!({"type": "totally_unknown_node"})]);
    assert!(
        render_expression(&bad).is_err(),
        "an unrenderable argument must raise in raising mode"
    );
    assert!(
        render_expression_safe(&bad).is_none(),
        "an unrenderable argument must be None in safe mode"
    );
}

/// Scenario: The Exasol dialect renders a CHAR CAST target as CHAR, not VARCHAR
#[test]
fn renders_cast_targets_in_the_exasol_dialect() {
    for (target, expected) in [
        (varchar_type(100), r#"CAST("X" AS VARCHAR(100))"#),
        (char_type(3), r#"CAST("X" AS CHAR(3) ASCII)"#),
        (data_type("VARCHAR"), r#"CAST("X" AS VARCHAR(2000000))"#),
        (
            json!({"type": "CHAR", "characterSet": "ASCII"}),
            r#"CAST("X" AS VARCHAR(2000000))"#,
        ),
    ] {
        let label = target.to_string();
        assert_eq!(
            render_expression_exasol(&cast(col("x"), target)).unwrap(),
            expected,
            "CAST to {label}"
        );
    }
}

/// Scenario: The Exasol dialect renders a CHAR CAST target as CHAR, not VARCHAR
#[test]
fn cast_char_target_diverges_between_dialects() {
    let expr = cast(col("c_varchar"), char_type(20));
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("C_VARCHAR" AS VARCHAR)"#
    );
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"CAST("C_VARCHAR" AS CHAR(20) ASCII)"#
    );
}

/// Scenario: Math scalar functions render verbatim in the Exasol dialect
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
        let expr = scalar_fn(exasol_name, [col("x")]);
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
        let expr = scalar_fn(exasol_name, [col("v"), num_lit(2)]);
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

/// Scenario: Math scalar functions render verbatim in the Exasol dialect
#[test]
fn renders_sign_as_native_sign_in_exasol_dialect() {
    let expr = scalar_fn(
        "SIGN",
        [scalar_fn(
            "SUB",
            [aggregate("SUM", [col("l_discount")]), double_lit(0.5)],
        )],
    );
    let exasol = render_expression_exasol(&expr).unwrap();
    assert_eq!(exasol, r#"SIGN((SUM("L_DISCOUNT") - 0.5))"#);
    assert!(
        !exasol.contains("signum"),
        "the Exasol dialect must not emit DataFusion's signum: {exasol}"
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"signum((SUM("L_DISCOUNT") - CAST('0.5' AS DOUBLE)))"#
    );
}

/// Scenario: String scalar functions render verbatim in the Exasol dialect
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
        let expr = scalar_fn(exasol_name, [col("s")]);
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

    let substr = scalar_fn("SUBSTR", [col("s"), num_lit(1), num_lit(3)]);
    assert_eq!(
        render_expression_exasol(&substr).unwrap(),
        r#"SUBSTR("S", 1, 3)"#
    );
    assert_eq!(render_expression(&substr).unwrap(), r#"substr("S", 1, 3)"#);
}

/// Scenario: String scalar functions render verbatim in the Exasol dialect
#[test]
fn renders_instr_locate_verbatim_with_start_arg_in_exasol_dialect() {
    // The DataFusion dialect's strpos drops a third (start) argument, a known limitation of
    // that rendering.
    for (expr, expected_exasol) in [
        (
            scalar_fn("INSTR", [str_lit("hello"), str_lit("l"), num_lit(3)]),
            "INSTR('hello', 'l', 3)",
        ),
        (
            scalar_fn("LOCATE", [str_lit("l"), str_lit("hello"), num_lit(3)]),
            "LOCATE('l', 'hello', 3)",
        ),
    ] {
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            expected_exasol,
            "{expr}"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            "strpos('hello', 'l')",
            "{expr}"
        );
    }
}

/// Scenario: GREATEST and LEAST translate to DataFusion greatest/least
#[test]
fn renders_greatest_least_verbatim_in_exasol_dialect() {
    for (expr, expected_exasol, expected_datafusion) in [
        (
            scalar_fn("GREATEST", [col("a"), col("b"), col("c")]),
            r#"GREATEST("A", "B", "C")"#,
            r#"CASE WHEN "A" IS NULL OR "B" IS NULL OR "C" IS NULL THEN NULL ELSE greatest("A", "B", "C") END"#,
        ),
        (
            scalar_fn("LEAST", [col("x"), num_lit(0)]),
            r#"LEAST("X", 0)"#,
            r#"CASE WHEN "X" IS NULL OR 0 IS NULL THEN NULL ELSE least("X", 0) END"#,
        ),
    ] {
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            expected_exasol,
            "{expr}"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            expected_datafusion,
            "{expr}"
        );
    }
}

/// Scenario: NULLIFZERO and ZEROIFNULL translate to NULLIF and COALESCE
#[test]
fn renders_nullifzero_zeroifnull_verbatim_in_exasol_dialect() {
    for (name, expected_exasol, expected_datafusion) in [
        ("NULLIFZERO", r#"NULLIFZERO("V")"#, r#"nullif("V", 0)"#),
        ("ZEROIFNULL", r#"ZEROIFNULL("V")"#, r#"coalesce("V", 0)"#),
    ] {
        let expr = scalar_fn(name, [col("v")]);
        assert_eq!(
            render_expression_exasol(&expr).unwrap(),
            expected_exasol,
            "{name}"
        );
        assert_eq!(
            render_expression(&expr).unwrap(),
            expected_datafusion,
            "{name}"
        );
    }
}

/// Scenario: NULLIF translates to the DataFusion nullif call
#[test]
fn renders_nullif_verbatim_in_exasol_dialect() {
    // The verbatim gate renders arguments in its own dialect, so the nested MOD must stay
    // `MOD(a, b)`, not `%`.
    let expr = scalar_fn(
        "NULLIF",
        [scalar_fn("MOD", [col("id"), num_lit("5")]), num_lit("0")],
    );
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"NULLIF(MOD("ID", 5), 0)"#
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(("ID" % 5), 0)"#
    );
}

/// Scenario: Field-shortcut date functions render verbatim in the Exasol dialect
#[test]
fn renders_date_field_shortcuts_verbatim_in_exasol_dialect() {
    for field in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"] {
        let expr = scalar_fn(field, [col("ts")]);
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
    let expr = scalar_fn("WEEK", [col("d")]);
    assert_eq!(render_expression_exasol(&expr).unwrap(), r#"WEEK("D")"#);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('week', "D")"#
    );
}

/// Scenario: DATE_TRUNC translates to the DataFusion date_trunc call
#[test]
fn renders_date_trunc_verbatim_in_exasol_dialect() {
    let expr = scalar_fn("DATE_TRUNC", [str_lit("month"), col("ts")]);
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
        let bare = scalar_fn(exasol_name, [col("s")]);
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

        let formatted = scalar_fn(exasol_name, [col("s"), str_lit("YYYY-MM-DD")]);
        assert_eq!(
            render_expression_exasol(&formatted).unwrap(),
            format!(r#"{exasol_name}("S", 'YYYY-MM-DD')"#),
            "the Exasol dialect must forward {exasol_name}'s format model verbatim"
        );
        assert_eq!(
            render_expression(&formatted).unwrap(),
            format!(r#"{df_name}(nullif("S", ''), '%Y-%m-%d')"#),
            "the DataFusion dialect must translate the format of {exasol_name}"
        );
    }
}

/// Scenario: A format the translation cannot express declines in the DataFusion dialect
#[test]
fn to_date_to_timestamp_decline_untranslatable_format() {
    let cases = [
        ("TO_DATE", vec![col("s"), col("f")], r#"TO_DATE("S", "F")"#),
        (
            "TO_TIMESTAMP",
            vec![col("s"), null_lit()],
            r#"TO_TIMESTAMP("S", NULL)"#,
        ),
        (
            "TO_DATE",
            vec![col("s"), str_lit("DD-MON-YYYY")],
            r#"TO_DATE("S", 'DD-MON-YYYY')"#,
        ),
        (
            "TO_DATE",
            vec![col("s"), str_lit("YYYY-MM-DD HH24:MI")],
            r#"TO_DATE("S", 'YYYY-MM-DD HH24:MI')"#,
        ),
        (
            "TO_TIMESTAMP",
            vec![col("s"), str_lit("YYYY-MM-DD"), str_lit("extra")],
            r#"TO_TIMESTAMP("S", 'YYYY-MM-DD', 'extra')"#,
        ),
        ("TO_DATE", vec![], "TO_DATE()"),
    ];
    for (name, arguments, exasol_sql) in cases {
        let expr = scalar_fn(name, arguments);
        assert!(render_expression(&expr).is_err(), "{exasol_sql} must err");
        assert_eq!(render_expression_safe(&expr), None, "{exasol_sql}");
        assert_eq!(
            render_expression_exasol(&expr).as_deref().ok(),
            Some(exasol_sql)
        );
    }
}

#[test]
fn renders_days_between_verbatim_in_exasol_dialect() {
    let expr = scalar_fn("DAYS_BETWEEN", [col("a"), col("b")]);
    assert_eq!(
        render_expression_exasol(&expr).unwrap(),
        r#"DAYS_BETWEEN("A", "B")"#
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CAST("A" AS DATE) - CAST("B" AS DATE))"#
    );
}

/// Scenario: Date-difference functions render verbatim in the Exasol dialect
#[test]
fn renders_between_family_verbatim_in_exasol_dialect() {
    let df_epoch = r#"(date_part('epoch', "A") - date_part('epoch', "B"))"#;
    let cases = [
        ("HOURS_BETWEEN", format!("({df_epoch} / 3600)")),
        ("MINUTES_BETWEEN", format!("({df_epoch} / 60)")),
        ("SECONDS_BETWEEN", df_epoch.to_string()),
    ];
    for (name, df_expected) in cases {
        let expr = scalar_fn(name, [col("a"), col("b")]);
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

/// Scenario: FLOAT_DIV stays outside the verbatim rule in both dialects
/// Scenario: The Exasol dialect never contains a DataFusion-only function name
#[test]
fn exasol_dialect_renders_declared_verbatim_surface() {
    struct ScalarFixture {
        name: &'static str,
        node: Json,
        shaped_exasol: Option<&'static str>,
    }

    fn verbatim(name: &'static str, args: Vec<Json>) -> ScalarFixture {
        ScalarFixture {
            name,
            node: scalar_fn(name, args),
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
        fixtures.push(verbatim(name, vec![col("v"), num_lit(2)]));
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
        fixtures.push(verbatim(name, vec![num_lit(65)]));
    }
    for name in ["SUBSTR", "LEFT", "RIGHT", "REPEAT", "LPAD", "RPAD"] {
        fixtures.push(verbatim(name, vec![col("s"), num_lit(3)]));
    }
    fixtures.push(verbatim("REPLACE", vec![col("s"), str_lit("a")]));
    fixtures.push(verbatim(
        "TRANSLATE",
        vec![col("s"), str_lit("ab"), str_lit("xy")],
    ));
    fixtures.push(verbatim("INSTR", vec![col("s"), str_lit("a")]));
    fixtures.push(verbatim("LOCATE", vec![str_lit("a"), col("s")]));

    for name in ["GREATEST", "LEAST", "NULLIF"] {
        fixtures.push(verbatim(name, vec![col("a"), col("b")]));
    }
    for name in ["NULLIFZERO", "ZEROIFNULL"] {
        fixtures.push(verbatim(name, vec![col("v")]));
    }

    for name in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "WEEK"] {
        fixtures.push(verbatim(name, vec![col("ts")]));
    }
    fixtures.push(verbatim("DATE_TRUNC", vec![str_lit("month"), col("ts")]));
    fixtures.push(verbatim("TO_DATE", vec![col("s"), str_lit("YYYY-MM-DD")]));
    fixtures.push(verbatim(
        "TO_TIMESTAMP",
        vec![col("s"), str_lit("YYYY-MM-DD HH24:MI:SS")],
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
            scalar_fn("ADD", [col("a"), col("b")]),
            r#"("A" + "B")"#,
        ),
        shaped(
            "SUB",
            scalar_fn("SUB", [col("a"), col("b")]),
            r#"("A" - "B")"#,
        ),
        shaped(
            "MULT",
            scalar_fn("MULT", [col("a"), col("b")]),
            r#"("A" * "B")"#,
        ),
        shaped(
            "FLOAT_DIV",
            scalar_fn("FLOAT_DIV", [col("a"), col("b")]),
            r#"("A" / "B")"#,
        ),
        shaped("NEG", scalar_fn("NEG", [col("a")]), r#"(-"A")"#),
        shaped(
            "MOD",
            scalar_fn("MOD", [col("a"), col("b")]),
            r#"MOD("A", "B")"#,
        ),
        shaped(
            "CONCAT",
            scalar_fn("CONCAT", [col("a"), col("b")]),
            r#"("A" || "B")"#,
        ),
        shaped(
            "CAST",
            cast(col("v"), varchar_type(50)),
            r#"CAST("V" AS VARCHAR(50))"#,
        ),
        shaped(
            "CASE",
            scalar_fn(
                "CASE",
                [
                    predicate_greater(col("x"), num_lit(0)),
                    num_lit(1),
                    num_lit(0),
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
            extract("YEAR", col("ts")),
            r#"EXTRACT(YEAR FROM "TS")"#,
            r#"date_part('YEAR', "TS")"#,
        ),
        (
            cast(col("v"), varchar_type(50)),
            r#"CAST("V" AS VARCHAR(50))"#,
            r#"CAST("V" AS VARCHAR)"#,
        ),
        (
            timestamp_lit("2024-03-01 12:34:56.789"),
            "TIMESTAMP '2024-03-01 12:34:56.789'",
            "arrow_cast('2024-03-01 12:34:56.789', 'Timestamp(Microsecond, None)')",
        ),
        (
            timestamp_utc_lit("2024-03-01 12:34:56.789"),
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

/// Scenario: Arithmetic operators translate to binary SQL expressions
#[test]
fn arithmetic_operators_render_identically_in_both_dialects() {
    for (node, expected) in [
        (scalar_fn("ADD", [col("a"), num_lit(1)]), r#"("A" + 1)"#),
        (scalar_fn("SUB", [col("a"), num_lit(1)]), r#"("A" - 1)"#),
        (scalar_fn("MULT", [col("a"), num_lit(1)]), r#"("A" * 1)"#),
        (scalar_fn("NEG", [col("a")]), r#"(-"A")"#),
    ] {
        let name = node["name"].as_str().unwrap();
        assert_eq!(
            render_expression(&node).unwrap(),
            expected,
            "{name} DataFusion dialect"
        );
        assert_eq!(
            render_expression_exasol(&node).unwrap(),
            expected,
            "{name} Exasol dialect"
        );
    }
}

/// Scenario: The Exasol dialect keeps rendering FLOAT_DIV as a bare division operator
#[test]
fn float_div_renders_checked_division_call_only_in_the_datafusion_dialect() {
    let float_div = scalar_fn("FLOAT_DIV", [col("a"), num_lit(1)]);
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
        (null_lit(), "NULL"),
        (bool_lit(true), "TRUE"),
        (bool_lit(false), "FALSE"),
        (str_lit("hello"), "'hello'"),
        (str_lit("it's"), "'it''s'"),
        (num_lit(42), "42"),
        (date_lit("2024-01-15"), "DATE '2024-01-15'"),
    ];
    for (node, expected) in cases {
        assert_eq!(
            render_expression(&node).unwrap(),
            expected,
            "{node} DataFusion dialect"
        );
        assert_eq!(
            render_expression_exasol(&node).unwrap(),
            expected,
            "{node} Exasol dialect"
        );
    }
}

/// Scenario: Literal nodes translate to SQL literal forms
#[test]
fn literal_double_renders_as_a_double_cast_only_in_the_datafusion_dialect() {
    for (value, datafusion, exasol) in [
        (
            json!("1.0000000000000001e+299"),
            "CAST('1.0000000000000001e+299' AS DOUBLE)",
            "1.0000000000000001e+299",
        ),
        (json!(0.5), "CAST('0.5' AS DOUBLE)", "0.5"),
        (Json::Null, "NULL", "NULL"),
    ] {
        let node = double_lit(value);
        assert_eq!(render_expression(&node).unwrap(), datafusion);
        assert_eq!(render_expression_exasol(&node).unwrap(), exasol);
    }
}

#[test]
fn a_binary_literal_renders_for_datafusion_only_and_only_as_hex() {
    let variable = binary_lit("0a1b");
    let fixed = json!({"type": "literal_binary", "value": "0a1b", "width": 2});

    assert_eq!(render_expression(&variable).unwrap(), "X'0a1b'");
    assert_eq!(
        render_expression(&fixed).unwrap(),
        "arrow_cast(X'0a1b', 'FixedSizeBinary(2)')"
    );
    assert!(render_expression_exasol(&variable).is_err());
    assert!(render_expression(&binary_lit("0a'")).is_err());
}

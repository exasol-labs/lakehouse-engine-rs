use super::*;
use crate::scalar_functions::{SCALAR_FUNCTIONS, ScalarFunction};
use crate::test_support::*;
use serde_json::json;

fn scan_filter(filter: &Json) -> Result<String, UdfError> {
    render_scan_filter(filter, &typed_columns::<&str>(&[]))
}

/// Exasol's `CAST("X" AS data_type)` node.
fn cast(data_type: Json) -> Json {
    cast_json(data_type, col("x"))
}

#[test]
fn renders_column_as_quoted_uppercase_ident() {
    let expr = col("region");
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""REGION""#);
}

#[test]
fn renders_column_with_embedded_quotes() {
    let expr = col(r#"my"col"#);
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""MY""COL""#);
}

#[test]
fn renders_table_qualified_column_when_alias_present() {
    let expr = alias_col("LHS_FACT", "id");
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""LHS_FACT"."ID""#);
}

#[test]
fn empty_table_alias_falls_back_to_bare_column() {
    let expr = alias_col("", "id");
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#""ID""#);
}

#[test]
fn renders_string_literal() {
    let expr = str_lit("hello");
    assert_eq!(render_expression(&expr).unwrap(), "'hello'");
}

#[test]
fn renders_string_literal_with_single_quote_escaped() {
    let expr = str_lit("it's");
    assert_eq!(render_expression(&expr).unwrap(), "'it''s'");
}

#[test]
fn renders_null_literal() {
    let expr = null_lit();
    assert_eq!(render_expression(&expr).unwrap(), "NULL");
}

#[test]
fn renders_bool_literal() {
    let t = bool_lit(true);
    let f = bool_lit(false);
    assert_eq!(render_expression(&t).unwrap(), "TRUE");
    assert_eq!(render_expression(&f).unwrap(), "FALSE");
}

#[test]
fn renders_numeric_literal() {
    let expr = num_lit(42);
    assert_eq!(render_expression(&expr).unwrap(), "42");
}

#[test]
fn renders_date_literal() {
    let expr = date_lit("2024-01-15");
    assert_eq!(render_expression(&expr).unwrap(), "DATE '2024-01-15'");
}

#[test]
fn renders_timestamp_literal() {
    let expr = timestamp_lit("2024-01-15 12:00:00");
    assert_eq!(
        render_expression(&expr).unwrap(),
        "arrow_cast('2024-01-15 12:00:00', 'Timestamp(Microsecond, None)')"
    );
}

#[test]
fn renders_far_future_timestamp_literal() {
    let expr = timestamp_lit("9999-12-31 23:59:59");
    assert_eq!(
        render_expression(&expr).unwrap(),
        "arrow_cast('9999-12-31 23:59:59', 'Timestamp(Microsecond, None)')"
    );
}

#[test]
fn timestamp_literal_with_sub_microsecond_digits_renders_nanoseconds() {
    for (value, unit) in [
        ("2025-06-01 00:00:00.123456789", "Nanosecond"),
        ("2025-06-01 00:00:00.1234567", "Nanosecond"),
        ("2025-06-01 00:00:00.123456000", "Microsecond"),
        ("2025-06-01 00:00:00.123456", "Microsecond"),
    ] {
        let expr = timestamp_lit(value);
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!("arrow_cast('{value}', 'Timestamp({unit}, None)')"),
            "literal {value}"
        );
    }
}

#[test]
fn renders_simple_equality() {
    let expr = predicate_equal(col("id"), num_lit(10));
    assert_eq!(render_expression(&expr).unwrap(), r#"("ID" = 10)"#);
}

#[test]
fn renders_and_predicate() {
    let expr = and([
        compare("predicate_greater", col("age"), num_lit(18)),
        compare("predicate_less", col("age"), num_lit(65)),
    ]);
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("AND"), "AND not found in: {sql}");
}

#[test]
fn renders_or_predicate() {
    let expr = or([
        predicate_equal(col("status"), str_lit("A")),
        predicate_equal(col("status"), str_lit("B")),
    ]);
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("OR"), "OR not found in: {sql}");
}

#[test]
fn renders_not_predicate() {
    let expr = not(predicate_equal(col("active"), bool_lit(true)));
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("NOT"), "NOT not found in: {sql}");
}

#[test]
fn renders_empty_and_as_true() {
    let expr = and([]);
    assert_eq!(render_expression(&expr).unwrap(), "TRUE");
}

#[test]
fn renders_empty_or_as_false() {
    let expr = or([]);
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

#[test]
fn renders_is_null() {
    let expr = is_null(col("x"));
    assert_eq!(render_expression(&expr).unwrap(), r#"("X" IS NULL)"#);
}

#[test]
fn renders_is_not_null() {
    let expr = is_not_null(col("x"));
    assert_eq!(render_expression(&expr).unwrap(), r#"("X" IS NOT NULL)"#);
}

#[test]
fn renders_in_constlist() {
    let expr = in_list(col("status"), [str_lit("A"), str_lit("B")]);
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("IN"), "IN not found: {sql}");
    assert!(sql.contains("'A'"), "'A' not found: {sql}");
    assert!(sql.contains("'B'"), "'B' not found: {sql}");
}

#[test]
fn renders_empty_in_as_false() {
    let expr = in_list(col("x"), []);
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

#[test]
fn renders_in_constlist_strips_null() {
    let expr = in_list(
        col("status"),
        [str_lit("A"), null_lit(), lit("literal_date", Json::Null)],
    );
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("'A'"), "'A' not found: {sql}");
    assert!(!sql.contains("NULL"), "NULL should not survive: {sql}");
}

#[test]
fn renders_all_null_in_as_false() {
    let expr = in_list(col("x"), [null_lit(), lit("literal_date", Json::Null)]);
    assert_eq!(render_expression(&expr).unwrap(), "FALSE");
}

#[test]
fn renders_not_in_constlist_strips_null() {
    let expr = not(in_list(
        col("status"),
        [str_lit("A"), null_lit(), lit("literal_date", Json::Null)],
    ));
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#"(NOT ("STATUS" IN ('A')))"#);
}

#[test]
fn renders_between() {
    let expr = between(col("age"), num_lit(18), num_lit(65));
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("BETWEEN"), "BETWEEN not found: {sql}");
    assert!(sql.contains("18"), "low bound not found: {sql}");
    assert!(sql.contains("65"), "high bound not found: {sql}");
}

#[test]
fn renders_like_without_escape() {
    let expr = like(col("name"), "A%");
    let sql = render_expression(&expr).unwrap();
    assert!(sql.contains("LIKE"), "LIKE not found: {sql}");
    assert!(!sql.contains("ESCAPE"), "ESCAPE should be absent: {sql}");
}

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
fn renders_arithmetic_add() {
    let expr = scalar_fn("ADD", [col("a"), num_lit(1)]);
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" + 1)"#);
}

#[test]
fn renders_arithmetic_sub() {
    let expr = scalar_fn("SUB", [col("a"), num_lit(1)]);
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" - 1)"#);
}

#[test]
fn renders_arithmetic_mul() {
    let expr = scalar_fn("MULT", [col("a"), num_lit(2)]);
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" * 2)"#);
}

#[test]
fn legacy_mul_name_is_not_recognized() {
    let expr = scalar_fn("MUL", [col("a"), col("b")]);
    assert!(
        render_expression(&expr).is_err(),
        "the obsolete \"MUL\" node name must not translate; Exasol emits \"MULT\""
    );
}

#[test]
fn renders_two_column_arithmetic_product() {
    let expr = scalar_fn("MULT", [col("l_extendedprice"), col("l_discount")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#
    );
}

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
fn float_div_calls_checked_division_for_column_left_operand_and_literal_right_operand() {
    let expr = scalar_fn("FLOAT_DIV", [col("a"), num_lit(2)]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}("A", 2)"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_column_left_operand_and_column_right_operand() {
    let expr = scalar_fn("FLOAT_DIV", [col("a"), col("b")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}("A", "B")"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_literal_left_operand_and_column_right_operand() {
    let expr = scalar_fn("FLOAT_DIV", [num_lit(10), col("b")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(10, "B")"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_literal_left_operand_and_literal_right_operand() {
    let expr = scalar_fn("FLOAT_DIV", [num_lit(10), num_lit(4)]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(10, 4)"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_nested_expression_left_and_column_right() {
    let expr = scalar_fn(
        "FLOAT_DIV",
        [scalar_fn("ADD", [col("a"), col("b")]), col("c")],
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(("A" + "B"), "C")"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_nested_expression_left_and_literal_right() {
    let expr = scalar_fn(
        "FLOAT_DIV",
        [scalar_fn("ADD", [col("a"), col("b")]), num_lit(2)],
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(("A" + "B"), 2)"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_aggregate_left_operand_and_column_right_operand() {
    let expr = scalar_fn(
        "FLOAT_DIV",
        [agg_item("SUM", Some("amount"), false), col("c")],
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(SUM("AMOUNT"), "C")"#)
    );
}

#[test]
fn float_div_calls_checked_division_for_aggregate_left_operand_and_literal_right_operand() {
    let expr = scalar_fn(
        "FLOAT_DIV",
        [agg_item("SUM", Some("amount"), false), num_lit(2)],
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(SUM("AMOUNT"), 2)"#)
    );
}

#[test]
fn float_div_with_null_left_operand_passes_the_null_literal_to_checked_division() {
    let expr = scalar_fn("FLOAT_DIV", [null_lit(), col("b")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(NULL, "B")"#)
    );
}

#[test]
fn float_div_with_null_right_operand_passes_null_to_checked_division() {
    let expr = scalar_fn("FLOAT_DIV", [col("a"), null_lit()]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}("A", NULL)"#)
    );
}

#[test]
fn float_div_null_over_zero_passes_both_literals_to_checked_division() {
    let expr = scalar_fn("FLOAT_DIV", [null_lit(), num_lit(0)]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}(NULL, 0)"#)
    );
}

#[test]
fn renders_arithmetic_neg() {
    let expr = scalar_fn("NEG", [col("a")]);
    assert_eq!(render_expression(&expr).unwrap(), r#"(-"A")"#);
}

#[test]
fn neg_composes_with_aggregate_decomposition() {
    let sum_neg = agg_item_expr("SUM", scalar_fn("NEG", [col("col")]), false);
    assert_eq!(render_expression(&sum_neg).unwrap(), r#"SUM((-"COL"))"#);
}

#[test]
fn renders_cast_varchar() {
    let expr = cast(json!({"type": "VARCHAR", "size": 100}));
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
}

#[test]
fn renders_cast_decimal() {
    let expr = cast(json!({"type": "DECIMAL", "precision": 10, "scale": 2}));
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("X" AS DECIMAL(10,2))"#
    );
}

#[test]
fn renders_cast_double() {
    let expr = cast(json!({"type": "DOUBLE"}));
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS DOUBLE)"#);
}

#[test]
fn renders_cast_date() {
    let expr = cast(json!({"type": "DATE"}));
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS DATE)"#);
}

#[test]
fn renders_cast_char_as_datafusion_varchar() {
    let expr = cast(json!({"type": "CHAR", "size": 3, "characterSet": "ASCII"}));
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS VARCHAR)"#);
}

#[test]
fn renders_cast_char_as_exasol_char() {
    let expr = cast(json!({"type": "CHAR", "size": 3, "characterSet": "ASCII"}));
    assert_eq!(
        render_expression_exasol(&expr),
        r#"CAST("X" AS CHAR(3) ASCII)"#
    );
}

#[test]
fn renders_cast_bool_to_varchar_as_plain_cast_in_exasol_dialect() {
    let expr = cast_json(
        json!({"type": "VARCHAR", "size": 10}),
        compare("predicate_greater", col("c_acctbal"), num_lit(0)),
    );
    assert_eq!(
        render_expression_exasol(&expr),
        r#"CAST(("C_ACCTBAL" > 0) AS VARCHAR(10))"#
    );
}

#[test]
fn renders_cast_boolean() {
    let expr = cast(json!({"type": "BOOLEAN"}));
    assert_eq!(render_expression(&expr).unwrap(), r#"CAST("X" AS BOOLEAN)"#);
}

#[test]
fn renders_cast_timestamp_without_local_time_zone() {
    let expr = cast(
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
        let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": p}));
        let expected = if p == 3 {
            r#"CAST("X" AS TIMESTAMP)"#.to_string()
        } else {
            format!(r#"CAST("X" AS TIMESTAMP({p}))"#)
        };
        assert_eq!(
            render_expression_exasol(&expr),
            expected,
            "Exasol dialect must render precision {p} as bare TIMESTAMP at 3, TIMESTAMP({p}) otherwise"
        );
    }

    for p in [0u64, 3, 6, 9] {
        let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": p}));
        assert_eq!(
            render_expression(&expr).unwrap(),
            format!(r#"CAST("X" AS TIMESTAMP({p}))"#),
            "DataFusion dialect must render TIMESTAMP({p}) verbatim"
        );
    }

    for p in [1u64, 2, 4, 5, 7, 8, 10, 99] {
        let expr = cast(json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": p}));
        assert!(
            render_expression(&expr).is_err(),
            "DataFusion dialect must decline TIMESTAMP({p})"
        );
    }

    for data_type in [
        json!({"type": "TIMESTAMP"}),
        json!({"type": "TIMESTAMP", "withLocalTimeZone": false}),
    ] {
        let expr = cast(data_type.clone());
        assert_eq!(
            render_expression(&expr).unwrap(),
            r#"CAST("X" AS TIMESTAMP(3))"#,
            "DataFusion dialect: absent precision must render millisecond TIMESTAMP(3) for {data_type}"
        );
        assert_eq!(
            render_expression_exasol(&expr),
            r#"CAST("X" AS TIMESTAMP)"#,
            "Exasol dialect: absent precision must render bare TIMESTAMP for {data_type}"
        );
    }
}

#[test]
fn cast_to_unsupported_target_declines() {
    let unsupported = [
        (
            json!({"type": "INTERVAL", "fromTo": "YEAR TO MONTH", "precision": 2}),
            "INTERVAL YEAR(2) TO MONTH",
        ),
        (
            json!({"type": "INTERVAL", "fromTo": "DAY TO SECONDS", "precision": 2, "fraction": 2}),
            "INTERVAL DAY(2) TO SECOND(2)",
        ),
        (json!({"type": "GEOMETRY", "srid": 4326}), "GEOMETRY(4326)"),
        (
            json!({"type": "HASHTYPE", "bytesize": 16}),
            "HASHTYPE(16 BYTE)",
        ),
        (
            json!({"type": "HASHTYPE", "bytesize": 1}),
            "HASHTYPE(1 BYTE)",
        ),
        (
            json!({"type": "TIMESTAMP", "withLocalTimeZone": true, "fractionalSecondsPrecision": 9}),
            "TIMESTAMP(9) WITH LOCAL TIME ZONE",
        ),
        (
            json!({"type": "TIMESTAMP", "withLocalTimeZone": true}),
            "TIMESTAMP WITH LOCAL TIME ZONE",
        ),
    ];
    for (data_type, exasol_target) in unsupported {
        let expr = cast(data_type.clone());
        assert!(
            render_expression(&expr).is_err(),
            "CAST to {data_type} must raise in the DataFusion dialect"
        );
        assert_eq!(
            render_expression_exasol(&expr),
            format!(r#"CAST("X" AS {exasol_target})"#),
            "the Exasol dialect must render CAST to {data_type}"
        );
    }
}

#[test]
fn exasol_dialect_renders_a_varchar_ascii_target() {
    let expr = cast(json!({"type": "VARCHAR", "size": 10, "characterSet": "ASCII"}));
    assert_eq!(
        render_expression_exasol(&expr),
        r#"CAST("X" AS VARCHAR(10) ASCII)"#
    );
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
fn a_null_scan_filter_renders_as_false() {
    for filter in [bool_lit(false), null_lit()] {
        assert_eq!(scan_filter(&filter).unwrap(), "FALSE");
    }
}

#[test]
fn conjoined_scan_filters_render_as_their_predicate_and() {
    let conjuncts = [
        predicate_equal(num_lit(1), num_lit(1)),
        predicate_equal(num_lit(2), num_lit(2)),
    ];
    let one_by_one = conjuncts.iter().map(|c| scan_filter(c).unwrap()).collect();
    assert_eq!(
        conjoin_scan_filters(one_by_one),
        scan_filter(&and(conjuncts)).ok()
    );
    assert_eq!(
        conjoin_scan_filters(vec!["NULL".into()]).as_deref(),
        Some("FALSE")
    );
    assert_eq!(conjoin_scan_filters(Vec::new()), None);
}

#[test]
fn renders_timestamp_utc_literal() {
    let expr = lit("literal_timestamp_utc", "2024-03-01 10:00:00");
    let sql = render_expression(&expr).unwrap();
    assert_eq!(
        sql,
        "arrow_cast('2024-03-01 10:00:00+00:00', 'Timestamp(Microsecond, Some(\"UTC\"))')"
    );
}

#[test]
fn renders_timestamp_literals_as_bare_timestamp_in_exasol_dialect() {
    let ts = timestamp_lit("2024-01-15 12:00:00");
    let ts_utc = lit("literal_timestamp_utc", "2024-03-01 10:00:00");

    let ts_exasol = render_expression_exasol(&ts);
    assert_eq!(ts_exasol, "TIMESTAMP '2024-01-15 12:00:00'");
    assert!(
        !ts_exasol.contains("arrow_cast"),
        "Exasol rejects arrow_cast with sqlCode 42000: {ts_exasol}"
    );

    assert_eq!(
        render_expression_exasol(&timestamp_lit("2024-01-15 12:00:00' OR '1'='1")),
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
    let ts_utc = lit("literal_timestamp_utc", value);

    let exasol = render_expression_exasol(&ts_utc);
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
    let real_wire_name = lit("literal_timestamputc", value);
    assert_eq!(
        render_expression_exasol(&real_wire_name),
        "CAST(CONVERT_TZ(TIMESTAMP '2024-03-01 09:00:00', 'UTC', SESSIONTIMEZONE) \
         AS TIMESTAMP WITH LOCAL TIME ZONE)"
    );

    assert!(render_expression(&real_wire_name).is_err());
}

#[test]
fn renders_null_valued_tstz_literal_bare_in_exasol_dialect() {
    for node_type in ["literal_timestamp_utc", "literal_timestamputc"] {
        for node in [
            json!({"type": node_type, "value": null}),
            json!({"type": node_type}),
        ] {
            assert_eq!(
                render_expression_exasol(&node),
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
                render_expression_exasol(&node),
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
        let expr = scalar_fn(exasol, [col("x")]);
        let sql = render_expression(&expr).unwrap();
        assert_eq!(sql, format!(r#"{df}("X")"#), "failed for {exasol}");
    }

    let expr = scalar_fn("POWER", [col("x"), num_lit(2)]);
    assert_eq!(render_expression(&expr).unwrap(), r#"power("X", 2)"#);

    let expr = scalar_fn("ATAN2", [col("y"), col("x")]);
    assert_eq!(render_expression(&expr).unwrap(), r#"atan2("Y", "X")"#);

    let expr = scalar_fn("ROUND", [col("v"), num_lit(2)]);
    assert_eq!(render_expression(&expr).unwrap(), r#"round("V", 2)"#);

    let expr = scalar_fn("TRUNC", [col("v")]);
    assert_eq!(render_expression(&expr).unwrap(), r#"trunc("V")"#);

    let expr = scalar_fn("ABS", [col("x"), col("y")]);
    assert!(render_expression(&expr).is_err());
}

#[test]
fn renders_mod_as_operator() {
    let expr = scalar_fn("MOD", [col("a"), num_lit(3)]);
    assert_eq!(render_expression(&expr).unwrap(), r#"("A" % 3)"#);
}

#[test]
fn renders_mod_as_function_call_in_exasol_dialect() {
    let expr = scalar_fn("MOD", [col("a"), num_lit(3)]);
    assert_eq!(render_expression_exasol(&expr), r#"MOD("A", 3)"#);
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
        let expr = scalar_fn(name, [col("s")]);
        let sql = render_expression(&expr).unwrap();
        assert_eq!(
            sql,
            format!(r#"{}("S")"#, name.to_lowercase()),
            "failed for {name}"
        );
    }

    let expr = scalar_fn("LENGTH", [col("s")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"character_length("S")"#
    );

    let expr = scalar_fn("OCTET_LENGTH", [col("s")]);
    assert_eq!(render_expression(&expr).unwrap(), r#"octet_length("S")"#);

    let expr = scalar_fn("UNICODE", [col("s")]);
    assert_eq!(render_expression(&expr).unwrap(), r#"ascii("S")"#);

    let expr = scalar_fn("UNICODECHR", [col("n")]);
    assert_eq!(render_expression(&expr).unwrap(), r#"chr("N")"#);

    let expr = scalar_fn("SUBSTR", [col("s"), num_lit(1), num_lit(3)]);
    assert_eq!(render_expression(&expr).unwrap(), r#"substr("S", 1, 3)"#);

    let expr = scalar_fn("INSTR", [str_lit("hello"), str_lit("ll")]);
    assert_eq!(render_expression(&expr).unwrap(), "strpos('hello', 'll')");

    let expr = scalar_fn("LOCATE", [str_lit("ll"), str_lit("hello")]);
    assert_eq!(render_expression(&expr).unwrap(), "strpos('hello', 'll')");
}

#[test]
fn renders_concat_as_nullif_wrapped_concat_call() {
    let expr = scalar_fn("CONCAT", [col("s"), str_lit("")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat("S", ''), '')"#
    );

    let expr = scalar_fn("CONCAT", [col("a"), col("b"), col("c")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat("A", "B", "C"), '')"#
    );
}

#[test]
fn renders_concat_as_chained_pipe_operator_in_exasol_dialect() {
    let expr = scalar_fn("CONCAT", [col("s"), str_lit("")]);
    assert_eq!(render_expression_exasol(&expr), r#"("S" || '')"#);

    let expr = scalar_fn("CONCAT", [col("a"), col("b"), col("c")]);
    assert_eq!(render_expression_exasol(&expr), r#"("A" || "B" || "C")"#);

    let expr = scalar_fn(
        "CONCAT",
        [predicate_equal(col("active"), bool_lit(true)), str_lit("")],
    );
    assert_eq!(
        render_expression_exasol(&expr),
        r#"(("ACTIVE" = TRUE) || '')"#
    );
}

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
    assert_eq!(render_expression_exasol(&expr), r#"(("A" || "B") || "C")"#);
}

#[test]
fn renders_concat_single_argument() {
    let expr = scalar_fn("CONCAT", [col("s")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"nullif(concat("S"), '')"#
    );
    assert_eq!(render_expression_exasol(&expr), r#"("S")"#);
}

#[test]
fn concat_without_a_renderable_argument_errors_in_the_datafusion_dialect() {
    let cases = [
        (
            json!({"type": "function_scalar", "name": "CONCAT"}),
            "function_scalar CONCAT missing 'arguments'",
        ),
        (
            scalar_fn("CONCAT", []),
            "function_scalar CONCAT requires at least 1 argument, got 0",
        ),
        (
            scalar_fn("CONCAT", [col("a"), Json::Null]),
            "CONCAT argument rendered to null",
        ),
    ];
    for (expr, expected) in cases {
        assert_eq!(
            render_expression(&expr).unwrap_err().to_string(),
            expected,
            "{expr}"
        );
    }
}

#[test]
fn renders_case_when() {
    let expr = scalar_fn(
        "CASE",
        [predicate_equal(col("status"), str_lit("A")), num_lit(1)],
    );
    let sql = render_expression(&expr).unwrap();
    assert_eq!(sql, r#"CASE WHEN ("STATUS" = 'A') THEN 1 END"#);

    let expr2 = scalar_fn(
        "CASE",
        [
            predicate_equal(col("x"), num_lit(1)),
            str_lit("one"),
            predicate_equal(col("x"), num_lit(2)),
            str_lit("two"),
            str_lit("other"),
        ],
    );
    let sql2 = render_expression(&expr2).unwrap();
    assert_eq!(
        sql2,
        r#"CASE WHEN ("X" = 1) THEN 'one' WHEN ("X" = 2) THEN 'two' ELSE 'other' END"#
    );

    let expr3 = scalar_fn("CASE", []);
    assert!(render_expression(&expr3).is_err());
}

#[test]
fn renders_greatest_least() {
    let expr = scalar_fn("GREATEST", [col("a"), col("b"), col("c")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN "A" IS NULL OR "B" IS NULL OR "C" IS NULL THEN NULL ELSE greatest("A", "B", "C") END"#
    );

    let expr2 = scalar_fn("LEAST", [col("x"), num_lit(0)]);
    assert_eq!(
        render_expression(&expr2).unwrap(),
        r#"CASE WHEN "X" IS NULL OR 0 IS NULL THEN NULL ELSE least("X", 0) END"#
    );
}

#[test]
fn renders_greatest_least_single_argument_guard() {
    let greatest = scalar_fn("GREATEST", [col("a")]);
    assert_eq!(
        render_expression(&greatest).unwrap(),
        r#"CASE WHEN "A" IS NULL THEN NULL ELSE greatest("A") END"#
    );

    let least = scalar_fn("LEAST", [col("a")]);
    assert_eq!(
        render_expression(&least).unwrap(),
        r#"CASE WHEN "A" IS NULL THEN NULL ELSE least("A") END"#
    );
}

#[test]
fn renders_greatest_least_with_literal_null_argument() {
    let expr = scalar_fn("LEAST", [col("x"), col("y"), null_lit()]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN "X" IS NULL OR "Y" IS NULL OR NULL IS NULL THEN NULL ELSE least("X", "Y", NULL) END"#
    );
}

#[test]
fn renders_greatest_least_nested_argument_once_referenced_twice() {
    let expr = scalar_fn("GREATEST", [scalar_fn("ABS", [col("y")]), col("z")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN abs("Y") IS NULL OR "Z" IS NULL THEN NULL ELSE greatest(abs("Y"), "Z") END"#
    );
}

#[test]
fn renders_nested_greatest_guard_referencing_the_inner_case_twice() {
    let expr = scalar_fn(
        "GREATEST",
        [scalar_fn("GREATEST", [col("a"), col("b")]), col("c")],
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CASE WHEN CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B") END IS NULL OR "C" IS NULL THEN NULL ELSE greatest(CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B") END, "C") END"#
    );
}

#[test]
fn renders_greatest_least_empty_argument_list_errors() {
    for name in ["GREATEST", "LEAST"] {
        let expr = scalar_fn(name, []);
        assert!(
            render_expression(&expr).is_err(),
            "{name} empty args must raise"
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
    }
}

#[test]
fn renders_nullifzero_zeroifnull() {
    let expr = scalar_fn("NULLIFZERO", [col("v")]);
    assert_eq!(render_expression(&expr).unwrap(), r#"nullif("V", 0)"#);

    let expr2 = scalar_fn("ZEROIFNULL", [col("v")]);
    assert_eq!(render_expression(&expr2).unwrap(), r#"coalesce("V", 0)"#);
}

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
    let expr = case_of(mod_node.clone(), [num_lit("0")], [null_lit(), mod_node]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE ("ID" % 5) WHEN 0 THEN NULL ELSE ("ID" % 5) END)"#
    );
}

#[test]
fn renders_searched_case_without_basis() {
    let expr = case_when(
        [compare("predicate_less", col("SCORE"), num_lit("50"))],
        [str_lit("low"), str_lit("high")],
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE WHEN ("SCORE" < 50) THEN 'low' ELSE 'high' END)"#
    );
}

#[test]
fn renders_case_without_else() {
    let expr = case_of(col("ID"), [num_lit("1")], [str_lit("one")]);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"(CASE "ID" WHEN 1 THEN 'one' END)"#
    );
}

#[test]
fn renders_extract() {
    let expr = extract("YEAR", col("ts"));
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('YEAR', "TS")"#
    );

    let expr2 = extract("MONTH", col("ts"));
    assert_eq!(
        render_expression(&expr2).unwrap(),
        r#"date_part('MONTH', "TS")"#
    );
}

#[test]
fn renders_extract_as_exasol_extract_from_in_exasol_dialect() {
    let expr = extract("DAY", col("ts"));
    assert_eq!(render_expression_exasol(&expr), r#"EXTRACT(DAY FROM "TS")"#);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('DAY', "TS")"#
    );
}

#[test]
fn renders_year_month_day_extract() {
    let shortcuts = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"];
    for field in shortcuts {
        let expr = scalar_fn(field, [col("ts")]);
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
    let expr = scalar_fn("DATE_TRUNC", [str_lit("month"), col("ts")]);
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
        let expr = scalar_fn(name, []);
        let err = render_expression(&expr).unwrap_err().to_string();
        assert!(
            err.contains("unsupported scalar function"),
            "{name} must fall through the generic unsupported-scalar-function path: {err}"
        );
        assert!(err.contains(name), "the error must name '{name}': {err}");
    }
}

#[test]
fn renders_to_date_to_timestamp() {
    let expr = scalar_fn("TO_DATE", [col("s")]);
    assert!(
        render_expression(&expr).is_err(),
        "without a format Exasol parses by the session"
    );

    let expr2 = scalar_fn("TO_DATE", [col("s"), str_lit("YYYY-MM-DD")]);
    assert_eq!(
        render_expression(&expr2).unwrap(),
        r#"to_date(nullif("S", ''), '%Y-%m-%d')"#
    );

    let expr3 = scalar_fn("TO_TIMESTAMP", [col("s")]);
    assert!(render_expression(&expr3).is_err());

    let expr4 = scalar_fn("TO_TIMESTAMP", [col("s"), str_lit("YYYY-MM-DD HH24:MI:SS")]);
    assert_eq!(
        render_expression(&expr4).unwrap(),
        r#"to_timestamp(nullif("S", ''), '%Y-%m-%d %H:%M:%S')"#
    );
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
    assert!(render_expression(&expr).is_err());
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
    let hours = scalar_fn("HOURS_BETWEEN", [col("a"), col("b")]);
    assert_eq!(
        render_expression(&hours).unwrap(),
        r#"((date_part('epoch', "A") - date_part('epoch', "B")) / 3600)"#
    );

    let minutes = scalar_fn("MINUTES_BETWEEN", [col("a"), col("b")]);
    assert_eq!(
        render_expression(&minutes).unwrap(),
        r#"((date_part('epoch', "A") - date_part('epoch', "B")) / 60)"#
    );

    let seconds = scalar_fn("SECONDS_BETWEEN", [col("a"), col("b")]);
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
        let one_arg = scalar_fn(name, [col("a")]);
        assert!(
            render_expression(&one_arg).is_err(),
            "{name} 1-arg must raise"
        );

        let three_args = scalar_fn(name, [col("a"), col("b"), col("c")]);
        assert!(
            render_expression(&three_args).is_err(),
            "{name} 3-arg must raise"
        );
    }
}

#[test]
fn second_with_precision_declines_for_datafusion_renders_for_exasol() {
    let expr = scalar_fn("SECOND", [col("ts"), num_lit(3)]);

    assert!(
        render_expression(&expr).is_err(),
        "SECOND(ts, 3) must decline under the DataFusion dialect"
    );
    assert_eq!(render_expression_exasol(&expr), r#"SECOND("TS", 3)"#);
}

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
    }
}

/// Scenario: Regexp scalar functions are deliberately not translated
#[test]
fn regexp_scalar_functions_decline_in_the_datafusion_dialect() {
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
    }
}

#[test]
fn regexp_like_predicate_declines_in_the_datafusion_dialect() {
    let expr = json!({
        "type": "predicate_like_regexp",
        "expression": col("name"),
        "pattern": str_lit("^A.*")
    });
    let expected = "unsupported expression node type: predicate_like_regexp";
    assert_eq!(render_expression(&expr).unwrap_err().to_string(), expected);
    assert!(scan_filter(&expr).is_err());
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
    }
}

#[test]
fn undeclared_scalar_function_declines_in_the_datafusion_dialect() {
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
    }
}

#[test]
fn unsupported_date_functions_decline_in_the_datafusion_dialect() {
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
    }
}

#[test]
fn render_expression_renders_aggregate_nodes() {
    let sum = agg_item("SUM", Some("col"), false);
    assert_eq!(render_expression(&sum).unwrap(), r#"SUM("COL")"#);

    let count_star = count_star();
    assert_eq!(render_expression(&count_star).unwrap(), "COUNT(*)");

    let count_distinct = agg_item("COUNT", Some("col"), true);
    assert_eq!(
        render_expression(&count_distinct).unwrap(),
        r#"COUNT(DISTINCT "COL")"#
    );

    let avg = agg_item("AVG", Some("col"), false);
    assert_eq!(render_expression(&avg).unwrap(), r#"AVG("COL")"#);

    let sum_qualified = agg_item_expr("SUM", alias_col("O", "amount"), false);
    assert_eq!(
        render_expression(&sum_qualified).unwrap(),
        r#"SUM("O"."AMOUNT")"#
    );
}

#[test]
fn render_expression_renders_scalar_wrapping_aggregates() {
    let sum_case = agg_item_expr(
        "SUM",
        scalar_fn(
            "CASE",
            [
                predicate_equal(col("l_returnflag"), str_lit("R")),
                num_lit(1),
                num_lit(0),
            ],
        ),
        false,
    );
    let count_star = count_star();
    let round = scalar_fn(
        "ROUND",
        [
            scalar_fn(
                "FLOAT_DIV",
                [
                    scalar_fn("MULT", [lit("literal_double", 100.0), sum_case]),
                    count_star,
                ],
            ),
            num_lit(2),
        ],
    );

    let sql = render_expression(&round).expect("scalar-over-aggregate must render");
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
fn renders_cast_varchar_exasol_dialect_includes_length() {
    let expr = cast(json!({"type": "VARCHAR", "size": 100}));
    assert_eq!(
        render_expression_exasol(&expr),
        r#"CAST("X" AS VARCHAR(100))"#
    );
}

#[test]
fn cast_char_target_diverges_between_dialects() {
    let expr = cast_json(
        json!({"type": "CHAR", "size": 20, "characterSet": "ASCII"}),
        col("c_varchar"),
    );
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"CAST("C_VARCHAR" AS VARCHAR)"#
    );
    assert_eq!(
        render_expression_exasol(&expr),
        r#"CAST("C_VARCHAR" AS CHAR(20) ASCII)"#
    );
}

/// Scenario: A pushdown request with a missing or invalid Exasol type fails the query
#[test]
fn cast_to_an_incomplete_target_is_refused() {
    // exasol_type_tests pins which targets the parser refuses; this pins that CAST reaches it.
    let data_type = json!({"type": "VARCHAR"});
    match render_expression(&cast(data_type.clone())) {
        Err(UdfError::User(message)) => assert!(
            message.contains(&data_type.to_string()),
            "the refusal must name {data_type}: {message}"
        ),
        other => panic!("CAST to {data_type} must be refused: {other:?}"),
    }
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
        let expr = scalar_fn(exasol_name, [col("x")]);
        assert_eq!(
            render_expression_exasol(&expr),
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
            render_expression_exasol(&expr),
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
    let expr = scalar_fn(
        "SIGN",
        [scalar_fn(
            "SUB",
            [
                agg_item("SUM", Some("l_discount"), false),
                lit("literal_double", 0.5),
            ],
        )],
    );
    let exasol = render_expression_exasol(&expr);
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
            render_expression_exasol(&expr),
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
    assert_eq!(render_expression_exasol(&substr), r#"SUBSTR("S", 1, 3)"#);
    assert_eq!(render_expression(&substr).unwrap(), r#"substr("S", 1, 3)"#);
}

#[test]
fn instr_locate_with_start_arg_render_in_exasol_and_decline_in_datafusion() {
    let instr = scalar_fn("INSTR", [str_lit("hello"), str_lit("l"), num_lit(3)]);
    assert_eq!(render_expression_exasol(&instr), "INSTR('hello', 'l', 3)");
    assert!(render_expression(&instr).is_err());

    let locate = scalar_fn("LOCATE", [str_lit("l"), str_lit("hello"), num_lit(3)]);
    assert_eq!(render_expression_exasol(&locate), "LOCATE('l', 'hello', 3)");
    assert!(render_expression(&locate).is_err());
}

#[test]
fn renders_greatest_least_verbatim_in_exasol_dialect() {
    let greatest = scalar_fn("GREATEST", [col("a"), col("b"), col("c")]);
    assert_eq!(
        render_expression_exasol(&greatest),
        r#"GREATEST("A", "B", "C")"#
    );
    assert_eq!(
        render_expression(&greatest).unwrap(),
        r#"CASE WHEN "A" IS NULL OR "B" IS NULL OR "C" IS NULL THEN NULL ELSE greatest("A", "B", "C") END"#
    );

    let least = scalar_fn("LEAST", [col("x"), num_lit(0)]);
    assert_eq!(render_expression_exasol(&least), r#"LEAST("X", 0)"#);
    assert_eq!(
        render_expression(&least).unwrap(),
        r#"CASE WHEN "X" IS NULL OR 0 IS NULL THEN NULL ELSE least("X", 0) END"#
    );
}

#[test]
fn renders_nullifzero_zeroifnull_verbatim_in_exasol_dialect() {
    let nullifzero = scalar_fn("NULLIFZERO", [col("v")]);
    assert_eq!(render_expression_exasol(&nullifzero), r#"NULLIFZERO("V")"#);
    assert_eq!(render_expression(&nullifzero).unwrap(), r#"nullif("V", 0)"#);

    let zeroifnull = scalar_fn("ZEROIFNULL", [col("v")]);
    assert_eq!(render_expression_exasol(&zeroifnull), r#"ZEROIFNULL("V")"#);
    assert_eq!(
        render_expression(&zeroifnull).unwrap(),
        r#"coalesce("V", 0)"#
    );
}

#[test]
fn renders_nullif_verbatim_in_exasol_dialect() {
    // The verbatim gate renders arguments in its own dialect, so the nested MOD must stay
    // `MOD(a, b)`, not `%`.
    let expr = scalar_fn(
        "NULLIF",
        [scalar_fn("MOD", [col("id"), num_lit("5")]), num_lit("0")],
    );
    assert_eq!(
        render_expression_exasol(&expr),
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
        let expr = scalar_fn(field, [col("ts")]);
        assert_eq!(
            render_expression_exasol(&expr),
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
    assert_eq!(render_expression_exasol(&expr), r#"WEEK("D")"#);
    assert_eq!(
        render_expression(&expr).unwrap(),
        r#"date_part('week', "D")"#
    );
}

#[test]
fn renders_date_trunc_verbatim_in_exasol_dialect() {
    let expr = scalar_fn("DATE_TRUNC", [str_lit("month"), col("ts")]);
    assert_eq!(
        render_expression_exasol(&expr),
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
            render_expression_exasol(&bare),
            format!(r#"{exasol_name}("S")"#),
            "the Exasol dialect must render {exasol_name} verbatim"
        );
        assert!(
            render_expression(&bare).is_err(),
            "without a format the DataFusion dialect must decline {exasol_name}"
        );

        let formatted = scalar_fn(exasol_name, [col("s"), str_lit("YYYY-MM-DD")]);
        assert_eq!(
            render_expression_exasol(&formatted),
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
        assert_eq!(render_expression_exasol(&expr), exasol_sql);
    }
}

#[test]
fn renders_days_between_verbatim_in_exasol_dialect() {
    let expr = scalar_fn("DAYS_BETWEEN", [col("a"), col("b")]);
    assert_eq!(render_expression_exasol(&expr), r#"DAYS_BETWEEN("A", "B")"#);
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
        let expr = scalar_fn(name, [col("a"), col("b")]);
        assert_eq!(
            render_expression_exasol(&expr),
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
    fixtures.push(verbatim("DIV", vec![col("a"), col("b")]));

    fixtures.extend([
        shaped(
            "ADD",
            scalar_fn("ADD", vec![col("a"), col("b")]),
            r#"("A" + "B")"#,
        ),
        shaped(
            "SUB",
            scalar_fn("SUB", vec![col("a"), col("b")]),
            r#"("A" - "B")"#,
        ),
        shaped(
            "MULT",
            scalar_fn("MULT", vec![col("a"), col("b")]),
            r#"("A" * "B")"#,
        ),
        shaped(
            "FLOAT_DIV",
            scalar_fn("FLOAT_DIV", vec![col("a"), col("b")]),
            r#"("A" / "B")"#,
        ),
        shaped("NEG", scalar_fn("NEG", vec![col("a")]), r#"(-"A")"#),
        shaped(
            "MOD",
            scalar_fn("MOD", vec![col("a"), col("b")]),
            r#"MOD("A", "B")"#,
        ),
        shaped(
            "CONCAT",
            scalar_fn("CONCAT", vec![col("a"), col("b")]),
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
            "CASE",
            scalar_fn(
                "CASE",
                vec![
                    compare("predicate_greater", col("x"), num_lit(0)),
                    num_lit(1),
                    num_lit(0),
                ],
            ),
            r#"CASE WHEN ("X" > 0) THEN 1 ELSE 0 END"#,
        ),
    ]);

    let missing: Vec<&str> = SCALAR_FUNCTIONS
        .iter()
        .map(|f| f.name)
        .filter(|name| !fixtures.iter().any(|f| f.name == *name))
        .collect();
    assert!(
        missing.is_empty(),
        "every name declared in SCALAR_FUNCTIONS needs a sweep fixture; missing: \
         {missing:?}"
    );
    let undeclared: Vec<&str> = fixtures
        .iter()
        .map(|f| f.name)
        .filter(|name| scalar_function(name).is_none())
        .collect();
    assert!(
        undeclared.is_empty(),
        "every sweep fixture must name a declared function; undeclared: {undeclared:?}"
    );
    assert_eq!(
        fixtures.len(),
        SCALAR_FUNCTIONS.len(),
        "the fixture map and the declaration must line up one to one; a duplicated row on \
         either side is the only way both subset checks above can pass at different sizes"
    );

    let mut swept: Vec<String> = Vec::new();

    for ScalarFunction {
        name: declared_name,
        exasol: form,
        ..
    } in SCALAR_FUNCTIONS
    {
        let fixture = fixtures
            .iter()
            .find(|f| f.name == *declared_name)
            .expect("fixture completeness is asserted above");
        // The only call that reaches the per-name arms: Exasol `VerbatimCall`s return
        // ahead of them.
        if let Err(err) = render_expression(&fixture.node) {
            panic!(
                "{declared_name} is declared in SCALAR_FUNCTIONS, which gates BOTH \
                 dialects from one lookup, so it MUST render in the DataFusion dialect too; \
                 it declined with {err:?}. A declared name that has lost its per-name arm \
                 still renders in the Exasol dialect through the verbatim gate, so this is \
                 the only assertion that catches it."
            );
        }
        let rendered = render_expression_exasol(&fixture.node);
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
                    .map(render_expression_exasol)
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
            cast_json(json!({"type": "VARCHAR", "size": 50}), col("v")),
            r#"CAST("V" AS VARCHAR(50))"#,
            r#"CAST("V" AS VARCHAR)"#,
        ),
        (
            timestamp_lit("2024-03-01 12:34:56.789"),
            "TIMESTAMP '2024-03-01 12:34:56.789'",
            "arrow_cast('2024-03-01 12:34:56.789', 'Timestamp(Microsecond, None)')",
        ),
        (
            lit("literal_timestamp_utc", "2024-03-01 12:34:56.789"),
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
        let rendered = render_expression_exasol(&node);
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
            CHECKED_INT_DIV_FN,
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
        let expr = scalar_fn(name, [col("a"), num_lit(1)]);
        let expected = format!(r#"("A" {op} 1)"#);
        assert_eq!(
            render_expression(&expr).unwrap(),
            expected,
            "{name} DataFusion dialect"
        );
        assert_eq!(
            render_expression_exasol(&expr),
            expected,
            "{name} Exasol dialect"
        );
    }

    let neg = scalar_fn("NEG", [col("a")]);
    let expected_neg = r#"(-"A")"#;
    assert_eq!(
        render_expression(&neg).unwrap(),
        expected_neg,
        "NEG DataFusion dialect"
    );
    assert_eq!(
        render_expression_exasol(&neg),
        expected_neg,
        "NEG Exasol dialect"
    );
}

#[test]
fn float_div_renders_checked_division_call_only_in_the_datafusion_dialect() {
    let float_div = scalar_fn("FLOAT_DIV", [col("a"), num_lit(1)]);
    assert_eq!(
        render_expression(&float_div).unwrap(),
        format!(r#"{CHECKED_FLOAT_DIV_FN}("A", 1)"#),
        "FLOAT_DIV DataFusion dialect"
    );
    assert_eq!(
        render_expression_exasol(&float_div),
        r#"("A" / 1)"#,
        "FLOAT_DIV Exasol dialect"
    );
}

#[test]
fn div_renders_checked_integer_division_only_in_the_datafusion_dialect() {
    let div = scalar_fn("DIV", [col("a"), num_lit(2)]);
    assert_eq!(
        render_expression(&div).unwrap(),
        format!(r#"{CHECKED_INT_DIV_FN}("A", 2)"#),
        "DIV DataFusion dialect"
    );
    assert_eq!(
        render_expression_exasol(&div),
        r#"DIV("A", 2)"#,
        "DIV Exasol dialect"
    );
}

#[test]
fn non_timestamp_literals_render_identically_in_both_dialects() {
    let cases: [(Json, &str); 6] = [
        (null_lit(), "NULL"),
        (bool_lit(true), "TRUE"),
        (bool_lit(false), "FALSE"),
        (str_lit("it's"), "'it''s'"),
        (num_lit(42), "42"),
        (date_lit("2024-01-15"), "DATE '2024-01-15'"),
    ];
    for (node, expected) in cases {
        let node_type = node["type"].as_str().unwrap();
        assert_eq!(
            render_expression(&node).unwrap(),
            expected,
            "{node_type} DataFusion dialect"
        );
        assert_eq!(
            render_expression_exasol(&node),
            expected,
            "{node_type} Exasol dialect"
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
        let node = lit("literal_double", value);
        assert_eq!(render_expression(&node).unwrap(), datafusion);
        assert_eq!(render_expression_exasol(&node), exasol);
    }
}

#[test]
fn a_binary_literal_renders_only_as_hex() {
    let variable = binary_lit("0a1b");
    let fixed = fixed_binary_lit("0a1b", 2);

    assert_eq!(render_expression(&variable).unwrap(), "X'0a1b'");
    assert_eq!(
        render_expression(&fixed).unwrap(),
        "arrow_cast(X'0a1b', 'FixedSizeBinary(2)')"
    );
    assert!(render_expression(&binary_lit("0a'")).is_err());
}

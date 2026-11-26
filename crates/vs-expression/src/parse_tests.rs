use super::*;
use crate::test_support::*;
use serde_json::json;

fn parse_error(node: &Json) -> String {
    match parse_expression(node, "filter") {
        Err(UdfError::User(message)) => message,
        other => panic!("{node} must fail to parse, got {other:?}"),
    }
}

#[test]
fn parses_a_column_with_its_table_and_alias() {
    assert_eq!(
        parse_expression(&aliased_col("T", "A", "x"), "filter").unwrap(),
        Expr::Column(Column {
            name: "x".into(),
            table_name: Some("T".into()),
            table_alias: Some("A".into()),
        })
    );
}

#[test]
fn a_cast_sent_as_a_scalar_function_parses_as_a_cast() {
    let mut node = cast_to(ExasolType::Date, col("X"));
    node["type"] = "function_scalar".into();

    assert_eq!(
        parse_expression(&node, "filter").unwrap(),
        parse_expression(&cast_to(ExasolType::Date, col("X")), "filter").unwrap()
    );
}

#[test]
fn a_literal_parses_to_the_value_its_type_names() {
    let text = |kind: &str| lit(kind, "2024-03-01 09:00:00.000");
    let value = || "2024-03-01 09:00:00.000".to_string();
    for (node, literal) in [
        (null_lit(), Literal::Null),
        (bool_lit(true), Literal::Bool(true)),
        (str_lit(""), Literal::String(String::new())),
        (
            lit("literal_double", "1e+300"),
            Literal::Double("1e+300".into()),
        ),
        (num_lit("-0.5"), Literal::ExactNumeric("-0.5".into())),
        (date_lit("2024-03-01"), Literal::Date("2024-03-01".into())),
        (text("literal_timestamp"), Literal::Timestamp(value())),
        (
            text("literal_timestamp_utc"),
            Literal::TimestampUtc(value()),
        ),
        (
            text("literal_timestamputc"),
            Literal::LocalTimeZoneTimestamp(value()),
        ),
    ] {
        assert_eq!(
            parse_expression(&node, "filter").unwrap(),
            Expr::Literal(literal),
            "{node}"
        );
    }
}

/// Scenario: A literal whose value is not of its type fails the request
#[test]
fn a_literal_whose_value_is_not_of_its_type_fails_naming_the_field() {
    for (node, problem) in [
        (lit("literal_bool", "banana"), "is not a boolean"),
        (lit("literal_bool", "TRUE"), "is not a boolean"),
        (lit("literal_bool", 1), "is not a boolean"),
        (json!({"type": "literal_bool"}), "is missing"),
        (lit("literal_exactnumeric", 1), "is not a string"),
        (lit("literal_double", 1.5), "is not a string"),
        (json!({"type": "literal_string"}), "is missing"),
        (lit("literal_string", Json::Null), "is missing"),
        (json!({"type": "literal_date"}), "is missing"),
        (lit("literal_timestamp", true), "is not a string"),
    ] {
        assert_eq!(
            parse_error(&node),
            format!("the pushdown request's filter.value {problem}"),
            "{node}"
        );
    }
}

#[test]
fn an_aggregate_without_arguments_is_a_star_call() {
    assert_eq!(
        parse_expression(&count_star(), "selectList[0]").unwrap(),
        Expr::Aggregate(AggregateCall {
            name: "COUNT".into(),
            arguments: Vec::new(),
            distinct: false,
        })
    );
}

#[test]
fn a_malformed_node_fails_naming_its_field() {
    let cases = [
        (
            json!("x"),
            "the pushdown request's filter is not an expression node",
        ),
        (
            json!({"type": "column"}),
            "the pushdown request's filter.name is missing",
        ),
        (
            json!({"type": "function_scalar", "arguments": [col("A")]}),
            "the pushdown request's filter.name is missing",
        ),
        (
            json!({"type": "predicate_equal", "left": col("A")}),
            "the pushdown request's filter.right is missing",
        ),
        (
            and([predicate_equal(
                col("A"),
                json!({"type": "literal_string", "value": [1]}),
            )]),
            "the pushdown request's filter.expressions[0].right.value is not a string",
        ),
        (
            json!({"type": "predicate_like_regexp", "expression": col("A")}),
            "the pushdown request's filter.type names the node type predicate_like_regexp, which the adapter does not translate",
        ),
        (
            json!({"type": "function_scalar_cast", "arguments": [col("A")]}),
            "the pushdown request's filter.dataType is missing",
        ),
        (
            json!({"type": "function_scalar_extract", "toExtract": "YEAR", "arguments": []}),
            "the pushdown request's filter.arguments holds 0 arguments, not 1",
        ),
        (
            case_when([bool_lit(true)], [num_lit(1), num_lit(2), num_lit(3)]),
            "the pushdown request's filter.results holds 3 results for 1 WHEN values, not one per WHEN plus an optional ELSE",
        ),
        (
            json_value_as(
                col("D"),
                "$.a",
                ExasolType::WIDEST_VARCHAR,
                json_behavior("SOMETIMES"),
                json_behavior("NULL"),
            ),
            "the pushdown request's filter.emptyBehavior.type names the behavior SOMETIMES",
        ),
    ];
    for (node, expected) in cases {
        assert_eq!(parse_error(&node), expected, "{node}");
    }
}

/// Scenario: A function call the adapter does not translate fails the request
#[test]
fn a_call_of_an_unknown_function_or_of_a_wrong_argument_count_fails_naming_the_field() {
    let cases = [
        (
            scalar_fn("NO_SUCH_FN", [col("A")]),
            "the pushdown request's filter.name names the scalar function NO_SUCH_FN, which the adapter does not translate",
        ),
        (
            scalar_fn("UPPER(A)) OR (1=1", [col("A")]),
            "the pushdown request's filter.name names the scalar function UPPER(A)) OR (1=1, which the adapter does not translate",
        ),
        (
            scalar_fn("UPPER", []),
            "the pushdown request's filter.arguments holds 0 arguments, but UPPER takes 1",
        ),
        (
            json!({"type": "function_scalar", "name": "LOWER"}),
            "the pushdown request's filter.arguments holds 0 arguments, but LOWER takes 1",
        ),
        (
            scalar_fn("UPPER", [col("A"), col("A"), col("A")]),
            "the pushdown request's filter.arguments holds 3 arguments, but UPPER takes 1",
        ),
        (
            scalar_fn("SUBSTR", [col("A")]),
            "the pushdown request's filter.arguments holds 1 arguments, but SUBSTR takes 2 to 3",
        ),
        (
            scalar_fn("CONCAT", []),
            "the pushdown request's filter.arguments holds 0 arguments, but CONCAT takes at least 1",
        ),
        (
            scalar_fn("CURRENT_DATE", [num_lit(3)]),
            "the pushdown request's filter.arguments holds 1 arguments, but CURRENT_DATE takes 0",
        ),
        (
            agg_item_expr("NO_SUCH_AGG", col("A"), false),
            "the pushdown request's filter.name names the aggregate function NO_SUCH_AGG, which the adapter does not translate",
        ),
        (
            agg_item_expr("COUNT(*) + MAX", col("A"), false),
            "the pushdown request's filter.name names the aggregate function COUNT(*) + MAX, which the adapter does not translate",
        ),
        (
            json!({"type": "function_aggregate", "name": "SUM"}),
            "the pushdown request's filter.arguments holds 0 arguments, but SUM takes 1",
        ),
        (
            json!({"type": "function_aggregate", "name": "SUM", "arguments": [col("A"), col("B")]}),
            "the pushdown request's filter.arguments holds 2 arguments, but SUM takes 1",
        ),
        (
            json!({"type": "function_aggregate", "name": "COUNT", "arguments": [col("A"), col("B")]}),
            "the pushdown request's filter.arguments holds 2 arguments, but COUNT takes 0 to 1",
        ),
    ];
    for (node, expected) in cases {
        assert_eq!(parse_error(&node), expected, "{node}");
    }
}

#[test]
fn a_call_of_each_argument_count_exasol_sends_parses() {
    let cases = [
        count_star(),
        agg_item_expr("COUNT", col("A"), true),
        agg_item_expr("var_pop", col("A"), false),
        scalar_fn("CURRENT_DATE", []),
        scalar_fn("CURRENT_TIMESTAMP", []),
        scalar_fn("SYSTIMESTAMP", [num_lit(6)]),
        scalar_fn("upper", [col("A")]),
        scalar_fn("INSTR", [col("A"), str_lit("a"), num_lit(1), num_lit(2)]),
        scalar_fn("CONCAT", [col("A"), col("B"), col("C"), col("D")]),
        scalar_fn("GREATEST", [col("A")]),
    ];
    for node in cases {
        assert!(parse_expression(&node, "filter").is_ok(), "{node}");
    }
}

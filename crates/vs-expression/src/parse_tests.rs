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
fn an_absent_or_null_literal_value_is_null() {
    for node in [
        json!({"type": "literal_string"}),
        json!({"type": "literal_string", "value": null}),
    ] {
        assert_eq!(
            parse_expression(&node, "filter").unwrap(),
            Expr::Literal(Literal::new(LiteralKind::String, Scalar::Null))
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
            json!({"type": "predicate_equal", "left": col("A")}),
            "the pushdown request's filter.right is missing",
        ),
        (
            and([predicate_equal(
                col("A"),
                json!({"type": "literal_string", "value": [1]}),
            )]),
            "the pushdown request's filter.expressions[0].right.value is not a scalar",
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

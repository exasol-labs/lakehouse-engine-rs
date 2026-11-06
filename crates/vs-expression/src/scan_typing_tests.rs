use super::*;
use crate::render_scan_expression;
use serde_json::json;

struct Columns(Vec<(&'static str, ExasolType)>);

impl ColumnTypeLookup for Columns {
    fn column_type(&self, column: &Json) -> Option<ExasolType> {
        let name = column.get("name")?.as_str()?.to_uppercase();
        self.0.iter().find(|(n, _)| *n == name).map(|(_, t)| *t)
    }
}

fn columns() -> Columns {
    Columns(vec![
        ("D", ExasolType::Date),
        ("TS", ExasolType::timestamp(6)),
        ("B", ExasolType::Boolean),
        ("F", ExasolType::DoublePrecision),
        ("N", ExasolType::decimal(9, 2)),
        ("I", ExasolType::decimal(9, 0)),
        ("S", ExasolType::WIDEST_VARCHAR),
        ("S10", ExasolType::varchar(10)),
    ])
}

fn col(name: &str) -> Json {
    json!({"type": "column", "name": name})
}

fn cast(arg: Json, data_type: Json) -> Json {
    json!({"type": "function_scalar_cast", "dataType": data_type, "arguments": [arg]})
}

fn varchar(size: u32) -> Json {
    json!({"type": "VARCHAR", "size": size, "characterSet": "UTF8"})
}

fn varchar_ascii(size: u32) -> Json {
    json!({"type": "VARCHAR", "size": size, "characterSet": "ASCII"})
}

fn char_type(size: u32) -> Json {
    json!({"type": "CHAR", "size": size, "characterSet": "UTF8"})
}

fn like(subject: Json) -> Json {
    json!({"type": "predicate_like", "expression": subject, "pattern": {"type": "literal_string", "value": "2024%"}})
}

fn scalar(name: &str, args: Vec<Json>) -> Json {
    json!({"type": "function_scalar", "name": name, "arguments": args})
}

fn render(expr: &Json) -> Result<String, UdfError> {
    render_scan_expression(expr, &columns())
}

#[test]
fn session_formatted_sources_decline_at_every_text_position() {
    for source in ["D", "TS", "F", "N"] {
        let cases = [
            ("explicit CAST", cast(col(source), varchar(2_000_000))),
            ("LIKE subject", like(col(source))),
            (
                "|| operand",
                scalar(
                    "CONCAT",
                    vec![col(source), json!({"type": "literal_string", "value": "x"})],
                ),
            ),
            (
                "string-function argument",
                scalar("UPPER", vec![col(source)]),
            ),
            (
                "nested",
                scalar(
                    "SUBSTR",
                    vec![
                        cast(col(source), varchar(2_000_000)),
                        json!({"type": "literal_exactnumeric", "value": 1}),
                    ],
                ),
            ),
        ];
        for (position, expr) in cases {
            assert!(
                render(&expr).is_err(),
                "{source} as {position} must decline, rendered {:?}",
                render(&expr)
            );
        }
    }
}

#[test]
fn integer_and_text_sources_keep_their_pushdown() {
    assert_eq!(
        render(&cast(col("I"), varchar(2_000_000))).unwrap(),
        r#"CAST(CAST(CAST("I" AS DECIMAL(38,0)) AS VARCHAR) AS VARCHAR)"#
    );
    assert_eq!(
        render(&like(col("I"))).unwrap(),
        r#"(CAST(CAST("I" AS DECIMAL(38,0)) AS VARCHAR) LIKE '2024%')"#
    );
    assert_eq!(render(&like(col("S"))).unwrap(), r#"("S" LIKE '2024%')"#);
    assert_eq!(
        render(&scalar("UPPER", vec![col("I")])).unwrap(),
        r#"upper(CAST(CAST("I" AS DECIMAL(38,0)) AS VARCHAR))"#
    );
}

#[test]
fn a_computed_integer_converts_like_a_column() {
    let sum = scalar(
        "ADD",
        vec![
            col("I"),
            json!({"type": "literal_exactnumeric", "value": 2023}),
        ],
    );
    assert!(render(&cast(sum, varchar(2_000_000))).is_ok());
    let quotient = scalar(
        "FLOAT_DIV",
        vec![
            col("I"),
            json!({"type": "literal_exactnumeric", "value": 2}),
        ],
    );
    assert!(render(&cast(quotient, varchar(2_000_000))).is_err());
}

#[test]
fn a_boolean_column_converts_to_exasol_casing() {
    assert_eq!(
        render(&cast(col("B"), varchar(2_000_000))).unwrap(),
        r#"CAST((CASE "B" WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END) AS VARCHAR)"#
    );
    assert!(render(&cast(col("B"), varchar(4))).is_err());
}

#[test]
fn sized_ascii_and_char_targets_push_only_when_the_source_guarantees_the_result() {
    assert!(render(&cast(col("S"), varchar(10))).is_err());
    assert!(render(&cast(col("S"), varchar_ascii(2_000_000))).is_err());
    assert!(render(&cast(col("S"), char_type(10))).is_err());
    assert!(render(&cast(col("S10"), varchar(10))).is_ok());
    assert!(render(&cast(col("I"), varchar(10))).is_ok());
    assert!(render(&cast(col("I"), varchar(9))).is_err());
    assert!(render(&cast(col("I"), varchar_ascii(10))).is_ok());
    assert!(render(&cast(col("I"), char_type(10))).is_err());
}

#[test]
fn text_parsed_by_session_settings_declines() {
    let text = scalar(
        "CONCAT",
        vec![
            json!({"type": "literal_string", "value": "2024-01-0"}),
            col("I"),
        ],
    );
    for target in [
        json!({"type": "DATE"}),
        json!({"type": "TIMESTAMP", "withLocalTimeZone": false, "fractionalSecondsPrecision": 3}),
        json!({"type": "DOUBLE"}),
        json!({"type": "DECIMAL", "precision": 10, "scale": 1}),
    ] {
        assert!(
            render(&cast(text.clone(), target.clone())).is_err(),
            "{target}"
        );
    }
    assert!(
        render(&cast(
            text,
            json!({"type": "DECIMAL", "precision": 10, "scale": 0})
        ))
        .is_ok()
    );
}

#[test]
fn a_comparison_without_text_conversion_is_untouched() {
    let filter = json!({"type": "predicate_less", "left": col("D"), "right": {"type": "literal_date", "value": "2025-01-01"}});
    assert_eq!(render(&filter).unwrap(), r#"("D" < DATE '2025-01-01')"#);
}

#[test]
fn comparing_text_with_a_date_or_number_declines() {
    let text = json!({"type": "literal_string", "value": "2024-01-01"});
    for column in ["D", "TS", "N", "F", "I"] {
        let filter = json!({"type": "predicate_equal", "left": col(column), "right": text});
        assert!(render(&filter).is_err(), "{column} = text must decline");
    }
    let filter = json!({"type": "predicate_equal", "left": col("S"), "right": text});
    assert!(render(&filter).is_ok());
}

#[test]
fn an_integer_compares_with_a_digits_only_literal() {
    let filter = |column: &str, value: &str| json!({"type": "predicate_equal", "left": col(column), "right": {"type": "literal_string", "value": value}});
    assert!(render(&filter("I", "3")).is_ok(), "I = '3' must render");
    for (column, value) in [("I", "3.5"), ("N", "3"), ("D", "2024-01-01")] {
        assert!(
            render(&filter(column, value)).is_err(),
            "{column} = '{value}' must decline"
        );
    }
}

fn text(value: &str) -> Json {
    json!({"type": "literal_string", "value": value})
}

fn searched_case(results: Vec<Json>) -> Json {
    let condition = json!({"type": "predicate_is_not_null", "expression": col("I")});
    json!({"type": "function_scalar_case", "arguments": [condition], "results": results})
}

#[test]
fn text_converted_implicitly_outside_a_comparison_declines() {
    let cases = [
        (
            "GREATEST",
            scalar("GREATEST", vec![col("D"), text("2024-01-02")]),
        ),
        ("LEAST", scalar("LEAST", vec![col("F"), text("1.5")])),
        (
            "NULLIF",
            scalar("NULLIF", vec![col("D"), text("2024-01-01")]),
        ),
        (
            "CASE result",
            searched_case(vec![col("D"), text("2024-01-02")]),
        ),
        (
            "simple CASE",
            json!({"type": "function_scalar_case", "basis": col("D"),
                "arguments": [text("2024-01-01")],
                "results": [{"type": "literal_exactnumeric", "value": "1"}, {"type": "literal_exactnumeric", "value": "0"}]}),
        ),
        (
            "date function",
            scalar("DAYS_BETWEEN", vec![col("D"), text("2024-01-01")]),
        ),
        ("arithmetic", scalar("ADD", vec![col("I"), text("1")])),
        (
            "numeric parameter",
            scalar("SUBSTR", vec![col("S"), text("2")]),
        ),
        (
            "EXTRACT",
            json!({"type": "function_scalar_extract", "toExtract": "YEAR", "arguments": [text("2024-01-01")]}),
        ),
        (
            "boolean operand",
            json!({"type": "predicate_and", "expressions": [text("TRUE"), {"type": "literal_bool", "value": true}]}),
        ),
    ];
    for (label, expr) in cases {
        assert!(render(&expr).is_err(), "{label} must decline");
    }
}

#[test]
fn text_where_exasol_takes_text_keeps_its_pushdown() {
    let cases = [
        (
            "GREATEST over text",
            scalar("GREATEST", vec![col("S"), text("m")]),
        ),
        (
            "CASE over text and NULL",
            searched_case(vec![text("a"), json!({"type": "literal_null"})]),
        ),
        (
            "simple CASE over text",
            json!({"type": "function_scalar_case", "basis": col("S"),
                "arguments": [text("a")],
                "results": [{"type": "literal_exactnumeric", "value": "1"}, {"type": "literal_exactnumeric", "value": "0"}]}),
        ),
        (
            "DATE_TRUNC unit",
            scalar("DATE_TRUNC", vec![text("month"), col("TS")]),
        ),
        (
            "TO_DATE format",
            scalar("TO_DATE", vec![col("S"), text("YYYY-MM-DD")]),
        ),
        (
            "LPAD fill",
            scalar(
                "LPAD",
                vec![
                    col("S"),
                    json!({"type": "literal_exactnumeric", "value": "5"}),
                    text("x"),
                ],
            ),
        ),
        (
            "integer and digits",
            json!({"type": "predicate_equal", "left": col("I"), "right": text("3")}),
        ),
    ];
    for (label, expr) in cases {
        assert!(
            render(&expr).is_ok(),
            "{label} must render: {:?}",
            render(&expr)
        );
    }
}

#[test]
fn exact_expression_type_is_the_type_the_request_states() {
    let sum = scalar(
        "ADD",
        vec![
            col("I"),
            json!({"type": "literal_exactnumeric", "value": 1}),
        ],
    );
    let cases = [
        (
            "CAST",
            cast(col("I"), varchar(10)),
            Some(ExasolType::varchar(10)),
        ),
        ("column", col("N"), Some(ExasolType::decimal(9, 2))),
        ("other node", sum, None),
    ];
    for (label, node, expected) in cases {
        assert_eq!(
            exact_expression_type(&node, &columns()),
            expected,
            "{label}"
        );
    }
}

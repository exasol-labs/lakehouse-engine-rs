use super::*;
use crate::exasol_type::exasol_type_to_json;
use crate::render_scan_expression;
use crate::test_support::*;

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

fn cast(arg: Json, target: ExasolType) -> Json {
    cast_json(exasol_type_to_json(&target), arg)
}

fn ascii_varchar(size: u32) -> ExasolType {
    ExasolType::Varchar { size, ascii: true }
}

fn date_like(subject: Json) -> Json {
    like(subject, "2024%")
}

fn render(expr: &Json) -> Result<String, UdfError> {
    render_scan_expression(expr, &columns())
}

#[test]
fn session_formatted_sources_decline_at_every_text_position() {
    for source in ["D", "TS", "F", "N"] {
        let cases = [
            (
                "explicit CAST",
                cast(col(source), ExasolType::WIDEST_VARCHAR),
            ),
            ("LIKE subject", date_like(col(source))),
            (
                "|| operand",
                scalar_fn("CONCAT", [col(source), str_lit("x")]),
            ),
            (
                "string-function argument",
                scalar_fn("UPPER", [col(source)]),
            ),
            (
                "nested",
                scalar_fn(
                    "SUBSTR",
                    [cast(col(source), ExasolType::WIDEST_VARCHAR), num_lit(1)],
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
        render(&cast(col("I"), ExasolType::WIDEST_VARCHAR)).unwrap(),
        r#"CAST(CAST(CAST("I" AS DECIMAL(38,0)) AS VARCHAR) AS VARCHAR)"#
    );
    assert_eq!(
        render(&date_like(col("I"))).unwrap(),
        r#"(CAST(CAST("I" AS DECIMAL(38,0)) AS VARCHAR) LIKE '2024%')"#
    );
    assert_eq!(
        render(&date_like(col("S"))).unwrap(),
        r#"("S" LIKE '2024%')"#
    );
    assert_eq!(
        render(&scalar_fn("UPPER", [col("I")])).unwrap(),
        r#"upper(CAST(CAST("I" AS DECIMAL(38,0)) AS VARCHAR))"#
    );
}

#[test]
fn a_computed_integer_converts_like_a_column() {
    let sum = scalar_fn("ADD", [col("I"), num_lit(2023)]);
    assert!(render(&cast(sum, ExasolType::WIDEST_VARCHAR)).is_ok());
    let quotient = scalar_fn("FLOAT_DIV", [col("I"), num_lit(2)]);
    assert!(render(&cast(quotient, ExasolType::WIDEST_VARCHAR)).is_err());
}

#[test]
fn a_boolean_column_converts_to_exasol_casing() {
    assert_eq!(
        render(&cast(col("B"), ExasolType::WIDEST_VARCHAR)).unwrap(),
        r#"CAST((CASE "B" WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END) AS VARCHAR)"#
    );
    assert!(render(&cast(col("B"), ExasolType::varchar(4))).is_err());
}

#[test]
fn sized_ascii_and_char_targets_push_only_when_the_source_guarantees_the_result() {
    assert!(render(&cast(col("S"), ExasolType::varchar(10))).is_err());
    assert!(render(&cast(col("S"), ascii_varchar(2_000_000))).is_err());
    assert!(render(&cast(col("S"), ExasolType::char(10))).is_err());
    assert!(render(&cast(col("S10"), ExasolType::varchar(10))).is_ok());
    assert!(render(&cast(col("I"), ExasolType::varchar(10))).is_ok());
    assert!(render(&cast(col("I"), ExasolType::varchar(9))).is_err());
    assert!(render(&cast(col("I"), ascii_varchar(10))).is_ok());
    assert!(render(&cast(col("I"), ExasolType::char(10))).is_err());
}

#[test]
fn text_parsed_by_session_settings_declines() {
    let text = scalar_fn("CONCAT", [str_lit("2024-01-0"), col("I")]);
    for target in [
        ExasolType::Date,
        ExasolType::timestamp(3),
        ExasolType::DoublePrecision,
        ExasolType::decimal(10, 1),
    ] {
        assert!(render(&cast(text.clone(), target)).is_err(), "{target}");
    }
    assert!(render(&cast(text, ExasolType::decimal(10, 0))).is_ok());
}

#[test]
fn a_comparison_without_text_conversion_is_untouched() {
    let filter = compare("predicate_less", col("D"), date_lit("2025-01-01"));
    assert_eq!(render(&filter).unwrap(), r#"("D" < DATE '2025-01-01')"#);
}

#[test]
fn comparing_text_with_a_date_or_number_declines() {
    for column in ["D", "TS", "N", "F", "I"] {
        let filter = predicate_equal(col(column), str_lit("2024-01-01"));
        assert!(render(&filter).is_err(), "{column} = text must decline");
    }
    let filter = predicate_equal(col("S"), str_lit("2024-01-01"));
    assert!(render(&filter).is_ok());
}

#[test]
fn an_integer_compares_with_a_digits_only_literal() {
    let filter = |column: &str, value: &str| predicate_equal(col(column), str_lit(value));
    assert!(render(&filter("I", "3")).is_ok(), "I = '3' must render");
    for (column, value) in [("I", "3.5"), ("N", "3"), ("D", "2024-01-01")] {
        assert!(
            render(&filter(column, value)).is_err(),
            "{column} = '{value}' must decline"
        );
    }
}

fn searched_case(results: [Json; 2]) -> Json {
    case_when([is_not_null(col("I"))], results)
}

#[test]
fn text_converted_implicitly_outside_a_comparison_declines() {
    let cases = [
        (
            "GREATEST",
            scalar_fn("GREATEST", [col("D"), str_lit("2024-01-02")]),
        ),
        ("LEAST", scalar_fn("LEAST", [col("F"), str_lit("1.5")])),
        (
            "NULLIF",
            scalar_fn("NULLIF", [col("D"), str_lit("2024-01-01")]),
        ),
        (
            "CASE result",
            searched_case([col("D"), str_lit("2024-01-02")]),
        ),
        (
            "simple CASE",
            case_of(col("D"), [str_lit("2024-01-01")], [num_lit(1), num_lit(0)]),
        ),
        (
            "date function",
            scalar_fn("DAYS_BETWEEN", [col("D"), str_lit("2024-01-01")]),
        ),
        ("arithmetic", scalar_fn("ADD", [col("I"), str_lit("1")])),
        (
            "numeric parameter",
            scalar_fn("SUBSTR", [col("S"), str_lit("2")]),
        ),
        ("EXTRACT", extract("YEAR", str_lit("2024-01-01"))),
        ("boolean operand", and([str_lit("TRUE"), bool_lit(true)])),
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
            scalar_fn("GREATEST", [col("S"), str_lit("m")]),
        ),
        (
            "CASE over text and NULL",
            searched_case([str_lit("a"), null_lit()]),
        ),
        (
            "simple CASE over text",
            case_of(col("S"), [str_lit("a")], [num_lit(1), num_lit(0)]),
        ),
        (
            "DATE_TRUNC unit",
            scalar_fn("DATE_TRUNC", [str_lit("month"), col("TS")]),
        ),
        (
            "TO_DATE format",
            scalar_fn("TO_DATE", [col("S"), str_lit("YYYY-MM-DD")]),
        ),
        (
            "LPAD fill",
            scalar_fn("LPAD", [col("S"), num_lit(5), str_lit("x")]),
        ),
        (
            "integer and digits",
            predicate_equal(col("I"), str_lit("3")),
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
    let sum = scalar_fn("ADD", [col("I"), num_lit(1)]);
    let cases = [
        (
            "CAST",
            cast(col("I"), ExasolType::varchar(10)),
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

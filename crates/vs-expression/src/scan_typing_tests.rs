use super::*;
use crate::test_support::*;
use crate::{SqlFragment, render_scan_expression};

fn columns() -> TypedColumns {
    typed_columns(&[
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

fn date_like(subject: Json) -> Json {
    like(subject, "2024%")
}

fn render(expr: &Json) -> Result<String, UdfError> {
    render_scan_expression(expr, &columns()).map(|fragment| fragment.sql)
}

#[test]
fn session_formatted_sources_decline_at_every_text_position() {
    for source in ["D", "TS", "F", "N"] {
        let cases = [
            (
                "explicit CAST",
                cast_to(ExasolType::WIDEST_VARCHAR, col(source)),
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
                    [cast_to(ExasolType::WIDEST_VARCHAR, col(source)), num_lit(1)],
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
        render(&cast_to(ExasolType::WIDEST_VARCHAR, col("I"))).unwrap(),
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
    assert!(render(&cast_to(ExasolType::WIDEST_VARCHAR, sum)).is_ok());
    let quotient = scalar_fn("FLOAT_DIV", [col("I"), num_lit(2)]);
    assert!(render(&cast_to(ExasolType::WIDEST_VARCHAR, quotient)).is_err());
}

#[test]
fn a_boolean_column_converts_to_exasol_casing() {
    assert_eq!(
        render(&cast_to(ExasolType::WIDEST_VARCHAR, col("B"))).unwrap(),
        r#"CAST((CASE "B" WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END) AS VARCHAR)"#
    );
    assert!(render(&cast_to(ExasolType::varchar(4), col("B"))).is_err());
}

#[test]
fn sized_ascii_and_char_targets_push_only_when_the_source_guarantees_the_result() {
    assert!(render(&cast_to(ExasolType::varchar(10), col("S"))).is_err());
    assert!(
        render(&cast_to(
            ExasolType::Varchar {
                size: 2_000_000,
                ascii: true
            },
            col("S")
        ))
        .is_err()
    );
    assert!(render(&cast_to(ExasolType::char(10), col("S"))).is_err());
    assert!(render(&cast_to(ExasolType::varchar(10), col("S10"))).is_ok());
    assert!(render(&cast_to(ExasolType::varchar(10), col("I"))).is_ok());
    assert!(render(&cast_to(ExasolType::varchar(9), col("I"))).is_err());
    assert!(
        render(&cast_to(
            ExasolType::Varchar {
                size: 10,
                ascii: true
            },
            col("I")
        ))
        .is_ok()
    );
    assert!(render(&cast_to(ExasolType::char(10), col("I"))).is_err());
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
        assert!(render(&cast_to(target, text.clone())).is_err(), "{target}");
    }
    assert!(render(&cast_to(ExasolType::decimal(10, 0), text)).is_ok());
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
            cast_to(ExasolType::varchar(10), col("I")),
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

#[test]
fn a_scan_fragment_carries_the_column_or_cast_type_it_emits() {
    let typed = |expr: &Json| render_scan_expression(expr, &columns()).unwrap().ty;
    assert_eq!(typed(&col("D")), Some(ExasolType::Date));
    assert_eq!(
        typed(&cast_to(ExasolType::decimal(12, 0), col("I"))),
        Some(ExasolType::decimal(12, 0))
    );
    assert_eq!(typed(&scalar_fn("UPPER", [col("S")])), None);
}

#[test]
fn a_scan_fragment_of_a_sized_text_cast_is_any_text() {
    let fragment =
        render_scan_expression(&cast_to(ExasolType::varchar(20), col("S10")), &columns());
    assert_eq!(fragment.unwrap().ty, Some(ExasolType::WIDEST_VARCHAR));
}

#[test]
fn an_exasol_fragment_keeps_its_cast_target_and_column_type() {
    let typed = |expr: &Json| crate::render_exasol_fragment(expr, &columns()).unwrap();
    assert_eq!(
        typed(&cast_to(ExasolType::char(3), col("S10"))),
        SqlFragment::new(r#"CAST("S10" AS CHAR(3))"#, Some(ExasolType::char(3)))
    );
    assert_eq!(typed(&col("N")).ty, Some(ExasolType::decimal(9, 2)));
    assert_eq!(typed(&null_lit()).ty, None);
}

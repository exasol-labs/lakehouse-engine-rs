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

/// Exasol converts these by the session's NLS settings: a date or non-integer number at a text
/// position, or text compared with a date or number.
#[test]
fn session_dependent_text_conversions_decline() {
    let mut cases: Vec<(String, Json)> = Vec::new();
    for source in ["D", "TS", "F", "N"] {
        let positions = [
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
        cases.extend(positions.map(|(position, expr)| (format!("{source} as {position}"), expr)));
    }
    for (column, value) in [
        ("D", "2024-01-01"),
        ("TS", "2024-01-01"),
        ("N", "2024-01-01"),
        ("F", "2024-01-01"),
        ("I", "2024-01-01"),
        ("I", "3.5"),
        ("N", "3"),
    ] {
        cases.push((
            format!("{column} = '{value}'"),
            predicate_equal(col(column), str_lit(value)),
        ));
    }
    for (label, expr) in cases {
        assert!(
            render(&expr).is_err(),
            "{label} must decline, rendered {:?}",
            render(&expr)
        );
    }
}

/// A DOUBLE declines wherever Exasol converts it to text, so a rule that converted every
/// argument would decline the numeric positions too.
#[test]
fn only_string_positions_convert_to_text() {
    let cases = [
        (
            "SUBSTR start",
            scalar_fn("SUBSTR", [col("S"), col("F")]),
            true,
        ),
        (
            "SUBSTR length",
            scalar_fn("SUBSTR", [col("S"), num_lit(1), col("F")]),
            true,
        ),
        (
            "SUBSTR subject",
            scalar_fn("SUBSTR", [col("F"), num_lit(1)]),
            false,
        ),
        (
            "REPEAT count",
            scalar_fn("REPEAT", [col("S"), col("F")]),
            true,
        ),
        ("LEFT length", scalar_fn("LEFT", [col("S"), col("F")]), true),
        (
            "RIGHT length",
            scalar_fn("RIGHT", [col("S"), col("F")]),
            true,
        ),
        (
            "LPAD/2 length",
            scalar_fn("LPAD", [col("S"), col("F")]),
            true,
        ),
        (
            "LPAD/3 length",
            scalar_fn("LPAD", [col("S"), col("F"), str_lit("x")]),
            true,
        ),
        (
            "LPAD/3 fill",
            scalar_fn("LPAD", [col("S"), num_lit(5), col("F")]),
            false,
        ),
        (
            "RPAD/3 fill",
            scalar_fn("RPAD", [col("S"), num_lit(5), col("F")]),
            false,
        ),
        (
            "CONCAT second operand",
            scalar_fn("CONCAT", [str_lit("x"), col("F")]),
            false,
        ),
        (
            "REPLACE replacement",
            scalar_fn("REPLACE", [col("S"), str_lit("a"), col("F")]),
            false,
        ),
        (
            "INSTR substring",
            scalar_fn("INSTR", [col("S"), col("F")]),
            false,
        ),
        ("CHR code", scalar_fn("CHR", [col("F")]), true),
        ("UNICODECHR code", scalar_fn("UNICODECHR", [col("F")]), true),
        (
            "a string function under CHR",
            scalar_fn("CHR", [scalar_fn("LENGTH", [col("F")])]),
            false,
        ),
    ];
    for (position, expr, pushed) in cases {
        let rendered = render(&expr);
        assert_eq!(rendered.is_ok(), pushed, "{position}: {rendered:?}");
    }
}

/// Scenario: A column of unknown type declines
#[test]
fn a_column_of_unknown_type_declines_where_it_meets_text() {
    for expr in [
        like(col("UNKNOWN"), "1%"),
        scalar_fn("UPPER", [col("UNKNOWN")]),
        predicate_equal(col("UNKNOWN"), str_lit("1")),
        cast_to(ExasolType::WIDEST_VARCHAR, col("UNKNOWN")),
    ] {
        assert!(render(&expr).is_err(), "{expr} must decline");
    }
    assert!(
        render(&predicate_equal(col("UNKNOWN"), num_lit(1))).is_ok(),
        "no text conversion, so the column's type does not matter"
    );
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
    let to_text = |expr| cast_to(ExasolType::WIDEST_VARCHAR, expr);
    let cases = [
        (
            "integer sum",
            to_text(scalar_fn("ADD", [col("I"), num_lit(2023)])),
            true,
        ),
        (
            "sum with a non-integer",
            to_text(scalar_fn("ADD", [col("I"), col("N")])),
            false,
        ),
        (
            "quotient",
            to_text(scalar_fn("FLOAT_DIV", [col("I"), num_lit(2)])),
            false,
        ),
        (
            "CASE over integers",
            to_text(searched_case([col("I"), num_lit(1)])),
            true,
        ),
        (
            "CASE over an integer and a boolean",
            to_text(searched_case([col("I"), col("B")])),
            false,
        ),
        (
            "ZEROIFNULL of an integer",
            scalar_fn(
                "CONCAT",
                [scalar_fn("ZEROIFNULL", [col("I")]), str_lit("x")],
            ),
            true,
        ),
        (
            "ZEROIFNULL of a decimal",
            scalar_fn(
                "CONCAT",
                [scalar_fn("ZEROIFNULL", [col("N")]), str_lit("x")],
            ),
            false,
        ),
        ("COUNT", to_text(count_star()), true),
        (
            "MIN of a date",
            scalar_fn(
                "CONCAT",
                [agg_item_expr("MIN", col("D"), false), str_lit("x")],
            ),
            false,
        ),
        (
            "EXTRACT YEAR",
            scalar_fn("CONCAT", [extract("YEAR", col("D")), str_lit("x")]),
            true,
        ),
        (
            "EXTRACT SECOND",
            scalar_fn("CONCAT", [extract("SECOND", col("TS")), str_lit("x")]),
            false,
        ),
    ];
    for (label, expr, pushed) in cases {
        let rendered = render(&expr);
        assert_eq!(rendered.is_ok(), pushed, "{label}: {rendered:?}");
    }
}

#[test]
fn a_boolean_column_converts_to_exasol_casing() {
    assert_eq!(
        render(&cast_to(ExasolType::WIDEST_VARCHAR, col("B"))).unwrap(),
        r#"CAST((CASE "B" WHEN TRUE THEN 'TRUE' WHEN FALSE THEN 'FALSE' ELSE NULL END) AS VARCHAR)"#
    );
    assert!(render(&cast_to(ExasolType::varchar(4), col("B"))).is_err());
    assert!(render(&cast_to(ExasolType::varchar(5), col("B"))).is_ok());
}

#[test]
fn text_casts_to_shorter_or_ascii_varchar_unchecked_and_decline_char() {
    let ascii = ascii_varchar;
    let cases = [
        (
            "text to shorter VARCHAR",
            ExasolType::varchar(10),
            col("S"),
            true,
        ),
        ("text to ASCII", ascii(2_000_000), col("S"), true),
        ("text to CHAR", ExasolType::char(10), col("S"), false),
        (
            "integer to fitting VARCHAR",
            ExasolType::varchar(10),
            col("I"),
            true,
        ),
        ("integer to fitting ASCII", ascii(10), col("I"), true),
        (
            "integer to short VARCHAR",
            ExasolType::varchar(9),
            col("I"),
            false,
        ),
        ("integer to CHAR", ExasolType::char(10), col("I"), false),
        (
            "integer literal to fitting VARCHAR",
            ExasolType::varchar(6),
            num_lit(12345),
            true,
        ),
        (
            "integer literal to short VARCHAR",
            ExasolType::varchar(3),
            num_lit(12345),
            false,
        ),
    ];
    for (label, target, source, pushed) in cases {
        let rendered = render(&cast_to(target, source));
        assert_eq!(rendered.is_ok(), pushed, "{label}: {rendered:?}");
    }
    assert_eq!(
        render(&cast_to(ExasolType::varchar(10), col("S"))).unwrap(),
        r#"CAST("S" AS VARCHAR)"#
    );
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
fn a_digits_only_literal_against_an_integer_renders_as_the_number_it_reads_as() {
    let cases = [
        (
            "overflowing the column",
            predicate_equal(col("I"), str_lit("2147483648")),
            r#"("I" = 2147483648)"#,
        ),
        (
            "leading zeros",
            predicate_equal(str_lit("007"), col("I")),
            r#"(7 = "I")"#,
        ),
        (
            "negative zero",
            predicate_equal(col("I"), str_lit("-0")),
            r#"("I" = 0)"#,
        ),
        (
            "IN",
            in_list(col("I"), [str_lit("1"), str_lit("-02")]),
            r#"("I" IN (1, -2))"#,
        ),
        (
            "BETWEEN",
            between(col("I"), str_lit("1"), str_lit("10")),
            r#"("I" BETWEEN 1 AND 10)"#,
        ),
        (
            "18 digits",
            predicate_equal(col("I"), str_lit("123456789012345678")),
            r#"("I" = 123456789012345678)"#,
        ),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(render(&expr).unwrap(), expected, "{label}");
    }
    for literal in ["1234567890123456789", "", "-", "1-2"] {
        assert!(
            render(&predicate_equal(col("I"), str_lit(literal))).is_err(),
            "'{literal}' is not a session-free integer"
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
        ("SUM", agg_item("SUM", Some("S"), false)),
        ("AVG", agg_item("AVG", Some("S"), false)),
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
        (
            "text and date-shaped text",
            predicate_equal(col("S"), str_lit("2024-01-01")),
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

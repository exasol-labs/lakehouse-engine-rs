use super::*;
use crate::test_support::*;
use crate::{CHECKED_INT_DIV_FN, SqlFragment, render_scan_expression};

fn columns() -> TypedColumns {
    typed_columns(&[
        ("D", ExasolType::Date),
        ("TS", ExasolType::timestamp(6)),
        ("TS0", ExasolType::timestamp(0)),
        ("TS3", ExasolType::timestamp(3)),
        ("TS9", ExasolType::timestamp(9)),
        (
            "LTZ",
            ExasolType::TimestampWithLocalTimeZone { precision: 6 },
        ),
        ("B", ExasolType::Boolean),
        ("F", ExasolType::DoublePrecision),
        ("N", ExasolType::decimal(9, 2)),
        ("I", ExasolType::decimal(9, 0)),
        ("I18", ExasolType::decimal(18, 0)),
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
            "integer quotient",
            to_text(scalar_fn("DIV", [col("I"), num_lit(2)])),
            true,
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
fn div_runs_in_the_scan_only_over_integer_operands() {
    let cases = [
        (
            "integer column and literal",
            scalar_fn("DIV", [col("I"), num_lit(2)]),
            true,
        ),
        (
            "two integer columns",
            scalar_fn("DIV", [col("I"), col("I18")]),
            true,
        ),
        (
            "NULL numerator",
            scalar_fn("DIV", [null_lit(), col("I")]),
            true,
        ),
        (
            "NULL divisor",
            scalar_fn("DIV", [col("I"), null_lit()]),
            true,
        ),
        (
            "nested integer sum",
            scalar_fn(
                "DIV",
                [scalar_fn("ADD", [col("I"), num_lit(1)]), num_lit(2)],
            ),
            true,
        ),
        (
            "scaled decimal numerator",
            scalar_fn("DIV", [col("N"), num_lit(2)]),
            false,
        ),
        (
            "scaled decimal divisor",
            scalar_fn("DIV", [col("I"), col("N")]),
            false,
        ),
        (
            "DOUBLE numerator",
            scalar_fn("DIV", [col("F"), num_lit(2)]),
            false,
        ),
        (
            "DOUBLE divisor",
            scalar_fn("DIV", [col("I"), col("F")]),
            false,
        ),
        (
            "text divisor",
            scalar_fn("DIV", [col("I"), col("S")]),
            false,
        ),
    ];
    for (label, expr, pushed) in cases {
        let rendered = render(&expr);
        assert_eq!(rendered.is_ok(), pushed, "{label}: {rendered:?}");
    }
    assert_eq!(
        render(&scalar_fn("DIV", [col("I"), num_lit(2)])).unwrap(),
        format!(r#"{CHECKED_INT_DIV_FN}("I", 2)"#)
    );
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

const B_AS_NUMBER: &str = r#"(CASE "B" WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)"#;

#[test]
fn a_boolean_casts_to_decimal_as_one_or_zero() {
    assert_eq!(
        render(&cast_to(ExasolType::decimal(18, 0), col("B"))).unwrap(),
        format!("CAST({B_AS_NUMBER} AS DECIMAL(18,0))")
    );
    assert_eq!(
        render(&cast_to(ExasolType::DoublePrecision, col("B"))).unwrap(),
        r#"CAST("B" AS DOUBLE)"#
    );
    assert!(render(&cast_to(ExasolType::decimal(1, 0), col("B"))).is_ok());
    assert!(render(&cast_to(ExasolType::decimal(2, 2), col("B"))).is_err());
}

#[test]
fn a_boolean_compares_with_a_number_as_one_or_zero() {
    let cases = [
        (
            "literal",
            predicate_equal(col("B"), num_lit(2)),
            format!("({B_AS_NUMBER} = 2)"),
        ),
        (
            "column",
            compare("predicate_less", col("F"), col("B")),
            format!(r#"("F" < {B_AS_NUMBER})"#),
        ),
        (
            "BETWEEN",
            between(col("B"), num_lit(0), col("N")),
            format!(r#"({B_AS_NUMBER} BETWEEN 0 AND "N")"#),
        ),
        (
            "IN over a number",
            in_list(col("I"), [bool_lit(true), num_lit(5)]),
            r#"("I" IN ((CASE TRUE WHEN TRUE THEN 1 WHEN FALSE THEN 0 END), 5))"#.to_string(),
        ),
        (
            "simple CASE",
            case_of(col("B"), [num_lit(1)], [bool_lit(true)]),
            format!("(CASE {B_AS_NUMBER} WHEN 1 THEN TRUE END)"),
        ),
        (
            "boolean against boolean",
            predicate_equal(col("B"), bool_lit(true)),
            r#"("B" = TRUE)"#.to_string(),
        ),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(render(&expr).unwrap(), expected, "{label}");
    }
    assert!(
        render(&in_list(col("B"), [num_lit(0), num_lit(1)])).is_err(),
        "an IN over a BOOLEAN converts its numbers to BOOLEAN"
    );
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
            "as many digits as the precision",
            predicate_equal(col("I"), str_lit("-999999999")),
            r#"("I" = -999999999)"#,
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
            predicate_equal(col("I18"), str_lit("123456789012345678")),
            r#"("I18" = 123456789012345678)"#,
        ),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(render(&expr).unwrap(), expected, "{label}");
    }
    for literal in ["1234567890123456789", "", "-", "1-2"] {
        assert!(
            render(&predicate_equal(col("I18"), str_lit(literal))).is_err(),
            "'{literal}' is not a session-free integer"
        );
    }
}

#[test]
fn a_digits_only_literal_wider_than_the_integer_precision_declines() {
    let cases = [
        (
            "IN",
            in_list(col("I"), [str_lit("7"), str_lit("2147483648")]),
        ),
        (
            "comparison",
            predicate_equal(col("I"), str_lit("2147483648")),
        ),
        (
            "BETWEEN",
            between(col("I"), str_lit("1"), str_lit("-2147483648")),
        ),
    ];
    for (label, expr) in cases {
        assert!(render(&expr).is_err(), "{label} must decline");
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
    let typed = |expr: &Json| crate::render_exasol_fragment(expr, &columns());
    assert_eq!(
        typed(&cast_to(ExasolType::char(3), col("S10"))),
        SqlFragment::new(r#"CAST("S10" AS CHAR(3))"#, Some(ExasolType::char(3)))
    );
    assert_eq!(typed(&col("N")).ty, Some(ExasolType::decimal(9, 2)));
    assert_eq!(typed(&null_lit()).ty, None);
}

/// DataFusion sums, averages, and deviates only numbers. Exasol also converts text by the
/// session's NLS settings and BOOLEAN to 1 or 0, so those run in Exasol.
#[test]
fn numeric_aggregates_take_only_numbers() {
    let numbers = [
        col("I"),
        col("N"),
        col("F"),
        scalar_fn("MULT", [col("I"), col("N")]),
        scalar_fn("SQRT", [col("I")]),
        scalar_fn("FLOAT_DIV", [col("I"), col("I")]),
        scalar_fn("LENGTH", [col("S")]),
        case_when([col("B")], [col("I"), col("N")]),
        cast_to(ExasolType::decimal(10, 2), col("I")),
    ];
    let not_numbers = [
        col("S"),
        col("B"),
        col("D"),
        col("TS"),
        col("UNKNOWN"),
        cast_to(ExasolType::varchar(30), col("I")),
        case_when([col("B")], [col("B"), bool_lit(false)]),
        scalar_fn("ADD", [col("D"), col("I")]),
        null_lit(),
    ];
    for name in ["SUM", "AVG", "STDDEV", "VAR_SAMP"] {
        for argument in &numbers {
            let expr = agg_item_expr(name, argument.clone(), false);
            assert!(render(&expr).is_ok(), "{expr} must stay in the scan");
        }
        for argument in &not_numbers {
            let expr = agg_item_expr(name, argument.clone(), false);
            assert!(render(&expr).is_err(), "{expr} must decline");
        }
    }
    for name in ["MIN", "MAX", "COUNT"] {
        for argument in [col("S"), null_lit()] {
            let expr = agg_item_expr(name, argument, false);
            assert!(render(&expr).is_ok(), "{expr} needs no number");
        }
    }
}

/// A function over integers that can return a fraction is a number, not an integer, so its text
/// still declines.
#[test]
fn a_fractional_function_of_integers_is_not_integer_text() {
    let fractional = [
        scalar_fn("SQRT", [col("I")]),
        scalar_fn("LN", [col("I")]),
        scalar_fn("ROUND", [col("I")]),
        scalar_fn("FLOAT_DIV", [col("I"), col("I")]),
    ];
    for function in fractional {
        let expr = cast_to(ExasolType::varchar(50), function);
        assert!(render(&expr).is_err(), "{expr} must decline");
    }
    let integer = cast_to(
        ExasolType::varchar(50),
        scalar_fn("MULT", [col("I"), col("I")]),
    );
    assert!(render(&integer).is_ok(), "{integer} is integer text");
}

fn type_of(expr: &Json) -> Inferred {
    type_scan_tree(expr, &columns()).unwrap().1
}

fn typed(expr: &Json) -> Json {
    type_scan_tree(expr, &columns()).unwrap().0
}

const fn timestamp(precision: u8) -> Inferred {
    Inferred::Exact(ExasolType::timestamp(precision))
}

const fn integer(precision: u8) -> Inferred {
    Inferred::Exact(ExasolType::decimal(precision, 0))
}

/// `NULLIF(a, b)` as Exasol sends it: `CASE a WHEN b THEN NULL ELSE a END`.
fn nullif(a: Json, b: Json) -> Json {
    case_of(a.clone(), [b], [null_lit(), a])
}

#[test]
fn date_and_timestamp_types_follow_their_source() {
    let cases = [
        ("DATE column", col("D"), DATE),
        ("TIMESTAMP(6) column", col("TS"), timestamp(6)),
        ("TIMESTAMP(0) column", col("TS0"), timestamp(0)),
        ("TIMESTAMP(9) column", col("TS9"), timestamp(9)),
        (
            "LTZ column",
            col("LTZ"),
            Inferred::Exact(ExasolType::TimestampWithLocalTimeZone { precision: 6 }),
        ),
        ("DATE literal", date_lit("2024-01-01"), DATE),
        (
            "TIMESTAMP literal without a fraction",
            timestamp_lit("2024-01-01 00:00:00"),
            timestamp(0),
        ),
        (
            "TIMESTAMP literal with 3 digits",
            timestamp_lit("2024-01-01 00:00:00.123"),
            timestamp(3),
        ),
        (
            "TIMESTAMP literal with 7 digits",
            timestamp_lit("2024-01-01 00:00:00.1234567"),
            timestamp(7),
        ),
        ("CAST to DATE", cast_to(ExasolType::Date, col("TS")), DATE),
        (
            "CAST to TIMESTAMP(3)",
            cast_to(ExasolType::timestamp(3), col("D")),
            timestamp(3),
        ),
        (
            "ADD_DAYS over a DATE",
            scalar_fn("ADD_DAYS", [col("D"), col("I")]),
            DATE,
        ),
        (
            "ADD_WEEKS over a DATE",
            scalar_fn("ADD_WEEKS", [col("D"), num_lit(1)]),
            DATE,
        ),
        (
            "ADD_HOURS over a DATE",
            scalar_fn("ADD_HOURS", [col("D"), num_lit(1)]),
            timestamp(0),
        ),
        (
            "ADD_MINUTES over a DATE",
            scalar_fn("ADD_MINUTES", [col("D"), num_lit(1)]),
            timestamp(0),
        ),
        (
            "ADD_DAYS over a TIMESTAMP(6)",
            scalar_fn("ADD_DAYS", [col("TS"), num_lit(1)]),
            timestamp(6),
        ),
        (
            "ADD_HOURS over a TIMESTAMP(0)",
            scalar_fn("ADD_HOURS", [col("TS0"), num_lit(1)]),
            timestamp(0),
        ),
        (
            "ADD_MINUTES over a TIMESTAMP(9)",
            scalar_fn("ADD_MINUTES", [col("TS9"), num_lit(1)]),
            timestamp(9),
        ),
        (
            "ADD_DAYS over ADD_HOURS over a DATE",
            scalar_fn(
                "ADD_DAYS",
                [scalar_fn("ADD_HOURS", [col("D"), num_lit(1)]), num_lit(1)],
            ),
            timestamp(0),
        ),
        ("MIN of a DATE", agg_item_expr("MIN", col("D"), false), DATE),
        (
            "MAX of a TIMESTAMP",
            agg_item_expr("MAX", col("TS"), false),
            timestamp(6),
        ),
        (
            "CASE with NULL branches over a DATE",
            case_when([col("B"), col("B")], [col("D"), null_lit(), null_lit()]),
            DATE,
        ),
        (
            "CASE with a NULL branch over a TIMESTAMP",
            case_when([col("B")], [null_lit(), col("TS")]),
            timestamp(6),
        ),
        (
            "CASE over two dates",
            case_when([col("B")], [col("D"), date_lit("1900-03-01")]),
            DATE,
        ),
        (
            "CASE over a TIMESTAMP(6) and a TIMESTAMP(0)",
            case_when([col("B")], [col("TS0"), col("TS")]),
            timestamp(6),
        ),
        (
            "CASE over a DATE and a TIMESTAMP(0), whose type differs by Exasol version",
            case_when([col("B")], [col("D"), col("TS0")]),
            Inferred::Unknown,
        ),
        (
            "CASE over a TIMESTAMP and an LTZ",
            case_when([col("B")], [col("TS"), col("LTZ")]),
            Inferred::Unknown,
        ),
        (
            "CASE over a TIMESTAMP and a TO_TIMESTAMP",
            case_when(
                [col("B")],
                [
                    col("TS"),
                    scalar_fn("TO_TIMESTAMP", [col("S"), str_lit("YYYY")]),
                ],
            ),
            Inferred::Partial(Partial::Timestamp),
        ),
        (
            "GREATEST over two dates",
            scalar_fn("GREATEST", [col("D"), col("D")]),
            DATE,
        ),
        (
            "LEAST over a TIMESTAMP(3) and a TIMESTAMP(9)",
            scalar_fn("LEAST", [col("TS3"), col("TS9")]),
            timestamp(9),
        ),
        (
            "TIMESTAMP literals with 2 and 4 digits",
            scalar_fn(
                "GREATEST",
                [
                    timestamp_lit("2024-01-01 00:00:00.12"),
                    timestamp_lit("2024-01-01 00:00:00.1200"),
                ],
            ),
            timestamp(4),
        ),
        (
            "NULLIF over a DATE and a TIMESTAMP",
            nullif(col("D"), col("TS")),
            DATE,
        ),
        (
            "NULLIF over a TIMESTAMP and a DATE",
            nullif(col("TS0"), col("D")),
            timestamp(0),
        ),
        (
            "DATE_TRUNC over a DATE",
            scalar_fn("DATE_TRUNC", [str_lit("month"), col("D")]),
            DATE,
        ),
        (
            "DATE_TRUNC over a TIMESTAMP(9)",
            scalar_fn("DATE_TRUNC", [str_lit("month"), col("TS9")]),
            timestamp(9),
        ),
        (
            "TO_DATE",
            scalar_fn("TO_DATE", [col("S"), str_lit("YYYY-MM-DD")]),
            DATE,
        ),
        (
            "TO_TIMESTAMP",
            scalar_fn("TO_TIMESTAMP", [col("S"), str_lit("YYYY-MM-DD")]),
            Inferred::Partial(Partial::Timestamp),
        ),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(type_of(&expr), expected, "{label}");
    }
}

#[test]
fn branches_of_integers_and_other_types_unify_as_exasol_does() {
    let cases = [
        (
            "CASE over DECIMAL(9,0) and DECIMAL(18,0)",
            case_when([col("B")], [col("I"), col("I18")]),
            integer(18),
        ),
        (
            "GREATEST over DECIMAL(9,0) and a 7-digit literal",
            scalar_fn("GREATEST", [num_lit(1234567), col("I")]),
            integer(9),
        ),
        (
            "NULLIF over DECIMAL(9,0) and DECIMAL(18,0)",
            nullif(col("I"), col("I18")),
            integer(9),
        ),
        (
            "CASE over an integer and a DECIMAL(9,2)",
            case_when([col("B")], [col("I"), col("N")]),
            Inferred::Partial(Partial::Number),
        ),
        (
            "CASE over an integer and a COUNT",
            case_when([col("B")], [col("I"), count_star()]),
            Inferred::Partial(Partial::Integer),
        ),
        (
            "CASE over two texts",
            case_when([col("B")], [col("S"), col("S10")]),
            Inferred::Partial(Partial::Text),
        ),
        (
            "CASE over two BOOLEANs",
            case_when([col("B")], [col("B"), bool_lit(false)]),
            BOOLEAN,
        ),
        (
            "CASE over a DATE and an integer",
            case_when([col("B")], [col("D"), col("I")]),
            Inferred::Unknown,
        ),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(type_of(&expr), expected, "{label}");
    }
}

#[test]
fn an_integer_branch_union_converts_to_text_by_its_widest_precision() {
    let union = || case_when([col("B")], [col("I"), num_lit(1)]);
    assert!(render(&cast_to(ExasolType::varchar(10), union())).is_ok());
    assert!(render(&cast_to(ExasolType::varchar(9), union())).is_err());
}

#[test]
fn timestamp_branches_cast_to_the_unit_holding_their_unified_precision() {
    let cases = [
        (
            case_when([col("B")], [col("TS0"), col("TS3")]),
            r#"(CASE WHEN "B" THEN CAST("TS0" AS TIMESTAMP(3)) ELSE CAST("TS3" AS TIMESTAMP(3)) END)"#,
        ),
        (
            case_when([col("B")], [col("TS3"), null_lit()]),
            r#"(CASE WHEN "B" THEN CAST("TS3" AS TIMESTAMP(3)) ELSE NULL END)"#,
        ),
        (
            case_when([col("B")], [col("D"), date_lit("2024-01-01")]),
            r#"(CASE WHEN "B" THEN "D" ELSE DATE '2024-01-01' END)"#,
        ),
    ];
    for (expr, expected) in cases {
        assert_eq!(render(&expr).unwrap(), expected);
    }
    let greatest = scalar_fn("GREATEST", [col("TS0"), col("TS")]);
    assert!(
        render(&greatest)
            .unwrap()
            .contains(r#"greatest(CAST("TS0" AS TIMESTAMP(6)), CAST("TS" AS TIMESTAMP(6)))"#)
    );
}

#[test]
fn date_trunc_over_a_date_returns_a_date() {
    assert_eq!(
        render(&scalar_fn("DATE_TRUNC", [str_lit("month"), col("D")])).unwrap(),
        r#"CAST(date_trunc('month', "D") AS DATE)"#
    );
    assert_eq!(
        render(&scalar_fn("DATE_TRUNC", [str_lit("month"), col("TS")])).unwrap(),
        r#"date_trunc('month', "TS")"#
    );
}

#[test]
fn date_and_timestamp_text_conversion_declines() {
    let sources = [
        col("D"),
        col("TS"),
        date_lit("2024-01-01"),
        timestamp_lit("2024-01-01 00:00:00"),
        scalar_fn("ADD_DAYS", [col("D"), num_lit(1)]),
    ];
    for source in sources {
        for expr in [
            cast_to(ExasolType::WIDEST_VARCHAR, source.clone()),
            date_like(source.clone()),
            scalar_fn("CONCAT", [source.clone(), str_lit("x")]),
            scalar_fn("UPPER", [source.clone()]),
        ] {
            assert!(render(&expr).is_err(), "{expr} must decline");
        }
    }
}

#[test]
fn a_boolean_date_addition_count_renders_as_one_or_zero() {
    let expr = scalar_fn("ADD_DAYS", [col("D"), col("B")]);
    assert_eq!(
        render(&expr).unwrap(),
        r#"vs_add_days(CAST("D" AS DATE), (CASE "B" WHEN TRUE THEN 1 WHEN FALSE THEN 0 END))"#
    );
}

#[test]
fn unmatched_date_addition_forms_decline() {
    let mut cases = Vec::new();
    for name in ["ADD_DAYS", "ADD_WEEKS", "ADD_HOURS", "ADD_MINUTES"] {
        let first_arguments = [
            ("TIMESTAMP WITH LOCAL TIME ZONE", col("LTZ")),
            ("text", col("S")),
            ("text literal", str_lit("2024-01-01")),
            ("NULL literal", null_lit()),
            ("a number", col("I")),
            ("a BOOLEAN", col("B")),
            (
                "CASE over a DATE and a TIMESTAMP WITH LOCAL TIME ZONE",
                case_when([col("B")], [col("D"), col("LTZ")]),
            ),
            ("TO_DATE without a format", scalar_fn("TO_DATE", [col("S")])),
            (
                "TO_TIMESTAMP, whose precision differs by Exasol version",
                scalar_fn("TO_TIMESTAMP", [col("S"), str_lit("YYYY-MM-DD")]),
            ),
            ("an unknown column", col("UNKNOWN")),
        ];
        for (label, first) in first_arguments {
            cases.push((
                format!("{name} over {label}"),
                scalar_fn(name, [first, num_lit(1)]),
            ));
        }
        for (label, count) in [
            ("text", col("S")),
            ("a DATE", col("D")),
            ("a TIMESTAMP", col("TS")),
            (
                "an exact literal of 1e30",
                lit("literal_exactnumeric", "1000000000000000000000000000000"),
            ),
            (
                "a DOUBLE literal of 1e30",
                lit("literal_double", "1.0000000000000000e+30"),
            ),
            (
                "a DOUBLE literal of -1e30",
                lit("literal_double", "-1.0000000000000000e+30"),
            ),
            (
                "an exact literal just above 1e10",
                lit("literal_exactnumeric", "10000000001"),
            ),
        ] {
            cases.push((
                format!("{name} with {label} as the count"),
                scalar_fn(name, [col("D"), count]),
            ));
        }
        cases.push((
            format!("{name} with one argument"),
            scalar_fn(name, [col("D")]),
        ));
        cases.push((
            format!("{name} with three arguments"),
            scalar_fn(name, [col("D"), num_lit(1), num_lit(2)]),
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

#[test]
fn a_date_addition_computes_in_the_unit_that_holds_its_argument() {
    let cases = [
        (col("TS0"), r#"vs_add_days(CAST("TS0" AS TIMESTAMP(6)), 1)"#),
        (col("TS"), r#"vs_add_days(CAST("TS" AS TIMESTAMP(6)), 1)"#),
        (col("TS9"), r#"vs_add_days(CAST("TS9" AS TIMESTAMP(9)), 1)"#),
        (
            timestamp_lit("2024-01-01 00:00:00.1000000"),
            "vs_add_days(CAST(arrow_cast('2024-01-01 00:00:00.1000000', 'Timestamp(Microsecond, None)') AS TIMESTAMP(9)), 1)",
        ),
    ];
    for (argument, expected) in cases {
        assert_eq!(
            render(&scalar_fn("ADD_DAYS", [argument, num_lit(1)])).unwrap(),
            expected
        );
    }
}

#[test]
fn overprecise_timestamp_literal_is_floored_to_the_column_precision() {
    let lit = || timestamp_lit("2025-06-01 00:00:00.123500");
    let floor = || timestamp_lit("2025-06-01 00:00:00.123");
    for (op, mirrored, want_op, want_right) in [
        ("predicate_equal", false, "predicate_notequal", col("TS3")),
        ("predicate_notequal", false, "predicate_equal", col("TS3")),
        ("predicate_less", false, "predicate_lessequal", floor()),
        ("predicate_lessequal", false, "predicate_lessequal", floor()),
        ("predicate_greater", false, "predicate_greater", floor()),
        (
            "predicate_greaterequal",
            false,
            "predicate_greater",
            floor(),
        ),
        ("predicate_less", true, "predicate_greater", floor()),
        (
            "predicate_greaterequal",
            true,
            "predicate_lessequal",
            floor(),
        ),
    ] {
        let filter = if mirrored {
            compare(op, lit(), col("TS3"))
        } else {
            compare(op, col("TS3"), lit())
        };
        assert_eq!(
            typed(&filter),
            compare(want_op, col("TS3"), want_right),
            "{op} mirrored={mirrored}"
        );
    }
}

#[test]
fn timestamp_literal_within_the_operand_precision_is_unchanged() {
    for (column, value) in [
        ("TS3", "2025-06-01 00:00:00.123000"),
        ("TS", "2025-06-01 00:00:00.123456"),
        ("TS9", "2025-06-01 00:00:00.123456789"),
        ("TS0", "2025-06-01 00:00:00"),
    ] {
        let filter = predicate_equal(col(column), timestamp_lit(value));
        assert_eq!(typed(&filter), filter, "{column} = {value}");
    }
}

#[test]
fn between_and_in_with_an_overprecise_timestamp_expand_to_floored_comparisons() {
    let fits = || timestamp_lit("2025-06-01 00:00:00.122");
    let over = || timestamp_lit("2025-06-01 00:00:00.123500");
    let floor = || timestamp_lit("2025-06-01 00:00:00.123");

    assert_eq!(
        typed(&between(col("TS3"), fits(), over())),
        and([
            compare("predicate_greaterequal", col("TS3"), fits()),
            compare("predicate_lessequal", col("TS3"), floor())
        ])
    );
    assert_eq!(
        typed(&in_list(col("TS3"), [over(), fits()])),
        or([
            compare("predicate_notequal", col("TS3"), col("TS3")),
            predicate_equal(col("TS3"), fits())
        ])
    );
    let fitting_in = in_list(col("TS3"), [fits()]);
    assert_eq!(typed(&fitting_in), fitting_in);
}

#[test]
fn overprecise_timestamp_literal_against_a_date_addition_is_floored_to_its_precision() {
    let call = || scalar_fn("ADD_HOURS", [col("D"), num_lit(1)]);
    // The typed pass casts the call's first argument to the type it computes in.
    let typed_call = || {
        scalar_fn(
            "ADD_HOURS",
            [cast_to(ExasolType::Date, col("D")), num_lit(1)],
        )
    };
    let over = || timestamp_lit("2024-01-01 01:00:00.0000001");
    let floor = || timestamp_lit("2024-01-01 01:00:00");
    let fits = || timestamp_lit("2024-01-02 01:00:00");
    let never_equal = || compare("predicate_notequal", typed_call(), typed_call());

    for (shape, filter, expected) in [
        ("=", predicate_equal(call(), over()), never_equal()),
        ("= mirrored", predicate_equal(over(), call()), never_equal()),
        (
            "= a literal finer than the TIMESTAMP(0) result",
            predicate_equal(call(), timestamp_lit("2024-01-01 01:00:00.123456")),
            never_equal(),
        ),
        (
            ">=",
            compare("predicate_greaterequal", call(), over()),
            compare("predicate_greater", typed_call(), floor()),
        ),
        (
            ">= mirrored",
            compare("predicate_greaterequal", over(), call()),
            compare("predicate_lessequal", typed_call(), floor()),
        ),
        (
            "BETWEEN",
            between(call(), over(), fits()),
            and([
                compare("predicate_greater", typed_call(), floor()),
                compare("predicate_lessequal", typed_call(), fits()),
            ]),
        ),
        (
            "IN",
            in_list(call(), [over(), fits()]),
            or([never_equal(), predicate_equal(typed_call(), fits())]),
        ),
    ] {
        assert_eq!(typed(&filter), expected, "{shape}");
    }

    for name in ["ADD_DAYS", "ADD_WEEKS", "ADD_MINUTES"] {
        let typed_call = scalar_fn(name, [cast_to(ExasolType::Date, col("D")), num_lit(1)]);
        assert_eq!(
            typed(&predicate_equal(
                scalar_fn(name, [col("D"), num_lit(1)]),
                over()
            )),
            compare("predicate_notequal", typed_call.clone(), typed_call),
            "{name}"
        );
    }

    let unchanged: [fn(Json, Json) -> Json; 5] = [
        |call, literal| predicate_equal(call, literal),
        |call, literal| predicate_equal(literal, call),
        |call, literal| compare("predicate_greaterequal", call, literal),
        |call, literal| between(call, literal.clone(), literal),
        |call, literal| in_list(call, [literal]),
    ];
    for shape in unchanged {
        let expected = shape(typed_call(), fits());
        assert_eq!(typed(&shape(call(), fits())), expected, "{expected}");
    }
    let nanosecond_call = |argument| scalar_fn("ADD_DAYS", [argument, num_lit(1)]);
    let nanosecond_literal = || timestamp_lit("2024-01-02 00:00:00.000000001");
    assert_eq!(
        typed(&predicate_equal(
            nanosecond_call(col("TS9")),
            nanosecond_literal()
        )),
        predicate_equal(
            nanosecond_call(cast_to(ExasolType::timestamp(9), col("TS9"))),
            nanosecond_literal()
        )
    );
}

#[test]
fn a_literal_against_a_unified_timestamp_is_floored_to_the_unified_precision() {
    let filter = compare(
        "predicate_greaterequal",
        case_when([col("B")], [col("TS0"), col("TS3")]),
        timestamp_lit("2024-01-01 00:00:00.0005"),
    );
    let typed_union = case_when(
        [col("B")],
        [
            cast_to(ExasolType::timestamp(3), col("TS0")),
            cast_to(ExasolType::timestamp(3), col("TS3")),
        ],
    );
    assert_eq!(
        typed(&filter),
        compare(
            "predicate_greater",
            typed_union,
            timestamp_lit("2024-01-01 00:00:00.000")
        )
    );
}

#[test]
fn a_constant_count_at_the_limit_renders() {
    let expr = scalar_fn(
        "ADD_MINUTES",
        [col("D"), lit("literal_exactnumeric", "10000000000")],
    );
    assert!(render(&expr).is_ok(), "rendered {:?}", render(&expr));
}

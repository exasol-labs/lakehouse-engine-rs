use super::*;
use crate::adapter::pushdown::test_support::{
    and, between, binary_lit, bool_lit, compare, date_lit, in_list, is_not_null, is_null, like,
    lit, not, null_lit, num_lit, or, predicate_equal, sales_col, sales_equal, scalar_fn, str_lit,
    timestamp_lit,
};
use arrow::datatypes::TimeUnit;
use serde_json::json;

/// A filter no partition column can decide.
fn on_a_data_column() -> Json {
    predicate_equal(sales_col("ID"), num_lit("1"))
}

fn file_with(key: &str, value: Option<&str>) -> BTreeMap<String, Option<String>> {
    BTreeMap::from([(key.to_string(), value.map(str::to_string))])
}

fn kept(filter: &Json, files: &[BTreeMap<String, Option<String>>]) -> Vec<bool> {
    kept_under(filter, &[], files)
}

fn kept_under(
    filter: &Json,
    declared: &[(String, DataType)],
    files: &[BTreeMap<String, Option<String>>],
) -> Vec<bool> {
    let predicate = PartitionPredicate::from_filter(Some(filter), declared);
    files.iter().map(|values| predicate.keeps(values)).collect()
}

fn year_files() -> [BTreeMap<String, Option<String>>; 3] {
    [
        file_with("year", Some("2026")),
        file_with("year", Some("2025")),
        file_with("year", None),
    ]
}

/// Byte order ranks B < a < é, unlike a case-insensitive or locale order.
fn region_files() -> [BTreeMap<String, Option<String>>; 4] {
    [
        file_with("region", Some("B")),
        file_with("region", Some("a")),
        file_with("region", Some("é")),
        file_with("region", None),
    ]
}

#[test]
fn partition_nodes_evaluate_under_three_valued_logic() {
    let files = year_files();
    let cases: Vec<(&str, Json, [bool; 3])> = vec![
        ("=", sales_equal("YEAR", "2026"), [true, false, false]),
        (
            "= with the column on the right",
            predicate_equal(str_lit("2026"), sales_col("YEAR")),
            [true, false, false],
        ),
        (
            "<>",
            compare("predicate_notequal", sales_col("YEAR"), str_lit("2026")),
            [false, true, false],
        ),
        (
            "IN",
            in_list(sales_col("YEAR"), [str_lit("2026"), str_lit("2024")]),
            [true, false, false],
        ),
        ("IS NULL", is_null(sales_col("YEAR")), [false, false, true]),
        (
            "IS NOT NULL",
            is_not_null(sales_col("YEAR")),
            [true, true, false],
        ),
        (
            "NOT over a NULL comparison stays NULL",
            not(sales_equal("YEAR", "2026")),
            [false, true, false],
        ),
        (
            "OR",
            or([sales_equal("YEAR", "2026"), is_null(sales_col("YEAR"))]),
            [true, false, true],
        ),
        (
            "AND",
            and([
                is_not_null(sales_col("YEAR")),
                not(sales_equal("YEAR", "2025")),
            ]),
            [true, false, false],
        ),
        (
            "NOT over IS NULL",
            not(is_null(sales_col("YEAR"))),
            [true, true, false],
        ),
    ];

    for (label, filter, expected) in cases {
        assert_eq!(
            kept(&filter, &files),
            expected,
            "{label}: kept per file [2026, 2025, NULL]"
        );
    }
}

#[test]
fn range_and_between_nodes_evaluate_under_three_valued_logic() {
    let files = region_files();
    let cases: Vec<(&str, Json, [bool; 4])> = vec![
        (
            ">",
            compare("predicate_greater", sales_col("REGION"), str_lit("Z")),
            [false, true, true, false],
        ),
        (
            ">=",
            compare("predicate_greaterequal", sales_col("REGION"), str_lit("a")),
            [false, true, true, false],
        ),
        (
            "<",
            compare("predicate_less", sales_col("REGION"), str_lit("z")),
            [true, true, false, false],
        ),
        (
            "<=",
            compare("predicate_lessequal", sales_col("REGION"), str_lit("a")),
            [true, true, false, false],
        ),
        (
            "> with the column on the right flips to <",
            compare("predicate_greater", str_lit("a"), sales_col("REGION")),
            [true, false, false, false],
        ),
        (
            "<= with the column on the right flips to >=",
            compare("predicate_lessequal", str_lit("a"), sales_col("REGION")),
            [false, true, true, false],
        ),
        (
            "BETWEEN",
            between(sales_col("REGION"), str_lit("B"), str_lit("a")),
            [true, true, false, false],
        ),
        (
            "BETWEEN with its bounds reversed holds for no value",
            between(sales_col("REGION"), str_lit("a"), str_lit("B")),
            [false, false, false, false],
        ),
        (
            "NOT BETWEEN",
            not(between(sales_col("REGION"), str_lit("B"), str_lit("a"))),
            [false, false, true, false],
        ),
    ];

    for (label, filter, expected) in cases {
        assert_eq!(
            kept(&filter, &files),
            expected,
            "{label}: kept per file [B, a, é, NULL] under byte order"
        );
    }
}

#[test]
fn a_non_partition_node_never_prunes() {
    let files = year_files();
    let cases: Vec<(&str, Json, [bool; 3])> = vec![
        (
            "a data-column comparison",
            on_a_data_column(),
            [true, true, true],
        ),
        ("NOT over it", not(on_a_data_column()), [true, true, true]),
        (
            "OR with a partition node",
            or([sales_equal("YEAR", "2025"), on_a_data_column()]),
            [true, true, true],
        ),
        (
            "AND with a partition node prunes by the partition node alone",
            and([sales_equal("YEAR", "2025"), on_a_data_column()]),
            [false, true, false],
        ),
        (
            "NOT over an AND it cannot decide",
            not(and([sales_equal("YEAR", "2026"), on_a_data_column()])),
            [true, true, true],
        ),
        (
            "NOT over an OR prunes only where the partition node decides the OR",
            not(or([sales_equal("YEAR", "2025"), on_a_data_column()])),
            [true, false, false],
        ),
        (
            "a column that names no partition key",
            sales_equal("NAME", "2025"),
            [true, true, true],
        ),
        (
            "a function over the partition column",
            predicate_equal(scalar_fn("UPPER", [sales_col("YEAR")]), str_lit("2025")),
            [true, true, true],
        ),
        (
            "an operator this module does not evaluate",
            like(sales_col("YEAR"), "2025%"),
            [true, true, true],
        ),
        (
            "two partition columns compared with each other",
            predicate_equal(sales_col("YEAR"), sales_col("YEAR")),
            [true, true, true],
        ),
        (
            "an AND with no operand",
            and(Vec::new()),
            [true, true, true],
        ),
        ("an OR with no operand", or(Vec::new()), [true, true, true]),
        (
            "an IN with no element",
            in_list(sales_col("YEAR"), Vec::new()),
            [true, true, true],
        ),
        (
            "a node without a type",
            json!({"left": sales_col("YEAR")}),
            [true, true, true],
        ),
        (
            "a numeric literal, which Exasol compares numerically",
            predicate_equal(sales_col("YEAR"), num_lit("2024")),
            [true, true, true],
        ),
        (
            "an empty string, which Exasol reads as NULL",
            sales_equal("YEAR", ""),
            [true, true, true],
        ),
        (
            "a NULL literal",
            predicate_equal(sales_col("YEAR"), null_lit()),
            [true, true, true],
        ),
        (
            "an IN list holding one non-string element",
            in_list(sales_col("YEAR"), [str_lit("2024"), num_lit("2023")]),
            [true, true, true],
        ),
        (
            "an IN list holding one empty string",
            in_list(sales_col("YEAR"), [str_lit("2024"), str_lit("")]),
            [true, true, true],
        ),
        (
            "a BETWEEN with one non-string bound",
            between(sales_col("YEAR"), str_lit("2023"), num_lit("2024")),
            [true, true, true],
        ),
        (
            "a range comparison against an empty string",
            compare("predicate_greater", sales_col("YEAR"), str_lit("")),
            [true, true, true],
        ),
    ];

    for (label, filter, expected) in cases {
        assert_eq!(
            kept(&filter, &files),
            expected,
            "{label}: kept per file [2026, 2025, NULL]"
        );
    }

    let unfiltered = PartitionPredicate::from_filter(None, &[]);
    assert!(
        files.iter().all(|values| unfiltered.keeps(values)),
        "an absent filter keeps every file"
    );
}

#[test]
fn a_filter_column_resolves_to_a_partition_key_by_its_uppercase_fold() {
    for key in ["year", "Year", "YEAR"] {
        let files = [file_with(key, Some("2026")), file_with(key, Some("2025"))];
        assert_eq!(
            kept(&sales_equal("YEAR", "2026"), &files),
            [true, false],
            "the Exasol column YEAR resolves the key '{key}'"
        );
    }

    let non_ascii = [
        file_with("größe", Some("xl")),
        file_with("größe", Some("s")),
    ];
    assert_eq!(
        kept(&sales_equal("GRÖSSE", "xl"), &non_ascii),
        [true, false],
        "the fold is Unicode uppercasing, the one the declaration uses"
    );
}

type FileValues = BTreeMap<String, Option<String>>;

fn typed_columns() -> Vec<(String, DataType)> {
    [
        ("year", DataType::Int32),
        ("day", DataType::Date32),
        ("amount", DataType::Decimal128(10, 2)),
        ("ts", DataType::Timestamp(TimeUnit::Microsecond, None)),
        ("region", DataType::Utf8),
        ("active", DataType::Boolean),
        ("ratio", DataType::Float64),
        ("ts_ms", DataType::Timestamp(TimeUnit::Millisecond, None)),
        ("tiny", DataType::Int8),
        ("payload", DataType::Binary),
    ]
    .into_iter()
    .map(|(name, data_type)| (name.to_string(), data_type))
    .collect()
}

fn two_files(key: &str, first: &str, second: &str) -> [FileValues; 2] {
    [file_with(key, Some(first)), file_with(key, Some(second))]
}

/// Scenario: A partition value compares under its column's declared type
#[test]
fn partition_values_compare_under_their_declared_type() {
    let declared = typed_columns();
    let cases: Vec<(&str, Json, [FileValues; 2])> = vec![
        (
            "year < 10 orders integers numerically, where a string order ranks '10' first",
            compare("predicate_less", sales_col("YEAR"), num_lit("10")),
            two_files("year", "9", "10"),
        ),
        (
            "10 > year flips to year < 10",
            compare("predicate_greater", num_lit("10"), sales_col("YEAR")),
            two_files("year", "9", "10"),
        ),
        (
            "an integer IN list",
            in_list(sales_col("YEAR"), [num_lit("9"), num_lit("11")]),
            two_files("year", "9", "10"),
        ),
        (
            "day = DATE '2024-03-01'",
            predicate_equal(sales_col("DAY"), date_lit("2024-03-01")),
            two_files("day", "2024-03-01", "2024-02-29"),
        ),
        (
            "a date BETWEEN",
            between(
                sales_col("DAY"),
                date_lit("2024-02-01"),
                date_lit("2024-02-29"),
            ),
            two_files("day", "2024-02-29", "2024-03-01"),
        ),
        (
            "amount >= 1.50",
            compare(
                "predicate_greaterequal",
                sales_col("AMOUNT"),
                num_lit("1.50"),
            ),
            two_files("amount", "1.5", "1.49"),
        ),
        (
            "an integral literal on a decimal column",
            compare("predicate_greater", sales_col("AMOUNT"), num_lit("2")),
            two_files("amount", "10.00", "1.99"),
        ),
        (
            "ts < TIMESTAMP '2024-03-01 00:00:00'",
            compare(
                "predicate_less",
                sales_col("TS"),
                timestamp_lit("2024-03-01 00:00:00"),
            ),
            two_files("ts", "2024-02-29 23:59:59.999999", "2024-03-01 00:00:00"),
        ),
        (
            "region = 'eu' in codepoint order",
            sales_equal("REGION", "eu"),
            two_files("region", "eu", "us"),
        ),
        (
            "active = TRUE",
            predicate_equal(sales_col("ACTIVE"), bool_lit(true)),
            two_files("active", "true", "false"),
        ),
    ];

    for (label, filter, files) in cases {
        assert_eq!(
            kept_under(&filter, &declared, &files),
            [true, false],
            "{label}: kept per file"
        );
    }
    assert_eq!(
        kept_under(
            &compare("predicate_less", sales_col("YEAR"), num_lit("10")),
            &declared,
            &[file_with("year", None)],
        ),
        [false],
        "a NULL value compares NULL under every declared type"
    );
}

/// Scenario: A comparison the declared type cannot decide exactly keeps the file
#[test]
fn an_undecidable_comparison_keeps_the_file() {
    let declared = typed_columns();
    let cases: Vec<(&str, Json, [FileValues; 2])> = vec![
        (
            "a string literal on an integer column",
            sales_equal("YEAR", "2024"),
            two_files("year", "2024", "2025"),
        ),
        (
            "a fractional literal on an integer column, which truncation would decide",
            predicate_equal(sales_col("YEAR"), num_lit("2024.5")),
            two_files("year", "2024", "2025"),
        ),
        (
            "an integer literal outside the column's range, which wrapping would decide",
            predicate_equal(sales_col("TINY"), num_lit("300")),
            two_files("tiny", "44", "45"),
        ),
        (
            "a literal finer than the decimal scale, which rounding would decide",
            predicate_equal(sales_col("AMOUNT"), num_lit("1.555")),
            two_files("amount", "1.55", "1.56"),
        ),
        (
            "a timestamp literal finer than the column's unit, which truncation would decide",
            compare(
                "predicate_less",
                sales_col("TS_MS"),
                timestamp_lit("2024-03-01 00:00:00.000500"),
            ),
            two_files("ts_ms", "2024-03-01 00:00:00", "2024-03-01 00:00:00.001"),
        ),
        (
            "a comparison on a float column",
            compare("predicate_greater", sales_col("RATIO"), num_lit("1.0")),
            two_files("ratio", "0.5", "2.0"),
        ),
        (
            "a double literal on an integer column",
            predicate_equal(sales_col("YEAR"), lit("literal_double", json!("2024"))),
            two_files("year", "2024", "2025"),
        ),
        (
            "a timestamp literal on a date column",
            predicate_equal(sales_col("DAY"), timestamp_lit("2024-03-01 00:00:00")),
            two_files("day", "2024-03-01", "2024-03-02"),
        ),
        (
            "a date literal on a timestamp column",
            predicate_equal(sales_col("TS"), date_lit("2024-03-01")),
            two_files("ts", "2024-03-01 00:00:00", "2024-03-02 00:00:00"),
        ),
        (
            "a UTC timestamp literal on a timestamp column",
            predicate_equal(
                sales_col("TS"),
                lit("literal_timestamp_utc", json!("2024-03-01 00:00:00")),
            ),
            two_files("ts", "2024-03-01 00:00:00", "2024-03-02 00:00:00"),
        ),
        (
            "a string literal on a boolean column",
            sales_equal("ACTIVE", "true"),
            two_files("active", "true", "false"),
        ),
        (
            "a string literal on a column of another declared type",
            sales_equal("PAYLOAD", "x"),
            two_files("payload", "x", "y"),
        ),
    ];

    for (label, filter, files) in cases {
        assert_eq!(
            kept_under(&filter, &declared, &files),
            [true, true],
            "{label}: kept per file"
        );
    }
    let year_2024 = predicate_equal(sales_col("YEAR"), num_lit("2024"));
    assert_eq!(
        kept_under(&year_2024, &declared, &two_files("year", "abc", "2025")),
        [true, false],
        "a value the declared type cannot convert keeps its file for the scan to fail"
    );
}

#[test]
fn an_empty_partition_value_is_null_as_the_scan_reads_it() {
    let files = [file_with("year", Some(""))];
    for (declared, bound) in [(Vec::new(), str_lit("9")), (typed_columns(), num_lit("9"))] {
        assert_eq!(
            kept_under(&is_null(sales_col("YEAR")), &declared, &files),
            [true],
            "IS NULL holds for an empty value under {declared:?}"
        );
        assert_eq!(
            kept_under(
                &compare("predicate_less", sales_col("YEAR"), bound),
                &declared,
                &files,
            ),
            [false],
            "a comparison against an empty value is NULL, never TRUE, under {declared:?}"
        );
    }
}

/// Scenario: A hex-literal filter on a binary column prunes files at plan time
#[test]
fn a_binary_literal_compares_a_logged_value_by_its_utf8_bytes() {
    let declared = [("PART".to_string(), DataType::Binary)];
    let files = [
        file_with("part", Some("AB")),
        file_with("part", Some("ab")),
        file_with("part", Some("")),
        file_with("part", None),
    ];
    assert_eq!(
        kept_under(
            &predicate_equal(sales_col("PART"), binary_lit("4142")),
            &declared,
            &files
        ),
        [true, false, false, false],
        "'AB' logs the bytes 41 42, and an empty value reads NULL"
    );
    assert_eq!(
        kept_under(
            &not(compare(
                "predicate_greater",
                sales_col("PART"),
                binary_lit("4142")
            )),
            &declared,
            &files
        ),
        [true, false, false, false]
    );
    assert_eq!(
        kept_under(&sales_equal("PART", "4142"), &declared, &files),
        [true; 4],
        "only the retyped literal names bytes"
    );
}

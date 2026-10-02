use super::*;
use crate::adapter::pushdown::test_support::filter_json::*;
use arrow::datatypes::TimeUnit;
use serde_json::json;

/// A filter no partition column can decide.
fn on_a_data_column() -> Json {
    compare("predicate_equal", column("ID"), number("1"))
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
        ("=", equal("YEAR", "2026"), [true, false, false]),
        (
            "= with the column on the right",
            compare("predicate_equal", string("2026"), column("YEAR")),
            [true, false, false],
        ),
        (
            "<>",
            compare("predicate_notequal", column("YEAR"), string("2026")),
            [false, true, false],
        ),
        (
            "IN",
            in_list("YEAR", vec![string("2026"), string("2024")]),
            [true, false, false],
        ),
        ("IS NULL", is_null("YEAR"), [false, false, true]),
        ("IS NOT NULL", is_not_null("YEAR"), [true, true, false]),
        (
            "NOT over a NULL comparison stays NULL",
            not(equal("YEAR", "2026")),
            [false, true, false],
        ),
        (
            "OR",
            or(vec![equal("YEAR", "2026"), is_null("YEAR")]),
            [true, false, true],
        ),
        (
            "AND",
            and(vec![is_not_null("YEAR"), not(equal("YEAR", "2025"))]),
            [true, false, false],
        ),
        (
            "NOT over IS NULL",
            not(is_null("YEAR")),
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
            compare("predicate_greater", column("REGION"), string("Z")),
            [false, true, true, false],
        ),
        (
            ">=",
            compare("predicate_greaterequal", column("REGION"), string("a")),
            [false, true, true, false],
        ),
        (
            "<",
            compare("predicate_less", column("REGION"), string("z")),
            [true, true, false, false],
        ),
        (
            "<=",
            compare("predicate_lessequal", column("REGION"), string("a")),
            [true, true, false, false],
        ),
        (
            "> with the column on the right flips to <",
            compare("predicate_greater", string("a"), column("REGION")),
            [true, false, false, false],
        ),
        (
            "<= with the column on the right flips to >=",
            compare("predicate_lessequal", string("a"), column("REGION")),
            [false, true, true, false],
        ),
        (
            "BETWEEN",
            between("REGION", string("B"), string("a")),
            [true, true, false, false],
        ),
        (
            "BETWEEN with its bounds reversed holds for no value",
            between("REGION", string("a"), string("B")),
            [false, false, false, false],
        ),
        (
            "NOT BETWEEN",
            not(between("REGION", string("B"), string("a"))),
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
            or(vec![equal("YEAR", "2025"), on_a_data_column()]),
            [true, true, true],
        ),
        (
            "AND with a partition node prunes by the partition node alone",
            and(vec![equal("YEAR", "2025"), on_a_data_column()]),
            [false, true, false],
        ),
        (
            "NOT over an AND it cannot decide",
            not(and(vec![equal("YEAR", "2026"), on_a_data_column()])),
            [true, true, true],
        ),
        (
            "NOT over an OR prunes only where the partition node decides the OR",
            not(or(vec![equal("YEAR", "2025"), on_a_data_column()])),
            [true, false, false],
        ),
        (
            "a column that names no partition key",
            equal("NAME", "2025"),
            [true, true, true],
        ),
        (
            "a function over the partition column",
            compare(
                "predicate_equal",
                json!({"type": "function_scalar", "name": "UPPER", "arguments": [column("YEAR")]}),
                string("2025"),
            ),
            [true, true, true],
        ),
        (
            "an operator this module does not evaluate",
            json!({"type": "predicate_like", "expression": column("YEAR"), "pattern": string("2025%")}),
            [true, true, true],
        ),
        (
            "two partition columns compared with each other",
            compare("predicate_equal", column("YEAR"), column("YEAR")),
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
            in_list("YEAR", Vec::new()),
            [true, true, true],
        ),
        (
            "a node without a type",
            json!({"left": column("YEAR")}),
            [true, true, true],
        ),
        (
            "a numeric literal, which Exasol compares numerically",
            compare("predicate_equal", column("YEAR"), number("2024")),
            [true, true, true],
        ),
        (
            "an empty string, which Exasol reads as NULL",
            equal("YEAR", ""),
            [true, true, true],
        ),
        (
            "a NULL literal",
            compare(
                "predicate_equal",
                column("YEAR"),
                json!({"type": "literal_null"}),
            ),
            [true, true, true],
        ),
        (
            "an IN list holding one non-string element",
            in_list("YEAR", vec![string("2024"), number("2023")]),
            [true, true, true],
        ),
        (
            "an IN list holding one empty string",
            in_list("YEAR", vec![string("2024"), string("")]),
            [true, true, true],
        ),
        (
            "a BETWEEN with one non-string bound",
            between("YEAR", string("2023"), number("2024")),
            [true, true, true],
        ),
        (
            "a range comparison against an empty string",
            compare("predicate_greater", column("YEAR"), string("")),
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
            kept(&equal("YEAR", "2026"), &files),
            [true, false],
            "the Exasol column YEAR resolves the key '{key}'"
        );
    }

    let non_ascii = [
        file_with("größe", Some("xl")),
        file_with("größe", Some("s")),
    ];
    assert_eq!(
        kept(&equal("GRÖSSE", "xl"), &non_ascii),
        [true, false],
        "the fold is Unicode uppercasing, the one the declaration uses"
    );
}

type FileValues = BTreeMap<String, Option<String>>;

fn literal(kind: &str, value: Json) -> Json {
    json!({"type": kind, "value": value})
}

fn date(value: &str) -> Json {
    literal("literal_date", json!(value))
}

fn timestamp(value: &str) -> Json {
    literal("literal_timestamp", json!(value))
}

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
            compare("predicate_less", column("YEAR"), number("10")),
            two_files("year", "9", "10"),
        ),
        (
            "10 > year flips to year < 10",
            compare("predicate_greater", number("10"), column("YEAR")),
            two_files("year", "9", "10"),
        ),
        (
            "an integer IN list",
            in_list("YEAR", vec![number("9"), number("11")]),
            two_files("year", "9", "10"),
        ),
        (
            "day = DATE '2024-03-01'",
            compare("predicate_equal", column("DAY"), date("2024-03-01")),
            two_files("day", "2024-03-01", "2024-02-29"),
        ),
        (
            "a date BETWEEN",
            between("DAY", date("2024-02-01"), date("2024-02-29")),
            two_files("day", "2024-02-29", "2024-03-01"),
        ),
        (
            "amount >= 1.50",
            compare("predicate_greaterequal", column("AMOUNT"), number("1.50")),
            two_files("amount", "1.5", "1.49"),
        ),
        (
            "an integral literal on a decimal column",
            compare("predicate_greater", column("AMOUNT"), number("2")),
            two_files("amount", "10.00", "1.99"),
        ),
        (
            "ts < TIMESTAMP '2024-03-01 00:00:00'",
            compare(
                "predicate_less",
                column("TS"),
                timestamp("2024-03-01 00:00:00"),
            ),
            two_files("ts", "2024-02-29 23:59:59.999999", "2024-03-01 00:00:00"),
        ),
        (
            "region = 'eu' in codepoint order",
            equal("REGION", "eu"),
            two_files("region", "eu", "us"),
        ),
        (
            "active = TRUE",
            compare(
                "predicate_equal",
                column("ACTIVE"),
                literal("literal_bool", json!(true)),
            ),
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
            &compare("predicate_less", column("YEAR"), number("10")),
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
            equal("YEAR", "2024"),
            two_files("year", "2024", "2025"),
        ),
        (
            "a fractional literal on an integer column, which truncation would decide",
            compare("predicate_equal", column("YEAR"), number("2024.5")),
            two_files("year", "2024", "2025"),
        ),
        (
            "an integer literal outside the column's range, which wrapping would decide",
            compare("predicate_equal", column("TINY"), number("300")),
            two_files("tiny", "44", "45"),
        ),
        (
            "a literal finer than the decimal scale, which rounding would decide",
            compare("predicate_equal", column("AMOUNT"), number("1.555")),
            two_files("amount", "1.55", "1.56"),
        ),
        (
            "a timestamp literal finer than the column's unit, which truncation would decide",
            compare(
                "predicate_less",
                column("TS_MS"),
                timestamp("2024-03-01 00:00:00.000500"),
            ),
            two_files("ts_ms", "2024-03-01 00:00:00", "2024-03-01 00:00:00.001"),
        ),
        (
            "a comparison on a float column",
            compare("predicate_greater", column("RATIO"), number("1.0")),
            two_files("ratio", "0.5", "2.0"),
        ),
        (
            "a double literal on an integer column",
            compare(
                "predicate_equal",
                column("YEAR"),
                literal("literal_double", json!("2024")),
            ),
            two_files("year", "2024", "2025"),
        ),
        (
            "a timestamp literal on a date column",
            compare(
                "predicate_equal",
                column("DAY"),
                timestamp("2024-03-01 00:00:00"),
            ),
            two_files("day", "2024-03-01", "2024-03-02"),
        ),
        (
            "a date literal on a timestamp column",
            compare("predicate_equal", column("TS"), date("2024-03-01")),
            two_files("ts", "2024-03-01 00:00:00", "2024-03-02 00:00:00"),
        ),
        (
            "a UTC timestamp literal on a timestamp column",
            compare(
                "predicate_equal",
                column("TS"),
                literal("literal_timestamp_utc", json!("2024-03-01 00:00:00")),
            ),
            two_files("ts", "2024-03-01 00:00:00", "2024-03-02 00:00:00"),
        ),
        (
            "a string literal on a boolean column",
            equal("ACTIVE", "true"),
            two_files("active", "true", "false"),
        ),
        (
            "a string literal on a column of another declared type",
            equal("PAYLOAD", "x"),
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
    let year_2024 = compare("predicate_equal", column("YEAR"), number("2024"));
    assert_eq!(
        kept_under(&year_2024, &declared, &two_files("year", "abc", "2025")),
        [true, false],
        "a value the declared type cannot convert keeps its file for the scan to fail"
    );
}

#[test]
fn an_empty_partition_value_is_null_as_the_scan_reads_it() {
    let files = [file_with("year", Some(""))];
    for (declared, bound) in [(Vec::new(), string("9")), (typed_columns(), number("9"))] {
        assert_eq!(
            kept_under(&is_null("YEAR"), &declared, &files),
            [true],
            "IS NULL holds for an empty value under {declared:?}"
        );
        assert_eq!(
            kept_under(
                &compare("predicate_less", column("YEAR"), bound),
                &declared,
                &files,
            ),
            [false],
            "a comparison against an empty value is NULL, never TRUE, under {declared:?}"
        );
    }
}

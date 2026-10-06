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

/// One column of one row group: its `[min, max]` bounds, its null count over `rows`, and whether
/// a NaN row may exist.
struct ColumnRange {
    bounds: Option<(ScalarValue, ScalarValue)>,
    null_count: Option<u64>,
    rows: u64,
    nan_possible: bool,
}

/// A row group as the footer view describes it: partition values first, then column ranges.
#[derive(Default)]
struct RangeFacts {
    partitions: FileValues,
    ranges: BTreeMap<String, ColumnRange>,
}

impl ColumnFacts for RangeFacts {
    fn orderings(&self, column: &str, literal: &ScalarValue) -> Option<Orderings> {
        if is_partition_key(&self.partitions, column) {
            return self.partitions.orderings(column, literal);
        }
        let range = self.ranges.get(column)?;
        if range.null_count == Some(range.rows) {
            return Some(Orderings::NONE);
        }
        let (min, max) = range.bounds.as_ref()?;
        Orderings::within(min, max, literal).map(|orderings| {
            if range.nan_possible {
                orderings.with_nan_row()
            } else {
                orderings
            }
        })
    }

    fn nulls(&self, column: &str) -> Option<Nulls> {
        if is_partition_key(&self.partitions, column) {
            return self.partitions.nulls(column);
        }
        let range = self.ranges.get(column)?;
        Some(Nulls::counted(range.null_count?, range.rows))
    }
}

fn int64(value: i64) -> ScalarValue {
    ScalarValue::Int64(Some(value))
}

fn float64(value: f64) -> ScalarValue {
    ScalarValue::Float64(Some(value))
}

/// A float row group may always hold a NaN row, which its bounds exclude.
fn bounded(column: &str, min: ScalarValue, max: ScalarValue) -> RangeFacts {
    let nan_possible = matches!(min, ScalarValue::Float64(_));
    RangeFacts {
        ranges: BTreeMap::from([(
            column.to_string(),
            ColumnRange {
                bounds: Some((min, max)),
                null_count: Some(0),
                rows: 4,
                nan_possible,
            },
        )]),
        ..RangeFacts::default()
    }
}

fn nulls_only(column: &str, null_count: Option<u64>) -> RangeFacts {
    RangeFacts {
        ranges: BTreeMap::from([(
            column.to_string(),
            ColumnRange {
                bounds: (null_count != Some(4)).then(|| (int64(1), int64(4))),
                null_count,
                rows: 4,
                nan_possible: false,
            },
        )]),
        ..RangeFacts::default()
    }
}

fn footer_typed(columns: &[(&str, DataType)]) -> Vec<(String, DataType)> {
    columns
        .iter()
        .map(|(name, data_type)| (name.to_string(), data_type.clone()))
        .collect()
}

fn footer_keeps(filter: &Json, columns: &[(&str, DataType)], facts: &RangeFacts) -> bool {
    PartitionPredicate::for_footer_statistics(Some(filter), &footer_typed(columns)).keeps(facts)
}

fn double(value: &str) -> Json {
    literal("literal_double", json!(value))
}

/// Scenario: Row-group statistics evaluate the filter under three-valued logic
#[test]
fn range_facts_evaluate_under_three_valued_logic() {
    let id = [("ID", DataType::Int64)];
    let ranges = [
        ("below [1, 4]", bounded("ID", int64(1), int64(4))),
        ("above [6, 9]", bounded("ID", int64(6), int64(9))),
        ("straddling [3, 7]", bounded("ID", int64(3), int64(7))),
        ("exactly [5, 5]", bounded("ID", int64(5), int64(5))),
    ];
    let five = || number("5");
    let cases: Vec<(&str, Json, [bool; 4])> = vec![
        (
            "=",
            compare("predicate_equal", column("ID"), five()),
            [false, false, true, true],
        ),
        (
            "<>",
            compare("predicate_notequal", column("ID"), five()),
            [true, true, true, false],
        ),
        (
            "<",
            compare("predicate_less", column("ID"), five()),
            [true, false, true, false],
        ),
        (
            "<=",
            compare("predicate_lessequal", column("ID"), five()),
            [true, false, true, true],
        ),
        (
            ">",
            compare("predicate_greater", column("ID"), five()),
            [false, true, true, false],
        ),
        (
            ">=",
            compare("predicate_greaterequal", column("ID"), five()),
            [false, true, true, true],
        ),
        (
            "> with the column on the right",
            compare("predicate_greater", five(), column("ID")),
            [true, false, true, false],
        ),
        (
            "NOT",
            not(compare("predicate_less", column("ID"), five())),
            [false, true, true, true],
        ),
        (
            "OR with an Opaque branch",
            or(vec![
                compare("predicate_greater", column("ID"), number("100")),
                json!({"type": "predicate_equal",
                       "left": {"type": "function_scalar", "name": "MULT",
                                "arguments": [column("ID"), number("2")]},
                       "right": number("6")}),
            ]),
            [true, true, true, true],
        ),
        (
            "IN",
            in_list("ID", vec![number("2"), number("8")]),
            [true, true, false, false],
        ),
        (
            "BETWEEN",
            between("ID", number("6"), number("7")),
            [false, true, true, false],
        ),
    ];
    for (label, filter, expected) in cases {
        let kept: Vec<bool> = ranges
            .iter()
            .map(|(_, facts)| footer_keeps(&filter, &id, facts))
            .collect();
        assert_eq!(
            kept,
            expected,
            "{label}: kept per row group {:?}",
            ranges.map(|(name, _)| name)
        );
    }

    let null_counts = [Some(0), Some(2), Some(4), None];
    for (label, filter, expected) in [
        ("IS NULL", is_null("ID"), [false, true, true, true]),
        ("IS NOT NULL", is_not_null("ID"), [true, true, false, true]),
    ] {
        let kept: Vec<bool> = null_counts
            .iter()
            .map(|&null_count| footer_keeps(&filter, &id, &nulls_only("ID", null_count)))
            .collect();
        assert_eq!(
            kept, expected,
            "{label}: kept per null count of 4 rows {null_counts:?}"
        );
    }

    let all_null = nulls_only("ID", Some(4));
    for (label, filter) in [
        ("=", compare("predicate_equal", column("ID"), five())),
        ("<>", compare("predicate_notequal", column("ID"), five())),
        (
            "NOT =",
            not(compare("predicate_equal", column("ID"), five())),
        ),
    ] {
        assert!(
            !footer_keeps(&filter, &id, &all_null),
            "{label}: an all-NULL row group reaches only NULL"
        );
    }

    let partitioned = |grp: &str, min: i64, max: i64| RangeFacts {
        partitions: file_with("grp", Some(grp)),
        ..bounded("ID", int64(min), int64(max))
    };
    let grp_or_id = or(vec![
        equal("GRP", "a"),
        compare("predicate_greater", column("ID"), number("100")),
    ]);
    let grp_and_id = [("ID", DataType::Int64), ("grp", DataType::Utf8)];
    assert_eq!(
        [
            footer_keeps(&grp_or_id, &grp_and_id, &partitioned("a", 1, 8)),
            footer_keeps(&grp_or_id, &grp_and_id, &partitioned("b", 9, 16)),
        ],
        [true, false],
        "a partition value and a data column under OR prune on both"
    );

    let boolean = [("B", DataType::Boolean)];
    let bools = |min: bool, max: bool| RangeFacts {
        ranges: BTreeMap::from([(
            "B".to_string(),
            ColumnRange {
                bounds: Some((
                    ScalarValue::Boolean(Some(min)),
                    ScalarValue::Boolean(Some(max)),
                )),
                null_count: Some(0),
                rows: 4,
                nan_possible: false,
            },
        )]),
        ..RangeFacts::default()
    };
    let b_true = || literal("literal_bool", json!(true));
    assert!(!footer_keeps(
        &compare("predicate_equal", column("B"), b_true()),
        &boolean,
        &bools(false, false)
    ));
    assert!(!footer_keeps(
        &compare("predicate_notequal", column("B"), b_true()),
        &boolean,
        &bools(true, true)
    ));
    assert!(footer_keeps(
        &compare("predicate_equal", column("B"), b_true()),
        &boolean,
        &bools(false, true)
    ));
}

/// Scenario: A float column prunes ordering and equality comparisons on its bounds alone
#[test]
fn float_comparisons_prune_on_bounds_and_negations_keep() {
    let d = [("D", DataType::Float64)];
    let one_to_four = bounded("D", float64(1.0), float64(4.0));
    let compare_d = |kind: &str, value: &str| compare(kind, column("D"), number(value));
    for (label, filter, expected) in [
        ("D > 5", compare_d("predicate_greater", "5"), false),
        ("D = 5", compare_d("predicate_equal", "5"), false),
        ("D < 5", compare_d("predicate_less", "5"), true),
        (
            "D IN (5, 6)",
            in_list("D", vec![number("5"), number("6")]),
            false,
        ),
        (
            "D BETWEEN 5 AND 6",
            between("D", number("5"), number("6")),
            false,
        ),
        ("NOT (D < 5)", not(compare_d("predicate_less", "5")), true),
        (
            "NOT (D > 0)",
            not(compare_d("predicate_greater", "0")),
            true,
        ),
        (
            "NOT (D IN (5, 6))",
            not(in_list("D", vec![number("5"), number("6")])),
            true,
        ),
        (
            "NOT (NOT (D > 5))",
            not(not(compare_d("predicate_greater", "5"))),
            false,
        ),
    ] {
        assert_eq!(
            footer_keeps(&filter, &d, &one_to_four),
            expected,
            "{label} over [1, 4] with a possible NaN row"
        );
    }
    assert!(
        footer_keeps(
            &compare_d("predicate_notequal", "3"),
            &d,
            &bounded("D", float64(3.0), float64(3.0))
        ),
        "D <> 3 over [3, 3] keeps the file for its possible NaN row"
    );
}

/// `C = literal` over a row group bounded `[value, value]` and one bounded just above it, which
/// is `[true, false]` exactly when the footer scope converts `literal` to `value`.
fn kept_at_and_above(column_type: DataType, literal: Json, value: f64) -> [bool; 2] {
    let filter = compare("predicate_equal", column("C"), literal);
    let columns = [("C", column_type)];
    [value, value.next_up()].map(|bound| {
        footer_keeps(
            &filter,
            &columns,
            &bounded("C", float64(bound), float64(bound)),
        )
    })
}

/// `C = literal` over a row group bounded `[1, 4]`, which excludes every literal it is given, so
/// the row group is kept only when the literal does not convert.
fn kept_outside_bounds(column_type: DataType, literal: Json) -> bool {
    footer_keeps(
        &compare("predicate_equal", column("C"), literal),
        &[("C", column_type)],
        &bounded("C", float64(1.0), float64(4.0)),
    )
}

/// Scenario: A float bound or literal is widened or rejected so it never drops a matching row
#[test]
fn float_literals_convert_to_the_scans_double_only_for_footer_statistics() {
    for (literal, value) in [
        (number("2"), 2.0),
        (number("2.0"), 2.0),
        (number("2.5"), 2.5),
        (double("2E0"), 2.0),
        (double("2.0000000000000000e+00"), 2.0),
        (number("0.1"), 0.1_f64),
    ] {
        assert_eq!(
            kept_at_and_above(DataType::Float64, literal.clone(), value),
            [true, false],
            "{literal} converts to the DOUBLE the scan parses"
        );
    }
    let float_partition = [("C".to_string(), DataType::Float64)];
    assert_eq!(
        kept_under(
            &compare("predicate_equal", column("C"), number("0.1")),
            &float_partition,
            &[file_with("c", Some("5"))],
        ),
        [true],
        "a float partition column keeps every file"
    );
    for non_finite in [double("1E400"), double("NaN"), double("inf")] {
        assert!(
            kept_outside_bounds(DataType::Float64, non_finite.clone()),
            "{non_finite} is not finite"
        );
    }
    assert_eq!(
        kept_at_and_above(DataType::Float32, number("16777216"), 16_777_216.0),
        [true, false],
        "an integer FLOAT represents exactly converts"
    );
    assert!(
        kept_outside_bounds(DataType::Float32, number("16777217")),
        "the scan rounds 16777217 to FLOAT before comparing"
    );
    assert_eq!(
        kept_at_and_above(DataType::Float32, number("4.5"), 4.5),
        [true, false],
        "the scan compares a FLOAT column against a non-integer literal in DOUBLE"
    );

    let outside_x = RangeFacts {
        ranges: BTreeMap::from([(
            "X".to_string(),
            ColumnRange {
                bounds: Some((
                    ScalarValue::Utf8(Some("b".to_string())),
                    ScalarValue::Utf8(Some("c".to_string())),
                )),
                null_count: Some(0),
                rows: 4,
                nan_possible: false,
            },
        )]),
        ..RangeFacts::default()
    };
    assert!(
        footer_keeps(&equal("X", "a"), &[], &outside_x),
        "an undeclared column is Opaque under the footer scope"
    );
    assert_eq!(
        kept(
            &equal("X", "a"),
            &[file_with("x", Some("a")), file_with("x", Some("b"))]
        ),
        [true, false],
        "an undeclared column compares as text under the partition scope"
    );
}

fn timestamp_at(unit: TimeUnit, seconds: i64, nanos: i64) -> ScalarValue {
    let value = Some(match unit {
        TimeUnit::Second => seconds,
        TimeUnit::Millisecond => seconds * 1_000 + nanos / 1_000_000,
        TimeUnit::Microsecond => seconds * 1_000_000 + nanos / 1_000,
        TimeUnit::Nanosecond => seconds * 1_000_000_000 + nanos,
    });
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(value, None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, None),
    }
}

const EPOCH_SECONDS_2024_01_01: i64 = 1_704_067_200;

/// Scenario: Row-group statistics evaluate the filter under three-valued logic
#[test]
fn footer_literals_convert_only_to_the_scans_value() {
    let amount = || DataType::Decimal128(10, 2);
    let cents = |value: i128| ScalarValue::Decimal128(Some(value), 10, 2);
    let instant = |unit: TimeUnit, offset_seconds: i64| {
        timestamp_at(unit, EPOCH_SECONDS_2024_01_01 + offset_seconds, 0)
    };
    let cases: Vec<(&str, Json, DataType, RangeFacts, bool)> = vec![
        (
            "AMOUNT > 1234567.89",
            compare("predicate_greater", column("C"), number("1234567.89")),
            amount(),
            bounded("C", cents(123_456_789), cents(123_456_789)),
            true,
        ),
        (
            "AMOUNT < 5",
            compare("predicate_less", column("C"), number("5")),
            amount(),
            bounded("C", cents(1_000), cents(2_000)),
            false,
        ),
        (
            "AMOUNT BETWEEN 300 AND 1234567.89",
            between("C", number("300"), number("1234567.89")),
            amount(),
            bounded("C", cents(10_000), cents(20_000)),
            true,
        ),
        (
            "TS = a literal with a non-zero ninth fraction digit",
            compare(
                "predicate_equal",
                column("C"),
                timestamp("2024-01-01 00:00:00.000000500"),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            bounded(
                "C",
                instant(TimeUnit::Nanosecond, 0),
                instant(TimeUnit::Nanosecond, 0),
            ),
            true,
        ),
        (
            "TS > a microsecond literal",
            compare(
                "predicate_greater",
                column("C"),
                timestamp("2024-01-01 00:00:01.000000"),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            bounded(
                "C",
                instant(TimeUnit::Nanosecond, 0),
                instant(TimeUnit::Nanosecond, 0),
            ),
            false,
        ),
        (
            "TS_MS = a literal finer than milliseconds",
            compare(
                "predicate_equal",
                column("C"),
                timestamp("2024-01-01 00:00:00.000500"),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            bounded(
                "C",
                instant(TimeUnit::Millisecond, 0),
                instant(TimeUnit::Millisecond, 0),
            ),
            true,
        ),
        (
            "TS_S < a whole-second literal",
            compare(
                "predicate_less",
                column("C"),
                timestamp("2024-01-01 00:00:00"),
            ),
            DataType::Timestamp(TimeUnit::Second, None),
            bounded(
                "C",
                instant(TimeUnit::Second, 1),
                instant(TimeUnit::Second, 2),
            ),
            false,
        ),
    ];
    for (label, filter, column_type, facts, expected) in cases {
        assert_eq!(
            footer_keeps(&filter, &[("C", column_type)], &facts),
            expected,
            "{label}"
        );
    }

    let nanosecond = || DataType::Timestamp(TimeUnit::Nanosecond, None);
    for (literal, column_type, [exact, other], excluding) in [
        (
            number("1234567.89"),
            amount(),
            ["1234567.89", "1234567.88"],
            bounded("C", cents(1_000), cents(2_000)),
        ),
        (
            timestamp("2024-01-01 00:00:00.000000500"),
            nanosecond(),
            [
                "2024-01-01 00:00:00.000000500",
                "2024-01-01 00:00:00.000000501",
            ],
            bounded(
                "C",
                instant(TimeUnit::Nanosecond, 1),
                instant(TimeUnit::Nanosecond, 2),
            ),
        ),
    ] {
        let filter = compare("predicate_equal", column("C"), literal.clone());
        assert_eq!(
            kept_under(
                &filter,
                &[("C".to_string(), column_type.clone())],
                &[file_with("c", Some(exact)), file_with("c", Some(other))],
            ),
            [true, false],
            "{literal} still converts exactly to {column_type} under the partition scope"
        );
        assert!(
            footer_keeps(&filter, &[("C", column_type.clone())], &excluding),
            "{literal} does not convert to {column_type} under the footer scope"
        );
    }
}

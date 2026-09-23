use super::*;
use serde_json::json;

fn column(name: &str) -> Json {
    json!({"type": "column", "name": name, "tableName": "SALES"})
}

fn string(value: &str) -> Json {
    json!({"type": "literal_string", "value": value})
}

fn number(value: &str) -> Json {
    json!({"type": "literal_exactnumeric", "value": value})
}

fn compare(kind: &str, left: Json, right: Json) -> Json {
    json!({"type": kind, "left": left, "right": right})
}

fn equal(name: &str, value: &str) -> Json {
    compare("predicate_equal", column(name), string(value))
}

fn not(expression: Json) -> Json {
    json!({"type": "predicate_not", "expression": expression})
}

fn and(expressions: Vec<Json>) -> Json {
    json!({"type": "predicate_and", "expressions": expressions})
}

fn or(expressions: Vec<Json>) -> Json {
    json!({"type": "predicate_or", "expressions": expressions})
}

fn is_null(name: &str) -> Json {
    json!({"type": "predicate_is_null", "expression": column(name)})
}

fn is_not_null(name: &str) -> Json {
    json!({"type": "predicate_is_not_null", "expression": column(name)})
}

fn in_list(name: &str, arguments: Vec<Json>) -> Json {
    json!({"type": "predicate_in_constlist", "expression": column(name), "arguments": arguments})
}

fn between(name: &str, low: Json, high: Json) -> Json {
    json!({"type": "predicate_between", "expression": column(name), "left": low, "right": high})
}

/// A filter no partition column can decide.
fn on_a_data_column() -> Json {
    compare("predicate_equal", column("ID"), number("1"))
}

fn file_with(key: &str, value: Option<&str>) -> BTreeMap<String, Option<String>> {
    BTreeMap::from([(key.to_string(), value.map(str::to_string))])
}

/// Which of `files` the filter keeps, in order.
fn kept(filter: &Json, files: &[BTreeMap<String, Option<String>>]) -> Vec<bool> {
    let predicate = PartitionPredicate::from_filter(Some(filter));
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
    ];

    for (label, filter, expected) in cases {
        assert_eq!(
            kept(&filter, &files),
            expected,
            "{label}: kept per file [2026, 2025, NULL]"
        );
    }

    let unfiltered = PartitionPredicate::from_filter(None);
    assert!(
        files.iter().all(|values| unfiltered.keeps(values)),
        "an absent filter keeps every file"
    );
}

#[test]
fn a_non_string_or_empty_literal_never_prunes() {
    let files = year_files();
    let cases: Vec<(&str, Json)> = vec![
        (
            "a numeric literal, which Exasol compares numerically",
            compare("predicate_equal", column("YEAR"), number("2024")),
        ),
        (
            "an empty string, which Exasol reads as NULL",
            equal("YEAR", ""),
        ),
        (
            "a NULL literal",
            compare(
                "predicate_equal",
                column("YEAR"),
                json!({"type": "literal_null"}),
            ),
        ),
        (
            "an IN list holding one non-string element",
            in_list("YEAR", vec![string("2024"), number("2023")]),
        ),
        (
            "an IN list holding one empty string",
            in_list("YEAR", vec![string("2024"), string("")]),
        ),
        (
            "a BETWEEN with one non-string bound",
            between("YEAR", string("2023"), number("2024")),
        ),
        (
            "a range comparison against an empty string",
            compare("predicate_greater", column("YEAR"), string("")),
        ),
    ];

    for (label, filter) in cases {
        assert_eq!(
            kept(&filter, &files),
            [true, true, true],
            "{label}: must keep every file"
        );
    }
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

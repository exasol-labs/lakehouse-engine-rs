use crate::adapter::pushdown::test_support::{
    alias_col, and, ast, between, binary_lit, column_types_under, compare, declared_column_types,
    fixed_binary_lit, in_list, not, nullable_field as field, or, predicate_equal, sales_col,
    sales_equal, scan_filter_sql, str_lit,
};

use super::*;
use crate::types::exasol_type::ExasolType;
use serde_json::Value as Json;
use vs_expression::CompareOp;

/// The `SALES` column `name`.
fn column(name: &str) -> Expr {
    ast(&sales_col(name))
}

/// [`super::compare_binary_bytes`] over the filter node `filter`.
fn compare_binary_bytes(filter: Json, columns: &ColumnTypes) -> Expr {
    super::compare_binary_bytes(ast(&filter), columns)
}

/// [`super::with_binary_literals`] over the filter node `filter`.
fn with_binary_literals(filter: Json, columns: &ColumnTypes) -> Expr {
    super::with_binary_literals(ast(&filter), columns)
}

/// The involved columns of a table whose `B` and `U` read as hex text and `S` as text.
const HEX_COLUMNS: [(&str, ExasolType); 3] = [
    ("B", ExasolType::WIDEST_VARCHAR),
    ("U", ExasolType::varchar(4)),
    ("S", ExasolType::WIDEST_VARCHAR),
];

fn binary_fields() -> [LogicalField; 3] {
    [
        field("b", "binary"),
        field("u", "fixed_size_binary(2)"),
        field("s", "utf8"),
    ]
}

fn hex_types() -> ColumnTypes {
    declared_column_types(&HEX_COLUMNS, &binary_fields())
}

fn rendered(filter: &Json) -> String {
    scan_filter_sql(filter, &hex_types()).expect("renders")
}

#[test]
fn a_lowercase_hex_comparison_compares_the_raw_bytes() {
    assert_eq!(
        rendered(&sales_equal("B", "0a1b")),
        "(\"_LH_BYTES_B\" = X'0a1b')"
    );
    assert_eq!(
        rendered(&compare("predicate_less", str_lit("0a1b"), sales_col("U"))),
        "(\"_LH_BYTES_U\" > arrow_cast(X'0a1b', 'FixedSizeBinary(2)'))",
        "a literal on the left mirrors the operator, and a fixed column gets a fixed literal"
    );
}

#[test]
fn an_operator_the_empty_value_satisfies_also_requires_a_non_empty_value() {
    for op in [
        "predicate_notequal",
        "predicate_less",
        "predicate_lessequal",
    ] {
        let sql = rendered(&compare(op, sales_col("B"), str_lit("0a")));
        assert!(sql.contains("(\"_LH_BYTES_B\" <> X'')"), "{op}: {sql}");
        let fixed = rendered(&compare(op, sales_col("U"), str_lit("0a0b")));
        assert!(
            !fixed.contains("X''"),
            "a fixed value is never empty: {fixed}"
        );
    }
    let sql = rendered(&compare(
        "predicate_greaterequal",
        sales_col("B"),
        str_lit("0a"),
    ));
    assert_eq!(sql, "(\"_LH_BYTES_B\" >= X'0a')");
}

#[test]
fn in_and_between_compare_the_raw_bytes_when_every_literal_is_hex() {
    let in_list = in_list(sales_col("B"), [str_lit("00"), str_lit("ff")]);
    assert_eq!(rendered(&in_list), "(\"_LH_BYTES_B\" IN (X'00', X'ff'))");
    let between = between(sales_col("B"), str_lit("00"), str_lit("0f"));
    assert_eq!(
        rendered(&between),
        "(\"_LH_BYTES_B\" BETWEEN X'00' AND X'0f')"
    );
}

#[test]
fn a_literal_that_names_no_byte_string_compares_the_hex_text() {
    let cases = [
        sales_equal("B", "0A1B"),
        sales_equal("B", "0a1"),
        sales_equal("B", ""),
        sales_equal("B", "zz"),
        sales_equal("U", "0a"),
        sales_equal("S", "0a"),
    ];
    for filter in cases {
        assert_eq!(
            compare_binary_bytes(filter.clone(), &hex_types()),
            ast(&filter),
            "{filter}"
        );
    }
}

#[test]
fn only_a_comparison_reached_through_and_or_is_rewritten() {
    let equal = sales_equal("B", "0a");
    let negated = not(equal.clone());
    assert_eq!(
        compare_binary_bytes(negated.clone(), &hex_types()),
        ast(&negated),
        "NOT turns the empty value's false into true, where Exasol keeps NULL"
    );
    let junction = or([and([equal, negated]), sales_equal("S", "x")]);
    assert_eq!(
        rendered(&junction),
        "(((\"_LH_BYTES_B\" = X'0a') AND (NOT (nullif(\"B\", '') = '0a'))) OR (\"S\" = 'x'))"
    );
}

/// Scenario: BINARY_AS_STRING reads a variable-length binary column as UTF-8 text
#[test]
fn binary_as_string_keeps_a_variable_length_filter_on_the_text() {
    let filter = and([sales_equal("B", "00ff"), sales_equal("U", "00ff")]);

    let rewritten = compare_binary_bytes(
        filter.clone(),
        &column_types_under(&HEX_COLUMNS, &binary_fields(), true),
    );

    let (rewritten, filter) = (rewritten.conjuncts(), ast(&filter));
    let filter = filter.conjuncts();
    assert_eq!(
        rewritten[0], filter[0],
        "a column read as text compares its text"
    );
    assert_ne!(
        rewritten[1], filter[1],
        "a fixed-length column keeps its hex rendering and its byte comparison"
    );
}

/// Scenario: A hex-literal filter on a binary column prunes files at plan time
#[test]
fn pruning_retypes_the_hex_literals_of_a_binary_column_at_any_depth() {
    let filter = not(or([
        sales_equal("B", "0a1b"),
        sales_equal("U", "0a0b"),
        between(sales_col("B"), str_lit("00"), str_lit("ff")),
        in_list(sales_col("B"), [str_lit("0a"), str_lit("0A")]),
    ]));
    assert_eq!(
        with_binary_literals(filter, &hex_types()),
        Expr::Not(Box::new(Expr::Or(vec![
            Expr::compare(CompareOp::Equal, column("B"), binary_lit("0a1b")),
            Expr::compare(CompareOp::Equal, column("U"), fixed_binary_lit("0a0b", 2)),
            Expr::Between {
                expression: Box::new(column("B")),
                low: Box::new(binary_lit("00")),
                high: Box::new(binary_lit("ff")),
            },
            Expr::InList {
                expression: Box::new(column("B")),
                values: vec![binary_lit("0a"), ast(&str_lit("0A"))],
            },
        ]))),
        "the column keeps its name, and an uppercase literal stays text"
    );
}

#[test]
fn pruning_leaves_text_columns_and_mismatched_literals_alone() {
    for (filter, under_binary_as_string) in [
        (sales_equal("S", "0a"), false),
        (sales_equal("U", "0a"), false),
        (sales_equal("B", "0a1"), false),
        (sales_equal("B", "0a"), true),
    ] {
        assert_eq!(
            with_binary_literals(
                filter.clone(),
                &column_types_under(&HEX_COLUMNS, &binary_fields(), under_binary_as_string)
            ),
            ast(&filter)
        );
    }
    assert_eq!(
        with_binary_literals(
            sales_equal("U", "0a0b"),
            &column_types_under(&HEX_COLUMNS, &binary_fields(), true)
        ),
        Expr::compare(CompareOp::Equal, column("U"), fixed_binary_lit("0a0b", 2)),
        "BINARY_AS_STRING leaves a fixed column hex"
    );
}

#[test]
fn a_binary_literal_carries_its_bytes() {
    assert_eq!(
        literal_binary_bytes(&binary_lit("00abff")),
        Some(vec![0x00, 0xab, 0xff])
    );
    assert_eq!(literal_binary_bytes(&ast(&str_lit("00abff"))), None);
}

#[test]
fn per_relation_detection_resolves_a_shared_column_name_by_its_own_relation() {
    let hex_x = [("X", ExasolType::WIDEST_VARCHAR)];
    let columns = ColumnTypes::per_relation([
        (
            "LHS_T0".to_string(),
            &declared_column_types(&hex_x, &[field("x", "binary")]),
        ),
        (
            "LHS_T1".to_string(),
            &declared_column_types(&hex_x, &[field("x", "utf8")]),
        ),
    ]);
    let on = |alias: &str| predicate_equal(alias_col(alias, "X"), str_lit("0a1b"));
    assert_eq!(
        scan_filter_sql(&on("LHS_T0"), &columns).expect("renders"),
        "(\"LHS_T0\".\"_LH_BYTES_X\" = X'0a1b')",
        "the binary leg compares its own relation's raw bytes"
    );
    assert_eq!(
        compare_binary_bytes(on("LHS_T1"), &columns),
        ast(&on("LHS_T1")),
        "the text leg's same-named column stays a text comparison"
    );
}

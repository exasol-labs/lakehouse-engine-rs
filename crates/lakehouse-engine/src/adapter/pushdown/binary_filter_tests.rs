use crate::adapter::pushdown::test_support::untyped_scan_filter;
use crate::adapter::pushdown::test_support::{
    alias_col, and, between, binary_lit, column_types_under, compare, declared_column_types,
    fixed_binary_lit, in_list, not, nullable_field as field, or, predicate_equal, sales_col,
    sales_equal, str_lit,
};

use super::*;
use crate::types::exasol_type::ExasolType;

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

fn rendered(filter: &Json) -> String {
    let types = declared_column_types(&HEX_COLUMNS, &binary_fields());
    vs_expression::render_scan_filter(&compare_binary_bytes(filter.clone(), &types), &types)
        .expect("renders")
        .expect("a comparison is never trivially true")
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
    for literal in ["0A1B", "0a1", "", "zz"] {
        let filter = sales_equal("B", literal);
        assert_eq!(
            compare_binary_bytes(
                filter.clone(),
                &declared_column_types(&HEX_COLUMNS, &binary_fields())
            ),
            filter,
            "{literal}"
        );
    }
    let wrong_width = sales_equal("U", "0a");
    assert_eq!(
        compare_binary_bytes(
            wrong_width.clone(),
            &declared_column_types(&HEX_COLUMNS, &binary_fields())
        ),
        wrong_width
    );
    let string_column = sales_equal("S", "0a");
    assert_eq!(
        compare_binary_bytes(
            string_column.clone(),
            &declared_column_types(&HEX_COLUMNS, &binary_fields())
        ),
        string_column
    );
}

#[test]
fn only_a_comparison_reached_through_and_or_is_rewritten() {
    let equal = sales_equal("B", "0a");
    let negated = not(equal.clone());
    assert_eq!(
        compare_binary_bytes(
            negated.clone(),
            &declared_column_types(&HEX_COLUMNS, &binary_fields())
        ),
        negated,
        "NOT turns the empty value's false into true, where Exasol keeps NULL"
    );
    let junction = or([and([equal, negated]), sales_equal("S", "x")]);
    assert_eq!(
        rendered(&junction),
        "(((\"_LH_BYTES_B\" = X'0a') AND (NOT (\"B\" = '0a'))) OR (\"S\" = 'x'))"
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

    assert_eq!(
        rewritten["expressions"][0], filter["expressions"][0],
        "a column read as text compares its text"
    );
    assert_ne!(
        rewritten["expressions"][1], filter["expressions"][1],
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
        with_binary_literals(
            filter,
            &declared_column_types(&HEX_COLUMNS, &binary_fields())
        ),
        not(or([
            predicate_equal(sales_col("B"), binary_lit("0a1b")),
            predicate_equal(sales_col("U"), fixed_binary_lit("0a0b", 2)),
            between(sales_col("B"), binary_lit("00"), binary_lit("ff")),
            in_list(sales_col("B"), [binary_lit("0a"), str_lit("0A")]),
        ])),
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
            filter
        );
    }
    assert_eq!(
        with_binary_literals(
            sales_equal("U", "0a0b"),
            &column_types_under(&HEX_COLUMNS, &binary_fields(), true)
        ),
        predicate_equal(sales_col("U"), fixed_binary_lit("0a0b", 2)),
        "BINARY_AS_STRING leaves a fixed column hex"
    );
}

#[test]
fn a_binary_literal_carries_its_bytes() {
    assert_eq!(
        literal_binary_bytes(&binary_lit("00abff")),
        Some(vec![0x00, 0xab, 0xff])
    );
    assert_eq!(literal_binary_bytes(&str_lit("00abff")), None);
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
        untyped_scan_filter(&compare_binary_bytes(on("LHS_T0"), &columns)).expect("renders"),
        "(\"LHS_T0\".\"_LH_BYTES_X\" = X'0a1b')",
        "the binary leg compares its own relation's raw bytes"
    );
    assert_eq!(
        compare_binary_bytes(on("LHS_T1"), &columns),
        on("LHS_T1"),
        "the text leg's same-named column stays a text comparison"
    );
}

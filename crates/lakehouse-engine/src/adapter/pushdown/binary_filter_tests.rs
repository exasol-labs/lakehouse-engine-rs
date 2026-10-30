use vs_expression::render_df_filter_safe;

use super::*;
use crate::adapter::pushdown::test_support::filter_json::{column, compare, string as text};
use crate::adapter::pushdown::test_support::nullable_field as field;

fn columns() -> BinaryColumns {
    columns_under(false)
}

fn columns_under(binary_as_string: bool) -> BinaryColumns {
    BinaryColumns::of(
        &[
            field("b", "binary"),
            field("u", "fixed_size_binary(2)"),
            field("s", "utf8"),
        ],
        binary_as_string,
    )
}

fn rendered(filter: &Json) -> String {
    render_df_filter_safe(&compare_binary_bytes(filter.clone(), &columns())).expect("renders")
}

#[test]
fn a_lowercase_hex_comparison_compares_the_raw_bytes() {
    assert_eq!(
        rendered(&compare("predicate_equal", column("B"), text("0a1b"))),
        "(\"_LH_BYTES_B\" = X'0a1b')"
    );
    assert_eq!(
        rendered(&compare("predicate_less", text("0a1b"), column("U"))),
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
        let sql = rendered(&compare(op, column("B"), text("0a")));
        assert!(sql.contains("(\"_LH_BYTES_B\" <> X'')"), "{op}: {sql}");
        let fixed = rendered(&compare(op, column("U"), text("0a0b")));
        assert!(
            !fixed.contains("X''"),
            "a fixed value is never empty: {fixed}"
        );
    }
    let sql = rendered(&compare("predicate_greaterequal", column("B"), text("0a")));
    assert_eq!(sql, "(\"_LH_BYTES_B\" >= X'0a')");
}

#[test]
fn in_and_between_compare_the_raw_bytes_when_every_literal_is_hex() {
    let in_list = json!({
        "type": "predicate_in_constlist",
        "expression": column("B"),
        "arguments": [text("00"), text("ff")],
    });
    assert_eq!(rendered(&in_list), "(\"_LH_BYTES_B\" IN (X'00', X'ff'))");
    let between = json!({
        "type": "predicate_between",
        "expression": column("B"),
        "left": text("00"),
        "right": text("0f"),
    });
    assert_eq!(
        rendered(&between),
        "(\"_LH_BYTES_B\" BETWEEN X'00' AND X'0f')"
    );
}

#[test]
fn a_literal_that_names_no_byte_string_compares_the_hex_text() {
    for literal in ["0A1B", "0a1", "", "zz"] {
        let filter = compare("predicate_equal", column("B"), text(literal));
        assert_eq!(
            compare_binary_bytes(filter.clone(), &columns()),
            filter,
            "{literal}"
        );
    }
    let wrong_width = compare("predicate_equal", column("U"), text("0a"));
    assert_eq!(
        compare_binary_bytes(wrong_width.clone(), &columns()),
        wrong_width
    );
    let string_column = compare("predicate_equal", column("S"), text("0a"));
    assert_eq!(
        compare_binary_bytes(string_column.clone(), &columns()),
        string_column
    );
}

#[test]
fn only_a_comparison_reached_through_and_or_is_rewritten() {
    let equal = compare("predicate_equal", column("B"), text("0a"));
    let negated = json!({"type": "predicate_not", "expression": equal});
    assert_eq!(
        compare_binary_bytes(negated.clone(), &columns()),
        negated,
        "NOT turns the empty value's false into true, where Exasol keeps NULL"
    );
    let junction = json!({
        "type": "predicate_or",
        "expressions": [
            {"type": "predicate_and", "expressions": [equal.clone(), negated.clone()]},
            compare("predicate_equal", column("S"), text("x")),
        ],
    });
    assert_eq!(
        rendered(&junction),
        "(((\"_LH_BYTES_B\" = X'0a') AND (NOT (\"B\" = '0a'))) OR (\"S\" = 'x'))"
    );
}

/// Scenario: BINARY_AS_STRING reads a variable-length binary column as UTF-8 text
#[test]
fn binary_as_string_keeps_a_variable_length_filter_on_the_text() {
    let filter = json!({"type": "predicate_and", "expressions": [
        compare("predicate_equal", column("B"), text("00ff")),
        compare("predicate_equal", column("U"), text("00ff")),
    ]});

    let rewritten = compare_binary_bytes(filter.clone(), &columns_under(true));

    assert_eq!(
        rewritten["expressions"][0], filter["expressions"][0],
        "a column read as text compares its text"
    );
    assert_ne!(
        rewritten["expressions"][1], filter["expressions"][1],
        "a fixed-length column keeps its hex rendering and its byte comparison"
    );
}

fn equal(column_name: &str, hex: &str) -> Json {
    compare("predicate_equal", column(column_name), text(hex))
}

/// Scenario: A hex-literal filter on a binary column prunes files at plan time
#[test]
fn pruning_retypes_the_hex_literals_of_a_binary_column_at_any_depth() {
    let filter = json!({
        "type": "predicate_not",
        "expression": {"type": "predicate_or", "expressions": [
            equal("B", "0a1b"),
            equal("U", "0a0b"),
            {"type": "predicate_between", "expression": column("B"),
             "left": text("00"), "right": text("ff")},
            {"type": "predicate_in_constlist", "expression": column("B"),
             "arguments": [text("0a"), text("0A")]},
        ]},
    });
    assert_eq!(
        with_binary_literals(filter, &columns()),
        json!({
            "type": "predicate_not",
            "expression": {"type": "predicate_or", "expressions": [
                compare("predicate_equal", column("B"), json!({"type": "literal_binary", "value": "0a1b"})),
                compare("predicate_equal", column("U"),
                    json!({"type": "literal_binary", "value": "0a0b", "width": 2})),
                {"type": "predicate_between", "expression": column("B"),
                 "left": {"type": "literal_binary", "value": "00"},
                 "right": {"type": "literal_binary", "value": "ff"}},
                {"type": "predicate_in_constlist", "expression": column("B"),
                 "arguments": [{"type": "literal_binary", "value": "0a"}, text("0A")]},
            ]},
        }),
        "the column keeps its name, and an uppercase literal stays text"
    );
}

#[test]
fn pruning_leaves_text_columns_and_mismatched_literals_alone() {
    for (filter, under_binary_as_string) in [
        (equal("S", "0a"), false),
        (equal("U", "0a"), false),
        (equal("B", "0a1"), false),
        (equal("B", "0a"), true),
    ] {
        assert_eq!(
            with_binary_literals(filter.clone(), &columns_under(under_binary_as_string)),
            filter
        );
    }
    assert_eq!(
        with_binary_literals(equal("U", "0a0b"), &columns_under(true)),
        compare(
            "predicate_equal",
            column("U"),
            json!({"type": "literal_binary", "value": "0a0b", "width": 2})
        ),
        "BINARY_AS_STRING leaves a fixed column hex"
    );
}

#[test]
fn a_binary_literal_carries_its_bytes() {
    assert_eq!(
        literal_binary_bytes(&json!({"type": "literal_binary", "value": "00abff"})),
        Some(vec![0x00, 0xab, 0xff])
    );
    assert_eq!(literal_binary_bytes(&text("00abff")), None);
}

use vs_expression::render_df_filter_safe;

use super::*;

fn columns() -> BinaryColumns {
    columns_under(false)
}

fn columns_under(binary_as_string: bool) -> BinaryColumns {
    let field = |name: &str, arrow_type: &str| LogicalField {
        field_id: None,
        name: name.to_string(),
        arrow_type: arrow_type.to_string(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    };
    BinaryColumns::of(
        &[
            field("b", "binary"),
            field("u", "fixed_size_binary(2)"),
            field("s", "utf8"),
        ],
        binary_as_string,
    )
}

fn column(name: &str) -> Json {
    json!({"type": "column", "name": name, "tableName": "T"})
}

fn text(value: &str) -> Json {
    json!({"type": "literal_string", "value": value})
}

fn compare(op: &str, left: Json, right: Json) -> Json {
    json!({"type": op, "left": left, "right": right})
}

fn rendered(filter: &Json) -> String {
    render_df_filter_safe(&compare_binary_bytes(filter, &columns())).expect("renders")
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
            compare_binary_bytes(&filter, &columns()),
            filter,
            "{literal}"
        );
    }
    let wrong_width = compare("predicate_equal", column("U"), text("0a"));
    assert_eq!(compare_binary_bytes(&wrong_width, &columns()), wrong_width);
    let string_column = compare("predicate_equal", column("S"), text("0a"));
    assert_eq!(
        compare_binary_bytes(&string_column, &columns()),
        string_column
    );
}

#[test]
fn only_a_comparison_reached_through_and_or_is_rewritten() {
    let equal = compare("predicate_equal", column("B"), text("0a"));
    let negated = json!({"type": "predicate_not", "expression": equal});
    assert_eq!(
        compare_binary_bytes(&negated, &columns()),
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

    let rewritten = compare_binary_bytes(&filter, &columns_under(true));

    assert_eq!(
        rewritten["expressions"][0], filter["expressions"][0],
        "a column read as text compares its text"
    );
    assert_ne!(
        rewritten["expressions"][1], filter["expressions"][1],
        "a fixed-length column keeps its hex rendering and its byte comparison"
    );
}

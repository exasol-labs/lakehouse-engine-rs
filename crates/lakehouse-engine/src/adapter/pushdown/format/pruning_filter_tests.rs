use super::*;
use crate::adapter::pushdown::test_support::{
    and, between, col, compare, in_list, is_not_null, is_null, like, not, num_lit, or,
    predicate_equal, str_lit,
};
use serde_json::json;

/// Renders each predicate as text, so a test reads the tree the parse lowered.
struct Rendered;

impl StatisticsTarget for Rendered {
    type Column = String;
    type Predicate = String;

    fn compare(
        &self,
        column: &String,
        comparison: Comparison,
        literal: &ScalarValue,
    ) -> Option<String> {
        let operator = match comparison {
            Comparison::Equal => "=",
            Comparison::NotEqual => "!=",
            Comparison::Less => "<",
            Comparison::LessEqual => "<=",
            Comparison::Greater => ">",
            Comparison::GreaterEqual => ">=",
        };
        Some(format!("{column} {operator} {literal}"))
    }

    fn in_list(&self, column: &String, literals: &[ScalarValue]) -> Option<String> {
        let literals: Vec<String> = literals.iter().map(ToString::to_string).collect();
        Some(format!("{column} IN ({})", literals.join(", ")))
    }

    fn null_test(&self, column: &String, is_null: bool) -> String {
        let not = if is_null { "" } else { "NOT " };
        format!("{column} IS {not}NULL")
    }

    fn starts_with(&self, column: &String, prefix: String) -> String {
        format!("{column} STARTS WITH {prefix}")
    }

    fn and(predicates: Vec<String>) -> String {
        format!("({})", predicates.join(" AND "))
    }

    fn or(predicates: Vec<String>) -> String {
        format!("({})", predicates.join(" OR "))
    }

    fn not(predicate: String) -> String {
        format!("NOT {predicate}")
    }
}

/// `node` lowered over an `int` column `id`, a `long` column `amount`, and a string column `name`.
fn translate(node: &Json) -> Option<String> {
    let resolve = |column: &str| {
        let data_type = match column.to_uppercase().as_str() {
            "ID" => DataType::Int32,
            "AMOUNT" => DataType::Int64,
            "NAME" => DataType::Utf8,
            _ => return None,
        };
        Some((column.to_lowercase(), Some(data_type)))
    };
    PruningFilter::parse(node, &resolve)
        .to_statistics(&Rendered)
        .map(|translated| translated.predicate)
}

fn id_equal(value: i64) -> Json {
    predicate_equal(col("ID"), num_lit(value))
}

/// Translates on no format, so every conjunct holding it is dropped.
fn opaque() -> Json {
    like(col("NAME"), "%a")
}

#[test]
fn every_leaf_lowers_to_the_predicate_it_implies() {
    let cases = [
        (id_equal(5), "id = 5"),
        (compare("predicate_less", num_lit(5), col("ID")), "id > 5"),
        (
            compare("predicate_notequal", col("ID"), num_lit(5)),
            "id != 5",
        ),
        (is_null(col("NAME")), "name IS NULL"),
        (is_not_null(col("NAME")), "name IS NOT NULL"),
        (
            in_list(col("NAME"), [str_lit("alice"), str_lit("bob")]),
            "name IN (alice, bob)",
        ),
        (
            between(col("AMOUNT"), num_lit(10), num_lit(100)),
            "(amount >= 10 AND amount <= 100)",
        ),
        (like(col("NAME"), "ab%"), "name STARTS WITH ab"),
        (like(col("NAME"), "ab_c"), "name STARTS WITH ab"),
        (like(col("NAME"), "ab"), "name = ab"),
    ];
    for (node, expected) in cases {
        assert_eq!(translate(&node).as_deref(), Some(expected), "{node}");
    }
}

#[test]
fn a_leaf_without_a_column_and_an_exact_literal_constrains_nothing() {
    let leaves = [
        predicate_equal(col("MISSING"), num_lit(1)),
        predicate_equal(col("ID"), str_lit("5")),
        predicate_equal(col("ID"), num_lit("2.5")),
        in_list(col("ID"), [num_lit(1), str_lit("two")]),
        in_list(col("ID"), []),
        between(col("ID"), str_lit("one"), str_lit("ten")),
        like(col("NAME"), "%ab"),
        like(col("ID"), "1%"),
        json!({"type": "predicate_like_regexp", "expression": col("NAME"), "pattern": str_lit("^B")}),
    ];
    for node in leaves {
        assert_eq!(translate(&node), None, "{node}");
    }
}

#[test]
fn and_drops_an_untranslated_conjunct_and_or_needs_every_disjunct() {
    let cases = [
        (and([id_equal(5), opaque()]), Some("(id = 5)")),
        (and([opaque(), opaque()]), None),
        (and([]), None),
        (
            between(col("AMOUNT"), str_lit("low"), num_lit(100)),
            Some("(amount <= 100)"),
        ),
        (or([id_equal(1), id_equal(2)]), Some("(id = 1 OR id = 2)")),
        (or([id_equal(1), opaque()]), None),
        (or([]), None),
    ];
    for (node, expected) in cases {
        assert_eq!(translate(&node).as_deref(), expected, "{node}");
    }
}

/// `NOT` over a widened predicate narrows it and prunes files holding matching rows (#466).
#[test]
fn not_negates_only_an_exact_translation() {
    let amount_below = |value: i64| compare("predicate_less", col("AMOUNT"), num_lit(value));
    let cases = [
        (not(id_equal(5)), Some("NOT id = 5")),
        (
            not(and([amount_below(30), id_equal(5)])),
            Some("NOT (amount < 30 AND id = 5)"),
        ),
        (
            not(between(col("AMOUNT"), num_lit(10), num_lit(20))),
            Some("NOT (amount >= 10 AND amount <= 20)"),
        ),
        (
            not(or([amount_below(10), id_equal(5)])),
            Some("NOT (amount < 10 OR id = 5)"),
        ),
        (
            not(like(col("NAME"), "ab%")),
            Some("NOT name STARTS WITH ab"),
        ),
        (not(opaque()), None),
        (not(and([amount_below(30), opaque()])), None),
        (not(and([opaque(), amount_below(30)])), None),
        (
            not(between(col("AMOUNT"), num_lit(10), num_lit(20.5))),
            None,
        ),
        (
            not(or([and([amount_below(30), opaque()]), id_equal(5)])),
            None,
        ),
        (not(like(col("NAME"), "ab_c")), None),
    ];
    for (node, expected) in cases {
        assert_eq!(translate(&node).as_deref(), expected, "{node}");
    }
}

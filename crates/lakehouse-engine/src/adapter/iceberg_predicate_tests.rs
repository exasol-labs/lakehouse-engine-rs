use super::*;
use crate::adapter::pushdown::test_support::nullable_field;
use iceberg::spec::{NestedField, Schema, Type};
use serde_json::json;
use std::sync::Arc;

fn test_schema() -> Schema {
    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                2,
                "amount",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                3,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                4,
                "event_date",
                Type::Primitive(PrimitiveType::Date),
            )),
            Arc::new(NestedField::optional(
                5,
                "score",
                Type::Primitive(PrimitiveType::Double),
            )),
        ])
        .build()
        .unwrap()
}

fn declared_field(field_id: i32, name: &str) -> LogicalField {
    LogicalField {
        field_id: Some(field_id),
        ..nullable_field(name, "utf8")
    }
}

/// Translates `filter` for a table whose declaration CREATE took from `schema` itself.
fn translate_as_created(filter: &Json, schema: &Schema) -> Option<Predicate> {
    let declared: Vec<LogicalField> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| declared_field(field.id, &field.name))
        .collect();
    to_iceberg_predicate(filter, &declared, schema)
}

fn translate(filter: &Json) -> Option<Predicate> {
    translate_as_created(filter, &test_schema())
}

fn col(name: &str) -> Json {
    json!({"type": "column", "name": name})
}
fn int_lit(v: i64) -> Json {
    json!({"type": "literal_exactnumeric", "value": v})
}
fn str_lit(s: &str) -> Json {
    json!({"type": "literal_string", "value": s})
}
fn date_lit(s: &str) -> Json {
    json!({"type": "literal_date", "value": s})
}

#[test]
fn leaf_equal_translates() {
    let node = json!({"type": "predicate_equal", "left": col("ID"), "right": int_lit(5)});
    let pred = translate(&node).expect("should translate equal");
    let s = format!("{pred}");
    assert!(
        s.contains("id") && s.contains('=') && s.contains('5'),
        "got: {s}"
    );

    let node2 = json!({"type": "predicate_equal", "left": int_lit(5), "right": col("ID")});
    let pred2 = translate(&node2).expect("reversed operands should translate");
    let s2 = format!("{pred2}");
    assert!(
        s2.contains("id") && s2.contains('=') && s2.contains('5'),
        "got: {s2}"
    );
}

#[test]
fn has_tz_offset_detects_explicit_zones() {
    assert!(has_tz_offset("2024-01-15T10:00:00Z"));
    assert!(has_tz_offset("2024-01-15T10:00:00+02:00"));
    assert!(has_tz_offset("2024-01-15T10:00:00-05:00"));
    assert!(!has_tz_offset("2024-01-15T10:00:00"));
    assert!(!has_tz_offset("2024-01-15 10:00:00"));
}

#[test]
fn leaf_less_than_translates() {
    let node = json!({"type": "predicate_less", "left": col("AMOUNT"), "right": int_lit(100)});
    let pred = translate(&node).unwrap();
    let s = format!("{pred}");
    assert!(s.contains("amount") && s.contains('<'), "got: {s}");

    let node2 = json!({"type": "predicate_less", "left": int_lit(100), "right": col("AMOUNT")});
    let pred2 = translate(&node2).unwrap();
    let s2 = format!("{pred2}");
    assert!(s2.contains("amount") && s2.contains('>'), "got: {s2}");
}

#[test]
fn leaf_lessequal_translates() {
    let node = json!({"type": "predicate_lessequal", "left": col("AMOUNT"), "right": int_lit(50)});
    let pred = translate(&node).unwrap();
    let s = format!("{pred}");
    assert!(s.contains("amount") && s.contains("<="), "got: {s}");
}

#[test]
fn leaf_is_null_translates() {
    let node = json!({"type": "predicate_is_null", "expression": col("NAME")});
    let pred = translate(&node).unwrap();
    let s = format!("{pred}");
    assert!(
        s.contains("name") && s.to_uppercase().contains("NULL"),
        "got: {s}"
    );
}

#[test]
fn leaf_is_not_null_translates() {
    let node = json!({"type": "predicate_is_not_null", "expression": col("NAME")});
    let pred = translate(&node).unwrap();
    let s = format!("{pred}");
    assert!(
        s.contains("name") && s.to_uppercase().contains("NOT NULL"),
        "got: {s}"
    );
}

#[test]
fn leaf_in_translates() {
    let node = json!({
        "type": "predicate_in_constlist",
        "expression": col("ID"),
        "arguments": [int_lit(1), int_lit(2), int_lit(3)]
    });
    let pred = translate(&node).unwrap();
    let s = format!("{pred}");
    assert!(
        s.contains("id") && s.to_uppercase().contains("IN"),
        "got: {s}"
    );
}

#[test]
fn leaf_in_with_string_translates() {
    let node = json!({
        "type": "predicate_in_constlist",
        "expression": col("NAME"),
        "arguments": [str_lit("alice"), str_lit("bob")]
    });
    let pred = translate(&node).unwrap();
    let s = format!("{pred}");
    assert!(
        s.contains("name") && s.to_uppercase().contains("IN"),
        "got: {s}"
    );
}

#[test]
fn between_desugars_to_range() {
    let node = json!({
        "type": "predicate_between",
        "expression": col("AMOUNT"),
        "left": int_lit(10),
        "right": int_lit(100)
    });
    let pred = translate(&node).unwrap();
    let s = format!("{pred}");
    assert!(s.contains("amount"), "got: {s}");
    assert!(
        s.contains(">=") || s.contains('>'),
        "low bound missing: {s}"
    );
    assert!(
        s.contains("<=") || s.contains('<'),
        "high bound missing: {s}"
    );
    assert!(s.to_uppercase().contains("AND"), "AND missing: {s}");
}

#[test]
fn leaf_date_translates() {
    let node = json!({
        "type": "predicate_equal",
        "left": col("EVENT_DATE"),
        "right": date_lit("2024-01-15")
    });
    let pred = translate(&node).unwrap();
    let s = format!("{pred}");
    assert!(s.contains("event_date"), "got: {s}");
}

#[test]
fn timestamp_literal_finer_than_microseconds_yields_no_datum() {
    for prim in [PrimitiveType::Timestamp, PrimitiveType::TimestampNs] {
        let at = |value| json!({"type": "literal_timestamp", "value": value});
        assert_eq!(
            literal_to_datum(&at("2024-01-15 10:30:00.123456789"), &prim),
            None
        );
        assert!(literal_to_datum(&at("2024-01-15 10:30:00.123456000"), &prim).is_some());
    }
}

#[test]
fn and_with_untranslatable_child_keeps_translatable_conjunct() {
    let translatable = json!({
        "type": "predicate_equal",
        "left": col("ID"),
        "right": int_lit(5)
    });
    let untranslatable = json!({
        "type": "predicate_like",
        "expression": col("NAME"),
        "pattern": {"type": "literal_string", "value": "A%"}
    });
    let node = json!({
        "type": "predicate_and",
        "expressions": [translatable, untranslatable]
    });
    let pred = translate(&node).expect("AND with one translatable child must return Some");
    let s = format!("{pred}");
    assert!(
        s.contains("id") && s.contains('=') && s.contains('5'),
        "got: {s}"
    );
    assert!(
        !s.to_uppercase().contains("LIKE"),
        "LIKE should be dropped: {s}"
    );
}

#[test]
fn and_all_untranslatable_returns_none() {
    let node = json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_like", "expression": col("NAME"), "pattern": str_lit("A%")},
            {"type": "predicate_like_regexp", "expression": col("NAME"), "pattern": str_lit("^B")}
        ]
    });
    assert!(translate(&node).is_none());
}

#[test]
fn or_with_untranslatable_child_returns_none() {
    let translatable = json!({
        "type": "predicate_equal",
        "left": col("ID"),
        "right": int_lit(5)
    });
    let untranslatable = json!({
        "type": "predicate_like",
        "expression": col("NAME"),
        "pattern": str_lit("A%")
    });
    let node = json!({
        "type": "predicate_or",
        "expressions": [translatable, untranslatable]
    });
    // Pruning on only the translatable branch would be unsound.
    assert!(
        translate(&node).is_none(),
        "OR with untranslatable branch must be None"
    );
}

#[test]
fn or_all_translatable_returns_some() {
    let node = json!({
        "type": "predicate_or",
        "expressions": [
            {"type": "predicate_equal", "left": col("ID"), "right": int_lit(1)},
            {"type": "predicate_equal", "left": col("ID"), "right": int_lit(2)}
        ]
    });
    let pred = translate(&node).expect("all-translatable OR must be Some");
    let s = format!("{pred}");
    assert!(s.to_uppercase().contains("OR"), "OR missing: {s}");
}

#[test]
fn not_of_untranslatable_returns_none() {
    let node = json!({
        "type": "predicate_not",
        "expression": {"type": "predicate_like", "expression": col("NAME"), "pattern": str_lit("A%")}
    });
    assert!(
        translate(&node).is_none(),
        "NOT of untranslatable must be None"
    );
}

#[test]
fn not_of_translatable_negates() {
    let node = json!({
        "type": "predicate_not",
        "expression": {"type": "predicate_less", "left": col("ID"), "right": int_lit(5)}
    });
    let pred = translate(&node).expect("NOT of translatable must be Some");
    let s = format!("{pred}");
    assert!(s.contains("id") && s.contains(">="), "got: {s}");
}

#[test]
fn unknown_column_returns_none() {
    let node = json!({
        "type": "predicate_equal",
        "left": col("NONEXISTENT"),
        "right": int_lit(1)
    });
    assert!(translate(&node).is_none(), "unknown column must be None");
}

#[test]
fn type_mismatch_returns_none() {
    let node = json!({
        "type": "predicate_equal",
        "left": col("ID"),
        "right": str_lit("not-a-number")
    });
    assert!(translate(&node).is_none(), "type mismatch must be None");
}

#[test]
fn notequal_returns_none() {
    let node = json!({
        "type": "predicate_notequal",
        "left": col("ID"),
        "right": int_lit(5)
    });
    assert!(
        translate(&node).is_none(),
        "predicate_notequal must not produce a pruning predicate"
    );
}

#[test]
fn in_with_type_mismatch_element_returns_none() {
    let node = json!({
        "type": "predicate_in_constlist",
        "expression": col("ID"),
        "arguments": [int_lit(1), str_lit("bad")]
    });
    assert!(
        translate(&node).is_none(),
        "IN with one bad element must be None"
    );
}

#[test]
fn between_with_one_failing_bound_keeps_other() {
    // The low bound fails; the high bound survives alone.
    let node = json!({
        "type": "predicate_between",
        "expression": col("AMOUNT"),
        "left": str_lit("bad"),
        "right": int_lit(100)
    });
    let pred = translate(&node).expect("BETWEEN with one valid bound must return Some");
    let s = format!("{pred}");
    assert!(s.contains("amount") && s.contains("<="), "got: {s}");
}

fn amount_below(v: i64) -> Json {
    json!({"type": "predicate_less", "left": col("AMOUNT"), "right": int_lit(v)})
}

fn name_like(pattern: &str) -> Json {
    json!({"type": "predicate_like", "expression": col("NAME"), "pattern": str_lit(pattern)})
}

fn not(expression: Json) -> Json {
    json!({"type": "predicate_not", "expression": expression})
}

fn and(expressions: [Json; 2]) -> Json {
    json!({"type": "predicate_and", "expressions": expressions})
}

#[test]
fn not_over_an_and_that_dropped_a_conjunct_returns_none() {
    for conjuncts in [
        [amount_below(30), name_like("x%")],
        [name_like("x%"), amount_below(30)],
    ] {
        let node = not(and(conjuncts));
        assert_eq!(translate(&node), None, "{node}");
    }
}

#[test]
fn not_over_a_between_that_dropped_a_bound_returns_none() {
    let node = not(json!({
        "type": "predicate_between",
        "expression": col("AMOUNT"),
        "left": int_lit(10),
        "right": {"type": "literal_exactnumeric", "value": 20.5}
    }));
    assert_eq!(translate(&node), None);
}

#[test]
fn not_over_an_or_with_an_inexact_branch_returns_none() {
    let node = not(json!({
        "type": "predicate_or",
        "expressions": [
            and([amount_below(30), name_like("x%")]),
            {"type": "predicate_equal", "left": col("ID"), "right": int_lit(5)}
        ]
    }));
    assert_eq!(translate(&node), None);
}

#[test]
fn not_over_a_fully_translated_and_negates_the_whole_conjunction() {
    let node = not(and([
        amount_below(30),
        json!({"type": "predicate_equal", "left": col("NAME"), "right": str_lit("a")}),
    ]));
    let expected = Reference::new("amount")
        .greater_than_or_equal_to(Datum::long(30))
        .or(Reference::new("name").not_equal_to(Datum::string("a")));
    assert_eq!(translate(&node), Some(expected));
}

#[test]
fn not_over_a_fully_translated_between_negates_both_bounds() {
    let node = not(json!({
        "type": "predicate_between",
        "expression": col("AMOUNT"),
        "left": int_lit(10),
        "right": int_lit(20)
    }));
    let expected = Reference::new("amount")
        .less_than(Datum::long(10))
        .or(Reference::new("amount").greater_than(Datum::long(20)));
    assert_eq!(translate(&node), Some(expected));
}

#[test]
fn not_over_a_fully_translated_or_negates_every_branch() {
    let node = not(json!({
        "type": "predicate_or",
        "expressions": [
            amount_below(10),
            {"type": "predicate_greater", "left": col("AMOUNT"), "right": int_lit(20)}
        ]
    }));
    let expected = Reference::new("amount")
        .greater_than_or_equal_to(Datum::long(10))
        .and(Reference::new("amount").less_than_or_equal_to(Datum::long(20)));
    assert_eq!(translate(&node), Some(expected));
}

fn int_schema(fields: &[(i32, &str)]) -> Schema {
    Schema::builder()
        .with_fields(
            fields
                .iter()
                .map(|&(id, name)| {
                    NestedField::optional(id, name, Type::Primitive(PrimitiveType::Int)).into()
                })
                .collect::<Vec<_>>(),
        )
        .build()
        .unwrap()
}

fn equal_to(column: &str, value: i64) -> Json {
    json!({"type": "predicate_equal", "left": col(column), "right": int_lit(value)})
}

#[test]
fn a_filter_column_whose_declared_id_was_dropped_imposes_no_constraint() {
    let declared = [declared_field(1, "id"), declared_field(2, "note")];
    let note_dropped = int_schema(&[(1, "id"), (3, "note")]);
    let on_both = and([equal_to("ID", 5), equal_to("NOTE", 1)]);

    assert_eq!(
        to_iceberg_predicate(&equal_to("NOTE", 1), &declared, &note_dropped),
        None,
        "field id 2 is gone; the later field `note` (id 3) is another column"
    );
    assert_eq!(
        to_iceberg_predicate(&on_both, &declared, &note_dropped),
        Some(Reference::new("id").equal_to(Datum::int(5))),
        "AND drops only the conjunct on the dropped id"
    );
    assert_eq!(
        to_iceberg_predicate(&not(on_both), &declared, &note_dropped),
        None,
        "NOT never negates the AND that dropped a conjunct"
    );
}

#[test]
fn a_filter_column_the_declaration_does_not_carry_imposes_no_constraint() {
    let declared = [declared_field(1, "id")];
    let added_since = int_schema(&[(1, "id"), (2, "extra")]);

    assert_eq!(
        to_iceberg_predicate(&equal_to("EXTRA", 1), &declared, &added_since),
        None
    );
}

use super::*;
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
    let schema = test_schema();

    let node = json!({"type": "predicate_equal", "left": col("ID"), "right": int_lit(5)});
    let pred = to_iceberg_predicate(&node, &schema).expect("should translate equal");
    let s = format!("{pred}");
    assert!(
        s.contains("id") && s.contains('=') && s.contains('5'),
        "got: {s}"
    );

    let node2 = json!({"type": "predicate_equal", "left": int_lit(5), "right": col("ID")});
    let pred2 = to_iceberg_predicate(&node2, &schema).expect("reversed operands should translate");
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
    let schema = test_schema();

    let node = json!({"type": "predicate_less", "left": col("AMOUNT"), "right": int_lit(100)});
    let pred = to_iceberg_predicate(&node, &schema).unwrap();
    let s = format!("{pred}");
    assert!(s.contains("amount") && s.contains('<'), "got: {s}");

    let node2 = json!({"type": "predicate_less", "left": int_lit(100), "right": col("AMOUNT")});
    let pred2 = to_iceberg_predicate(&node2, &schema).unwrap();
    let s2 = format!("{pred2}");
    assert!(s2.contains("amount") && s2.contains('>'), "got: {s2}");
}

#[test]
fn leaf_lessequal_translates() {
    let schema = test_schema();

    let node = json!({"type": "predicate_lessequal", "left": col("AMOUNT"), "right": int_lit(50)});
    let pred = to_iceberg_predicate(&node, &schema).unwrap();
    let s = format!("{pred}");
    assert!(s.contains("amount") && s.contains("<="), "got: {s}");
}

#[test]
fn leaf_is_null_translates() {
    let schema = test_schema();
    let node = json!({"type": "predicate_is_null", "expression": col("NAME")});
    let pred = to_iceberg_predicate(&node, &schema).unwrap();
    let s = format!("{pred}");
    assert!(
        s.contains("name") && s.to_uppercase().contains("NULL"),
        "got: {s}"
    );
}

#[test]
fn leaf_is_not_null_translates() {
    let schema = test_schema();
    let node = json!({"type": "predicate_is_not_null", "expression": col("NAME")});
    let pred = to_iceberg_predicate(&node, &schema).unwrap();
    let s = format!("{pred}");
    assert!(
        s.contains("name") && s.to_uppercase().contains("NOT NULL"),
        "got: {s}"
    );
}

#[test]
fn leaf_in_translates() {
    let schema = test_schema();
    let node = json!({
        "type": "predicate_in_constlist",
        "expression": col("ID"),
        "arguments": [int_lit(1), int_lit(2), int_lit(3)]
    });
    let pred = to_iceberg_predicate(&node, &schema).unwrap();
    let s = format!("{pred}");
    assert!(
        s.contains("id") && s.to_uppercase().contains("IN"),
        "got: {s}"
    );
}

#[test]
fn leaf_in_with_string_translates() {
    let schema = test_schema();
    let node = json!({
        "type": "predicate_in_constlist",
        "expression": col("NAME"),
        "arguments": [str_lit("alice"), str_lit("bob")]
    });
    let pred = to_iceberg_predicate(&node, &schema).unwrap();
    let s = format!("{pred}");
    assert!(
        s.contains("name") && s.to_uppercase().contains("IN"),
        "got: {s}"
    );
}

#[test]
fn between_desugars_to_range() {
    let schema = test_schema();
    let node = json!({
        "type": "predicate_between",
        "expression": col("AMOUNT"),
        "left": int_lit(10),
        "right": int_lit(100)
    });
    let pred = to_iceberg_predicate(&node, &schema).unwrap();
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
    let schema = test_schema();
    let node = json!({
        "type": "predicate_equal",
        "left": col("EVENT_DATE"),
        "right": date_lit("2024-01-15")
    });
    let pred = to_iceberg_predicate(&node, &schema).unwrap();
    let s = format!("{pred}");
    assert!(s.contains("event_date"), "got: {s}");
}

#[test]
fn and_with_untranslatable_child_keeps_translatable_conjunct() {
    let schema = test_schema();
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
    let pred = to_iceberg_predicate(&node, &schema)
        .expect("AND with one translatable child must return Some");
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
    let schema = test_schema();
    let node = json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_like", "expression": col("NAME"), "pattern": str_lit("A%")},
            {"type": "predicate_like_regexp", "expression": col("NAME"), "pattern": str_lit("^B")}
        ]
    });
    assert!(to_iceberg_predicate(&node, &schema).is_none());
}

#[test]
fn or_with_untranslatable_child_returns_none() {
    let schema = test_schema();
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
        to_iceberg_predicate(&node, &schema).is_none(),
        "OR with untranslatable branch must be None"
    );
}

#[test]
fn or_all_translatable_returns_some() {
    let schema = test_schema();
    let node = json!({
        "type": "predicate_or",
        "expressions": [
            {"type": "predicate_equal", "left": col("ID"), "right": int_lit(1)},
            {"type": "predicate_equal", "left": col("ID"), "right": int_lit(2)}
        ]
    });
    let pred = to_iceberg_predicate(&node, &schema).expect("all-translatable OR must be Some");
    let s = format!("{pred}");
    assert!(s.to_uppercase().contains("OR"), "OR missing: {s}");
}

#[test]
fn not_of_untranslatable_returns_none() {
    let schema = test_schema();
    let node = json!({
        "type": "predicate_not",
        "expression": {"type": "predicate_like", "expression": col("NAME"), "pattern": str_lit("A%")}
    });
    assert!(
        to_iceberg_predicate(&node, &schema).is_none(),
        "NOT of untranslatable must be None"
    );
}

#[test]
fn not_of_translatable_negates() {
    let schema = test_schema();
    let node = json!({
        "type": "predicate_not",
        "expression": {"type": "predicate_less", "left": col("ID"), "right": int_lit(5)}
    });
    let pred = to_iceberg_predicate(&node, &schema).expect("NOT of translatable must be Some");
    let s = format!("{pred}");
    assert!(s.contains("id") && s.contains(">="), "got: {s}");
}

#[test]
fn unknown_column_returns_none() {
    let schema = test_schema();
    let node = json!({
        "type": "predicate_equal",
        "left": col("NONEXISTENT"),
        "right": int_lit(1)
    });
    assert!(
        to_iceberg_predicate(&node, &schema).is_none(),
        "unknown column must be None"
    );
}

#[test]
fn type_mismatch_returns_none() {
    let schema = test_schema();
    let node = json!({
        "type": "predicate_equal",
        "left": col("ID"),
        "right": str_lit("not-a-number")
    });
    assert!(
        to_iceberg_predicate(&node, &schema).is_none(),
        "type mismatch must be None"
    );
}

#[test]
fn notequal_returns_none() {
    let schema = test_schema();
    let node = json!({
        "type": "predicate_notequal",
        "left": col("ID"),
        "right": int_lit(5)
    });
    assert!(
        to_iceberg_predicate(&node, &schema).is_none(),
        "predicate_notequal must not produce a pruning predicate"
    );
}

#[test]
fn in_with_type_mismatch_element_returns_none() {
    let schema = test_schema();
    let node = json!({
        "type": "predicate_in_constlist",
        "expression": col("ID"),
        "arguments": [int_lit(1), str_lit("bad")]
    });
    assert!(
        to_iceberg_predicate(&node, &schema).is_none(),
        "IN with one bad element must be None"
    );
}

#[test]
fn between_with_one_failing_bound_keeps_other() {
    let schema = test_schema();
    // The low bound fails; the high bound survives alone.
    let node = json!({
        "type": "predicate_between",
        "expression": col("AMOUNT"),
        "left": str_lit("bad"),
        "right": int_lit(100)
    });
    let pred = to_iceberg_predicate(&node, &schema)
        .expect("BETWEEN with one valid bound must return Some");
    let s = format!("{pred}");
    assert!(s.contains("amount") && s.contains("<="), "got: {s}");
}

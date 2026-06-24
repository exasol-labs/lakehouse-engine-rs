/// Translates Exasol pushdown filter JSON into a sound Iceberg pruning predicate.
///
/// The predicate is pruning-only: every conjunct it emits is logically implied by
/// the user predicate. DataFusion remains the sole row-level correctness backstop.
/// A node that cannot be translated soundly is dropped — this can only widen the
/// surviving file set, never narrow it past correctness.
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, NestedFieldRef, PrimitiveType, Schema};
use serde_json::Value as Json;

// ---------------------------------------------------------------------------
// Column resolution (task 1.2)
// ---------------------------------------------------------------------------

/// Resolve an Exasol (uppercase) column name to its Iceberg field via
/// case-insensitive lookup, returning the exact Iceberg field name and its
/// primitive type.
///
/// Returns `None` when the field is absent or not a primitive type.
fn resolve_column<'s>(col_name: &str, schema: &'s Schema) -> Option<(&'s str, &'s PrimitiveType)> {
    let field: &NestedFieldRef = schema.field_by_name_case_insensitive(col_name)?;
    let prim = field.field_type.as_primitive_type()?;
    Some((&field.name, prim))
}

// ---------------------------------------------------------------------------
// Literal → Datum (task 1.3)
// ---------------------------------------------------------------------------

/// Build a typed `Datum` from a filter-JSON literal node, keyed on the
/// resolved Iceberg `PrimitiveType`.
///
/// Returns `None` on any type mismatch or unparsable value. Never panics.
fn literal_to_datum(lit: &Json, prim: &PrimitiveType) -> Option<Datum> {
    let kind = lit.get("type")?.as_str()?;

    match (kind, prim) {
        // Boolean
        ("literal_bool", PrimitiveType::Boolean) => {
            let v = lit.get("value")?;
            let b = match v {
                Json::Bool(b) => *b,
                Json::String(s) => s == "true" || s == "TRUE",
                Json::Number(n) => n.as_i64() == Some(1),
                _ => return None,
            };
            Some(Datum::bool(b))
        }

        // Int (32-bit)
        ("literal_exactnumeric" | "literal_double", PrimitiveType::Int) => {
            let v = lit.get("value")?;
            parse_i32(v).map(Datum::int)
        }

        // Long (64-bit)
        ("literal_exactnumeric" | "literal_double", PrimitiveType::Long) => {
            let v = lit.get("value")?;
            parse_i64(v).map(Datum::long)
        }

        // Float
        ("literal_exactnumeric" | "literal_double", PrimitiveType::Float) => {
            let v = lit.get("value")?;
            parse_f64(v).map(|f| Datum::float(f as f32))
        }

        // Double
        ("literal_exactnumeric" | "literal_double", PrimitiveType::Double) => {
            let v = lit.get("value")?;
            parse_f64(v).map(Datum::double)
        }

        // String
        ("literal_string", PrimitiveType::String) => {
            let s = lit.get("value")?.as_str()?;
            Some(Datum::string(s))
        }

        // Date — Iceberg stores days since epoch; use Datum::date_from_str
        ("literal_date", PrimitiveType::Date) => {
            let s = lit.get("value")?.as_str()?;
            Datum::date_from_str(s).ok()
        }

        // Timestamp (no timezone) — parse "YYYY-MM-DD HH:MM:SS[.f]" or ISO-8601
        ("literal_timestamp", PrimitiveType::Timestamp | PrimitiveType::TimestampNs) => {
            let s = lit.get("value")?.as_str()?;
            parse_timestamp_no_tz(s, prim)
        }

        // Timestamp with timezone — parse RFC3339 / "YYYY-MM-DD HH:MM:SS+HH:MM"
        ("literal_timestamp_utc", PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs) => {
            let s = lit.get("value")?.as_str()?;
            parse_timestamptz(s, prim)
        }

        _ => None,
    }
}

fn parse_i32(v: &Json) -> Option<i32> {
    match v {
        Json::Number(n) => n.as_i64().and_then(|i| i32::try_from(i).ok()),
        Json::String(s) => s.trim().parse::<i32>().ok(),
        _ => None,
    }
}

fn parse_i64(v: &Json) -> Option<i64> {
    match v {
        Json::Number(n) => n.as_i64(),
        Json::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn parse_f64(v: &Json) -> Option<f64> {
    match v {
        Json::Number(n) => n.as_f64(),
        Json::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_timestamp_no_tz(s: &str, prim: &PrimitiveType) -> Option<Datum> {
    // Try T-separated ISO-8601, then space-separated.
    let s_t = if s.contains('T') {
        s.to_owned()
    } else {
        s.replacen(' ', "T", 1)
    };
    match prim {
        PrimitiveType::Timestamp => Datum::timestamp_from_str(&s_t).ok(),
        PrimitiveType::TimestampNs => {
            let us = Datum::timestamp_from_str(&s_t).ok()?;
            let micros = long_from_datum(us)?;
            Some(Datum::timestamp_nanos(micros.checked_mul(1_000)?))
        }
        _ => None,
    }
}

/// True when the timestamp string already carries an explicit timezone: a
/// trailing `Z`, or a `+HH:MM` / `-HH:MM` offset in its final six characters
/// (so a `-` inside the date portion is not mistaken for an offset).
fn has_tz_offset(s: &str) -> bool {
    if s.ends_with('Z') {
        return true;
    }
    let tail = &s[s.len().saturating_sub(6)..];
    (tail.starts_with('+') || tail.starts_with('-')) && tail.contains(':')
}

fn parse_timestamptz(s: &str, prim: &PrimitiveType) -> Option<Datum> {
    // Append UTC offset if missing so DateTime::from_str can parse it.
    let s_tz = if has_tz_offset(s) {
        s.to_owned()
    } else {
        format!("{s}+00:00")
    };
    match prim {
        PrimitiveType::Timestamptz => Datum::timestamptz_from_str(&s_tz).ok(),
        PrimitiveType::TimestamptzNs => {
            let us = Datum::timestamptz_from_str(&s_tz).ok()?;
            let micros = long_from_datum(us)?;
            Some(Datum::timestamptz_nanos(micros.checked_mul(1_000)?))
        }
        _ => None,
    }
}

/// Extract the underlying microsecond `Long` from a timestamp/timestamptz micros datum.
fn long_from_datum(d: Datum) -> Option<i64> {
    use iceberg::spec::{Literal, PrimitiveLiteral};
    match Literal::from(d) {
        Literal::Primitive(PrimitiveLiteral::Long(v)) => Some(v),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Column extraction from a comparison node operand
// ---------------------------------------------------------------------------

fn extract_column(node: &Json) -> Option<&str> {
    if node.get("type")?.as_str()? == "column" {
        node.get("name")?.as_str()
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Core translator (task 1.4)
// ---------------------------------------------------------------------------

/// Translate a Exasol pushdown filter JSON node into an Iceberg pruning
/// predicate against the given schema.
///
/// Returns `None` when the node (or any required part of it) cannot be
/// translated soundly.  `None` is "no constraint" — the caller must treat it
/// as "pass all files", never as "pass no files".
pub fn to_iceberg_predicate(filter_json: &Json, schema: &Schema) -> Option<Predicate> {
    let kind = filter_json.get("type")?.as_str()?;

    match kind {
        // --- Binary comparisons ---
        "predicate_equal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal" => translate_binary(filter_json, kind, schema),

        // predicate_notequal: not soundly prunable to a single range.
        "predicate_notequal" => None,

        // --- Logical connectives ---
        "predicate_and" => {
            let exprs = filter_json.get("expressions")?.as_array()?;
            fold_and(exprs, schema)
        }
        "predicate_or" => {
            let exprs = filter_json.get("expressions")?.as_array()?;
            fold_or(exprs, schema)
        }
        "predicate_not" => {
            let inner = filter_json.get("expression")?;
            let child = to_iceberg_predicate(inner, schema)?;
            Some(child.negate())
        }

        // --- Unary predicates ---
        "predicate_is_null" => {
            let col_node = filter_json.get("expression")?;
            let col_name = extract_column(col_node)?;
            let (exact_name, _prim) = resolve_column(col_name, schema)?;
            Some(Reference::new(exact_name).is_null())
        }
        "predicate_is_not_null" => {
            let col_node = filter_json.get("expression")?;
            let col_name = extract_column(col_node)?;
            let (exact_name, _prim) = resolve_column(col_name, schema)?;
            Some(Reference::new(exact_name).is_not_null())
        }

        // --- IN ---
        "predicate_in_constlist" => translate_in(filter_json, schema),

        // --- BETWEEN → col >= low AND col <= high ---
        "predicate_between" => translate_between(filter_json, schema),

        // Everything else is untranslatable.
        _ => None,
    }
}

fn translate_binary(node: &Json, kind: &str, schema: &Schema) -> Option<Predicate> {
    let left = node.get("left")?;
    let right = node.get("right")?;

    // Determine which side is the column and which is the literal.
    let (col_name, lit_node, col_is_left) = if let Some(name) = extract_column(left) {
        (name, right, true)
    } else if let Some(name) = extract_column(right) {
        (name, left, false)
    } else {
        return None;
    };

    let (exact_name, prim) = resolve_column(col_name, schema)?;
    let datum = literal_to_datum(lit_node, prim)?;
    let reference = Reference::new(exact_name);

    // When the column is on the RIGHT, invert the operator.
    // e.g. `literal < col` ≡ `col > literal`
    let effective_kind = if col_is_left {
        kind
    } else {
        flip_operator(kind)?
    };

    let pred = match effective_kind {
        "predicate_equal" => reference.equal_to(datum),
        "predicate_less" => reference.less_than(datum),
        "predicate_lessequal" => reference.less_than_or_equal_to(datum),
        "predicate_greater" => reference.greater_than(datum),
        "predicate_greaterequal" => reference.greater_than_or_equal_to(datum),
        _ => return None,
    };
    Some(pred)
}

/// Flip a binary comparison operator (when the column is on the right).
fn flip_operator(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "predicate_less" => "predicate_greater",
        "predicate_lessequal" => "predicate_greaterequal",
        "predicate_greater" => "predicate_less",
        "predicate_greaterequal" => "predicate_lessequal",
        "predicate_equal" => "predicate_equal",
        _ => return None,
    })
}

/// AND semantics: combine Some children; drop None children.
/// If all children are None, return None.
fn fold_and(exprs: &[Json], schema: &Schema) -> Option<Predicate> {
    let mut acc: Option<Predicate> = None;
    for expr in exprs {
        if let Some(child) = to_iceberg_predicate(expr, schema) {
            acc = Some(match acc {
                None => child,
                Some(prev) => prev.and(child),
            });
        }
    }
    acc
}

/// OR semantics: if ANY child is None, return None.
/// Only when ALL children translate, fold with `or`.
fn fold_or(exprs: &[Json], schema: &Schema) -> Option<Predicate> {
    if exprs.is_empty() {
        return None;
    }
    let mut acc: Option<Predicate> = None;
    for expr in exprs {
        let child = to_iceberg_predicate(expr, schema)?;
        acc = Some(match acc {
            None => child,
            Some(prev) => prev.or(child),
        });
    }
    acc
}

/// IN: translate only when ALL list elements build a Datum.
/// A single untranslatable element means a matching row could exist outside
/// the translated set — unsound to prune.
fn translate_in(node: &Json, schema: &Schema) -> Option<Predicate> {
    let col_node = node.get("expression")?;
    let col_name = extract_column(col_node)?;
    let (exact_name, prim) = resolve_column(col_name, schema)?;

    let args = node.get("arguments")?.as_array()?;
    if args.is_empty() {
        return None;
    }
    let datums: Option<Vec<Datum>> = args.iter().map(|a| literal_to_datum(a, prim)).collect();
    let datums = datums?;
    Some(Reference::new(exact_name).is_in(datums))
}

/// BETWEEN: desugar to `col >= low AND col <= high`.
/// Either bound alone is still implied by BETWEEN, so a failing bound is
/// dropped under the implicit AND (sound: drops one conjunct, widens set).
fn translate_between(node: &Json, schema: &Schema) -> Option<Predicate> {
    let col_node = node.get("expression")?;
    let col_name = extract_column(col_node)?;
    let (exact_name, prim) = resolve_column(col_name, schema)?;

    let low_node = node.get("left");
    let high_node = node.get("right");

    let low_pred = low_node
        .and_then(|n| literal_to_datum(n, prim))
        .map(|d| Reference::new(exact_name).greater_than_or_equal_to(d));
    let high_pred = high_node
        .and_then(|n| literal_to_datum(n, prim))
        .map(|d| Reference::new(exact_name).less_than_or_equal_to(d));

    match (low_pred, high_pred) {
        (Some(lo), Some(hi)) => Some(lo.and(hi)),
        (Some(lo), None) => Some(lo),
        (None, Some(hi)) => Some(hi),
        (None, None) => None,
    }
}

// ---------------------------------------------------------------------------
// Tests (tasks 3.1–3.5)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, Schema, Type};
    use serde_json::json;
    use std::sync::Arc;

    fn test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                // id: Int (field_id=1)
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                // amount: Long (field_id=2)
                Arc::new(NestedField::optional(
                    2,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
                // name: String (field_id=3)
                Arc::new(NestedField::optional(
                    3,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
                // event_date: Date (field_id=4)
                Arc::new(NestedField::optional(
                    4,
                    "event_date",
                    Type::Primitive(PrimitiveType::Date),
                )),
                // score: Double (field_id=5)
                Arc::new(NestedField::optional(
                    5,
                    "score",
                    Type::Primitive(PrimitiveType::Double),
                )),
            ])
            .build()
            .unwrap()
    }

    // --- helpers to build column / literal nodes ---

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

    // --- 3.1: Leaf translations ---

    #[test]
    fn leaf_equal_translates() {
        let schema = test_schema();

        // col = lit (column on left)
        let node = json!({"type": "predicate_equal", "left": col("ID"), "right": int_lit(5)});
        let pred = to_iceberg_predicate(&node, &schema).expect("should translate equal");
        let s = format!("{pred}");
        assert!(
            s.contains("id") && s.contains('=') && s.contains('5'),
            "got: {s}"
        );

        // lit = col (column on right — should produce same effective predicate)
        let node2 = json!({"type": "predicate_equal", "left": int_lit(5), "right": col("ID")});
        let pred2 =
            to_iceberg_predicate(&node2, &schema).expect("reversed operands should translate");
        let s2 = format!("{pred2}");
        assert!(
            s2.contains("id") && s2.contains('=') && s2.contains('5'),
            "got: {s2}"
        );
    }

    #[test]
    fn has_tz_offset_detects_explicit_zones() {
        // Trailing Z and positive/negative offsets are explicit zones.
        assert!(has_tz_offset("2024-01-15T10:00:00Z"));
        assert!(has_tz_offset("2024-01-15T10:00:00+02:00"));
        assert!(has_tz_offset("2024-01-15T10:00:00-05:00"));
        // A bare timestamp (hyphens only in the date) carries no zone.
        assert!(!has_tz_offset("2024-01-15T10:00:00"));
        assert!(!has_tz_offset("2024-01-15 10:00:00"));
    }

    #[test]
    fn leaf_less_than_translates() {
        let schema = test_schema();

        // col < lit
        let node = json!({"type": "predicate_less", "left": col("AMOUNT"), "right": int_lit(100)});
        let pred = to_iceberg_predicate(&node, &schema).unwrap();
        let s = format!("{pred}");
        assert!(s.contains("amount") && s.contains('<'), "got: {s}");

        // lit < col → col > lit
        let node2 = json!({"type": "predicate_less", "left": int_lit(100), "right": col("AMOUNT")});
        let pred2 = to_iceberg_predicate(&node2, &schema).unwrap();
        let s2 = format!("{pred2}");
        assert!(s2.contains("amount") && s2.contains('>'), "got: {s2}");
    }

    #[test]
    fn leaf_lessequal_translates() {
        let schema = test_schema();

        let node =
            json!({"type": "predicate_lessequal", "left": col("AMOUNT"), "right": int_lit(50)});
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
        // BETWEEN desugars to (amount >= 10) AND (amount <= 100)
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

    // --- 3.2: AND with one untranslatable child keeps the translatable conjunct ---

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
        // The LIKE half must be absent (dropped, not surfaced as a constraint).
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

    // --- 3.3: OR with one untranslatable child returns None ---

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
        // MUST return None — pruning on only the translatable branch would be unsound.
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

    // --- 3.4: NOT of untranslatable → None; NOT of translatable → negated ---

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
        // NOT (id < 5) should produce id >= 5
        let node = json!({
            "type": "predicate_not",
            "expression": {"type": "predicate_less", "left": col("ID"), "right": int_lit(5)}
        });
        let pred = to_iceberg_predicate(&node, &schema).expect("NOT of translatable must be Some");
        let s = format!("{pred}");
        // negate() turns LessThan into GreaterThanOrEq
        assert!(s.contains("id") && s.contains(">="), "got: {s}");
    }

    // --- 3.5: Unknown column / type mismatch → None ---

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
        // ID is an Int; a string literal should not produce a Datum → None.
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
        // ID is Int; one element is a string → whole IN must be None.
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
        // AMOUNT (Long) BETWEEN string "bad" AND 100
        // Low bound fails; high bound should survive alone.
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
}

//! Pruning-only: every emitted predicate is implied by the user predicate. An untranslatable node
//! is dropped, widening the file set, and `NOT` negates only an exactly translated child.

use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, NestedFieldRef, PrimitiveType, Schema};
use serde_json::Value as Json;

fn resolve_column<'s>(col_name: &str, schema: &'s Schema) -> Option<(&'s str, &'s PrimitiveType)> {
    let field: &NestedFieldRef = schema.field_by_name_case_insensitive(col_name)?;
    let prim = field.field_type.as_primitive_type()?;
    Some((&field.name, prim))
}

fn literal_to_datum(lit: &Json, prim: &PrimitiveType) -> Option<Datum> {
    let kind = lit.get("type")?.as_str()?;

    match (kind, prim) {
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

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Int) => {
            let v = lit.get("value")?;
            parse_i32(v).map(Datum::int)
        }

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Long) => {
            let v = lit.get("value")?;
            parse_i64(v).map(Datum::long)
        }

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Float) => {
            let v = lit.get("value")?;
            parse_f64(v).map(|f| Datum::float(f as f32))
        }

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Double) => {
            let v = lit.get("value")?;
            parse_f64(v).map(Datum::double)
        }

        ("literal_string", PrimitiveType::String) => {
            let s = lit.get("value")?.as_str()?;
            Some(Datum::string(s))
        }

        ("literal_date", PrimitiveType::Date) => {
            let s = lit.get("value")?.as_str()?;
            Datum::date_from_str(s).ok()
        }

        ("literal_timestamp", PrimitiveType::Timestamp | PrimitiveType::TimestampNs) => {
            let s = lit.get("value")?.as_str()?;
            parse_timestamp_no_tz(s, prim)
        }

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

/// Only the final six characters are checked, so a `-` inside the date is not an offset.
fn has_tz_offset(s: &str) -> bool {
    if s.ends_with('Z') {
        return true;
    }
    let tail = &s[s.len().saturating_sub(6)..];
    (tail.starts_with('+') || tail.starts_with('-')) && tail.contains(':')
}

fn parse_timestamptz(s: &str, prim: &PrimitiveType) -> Option<Datum> {
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

fn long_from_datum(d: Datum) -> Option<i64> {
    use iceberg::spec::{Literal, PrimitiveLiteral};
    match Literal::from(d) {
        Literal::Primitive(PrimitiveLiteral::Long(v)) => Some(v),
        _ => None,
    }
}

fn extract_column(node: &Json) -> Option<&str> {
    if node.get("type")?.as_str()? == "column" {
        node.get("name")?.as_str()
    } else {
        None
    }
}

/// `None` means no constraint: the caller must pass all files, never none.
pub fn to_iceberg_predicate(filter_json: &Json, schema: &Schema) -> Option<Predicate> {
    translate(filter_json, schema).map(|translated| translated.predicate)
}

/// Only an exact translation may be negated: `NOT` over a widened child would narrow it,
/// pruning files that still hold matching rows (#466).
struct Translated {
    predicate: Predicate,
    exact: bool,
}

impl Translated {
    fn exact(predicate: Predicate) -> Self {
        Self {
            predicate,
            exact: true,
        }
    }
}

fn translate(filter_json: &Json, schema: &Schema) -> Option<Translated> {
    let kind = filter_json.get("type")?.as_str()?;

    match kind {
        "predicate_equal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal" => {
            translate_binary(filter_json, kind, schema).map(Translated::exact)
        }

        // Not soundly prunable to a single range.
        "predicate_notequal" => None,

        "predicate_and" => {
            let exprs = filter_json.get("expressions")?.as_array()?;
            fold_and(exprs.iter().map(|expr| translate(expr, schema)))
        }
        "predicate_or" => {
            let exprs = filter_json.get("expressions")?.as_array()?;
            fold_or(exprs.iter().map(|expr| translate(expr, schema)))
        }
        "predicate_not" => {
            let inner = filter_json.get("expression")?;
            let Translated { predicate, exact } = translate(inner, schema)?;
            exact.then(|| Translated::exact(predicate.negate()))
        }

        "predicate_is_null" => {
            let col_node = filter_json.get("expression")?;
            let col_name = extract_column(col_node)?;
            let (exact_name, _prim) = resolve_column(col_name, schema)?;
            Some(Translated::exact(Reference::new(exact_name).is_null()))
        }
        "predicate_is_not_null" => {
            let col_node = filter_json.get("expression")?;
            let col_name = extract_column(col_node)?;
            let (exact_name, _prim) = resolve_column(col_name, schema)?;
            Some(Translated::exact(Reference::new(exact_name).is_not_null()))
        }

        "predicate_in_constlist" => translate_in(filter_json, schema).map(Translated::exact),

        "predicate_between" => translate_between(filter_json, schema),

        _ => None,
    }
}

fn translate_binary(node: &Json, kind: &str, schema: &Schema) -> Option<Predicate> {
    let left = node.get("left")?;
    let right = node.get("right")?;

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

fn fold_and(children: impl Iterator<Item = Option<Translated>>) -> Option<Translated> {
    let mut predicates = Vec::new();
    let mut exact = true;
    for child in children {
        match child {
            Some(translated) => {
                exact &= translated.exact;
                predicates.push(translated.predicate);
            }
            None => exact = false,
        }
    }
    let predicate = predicates.into_iter().reduce(Predicate::and)?;
    Some(Translated { predicate, exact })
}

fn fold_or(children: impl Iterator<Item = Option<Translated>>) -> Option<Translated> {
    let translated: Vec<Translated> = children.collect::<Option<_>>()?;
    let exact = translated.iter().all(|child| child.exact);
    let predicate = translated
        .into_iter()
        .map(|child| child.predicate)
        .reduce(Predicate::or)?;
    Some(Translated { predicate, exact })
}

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

/// Either bound alone is implied by BETWEEN, so a failed bound drops only its own conjunct
/// and leaves the result inexact.
fn translate_between(node: &Json, schema: &Schema) -> Option<Translated> {
    let col_node = node.get("expression")?;
    let col_name = extract_column(col_node)?;
    let (exact_name, prim) = resolve_column(col_name, schema)?;

    let low = node
        .get("left")
        .and_then(|n| literal_to_datum(n, prim))
        .map(|d| Translated::exact(Reference::new(exact_name).greater_than_or_equal_to(d)));
    let high = node
        .get("right")
        .and_then(|n| literal_to_datum(n, prim))
        .map(|d| Translated::exact(Reference::new(exact_name).less_than_or_equal_to(d)));

    fold_and([low, high].into_iter())
}

#[cfg(test)]
#[path = "iceberg_predicate_tests.rs"]
mod tests;

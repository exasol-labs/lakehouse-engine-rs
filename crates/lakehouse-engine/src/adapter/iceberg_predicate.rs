//! Pruning-only: every emitted predicate is implied by the user predicate. An untranslatable node
//! is dropped, widening the file set, and `NOT` negates only an exactly translated child.

use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Schema};
use serde_json::Value as Json;

use crate::adapter::pushdown::literal_binary_bytes;
use crate::scan::spec::LogicalField;
use crate::types::mapping::timestamp_literal_digits;

/// Resolves a filter column through its declared field id to the field the scanned schema carries
/// under that id: bounds are stored by field id, so a rename must not prune by another's bounds.
struct FilterColumns<'a> {
    declared: &'a [LogicalField],
    scanned: &'a Schema,
}

impl<'a> FilterColumns<'a> {
    fn resolve(&self, node: &Json) -> Option<(&'a str, &'a PrimitiveType)> {
        if node.get("type")?.as_str()? != "column" {
            return None;
        }
        let column = node.get("name")?.as_str()?.to_uppercase();
        let is_column = |field: &&LogicalField| field.name.to_uppercase() == column;
        let field_id = self.declared.iter().find(is_column)?.field_id?;
        let name = self.scanned.name_by_field_id(field_id)?;
        let field = self.scanned.field_by_id(field_id)?;
        Some((name, field.field_type.as_primitive_type()?))
    }
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

        // `Datum` parses at most microseconds; a truncated finer literal would prune matching files.
        ("literal_timestamp", PrimitiveType::Timestamp | PrimitiveType::TimestampNs) => {
            let s = lit.get("value")?.as_str()?;
            (timestamp_literal_digits(s) <= 6).then(|| parse_timestamp_no_tz(s, prim))?
        }

        ("literal_timestamp_utc", PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs) => {
            let s = lit.get("value")?.as_str()?;
            (timestamp_literal_digits(s) <= 6).then(|| parse_timestamptz(s, prim))?
        }

        ("literal_binary", PrimitiveType::Binary) => literal_binary_bytes(lit).map(Datum::binary),
        ("literal_binary", PrimitiveType::Fixed(width)) => literal_binary_bytes(lit)
            .filter(|bytes| u64::try_from(bytes.len()).ok() == Some(*width))
            .map(Datum::fixed),

        // No `uuid` arm: writers store `uuid` bounds in signed or unsigned order and iceberg-rust
        // reads them unsigned, so pruning could drop matching files (apache/iceberg#14216).
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

/// `None` means no constraint: the caller must pass all files, never none. `scanned` is the schema
/// the scan binds the predicate against; a column whose `declared` field id it lacks imposes none.
pub fn to_iceberg_predicate(
    filter_json: &Json,
    declared: &[LogicalField],
    scanned: &Schema,
) -> Option<Predicate> {
    let columns = FilterColumns { declared, scanned };
    translate(filter_json, &columns).map(|translated| translated.predicate)
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

fn translate(filter_json: &Json, columns: &FilterColumns) -> Option<Translated> {
    let kind = filter_json.get("type")?.as_str()?;

    match kind {
        "predicate_equal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal" => {
            translate_binary(filter_json, kind, columns).map(Translated::exact)
        }

        // Not soundly prunable to a single range.
        "predicate_notequal" => None,

        "predicate_and" => {
            let exprs = filter_json.get("expressions")?.as_array()?;
            fold_and(exprs.iter().map(|expr| translate(expr, columns)))
        }
        "predicate_or" => {
            let exprs = filter_json.get("expressions")?.as_array()?;
            fold_or(exprs.iter().map(|expr| translate(expr, columns)))
        }
        "predicate_not" => {
            let inner = filter_json.get("expression")?;
            let Translated { predicate, exact } = translate(inner, columns)?;
            exact.then(|| Translated::exact(predicate.negate()))
        }

        "predicate_is_null" => {
            let (name, _) = columns.resolve(filter_json.get("expression")?)?;
            Some(Translated::exact(Reference::new(name).is_null()))
        }
        "predicate_is_not_null" => {
            let (name, _) = columns.resolve(filter_json.get("expression")?)?;
            Some(Translated::exact(Reference::new(name).is_not_null()))
        }

        "predicate_in_constlist" => translate_in(filter_json, columns).map(Translated::exact),

        "predicate_between" => translate_between(filter_json, columns),

        _ => None,
    }
}

fn translate_binary(node: &Json, kind: &str, columns: &FilterColumns) -> Option<Predicate> {
    let left = node.get("left")?;
    let right = node.get("right")?;

    let ((name, prim), lit_node, col_is_left) = match columns.resolve(left) {
        Some(column) => (column, right, true),
        None => (columns.resolve(right)?, left, false),
    };

    let datum = literal_to_datum(lit_node, prim)?;
    let reference = Reference::new(name);

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

fn translate_in(node: &Json, columns: &FilterColumns) -> Option<Predicate> {
    let (name, prim) = columns.resolve(node.get("expression")?)?;

    let args = node.get("arguments")?.as_array()?;
    if args.is_empty() {
        return None;
    }
    let datums: Option<Vec<Datum>> = args.iter().map(|a| literal_to_datum(a, prim)).collect();
    let datums = datums?;
    Some(Reference::new(name).is_in(datums))
}

/// Either bound alone is implied by BETWEEN, so a failed bound drops only its own conjunct
/// and leaves the result inexact.
fn translate_between(node: &Json, columns: &FilterColumns) -> Option<Translated> {
    let (name, prim) = columns.resolve(node.get("expression")?)?;

    let low = node
        .get("left")
        .and_then(|n| literal_to_datum(n, prim))
        .map(|d| Translated::exact(Reference::new(name).greater_than_or_equal_to(d)));
    let high = node
        .get("right")
        .and_then(|n| literal_to_datum(n, prim))
        .map(|d| Translated::exact(Reference::new(name).less_than_or_equal_to(d)));

    fold_and([low, high].into_iter())
}

#[cfg(test)]
#[path = "iceberg_predicate_tests.rs"]
mod tests;

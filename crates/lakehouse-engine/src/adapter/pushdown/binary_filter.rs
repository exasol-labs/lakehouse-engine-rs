//! A binary column reads as its lowercase hex text, which sorts as its bytes do. A comparison
//! against a lowercase hex literal can therefore compare the raw bytes with a binary literal,
//! which Parquet statistics prune and which returns the same rows.

use std::collections::HashMap;

use arrow::datatypes::DataType;
use serde_json::{Value as Json, json};

use crate::scan::raw_bytes_alias;
use crate::scan::spec::LogicalField;
use crate::types::mapping::arrow_type_from_tag;

/// Keyed by uppercase column name; a fixed-length column carries its byte width.
#[derive(Debug, Default)]
pub(crate) struct BinaryColumns(HashMap<String, Option<i32>>);

impl BinaryColumns {
    pub(crate) fn of(logical_schema: &[LogicalField]) -> Self {
        Self(
            logical_schema
                .iter()
                .filter_map(|field| {
                    let width = match arrow_type_from_tag(&field.arrow_type) {
                        DataType::Binary => None,
                        DataType::FixedSizeBinary(width) => Some(width),
                        _ => return None,
                    };
                    Some((field.name.to_uppercase(), width))
                })
                .collect(),
        )
    }

    fn resolve(&self, node: &Json) -> Option<(String, Option<i32>)> {
        if node.get("type")?.as_str()? != "column" {
            return None;
        }
        let name = node.get("name")?.as_str()?.to_uppercase();
        let width = *self.0.get(&name)?;
        Some((name, width))
    }
}

/// Rewrites only the comparisons the root reaches through AND and OR, where a false result drops
/// a row exactly as a NULL one does, so the bytes may stand in for the hex text whose empty value
/// Exasol reads as NULL.
pub(crate) fn compare_binary_bytes(node: &Json, columns: &BinaryColumns) -> Json {
    match node.get("type").and_then(Json::as_str) {
        Some("predicate_and" | "predicate_or") => {
            let mut rewritten = node.clone();
            if let Some(Json::Array(children)) = rewritten.get_mut("expressions") {
                for child in children {
                    *child = compare_binary_bytes(child, columns);
                }
            }
            rewritten
        }
        _ => byte_comparison(node, columns).unwrap_or_else(|| node.clone()),
    }
}

fn byte_comparison(node: &Json, columns: &BinaryColumns) -> Option<Json> {
    match node.get("type")?.as_str()? {
        "predicate_in_constlist" => {
            let (column, width) = columns.resolve(node.get("expression")?)?;
            let values = node
                .get("arguments")?
                .as_array()?
                .iter()
                .map(|value| byte_literal(value, width))
                .collect::<Option<Vec<_>>>()?;
            Some(json!({
                "type": "predicate_in_constlist",
                "expression": raw_bytes(&column),
                "arguments": values,
            }))
        }
        "predicate_between" => {
            let (column, width) = columns.resolve(node.get("expression")?)?;
            Some(json!({
                "type": "predicate_between",
                "expression": raw_bytes(&column),
                "left": byte_literal(node.get("left")?, width)?,
                "right": byte_literal(node.get("right")?, width)?,
            }))
        }
        op @ ("predicate_equal"
        | "predicate_notequal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal") => {
            let (left, right) = (node.get("left")?, node.get("right")?);
            let ((column, width), literal, op) = match columns.resolve(left) {
                Some(column) => (column, right, op),
                None => (columns.resolve(right)?, left, mirrored(op)),
            };
            let comparison = json!({
                "type": op,
                "left": raw_bytes(&column),
                "right": byte_literal(literal, width)?,
            });
            // The empty value sorts first, so only these operators would keep the row Exasol
            // reads as NULL; a fixed-length value is never empty.
            let admits_empty = matches!(
                op,
                "predicate_notequal" | "predicate_less" | "predicate_lessequal"
            );
            if width.is_none() && admits_empty {
                let non_empty = json!({
                    "type": "predicate_notequal",
                    "left": raw_bytes(&column),
                    "right": {"type": "literal_binary", "value": ""},
                });
                return Some(json!({
                    "type": "predicate_and",
                    "expressions": [comparison, non_empty],
                }));
            }
            Some(comparison)
        }
        _ => None,
    }
}

fn mirrored(op: &str) -> &str {
    match op {
        "predicate_less" => "predicate_greater",
        "predicate_lessequal" => "predicate_greaterequal",
        "predicate_greater" => "predicate_less",
        "predicate_greaterequal" => "predicate_lessequal",
        symmetric => symmetric,
    }
}

fn raw_bytes(column: &str) -> Json {
    json!({"type": "column", "name": raw_bytes_alias(column)})
}

/// Only lowercase hex of whole bytes names a byte string: Exasol compares text case-sensitively,
/// and the column never renders an uppercase digit. An empty string is Exasol's NULL.
fn byte_literal(node: &Json, width: Option<i32>) -> Option<Json> {
    if node.get("type")?.as_str()? != "literal_string" {
        return None;
    }
    let hex = node.get("value")?.as_str()?;
    let whole_bytes = !hex.is_empty() && hex.len() % 2 == 0;
    let lowercase = hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    let fits = width.is_none_or(|width| usize::try_from(width).ok() == Some(hex.len() / 2));
    (whole_bytes && lowercase && fits).then(|| match width {
        Some(width) => json!({"type": "literal_binary", "value": hex, "width": width}),
        None => json!({"type": "literal_binary", "value": hex}),
    })
}

#[cfg(test)]
#[path = "binary_filter_tests.rs"]
mod tests;

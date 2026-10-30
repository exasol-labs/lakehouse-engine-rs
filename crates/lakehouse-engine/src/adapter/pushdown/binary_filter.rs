//! A binary column reads as its lowercase hex text, which sorts as its bytes do. A comparison
//! against a lowercase hex literal can therefore compare the raw bytes with a binary literal,
//! which Parquet statistics prune and which returns the same rows.

use std::collections::HashMap;

use arrow::datatypes::DataType;
use serde_json::{Value as Json, json};

use super::format::filter_json::{Comparison, column_name};
use crate::scan::raw_bytes_alias;
use crate::scan::spec::LogicalField;
use crate::types::mapping::arrow_type_from_tag;

/// Keyed by uppercase column name; a fixed-length column carries its byte width.
#[derive(Debug, Default)]
pub(crate) struct BinaryColumns(HashMap<String, Option<i32>>);

impl BinaryColumns {
    /// Under `binary_as_string` a variable-length column reads as text, so its filters stay text.
    pub(crate) fn of(logical_schema: &[LogicalField], binary_as_string: bool) -> Self {
        Self(
            logical_schema
                .iter()
                .filter_map(|field| {
                    let width = match arrow_type_from_tag(&field.arrow_type) {
                        DataType::Binary if !binary_as_string => None,
                        DataType::FixedSizeBinary(width) => Some(width),
                        _ => return None,
                    };
                    Some((field.name.to_uppercase(), width))
                })
                .collect(),
        )
    }

    fn resolve(&self, node: &Json) -> Option<(String, Option<i32>)> {
        let name = column_name(node)?.to_uppercase();
        let width = *self.0.get(&name)?;
        Some((name, width))
    }
}

/// Rewrites only the comparisons the root reaches through AND and OR, where a false result drops
/// a row exactly as a NULL one does, so the bytes may stand in for the hex text whose empty value
/// Exasol reads as NULL.
pub(crate) fn compare_binary_bytes(mut node: Json, columns: &BinaryColumns) -> Json {
    match node.get("type").and_then(Json::as_str) {
        Some("predicate_and" | "predicate_or") => {
            if let Some(Json::Array(children)) = node.get_mut("expressions") {
                for child in children {
                    *child = compare_binary_bytes(child.take(), columns);
                }
            }
            node
        }
        _ => byte_comparison(&node, columns).unwrap_or(node),
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
        kind => {
            let op = Comparison::from_node_type(kind)?;
            let (left, right) = (node.get("left")?, node.get("right")?);
            let ((column, width), literal, op) = match columns.resolve(left) {
                Some(column) => (column, right, op),
                None => (columns.resolve(right)?, left, op.with_operands_swapped()),
            };
            let comparison = json!({
                "type": op.node_type(),
                "left": raw_bytes(&column),
                "right": byte_literal(literal, width)?,
            });
            // The empty value sorts first, so only these operators would keep the row Exasol
            // reads as NULL; a fixed-length value is never empty.
            let admits_empty = matches!(
                op,
                Comparison::NotEqual | Comparison::Less | Comparison::LessEqual
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
    let digits = node.get("value")?.as_str()?;
    if digits.bytes().any(|b| b.is_ascii_uppercase()) {
        return None;
    }
    let bytes = hex::decode(digits).ok().filter(|bytes| !bytes.is_empty())?;
    let fits = width.is_none_or(|width| usize::try_from(width).ok() == Some(bytes.len()));
    fits.then(|| match width {
        Some(width) => json!({"type": "literal_binary", "value": digits, "width": width}),
        None => json!({"type": "literal_binary", "value": digits}),
    })
}

/// For format-level pruning: retypes each hex literal a binary column is compared with, at any
/// depth. The raw bytes differ from the hex text only on an empty value, which the text reads as
/// NULL, so the retyped tree is TRUE on every row the original is and never prunes a match.
pub(crate) fn with_binary_literals(mut node: Json, columns: &BinaryColumns) -> Json {
    retype_binary_literals(&mut node, columns);
    node
}

fn retype_binary_literals(node: &mut Json, columns: &BinaryColumns) {
    let operands: &[&str] = match node.get("type").and_then(Json::as_str) {
        Some("predicate_and" | "predicate_or") => {
            if let Some(Json::Array(children)) = node.get_mut("expressions") {
                for child in children {
                    retype_binary_literals(child, columns);
                }
            }
            return;
        }
        Some("predicate_not") => {
            if let Some(child) = node.get_mut("expression") {
                retype_binary_literals(child, columns);
            }
            return;
        }
        Some("predicate_in_constlist") => &["expression", "arguments"],
        Some("predicate_between") => &["expression", "left", "right"],
        Some(kind) if Comparison::from_node_type(kind).is_some() => &["left", "right"],
        _ => return,
    };
    let Some((_, width)) = operands
        .iter()
        .filter_map(|key| columns.resolve(node.get(*key)?))
        .next()
    else {
        return;
    };
    for key in operands {
        match node.get_mut(*key) {
            Some(Json::Array(values)) => values
                .iter_mut()
                .for_each(|value| retype_literal(value, width)),
            Some(operand) => retype_literal(operand, width),
            None => {}
        }
    }
}

fn retype_literal(node: &mut Json, width: Option<i32>) {
    if let Some(literal) = byte_literal(node, width) {
        *node = literal;
    }
}

/// The bytes of a `literal_binary` node that [`with_binary_literals`] wrote.
pub(crate) fn literal_binary_bytes(node: &Json) -> Option<Vec<u8>> {
    if node.get("type")?.as_str()? != "literal_binary" {
        return None;
    }
    hex::decode(node.get("value")?.as_str()?).ok()
}

#[cfg(test)]
#[path = "binary_filter_tests.rs"]
mod tests;

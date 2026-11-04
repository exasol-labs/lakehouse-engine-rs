//! A binary column reads as its lowercase hex text, which sorts as its bytes do. A comparison
//! against a lowercase hex literal can therefore compare the raw bytes with a binary literal,
//! which Parquet statistics prune and which returns the same rows.

use arrow::datatypes::DataType;
use serde_json::{Value as Json, json};
use vs_expression::ColumnNode;

use super::format::filter_json::Comparison;
use super::support::{ColumnTypes, column_node_ref};
use crate::scan::raw_bytes_alias;
use crate::scan::spec::{ColumnRef, LogicalField};
use crate::types::mapping::arrow_type_from_tag;

/// How the scan reads a binary column's raw bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BinaryLayout {
    Variable,
    /// A fixed-length column carries its byte width.
    Fixed(i32),
}

impl BinaryLayout {
    /// `None` for a field the scan reads as text. Under `binary_as_string` a variable-length
    /// column reads as text, so its filters stay text.
    pub(super) fn of(field: &LogicalField, binary_as_string: bool) -> Option<Self> {
        match arrow_type_from_tag(&field.arrow_type) {
            DataType::Binary if !binary_as_string => Some(Self::Variable),
            DataType::FixedSizeBinary(width) => Some(Self::Fixed(width)),
            _ => None,
        }
    }
}

/// The node's own (relation alias, column) pair, so the raw-bytes column it becomes stays on the
/// node's relation, with the layout of the binary column it binds.
fn binary_column(columns: &ColumnTypes, node: &Json) -> Option<(ColumnRef, BinaryLayout)> {
    let column = column_node_ref(node)?;
    let layout = columns.get(&column)?.binary?;
    Some((column, layout))
}

/// Rewrites only the comparisons the root reaches through AND and OR, where a false result drops
/// a row exactly as a NULL one does, so the bytes may stand in for the hex text whose empty value
/// Exasol reads as NULL.
pub(crate) fn compare_binary_bytes(mut node: Json, columns: &ColumnTypes) -> Json {
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

fn byte_comparison(node: &Json, columns: &ColumnTypes) -> Option<Json> {
    match node.get("type")?.as_str()? {
        "predicate_in_constlist" => {
            let (column, layout) = binary_column(columns, node.get("expression")?)?;
            let values = node
                .get("arguments")?
                .as_array()?
                .iter()
                .map(|value| byte_literal(value, layout))
                .collect::<Option<Vec<_>>>()?;
            Some(json!({
                "type": "predicate_in_constlist",
                "expression": raw_bytes(&column),
                "arguments": values,
            }))
        }
        "predicate_between" => {
            let (column, layout) = binary_column(columns, node.get("expression")?)?;
            Some(json!({
                "type": "predicate_between",
                "expression": raw_bytes(&column),
                "left": byte_literal(node.get("left")?, layout)?,
                "right": byte_literal(node.get("right")?, layout)?,
            }))
        }
        kind => {
            let op = Comparison::from_node_type(kind)?;
            let (left, right) = (node.get("left")?, node.get("right")?);
            let ((column, layout), literal, op) = match binary_column(columns, left) {
                Some(column) => (column, right, op),
                None => (
                    binary_column(columns, right)?,
                    left,
                    op.with_operands_swapped(),
                ),
            };
            let comparison = json!({
                "type": op.node_type(),
                "left": raw_bytes(&column),
                "right": byte_literal(literal, layout)?,
            });
            // The empty value sorts first, so only these operators would keep the row Exasol
            // reads as NULL; a fixed-length value is never empty.
            let admits_empty = matches!(
                op,
                Comparison::NotEqual | Comparison::Less | Comparison::LessEqual
            );
            if layout == BinaryLayout::Variable && admits_empty {
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

fn raw_bytes(column: &ColumnRef) -> Json {
    let name = raw_bytes_alias(&column.name);
    ColumnNode::to_json(&name, None, column.relation.as_deref())
}

/// Only lowercase hex of whole bytes names a byte string: Exasol compares text case-sensitively,
/// and the column never renders an uppercase digit. An empty string is Exasol's NULL.
fn byte_literal(node: &Json, layout: BinaryLayout) -> Option<Json> {
    if node.get("type")?.as_str()? != "literal_string" {
        return None;
    }
    let digits = node.get("value")?.as_str()?;
    if digits.bytes().any(|b| b.is_ascii_uppercase()) {
        return None;
    }
    let bytes = hex::decode(digits).ok().filter(|bytes| !bytes.is_empty())?;
    match layout {
        BinaryLayout::Variable => Some(json!({"type": "literal_binary", "value": digits})),
        BinaryLayout::Fixed(width) => (usize::try_from(width).ok() == Some(bytes.len()))
            .then(|| json!({"type": "literal_binary", "value": digits, "width": width})),
    }
}

/// For format-level pruning: retypes each hex literal a binary column is compared with, at any
/// depth. The raw bytes differ from the hex text only on an empty value, which the text reads as
/// NULL, so the retyped tree is TRUE on every row the original is and never prunes a match.
pub(crate) fn with_binary_literals(mut node: Json, columns: &ColumnTypes) -> Json {
    retype_binary_literals(&mut node, columns);
    node
}

fn retype_binary_literals(node: &mut Json, columns: &ColumnTypes) {
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
    let Some((_, layout)) = operands
        .iter()
        .filter_map(|key| binary_column(columns, node.get(*key)?))
        .next()
    else {
        return;
    };
    for key in operands {
        match node.get_mut(*key) {
            Some(Json::Array(values)) => values
                .iter_mut()
                .for_each(|value| retype_literal(value, layout)),
            Some(operand) => retype_literal(operand, layout),
            None => {}
        }
    }
}

fn retype_literal(node: &mut Json, layout: BinaryLayout) {
    if let Some(literal) = byte_literal(node, layout) {
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

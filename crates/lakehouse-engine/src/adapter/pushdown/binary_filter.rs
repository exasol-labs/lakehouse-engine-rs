//! A binary column reads as its lowercase hex text, which sorts as its bytes do. A comparison
//! against a lowercase hex literal can therefore compare the raw bytes with a binary literal,
//! which Parquet statistics prune and which returns the same rows.

use arrow::datatypes::DataType;
use vs_expression::{Column, CompareOp, Expr, Literal, LiteralKind, Scalar};

use super::support::{ColumnTypes, column_ref};
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
fn binary_column(columns: &ColumnTypes, node: &Expr) -> Option<(ColumnRef, BinaryLayout)> {
    let column = column_ref(node.as_column()?);
    let layout = columns.get(&column)?.binary?;
    Some((column, layout))
}

/// Rewrites only the comparisons the root reaches through AND and OR, where a false result drops
/// a row exactly as a NULL one does, so the bytes may stand in for the hex text whose empty value
/// Exasol reads as NULL.
pub(crate) fn compare_binary_bytes(node: Expr, columns: &ColumnTypes) -> Expr {
    let each = |children: Vec<Expr>| {
        children
            .into_iter()
            .map(|child| compare_binary_bytes(child, columns))
            .collect()
    };
    match node {
        Expr::And(children) => Expr::And(each(children)),
        Expr::Or(children) => Expr::Or(each(children)),
        _ => byte_comparison(&node, columns).unwrap_or(node),
    }
}

fn byte_comparison(node: &Expr, columns: &ColumnTypes) -> Option<Expr> {
    match node {
        Expr::InList { expression, values } => {
            let (column, layout) = binary_column(columns, expression)?;
            let values = values
                .iter()
                .map(|value| byte_literal(value, layout))
                .collect::<Option<Vec<_>>>()?;
            Some(Expr::InList {
                expression: Box::new(raw_bytes(&column)),
                values,
            })
        }
        Expr::Between {
            expression,
            low,
            high,
        } => {
            let (column, layout) = binary_column(columns, expression)?;
            Some(Expr::Between {
                expression: Box::new(raw_bytes(&column)),
                low: Box::new(byte_literal(low, layout)?),
                high: Box::new(byte_literal(high, layout)?),
            })
        }
        Expr::Compare { op, left, right } => {
            let ((column, layout), literal, op) = match binary_column(columns, left) {
                Some(column) => (column, right, *op),
                None => (binary_column(columns, right)?, left, op.mirrored()),
            };
            let comparison = Expr::compare(op, raw_bytes(&column), byte_literal(literal, layout)?);
            // The empty value sorts first, so only these operators would keep the row Exasol
            // reads as NULL; a fixed-length value is never empty.
            let admits_empty = matches!(
                op,
                CompareOp::NotEqual | CompareOp::Less | CompareOp::LessEqual
            );
            if layout == BinaryLayout::Variable && admits_empty {
                let empty = Literal::new(
                    LiteralKind::Binary { width: None },
                    Scalar::String(String::new()),
                );
                let non_empty = Expr::compare(
                    CompareOp::NotEqual,
                    raw_bytes(&column),
                    Expr::Literal(empty),
                );
                return Some(Expr::And(vec![comparison, non_empty]));
            }
            Some(comparison)
        }
        _ => None,
    }
}

fn raw_bytes(column: &ColumnRef) -> Expr {
    Expr::Column(Column::aliased(
        raw_bytes_alias(&column.name),
        column.relation.as_deref(),
    ))
}

/// Only lowercase hex of whole bytes names a byte string: Exasol compares text case-sensitively,
/// and the column never renders an uppercase digit. An empty string is Exasol's NULL.
fn byte_literal(node: &Expr, layout: BinaryLayout) -> Option<Expr> {
    let literal = node.as_literal()?;
    if literal.kind != LiteralKind::String {
        return None;
    }
    let digits = literal.value.as_str()?;
    if digits.bytes().any(|b| b.is_ascii_uppercase()) {
        return None;
    }
    let bytes = hex::decode(digits).ok().filter(|bytes| !bytes.is_empty())?;
    let width = match layout {
        BinaryLayout::Variable => None,
        BinaryLayout::Fixed(width) => {
            if usize::try_from(width).ok() != Some(bytes.len()) {
                return None;
            }
            Some(u64::try_from(width).ok()?)
        }
    };
    Some(Expr::Literal(Literal::new(
        LiteralKind::Binary { width },
        Scalar::String(digits.to_string()),
    )))
}

/// For format-level pruning: retypes each hex literal a binary column is compared with, at any
/// depth. The raw bytes differ from the hex text only on an empty value, which the text reads as
/// NULL, so the retyped tree is TRUE on every row the original is and never prunes a match.
pub(crate) fn with_binary_literals(mut node: Expr, columns: &ColumnTypes) -> Expr {
    retype_binary_literals(&mut node, columns);
    node
}

fn retype_binary_literals(node: &mut Expr, columns: &ColumnTypes) {
    let operands: Vec<&mut Expr> = match node {
        Expr::And(children) | Expr::Or(children) => {
            for child in children {
                retype_binary_literals(child, columns);
            }
            return;
        }
        Expr::Not(child) => {
            retype_binary_literals(child, columns);
            return;
        }
        Expr::InList { expression, values } => {
            std::iter::once(&mut **expression).chain(values).collect()
        }
        Expr::Between {
            expression,
            low,
            high,
        } => vec![expression, low, high]
            .into_iter()
            .map(|operand| &mut **operand)
            .collect(),
        Expr::Compare { left, right, .. } => vec![&mut **left, &mut **right],
        _ => return,
    };
    let Some((_, layout)) = operands
        .iter()
        .find_map(|operand| binary_column(columns, operand))
    else {
        return;
    };
    for operand in operands {
        if let Some(literal) = byte_literal(operand, layout) {
            *operand = literal;
        }
    }
}

/// The bytes of a binary literal that [`with_binary_literals`] wrote.
pub(crate) fn literal_binary_bytes(node: &Expr) -> Option<Vec<u8>> {
    let literal = node.as_literal()?;
    if !matches!(literal.kind, LiteralKind::Binary { .. }) {
        return None;
    }
    hex::decode(literal.value.as_str()?).ok()
}

#[cfg(test)]
#[path = "binary_filter_tests.rs"]
mod tests;

//! Shape readers for Exasol pushdown filter-JSON nodes, shared by the pruning backends.

use serde_json::Value as Json;
use std::cmp::Ordering;

#[derive(Clone, Copy)]
pub(super) enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

impl Comparison {
    pub(super) fn from_node_type(kind: &str) -> Option<Self> {
        Some(match kind {
            "predicate_equal" => Self::Equal,
            "predicate_notequal" => Self::NotEqual,
            "predicate_less" => Self::Less,
            "predicate_lessequal" => Self::LessEqual,
            "predicate_greater" => Self::Greater,
            "predicate_greaterequal" => Self::GreaterEqual,
            _ => return None,
        })
    }

    pub(super) fn with_operands_swapped(self) -> Self {
        match self {
            Self::Less => Self::Greater,
            Self::LessEqual => Self::GreaterEqual,
            Self::Greater => Self::Less,
            Self::GreaterEqual => Self::LessEqual,
            symmetric => symmetric,
        }
    }

    pub(super) fn holds(self, ordering: Ordering) -> bool {
        match self {
            Self::Equal => ordering == Ordering::Equal,
            Self::NotEqual => ordering != Ordering::Equal,
            Self::Less => ordering == Ordering::Less,
            Self::LessEqual => ordering != Ordering::Greater,
            Self::Greater => ordering == Ordering::Greater,
            Self::GreaterEqual => ordering != Ordering::Less,
        }
    }
}

/// The name of a `column` node, as the request spells it.
pub(super) fn column_name(node: &Json) -> Option<&str> {
    if node.get("type")?.as_str()? != "column" {
        return None;
    }
    node.get("name")?.as_str()
}

/// Exasol reads `''` as NULL, so an empty string constrains nothing.
pub(super) fn non_empty_str(value: &Json) -> Option<&str> {
    let s = value.as_str()?;
    (!s.is_empty()).then_some(s)
}

/// An AND/OR node's operands; `None` when empty, since an empty OR would prune every file.
pub(super) fn operands(node: &Json) -> Option<&[Json]> {
    non_empty(node.get("expressions")?)
}

/// A comparison's column, its other operand, and the comparison as read with the column on the
/// left.
pub(super) fn comparison_operands(
    node: &Json,
    comparison: Comparison,
) -> Option<(&str, &Json, Comparison)> {
    let left = node.get("left")?;
    let right = node.get("right")?;
    if let Some(column) = column_name(left) {
        return Some((column, right, comparison));
    }
    Some((
        column_name(right)?,
        left,
        comparison.with_operands_swapped(),
    ))
}

/// The column of an IS [NOT] NULL, IN, or BETWEEN node.
pub(super) fn subject_column(node: &Json) -> Option<&str> {
    column_name(node.get("expression")?)
}

/// An IN node's column and its elements; `None` when empty, since an empty IN would prune every
/// file.
pub(super) fn in_operands(node: &Json) -> Option<(&str, &[Json])> {
    Some((subject_column(node)?, non_empty(node.get("arguments")?)?))
}

/// A BETWEEN node's column and its low and high bounds.
pub(super) fn between_operands(node: &Json) -> Option<(&str, Option<&Json>, Option<&Json>)> {
    Some((subject_column(node)?, node.get("left"), node.get("right")))
}

fn non_empty(array: &Json) -> Option<&[Json]> {
    let items = array.as_array()?;
    (!items.is_empty()).then_some(items.as_slice())
}

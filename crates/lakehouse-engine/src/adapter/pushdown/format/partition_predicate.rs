use serde_json::Value as Json;
use std::collections::BTreeMap;

use super::filter_json::{
    Comparison, between_operands, comparison_operands, in_operands, non_empty_str, operands,
    subject_column,
};

/// A file is kept iff TRUE is reachable at the root; an untranslatable node reaches every truth
/// value, so it can only widen the kept set. Strings compare in codepoint order, as Exasol does.
pub(super) struct PartitionPredicate {
    root: Node,
}

impl PartitionPredicate {
    /// `None` keeps every file.
    pub(super) fn from_filter(filter_json: Option<&Json>) -> Self {
        Self {
            root: filter_json.map_or(Node::Opaque, translate),
        }
    }

    /// Column names match partition keys case-insensitively (uppercase fold).
    pub(super) fn keeps(&self, partition_values: &BTreeMap<String, Option<String>>) -> bool {
        self.root.reachable(partition_values).can_be_true
    }
}

enum Node {
    Compare {
        column: String,
        comparison: Comparison,
        literal: String,
    },
    IsNull {
        column: String,
    },
    And(Vec<Node>),
    Or(Vec<Node>),
    Not(Box<Node>),
    Opaque,
}

/// The SQL truth values a node can reach across a file's rows; neither flag set means only NULL.
#[derive(Clone, Copy)]
struct Reachable {
    can_be_true: bool,
    can_be_false: bool,
}

impl Reachable {
    const ANY: Self = Self {
        can_be_true: true,
        can_be_false: true,
    };

    fn exactly(truth: Option<bool>) -> Self {
        Self {
            can_be_true: truth == Some(true),
            can_be_false: truth == Some(false),
        }
    }

    fn and(self, other: Self) -> Self {
        Self {
            can_be_true: self.can_be_true && other.can_be_true,
            can_be_false: self.can_be_false || other.can_be_false,
        }
    }

    fn or(self, other: Self) -> Self {
        Self {
            can_be_true: self.can_be_true || other.can_be_true,
            can_be_false: self.can_be_false && other.can_be_false,
        }
    }

    fn negated(self) -> Self {
        Self {
            can_be_true: self.can_be_false,
            can_be_false: self.can_be_true,
        }
    }
}

impl Node {
    fn reachable(&self, values: &BTreeMap<String, Option<String>>) -> Reachable {
        match self {
            Self::Compare {
                column,
                comparison,
                literal,
            } => partition_value(values, column).map_or(Reachable::ANY, |value| {
                Reachable::exactly(value.map(|value| comparison.holds(value.cmp(literal.as_str()))))
            }),
            Self::IsNull { column } => partition_value(values, column)
                .map_or(Reachable::ANY, |value| {
                    Reachable::exactly(Some(value.is_none()))
                }),
            Self::And(operands) => operands
                .iter()
                .map(|operand| operand.reachable(values))
                .fold(Reachable::exactly(Some(true)), Reachable::and),
            Self::Or(operands) => operands
                .iter()
                .map(|operand| operand.reachable(values))
                .fold(Reachable::exactly(Some(false)), Reachable::or),
            Self::Not(operand) => operand.reachable(values).negated(),
            Self::Opaque => Reachable::ANY,
        }
    }
}

/// `None` when `column` names no partition key; `Some(None)` when the file's value is NULL.
fn partition_value<'a>(
    values: &'a BTreeMap<String, Option<String>>,
    column: &str,
) -> Option<Option<&'a str>> {
    values
        .iter()
        .find(|(key, _)| key.chars().flat_map(char::to_uppercase).eq(column.chars()))
        .map(|(_, value)| value.as_deref())
}

fn translate(node: &Json) -> Node {
    let Some(kind) = node.get("type").and_then(Json::as_str) else {
        return Node::Opaque;
    };
    match kind {
        "predicate_and" => translate_operands(node).map_or(Node::Opaque, Node::And),
        "predicate_or" => translate_operands(node).map_or(Node::Opaque, Node::Or),
        "predicate_not" => node.get("expression").map_or(Node::Opaque, |operand| {
            Node::Not(Box::new(translate(operand)))
        }),
        "predicate_is_null" => translate_is_null(node).unwrap_or(Node::Opaque),
        "predicate_is_not_null" => {
            translate_is_null(node).map_or(Node::Opaque, |is_null| Node::Not(Box::new(is_null)))
        }
        "predicate_in_constlist" => translate_in(node).unwrap_or(Node::Opaque),
        "predicate_between" => translate_between(node).unwrap_or(Node::Opaque),
        other => Comparison::from_node_type(other)
            .and_then(|comparison| translate_comparison(node, comparison))
            .unwrap_or(Node::Opaque),
    }
}

fn translate_operands(node: &Json) -> Option<Vec<Node>> {
    Some(operands(node)?.iter().map(translate).collect())
}

fn translate_is_null(node: &Json) -> Option<Node> {
    Some(Node::IsNull {
        column: subject_column(node)?.to_uppercase(),
    })
}

fn translate_comparison(node: &Json, comparison: Comparison) -> Option<Node> {
    let (column, literal, comparison) = comparison_operands(node, comparison)?;
    Some(Node::Compare {
        column: column.to_uppercase(),
        comparison,
        literal: string_literal(Some(literal))?,
    })
}

fn translate_in(node: &Json) -> Option<Node> {
    let (column, arguments) = in_operands(node)?;
    let column = column.to_uppercase();
    let equalities = arguments
        .iter()
        .map(|argument| {
            Some(Node::Compare {
                column: column.clone(),
                comparison: Comparison::Equal,
                literal: string_literal(Some(argument))?,
            })
        })
        .collect::<Option<Vec<Node>>>()?;
    Some(Node::Or(equalities))
}

fn translate_between(node: &Json) -> Option<Node> {
    let (column, low, high) = between_operands(node)?;
    let column = column.to_uppercase();
    let low = string_literal(low)?;
    let high = string_literal(high)?;
    Some(Node::And(vec![
        Node::Compare {
            column: column.clone(),
            comparison: Comparison::GreaterEqual,
            literal: low,
        },
        Node::Compare {
            column,
            comparison: Comparison::LessEqual,
            literal: high,
        },
    ]))
}

/// Only a non-empty string literal compares as a string: Exasol compares a VARCHAR against a number
/// numerically.
fn string_literal(node: Option<&Json>) -> Option<String> {
    let node = node?;
    if node.get("type")?.as_str()? != "literal_string" {
        return None;
    }
    non_empty_str(node.get("value")?).map(str::to_string)
}

#[cfg(test)]
#[path = "partition_predicate_tests.rs"]
mod tests;

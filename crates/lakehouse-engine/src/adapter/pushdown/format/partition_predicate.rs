use serde_json::Value as Json;
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// Decides, from one file's partition values alone, whether any of its rows can satisfy a
/// pushdown filter.
///
/// Each filter node evaluates to the SET of SQL truth values its rows can reach. A node comparing
/// a partition column with string literals is exact per file, because a partition value is the
/// same for every row of its file. Every other node can reach TRUE, FALSE, and NULL, because only
/// the rows themselves decide it. A file is kept iff TRUE is reachable at the root, so a node this
/// type cannot evaluate widens the kept set and never narrows it: soundness holds by construction,
/// with no separate bookkeeping of which subtrees translated exactly.
///
/// Strings compare in Rust's native `str` order (UTF-8 byte, i.e. codepoint, order), which is
/// DataFusion's `Utf8` order and, as verified live, Exasol's own `VARCHAR` order.
pub(super) struct PartitionPredicate {
    root: Node,
}

impl PartitionPredicate {
    /// Translates `filter_json` once, so each file costs only an evaluation; `None` keeps every
    /// file.
    pub(super) fn from_filter(filter_json: Option<&Json>) -> Self {
        Self {
            root: filter_json.map_or(Node::Opaque, translate),
        }
    }

    /// Whether any row of the file carrying `partition_values` can satisfy the filter. A filter
    /// column names a partition key when both fold to the same uppercase name, the fold the
    /// table's declaration applies.
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
    InList {
        column: String,
        literals: Vec<String>,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    And(Vec<Node>),
    Or(Vec<Node>),
    Not(Box<Node>),
    Opaque,
}

#[derive(Clone, Copy)]
enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

impl Comparison {
    fn from_node_type(kind: &str) -> Option<Self> {
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

    fn with_operands_swapped(self) -> Self {
        match self {
            Self::Less => Self::Greater,
            Self::LessEqual => Self::GreaterEqual,
            Self::Greater => Self::Less,
            Self::GreaterEqual => Self::LessEqual,
            symmetric => symmetric,
        }
    }

    fn holds(self, ordering: Ordering) -> bool {
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

/// The SQL truth values a node can reach across a file's rows; never empty.
#[derive(Clone, Copy)]
struct Reachable {
    can_be_true: bool,
    can_be_false: bool,
    can_be_null: bool,
}

impl Reachable {
    const ANY: Self = Self {
        can_be_true: true,
        can_be_false: true,
        can_be_null: true,
    };

    fn exactly(truth: Option<bool>) -> Self {
        Self {
            can_be_true: truth == Some(true),
            can_be_false: truth == Some(false),
            can_be_null: truth.is_none(),
        }
    }

    fn and(self, other: Self) -> Self {
        Self {
            can_be_true: self.can_be_true && other.can_be_true,
            can_be_false: self.can_be_false || other.can_be_false,
            can_be_null: (self.can_be_null && (other.can_be_true || other.can_be_null))
                || (other.can_be_null && (self.can_be_true || self.can_be_null)),
        }
    }

    fn or(self, other: Self) -> Self {
        Self {
            can_be_true: self.can_be_true || other.can_be_true,
            can_be_false: self.can_be_false && other.can_be_false,
            can_be_null: (self.can_be_null && (other.can_be_false || other.can_be_null))
                || (other.can_be_null && (self.can_be_false || self.can_be_null)),
        }
    }

    fn negated(self) -> Self {
        Self {
            can_be_true: self.can_be_false,
            can_be_false: self.can_be_true,
            can_be_null: self.can_be_null,
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
            } => on_partition_value(values, column, |value| {
                comparison.holds(value.cmp(literal.as_str()))
            }),
            Self::InList { column, literals } => on_partition_value(values, column, |value| {
                literals.iter().any(|literal| literal == value)
            }),
            Self::IsNull { column } => partition_value(values, column)
                .map_or(Reachable::ANY, |value| {
                    Reachable::exactly(Some(value.is_none()))
                }),
            Self::IsNotNull { column } => partition_value(values, column)
                .map_or(Reachable::ANY, |value| {
                    Reachable::exactly(Some(value.is_some()))
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

fn on_partition_value(
    values: &BTreeMap<String, Option<String>>,
    column: &str,
    test: impl Fn(&str) -> bool,
) -> Reachable {
    partition_value(values, column)
        .map_or(Reachable::ANY, |value| Reachable::exactly(value.map(test)))
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
        "predicate_and" => operands(node).map_or(Node::Opaque, Node::And),
        "predicate_or" => operands(node).map_or(Node::Opaque, Node::Or),
        "predicate_not" => node.get("expression").map_or(Node::Opaque, |operand| {
            Node::Not(Box::new(translate(operand)))
        }),
        "predicate_is_null" => {
            column_of(node.get("expression")).map_or(Node::Opaque, |column| Node::IsNull { column })
        }
        "predicate_is_not_null" => column_of(node.get("expression"))
            .map_or(Node::Opaque, |column| Node::IsNotNull { column }),
        "predicate_in_constlist" => translate_in(node).unwrap_or(Node::Opaque),
        "predicate_between" => translate_between(node).unwrap_or(Node::Opaque),
        other => Comparison::from_node_type(other)
            .and_then(|comparison| translate_comparison(node, comparison))
            .unwrap_or(Node::Opaque),
    }
}

/// `None` for an empty operand list, which Exasol never sends: an empty OR would otherwise
/// evaluate FALSE and prune every file.
fn operands(node: &Json) -> Option<Vec<Node>> {
    let operands = node.get("expressions")?.as_array()?;
    (!operands.is_empty()).then(|| operands.iter().map(translate).collect())
}

fn translate_comparison(node: &Json, comparison: Comparison) -> Option<Node> {
    let left = node.get("left");
    let right = node.get("right");
    let (column, literal, comparison) = match (column_of(left), string_literal(right)) {
        (Some(column), Some(literal)) => (column, literal, comparison),
        _ => (
            column_of(right)?,
            string_literal(left)?,
            comparison.with_operands_swapped(),
        ),
    };
    Some(Node::Compare {
        column,
        comparison,
        literal,
    })
}

fn translate_in(node: &Json) -> Option<Node> {
    let column = column_of(node.get("expression"))?;
    let arguments = node.get("arguments")?.as_array()?;
    if arguments.is_empty() {
        return None;
    }
    let literals = arguments
        .iter()
        .map(|argument| string_literal(Some(argument)))
        .collect::<Option<Vec<String>>>()?;
    Some(Node::InList { column, literals })
}

fn translate_between(node: &Json) -> Option<Node> {
    let column = column_of(node.get("expression"))?;
    let low = string_literal(node.get("left"))?;
    let high = string_literal(node.get("right"))?;
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

fn column_of(node: Option<&Json>) -> Option<String> {
    let node = node?;
    (node.get("type")?.as_str()? == "column")
        .then(|| node.get("name")?.as_str().map(str::to_uppercase))
        .flatten()
}

/// Only a non-empty string literal compares as a string: Exasol reads `''` as NULL and compares a
/// VARCHAR against a number numerically.
fn string_literal(node: Option<&Json>) -> Option<String> {
    let node = node?;
    if node.get("type")?.as_str()? != "literal_string" {
        return None;
    }
    let value = node.get("value")?.as_str()?;
    (!value.is_empty()).then(|| value.to_string())
}

#[cfg(test)]
#[path = "partition_predicate_tests.rs"]
mod tests;

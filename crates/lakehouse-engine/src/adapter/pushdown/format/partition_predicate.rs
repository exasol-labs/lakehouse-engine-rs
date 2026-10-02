use arrow::datatypes::{DataType, TimeUnit};
use datafusion::scalar::ScalarValue;
use serde_json::Value as Json;
use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::scan::{non_null_partition_value, partition_scalar};

use super::filter_json::{
    Comparison, between_operands, comparison_operands, in_operands, non_empty_str, operands,
    subject_column,
};

/// A file is kept iff TRUE is reachable at the root; an untranslatable node reaches every truth
/// value, so it can only widen the kept set. A value and a literal compare under the partition
/// column's declared type, each converted as the scan converts a partition value.
pub(super) struct PartitionPredicate {
    root: Node,
}

impl PartitionPredicate {
    /// `declared` types the partition columns (uppercase fold); an undeclared one compares as
    /// `utf8`, the type direct storage declares. `None` keeps every file.
    pub(super) fn from_filter(filter_json: Option<&Json>, declared: &[(String, DataType)]) -> Self {
        let translator = Translator { declared };
        Self {
            root: filter_json.map_or(Node::Opaque, |node| translator.translate(node)),
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
        literal: ScalarValue,
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
            } => partition_value(values, column).map_or(Reachable::ANY, |value| match value {
                None => Reachable::exactly(None),
                Some(value) => typed_ordering(value, literal).map_or(Reachable::ANY, |ordering| {
                    Reachable::exactly(Some(comparison.holds(ordering)))
                }),
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

/// `None` when `column` names no partition key; `Some(None)` when the file's value is NULL, which
/// an empty value is too, as the scan reads it.
fn partition_value<'a>(
    values: &'a BTreeMap<String, Option<String>>,
    column: &str,
) -> Option<Option<&'a str>> {
    values
        .iter()
        .find(|(key, _)| folds_to(key, column))
        .map(|(_, value)| non_null_partition_value(value.as_deref()))
}

fn folds_to(key: &str, column: &str) -> bool {
    key.chars().flat_map(char::to_uppercase).eq(column.chars())
}

/// `None` for a value that does not convert, so its file is kept for the scan's own conversion to
/// fail the query.
fn typed_ordering(value: &str, literal: &ScalarValue) -> Option<Ordering> {
    // A string value converts to itself, so it compares as-is without the conversion's copy.
    if let ScalarValue::Utf8(Some(literal))
    | ScalarValue::LargeUtf8(Some(literal))
    | ScalarValue::Utf8View(Some(literal)) = literal
    {
        return Some(value.cmp(literal.as_str()));
    }
    let value = partition_scalar(value, &literal.data_type()).ok()?;
    if value.is_null() {
        return None;
    }
    value.partial_cmp(literal)
}

struct Translator<'a> {
    declared: &'a [(String, DataType)],
}

impl Translator<'_> {
    fn translate(&self, node: &Json) -> Node {
        let Some(kind) = node.get("type").and_then(Json::as_str) else {
            return Node::Opaque;
        };
        match kind {
            "predicate_and" => self
                .translate_operands(node)
                .map_or(Node::Opaque, Node::And),
            "predicate_or" => self.translate_operands(node).map_or(Node::Opaque, Node::Or),
            "predicate_not" => node.get("expression").map_or(Node::Opaque, |operand| {
                Node::Not(Box::new(self.translate(operand)))
            }),
            "predicate_is_null" => translate_is_null(node).unwrap_or(Node::Opaque),
            "predicate_is_not_null" => {
                translate_is_null(node).map_or(Node::Opaque, |is_null| Node::Not(Box::new(is_null)))
            }
            "predicate_in_constlist" => self.translate_in(node).unwrap_or(Node::Opaque),
            "predicate_between" => self.translate_between(node).unwrap_or(Node::Opaque),
            other => Comparison::from_node_type(other)
                .and_then(|comparison| self.translate_comparison(node, comparison))
                .unwrap_or(Node::Opaque),
        }
    }

    fn translate_operands(&self, node: &Json) -> Option<Vec<Node>> {
        Some(
            operands(node)?
                .iter()
                .map(|operand| self.translate(operand))
                .collect(),
        )
    }

    fn translate_comparison(&self, node: &Json, comparison: Comparison) -> Option<Node> {
        let (column, literal, comparison) = comparison_operands(node, comparison)?;
        let column = column.to_uppercase();
        Some(Node::Compare {
            literal: self.typed_literal(&column, literal)?,
            column,
            comparison,
        })
    }

    fn translate_in(&self, node: &Json) -> Option<Node> {
        let (column, arguments) = in_operands(node)?;
        let column = column.to_uppercase();
        let equalities = arguments
            .iter()
            .map(|argument| {
                Some(Node::Compare {
                    column: column.clone(),
                    comparison: Comparison::Equal,
                    literal: self.typed_literal(&column, argument)?,
                })
            })
            .collect::<Option<Vec<Node>>>()?;
        Some(Node::Or(equalities))
    }

    fn translate_between(&self, node: &Json) -> Option<Node> {
        let (column, low, high) = between_operands(node)?;
        let column = column.to_uppercase();
        let low = self.typed_literal(&column, low?)?;
        let high = self.typed_literal(&column, high?)?;
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

    fn typed_literal(&self, column: &str, literal: &Json) -> Option<ScalarValue> {
        let declared = self
            .declared
            .iter()
            .find(|(name, _)| folds_to(name, column))
            .map_or(DataType::Utf8, |(_, data_type)| data_type.clone());
        literal_under(literal, &declared)
    }
}

fn translate_is_null(node: &Json) -> Option<Node> {
    Some(Node::IsNull {
        column: subject_column(node)?.to_uppercase(),
    })
}

/// Converts only exactly, since a rounded literal could prune a file holding a matching row, and
/// never an empty string literal, which Exasol reads as NULL.
fn literal_under(literal: &Json, declared: &DataType) -> Option<ScalarValue> {
    let kind = literal.get("type")?.as_str()?;
    let value = literal.get("value")?;
    let text = match (kind, declared) {
        ("literal_string", DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            non_empty_str(value)?.to_string()
        }
        ("literal_exactnumeric", integer) if integer.is_integer() => exact_numeral(value, 0)?,
        (
            "literal_exactnumeric",
            DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale),
        ) => exact_numeral(value, *scale)?,
        ("literal_date", DataType::Date32 | DataType::Date64) => non_empty_str(value)?.to_string(),
        ("literal_timestamp", DataType::Timestamp(unit, _)) => exact_timestamp(value, *unit)?,
        ("literal_bool", DataType::Boolean) => value.as_bool()?.to_string(),
        _ => return None,
    };
    ScalarValue::try_from_string(text, declared)
        .ok()
        .filter(|scalar| !scalar.is_null())
}

/// A plain `[+-]digits[.digits]` numeral with at most `scale` fraction digits; the string cast
/// would round a finer one.
fn exact_numeral(value: &Json, scale: i8) -> Option<String> {
    let numeral = match value {
        Json::String(numeral) => numeral.clone(),
        Json::Number(number) => number.to_string(),
        _ => return None,
    };
    let unsigned = numeral.strip_prefix(['+', '-']).unwrap_or(&numeral);
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (unsigned, None),
    };
    let is_digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    let fraction_digits = match fraction {
        None => 0,
        Some(fraction) if is_digits(fraction) => fraction.len(),
        Some(_) => return None,
    };
    (is_digits(whole) && fraction_digits <= usize::try_from(scale).ok()?).then_some(numeral)
}

/// The string cast truncates a fraction finer than `unit`, so such a literal does not convert.
fn exact_timestamp(value: &Json, unit: TimeUnit) -> Option<String> {
    let text = non_empty_str(value)?;
    let fraction = text.rsplit_once('.').map_or("", |(_, fraction)| fraction);
    let unit_digits = match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    };
    (fraction.trim_end_matches('0').len() <= unit_digits).then(|| text.to_string())
}

#[cfg(test)]
#[path = "partition_predicate_tests.rs"]
mod tests;

use arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;
use serde_json::Value as Json;
use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::scan::{non_null_partition_value, partition_scalar};

use super::filter_json::Comparison;
use super::pruning_filter::PruningFilter;

/// A file is kept iff TRUE is reachable at the root; an untranslatable node reaches every truth
/// value, so it can only widen the kept set. A value and a literal compare under the partition
/// column's declared type, each converted as the scan converts a partition value.
pub(super) struct PartitionPredicate {
    root: PruningFilter<String>,
}

impl PartitionPredicate {
    /// `declared` types the partition columns (uppercase fold); an undeclared one compares as
    /// `utf8`, the type direct storage declares. `None` keeps every file.
    pub(super) fn from_filter(filter_json: Option<&Json>, declared: &[(String, DataType)]) -> Self {
        let resolve = |column: &str| {
            let column = column.to_uppercase();
            let declared = declared
                .iter()
                .find(|(name, _)| folds_to(name, &column))
                .map_or(DataType::Utf8, |(_, data_type)| data_type.clone());
            Some((column, Some(declared)))
        };
        Self {
            root: filter_json.map_or(PruningFilter::Opaque, |node| {
                PruningFilter::parse(node, &resolve)
            }),
        }
    }

    /// Column names match partition keys case-insensitively (uppercase fold).
    pub(super) fn keeps(&self, partition_values: &BTreeMap<String, Option<String>>) -> bool {
        reachable(&self.root, partition_values).can_be_true
    }
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

fn reachable(
    filter: &PruningFilter<String>,
    values: &BTreeMap<String, Option<String>>,
) -> Reachable {
    let of_value = |column: &str, truth: &dyn Fn(&str) -> Reachable| {
        partition_value(values, column).map_or(Reachable::ANY, |value| {
            value.map_or(Reachable::exactly(None), truth)
        })
    };
    match filter {
        PruningFilter::Compare {
            column,
            comparison,
            literal,
        } => of_value(column, &|value| compared(value, *comparison, literal)),
        PruningFilter::InList { column, literals } => of_value(column, &|value| {
            literals
                .iter()
                .map(|literal| compared(value, Comparison::Equal, literal))
                .fold(Reachable::exactly(Some(false)), Reachable::or)
        }),
        PruningFilter::NullTest { column, is_null } => partition_value(values, column)
            .map_or(Reachable::ANY, |value| {
                Reachable::exactly(Some(value.is_none() == *is_null))
            }),
        PruningFilter::Like { column, pattern } => of_value(column, &|value| {
            Reachable::exactly(Some(pattern.matches(value)))
        }),
        PruningFilter::And(operands) => operands
            .iter()
            .map(|operand| reachable(operand, values))
            .fold(Reachable::exactly(Some(true)), Reachable::and),
        PruningFilter::Or(operands) => operands
            .iter()
            .map(|operand| reachable(operand, values))
            .fold(Reachable::exactly(Some(false)), Reachable::or),
        PruningFilter::Not(operand) => reachable(operand, values).negated(),
        PruningFilter::Opaque => Reachable::ANY,
    }
}

fn compared(value: &str, comparison: Comparison, literal: &ScalarValue) -> Reachable {
    typed_ordering(value, literal).map_or(Reachable::ANY, |ordering| {
        Reachable::exactly(Some(comparison.holds(ordering)))
    })
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
    // A catalog logs a binary value as a string, whose UTF-8 bytes the reader hex-encodes only
    // after pruning (`hex_binary_partition_values`).
    if let ScalarValue::Binary(Some(literal)) = literal {
        return Some(value.as_bytes().cmp(literal.as_slice()));
    }
    let value = partition_scalar(value, &literal.data_type()).ok()?;
    // `ScalarValue` orders floats totally, which puts -0.0 below 0.0, but SQL compares them equal.
    match (&value, literal) {
        (_, _) if value.is_null() => None,
        (ScalarValue::Float32(Some(value)), ScalarValue::Float32(Some(literal))) => {
            value.partial_cmp(literal)
        }
        (ScalarValue::Float64(Some(value)), ScalarValue::Float64(Some(literal))) => {
            value.partial_cmp(literal)
        }
        _ => value.partial_cmp(literal),
    }
}

#[cfg(test)]
#[path = "partition_predicate_tests.rs"]
mod tests;

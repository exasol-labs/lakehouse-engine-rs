//! The one parse of a pushed filter that every pruning path reads, so Iceberg, Delta, and Hive
//! partitions prune the same shapes. A rewrite of a new shape belongs here, not in a path.

use arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;
use serde_json::Value as Json;

use super::filter_json::{
    Comparison, LikeBound, LikePattern, Translated, between_operands, comparison_operands,
    fold_and, fold_or, in_operands, like_operands, operands, subject_column,
};
use super::literal::exact_literal;

/// A filter as a pruning path reads it. `C` is a column as the path resolved it, and every
/// literal holds the column's type exactly. `Opaque` constrains nothing.
#[derive(Debug)]
pub(super) enum PruningFilter<C> {
    Compare {
        column: C,
        comparison: Comparison,
        literal: ScalarValue,
    },
    /// Never empty.
    InList {
        column: C,
        literals: Vec<ScalarValue>,
    },
    NullTest {
        column: C,
        is_null: bool,
    },
    /// Only over a string column.
    Like {
        column: C,
        pattern: LikePattern,
    },
    /// Never empty.
    And(Vec<Self>),
    /// Never empty.
    Or(Vec<Self>),
    Not(Box<Self>),
    Opaque,
}

impl<C: Clone> PruningFilter<C> {
    /// `resolve` maps a filter column to the path's column and its Arrow type, `None` for a type
    /// the path compares no literal with. A column it does not resolve constrains nothing.
    pub(super) fn parse(
        node: &Json,
        resolve: &impl Fn(&str) -> Option<(C, Option<DataType>)>,
    ) -> Self {
        Self::try_parse(node, resolve).unwrap_or(Self::Opaque)
    }

    fn try_parse(
        node: &Json,
        resolve: &impl Fn(&str) -> Option<(C, Option<DataType>)>,
    ) -> Option<Self> {
        let kind = node.get("type")?.as_str()?;
        if let Some(comparison) = Comparison::from_node_type(kind) {
            let (column, literal, comparison) = comparison_operands(node, comparison)?;
            let (column, data_type) = resolve(column)?;
            return Some(Self::Compare {
                literal: exact_literal(literal, &data_type?)?,
                column,
                comparison,
            });
        }
        let parse_all = |nodes: &[Json]| -> Vec<Self> {
            nodes
                .iter()
                .map(|node| Self::parse(node, resolve))
                .collect()
        };
        Some(match kind {
            "predicate_and" => Self::And(parse_all(operands(node)?)),
            "predicate_or" => Self::Or(parse_all(operands(node)?)),
            "predicate_not" => Self::Not(Box::new(Self::parse(node.get("expression")?, resolve))),
            "predicate_is_null" | "predicate_is_not_null" => Self::NullTest {
                column: resolve(subject_column(node)?)?.0,
                is_null: kind == "predicate_is_null",
            },
            "predicate_in_constlist" => {
                let (column, arguments) = in_operands(node)?;
                let (column, data_type) = resolve(column)?;
                let data_type = data_type?;
                Self::InList {
                    literals: arguments
                        .iter()
                        .map(|argument| exact_literal(argument, &data_type))
                        .collect::<Option<_>>()?,
                    column,
                }
            }
            "predicate_between" => {
                let (column, low, high) = between_operands(node)?;
                let (column, data_type) = resolve(column)?;
                let data_type = data_type?;
                let convert = |literal: Option<&Json>| exact_literal(literal?, &data_type);
                let (low, high) = (convert(low), convert(high));
                // Exasol compares a string column with mixed-kind bounds as numbers, string bound
                // too. Otherwise either bound alone is implied, so an unconvertible one is opaque.
                if data_type.is_string() && (low.is_none() || high.is_none()) {
                    return None;
                }
                let bound = |literal: Option<ScalarValue>, comparison| {
                    literal.map_or(Self::Opaque, |literal| Self::Compare {
                        column: column.clone(),
                        comparison,
                        literal,
                    })
                };
                Self::And(vec![
                    bound(low, Comparison::GreaterEqual),
                    bound(high, Comparison::LessEqual),
                ])
            }
            "predicate_like" => {
                let (column, pattern) = like_operands(node)?;
                let (column, data_type) = resolve(column)?;
                data_type?.is_string().then_some(())?;
                Self::Like { column, pattern }
            }
            _ => return None,
        })
    }
}

/// A table format whose pruning evaluates a predicate against per-file statistics.
pub(super) trait StatisticsTarget {
    type Column;
    type Predicate;

    /// `None` when the format cannot hold the literal, which constrains nothing.
    fn compare(
        &self,
        column: &Self::Column,
        comparison: Comparison,
        literal: &ScalarValue,
    ) -> Option<Self::Predicate>;
    /// `None` when the format cannot hold a literal, which constrains nothing.
    fn in_list(&self, column: &Self::Column, literals: &[ScalarValue]) -> Option<Self::Predicate>;
    fn null_test(&self, column: &Self::Column, is_null: bool) -> Self::Predicate;
    /// True exactly for the values that start with `prefix`.
    fn starts_with(&self, column: &Self::Column, prefix: String) -> Self::Predicate;
    /// Called with at least one predicate.
    fn and(predicates: Vec<Self::Predicate>) -> Self::Predicate;
    /// Called with at least one predicate.
    fn or(predicates: Vec<Self::Predicate>) -> Self::Predicate;
    fn not(predicate: Self::Predicate) -> Self::Predicate;
}

impl<C> PruningFilter<C> {
    /// Every emitted node is implied by the node it translates. `None` means no constraint: the
    /// caller must keep every file, never none.
    pub(super) fn to_statistics<T: StatisticsTarget<Column = C>>(
        &self,
        target: &T,
    ) -> Option<Translated<T::Predicate>> {
        match self {
            Self::Compare {
                column,
                comparison,
                literal,
            } => target
                .compare(column, *comparison, literal)
                .map(Translated::exact),
            Self::InList { column, literals } => {
                target.in_list(column, literals).map(Translated::exact)
            }
            Self::NullTest { column, is_null } => {
                Some(Translated::exact(target.null_test(column, *is_null)))
            }
            Self::Like { column, pattern } => Some(match pattern.bound()? {
                LikeBound::Equal(value) => Translated::exact(target.compare(
                    column,
                    Comparison::Equal,
                    &ScalarValue::Utf8(Some(value)),
                )?),
                LikeBound::Prefix { prefix, exact } => Translated {
                    predicate: target.starts_with(column, prefix),
                    exact,
                },
            }),
            Self::And(operands) => fold_and(
                operands.iter().map(|operand| operand.to_statistics(target)),
                T::and,
            ),
            Self::Or(operands) => fold_or(
                operands.iter().map(|operand| operand.to_statistics(target)),
                T::or,
            ),
            Self::Not(operand) => operand.to_statistics(target)?.negated(T::not),
            Self::Opaque => None,
        }
    }
}

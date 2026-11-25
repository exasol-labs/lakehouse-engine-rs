//! The one parse of a pushed filter that every pruning path reads, so Iceberg, Delta, and Hive
//! partitions prune the same shapes. A rewrite of a new shape belongs here, not in a path.

use arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;
use vs_expression::{CompareOp, Expr};

use super::filter_operands::{
    LikeBound, LikePattern, Translated, column_name, comparison_operands, fold_and, fold_or,
    like_operands, operands,
};
use super::literal::exact_literal;

/// A filter as a pruning path reads it. `C` is a column as the path resolved it, and every
/// literal holds the column's type exactly. `Opaque` constrains nothing.
#[derive(Debug)]
pub(super) enum PruningFilter<C> {
    Compare {
        column: C,
        comparison: CompareOp,
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
        node: &Expr,
        resolve: &impl Fn(&str) -> Option<(C, Option<DataType>)>,
    ) -> Self {
        Self::try_parse(node, resolve).unwrap_or(Self::Opaque)
    }

    fn try_parse(
        node: &Expr,
        resolve: &impl Fn(&str) -> Option<(C, Option<DataType>)>,
    ) -> Option<Self> {
        let parse_all = |nodes: &[Expr]| -> Vec<Self> {
            nodes
                .iter()
                .map(|node| Self::parse(node, resolve))
                .collect()
        };
        Some(match node {
            Expr::Compare { op, left, right } => {
                let (column, literal, comparison) = comparison_operands(left, right, *op)?;
                let (column, data_type) = resolve(column)?;
                Self::Compare {
                    literal: exact_literal(literal, &data_type?)?,
                    column,
                    comparison,
                }
            }
            Expr::And(items) => Self::And(parse_all(operands(items)?)),
            Expr::Or(items) => Self::Or(parse_all(operands(items)?)),
            Expr::Not(inner) => Self::Not(Box::new(Self::parse(inner, resolve))),
            Expr::IsNull(subject) | Expr::IsNotNull(subject) => {
                let (column, data_type) = resolve(column_name(subject)?)?;
                let is_null = matches!(node, Expr::IsNull(_));
                let empty = data_type.as_ref().and_then(empty_value).map(|literal| {
                    let comparison = if is_null {
                        CompareOp::Equal
                    } else {
                        CompareOp::NotEqual
                    };
                    Self::Compare {
                        column: column.clone(),
                        comparison,
                        literal,
                    }
                });
                let null_test = Self::NullTest { column, is_null };
                match empty {
                    None => null_test,
                    Some(empty) if is_null => Self::Or(vec![null_test, empty]),
                    Some(empty) => Self::And(vec![null_test, empty]),
                }
            }
            Expr::InList { expression, values } => {
                let arguments = operands(values)?;
                let (column, data_type) = resolve(column_name(expression)?)?;
                let data_type = data_type?;
                Self::InList {
                    literals: arguments
                        .iter()
                        .map(|argument| exact_literal(argument, &data_type))
                        .collect::<Option<_>>()?,
                    column,
                }
            }
            Expr::Between {
                expression,
                low,
                high,
            } => {
                let (column, data_type) = resolve(column_name(expression)?)?;
                let data_type = data_type?;
                // Exasol compares each bound in its own kind, so either bound alone is implied and
                // an unconvertible one is opaque.
                let bound = |literal: &Expr, comparison| {
                    exact_literal(literal, &data_type).map_or(Self::Opaque, |literal| {
                        Self::Compare {
                            column: column.clone(),
                            comparison,
                            literal,
                        }
                    })
                };
                Self::And(vec![
                    bound(low, CompareOp::GreaterEqual),
                    bound(high, CompareOp::LessEqual),
                ])
            }
            Expr::Like {
                expression,
                pattern,
                escape,
            } => {
                let (column, pattern) = like_operands(expression, pattern, escape.as_deref())?;
                let (column, data_type) = resolve(column)?;
                data_type?.is_string().then_some(())?;
                Self::Like { column, pattern }
            }
            _ => return None,
        })
    }
}

/// The value of a `data_type` column that Exasol reads as NULL although a file's null count
/// leaves it out: the empty string, and the empty value of a variable-length binary column.
fn empty_value(data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Binary => Some(ScalarValue::Binary(Some(Vec::new()))),
        text if text.is_string() => ScalarValue::try_from_string(String::new(), text).ok(),
        _ => None,
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
        comparison: CompareOp,
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
                    CompareOp::Equal,
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

#[cfg(test)]
#[path = "pruning_filter_tests.rs"]
mod tests;

//! The one walk over a filter that builds a statistics pruning predicate, so Iceberg and Delta
//! translate the same shapes. Every emitted node is implied by the filter node it translates.

use serde_json::Value as Json;

use super::filter_json::{
    Comparison, LikeBound, Translated, between_operands, comparison_operands, fold_and, fold_or,
    in_operands, like_operands, operands, subject_column,
};

/// A table format's pruning predicate: how it resolves a filter column, converts a literal, and
/// builds each node. A column or literal it cannot resolve constrains nothing.
pub(super) trait PruningTarget {
    type Predicate;
    type Column: Copy;
    type Literal;

    fn resolve(&self, column: &str) -> Option<Self::Column>;
    fn literal(&self, column: Self::Column, literal: &Json) -> Option<Self::Literal>;
    fn is_string(&self, column: Self::Column) -> bool;
    fn string(&self, value: String) -> Self::Literal;
    fn compare(
        &self,
        column: Self::Column,
        comparison: Comparison,
        literal: Self::Literal,
    ) -> Self::Predicate;
    fn null_test(&self, column: Self::Column, is_null: bool) -> Self::Predicate;
    fn in_list(&self, column: Self::Column, literals: Vec<Self::Literal>) -> Self::Predicate;
    /// True exactly for the values that start with `prefix`.
    fn starts_with(&self, column: Self::Column, prefix: String) -> Self::Predicate;
    /// Called with at least one predicate.
    fn and(predicates: Vec<Self::Predicate>) -> Self::Predicate;
    /// Called with at least one predicate.
    fn or(predicates: Vec<Self::Predicate>) -> Self::Predicate;
    fn not(predicate: Self::Predicate) -> Self::Predicate;
}

/// `None` means no constraint: the caller must keep every file, never none.
pub(super) fn translate<T: PruningTarget>(
    node: &Json,
    target: &T,
) -> Option<Translated<T::Predicate>> {
    let kind = node.get("type")?.as_str()?;
    if let Some(comparison) = Comparison::from_node_type(kind) {
        let (column, literal, comparison) = comparison_operands(node, comparison)?;
        let column = target.resolve(column)?;
        let literal = target.literal(column, literal)?;
        return Some(Translated::exact(
            target.compare(column, comparison, literal),
        ));
    }
    let child = |child: &Json| translate(child, target);
    match kind {
        "predicate_and" => fold_and(operands(node)?.iter().map(child), T::and),
        "predicate_or" => fold_or(operands(node)?.iter().map(child), T::or),
        "predicate_not" => translate(node.get("expression")?, target)?.negated(T::not),
        "predicate_is_null" | "predicate_is_not_null" => {
            let column = target.resolve(subject_column(node)?)?;
            let is_null = kind == "predicate_is_null";
            Some(Translated::exact(target.null_test(column, is_null)))
        }
        "predicate_in_constlist" => {
            let (column, arguments) = in_operands(node)?;
            let column = target.resolve(column)?;
            let literals = arguments
                .iter()
                .map(|argument| target.literal(column, argument))
                .collect::<Option<Vec<_>>>()?;
            Some(Translated::exact(target.in_list(column, literals)))
        }
        "predicate_between" => {
            let (column, low, high) = between_operands(node)?;
            let column = target.resolve(column)?;
            // Either bound alone is implied by BETWEEN, so an untranslatable bound is dropped.
            let bound = |literal: Option<&Json>, comparison| {
                let literal = target.literal(column, literal?)?;
                Some(Translated::exact(
                    target.compare(column, comparison, literal),
                ))
            };
            fold_and(
                [
                    bound(low, Comparison::GreaterEqual),
                    bound(high, Comparison::LessEqual),
                ],
                T::and,
            )
        }
        "predicate_like" => {
            let (column, pattern) = like_operands(node)?;
            let column = target.resolve(column)?;
            target.is_string(column).then_some(())?;
            Some(match pattern.bound()? {
                LikeBound::Equal(value) => Translated::exact(target.compare(
                    column,
                    Comparison::Equal,
                    target.string(value),
                )),
                LikeBound::Prefix { prefix, exact } => Translated {
                    predicate: target.starts_with(column, prefix),
                    exact,
                },
            })
        }
        _ => None,
    }
}

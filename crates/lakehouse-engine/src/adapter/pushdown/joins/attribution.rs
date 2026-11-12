use serde_json::Value as Json;
use std::collections::BTreeSet;

use super::planning::{DetectedJoin, JoinLeaf};
use crate::adapter::pushdown::support::{InvolvedColumns, undeclared_column_error};
use crate::scan::spec::relation_alias;
use exasol_udf_sdk::error::UdfError;
use vs_expression::{ColumnNode, rewrite_table_aliases, walk_column_nodes};

/// The invariant every leg lookup after [`JoinLegs::ensure_columns_declared`] relies on.
const BOUND: &str = "ensure_columns_declared binds every column of the request to one leg";

#[derive(Debug, Clone)]
struct LegKey {
    table_name: String,
    table_alias: Option<String>,
    leg: usize,
}

/// The sole resolver of which join leg (one occurrence of a table in the FROM tree) a
/// `column` node belongs to.
///
/// `tableName` alone collapses a self-join's occurrences into one leg (#361's cross
/// product), so the key is (`tableName`, `tableAlias`), the alias compared verbatim since
/// Exasol does not fold it. SQL makes the pair injective (`FROM T a JOIN T a` is illegal,
/// at most one occurrence is alias-less), so an absent alias is an identity of its own.
/// When a `tableName` names exactly one leg the alias is not consulted: Exasol stamps no
/// `tableAlias` on an unaliased FROM clause.
#[derive(Debug, Clone)]
pub(super) struct JoinLegs {
    keys: Vec<LegKey>,
}

impl JoinLegs {
    /// Private: [`DetectedJoin::legs`] is the only production constructor of a multi-leg
    /// binding, so legs cannot be bound to another request's leaves.
    fn from_leaves(leaves: &[JoinLeaf]) -> Self {
        let keys = leaves
            .iter()
            .enumerate()
            .map(|(leg, leaf)| LegKey {
                table_name: leaf.table_name.clone(),
                table_alias: leaf.table_alias.clone(),
                leg,
            })
            .collect();
        Self { keys }
    }

    /// The request's one table is leg 0: Exasol sends a request with no join for one table.
    pub(super) fn for_single_scan(request: &Json) -> Self {
        let keys = request
            .pointer("/involvedTables/0/name")
            .and_then(Json::as_str)
            .map(|name| LegKey {
                table_name: name.to_string(),
                table_alias: None,
                leg: 0,
            })
            .into_iter()
            .collect();
        Self { keys }
    }

    pub(super) fn leg_alias(&self, leg: usize) -> String {
        relation_alias(leg)
    }

    /// The leg of a column of the request. Panics on a column that [`Self::ensure_columns_declared`]
    /// would have rejected.
    pub(super) fn leg(&self, column: ColumnNode<'_>) -> usize {
        self.leg_of(column).expect(BOUND)
    }

    /// The sole answer to leg identity; no other module reads `tableName` for it. `None` only for
    /// a request Exasol does not send: a column with no `tableName`, a `tableName` the query does
    /// not read, or a self-join `tableAlias` that names no occurrence.
    fn leg_of(&self, column: ColumnNode<'_>) -> Option<usize> {
        let table_name = column.table_name?;
        let matching: Vec<&LegKey> = self
            .keys
            .iter()
            .filter(|key| key.table_name.eq_ignore_ascii_case(table_name))
            .collect();
        let first = matching.first()?;
        if matching.iter().all(|key| key.leg == first.leg) {
            return Some(first.leg);
        }
        matching
            .iter()
            .find(|key| key.table_alias.as_deref() == column.table_alias)
            .map(|key| key.leg)
    }

    fn legs_referenced(&self, expr: &Json) -> BTreeSet<usize> {
        let mut legs = BTreeSet::new();
        walk_column_nodes(expr, &mut |column| {
            legs.insert(self.leg(column));
        });
        legs
    }

    /// The highest referenced leg index: the earliest join point of a left-to-right chain with
    /// every referenced leg in scope. `None` when the tree has no column, so the caller must
    /// apply it where every leg is in scope.
    pub(super) fn attachment_leg(&self, expr: &Json) -> Option<usize> {
        self.legs_referenced(expr).last().copied()
    }

    /// `None` when the conjunct has no column or spans several legs; the caller then applies it
    /// where every leg is in scope. Pruning a single-leg conjunct early is sound for an inner
    /// join: a row failing it can never survive the join.
    pub(super) fn conjunct_leg(&self, conjunct: &Json) -> Option<usize> {
        match *Vec::from_iter(self.legs_referenced(conjunct)) {
            [leg] => Some(leg),
            _ => None,
        }
    }

    /// Overwrites each `column`'s `tableAlias` with its leg's `LHS_T{leg}` alias, since a bare
    /// name is ambiguous once a table occurs on several legs. Apply it once, to a raw request
    /// tree: an `LHS_T{leg}` alias matches no leaf alias, so a self-join's output does not
    /// qualify again.
    pub(super) fn qualify(&self, expr: &Json) -> Json {
        rewrite_table_aliases(expr, &mut |column| self.leg_alias(self.leg(column)))
    }

    /// Fails the request unless every column node of `pushdown` binds one leg and `declared[leg]`
    /// declares it (#615). It runs before any render, so every later leg and type lookup of a
    /// request column binds.
    pub(super) fn ensure_columns_declared(
        &self,
        pushdown: &Json,
        declared: &[&InvolvedColumns<'_>],
    ) -> Result<(), UdfError> {
        let mut failure = None;
        walk_column_nodes(pushdown, &mut |column| {
            if failure.is_some() {
                return;
            }
            let name = column.upper_name();
            failure = match self.leg_of(column) {
                None => Some(unbound_column_error(column)),
                Some(leg) if !declared.get(leg).is_some_and(|table| table.declares(&name)) => {
                    Some(undeclared_column_error(&name))
                }
                Some(_) => None,
            };
        });
        failure.map_or(Ok(()), Err)
    }
}

/// Binding the column to some table anyway could return another table's values.
fn unbound_column_error(column: ColumnNode<'_>) -> UdfError {
    let table = column.table_name.unwrap_or_default();
    let alias = column
        .table_alias
        .map_or_else(|| "no alias".to_string(), |alias| format!("alias {alias}"));
    UdfError::User(format!(
        "the pushdown request names column {} of table {table} with {alias}, which binds no \
         single table of the query",
        column.upper_name()
    ))
}

/// Leg binding is derived here, in the module owning leg identity, so it has one owner.
impl DetectedJoin {
    /// One leg per FROM-tree leaf in left-to-right order, so a self-join's occurrences stay
    /// distinct legs.
    pub(super) fn legs(&self) -> JoinLegs {
        JoinLegs::from_leaves(&self.tables)
    }
}

#[cfg(test)]
#[path = "attribution_tests.rs"]
mod tests;

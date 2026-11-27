use std::collections::BTreeSet;

use super::planning::{DetectedJoin, JoinLeaf};
use crate::adapter::pushdown::request::{AdapterRequest, Clause, PushdownRequest};
use crate::adapter::pushdown::support::InvolvedColumns;
use crate::scan::spec::relation_alias;
use exasol_udf_sdk::error::UdfError;
use vs_expression::{Column, Expr};

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
    pub(super) fn for_single_scan(request: &AdapterRequest) -> Self {
        let keys = request
            .involved_tables()
            .first()
            .map(|table| LegKey {
                table_name: table.name.clone(),
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
    pub(super) fn leg(&self, column: &Column) -> usize {
        self.leg_of(column).expect(BOUND)
    }

    /// The sole answer to leg identity; no other module reads `tableName` for it. `None` only for
    /// a request Exasol does not send: a column with no `tableName`, a `tableName` the query does
    /// not read, or a self-join `tableAlias` that names no occurrence.
    fn leg_of(&self, column: &Column) -> Option<usize> {
        let table_name = column.table_name.as_deref()?;
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
            .find(|key| key.table_alias == column.table_alias)
            .map(|key| key.leg)
    }

    /// The legs whose columns `expr` reads, empty for an expression that reads no column.
    pub(super) fn legs_referenced(&self, expr: &Expr) -> BTreeSet<usize> {
        expr.columns()
            .into_iter()
            .map(|column| self.leg(column))
            .collect()
    }

    /// Overwrites each `column`'s `tableAlias` with its leg's `LHS_T{leg}` alias, since a bare
    /// name is ambiguous once a table occurs on several legs. Apply it once, to a raw request
    /// tree: an `LHS_T{leg}` alias matches no leaf alias, so a self-join's output does not
    /// qualify again.
    pub(super) fn qualify(&self, expr: &Expr) -> Expr {
        expr.with_table_aliases(&mut |column| self.leg_alias(self.leg(column)))
    }

    /// The request with every clause over its FROM qualified by leg, as a broadcast scan renders
    /// them.
    pub(super) fn qualify_request(&self, pushdown: &PushdownRequest) -> PushdownRequest {
        pushdown.map_expressions(
            &[
                Clause::SelectList,
                Clause::Filter,
                Clause::GroupBy,
                Clause::Having,
                Clause::OrderBy,
            ],
            |tree| self.qualify(tree),
        )
    }

    /// Fails the request unless every column node of `pushdown` binds one leg and `declared[leg]`
    /// declares it (#615). It runs before any render, so every later leg and type lookup of a
    /// request column binds.
    pub(super) fn ensure_columns_declared(
        &self,
        pushdown: &PushdownRequest,
        declared: &[&InvolvedColumns<'_>],
    ) -> Result<(), UdfError> {
        let failure = pushdown
            .expressions(&Clause::ALL)
            .into_iter()
            .flat_map(Expr::columns)
            .find_map(|column| {
                let name = column.upper_name();
                match self.leg_of(column) {
                    None => Some(unbound_column_error(column)),
                    Some(leg) if !declared.get(leg).is_some_and(|table| table.declares(&name)) => {
                        Some(undeclared_column_error(&name))
                    }
                    Some(_) => None,
                }
            });
        failure.map_or(Ok(()), Err)
    }
}

/// Binding the column to some table anyway could return another table's values.
fn unbound_column_error(column: &Column) -> UdfError {
    let table = column.table_name.as_deref().unwrap_or_default();
    let alias = column
        .table_alias
        .as_deref()
        .map_or_else(|| "no alias".to_string(), |alias| format!("alias {alias}"));
    UdfError::User(format!(
        "the pushdown request names column {} of table {table} with {alias}, which binds no \
         single table of the query",
        column.upper_name()
    ))
}

/// Exasol declares every column a query reads in `involvedTables`, so this is an adapter bug or
/// a request shape it does not know.
fn undeclared_column_error(name: &str) -> UdfError {
    UdfError::User(format!(
        "the pushdown request names column {name}, which its involved table does not declare"
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

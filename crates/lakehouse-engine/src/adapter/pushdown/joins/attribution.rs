use serde_json::Value as Json;
use std::collections::BTreeSet;
use std::fmt;

use super::planning::{DetectedJoin, JoinLeaf};
use crate::adapter::pushdown::support::{InvolvedColumns, undeclared_column_error};
use crate::scan::spec::relation_alias;
use exasol_udf_sdk::error::UdfError;
use vs_expression::{ColumnNode, rewrite_table_aliases, walk_column_nodes};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnLeg {
    Leg(usize),
    /// No `tableName`, or one no leg declares. A single scan leaves it unqualified, which the
    /// N = 1 wrapper's name collapse relies on; a join fails the request ([`JoinLegs::bind`]).
    NoLeg,
    /// `tableName` names several legs and `tableAlias` matches none; picking one arbitrarily
    /// would return wrong rows.
    Unattributable,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct LegReferences {
    pub legs: BTreeSet<usize>,
    /// Untagged, naming an undeclared table, or unattributable: all mean no single leg can
    /// claim the tree.
    pub has_unattributed: bool,
    /// Distinguishes a column-free expression from one whose columns could not be attributed.
    pub any_column: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnattributableColumn {
    table_name: String,
    table_alias: Option<String>,
    column_name: String,
}

impl UnattributableColumn {
    fn of(column: ColumnNode<'_>) -> Self {
        Self {
            table_name: column.table_name.unwrap_or_default().to_string(),
            table_alias: column.table_alias.map(str::to_string),
            column_name: column.name.to_string(),
        }
    }
}

impl fmt::Display for UnattributableColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let column = &self.column_name;
        match &self.table_alias {
            Some(alias) => write!(
                f,
                "column {column} of table {} with alias {alias}",
                self.table_name
            ),
            None => write!(
                f,
                "column {column} of table {} with no alias",
                self.table_name
            ),
        }
    }
}

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
    leg_count: usize,
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
        Self {
            keys,
            leg_count: leaves.len(),
        }
    }

    /// Every `involvedTables[].name` maps to leg 0, so [`ColumnLeg::Unattributable`] cannot
    /// arise. No `involvedTables` yields no keys, qualifying nothing.
    pub(super) fn for_single_scan(request: &Json) -> Self {
        let keys = request
            .get("involvedTables")
            .and_then(|tables| tables.as_array())
            .map(|tables| {
                tables
                    .iter()
                    .filter_map(|table| table.get("name").and_then(|name| name.as_str()))
                    .map(|name| LegKey {
                        table_name: name.to_string(),
                        table_alias: None,
                        leg: 0,
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self { keys, leg_count: 1 }
    }

    pub(super) fn leg_count(&self) -> usize {
        self.leg_count
    }

    pub(super) fn leg_alias(&self, leg: usize) -> String {
        relation_alias(leg)
    }

    /// The sole answer to leg identity; no other module reads `tableName` for it.
    pub(super) fn resolve_column(&self, column: ColumnNode<'_>) -> ColumnLeg {
        let Some(table_name) = column.table_name else {
            return ColumnLeg::NoLeg;
        };
        let matching: Vec<&LegKey> = self
            .keys
            .iter()
            .filter(|key| key.table_name.eq_ignore_ascii_case(table_name))
            .collect();
        let Some(first) = matching.first() else {
            return ColumnLeg::NoLeg;
        };
        if matching.iter().all(|key| key.leg == first.leg) {
            return ColumnLeg::Leg(first.leg);
        }
        match matching
            .iter()
            .find(|key| key.table_alias.as_deref() == column.table_alias)
        {
            Some(key) => ColumnLeg::Leg(key.leg),
            None => ColumnLeg::Unattributable,
        }
    }

    fn legs_referenced(&self, expr: &Json) -> LegReferences {
        let mut referenced = LegReferences::default();
        walk_column_nodes(expr, &mut |column| {
            referenced.any_column = true;
            match self.resolve_column(column) {
                ColumnLeg::Leg(leg) => {
                    referenced.legs.insert(leg);
                }
                ColumnLeg::NoLeg | ColumnLeg::Unattributable => {
                    referenced.has_unattributed = true;
                }
            }
        });
        referenced
    }

    /// The highest referenced leg index: the earliest join point of a left-to-right chain with
    /// every referenced leg in scope. `None` when the tree has no column or an unattributed one,
    /// so the caller must apply it where every leg is in scope.
    pub(super) fn attachment_leg(&self, expr: &Json) -> Option<usize> {
        let referenced = self.legs_referenced(expr);
        if !referenced.any_column || referenced.has_unattributed {
            return None;
        }
        referenced.legs.into_iter().next_back()
    }

    /// `None` when the conjunct has no column, an unattributed column, or spans several legs;
    /// the caller then applies it where every leg is in scope. Pruning a single-leg conjunct
    /// early is sound for an inner join: a row failing it can never survive the join.
    pub(super) fn conjunct_leg(&self, conjunct: &Json) -> Option<usize> {
        let referenced = self.legs_referenced(conjunct);
        if referenced.has_unattributed || !referenced.any_column || referenced.legs.len() != 1 {
            return None;
        }
        referenced.legs.into_iter().next()
    }

    /// Overwrites each `column`'s `tableAlias` with its leg's `LHS_T{leg}` alias, since a bare
    /// name is ambiguous once a table occurs on several legs. A single scan's untagged column is
    /// left untouched. Apply it once, to a raw request tree: an `LHS_T{leg}` alias matches no
    /// leaf alias, so re-qualifying a self-join's output fails, or binds the other leg when the
    /// query names its occurrences `LHS_T*`. `Err` as [`Self::bind`].
    pub(super) fn qualify(&self, expr: &Json) -> Result<Json, UdfError> {
        rewrite_table_aliases(expr, &mut |column| {
            Ok(self.bind(column)?.map(|leg| self.leg_alias(leg)))
        })
    }

    /// Fails the request unless every column node of `pushdown` binds one leg and `declared[leg]`
    /// declares it (#615). It runs before any render, so no later lookup or [`Self::qualify`] of
    /// a request column can miss.
    pub(crate) fn ensure_columns_declared(
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
            match self.bind(column) {
                Err(error) => failure = Some(error),
                Ok(leg)
                    if !declared
                        .get(leg.unwrap_or(0))
                        .is_some_and(|involved| involved.declares(&name)) =>
                {
                    failure = Some(undeclared_column_error(&name));
                }
                Ok(_) => {}
            }
        });
        failure.map_or(Ok(()), Err)
    }

    /// The one rule for which leg a column binds. `Ok(None)` is a single scan's column that
    /// [`Self::resolve_column`] places on no leg: the scan has one table, so it binds that one,
    /// left unqualified. In a join such a column, or an unattributable one, fails the request,
    /// since binding it to either leg could return the other leg's values.
    pub(super) fn bind(&self, column: ColumnNode<'_>) -> Result<Option<usize>, UdfError> {
        match self.resolve_column(column) {
            ColumnLeg::Leg(leg) => Ok(Some(leg)),
            ColumnLeg::NoLeg if self.leg_count == 1 => Ok(None),
            ColumnLeg::NoLeg if column.table_name.is_none_or(str::is_empty) => {
                Err(UdfError::User(format!(
                    "the pushdown request names column {} with no table, so it binds no table \
                     of the join",
                    column.upper_name()
                )))
            }
            ColumnLeg::NoLeg | ColumnLeg::Unattributable => Err(UdfError::User(format!(
                "the pushdown request names {}, which binds no single table of the query",
                UnattributableColumn::of(column)
            ))),
        }
    }
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

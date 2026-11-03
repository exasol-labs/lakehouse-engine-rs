use serde_json::Value as Json;
use std::collections::BTreeSet;
use std::fmt;

use super::planning::{DetectedJoin, JoinLeaf};
use crate::scan::spec::relation_alias;
use vs_expression::{ColumnNode, rewrite_table_aliases, walk_column_nodes};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnLeg {
    Leg(usize),
    /// No `tableName`, or one no leg declares. Left unqualified, which the N = 1 wrapper's
    /// name collapse relies on.
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
pub(super) struct UnattributableColumn {
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

    /// [`Self::qualify`] for a path that has no unqualified rendering to fall back on: a
    /// [`ColumnLeg::NoLeg`] column fails like an [`ColumnLeg::Unattributable`] one, since
    /// binding it to either leg could return the other leg's values. Like [`Self::qualify`], it
    /// takes a raw request tree, never its own output.
    pub(super) fn qualify_every_column(&self, expr: &Json) -> Result<Json, UnattributableColumn> {
        self.qualify_tree(expr, true)
    }

    /// Overwrites each `column`'s `tableAlias` with its leg's `LHS_T{leg}` alias, since a bare
    /// name is ambiguous once a table occurs on several legs. [`ColumnLeg::NoLeg`] columns are
    /// left untouched. Apply it once, to a raw request tree: an `LHS_T{leg}` alias matches no
    /// leaf alias, so re-qualifying a self-join's output fails as unattributable, or binds the
    /// other leg when the query names its occurrences `LHS_T*`.
    pub(super) fn qualify(&self, expr: &Json) -> Result<Json, UnattributableColumn> {
        self.qualify_tree(expr, false)
    }

    /// `strict` turns a [`ColumnLeg::NoLeg`] column into an error instead of leaving it as is.
    fn qualify_tree(&self, expr: &Json, strict: bool) -> Result<Json, UnattributableColumn> {
        rewrite_table_aliases(expr, &mut |column| match self.resolve_column(column) {
            ColumnLeg::Leg(leg) => Ok(Some(self.leg_alias(leg))),
            ColumnLeg::NoLeg if !strict => Ok(column.table_alias.map(str::to_string)),
            ColumnLeg::NoLeg | ColumnLeg::Unattributable => Err(UnattributableColumn::of(column)),
        })
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

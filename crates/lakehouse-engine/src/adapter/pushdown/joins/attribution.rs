use serde_json::Value as Json;
use std::collections::BTreeSet;
use std::fmt;

use super::super::support::walk_column_nodes;
use super::planning::{DetectedJoin, JoinLeaf};

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
    column_name: Option<String>,
}

impl UnattributableColumn {
    fn of(column: &serde_json::Map<String, Json>) -> Self {
        let text = |key: &str| {
            column
                .get(key)
                .and_then(|value| value.as_str())
                .map(str::to_string)
        };
        Self {
            table_name: text("tableName").unwrap_or_default(),
            table_alias: text("tableAlias"),
            column_name: text("name"),
        }
    }
}

impl fmt::Display for UnattributableColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let column = self.column_name.as_deref().unwrap_or("<unnamed>");
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
        format!("LHS_T{leg}")
    }

    /// The sole answer to leg identity; no other module reads `tableName` for it.
    pub(super) fn resolve_column(&self, column: &serde_json::Map<String, Json>) -> ColumnLeg {
        let Some(table_name) = column.get("tableName").and_then(|name| name.as_str()) else {
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
        let alias = column.get("tableAlias").and_then(|alias| alias.as_str());
        match matching
            .iter()
            .find(|key| key.table_alias.as_deref() == alias)
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
    /// name is ambiguous once a table occurs on several legs. [`ColumnLeg::NoLeg`] columns are
    /// left untouched.
    pub(super) fn qualify(&self, expr: &Json) -> Result<Json, UnattributableColumn> {
        match expr {
            Json::Object(map) => {
                let mut out = serde_json::Map::with_capacity(map.len() + 1);
                for (key, value) in map {
                    out.insert(key.clone(), self.qualify(value)?);
                }
                if map.get("type").and_then(|node| node.as_str()) == Some("column") {
                    match self.resolve_column(map) {
                        ColumnLeg::Leg(leg) => {
                            out.insert("tableAlias".to_string(), Json::String(self.leg_alias(leg)));
                        }
                        ColumnLeg::NoLeg => {}
                        ColumnLeg::Unattributable => return Err(UnattributableColumn::of(map)),
                    }
                }
                Ok(Json::Object(out))
            }
            Json::Array(items) => items
                .iter()
                .map(|item| self.qualify(item))
                .collect::<Result<Vec<Json>, UnattributableColumn>>()
                .map(Json::Array),
            other => Ok(other.clone()),
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

use serde_json::Value as Json;
use std::collections::BTreeSet;
use std::fmt;

use super::super::support::walk_column_nodes;
use super::planning::{DetectedJoin, JoinLeaf};

/// Which leg a `column` node belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnLeg {
    /// The reference belongs to this leg.
    Leg(usize),
    /// The reference names no leg of this binding: it carries no `tableName`, or one
    /// no leg declares. Left unqualified, which is what the N = 1 wrapper's
    /// deliberate name collapse relies on.
    NoLeg,
    /// The reference's `tableName` names two or more legs and its `tableAlias`
    /// matches none of them, so no leg can be chosen. Fails loudly rather than
    /// resolving — an arbitrary leg is the wrong-rows failure this type removes.
    Unattributable,
}

/// The legs an expression tree references.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct LegReferences {
    /// Every leg a `column` of the tree resolved to.
    pub legs: BTreeSet<usize>,
    /// Whether any `column` resolved to no leg at all — untagged, naming a table no
    /// leg declares, or unattributable. All three mean the same thing to a leg-local
    /// decision: the tree cannot be claimed by one leg.
    pub has_unattributed: bool,
    /// Whether the tree references any `column` node at all — a column-free
    /// expression is distinct from one whose columns could not be attributed.
    pub any_column: bool,
}

/// A `column` reference no leg key matches, named well enough for a client-facing
/// error to say which reference could not be placed.
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

/// The sole resolver of which JOIN LEG — one OCCURRENCE of a table in the FROM tree —
/// a pushdown `column` node belongs to.
///
/// `tableName` is the wrong currency for that question: it names a TABLE, and table
/// and occurrence coincide only while no table appears twice. Keying on it collapses
/// a self-join's occurrences into one leg, which renders every reference against the
/// same subquery — issue #361's cross product. The leg key is therefore the PAIR
/// (`tableName`, `tableAlias`), matched against the FROM-tree leaves'
/// (`table_name`, `table_alias`), with the alias compared verbatim because Exasol
/// applies no case folding to it on either side.
///
/// The pair is injective by SQL's own rules: within one `tableName` two occurrences
/// cannot share an alias (`FROM T a JOIN T a` is illegal) and at most one may be
/// alias-less (`FROM T JOIN T` is an ambiguous reference and is rejected). An absent
/// alias is thus a leg identity of its own, not a missing value, and no alias
/// sorting, occurrence counting, or positional guess is needed. Where a `tableName`
/// names exactly ONE leg the alias is never consulted at all — Exasol stamps no
/// `tableAlias` on an unaliased FROM clause, so requiring one would break every
/// unaliased join.
#[derive(Debug, Clone)]
pub(super) struct JoinLegs {
    keys: Vec<LegKey>,
    leg_count: usize,
}

impl JoinLegs {
    /// Private on purpose: [`DetectedJoin::legs`] is the ONLY production constructor
    /// of a multi-leg binding, so no caller can bind legs to leaves of a different
    /// request or invent leaves of its own.
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

    /// Builds a binding for the N = 1 (no join) case: every `involvedTables[].name`
    /// in the pushdown request maps onto the single leg 0, so `resolve_column`'s
    /// alias branch is unreachable here and [`ColumnLeg::Unattributable`] can never
    /// arise — there is only one leg to attribute to. An absent or empty
    /// `involvedTables` yields a binding with no keys at all, which qualifies
    /// nothing (every column resolves to [`ColumnLeg::NoLeg`]).
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

    /// The number of legs in this binding, one per FROM-tree leaf of the join it
    /// was derived from.
    pub(super) fn leg_count(&self) -> usize {
        self.leg_count
    }

    /// The `LHS_T{leg}` alias this binding renders for the given leg's fan-out
    /// subquery.
    pub(super) fn leg_alias(&self, leg: usize) -> String {
        format!("LHS_T{leg}")
    }

    /// The leg one `column` node belongs to — the SOLE answer to that question, and
    /// the reason no other module reads `tableName` to decide leg identity.
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

    /// The leg whose join point an expression tree attaches to: the HIGHEST leg index
    /// it references, which is the EARLIEST join point of a left-to-right chain at
    /// which every leg the tree references is already in scope.
    ///
    /// `None` when the tree references no `column` at all, or when any of its columns
    /// could not be attributed to a leg — neither can be placed at a join point, so
    /// the caller must apply it where every leg is in scope instead. This is the sole
    /// answer to "where does this condition belong", so no caller reads the leg set,
    /// the unattributed flag, or the column-free flag itself.
    pub(super) fn attachment_leg(&self, expr: &Json) -> Option<usize> {
        let referenced = self.legs_referenced(expr);
        if !referenced.any_column || referenced.has_unattributed {
            return None;
        }
        referenced.legs.into_iter().next_back()
    }

    /// The single leg a conjunct is local to, or `None` when it cannot be pruned to
    /// one leg: the conjunct references no column at all; one of its columns
    /// resolved to no leg ([`ColumnLeg::NoLeg`] — untagged, or naming a table this
    /// binding does not declare); one of its columns could not be attributed to any
    /// leg ([`ColumnLeg::Unattributable`]); or its columns span more than one leg.
    /// In every `None` case the caller must apply the conjunct where every leg is
    /// in scope instead of pruning early.
    ///
    /// Pruning IS sound for an inner join: a row on the referenced leg that fails a
    /// conjunct mentioning only that leg can never survive the join anyway, so
    /// filtering it out of that leg's fan-out before the join changes no downstream
    /// row.
    pub(super) fn conjunct_leg(&self, conjunct: &Json) -> Option<usize> {
        let referenced = self.legs_referenced(conjunct);
        if referenced.has_unattributed || !referenced.any_column || referenced.legs.len() != 1 {
            return None;
        }
        referenced.legs.into_iter().next()
    }

    /// Rewrites every `column` node in `expr` to carry this binding's `tableAlias`
    /// for the leg it resolves to, overwriting any `tableAlias` the request already
    /// supplied. A bare column name is ambiguous once the same table occurs on more
    /// than one leg; qualifying it against its own leg's `LHS_T{leg}` fan-out
    /// subquery is unambiguous because each leg is rendered as its own subquery. A
    /// column that resolves to [`ColumnLeg::NoLeg`] is left untouched. Fails with
    /// [`UnattributableColumn`] the first time a column resolves to
    /// [`ColumnLeg::Unattributable`].
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

/// The leg binding of a detected join is derived HERE — in the module that owns leg
/// identity — rather than at each render site, so the derivation from a join's leaves
/// has exactly one owner.
impl DetectedJoin {
    /// This join's leg binding: one leg per FROM-tree leaf, in the tree's own
    /// left-to-right order, so a self-join's two occurrences stay two legs.
    ///
    /// The ONLY way to obtain a multi-leg [`JoinLegs`] (the N = 1 wrapper's binding
    /// comes from [`JoinLegs::for_single_scan`]): a caller can neither bind a request's
    /// references against another request's legs nor invent leaves of its own.
    pub(super) fn legs(&self) -> JoinLegs {
        JoinLegs::from_leaves(&self.tables)
    }
}

#[cfg(test)]
#[path = "attribution_tests.rs"]
mod tests;

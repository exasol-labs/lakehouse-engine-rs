use crate::types::exasol_type::ExasolType;
use std::cmp::Reverse;
use std::collections::{BTreeSet, HashSet};
use vs_expression::{CompareOp, Expr};

use super::super::request::OutputContract;
use super::super::support::ColumnTypes;
use super::attribution::JoinLegs;
use super::condition_filters::join_key_legs;
use super::plan::{JoinPlan, LegGroup, exasol_runs_tail};
use super::rendering::{fan_out_holds, leg_columns_named_in, place_conditions};

/// One top-level conjunct of the WHERE, and whether the scan renders it.
pub(super) struct WhereConjunct<'a> {
    pub tree: &'a Expr,
    pub renders: bool,
}

/// What the grouping judges, per leg and for the request. Nothing rendered: a render decline is
/// the renderer's, and it takes the next pair.
pub(super) struct JoinGraph<'a> {
    pub legs: &'a JoinLegs,
    /// Per leg, its file bytes after its own pruning.
    pub bytes: &'a [u64],
    /// Per leg, its declared columns.
    pub columns: &'a [&'a ColumnTypes],
    /// Every join condition, its filters already moved to WHERE.
    pub conditions: &'a [Expr],
    pub conjuncts: &'a [WhereConjunct<'a>],
    /// The select list, `GROUP BY`, `HAVING`, and `ORDER BY`.
    pub above: &'a [&'a Expr],
    pub output: &'a OutputContract,
    /// Exasol postprocessing: the request aggregates, groups, or carries a HAVING.
    pub aggregates: bool,
    pub max_bytes: u64,
}

/// Groups the legs: the eligible pairs that a join key joins, as broadcast fan-outs, best first.
/// The renderer broadcasts the first that renders.
pub(super) fn group_legs(graph: &JoinGraph<'_>) -> Vec<LegGroup> {
    let mut ranked: Vec<LegGroup> = candidate_pairs(graph)
        .into_iter()
        .map(|(a, b)| pair_roles(graph, a, b))
        .filter(|pair| eligible(graph, pair))
        .collect();
    ranked.sort_by_key(|pair| {
        let (fact, dimension) = (pair.sharded, pair.broadcast[0]);
        (
            Reverse(has_own_filter(graph, dimension)),
            Reverse(graph.bytes[fact]),
            graph.bytes[dimension],
            pair.legs(),
        )
    });
    ranked
}

fn candidate_pairs(graph: &JoinGraph<'_>) -> BTreeSet<(usize, usize)> {
    graph
        .conditions
        .iter()
        .flat_map(Expr::conjuncts)
        .filter_map(|conjunct| join_key_legs(conjunct, graph.legs))
        .collect()
}

/// The smaller leg by bytes is the dimension. On a tie the first leg in FROM-tree order is, so
/// the choice of the sharded leg is deterministic.
fn pair_roles(graph: &JoinGraph<'_>, a: usize, b: usize) -> LegGroup {
    match graph.bytes[a] <= graph.bytes[b] {
        true => LegGroup::pair(b, a),
        false => LegGroup::pair(a, b),
    }
}

fn has_own_filter(graph: &JoinGraph<'_>, leg: usize) -> bool {
    graph.conjuncts.iter().any(|conjunct| {
        conjunct.renders && fan_out_holds(graph.legs, &LegGroup::plain(leg), conjunct.tree)
    })
}

/// The dimension is within `JOIN_BROADCAST_MAX_BYTES`, and every column Exasol reads of it over
/// the pair is width-bounded when Exasol stores the pair's rows before it aggregates them, joins
/// them with another leg, or filters them.
fn eligible(graph: &JoinGraph<'_>, pair: &LegGroup) -> bool {
    let dimension = pair.broadcast[0];
    let plan = JoinPlan::broadcasting(graph.bytes.len(), pair);
    let declined = graph.conjuncts.iter().any(|conjunct| !conjunct.renders);
    graph.bytes[dimension] <= graph.max_bytes
        && (!exasol_runs_tail(plan.fan_outs().len(), graph.aggregates, declined)
            || width_bounded(graph, dimension, &dimension_reads(graph, &plan, dimension)))
}

/// The dimension columns Exasol reads over `plan`: those of the clauses above it, of the join
/// conditions the pair's scan does not run, and of the conjuncts left for Exasol's WHERE.
fn dimension_reads(graph: &JoinGraph<'_>, plan: &JoinPlan, dimension: usize) -> ColumnTypes {
    if *graph.output == OutputContract::WholeRow {
        return graph.columns[dimension].narrowed(|_| true);
    }
    let filter = Expr::conjoin(graph.conjuncts.iter().map(|c| c.tree.clone()).collect());
    let placed = place_conditions(filter.as_ref(), graph.conditions, graph.legs, plan);
    let declined = graph
        .conjuncts
        .iter()
        .filter(|conjunct| !conjunct.renders)
        .map(|conjunct| conjunct.tree);
    leg_columns_named_in(
        graph
            .above
            .iter()
            .copied()
            .chain(placed.on.iter().flatten())
            .chain(&placed.residual)
            .chain(declined),
        graph.legs,
        dimension,
        graph.columns[dimension],
    )
}

/// The fan-out repeats each read column once per joined row. Only a fixed-width type or a
/// literal-bounding WHERE conjunct keeps such a column narrow.
fn width_bounded(graph: &JoinGraph<'_>, dimension: usize, read: &ColumnTypes) -> bool {
    let bounded_text = literal_bounded_columns(graph, dimension);
    read.iter().all(|(column, column_type)| {
        matches!(
            column_type.exa_type,
            ExasolType::Boolean
                | ExasolType::Decimal(_)
                | ExasolType::DoublePrecision
                | ExasolType::Date
                | ExasolType::Timestamp { .. }
        ) || (column_type.exa_type.is_character() && bounded_text.contains(&column.name))
    })
}

/// Leg `leg`'s columns that a top-level WHERE conjunct the scan renders, `col = 'x'` or
/// `col IN ('x', 'y')`, limits to string literals. Exasol applies a declined one only after the
/// fan-out has emitted the column, so it bounds nothing.
fn literal_bounded_columns(graph: &JoinGraph<'_>, leg: usize) -> HashSet<String> {
    let is_literal = |node: &Expr| node.as_string_literal().is_some();
    let leg_column = |node: &Expr| {
        node.as_column()
            .filter(|column| graph.legs.leg(column) == leg)
            .map(|column| column.ascii_upper_name())
    };
    graph
        .conjuncts
        .iter()
        .filter(|conjunct| conjunct.renders)
        .filter_map(|conjunct| match conjunct.tree {
            Expr::Compare {
                op: CompareOp::Equal,
                left,
                right,
            } => {
                if is_literal(right) {
                    leg_column(left)
                } else if is_literal(left) {
                    leg_column(right)
                } else {
                    None
                }
            }
            Expr::InList { expression, values } => {
                if values.is_empty() || !values.iter().all(is_literal) {
                    return None;
                }
                leg_column(expression)
            }
            _ => None,
        })
        .collect()
}

#[cfg(test)]
#[path = "broadcast_policy_tests.rs"]
mod tests;

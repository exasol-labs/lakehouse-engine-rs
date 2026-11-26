//! Splits each inner join's condition into its join equalities and its filters before join
//! planning, so a filter written in `ON` is planned exactly as the same filter in `WHERE`.
//! Three rules live here and change independently: which conjuncts stay in a condition
//! ([`is_join_equality`]), the move ([`move_condition_filters`]), and the broadcast join-key test
//! ([`join_key_legs`]).

use std::collections::BTreeSet;
use vs_expression::{CompareOp, Expr};

use super::super::request::{FromClause, JoinKind, PushdownRequest};
use super::attribution::JoinLegs;

/// The two legs `conjunct` joins, lower first, when it is a join key: an `=` whose sides each read
/// exactly one leg, different legs, expressions included. A broadcast pair hands DataFusion its
/// keys as hash-join keys.
pub(super) fn join_key_legs(conjunct: &Expr, legs: &JoinLegs) -> Option<(usize, usize)> {
    let (left, right) = equality_side_legs(conjunct, legs)?;
    match (
        Vec::from_iter(left).as_slice(),
        Vec::from_iter(right).as_slice(),
    ) {
        (&[l], &[r]) if l != r => Some((l.min(r), l.max(r))),
        _ => None,
    }
}

/// `pushdown` with every conjunct of an inner join's condition other than a join equality moved
/// into its top-level filter, after the conjuncts the filter already holds. `None` when no
/// condition has a conjunct to move. Every `column` node of `pushdown` must bind a leg of `legs`.
pub(super) fn move_condition_filters(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
) -> Option<PushdownRequest> {
    let mut from = pushdown.from()?.clone();
    let mut moved = Vec::new();
    split_conditions(&mut from, false, legs, &mut moved);
    if moved.is_empty() {
        return None;
    }
    let mut conjuncts: Vec<Expr> = pushdown
        .filter()
        .map(|filter| filter.conjuncts().into_iter().cloned().collect())
        .unwrap_or_default();
    conjuncts.extend(moved);
    let filter = Expr::conjoin(conjuncts);
    debug_assert!(filter.is_some(), "at least one moved conjunct");
    Some(pushdown.with_from_and_filter(from, filter))
}

/// `null_supplying` is set inside an outer join's null-supplying side, where `WHERE` would also
/// drop the outer join's NULL-extended rows, so no condition there moves.
fn split_conditions(
    node: &mut FromClause,
    null_supplying: bool,
    legs: &JoinLegs,
    moved: &mut Vec<Expr>,
) {
    let FromClause::Join {
        kind,
        left,
        right,
        condition,
    } = node
    else {
        return;
    };
    let (left_null_supplying, right_null_supplying) = match kind {
        JoinKind::Inner => (null_supplying, null_supplying),
        JoinKind::LeftOuter => (null_supplying, true),
        JoinKind::RightOuter => (true, null_supplying),
        JoinKind::FullOuter => (true, true),
    };
    split_conditions(left, left_null_supplying, legs, moved);
    split_conditions(right, right_null_supplying, legs, moved);
    if *kind != JoinKind::Inner || null_supplying {
        return;
    }
    let (equalities, filters): (Vec<Expr>, Vec<Expr>) = condition
        .conjuncts()
        .into_iter()
        .cloned()
        .partition(|conjunct| is_join_equality(conjunct, legs));
    // Exasol sends no condition without a join equality; keeping it whole leaves no join empty.
    if equalities.is_empty() || filters.is_empty() {
        return;
    }
    if let Some(equalities) = Expr::conjoin(equalities) {
        *condition = equalities;
    }
    moved.extend(filters);
}

/// The conjunct that stays in its join's condition: an `=` whose sides each read at least one leg
/// and no leg on both sides.
fn is_join_equality(conjunct: &Expr, legs: &JoinLegs) -> bool {
    equality_side_legs(conjunct, legs).is_some_and(|(left, right)| {
        !left.is_empty() && !right.is_empty() && left.is_disjoint(&right)
    })
}

fn equality_side_legs(
    conjunct: &Expr,
    legs: &JoinLegs,
) -> Option<(BTreeSet<usize>, BTreeSet<usize>)> {
    let Expr::Compare {
        op: CompareOp::Equal,
        left,
        right,
    } = conjunct
    else {
        return None;
    };
    Some((legs.legs_referenced(left), legs.legs_referenced(right)))
}

#[cfg(test)]
#[path = "condition_filters_tests.rs"]
mod tests;

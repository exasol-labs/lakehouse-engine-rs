use serde_json::Value as Json;

use super::super::support::{OutputContract, PushdownRequest, aggregation_type, column_node_ref};
use super::super::topn::ParsedSortKey;
use super::attribution::JoinLegs;
use super::planning::{JoinWindowPlan, classify_join_window};

/// One fan-out of a join plan: its scan shards the files of `sharded` and joins every file of
/// each `broadcast` leg into each shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super) struct LegGroup {
    pub sharded: usize,
    pub broadcast: Vec<usize>,
}

impl LegGroup {
    pub(in super::super) fn plain(leg: usize) -> Self {
        Self {
            sharded: leg,
            broadcast: Vec::new(),
        }
    }

    pub(in super::super) fn pair(fact: usize, dimension: usize) -> Self {
        Self {
            sharded: fact,
            broadcast: vec![dimension],
        }
    }

    /// Its legs in FROM-tree order.
    pub(in super::super) fn legs(&self) -> Vec<usize> {
        let mut legs: Vec<usize> = std::iter::once(self.sharded)
            .chain(self.broadcast.iter().copied())
            .collect();
        legs.sort_unstable();
        legs
    }

    pub(in super::super) fn holds(&self, leg: usize) -> bool {
        self.sharded == leg || self.broadcast.contains(&leg)
    }

    pub(in super::super) fn is_plain(&self) -> bool {
        self.broadcast.is_empty()
    }
}

/// Every leg in exactly one fan-out. The fan-outs are in Exasol's join-chain order, each at the
/// position of its lowest leg, so a condition comes into scope no later than in the N-scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super) struct JoinPlan {
    fan_outs: Vec<LegGroup>,
}

impl JoinPlan {
    /// The plan in which no leg is broadcast.
    pub(in super::super) fn n_scan(leg_count: usize) -> Self {
        Self {
            fan_outs: (0..leg_count).map(LegGroup::plain).collect(),
        }
    }

    /// `pair` as one fan-out, every other leg plain.
    pub(in super::super) fn broadcasting(leg_count: usize, pair: &LegGroup) -> Self {
        let first = pair.legs()[0];
        let fan_outs = (0..leg_count)
            .filter_map(|leg| match pair.holds(leg) {
                true if leg == first => Some(pair.clone()),
                true => None,
                false => Some(LegGroup::plain(leg)),
            })
            .collect();
        Self { fan_outs }
    }

    pub(in super::super) fn fan_outs(&self) -> &[LegGroup] {
        &self.fan_outs
    }

    /// The chain position of the fan-out that holds `leg`.
    pub(in super::super) fn position(&self, leg: usize) -> usize {
        self.fan_outs
            .iter()
            .position(|fan_out| fan_out.holds(leg))
            .expect("every leg lies in one fan-out")
    }
}

/// Whether the shards run the request's aggregation, ordering, and limit: the plan is one
/// broadcast fan-out, and Exasol filters and aggregates nothing over it. Otherwise Exasol runs them
/// over the fan-outs.
pub(super) fn tail_runs_in_shards(
    plan: &JoinPlan,
    residual: Option<&Json>,
    pushdown: &PushdownRequest,
) -> bool {
    plan.fan_outs().len() == 1 && residual.is_none() && aggregation_type(pushdown.json()).is_none()
}

/// Whether a two-leg broadcast can serve the request's window, so its pair may be broadcast at
/// all (#675). Exasol renders the window of a join of more legs or of an aggregating request, as
/// [`tail_runs_in_shards`] decides. Otherwise the window must classify, and every sort key must be
/// a column the select list projects, as the shards bind it.
pub(super) fn broadcast_serves_window(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
    leg_count: usize,
) -> bool {
    if leg_count > 2 || aggregation_type(pushdown.json()).is_some() {
        return true;
    }
    let qualified = legs.qualify_request(pushdown);
    let qualified = qualified.json();
    match classify_join_window(qualified) {
        JoinWindowPlan::BroadcastUnservable => false,
        JoinWindowPlan::Unbounded | JoinWindowPlan::BareLimit(_) => true,
        JoinWindowPlan::Ordered { keys, .. } => {
            let projected: Vec<_> = match pushdown.output() {
                OutputContract::WholeRow => return true,
                OutputContract::AnyOneColumn => Vec::new(),
                OutputContract::Items => qualified["selectList"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(column_node_ref)
                    .collect(),
            };
            keys.iter().all(|key| match key {
                ParsedSortKey::Column(key) => projected.contains(&key.column),
                ParsedSortKey::Expression { .. } => false,
            })
        }
    }
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod tests;

use super::super::request::{OutputContract, PushdownRequest};
use super::super::support::column_node_ref;
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

/// Whether Exasol runs the request's aggregation, ordering, and limit over a plan's fan-outs
/// rather than the shards: the plan has several fan-outs, the request aggregates, or Exasol's
/// WHERE applies a conjunct over them.
pub(super) fn exasol_runs_tail(fan_outs: usize, aggregates: bool, exasol_filters: bool) -> bool {
    fan_outs > 1 || aggregates || exasol_filters
}

/// Whether a two-leg broadcast can serve the request's window, so its pair may be broadcast at
/// all (#675). Exasol renders the window when [`exasol_runs_tail`] holds, so `exasol_filters`
/// says whether a WHERE conjunct is left for Exasol. Otherwise the window must classify, and
/// every sort key must be a column the select list projects, as the shards bind it.
pub(super) fn broadcast_serves_window(
    pushdown: &PushdownRequest,
    legs: &JoinLegs,
    leg_count: usize,
    exasol_filters: bool,
) -> bool {
    let aggregates = pushdown.aggregation_type().is_some();
    // A broadcast plan holds one fan-out fewer than legs: the pair shares one.
    if exasol_runs_tail(leg_count - 1, aggregates, exasol_filters) {
        return true;
    }
    let qualified = legs.qualify_request(pushdown);
    match classify_join_window(&qualified) {
        JoinWindowPlan::BroadcastUnservable => false,
        JoinWindowPlan::Unbounded | JoinWindowPlan::BareLimit(_) => true,
        JoinWindowPlan::Ordered { keys, .. } => {
            let projected: Vec<_> = match pushdown.output() {
                OutputContract::WholeRow => return true,
                OutputContract::AnyOneColumn | OutputContract::Items => qualified
                    .select_items()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(column_node_ref)
                    .collect(),
            };
            keys.iter().all(
                |key| matches!(key, ParsedSortKey::Column(key) if projected.contains(&key.column)),
            )
        }
    }
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod tests;

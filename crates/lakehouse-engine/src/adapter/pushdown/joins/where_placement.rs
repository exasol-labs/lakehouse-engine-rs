//! Where each top-level conjunct of a request's WHERE applies, exactly once: in a scan's filter or
//! in the Exasol wrapper's WHERE (`_decision/045`). A wrapper's fan-out emits the columns of the
//! wrapper's part only, and only a constructor here decides that part (#680).

use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::super::context::PushdownContext;
use super::super::request::PushdownRequest;
use super::super::support::ScanTranslator;
use super::attribution::JoinLegs;
use super::planning::ResolvedJoinSide;
use super::rendering::{
    conjoin_filters, cross_leg_residual_filter, leg_local_filter, screen_leg_filter,
};

#[derive(Debug, Clone, PartialEq)]
pub(in super::super) struct WherePlacement {
    /// Per scanned leg, the rendered filter its scan applies.
    scans: Vec<Option<String>>,
    /// The raw conjuncts the wrapper applies, as Exasol sent them.
    wrapper: Option<Json>,
}

impl WherePlacement {
    /// The WHERE of a request over one scan. A filter that renders whole keeps its one rendered
    /// SQL. Otherwise each top-level conjunct renders on its own, so a declined conjunct takes only
    /// itself out of the scan.
    pub(in super::super) fn single_scan(
        scan: ScanTranslator<'_>,
        pushdown: &PushdownRequest,
    ) -> Self {
        let Some(raw) = pushdown.filter() else {
            return Self::one_scan(None, None);
        };
        if let Some(sql) = scan.filter(raw) {
            return Self::one_scan(Some(sql), None);
        }
        match scan.screen_conjuncts(raw, Json::clone) {
            (Some(pushed), Some(declined)) => Self::one_scan(Some(pushed), Some(declined)),
            _ => Self::one_scan(None, Some(raw.clone())),
        }
    }

    /// The WHERE of a request over one fan-out per leg. A conjunct reaches a leg's scan only when
    /// it reads that leg alone and renders against that leg's own columns, because two legs may
    /// declare one name with different types. Every other conjunct is the wrapper's.
    pub(super) fn n_scan(
        pushdown: &PushdownRequest,
        legs: &JoinLegs,
        sides: &[ResolvedJoinSide],
        context: &PushdownContext<'_>,
    ) -> Result<Self, UdfError> {
        let filter = pushdown.filter();
        let mut scans = Vec::with_capacity(sides.len());
        let mut declined = None;
        for (leg, side) in sides.iter().enumerate() {
            let (scan, side_declined) = match filter.and_then(|f| leg_local_filter(f, legs, leg)) {
                Some(side_local) => screen_leg_filter(&side_local, &side.column_types, context)?,
                None => (None, None),
            };
            // Disjoint by attribution, so no conjunct is double-applied.
            declined = conjoin_filters(declined, side_declined);
            scans.push(scan);
        }
        let wrapper = conjoin_filters(
            filter.and_then(|f| cross_leg_residual_filter(f, legs)),
            declined,
        );
        Ok(Self { scans, wrapper })
    }

    /// The wrapper applies the whole WHERE, as it does over a relation no scan feeds.
    pub(in super::super) fn whole(pushdown: &PushdownRequest) -> Self {
        Self {
            scans: Vec::new(),
            wrapper: pushdown.filter().cloned(),
        }
    }

    fn one_scan(scan: Option<String>, wrapper: Option<Json>) -> Self {
        Self {
            scans: vec![scan],
            wrapper,
        }
    }

    /// The filter leg `leg`'s scan applies.
    pub(in super::super) fn scan_filter(&self, leg: usize) -> Option<String> {
        self.scans.get(leg).cloned().flatten()
    }

    /// The conjuncts the wrapper applies itself.
    pub(in super::super) fn wrapper(&self) -> Option<&Json> {
        self.wrapper.as_ref()
    }
}

#[cfg(test)]
#[path = "where_placement_tests.rs"]
pub(super) mod tests;

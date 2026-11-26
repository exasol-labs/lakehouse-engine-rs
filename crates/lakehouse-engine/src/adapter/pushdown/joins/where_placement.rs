//! Where each top-level conjunct of a request's WHERE applies, exactly once: in a scan's filter or
//! in the Exasol wrapper's WHERE (`_decision/045`). A wrapper's fan-out emits the columns of the
//! wrapper's part only, and only a constructor here decides that part (#680).

use exasol_udf_sdk::error::UdfError;
use vs_expression::Expr;

use super::super::context::PushdownContext;
use super::super::request::PushdownRequest;
use super::super::support::ScanTranslator;
use super::attribution::JoinLegs;
use super::plan::JoinPlan;
use super::planning::ResolvedJoinSide;
use super::rendering::{
    ConditionPlacement, conjoin_filters, screen_leg_filter, screen_pair_filter,
};

#[derive(Debug, Clone, PartialEq)]
pub(in super::super) struct WherePlacement {
    /// Per fan-out in chain order, the rendered filter its scan applies.
    scans: Vec<Option<String>>,
    /// The raw conjuncts the wrapper applies, as Exasol sent them.
    wrapper: Option<Expr>,
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
        match scan.screen_conjuncts(raw, Expr::clone) {
            (Some(pushed), Some(declined)) => Self::one_scan(Some(pushed), Some(declined)),
            _ => Self::one_scan(None, Some(raw.clone())),
        }
    }

    /// The WHERE of a request over `plan`'s fan-outs, from where `conditions` places each
    /// conjunct. A fan-out's local conjuncts render against its own legs' columns, because two
    /// legs may declare one name with different types, and each one its scan declines is the
    /// wrapper's, beside the residual no fan-out holds.
    pub(super) fn of_plan(
        conditions: &ConditionPlacement,
        plan: &JoinPlan,
        legs: &JoinLegs,
        sides: &[ResolvedJoinSide],
        context: &PushdownContext<'_>,
    ) -> Result<Self, UdfError> {
        let mut scans = Vec::with_capacity(plan.fan_outs().len());
        let mut declined = None;
        for (fan_out, local) in plan.fan_outs().iter().zip(&conditions.local) {
            let (scan, fan_out_declined) = match local {
                None => (None, None),
                Some(local) if fan_out.is_plain() => {
                    screen_leg_filter(local, &sides[fan_out.sharded].column_types, context)?
                }
                Some(local) => screen_pair_filter(local, legs, sides, context)?,
            };
            // Disjoint by placement, so no conjunct is double-applied.
            declined = conjoin_filters(declined, fan_out_declined);
            scans.push(scan);
        }
        Ok(Self {
            scans,
            wrapper: conjoin_filters(conditions.residual.clone(), declined),
        })
    }

    /// The wrapper applies the whole WHERE, as it does over a relation no scan feeds.
    pub(in super::super) fn whole(pushdown: &PushdownRequest) -> Self {
        Self {
            scans: Vec::new(),
            wrapper: pushdown.filter().cloned(),
        }
    }

    fn one_scan(scan: Option<String>, wrapper: Option<Expr>) -> Self {
        Self {
            scans: vec![scan],
            wrapper,
        }
    }

    /// The filter the scan of the fan-out at chain position `fan_out` applies.
    pub(in super::super) fn scan_filter(&self, fan_out: usize) -> Option<String> {
        self.scans.get(fan_out).cloned().flatten()
    }

    /// The conjuncts the wrapper applies itself.
    pub(in super::super) fn wrapper(&self) -> Option<&Expr> {
        self.wrapper.as_ref()
    }
}

#[cfg(test)]
#[path = "where_placement_tests.rs"]
mod tests;

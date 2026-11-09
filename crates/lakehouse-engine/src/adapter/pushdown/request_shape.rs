//! Shared request-shape classifier: the single routing decision consumed by both
//! the non-empty dispatcher and the empty-result path, so their output shapes are
//! identical by construction (#175).
//!
//! Grouped HAVING/ORDER BY expressibility is a routing predicate: a decline routes
//! to [`RequestShape::GroupByWrapper`], which renders both natively, rather than
//! dropping them (#195) or erroring (#198).

use super::grouped_agg::{
    GroupedOrderBy, MergeScope, build_grouped_order_by_clause, render_having_over_merge,
};
use super::single_group_agg::{SingleGroupItem, SingleGroupPlans, has_distinct};
use super::support::{PushdownRequest, ScanTranslator};
use super::{
    GroupedAggregateDetection, detect_aggregates, detect_group_by_aggregates,
    validate_agg_col_types,
};

#[derive(Debug)]
pub(super) enum RequestShape {
    /// `having` and `order_by` are already rendered over the merge decomposition; a
    /// HAVING or ORDER BY that cannot be expressed there routes to `GroupByWrapper`.
    Grouped {
        detection: GroupedAggregateDetection,
        having: Option<String>,
        order_by: Option<String>,
    },
    /// A GROUP BY request that did not decompose. Routes to the wrapper whose output
    /// columns are the `selectList` items, never a bare row scan (Exasol's positional
    /// column-count check fails with `04000`).
    GroupByWrapper,
    /// Ordinary aggregate column types passed [`validate_agg_col_types`].
    SingleGroupAgg {
        items: Vec<SingleGroupItem>,
        plans: SingleGroupPlans,
        having: Option<String>,
    },
    SingleGroupWrapper,
    RowScan,
}

pub(super) fn classify_request_shape(
    pushdown: &PushdownRequest,
    scan: ScanTranslator<'_>,
) -> RequestShape {
    let pushdown_req = pushdown.json();
    // A SUM over a non-numeric column would produce an opaque UDF error; demote.
    if let Some(detection) = detect_group_by_aggregates(pushdown, scan)
        && validate_agg_col_types(&detection.plans, scan.columns())
    {
        let scope = MergeScope {
            plans: &detection.plans,
            merged: &detection.merged,
            group_keys: &detection.group_keys,
        };
        // The merge wrapper only has `GK_*`/`PARTIAL_*` columns, so an aggregate absent
        // from the select list cannot be sorted on there (#198).
        let order_by = match build_grouped_order_by_clause(pushdown_req, &scope, scan) {
            Some(GroupedOrderBy::Clause(clause)) => Some(clause),
            Some(GroupedOrderBy::Unresolvable) => return RequestShape::GroupByWrapper,
            None => None,
        };
        let having = match pushdown_req.get("having").filter(|h| !h.is_null()) {
            None => None,
            Some(node) => match render_having_over_merge(node, &scope, scan) {
                Some(sql) => Some(sql),
                None => return RequestShape::GroupByWrapper,
            },
        };
        return RequestShape::Grouped {
            detection,
            having,
            order_by,
        };
    }

    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by") {
        return RequestShape::GroupByWrapper;
    }

    // Validated against ordinary plans only: a distinct item is a row-scan fan-out. A nested
    // aggregate without an exact type declines to the wrapper (#476).
    let Some((items, plans)) = detect_aggregates(pushdown, scan).and_then(|items| {
        let plans = SingleGroupPlans::new(&items, pushdown.select_types(), scan)?;
        validate_agg_col_types(&plans.plans, scan.columns()).then_some((items, plans))
    }) else {
        return if pushdown_req.get("aggregationType").and_then(|v| v.as_str())
            == Some("single_group")
        {
            RequestShape::SingleGroupWrapper
        } else {
            RequestShape::RowScan
        };
    };
    let having = match pushdown_req.get("having").filter(|h| !h.is_null()) {
        None => None,
        Some(_) if has_distinct(&items) => return RequestShape::SingleGroupWrapper,
        Some(node) => {
            let scope = MergeScope {
                plans: &plans.plans,
                merged: &plans.merged,
                group_keys: &[],
            };
            match render_having_over_merge(node, &scope, scan) {
                Some(sql) => Some(sql),
                None => return RequestShape::SingleGroupWrapper,
            }
        }
    };
    RequestShape::SingleGroupAgg {
        items,
        plans,
        having,
    }
}

#[cfg(test)]
#[path = "request_shape_tests.rs"]
mod tests;

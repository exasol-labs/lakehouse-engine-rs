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
use super::scalar_over_agg::cast_merge_items;
use super::single_group_agg::{SingleGroupItem, has_distinct, single_group_plan_types};
use super::support::{PushdownRequest, ScanTranslator};
use super::{
    GroupedAggregateDetection, detect_aggregates, detect_group_by_aggregates, ordinary_plans,
    validate_agg_col_types,
};
use crate::types::exasol_type::ExasolType;

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
    /// Ordinary aggregate column types passed [`validate_agg_col_types`]. `plan_types` is
    /// aligned with `ordinary_plans(items)`, every slot exactly typed.
    SingleGroupAgg {
        items: Vec<SingleGroupItem>,
        plan_types: Vec<ExasolType>,
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
        let merged = cast_merge_items(&detection.plans, &detection.plan_types, scan.columns());
        let scope = MergeScope {
            plans: &detection.plans,
            merged: &merged,
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
    let Some((items, plan_types)) = detect_aggregates(pushdown, scan)
        .filter(|it| validate_agg_col_types(&ordinary_plans(it, scan), scan.columns()))
        .and_then(|it| {
            let plan_types = single_group_plan_types(pushdown.select_types(), &it, scan)?;
            Some((it, plan_types))
        })
    else {
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
            let plans = ordinary_plans(&items, scan);
            let merged = cast_merge_items(&plans, &plan_types, scan.columns());
            let scope = MergeScope {
                plans: &plans,
                merged: &merged,
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
        plan_types,
        having,
    }
}

#[cfg(test)]
#[path = "request_shape_tests.rs"]
mod tests;

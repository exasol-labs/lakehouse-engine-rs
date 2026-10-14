//! Shared request-shape classifier: the single routing decision consumed by both
//! the non-empty dispatcher and the empty-result path, so their output shapes are
//! identical by construction (#175).
//!
//! Grouped HAVING/ORDER BY expressibility is a routing predicate: a decline routes
//! to [`RequestShape::GroupByWrapper`], which renders both natively, rather than
//! dropping them (#195) or erroring (#198).

use serde_json::Value as Json;

use super::grouped_agg::{GroupedOrderBy, build_grouped_order_by_clause, render_having_over_merge};
use super::single_group_agg::SingleGroupItem;
use super::{
    GroupedAggregateDetection, detect_aggregates, detect_group_by_aggregates, ordinary_plans,
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
    },
    RowScan,
}

pub(super) fn classify_request_shape(
    pushdown_req: &Json,
    col_types: &[(String, String)],
) -> RequestShape {
    if let Some(detection) = detect_group_by_aggregates(pushdown_req) {
        // A SUM over a non-numeric column would produce an opaque UDF error; demote.
        if validate_agg_col_types(&detection.plans, col_types) {
            // The merge wrapper only has `GK_*`/`PARTIAL_*` columns, so an aggregate absent
            // from the select list cannot be sorted on there (#198).
            let order_by = match build_grouped_order_by_clause(pushdown_req, &detection) {
                Some(GroupedOrderBy::Clause(clause)) => Some(clause),
                Some(GroupedOrderBy::Unresolvable) => return RequestShape::GroupByWrapper,
                None => None,
            };
            match pushdown_req.get("having").filter(|h| !h.is_null()) {
                None => {
                    return RequestShape::Grouped {
                        detection,
                        having: None,
                        order_by,
                    };
                }
                Some(node) => {
                    if let Some(sql) = render_having_over_merge(node, &detection.plans) {
                        return RequestShape::Grouped {
                            detection,
                            having: Some(sql),
                            order_by,
                        };
                    }
                }
            }
        }
    }

    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by") {
        return RequestShape::GroupByWrapper;
    }

    // Validated against ordinary plans only: a distinct item is a row-scan fan-out.
    match detect_aggregates(pushdown_req)
        .filter(|it| validate_agg_col_types(&ordinary_plans(it), col_types))
    {
        Some(items) => RequestShape::SingleGroupAgg { items },
        None => RequestShape::RowScan,
    }
}

#[cfg(test)]
#[path = "request_shape_tests.rs"]
mod tests;

//! Shared request-shape classifier: the single routing decision consumed by both
//! the non-empty dispatcher and the empty-result path, so their output shapes are
//! identical by construction (#175).
//!
//! Grouped HAVING/ORDER BY expressibility is a routing predicate: a decline routes
//! to [`RequestShape::GroupByWrapper`], which renders both natively, rather than
//! dropping them (#195) or erroring (#198).

use crate::scan::spec::{CommonScanSpec, FileEntry, ScanSpec};

use super::context::Shards;
use super::grouped_agg::{
    GroupedOrderBy, MergeScope, build_grouped_aggregate_scan_sql, build_grouped_order_by_clause,
    render_having_over_merge,
};
use super::request::{AggregationType, PushdownRequest};
use super::scalar_over_agg::TypedPlans;
use super::single_group_agg::{
    SingleGroupItem, has_distinct, single_group_merge_select, single_group_plans,
};
use super::support::{AggregateMergeInputs, ScanTranslator, build_scan_driving_sql};
use super::{GroupedAggregateDetection, detect_aggregates, detect_group_by_aggregates};

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
    SingleGroupAgg {
        items: Vec<SingleGroupItem>,
        aggregates: TypedPlans,
        having: Option<String>,
    },
    SingleGroupWrapper,
    RowScan,
}

impl RequestShape {
    /// Whether the shards aggregate and Exasol merges their partial rows
    /// ([`split_aggregate_sql`]).
    pub(super) fn splits(&self) -> bool {
        match self {
            RequestShape::Grouped { .. } => true,
            RequestShape::SingleGroupAgg { items, .. } => !has_distinct(items),
            _ => false,
        }
    }
}

/// The SQL of a request whose aggregation splits: each shard of `fan_out`, one table or a
/// broadcast join, emits partial aggregates, and Exasol merges them and applies HAVING, ORDER BY,
/// and `limit`. `None` when `shape` does not split or its merge select declines, so the caller
/// takes its fallback: a shorter select list fails Exasol's positional validation.
pub(super) fn split_aggregate_sql<E: Clone + Into<FileEntry>>(
    shape: RequestShape,
    pushdown: &PushdownRequest,
    scan: ScanTranslator<'_>,
    fan_out: CommonScanSpec,
    shards: &Shards<E>,
    limit: Option<u64>,
) -> Option<String> {
    match shape {
        RequestShape::Grouped {
            detection,
            having,
            order_by,
        } => {
            let GroupedAggregateDetection {
                group_keys,
                group_key_types,
                aggregates,
                select_items,
            } = detection;
            // Empty `projection` is inert on an aggregate dispatch (#145).
            let spec = ScanSpec {
                common: CommonScanSpec {
                    aggregates: Some(aggregates.plans.clone()),
                    group_keys: Some(group_keys),
                    ..fan_out
                },
                files: vec![],
            };
            // Types come from detection, never a `selectList`-keyed lookup: nested aggregates
            // would misalign it. The merge renders its own ORDER BY before the LIMIT, and the
            // per-shard partial never carries a LIMIT (decision [4]).
            build_grouped_aggregate_scan_sql(
                &spec,
                shards,
                &group_key_types,
                &aggregates,
                &select_items,
                pushdown.select_types(),
                limit,
                pushdown.offset(),
                scan,
                having.as_deref(),
                order_by.as_deref(),
            )
        }
        RequestShape::SingleGroupAgg {
            items,
            aggregates,
            having,
        } => {
            // `None` for a `COUNT(DISTINCT)`, which has no merge expression.
            let merge_select =
                single_group_merge_select(&items, &aggregates, pushdown.select_types(), scan)?;
            let TypedPlans { plans, emits, .. } = aggregates;
            let inputs = AggregateMergeInputs::new(emits, merge_select, limit)?.with_having(having);
            let spec = ScanSpec {
                common: CommonScanSpec {
                    aggregates: Some(plans),
                    ..fan_out
                },
                files: vec![],
            };
            Some(build_scan_driving_sql(
                &spec,
                shards,
                &[],
                limit,
                Some(&inputs),
            ))
        }
        _ => None,
    }
}

pub(super) fn classify_request_shape(
    pushdown: &PushdownRequest,
    scan: ScanTranslator<'_>,
) -> RequestShape {
    if let Some(detection) = detect_group_by_aggregates(pushdown, scan) {
        let scope = MergeScope {
            aggregates: &detection.aggregates,
            group_keys: &detection.group_keys,
        };
        // The merge wrapper only has `GK_*`/`PARTIAL_*` columns, so a sort key over an
        // undecomposable aggregate cannot be sorted on there (#198).
        let order_by = match build_grouped_order_by_clause(pushdown, &scope, scan) {
            Some(GroupedOrderBy::Clause(clause)) => Some(clause),
            Some(GroupedOrderBy::Unresolvable) => return RequestShape::GroupByWrapper,
            None => None,
        };
        let having = match pushdown.having() {
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

    if pushdown.aggregation_type() == Some(AggregationType::GroupBy) {
        return RequestShape::GroupByWrapper;
    }

    // A nested aggregate without an exact type declines to the wrapper (#476).
    let Some((items, aggregates)) = detect_aggregates(pushdown, scan).and_then(|items| {
        let aggregates = single_group_plans(&items, pushdown.select_types(), scan)?;
        Some((items, aggregates))
    }) else {
        return if pushdown.aggregation_type() == Some(AggregationType::SingleGroup) {
            RequestShape::SingleGroupWrapper
        } else {
            RequestShape::RowScan
        };
    };
    let having = match pushdown.having() {
        None => None,
        Some(_) if has_distinct(&items) => return RequestShape::SingleGroupWrapper,
        Some(node) => {
            let scope = MergeScope {
                aggregates: &aggregates,
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
        aggregates,
        having,
    }
}

#[cfg(test)]
#[path = "request_shape_tests.rs"]
mod tests;

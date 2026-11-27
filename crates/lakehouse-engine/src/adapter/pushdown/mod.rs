//! Pushdown planning: resolve the file list once and build the SQL that invokes
//! the scan UDF with an explicit file list.
//!
//! A predicate the adapter cannot translate into the DataFusion scan must be
//! self-applied by the adapter, never omitted: Exasol does not re-apply a delegated
//! capability (`specs/_decision/045`). No credential value may appear in any
//! returned SQL or error message.

#[cfg(test)]
use crate::adapter::catalog_kind::CatalogKind;
#[cfg(test)]
use crate::adapter::connection::ConnectionCreds;
use crate::adapter::permission::PermissionCheck;
use crate::adapter::statement_state::StatementCatalog;
use crate::scan::spec::{CatalogProps, CommonScanSpec, ScanSpec};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

mod binary_filter;
pub(crate) use binary_filter::literal_binary_bytes;

mod request;
pub(crate) use request::AdapterRequest;
use request::Clause;
pub use request::PushdownRequest;

mod support;
pub use support::{
    AggregateMergeInputs, ColumnTypes, FanOut, ProjectedColumn, ScanTranslator,
    build_fan_out_inner, build_scan_driving_sql, shard_count,
};
use support::{
    Emitted, RowProjection, involved_columns, project_columns, projection_items, pushdown_response,
};

mod clock;
pub(crate) use clock::{StatementClock, statement_instant};

mod context;
pub(crate) use context::PushdownContext;
pub use context::{ScanUdfs, Shards};

mod empty_result;
use empty_result::empty_result_sql;

mod shard_paths;

mod format;
pub(crate) use format::declare_columns;
pub use format::{ConnectionStorage, LoadedTable, RefusedColumn, ResolvedScan};

mod scan_catalog;
pub(crate) use scan_catalog::ScanCatalog;

mod scan_resolution;
use scan_resolution::TableScanResolver;

mod refused_columns;
use refused_columns::ensure_no_refused_column_referenced;

mod topn;
use topn::{detect_topn, parse_order_by_keys};

mod single_group_agg;
pub use single_group_agg::{detect_aggregates, single_group_plans};
use single_group_agg::{has_distinct, is_lone_count_distinct};

mod grouped_agg;
pub use grouped_agg::{
    GroupedAggregateDetection, GroupedSelectItem, build_grouped_aggregate_scan_sql,
    detect_group_by_aggregates,
};

mod scalar_over_agg;
pub use scalar_over_agg::TypedPlans;

mod request_shape;
use request_shape::{RequestShape, classify_request_shape, split_aggregate_sql};

mod joins;
// Several re-exports are consumed only by tests and the reachability probe.
#[allow(unused_imports)]
pub(crate) use joins::{
    DetectedJoin, JoinLeaf, JoinShape, RenderedJoinPushdown, ResolvedJoinSide, detect_join,
};
use joins::{
    EmptyRelation, WherePlacement, ensure_columns_declared, plan_join,
    qualified_single_table_fallback_pushdown, single_table_emission,
};

#[cfg(test)]
use crate::scan::spec::{AggKind, AggregatePlan, FileEntry, LogicalField, StorageBackend};
#[cfg(test)]
use support::{DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, scan_storage_for};
#[cfg(test)]
use test_support::scan_filter_sql;

#[cfg(test)]
#[path = "test_support_tests.rs"]
pub(crate) mod test_support;

#[cfg(test)]
#[path = "dispatch_golden_tests.rs"]
mod dispatch_golden;

pub(crate) async fn handle_pushdown(
    request: &AdapterRequest,
    statement: &StatementCatalog,
    permission: &PermissionCheck,
    catalog: &CatalogProps,
    context: &PushdownContext<'_>,
) -> Result<Json, UdfError> {
    let pushdown = context.clock.fold(request.pushdown())?;

    // Join handling must run first: this path resolves only `involvedTables[0]`, so a
    // join falling through would silently scan one table.
    match detect_join(request, &pushdown)? {
        JoinShape::NotAJoin => {}
        JoinShape::Join(join) => {
            return plan_join(request, &pushdown, &join, statement, permission, context).await;
        }
    }

    // Strip every `tableAlias` after the join gate and before any read (#193).
    let pushdown = pushdown.map_expressions(&Clause::ALL, |tree| tree.strip_table_alias());

    let involved = involved_columns(request.involved_tables().first());
    ensure_columns_declared(request, &pushdown, None, &[&involved])?;

    let limit = pushdown.limit();

    // A limit is withheld from any ORDER BY request not matched as a bounded top-N, so
    // a bare LIMIT never precedes an ordering the adapter did not render (decision [4]).
    let has_order_by = !pushdown.order_by().is_empty();

    let resolver =
        TableScanResolver::for_request(statement, permission, &[catalog.table.as_str()], context)
            .await?;
    let (resolved, col_types) = resolver
        .resolve(&catalog.table, pushdown.filter(), &involved)
        .await?;
    let scan = ScanTranslator::over(
        &col_types,
        context.version,
        &pushdown.expressions(&Clause::ALL),
    )?;
    // Before the zero-files early return: a refused column must error, never yield an empty
    // result.
    ensure_no_refused_column_referenced(&pushdown, &resolved.refused_columns)?;
    // Every column the request names, the row scan's projection and sort keys among them.
    let emitted = single_table_emission(
        request,
        &pushdown,
        &WherePlacement::whole(&pushdown),
        &col_types,
    )?;
    let projection = project_columns(&pushdown, scan, &emitted)?;
    let placement = WherePlacement::single_scan(scan, &pushdown);
    let base = context.common_spec(&resolved)?;

    if resolved.files.is_empty() {
        return empty_result_sql(
            &pushdown,
            &pushdown,
            &projection,
            scan,
            &EmptyRelation::single_scan(request, emitted),
            context,
        );
    }

    let shards = context.shards(resolved.files, &resolved.table_root);
    build_dispatch_sql(
        request,
        &pushdown,
        projection,
        &emitted,
        scan,
        placement,
        limit,
        has_order_by,
        &shards,
        base,
        context,
    )
}

/// `emitted` holds every column the request names, which the bare row scan and the lone
/// `COUNT(DISTINCT)` fan-out draw their columns from. The qualified wrapper derives its own from
/// the WHERE it renders.
#[allow(clippy::too_many_arguments)]
fn build_dispatch_sql(
    request: &AdapterRequest,
    pushdown: &PushdownRequest,
    projection: RowProjection,
    emitted: &Emitted,
    scan: ScanTranslator<'_>,
    placement: WherePlacement,
    limit: Option<u64>,
    has_order_by: bool,
    shards: &Shards,
    base: CommonScanSpec,
    context: &PushdownContext<'_>,
) -> Result<Json, UdfError> {
    let fallback = || {
        qualified_single_table_fallback_pushdown(
            request,
            pushdown,
            &base,
            &placement,
            shards,
            scan.columns(),
            context,
        )
    };

    // Ahead of shape routing so it applies before aggregating, grouping, and
    // truncating (`_decision/045`).
    if placement.wrapper().is_some() {
        return fallback();
    }
    let filter = placement.scan_filter(0);

    let split = |shape: RequestShape, shard_limit: Option<u64>| {
        let fan_out = CommonScanSpec {
            filter: filter.clone(),
            limit: shard_limit,
            ..base.clone()
        };
        match split_aggregate_sql(shape, pushdown, scan, fan_out, shards, limit) {
            Some(sql) => Ok(pushdown_response(sql)),
            // Defensive: a shortened select list fails Exasol's positional validation.
            None => fallback(),
        }
    };

    let mut projection = match classify_request_shape(pushdown, scan) {
        shape @ RequestShape::Grouped { .. } => return split(shape, limit),
        RequestShape::GroupByWrapper | RequestShape::SingleGroupWrapper => {
            // Never fall through to the bare row scan: Exasol expects exactly the
            // `selectList` columns (`04000` otherwise). The qualified wrapper lets Exasol
            // aggregate the materialized rows (#82). No outer WHERE is needed: an
            // untranslatable filter never reaches this arm.
            return fallback();
        }
        RequestShape::SingleGroupAgg {
            items,
            aggregates,
            having,
        } => {
            // Exasol rejects OFFSET in an ungrouped aggregate (`42000`) before the adapter
            // is consulted.
            debug_assert!(
                pushdown.offset() == 0,
                "fact 6: Exasol rejects OFFSET in an ungrouped aggregated select \
                 (sqlCode 42000) before the adapter is consulted, so this wrapper \
                 can never see a non-zero offset"
            );
            // Lone bare-column COUNT(DISTINCT): the only fan-out shape. LIMIT goes only on
            // the outer wrapper; inside the fan-out it would truncate a shard's distinct set.
            if is_lone_count_distinct(&items) {
                let base_spec = ScanSpec {
                    common: CommonScanSpec {
                        filter: filter.clone(),
                        ..base.clone()
                    },
                    files: vec![],
                };
                let sql = support::build_count_distinct_scan_sql(
                    &base_spec, shards, &items, emitted, limit,
                )?;
                return Ok(pushdown_response(sql));
            }
            // Multiple or mixed distincts: the bare row scan would fail Exasol's positional
            // validation (`04000`), and per-distinct fan-outs cannot be composed as scalar
            // subqueries ("emitting function in expression"). Use the qualified wrapper,
            // narrowed to referenced columns (#160).
            if has_distinct(&items) {
                return fallback();
            }
            // A limit never precedes an ordering the adapter did not render (decision [4]).
            let shard_limit = if has_order_by { None } else { limit };
            return split(
                RequestShape::SingleGroupAgg {
                    items,
                    aggregates,
                    having,
                },
                shard_limit,
            );
        }
        // An item only Exasol computes goes to the qualified wrapper. `project_columns` decides
        // it, never a column-count comparison, which can coincide (#196).
        RequestShape::RowScan => match projection {
            RowProjection::Wrapped => return fallback(),
            RowProjection::Direct(projection) => projection,
        },
    };

    // Must run before the declined-path sort-key extension below: an appended hidden
    // column could otherwise make a shape match top-N, whose output has no wrapping
    // SELECT to drop it (#225).
    let order_by = detect_topn(
        pushdown,
        &projection_items(&projection),
        &base.logical_schema,
    )
    .unwrap_or_default();

    // Exasol requires ORDER BY for a pushed OFFSET, and a non-zero offset declines
    // top-N, so `effective_limit` is nulled and no LIMIT/OFFSET renders without an
    // ORDER BY (#191).
    debug_assert!(
        pushdown.offset() == 0 || has_order_by,
        "fact 5: a non-zero offset must never arrive without a non-empty orderBy"
    );

    let effective_limit = if has_order_by && order_by.is_empty() {
        None
    } else {
        limit
    };

    // Declined ORDER BY: missing sort-key columns are appended as hidden columns, and
    // the wrapper below drops them again (#225). Must run after `detect_topn`.
    let visible_count = projection.len();
    let declined_order_by = has_order_by && order_by.is_empty();
    let declined_sort_keys = if declined_order_by {
        let mut keys = parse_order_by_keys(pushdown)?;
        topn::sort_by_select_items(&mut keys, pushdown, &projection);
        topn::extend_projection_with_sort_keys(&mut projection, &keys, emitted)?;
        keys
    } else {
        Vec::new()
    };

    let proj_items = projection_items(&projection);
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            filter,
            limit: effective_limit,
            order_by,
            ..base.clone()
        },
        files: vec![],
    };

    let sql = build_scan_driving_sql(&spec_template, shards, &projection, effective_limit, None);

    // Exasol does not re-apply a delegated sort/limit, so wrap the unbounded fan-out
    // in a global ORDER BY naming only the visible columns; a wider row fails
    // positional validation (`04000`).
    let sql = if declined_order_by {
        topn::wrap_declined_order_by(
            &sql,
            &proj_items,
            visible_count,
            &declined_sort_keys,
            limit,
            pushdown.offset(),
        )
    } else {
        sql
    };

    Ok(pushdown_response(sql))
}

#[cfg(test)]
#[path = "pushdown_tests.rs"]
mod tests;

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
use crate::adapter::get_properties;
use crate::adapter::permission::PermissionCheck;
use crate::adapter::properties::resolve_binary_as_string;
use crate::adapter::statement_state::StatementCatalog;
use crate::scan::spec::{
    CatalogProps, CommonScanSpec, FileEntry, LogicalField, NameMappingEntry, ScanSpec, ScanStorage,
};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

mod binary_filter;
pub(crate) use binary_filter::literal_binary_bytes;

mod support;
pub use support::{
    AggregateMergeInputs, ColumnTypes, ProjectedColumn, PushdownRequest, ScanTranslator,
    build_fan_out_inner, build_scan_driving_sql, shard_count,
};
use support::{
    DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, extract_limit, involved_columns, order_by_present,
    project_columns, projection_items, scan_storage_for,
};
use vs_expression::strip_table_alias;

mod empty_result;
use empty_result::empty_result_sql;

mod shard_paths;
use shard_paths::relativize_shards_to_root;

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
use single_group_agg::{has_distinct, is_lone_count_distinct, single_group_merge_select};

mod grouped_agg;
pub use grouped_agg::{
    GroupedAggregateDetection, GroupedSelectItem, build_grouped_aggregate_scan_sql,
    detect_group_by_aggregates,
};

mod scalar_over_agg;
pub use scalar_over_agg::TypedPlans;

mod request_shape;
use request_shape::{RequestShape, classify_request_shape};

mod joins;
// Several re-exports are consumed only by tests and the reachability probe.
#[allow(unused_imports)]
pub(crate) use joins::{
    DetectedJoin, IneligibleJoinReason, JoinLeaf, JoinShape, JoinSides, RenderedJoinPushdown,
    ResolvedJoinSide, detect_join, render_broadcast_join,
};
use joins::{
    EmptyRelation, ineligible_join_decline, plan_join, qualified_single_table_fallback_pushdown,
    qualify_udf,
};

#[cfg(test)]
use crate::scan::spec::{AggKind, AggregatePlan, StorageBackend};
#[cfg(test)]
use test_support::scan_filter_sql;

#[cfg(test)]
#[path = "test_support_tests.rs"]
pub(crate) mod test_support;

#[cfg(test)]
#[path = "dispatch_golden_tests.rs"]
mod dispatch_golden;

/// `join_broadcast_max_bytes`: a two-table inner equi-join broadcasts its smaller
/// side when that side's manifest byte size is at or below this threshold.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_pushdown(
    request: &Json,
    statement: &StatementCatalog,
    permission: &PermissionCheck,
    catalog: &CatalogProps,
    scan_schema: Option<&str>,
    cluster_nodes: usize,
    parallelism_factor: usize,
    df_target_partitions: usize,
    df_batch_size: usize,
    df_threads_per_udf: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    s3_max_connections: usize,
    join_broadcast_max_bytes: u64,
) -> Result<Json, UdfError> {
    let pushdown = PushdownRequest::parse(
        request
            .get("pushdownRequest")
            .cloned()
            .unwrap_or(Json::Null),
    )?;

    let props = get_properties(request);

    // Join handling must run first: this path resolves only `involvedTables[0]`, so a
    // join falling through would silently scan one table. `Ineligible` is a hard error
    // because Exasol does not re-plan on an adapter error.
    match detect_join(request, pushdown.json())? {
        JoinShape::NotAJoin => {}
        JoinShape::Ineligible(reason) => return Err(ineligible_join_decline(reason)),
        JoinShape::Join(join) => {
            return plan_join(
                request,
                &pushdown,
                &join,
                statement,
                permission,
                &props,
                scan_schema,
                cluster_nodes,
                parallelism_factor,
                df_target_partitions,
                df_batch_size,
                df_threads_per_udf,
                memory_pool_fraction,
                instance_overhead_mb,
                s3_max_connections,
                join_broadcast_max_bytes,
            )
            .await;
        }
    }

    // Strip every `tableAlias` after the join gate and before any read (#193).
    let pushdown = pushdown.with_json(strip_table_alias(pushdown.json()));
    let pushdown_req = pushdown.json();

    let filter_json_raw = pushdown_req.get("filter").filter(|f| !f.is_null());

    let involved = involved_columns(request, |tables| tables.first())?;

    let limit = extract_limit(pushdown_req);

    // A limit is withheld from any ORDER BY request not matched as a bounded top-N, so
    // a bare LIMIT never precedes an ordering the adapter did not render (decision [4]).
    let has_order_by = order_by_present(pushdown_req);

    let resolver =
        TableScanResolver::for_request(statement, permission, &[catalog.table.as_str()], &props)
            .await?;
    let (
        ResolvedScan {
            files,
            effective_storage,
            logical_schema,
            table_root,
            name_mapping,
            partition_columns,
            refused_columns,
        },
        col_types,
    ) = resolver
        .resolve(&catalog.table, filter_json_raw, &involved)
        .await?;
    let scan = ScanTranslator::new(&col_types);
    let (projection, projection_widened) = project_columns(&pushdown, scan)?;
    // `filter_json_raw` stays unmodified: format-level pruning must see the original
    // predicate tree, whatever the adapter declines.
    let binary_as_string = resolve_binary_as_string(&props);
    let (filter, declined_filter) = scan.split_where(filter_json_raw);
    let conn = statement.connection();
    let scan_storage = scan_storage_for(
        &conn.creds,
        &conn.connection_name,
        conn.allow_http,
        &effective_storage,
        conn.sealed_storage_key.as_ref(),
    )?;

    // Before the zero-files early return: a refused column must error, never yield an
    // empty result.
    ensure_no_refused_column_referenced(
        request,
        (!projection_widened)
            .then(|| projection_items(&projection))
            .as_deref(),
        &refused_columns,
    )?;

    if files.is_empty() {
        return empty_result_sql(
            &pushdown,
            &projection,
            projection_widened,
            scan,
            &EmptyRelation::single_scan(request, &col_types),
        );
    }

    let g = shard_count(cluster_nodes, parallelism_factor, files.len());
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let shards = relativize_shards_to_root(shards, &table_root);

    // Schema-qualified: the pushdown query runs outside the adapter script's schema.
    let udf_name = qualify_udf(scan_schema, SCAN_UDF_NAME);
    let distribute_udf_name = qualify_udf(scan_schema, DISTRIBUTE_FILES_UDF_NAME);

    build_dispatch_sql(
        request,
        &pushdown,
        projection,
        projection_widened,
        scan,
        filter,
        declined_filter,
        limit,
        has_order_by,
        &shards,
        table_root,
        logical_schema,
        name_mapping,
        partition_columns,
        &scan_storage,
        &udf_name,
        &distribute_udf_name,
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
        binary_as_string,
    )
}

/// `projection_widened` means `projection` is the full base row rather
/// than one item per select-list item (#196). `filter` and `declined_filter` are
/// never both `Some`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_dispatch_sql(
    request: &Json,
    pushdown: &PushdownRequest,
    mut projection: Vec<ProjectedColumn>,
    projection_widened: bool,
    scan: ScanTranslator<'_>,
    filter: Option<String>,
    declined_filter: Option<&Json>,
    limit: Option<u64>,
    has_order_by: bool,
    shards: &[Vec<FileEntry>],
    table_root: String,
    logical_schema: Vec<LogicalField>,
    name_mapping: Vec<NameMappingEntry>,
    partition_columns: Vec<String>,
    scan_storage: &ScanStorage,
    udf_name: &str,
    distribute_udf_name: &str,
    df_target_partitions: usize,
    df_batch_size: usize,
    df_threads_per_udf: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    s3_max_connections: usize,
    binary_as_string: bool,
) -> Result<Json, UdfError> {
    let pushdown_req = pushdown.json();
    let base = CommonScanSpec {
        table_root: table_root.clone(),
        projection: Vec::new(),
        filter: None,
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        distinct: false,
        logical_schema: logical_schema.clone(),
        name_mapping: name_mapping.clone(),
        join: None,
        partition_columns,
        storage: scan_storage.clone(),
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
        binary_as_string,
    };

    // Ahead of shape routing so it applies before aggregating, grouping, and
    // truncating (`_decision/045`).
    if let Some(declined) = declined_filter {
        return qualified_single_table_fallback_pushdown(
            request,
            pushdown,
            &base,
            None,
            shards,
            scan.columns(),
            udf_name,
            distribute_udf_name,
            Some(declined),
        );
    }

    let fallback = || {
        qualified_single_table_fallback_pushdown(
            request,
            pushdown,
            &base,
            filter.clone(),
            shards,
            scan.columns(),
            udf_name,
            distribute_udf_name,
            None,
        )
    };

    let single_group_merge = match classify_request_shape(pushdown, scan) {
        RequestShape::Grouped {
            detection,
            having,
            order_by: grouped_order_by,
        } => {
            let GroupedAggregateDetection {
                group_keys,
                group_key_types,
                aggregates,
                select_items,
            } = detection;
            // Safe to apply the outer LIMIT: the merge renders its own ORDER BY, and the
            // per-shard partial never carries a LIMIT (decision [4]).
            let grouped_limit = limit;
            // Empty `projection` is inert on an aggregate dispatch (#145).
            let spec_template = ScanSpec {
                common: CommonScanSpec {
                    filter: filter.clone(),
                    limit: grouped_limit,
                    aggregates: Some(aggregates.plans.clone()),
                    group_keys: Some(group_keys.clone()),
                    ..base.clone()
                },
                files: vec![],
            };
            // Types come from detection, never a `selectList`-keyed lookup: nested aggregates
            // would misalign it.
            let sql = build_grouped_aggregate_scan_sql(
                &spec_template,
                shards,
                &group_key_types,
                &aggregates,
                &select_items,
                pushdown.select_types(),
                grouped_limit,
                support::extract_offset(pushdown_req),
                scan,
                udf_name,
                distribute_udf_name,
                having.as_deref(),
                grouped_order_by.as_deref(),
            );
            let Some(sql) = sql else {
                // Defensive: a shortened select list fails Exasol's positional validation.
                return fallback();
            };
            return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
        }
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
            // Lone bare-column COUNT(DISTINCT): the only fan-out shape. LIMIT goes only on
            // the outer wrapper; inside the fan-out it would truncate a shard's distinct set.
            // Exasol rejects OFFSET in an ungrouped aggregate (`42000`) before the adapter
            // is consulted.
            if is_lone_count_distinct(&items) {
                debug_assert!(
                    support::extract_offset(pushdown_req) == 0,
                    "fact 6: Exasol rejects OFFSET in an ungrouped aggregated select \
                     (sqlCode 42000) before the adapter is consulted, so this wrapper \
                     can never see a non-zero offset"
                );
                let base_spec = ScanSpec {
                    common: CommonScanSpec {
                        filter: filter.clone(),
                        ..base.clone()
                    },
                    files: vec![],
                };
                let Some(sql) = support::build_count_distinct_scan_sql(
                    &base_spec,
                    shards,
                    &items,
                    scan.columns(),
                    limit,
                    udf_name,
                    distribute_udf_name,
                ) else {
                    return fallback();
                };
                return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
            }
            // Multiple or mixed distincts: the bare row scan would fail Exasol's positional
            // validation (`04000`), and per-distinct fan-outs cannot be composed as scalar
            // subqueries ("emitting function in expression"). Use the qualified wrapper,
            // narrowed to referenced columns (#160).
            if has_distinct(&items) {
                return fallback();
            }
            let merge_select =
                single_group_merge_select(&items, &aggregates, pushdown.select_types(), scan);
            let TypedPlans { plans, emits, .. } = aggregates;
            let merge_inputs = merge_select.and_then(|merge_select| {
                AggregateMergeInputs::new(emits, merge_select, limit)
                    .map(|inputs| inputs.with_having(having))
            });
            let Some(merge_inputs) = merge_inputs else {
                // Defensive: a shortened select list fails Exasol's positional validation.
                return fallback();
            };
            Some((plans, merge_inputs))
        }
        RequestShape::RowScan => {
            // A widened projection must go to the qualified wrapper: Exasol validates the
            // returned columns positionally (`04000`). Decided by `project_columns`'s own
            // widening signal, never by comparing column counts, which misses widenings
            // whose base-row width equals the select-list arity (#196).
            if projection_widened {
                return fallback();
            }
            None
        }
    };

    // One Option, so an aggregate spec can never pair with absent merge inputs.
    let (aggregates, merge_inputs) = single_group_merge
        .map(|(plans, inputs)| (Some(plans), Some(inputs)))
        .unwrap_or((None, None));

    // Must run before the declined-path sort-key extension below: an appended hidden
    // column could otherwise make a shape match top-N, whose output has no wrapping
    // SELECT to drop it (#225).
    let topn = if aggregates.is_none() {
        detect_topn(
            request,
            pushdown_req,
            &projection_items(&projection),
            &logical_schema,
        )
    } else {
        None
    };
    let order_by = topn.unwrap_or_default();

    // Exasol requires ORDER BY for a pushed OFFSET, and a non-zero offset declines
    // top-N, so `effective_limit` is nulled and no LIMIT/OFFSET renders without an
    // ORDER BY (#191).
    debug_assert!(
        support::extract_offset(pushdown_req) == 0 || has_order_by,
        "fact 5: a non-zero offset must never arrive without a non-empty orderBy"
    );

    let effective_limit = if has_order_by && order_by.is_empty() {
        None
    } else {
        limit
    };

    // Declined ORDER BY: missing sort-key columns are appended as hidden columns, and
    // the wrapper below drops them again (#225). Must run after `detect_topn` and
    // before `spec_template`, so EMITS and the scan projection stay in sync.
    let visible_count = projection.len();
    let declined_order_by = has_order_by && order_by.is_empty() && aggregates.is_none();
    let declined_sort_keys = if declined_order_by {
        let keys = parse_order_by_keys(pushdown_req);
        // Exasol does not re-sort a delegated ordering, so an unrenderable key must
        // decline before any SQL is built (#198).
        topn::ensure_every_sort_key_renders(&keys)?;
        topn::extend_projection_with_sort_keys(&mut projection, &keys, scan.columns());
        keys
    } else {
        Vec::new()
    };

    let has_aggregates = aggregates.is_some();
    let proj_items = projection_items(&projection);

    let spec_template = ScanSpec {
        common: CommonScanSpec {
            // Empty on the aggregate sub-path (inert, keeps EXPLAIN VIRTUAL accurate, #145);
            // the row-scan sub-path's projection drives EMITS and the scan.
            projection: if has_aggregates {
                Vec::new()
            } else {
                proj_items.clone()
            },
            filter,
            limit: effective_limit,
            order_by,
            aggregates,
            ..base.clone()
        },
        files: vec![],
    };

    debug_assert!(
        !has_aggregates || support::extract_offset(pushdown_req) == 0,
        "fact 6: Exasol rejects OFFSET in an ungrouped aggregated select \
         (sqlCode 42000) before the adapter is consulted, so the single-group \
         aggregate merge can never see a non-zero offset"
    );
    let sql = build_scan_driving_sql(
        &spec_template,
        shards,
        &projection,
        effective_limit,
        merge_inputs.as_ref(),
        udf_name,
        distribute_udf_name,
    );

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
            support::extract_offset(pushdown_req),
        )
    } else {
        sql
    };

    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

#[cfg(test)]
#[path = "pushdown_tests.rs"]
mod tests;

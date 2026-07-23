use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{
    CatalogProps, CommonScanSpec, FileEntry, LogicalField, NameMappingEntry, ProjectionItem,
    ScanSpec, StorageProps, render_order_by_clause,
};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
/// Pushdown planning: resolve the Iceberg file list ONCE and build the
/// scan-driving SQL that invokes the LAKEHOUSE_SCAN SCALAR EMIT UDF.
///
/// Architecture invariants:
/// - File list resolved exactly ONCE here, in the planning layer.
/// - The scan SCALAR EMIT UDF receives the explicit file list; it NEVER discovers files.
/// - A predicate the adapter cannot translate is OMITTED from the spec
///   (correctness backstop: Exasol keeps the predicate at its own level).
/// - LIMIT appears in both the scan spec and the returned SQL (correctness backstop).
/// - Catalog/connection auth credentials (OAuth token, bearer, etc.) NEVER appear
///   in any returned SQL string or error message. Storage (S3) credentials are a
///   documented exception — see `handle_pushdown`'s doc comment.
use vs_expression::render_df_filter_safe;

mod support;
use support::{
    DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, aggregate_exasol_types, extract_all_column_types,
    extract_limit, extract_projection, order_by_present,
};
pub use support::{build_fan_out_inner, build_scan_driving_sql, shard_count};

mod credentials;
pub use credentials::{extract_vended_keys, merge_vended_into_storage};

mod namespace;
pub use namespace::list_namespace_tables;

mod file_resolution;
use file_resolution::{empty_result_sql, encode_initial_default, relativize_shards_to_root};
pub use file_resolution::{resolve_file_list, resolve_table_schema};

mod topn;
use topn::{detect_topn, parse_order_by_keys};

mod single_group_agg;
pub use single_group_agg::{detect_aggregates, ordinary_plans};
use single_group_agg::{has_distinct, is_lone_count_distinct};

mod grouped_agg;
pub use grouped_agg::{
    GroupedAggregateDetection, GroupedSelectItem, build_grouped_aggregate_scan_sql,
    detect_group_by_aggregates, validate_agg_col_types,
};
use grouped_agg::{
    GroupedOrderBy, build_grouped_order_by_clause, group_key_exasol_types, render_having_over_merge,
};

mod request_shape;
use request_shape::{RequestShape, classify_request_shape};

mod joins;
// The join types plus `render_broadcast_join` are re-exported to preserve the
// pre-refactor `crate::adapter::pushdown::<name>` surface; several are consumed
// only by the `#[cfg(test)]` reachability probe and tests, so a non-test build
// reads the re-export as unused.
#[allow(unused_imports)]
pub(crate) use joins::{
    DetectedJoin, IneligibleJoinReason, JoinLeaf, JoinShape, JoinSides, RenderedJoinPushdown,
    ResolvedJoinSide, detect_join, render_broadcast_join,
};
use joins::{
    ineligible_join_decline, plan_join, qualified_single_table_fallback_pushdown, qualify_udf,
};

#[cfg(test)]
use crate::scan::spec::{AggKind, AggregatePlan};

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod dispatch_golden;

/// Resolve the Iceberg snapshot + file list and build pushdown SQL.
///
/// `cluster_nodes` — the number of Exasol nodes read from the `CLUSTER_NODES`
/// adapterNotes entry (default 1 when absent or unparseable).
///
/// `parallelism_factor` — the oversubscription multiplier read from the
/// `PARALLELISM_FACTOR` adapterNotes entry (default 8).
///
/// `join_broadcast_max_bytes` — the byte-size threshold read from the
/// `JOIN_BROADCAST_MAX_BYTES` adapterNotes entry (default 128 MiB); a two-table
/// inner equi-join broadcasts its smaller side when that side's Iceberg-manifest
/// byte size is at or below this threshold. See backlog BL-001 / plan
/// `add-join-pushdown-broadcast`.
///
/// `creds` — the resolved CONNECTION credentials, used to determine whether
/// to sign catalog requests and whether to apply vended S3 credentials.
///
/// Returns JSON `{"type":"pushdown","sql":"..."}`.
///
/// ponytail: The S3 access/secret/session-token keys are embedded verbatim in the
/// scan-driving SQL literal (inside the `ScanSpec` JSON), which Exasol may log or
/// surface in its query profile / audit trail. PoC-accepted tradeoff. The upgrade
/// path is to pass credentials via a CONNECTION object (referenced by name, never
/// inlined) or to fetch them over connect-back at scan time so they never appear
/// in any SQL text. Error paths already redact these values.
#[allow(clippy::too_many_arguments)]
pub async fn handle_pushdown(
    request: &Json,
    catalog_uri: &str,
    storage: &StorageProps,
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
    creds: &ConnectionCreds,
) -> Result<Json, UdfError> {
    let pushdown_req = request
        .get("pushdownRequest")
        .cloned()
        .unwrap_or(Json::Null);

    // Inner-join handling MUST run before the single-table path. `handle_pushdown`
    // is invoked once per pushdown REQUEST, resolving only `involvedTables[0]`
    // (adapter::mod::handle_pushdown_request); a join-shaped `from` that fell through
    // would scan just the first table and silently drop the join. `NotAJoin` is
    // today's normal single-table request — fall through unchanged. `Ineligible` is a
    // shape the adapter cannot render at all (a non-inner join node, or a malformed
    // shape), so it is a hard client-facing error (Exasol does not re-plan on an
    // adapter error). `Join` is served here by the single unified join path and
    // returns directly.
    match detect_join(request, &pushdown_req)? {
        JoinShape::NotAJoin => {}
        JoinShape::Ineligible(reason) => return Err(ineligible_join_decline(reason)),
        JoinShape::Join(join) => {
            return plan_join(
                request,
                &pushdown_req,
                &join,
                catalog_uri,
                storage,
                catalog,
                creds,
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

    let (proj_cols, proj_types) = extract_projection(request, &pushdown_req)?;

    let filter_json_raw = pushdown_req.get("filter").filter(|f| !f.is_null());

    let filter = filter_json_raw.and_then(render_df_filter_safe);

    let limit = extract_limit(&pushdown_req);

    // Whether Exasol pushed an ORDER BY. Drives the anti-wrong-truncation guard
    // (decision [4]): a limit is withheld from every ORDER-BY-carrying request the
    // adapter does not match as a bounded top-N, so a bare per-shard/outer LIMIT is
    // never emitted ahead of an ordering the adapter did not itself render.
    let has_order_by = order_by_present(&pushdown_req);

    let col_types = extract_all_column_types(request);

    // Resolve file list exactly once. The returned `effective_storage` carries
    // vended STS creds when use_vended_credentials is true; otherwise it equals
    // the static `storage` passed in. Every per-shard ScanSpec uses this storage.
    // filter_json_raw is forwarded for Iceberg-level file pruning; ScanSpec.filter
    // (DataFusion SQL string) is set separately above and left completely unchanged.
    let (files, effective_storage, logical_schema, table_root, name_mapping) =
        resolve_file_list(catalog_uri, catalog, storage, creds, filter_json_raw).await?;
    let storage = &effective_storage;

    if files.is_empty() {
        return empty_result_sql(&pushdown_req, &proj_cols, &proj_types, &col_types);
    }

    // Compute G = shard_count(node_count, parallelism_factor, file_count) and
    // partition files into G byte-balanced work-unit shards (GROUP BY shard_key fan-out).
    let g = shard_count(cluster_nodes, parallelism_factor, files.len());
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    // Emit each under-root file path relative to `table_root` (carried once in the
    // common blob) so the per-shard payload stops repeating the table-location
    // prefix. Sizes and shard membership are unchanged; paths not under the root
    // stay absolute. The scan UDF rejoins relative paths onto `table_root`.
    let shards = relativize_shards_to_root(shards, &table_root);

    // The scan and distributor UDFs must be schema-qualified: the pushdown query
    // executes outside the adapter script's schema, so an unqualified name would not
    // resolve ("function or script LAKEHOUSE_SCAN not found").
    let udf_name = qualify_udf(scan_schema, SCAN_UDF_NAME);
    let distribute_udf_name = qualify_udf(scan_schema, DISTRIBUTE_FILES_UDF_NAME);

    build_dispatch_sql(
        request,
        &pushdown_req,
        proj_cols,
        proj_types,
        col_types,
        filter,
        limit,
        has_order_by,
        &shards,
        table_root,
        logical_schema,
        name_mapping,
        storage,
        &udf_name,
        &distribute_udf_name,
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
    )
}

/// Build the dispatch SQL for a resolved, non-empty pushdown request.
///
/// Extracted verbatim from `handle_pushdown`'s post-resolution dispatch body
/// (issue #175 / plan `refactor-scan-spec-dispatch-dedup`, task 1.1): a pure,
/// behavior-preserving move — no field, clause, argument, or ordering change.
/// `handle_pushdown` resolves the file list, shards it, and qualifies the UDF
/// names before calling this; every parameter here is an already-resolved
/// input from that resolution.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_dispatch_sql(
    request: &Json,
    pushdown_req: &Json,
    proj_cols: Vec<ProjectionItem>,
    proj_types: Vec<String>,
    col_types: Vec<(String, String)>,
    filter: Option<String>,
    limit: Option<u64>,
    has_order_by: bool,
    shards: &[Vec<FileEntry>],
    table_root: String,
    logical_schema: Vec<LogicalField>,
    name_mapping: Vec<NameMappingEntry>,
    storage: &StorageProps,
    udf_name: &str,
    distribute_udf_name: &str,
    df_target_partitions: usize,
    df_batch_size: usize,
    df_threads_per_udf: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    s3_max_connections: usize,
) -> Result<Json, UdfError> {
    // Shard-invariant fields shared by every fan-out `ScanSpec` this dispatcher
    // builds below. Each site spreads `..base.clone()` and sets only the fields
    // that differ; a field left unset keeps the inert placeholder here
    // unchanged (empty projection/order_by/emit_exa_types, no filter/limit/
    // aggregates/group_keys, `distinct: false` — the same neutral defaults
    // every non-aggregate, non-projecting site already needed).
    let base = CommonScanSpec {
        table_root: table_root.clone(),
        projection: Vec::new(),
        filter: None,
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        distinct: false,
        emit_exa_types: Vec::new(),
        logical_schema: logical_schema.clone(),
        name_mapping: name_mapping.clone(),
        join: None,
        storage: storage.clone(),
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
    };

    // One shared classifier decides the routing shape for BOTH this dispatcher and
    // the empty-result path (`file_resolution::empty_result_sql`), so their output
    // shapes are identical by construction rather than by two hand-synced routing
    // trees. The 3-tier priority (grouped → single-group → row scan), the numeric
    // gates, and the non-numeric-grouped-with-HAVING decline all live in the
    // classifier; each arm below renders ONLY its own SQL. The fall-through arms
    // (ordinary single-group aggregate, row scan) yield the shared `aggregates`
    // input the row-scan/partial-aggregate rendering below consumes (`Some` ordinary
    // plans for the aggregate sub-path, `None` for a row scan).
    let aggregates = match classify_request_shape(pushdown_req, &col_types)? {
        RequestShape::Grouped { detection, having } => {
            let GroupedAggregateDetection {
                group_keys,
                plans: grouped_agg_plans,
                plan_types: grouped_agg_types,
                select_items,
            } = detection;
            // Render the HAVING against the merge decomposition: each aggregate
            // reference is rewritten to its merged expression (SUM(score) →
            // SUM("PARTIAL_sum_0")). Applied in the OUTER wrapper only, never in
            // the per-shard scan. If a HAVING is present but cannot be rendered
            // over the merge, decline the grouped pushdown (Err) — silently
            // dropping it would yield wrong results because Exasol will not
            // re-apply a HAVING we advertised AGGREGATE_HAVING for. This is a
            // RENDERING decline, distinct from the classifier's routing decline.
            let having = match having {
                Some(node) => match render_having_over_merge(node, &grouped_agg_plans) {
                    Some(sql) => Some(sql),
                    None => {
                        return Err(UdfError::User(
                            "grouped aggregate pushdown declined: HAVING references an \
                             aggregate that cannot be merged or an unsupported node; \
                             this is a hard error, not a native re-plan"
                                .into(),
                        ));
                    }
                },
                None => None,
            };
            // Grouped aggregate pushdown path. Once ORDER_BY_COLUMN is advertised,
            // Exasol delegates any ORDER BY on the grouped output and NO LONGER
            // re-sorts the rows the adapter returns (add-topn-pushdown B6), so the
            // merge SQL must render its own explicit final ORDER BY over the grouped
            // output columns. Resolve it now: a pushed sort key that cannot be mapped
            // to a grouped output column is a shape SQL forbids — decline the pushdown
            // as a hard error rather than emit an unsorted merge.
            let grouped_order_by =
                match build_grouped_order_by_clause(pushdown_req, &group_keys, &select_items) {
                    Some(GroupedOrderBy::Clause(clause)) => Some(clause),
                    Some(GroupedOrderBy::Unresolvable) => {
                        return Err(UdfError::User(
                            "grouped aggregate pushdown declined: ORDER BY references a \
                         column that is not a grouped output column; this is a hard \
                         error, not a native re-plan"
                                .into(),
                        ));
                    }
                    None => None,
                };
            // With the ordering now rendered explicitly, the outer LIMIT is a true
            // global top-N over the merged groups, so it is safe to apply. When there
            // is no ORDER BY it stays a plain grouped LIMIT (unchanged). The per-shard
            // partial scan still never carries a LIMIT (the fan-out common blob is
            // rebuilt with `limit = None`), preserving the anti-wrong-truncation
            // invariant (decision [4]).
            let grouped_limit = limit;
            // This branch is ALWAYS an aggregate dispatch — see `ScanSpec::projection`
            // doc for why an empty `projection` is inert here, not "all columns"
            // (#145). Aggregate scans also emit via the freely-coercing Value path,
            // not the strict emit_batch IPC path, so `emit_exa_types` needs no
            // per-column declared types either — both stay at `base`'s empty
            // placeholder, so neither is set explicitly below.
            let spec_template = ScanSpec {
                common: CommonScanSpec {
                    filter,
                    limit: grouped_limit,
                    aggregates: Some(grouped_agg_plans.clone()),
                    group_keys: Some(group_keys.clone()),
                    ..base.clone()
                },
                files: vec![],
            };
            let group_key_types = group_key_exasol_types(pushdown_req, &group_keys, &select_items);
            // Per-plan declared types, aligned 1:1 with `grouped_agg_plans` (which
            // now includes aggregates nested inside a scalar-over-aggregate item).
            // `aggregate_exasol_types` keyed off top-level select items only and
            // would misalign; the detection-built `plan_types` is the aligned source.
            let aggregate_types = grouped_agg_types;
            let sql = build_grouped_aggregate_scan_sql(
                &spec_template,
                shards,
                &group_keys,
                &group_key_types,
                &grouped_agg_plans,
                &aggregate_types,
                &select_items,
                grouped_limit,
                &col_types,
                udf_name,
                distribute_udf_name,
                having.as_deref(),
                grouped_order_by.as_deref(),
            );
            return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
        }
        RequestShape::GroupByWrapper => {
            // A GROUP BY request that did NOT push down as a grouped partial/merge
            // must NEVER fall through to the bare row scan: for a grouped request
            // Exasol expects the pushdown query to return exactly the `selectList`
            // columns, but a raw full-row scan returns the projected source columns
            // instead → SQL state `04000` "Expected number of columns is N but
            // pushdown query has M". Route it to a qualified single-table wrapper —
            // the join N-scan fallback at N=1 — that renders the exact grouped select
            // list (aggregates verbatim) over a materialized sharded raw scan so
            // Exasol's core engine aggregates the returned rows (issue #82).
            //
            // Per-shard scan stays LIMIT-free and sort-free (no aggregates, no group
            // keys); the group keys, HAVING, ORDER BY, and LIMIT go in the outer
            // wrapper only. The WHERE filter is pushed into the scan (advertised
            // filter capabilities carry only translatable predicates), exactly as the
            // grouped push-down path does — no outer WHERE needed.
            return qualified_single_table_fallback_pushdown(
                request,
                pushdown_req,
                &base,
                filter.clone(),
                shards,
                &col_types,
                udf_name,
                distribute_udf_name,
            );
        }
        RequestShape::SingleGroupAgg { items } => {
            // Case 1 COUNT(DISTINCT) path: EXACTLY one COUNT(DISTINCT <bare column>)
            // and nothing else. This is the ONLY count-distinct shape that fans out —
            // a dedicated DISTINCT row-scan counted by a native COUNT(DISTINCT "V").
            // The request-level LIMIT lands ONLY on that outer wrapper — never inside
            // the fan-out sub-scan (a leaked LIMIT would truncate a shard's local
            // distinct set → a wrong count) — and is withheld entirely when an ORDER
            // BY the adapter did not render is present (anti-wrong-truncation guard,
            // decision [4]). The base spec carries no projection/aggregates/limit/
            // order-by/distinct: the wrapper builder derives the fan-out from it.
            if is_lone_count_distinct(&items) {
                let cd_limit = if has_order_by { None } else { limit };
                let base_spec = ScanSpec {
                    common: CommonScanSpec {
                        filter: filter.clone(),
                        ..base.clone()
                    },
                    files: vec![],
                };
                let sql = support::build_count_distinct_scan_sql(
                    &base_spec,
                    shards,
                    &items,
                    &col_types,
                    cd_limit,
                    udf_name,
                    distribute_udf_name,
                );
                return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
            }
            // Case 2/3 single-group COUNT(DISTINCT) decline: MORE THAN ONE
            // COUNT(DISTINCT), or a distinct mixed with an ordinary SUM/MIN/MAX/COUNT/
            // AVG aggregate. Like the grouped guard, it MUST NOT fall through to the
            // bare row scan below: a raw full-row scan returns the projected source
            // columns where Exasol's pushdown validation expects one column per
            // aggregate select item → SQL state `04000`, because Exasol never
            // re-aggregates a declined pushdown (it runs the returned SQL as the final
            // answer as-is). A per-distinct fan-out likewise cannot be composed as
            // sibling SELECT-list scalar subqueries (Exasol rejects an emitting UDF
            // nested in a scalar subquery, `04000` "emitting function in expression").
            // Route it to the shared qualified single-table wrapper (the join N-scan
            // fallback at N = 1), which renders the exact single-group select list —
            // every COUNT(DISTINCT) and ordinary aggregate spliced verbatim — over a
            // materialized sharded raw scan narrowed to only the referenced columns
            // (issue #160), so Exasol's core engine aggregates the returned rows and
            // the result column count matches its positional validation.
            if has_distinct(&items) {
                return qualified_single_table_fallback_pushdown(
                    request,
                    pushdown_req,
                    &base,
                    filter.clone(),
                    shards,
                    &col_types,
                    udf_name,
                    distribute_udf_name,
                );
            }
            // No distinct item: the ordinary single-group aggregate plans drive the
            // shared per-shard partial/merge scan below.
            Some(ordinary_plans(&items))
        }
        // No decomposable aggregate (or the numeric gate demoted it) → row scan.
        RequestShape::RowScan => None,
    };

    // Ordered top-N applies ONLY to the pure row-scan path (no aggregates). On a
    // match the sort keys are carried into the common blob (per-shard bounded sort)
    // and the outer wrapper renders `ORDER BY … LIMIT n`.
    let topn = if aggregates.is_none() {
        detect_topn(request, pushdown_req, &proj_cols, &logical_schema)
    } else {
        None
    };
    let order_by = topn.unwrap_or_default();

    // Withhold the limit when an ORDER BY is present but the shape is not a matched
    // top-N (`order_by` empty): never a bare per-shard/outer LIMIT ahead of an
    // ordering the adapter did not render (decision [4]). A matched top-N keeps the
    // limit (bounded per-shard sort + outer merge limit); a plain LIMIT-only query
    // (no ORDER BY) is unchanged.
    let effective_limit = if has_order_by && order_by.is_empty() {
        None
    } else {
        limit
    };

    // Computed once before the struct literal moves `aggregates` into its field:
    // both `projection` and `emit_exa_types` are emptied on the aggregate sub-path
    // of this shared `spec_template` (see their field comments below).
    let has_aggregates = aggregates.is_some();

    let spec_template = ScanSpec {
        common: CommonScanSpec {
            // This `spec_template` is SHARED between the single-group aggregate sub-path
            // (`aggregates.is_some()`) and the row-scan sub-path. On the aggregate
            // sub-path the scan never reads `projection` (the referenced columns live in
            // `aggregates`; DataFusion prunes the physical Parquet read from the query
            // text), so it is emptied — an inert value that keeps EXPLAIN VIRTUAL
            // accurate (#145). The row-scan sub-path MUST keep its projection: it drives
            // both the EMITS clause and the pushed-down scan, so `proj_cols` is preserved
            // whenever there are no aggregates.
            projection: if has_aggregates {
                Vec::new()
            } else {
                proj_cols.clone()
            },
            filter,
            limit: effective_limit,
            order_by,
            aggregates,
            // Like `projection` above, this field is SHARED via this `spec_template`
            // between the single-group aggregate sub-path and the row-scan sub-path.
            // The aggregate scan emits via the freely-coercing Value path and never
            // reads `emit_exa_types` (matching the grouped branch, which empties it),
            // so it is emptied when `aggregates.is_some()` — an inert value that keeps
            // the EXPLAIN VIRTUAL common blob accurate instead of leaking a full
            // base-table type list (#145, the sibling symptom to `projection`). The
            // row-scan sub-path MUST keep `proj_types`: the scan coerces each emitted
            // Arrow column to the type its declared ExaType accepts before emit_batch,
            // and it is the same list the EMITS clause is built from.
            emit_exa_types: if has_aggregates {
                Vec::new()
            } else {
                proj_types.clone()
            },
            ..base.clone()
        },
        files: vec![],
    };

    let aggregate_types = aggregate_exasol_types(pushdown_req);
    let sql = build_scan_driving_sql(
        &spec_template,
        shards,
        &proj_cols,
        &proj_types,
        effective_limit,
        &col_types,
        &aggregate_types,
        udf_name,
        distribute_udf_name,
    );

    // Row-scan DECLINE path (add-topn-pushdown B6): an ORDER BY was pushed but the
    // shape did not match the bounded top-N (`order_by` empty) — e.g. a sort key
    // that is unprojected or JSON-fallback-typed, or a bare ORDER BY with no LIMIT.
    // Once ORDER_BY_COLUMN is advertised Exasol delegates the ordering and NO LONGER
    // re-applies its own backstop sort/limit on the returned rows, so the adapter
    // reproduces that former backstop as self-contained SQL: wrap the unbounded
    // fan-out in a global ORDER BY (plus the original LIMIT, if any). The per-shard
    // common blob still carries no sort keys and no LIMIT (anti-wrong-truncation
    // invariant, decision [4]); this is the unoptimized correctness restoration, not
    // the bounded per-shard top-N.
    let declined_order_by = has_order_by
        && spec_template.common.order_by.is_empty()
        && spec_template.common.aggregates.is_none();
    let sql = if declined_order_by {
        let keys = parse_order_by_keys(pushdown_req);
        if keys.is_empty() {
            sql
        } else {
            let mut wrapped = format!(
                "SELECT * FROM ({sql}) ORDER BY {}",
                render_order_by_clause(&keys)
            );
            if let Some(n) = limit {
                wrapped.push_str(&format!(" LIMIT {n}"));
            }
            wrapped
        }
    } else {
        sql
    };

    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

/// Build the logical schema (`Vec<LogicalField>`) from an Iceberg current schema.
///
/// Iterates over the top-level struct fields of `schema` and maps each to a
/// `LogicalField` carrying its Iceberg field-id, current name, Arrow type tag,
/// and nullability (required → `false`, optional → `true`).
pub(crate) fn build_logical_schema(schema: &iceberg::spec::Schema) -> Vec<LogicalField> {
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| {
            let arrow_dt = crate::types::mapping::iceberg_type_to_arrow(&f.field_type);
            let arrow_type = crate::types::mapping::arrow_type_to_tag(&arrow_dt);
            LogicalField {
                field_id: f.id,
                name: f.name.clone(),
                arrow_type,
                nullable: !f.required,
                initial_default: encode_initial_default(f),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    // ---------------------------------------------------------------------------
    // ScanSpec GROUP BY — group-key fragments propagated to the scan spec
    // ---------------------------------------------------------------------------

    /// Grouped scan spec carries group-key rendered SQL fragments.
    #[test]
    fn grouped_scan_spec_carries_group_keys() {
        let group_keys = vec!["\"REGION\"".to_string(), "YEAR(\"TS\")".to_string()];
        let spec = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(vec![AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                    arg_expr: None,
                }]),
                group_keys: Some(group_keys.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![FileEntry::new("s3://w/f0.parquet", 1)],
        };
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).expect("must round-trip");
        let keys = back.common.group_keys.expect("group_keys must be present");
        assert_eq!(keys, group_keys, "group_keys must survive spec round-trip");
    }

    /// Scenario: A LIKE-only filter still yields a valid `ScanSpec.filter` (DataFusion
    /// evaluates it) while `to_iceberg_predicate` returns `None` (no file pruning).
    ///
    /// This confirms the correctness invariant: LIKE is not prunable but remains
    /// fully enforced by DataFusion.
    #[test]
    fn like_filter_yields_df_string_and_no_iceberg_predicate() {
        use crate::adapter::iceberg_predicate::to_iceberg_predicate;
        use iceberg::spec::{NestedField, Schema, Type};
        use std::sync::Arc;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "name",
                Type::Primitive(iceberg::spec::PrimitiveType::String),
            ))])
            .build()
            .unwrap();

        let filter_json = serde_json::json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "name"},
            "pattern": {"type": "literal_string", "value": "A%"}
        });

        // DataFusion path must still yield Some (LIKE is translatable to DataFusion SQL).
        let df_filter = render_df_filter_safe(&filter_json);
        assert!(
            df_filter.is_some(),
            "LIKE filter must still produce a DataFusion SQL string: {df_filter:?}"
        );

        // Iceberg path must be None — LIKE is not soundly prunable.
        let iceberg_pred = to_iceberg_predicate(&filter_json, &schema);
        assert!(
            iceberg_pred.is_none(),
            "LIKE filter must produce no Iceberg predicate"
        );
    }

    /// Scenario: Catalog auth props — and the whole catalog block — are never placed
    /// in any scan spec.
    ///
    /// The UDF-boundary secret invariant: auth lives on `ConnectionCreds` and is
    /// consumed only in the planning-layer catalog build. A `ScanSpec` (serialized
    /// for the UDF boundary) must carry no catalog block at all, none of the auth
    /// field NAMES, nor any auth VALUE — the scan UDF never calls the catalog.
    #[test]
    fn scan_spec_carries_no_catalog_block() {
        // Distinctive sentinels: any of these surfacing in the serialized spec is a leak.
        const TOKEN_SENTINEL: &str = "TOKEN_SENTINEL_VALUE";
        const SECRET_SENTINEL: &str = "CLIENT_SECRET_SENTINEL_VALUE";
        const OAUTH_URI_SENTINEL: &str = "https://oauth-uri-sentinel.example/token";
        const SCOPE_SENTINEL: &str = "SCOPE_SENTINEL_VALUE";

        // Build a spec exactly as handle_pushdown does — auth creds exist but are
        // NEVER threaded into ScanSpec (it has no auth fields by construction).
        let spec = ScanSpec {
            common: CommonScanSpec {
                projection: vec!["ID".into(), "NAME".into()],
                filter: Some("(\"ID\" > 10)".into()),
                limit: Some(100),
                emit_exa_types: vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![FileEntry::new(
                "s3://warehouse/db/events/part-00000.parquet",
                1,
            )],
        };

        let json = spec.to_json();

        // The dropped `catalog` block must not appear in the full spec nor the
        // shard-invariant common blob (the scan UDF never touches the catalog).
        assert!(
            !json.contains("catalog"),
            "ScanSpec JSON must not carry a catalog block: {json}"
        );
        assert!(
            !spec.to_common_json().contains("catalog"),
            "common blob must not carry a catalog block: {}",
            spec.to_common_json()
        );

        // No auth field NAMES (planning-layer concepts) in the serialized spec.
        for field in [
            "token",
            "credential",
            "client_id",
            "client_secret",
            "oauth2_server_uri",
            "oauth2-server-uri",
            "scope",
        ] {
            assert!(
                !json.contains(field),
                "ScanSpec JSON must not carry auth field '{field}': {json}"
            );
        }

        // No auth VALUES, even if a future refactor wired creds in by mistake.
        for value in [
            TOKEN_SENTINEL,
            SECRET_SENTINEL,
            OAUTH_URI_SENTINEL,
            SCOPE_SENTINEL,
        ] {
            assert!(
                !json.contains(value),
                "ScanSpec JSON must not carry auth value '{value}': {json}"
            );
        }

        // The storage block carries only the S3 storage credentials, exactly as
        // in the established credential flows.
        assert!(
            json.contains("minioadmin"),
            "storage S3 creds must still be present: {json}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 3.2 — Pushdown spec carries logical schema field-ids
    // ---------------------------------------------------------------------------

    /// Scenario (pushdown-planning): A pushdown request produces a scan spec whose
    /// `logical_schema` carries the expected field-ids, current names, and nullability.
    ///
    /// Builds an in-memory Iceberg schema and verifies that `build_logical_schema`
    /// produces a `Vec<LogicalField>` with the correct field-id, name, arrow_type
    /// tag, and nullable flag for each field. This covers: required field (nullable=false),
    /// optional field (nullable=true), and multiple Iceberg type families.
    #[test]
    fn pushdown_carries_logical_schema_in_common_arg() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;

        // Construct an Iceberg schema with 4 fields covering required, optional,
        // and several type families.
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "score",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::required(
                    3,
                    "label",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "amount",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 18,
                        scale: 4,
                    }),
                )),
            ])
            .build()
            .unwrap();

        let logical = build_logical_schema(&schema);

        assert_eq!(logical.len(), 4, "must carry all 4 fields");

        // Field 1: required Int → nullable=false, arrow_type="int32"
        assert_eq!(logical[0].field_id, 1);
        assert_eq!(logical[0].name, "id");
        assert_eq!(logical[0].arrow_type, "int32");
        assert!(
            !logical[0].nullable,
            "required field must have nullable=false"
        );

        // Field 2: optional Double → nullable=true, arrow_type="float64"
        assert_eq!(logical[1].field_id, 2);
        assert_eq!(logical[1].name, "score");
        assert_eq!(logical[1].arrow_type, "float64");
        assert!(
            logical[1].nullable,
            "optional field must have nullable=true"
        );

        // Field 3: required String → nullable=false, arrow_type="utf8"
        assert_eq!(logical[2].field_id, 3);
        assert_eq!(logical[2].name, "label");
        assert_eq!(logical[2].arrow_type, "utf8");
        assert!(!logical[2].nullable);

        // Field 4: optional Decimal(18,4) → nullable=true, arrow_type="decimal128(18,4)"
        assert_eq!(logical[3].field_id, 4);
        assert_eq!(logical[3].name, "amount");
        assert_eq!(logical[3].arrow_type, "decimal128(18,4)");
        assert!(logical[3].nullable);

        // Verify round-trip through ScanSpec: logical_schema survives JSON serde.
        let spec = ScanSpec {
            common: CommonScanSpec {
                logical_schema: logical.clone(),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.common.logical_schema.len(),
            4,
            "logical_schema must survive ScanSpec JSON round-trip"
        );
        assert_eq!(back.common.logical_schema[0], logical[0]);
        assert_eq!(back.common.logical_schema[3], logical[3]);

        // The logical schema is a shard-invariant field, so it must be carried in the
        // common (arg 0) blob — the scan UDF reads it identically for every shard.
        let common_json = spec.to_common_json();
        let common_back = crate::scan::spec::CommonScanSpec::from_json(&common_json).unwrap();
        assert_eq!(
            common_back.logical_schema, logical,
            "logical_schema must be carried in the common arg"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 3.1 — build_logical_schema encodes the Iceberg initial-default
    // (Iceberg column-projection rule 3), once per query, into the scan spec.
    // ---------------------------------------------------------------------------

    /// The VS encodes each field's Iceberg `initial-default` once per query into
    /// the scan spec: a PRIMITIVE required-with-default and a PRIMITIVE
    /// nullable-with-default each carry their default as the raw scalar text keyed
    /// to the field's Arrow-type tag.
    #[test]
    fn build_logical_schema_encodes_primitive_initial_default() {
        use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                // Required (nullable=false) Long with an initial-default.
                Arc::new(
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
                        .with_initial_default(Literal::long(7)),
                ),
                // Nullable (optional) String with an initial-default.
                Arc::new(
                    NestedField::optional(2, "note", Type::Primitive(PrimitiveType::String))
                        .with_initial_default(Literal::string("hi")),
                ),
            ])
            .build()
            .unwrap();

        let logical = build_logical_schema(&schema);

        assert_eq!(logical.len(), 2);

        // Required-with-default encodes the raw i64 scalar as decimal text.
        assert_eq!(logical[0].field_id, 1);
        assert!(!logical[0].nullable, "required field must be non-nullable");
        assert_eq!(logical[0].arrow_type, "int64");
        assert_eq!(
            logical[0].initial_default.as_deref(),
            Some("7"),
            "required-with-default must encode its default"
        );

        // Nullable-with-default encodes the string value verbatim.
        assert_eq!(logical[1].field_id, 2);
        assert!(logical[1].nullable, "optional field must be nullable");
        assert_eq!(logical[1].arrow_type, "utf8");
        assert_eq!(
            logical[1].initial_default.as_deref(),
            Some("hi"),
            "nullable-with-default must encode its default"
        );
    }

    /// A field with NO `initial-default` encodes no default (`None`).
    #[test]
    fn build_logical_schema_omits_default_for_no_default_field() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "plain",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();

        let logical = build_logical_schema(&schema);

        assert_eq!(logical.len(), 1);
        assert!(
            logical[0].initial_default.is_none(),
            "a field without an initial-default must encode None"
        );
    }

    /// A NON-primitive (struct) `initial-default` encodes NO default: Exasol has no
    /// struct type (it surfaces as JSON-fallback VARCHAR), so the default is dropped
    /// and the column falls through to NULL / required-error downstream — a
    /// deliberate trade-off, not a silent gap.
    #[test]
    fn build_logical_schema_omits_non_primitive_default() {
        use iceberg::spec::{
            Literal, NestedField, PrimitiveType, Schema, Struct, StructType, Type,
        };
        use std::sync::Arc;

        let struct_type = Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
            100,
            "x",
            Type::Primitive(PrimitiveType::Int),
        ))]));
        let struct_default = Literal::Struct(Struct::from_iter([Some(Literal::int(7))]));

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(
                NestedField::optional(1, "meta", struct_type).with_initial_default(struct_default),
            )])
            .build()
            .unwrap();

        let logical = build_logical_schema(&schema);

        assert_eq!(logical.len(), 1);
        assert_eq!(
            logical[0].arrow_type, "utf8",
            "a struct maps to the JSON-fallback utf8 tag"
        );
        assert!(
            logical[0].initial_default.is_none(),
            "a non-primitive struct initial-default must encode NO default"
        );
    }

    /// `write-default` is never read: a field carrying ONLY a `write-default`
    /// (no `initial-default`) encodes `None` — writes are irrelevant to reads.
    #[test]
    fn build_logical_schema_ignores_write_default() {
        use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(
                NestedField::optional(1, "w", Type::Primitive(PrimitiveType::Int))
                    .with_write_default(Literal::int(5)),
            )])
            .build()
            .unwrap();

        let logical = build_logical_schema(&schema);

        assert_eq!(logical.len(), 1);
        assert!(
            logical[0].initial_default.is_none(),
            "write-default must never be read into initial_default"
        );
    }

    /// The encoded default form is credential-free: it is a bare scalar value, so
    /// the serialized `LogicalField` carrying it contains no storage credential.
    #[test]
    fn build_logical_schema_default_encoding_is_credential_free() {
        use iceberg::spec::{Literal, NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(
                NestedField::optional(1, "label", Type::Primitive(PrimitiveType::String))
                    .with_initial_default(Literal::string("plain-default")),
            )])
            .build()
            .unwrap();

        let logical = build_logical_schema(&schema);
        assert_eq!(logical[0].initial_default.as_deref(), Some("plain-default"));

        // Serializing the default carrier introduces no credential material — the
        // encoding is a bare scalar, never a connection/storage blob.
        let json = serde_json::to_string(&logical).unwrap();
        for marker in ["access_key", "secret_key", "session_token", "endpoint"] {
            assert!(
                !json.contains(marker),
                "encoded default carrier must be credential-free, found '{marker}': {json}"
            );
        }
    }

    /// A default-less schema round-trips unchanged: every `LogicalField` carries
    /// `None`, the field is absent from the serialized JSON, and a spec authored
    /// before the field existed deserializes identically (backward-compatible).
    #[test]
    fn build_logical_schema_default_less_spec_round_trips_unchanged() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        let logical = build_logical_schema(&schema);
        assert!(
            logical.iter().all(|f| f.initial_default.is_none()),
            "a default-less schema must encode no defaults"
        );

        let spec = ScanSpec {
            common: CommonScanSpec {
                logical_schema: logical.clone(),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let json = spec.to_json();
        assert!(
            !json.contains("initial_default"),
            "absent defaults must be omitted from JSON: {json}"
        );
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.common.logical_schema, logical,
            "a default-less spec must round-trip unchanged"
        );
    }
}

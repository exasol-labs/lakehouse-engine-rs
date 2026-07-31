use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{
    CatalogProps, CommonScanSpec, FileEntry, LogicalField, NameMappingEntry, ProjectionItem,
    ScanSpec, StorageBackend,
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
    DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, aggregate_exasol_types, apply_type_rewrites,
    extract_all_column_types, extract_limit, extract_projection, order_by_present,
    strip_table_alias,
};
pub use support::{build_fan_out_inner, build_scan_driving_sql, shard_count};

use lakehouse_catalog::{CatalogSession, parse_table_ident};

mod file_resolution;
use file_resolution::{empty_result_sql, encode_initial_default, relativize_shards_to_root};
pub use file_resolution::{resolve_file_list, resolve_table_schema};

mod topn;
use topn::{detect_topn, parse_order_by_keys};

mod single_group_agg;
pub use single_group_agg::{detect_aggregates, ordinary_plans};
use single_group_agg::{has_distinct, is_lone_count_distinct};

mod grouped_agg;
use grouped_agg::group_key_exasol_types;
pub use grouped_agg::{
    GroupedAggregateDetection, GroupedSelectItem, build_grouped_aggregate_scan_sql,
    detect_group_by_aggregates, validate_agg_col_types,
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
    storage: &StorageBackend,
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
            // Parse-before-config (intent-fidelity): validate every involved-table
            // identifier at the pushdown seam BEFORE `CatalogSession::resolve` issues
            // the `/v1/config` lookup, so a malformed identifier issues zero catalog
            // HTTP and returns the same parse error. Building the session here (once)
            // and threading `&session` into every leg is what makes a per-leg rebuild
            // structurally inexpressible.
            for leaf in &join.tables {
                parse_table_ident(&leaf.iceberg_ident)?;
            }
            let session = CatalogSession::resolve(catalog_uri, &catalog.warehouse, creds).await?;
            return plan_join(
                request,
                &pushdown_req,
                &join,
                &session,
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

    // Single-table chokepoint (issue #193): strip every `tableAlias` here, after the
    // join gate (which returned above on the original, alias-carrying request) and
    // before the first read of `pushdown_req` below, so the shadowing rebind covers
    // every downstream render. See `strip_table_alias`'s doc comment for why.
    let pushdown_req = strip_table_alias(&pushdown_req);

    let (proj_cols, proj_types, projection_widened) = extract_projection(request, &pushdown_req)?;

    let filter_json_raw = pushdown_req.get("filter").filter(|f| !f.is_null());

    let col_types = extract_all_column_types(request);

    // The rewritten filter feeds ONLY the DataFusion-bound scan filter;
    // `filter_json_raw` itself is left completely unmodified for the later
    // `resolve_file_list` Iceberg-level pruning call below, which must see the
    // original, un-rewritten predicate tree.
    let filter = filter_json_raw
        .and_then(|f| apply_type_rewrites(f, &col_types))
        .and_then(|f| render_df_filter_safe(&f));

    let limit = extract_limit(&pushdown_req);

    // Whether Exasol pushed an ORDER BY. Drives the anti-wrong-truncation guard
    // (decision [4]): a limit is withheld from every ORDER-BY-carrying request the
    // adapter does not match as a bounded top-N, so a bare per-shard/outer LIMIT is
    // never emitted ahead of an ordering the adapter did not itself render.
    let has_order_by = order_by_present(&pushdown_req);

    parse_table_ident(&catalog.table)?;

    // Resolve the file list exactly once, on one session built once for this request.
    // The returned `effective_storage` carries vended STS creds when
    // use_vended_credentials is true; otherwise it equals the static `storage` passed
    // in. Every per-shard ScanSpec uses this storage. filter_json_raw is forwarded for
    // Iceberg-level file pruning; ScanSpec.filter (DataFusion SQL string) is set
    // separately above and left completely unchanged.
    let session = CatalogSession::resolve(catalog_uri, &catalog.warehouse, creds).await?;
    let (files, effective_storage, logical_schema, table_root, name_mapping) =
        resolve_file_list(&session, catalog, storage, creds, filter_json_raw).await?;
    let storage = &effective_storage;

    if files.is_empty() {
        return empty_result_sql(
            &pushdown_req,
            &proj_cols,
            &proj_types,
            projection_widened,
            &col_types,
        );
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
        projection_widened,
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
///
/// `projection_widened` is `extract_projection`'s widening signal for the
/// `proj_cols`/`proj_types` pair passed alongside it: `true` means they are the full
/// base row rather than one item per select-list item (#196).
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_dispatch_sql(
    request: &Json,
    pushdown_req: &Json,
    mut proj_cols: Vec<ProjectionItem>,
    mut proj_types: Vec<String>,
    projection_widened: bool,
    col_types: Vec<(String, String)>,
    filter: Option<String>,
    limit: Option<u64>,
    has_order_by: bool,
    shards: &[Vec<FileEntry>],
    table_root: String,
    logical_schema: Vec<LogicalField>,
    name_mapping: Vec<NameMappingEntry>,
    storage: &StorageBackend,
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
    // gates, and the grouped HAVING merge-render — whose failure is a route to
    // `GroupByWrapper`, not an error — all live in the classifier; each arm below
    // renders ONLY its own SQL. The fall-through arms
    // (ordinary single-group aggregate, row scan) yield the shared `aggregates`
    // input the row-scan/partial-aggregate rendering below consumes (`Some` ordinary
    // plans for the aggregate sub-path, `None` for a row scan).
    let aggregates = match classify_request_shape(pushdown_req, &col_types) {
        RequestShape::Grouped {
            detection,
            having,
            order_by: grouped_order_by,
        } => {
            let GroupedAggregateDetection {
                group_keys,
                plans: grouped_agg_plans,
                plan_types: grouped_agg_types,
                select_items,
            } = detection;
            // `having` arrives ALREADY rendered over the merge decomposition (each
            // aggregate reference rewritten to its merged expression, SUM(score) →
            // SUM("PARTIAL_sum_0")) — the classifier renders it, because a HAVING
            // that does not render routes to `GroupByWrapper` instead of reaching
            // this arm.

            // `grouped_order_by` likewise arrives ALREADY RESOLVED over the merge
            // decomposition (a group key as its positional output ordinal, an
            // aggregate as its merged PARTIAL_* expression). Once ORDER_BY_COLUMN is
            // advertised Exasol delegates any ORDER BY on the grouped output and NO
            // LONGER re-sorts the rows the adapter returns (add-topn-pushdown B6), so
            // the merge SQL must render its own explicit final ORDER BY — and an
            // ordering the merge cannot express routes to `GroupByWrapper` instead of
            // reaching this arm (issue #198).

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
                support::extract_offset(pushdown_req),
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
            // distinct set → a wrong count). The base spec carries no projection/
            // aggregates/limit/order-by/distinct: the wrapper builder derives the
            // fan-out from it.
            //
            // No offset ever reaches this site (fact 6, issue #191): Exasol rejects
            // an OFFSET in ANY ungrouped aggregated select with sqlCode 42000 before
            // the adapter is consulted, so `build_count_distinct_scan_sql` takes no
            // offset parameter and this `debug_assert!` documents the invariant
            // rather than guarding against something reachable (it compiles out of
            // the release-profile `.so`; the live backstop is the e2e sqlCode 42000
            // assertion).
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
                let sql = support::build_count_distinct_scan_sql(
                    &base_spec,
                    shards,
                    &items,
                    &col_types,
                    limit,
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
        RequestShape::RowScan => {
            // A real (non-empty) selectList that `project_columns` could not render
            // item-for-item — e.g. `string_function_arg_type_guard` declining a
            // select-list item's non-coercible argument type (issue #210), or a
            // declared type Exasol rejects as an EMITS output (#234) — widens to the
            // base-row projection (every source column, bare) instead of one item per
            // select-list item. Exasol's pushdown validation is positional: the
            // returned SQL must carry exactly the selectList's columns, in its order,
            // with its declared types, or it hard-errors — `04000` "Expected number of
            // columns is N but pushdown query has M" when the counts differ, and
            // `04000` "Data type mismatch in column number K" when they coincide but
            // the types do not. Route a widened projection to the same qualified
            // single-table wrapper the `GroupByWrapper` and multi-`DISTINCT` declines
            // above use: it renders the exact original select list (the declined item
            // included) as native Exasol SQL over a raw, referenced-column-only scan,
            // so Exasol evaluates the item itself.
            //
            // The routing decision is `project_columns`'s OWN widening signal, piped
            // here as `projection_widened` — never a comparison of `proj_cols.len()`
            // against the selectList's item count. That count comparison, which this
            // replaces, was a lossy re-derivation blind in two directions (#196): it
            // missed every widening whose base-row column count happens to equal the
            // select-list arity (reproduced live — a 10-item select list over a
            // 10-column table returned `04000` "Data type mismatch in column number
            // 10"), and being local to this arm it never ran on the empty-result or
            // broadcast-join paths, which consume the same widened projection.
            if projection_widened {
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
            None
        }
    };

    // Ordered top-N applies ONLY to the pure row-scan path (no aggregates). On a
    // match the sort keys are carried into the common blob (per-shard bounded sort)
    // and the outer wrapper renders `ORDER BY … LIMIT n`.
    //
    // `proj_cols` is passed here EXACTLY as `extract_projection` derived it: the
    // declined-path sort-key extension below deliberately runs AFTER this call
    // (issues #225 / #189, decision [2]). Extending first — as the removed #190
    // full-base-row widening did — would let an appended column make an otherwise
    // ineligible shape match the bounded top-N, whose rendering path emits
    // `proj_cols` directly as the FINAL visible EMITS with no wrapping SELECT. A
    // hidden column would then leak into the result and reintroduce the very arity
    // mismatch this fix removes.
    let topn = if aggregates.is_none() {
        detect_topn(request, pushdown_req, &proj_cols, &logical_schema)
    } else {
        None
    };
    let order_by = topn.unwrap_or_default();

    // Fact 5 (issue #191): `extract_offset(pushdown_req) > 0` NEVER arrives without a
    // non-empty `orderBy` — Exasol's grammar requires an ORDER BY for a pushed OFFSET,
    // and withholds `limit` entirely when it cannot delegate the ordering it cannot
    // express (live capture, plan.md rows 1-13). The two guards below are CHAINED on
    // `has_order_by`, not independent: a non-zero offset declines the bounded top-N
    // (`detect_topn`, above) so `order_by` is empty here, which is what NULLS
    // `effective_limit` next and keeps S3 (`build_row_scan_sql`) from ever rendering a
    // `LIMIT`/`OFFSET` with no `ORDER BY` beside it. This `debug_assert!` documents the
    // invariant only — it compiles out of the release-profile `.so`; the live backstop
    // is Task 8's unrenderable-ordering e2e canary.
    debug_assert!(
        support::extract_offset(pushdown_req) == 0 || has_order_by,
        "fact 5: a non-zero offset must never arrive without a non-empty orderBy"
    );

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

    // Row-scan DECLINE path, part 1 of 2 (issues #225 / #189): an ORDER BY was pushed
    // but the shape did not match the bounded top-N (`order_by` empty). Such a sort
    // key need not be emitted by the derived projection at all — it may name a
    // different column, or be referenced only INSIDE a projected expression — so the
    // scan's emitted-column set is EXTENDED with each missing sort-key column as a
    // HIDDEN column. Part 2 (the wrapper, below) names only the visible columns
    // explicitly, so every hidden column is dropped from the query's result again.
    //
    // `visible_count` is the number of projection items `extract_projection` already
    // derived before this extension runs (`proj_cols.len()` at this point) — NOT
    // necessarily the raw select-list arity. A widened projection never reaches this
    // point at all: the `RowScan` arm above returns to the qualified single-table
    // wrapper on `projection_widened`, ahead of both `detect_topn` and this extension
    // (#196), so `proj_cols` here is always the per-select-list-item derivation.
    //
    // Position is load-bearing on BOTH sides. AFTER `detect_topn` (see its comment
    // above), and BEFORE the `spec_template` literal below: that literal derives the
    // common blob's `projection` and `emit_exa_types` from these same two vectors that
    // `build_scan_driving_sql` renders the EMITS clause from, so extending afterwards
    // would declare a hidden column in EMITS that the scan spec never projects — and
    // the UDF would never emit it.
    let visible_count = proj_cols.len();
    let declined_order_by = has_order_by && order_by.is_empty() && aggregates.is_none();
    let declined_sort_keys = if declined_order_by {
        let keys = parse_order_by_keys(pushdown_req);
        // Correctness-safety guard (issue #198): Exasol delegated this ordering and no
        // longer re-sorts, so a key that renders nothing must decline HERE — before the
        // projection is extended and before any SQL is built. Rendering only the
        // surviving keys would answer a different query than the one asked, silently.
        topn::ensure_every_sort_key_renders(&keys)?;
        topn::extend_projection_with_sort_keys(&mut proj_cols, &mut proj_types, &keys, &col_types);
        keys
    } else {
        Vec::new()
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
    // Fact 6 (issue #191): when this call drives the ordinary single-group
    // aggregate merge (`has_aggregates`, i.e. `build_aggregate_scan_sql`), no
    // offset can ever reach it — Exasol rejects an OFFSET in ANY ungrouped
    // aggregated select with sqlCode 42000 before the adapter is consulted, so
    // `build_aggregate_scan_sql` takes no offset parameter. This `debug_assert!`
    // documents that unreachability rather than guarding against it (it compiles
    // out of the release-profile `.so`; the live backstop is the e2e sqlCode
    // 42000 assertion). It says nothing about the row-scan sub-path this same
    // call also drives when `has_aggregates` is false.
    debug_assert!(
        !has_aggregates || support::extract_offset(pushdown_req) == 0,
        "fact 6: Exasol rejects OFFSET in an ungrouped aggregated select \
         (sqlCode 42000) before the adapter is consulted, so the single-group \
         aggregate merge can never see a non-zero offset"
    );
    let sql = build_scan_driving_sql(
        &spec_template,
        shards,
        &proj_cols,
        &proj_types,
        effective_limit,
        limit,
        &col_types,
        &aggregate_types,
        udf_name,
        distribute_udf_name,
    );

    // Row-scan DECLINE path, part 2 of 2 (add-topn-pushdown B6; issues #225 / #189).
    // Once ORDER_BY_COLUMN is advertised Exasol delegates the ordering and NO LONGER
    // re-applies its own backstop sort/limit on the returned rows, so the adapter
    // reproduces that former backstop as self-contained SQL: wrap the unbounded
    // fan-out in a global ORDER BY (plus the original LIMIT, if any).
    //
    // The wrapper names the FIRST `visible_count` projection items explicitly rather
    // than `SELECT *`, so any sort-key column part 1 appended above stays HIDDEN — the
    // outer ORDER BY binds against it, and it is dropped from the query's result. That
    // is what keeps the returned column count equal to the derived projection's, which
    // Exasol validates POSITIONALLY against the original select list (a wider row is
    // rejected outright with `sqlCode 04000`, never re-projected).
    //
    // The per-shard common blob still carries no sort keys and no LIMIT
    // (anti-wrong-truncation invariant, decision [4]); this is the unoptimized
    // correctness restoration, not the bounded per-shard top-N.
    let sql = if declined_order_by {
        topn::wrap_declined_order_by(
            &sql,
            &proj_cols,
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
    use crate::scan::spec::{CommonScanSpec, FileEntry, ScanSpec, StorageProps};

    // ---------------------------------------------------------------------------
    // Task 4.4 — catalog-auth secrets never in ScanSpec
    //
    // Relocated from the former `pushdown/credentials.rs` when that module moved
    // into `lakehouse-catalog`: the assertion is about the ENGINE's scan-spec
    // serialization, and the catalog crate must not name `ScanSpec`,
    // `CommonScanSpec`, or `FileEntry`. The four vended sentinels it reads are
    // re-declared here with the same literal values the crate's own
    // `test_support` uses, so both sides' assertions stay comparable.
    // ---------------------------------------------------------------------------

    const VENDED_AK: &str = "VENDED_AK_SENTINEL";
    const VENDED_SK: &str = "VENDED_SK_SENTINEL";
    const VENDED_TOK: &str = "VENDED_TOKEN_SENTINEL";
    const VENDED_REGION: &str = "eu-west-2";

    /// Scenario: Catalog auth props are never placed in any scan spec, even when
    /// `use_vended_credentials` is enabled and vended creds are in the storage.
    ///
    /// The ScanSpec must carry ONLY S3 storage credentials (vended or static).
    /// Auth fields (`token`, `client_secret`, etc.) must never appear in the JSON.
    #[test]
    fn catalog_auth_secrets_never_in_scan_spec_with_vending() {
        // Build a spec with VENDED storage credentials (simulating what
        // resolve_file_list returns after vended extraction).
        let vended_storage = StorageBackend::S3(StorageProps {
            endpoint: "https://s3.amazonaws.com".into(),
            region: VENDED_REGION.into(),
            access_key: VENDED_AK.into(),
            secret_key: VENDED_SK.into(),
            session_token: Some(VENDED_TOK.into()),
            path_style: false,
            ..Default::default()
        });

        let spec = ScanSpec {
            common: CommonScanSpec {
                projection: vec!["ID".into()],
                emit_exa_types: vec!["DECIMAL(20,0)".into()],
                storage: vended_storage,
                ..Default::default()
            },
            files: vec![FileEntry::new(
                "s3://warehouse/db/events/part-00000.parquet",
                1,
            )],
        };

        let json = spec.to_json();

        // Auth field NAMES must never appear as JSON keys in the serialized spec.
        // Check for the exact key pattern `"<field>":` to avoid false-positives
        // from legitimate substrings (e.g. `"session_token"` contains `"token"`).
        for field in [
            "\"token\":",
            "\"credential\":",
            "\"client_id\":",
            "\"client_secret\":",
            "\"oauth2_server_uri\":",
            "\"oauth2-server-uri\":",
            // scope is too short and appears in storage endpoint strings, so it
            // is checked by key name only, above, not by a sentinel value.
        ] {
            assert!(
                !json.contains(field),
                "ScanSpec JSON must not carry auth field key '{field}': {json}"
            );
        }

        // Vended credentials MUST be present in the storage block.
        assert!(
            json.contains(VENDED_AK),
            "vended access_key must be in storage: {json}"
        );
        assert!(
            json.contains(VENDED_TOK),
            "vended session_token must be in storage: {json}"
        );
    }

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

    /// Wiring sanity: the WHERE-clause filter chain composes
    /// `string_function_arg_type_guard` and `rewrite_decimal_stringifications` between
    /// `like_subject_type_guard` and `render_df_filter_safe`, so a
    /// `LENGTH(<DECIMAL column>) > 5` predicate renders with Exasol's trailing-zero-trim
    /// form wrapping the column EXACTLY ONCE (issue #211's headline COUNT-divergence
    /// repro) — NOT a bare `character_length("C_DECIMAL_A")` over DataFusion's untrimmed
    /// decimal→string, and NOT a double-wrapped trim. `string_function_arg_type_guard`
    /// coerces `LENGTH`'s bare DECIMAL argument into a `decimal_to_varchar_exasol` node
    /// first, so by the time `rewrite_decimal_stringifications` runs, the argument is no
    /// longer a bare column and its own CONCAT/LENGTH-specific DECIMAL handling is a
    /// no-op — a composition `string_function_arg_type_guard`'s own unit tests cannot
    /// observe, since `rewrite_decimal_stringifications` is only chained after it here.
    /// Calls the same pipeline function `handle_pushdown` calls
    /// (`apply_type_rewrites`, then `render_df_filter_safe`) on the
    /// DataFusion-bound filter tree.
    #[test]
    fn where_filter_decimal_stringification_rewritten_to_trim() {
        let col_types = vec![("C_DECIMAL_A".to_string(), "DECIMAL(10,2)".to_string())];
        let filter_json = serde_json::json!({
            "type": "predicate_greater",
            "left": {
                "type": "function_scalar",
                "name": "LENGTH",
                "arguments": [{"type": "column", "name": "c_decimal_a"}]
            },
            "right": {"type": "literal_exactnumeric", "value": 5}
        });

        let rendered = Some(&filter_json)
            .and_then(|f| apply_type_rewrites(f, &col_types))
            .and_then(|f| render_df_filter_safe(&f))
            .expect("LENGTH(decimal) > 5 must render to a DataFusion filter");

        let trim_wrapper = "regexp_replace(regexp_replace(CAST(";
        assert_eq!(
            rendered.matches(trim_wrapper).count(),
            1,
            "the rewritten filter must carry the Exasol decimal-trim form EXACTLY ONCE \
             (string-fn guard wraps it, decimal rewrite must then no-op): {rendered}"
        );
        assert!(
            !rendered.contains(r#"character_length("C_DECIMAL_A")"#),
            "the filter must NOT stringify the bare decimal column untrimmed: {rendered}"
        );
    }

    /// Exhaustive coverage: a DECIMAL column in a NON-stringifying WHERE
    /// filter context (`c_decimal_a > 5`, a `predicate_greater` — not a stringifier)
    /// renders EXACTLY as before this fix through the same pipeline function
    /// (`apply_type_rewrites`) as
    /// `where_filter_decimal_stringification_rewritten_to_trim` — the DECIMAL column
    /// stays a bare, unwrapped column reference, proving the WHERE-path wiring doesn't
    /// over-wrap a non-stringifying context. `predicate_greater` is not a
    /// `function_scalar`, so `string_function_arg_type_guard` has nothing to dispatch on
    /// here and the rendering is byte-identical to before this guard was wired in.
    #[test]
    fn filter_decimal_comparison_not_rewritten() {
        let col_types = vec![("C_DECIMAL_A".to_string(), "DECIMAL(10,2)".to_string())];
        let filter_json = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "c_decimal_a"},
            "right": {"type": "literal_exactnumeric", "value": 5}
        });

        let rendered = Some(&filter_json)
            .and_then(|f| apply_type_rewrites(f, &col_types))
            .and_then(|f| render_df_filter_safe(&f))
            .expect("c_decimal_a > 5 must render to a DataFusion filter");

        assert_eq!(
            rendered, r#"("C_DECIMAL_A" > 5)"#,
            "a DECIMAL column in a comparison must stay a bare, unwrapped column reference: {rendered}"
        );
        assert!(
            !rendered.contains("regexp_replace"),
            "a non-stringifying filter context must not be trimmed: {rendered}"
        );
    }

    /// `UPPER(c_decimal_a) = 'X'` is a `predicate_equal`, whose `function_scalar` sits
    /// under `left`. `string_function_arg_type_guard`'s post-order recursion — sharing
    /// `rewrite_expr_tree`'s broad curated field list with `rewrite_decimal_stringifications`
    /// — reaches it there, coercing the DECIMAL argument into the trimmed
    /// `decimal_to_varchar_exasol` form through the same pipeline function
    /// `handle_pushdown` calls (issue #210).
    #[test]
    fn where_filter_string_fn_under_comparison_predicate_coerced() {
        let col_types = vec![("C_DECIMAL_A".to_string(), "DECIMAL(10,2)".to_string())];
        let filter_json = serde_json::json!({
            "type": "predicate_equal",
            "left": {
                "type": "function_scalar",
                "name": "UPPER",
                "arguments": [{"type": "column", "name": "c_decimal_a"}]
            },
            "right": {"type": "literal_string", "value": "X"}
        });

        let rendered = Some(&filter_json)
            .and_then(|f| apply_type_rewrites(f, &col_types))
            .and_then(|f| render_df_filter_safe(&f))
            .expect("UPPER(decimal) = 'X' must render to a DataFusion filter");

        assert!(
            rendered.contains("regexp_replace(regexp_replace(CAST("),
            "the DECIMAL argument nested under predicate_equal's left must be coerced \
             into the Exasol decimal-trim form: {rendered}"
        );
    }

    /// `UPPER(c_double) = 'X'` must decline through the same pipeline function
    /// `handle_pushdown` calls: DOUBLE PRECISION has no safe cast-to-text form that
    /// matches Exasol's own conversion (same reasoning as `guard_like_subject`'s
    /// BOOLEAN/DOUBLE/TIMESTAMP declines), so the whole filter is omitted rather than
    /// pushed with a possibly-wrong text comparison — Exasol evaluates the predicate
    /// natively instead (issue #210).
    #[test]
    fn where_filter_string_fn_over_double_declines() {
        let col_types = vec![("C_DOUBLE_A".to_string(), "DOUBLE PRECISION".to_string())];
        let filter_json = serde_json::json!({
            "type": "predicate_equal",
            "left": {
                "type": "function_scalar",
                "name": "UPPER",
                "arguments": [{"type": "column", "name": "c_double_a"}]
            },
            "right": {"type": "literal_string", "value": "X"}
        });

        let rendered = Some(&filter_json)
            .and_then(|f| apply_type_rewrites(f, &col_types))
            .and_then(|f| render_df_filter_safe(&f));

        assert!(
            rendered.is_none(),
            "UPPER over a DOUBLE PRECISION column must decline the whole filter, \
             not push a possibly-wrong text comparison: {rendered:?}"
        );
    }

    /// `UPPER(c_decimal_a) LIKE '1%'` proves the new guard's coercion reaches INSIDE a
    /// LIKE subject that `like_subject_type_guard`'s own `guard_like_subject` leaves
    /// completely untouched: the LIKE subject here is a `function_scalar` (`UPPER`), not
    /// a bare `column`, so `guard_like_subject`'s bare-column dispatch has nothing to do
    /// and passes the node through unchanged. `string_function_arg_type_guard` then
    /// coerces the DECIMAL argument nested inside that same `UPPER` call (issue #210).
    #[test]
    fn where_filter_upper_decimal_inside_like_subject_coerced() {
        let col_types = vec![("C_DECIMAL_A".to_string(), "DECIMAL(10,2)".to_string())];
        let filter_json = serde_json::json!({
            "type": "predicate_like",
            "expression": {
                "type": "function_scalar",
                "name": "UPPER",
                "arguments": [{"type": "column", "name": "c_decimal_a"}]
            },
            "pattern": {"type": "literal_string", "value": "1%"}
        });

        let rendered = Some(&filter_json)
            .and_then(|f| apply_type_rewrites(f, &col_types))
            .and_then(|f| render_df_filter_safe(&f))
            .expect("UPPER(decimal) LIKE '1%' must render to a DataFusion filter");

        assert!(
            rendered.contains("regexp_replace(regexp_replace(CAST("),
            "the DECIMAL argument nested inside the LIKE subject's UPPER call must be \
             coerced into the Exasol decimal-trim form, even though guard_like_subject \
             itself leaves this non-bare-column LIKE subject untouched: {rendered}"
        );
    }

    /// Regression (#207 blind spot), through the same pipeline function
    /// `handle_pushdown` calls: a DECIMAL-typed LIKE buried inside a
    /// `function_scalar_case`'s `arguments`, itself nested under `predicate_equal`'s
    /// `left`, must decline the whole filter — a `LIKE` at this non-junction position
    /// is type-guarded like any other.
    #[test]
    fn where_filter_like_decimal_inside_case_declines_whole_filter() {
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(9,2)".to_string())];
        let filter_json = serde_json::json!({
            "type": "predicate_equal",
            "left": {
                "type": "function_scalar_case",
                "name": "CASE",
                "arguments": [
                    {
                        "type": "predicate_like",
                        "expression": {"type": "column", "name": "amount"},
                        "pattern": {"type": "literal_string", "value": "9%"}
                    }
                ],
                "results": [
                    {"type": "literal_exactnumeric", "value": 1},
                    {"type": "literal_exactnumeric", "value": 0}
                ]
            },
            "right": {"type": "literal_exactnumeric", "value": 1}
        });

        let rendered = Some(&filter_json)
            .and_then(|f| apply_type_rewrites(f, &col_types))
            .and_then(|f| render_df_filter_safe(&f));

        assert!(
            rendered.is_none(),
            "a DECIMAL LIKE buried inside a function_scalar_case under predicate_equal's \
             left must decline the whole filter through the full wired chain, not push a \
             possibly-wrong native comparison: {rendered:?}"
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

    // ---------------------------------------------------------------------------
    // Declined-ORDER-BY hidden sort-key columns (issues #225 / #189)
    // ---------------------------------------------------------------------------

    /// The fixed four-column `EVENTS` universe every guard test projects against
    /// (mirrors `dispatch_golden`'s `base_col_types`).
    fn guard_col_types() -> Vec<(String, String)> {
        vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
            ("AMOUNT".to_string(), "DECIMAL(18,2)".to_string()),
            ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ]
    }

    /// Wrap a `pushdownRequest` body with the fixed `EVENTS` `involvedTables` block
    /// (mirrors `dispatch_golden::events_request`).
    fn guard_events_request(pushdown_req: Json) -> Json {
        serde_json::json!({
            "involvedTables": [{
                "name": "EVENTS",
                "columns": [
                    {"name": "REGION", "dataType": {"type": "varchar", "size": 2000000}},
                    {"name": "NAME", "dataType": {"type": "varchar", "size": 2000000}},
                    {"name": "AMOUNT", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                ],
            }],
            "pushdownRequest": pushdown_req,
        })
    }

    /// Drive `build_dispatch_sql` — the real dispatcher, exactly as `dispatch_golden`
    /// exercises it — for `request`/`proj_cols`/`proj_types`, returning the `sql`
    /// field of its pushdown response. `has_order_by` is always `true`: every guard
    /// test pushes an ORDER BY.
    ///
    /// `projection_widened` is `extract_projection`'s widening signal for the
    /// `proj_cols`/`proj_types` pair — the flag the dispatcher routes on (#196). The
    /// declined-`ORDER BY` guard tests all pass `false`; the two widening-routing
    /// tests pass the same inputs under both values.
    fn guard_dispatch_sql(
        request: &Json,
        proj_cols: Vec<ProjectionItem>,
        proj_types: Vec<String>,
        projection_widened: bool,
        limit: Option<u64>,
        logical_schema: Vec<LogicalField>,
    ) -> String {
        let result = guard_dispatch_result(
            request,
            proj_cols,
            proj_types,
            projection_widened,
            limit,
            logical_schema,
        )
        .expect("build_dispatch_sql must succeed for this declined-ORDER-BY fixture");
        result["sql"]
            .as_str()
            .expect("pushdown response must carry a sql field")
            .to_string()
    }

    /// [`guard_dispatch_sql`] WITHOUT the success expectation, for the decline
    /// assertions: an unrenderable pushed sort key is a `User` error, not SQL.
    ///
    /// `has_order_by` is DERIVED here via the production `order_by_present` rather
    /// than hardcoded, so a fixture carrying no `orderBy` exercises the real
    /// non-declined route.
    fn guard_dispatch_result(
        request: &Json,
        proj_cols: Vec<ProjectionItem>,
        proj_types: Vec<String>,
        projection_widened: bool,
        limit: Option<u64>,
        logical_schema: Vec<LogicalField>,
    ) -> Result<Json, UdfError> {
        let pushdown_req = pd(request);
        let has_order_by = order_by_present(&pushdown_req);
        build_dispatch_sql(
            request,
            &pushdown_req,
            proj_cols,
            proj_types,
            projection_widened,
            guard_col_types(),
            None,
            limit,
            has_order_by,
            &[vec![FileEntry::new("data/part-0.parquet", 1_000)]],
            "s3://warehouse/db/events".to_string(),
            logical_schema,
            Vec::new(),
            &sample_storage(),
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            4,
            8192,
            2,
            0.6,
            200,
            8,
        )
    }

    /// Scenario (pushdown-planning-capability-extensions, issues #225 / #189): a
    /// literal-only select list (`SELECT 1 FROM EVENTS`) with an `ORDER BY` on a column
    /// the derived projection does not emit (`NAME`) APPENDS that sort key to the scan
    /// as a HIDDEN column, and the wrapper names only the visible item explicitly.
    ///
    /// This replaces the former full-base-row widening (issue #190), which forced the
    /// scan's emitted set and the query's visible set equal and therefore returned all
    /// four base columns where Exasol positionally expects the derived projection's one
    /// — `sqlCode 04000 "Expected number of columns is 1 but pushdown query has N"`
    /// (#225). The `REGION` / `AMOUNT` / `"ID"` absence assertions are what pin that.
    ///
    /// `logical_schema` is deliberately EMPTY so `detect_topn` declines regardless of
    /// the projection (it requires a logical-schema entry per sort key), isolating the
    /// extension + wrapper shape from the top-N-match decision. That also makes this
    /// test order-blind by construction — the extend-after-`detect_topn` invariant is
    /// pinned separately by `declined_order_by_extension_runs_after_topn_detection`.
    #[test]
    fn declined_order_by_appends_unprojected_sort_key_as_hidden_column() {
        let request = guard_events_request(serde_json::json!({
            "selectList": [{"type": "literal_exactnumeric", "value": 1}],
            "selectListDataTypes": [{"type": "decimal", "precision": 1, "scale": 0}],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "name": "NAME"},
                "isAscending": true,
                "nullsLast": true
            }],
            "limit": {"numElements": 10}
        }));
        let proj_cols = vec![ProjectionItem::Expr {
            expr: "1".to_string(),
        }];
        let proj_types = vec!["DECIMAL(1,0)".to_string()];

        let sql = guard_dispatch_sql(&request, proj_cols, proj_types, false, Some(10), Vec::new());

        // The scan spec APPENDS the sort key AFTER the original expression item, so the
        // per-shard scan actually emits the column the outer ORDER BY binds against.
        assert!(
            sql.contains(r#""projection":[{"expr":"1"},"NAME"]"#),
            "sort key NAME must be APPENDED to the derived projection: {sql}"
        );
        assert!(
            sql.contains(r#"EMITS ("_LH_PROJ_0" DECIMAL(1,0), "NAME" VARCHAR(2000000))"#),
            "EMITS must carry the visible expression column plus the hidden sort key: {sql}"
        );
        // One visible column, matching the one-item derived projection: the wrapper's
        // list is joined immediately ahead of ` FROM (`, so this pins the exact arity.
        assert!(
            sql.contains(r#"SELECT "_LH_PROJ_0" FROM ("#),
            "the wrapper must name ONLY the visible projection item: {sql}"
        );
        assert!(
            sql.contains(r#"ORDER BY "NAME""#),
            "the wrapper's outer ORDER BY must bind the hidden sort key: {sql}"
        );
        assert!(
            !sql.contains("REGION") && !sql.contains("AMOUNT") && !sql.contains("\"ID\""),
            "the projection must NOT widen to the full base row: {sql}"
        );
    }

    /// Scenario (pushdown-planning-capability-extensions, issues #225 / #189), the
    /// bare-column shape: `SELECT name FROM EVENTS ORDER BY id` — one bare-projected
    /// column, an `ORDER BY` on a DIFFERENT unprojected column, no `LIMIT`.
    ///
    /// The scan's emitted set and the query's visible set are two different sets:
    /// `"ID"` is EMITTED (so the outer `ORDER BY` can bind it) yet absent from the
    /// visible select list, so the returned arity stays 1 — what Exasol validates
    /// positionally. `SELECT *` would return 2 and be rejected with `04000`.
    ///
    /// The absent `LIMIT` is what makes `detect_topn` decline here (a top-N needs a
    /// bound), so no `logical_schema` entry is required.
    #[test]
    fn declined_order_by_wrapper_selects_only_original_select_list() {
        let request = guard_events_request(serde_json::json!({
            "selectList": [{"type": "column", "name": "NAME"}],
            "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "name": "ID"},
                "isAscending": true,
                "nullsLast": true
            }]
        }));
        let proj_cols = vec![ProjectionItem::Column("NAME".to_string())];
        let proj_types = vec!["VARCHAR(2000000)".to_string()];

        let sql = guard_dispatch_sql(&request, proj_cols, proj_types, false, None, Vec::new());

        assert!(
            sql.contains(r#"SELECT "NAME" FROM ("#),
            "the wrapper must name exactly the one derived projection item: {sql}"
        );
        assert!(
            emits_clause(&sql).contains("\"ID\""),
            "the scan must EMIT the hidden sort key: {}",
            emits_clause(&sql)
        );
        assert!(
            !outer_select_list(&sql).contains("\"ID\""),
            "the hidden sort key must NOT be visible in the outer select list: {}",
            outer_select_list(&sql)
        );
        assert!(
            sql.contains(r#"ORDER BY "ID""#),
            "the wrapper's outer ORDER BY must bind the hidden sort key: {sql}"
        );
        assert!(
            !sql.contains("SELECT *"),
            "the wrapper must never fall back to SELECT * over the wider emitted row: {sql}"
        );
    }

    /// Scenario (pushdown-planning-capability-extensions): hidden sort-key columns are
    /// appended AT MOST ONCE. `ORDER BY name, id, name, id` over a projection that
    /// already bare-projects `NAME` exercises BOTH dedupe paths in one fixture:
    /// `NAME` is already emitted so it is never appended, and `ID` — named by two
    /// sort keys — is appended exactly ONCE, because the membership test re-scans
    /// `proj_cols` as it grows. A repeated EMITS identifier is a duplicate-column
    /// error, so "not twice" is the assertion that matters.
    #[test]
    fn declined_order_by_dedupes_repeated_and_projected_sort_keys() {
        let sort_key = |name: &str| {
            serde_json::json!({
                "type": "order_by_element",
                "expression": {"type": "column", "name": name},
                "isAscending": true,
                "nullsLast": true
            })
        };
        let request = guard_events_request(serde_json::json!({
            "selectList": [{"type": "column", "name": "NAME"}],
            "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
            "orderBy": [
                sort_key("NAME"),
                sort_key("ID"),
                sort_key("NAME"),
                sort_key("ID"),
            ]
        }));
        let proj_cols = vec![ProjectionItem::Column("NAME".to_string())];
        let proj_types = vec!["VARCHAR(2000000)".to_string()];

        let sql = guard_dispatch_sql(&request, proj_cols, proj_types, false, None, Vec::new());

        assert!(
            sql.contains(r#""projection":["NAME","ID"]"#),
            "the already-projected NAME must not be re-appended and ID must be \
             appended once: {sql}"
        );
        let emits = emits_clause(&sql);
        assert_eq!(
            emits.matches("\"NAME\"").count(),
            1,
            "the already-visible NAME must appear in EMITS exactly once: {emits}"
        );
        assert_eq!(
            emits.matches("\"ID\"").count(),
            1,
            "a column named by two sort keys must be appended exactly once: {emits}"
        );
        assert_eq!(
            outer_select_list(&sql),
            "\"NAME\"",
            "the extension must not change the VISIBLE column count: {sql}"
        );
    }

    /// Companion scenario: when every pushed sort key IS already a bare-projected
    /// column the extension is INERT — nothing appended, nothing widened — and the
    /// legitimately matched bounded top-N still forms exactly as before.
    ///
    /// The matched path never reaches the declined-wrapper code at all: it renders
    /// `proj_cols` directly as the FINAL visible EMITS with no wrapping
    /// `SELECT … FROM (`, and carries the sort keys plus the limit into the per-shard
    /// common blob. That is precisely why the extension must not run ahead of
    /// `detect_topn` — a hidden column would leak straight into this path's result.
    #[test]
    fn declined_order_by_all_keys_projected_leaves_projection_untouched() {
        let request = guard_events_request(serde_json::json!({
            "selectList": [{"type": "column", "name": "NAME"}],
            "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "name": "NAME"},
                "isAscending": true,
                "nullsLast": true
            }],
            "limit": {"numElements": 5}
        }));
        let proj_cols = vec![ProjectionItem::Column("NAME".to_string())];
        let proj_types = vec!["VARCHAR(2000000)".to_string()];
        let logical_schema = vec![LogicalField {
            field_id: 2,
            name: "NAME".to_string(),
            arrow_type: "utf8".to_string(),
            nullable: true,
            initial_default: None,
        }];

        let sql = guard_dispatch_sql(
            &request,
            proj_cols,
            proj_types,
            false,
            Some(5),
            logical_schema,
        );

        assert!(
            sql.contains(r#""projection":["NAME"]"#),
            "an already-projected sort key must leave the projection untouched: {sql}"
        );
        assert!(
            !sql.contains("REGION") && !sql.contains("AMOUNT") && !sql.contains("\"ID\""),
            "nothing must be appended or widened when the sort key is projected: {sql}"
        );
        assert!(
            sql.contains(r#"ORDER BY "NAME""#) && sql.contains("LIMIT 5"),
            "a matched top-N must still form (sort key projected, native type): {sql}"
        );
        // The fan-out IS the outermost query: no declined-path wrapper around it.
        assert!(
            sql.starts_with("SELECT LAKEHOUSE_SCAN(") && !sql.contains(" FROM ("),
            "a matched top-N must not be wrapped in an outer SELECT … FROM (: {sql}"
        );
        // Only the matched path pushes the bounded sort and the limit per shard.
        let common = common_arg_literal(&sql);
        assert!(
            common.contains(r#""order_by":[{"column":"NAME","ascending":true,"nulls_last":true}]"#)
                && common.contains(r#""limit":5"#),
            "a matched top-N must carry the per-shard sort keys and limit: {common}"
        );
    }

    /// S3 (`build_row_scan_sql`) is unreachable with an offset because the decline
    /// (issue #191, fact 5) NULLS `effective_limit` before it ever reaches that
    /// builder. Same fixture as
    /// `declined_order_by_all_keys_projected_leaves_projection_untouched` — every
    /// `detect_topn` guard would MATCH (single table, `NAME` projected as a bare
    /// column, a populated non-JSON-fallback logical schema) — except this request
    /// carries a NON-ZERO `offset`, which declines the bounded top-N and therefore
    /// nulls `effective_limit`: neither the per-shard fan-out nor a bare outer
    /// `LIMIT`/`OFFSET` may render ahead of the declined wrapper's own
    /// `ORDER BY … LIMIT n OFFSET m` (through the shared `render_limit_offset` seam).
    #[test]
    fn nonzero_offset_nulls_the_effective_limit() {
        let request = guard_events_request(serde_json::json!({
            "selectList": [{"type": "column", "name": "NAME"}],
            "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "name": "NAME"},
                "isAscending": true,
                "nullsLast": true
            }],
            "limit": {"numElements": 5, "offset": 2}
        }));
        let proj_cols = vec![ProjectionItem::Column("NAME".to_string())];
        let proj_types = vec!["VARCHAR(2000000)".to_string()];
        let logical_schema = vec![LogicalField {
            field_id: 2,
            name: "NAME".to_string(),
            arrow_type: "utf8".to_string(),
            nullable: true,
            initial_default: None,
        }];

        let sql = guard_dispatch_sql(
            &request,
            proj_cols,
            proj_types,
            false,
            Some(5),
            logical_schema,
        );

        // The declined wrapper renders the offset window exactly once, on its own
        // ORDER BY — never a bare per-shard/outer LIMIT ahead of it.
        assert_eq!(
            sql.matches("LIMIT").count(),
            1,
            "effective_limit must be nulled: no LIMIT may reach the fan-out ahead of \
             the declined wrapper's own window: {sql}"
        );
        assert!(
            sql.contains(r#"ORDER BY "NAME" ASC NULLS LAST LIMIT 5 OFFSET 2"#),
            "the declined wrapper must render the offset beside its own ORDER BY: {sql}"
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("\"limit\"") && !common.contains("\"order_by\""),
            "the per-shard common blob must carry neither bound once effective_limit \
             is nulled: {common}"
        );
    }

    /// The projection extension runs strictly AFTER `detect_topn` (decision [2]) — the
    /// plan's most load-bearing ordering invariant, and one that is SILENT when
    /// violated (a mis-ordered implementation reintroduces `04000` with a green suite).
    ///
    /// The fixture deliberately gives `detect_topn` everything it needs to MATCH
    /// except a projected sort key: exactly one involved table, `ORDER BY "NAME"` ASC
    /// NULLS LAST, a `LIMIT 5`, and a POPULATED `logical_schema` typing `NAME` as
    /// `utf8` (not a JSON-fallback type). Only the CALL ORDER decides the outcome:
    ///
    /// - Correct order: `detect_topn` sees the pre-extension `[Expr("1")]`, finds
    ///   `NAME` unprojected and declines; the declined path then appends `NAME` as a
    ///   hidden column and renders the wrapper. Nothing per-shard.
    /// - Extension first: `proj_cols` would already be `[Expr("1"), Column("NAME")]`,
    ///   so every remaining `detect_topn` guard passes, the bounded top-N MATCHES,
    ///   `"order_by"` and `"limit":5` land in the common blob, and NO wrapper is
    ///   rendered — failing all three assertions below. That path emits `proj_cols` as
    ///   the FINAL visible EMITS, so the hidden `NAME` would leak into the result too.
    ///
    /// A `detect_topn`-only assertion over the pre-extension projection cannot pin
    /// this: it holds whatever the call order (see `topn.rs`'s
    /// `unsupported_order_by_shape_declines_topn`). Nor can the sibling tests above —
    /// they force the decline via an empty `logical_schema` or an absent `LIMIT`, both
    /// order-blind.
    #[test]
    fn declined_order_by_extension_runs_after_topn_detection() {
        let request = guard_events_request(serde_json::json!({
            "selectList": [{"type": "literal_exactnumeric", "value": 1}],
            "selectListDataTypes": [{"type": "decimal", "precision": 1, "scale": 0}],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "name": "NAME"},
                "isAscending": true,
                "nullsLast": true
            }],
            "limit": {"numElements": 5}
        }));
        let proj_cols = vec![ProjectionItem::Expr {
            expr: "1".to_string(),
        }];
        let proj_types = vec!["DECIMAL(1,0)".to_string()];
        // NAME as `utf8`: a native, non-JSON-fallback type, so the JSON-fallback guard
        // would NOT be what declines the top-N had the extension already run.
        let logical_schema = vec![LogicalField {
            field_id: 2,
            name: "NAME".to_string(),
            arrow_type: "utf8".to_string(),
            nullable: true,
            initial_default: None,
        }];

        let sql = guard_dispatch_sql(
            &request,
            proj_cols,
            proj_types,
            false,
            Some(5),
            logical_schema,
        );

        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("\"limit\""),
            "the top-N must have DECLINED, so no per-shard limit may reach the common \
             blob — the extension ran before detect_topn: {common}"
        );
        assert!(
            !common.contains("order_by"),
            "the top-N must have DECLINED, so no per-shard sort keys may reach the \
             common blob — the extension ran before detect_topn: {common}"
        );
        assert!(
            sql.contains(r#"SELECT "_LH_PROJ_0" FROM ("#),
            "the declined path must render the hidden-column wrapper; a matched top-N \
             renders none — the extension ran before detect_topn: {sql}"
        );
    }

    /// Scenario (pushdown-planning-capability-extensions, issue #198): "An ORDER BY
    /// the adapter cannot bound as a top-N remains correctness-safe."
    ///
    /// Exasol DELEGATES a pushed ordering and no longer re-applies its own backstop
    /// sort, so the declined row-scan path has exactly two correctness-safe outcomes:
    /// render the ordering in FULL, or decline with a `User` error naming the key.
    /// Returning SQL that reproduces only PART of the pushed ordering is the
    /// silent-wrong-order outcome this guard exists to make unreachable.
    ///
    /// Three facets, and facet (b) is why the guard tests ANY unrenderable element
    /// rather than ALL of them:
    /// (a) every element unrenderable — both kinds: an expression node NEITHER
    ///     dialect knows, and a bare `column` node missing its `nullsLast` flag
    ///     (direction / NULL placement is never silently defaulted). This SUPERSEDES
    ///     `fix-225`'s "return the unwrapped SQL unchanged" rule for a NON-EMPTY
    ///     `orderBy`.
    /// (b) MIXED — one renderable key and one not. An `all`-shaped guard would pass
    ///     this through and render a partial ordering, which is precisely the silent
    ///     corruption; only the unrenderable key's own ordering would be lost, and
    ///     nothing downstream would notice.
    /// (c) ABSENT `orderBy` — unchanged: the unwrapped fan-out, no wrapper, no
    ///     decline. Nothing was delegated, so nothing must be reproduced.
    #[test]
    fn declined_order_by_renders_every_reachable_ordering_or_declines() {
        let unrenderable_expression = serde_json::json!({
            "type": "order_by_element",
            "expression": {"type": "no_such_node_type_in_either_dialect"},
            "isAscending": true,
            "nullsLast": true
        });
        // A bare column node whose NULL placement is absent: renderable as an
        // identifier, but not as an ORDER BY element.
        let column_missing_nulls_last = serde_json::json!({
            "type": "order_by_element",
            "expression": {"type": "column", "name": "ID"},
            "isAscending": true
        });
        let renderable_expression = serde_json::json!({
            "type": "order_by_element",
            "expression": {
                "type": "function_scalar",
                "name": "ABS",
                "arguments": [{"type": "column", "name": "AMOUNT", "tableName": "EVENTS"}]
            },
            "isAscending": false,
            "nullsLast": true
        });
        let declining_shapes = [
            (
                "every element unrenderable",
                serde_json::json!([unrenderable_expression, column_missing_nulls_last]),
            ),
            (
                "renderable key first, unrenderable second",
                serde_json::json!([renderable_expression, unrenderable_expression]),
            ),
            (
                "unrenderable key first, renderable second",
                serde_json::json!([unrenderable_expression, renderable_expression]),
            ),
        ];

        for (facet, order_by) in declining_shapes {
            let request = guard_events_request(serde_json::json!({
                "selectList": [{"type": "column", "name": "NAME"}],
                "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
                "orderBy": order_by,
                "limit": {"numElements": 7}
            }));
            let err = guard_dispatch_result(
                &request,
                vec![ProjectionItem::Column("NAME".to_string())],
                vec!["VARCHAR(2000000)".to_string()],
                false,
                Some(7),
                Vec::new(),
            )
            .expect_err(&format!(
                "{facet}: a pushed ordering the adapter cannot reproduce in full must \
                 decline, never return SQL"
            ));
            match err {
                UdfError::User(msg) => {
                    assert!(
                        msg.contains("ORDER BY") && msg.contains("declined"),
                        "{facet}: the decline must name the unrenderable ORDER BY key: {msg}"
                    );
                    assert!(
                        msg.contains("not a native re-plan"),
                        "{facet}: the decline is a HARD error — Exasol does not re-plan \
                         natively, so the message must not imply a retry: {msg}"
                    );
                }
                other => panic!("{facet}: must be a User decline, got {other:?}"),
            }
        }

        // (c) No `orderBy` at all: nothing was delegated, so the fan-out is returned
        // unwrapped and the LIMIT is NOT withheld.
        let unordered = guard_events_request(serde_json::json!({
            "selectList": [{"type": "column", "name": "NAME"}],
            "selectListDataTypes": [{"type": "varchar", "size": 2000000}],
            "limit": {"numElements": 7}
        }));
        let sql = guard_dispatch_sql(
            &unordered,
            vec![ProjectionItem::Column("NAME".to_string())],
            vec!["VARCHAR(2000000)".to_string()],
            false,
            Some(7),
            Vec::new(),
        );
        assert!(
            !sql.contains("ORDER BY"),
            "an absent orderBy must emit no ORDER BY at all: {sql}"
        );
        assert!(
            sql.starts_with("SELECT LAKEHOUSE_SCAN(") && !sql.contains(" FROM ("),
            "an absent orderBy must leave the fan-out unwrapped: {sql}"
        );
        assert!(
            sql.contains("LIMIT 7"),
            "an absent orderBy must not withhold the request LIMIT: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // COUNT(DISTINCT) wrapper limit withholding is dead code (issue #191)
    // ---------------------------------------------------------------------------

    /// Regression (issue #191, plan `fix-191-order-by-offset`): a lone
    /// `COUNT(DISTINCT)` request (Case 1) carrying BOTH a request-level `orderBy`
    /// and a request-level LIMIT must render that LIMIT on the outer wrapper.
    /// The now-deleted withholding (`let cd_limit = if has_order_by { None } else
    /// { limit };`) used to drop the limit in exactly this case — dead code,
    /// because Exasol never actually pushes an `orderBy` on an ungrouped
    /// aggregate request (fact 7), but the withholding branch fired on ANY
    /// `orderBy` this fixture forces regardless of whether Exasol would send one.
    #[test]
    fn lone_count_distinct_with_order_by_still_renders_limit() {
        let request = guard_events_request(serde_json::json!({
            "selectList": [agg_item("COUNT", Some("ID"), true)],
            "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                "isAscending": true,
                "nullsLast": true
            }]
        }));

        let sql = guard_dispatch_sql(
            &request,
            Vec::new(),
            Vec::new(),
            false,
            Some(10),
            Vec::new(),
        );

        assert!(
            sql.trim_end().ends_with("LIMIT 10"),
            "the wrapper must render the request's raw limit even though an \
             orderBy is present: {sql}"
        );
        assert!(
            !sql.contains("OFFSET"),
            "no offset can ever reach this wrapper (fact 6 — Exasol rejects OFFSET \
             on an ungrouped aggregated select before the adapter is consulted): {sql}"
        );
        assert_eq!(
            sql.matches("LIMIT").count(),
            1,
            "the LIMIT must land on the outer wrapper only, never leak into the \
             per-shard distinct fan-out sub-scan: {sql}"
        );
        assert!(
            !sql.contains("ORDER BY"),
            "the per-shard fan-out stays sort-free: no per-shard scan spec ever \
             carries an ORDER BY on this path: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Widened-projection routing at coincidental arity (issues #196 / #234)
    // ---------------------------------------------------------------------------

    /// A `RequestShape::RowScan` request whose select-list arity EQUALS the base
    /// table's column count: four bare `EVENTS` columns, plus an `ORDER BY` the
    /// adapter does not match as a bounded top-N. Both routing tests below drive the
    /// dispatcher with these identical inputs and differ ONLY in the widening flag.
    fn widening_arity_coincidence_request() -> Json {
        guard_events_request(serde_json::json!({
            "selectList": [
                {"type": "column", "name": "REGION", "tableName": "EVENTS"},
                {"type": "column", "name": "NAME", "tableName": "EVENTS"},
                {"type": "column", "name": "AMOUNT", "tableName": "EVENTS"},
                {"type": "column", "name": "ID", "tableName": "EVENTS"},
            ],
            "selectListDataTypes": [
                {"type": "varchar", "size": 2000000},
                {"type": "varchar", "size": 2000000},
                {"type": "decimal", "precision": 18, "scale": 2},
                {"type": "decimal", "precision": 20, "scale": 0},
            ],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "column", "name": "NAME", "tableName": "EVENTS"},
                "isAscending": true,
                "nullsLast": true
            }]
        }))
    }

    /// The four-column full base row, as `project_columns` returns it when it widens.
    fn widening_arity_coincidence_projection() -> (Vec<ProjectionItem>, Vec<String>) {
        let cols = guard_col_types()
            .into_iter()
            .map(|(name, ty)| (ProjectionItem::Column(name), ty))
            .collect::<Vec<_>>();
        (
            cols.iter().map(|(c, _)| c.clone()).collect(),
            cols.iter().map(|(_, t)| t.clone()).collect(),
        )
    }

    /// Scenario (pushdown-planning-capability-extensions, issues #196 / #234): a
    /// WIDENED derived projection routes to `qualified_single_table_fallback_pushdown`
    /// even when its column count COINCIDES with the select-list arity.
    ///
    /// The count comparison this replaced was blind here — four base columns against
    /// four select-list items looks like a clean per-item derivation, so the request
    /// reached the raw scan path and Exasol rejected the positionally-mismatched types
    /// (`04000` "Data type mismatch in column number N", reproduced live on a 10-item
    /// select list over a 10-column table). Routing on the producer's own widening
    /// signal cannot be fooled by the coincidence.
    ///
    /// The `ORDER BY` also pins the early-return POSITION: the wrapper's own outer
    /// `ORDER BY` is what orders the result, so the widened projection never reached
    /// `detect_topn` or the declined-`ORDER BY` hidden-sort-key extension.
    #[test]
    fn dispatch_widened_projection_at_matching_arity_routes_to_wrapper() {
        let request = widening_arity_coincidence_request();
        let (proj_cols, proj_types) = widening_arity_coincidence_projection();
        assert_eq!(
            proj_cols.len(),
            request["pushdownRequest"]["selectList"]
                .as_array()
                .expect("fixture select list")
                .len(),
            "the fixture must hold the arity coincidence the count comparison missed"
        );

        let sql = guard_dispatch_sql(&request, proj_cols, proj_types, true, None, Vec::new());

        assert!(
            sql.contains(r#"AS "LHS_T0""#),
            "a widened projection must route to the qualified single-table wrapper: {sql}"
        );
        assert!(
            sql.contains(
                r#"SELECT "LHS_T0"."REGION", "LHS_T0"."NAME", "LHS_T0"."AMOUNT", "LHS_T0"."ID" FROM ("#
            ),
            "the wrapper must render the ORIGINAL select list, qualified, so Exasol's \
             positional validation sees its own items: {sql}"
        );
        assert!(
            sql.contains(r#"ORDER BY "LHS_T0"."NAME""#),
            "the wrapper's own outer ORDER BY must order the result: {sql}"
        );
    }

    /// The mirror of `dispatch_widened_projection_at_matching_arity_routes_to_wrapper`:
    /// the SAME request and the SAME four-item projection with the widening flag CLEAR
    /// — a genuine `SELECT region, name, amount, id ... ORDER BY name` — stays on the
    /// ordinary scan path and is NOT wrapped in the qualified fallback.
    ///
    /// This pins the signal as load-bearing in BOTH directions: a later `, _`
    /// destructuring that swallows the flag, or a hardcoded `true`, fails a host test
    /// instead of silently unaccelerating every row scan.
    #[test]
    fn dispatch_non_widened_projection_at_matching_arity_takes_scan_path() {
        let request = widening_arity_coincidence_request();
        let (proj_cols, proj_types) = widening_arity_coincidence_projection();

        let sql = guard_dispatch_sql(&request, proj_cols, proj_types, false, None, Vec::new());

        assert!(
            !sql.contains("LHS_T0"),
            "a per-select-list-item projection must NOT be routed to the qualified \
             single-table wrapper: {sql}"
        );
        assert!(
            sql.contains(&format!("{SCAN_UDF_NAME}(")),
            "the ordinary scan path must still drive the sharded scan UDF: {sql}"
        );
        assert!(
            sql.contains(r#"SELECT "REGION", "NAME", "AMOUNT", "ID" FROM ("#),
            "the scan path must emit the derived projection unqualified: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Parse-before-config ordering — regression coverage
    // ---------------------------------------------------------------------------

    /// A malformed `catalog.table` identifier against an unreachable `catalog_uri`
    /// must fail with `parse_table_ident`'s own error, not a transport error from
    /// the unreachable host.
    ///
    /// Proves `handle_pushdown` validates the identifier BEFORE
    /// `CatalogSession::resolve` runs the OAuth2 client-credentials grant (the
    /// only branch of `resolve_catalog_auth` that makes network contact — the
    /// no-auth and static-token branches never touch the network at all, so this
    /// test would pass vacuously against a broken build-then-validate ordering
    /// unless creds force the OAuth2 branch). `catalog_uri` is a closed local
    /// port (`127.0.0.1:1`, connection refused) so a wrongly-ordered
    /// implementation fails fast with a transport error instead of hanging.
    #[tokio::test]
    async fn malformed_table_ident_fails_before_any_catalog_contact() {
        let creds = ConnectionCreds {
            warehouse: "warehouse".into(),
            endpoint: "http://minio:9000".into(),
            region: "us-east-1".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            session_token: None,
            path_style: true,
            use_sigv4: false,
            use_vended_credentials: false,
            token: None,
            client_id: Some("oauth-client-id-sentinel".into()),
            client_secret: Some("oauth-client-secret-sentinel".into()),
            oauth2_server_uri: None,
            scope: None,
            account_name: None,
            account_key: None,
            sas_token: None,
        };

        let catalog = CatalogProps {
            warehouse: "warehouse".into(),
            // No '.' separator: fails `parse_table_ident`'s validation before any
            // catalog HTTP is issued.
            table: "malformed_identifier_with_no_namespace_separator".into(),
        };

        // Two-column universe (non-empty): an empty `columns` array fails in
        // `project_columns` before the code path under test even runs, which
        // would mask the ordering this test proves.
        let request = nq4_request();

        let result = handle_pushdown(
            &request,
            "http://127.0.0.1:1",
            &sample_storage(),
            &catalog,
            None,
            1,
            1,
            1,
            1024,
            1,
            0.6,
            200,
            4,
            1024,
            &creds,
        )
        .await;

        let err = result.expect_err("a malformed table identifier must fail");
        let message = err.to_string();
        assert!(
            message.contains("namespace.table"),
            "error must be parse_table_ident's own error, got: {message}"
        );
        assert!(
            !message.contains("OAuth2"),
            "error must not be the OAuth2 token request/transport error \
             (would mean the session was built before the identifier was \
             validated): {message}"
        );
    }
}

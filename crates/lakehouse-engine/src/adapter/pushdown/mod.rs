//! Pushdown planning: resolve the Iceberg file list ONCE and build the
//! scan-driving SQL that invokes the LAKEHOUSE_SCAN SCALAR EMIT UDF.
//!
//! Architecture invariants:
//! - File list resolved exactly ONCE here, in the planning layer.
//! - The scan SCALAR EMIT UDF receives the explicit file list; it NEVER discovers files.
//! - A predicate the adapter cannot faithfully translate into the DataFusion scan is
//!   self-applied by the adapter itself (e.g. as an outer WHERE), never OMITTED from
//!   the spec. There is no Exasol-side fallback to defer to — see CLAUDE.md
//!   § "Virtual Schema pushdown delegation" and `specs/_decision/045`.
//! - LIMIT appears in both the scan spec and the returned SQL (correctness backstop).
//! - Catalog/connection auth credentials (OAuth token, bearer, etc.) NEVER appear
//!   in any returned SQL string or error message. Storage (S3) credentials are a
//!   documented exception — see `handle_pushdown`'s doc comment.

use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{
    CatalogProps, CommonScanSpec, FileEntry, LogicalField, NameMappingEntry, ProjectionItem,
    ScanSpec, StorageBackend,
};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

mod support;
use support::{
    DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, aggregate_exasol_types, classify_where_filter,
    extract_all_column_types, extract_limit, extract_projection, order_by_present,
    strip_table_alias,
};
pub use support::{build_fan_out_inner, build_scan_driving_sql, shard_count};

use lakehouse_catalog::{CatalogSession, parse_table_ident};

mod file_resolution;
pub use file_resolution::resolve_file_list;
use file_resolution::{empty_result_sql, encode_initial_default, relativize_shards_to_root};

mod format;
pub use format::{ConnectionStorage, FormatReader, ResolvedScan, ScanSource, format_reader};

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
use grouped_agg::{blank_pad_char_group_keys, group_key_exasol_types};

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
// The filter pipeline's two halves, imported for the test mirrors that pin their
// composition; production reaches them through `classify_where_filter`.
#[cfg(test)]
use support::apply_type_rewrites;
#[cfg(test)]
use vs_expression::render_df_filter_safe;

#[cfg(test)]
#[path = "test_support_tests.rs"]
mod test_support;

#[cfg(test)]
#[path = "dispatch_golden_tests.rs"]
mod dispatch_golden;

/// Resolve the Iceberg snapshot + file list and build pushdown SQL.
///
/// `cluster_nodes` — the number of Exasol nodes, captured from `ctx.node_count()`
/// in `dispatch`'s pushdown arm (default 1 when the handshake reports 0).
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
/// to sign catalog requests and whether to apply vended storage credentials.
///
/// `allow_http` — the resolved `ALLOW_HTTP` property; under vending it is the
/// operator's consent gate for plaintext transport.
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
    allow_http: bool,
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
                allow_http,
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

    // ONE classification of the request's WHERE filter, owned by
    // `classify_where_filter`: `filter` is the DataFusion-bound scan-spec predicate,
    // `declined_filter` the original tree the adapter must self-apply because the
    // scan cannot carry it. At most one is `Some`. `filter_json_raw` itself is left
    // completely unmodified for the later `resolve_file_list` Iceberg-level pruning
    // call below, which must see the original, un-rewritten predicate tree — a
    // decline changes what the ADAPTER renders, never what pruning sees.
    let (filter, declined_filter) = classify_where_filter(filter_json_raw, &col_types);

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
    let (files, effective_storage, logical_schema, table_root, name_mapping) = resolve_file_list(
        &session,
        catalog,
        storage,
        creds,
        allow_http,
        filter_json_raw,
    )
    .await?;
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
        declined_filter,
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
///
/// `filter` and `declined_filter` are the two halves of `classify_where_filter`'s
/// single classification and are never both `Some`: `filter` is the predicate the
/// scan spec carries, `declined_filter` the ORIGINAL tree the adapter must self-apply
/// because the scan cannot carry it. This dispatcher does not re-derive
/// renderability — that classification has exactly one owner.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_dispatch_sql(
    request: &Json,
    pushdown_req: &Json,
    mut proj_cols: Vec<ProjectionItem>,
    mut proj_types: Vec<String>,
    projection_widened: bool,
    col_types: Vec<(String, String)>,
    filter: Option<String>,
    declined_filter: Option<&Json>,
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
        delta: None,
        storage: storage.clone(),
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
    };

    // Declined WHERE route, ahead of shape routing so it applies before aggregating,
    // grouping, and truncating (see `_decision/045`).
    if let Some(declined) = declined_filter {
        return qualified_single_table_fallback_pushdown(
            request,
            pushdown_req,
            &base,
            None,
            shards,
            &col_types,
            udf_name,
            distribute_udf_name,
            Some(declined),
        );
    }

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
            // Resolved BEFORE the spec: the DataFusion-side group keys are derived
            // from these declared types, because a CHAR(n)-declared key must be
            // blank-padded to n to reproduce Exasol's own CHAR grouping (#192).
            // ONLY the spec copy is padded — the `classify_request_shape` ORDER BY
            // resolution (`build_grouped_order_by_clause`) and
            // `build_grouped_aggregate_scan_sql` below keep the unpadded fragments,
            // which are what a pushed ORDER BY is matched against.
            let group_key_types = group_key_exasol_types(pushdown_req, &group_keys, &select_items);
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
                    group_keys: Some(blank_pad_char_group_keys(&group_keys, &group_key_types)),
                    ..base.clone()
                },
                files: vec![],
            };
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
            // wrapper only. The WHERE filter is pushed into the scan, exactly as the
            // grouped push-down path does, and needs no outer WHERE — not because an
            // advertised capability guarantees a translatable predicate (it does not),
            // but because a predicate the scan cannot carry never reaches this arm: the
            // declined-filter route above intercepts it and self-applies it.
            return qualified_single_table_fallback_pushdown(
                request,
                pushdown_req,
                &base,
                filter.clone(),
                shards,
                &col_types,
                udf_name,
                distribute_udf_name,
                None,
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
                    None,
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
                    None,
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
#[path = "pushdown_tests.rs"]
mod tests;

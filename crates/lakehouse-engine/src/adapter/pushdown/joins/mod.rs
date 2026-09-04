use crate::adapter::ResolvedConnectionConfig;
#[cfg(test)]
use crate::scan::spec::StorageBackend;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::ConnectionStorage;
use super::empty_result::empty_result_sql;
use super::refused_columns::ensure_no_touched_column_is_refused;
use super::scan_resolution::TableScanResolver;
use super::support::{DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, project_columns, quote_ident};

mod attribution;
mod planning;
mod rendering;
mod sql_builders;

pub(crate) use planning::{
    DetectedJoin, IneligibleJoinReason, JoinLeaf, JoinShape, JoinSides, ResolvedJoinSide,
    detect_join,
};
pub(crate) use sql_builders::{RenderedJoinPushdown, render_broadcast_join};

pub(super) use sql_builders::qualified_single_table_fallback_pushdown;
// Only test modules (`grouped_agg.rs`, `support.rs`) reach these two directly now;
// production callers go through `qualified_single_table_fallback_pushdown` above.
#[cfg(test)]
pub(super) use sql_builders::{
    build_qualified_single_table_fallback_sql, referenced_column_projection,
};

pub(super) use planning::JoinWindowPlan;
use planning::{
    classify_join_window, involved_table_columns, resolve_one_join_side, select_broadcast_sides,
};
use rendering::{has_no_explicit_select_list, leg_local_filter, possible_side_column_names};
// Re-exported `pub(super)` (not merely `use`) so the dispatch-golden test module
// (a sibling of `joins` under `pushdown`, gated `#[cfg(test)]`) can drive both
// join SQL builders directly to pin cross-site golden-SQL fixtures — the same
// reachability pattern already used for `qualified_single_table_fallback_pushdown`
// above.
pub(super) use sql_builders::{
    JoinScanRequestConfig, build_broadcast_join_sql, build_n_scan_join_sql,
};

/// Schema-qualify a UDF/script name for a pushdown-driving query.
///
/// The generated SQL runs outside the adapter script's own schema, so an
/// unqualified name would fail to resolve. Shared by the single-table path and the
/// join planner so both qualify identically.
pub(super) fn qualify_udf(scan_schema: Option<&str>, udf: &str) -> String {
    match scan_schema {
        Some(schema) if !schema.is_empty() => format!("{}.{}", quote_ident(schema), udf),
        _ => udf.to_string(),
    }
}

/// The `User` decline error for a join `from` clause the adapter cannot render at
/// all — the genuine last resort.
///
/// Spanning more than two tables, needing Exasol postprocessing, or overlapping
/// column names are NEVER reasons to reach here — every such inner join is served
/// by the unified fallback. Only a non-inner join node in the tree or a malformed
/// shape lands here, and falling through to the single-table path would scan only
/// the first involved table and silently drop the join. So the only safe outcome is
/// a `User` error — surfaced by the FFI shim as a hard `F-UDF-CL-RUST-9001` client
/// error with no native re-plan (`vs-adapter/pushdown-planning-join` "declined
/// safely", last resort).
///
/// Not merged with `sql_builders::join_render_decline`: that one covers the six
/// separate qualified-N-scan render-decline sites (an unrenderable select-list
/// item, join condition, GROUP BY key, HAVING, ORDER BY key, or missing column
/// metadata), each a plain `{clause}; this is a hard error, not a native re-plan`
/// sentence with no extra clause inserted.
pub(super) fn ineligible_join_decline(reason: IneligibleJoinReason) -> UdfError {
    let detail = match reason {
        IneligibleJoinReason::NotInnerJoinType => "the join is not an inner join",
        IneligibleJoinReason::UnsupportedShape => "the join `from` clause has an unsupported shape",
    };
    UdfError::User(format!(
        "join pushdown declined: {detail}; the adapter cannot render this join shape, \
         so this is a hard error, not a native re-plan"
    ))
}

/// Refuses the join when a side's format reader declined a column this request
/// reads or emits FROM THAT SIDE.
///
/// Attribution is per side, never request-global: a refusal belongs to the table
/// that raised it, so a name refused on one side must not refuse a query that reads
/// only the other side's same-named mappable column. A `column` node carrying no
/// `tableName` is charged to every side. A request with no explicit select list
/// emits each side's declared row without naming a single column, so every side is
/// then charged everything the request declares for it; any other select list names
/// its emitted columns itself and the walk has already attributed them.
///
/// Lives beside its one call site rather than in `planning`, which must not reach
/// into `rendering` — the module that owns column-to-side attribution.
fn ensure_no_side_refuses_a_referenced_column(
    request: &Json,
    pushdown_req: &Json,
    sides: &[ResolvedJoinSide],
) -> Result<(), UdfError> {
    if sides.iter().all(|side| side.refused_columns.is_empty()) {
        return Ok(());
    }
    let emits_unnamed_row = has_no_explicit_select_list(pushdown_req);
    for side in sides {
        let mut touched = possible_side_column_names(request, &side.table_name);
        if emits_unnamed_row {
            touched.extend(
                involved_table_columns(request, &side.table_name)
                    .into_iter()
                    .map(|(name, _)| name),
            );
        }
        ensure_no_touched_column_is_refused(&touched, &side.refused_columns)?;
    }
    Ok(())
}

/// Plan an inner join (N ≥ 2 involved tables) through the SINGLE unified join path.
///
/// Resolves each involved table's file list, logical schema, and byte size ONCE
/// (one catalog resolution per table, never per shard), pruned by that table's
/// side-local WHERE conjuncts. An inner join with any empty side yields zero rows,
/// so an empty side short-circuits to the shape-correct empty result over the
/// combined N-table column universe (in stable side order, matching the fallback's
/// full-row projection).
///
/// Broadcast is an OPTIMIZATION selected inside this one path — never a second
/// implementation. It is taken only for a two-table (N = 2) equi-join whose smaller
/// side fits `join_broadcast_max_bytes`, whose request carries no aggregation and no
/// window the broadcast path cannot serve (`classify_join_window`), and whose
/// bare-name broadcast render succeeds (disjoint column names + renderable
/// condition — `render_broadcast_join` returns `Ok(None)` otherwise, a clean
/// fall-through, never an error). Every other inner join — N ≥ 3, above threshold,
/// non-equi, overlapping columns, or needing postprocessing — takes the SOLE
/// fallback renderer, [`build_n_scan_join_sql`], which scans each table through its
/// own sharded fan-out and reconstructs the join in Exasol's core engine.
///
/// No plan-time check compares the sides' storage backends: each side is READ through
/// its own store, built from its own backend, so a join across two variants or two ADLS
/// accounts resolves to two distinct DataFusion registry keys and is served (see
/// `build_side_store`, `crates/lakehouse-engine/src/scan/object_store.rs`). The one
/// shape the scan genuinely
/// cannot serve — two sides collapsing onto ONE registry key while needing different
/// stores, i.e. two containers of one ADLS storage account — is owned by the scan's own
/// `validate_sides_share_one_store` precondition, which is stated over the derived store
/// URLs the collapse is a property of; a plan-time copy would have to re-derive
/// DataFusion's key formula here to say the same thing. A hard `Err` therefore leaves
/// this path only when it is delegated to the fallback builder for a wrapper that
/// genuinely cannot be built.
#[allow(clippy::too_many_arguments)]
pub(super) async fn plan_join(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
    conn: &ResolvedConnectionConfig,
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
    // Resolve each side ONCE (one catalog resolution per involved table, never per
    // shard), through the SAME per-request resolver, each pruned by its own
    // leg-local WHERE conjuncts for format-level manifest pruning — attributed by
    // LEG, so both a shared-column-name case and a repeated table stay correct.
    let filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    let connection = ConnectionStorage {
        storage: &conn.storage,
        creds: &conn.creds,
        allow_http: conn.allow_http,
    };
    // Every leg's identifier is validated inside the resolver's own catalog-kind
    // match, ahead of any catalog HTTP, so a malformed leg costs no catalog
    // round-trip. Building the resolver here (once) and threading `&resolver` into
    // every leg is what makes a per-leg rebuild structurally inexpressible.
    let identifiers: Vec<&str> = join
        .tables
        .iter()
        .map(|leaf| leaf.table_identifier.as_str())
        .collect();
    let resolver = TableScanResolver::for_request(
        conn.catalog_kind,
        &conn.catalog_uri,
        connection,
        &identifiers,
    )
    .await?;
    // ONE leg per FROM-tree leaf, so the resolve loop is driven by LEG INDEX: a
    // self-join's two occurrences share a `tableName`, and keying pruning on the name
    // would hand each occurrence the other's predicate too — over-filtered rows with
    // no error.
    let legs = join.legs();
    let mut sides = Vec::with_capacity(join.tables.len());
    for (leg, leaf) in join.tables.iter().enumerate() {
        let side_filter = filter.and_then(|f| leg_local_filter(f, &legs, leg));
        let side = resolve_one_join_side(
            &leaf.table_name,
            &leaf.table_identifier,
            &resolver,
            side_filter.as_ref(),
        )
        .await?;
        sides.push(side);
    }

    ensure_no_side_refuses_a_referenced_column(request, pushdown_req, &sides)?;

    // An inner join with any empty side is empty regardless of the plan. Emit the
    // shape-correct empty result over the combined N-table column universe (stable
    // side order) rather than a fan-out over an empty file list.
    if sides.iter().any(|s| s.files.is_empty()) {
        let mut combined = Vec::new();
        for leaf in &join.tables {
            combined.extend(involved_table_columns(request, &leaf.table_name));
        }
        let (proj_cols, proj_types, widened) = project_columns(pushdown_req, combined.clone())?;
        return empty_result_sql(pushdown_req, &proj_cols, &proj_types, widened, &combined);
    }

    let udf_name = qualify_udf(scan_schema, SCAN_UDF_NAME);
    let distribute_udf_name = qualify_udf(scan_schema, DISTRIBUTE_FILES_UDF_NAME);
    let inputs = JoinScanRequestConfig {
        cluster_nodes,
        parallelism_factor,
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
        connection: conn,
    };

    // Broadcast eligibility is a PROPERTY of the request, computed here: exactly two
    // involved tables, a `predicate_equal` condition, and a window the broadcast path
    // can serve. The window is classified BEFORE the sizing and the render, so a
    // request Exasol must post-process never reaches `render_broadcast_join` — whose
    // `Err` arm on absent column metadata stays unreachable from here. When
    // eligibility holds, size the two sides (smaller = dimension) and take the
    // broadcast fan-out iff the dimension fits the threshold AND the bare-name render
    // succeeds. Any miss falls through to the N-scan fallback below — never an error.
    let is_equi =
        join.conditions[0].get("type").and_then(|t| t.as_str()) == Some("predicate_equal");
    let window = classify_join_window(pushdown_req);
    if join.tables.len() == 2 && is_equi && !matches!(window, JoinWindowPlan::ExasolPostProcessed) {
        let candidate =
            select_broadcast_sides(sides[0].clone(), sides[1].clone(), join_broadcast_max_bytes);
        if candidate.broadcast_eligible
            && let Some(rendered) = render_broadcast_join(request, pushdown_req, join)?
            && let Some(sql) = build_broadcast_join_sql(
                &candidate,
                &rendered,
                window,
                &inputs,
                &udf_name,
                &distribute_udf_name,
            )?
        {
            return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
        }
    }

    let sql = build_n_scan_join_sql(
        request,
        pushdown_req,
        join,
        &sides,
        &inputs,
        &udf_name,
        &distribute_udf_name,
    )?;
    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

#[cfg(test)]
#[path = "joins_tests.rs"]
mod tests;

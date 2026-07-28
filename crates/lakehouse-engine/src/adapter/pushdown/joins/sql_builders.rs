use crate::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, ProjectionItem, ScanSpec, render_ordered,
};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use std::collections::HashMap;
use vs_expression::render_df_filter_safe;

use super::super::file_resolution::relativize_shards_to_root;
use super::super::support::{
    build_scan_driving_sql, collect_all_column_names, extract_limit, quote_ident, shard_count,
    strip_table_alias,
};
use super::super::topn::parse_sort_flags;
use super::planning::{
    DetectedJoin, JoinSides, ResolvedJoinSide, disjoint_schema_guard, involved_table_columns,
};
use super::rendering::{
    collect_column_tables, cross_side_residual_filter, extract_join_projection,
    projection_item_select_sql, referenced_side_columns, render_df_filter_qualified,
    render_expression_qualified, render_join_condition, render_selectlist_item_qualified,
    side_local_filter,
};

/// The translator-reuse artifacts for a broadcast inner equi-join, rendered once
/// in the VS planning layer and consumed by the broadcast fan-out SQL builder.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RenderedJoinPushdown {
    /// The rendered DataFusion SQL boolean join condition (→ [`JoinSpec::condition`]).
    pub condition: String,
    /// The rendered cross-table WHERE filter, or `None` when the request carries
    /// none (or it is trivially true and Exasol keeps it as a backstop).
    pub filter: Option<String>,
    /// The cross-table projection, spanning columns from both tables, in order.
    pub projection: Vec<ProjectionItem>,
    /// The Exasol EMITS type per projected column, positionally aligned with
    /// `projection`.
    pub projection_types: Vec<String>,
}

/// Render every `vs-expression` artifact a broadcast inner equi-join needs, after
/// enforcing the disjoint-column-name guard.
///
/// Broadcast is a two-table optimization, so `join.tables[0]`/`[1]` are the two
/// involved tables and `join.conditions[0]` is the equi-condition. Returns
/// `Ok(None)` — a clean decline, NOT an error — when the two tables share any
/// column name (the guard fails), the equi-condition cannot be rendered, or the
/// derived projection widened to the full base row (#196); the caller then falls
/// through to the deterministic N-scan fallback, exactly as for any other join off
/// the broadcast path. `Ok(Some(..))` carries the rendered join
/// condition, the cross-table WHERE filter, and the cross-table projection with its
/// EMITS types. `Err` is reserved for a genuinely malformed request with no column
/// metadata at all (the same contract [`project_columns`] enforces for the
/// single-table path).
///
/// Rendering is side-agnostic: the translator emits bare column names, so the
/// result does not depend on which side is later selected as fact vs dimension.
pub(crate) fn render_broadcast_join(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
) -> Result<Option<RenderedJoinPushdown>, UdfError> {
    let left_cols = involved_table_columns(request, &join.tables[0].table_name);
    let right_cols = involved_table_columns(request, &join.tables[1].table_name);
    if !disjoint_schema_guard(&left_cols, &right_cols) {
        return Ok(None);
    }

    let condition = match render_join_condition(&join.conditions[0]) {
        Some(condition) => condition,
        None => return Ok(None),
    };

    let filter = pushdown_req
        .get("filter")
        .filter(|f| !f.is_null())
        .and_then(render_df_filter_safe);

    let (projection, projection_types, widened) =
        extract_join_projection(request, pushdown_req, join)?;
    // The derived projection is the full two-table base row, not one item per
    // select-list item, so a broadcast fan-out would emit the wrong column shape.
    // Decline to the unified N-scan fallback, which re-renders the select list
    // table-qualified in the Exasol dialect over its own wrapper (#196).
    if widened {
        return Ok(None);
    }

    Ok(Some(RenderedJoinPushdown {
        condition,
        filter,
        projection,
        projection_types,
    }))
}

/// Side `i`'s Exasol virtual table name (UPPERCASE) maps to `aliases[i]`
/// (`LHS_T{i}`), so every column reference the N-scan wrapper renders is
/// table-qualified from its `tableName`.
fn build_n_scan_alias_map(
    sides: &[ResolvedJoinSide],
    aliases: &[String],
) -> HashMap<String, String> {
    sides
        .iter()
        .zip(aliases)
        .map(|(side, alias)| (side.table_name.to_ascii_uppercase(), alias.clone()))
        .collect()
}

/// Render the N-scan fallback's FROM as a left-to-right `INNER JOIN … ON` chain and
/// return it together with any join conditions that could not be attached to a join
/// point (untagged, or referencing no known leg). Those unattachable conditions
/// become outer-WHERE residual conjuncts — for an inner join a condition in the
/// WHERE is result-equivalent to the same condition in an `ON` clause, so this is a
/// safe last-resort backstop.
///
/// `conditions[i]` is the pre-rendered, table-qualified SQL for `raw_conditions[i]`.
/// Each condition GREEDILY attaches to the earliest join point where every table it
/// touches is in scope — the join point that brings its highest-indexed leg in.
/// Scope is resolved by the SET of `tableName`s the raw condition references
/// (via [`collect_column_tables`]), NEVER by column name, so two legs sharing a
/// column name can never fool the attachment. A join point with no attached
/// condition renders `ON 1=1`.
fn build_n_scan_join_from(
    fan_outs: &[String],
    aliases: &[String],
    raw_conditions: &[Json],
    conditions: &[String],
    sides: &[ResolvedJoinSide],
) -> (String, Vec<String>) {
    let leg_index: HashMap<String, usize> = sides
        .iter()
        .enumerate()
        .map(|(i, s)| (s.table_name.to_ascii_uppercase(), i))
        .collect();
    let last_join_point = aliases.len().saturating_sub(1);

    let mut on_at: Vec<Vec<String>> = vec![Vec::new(); aliases.len()];
    let mut residual: Vec<String> = Vec::new();
    for (raw, rendered) in raw_conditions.iter().zip(conditions) {
        let mut tables = std::collections::HashSet::new();
        let mut has_untagged = false;
        let mut any_column = false;
        collect_column_tables(raw, &mut tables, &mut has_untagged, &mut any_column);
        let resolvable =
            any_column && !has_untagged && tables.iter().all(|t| leg_index.contains_key(t));
        match resolvable
            .then(|| tables.iter().map(|t| leg_index[t]).max())
            .flatten()
        {
            // The earliest join point in scope is the one bringing the
            // highest-indexed leg in; clamp to a real join point (≥ 1, ≤ last).
            // Guard `last_join_point >= 1` (i.e. at least one join exists) first:
            // with a single leg there is no join point to attach to (and
            // `clamp(1, 0)` would panic since min > max), so fall through to
            // residual; behavior for N≥2 is unchanged.
            Some(m) if last_join_point >= 1 => {
                on_at[m.clamp(1, last_join_point)].push(rendered.clone())
            }
            _ => residual.push(rendered.clone()),
        }
    }

    let mut from = format!("({}) AS {}", fan_outs[0], quote_ident(&aliases[0]));
    for k in 1..aliases.len() {
        let on = if on_at[k].is_empty() {
            "1=1".to_string()
        } else {
            on_at[k]
                .iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        from.push_str(&format!(
            " INNER JOIN ({}) AS {} ON {on}",
            fan_outs[k],
            quote_ident(&aliases[k])
        ));
    }
    (from, residual)
}

/// Every column of all involved tables as a table-qualified projection item, in
/// side order. `cols_per_side[i]` belongs to the side aliased `aliases[i]`.
fn n_full_row_qualified_items(
    aliases: &[String],
    cols_per_side: &[Vec<(String, String)>],
) -> Vec<ProjectionItem> {
    aliases
        .iter()
        .zip(cols_per_side)
        .flat_map(|(alias, cols)| {
            cols.iter().map(move |(name, _)| ProjectionItem::Expr {
                expr: format!("{}.{}", quote_ident(alias), quote_ident(name)),
            })
        })
        .collect()
}

/// The N-scan wrapper's outer SELECT list, table-qualified. An absent/empty select
/// list projects every column of all involved tables in side order. An item that
/// cannot be rendered is a last-resort hard error (no native re-plan).
fn n_scan_join_select_items(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
    aliases: &[String],
    cols_per_side: &[Vec<(String, String)>],
) -> Result<Vec<ProjectionItem>, UdfError> {
    match pushdown_req.get("selectList") {
        Some(Json::Array(list)) if !list.is_empty() => {
            let mut items = Vec::with_capacity(list.len());
            for item in list {
                let sql = render_selectlist_item_qualified(item, alias_of).ok_or_else(|| {
                    UdfError::User(
                        "join pushdown declined: a select-list item could not be rendered for the \
                         qualified N-scan join; this is a hard error, not a native re-plan"
                            .into(),
                    )
                })?;
                items.push(ProjectionItem::Expr { expr: sql });
            }
            Ok(items)
        }
        _ => Ok(n_full_row_qualified_items(aliases, cols_per_side)),
    }
}

/// The outer wrapper's SELECT-list SQL plus its trailing GROUP BY / HAVING /
/// ORDER BY / LIMIT clause suffix, shared by the N-scan join wrapper and the grouped
/// single-table fallback — both render the same clauses table-qualified over their
/// own FROM. `select` is the SELECT body (`*`, or the qualified items joined by
/// `, `); `trailing` is the pre-assembled clause suffix (each clause carrying its own
/// leading space) the caller appends verbatim after its FROM — and, for the N-scan
/// wrapper, after its WHERE. The declining precedence is preserved by computing the
/// clauses in order: SELECT item, GROUP BY, HAVING, ORDER BY (so the first
/// unrenderable clause is the one that surfaces its hard error).
struct OuterWrapperClauses {
    select: String,
    trailing: String,
}

fn outer_wrapper_clauses(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
    aliases: &[String],
    cols_per_side: &[Vec<(String, String)>],
) -> Result<OuterWrapperClauses, UdfError> {
    let select_items = n_scan_join_select_items(pushdown_req, alias_of, aliases, cols_per_side)?;
    let group_by = qualified_join_group_by(pushdown_req, alias_of)?;
    let having = qualified_join_having(pushdown_req, alias_of)?;
    let order_by = qualified_join_order_by(pushdown_req, alias_of)?;
    let limit = extract_limit(pushdown_req);

    let select = if select_items.is_empty() {
        "*".to_string()
    } else {
        select_items
            .iter()
            .map(projection_item_select_sql)
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut trailing = String::new();
    if let Some(clause) = group_by {
        trailing.push_str(&format!(" GROUP BY {clause}"));
    }
    if let Some(clause) = having {
        trailing.push_str(&format!(" HAVING {clause}"));
    }
    if let Some(clause) = order_by {
        trailing.push_str(&format!(" ORDER BY {clause}"));
    }
    if let Some(n) = limit {
        trailing.push_str(&format!(" LIMIT {n}"));
    }

    Ok(OuterWrapperClauses { select, trailing })
}

/// Build the N-scan (N ≥ 2) unaccelerated inner-join SQL — the SOLE unaccelerated
/// fallback renderer (the two-involved-table case is simply N = 2). Each involved
/// table is scanned through its own sharded fan-out and reconstructed into the
/// original inner join by Exasol's core engine via a left-to-right `INNER JOIN … ON`
/// chain.
///
/// Each side emits its full column set (narrowed to the columns the wrapper actually
/// references across all clauses), so the outer wrapper's SELECT, every join
/// condition, WHERE, aggregate, GROUP BY, HAVING, and ORDER BY can reference any
/// column the join needs — all rendered TABLE-QUALIFIED (`"LHS_T{i}"."COL"`) from
/// each `column` node's `tableName`, so the wrapper is correct whether or not any
/// two involved tables share a column name.
///
/// The FROM is a left-to-right `INNER JOIN … ON` chain: each join
/// condition greedily attaches to the earliest join point where every table it
/// touches is in scope, resolved by the SET of `tableName`s the condition references
/// (never by column name, so shared column names cannot misroute scope); a join
/// point with no newly-resolvable condition renders `ON 1=1`. Each side's side-local
/// WHERE conjuncts are pushed into that side's fan-out leg; only cross-table /
/// OR-spanning / untagged residual conjuncts (and any untaggable join condition)
/// remain in the outer WHERE, each parenthesized so a top-level `OR` cannot bind
/// across the ANDs. For an inner join this is result-equivalent to single-node
/// evaluation, independent of join order and of shared column names.
///
/// Returns an `Err` (a hard client-facing error, no native re-plan) only when the
/// wrapper genuinely cannot be built: an involved table carries no column metadata,
/// or a join condition (or a pushed select/GROUP BY/HAVING/ORDER BY element) cannot
/// be rendered at all.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_n_scan_join_sql(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    tuning: &JoinScanTuning,
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    let cols_per_side: Vec<Vec<(String, String)>> = sides
        .iter()
        .map(|s| involved_table_columns(request, &s.table_name))
        .collect();
    if cols_per_side.iter().any(|c| c.is_empty()) {
        return Err(UdfError::User(
            "join pushdown declined: an involved table carries no column metadata, so the \
             unaccelerated N-scan fallback cannot be built; this is a hard error, not a \
             native re-plan"
                .into(),
        ));
    }

    let aliases: Vec<String> = (0..sides.len()).map(|i| format!("LHS_T{i}")).collect();
    let alias_of = build_n_scan_alias_map(sides, &aliases);

    // Every join-tree condition, table-qualified. A condition is the one clause with
    // no lower fallback: if it cannot be rendered even qualified, no correct join SQL
    // exists → last-resort hard error (no native re-plan).
    let mut conditions = Vec::with_capacity(join.conditions.len());
    for cond in &join.conditions {
        let rendered = render_expression_qualified(cond, &alias_of).ok_or_else(|| {
            UdfError::User(
                "join pushdown declined: a join condition could not be rendered against the \
                 qualified N-scan schema; this is a hard error, not a native re-plan"
                    .into(),
            )
        })?;
        conditions.push(rendered);
    }

    // The outer WHERE keeps ONLY the residual conjuncts NOT side-local to a
    // single leg (cross-table, OR-spanning, or untagged); every side-local conjunct
    // is pushed into its leg's fan-out below and never re-applied here. The partition
    // is exact and total (see `side_local_filter` vs `cross_side_residual_filter`).
    let filter = pushdown_req
        .get("filter")
        .filter(|f| !f.is_null())
        .and_then(cross_side_residual_filter)
        .and_then(|residual| render_df_filter_qualified(&residual, &alias_of));

    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown_req, &alias_of, &aliases, &cols_per_side)?;

    // Per-side fan-out: narrow each leg's projection to the columns the wrapper
    // references (across the SELECT list, ALL join conditions, WHERE, GROUP BY,
    // HAVING, and ORDER BY), and push each side's side-local WHERE conjuncts down as a
    // DataFusion filter. Cross-table and OR-spanning conjuncts stay only in the outer
    // WHERE (`filter`), the correctness backstop. All N-1 conditions are passed as one
    // JSON array so `referenced_side_columns` (which walks arbitrary nodes) keeps a
    // side's column referenced by ANY condition.
    let where_filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    let all_conditions = Json::Array(join.conditions.clone());
    let mut fan_outs = Vec::with_capacity(sides.len());
    for (i, side) in sides.iter().enumerate() {
        let narrowed = referenced_side_columns(
            pushdown_req,
            &all_conditions,
            &side.table_name,
            &cols_per_side[i],
        );
        let side_filter = where_filter.and_then(|f| side_local_filter(f, &side.table_name));
        fan_outs.push(build_side_fan_out_sql(
            side,
            &narrowed,
            side_filter.as_ref(),
            tuning,
            udf_name,
            distribute_udf_name,
        ));
    }

    // Assemble the INNER JOIN … ON chain. FROM is the chain of
    // aliased fan-out legs with each condition greedily attached by table-name set;
    // the outer WHERE carries the residual filter plus any untaggable join condition.
    let (from, residual_conditions) =
        build_n_scan_join_from(&fan_outs, &aliases, &join.conditions, &conditions, sides);

    let mut where_parts: Vec<String> = residual_conditions
        .iter()
        .map(|c| format!("({c})"))
        .collect();
    if let Some(f) = &filter {
        where_parts.push(format!("({f})"));
    }

    let mut sql = format!("SELECT {select} FROM {from}");
    if !where_parts.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
    }
    sql.push_str(&trailing);
    Ok(sql)
}

/// The DataFusion execution + sharding knobs threaded into join SQL building.
///
/// Bundled so the two join SQL builders take one config parameter instead of eight
/// positional numbers whose order is easy to transpose (guardrails: few arguments,
/// config at high levels).
pub(super) struct JoinScanTuning {
    pub(super) cluster_nodes: usize,
    pub(super) parallelism_factor: usize,
    pub(super) df_target_partitions: usize,
    pub(super) df_batch_size: usize,
    pub(super) df_threads_per_udf: usize,
    pub(super) memory_pool_fraction: f64,
    pub(super) instance_overhead_mb: u64,
    pub(super) s3_max_connections: usize,
}

/// Relativize one file list against its table root (single-list convenience over
/// [`relativize_shards_to_root`], preserving order and byte sizes).
fn relativize_files_to_root(files: Vec<FileEntry>, table_root: &str) -> Vec<FileEntry> {
    relativize_shards_to_root(vec![files], table_root)
        .pop()
        .unwrap_or_default()
}

/// Assemble the shard-invariant [`ScanSpec`] both join fan-out builders emit: an
/// empty `files` (the shards travel separately), no limit / order / aggregate /
/// group, and the six DataFusion + S3 tuning knobs copied from `tuning`. `primary`
/// is the side the spec scans (its `table_root`, `logical_schema`, `name_mapping`,
/// and effective `storage`); `projection`, `filter`, `emit_exa_types`, and `join`
/// are the only per-path differences (the N-scan leg passes `join: None`, the
/// broadcast path passes the dimension-side join block).
fn join_fan_out_scan_spec(
    primary: &ResolvedJoinSide,
    projection: Vec<ProjectionItem>,
    filter: Option<String>,
    emit_exa_types: Vec<String>,
    join: Option<JoinSpec>,
    tuning: &JoinScanTuning,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root: primary.table_root.clone(),
            projection,
            filter,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: false,
            emit_exa_types,
            logical_schema: primary.logical_schema.clone(),
            name_mapping: primary.name_mapping.clone(),
            join,
            storage: primary.effective_storage.clone(),
            df_target_partitions: tuning.df_target_partitions,
            df_batch_size: tuning.df_batch_size,
            df_threads_per_udf: tuning.df_threads_per_udf,
            memory_pool_fraction: tuning.memory_pool_fraction,
            instance_overhead_mb: tuning.instance_overhead_mb,
            s3_max_connections: tuning.s3_max_connections,
        },
        files: vec![],
    }
}

/// Build one side's single-table sharded fan-out SQL (an outer ungrouped scalar
/// `LAKEHOUSE_SCAN` over the nested distributor, or a from-less scalar call on
/// literals for a single shard — no `SELECT * FROM (...)` wrapper),
/// emitting the columns the outer wrapper references for this side and pushing this
/// side's SIDE-LOCAL WHERE conjuncts down as a DataFusion filter. No join block, no
/// limit push. Used for BOTH sides of the unaccelerated fallback: the outer Exasol
/// query (see [`build_n_scan_join_sql`]) still applies the projection, conditions, and
/// the FULL `WHERE`, so `columns` (the side's narrowed `(UPPERCASE name, Exasol
/// type)` list, see [`referenced_side_columns`]) must expose every column any outer
/// clause references. `side_filter` (see [`side_local_filter`]) is rendered bare-name
/// via `render_df_filter_safe` so DataFusion row-group-prunes and row-filters this
/// leg before emitting, rather than shipping every row for Exasol to filter.
pub(super) fn build_side_fan_out_sql(
    side: &ResolvedJoinSide,
    columns: &[(String, String)],
    side_filter: Option<&Json>,
    tuning: &JoinScanTuning,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let proj_cols: Vec<ProjectionItem> = columns
        .iter()
        .map(|(name, _)| ProjectionItem::Column(name.clone()))
        .collect();
    let proj_types: Vec<String> = columns.iter().map(|(_, ty)| ty.clone()).collect();

    let g = shard_count(
        tuning.cluster_nodes,
        tuning.parallelism_factor,
        side.files.len(),
    );
    let shards = crate::adapter::sharding::partition_files_by_bytes(side.files.clone(), g);
    let shards = relativize_shards_to_root(shards, &side.table_root);

    // Render BARE (strip Exasol's `tableAlias`): the fan-out is a single-table
    // scan whose relation exposes bare uppercase column names, so an
    // alias-qualified reference would not resolve — exactly the single-table
    // scan path's contract. The outer wrapper's WHERE re-qualifies separately.
    let filter = side_filter
        .map(strip_table_alias)
        .and_then(|f| render_df_filter_safe(&f));
    let spec = join_fan_out_scan_spec(
        side,
        proj_cols.clone(),
        filter,
        proj_types.clone(),
        None,
        tuning,
    );
    build_scan_driving_sql(
        &spec,
        &shards,
        &proj_cols,
        &proj_types,
        None,
        None,
        &[],
        &[],
        udf_name,
        distribute_udf_name,
    )
}

/// Build the broadcast fan-out scan-driving SQL.
///
/// The fact (larger) side is sharded into G byte-balanced work units exactly as the
/// single-table path does; the dimension (smaller) side's FULL file list, table
/// root, logical schema, join type, and rendered condition ride ONCE in the
/// shard-invariant common blob's join block ([`JoinSpec`]). Every shard invocation
/// therefore re-scans the same dimension side and joins it against its fact-file
/// subset node-locally, with no cross-shard exchange. Reuses [`build_scan_driving_sql`]
/// unchanged — the join block travels transparently inside the common blob.
///
/// One `StorageProps` serves both registered tables inside the single DataFusion
/// session; the fact side's effective storage is used. When vended credentials are
/// disabled (the common MinIO case) both sides' effective storage is identical, so
/// this is exact; with per-prefix vended STS creds both tables must be readable with
/// the fact side's grant (both live under one warehouse for the broadcast target).
pub(super) fn build_broadcast_join_sql(
    sides: &JoinSides,
    rendered: &RenderedJoinPushdown,
    tuning: &JoinScanTuning,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let fact = &sides.fact;
    let dimension = &sides.dimension;

    let g = shard_count(
        tuning.cluster_nodes,
        tuning.parallelism_factor,
        fact.files.len(),
    );
    let shards = crate::adapter::sharding::partition_files_by_bytes(fact.files.clone(), g);
    let shards = relativize_shards_to_root(shards, &fact.table_root);

    let join = JoinSpec {
        table_root: dimension.table_root.clone(),
        files: relativize_files_to_root(dimension.files.clone(), &dimension.table_root),
        logical_schema: dimension.logical_schema.clone(),
        name_mapping: dimension.name_mapping.clone(),
        join_type: JoinType::Inner,
        condition: rendered.condition.clone(),
    };

    let spec = join_fan_out_scan_spec(
        fact,
        rendered.projection.clone(),
        rendered.filter.clone(),
        rendered.projection_types.clone(),
        Some(join),
        tuning,
    );

    build_scan_driving_sql(
        &spec,
        &shards,
        &rendered.projection,
        &rendered.projection_types,
        None,
        None,
        &[],
        &[],
        udf_name,
        distribute_udf_name,
    )
}

/// The N-scan wrapper's `GROUP BY` clause (without the keyword), table-qualified.
/// `None` when the request carries no non-empty `groupBy`. A group key that cannot
/// be rendered is a last-resort hard error (no native re-plan).
fn qualified_join_group_by(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
) -> Result<Option<String>, UdfError> {
    let keys = match pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
    {
        Some(keys) => keys,
        None => return Ok(None),
    };
    let mut parts = Vec::with_capacity(keys.len());
    for key in keys {
        parts.push(render_expression_qualified(key, alias_of).ok_or_else(|| {
            UdfError::User(
                "join pushdown declined: a GROUP BY key could not be rendered for the qualified \
                 N-scan join; this is a hard error, not a native re-plan"
                    .into(),
            )
        })?);
    }
    Ok(Some(parts.join(", ")))
}

/// The N-scan wrapper's `HAVING` clause (without the keyword), table-qualified.
/// `None` when the request carries no `having`. An unrenderable HAVING is a
/// last-resort hard error (dropping it would return wrong rows; no native re-plan).
fn qualified_join_having(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
) -> Result<Option<String>, UdfError> {
    match pushdown_req.get("having").filter(|h| !h.is_null()) {
        Some(having) => Ok(Some(
            render_expression_qualified(having, alias_of).ok_or_else(|| {
                UdfError::User(
                    "join pushdown declined: HAVING could not be rendered for the qualified \
                     N-scan join; this is a hard error, not a native re-plan"
                        .into(),
                )
            })?,
        )),
        None => Ok(None),
    }
}

/// The N-scan wrapper's `ORDER BY` clause (without the keyword), table-qualified.
/// `None` when the request carries no non-empty `orderBy`. Any expression an
/// involved-table column can render against — bare column or arbitrary
/// expression tree — is rendered via [`render_expression_qualified`]; an element
/// whose expression does not render (or whose direction/NULL-placement flags are
/// absent) is a last-resort hard error (dropping it would return an unordered
/// result Exasol delegated and no longer re-sorts; no native re-plan).
fn qualified_join_order_by(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
) -> Result<Option<String>, UdfError> {
    let elements = match pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
    {
        Some(elements) => elements,
        None => return Ok(None),
    };
    let decline = || {
        UdfError::User(
            "join pushdown declined: an ORDER BY key could not be rendered for the qualified \
             N-scan join; this is a hard error, not a native re-plan"
                .into(),
        )
    };
    let mut parts = Vec::with_capacity(elements.len());
    for element in elements {
        let (ascending, nulls_last) = parse_sort_flags(element).ok_or_else(decline)?;
        let expr = element.get("expression").ok_or_else(decline)?;
        let rendered = render_expression_qualified(expr, alias_of).ok_or_else(decline)?;
        parts.push(render_ordered(&rendered, ascending, nulls_last));
    }
    Ok(Some(parts.join(", ")))
}

/// The subset of `all_cols` the qualified single-table wrapper actually references,
/// as positionally-aligned `(ProjectionItem::Column, Exasol type)` lists — the shared
/// inner-scan projection for BOTH decline wrappers (grouped and single-group Case
/// 2/3), replacing the old whole-table `full_row_projection` (issue #160).
///
/// Walks the FULL expression tree of every clause the wrapper renders — the SELECT
/// list, WHERE filter, GROUP BY keys, HAVING, and ORDER BY — via
/// [`collect_all_column_names`], so every column the rendered SQL names is projected
/// and none is missing at runtime. Column order and Exasol types are preserved from
/// `all_cols`. Always returns at least one column (an empty EMITS clause is invalid
/// in Exasol): when the request references no source column it falls back to the
/// first column of `all_cols`.
pub(in super::super) fn referenced_column_projection(
    pushdown_req: &Json,
    all_cols: &[(String, String)],
) -> (Vec<ProjectionItem>, Vec<String>) {
    let mut names = std::collections::HashSet::new();
    if let Some(list) = pushdown_req.get("selectList") {
        collect_all_column_names(list, &mut names);
    }
    if let Some(f) = pushdown_req.get("filter").filter(|f| !f.is_null()) {
        collect_all_column_names(f, &mut names);
    }
    for key in ["groupBy", "orderBy"] {
        if let Some(v) = pushdown_req.get(key) {
            collect_all_column_names(v, &mut names);
        }
    }
    if let Some(h) = pushdown_req.get("having").filter(|h| !h.is_null()) {
        collect_all_column_names(h, &mut names);
    }

    let mut cols = Vec::new();
    let mut types = Vec::new();
    for (name, ty) in all_cols {
        if names.contains(name) {
            cols.push(ProjectionItem::Column(name.clone()));
            types.push(ty.clone());
        }
    }
    // Guarantee at least one projected column: an empty EMITS clause is invalid in
    // Exasol. A request referencing no source column falls back to the first column.
    if cols.is_empty()
        && let Some((name, ty)) = all_cols.first()
    {
        cols.push(ProjectionItem::Column(name.clone()));
        types.push(ty.clone());
    }
    (cols, types)
}

/// Build the qualified single-table wrapper for an aggregate request that could not
/// be decomposed into the partial/merge plan. Serves BOTH decline paths: a GROUP BY
/// request (an undecomposable scalar-over-aggregate item, a non-numeric aggregate
/// with no HAVING, or any other non-pushable grouped shape) AND a single-group Case
/// 2/3 `COUNT(DISTINCT)` request (more than one distinct, or a distinct mixed with an
/// ordinary aggregate) that cannot fan out. This is the join N-scan fallback at
/// N = 1: one aliased raw fan-out subquery, no cross-join and no join condition, with
/// the exact select list, GROUP BY (rendered only when the request carries one — so
/// the single-group shape emits no GROUP BY), HAVING, ORDER BY, and LIMIT rendered as
/// ordinary Exasol SQL over it, so Exasol's core engine computes the aggregate over
/// the returned rows.
///
/// Reuses the join path's qualified renderers verbatim: the single table is aliased
/// `LHS_T0`, every column reference is table-qualified against that alias, and
/// aggregates are spliced verbatim by the `vs-expression` translator (Exasol
/// aggregates over materialized rows, not over merged partials). The per-shard scan
/// stays LIMIT-free and sort-free (`fan_out_spec` carries no limit/order_by); the
/// group keys, HAVING, ORDER BY, and LIMIT live only in the outer wrapper. The WHERE
/// filter is applied inside the scan (via `fan_out_spec.filter`), so no outer WHERE
/// is needed — mirroring the grouped push-down path. The result column count and
/// per-column types match Exasol's positional `selectListDataTypes` validation, so
/// this never emits the `04000`-triggering bare row scan.
pub(in super::super) fn build_qualified_single_table_fallback_sql<E: Clone + Into<FileEntry>>(
    request: &Json,
    pushdown_req: &Json,
    fan_out_spec: &ScanSpec,
    shards: &[Vec<E>],
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    const ALIAS: &str = "LHS_T0";

    // Alias EVERY involved table name to the single subquery alias, so a column
    // node's `tableName` (or a stale request `tableAlias`) resolves to `"LHS_T0"`.
    let alias_of: HashMap<String, String> = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .map(|tables| {
            tables
                .iter()
                .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
                .map(|name| (name.to_ascii_uppercase(), ALIAS.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let aliases = vec![ALIAS.to_string()];

    // The scan exposes the full base row; reconstruct the `(name, type)` universe
    // from the fan-out spec so the no-select-list fallback (unusual for a grouped
    // request) still resolves types from the one side.
    let all_cols: Vec<(String, String)> = fan_out_spec
        .common
        .projection
        .iter()
        .zip(fan_out_spec.common.emit_exa_types.iter())
        .filter_map(|(item, ty)| match item {
            ProjectionItem::Column(name) => Some((name.clone(), ty.clone())),
            ProjectionItem::Expr { .. } => None,
        })
        .collect();
    let cols_per_side = vec![all_cols];

    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown_req, &alias_of, &aliases, &cols_per_side)?;

    // One aliased raw sharded fan-out. LIMIT-free / sort-free / no aggregates — the
    // fan-out spec already guarantees this.
    let proj_cols = fan_out_spec.common.projection.clone();
    let proj_types = fan_out_spec.common.emit_exa_types.clone();
    let fan_out = build_scan_driving_sql(
        fan_out_spec,
        shards,
        &proj_cols,
        &proj_types,
        None,
        None,
        &[],
        &[],
        udf_name,
        distribute_udf_name,
    );

    let mut sql = format!("SELECT {select} FROM ({fan_out}) AS {}", quote_ident(ALIAS));
    sql.push_str(&trailing);
    Ok(sql)
}

/// Dispatch a request to the qualified single-table fallback wrapper, from the
/// shared shard-invariant `base` `build_dispatch_sql` builds once.
///
/// Both `build_dispatch_sql` decline guards — the group-by-not-decomposed guard
/// and the multi/mixed `COUNT(DISTINCT)` guard — reach this same shape: derive the
/// referenced-column projection, build the fan-out spec from `base` with only the
/// projection/filter/emit-types set (every other field, including LIMIT/ORDER
/// BY/aggregates/group keys/distinct, stays at `base`'s neutral placeholder — the
/// fan-out is always LIMIT-free and sort-free here, see
/// [`build_qualified_single_table_fallback_sql`]'s doc), render the wrapper SQL, and
/// wrap it in the pushdown response envelope.
#[allow(clippy::too_many_arguments)]
pub(in super::super) fn qualified_single_table_fallback_pushdown(
    request: &Json,
    pushdown_req: &Json,
    base: &CommonScanSpec,
    filter: Option<String>,
    shards: &[Vec<FileEntry>],
    col_types: &[(String, String)],
    udf_name: &str,
    distribute_udf_name: &str,
) -> Result<Json, UdfError> {
    let (fb_proj_cols, fb_proj_types) = referenced_column_projection(pushdown_req, col_types);
    let fan_out_spec = ScanSpec {
        common: CommonScanSpec {
            projection: fb_proj_cols,
            filter,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: false,
            emit_exa_types: fb_proj_types,
            ..base.clone()
        },
        files: vec![],
    };
    let sql = build_qualified_single_table_fallback_sql(
        request,
        pushdown_req,
        &fan_out_spec,
        shards,
        udf_name,
        distribute_udf_name,
    )?;
    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

#[cfg(test)]
mod tests {
    use super::super::super::support::{DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME};
    use super::super::ineligible_join_decline;
    use super::super::planning::{
        IneligibleJoinReason, JoinShape, detect_join, join_requires_exasol_postprocessing,
    };
    use super::super::tests::{
        detected_join, equi_condition, join_request, nq3_join_request, resolved_side,
        three_table_join_request, two_scan_tuning,
    };
    use super::*;
    use crate::adapter::pushdown::test_support::*;
    use vs_expression::{render_expression_exasol_safe, render_expression_safe};

    /// The Q1-shape three-table inner-join pushdown request:
    /// `(SUPPLIER ⋈ NATION) ⋈ REGION`, all three in `TABLE_MAP`. Leaves in stable
    /// tree order SUPPLIER, NATION, REGION; two join conditions
    /// (`S_NATIONKEY=N_NATIONKEY`, `N_REGIONKEY=R_REGIONKEY`).
    fn q1_join_request() -> Json {
        serde_json::json!({
            "involvedTables": [
                {"name": "SUPPLIER", "columns": [
                    {"name": "S_SUPPKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "S_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "S_NAME", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "NATION", "columns": [
                    {"name": "N_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "N_REGIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
                {"name": "REGION", "columns": [
                    {"name": "R_REGIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "R_NAME", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"type": "join", "join_type": "inner",
                        "left": {"name": "SUPPLIER", "type": "table"},
                        "right": {"name": "NATION", "type": "table"},
                        "condition": {"type": "predicate_equal",
                            "left": {"type": "column", "name": "S_NATIONKEY", "tableName": "SUPPLIER"},
                            "right": {"type": "column", "name": "N_NATIONKEY", "tableName": "NATION"}}},
                    "right": {"name": "REGION", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "N_REGIONKEY", "tableName": "NATION"},
                        "right": {"type": "column", "name": "R_REGIONKEY", "tableName": "REGION"}}},
                "selectList": [
                    {"type": "column", "name": "S_NAME", "tableName": "SUPPLIER"},
                    {"type": "column", "name": "R_NAME", "tableName": "REGION"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"SUPPLIER": "lh.supplier", "NATION": "lh.nation", "REGION": "lh.region"}})
                    .to_string()},
        })
    }

    // -----------------------------------------------------------------------
    // Join SQL-shape and decline routing
    // -----------------------------------------------------------------------

    /// pushdown-planning-join "A join outside the broadcast contract is declined
    /// safely". Two independent facets are asserted together because they are the
    /// two ways a join leaves the broadcast contract:
    ///
    /// 1. A shape `detect_join` classifies `Ineligible` (a non-inner join node in the
    ///    tree, or a malformed shape) cannot be rendered at all — so it MUST map to a
    ///    `User` decline error, NEVER fall through to the single-table path (which
    ///    would scan only the first involved table and silently drop the join).
    ///    Spanning more than two tables, non-equi, and overlapping column names are
    ///    NOT Ineligible — they are served by the unified fallback.
    /// 2. An otherwise-eligible join whose two tables share a column name fails the
    ///    disjoint-column guard, so `render_broadcast_join` declines with `Ok(None)`.
    ///    The `vs-expression` translator emits only bare column names, so a two-scan
    ///    wrapper would carry an ambiguous `ON`/`WHERE`/`SELECT` — hence the router
    ///    treats `None` as "fallback cannot be built" and errors rather than emit a
    ///    wrong plan.
    #[test]
    fn join_outside_contract_declined_safely() {
        // Facet 1: every ineligible reason declines to a HARD error — a
        // client-facing F-UDF-CL-RUST-9001, NEVER a native re-plan. The message must
        // say so plainly (contains "declined"/"cannot") and MUST NOT claim a retry.
        for reason in [
            IneligibleJoinReason::NotInnerJoinType,
            IneligibleJoinReason::UnsupportedShape,
        ] {
            let err = ineligible_join_decline(reason);
            match err {
                UdfError::User(msg) => {
                    assert!(
                        msg.contains("join pushdown declined") && msg.contains("cannot"),
                        "ineligible reason {reason:?} must be a plain hard-error decline: {msg}"
                    );
                    assert!(
                        !msg.contains("retry"),
                        "ineligible reason {reason:?} must NOT claim a native retry: {msg}"
                    );
                }
                other => panic!("ineligible join must be a User decline, got {other:?}"),
            }
        }

        // An outer join reaches the decline path as Ineligible, never Join.
        let outer = join_request(
            serde_json::json!({"join_type": "left_outer"}),
            equi_condition(),
        );
        assert!(
            matches!(
                detect_join(&outer, &pd(&outer)),
                Ok(JoinShape::Ineligible(
                    IneligibleJoinReason::NotInnerJoinType
                ))
            ),
            "an outer join must classify Ineligible so the decline path is taken"
        );

        // Facet 2: overlapping column names → render declines with Ok(None).
        let mut request = join_request(Json::Null, equi_condition());
        for table_idx in [0, 1] {
            request["involvedTables"][table_idx]["columns"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "name": "SHARED_COL",
                    "dataType": {"type": "varchar", "size": 10}
                }));
        }
        let detected = detected_join(&request);
        let rendered = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("guard failure is a decline, not an error");
        assert!(
            rendered.is_none(),
            "overlapping column names must decline broadcast rendering (Ok(None))"
        );
    }

    /// A widened derived projection is the full two-table base row, not one item per
    /// select-list item, so a broadcast fan-out would emit the wrong column shape.
    /// `render_broadcast_join` must take its EXISTING clean-decline exit (`Ok(None)`,
    /// never an error) and let the unified N-scan wrapper re-render the select list
    /// table-qualified (#196).
    ///
    /// The widening trigger is issue #234's own: a rendered item whose declared type
    /// is not a valid UDF EMITS output (`TIMESTAMP WITH LOCAL TIME ZONE`, sqlCode
    /// 22002). The request carries no aggregate/GROUP BY/ORDER BY/LIMIT/HAVING, so it
    /// genuinely reaches broadcast eligibility live rather than being skipped by
    /// `join_requires_exasol_postprocessing`. The same request with an EMITS-valid
    /// declared type renders, so the decline is caused by the widening alone.
    #[test]
    fn broadcast_join_declines_widened_projection() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["selectList"] = serde_json::json!([
            {"type": "function_scalar", "name": "UPPER", "arguments": [
                {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}]},
        ]);
        let pushdown_req = pd(&request);
        assert!(
            !join_requires_exasol_postprocessing(&pushdown_req),
            "the fixture must reach broadcast eligibility, not be skipped upstream"
        );
        let detected = detected_join(&request);

        // Control: an EMITS-valid declared type does not widen, so broadcast renders.
        let mut ok_req = request.clone();
        ok_req["pushdownRequest"]["selectListDataTypes"] =
            serde_json::json!([{"type": "varchar", "size": 100}]);
        let (_, _, ok_widened) =
            extract_join_projection(&ok_req, &pd(&ok_req), &detected).expect("projection derives");
        assert!(!ok_widened, "the control fixture must NOT widen");
        assert!(
            render_broadcast_join(&ok_req, &pd(&ok_req), &detected)
                .expect("the control must not error")
                .is_some(),
            "the control must render a broadcast plan, so only the widening differs"
        );

        // An EMITS-rejected declared type widens the projection to the full base row.
        request["pushdownRequest"]["selectListDataTypes"] =
            serde_json::json!([{"type": "timestamp", "withLocalTimeZone": true}]);
        let pushdown_req = pd(&request);
        let (projection, _, widened) = extract_join_projection(&request, &pushdown_req, &detected)
            .expect("projection derives");
        assert!(
            widened && projection.len() == 4,
            "precondition: the one-item select list must widen to the 4-column \
             two-table base row"
        );

        assert!(
            render_broadcast_join(&request, &pushdown_req, &detected)
                .expect("a widened projection is a clean decline, NOT an error")
                .is_none(),
            "a widened projection must decline broadcast rendering (Ok(None)) so the \
             request falls through to the unified N-scan fallback"
        );
    }

    /// The unified fallback (N = 2): each side scanned through its own sharded
    /// fan-out, joined by an `INNER JOIN … ON` chain (the join condition on the join
    /// point), projecting the qualified select list. The single ORDERS-side-local
    /// filter is pushed into the ORDERS leg, so the outer WHERE has no residual. The
    /// two-table case uses the SAME `LHS_T*` renderer as N ≥ 3.
    #[test]
    fn two_table_join_falls_back_to_unified_n_scan_wrapper() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["filter"] = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
            "right": {"type": "literal_string", "value": "1995-01-01"}
        });
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("the two-table unified fallback must build");

        for alias in ["LHS_T0", "LHS_T1"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "both side fan-outs must appear as aliased derived-table subqueries: {sql}"
            );
        }
        assert!(
            sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the equi-condition must attach table-qualified as the join point's ON clause: {sql}"
        );
        assert!(
            sql.contains(r#"SELECT "LHS_T0"."C_NAME", "LHS_T1"."O_ORDERDATE" FROM"#),
            "the cross-table projection must drive the outer SELECT in order: {sql}"
        );
        // The lone ORDERS-side-local filter is pushed into the ORDERS leg, so no
        // residual conjunct remains and there is no outer WHERE.
        assert!(
            sql.contains("'1995-01-01'"),
            "the ORDERS-side-local filter must be pushed into that leg's fan-out: {sql}"
        );
        assert!(
            !sql.contains(" WHERE "),
            "every side-local filter is pushed into its leg, so no residual outer WHERE: {sql}"
        );
        // The unified fallback is an INNER JOIN chain, never a broadcast join block.
        assert!(sql.contains("INNER JOIN"), "{sql}");
        assert!(
            !sql.contains("\"join\":{"),
            "the fallback must not embed a broadcast join block: {sql}"
        );
    }

    // -----------------------------------------------------------------------
    // Qualified two-scan fallback: rendering independent of the disjoint-column
    // guard, and aggregate-over-join routed through two-scan.
    // -----------------------------------------------------------------------

    /// A join whose two tables share a column name (`ID`) fails the disjoint guard
    /// (so the broadcast path declines), but the unified N-scan fallback still builds
    /// a correct, UNAMBIGUOUS wrapper (N = 2): the condition and projection reference
    /// `"LHS_T0"."ID"` / `"LHS_T1"."ID"`, never a bare ambiguous `"ID"`. This is the
    /// `EVENTS ⋈ LABELS ON a.id = b.id` regression.
    #[test]
    fn colliding_columns_render_qualified_unified_wrapper_without_error() {
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "EVENTS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "SCORE", "dataType": {"type": "double"}}]},
                {"name": "LABELS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "LABEL", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"name": "EVENTS", "type": "table"},
                    "right": {"name": "LABELS", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                        "right": {"type": "column", "name": "ID", "tableName": "LABELS"}}},
                "selectList": [
                    {"type": "column", "name": "ID", "tableName": "EVENTS"},
                    {"type": "column", "name": "LABEL", "tableName": "LABELS"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP": {"EVENTS": "lh.events", "LABELS": "lh.labels"}})
                    .to_string()},
        });

        // Precondition: the shared ID column fails the disjoint guard, so broadcast
        // rendering declines (Ok(None)) — the very reason the OLD code errored.
        let left = involved_table_columns(&request, "EVENTS");
        let right = involved_table_columns(&request, "LABELS");
        assert!(!disjoint_schema_guard(&left, &right));
        let detected = detected_join(&request);
        assert!(
            render_broadcast_join(&request, &pd(&request), &detected)
                .unwrap()
                .is_none()
        );

        let sides = vec![
            resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 100)]),
            resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("the qualified unified fallback must build despite the column-name collision");

        assert!(
            sql.contains(r#"("LHS_T0"."ID" = "LHS_T1"."ID")"#),
            "the equi-condition must be table-qualified, never bare/ambiguous: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T0"."ID""#) && sql.contains(r#""LHS_T1"."LABEL""#),
            "the projection must be table-qualified per owning side: {sql}"
        );
        assert!(sql.contains("INNER JOIN"), "{sql}");
    }

    /// The N-scan (N≥3) builder produces an `INNER JOIN … ON` chain — N distinct
    /// `LHS_T*` fan-out aliases, every one of the N-1 join conditions rendered
    /// table-qualified and greedily attached to its join point, and the select list
    /// qualified to its owning side — never an `Err` for an all-inner tree over
    /// resolvable tables (pushdown-planning-join "A three-or-more-table inner join
    /// falls back to an N-scan unaccelerated wrapper").
    #[test]
    fn build_n_scan_join_sql_produces_qualified_n_scan_wrapper() {
        let request = three_table_join_request();
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
            resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 500)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("an all-inner N-scan wrapper must build, never Err");

        for alias in ["LHS_T0", "LHS_T1", "LHS_T2"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "missing distinct fan-out alias {alias}: {sql}"
            );
        }
        assert!(
            sql.contains(r#""LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY""#),
            "first join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."O_ORDERKEY" = "LHS_T2"."L_ORDERKEY""#),
            "second join condition must be table-qualified: {sql}"
        );
        assert_eq!(
            sql.matches("INNER JOIN").count(),
            2,
            "conditions must attach across a two-hop INNER JOIN … ON chain: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T0"."C_NAME""#) && sql.contains(r#""LHS_T2"."L_QUANTITY""#),
            "the select list must be qualified to each column's owning side: {sql}"
        );
    }

    /// The N-scan builder also handles the Q1 shape (`supplier⋈nation⋈region`): three
    /// distinct `LHS_T*` fan-out aliases and both join conditions rendered
    /// table-qualified, never an `Err`.
    #[test]
    fn build_n_scan_join_sql_for_q1_shape_supplier_nation_region() {
        let request = q1_join_request();
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("SUPPLIER", vec![("s3://w/s-0.parquet", 10)]),
            resolved_side("NATION", vec![("s3://w/n-0.parquet", 5)]),
            resolved_side("REGION", vec![("s3://w/r-0.parquet", 2)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("the Q1-shape (supplier⋈nation⋈region) must build, never Err");

        for alias in ["LHS_T0", "LHS_T1", "LHS_T2"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "missing distinct fan-out alias {alias}: {sql}"
            );
        }
        assert!(
            sql.contains(r#""LHS_T0"."S_NATIONKEY" = "LHS_T1"."N_NATIONKEY""#),
            "first join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."N_REGIONKEY" = "LHS_T2"."R_REGIONKEY""#),
            "second join condition must be table-qualified: {sql}"
        );
    }

    /// The N-scan builder also handles the NQ3 shape
    /// (`part⋈partsupp⋈supplier⋈nation`, N=4): four distinct `LHS_T*` fan-out
    /// aliases and all three join conditions rendered table-qualified, never an
    /// `Err` — the builder generalizes past N=3.
    #[test]
    fn build_n_scan_join_sql_for_nq3_shape_part_partsupp_supplier_nation() {
        let request = nq3_join_request();
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("PART", vec![("s3://w/p-0.parquet", 10)]),
            resolved_side("PARTSUPP", vec![("s3://w/ps-0.parquet", 40)]),
            resolved_side("SUPPLIER", vec![("s3://w/s-0.parquet", 5)]),
            resolved_side("NATION", vec![("s3://w/n-0.parquet", 3)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("the NQ3-shape (part⋈partsupp⋈supplier⋈nation) must build, never Err");

        for alias in ["LHS_T0", "LHS_T1", "LHS_T2", "LHS_T3"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "missing distinct fan-out alias {alias}: {sql}"
            );
        }
        assert!(
            sql.contains(r#""LHS_T0"."P_PARTKEY" = "LHS_T1"."PS_PARTKEY""#),
            "first join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."PS_SUPPKEY" = "LHS_T2"."S_SUPPKEY""#),
            "second join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T2"."S_NATIONKEY" = "LHS_T3"."N_NATIONKEY""#),
            "third join condition must be table-qualified: {sql}"
        );
    }

    /// Three tables that ALL share a column name (`ID`) — the N-table analog of
    /// `colliding_columns_render_qualified_two_scan_without_error` — still build a
    /// correct, unambiguous N-scan wrapper: every `ID` reference (both join
    /// conditions and the select list) is table-qualified, never bare.
    #[test]
    fn build_n_scan_join_sql_renders_qualified_when_three_tables_share_column_name() {
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "EVENTS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "SCORE", "dataType": {"type": "double"}}]},
                {"name": "LABELS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "LABEL", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "TAGS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "TAG_NAME", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"type": "join", "join_type": "inner",
                        "left": {"name": "EVENTS", "type": "table"},
                        "right": {"name": "LABELS", "type": "table"},
                        "condition": {"type": "predicate_equal",
                            "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                            "right": {"type": "column", "name": "ID", "tableName": "LABELS"}}},
                    "right": {"name": "TAGS", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "ID", "tableName": "LABELS"},
                        "right": {"type": "column", "name": "ID", "tableName": "TAGS"}}},
                "selectList": [
                    {"type": "column", "name": "ID", "tableName": "EVENTS"},
                    {"type": "column", "name": "LABEL", "tableName": "LABELS"},
                    {"type": "column", "name": "TAG_NAME", "tableName": "TAGS"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"EVENTS": "lh.events", "LABELS": "lh.labels", "TAGS": "lh.tags"}})
                    .to_string()},
        });
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 100)]),
            resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]),
            resolved_side("TAGS", vec![("s3://w/t-0.parquet", 10)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("three tables sharing an ID column must still build, never Err");

        assert!(
            sql.contains(r#""LHS_T0"."ID" = "LHS_T1"."ID""#),
            "first condition must be table-qualified, never bare/ambiguous: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."ID" = "LHS_T2"."ID""#),
            "second condition must be table-qualified, never bare/ambiguous: {sql}"
        );
        // The outer wrapper's own SELECT list (as opposed to each independently
        // scanned, unambiguous per-side fan-out's inner projection) must qualify
        // every shared `ID` reference — never a bare, cross-side-ambiguous `"ID"`.
        assert!(
            sql.starts_with(r#"SELECT "LHS_T0"."ID", "LHS_T1"."LABEL", "LHS_T2"."TAG_NAME" FROM "#),
            "the outer SELECT list must qualify the shared ID column, never bare: {sql}"
        );
    }

    /// The two-table above-broadcast-threshold fallback renders
    /// its FROM as a left-to-right `INNER JOIN … ON` chain (not a comma cross-join +
    /// flat WHERE). The single equi-condition attaches as the join point's `ON`
    /// clause, table-qualified, at the point that brings the second leg into scope.
    #[test]
    fn above_threshold_join_falls_back_inner_join_on() {
        let request = join_request(Json::Null, equi_condition());
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("the above-threshold two-table fallback must build");

        assert!(
            sql.contains("INNER JOIN"),
            "the fallback FROM must be an INNER JOIN chain, not a comma cross-join: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T0" INNER JOIN"#),
            "the first leg must be the left side of the INNER JOIN chain: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the equi-condition must attach table-qualified as the join point's ON clause: {sql}"
        );
        assert!(
            !sql.contains(r#"AS "LHS_T0", "#),
            "the legacy comma cross-join between legs must be gone: {sql}"
        );
    }

    /// A three-table inner join renders a two-hop
    /// `INNER JOIN … ON` chain, each condition greedily attached at the earliest
    /// join point where all its tables are in scope (by table-name set). No residual
    /// filter → no outer WHERE.
    #[test]
    fn three_table_join_inner_join_on_chain() {
        let request = three_table_join_request();
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
            resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 500)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("the three-table inner-join chain must build");

        assert_eq!(
            sql.matches("INNER JOIN").count(),
            2,
            "N=3 tables → a two-hop INNER JOIN chain: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the first condition attaches at the join point bringing LHS_T1 into scope: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T2" ON (("LHS_T1"."O_ORDERKEY" = "LHS_T2"."L_ORDERKEY"))"#),
            "the second condition attaches at the join point bringing LHS_T2 into scope: {sql}"
        );
        assert!(
            !sql.contains(" WHERE "),
            "every condition lives in an ON clause and there is no residual filter, so no \
             outer WHERE: {sql}"
        );
    }

    /// Greedy-attach by table-name set AND the WHERE split.
    /// A star shape `(N1 ⋈ (N2 ⋈ FACT))` where BOTH conditions reference FACT (the
    /// deepest leaf, `LHS_T2`): both attach at the last join point, so the middle
    /// join point (bringing `LHS_T2`'s sibling `LHS_T1` into scope) has no
    /// newly-resolvable condition and renders `ON 1=1`. A CUSTOMER-side-local WHERE
    /// conjunct is pushed into that leg's fan-out (never re-applied in the outer
    /// WHERE); only the cross-table residual conjunct survives in the outer WHERE.
    #[test]
    fn join_conditions_greedy_attach_and_side_local_pushdown() {
        let cond_n2_fact = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "N2_KEY", "tableName": "N2"},
            "right": {"type": "column", "name": "F_N2KEY", "tableName": "FACT"}});
        let cond_n1_fact = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "N1_KEY", "tableName": "N1"},
            "right": {"type": "column", "name": "F_N1KEY", "tableName": "FACT"}});
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "N1", "columns": [
                    {"name": "N1_KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "N1_NAME", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "N2", "columns": [
                    {"name": "N2_KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
                {"name": "FACT", "columns": [
                    {"name": "F_N1KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "F_N2KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "F_VALUE", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"name": "N1", "type": "table"},
                    "right": {"type": "join", "join_type": "inner",
                        "left": {"name": "N2", "type": "table"},
                        "right": {"name": "FACT", "type": "table"},
                        "condition": cond_n2_fact},
                    "condition": cond_n1_fact},
                "selectList": [
                    {"type": "column", "name": "N1_NAME", "tableName": "N1"},
                    {"type": "column", "name": "F_VALUE", "tableName": "FACT"}],
                "filter": {"type": "predicate_and", "expressions": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "N1_NAME", "tableName": "N1"},
                     "right": {"type": "literal_string", "value": "ACME"}},
                    {"type": "predicate_greater",
                     "left": {"type": "column", "name": "F_VALUE", "tableName": "FACT"},
                     "right": {"type": "column", "name": "N1_KEY", "tableName": "N1"}}]},
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"N1": "lh.n1", "N2": "lh.n2", "FACT": "lh.fact"}})
                    .to_string()},
        });
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("N1", vec![("s3://w/n1-0.parquet", 10)]),
            resolved_side("N2", vec![("s3://w/n2-0.parquet", 10)]),
            resolved_side("FACT", vec![("s3://w/f-0.parquet", 500)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("the star-shape greedy-attach fallback must build");

        // The middle join point brings N2 (LHS_T1) into scope but neither condition is
        // resolvable there (both also need FACT / LHS_T2) → ON 1=1.
        assert!(
            sql.contains(r#"AS "LHS_T1" ON 1=1"#),
            "a join point with no newly-resolvable condition must render ON 1=1: {sql}"
        );
        // Both conditions greedily attach at the last join point (LHS_T2), AND-conjoined.
        assert!(
            sql.contains(r#"AS "LHS_T2" ON (("LHS_T1"."N2_KEY" = "LHS_T2"."F_N2KEY")) AND (("LHS_T0"."N1_KEY" = "LHS_T2"."F_N1KEY"))"#),
            "both FACT-touching conditions must attach greedily at the final join point: {sql}"
        );

        // The N1-side-local conjunct is pushed into N1's fan-out leg…
        assert!(
            sql.contains("'ACME'"),
            "the side-local conjunct must be pushed into its leg's fan-out: {sql}"
        );
        // …and NOT re-applied in the outer WHERE, which keeps only the cross-table residual.
        let where_clause = &sql[sql
            .find(" WHERE ")
            .expect("the cross-table residual must remain in an outer WHERE")..];
        assert!(
            !where_clause.contains("ACME"),
            "the side-local conjunct must NOT be duplicated in the outer WHERE: {sql}"
        );
        assert!(
            where_clause.contains(r#""LHS_T2"."F_VALUE""#)
                && where_clause.contains(r#""LHS_T0"."N1_KEY""#),
            "the cross-table residual conjunct must render qualified in the outer WHERE: {sql}"
        );
    }

    /// An aggregate over a join (`COUNT(*), MIN(o.O_ORDERDATE)`) routes through the
    /// unified N-scan wrapper and lets Exasol evaluate the aggregate over the
    /// materialized join — a two-column result (`COUNT(*)`,
    /// `MIN("LHS_T1"."O_ORDERDATE")`), not the full-row projection the old code
    /// emitted (which produced the "expected 2 columns but pushdown has 5" failure).
    #[test]
    fn aggregate_over_join_renders_exasol_aggregate_over_unified_wrapper() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["selectList"] = serde_json::json!([
            {"type": "function_aggregate", "name": "COUNT", "arguments": []},
            {"type": "function_aggregate", "name": "MIN", "arguments": [
                {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"}]},
        ]);

        assert!(
            join_requires_exasol_postprocessing(&pd(&request)),
            "an aggregate select list must force the Exasol-executed fallback path"
        );

        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("aggregate-over-join must build the unified wrapper");

        assert!(sql.contains("COUNT(*)"), "COUNT(*) must be rendered: {sql}");
        assert!(
            sql.contains(r#"MIN("LHS_T1"."O_ORDERDATE")"#),
            "MIN must qualify its argument to the owning side: {sql}"
        );
        assert!(
            sql.starts_with(r#"SELECT COUNT(*), MIN("LHS_T1"."O_ORDERDATE") FROM"#),
            "the outer SELECT must be exactly the two aggregate columns: {sql}"
        );
        assert!(
            sql.contains("INNER JOIN") && !sql.contains("\"join\":{"),
            "aggregate-over-join is an INNER JOIN chain fallback, never a broadcast block: {sql}"
        );
    }

    /// A three-side `alias_of` map ({CUSTOMER→LHS_T0, ORDERS→LHS_T1,
    /// LINEITEM→LHS_T2}) for the seam-unification tests, matching the `LHS_T*` scheme
    /// [`build_n_scan_alias_map`] produces from resolved sides.
    fn seam_alias_of() -> HashMap<String, String> {
        HashMap::from([
            ("CUSTOMER".to_string(), "LHS_T0".to_string()),
            ("ORDERS".to_string(), "LHS_T1".to_string()),
            ("LINEITEM".to_string(), "LHS_T2".to_string()),
        ])
    }

    /// A select item that is a SCALAR FUNCTION WRAPPING AGGREGATES — e.g.
    /// `ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1 ELSE 0 END) / COUNT(*), 2)`
    /// — renders through `render_selectlist_item_qualified` (NOT `None`, no decline),
    /// with its nested aggregates spliced verbatim and its nested column argument
    /// table-qualified to the owning side. Before the vs-expression aggregate arm +
    /// seam unification this recursed into the translator's catch-all and returned
    /// `None`, declining the whole grouped-join pushdown at every arity.
    #[test]
    fn render_selectlist_item_qualified_renders_scalar_over_aggregate() {
        let alias_of = seam_alias_of();
        let sum_case = serde_json::json!({
            "type": "function_aggregate", "name": "SUM", "distinct": false,
            "arguments": [{
                "type": "function_scalar", "name": "CASE", "arguments": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "L_RETURNFLAG", "tableName": "LINEITEM"},
                     "right": {"type": "literal_string", "value": "R"}},
                    {"type": "literal_exactnumeric", "value": 1},
                    {"type": "literal_exactnumeric", "value": 0}]}]
        });
        let count_star = serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false
        });
        let item = serde_json::json!({
            "type": "function_scalar", "name": "ROUND", "arguments": [
                {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                    {"type": "function_scalar", "name": "MULT", "arguments": [
                        {"type": "literal_double", "value": 100.0},
                        sum_case]},
                    count_star]},
                {"type": "literal_exactnumeric", "value": 2}]
        });

        let sql = render_selectlist_item_qualified(&item, &alias_of)
            .expect("a scalar-over-aggregate item must render, never decline to None");
        assert!(
            sql.contains(r#"SUM(CASE WHEN ("LHS_T2"."L_RETURNFLAG" = 'R') THEN 1 ELSE 0 END)"#),
            "the nested SUM(CASE ...) must render with its column table-qualified: {sql}"
        );
        assert!(
            sql.contains("COUNT(*)"),
            "the nested COUNT(*) must render as the star case: {sql}"
        );
    }

    /// A byte-compatibility guard: a TOP-LEVEL bare aggregate renders through the
    /// unified `render_selectlist_item_qualified` byte-identically to the
    /// former dedicated `render_aggregate_qualified` — a single-arg aggregate as
    /// `NAME("ALIAS"."COL")`, `COUNT(*)` as `COUNT(*)`, and `DISTINCT` preserved. The
    /// exact expected strings are captured here so any future drift at the seam fails.
    #[test]
    fn render_selectlist_item_qualified_top_level_aggregate_byte_compatible() {
        let alias_of = seam_alias_of();

        let sum = serde_json::json!({
            "type": "function_aggregate", "name": "SUM", "distinct": false,
            "arguments": [{"type": "column", "name": "O_TOTALPRICE", "tableName": "ORDERS"}]
        });
        assert_eq!(
            render_selectlist_item_qualified(&sum, &alias_of).as_deref(),
            Some(r#"SUM("LHS_T1"."O_TOTALPRICE")"#)
        );

        let count_star = serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false
        });
        assert_eq!(
            render_selectlist_item_qualified(&count_star, &alias_of).as_deref(),
            Some("COUNT(*)")
        );

        let count_distinct = serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "distinct": true,
            "arguments": [{"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"}]
        });
        assert_eq!(
            render_selectlist_item_qualified(&count_distinct, &alias_of).as_deref(),
            Some(r#"COUNT(DISTINCT "LHS_T0"."C_CUSTKEY")"#)
        );
    }

    /// The exact failing-E2E shape: `COUNT(DISTINCT CAST(col AS CHAR(20)))`
    /// routed to the qualified wrapper must render the CAST target
    /// LENGTH-QUALIFIED (`VARCHAR(20)`), never bare `VARCHAR`. The qualified
    /// wrapper SQL is parsed by Exasol's own engine, whose VARCHAR type REQUIRES
    /// a length — a bare `VARCHAR` is the "unexpected ')', expecting '('" parse
    /// error (`count_distinct_expression_arg_via_wrapper_matches_single_node`).
    /// This guards the join/qualified-wrapper half of the Exasol-dialect CAST
    /// split under plain `cargo test`, without Docker/Exasol.
    #[test]
    fn qualified_count_distinct_cast_char_renders_length_qualified_exasol_varchar() {
        let alias_of = seam_alias_of();
        let item = serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "distinct": true,
            "arguments": [{
                "type": "function_scalar_cast", "name": "CAST",
                "arguments": [{"type": "column", "name": "C_VARCHAR", "tableName": "CUSTOMER"}],
                "dataType": {"type": "CHAR", "size": 20, "characterSet": "ASCII"}
            }]
        });
        let sql = render_selectlist_item_qualified(&item, &alias_of)
            .expect("COUNT(DISTINCT CAST(col AS CHAR(20))) must render for the qualified wrapper");
        assert!(
            sql.contains("VARCHAR(20)"),
            "Exasol-parsed qualified wrapper needs a length-qualified CAST target: {sql}"
        );
        assert!(
            !sql.contains("AS VARCHAR)"),
            "must NOT emit a bare length-less VARCHAR (Exasol rejects it): {sql}"
        );
        assert!(
            sql.contains(r#"COUNT(DISTINCT CAST("LHS_T0"."C_VARCHAR" AS VARCHAR(20)))"#),
            "full qualified COUNT(DISTINCT CAST(...)) shape must match: {sql}"
        );
    }

    /// A bare-column ORDER BY over a join is rendered table-qualified in the unified
    /// wrapper (with explicit direction + NULL placement), so Exasol — which has
    /// delegated the ordering — sorts on the unambiguous, owning-side column.
    #[test]
    fn order_by_over_join_renders_qualified_in_unified_wrapper() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["orderBy"] = serde_json::json!([
            {"expression": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
             "isAscending": true, "nullsLast": false},
        ]);

        assert!(join_requires_exasol_postprocessing(&pd(&request)));

        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("ordered unified wrapper must build");
        assert!(
            sql.contains(r#"ORDER BY "LHS_T1"."O_ORDERDATE" ASC NULLS FIRST"#),
            "ORDER BY must be table-qualified with explicit direction/nulls: {sql}"
        );
    }

    /// An EXPRESSION (non-bare-column) ORDER BY key over a join — e.g.
    /// `UPPER(orders.o_orderstatus)` — is rendered table-qualified in the unified
    /// wrapper, with explicit direction + NULL placement, exactly like a bare
    /// column (#198). Before this, `qualified_join_order_by` only accepted a bare
    /// `column` expression node and declined (hard error) on anything else,
    /// hiding a renderable ORDER BY expression behind a spurious pushdown failure.
    #[test]
    fn order_by_expression_renders_qualified_in_unified_wrapper() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["orderBy"] = serde_json::json!([
            {"expression": {"type": "function_scalar", "name": "UPPER", "arguments": [
                {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"}]},
             "isAscending": false, "nullsLast": true},
        ]);

        assert!(join_requires_exasol_postprocessing(&pd(&request)));

        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("ordered unified wrapper must build for a renderable expression sort key");
        assert!(
            sql.contains(r#"ORDER BY UPPER("LHS_T1"."O_ORDERDATE") DESC NULLS LAST"#),
            "the expression ORDER BY key must be table-qualified with explicit \
             direction/nulls, not declined: {sql}"
        );
    }

    /// `join_requires_exasol_postprocessing` fires for every clause the broadcast
    /// in-UDF join cannot serve, and is false for a plain projection+filter join.
    #[test]
    fn post_processing_predicate_covers_every_forcing_clause() {
        let plain = join_request(Json::Null, equi_condition());
        assert!(!join_requires_exasol_postprocessing(&pd(&plain)));

        let mut limited = join_request(Json::Null, equi_condition());
        limited["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 10});
        assert!(join_requires_exasol_postprocessing(&pd(&limited)));

        let mut grouped = join_request(Json::Null, equi_condition());
        grouped["pushdownRequest"]["groupBy"] =
            serde_json::json!([{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}]);
        assert!(join_requires_exasol_postprocessing(&pd(&grouped)));

        let mut having = join_request(Json::Null, equi_condition());
        having["pushdownRequest"]["having"] =
            serde_json::json!({"type": "literal_bool", "value": true});
        assert!(join_requires_exasol_postprocessing(&pd(&having)));
    }

    // -----------------------------------------------------------------------
    // Golden-SQL characterization gate. Full-string byte-identity freeze of each
    // join code path this refactor's duplication reductions can touch. Re-run
    // after every dedup extraction.
    // -----------------------------------------------------------------------

    #[test]
    fn golden_broadcast_join_sql_unchanged() {
        let fact = resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 1000)]);
        let dimension = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 10)]);
        let sides = JoinSides {
            fact,
            dimension,
            broadcast_eligible: true,
        };
        let rendered = RenderedJoinPushdown {
            condition: r#"("L_ORDERKEY" = "O_ORDERKEY")"#.to_string(),
            filter: Some(r#"("L_QUANTITY" > 5)"#.to_string()),
            projection: vec![
                ProjectionItem::Column("L_ORDERKEY".to_string()),
                ProjectionItem::Column("O_ORDERDATE".to_string()),
            ],
            projection_types: vec!["DECIMAL(20,0)".to_string(), "DATE".to_string()],
        };
        let actual =
            build_broadcast_join_sql(&sides, &rendered, &two_scan_tuning(), "SCAN", "DISTRIBUTE");
        assert_eq!(
            actual,
            r#"SELECT SCAN('{"table_root":"s3://warehouse/lh/lineitem","projection":["L_ORDERKEY","O_ORDERDATE"],"filter":"(\"L_QUANTITY\" > 5)","emit_exa_types":["DECIMAL(20,0)","DATE"],"logical_schema":[{"field_id":1,"name":"LINEITEM_KEY","arrow_type":"int64","nullable":false}],"join":{"table_root":"s3://warehouse/lh/orders","files":[["s3://w/o-0.parquet",10]],"logical_schema":[{"field_id":1,"name":"ORDERS_KEY","arrow_type":"int64","nullable":false}],"join_type":"inner","condition":"(\"L_ORDERKEY\" = \"O_ORDERKEY\")"},"storage":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":0,"s3_max_connections":1}', '[["s3://w/l-0.parquet",1000]]') EMITS ("L_ORDERKEY" DECIMAL(20,0), "O_ORDERDATE" DATE)"#
        );
    }

    #[test]
    fn golden_n_scan_join_sql_unchanged() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["filter"] = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                 "right": {"type": "literal_string", "value": "1995-01-01"}},
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                 "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}},
            ],
        });
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let actual = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("the two-table unified fallback must build");
        assert_eq!(
            actual,
            r#"SELECT "LHS_T0"."C_NAME", "LHS_T1"."O_ORDERDATE" FROM (SELECT SCAN('{"table_root":"s3://warehouse/lh/customer","projection":["C_CUSTKEY","C_NAME"],"filter":"(\"C_NAME\" = ''ACME'')","emit_exa_types":["DECIMAL(20,0)","VARCHAR(100)"],"logical_schema":[{"field_id":1,"name":"CUSTOMER_KEY","arrow_type":"int64","nullable":false}],"storage":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":0,"s3_max_connections":1}', '[["s3://w/c-0.parquet",10]]') EMITS ("C_CUSTKEY" DECIMAL(20,0), "C_NAME" VARCHAR(100))) AS "LHS_T0" INNER JOIN (SELECT SCAN('{"table_root":"s3://warehouse/lh/orders","projection":["O_CUSTKEY","O_ORDERDATE"],"filter":"(\"O_ORDERDATE\" > ''1995-01-01'')","emit_exa_types":["DECIMAL(20,0)","DATE"],"logical_schema":[{"field_id":1,"name":"ORDERS_KEY","arrow_type":"int64","nullable":false}],"storage":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":0,"s3_max_connections":1}', '[["s3://w/o-0.parquet",100]]') EMITS ("O_CUSTKEY" DECIMAL(20,0), "O_ORDERDATE" DATE)) AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY")) WHERE (("LHS_T0"."C_CUSTKEY" > "LHS_T1"."O_CUSTKEY"))"#
        );
    }

    #[test]
    fn golden_grouped_qualified_fallback_sql_unchanged() {
        let request = serde_json::json!({
            "involvedTables": [{"name": "CUSTOMER", "columns": [
                {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}},
            ]}],
        });
        let pushdown_req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}],
            "selectList": [
                {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                agg_item("COUNT", None, false),
            ],
        });
        let fan_out_spec = ScanSpec {
            common: CommonScanSpec {
                projection: vec![
                    ProjectionItem::Column("C_CUSTKEY".to_string()),
                    ProjectionItem::Column("C_NAME".to_string()),
                ],
                emit_exa_types: vec!["DECIMAL(20,0)".to_string(), "VARCHAR(100)".to_string()],
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let actual = build_qualified_single_table_fallback_sql(
            &request,
            &pushdown_req,
            &fan_out_spec,
            &[vec![("s3://w/c-0.parquet".to_string(), 10u64)]],
            "SCAN",
            "DISTRIBUTE",
        )
        .expect("the grouped qualified fallback must build");
        assert_eq!(
            actual,
            r#"SELECT "LHS_T0"."C_NAME", COUNT(*) FROM (SELECT SCAN('{"projection":["C_CUSTKEY","C_NAME"],"emit_exa_types":["DECIMAL(20,0)","VARCHAR(100)"],"storage":{"endpoint":"http://minio:9000","region":"us-east-1","access_key":"minioadmin","secret_key":"minioadmin","allow_http":true,"path_style":true},"df_target_partitions":1,"df_batch_size":8192,"df_threads_per_udf":1,"memory_pool_fraction":0.6,"instance_overhead_mb":200,"s3_max_connections":8}', '[["s3://w/c-0.parquet",10]]') EMITS ("C_CUSTKEY" DECIMAL(20,0), "C_NAME" VARCHAR(100))) AS "LHS_T0" GROUP BY "LHS_T0"."C_NAME""#
        );
    }

    // -----------------------------------------------------------------------
    // Shared referenced-column narrowing (issue #160)
    // -----------------------------------------------------------------------

    /// Issue #160 regression: BOTH decline wrappers — the grouped decline wrapper AND
    /// the single-group Case 2/3 `COUNT(DISTINCT)` wrapper — obtain their inner-scan
    /// projection from the ONE shared `referenced_column_projection` helper, which
    /// narrows to only the columns the wrapper references and NEVER falls back to the
    /// full base-table schema.
    ///
    /// The narrowing walks the FULL expression tree of every clause the wrapper
    /// renders: a column referenced ONLY inside an aggregate argument
    /// (`SUM(CASE WHEN region='R' ...)` surfaces `REGION`), a column referenced ONLY in
    /// HAVING, and a column referenced ONLY in ORDER BY (and the WHERE filter) are all
    /// surfaced, while an unreferenced base-table column is dropped. Then each wrapper
    /// is built from that narrowed projection and asserted to omit the unreferenced
    /// column entirely (its EMITS clause names only referenced columns).
    #[test]
    fn fallback_projection_narrows_to_referenced_columns() {
        fn col(name: &str, ty: &str) -> (String, String) {
            (name.to_string(), ty.to_string())
        }
        fn spec_with(projection: Vec<ProjectionItem>, emit_exa_types: Vec<String>) -> ScanSpec {
            ScanSpec {
                common: CommonScanSpec {
                    projection,
                    emit_exa_types,
                    storage: sample_storage(),
                    ..Default::default()
                },
                files: vec![],
            }
        }
        fn proj_names(proj: &[ProjectionItem]) -> Vec<String> {
            proj.iter()
                .map(|p| match p {
                    ProjectionItem::Column(n) => n.clone(),
                    ProjectionItem::Expr { .. } => panic!("narrowing must yield columns only"),
                })
                .collect()
        }

        let request = serde_json::json!({"involvedTables": [{"name": "T"}]});
        let shards = [vec![("s3://wh/f0.parquet".to_string(), 1u64)]];

        // --- The shared helper narrows across EVERY reference site (pure #160 core). ---
        // GK: group key + bare select; REGION: only inside SUM(CASE ...); HCOL: only in
        // HAVING; OCOL: only in ORDER BY; FCOL: only in the WHERE filter;
        // IRRELEVANT_COL: never referenced.
        let all_cols = vec![
            col("GK", "VARCHAR(10)"),
            col("REGION", "VARCHAR(10)"),
            col("HCOL", "DECIMAL(18,0)"),
            col("OCOL", "DECIMAL(18,0)"),
            col("FCOL", "DECIMAL(18,0)"),
            col("IRRELEVANT_COL", "VARCHAR(10)"),
        ];
        let rich_req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "GK", "tableName": "T"}],
            "selectList": [
                {"type": "column", "name": "GK", "tableName": "T"},
                {"type": "function_aggregate", "name": "SUM", "distinct": false, "arguments": [
                    {"type": "function_scalar", "name": "CASE", "arguments": [
                        {"type": "predicate_equal",
                         "left": {"type": "column", "name": "REGION", "tableName": "T"},
                         "right": {"type": "literal_string", "value": "R"}},
                        {"type": "literal_exactnumeric", "value": 1},
                        {"type": "literal_exactnumeric", "value": 0}]}]}
            ],
            "having": {"type": "predicate_greater",
                "left": {"type": "function_aggregate", "name": "SUM", "distinct": false,
                         "arguments": [{"type": "column", "name": "HCOL", "tableName": "T"}]},
                "right": {"type": "literal_exactnumeric", "value": 10}},
            "orderBy": [{"expression": {"type": "column", "name": "OCOL", "tableName": "T"},
                         "isAscending": true, "nullsLast": false}],
            "filter": {"type": "predicate_equal",
                "left": {"type": "column", "name": "FCOL", "tableName": "T"},
                "right": {"type": "literal_exactnumeric", "value": 5}},
        });
        let (proj, types) = referenced_column_projection(&rich_req, &all_cols);
        let names = proj_names(&proj);
        for expected in ["GK", "REGION", "HCOL", "OCOL", "FCOL"] {
            assert!(
                names.contains(&expected.to_string()),
                "#160: {expected} is referenced (select/aggregate-arg/CASE/HAVING/ORDER BY/\
                 filter) and MUST be surfaced: {names:?}"
            );
        }
        assert!(
            !names.contains(&"IRRELEVANT_COL".to_string()),
            "#160: an unreferenced base-table column must be narrowed out, never the \
             full schema: {names:?}"
        );
        assert_eq!(
            names.len(),
            5,
            "narrowed to EXACTLY the 5 referenced columns, not the full 6-column \
             base-table schema: {names:?}"
        );
        assert_eq!(
            types.len(),
            5,
            "types stay positionally aligned with columns"
        );

        // --- Grouped decline wrapper is BUILT from the narrowed projection. ---
        // REGION is referenced ONLY inside the SUM(CASE ...) aggregate argument.
        let grouped_all = vec![
            col("GK", "VARCHAR(10)"),
            col("REGION", "VARCHAR(10)"),
            col("IRRELEVANT_COL", "VARCHAR(10)"),
        ];
        let grouped_req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "GK", "tableName": "T"}],
            "selectList": [
                {"type": "column", "name": "GK", "tableName": "T"},
                {"type": "function_aggregate", "name": "SUM", "distinct": false, "arguments": [
                    {"type": "function_scalar", "name": "CASE", "arguments": [
                        {"type": "predicate_equal",
                         "left": {"type": "column", "name": "REGION", "tableName": "T"},
                         "right": {"type": "literal_string", "value": "R"}},
                        {"type": "literal_exactnumeric", "value": 1},
                        {"type": "literal_exactnumeric", "value": 0}]}]}
            ],
        });
        let (gproj, gtypes) = referenced_column_projection(&grouped_req, &grouped_all);
        assert_eq!(
            proj_names(&gproj),
            vec!["GK".to_string(), "REGION".to_string()],
            "the grouped inner scan narrows to GK + REGION (nested in SUM(CASE ...)), \
             never IRRELEVANT_COL"
        );
        let gsql = build_qualified_single_table_fallback_sql(
            &request,
            &grouped_req,
            &spec_with(gproj, gtypes),
            &shards,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        )
        .expect("grouped decline wrapper must build");
        assert!(
            !gsql.contains("IRRELEVANT_COL"),
            "#160: the grouped wrapper's inner scan must NOT emit the unreferenced \
             column: {gsql}"
        );
        assert!(
            gsql.contains("REGION")
                && gsql.contains(" GROUP BY ")
                && gsql.contains(r#"AS "LHS_T0""#),
            "the grouped wrapper renders the aggregate over the narrowed aliased scan: {gsql}"
        );

        // --- Single-group Case 2/3 wrapper is BUILT from the same shared helper. ---
        let sg_all = vec![
            col("A", "VARCHAR(10)"),
            col("B", "VARCHAR(10)"),
            col("IRRELEVANT_COL", "VARCHAR(10)"),
        ];
        let sg_req = serde_json::json!({
            "selectList": [
                {"type": "function_aggregate", "name": "COUNT", "distinct": true,
                 "arguments": [{"type": "column", "name": "A", "tableName": "T"}]},
                {"type": "function_aggregate", "name": "COUNT", "distinct": true,
                 "arguments": [{"type": "column", "name": "B", "tableName": "T"}]},
            ],
        });
        let (sproj, stypes) = referenced_column_projection(&sg_req, &sg_all);
        assert_eq!(
            proj_names(&sproj),
            vec!["A".to_string(), "B".to_string()],
            "the single-group Case 2/3 inner scan narrows to A + B, never IRRELEVANT_COL"
        );
        let ssql = build_qualified_single_table_fallback_sql(
            &request,
            &sg_req,
            &spec_with(sproj, stypes),
            &shards,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        )
        .expect("single-group Case 2/3 wrapper must build");
        assert!(
            !ssql.contains("IRRELEVANT_COL"),
            "#160: the single-group Case 2/3 wrapper's inner scan must NOT emit the \
             unreferenced column: {ssql}"
        );
        assert_eq!(
            ssql.matches("COUNT(DISTINCT").count(),
            2,
            "both COUNT(DISTINCT) aggregates spliced verbatim into the wrapper: {ssql}"
        );
        assert!(
            !ssql.contains(" GROUP BY ") && ssql.contains(r#"AS "LHS_T0""#),
            "the single-group wrapper renders NO GROUP BY over the aliased scan: {ssql}"
        );
    }

    // -----------------------------------------------------------------------
    // The one widening trigger the wrapper cannot render: a node type NEITHER
    // dialect knows. Both wrapper entry points reach the same refusal site in
    // `n_scan_join_select_items`, and both hard-error there — Exasol receives an
    // error, never a wrong-shaped result. Pre-existing behaviour, pinned here (#196).
    // -----------------------------------------------------------------------

    /// A select-list node no dialect renders: the DataFusion dialect declines it (so
    /// `project_columns` widens and the broadcast plan cleanly falls through), and
    /// the Exasol dialect declines it too, so the N-scan wrapper's select list cannot
    /// be rendered. That is a `UdfError::User`, NOT a silent full-row fallback.
    #[test]
    fn n_scan_join_untranslatable_select_item_is_hard_error() {
        let unknown = serde_json::json!({"type": "no_such_node_type_in_either_dialect"});
        assert!(
            render_expression_safe(&unknown).is_none()
                && render_expression_exasol_safe(&unknown).is_none(),
            "the fixture node must be untranslatable under BOTH dialects"
        );

        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["selectList"] = serde_json::json!([
            {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
            unknown,
        ]);
        let pushdown_req = pd(&request);
        let detected = detected_join(&request);
        assert!(
            render_broadcast_join(&request, &pushdown_req, &detected)
                .expect("the broadcast decline is never an error")
                .is_none(),
            "the widened projection declines broadcast, so the request reaches the \
             N-scan fallback"
        );

        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let err = build_n_scan_join_sql(
            &request,
            &pushdown_req,
            &detected,
            &sides,
            &two_scan_tuning(),
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        )
        .expect_err("an untranslatable select-list item must be a hard error");
        assert!(
            matches!(&err, UdfError::User(msg) if msg.contains("select-list item could not be rendered")),
            "the refusal must come from the select-list render site: {err}"
        );
    }

    /// The SINGLE-TABLE route to the same refusal site:
    /// `qualified_single_table_fallback_pushdown` →
    /// `build_qualified_single_table_fallback_sql` → `outer_wrapper_clauses` →
    /// `n_scan_join_select_items`. This is the path the widened-projection dispatch
    /// routing feeds most directly, so an untranslatable item must surface an error
    /// there too — never a wrong-shaped result.
    #[test]
    fn qualified_single_table_untranslatable_select_item_is_hard_error() {
        let request = serde_json::json!({
            "involvedTables": [{"name": "T", "columns": [
                {"name": "ID", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]}],
        });
        let pushdown_req = serde_json::json!({
            "selectList": [
                {"type": "column", "name": "ID", "tableName": "T"},
                {"type": "no_such_node_type_in_either_dialect"},
            ],
        });
        let col_types = vec![("ID".to_string(), "DECIMAL(20,0)".to_string())];
        let base = CommonScanSpec {
            storage: sample_storage(),
            ..Default::default()
        };
        let shards = vec![vec![FileEntry::new("s3://w/f-0.parquet", 10)]];

        let err = qualified_single_table_fallback_pushdown(
            &request,
            &pushdown_req,
            &base,
            None,
            &shards,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        )
        .expect_err("an untranslatable select-list item must be a hard error");
        assert!(
            matches!(&err, UdfError::User(msg) if msg.contains("select-list item could not be rendered")),
            "the refusal must come from the shared select-list render site: {err}"
        );
    }

    /// join-fallback scenario (#198, task 1.2), asserted at the FAMILY level: all
    /// four qualified-wrapper entry points (N-scan join, grouped `GroupByWrapper`,
    /// Case 2/3 `COUNT(DISTINCT)`, widened-projection row scan) reach this one
    /// `outer_wrapper_clauses` seam, so asserting here covers every one of them at
    /// once. A renderable EXPRESSION sort key must render table-qualified with its
    /// direction and NULL placement — before #198 `qualified_join_order_by` accepted
    /// only a bare `column` node and turned this into a spurious hard decline.
    ///
    /// The same fixture pins the trailing clause ORDER against regression: the
    /// builder appends GROUP BY → HAVING → ORDER BY → LIMIT, and a `LIMIT` emitted
    /// ahead of the `ORDER BY` would truncate BEFORE the sort — the unit-level
    /// companion to the grouped top-N-groups e2e case's group-set equality.
    #[test]
    fn outer_wrapper_renders_qualified_expression_sort_key() {
        let alias_of: HashMap<String, String> = [("T".to_string(), "LHS_T0".to_string())]
            .into_iter()
            .collect();
        let aliases = vec!["LHS_T0".to_string()];
        let cols_per_side = vec![vec![
            ("ID".to_string(), "DECIMAL(20,0)".to_string()),
            ("NAME".to_string(), "VARCHAR(100)".to_string()),
        ]];
        // Group-key-only select list ordered by an expression the select list never
        // names — issue #198's own shape — plus a LIMIT that must apply after it.
        let pushdown_req = serde_json::json!({
            "selectList": [{"type": "column", "name": "ID", "tableName": "T"}],
            "groupBy": [{"type": "column", "name": "ID", "tableName": "T"}],
            "orderBy": [{
                "type": "order_by_element",
                "expression": {"type": "function_scalar", "name": "UPPER", "arguments": [
                    {"type": "column", "name": "NAME", "tableName": "T"}]},
                "isAscending": false,
                "nullsLast": true
            }],
            "limit": {"numElements": 4}
        });

        let clauses = outer_wrapper_clauses(&pushdown_req, &alias_of, &aliases, &cols_per_side)
            .expect("a renderable expression sort key must render, not decline");

        assert!(
            clauses
                .trailing
                .contains(r#"ORDER BY UPPER("LHS_T0"."NAME") DESC NULLS LAST"#),
            "the expression sort key must be table-qualified with explicit \
             direction/nulls: {}",
            clauses.trailing
        );
        let order_at = clauses
            .trailing
            .find("ORDER BY")
            .expect("the trailing clauses must carry the ORDER BY");
        let limit_at = clauses
            .trailing
            .find("LIMIT")
            .expect("the trailing clauses must carry the LIMIT");
        assert!(
            order_at < limit_at,
            "LIMIT must follow ORDER BY, or it truncates before the sort: {}",
            clauses.trailing
        );
    }
}

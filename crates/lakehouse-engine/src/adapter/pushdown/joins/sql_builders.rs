use crate::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, ProjectionItem, ScanSpec, render_ordered,
};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::{render_df_filter_safe, render_expression_safe};

use super::super::shard_paths::relativize_shards_to_root;
use super::super::support::{
    build_scan_driving_sql, classify_where_filter, collect_all_column_names, extract_limit,
    extract_offset, quote_ident, render_limit_offset, shard_count, strip_table_alias,
};
use super::super::topn::{ParsedSortKey, parse_sort_flags, wrap_declined_order_by};
use super::attribution::{JoinLegs, UnattributableColumn};
use super::planning::{
    DetectedJoin, JoinSides, JoinWindowPlan, ResolvedJoinSide, disjoint_schema_guard,
    involved_table_columns,
};
use super::rendering::{
    conjoin_filters, cross_leg_residual_filter, declined_only, extract_join_projection,
    join_col_types, leg_local_filter, projection_item_select_sql, referenced_clause_values,
    referenced_leg_columns, render_df_filter_qualified, render_expression_qualified,
    renderable_only, type_screened_leg_filter,
};

/// The translator-reuse artifacts for a broadcast inner equi-join, rendered once
/// in the VS planning layer and consumed by the broadcast fan-out SQL builder.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RenderedJoinPushdown {
    /// The rendered DataFusion SQL boolean join condition (→ [`JoinSpec::condition`]).
    pub condition: String,
    /// The rendered cross-table WHERE filter, or `None` when the request carries
    /// none or it renders trivially true. NEVER a declined filter: a decline
    /// forfeits the broadcast plan entirely and falls through to the N-scan
    /// wrapper instead, so this field carries no decline case to self-apply.
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
/// column name (the guard fails), the equi-condition cannot be rendered, the
/// derived projection widened to the full base row (#196), or the WHERE filter
/// declines through the SAME type-rewrite pipeline the single-table WHERE surface
/// runs: [`classify_where_filter`], over `col_types` — the UNION of both sides'
/// column types built by [`join_col_types`], the sole producer of that universe. A
/// broadcast plan has no outer `WHERE` to catch a declined predicate, so it must
/// fall through to the N-scan fallback, whose wrapper self-applies it instead. The
/// caller then falls through to the deterministic N-scan fallback, exactly as for
/// any other join off the broadcast path. `Ok(Some(..))` carries the rendered join
/// condition, the cross-table WHERE filter, and the cross-table projection with its
/// EMITS types. `Err` is reserved for a genuinely malformed request with no column
/// metadata at all (the same contract [`project_columns`] enforces for the
/// single-table path).
///
/// The disjoint-schema guard MUST run BEFORE `col_types` is built and the
/// type-rewrite pass runs over it: a bare column name in the filter resolves
/// against the UNION of both sides' types, and that union names exactly one Exasol
/// type per name only once the guard has proven the two sides share no column
/// name — building the union first, or over a guard that had failed, could pick
/// either side's type for what would then be an ambiguous shared name.
///
/// Rendering is made side-agnostic HERE: the condition, filter, and select list all
/// carry Exasol's native `tableAlias` for an aliased join query, but `build_join_sql`
/// wraps each side in an unaliased derived sub-SELECT, so an alias-qualified
/// reference would not resolve (`No field named "O"."O_ORDERDATE"`). This function
/// therefore strips `tableAlias` before every render call to GUARANTEE bare column
/// names reach the translator — safe only because the guard immediately above has
/// already proven the two sides share no column name, so a bare name is unambiguous.
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

    let bare_condition = strip_table_alias(&join.conditions[0]);
    // Uses `render_expression_safe`, not the filter renderer, so a boolean is
    // returned verbatim rather than suppressed as trivially true.
    let condition = match render_expression_safe(&bare_condition) {
        Some(condition) => condition,
        None => return Ok(None),
    };

    let col_types = join_col_types(request, join);
    let bare_pushdown_req = strip_table_alias(pushdown_req);
    let bare_filter_json = bare_pushdown_req.get("filter").filter(|f| !f.is_null());
    let (filter, declined) = classify_where_filter(bare_filter_json, &col_types);
    if declined.is_some() {
        return Ok(None);
    }

    let (projection, projection_types, widened) =
        extract_join_projection(request, &bare_pushdown_req, join)?;
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

/// Render the N-scan fallback's FROM as a left-to-right `INNER JOIN … ON` chain over
/// `fan_outs` — one aliased leg per fan-out, so the chain never names a leg it does not
/// emit — and return it together with any join conditions that could not be attached to
/// a join point (referencing no leg, or a reference no leg can be chosen for). Those
/// unattachable conditions become outer-WHERE residual conjuncts — for an inner join a
/// condition in the WHERE is result-equivalent to the same condition in an `ON` clause,
/// so this is a safe last-resort backstop.
///
/// `conditions[i]` is the pre-rendered, table-qualified SQL for `raw_conditions[i]`.
/// Each condition GREEDILY attaches to the earliest join point where every leg it
/// touches is in scope, which is the one [`JoinLegs::attachment_leg`] names — never
/// decided here, and never by table name or column name, so neither two legs sharing a
/// column name nor two legs of ONE table can fool the attachment. A join point with no
/// attached condition renders `ON 1=1`.
fn build_n_scan_join_from(
    fan_outs: &[String],
    legs: &JoinLegs,
    raw_conditions: &[Json],
    conditions: &[String],
) -> (String, Vec<String>) {
    // Every bound comes from `fan_outs`, the slice the chain indexes, so no second
    // count can drift out of step with it.
    let last_join_point = fan_outs.len().saturating_sub(1);

    let mut on_at: Vec<Vec<String>> = vec![Vec::new(); fan_outs.len()];
    let mut residual: Vec<String> = Vec::new();
    for (raw, rendered) in raw_conditions.iter().zip(conditions) {
        // Clamp to a real join point (≥ 1, ≤ last). The `last_join_point >= 1` guard
        // comes first: with a single leg there is no join point to attach to (and
        // `clamp(1, 0)` would panic), so such a condition falls through to residual.
        match legs.attachment_leg(raw) {
            Some(m) if last_join_point >= 1 => {
                on_at[m.clamp(1, last_join_point)].push(rendered.clone())
            }
            _ => residual.push(rendered.clone()),
        }
    }

    let mut from = format!("({}) AS {}", fan_outs[0], quote_ident(&legs.leg_alias(0)));
    for (k, fan_out) in fan_outs.iter().enumerate().skip(1) {
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
            " INNER JOIN ({fan_out}) AS {} ON {on}",
            quote_ident(&legs.leg_alias(k))
        ));
    }
    (from, residual)
}

/// Every column of all legs as a leg-qualified projection item, in leg order.
/// `cols_per_leg[i]` belongs to the leg aliased [`JoinLegs::leg_alias`]`(i)`.
fn n_full_row_qualified_items(
    legs: &JoinLegs,
    cols_per_leg: &[Vec<(String, String)>],
) -> Vec<ProjectionItem> {
    cols_per_leg
        .iter()
        .enumerate()
        .flat_map(|(leg, cols)| {
            let alias = quote_ident(&legs.leg_alias(leg));
            cols.iter().map(move |(name, _)| ProjectionItem::Expr {
                expr: format!("{alias}.{}", quote_ident(name)),
            })
        })
        .collect()
}

/// Shard one join side's files into G byte-balanced work units and root-relativize
/// them for [`build_scan_driving_sql`]: `shard_count` → `partition_files_by_bytes` →
/// `relativize_shards_to_root`. The shared prefix of [`build_side_fan_out_sql`]
/// (over its own side) and [`build_broadcast_join_sql`] (over the fact side).
///
/// Takes `&ResolvedJoinSide` rather than separate `files`/`table_root` arguments:
/// both call sites already hold one, so the tighter signature cannot be called
/// with a mismatched files/root pair.
fn shard_side(side: &ResolvedJoinSide, tuning: &JoinScanTuning) -> Vec<Vec<FileEntry>> {
    let g = shard_count(
        tuning.cluster_nodes,
        tuning.parallelism_factor,
        side.files.len(),
    );
    let shards = crate::adapter::sharding::partition_files_by_bytes(side.files.clone(), g);
    relativize_shards_to_root(shards, &side.table_root)
}

/// The shared `User` decline template for the seven qualified N-scan decline sites —
/// a select-list item, an involved table's missing column metadata, a leg count that
/// disagrees with the resolved sides, a join condition, a GROUP BY key, HAVING, or an
/// ORDER BY key that cannot be rendered.
/// Each caller passes only its own clause fragment; the surrounding sentence (hard
/// error, no native re-plan) is the one decision this constructor owns.
///
/// Not merged with [`super::ineligible_join_decline`]: that one covers a single,
/// separate case — a join `from` shape the adapter cannot render into ANY SQL at
/// all (wrong join type or malformed tree) — and its message inserts an extra
/// clause (`the adapter cannot render this join shape, `) before the shared tail,
/// so it is a different sentence, not an eighth instance of this one.
fn join_render_decline(clause: &str) -> UdfError {
    UdfError::User(format!(
        "join pushdown declined: {clause}; this is a hard error, not a native re-plan"
    ))
}

/// The same `User` decline for a `column` reference [`JoinLegs`] cannot place on a
/// leg — its `tableName` names two or more legs and its `tableAlias` matches none of
/// them. Reached from every qualified render site, because any of them may be the
/// first to walk the offending reference; picking a leg arbitrarily instead would
/// return silently wrong rows.
fn unattributable_decline(column: UnattributableColumn) -> UdfError {
    join_render_decline(&format!(
        "{column} could not be attributed to a join leg, so no correct qualified \
         reference exists"
    ))
}

/// `Ok(Some(sql))` rendered, `Ok(None)` trivially true, `Err` when neither dialect
/// renders it (see `_decision/045`) or a reference cannot be placed on a leg.
fn render_self_applied_where(
    tree: &Json,
    legs: &JoinLegs,
    subject: &str,
) -> Result<Option<String>, UdfError> {
    if let Some(sql) = render_df_filter_qualified(tree, legs).map_err(unattributable_decline)? {
        return Ok(Some(sql));
    }
    if render_expression_qualified(tree, legs)
        .map_err(unattributable_decline)?
        .is_some()
    {
        return Ok(None);
    }
    let tree_json = serde_json::to_string(tree).unwrap_or_default();
    Err(join_render_decline(&format!(
        "{subject} could be rendered by neither dialect, so it could be applied nowhere: {tree_json}"
    )))
}

/// The N-scan wrapper's outer SELECT list, leg-qualified. An absent/empty select
/// list projects every column of every leg in leg order. An item that cannot be
/// rendered — or a reference no leg can claim — is a last-resort hard error (no
/// native re-plan).
fn n_scan_join_select_items(
    pushdown_req: &Json,
    legs: &JoinLegs,
    cols_per_leg: &[Vec<(String, String)>],
) -> Result<Vec<ProjectionItem>, UdfError> {
    match pushdown_req.get("selectList") {
        Some(Json::Array(list)) if !list.is_empty() => {
            let mut items = Vec::with_capacity(list.len());
            for item in list {
                let sql = render_expression_qualified(item, legs)
                    .map_err(unattributable_decline)?
                    .ok_or_else(|| {
                        join_render_decline(
                            "a select-list item could not be rendered for the qualified N-scan join",
                        )
                    })?;
                items.push(ProjectionItem::Expr { expr: sql });
            }
            Ok(items)
        }
        _ => Ok(n_full_row_qualified_items(legs, cols_per_leg)),
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
///
/// The window (`LIMIT n [OFFSET m]`) is rendered LAST, through the shared
/// [`render_limit_offset`] seam, so it applies after the sort rather than before it.
struct OuterWrapperClauses {
    select: String,
    trailing: String,
}

fn outer_wrapper_clauses(
    pushdown_req: &Json,
    legs: &JoinLegs,
    cols_per_leg: &[Vec<(String, String)>],
) -> Result<OuterWrapperClauses, UdfError> {
    let select_items = n_scan_join_select_items(pushdown_req, legs, cols_per_leg)?;
    let group_by = qualified_join_group_by(pushdown_req, legs)?;
    let having = qualified_join_having(pushdown_req, legs)?;
    let order_by = qualified_join_order_by(pushdown_req, legs)?;
    let limit = extract_limit(pushdown_req);
    let offset = extract_offset(pushdown_req);
    // Exasol withholds `limit` ENTIRELY when it cannot delegate an ordering, so an
    // offset never arrives without a non-empty `orderBy` (#191, verified live). Pinned,
    // not enforced: a decline here would be a hard client-facing failure on all four
    // wrapper entry points, guarding a state no request can reach.
    debug_assert!(
        offset == 0 || order_by.is_some(),
        "fact 5: Exasol withholds `limit` entirely when it cannot delegate an ordering, \
         so a non-zero offset must never arrive without a non-empty orderBy"
    );

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
    trailing.push_str(&render_limit_offset(limit, offset));

    Ok(OuterWrapperClauses { select, trailing })
}

/// Build the N-scan (N ≥ 2) unaccelerated inner-join SQL — the SOLE unaccelerated
/// fallback renderer (the two-involved-table case is simply N = 2). Each involved
/// table is scanned through its own sharded fan-out and reconstructed into the
/// original inner join by Exasol's core engine via a left-to-right `INNER JOIN … ON`
/// chain.
///
/// Every attribution decision — which leg a select item, condition, conjunct, or
/// narrowed column belongs to — is delegated to [`JoinLegs`], the one owner of leg
/// identity, built here from the FROM-tree leaves. A LEG is one OCCURRENCE of a
/// table, so a self-join's two occurrences stay two legs instead of collapsing onto
/// the first (issue #361).
///
/// Each leg emits its full column set (narrowed to the columns the wrapper actually
/// references across all clauses), so the outer wrapper's SELECT, every join
/// condition, WHERE, aggregate, GROUP BY, HAVING, and ORDER BY can reference any
/// column the join needs — all rendered LEG-QUALIFIED (`"LHS_T{i}"."COL"`), so the
/// wrapper is correct whether or not any two legs share a column name or a table.
///
/// The FROM is a left-to-right `INNER JOIN … ON` chain: each join
/// condition greedily attaches to the earliest join point where every leg it
/// touches is in scope, resolved by the SET of LEGS the condition references
/// (never by table or column name, so neither can misroute scope); a join
/// point with no newly-resolvable condition renders `ON 1=1`. Each leg's leg-local
/// WHERE conjuncts are pushed into that leg's fan-out leg, but only those that pass
/// BOTH screens: the syntactic [`renderable_only`] one, and then
/// [`type_screened_leg_filter`] against THAT SIDE's own column types — which also
/// REWRITES what it accepts (a `DATE` `LIKE` subject becomes `CAST(… AS VARCHAR)`), so
/// a leg receives the rewritten tree the DataFusion scan can actually coerce.
/// Cross-table / OR-spanning / untagged residual conjuncts, every DataFusion-DECLINED
/// conjunct, every conjunct the per-side type screen hands back, and any untaggable
/// join condition remain in the outer WHERE, each parenthesized so a top-level `OR`
/// cannot bind across the ANDs. Nothing is ever omitted — a predicate no leg can apply
/// is the wrapper's own to render (`pushdown`'s module header). For an inner join this
/// is result-equivalent to single-node evaluation, independent of join order and of
/// shared column names.
///
/// The per-side fan-out loop therefore runs BEFORE the residual is assembled: the type
/// screen is per side and post-attribution, so which conjuncts the residual must carry
/// is not known until every side has been screened. Assembling the residual first and
/// subtracting afterwards would leave a window in which a conjunct belongs to neither
/// half.
///
/// Returns an `Err` (a hard client-facing error, no native re-plan) only when the
/// wrapper genuinely cannot be built: an involved table carries no column metadata,
/// a join condition (or a pushed select/GROUP BY/HAVING/ORDER BY element) cannot be
/// rendered at all, or the residual WHERE set is renderable by NEITHER dialect — a
/// predicate applicable nowhere must fail the query, not silently return unfiltered
/// rows.
#[allow(clippy::too_many_arguments)]
pub(in super::super) fn build_n_scan_join_sql(
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
        return Err(join_render_decline(
            "an involved table carries no column metadata, so the unaccelerated N-scan \
             fallback cannot be built",
        ));
    }

    // ONE leg per FROM-tree leaf, in the same order `sides` was resolved in — the sole
    // owner of which leg a reference belongs to, so two occurrences of one table stay
    // two legs instead of collapsing onto the first.
    let legs = join.legs();
    // A real guard, not a debug assertion: a leg index indexes `sides` and `fan_outs`
    // too, so a drift between the two counts would be an out-of-bounds panic inside a
    // release UDF rather than a client-visible decline.
    if legs.leg_count() != sides.len() {
        return Err(join_render_decline(&format!(
            "leg count ({}) and resolved-side count ({}) disagree, so a leg index cannot \
             index the resolved sides",
            legs.leg_count(),
            sides.len()
        )));
    }

    // Every join-tree condition, leg-qualified. A condition is the one clause with
    // no lower fallback: if it cannot be rendered even qualified, no correct join SQL
    // exists → last-resort hard error (no native re-plan).
    let mut conditions = Vec::with_capacity(join.conditions.len());
    for cond in &join.conditions {
        let rendered = render_expression_qualified(cond, &legs)
            .map_err(unattributable_decline)?
            .ok_or_else(|| {
                join_render_decline(
                    "a join condition could not be rendered against the qualified N-scan schema",
                )
            })?;
        conditions.push(rendered);
    }

    let where_filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    let leg_eligible = where_filter.and_then(renderable_only);

    // Per-side fan-out, and it MUST run before the residual is assembled: the per-side
    // TYPE screen can hand a conjunct BACK to the residual, so the residual set is not
    // yet known here. Each leg's projection is narrowed to the columns the wrapper
    // references (across the SELECT list, ALL join conditions, WHERE, GROUP BY,
    // HAVING, and ORDER BY), and each side's side-local WHERE conjuncts are pushed
    // down as a DataFusion filter through TWO screens: the syntactic one already
    // applied to `leg_eligible`, then `type_screened_leg_filter` against THAT SIDE's
    // own column types — so neither a conjunct DataFusion cannot render nor one it
    // would refuse to coerce reaches a leg, and the leg's own render cannot decline.
    // All N-1 conditions are passed as one JSON array so `referenced_leg_columns`
    // (which walks arbitrary nodes) keeps a side's column referenced by ANY condition.
    let all_conditions = Json::Array(join.conditions.clone());
    let mut fan_outs = Vec::with_capacity(sides.len());
    let mut type_declined: Option<Json> = None;
    for (i, side) in sides.iter().enumerate() {
        let narrowed =
            referenced_leg_columns(pushdown_req, &all_conditions, &legs, i, &cols_per_side[i]);
        let (side_filter, side_declined) = match leg_eligible
            .as_ref()
            .and_then(|f| leg_local_filter(f, &legs, i))
        {
            Some(side_local) => type_screened_leg_filter(&side_local, &cols_per_side[i]),
            None => (None, None),
        };
        // Disjoint by attribution: each leg's leg-local slice is its own, so the
        // accumulated set can never double-apply a conjunct.
        type_declined = conjoin_filters(type_declined, side_declined);
        fan_outs.push(build_side_fan_out_sql(
            side,
            &narrowed,
            side_filter.as_ref(),
            tuning,
            udf_name,
            distribute_udf_name,
        ));
    }

    // The residual is the AND of three DISJOINT sets, and together with the per-side
    // leg filters above they partition the request's filter exactly: the renderable
    // conjuncts no single side owns, the syntactically-declined ones, and the ones the
    // per-side type screen just handed back.
    let residual = conjoin_filters(
        conjoin_filters(
            leg_eligible
                .as_ref()
                .and_then(|f| cross_leg_residual_filter(f, &legs)),
            where_filter.and_then(declined_only),
        ),
        type_declined,
    );
    let filter = match &residual {
        None => None,
        Some(tree) => render_self_applied_where(tree, &legs, "a residual WHERE conjunct")?,
    };

    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown_req, &legs, &cols_per_side)?;

    // Assemble the INNER JOIN … ON chain. FROM is the chain of
    // aliased fan-out legs with each condition greedily attached by leg set;
    // the outer WHERE carries the residual filter plus any unattachable join condition.
    let (from, residual_conditions) =
        build_n_scan_join_from(&fan_outs, &legs, &join.conditions, &conditions);

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
pub(in super::super) struct JoinScanTuning {
    pub(in super::super) cluster_nodes: usize,
    pub(in super::super) parallelism_factor: usize,
    pub(in super::super) df_target_partitions: usize,
    pub(in super::super) df_batch_size: usize,
    pub(in super::super) df_threads_per_udf: usize,
    pub(in super::super) memory_pool_fraction: f64,
    pub(in super::super) instance_overhead_mb: u64,
    pub(in super::super) s3_max_connections: usize,
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
/// is the side the spec scans, and `common.storage` carries ONLY that scanned
/// side's own effective `storage` (`table_root`, `logical_schema`, `name_mapping`
/// likewise come from `primary`); `projection`, `filter`, `emit_exa_types`, and
/// `join` are the only per-path differences (the N-scan leg passes `join: None`;
/// the broadcast path passes the dimension-side join block, which carries the
/// dimension's own effective storage in `join.storage` rather than riding in
/// `primary`'s).
///
/// `common.limit` is set to `None` here UNCONDITIONALLY — this helper never puts a
/// row cap on that field. A post-join cap instead rides inside the `join` block the
/// caller hands in, as [`JoinSpec::post_join_limit`].
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
            partition_columns: primary.partition_columns.clone(),
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
/// query (see [`build_n_scan_join_sql`]) applies the projection, the conditions, and
/// exactly the RESIDUAL `WHERE` set — the conjuncts no leg applies (cross-table,
/// OR-spanning, untagged, column-free, or DataFusion-declined) — so `columns` (the
/// side's narrowed `(UPPERCASE name, Exasol type)` list, see
/// [`referenced_leg_columns`]) must expose every column any outer clause
/// references. `side_filter` (see [`leg_local_filter`]) arrives both PRE-SCREENED and
/// PRE-REWRITTEN: syntactically renderable per [`renderable_only`], and then accepted
/// AND type-rewritten for this side's own column types by
/// [`type_screened_leg_filter`] — so this leg's own `render_df_filter_safe` cannot
/// decline it away, and it carries no expression the DataFusion scan would refuse to
/// coerce at execution time. Applying the rewrites HERE instead would be wrong: this
/// function cannot tell which conjuncts a decline should send to the outer wrapper, and
/// a decline it swallowed would be applied nowhere. It is rendered bare-name so
/// DataFusion row-group-prunes and row-filters this leg before emitting, rather
/// than shipping every row for Exasol to filter.
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

    let shards = shard_side(side, tuning);

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
        &[],
        None,
        udf_name,
        distribute_udf_name,
    )
}

fn binds_to_projection(key: &ParsedSortKey, projection: &[ProjectionItem]) -> bool {
    let ParsedSortKey::Column(key) = key else {
        return false;
    };
    projection
        .iter()
        .any(|item| matches!(item, ProjectionItem::Column(name) if *name == key.column))
}

/// Build the broadcast fan-out scan-driving SQL, or `None` when the request's
/// window leaves the broadcast contract and the caller must fall through to the
/// N-scan wrapper — the same clean fall-through [`render_broadcast_join`]'s
/// `Ok(None)` already uses, never an error.
///
/// The fact (larger) side is sharded into G byte-balanced work units exactly as the
/// single-table path does; the dimension (smaller) side's FULL file list, table
/// root, logical schema, join type, and rendered condition ride ONCE in the
/// shard-invariant common blob's join block ([`JoinSpec`]). Every shard invocation
/// therefore re-scans the same dimension side and joins it against its fact-file
/// subset node-locally, with no cross-shard exchange. Reuses [`build_scan_driving_sql`]
/// unchanged — the join block travels transparently inside the common blob.
///
/// Each side carries its own effective `StorageBackend`: the fact side's rides in
/// `common.storage` (as on every other scan path); the dimension side's rides in
/// `join.storage`, set below from `dimension.effective_storage`. A vended
/// credential is scoped to the table it was resolved for, so the two sides' file
/// lists must never be read through one shared storage value.
///
/// `window` decides where the request's row window lands, and it lands only ever
/// AFTER the node-local join — never on a side's scanned input, for the reason
/// stated once in [`JoinSpec::post_join_limit`]. An unordered cap composes per
/// shard, so it rides in the join block AND on the outer merge; an ordered window
/// is global, so it rides on an outer wrapper with every shard left unbounded.
///
/// Each side's `partition_columns` ride in that side's own spec block: the fact
/// side's in the common blob, the dimension side's in this [`JoinSpec`].
pub(in super::super) fn build_broadcast_join_sql(
    sides: &JoinSides,
    rendered: &RenderedJoinPushdown,
    window: JoinWindowPlan,
    tuning: &JoinScanTuning,
    udf_name: &str,
    distribute_udf_name: &str,
) -> Option<String> {
    let (shard_cap, ordering) = match window {
        JoinWindowPlan::Unbounded => (None, None),
        JoinWindowPlan::BareLimit(n) => (Some(n), None),
        JoinWindowPlan::Ordered {
            keys,
            limit,
            offset,
        } => {
            // The projection-membership downgrade the classifier structurally cannot
            // make: no projection exists yet at classification time, and the
            // wrapper's ORDER BY binds against the fan-out's EMITTED columns — this
            // path appends no hidden ones, so an unprojected key has nothing to bind
            // to.
            if !keys
                .iter()
                .all(|key| binds_to_projection(key, &rendered.projection))
            {
                return None;
            }
            (None, Some((keys, limit, offset)))
        }
        JoinWindowPlan::ExasolPostProcessed => return None,
    };

    let fact = &sides.fact;
    let dimension = &sides.dimension;

    let shards = shard_side(fact, tuning);

    let join = JoinSpec {
        table_root: dimension.table_root.clone(),
        files: relativize_files_to_root(dimension.files.clone(), &dimension.table_root),
        logical_schema: dimension.logical_schema.clone(),
        name_mapping: dimension.name_mapping.clone(),
        join_type: JoinType::Inner,
        condition: rendered.condition.clone(),
        post_join_limit: shard_cap,
        partition_columns: dimension.partition_columns.clone(),
        storage: dimension.effective_storage.clone(),
    };

    let spec = join_fan_out_scan_spec(
        fact,
        rendered.projection.clone(),
        rendered.filter.clone(),
        rendered.projection_types.clone(),
        Some(join),
        tuning,
    );

    let fan_out = build_scan_driving_sql(
        &spec,
        &shards,
        &rendered.projection,
        &rendered.projection_types,
        shard_cap,
        &[],
        None,
        udf_name,
        distribute_udf_name,
    );

    let Some((keys, limit, offset)) = ordering else {
        return Some(fan_out);
    };
    let wrapped = wrap_declined_order_by(
        &fan_out,
        &rendered.projection,
        rendered.projection.len(),
        &keys,
        limit,
        offset,
    );
    // The wrapper returns its input UNCHANGED when no key rendered an ordering. No
    // upstream `ensure_every_sort_key_renders` supplies that precondition here, and
    // emitting the bare fan-out would answer an advertised ORDER_BY_COLUMN with
    // silently unordered rows — so fail loudly, and fall back rather than answer
    // wrongly in release.
    debug_assert_ne!(
        wrapped, fan_out,
        "an Ordered window must render an ORDER BY"
    );
    (wrapped != fan_out).then_some(wrapped)
}

/// The N-scan wrapper's `GROUP BY` clause (without the keyword), table-qualified.
/// `None` when the request carries no non-empty `groupBy`. A group key that cannot
/// be rendered is a last-resort hard error (no native re-plan).
fn qualified_join_group_by(
    pushdown_req: &Json,
    legs: &JoinLegs,
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
        parts.push(
            render_expression_qualified(key, legs)
                .map_err(unattributable_decline)?
                .ok_or_else(|| {
                    join_render_decline(
                        "a GROUP BY key could not be rendered for the qualified N-scan join",
                    )
                })?,
        );
    }
    Ok(Some(parts.join(", ")))
}

/// The N-scan wrapper's `HAVING` clause (without the keyword), table-qualified.
/// `None` when the request carries no `having`. An unrenderable HAVING is a
/// last-resort hard error (dropping it would return wrong rows; no native re-plan).
fn qualified_join_having(pushdown_req: &Json, legs: &JoinLegs) -> Result<Option<String>, UdfError> {
    match pushdown_req.get("having").filter(|h| !h.is_null()) {
        Some(having) => Ok(Some(
            render_expression_qualified(having, legs)
                .map_err(unattributable_decline)?
                .ok_or_else(|| {
                    join_render_decline(
                        "HAVING could not be rendered for the qualified N-scan join",
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
    legs: &JoinLegs,
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
        join_render_decline("an ORDER BY key could not be rendered for the qualified N-scan join")
    };
    let mut parts = Vec::with_capacity(elements.len());
    for element in elements {
        let (ascending, nulls_last) = parse_sort_flags(element).ok_or_else(decline)?;
        let expr = element.get("expression").ok_or_else(decline)?;
        let rendered = render_expression_qualified(expr, legs)
            .map_err(unattributable_decline)?
            .ok_or_else(decline)?;
        parts.push(render_ordered(&rendered, ascending, nulls_last));
    }
    Ok(Some(parts.join(", ")))
}

/// The subset of `all_cols` the qualified single-table wrapper actually references,
/// as positionally-aligned `(ProjectionItem::Column, Exasol type)` lists — the shared
/// inner-scan projection for BOTH decline wrappers (grouped and single-group Case
/// 2/3), replacing the old whole-table `full_row_projection` (issue #160).
///
/// A request carrying NO select list is the one shape that must NOT narrow: it is a
/// genuine `SELECT *`, the wrapper's own select-list renderer enumerates every
/// projected column ([`n_scan_join_select_items`]'s fallback arm), and Exasol
/// validates that row positionally against the FULL base row — so a narrowed
/// projection would emit a short row it rejects with `04000` "Expected number of
/// columns". Both arms therefore share ONE test, and it is deliberately permissive
/// (Postel's law): the live wire form is an ABSENT `selectList` key — captured from
/// the Docker container via `EXPLAIN VIRTUAL`, with `selectListDataTypes` still
/// carrying the full row beside it — while the protocol documents the same intent as
/// an EMPTY select list, so absent, JSON `null`, `[]`, and a non-array are all
/// accepted as "no select list" and a future Exasol that switches wire form needs no
/// change here.
///
/// Walks the FULL expression tree of every clause the wrapper renders — the clause set
/// [`referenced_clause_values`] owns — collecting through [`collect_all_column_names`]'s
/// Unicode fold, so every column the rendered SQL names is projected and none is
/// missing at runtime. Column order and Exasol types are preserved from `all_cols`.
/// Always returns at least one column (an empty EMITS clause is invalid in Exasol):
/// when the request references no source column it falls back to the first column of
/// `all_cols`, unlike [`referenced_leg_columns`], whose empty-narrowing fallback is
/// its whole column set.
pub(in super::super) fn referenced_column_projection(
    pushdown_req: &Json,
    all_cols: &[(String, String)],
) -> (Vec<ProjectionItem>, Vec<String>) {
    // No select list ⇒ `SELECT *` ⇒ the full base row, never a narrowing (see doc).
    // Accepts every "no select list" wire form Exasol might use, not only the absent
    // key it sends today.
    if !matches!(pushdown_req.get("selectList"), Some(Json::Array(list)) if !list.is_empty()) {
        return (
            all_cols
                .iter()
                .map(|(name, _)| ProjectionItem::Column(name.clone()))
                .collect(),
            all_cols.iter().map(|(_, ty)| ty.clone()).collect(),
        );
    }

    let mut names = std::collections::HashSet::new();
    referenced_clause_values(pushdown_req, |v| collect_all_column_names(v, &mut names));

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
/// group keys, HAVING, ORDER BY, and LIMIT live only in the outer wrapper.
///
/// The WHERE filter normally travels INSIDE the scan (via `fan_out_spec.filter`),
/// mirroring the grouped push-down path. `declined_filter` is the exception, and the
/// reason this wrapper is also the single-table decline route: a predicate the
/// DataFusion dialect cannot render is passed here as its ORIGINAL tree and rendered
/// as the wrapper's own `WHERE`, in Exasol dialect, table-qualified against the
/// `LHS_T0` alias. Its position — after the raw fan-out, before `trailing` — is what
/// makes one route correct for all five request shapes: the fan-out is aggregate-,
/// sort- and LIMIT-free by construction, so the predicate restricts the rows the
/// GROUP BY, HAVING, ORDER BY, and LIMIT consume rather than their output. Callers
/// MUST leave `fan_out_spec.filter` at `None` whenever they pass a `declined_filter`,
/// so the predicate is applied exactly once. Deciding WHICH predicates are declined
/// belongs to the caller (`build_dispatch_sql`), never to this builder.
///
/// The result column count and per-column types match Exasol's positional
/// `selectListDataTypes` validation, so this never emits the `04000`-triggering bare
/// row scan.
pub(in super::super) fn build_qualified_single_table_fallback_sql<E: Clone + Into<FileEntry>>(
    request: &Json,
    pushdown_req: &Json,
    fan_out_spec: &ScanSpec,
    shards: &[Vec<E>],
    udf_name: &str,
    distribute_udf_name: &str,
    declined_filter: Option<&Json>,
) -> Result<String, UdfError> {
    // ONE leg, onto which every involved table name collapses, so a column node's
    // `tableName` (or a stale request `tableAlias`) resolves to `"LHS_T0"` and a name
    // no involved table declares stays unqualified.
    let legs = JoinLegs::for_single_scan(request);
    let alias = legs.leg_alias(0);

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
    let cols_per_leg = vec![all_cols];

    let OuterWrapperClauses { select, trailing } =
        outer_wrapper_clauses(pushdown_req, &legs, &cols_per_leg)?;

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
        &[],
        None,
        udf_name,
        distribute_udf_name,
    );

    let where_clause = match declined_filter {
        None => None,
        Some(tree) => render_self_applied_where(tree, &legs, "a declined WHERE predicate")?,
    };

    let mut sql = format!(
        "SELECT {select} FROM ({fan_out}) AS {}",
        quote_ident(&alias)
    );
    if let Some(clause) = &where_clause {
        sql.push_str(&format!(" WHERE {clause}"));
    }
    sql.push_str(&trailing);
    Ok(sql)
}

/// Dispatch a request to the qualified single-table fallback wrapper, from the
/// shared shard-invariant `base` `build_dispatch_sql` builds once.
///
/// Every `build_dispatch_sql` decline guard — the group-by-not-decomposed guard, the
/// multi/mixed `COUNT(DISTINCT)` guard, the widened-projection guard, and the
/// declined-WHERE-filter guard — reaches this same shape: derive the inner-scan
/// projection, build the fan-out spec from `base` with only the projection/filter/
/// emit-types set (every other field, including LIMIT/ORDER BY/aggregates/group
/// keys/distinct, stays at `base`'s neutral placeholder — the fan-out is always
/// LIMIT-free and sort-free here, see [`build_qualified_single_table_fallback_sql`]'s
/// doc), render the wrapper SQL, and wrap it in the pushdown response envelope.
///
/// `declined_filter` is the predicate the wrapper must self-apply as its own `WHERE`
/// (see [`build_qualified_single_table_fallback_sql`]); `filter` MUST be `None`
/// alongside it so the predicate is applied exactly once. It does NOT decide the
/// projection. The decline route does reach the one shape that must project the FULL
/// base row — a genuine `SELECT *`, whose request carries no select list — but the
/// reason is the select list, not the decline, so that arm lives inside
/// [`referenced_column_projection`] and is keyed off what Exasol sent. A declined
/// filter over a REAL select list therefore keeps the referenced-column narrowing
/// (#160), which matters most on exactly this route: the fan-out carries no filter
/// here, so every row ships and column width is the only lever left.
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
    declined_filter: Option<&Json>,
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
        declined_filter,
    )?;
    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

#[cfg(test)]
#[path = "sql_builders_tests.rs"]
mod tests;

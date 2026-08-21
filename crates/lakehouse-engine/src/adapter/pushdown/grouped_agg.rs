//! GROUP BY aggregate detection, planning, and merge-wrapper SQL generation.
//!
//! Extracted verbatim from the former flat `pushdown.rs`.

use crate::scan::spec::{
    AggKind, AggregatePlan, FileEntry, PartialAggColumn, ScanSpec, partial_column_name,
    render_ordered,
};
use crate::types::mapping::{exasol_type_from_json, parse_decimal_args};
use serde_json::Value as Json;
use vs_expression::render_expression;

use super::scalar_over_agg::{
    cast_merge_items, classify_scalar_over_aggregate, fold_aggregate_plan, merge_select_items,
    parse_agg_item, render_scalar_over_merge,
};
use super::support::{
    build_fan_out_inner, cast_to_declared_type, declared_select_type, render_limit_offset,
};
use super::topn::parse_sort_flags;

/// Classification of one `selectList` item in a grouped-aggregate pushdown.
///
/// Each variant carries the item's original `selectList` ordinal so the outer
/// wrapper SELECT, its cast list, and its GROUP BY list can be assembled in the
/// user's `selectList` order for any interleaving of keys and aggregates. Exasol
/// validates the outer wrapper SELECT positionally against `selectListDataTypes`,
/// so this order must be preserved end-to-end.
// `Eq` is intentionally NOT derived: the `ScalarOverAggregate` variant carries a
// raw `serde_json::Value` node (which is `PartialEq` but not `Eq` — it can hold
// floats). `PartialEq` is all the tests and detection need.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupedSelectItem {
    /// A group-key projection. `group_key_slot` indexes `group_keys` (and the
    /// scan-side `GK_{slot}` EMITS column); `select_index` is the item's original
    /// `selectList` ordinal.
    GroupKey {
        group_key_slot: usize,
        select_index: usize,
    },
    /// An aggregate. `plan_slot` indexes `plans` (and the merged-aggregate items);
    /// `select_index` is the item's original `selectList` ordinal.
    Aggregate {
        plan_slot: usize,
        select_index: usize,
    },
    /// A constant/literal projection placeholder (Exasol's "count the groups"
    /// rewrite: a `selectList` composed only of a `literal_null` when the outer
    /// query needs the row-per-group shape but not the inner values). It
    /// contributes NO aggregate plan, so the grouped scan emits one row per
    /// distinct group. `projection` is the ready-to-emit outer-wrapper SELECT
    /// expression (the rendered literal, cast to its declared Exasol type — e.g.
    /// `CAST(NULL AS BOOLEAN)`), never a bare literal reused as a column
    /// identifier. `select_index` is the item's original `selectList` ordinal.
    Constant {
        select_index: usize,
        projection: String,
    },
    /// A scalar/arithmetic `selectList` expression that WRAPS one or more nested
    /// `function_aggregate` nodes (e.g. `ROUND(100.0 * SUM(CASE …) / COUNT(*), 2)`).
    /// The scalar wrapper itself is not decomposable — only its inner aggregates
    /// are: each is folded into the shared `plans` list (deduplicated by
    /// `AggregatePlan` equality) at detection, and the wrapper is rendered over the
    /// MERGED partials in the outer wrapper (never per shard, never over a source
    /// column). `node` is the raw `selectList` node, rewritten by
    /// `render_scalar_over_merge` at build time; `declared_type` is the item's own
    /// `selectListDataTypes` Exasol type (resolved once at detection), applied as the
    /// outer-wrapper CAST so Exasol's positional pushdown-column-type check passes.
    /// `select_index` is the item's original `selectList` ordinal.
    ScalarOverAggregate {
        select_index: usize,
        node: Json,
        declared_type: String,
    },
}

/// The original `selectList` ordinal of a classified item.
pub(super) fn select_item_index(item: &GroupedSelectItem) -> usize {
    match item {
        GroupedSelectItem::GroupKey { select_index, .. }
        | GroupedSelectItem::Aggregate { select_index, .. }
        | GroupedSelectItem::Constant { select_index, .. }
        | GroupedSelectItem::ScalarOverAggregate { select_index, .. } => *select_index,
    }
}

/// Result of detecting a GROUP BY aggregate pushdown.
///
/// `group_keys` and `plans` are the disjoint keys-first fan-out lists (unchanged
/// wire shape). `select_items` is the ordered, per-`selectList`-item
/// classification that preserves the user's select-list order so the outer
/// wrapper SELECT can be re-assembled positionally.
#[derive(Debug, Clone)]
pub struct GroupedAggregateDetection {
    /// Rendered DataFusion SQL fragment for each `groupBy` expression, in order.
    pub group_keys: Vec<String>,
    /// Aggregate plans, deduplicated by `AggregatePlan` equality. Includes both
    /// top-level `function_aggregate` select items AND aggregates nested inside a
    /// `ScalarOverAggregate` select item — a `COUNT(*)` used bare and inside a
    /// scalar collapses to ONE plan here.
    pub plans: Vec<AggregatePlan>,
    /// The Exasol-declared result type of each plan, positionally aligned 1:1 with
    /// `plans` (NOT with the `selectList`). A top-level aggregate contributes its
    /// own `selectListDataTypes` type; a plan seen only nested inside a scalar has
    /// no `selectList` ordinal of its own and defaults to `DOUBLE PRECISION` (its
    /// merged form is rendered UNCAST inside the scalar wrapper anyway — the wrapper
    /// item is cast to its OWN declared type).
    pub plan_types: Vec<String>,
    /// One entry per `selectList` item, in `selectList` order.
    pub select_items: Vec<GroupedSelectItem>,
}

/// Build the outer-wrapper SELECT expression for a constant/literal `selectList`
/// item, cast to the Exasol type Exasol declared for that ordinal.
///
/// `rendered` is the literal already rendered to SQL (e.g. `NULL`, `'x'`, `5`).
/// The result is placed in the outer wrapper SELECT (`SELECT <expr> FROM (...)
/// GROUP BY GK_*`), so it must be a self-contained expression, never a column
/// reference. Casting to the declared type keeps the pushdown output column type
/// matching what Exasol validates positionally against `selectListDataTypes`.
fn constant_projection_sql(pushdown_req: &Json, select_index: usize, rendered: &str) -> String {
    let declared = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array())
        .and_then(|d| d.get(select_index))
        .map(exasol_type_from_json);
    cast_to_declared_type(rendered, declared.as_deref())
}

/// `selectList` item types that render to a bare literal value rather than a
/// source column or a translatable scan-side expression.
///
/// Shared by `detect_group_by_aggregates` (classifies these as
/// `GroupedSelectItem::Constant`, per its doc comment above) and
/// `extract_projection` (routes these to the full-row fallback) so the two
/// call sites can never drift apart again (issue #52: `literal_bool` was
/// missing from one of the two copy-pasted lists).
const LITERAL_SELECTLIST_TYPES: &[&str] = &[
    "literal_null",
    "literal_bool",
    "literal_string",
    "literal_exactnumeric",
    "literal_double",
    "literal_date",
    "literal_timestamp",
    "literal_timestamp_utc",
    "literal_timestamputc",
];

/// Whether a `selectList` item's `type` is a bare literal (see
/// `LITERAL_SELECTLIST_TYPES`).
pub(super) fn is_literal_selectlist_item(item_type: &str) -> bool {
    LITERAL_SELECTLIST_TYPES.contains(&item_type)
}

/// Detect a GROUP BY aggregate pushdown and return the rendered group-key SQL
/// fragments, the aggregate plans, and the ordered per-item classification.
///
/// Returns `Some(GroupedAggregateDetection)` only when **all** of the following
/// hold:
/// - `aggregationType` is exactly `"group_by"`.
/// - `groupBy` is a non-empty array.
/// - Every element of `groupBy` renders successfully via `render_expression`
///   (any failure → `None` for the whole call).
/// - Every element of `selectList` is either a `function_aggregate` (contributes
///   an `AggregatePlan`) or a group-key projection — a plain `column` reference
///   or a scalar expression whose rendered SQL matches one of the group keys.
///   Any other type → `None`.
/// - The `selectList` is non-empty.
/// - No `function_aggregate` item uses `distinct: true`.
///
/// Returns `None` on any unsupported shape; the caller falls back to row
/// scanning or single-group aggregate detection.
pub fn detect_group_by_aggregates(pushdown_req: &Json) -> Option<GroupedAggregateDetection> {
    // Must be a GROUP BY aggregate request.
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) != Some("group_by") {
        return None;
    }

    // GROUP BY array must be present and non-empty.
    let group_by = pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;

    // Render each GROUP BY expression; any failure collapses the whole result.
    let mut group_keys = Vec::with_capacity(group_by.len());
    for node in group_by {
        match render_expression(node) {
            Ok(sql) => group_keys.push(sql),
            Err(_) => return None,
        }
    }

    // Classify each select-list item, preserving its original ordinal.
    let list = pushdown_req.get("selectList").and_then(|v| v.as_array())?;
    if list.is_empty() {
        return None;
    }

    let mut plans = Vec::new();
    let mut plan_types = Vec::new();
    let mut select_items = Vec::with_capacity(list.len());
    for (select_index, item) in list.iter().enumerate() {
        let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match item_type {
            "function_aggregate" => {
                let plan = parse_agg_item(item)?;
                // A top-level aggregate carries its own authoritative declared type.
                let plan_slot = fold_aggregate_plan(
                    &mut plans,
                    &mut plan_types,
                    plan,
                    Some(declared_select_type(pushdown_req, select_index)),
                );
                select_items.push(GroupedSelectItem::Aggregate {
                    plan_slot,
                    select_index,
                });
            }
            t if is_literal_selectlist_item(t) => {
                // A bare literal is a constant projection, not a group-key
                // reference — see the `Constant` variant's doc comment above
                // for the "count the groups" rationale.
                let rendered = render_expression(item).ok()?;
                let projection = constant_projection_sql(pushdown_req, select_index, &rendered);
                select_items.push(GroupedSelectItem::Constant {
                    select_index,
                    projection,
                });
            }
            _ => {
                // First: a group-key projection — a plain column reference, or a
                // scalar expression that renders to one of the group keys (e.g.
                // SELECT MOD(id,4) ... GROUP BY MOD(id,4)) emitted via GK_*.
                if let Some(group_key_slot) = render_expression(item)
                    .ok()
                    .and_then(|sql| group_keys.iter().position(|gk| *gk == sql))
                {
                    select_items.push(GroupedSelectItem::GroupKey {
                        group_key_slot,
                        select_index,
                    });
                    continue;
                }
                // Otherwise: a scalar function / arithmetic node WRAPPING one or more
                // aggregates (e.g. `ROUND(100.0 * SUM(CASE …) / COUNT(*), 2)`). Fold
                // each nested aggregate into the shared `plans` list (deduplicated by
                // `AggregatePlan` equality) and classify the item as
                // `ScalarOverAggregate`. `None` here declines the WHOLE grouped
                // detection → the caller routes to the qualified single-table
                // wrapper fallback (never a bare row scan).
                let nested = classify_scalar_over_aggregate(item)?;
                for plan in nested {
                    // Nested-only aggregates have no `selectList` ordinal of their
                    // own → default declared type (DOUBLE PRECISION); a later/earlier
                    // top-level occurrence upgrades it via `fold_aggregate_plan`.
                    fold_aggregate_plan(&mut plans, &mut plan_types, plan, None);
                }
                select_items.push(GroupedSelectItem::ScalarOverAggregate {
                    select_index,
                    node: item.clone(),
                    declared_type: declared_select_type(pushdown_req, select_index),
                });
            }
        }
    }

    Some(GroupedAggregateDetection {
        group_keys,
        plans,
        plan_types,
        select_items,
    })
}

/// Resolve the Exasol-declared type of each group key, from `selectListDataTypes`
/// where the key is also projected and from the key's own `groupBy` node where it
/// is not.
///
/// Two sources, in this precedence:
///
/// 1. `selectListDataTypes[select_index]`, for a key that also appears in
///    `selectList`. The slot is located via the detection classification, which
///    records the group-key projection's own `selectList` ordinal. Matching by
///    index (not by comparing rendered SQL strings) keeps the type correct even
///    when an expression key's `groupBy` and `selectList` renderings differ in
///    whitespace or casing. This source is authoritative: Exasol validates the
///    outer wrapper SELECT positionally against `selectListDataTypes`, so the
///    declared type of a projected key is the one Exasol type-checks.
/// 2. `groupBy[slot]["dataType"]`, for a key with no `selectList` ordinal at all
///    (`SELECT COUNT(*) … GROUP BY CAST(c AS CHAR(20))`). Only a node that
///    declares its own result type carries this — a `function_scalar_cast` does;
///    a bare `column` does not. Without it such a slot kept the "unknown width"
///    default, and a `CHAR(n)`-declared key reached DataFusion unpadded: `'ab'`
///    and `'ab   '` stayed two groups where Exasol returns one, with no outer
///    `CAST("GK_i" AS CHAR(n))` on this path to surface it as a type error (#192).
///
/// Slot `i` corresponds to `groupBy[i]`: `detect_group_by_aggregates` renders
/// exactly one `group_keys` entry per `groupBy` element, in order.
///
/// Falls back to `VARCHAR(2000000)` — the module's "unknown width" placeholder —
/// when neither source declares a type.
pub(super) fn group_key_exasol_types(
    pushdown_req: &Json,
    group_keys: &[String],
    select_items: &[GroupedSelectItem],
) -> Vec<String> {
    let declared_types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array());
    let mut types: Vec<Option<String>> = vec![None; group_keys.len()];
    for item in select_items {
        if let GroupedSelectItem::GroupKey {
            group_key_slot,
            select_index,
        } = item
            && let Some(ty) = declared_types
                .and_then(|d| d.get(*select_index))
                .map(exasol_type_from_json)
            && let Some(slot) = types.get_mut(*group_key_slot)
        {
            *slot = Some(ty);
        }
    }
    let group_by = pushdown_req.get("groupBy").and_then(|v| v.as_array());
    for (slot, resolved) in types.iter_mut().enumerate() {
        if resolved.is_none() {
            *resolved = group_by
                .and_then(|nodes| nodes.get(slot))
                .and_then(|node| node.get("dataType"))
                .map(exasol_type_from_json);
        }
    }
    types
        .into_iter()
        .map(|ty| ty.unwrap_or_else(|| "VARCHAR(2000000)".to_string()))
        .collect()
}

/// Blank-pad every `CHAR(n)`-declared group key to `n` characters, returning a
/// DataFusion-side copy of the group-key fragments.
///
/// Exasol's `CAST(x AS CHAR(n))` blank-pads to the declared width, so values that
/// differ only in trailing blanks are ONE group natively. DataFusion has no
/// fixed-width character type and does not pad, so it would emit them as distinct
/// partial groups and the outer merge — which re-groups on the unpadded `GK_*`
/// staging column — would return a row per variant where Exasol returns one
/// (issue #192). Padding the DataFusion-side key makes the staging values equal by
/// construction, so the merge collapses them exactly as Exasol would.
///
/// The pad is guarded by a length test rather than applied as a bare
/// `rpad(x, n)`: `rpad` TRUNCATES an over-length value, which would silently merge
/// a too-wide key into a wrong group and return rows for a query Exasol answers
/// with its 22001 truncation error. The `ELSE` branch hands an over-length value on
/// unmodified so the outer `CAST("GK_i" AS CHAR(n))` still raises that error.
///
/// Only this copy is padded. The unpadded fragments remain the match keys for
/// [`build_grouped_order_by_clause`], which resolves a pushed `ORDER BY` by
/// rendered-SQL equality and would decline the pushdown against a padded copy.
///
/// This is the one DataFusion-dialect SQL fragment the adapter synthesises
/// directly (`character_length`, `rpad`, `CASE`/`ELSE`) instead of routing
/// through `vs-expression`'s `Dialect::DataFusion` renderer. Every other
/// fragment reaching `ScanSpec` (`filter`, `projection`, `group_keys` before
/// this pad) is produced by that renderer; there is no VS expression node to
/// render here because the pad is a width-normalization the adapter invents
/// to make Exasol's native blank-padding semantics hold on the DataFusion
/// side, not a translation of anything in the pushdown request. The
/// `#[tokio::test]`s `padded_group_key_merges_trailing_blank_variants_without_truncating`
/// and `padded_case_fragment_plans_and_evaluates_in_datafusion` execute this
/// exact fragment through a real DataFusion `SessionContext` to pin it against
/// the planner DataFusion actually accepts.
pub(super) fn blank_pad_char_group_keys(
    group_keys: &[String],
    group_key_types: &[String],
) -> Vec<String> {
    group_keys
        .iter()
        .enumerate()
        .map(
            |(slot, fragment)| match group_key_types.get(slot).and_then(|ty| char_width(ty)) {
                Some(width) => format!(
                    "CASE WHEN character_length({fragment}) < {width} \
                     THEN rpad({fragment}, {width}) ELSE {fragment} END"
                ),
                None => fragment.clone(),
            },
        )
        .collect()
}

/// The declared width of a `CHAR(n)` type, or `None` for any other type.
///
/// Reads the digits BETWEEN the parentheses: an ASCII-declared CHAR arrives as
/// `CHAR(3) ASCII`, so trimming a trailing `)` off the whole string would find no
/// width and silently skip padding on the #192 primary shape. The `CHAR(` prefix is
/// anchored, so `VARCHAR(n)` — which contains `CHAR(` — never matches.
fn char_width(declared_type: &str) -> Option<u32> {
    declared_type
        .strip_prefix("CHAR(")?
        .split_once(')')?
        .0
        .parse()
        .ok()
}

/// Build the grouped aggregate scan SQL.
///
/// ## Two-level grouping
///
/// Inner level: a `GROUP BY shard_key` fan-out runs one UDF invocation per shard.
/// Each shard returns partial per-group results (DataFusion GROUP BY user keys inside
/// the shard).  Outer level: Exasol re-groups on the user group-key columns and merges
/// the partial aggregates.
///
/// ## EMITS column contract (Phase 3 / Group E must match this exactly)
///
/// Columns appear in this order, left to right:
///
/// 1. Group-key columns: `GK_0 VARCHAR(2000000)`, `GK_1 VARCHAR(2000000)`, …
///    `GK_{n-1} VARCHAR(2000000)` — one column per group key, always VARCHAR(2000000)
///    (Group E serialises the DataFusion group-key value to a string before emitting).
///
/// 2. Partial aggregate columns: same layout and naming as `partial_emits_items`
///    (`PARTIAL_count_i`, `PARTIAL_sum_i`, `PARTIAL_min_i`, `PARTIAL_max_i`,
///    `PARTIAL_avg_sum_i` / `PARTIAL_avg_cnt_i`,
///    `PARTIAL_stat_cnt_i` / `PARTIAL_stat_sum_i` / `PARTIAL_stat_sumsq_i`).
///
/// ## HAVING
///
/// `having` is an already-rendered DataFusion SQL fragment applied in the OUTER wrapper
/// only (after `GROUP BY`). Never pushed into the shard scan — a per-shard HAVING would
/// incorrectly discard groups that only clear the threshold after merging across shards.
///
/// ## LIMIT / OFFSET
///
/// LIMIT and OFFSET are never pushed into a shard spec for grouped queries (shard
/// emits all partial groups; the outer wrapper applies the final `LIMIT n OFFSET m`
/// when needed, through the shared [`render_limit_offset`] seam — a zero offset
/// renders the pre-offset ` LIMIT {n}` string byte-for-byte, fix-191-order-by-offset).
/// Build the explicit final `ORDER BY` element list for a grouped-aggregate merge.
///
/// Once `ORDER_BY_COLUMN` is advertised Exasol delegates the ORDER BY and no longer
/// re-sorts the grouped rows the adapter returns (add-topn-pushdown B6), so the merge
/// SQL must sort itself. The outer wrapper's output columns are the stringified
/// `GK_*` staging columns re-cast to their declared types and the merged aggregates —
/// NOT the source column names — so each sort key is rendered as a POSITIONAL output
/// ordinal (`ORDER BY 1 ...`). The ordinal references the type-cast output expression
/// (e.g. `CAST("GK_0" AS DECIMAL(20,0))`), so it sorts on the native value, never the
/// lexicographic VARCHAR `GK_*` staging column (a plain `ORDER BY "GK_0"` would sort
/// `1,10,11,2,…`, corrupting a numeric order).
///
/// A sort key that IS a group key is matched to its group-key slot exactly as
/// `detect_group_by_aggregates` matches select items (rendered-SQL equality), then to
/// that group key's `selectList` ordinal (its output position, since the outer SELECT
/// is assembled in `selectList` order with no gaps).
///
/// A sort key that is an AGGREGATE among the detected plans is rewritten to that
/// aggregate's MERGED expression over the `PARTIAL_*` columns by
/// [`render_having_over_merge`] — the same rewriter and the same `AggregatePlan`
/// equality match the merged HAVING uses. The merge wrapper is a GROUP BY query, so
/// its `ORDER BY` may reference an aggregate expression directly; no hidden output
/// column is added, so Exasol's positional `selectListDataTypes` validation of the
/// visible SELECT list is unaffected.
///
/// Returns `None` when there is no `orderBy`. Anything else — an aggregate absent from
/// the plans (no `PARTIAL_*` column exists and the adapter will not fabricate one), a
/// bare column that is no group key, a node the merge rewriter does not express — is
/// [`GroupedOrderBy::Unresolvable`], which routes the request to the qualified
/// single-table wrapper (issue #198).
pub(super) fn build_grouped_order_by_clause(
    pushdown_req: &Json,
    detection: &GroupedAggregateDetection,
) -> Option<GroupedOrderBy> {
    let elements = pushdown_req.get("orderBy").and_then(|v| v.as_array())?;
    if elements.is_empty() {
        return None;
    }
    let mut parts = Vec::with_capacity(elements.len());
    for element in elements {
        // Flags only: an aggregate sort key is no bare `column` node, so
        // `parse_sort_key_element` would yield no `SortKey` to render through. A
        // missing flag is never defaulted — a wrong guess is a wrong order.
        let Some((ascending, nulls_last)) = parse_sort_flags(element) else {
            return Some(GroupedOrderBy::Unresolvable);
        };
        let Some(expr) = element.get("expression") else {
            return Some(GroupedOrderBy::Unresolvable);
        };
        let ordering = match group_key_output_ordinal(expr, detection) {
            Some(ordinal) => ordinal.to_string(),
            None => match render_having_over_merge(expr, &detection.plans) {
                Some(merged) => merged,
                None => return Some(GroupedOrderBy::Unresolvable),
            },
        };
        parts.push(render_ordered(&ordering, ascending, nulls_last));
    }
    Some(GroupedOrderBy::Clause(parts.join(", ")))
}

/// The 1-based merge-output ordinal of a sort-key expression that IS one of the
/// group keys, or `None` when it is not a group key (or its group key has no
/// `selectList` ordinal of its own).
fn group_key_output_ordinal(expr: &Json, detection: &GroupedAggregateDetection) -> Option<usize> {
    let rendered = render_expression(expr).ok()?;
    let slot = detection.group_keys.iter().position(|gk| *gk == rendered)?;
    detection.select_items.iter().find_map(|it| match it {
        GroupedSelectItem::GroupKey {
            group_key_slot,
            select_index,
        } if *group_key_slot == slot => Some(select_index + 1),
        _ => None,
    })
}

/// Outcome of resolving a grouped-aggregate merge `ORDER BY` (see
/// [`build_grouped_order_by_clause`]). `Unresolvable` marks a pushed sort key the
/// merge decomposition cannot express; `classify_request_shape` routes such a
/// request to the qualified single-table wrapper, which renders the ordering
/// natively over materialized rows, rather than emitting a merge that would silently
/// drop it.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum GroupedOrderBy {
    Clause(String),
    Unresolvable,
}

// ponytail: well over the lint threshold now, but the function is called in only
// two places and every argument is a distinct, already-resolved plan input (no
// natural sub-grouping) — a params struct would just rename the boilerplate.
#[allow(clippy::too_many_arguments)]
pub fn build_grouped_aggregate_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    group_keys: &[String],
    group_key_types: &[String],
    aggregates: &[AggregatePlan],
    aggregate_types: &[String],
    select_items: &[GroupedSelectItem],
    limit: Option<u64>,
    offset: u64,
    col_types: &[(String, String)],
    udf_name: &str,
    distribute_udf_name: &str,
    having: Option<&str>,
    order_by: Option<&str>,
) -> String {
    // Build EMITS: GK_* columns first, then PARTIAL_* columns.
    let gk_emits: Vec<String> = (0..group_keys.len())
        .map(|i| format!(r#""GK_{i}" VARCHAR(2000000)"#))
        .collect();
    let partial_items = partial_emits_items(aggregates, col_types, aggregate_types);
    let all_emits: Vec<String> = gk_emits
        .iter()
        .chain(partial_items.iter())
        .cloned()
        .collect();
    let emits = all_emits.join(", ");

    // Build outer merge SELECT: GK_* columns + merged aggregates.
    // The scan stringifies every group key into a VARCHAR EMITS column; the outer
    // wrapper casts each back to its Exasol-declared type so the virtual-table result
    // column type matches what Exasol expects (e.g. DECIMAL for MOD(id,4)).
    let gk_select: Vec<String> = (0..group_keys.len())
        .map(|i| {
            cast_to_declared_type(
                &format!(r#""GK_{i}""#),
                group_key_types.get(i).map(String::as_str),
            )
        })
        .collect();
    let merge_items = cast_merge_items(aggregates, aggregate_types);
    let merged_partials = merge_select_items(aggregates);

    // Assemble the outer SELECT in the user's selectList order: each classified
    // item is placed at its original ordinal, interleaving group-key casts and
    // merged aggregates as the user wrote them. Exasol validates this SELECT's
    // column types positionally against selectListDataTypes, so keys-first
    // ordering (the inner fan-out shape) would transpose columns whenever an
    // aggregate precedes or interleaves with a key.
    let mut ordered = select_items.to_vec();
    ordered.sort_by_key(select_item_index);
    let outer_select: Vec<String> = ordered
        .iter()
        .filter_map(|item| match item {
            GroupedSelectItem::GroupKey { group_key_slot, .. } => {
                gk_select.get(*group_key_slot).cloned()
            }
            GroupedSelectItem::Aggregate { plan_slot, .. } => merge_items.get(*plan_slot).cloned(),
            // A constant placeholder projects its own pre-rendered, type-cast
            // expression (e.g. `CAST(NULL AS BOOLEAN)`); one row survives per
            // distinct group via the outer `GROUP BY GK_*`.
            GroupedSelectItem::Constant { projection, .. } => Some(projection.clone()),
            // A scalar-over-aggregate item: render the scalar wrapper over the MERGED
            // partials (each nested aggregate rewritten to its `PARTIAL_*` merge
            // expression), then CAST to the item's own declared type so Exasol's
            // positional pushdown-column-type check passes. Detection has already
            // validated decomposability + renderability, so this render succeeds.
            GroupedSelectItem::ScalarOverAggregate {
                node,
                declared_type,
                ..
            } => render_scalar_over_merge(node, aggregates, &merged_partials)
                .map(|expr| cast_to_declared_type(&expr, Some(declared_type))),
        })
        .collect();
    let outer_select_str = outer_select.join(", ");

    // Group BY in outer: GK_0, GK_1, ... The set of group-key columns is fixed;
    // outer GROUP BY order does not affect grouping semantics.
    let outer_group_by: Vec<String> = (0..group_keys.len())
        .map(|i| format!(r#""GK_{i}""#))
        .collect();
    let outer_group_by_str = outer_group_by.join(", ");

    // Build the inner fan-out. The common blob is shared by ALL shards, so build it
    // ONCE with `limit = None`: this structurally guarantees the "LIMIT never in a
    // per-shard partial" invariant (partial groups from every shard must be emitted
    // and merged by the outer wrapper). There is no per-shard spec left to strip.
    // The primitive nests the `GROUP BY shard_key` fan-out in the distributor (or
    // short-circuits to a from-less scalar call for a single shard); the outer wrapper
    // below re-groups the emitted per-shard partials on the user's group keys.
    let mut common_template = spec_template.clone();
    common_template.common.limit = None;
    let fan_out = build_fan_out_inner(
        &common_template,
        shards,
        &emits,
        udf_name,
        distribute_udf_name,
    );

    let mut sql =
        format!("SELECT {outer_select_str} FROM ({fan_out}) GROUP BY {outer_group_by_str}");

    // HAVING: applied in outer wrapper only, never pushed into shard scan.
    if let Some(h) = having.filter(|h| !h.is_empty()) {
        sql.push_str(" HAVING ");
        sql.push_str(h);
    }

    // Explicit merge ORDER BY (add-topn-pushdown B6): SQL requires it after HAVING
    // and before LIMIT. Rendered as positional output ordinals so it sorts the
    // type-cast output, not the lexicographic VARCHAR GK_* staging columns.
    if let Some(ob) = order_by.filter(|s| !s.is_empty()) {
        sql.push_str(" ORDER BY ");
        sql.push_str(ob);
    }

    sql.push_str(&render_limit_offset(limit, offset));
    sql
}

/// Build the EMITS items for the aggregate fan-out, following the COLUMN CONTRACT.
///
/// [`AggKind::partial_columns`] owns which columns exist and in what order, and
/// [`partial_column_name`] owns what each is called; this function owns only each
/// column's Exasol type.
///
/// `col_types` maps uppercase column names to their Exasol type strings.
/// MIN/MAX partial columns use the target column's exact type.
/// SUM partial columns: DOUBLE PRECISION stays DOUBLE PRECISION; DECIMAL(p,s) widens to
/// DECIMAL(36,s) to avoid overflow; any other type falls back (callers should have validated
/// via `validate_agg_col_types` before reaching here — see handle_pushdown).
/// Every counting column is DECIMAL(20,0). AVG's partial sum and the stat family's
/// sum/sumsq are DOUBLE PRECISION — AVG is inherently fractional, and the
/// sufficient statistics are reconstructed in floating point.
pub(super) fn partial_emits_items(
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
    aggregate_types: &[String],
) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| {
            // Declared aggregate result type at this ordinal — the caller's
            // per-plan `aggregate_types` list, resolved from `selectListDataTypes`;
            // the sole type source for an expression-argument aggregate, which has
            // no source column.
            let declared = aggregate_types.get(i).map(String::as_str);
            plan.kind.partial_columns().iter().map(move |col| {
                let ty = match col {
                    PartialAggColumn::CountStar
                    | PartialAggColumn::CountArg
                    | PartialAggColumn::AvgCnt
                    | PartialAggColumn::StatCnt => "DECIMAL(20,0)".to_string(),
                    PartialAggColumn::AvgSum
                    | PartialAggColumn::StatSum
                    | PartialAggColumn::StatSumSq => "DOUBLE PRECISION".to_string(),
                    PartialAggColumn::Sum => sum_emit_type(&col_type_for(
                        plan.column.as_deref(),
                        plan.arg_expr.as_deref(),
                        col_types,
                        declared,
                    )),
                    PartialAggColumn::Min | PartialAggColumn::Max => col_type_for(
                        plan.column.as_deref(),
                        plan.arg_expr.as_deref(),
                        col_types,
                        declared,
                    ),
                };
                let name = partial_column_name(*col, i);
                format!(r#""{name}" {ty}"#)
            })
        })
        .collect()
}

/// Look up the Exasol type used to size an aggregate's partial/merge column.
///
/// For a bare-column aggregate the type is the target column's own Exasol type
/// (from `col_types`), falling back to `DOUBLE PRECISION` when the column is
/// absent from the map. For an expression-argument aggregate (`arg_expr` set,
/// no source `column`) there is no source column to look up, so the type is the
/// aggregate item's declared result type (`declared`, the caller's per-plan
/// `aggregate_types` list, resolved from `selectListDataTypes`); when the declared
/// type is unavailable it falls back to the column-map lookup (then `DOUBLE PRECISION`).
pub(super) fn col_type_for(
    column: Option<&str>,
    arg_expr: Option<&str>,
    col_types: &[(String, String)],
    declared: Option<&str>,
) -> String {
    if column.is_none()
        && arg_expr.is_some()
        && let Some(ty) = declared
    {
        return ty.to_string();
    }
    column
        .and_then(|col| {
            col_types
                .iter()
                .find(|(n, _)| n == col)
                .map(|(_, t)| t.clone())
        })
        .unwrap_or_else(|| "DOUBLE PRECISION".to_string())
}

/// Map a column's Exasol type to the appropriate SUM partial EMITS type.
///
/// DOUBLE PRECISION => DOUBLE PRECISION (no change).
/// DECIMAL(p,s) => DECIMAL(36,s) (widened to max Exasol precision, preserving scale);
/// an absent scale defaults to 0, per the shared `parse_decimal_args` contract.
/// Any other type (DATE, TIMESTAMP, VARCHAR, BOOLEAN) — and any DECIMAL declaration
/// `parse_decimal_args` rejects — => DOUBLE PRECISION as an emergency fallback
/// (callers should have validated before reaching here).
fn sum_emit_type(col_ty: &str) -> String {
    if col_ty == "DOUBLE PRECISION" {
        return "DOUBLE PRECISION".to_string();
    }
    // No uppercasing step: every producer of `col_ty` already emits uppercase, and
    // adding one would change the answer for a lowercase input.
    if let Some((_p, s)) = parse_decimal_args(col_ty) {
        return format!("DECIMAL(36,{s})");
    }
    // Non-numeric type: validation should have caught this, but fall back gracefully.
    "DOUBLE PRECISION".to_string()
}

/// Return `true` if all SUM/MIN/MAX/stat targets have a supported Exasol column type.
///
/// SUM and the STDDEV/VARIANCE family are only valid over DOUBLE PRECISION or DECIMAL columns.
/// MIN/MAX are valid over any comparable type (DATE, TIMESTAMP, VARCHAR included).
/// Returns `false` (fall back to row scan) when any SUM or stat aggregate targets a
/// non-numeric column.
///
/// An expression-argument SUM/stat (`arg_expr` set, no source `column`) passes:
/// its partial type is derived from the declared aggregate result type in
/// `partial_emits_items`, and Exasol only declares such aggregates over numeric
/// results — so the column-map lookup here (which has no entry) safely resolves
/// to the numeric `DOUBLE PRECISION` fallback rather than a spurious fall-back.
pub fn validate_agg_col_types(
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
) -> bool {
    for plan in aggregates {
        let needs_numeric = matches!(
            plan.kind,
            AggKind::Sum
                | AggKind::VarPop
                | AggKind::VarSamp
                | AggKind::StddevPop
                | AggKind::StddevSamp
        );
        if needs_numeric {
            let ty = col_type_for(
                plan.column.as_deref(),
                plan.arg_expr.as_deref(),
                col_types,
                None,
            );
            if !is_numeric_exasol_type(&ty) {
                return false;
            }
        }
    }
    true
}

/// Return `true` for Exasol types that support SUM (DOUBLE PRECISION, DECIMAL).
fn is_numeric_exasol_type(ty: &str) -> bool {
    ty == "DOUBLE PRECISION" || ty.starts_with("DECIMAL(")
}

/// Render a HAVING predicate for the OUTER merge wrapper.
///
/// The outer wrapper's only columns are `GK_*` and `PARTIAL_*` — there is no
/// source column (e.g. `SCORE`) available there. So each `function_aggregate`
/// reference in the predicate is rewritten to its merged expression (e.g.
/// `SUM(score)` → `SUM("PARTIAL_sum_0")`), matched to `plans` by
/// `AggregatePlan` equality (kind + source column). Non-aggregate leaves
/// (columns, literals, scalar functions, arithmetic) delegate to
/// `render_expression`.
///
/// Returns `None` if the predicate references an aggregate not among `plans`
/// (cannot be merged) or contains an unsupported node — the caller then
/// declines the grouped pushdown rather than emit a wrong or dropped HAVING.
pub(super) fn render_having_over_merge(node: &Json, plans: &[AggregatePlan]) -> Option<String> {
    if !node.is_object() {
        return None;
    }
    let kind = node.get("type").and_then(|t| t.as_str())?;
    let child = |key: &str| node.get(key);

    // An aggregate reference: rewrite to its uncast merged expression. Uncast is
    // correct here — the comparison is against the raw merged numeric value; the
    // CAST in `cast_merge_items` is only for output-column typing.
    if kind == "function_aggregate" {
        let plan = parse_agg_item(node)?;
        let idx = plans.iter().position(|p| *p == plan)?;
        return merge_select_items(plans).into_iter().nth(idx);
    }

    // Boolean / comparison predicate nodes that can appear in a HAVING. Operator
    // strings and parenthesization mirror `vs-expression`'s renderer so output
    // matches conventions.
    match kind {
        "predicate_and" => render_having_junction(child("expressions"), plans, " AND "),
        "predicate_or" => render_having_junction(child("expressions"), plans, " OR "),
        "predicate_not" => {
            let inner = render_having_operand(child("expression"), plans)?;
            Some(format!("(NOT {inner})"))
        }
        "predicate_equal"
        | "predicate_notequal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal" => {
            let op = match kind {
                "predicate_equal" => "=",
                "predicate_notequal" => "<>",
                "predicate_less" => "<",
                "predicate_lessequal" => "<=",
                "predicate_greater" => ">",
                "predicate_greaterequal" => ">=",
                _ => unreachable!(),
            };
            let left = render_having_operand(child("left"), plans)?;
            let right = render_having_operand(child("right"), plans)?;
            Some(format!("({left} {op} {right})"))
        }
        "predicate_between" => {
            let target = render_having_operand(child("expression"), plans)?;
            let low = render_having_operand(child("left"), plans)?;
            let high = render_having_operand(child("right"), plans)?;
            Some(format!("({target} BETWEEN {low} AND {high})"))
        }
        "predicate_is_null" => {
            let inner = render_having_operand(child("expression"), plans)?;
            Some(format!("({inner} IS NULL)"))
        }
        "predicate_is_not_null" => {
            let inner = render_having_operand(child("expression"), plans)?;
            Some(format!("({inner} IS NOT NULL)"))
        }
        _ => None,
    }
}

/// Render a HAVING operand: a `function_aggregate` rewrites to its merged
/// expression; any other node (column, literal, scalar function, arithmetic,
/// or nested predicate) delegates to `render_having_over_merge` — which itself
/// falls back to `render_expression` for non-predicate, non-aggregate nodes.
fn render_having_operand(node: Option<&Json>, plans: &[AggregatePlan]) -> Option<String> {
    let node = node.filter(|n| !n.is_null())?;
    let kind = node.get("type").and_then(|t| t.as_str())?;
    match kind {
        "function_aggregate"
        | "predicate_and"
        | "predicate_or"
        | "predicate_not"
        | "predicate_equal"
        | "predicate_notequal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal"
        | "predicate_between"
        | "predicate_is_null"
        | "predicate_is_not_null" => render_having_over_merge(node, plans),
        // Any other node (literal, column, scalar function, arithmetic): render over
        // the merge wrapper, rewriting EVERY nested `function_aggregate` to its merged
        // `PARTIAL_*` expression. A scalar function wrapping an aggregate (e.g.
        // `ROUND(SUM(x) / COUNT(*), 2)`) is thus rewritten correctly rather than
        // rendered verbatim over absent source columns — the fix that closes issue
        // #82's gap, which also covers a scalar-over-aggregate inside a HAVING. A
        // node with no nested aggregate renders exactly as `vs-expression` would.
        _ => render_scalar_over_merge(node, plans, &merge_select_items(plans)),
    }
}

/// Render an AND/OR junction over the outer merge wrapper, mirroring
/// `vs-expression`'s `render_junction`: single child unwrapped, multiple joined
/// and parenthesized. Any child that fails to render collapses the junction.
fn render_having_junction(
    expressions: Option<&Json>,
    plans: &[AggregatePlan],
    op: &str,
) -> Option<String> {
    let items = expressions?.as_array()?;
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        parts.push(render_having_over_merge(item, plans)?);
    }
    match parts.len() {
        0 => None,
        1 => parts.into_iter().next(),
        _ => Some(format!("({})", parts.join(op))),
    }
}

#[cfg(test)]
#[path = "grouped_agg_tests.rs"]
mod tests;

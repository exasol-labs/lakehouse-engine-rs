//! GROUP BY aggregate detection, planning, and merge-wrapper SQL generation.
//!
//! Extracted verbatim from the former flat `pushdown.rs`.

use crate::scan::spec::{
    AggKind, AggregatePlan, FileEntry, PartialAggColumn, ScanSpec, partial_column_name,
    render_ordered,
};
use crate::types::mapping::{exasol_type_from_json, parse_decimal_args};
use serde_json::Value as Json;
use vs_expression::{render_expression, render_expression_exasol};

use super::single_group_agg::parse_agg_item;
use super::support::{
    build_fan_out_inner, cast_to_declared_type, quote_ident, render_limit_offset,
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
    /// item is cast to its OWN declared type). Replaces `aggregate_exasol_types` on
    /// the grouped path, which keyed off top-level select items only and would
    /// misalign once nested aggregates join `plans`.
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

    let declared_type_at = |select_index: usize| -> String {
        pushdown_req
            .get("selectListDataTypes")
            .and_then(|v| v.as_array())
            .and_then(|d| d.get(select_index))
            .map(exasol_type_from_json)
            .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
    };

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
                    Some(declared_type_at(select_index)),
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
                    declared_type: declared_type_at(select_index),
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

/// Fold an aggregate plan into the shared `plans`/`plan_types` lists, deduplicating
/// by `AggregatePlan` equality (kind + argument) so an aggregate used more than once
/// across the select list — bare AND nested inside a scalar — collapses to ONE
/// `PARTIAL_*` column (decision-log [4]). Returns the plan's slot.
///
/// `declared` is `Some` for a top-level `function_aggregate` select item (its
/// authoritative `selectListDataTypes` type) and `None` for an aggregate seen only
/// nested inside a scalar. A `Some` declared type always wins: it overwrites a slot
/// that a nested occurrence created with the default, so a bare aggregate's output
/// CAST uses the type Exasol declared for it regardless of select-list order.
fn fold_aggregate_plan(
    plans: &mut Vec<AggregatePlan>,
    plan_types: &mut Vec<String>,
    plan: AggregatePlan,
    declared: Option<String>,
) -> usize {
    match plans.iter().position(|p| *p == plan) {
        Some(slot) => {
            if let Some(ty) = declared {
                plan_types[slot] = ty;
            }
            slot
        }
        None => {
            let slot = plans.len();
            plans.push(plan);
            plan_types.push(declared.unwrap_or_else(|| "DOUBLE PRECISION".to_string()));
            slot
        }
    }
}

/// Sentinel `column` name substituted for the i-th nested aggregate while rendering
/// a scalar-over-aggregate node through the `vs-expression` translator. Distinctive
/// and already uppercase so it survives the translator's `column` uppercasing and
/// cannot collide with a real column; the rendered token is later string-replaced
/// with the aggregate's merged `PARTIAL_*` expression.
fn agg_sentinel_name(i: usize) -> String {
    format!("__LH_AGG_MERGE_{i}__")
}

/// The exact SQL token `vs-expression` emits for the i-th aggregate sentinel column
/// (a quoted identifier), used as the string-replacement target.
fn agg_sentinel_token(i: usize) -> String {
    quote_ident(&agg_sentinel_name(i))
}

/// Build the sentinel `column` node for the i-th nested aggregate.
fn sentinel_column_node(i: usize) -> Json {
    serde_json::json!({ "type": "column", "name": agg_sentinel_name(i) })
}

/// Deep-clone `node`, replacing every nested `function_aggregate` subtree with a
/// sentinel `column` node (`__LH_AGG_MERGE_{i}__`) and collecting the original
/// aggregate nodes in sentinel order. Recursion STOPS at a `function_aggregate`
/// (its arguments are subsumed into the aggregate and rewritten wholesale), so a
/// `column` inside an aggregate (e.g. inside `SUM(CASE … col …)`) is never treated
/// as a residual. `residual_column` is set when a bare `column` appears OUTSIDE any
/// aggregate — the outer merge wrapper exposes only `GK_*`/`PARTIAL_*` columns, so
/// such a node cannot be rendered there and disqualifies the scalar-over-aggregate
/// classification (the request routes to the qualified fallback instead).
fn sentinelize_aggregates(
    node: &Json,
    aggregates: &mut Vec<Json>,
    residual_column: &mut bool,
) -> Json {
    match node {
        Json::Object(map) => match map.get("type").and_then(|t| t.as_str()) {
            Some("function_aggregate") => {
                let i = aggregates.len();
                aggregates.push(node.clone());
                sentinel_column_node(i)
            }
            kind => {
                if kind == Some("column") {
                    *residual_column = true;
                }
                let mut out = serde_json::Map::with_capacity(map.len());
                for (key, value) in map {
                    out.insert(
                        key.clone(),
                        sentinelize_aggregates(value, aggregates, residual_column),
                    );
                }
                Json::Object(out)
            }
        },
        Json::Array(items) => Json::Array(
            items
                .iter()
                .map(|v| sentinelize_aggregates(v, aggregates, residual_column))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Classify a `selectList` item as a scalar-over-aggregate: a scalar/arithmetic node
/// wrapping one or more nested `function_aggregate` nodes, every one decomposable via
/// `parse_agg_item` (so `DISTINCT`, an unsupported function, or an untranslatable
/// argument declines), with no bare source `column` outside those aggregates and a
/// residual structure the `vs-expression` translator can render. Returns the nested
/// plans in encounter order, or `None` to decline (→ qualified fallback).
fn classify_scalar_over_aggregate(node: &Json) -> Option<Vec<AggregatePlan>> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    // Not a scalar-over-aggregate: no aggregate to decompose, or a source column the
    // outer merge wrapper cannot reference.
    if aggregates.is_empty() || residual_column {
        return None;
    }
    // The residual scalar/arithmetic structure (with aggregates sentinelized) must be
    // renderable by the translator — otherwise the outer wrapper cannot be built.
    render_expression(&sentinel_tree).ok()?;
    aggregates.iter().map(parse_agg_item).collect()
}

/// Render a scalar/arithmetic node over the OUTER merge wrapper: every nested
/// `function_aggregate` is rewritten to its merged `PARTIAL_*` expression (matched to
/// `plans` by `AggregatePlan` equality), and the surrounding scalar/arithmetic
/// structure is rendered verbatim by the `vs-expression` translator. This is the one
/// merge-rewrite path shared by the grouped select list AND a scalar-over-aggregate
/// inside a HAVING (decision-log [2]).
///
/// It reuses the translator by SUBSTITUTION rather than re-implementing its scalar
/// arms: each aggregate subtree is replaced with a distinctive sentinel `column`,
/// the tree is rendered once, then each sentinel token is string-replaced with the
/// aggregate's merged expression. This inherits every scalar/arithmetic node type,
/// operator string, and parenthesization the translator supports with zero risk of
/// drifting from it. Returns `None` if the structure cannot be rendered or a nested
/// aggregate is not among `plans` (cannot be merged).
fn render_scalar_over_merge(node: &Json, plans: &[AggregatePlan]) -> Option<String> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    let merged = merge_select_items(plans);
    // Exasol dialect: this SQL is spliced verbatim into the OUTER merge wrapper,
    // which Exasol's own core engine parses — so a CAST target needs Exasol
    // syntax (length-qualified `VARCHAR(n)`), unlike the DataFusion-side
    // renderability check in `classify_scalar_over_aggregate`.
    let mut sql = render_expression_exasol(&sentinel_tree).ok()?;
    for (i, agg) in aggregates.iter().enumerate() {
        let plan = parse_agg_item(agg)?;
        let slot = plans.iter().position(|p| *p == plan)?;
        sql = sql.replace(&agg_sentinel_token(i), merged.get(slot)?);
    }
    Some(sql)
}

/// Resolve the Exasol-declared type of each group key from `selectListDataTypes`.
///
/// Each group-key slot is located via the detection classification, which
/// records the group-key projection's own `selectList` ordinal; the parallel
/// `selectListDataTypes` array at that ordinal gives its declared result type.
/// Matching by index (not by comparing rendered SQL strings) keeps the type
/// correct even when an expression key's `groupBy` and `selectList` renderings
/// differ in whitespace or casing. Falls back to `VARCHAR(2000000)` when the
/// type cannot be located.
pub(super) fn group_key_exasol_types(
    pushdown_req: &Json,
    group_keys: &[String],
    select_items: &[GroupedSelectItem],
) -> Vec<String> {
    let declared_types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array());
    let mut types = vec!["VARCHAR(2000000)".to_string(); group_keys.len()];
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
            *slot = ty;
        }
    }
    types
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
            } => render_scalar_over_merge(node, aggregates)
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
            // Declared aggregate result type at this ordinal (from
            // `aggregate_exasol_types`/`selectListDataTypes`); the sole type source
            // for an expression-argument aggregate, which has no source column.
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
/// aggregate item's declared result type (`declared`, from
/// `aggregate_exasol_types`/`selectListDataTypes`); when the declared type is
/// unavailable it falls back to the column-map lookup (then `DOUBLE PRECISION`).
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

/// The three König–Huygens SQL fragments shared by all four statistical merge
/// formulas, rendered over the partial columns of the aggregate at ordinal `i`.
///
/// Held as one value so [`merge_select_items`]' four statistical arms compose
/// them by name instead of re-inlining the text: a variance is
/// `numer / pop_denom` or `numer / samp_denom`, and a standard deviation is
/// [`stddev_of`] applied to either. `numer` carries its own outer parentheses,
/// so no composition adds any.
struct StatMergeFragments {
    numer: String,
    pop_denom: String,
    samp_denom: String,
}

impl StatMergeFragments {
    fn for_ordinal(i: usize) -> Self {
        let cnt = partial_column_name(PartialAggColumn::StatCnt, i);
        let sum = partial_column_name(PartialAggColumn::StatSum, i);
        let sumsq = partial_column_name(PartialAggColumn::StatSumSq, i);
        let pop_denom = format!(r#"NULLIF(SUM("{cnt}"), 0)"#);
        let samp_denom =
            format!(r#"CASE WHEN SUM("{cnt}") <= 1 THEN NULL ELSE SUM("{cnt}") - 1 END"#);
        let numer = format!(r#"(SUM("{sumsq}") - SUM("{sum}") * SUM("{sum}") / {pop_denom})"#);
        Self {
            numer,
            pop_denom,
            samp_denom,
        }
    }
}

/// Render the NULL-preserving square root of a variance expression.
///
/// Adds exactly one parenthesis pair — around the `IS NULL` subject — and none
/// around the `GREATEST` argument; that is the nesting the merge SQL is pinned
/// to. See [`merge_select_items`] for why the `IS NULL` test cannot be folded
/// into `GREATEST`.
fn stddev_of(var: &str) -> String {
    format!("CASE WHEN ({var}) IS NULL THEN NULL ELSE SQRT(GREATEST(0.0, {var})) END")
}

/// Build the outer merge SELECT items following the COLUMN CONTRACT.
///
/// Every partial column name comes from [`partial_column_name`], so a rename can
/// never land here without also landing in the scan's aliases and the `EMITS`
/// clause; this function owns only the re-aggregation formula per [`AggKind`].
///
/// AVG uses `SUM(sum) / NULLIF(SUM(cnt), 0)` — the NULLIF guard ensures division
/// by zero yields NULL rather than an error (Exasol: `x / NULL = NULL`).
///
/// STDDEV/VARIANCE sufficient-statistics reconstruction (König–Huygens identity):
///   numer    = SUM(sumsq) - SUM(sum)² / NULLIF(SUM(cnt), 0)
///   var_pop  = numer / NULLIF(SUM(cnt), 0)          [NULL when cnt = 0]
///   var_samp = numer / (SUM(cnt) - 1)               [NULL when cnt ≤ 1, via CASE]
///
///   stddev_pop/samp = CASE WHEN var IS NULL THEN NULL
///                          ELSE SQRT(GREATEST(0.0, var)) END
///
///   The CASE guard is required because Exasol's `GREATEST(0.0, NULL) = 0.0`
///   (returns the max of non-NULL inputs; only returns NULL if ALL inputs are NULL),
///   so a bare `SQRT(GREATEST(0.0, NULL))` would yield `0.0` instead of NULL for
///   empty tables (N=0, pop) and single-row groups (N=1, samp).
///   The GREATEST(0.0, …) inside the ELSE branch guards against tiny-negative
///   float rounding artifacts that would otherwise cause SQRT to error.
fn merge_select_items(aggregates: &[AggregatePlan]) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .map(|(i, plan)| match plan.kind {
            AggKind::Count => {
                let count = partial_column_name(PartialAggColumn::CountStar, i);
                format!(r#"SUM("{count}")"#)
            }
            AggKind::CountCol => {
                let count = partial_column_name(PartialAggColumn::CountArg, i);
                format!(r#"SUM("{count}")"#)
            }
            AggKind::Sum => {
                let sum = partial_column_name(PartialAggColumn::Sum, i);
                format!(r#"SUM("{sum}")"#)
            }
            AggKind::Min => {
                let min = partial_column_name(PartialAggColumn::Min, i);
                format!(r#"MIN("{min}")"#)
            }
            AggKind::Max => {
                let max = partial_column_name(PartialAggColumn::Max, i);
                format!(r#"MAX("{max}")"#)
            }
            AggKind::Avg => {
                let sum = partial_column_name(PartialAggColumn::AvgSum, i);
                let cnt = partial_column_name(PartialAggColumn::AvgCnt, i);
                format!(r#"SUM("{sum}") / NULLIF(SUM("{cnt}"), 0)"#)
            }
            AggKind::VarPop => {
                let stat = StatMergeFragments::for_ordinal(i);
                format!("{} / {}", stat.numer, stat.pop_denom)
            }
            AggKind::VarSamp => {
                let stat = StatMergeFragments::for_ordinal(i);
                format!("{} / {}", stat.numer, stat.samp_denom)
            }
            AggKind::StddevPop => {
                let stat = StatMergeFragments::for_ordinal(i);
                stddev_of(&format!("{} / {}", stat.numer, stat.pop_denom))
            }
            AggKind::StddevSamp => {
                let stat = StatMergeFragments::for_ordinal(i);
                stddev_of(&format!("{} / {}", stat.numer, stat.samp_denom))
            }
        })
        .collect()
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
        _ => render_scalar_over_merge(node, plans),
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

/// Build the outer merge SELECT items, each cast to its Exasol-declared result type.
///
/// The merge expression (e.g. `SUM("PARTIAL_count_0")` over DECIMAL(20,0) partials →
/// DECIMAL(31,0)) must match the type Exasol declared for that select-list column
/// (COUNT(score) → DECIMAL(18,0)); Exasol strictly validates the pushdown output
/// column types. When no declared type is available (or it is VARCHAR(2000000)),
/// the merge expression is emitted uncast.
pub(super) fn cast_merge_items(
    aggregates: &[AggregatePlan],
    aggregate_types: &[String],
) -> Vec<String> {
    merge_select_items(aggregates)
        .into_iter()
        .enumerate()
        .map(|(i, expr)| cast_to_declared_type(&expr, aggregate_types.get(i).map(String::as_str)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::detect_aggregates;
    use super::super::joins::{
        build_qualified_single_table_fallback_sql, referenced_column_projection,
    };
    use super::super::support::{
        DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, aggregate_exasol_types, extract_all_column_types,
        shard_count,
    };
    use super::super::test_support::*;
    use super::*;
    use crate::scan::spec::CommonScanSpec;

    // NOTE on the `sum_emit_type` tests below: routing `sum_emit_type` through the
    // canonical `parse_decimal_args` makes it GAIN a whitespace-trimming step it did
    // not have before, because `parse_decimal_args` trims each argument before
    // parsing. `DECIMAL(10, 2)` therefore yields `DECIMAL(36,2)` where it used to
    // yield `DECIMAL(36, 2)` — the raw scale slice echoed verbatim. That is an
    // INTENDED consequence of consolidation, not an incidental one, and it is
    // unreachable from every producer of `col_ty` in this repo (each emits a
    // canonical, already-trimmed `DECIMAL(p,s)` under a `p,s <= 36` guard).

    /// The one representative neither invariant generates: with no comma there is no
    /// scale text to diverge. The move comes solely from `parse_decimal_args`
    /// defaulting an absent scale to `0`, where `sum_emit_type` used to require a
    /// comma and decline the input entirely.
    #[test]
    fn sum_emit_type_absent_scale_widens_to_a_scale_zero_decimal() {
        assert_eq!(sum_emit_type("DECIMAL(10)"), "DECIMAL(36,0)");
    }

    /// Invariant (a) as a property over an OPEN input set: for every scale text that
    /// is not already the canonical `i8` rendering, the answer is never the raw echo
    /// the pre-consolidation parser produced. Only a canonical rendering — or the
    /// numeric fallback — can emerge from a parsed `i8`. An open set is the right
    /// shape here because the pre-consolidation parser echoed the raw scale text
    /// without reading it, so the diverging input set has no closed enumeration.
    ///
    /// The rows cover one divergence class each: untrimmed whitespace (the gained
    /// trimming step); a leading `+` or a leading zero, which `i8` parsing accepts
    /// and which therefore can only re-emerge canonically; a non-numeric scale, which
    /// used to be interpolated verbatim into an EMITS type Exasol cannot parse and now
    /// declines to the numeric fallback; a scale outside `i8`; and a further comma,
    /// where the old `split_once(',')` kept `2,3` as the scale text while
    /// `parse_decimal_args` rejects a third argument outright.
    #[test]
    fn sum_emit_type_never_echoes_a_non_canonical_scale_text() {
        // (raw scale text, canonical answer once parsed) — `None` = the parser
        // rejects the text, so the answer is the numeric fallback.
        let non_canonical: &[(&str, Option<&str>)] = &[
            (" 2", Some("DECIMAL(36,2)")),
            ("2 ", Some("DECIMAL(36,2)")),
            ("+2", Some("DECIMAL(36,2)")),
            ("02", Some("DECIMAL(36,2)")),
            ("-02", Some("DECIMAL(36,-2)")),
            ("X", None),
            ("2,3", None),
            ("200", None),
            ("", None),
        ];
        for (raw_scale, canonical) in non_canonical {
            let answer = sum_emit_type(&format!("DECIMAL(10,{raw_scale})"));
            assert_ne!(
                answer,
                format!("DECIMAL(36,{raw_scale})"),
                "a non-canonical scale text must never be echoed verbatim"
            );
            assert_eq!(
                answer,
                canonical.unwrap_or("DOUBLE PRECISION"),
                "wrong answer for scale text {raw_scale:?}"
            );
        }
    }

    /// Invariant (b) as a property over an OPEN input set: every precision
    /// `parse_decimal_args` rejects now declines to the numeric fallback, where it
    /// used to yield `DECIMAL(36,2)` regardless — the pre-consolidation parser bound
    /// the precision as `_p` and never read it, so even an unrepresentable precision
    /// borrowed a `DECIMAL(36,…)` width. That non-reading is also why the diverging
    /// set is open rather than a closed enumeration. The rows cover one rejection
    /// class each: a precision outside `u8`, a negative one, a non-numeric one, and
    /// an empty or whitespace-only one.
    #[test]
    fn sum_emit_type_declines_every_precision_the_parser_rejects() {
        for rejected_precision in ["300", "256", "-1", "X", "", " "] {
            assert_eq!(
                sum_emit_type(&format!("DECIMAL({rejected_precision},2)")),
                "DOUBLE PRECISION",
                "precision {rejected_precision:?} is rejected by the parser, so the \
                 aggregate must fall back rather than borrow a DECIMAL(36,…) width"
            );
        }
    }

    /// A grouped-aggregate merge item that CASTs a scalar-over-aggregate to a
    /// CHAR/VARCHAR target must render the CAST target LENGTH-QUALIFIED
    /// (`VARCHAR(20)`): `render_scalar_over_merge`'s output is spliced into the
    /// OUTER merge wrapper that Exasol's own engine parses, where a bare
    /// length-less `VARCHAR` is the exact "unexpected ')', expecting '('" parse
    /// error this fix addresses. Guards the grouped-merge half of the
    /// Exasol-dialect CAST split; the DataFusion-side renderability check in
    /// `classify_scalar_over_aggregate` deliberately keeps bare `VARCHAR`.
    #[test]
    fn scalar_over_merge_casts_to_length_qualified_exasol_varchar() {
        let sum_node = serde_json::json!({
            "type": "function_aggregate", "name": "SUM", "distinct": false,
            "arguments": [{"type": "column", "name": "x"}]
        });
        let plans = vec![parse_agg_item(&sum_node).expect("SUM(x) must parse to a plan")];
        let node = serde_json::json!({
            "type": "function_scalar_cast", "name": "CAST",
            "arguments": [sum_node],
            "dataType": {"type": "CHAR", "size": 20, "characterSet": "ASCII"}
        });
        let sql = render_scalar_over_merge(&node, &plans)
            .expect("CAST over a mergeable aggregate must render");
        assert!(
            sql.contains("VARCHAR(20)"),
            "Exasol-parsed merge wrapper needs a length-qualified CAST target: {sql}"
        );
        assert!(
            !sql.contains("AS VARCHAR)"),
            "must NOT emit a bare length-less VARCHAR (Exasol rejects it): {sql}"
        );
    }

    /// Scenario (capability-extensions): a GROUP BY request carrying a
    /// COUNT(DISTINCT) still declines (falls back to row scanning); grouped
    /// distinct is explicitly out of scope.
    #[test]
    fn grouped_count_distinct_falls_back_to_row_scan() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("COUNT", Some("L_SHIPMODE"), true),
            ],
        });
        assert!(
            detect_group_by_aggregates(&req).is_none(),
            "grouped COUNT(DISTINCT) must still decline (row-scan fallback)"
        );
        // A non-grouped detection also declines this shape (it has a GROUP BY).
        assert!(
            detect_aggregates(&req).is_none(),
            "the single-group path rejects any request carrying a non-empty GROUP BY"
        );
    }

    /// R.1: MIN/MAX over a DATE column must EMIT DATE, not DOUBLE PRECISION.
    #[test]
    fn partial_emits_min_max_preserve_date_timestamp_type() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("EVENT_DATE".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("EVENT_TS".into()),
                arg_expr: None,
            },
        ];
        let col_types = vec![
            ("EVENT_DATE".to_string(), "DATE".to_string()),
            ("EVENT_TS".to_string(), "TIMESTAMP".to_string()),
        ];
        let emits = partial_emits_items(&plans, &col_types, &[]);
        assert!(
            emits[0].contains("DATE") && !emits[0].contains("DOUBLE"),
            "MIN over DATE must emit DATE, not DOUBLE: {:?}",
            emits[0]
        );
        assert!(
            emits[1].contains("TIMESTAMP") && !emits[1].contains("DOUBLE"),
            "MAX over TIMESTAMP must emit TIMESTAMP, not DOUBLE: {:?}",
            emits[1]
        );
    }

    /// R.1: SUM over a DECIMAL(20,0) integer column must emit DECIMAL(36,0), not DOUBLE.
    #[test]
    fn partial_emits_sum_integer_stays_decimal() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }];
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(20,0)".to_string())];
        let emits = partial_emits_items(&plans, &col_types, &[]);
        assert!(
            emits[0].contains("DECIMAL") && !emits[0].contains("DOUBLE"),
            "SUM over DECIMAL integer must emit DECIMAL, not DOUBLE: {:?}",
            emits[0]
        );
        // Scale must be 0 (preserved from original DECIMAL(20,0)).
        assert!(
            emits[0].contains("DECIMAL(36,0)"),
            "SUM over DECIMAL(20,0) must widen to DECIMAL(36,0): {:?}",
            emits[0]
        );
    }

    /// R.1: SUM over a DOUBLE PRECISION column stays DOUBLE PRECISION.
    #[test]
    fn partial_emits_sum_double_stays_double() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
            arg_expr: None,
        }];
        let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let emits = partial_emits_items(&plans, &col_types, &[]);
        assert!(
            emits[0].contains("DOUBLE PRECISION"),
            "SUM over DOUBLE must emit DOUBLE PRECISION: {:?}",
            emits[0]
        );
    }

    /// R.1: SUM over a VARCHAR/DATE column => validate_agg_col_types returns false (fall back).
    #[test]
    fn aggregate_falls_back_to_row_scan_for_sum_of_non_numeric() {
        let col_types_varchar = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];
        let sum_varchar = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("NAME".into()),
            arg_expr: None,
        }];
        assert!(
            !validate_agg_col_types(&sum_varchar, &col_types_varchar),
            "SUM over VARCHAR must fail validation (fall back to row scan)"
        );

        let col_types_date = vec![("EVENT_DATE".to_string(), "DATE".to_string())];
        let sum_date = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("EVENT_DATE".into()),
            arg_expr: None,
        }];
        assert!(
            !validate_agg_col_types(&sum_date, &col_types_date),
            "SUM over DATE must fail validation (fall back to row scan)"
        );
    }

    /// A grouped aggregate whose SUM targets a VARCHAR column must fall back to row
    /// scan (return None from detect_group_by_aggregates + validate_agg_col_types) —
    /// the same guard as the single-group path — rather than producing grouped scan SQL
    /// that would generate an opaque UDF error at execution time.
    #[test]
    fn grouped_aggregate_sum_over_varchar_falls_back_via_type_validation() {
        // Simulate the detection + validation sequence that handle_pushdown runs.
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("NAME"), false), // NAME is VARCHAR — invalid for SUM
            ],
        });

        // detect_group_by_aggregates must accept the shape (it doesn't know types).
        let detected = detect_group_by_aggregates(&req);
        assert!(
            detected.is_some(),
            "detect_group_by_aggregates must accept the shape: {req}"
        );
        let agg_plans = detected.unwrap().plans;

        // Validation with VARCHAR col_types must fail — triggering fall-back.
        let col_types = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ];
        assert!(
            !validate_agg_col_types(&agg_plans, &col_types),
            "validate_agg_col_types must fail for SUM over VARCHAR (fall back to row scan)"
        );

        // Confirm that a DATE column also fails for SUM.
        let col_types_date = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("NAME".to_string(), "DATE".to_string()),
        ];
        assert!(
            !validate_agg_col_types(&agg_plans, &col_types_date),
            "validate_agg_col_types must fail for SUM over DATE (fall back to row scan)"
        );

        // Confirm a numeric type passes (no fall back).
        let col_types_numeric = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("NAME".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        assert!(
            validate_agg_col_types(&agg_plans, &col_types_numeric),
            "validate_agg_col_types must pass for SUM over DOUBLE PRECISION"
        );
    }

    fn make_group_by_request(
        group_by: serde_json::Value,
        select_list: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": group_by,
            "selectList": select_list,
        })
    }

    /// Like `make_group_by_request`, but also carries `selectListDataTypes` so
    /// ordering + type-position assertions are possible (positional matching
    /// against the outer wrapper SELECT and group-key type resolution).
    fn make_group_by_request_with_types(
        group_by: serde_json::Value,
        select_list: serde_json::Value,
        select_list_data_types: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": group_by,
            "selectList": select_list,
            "selectListDataTypes": select_list_data_types,
        })
    }

    /// `MOD(<col>, <divisor>)` as a `function_scalar` node — renders to
    /// `("<COL>" % <divisor>)` via `render_expression`. Used to build the #33
    /// repro (`SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)`) and its
    /// interleaved/HAVING variants.
    fn mod_item(col: &str, divisor: i64) -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar",
            "name": "MOD",
            "arguments": [
                {"type": "column", "name": col},
                {"type": "literal_exactnumeric", "value": divisor},
            ],
        })
    }

    /// `UPPER(<col>)` as a `function_scalar` node — renders to `upper("<COL>")`
    /// via `render_expression`. Used to build all-expression multi-key GROUP BY
    /// tuples where every element (not just some) is an expression.
    fn upper_item(col: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar",
            "name": "UPPER",
            "arguments": [
                {"type": "column", "name": col},
            ],
        })
    }

    /// A DECIMAL `selectListDataTypes` entry, per the `exasol_type_from_json` shape.
    fn decimal_type(precision: u64, scale: u64) -> serde_json::Value {
        serde_json::json!({"type": "decimal", "precision": precision, "scale": scale})
    }

    /// Column reference in GROUP BY renders to a quoted identifier.
    #[test]
    fn detect_group_by_aggregates_column_key() {
        let req = make_group_by_request(
            serde_json::json!([{"type": "column", "name": "REGION"}]),
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                agg_item("COUNT", None, false),
            ]),
        );
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        let GroupedAggregateDetection {
            group_keys: keys,
            plans,
            ..
        } = result;
        assert_eq!(keys.len(), 1, "one group key");
        assert!(
            keys[0].contains("REGION"),
            "group key must reference REGION: {:?}",
            keys[0]
        );
        assert_eq!(plans.len(), 1, "one aggregate plan");
        assert_eq!(plans[0].kind, AggKind::Count);
    }

    /// Build a minimal grouped `ScanSpec` for the merge-SQL builder tests.
    fn grouped_spec(result: &GroupedAggregateDetection) -> ScanSpec {
        ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(result.plans.clone()),
                group_keys: Some(result.group_keys.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        }
    }

    /// A grouped aggregate whose request carries an `orderBy` on a group key but
    /// NO `limit` must still render an explicit final `ORDER BY` in its merge SQL:
    /// once `ORDER_BY_COLUMN` is advertised Exasol no longer re-sorts the grouped
    /// output, so a plain `GROUP BY … ORDER BY` must sort itself (add-topn-pushdown
    /// B6). The sort key is rendered as a POSITIONAL output ordinal so it sorts the
    /// type-cast output, not the lexicographic VARCHAR `GK_*` staging column.
    #[test]
    fn grouped_order_by_no_limit_renders_explicit_merge_order_by() {
        let mut req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "ID"}]),
            serde_json::json!([
                {"type": "column", "name": "ID"},
                agg_item("COUNT", None, false),
            ]),
            serde_json::json!([decimal_type(20, 0), decimal_type(20, 0)]),
        );
        // ORDER BY id ASC NULLS LAST, and deliberately NO "limit" key.
        req["orderBy"] = serde_json::json!([{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "ID"},
            "isAscending": true,
            "nullsLast": true,
        }]);

        let result = detect_group_by_aggregates(&req).expect("grouped aggregate");
        // The group key ID is output column 1 → positional ordinal, explicit dir+nulls.
        assert_eq!(
            build_grouped_order_by_clause(&req, &result),
            Some(GroupedOrderBy::Clause("1 ASC NULLS LAST".to_string())),
            "grouped ORDER BY must map the sort key to its 1-based output ordinal"
        );

        let group_key_types =
            group_key_exasol_types(&req, &result.group_keys, &result.select_items);
        let sql = build_grouped_aggregate_scan_sql(
            &grouped_spec(&result),
            &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
            &result.group_keys,
            &group_key_types,
            &result.plans,
            &[],
            &result.select_items,
            None,
            0,
            &[("ID".to_string(), "DECIMAL(20,0)".to_string())],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            Some("1 ASC NULLS LAST"),
        );
        assert!(
            sql.contains(" ORDER BY 1 ASC NULLS LAST"),
            "merge SQL must render the explicit final ORDER BY: {sql}"
        );
        // No LIMIT was requested, so none is rendered.
        assert!(!sql.contains("LIMIT"), "no LIMIT requested: {sql}");
    }

    /// An `ORDER BY` on an aggregate that IS among the detected select-list plans
    /// resolves to that aggregate's MERGED expression over the `PARTIAL_*` columns —
    /// the same rewrite, by the same `AggregatePlan`-equality match, the merged
    /// HAVING uses (issue #198). A group-key element mixed into the same `orderBy`
    /// still renders as its positional output ordinal, unchanged.
    #[test]
    fn grouped_order_by_select_list_aggregate_renders_merged_partial() {
        let mut req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "ID"}]),
            serde_json::json!([
                {"type": "column", "name": "ID"},
                agg_item("SUM", Some("AMOUNT"), false),
            ]),
            serde_json::json!([decimal_type(20, 0), decimal_type(36, 2)]),
        );
        req["orderBy"] = serde_json::json!([
            {
                "type": "order_by_element",
                "expression": agg_item("SUM", Some("AMOUNT"), false),
                "isAscending": false,
                "nullsLast": true,
            },
            {
                "type": "order_by_element",
                "expression": {"type": "column", "name": "ID"},
                "isAscending": true,
                "nullsLast": false,
            },
        ]);

        let detection = detect_group_by_aggregates(&req).expect("grouped aggregate");
        assert_eq!(
            build_grouped_order_by_clause(&req, &detection),
            Some(GroupedOrderBy::Clause(
                r#"SUM("PARTIAL_sum_0") DESC NULLS LAST, 1 ASC NULLS FIRST"#.to_string()
            )),
            "an aggregate sort key must render as its merged partial, a group key as its ordinal"
        );
    }

    /// An `ORDER BY` on an aggregate ABSENT from the detected plans has no
    /// `PARTIAL_*` column to merge over, and the adapter does not fabricate one:
    /// the resolution reports `Unresolvable`, which `classify_request_shape` turns
    /// into a `GroupByWrapper` route (issue #198).
    #[test]
    fn grouped_order_by_aggregate_absent_from_plans_is_unresolvable() {
        let mut req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "ID"}]),
            serde_json::json!([
                {"type": "column", "name": "ID"},
                agg_item("COUNT", None, false),
            ]),
            serde_json::json!([decimal_type(20, 0), decimal_type(20, 0)]),
        );
        req["orderBy"] = serde_json::json!([{
            "type": "order_by_element",
            "expression": agg_item("SUM", Some("AMOUNT"), false),
            "isAscending": false,
            "nullsLast": true,
        }]);

        let detection = detect_group_by_aggregates(&req).expect("grouped aggregate");
        assert_eq!(
            build_grouped_order_by_clause(&req, &detection),
            Some(GroupedOrderBy::Unresolvable),
            "an aggregate with no matching plan must not resolve to a fabricated partial"
        );
    }

    /// Scalar expression in GROUP BY (e.g., function_scalar YEAR) renders via render_expression.
    #[test]
    fn detect_group_by_aggregates_expression_key() {
        // A predicate_equal used as an expression key — render_expression can handle it.
        let req = make_group_by_request(
            serde_json::json!([{
                "type": "predicate_equal",
                "left": {"type": "column", "name": "STATUS"},
                "right": {"type": "literal_string", "value": "active"},
            }]),
            serde_json::json!([agg_item("SUM", Some("AMOUNT"), false),]),
        );
        let result = detect_group_by_aggregates(&req);
        // predicate_equal renders to (STATUS = 'active'), so it should succeed.
        assert!(result.is_some(), "renderable expression key must succeed");
        let GroupedAggregateDetection {
            group_keys: keys,
            plans,
            ..
        } = result.unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].contains("="), "rendered expression must contain =");
        assert_eq!(plans[0].kind, AggKind::Sum);
    }

    /// An unsupported expression in GROUP BY causes the whole function to return None.
    #[test]
    fn detect_group_by_unsupported_expression_falls_back() {
        let req = make_group_by_request(
            serde_json::json!([{"type": "fn_custom_unsupported", "name": "MYSTERY"}]),
            serde_json::json!([agg_item("COUNT", None, false)]),
        );
        assert!(
            detect_group_by_aggregates(&req).is_none(),
            "unsupported expression must fall back to None"
        );
    }

    /// Select list with a non-aggregate, non-column item causes fallback.
    #[test]
    fn detect_group_by_mixed_select_falls_back() {
        // function_scalar in selectList is not an aggregate and not a plain column.
        let req = make_group_by_request(
            serde_json::json!([{"type": "column", "name": "REGION"}]),
            serde_json::json!([
                {"type": "function_scalar", "name": "YEAR", "arguments": [{"type": "column", "name": "TS"}]},
                agg_item("COUNT", None, false),
            ]),
        );
        assert!(
            detect_group_by_aggregates(&req).is_none(),
            "non-aggregate non-column in selectList must fall back"
        );
    }

    /// Issue #52 regression guard (decision-log entry [4]): the exact composed
    /// `pushdownRequest` Exasol emits for
    /// `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM EVENTS GROUP BY id) t`
    /// — a real `groupBy` but a `selectList` of only a `literal_null` placeholder
    /// (Exasol's "count the groups" rewrite: the outer query needs only the
    /// per-group row count, not the inner values). Fed verbatim (including the
    /// `from`/`type`/`columnNr`/`tableName` fields the detection path ignores,
    /// to prove they don't perturb parsing) from the spike's captured JSON.
    ///
    /// Detection must preserve the GROUP BY (return `Some` with real group keys
    /// and NO aggregate plan) instead of falling back to a row scan — a row-scan
    /// fallback returns one row per source row, not per group, which is only
    /// accidentally correct when the group column happens to be unique (see
    /// decision-log entry [4]'s caveat). The rendered scan SQL must never
    /// reference a phantom `"NULL"` column identifier and must retain a real
    /// `GROUP BY` clause.
    #[test]
    fn composed_nested_aggregate_request_does_not_reference_phantom_column() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "from": { "name": "EVENTS", "type": "table" },
            "groupBy": [
                { "columnNr": 0, "name": "ID", "tableName": "EVENTS", "type": "column" }
            ],
            "selectList": [ { "type": "literal_null" } ],
            "selectListDataTypes": [ { "type": "BOOLEAN" } ],
            "type": "select"
        });
        let result = detect_group_by_aggregates(&req).expect(
            "composed literal-only selectList must preserve GROUP BY, not fall back to row scan",
        );
        assert_eq!(result.group_keys.len(), 1, "one group key from groupBy");
        assert!(
            result.group_keys[0].contains("ID"),
            "group key must reference ID: {:?}",
            result.group_keys[0]
        );
        assert!(
            result.plans.is_empty(),
            "a literal placeholder contributes no aggregate plan"
        );
        assert!(
            matches!(
                result.select_items.as_slice(),
                [GroupedSelectItem::Constant {
                    select_index: 0,
                    ..
                }]
            ),
            "the literal_null item must classify as a Constant: {:?}",
            result.select_items
        );

        // The generated grouped scan SQL must group by GK_0 and must never
        // reference a phantom "NULL" column identifier.
        let group_key_types =
            group_key_exasol_types(&req, &result.group_keys, &result.select_items);
        let sql = build_grouped_aggregate_scan_sql(
            &ScanSpec {
                common: CommonScanSpec {
                    aggregates: Some(result.plans.clone()),
                    group_keys: Some(result.group_keys.clone()),
                    storage: sample_storage(),
                    ..Default::default()
                },
                files: vec![],
            },
            &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
            &result.group_keys,
            &group_key_types,
            &result.plans,
            &[],
            &result.select_items,
            None,
            0,
            &[("ID".to_string(), "DECIMAL(20,0)".to_string())],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            None,
        );
        assert!(
            !sql.contains(r#""NULL""#),
            "grouped scan SQL must not reference a phantom \"NULL\" identifier: {sql}"
        );
        assert!(
            sql.contains(r#"GROUP BY "GK_0""#),
            "outer wrapper must group by GK_0 to yield one row per distinct group: {sql}"
        );
        // The constant placeholder projects a typed literal (declared BOOLEAN),
        // not an empty select list and not a bare-literal column reference.
        assert!(
            sql.contains("SELECT CAST(NULL AS BOOLEAN) FROM"),
            "outer wrapper must project the type-cast constant placeholder: {sql}"
        );
    }

    /// Code-review follow-up on issue #52: `literal_bool` was missing from the
    /// literal-type set used to classify grouped `selectList` constants (only
    /// `literal_null` and six other literal kinds were listed, and the
    /// renderer in `vs-expression` supports `literal_bool` — see
    /// `render_expression`). A boolean literal placeholder in a grouped
    /// selectList (e.g. `SELECT k, TRUE AS flag, COUNT(*) FROM t GROUP BY k`)
    /// used to fall through to the group-key-matching `_` arm, fail to match
    /// any group key, and abort the ENTIRE grouped-aggregate detection to
    /// `None` — exactly the bug class the `literal_null` case guards against,
    /// just for `literal_bool`. `LITERAL_SELECTLIST_TYPES` closes this gap.
    #[test]
    fn literal_bool_selectlist_item_classifies_as_constant_not_group_key() {
        let req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "ID"}]),
            serde_json::json!([
                {"type": "column", "name": "ID"},
                {"type": "literal_bool", "value": true},
                agg_item("COUNT", None, false),
            ]),
            serde_json::json!([
                decimal_type(20, 0),
                serde_json::json!({"type": "boolean"}),
                decimal_type(20, 0),
            ]),
        );
        let result = detect_group_by_aggregates(&req).expect(
            "a literal_bool selectList item must classify as Constant, not abort detection to None",
        );
        assert!(
            matches!(
                result.select_items[1],
                GroupedSelectItem::Constant {
                    select_index: 1,
                    ..
                }
            ),
            "the literal_bool item must classify as a Constant, not fall through \
             to the group-key arm: {:?}",
            result.select_items
        );
    }

    /// #33 repro: an aggregate placed before the single group key in the
    /// selectList must classify with `select_index` 0 for the aggregate and 1
    /// for the group key — the original ordinals, not a keys-first reorder.
    #[test]
    fn detect_group_by_aggregates_preserves_select_list_order() {
        // SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)
        let req = make_group_by_request(
            serde_json::json!([mod_item("ID", 4)]),
            serde_json::json!([agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)]),
        );
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(result.group_keys.len(), 1, "one group key");
        assert_eq!(result.plans.len(), 1, "one aggregate plan");
        assert_eq!(
            result.select_items,
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            "classification must preserve original select-list ordinals: {:?}",
            result.select_items
        );
    }

    /// Interleaved multi-key GROUP BY: `SELECT k1, SUM(score), k2 ... GROUP BY k1, k2`.
    /// Each classified item must carry its own selectList ordinal and the
    /// correct group-key slot (k1 → slot 0, k2 → slot 1), even though the
    /// aggregate sits between them in the select list.
    #[test]
    fn detect_group_by_aggregates_interleaved_multi_key_preserves_order() {
        let req = make_group_by_request(
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                {"type": "column", "name": "YEAR"},
            ]),
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("SCORE"), false),
                {"type": "column", "name": "YEAR"},
            ]),
        );
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(result.group_keys.len(), 2, "two group keys");
        assert_eq!(result.plans.len(), 1, "one aggregate plan");
        assert_eq!(
            result.select_items,
            vec![
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 1,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 1,
                    select_index: 2,
                },
            ],
            "classification must preserve interleaved ordinals: {:?}",
            result.select_items
        );
    }

    /// Expression group key placed after an aggregate:
    /// `SELECT COUNT(*), MOD(id,4) ... GROUP BY MOD(id,4)`.
    #[test]
    fn detect_group_by_aggregates_expr_key_after_agg_preserves_order() {
        let req = make_group_by_request(
            serde_json::json!([mod_item("ID", 4)]),
            serde_json::json!([agg_item("COUNT", None, false), mod_item("ID", 4)]),
        );
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(
            result.select_items,
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            "expression key after aggregate must classify by original ordinal: {:?}",
            result.select_items
        );
    }

    /// Aggregate-first GROUP BY with HAVING present: HAVING does not change
    /// selectList classification, but this exercises the same aggregate-first
    /// shape that flows into the HAVING-present outer-wrapper path.
    #[test]
    fn detect_group_by_aggregates_aggregate_first_with_having_preserves_order() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [mod_item("ID", 4)],
            "selectList": [agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)],
            "having": {
                "type": "predicate_greater",
                "left": agg_item("SUM", Some("SCORE"), false),
                "right": {"type": "literal_exactnumeric", "value": 100},
            },
        });
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(
            result.select_items,
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            "HAVING presence must not affect selectList classification order: {:?}",
            result.select_items
        );
    }

    /// All-expression multi-key GROUP BY: `SELECT MOD(id,4), UPPER(name), COUNT(*)
    /// ... GROUP BY MOD(id,4), UPPER(name)`. Every tuple element is an expression
    /// (none a plain column) and must still be detected, each rendered on its own,
    /// and each element must appear rendered individually (not merged/collapsed)
    /// in the SQL built from the detection. If one element of the tuple is
    /// untranslatable, the whole detection must fall back to `None` (full
    /// raw-scan fallback), not a partial/degraded pushdown.
    #[test]
    fn detect_group_by_all_expression_multi_key() {
        let req = make_group_by_request(
            serde_json::json!([mod_item("ID", 4), upper_item("NAME")]),
            serde_json::json!([
                mod_item("ID", 4),
                upper_item("NAME"),
                agg_item("COUNT", None, false),
            ]),
        );
        let result =
            detect_group_by_aggregates(&req).expect("all-expression multi-key must detect");
        assert_eq!(result.group_keys.len(), 2, "two expression group keys");
        assert!(
            result.group_keys[0].contains('%') && result.group_keys[0].contains('4'),
            "key 0 must render the MOD expression: {:?}",
            result.group_keys
        );
        assert!(
            result.group_keys[1].to_lowercase().contains("upper"),
            "key 1 must render the UPPER expression: {:?}",
            result.group_keys
        );
        assert_eq!(result.plans.len(), 1, "one aggregate plan");
        assert_eq!(
            result.select_items,
            vec![
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 1,
                    select_index: 1,
                },
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 2,
                },
            ],
            "each expression key must classify to its own slot: {:?}",
            result.select_items
        );

        // Each element must be rendered per-element (not merged) in the built SQL:
        // the per-shard scan spec's common blob carries both rendered fragments
        // verbatim, embedded in the SQL literal that drives the UDF call.
        let col_types: Vec<(String, String)> = vec![];
        let group_key_types = vec!["VARCHAR(2000000)".to_string(); 2];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(result.plans.clone()),
                group_keys: Some(result.group_keys.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &result.group_keys,
            &group_key_types,
            &result.plans,
            &aggregate_types,
            &result.select_items,
            None,
            0,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            None,
        );
        assert!(
            sql.contains("% 4"),
            "built SQL must carry the MOD key rendered on its own: {sql}"
        );
        assert!(
            sql.to_lowercase().contains("upper("),
            "built SQL must carry the UPPER key rendered on its own: {sql}"
        );
        assert!(
            sql.contains(r#""GK_0""#) && sql.contains(r#""GK_1""#),
            "built SQL must emit both group-key slots: {sql}"
        );

        // One untranslatable element in the tuple must collapse detection to None.
        let bad_req = make_group_by_request(
            serde_json::json!([mod_item("ID", 4), {"type": "fn_custom_unsupported", "name": "MYSTERY"}]),
            serde_json::json!([
                mod_item("ID", 4),
                {"type": "fn_custom_unsupported", "name": "MYSTERY"},
                agg_item("COUNT", None, false),
            ]),
        );
        assert!(
            detect_group_by_aggregates(&bad_req).is_none(),
            "one untranslatable tuple element must force full fallback to None"
        );
    }

    /// Helper: build grouped aggregate scan SQL.
    /// Keys-first classification: group keys at ordinals 0..m, aggregates after.
    fn keys_first_select_items(group_keys: usize, aggregates: usize) -> Vec<GroupedSelectItem> {
        let mut items = Vec::with_capacity(group_keys + aggregates);
        for slot in 0..group_keys {
            items.push(GroupedSelectItem::GroupKey {
                group_key_slot: slot,
                select_index: slot,
            });
        }
        for slot in 0..aggregates {
            items.push(GroupedSelectItem::Aggregate {
                plan_slot: slot,
                select_index: group_keys + slot,
            });
        }
        items
    }

    fn build_grouped_agg_sql(
        group_keys: Vec<String>,
        agg_plans: Vec<AggregatePlan>,
        files: Vec<String>,
        g: usize,
    ) -> String {
        let col_types: Vec<(String, String)> = vec![
            ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
            ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(agg_plans.clone()),
                group_keys: Some(group_keys.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let files_with_sizes: Vec<FileEntry> =
            files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
        let shards = crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, g);
        let select_items = keys_first_select_items(group_keys.len(), agg_plans.len());
        build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &group_keys,
            &[],
            &agg_plans,
            &[],
            &select_items,
            None,
            0,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            None,
        )
    }

    /// Grouped scan-driving SQL fans out via GROUP BY shard_key over G work units,
    /// serializing the common blob once and one files literal per shard.
    #[test]
    fn grouped_fan_out_common_once_files_per_shard() {
        // Two distinct files, forced onto two shards (2 nodes × factor 1).
        let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
        let g = shard_count(2, 1, files.len());
        let sql = build_grouped_agg_sql(
            vec!["\"REGION\"".into()],
            vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }],
            files,
            g,
        );
        assert!(
            !sql.contains("IPROC()"),
            "grouped SQL must NOT contain IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY shard_key"),
            "grouped SQL inner must GROUP BY shard_key: {sql}"
        );
        assert!(
            sql.contains("AS shards(shard_key, files)"),
            "grouped fan-out must alias the VALUES table as shards(shard_key, files): {sql}"
        );

        // Common blob (credentials + tuning) serialized once, not per shard.
        assert_eq!(
            sql.matches("http://minio:9000").count(),
            1,
            "grouped common blob (endpoint) must appear exactly once: {sql}"
        );
        assert_eq!(
            sql.matches("memory_pool_fraction").count(),
            1,
            "grouped common blob (tuning payload) must appear exactly once: {sql}"
        );

        // Each shard's file appears exactly once, in its own VALUES row.
        for file in ["f0.parquet", "f1.parquet"] {
            assert_eq!(
                sql.matches(file).count(),
                1,
                "grouped shard file {file} must appear exactly once: {sql}"
            );
        }
    }

    /// The `GROUP BY shard_key` fan-out lives INSIDE the distributor subquery, while
    /// the OUTER wrapper re-groups the per-shard partials on the user's group keys
    /// (`GROUP BY "GK_0"`) over the scalar scan (decision [5]/[7]). The two GROUP BYs
    /// are at different query levels: shard_key groups the fan-out `VALUES` rows for
    /// round-robin distribution; GK_* re-groups the partial groups every shard emits.
    #[test]
    fn grouped_group_by_shard_key_inside_distributor() {
        let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
        let g = shard_count(2, 1, files.len());
        let sql = build_grouped_agg_sql(
            vec!["\"REGION\"".into()],
            vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }],
            files,
            g,
        );

        // The distributor carries the shard_key fan-out.
        assert!(
            sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
            "the shard_key fan-out must live in the distributor subquery: {sql}"
        );
        // The outer wrapper re-groups on the user key staging column.
        assert!(
            sql.trim_end().ends_with(r#"GROUP BY "GK_0""#),
            "the outer wrapper must re-group on the user group key GK_0: {sql}"
        );
        // The shard_key GROUP BY is nested strictly BEFORE the outer GK_0 GROUP BY:
        // the distributor's grouping is not the outer one.
        let shard_gb = sql
            .find("GROUP BY shard_key")
            .expect("shard_key GROUP BY present");
        let gk_gb = sql
            .find(r#"GROUP BY "GK_0""#)
            .expect("GK_0 GROUP BY present");
        assert!(
            shard_gb < gk_gb,
            "shard_key GROUP BY (distributor) must precede the outer GK_0 GROUP BY: {sql}"
        );
        // No materializing SELECT * wrapper between the outer re-group and the scan.
        assert!(
            !sql.contains("SELECT * FROM ("),
            "grouped wrapper must not use a SELECT * materialization boundary: {sql}"
        );
    }

    /// Single-shard grouped: the outer re-group sits over a from-less scalar scan on
    /// literals — the distributor short-circuits (no `VALUES`, no shard_key grouping).
    #[test]
    fn grouped_single_shard_short_circuits_distributor() {
        let sql = build_grouped_agg_sql(
            vec!["\"REGION\"".into()],
            vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }],
            vec!["s3://w/only.parquet".into()],
            1,
        );

        assert!(
            !sql.contains("VALUES") && !sql.contains("shard_key"),
            "single-shard grouped must short-circuit the distributor: {sql}"
        );
        assert!(
            sql.contains(&format!("FROM (SELECT {SCAN_UDF_NAME}(")),
            "the outer re-group reads directly from the from-less scalar scan: {sql}"
        );
        assert!(
            sql.trim_end().ends_with(r#"GROUP BY "GK_0""#),
            "the outer wrapper still re-groups on the user group key GK_0: {sql}"
        );
    }

    /// LIMIT is NOT pushed into the shard scan for a grouped query. The shared common
    /// blob (arg 0) must not carry "limit"; only the outer wrapper may apply LIMIT.
    #[test]
    fn grouped_common_blob_has_no_limit() {
        let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
        let g = shard_count(1, 1, files.len());
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                limit: Some(100), // LIMIT should NOT appear inside the shard spec JSON
                aggregates: Some(vec![AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                    arg_expr: None,
                }]),
                group_keys: Some(vec!["\"REGION\"".into()]),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &["\"REGION\"".to_string()],
            &[],
            &[AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }],
            &[],
            &keys_first_select_items(1, 1),
            Some(100),
            0,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            None,
        );
        // The shared common blob (arg 0) is built once with limit = None, so it must
        // NOT carry a "limit" key — this is the structural LIMIT-exclusion invariant.
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("\"limit\""),
            "grouped common blob must NOT carry limit: {common}"
        );
        // The outer wrapper may still apply the final LIMIT.
        assert!(
            sql.contains("LIMIT 100"),
            "outer wrapper should still apply the final LIMIT: {sql}"
        );
    }

    /// A nonzero offset must never reach the per-shard fan-out spec: the common
    /// blob shared by every shard carries neither "limit" nor an "offset" key —
    /// there is no offset field on `CommonScanSpec` at all (design invariant: no
    /// `ScanSpec`/UDF wire change), so this also pins that no such field leaks into
    /// the shared JSON. The outer wrapper is the only place the offset renders
    /// (fix-191-order-by-offset).
    #[test]
    fn grouped_merge_offset_never_reaches_per_shard_spec() {
        let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
        let g = shard_count(1, 1, files.len());
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                limit: Some(100),
                aggregates: Some(vec![AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                    arg_expr: None,
                }]),
                group_keys: Some(vec!["\"REGION\"".into()]),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &["\"REGION\"".to_string()],
            &[],
            &[AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }],
            &[],
            &keys_first_select_items(1, 1),
            Some(100),
            3,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            Some("1 ASC NULLS LAST"),
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("\"limit\"") && !common.contains("\"offset\""),
            "grouped common blob must NOT carry limit or offset: {common}"
        );
        assert!(
            sql.contains("ORDER BY 1 ASC NULLS LAST LIMIT 100 OFFSET 3"),
            "outer wrapper applies the final ORDER BY ... LIMIT ... OFFSET: {sql}"
        );
    }

    /// Byte-identical requirement (fix-191-order-by-offset): a zero offset renders
    /// the exact pre-change ` LIMIT {n}` string with no OFFSET token, so every
    /// already-correct SQL-shape assertion for the grouped-agg path keeps passing
    /// unchanged.
    #[test]
    fn grouped_merge_zero_offset_is_byte_identical_to_bare_limit() {
        let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
        let g = shard_count(1, 1, files.len());
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                limit: Some(100),
                aggregates: Some(vec![AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                    arg_expr: None,
                }]),
                group_keys: Some(vec!["\"REGION\"".into()]),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &["\"REGION\"".to_string()],
            &[],
            &[AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }],
            &[],
            &keys_first_select_items(1, 1),
            Some(100),
            0,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            None,
        );
        assert!(
            sql.ends_with(" LIMIT 100"),
            "zero offset must render the bare pre-offset LIMIT clause: {sql}"
        );
        assert!(
            !sql.contains("OFFSET"),
            "zero offset must never render an OFFSET token: {sql}"
        );
    }

    /// The grouped merge renders `GROUP BY … ORDER BY … LIMIT n OFFSET m` in that
    /// exact clause order (fix-191-order-by-offset, capture rows 5-8):
    /// `render_limit_offset` is the shared seam every reachable wrapper calls, and
    /// this pins the grouped merge's wiring into it.
    #[test]
    fn grouped_merge_renders_limit_offset_in_clause_order() {
        let mut req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "ID"}]),
            serde_json::json!([
                {"type": "column", "name": "ID"},
                agg_item("COUNT", None, false),
            ]),
            serde_json::json!([decimal_type(20, 0), decimal_type(20, 0)]),
        );
        req["orderBy"] = serde_json::json!([{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "ID"},
            "isAscending": true,
            "nullsLast": true,
        }]);

        let result = detect_group_by_aggregates(&req).expect("grouped aggregate");
        let group_key_types =
            group_key_exasol_types(&req, &result.group_keys, &result.select_items);
        let sql = build_grouped_aggregate_scan_sql(
            &grouped_spec(&result),
            &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
            &result.group_keys,
            &group_key_types,
            &result.plans,
            &[],
            &result.select_items,
            Some(2),
            1,
            &[("ID".to_string(), "DECIMAL(20,0)".to_string())],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            Some("1 ASC NULLS LAST"),
        );
        assert!(
            sql.ends_with(" ORDER BY 1 ASC NULLS LAST LIMIT 2 OFFSET 1"),
            "merge SQL must render GROUP BY … ORDER BY … LIMIT n OFFSET m in that order: {sql}"
        );
        let group_by_pos = sql.find("GROUP BY").expect("must contain GROUP BY");
        let order_by_pos = sql.find(" ORDER BY").expect("must contain ORDER BY");
        let limit_pos = sql.find(" LIMIT").expect("must contain LIMIT");
        let offset_pos = sql.find(" OFFSET").expect("must contain OFFSET");
        assert!(
            group_by_pos < order_by_pos && order_by_pos < limit_pos && limit_pos < offset_pos,
            "clauses must appear in GROUP BY, ORDER BY, LIMIT, OFFSET order: {sql}"
        );
    }

    /// Grouped aggregate wrapper SQL re-groups partial results per user group key.
    #[test]
    fn grouped_aggregate_wrapper_sql_groups_by_user_key_cols() {
        let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
        let g = shard_count(2, 1, files.len());
        let sql = build_grouped_agg_sql(
            vec!["\"REGION\"".into(), "\"YEAR\"".into()],
            vec![
                AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                    arg_expr: None,
                },
                AggregatePlan {
                    kind: AggKind::Sum,
                    column: Some("AMOUNT".into()),
                    arg_expr: None,
                },
            ],
            files,
            g,
        );
        // Outer wrapper must GROUP BY GK_0, GK_1 (the group key columns).
        assert!(
            sql.contains("GK_0"),
            "wrapper SQL must reference GK_0: {sql}"
        );
        assert!(
            sql.contains("GK_1"),
            "wrapper SQL must reference GK_1: {sql}"
        );
        // Outer GROUP BY must merge partial aggregates.
        assert!(
            sql.contains("SUM("),
            "wrapper must contain SUM for merge: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "wrapper must reference PARTIAL_count_0: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_sum_1"),
            "wrapper must reference PARTIAL_sum_1: {sql}"
        );
        // Outer must have GROUP BY GK_0, GK_1.
        let outer_group_by = sql
            .rfind("GROUP BY")
            .expect("must have GROUP BY in outer wrapper");
        let outer_group_by_clause = &sql[outer_group_by..];
        assert!(
            outer_group_by_clause.contains("GK_0"),
            "outer GROUP BY must include GK_0: {outer_group_by_clause}"
        );
        assert!(
            outer_group_by_clause.contains("GK_1"),
            "outer GROUP BY must include GK_1: {outer_group_by_clause}"
        );
    }

    /// Extract the outer wrapper's SELECT list (between the leading `SELECT `
    /// and the `FROM (` that opens the fan-out subselect), split on the
    /// top-level commas of each column expression. Aggregate expressions and
    /// CAST(...) fragments never contain a bare `, ` outside of nested
    /// parens/quotes for the shapes used in these tests (SUM/COUNT merges and
    /// CAST("GK_i" AS ...)), so a paren-depth-aware split is sufficient.
    fn outer_select_items(sql: &str) -> Vec<String> {
        let from_pos = sql
            .find(" FROM (")
            .expect("must have outer FROM (: sql={sql}");
        let select_str = &sql["SELECT ".len()..from_pos];
        let mut items = Vec::new();
        let mut depth = 0i32;
        let mut current = String::new();
        for ch in select_str.chars() {
            match ch {
                '(' => {
                    depth += 1;
                    current.push(ch);
                }
                ')' => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 => {
                    items.push(current.trim().to_string());
                    current = String::new();
                }
                _ => current.push(ch),
            }
        }
        if !current.trim().is_empty() {
            items.push(current.trim().to_string());
        }
        items
    }

    /// Build grouped aggregate scan SQL with explicit (non-keys-first) `select_items`
    /// and declared group-key types, so ordering + CAST type can be asserted.
    fn build_grouped_agg_sql_with_select_items(
        group_keys: Vec<String>,
        group_key_types: Vec<String>,
        agg_plans: Vec<AggregatePlan>,
        aggregate_types: Vec<String>,
        select_items: Vec<GroupedSelectItem>,
        having: Option<&str>,
    ) -> String {
        let col_types: Vec<(String, String)> = vec![
            ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
            ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(agg_plans.clone()),
                group_keys: Some(group_keys.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
        build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &group_keys,
            &group_key_types,
            &agg_plans,
            &aggregate_types,
            &select_items,
            None,
            0,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            having,
            None,
        )
    }

    /// #33 repro: `SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)`.
    /// The outer wrapper SELECT must place the merged SUM at position 0 and
    /// the CAST'd group key at position 1 — matching the user's selectList
    /// order, not the inner fan-out's keys-first shape.
    #[test]
    fn grouped_wrapper_agg_before_key_ordering() {
        let sql = build_grouped_agg_sql_with_select_items(
            vec![r#"("ID" % 4)"#.to_string()],
            vec!["DECIMAL(9,0)".to_string()],
            vec![AggregatePlan {
                kind: AggKind::Sum,
                column: Some("SCORE".into()),
                arg_expr: None,
            }],
            vec!["DOUBLE PRECISION".to_string()],
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            None,
        );
        let items = outer_select_items(&sql);
        assert_eq!(
            items.len(),
            2,
            "outer SELECT must have exactly 2 items: {items:?}"
        );
        assert!(
            items[0].contains("PARTIAL_sum_0") && items[0].starts_with("CAST(SUM("),
            "position 0 must be the merged aggregate: {items:?}"
        );
        assert!(
            items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
            "position 1 must be the CAST'd group key with its declared type: {items:?}"
        );
    }

    /// Interleaved multi-key: `SELECT k1, SUM(score), k2 ... GROUP BY k1, k2`.
    /// Outer SELECT order must be [key0, aggregate, key1], matching selectList.
    #[test]
    fn grouped_wrapper_interleaved_multi_key_ordering() {
        let sql = build_grouped_agg_sql_with_select_items(
            vec![r#""REGION""#.to_string(), r#""YEAR""#.to_string()],
            vec!["VARCHAR(100)".to_string(), "DECIMAL(4,0)".to_string()],
            vec![AggregatePlan {
                kind: AggKind::Sum,
                column: Some("SCORE".into()),
                arg_expr: None,
            }],
            vec!["DOUBLE PRECISION".to_string()],
            vec![
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 1,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 1,
                    select_index: 2,
                },
            ],
            None,
        );
        let items = outer_select_items(&sql);
        assert_eq!(
            items.len(),
            3,
            "outer SELECT must have exactly 3 items: {items:?}"
        );
        assert!(
            items[0].starts_with("CAST(\"GK_0\" AS VARCHAR(100))"),
            "position 0 must be key0's CAST: {items:?}"
        );
        assert!(
            items[1].contains("PARTIAL_sum_0") && items[1].starts_with("CAST(SUM("),
            "position 1 must be the merged aggregate: {items:?}"
        );
        assert!(
            items[2].starts_with("CAST(\"GK_1\" AS DECIMAL(4,0))"),
            "position 2 must be key1's CAST: {items:?}"
        );
    }

    /// Expression group key after an aggregate: `SELECT COUNT(*), MOD(id,4) ...
    /// GROUP BY MOD(id,4)`. The key's declared type (DECIMAL, from
    /// selectListDataTypes at its own select_index) must be preserved — this
    /// is what stops the silent VARCHAR(2000000) fallback for #33 sub-case 3.
    #[test]
    fn grouped_wrapper_expr_key_after_agg_ordering() {
        let sql = build_grouped_agg_sql_with_select_items(
            vec![r#"("ID" % 4)"#.to_string()],
            vec!["DECIMAL(9,0)".to_string()],
            vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }],
            vec!["DECIMAL(18,0)".to_string()],
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            None,
        );
        let items = outer_select_items(&sql);
        assert_eq!(items.len(), 2, "outer SELECT must have 2 items: {items:?}");
        assert!(
            items[0].contains("PARTIAL_count_0") && items[0].starts_with("CAST(SUM("),
            "position 0 must be the merged COUNT: {items:?}"
        );
        assert!(
            items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
            "position 1 must be the CAST'd group key, not a VARCHAR fallback: {items:?}"
        );
    }

    /// Aggregate-first GROUP BY with HAVING: `SELECT SUM(score), MOD(id,4) ...
    /// GROUP BY MOD(id,4) HAVING SUM(score) > n`. Outer SELECT order must still
    /// follow selectList (aggregate first) and HAVING must be appended after
    /// GROUP BY, exercising the HAVING-present outer-wrapper path together with
    /// non-keys-first ordering.
    #[test]
    fn grouped_wrapper_agg_first_with_having_ordering() {
        let sql = build_grouped_agg_sql_with_select_items(
            vec![r#"("ID" % 4)"#.to_string()],
            vec!["DECIMAL(9,0)".to_string()],
            vec![AggregatePlan {
                kind: AggKind::Sum,
                column: Some("SCORE".into()),
                arg_expr: None,
            }],
            vec!["DOUBLE PRECISION".to_string()],
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            Some(r#"(SUM("PARTIAL_sum_0") > 100)"#),
        );
        let having_pos = sql.find("HAVING").expect("must contain HAVING: {sql}");
        let group_by_pos = sql.find("GROUP BY").expect("must contain GROUP BY: {sql}");
        assert!(
            having_pos > group_by_pos,
            "HAVING must appear after GROUP BY: {sql}"
        );
        let select_only = &sql[..group_by_pos];
        let items = outer_select_items(select_only);
        assert_eq!(items.len(), 2, "outer SELECT must have 2 items: {items:?}");
        assert!(
            items[0].contains("PARTIAL_sum_0") && items[0].starts_with("CAST(SUM("),
            "position 0 must be the merged aggregate even with HAVING present: {items:?}"
        );
        assert!(
            items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
            "position 1 must be the CAST'd group key even with HAVING present: {items:?}"
        );
    }

    /// `CASE WHEN <col> = 'R' THEN 1 ELSE 0 END` — the conditional-count inner
    /// expression wrapped by #82's ROUND(...) select item.
    fn case_flag_eq(col: &str, val: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": col},
                 "right": {"type": "literal_string", "value": val}}
            ],
            "results": [
                {"type": "literal_exactnumeric", "value": 1},
                {"type": "literal_exactnumeric", "value": 0}
            ]
        })
    }

    /// #82's scalar-over-aggregate select item:
    /// `ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG='R' THEN 1 ELSE 0 END) / COUNT(*), 2)`.
    fn round_pct_over_aggregates() -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar",
            "name": "ROUND",
            "arguments": [
                {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                    {"type": "function_scalar", "name": "MULT", "arguments": [
                        {"type": "literal_double", "value": 100.0},
                        agg_item_expr("SUM", case_flag_eq("L_RETURNFLAG", "R"), false)
                    ]},
                    agg_item("COUNT", None, false)
                ]},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        })
    }

    fn soa_col_types() -> Vec<(String, String)> {
        vec![
            ("L_RETURNFLAG".to_string(), "VARCHAR(1)".to_string()),
            ("L_QUANTITY".to_string(), "DECIMAL(36,2)".to_string()),
            (
                "L_EXTENDEDPRICE".to_string(),
                "DOUBLE PRECISION".to_string(),
            ),
        ]
    }

    /// Drive detection then the outer-wrapper builder with the detection outputs
    /// (plans + the plans-aligned `plan_types`), mirroring the production grouped
    /// branch of `handle_pushdown`.
    fn build_grouped_from_detection(req: &serde_json::Value) -> String {
        let d = detect_group_by_aggregates(req)
            .expect("must detect the grouped scalar-over-aggregate pushdown");
        let group_key_types = group_key_exasol_types(req, &d.group_keys, &d.select_items);
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(d.plans.clone()),
                group_keys: Some(d.group_keys.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        build_grouped_aggregate_scan_sql(
            &spec_template,
            &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
            &d.group_keys,
            &group_key_types,
            &d.plans,
            &d.plan_types,
            &d.select_items,
            None,
            0,
            &soa_col_types(),
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            None,
        )
    }

    /// Task 3.1: `detect_group_by_aggregates` over #82's select list (plus a bare
    /// `COUNT(*)` item) classifies the ROUND(...) item as `ScalarOverAggregate` and
    /// folds its inner `SUM(CASE …)` + `COUNT(*)` into the shared plan list — the
    /// nested `COUNT(*)` deduplicated against the bare `COUNT(*)` so there is exactly
    /// ONE count plan (one `PARTIAL_*` column).
    #[test]
    fn grouped_scalar_over_aggregate_detects_and_dedups_inner_aggregates() {
        let req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "L_RETURNFLAG"}]),
            serde_json::json!([
                {"type": "column", "name": "L_RETURNFLAG"},
                agg_item("SUM", Some("L_QUANTITY"), false),
                agg_item("AVG", Some("L_EXTENDEDPRICE"), false),
                round_pct_over_aggregates(),
                agg_item("COUNT", None, false),
            ]),
            serde_json::json!([
                serde_json::json!({"type": "varchar", "size": 1}),
                decimal_type(36, 2),
                serde_json::json!({"type": "double"}),
                decimal_type(5, 2),
                decimal_type(18, 0),
            ]),
        );
        let d =
            detect_group_by_aggregates(&req).expect("must detect grouped scalar-over-aggregate");

        // The ROUND item is classified as a scalar-over-aggregate at its own ordinal,
        // carrying its own declared type.
        assert!(
            matches!(
                &d.select_items[3],
                GroupedSelectItem::ScalarOverAggregate {
                    select_index: 3,
                    declared_type,
                    ..
                } if declared_type == "DECIMAL(5,2)"
            ),
            "item 3 must be a ScalarOverAggregate with its declared type: {:?}",
            d.select_items[3]
        );

        // Plans: SUM(L_QUANTITY), AVG(L_EXTENDEDPRICE), SUM(CASE …), COUNT(*) — the
        // nested COUNT(*) and the bare COUNT(*) collapse to ONE plan.
        assert_eq!(
            d.plans.len(),
            4,
            "inner SUM(CASE) + COUNT(*) fold in; the two COUNT(*) dedup to one: {:?}",
            d.plans
        );
        let count_plans = d
            .plans
            .iter()
            .filter(|p| matches!(p.kind, AggKind::Count | AggKind::CountCol))
            .count();
        assert_eq!(
            count_plans, 1,
            "the shared COUNT(*) must be a single plan: {:?}",
            d.plans
        );

        // The bare COUNT(*) select item (index 4) points at the SAME slot the nested
        // COUNT(*) folded into.
        let count_slot = d
            .plans
            .iter()
            .position(|p| matches!(p.kind, AggKind::Count | AggKind::CountCol))
            .unwrap();
        assert!(
            matches!(
                d.select_items[4],
                GroupedSelectItem::Aggregate { plan_slot, select_index: 4 } if plan_slot == count_slot
            ),
            "the bare COUNT(*) must reuse the shared count slot {count_slot}: {:?}",
            d.select_items[4]
        );
    }

    /// Task 3.2: the outer wrapper renders the scalar-over-aggregate column over the
    /// MERGED partials (`ROUND(… SUM("PARTIAL_*") / SUM("PARTIAL_*") …)`), cast to its
    /// declared type, with NO source-column reference; the outer SELECT column count
    /// equals the `selectList` length.
    #[test]
    fn grouped_scalar_over_aggregate_renders_merged_partials() {
        let req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "L_RETURNFLAG"}]),
            serde_json::json!([
                {"type": "column", "name": "L_RETURNFLAG"},
                agg_item("SUM", Some("L_QUANTITY"), false),
                agg_item("AVG", Some("L_EXTENDEDPRICE"), false),
                round_pct_over_aggregates(),
            ]),
            serde_json::json!([
                serde_json::json!({"type": "varchar", "size": 1}),
                decimal_type(36, 2),
                serde_json::json!({"type": "double"}),
                decimal_type(5, 2),
            ]),
        );
        let sql = build_grouped_from_detection(&req);
        let items = outer_select_items(&sql);
        assert_eq!(
            items.len(),
            4,
            "outer SELECT must have one item per selectList item: {items:?}"
        );

        let soa = &items[3];
        assert!(
            soa.contains("PARTIAL_"),
            "wrapper item must be over merged partials: {soa}"
        );
        assert!(
            soa.contains("SUM(\"PARTIAL_") && soa.contains("ROUND("),
            "wrapper must render ROUND over merged SUM(PARTIAL_*) partials: {soa}"
        );
        assert!(
            soa.starts_with("CAST(") && soa.contains("DECIMAL(5,2)"),
            "wrapper item must be CAST to its declared type at its own ordinal: {soa}"
        );
        // The nested aggregates' argument structure (the CASE, and every source
        // column) is subsumed into the PARTIAL_* rewrite — the outer wrapper exposes
        // only GK_*/PARTIAL_* columns.
        assert!(
            !soa.contains("CASE"),
            "the CASE must be folded into a PARTIAL_* column: {soa}"
        );
        assert!(
            !soa.contains("L_RETURNFLAG") && !soa.contains("L_QUANTITY"),
            "wrapper item must not reference any source column: {soa}"
        );
    }

    /// Task 3.3: a scalar-over-aggregate placed BEFORE the group key and a plain
    /// aggregate yields outer SELECT items in `selectList` order, each cast from
    /// `selectListDataTypes` at its own ordinal.
    #[test]
    fn grouped_scalar_over_aggregate_preserves_selectlist_order() {
        let req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "L_RETURNFLAG"}]),
            serde_json::json!([
                round_pct_over_aggregates(),
                {"type": "column", "name": "L_RETURNFLAG"},
                agg_item("SUM", Some("L_QUANTITY"), false),
            ]),
            serde_json::json!([
                decimal_type(5, 2),
                serde_json::json!({"type": "varchar", "size": 1}),
                decimal_type(36, 2),
            ]),
        );
        let sql = build_grouped_from_detection(&req);
        let items = outer_select_items(&sql);
        assert_eq!(
            items.len(),
            3,
            "outer SELECT must have 3 items in selectList order: {items:?}"
        );
        assert!(
            items[0].starts_with("CAST(")
                && items[0].contains("ROUND(")
                && items[0].contains("DECIMAL(5,2)"),
            "position 0 must be the scalar-over-aggregate, cast to its own type: {items:?}"
        );
        assert!(
            items[1].starts_with("CAST(\"GK_0\" AS VARCHAR(1))"),
            "position 1 must be the CAST'd group key at its own ordinal: {items:?}"
        );
        assert!(
            items[2].starts_with("CAST(SUM(\"PARTIAL_") && items[2].contains("DECIMAL(36,2)"),
            "position 2 must be the merged plain aggregate, cast to its own type: {items:?}"
        );
    }

    /// Task 3.4: a grouped request whose scalar-over-aggregate wraps a
    /// `COUNT(DISTINCT …)` (undecomposable) declines grouped detection and routes to
    /// the qualified single-table wrapper — `SELECT <selectList> FROM (<raw scan>) AS
    /// "LHS_T0" GROUP BY …` with a `selectList`-matching column count — NOT a bare
    /// `SELECT * FROM (…)` row scan (the `04000` bug).
    #[test]
    fn grouped_undecomposable_falls_back_to_qualified_wrapper() {
        let pushdown_req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "L_RETURNFLAG", "tableName": "LINEITEM"}],
            "selectList": [
                {"type": "column", "name": "L_RETURNFLAG", "tableName": "LINEITEM"},
                {"type": "function_scalar", "name": "ROUND", "arguments": [
                    {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                        agg_item_expr("SUM", serde_json::json!({"type": "column", "name": "X", "tableName": "LINEITEM"}), false),
                        agg_item_expr("COUNT", serde_json::json!({"type": "column", "name": "Y", "tableName": "LINEITEM"}), true)
                    ]},
                    {"type": "literal_exactnumeric", "value": 2}
                ]}
            ],
            "selectListDataTypes": [
                serde_json::json!({"type": "varchar", "size": 1}),
                decimal_type(5, 2),
            ],
        });

        // The COUNT(DISTINCT) inner aggregate is undecomposable → detection declines.
        assert!(
            detect_group_by_aggregates(&pushdown_req).is_none(),
            "a nested COUNT(DISTINCT) must decline the grouped partial/merge path"
        );

        let request = serde_json::json!({
            "involvedTables": [{"name": "LINEITEM", "columns": [
                {"name": "L_RETURNFLAG", "dataType": {"type": "varchar", "size": 1}},
                {"name": "X", "dataType": {"type": "double"}},
                {"name": "Y", "dataType": {"type": "double"}},
            ]}]
        });
        let all_cols = extract_all_column_types(&request);
        // The shared referenced-column helper (issue #160) narrows the inner scan to
        // only the columns the wrapper references — here L_RETURNFLAG (GROUP BY +
        // select) and X, Y (nested inside the SUM/COUNT aggregate arguments), which is
        // the whole table, so the wrapper shape is identical to the old full-row scan.
        let (proj_cols, proj_types) = referenced_column_projection(&pushdown_req, &all_cols);
        let fan_out_spec = ScanSpec {
            common: CommonScanSpec {
                projection: proj_cols,
                emit_exa_types: proj_types,
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let sql = build_qualified_single_table_fallback_sql(
            &request,
            &pushdown_req,
            &fan_out_spec,
            &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
        )
        .expect("qualified fallback must build");

        assert!(
            !sql.starts_with("SELECT * FROM"),
            "fallback must NOT be a bare row scan (the 04000 bug): {sql}"
        );
        assert!(
            sql.contains(" GROUP BY "),
            "fallback must render the GROUP BY: {sql}"
        );
        assert!(
            sql.contains("FROM (") && sql.contains("AS \"LHS_T0\""),
            "fallback must wrap one aliased raw fan-out subquery: {sql}"
        );
        assert!(
            sql.contains("COUNT(DISTINCT"),
            "the undecomposable aggregate is rendered verbatim for Exasol to compute: {sql}"
        );
        // The FIRST ` FROM (` is the outer wrapper's (the fan-out subquery's own
        // FROM comes later), so `outer_select_items` extracts the wrapper's SELECT.
        let items = outer_select_items(&sql);
        assert_eq!(
            items.len(),
            2,
            "the wrapper must return exactly the selectList columns, not a full row: {items:?}"
        );
    }

    /// A HAVING `SUM(score) > literal` node built as Exasol sends it (a
    /// `predicate_greater` whose `left` is a `function_aggregate`) must render
    /// against the MERGE decomposition: the aggregate reference becomes the
    /// merged partial expression `SUM("PARTIAL_sum_0")`, NOT the source column
    /// `SUM("SCORE")` (which does not exist in the outer wrapper). This is the
    /// #33 HAVING repro (`... GROUP BY MOD(id,4) HAVING SUM(score) > 250`).
    #[test]
    fn render_having_over_merge_rewrites_aggregate_to_partial() {
        let having = serde_json::json!({
            "type": "predicate_greater",
            "left": agg_item("SUM", Some("SCORE"), false),
            "right": {"type": "literal_exactnumeric", "value": 250},
        });
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
            arg_expr: None,
        }];
        let rendered = render_having_over_merge(&having, &plans)
            .expect("HAVING over a known aggregate must render");
        assert_eq!(
            rendered, r#"(SUM("PARTIAL_sum_0") > 250)"#,
            "HAVING must reference the merged partial, not the source column: {rendered}"
        );
        assert!(
            !rendered.contains(r#""SCORE""#) && !rendered.contains("SUM(\"SCORE\")"),
            "HAVING must NOT reference the source column SCORE: {rendered}"
        );
    }

    /// The full outer-wrapper SQL for the #33 HAVING repro must carry the merged
    /// HAVING `SUM("PARTIAL_sum_0") > 250` and must not reference the source
    /// `SCORE` column in the HAVING clause.
    #[test]
    fn grouped_wrapper_having_over_aggregate_uses_merge_expression() {
        let req = make_group_by_request_with_types(
            serde_json::json!([mod_item("ID", 4)]),
            serde_json::json!([agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)]),
            serde_json::json!([
                {"type": "double"},
                decimal_type(9, 0),
            ]),
        );
        let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        let group_key_types =
            group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);
        let aggregate_types = aggregate_exasol_types(&req);

        let having_node = serde_json::json!({
            "type": "predicate_greater",
            "left": agg_item("SUM", Some("SCORE"), false),
            "right": {"type": "literal_exactnumeric", "value": 250},
        });
        let having = render_having_over_merge(&having_node, &detection.plans)
            .expect("HAVING must render over the merge decomposition");

        let col_types: Vec<(String, String)> =
            vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(detection.plans.clone()),
                group_keys: Some(detection.group_keys.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &detection.group_keys,
            &group_key_types,
            &detection.plans,
            &aggregate_types,
            &detection.select_items,
            None,
            0,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            Some(&having),
            None,
        );
        let having_pos = sql.find("HAVING").expect("must contain HAVING");
        let having_clause = &sql[having_pos..];
        assert!(
            having_clause.contains(r#"SUM("PARTIAL_sum_0") > 250"#),
            "HAVING clause must use the merge expression: {having_clause}"
        );
        assert!(
            !having_clause.contains(r#""SCORE""#) && !having_clause.contains("SUM(\"SCORE\")"),
            "HAVING clause must NOT reference the source SCORE column: {having_clause}"
        );
    }

    /// A HAVING referencing an aggregate that is NOT present among the plans
    /// (e.g. `COUNT(*)` when only `SUM(score)` was projected) cannot be merged,
    /// so `render_having_over_merge` returns None — the signal for
    /// `classify_request_shape` to route the request to `RequestShape::GroupByWrapper`
    /// rather than drop the HAVING.
    #[test]
    fn render_having_over_merge_declines_unknown_aggregate() {
        let having = serde_json::json!({
            "type": "predicate_greater",
            "left": agg_item("COUNT", None, false),
            "right": {"type": "literal_exactnumeric", "value": 10},
        });
        // Only SUM(score) was projected — COUNT(*) has no matching plan.
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
            arg_expr: None,
        }];
        assert!(
            render_having_over_merge(&having, &plans).is_none(),
            "HAVING over an aggregate absent from the plans must not render"
        );
    }

    /// End-to-end wiring: `detect_group_by_aggregates`'s classification output
    /// feeds directly into `build_grouped_aggregate_scan_sql` and the outer
    /// wrapper SELECT follows the original selectList order (#33 repro, driven
    /// through both functions together rather than a hand-built select_items).
    #[test]
    fn grouped_wrapper_outer_select_follows_select_list_order() {
        let req = make_group_by_request_with_types(
            serde_json::json!([mod_item("ID", 4)]),
            serde_json::json!([agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)]),
            serde_json::json!([
                {"type": "double"},
                decimal_type(9, 0),
            ]),
        );
        let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        let group_key_types =
            group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);
        let aggregate_types = aggregate_exasol_types(&req);

        let col_types: Vec<(String, String)> =
            vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(detection.plans.clone()),
                group_keys: Some(detection.group_keys.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &detection.group_keys,
            &group_key_types,
            &detection.plans,
            &aggregate_types,
            &detection.select_items,
            None,
            0,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            None,
        );

        let items = outer_select_items(&sql);
        assert_eq!(items.len(), 2, "outer SELECT must have 2 items: {items:?}");
        assert!(
            items[0].contains("PARTIAL_sum_0") && items[0].starts_with("CAST(SUM("),
            "position 0 must be the merged SUM (selectList order): {items:?}"
        );
        assert!(
            items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
            "position 1 must be the CAST'd group key with its declared type: {items:?}"
        );
    }

    /// Multi-key grouped SQL build with HAVING and LIMIT: `SELECT REGION,
    /// SUM(score), MOD(id,4) ... GROUP BY REGION, MOD(id,4) HAVING SUM(score) >
    /// 100 LIMIT 2`. HAVING and LIMIT must be placed ONLY in the outer wrapper —
    /// never in the per-shard partial scan, which must emit every partial group
    /// from every shard for the outer wrapper to merge and filter correctly.
    #[test]
    fn grouped_wrapper_multi_key_having_and_limit_outer_only() {
        let req = make_group_by_request_with_types(
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                mod_item("ID", 4),
            ]),
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("SCORE"), false),
                mod_item("ID", 4),
            ]),
            serde_json::json!([
                {"type": "varchar", "size": 100},
                {"type": "double"},
                decimal_type(9, 0),
            ]),
        );
        let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(detection.group_keys.len(), 2, "two group keys");
        let group_key_types =
            group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);
        let aggregate_types = aggregate_exasol_types(&req);

        let having_node = serde_json::json!({
            "type": "predicate_greater",
            "left": agg_item("SUM", Some("SCORE"), false),
            "right": {"type": "literal_exactnumeric", "value": 100},
        });
        let having = render_having_over_merge(&having_node, &detection.plans)
            .expect("HAVING must render over the merge decomposition");

        let col_types: Vec<(String, String)> =
            vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(detection.plans.clone()),
                group_keys: Some(detection.group_keys.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        // Multiple shards so the inner scan is a real `GROUP BY shard_key` fan-out,
        // not the single-shard direct-call shortcut.
        let shards = vec![
            vec![("s3://wh/f0.parquet".to_string(), 1u64)],
            vec![("s3://wh/f1.parquet".to_string(), 1u64)],
        ];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &detection.group_keys,
            &group_key_types,
            &detection.plans,
            &aggregate_types,
            &detection.select_items,
            Some(2),
            0,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            Some(&having),
            None,
        );

        // The per-shard partial scan ends at "GROUP BY shard_key"; everything up to
        // and including that point must carry neither HAVING nor LIMIT.
        let shard_group_end = sql
            .find("GROUP BY shard_key")
            .map(|i| i + "GROUP BY shard_key".len())
            .unwrap_or_else(|| panic!("must contain the inner per-shard fan-out: {sql}"));
        let inner_part = &sql[..shard_group_end];
        assert!(
            !inner_part.contains("HAVING"),
            "HAVING must not appear in the per-shard partial scan: {inner_part}"
        );
        assert!(
            !inner_part.contains("LIMIT"),
            "LIMIT must not appear in the per-shard partial scan: {inner_part}"
        );

        // Everything after the per-shard scan is the outer wrapper: it must carry
        // its own multi-key GROUP BY, then HAVING, then LIMIT, in that order.
        let outer_part = &sql[shard_group_end..];
        let outer_group_by_pos = outer_part
            .find("GROUP BY")
            .unwrap_or_else(|| panic!("outer wrapper must have its own GROUP BY: {outer_part}"));
        assert!(
            outer_part.contains(r#""GK_0""#) && outer_part.contains(r#""GK_1""#),
            "outer GROUP BY must reference both group-key slots: {outer_part}"
        );
        let having_pos = outer_part
            .find("HAVING")
            .unwrap_or_else(|| panic!("HAVING must appear in the outer wrapper: {outer_part}"));
        let limit_pos = outer_part
            .find("LIMIT 2")
            .unwrap_or_else(|| panic!("LIMIT must appear in the outer wrapper: {outer_part}"));
        assert!(
            outer_group_by_pos < having_pos,
            "outer GROUP BY must precede HAVING: {outer_part}"
        );
        assert!(
            having_pos < limit_pos,
            "HAVING must precede LIMIT in the outer wrapper: {outer_part}"
        );
    }

    /// An expression group key whose `groupBy` and `selectList` renderings
    /// differ only by whitespace/casing must still resolve its declared type
    /// by index (via `select_items`), not by comparing rendered SQL strings —
    /// which would silently fall back to VARCHAR(2000000) on any drift.
    #[test]
    fn group_key_type_resolved_by_index_not_string_match() {
        // groupBy renders "(\"ID\" % 4)" (see MOD rendering); simulate a
        // whitespace/casing-drifted selectList rendering by using a
        // hand-built classification whose select_index points at a
        // selectListDataTypes slot the rendered-string form would never find.
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [mod_item("ID", 4)],
            "selectList": [
                agg_item("SUM", Some("SCORE"), false),
                mod_item("ID", 4),
            ],
            "selectListDataTypes": [
                {"type": "double"},
                decimal_type(9, 0),
            ],
        });
        let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");

        // Sanity: the real detection path already resolves this correctly by
        // index. Now prove the mechanism is index-based, not string-based, by
        // building a classification where the rendered groupBy fragment would
        // NOT string-match the (hypothetically drifted) selectList rendering,
        // yet the index-based lookup still finds DECIMAL(9,0) because it reads
        // selectListDataTypes[select_index] directly.
        let group_keys = vec![r#"("id" % 4)"#.to_string()]; // lowercase drift vs GK render
        let select_items = detection.select_items.clone();
        let types = group_key_exasol_types(&req, &group_keys, &select_items);

        assert_eq!(
            types,
            vec!["DECIMAL(9,0)".to_string()],
            "type must resolve via select_index, not via string-matching the (drifted) \
             rendered group key: {types:?}"
        );
    }

    /// Mixed-type multi-key GROUP BY: `SELECT REGION, MOD(id,4), COUNT(*) ...
    /// GROUP BY REGION, MOD(id,4)`. `REGION` is a plain column declared VARCHAR;
    /// `MOD(id,4)` is an expression declared DECIMAL. Each `GK_{i}` must resolve
    /// its own declared type by its own `selectList` index — a shared/defaulted
    /// VARCHAR for both would silently lose the DECIMAL key's real type.
    #[test]
    fn group_key_types_multi_key_mixed_types() {
        let req = make_group_by_request_with_types(
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                mod_item("ID", 4),
            ]),
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                mod_item("ID", 4),
                agg_item("COUNT", None, false),
            ]),
            serde_json::json!([
                {"type": "varchar", "size": 100},
                decimal_type(9, 0),
                decimal_type(18, 0),
            ]),
        );
        let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(detection.group_keys.len(), 2, "two group keys");

        let types = group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);

        assert_eq!(types.len(), 2, "one declared type per group key");
        assert_eq!(
            types[0], "VARCHAR(100)",
            "the REGION key must resolve its own VARCHAR type, at its own select index: {types:?}"
        );
        assert_eq!(
            types[1], "DECIMAL(9,0)",
            "the MOD(id,4) key must resolve its own DECIMAL type, not a shared/defaulted \
             VARCHAR: {types:?}"
        );
    }

    /// aggregationType missing or not "group_by" returns None.
    #[test]
    fn detect_group_by_aggregates_no_group_by_type_returns_none() {
        // No aggregationType.
        let req1 = serde_json::json!({
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [agg_item("COUNT", None, false)],
        });
        assert!(detect_group_by_aggregates(&req1).is_none());

        // aggregationType is "single_group".
        let req2 = serde_json::json!({
            "aggregationType": "single_group",
            "selectList": [agg_item("COUNT", None, false)],
        });
        assert!(detect_group_by_aggregates(&req2).is_none());
    }

    /// Empty groupBy array returns None.
    #[test]
    fn detect_group_by_aggregates_empty_group_by_returns_none() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [],
            "selectList": [agg_item("SUM", Some("AMOUNT"), false)],
        });
        assert!(detect_group_by_aggregates(&req).is_none());
    }

    /// partial_emits_items produces 3 columns for stat aggregates.
    #[test]
    fn stat_aggregate_emits_three_partial_columns() {
        for kind in &[
            AggKind::VarPop,
            AggKind::VarSamp,
            AggKind::StddevPop,
            AggKind::StddevSamp,
        ] {
            let plans = vec![AggregatePlan {
                kind: kind.clone(),
                column: Some("SCORE".into()),
                arg_expr: None,
            }];
            let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
            let items = partial_emits_items(&plans, &col_types, &[]);
            assert_eq!(
                items.len(),
                3,
                "{kind:?} must emit 3 partial columns, got: {items:?}"
            );
            assert!(
                items[0].contains("PARTIAL_stat_cnt_0"),
                "first column must be cnt: {items:?}"
            );
            assert!(
                items[1].contains("PARTIAL_stat_sum_0"),
                "second column must be sum: {items:?}"
            );
            assert!(
                items[2].contains("PARTIAL_stat_sumsq_0"),
                "third column must be sumsq: {items:?}"
            );
        }
    }

    /// The scan's partial SELECT list and the adapter's `EMITS` clause name the
    /// same partial columns, in the same order, for every `AggKind`.
    ///
    /// The two lists are built in different modules and are otherwise only
    /// validated against each other at query time inside Exasol, where a
    /// mismatch surfaces as a wrong value or an `EMITS` arity error rather than
    /// as a test failure. The variant list below is an explicit literal: a
    /// variant added later that it omits is caught by the compile error
    /// `AggKind::partial_columns` raises, not here, so this test asserts
    /// alignment and never doubles as an exhaustiveness check it cannot enforce.
    #[test]
    fn scan_select_list_and_emits_agree_per_agg_kind() {
        /// Every `PARTIAL_…` name in `text`, in order of appearance. Both sides
        /// terminate the name with a double quote — the scan as
        /// `AS "PARTIAL_…"`, the `EMITS` item as `"PARTIAL_…" <type>`.
        fn partial_names_in(text: &str) -> Vec<String> {
            let mut names = Vec::new();
            let mut rest = text;
            while let Some(start) = rest.find("PARTIAL_") {
                let tail = &rest[start..];
                let end = tail
                    .find('"')
                    .expect("a PARTIAL_ name is always double-quote terminated");
                names.push(tail[..end].to_string());
                rest = &tail[end..];
            }
            names
        }

        let all_kinds = [
            AggKind::Count,
            AggKind::CountCol,
            AggKind::Sum,
            AggKind::Min,
            AggKind::Max,
            AggKind::Avg,
            AggKind::VarPop,
            AggKind::VarSamp,
            AggKind::StddevPop,
            AggKind::StddevSamp,
        ];
        let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];

        let plan_for = |kind: &AggKind| AggregatePlan {
            kind: kind.clone(),
            column: match kind {
                AggKind::Count => None,
                _ => Some("SCORE".to_string()),
            },
            arg_expr: None,
        };

        for kind in &all_kinds {
            let plans = vec![plan_for(kind)];
            let scan_names =
                partial_names_in(&crate::scan::build_partial_agg_sql(&plans, "aliased"));
            let emits_names =
                partial_names_in(&partial_emits_items(&plans, &col_types, &[]).join(", "));
            assert_eq!(
                scan_names, emits_names,
                "{kind:?}: scan SELECT list and EMITS clause disagree"
            );
        }

        // The same agreement under mixed arities, where a plan ordinal and a
        // column ordinal diverge — the shape a per-kind check cannot reach.
        let mixed: Vec<AggregatePlan> = all_kinds.iter().map(plan_for).collect();
        assert_eq!(
            partial_names_in(&crate::scan::build_partial_agg_sql(&mixed, "aliased")),
            partial_names_in(&partial_emits_items(&mixed, &col_types, &[]).join(", ")),
            "mixed-arity plan list: scan SELECT list and EMITS clause disagree"
        );
    }

    /// merge_select_items produces the correct reconstruction SQL for VAR_POP.
    #[test]
    fn var_pop_merge_formula_divides_by_n() {
        let plans = vec![AggregatePlan {
            kind: AggKind::VarPop,
            column: Some("X".into()),
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must contain NULLIF(..., 0) guard on the count
        assert!(
            sql.contains("NULLIF"),
            "var_pop merge must guard zero count: {sql}"
        );
        // Must NOT divide by (count - 1)
        assert!(
            !sql.contains("- 1"),
            "var_pop must not subtract 1 from count: {sql}"
        );
    }

    /// merge_select_items for VAR_SAMP divides by N-1 and guards N<=1 → NULL.
    #[test]
    fn var_samp_merge_formula_divides_by_n_minus_1() {
        let plans = vec![AggregatePlan {
            kind: AggKind::VarSamp,
            column: Some("X".into()),
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must use CASE WHEN … <= 1 THEN NULL to guard count<=1 → NULL.
        // Checking both `<= 1` and `CASE` ensures the N-1 sample divisor guard
        // is specifically present — not just any CASE or NULLIF in the expression.
        assert!(
            sql.contains("<= 1"),
            "var_samp merge must guard count<=1 with '<= 1': {sql}"
        );
        assert!(
            sql.contains("CASE"),
            "var_samp merge must use CASE for N<=1 guard: {sql}"
        );
    }

    /// STDDEV_POP merge formula wraps variance in SQRT.
    #[test]
    fn stddev_pop_merge_formula_uses_sqrt() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevPop,
            column: Some("X".into()),
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans).join(", ");
        assert!(sql.contains("SQRT("), "stddev_pop must use SQRT: {sql}");
        assert!(
            !sql.contains("- 1"),
            "stddev_pop must not subtract 1: {sql}"
        );
    }

    /// STDDEV_SAMP merge formula wraps variance-samp in SQRT.
    #[test]
    fn stddev_samp_merge_formula_uses_sqrt_and_n_minus_1() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevSamp,
            column: Some("X".into()),
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans).join(", ");
        assert!(sql.contains("SQRT("), "stddev_samp must use SQRT: {sql}");
        // N-1 guard: removing the N<=1 CASE would break this assertion.
        assert!(
            sql.contains("<= 1"),
            "stddev_samp must guard N<=1 (sample divisor): {sql}"
        );
        assert!(
            sql.contains("CASE"),
            "stddev_samp must use CASE for N<=1 guard: {sql}"
        );
    }

    /// StddevPop merge SQL passes NULL through (N=0 → var_pop is NULL → stddev_pop NULL).
    ///
    /// Exasol `GREATEST(0.0, NULL) = 0.0` — a bare SQRT(GREATEST(...)) returns 0.0
    /// when cnt=0, not NULL. The correct form wraps in CASE WHEN IS NULL THEN NULL.
    #[test]
    fn stddev_pop_merge_null_passthrough_for_n_zero() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevPop,
            column: Some("X".into()),
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must contain a NULL guard (CASE … IS NULL) that wraps the whole expression.
        assert!(
            sql.contains("IS NULL"),
            "stddev_pop must pass NULL through for N=0 via IS NULL guard: {sql}"
        );
        // The GREATEST guard against tiny-negative float rounding must still be present.
        assert!(
            sql.contains("GREATEST"),
            "stddev_pop must keep GREATEST rounding guard: {sql}"
        );
    }

    /// StddevSamp merge SQL passes NULL through for N=0 and N=1.
    ///
    /// var_samp is NULL when cnt<=1 (CASE guard). Wrapping in CASE WHEN IS NULL
    /// ensures SQRT does not receive 0.0 via GREATEST(0.0, NULL) = 0.0.
    #[test]
    fn stddev_samp_merge_null_passthrough_for_n_zero_and_n_one() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevSamp,
            column: Some("X".into()),
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must contain a NULL guard that wraps the whole expression.
        assert!(
            sql.contains("IS NULL"),
            "stddev_samp must pass NULL through for N<=1 via IS NULL guard: {sql}"
        );
        // The GREATEST guard against tiny-negative float rounding must still be present.
        assert!(
            sql.contains("GREATEST"),
            "stddev_samp must keep GREATEST rounding guard: {sql}"
        );
    }

    /// HAVING is rendered and appears in the outer GROUP BY wrapper SQL.
    #[test]
    fn having_clause_appears_in_outer_wrapper_only() {
        // Build a grouped aggregate SQL with a HAVING predicate.
        let having_filter = Some(r#"(SUM("AMOUNT") > 100)"#.to_string());
        let spec_template = ScanSpec {
            common: CommonScanSpec {
                projection: vec!["REGION".into(), "AMOUNT".into()],
                aggregates: Some(vec![AggregatePlan {
                    kind: AggKind::Sum,
                    column: Some("AMOUNT".into()),
                    arg_expr: None,
                }]),
                group_keys: Some(vec![r#""REGION""#.to_string()]),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        };
        let shards = vec![vec![("s3://wh/f.parquet".to_string(), 1u64)]];
        let col_types = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &[r#""REGION""#.to_string()],
            &[],
            &[AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            }],
            &[],
            &keys_first_select_items(1, 1),
            None,
            0,
            &col_types,
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            having_filter.as_deref(),
            None,
        );
        // HAVING must appear in the outer wrapper (after GROUP BY)
        assert!(
            sql.contains("HAVING"),
            "outer wrapper must contain HAVING: {sql}"
        );
        assert!(
            sql.contains("100"),
            "HAVING predicate value must be in SQL: {sql}"
        );
        // HAVING must come after GROUP BY
        let having_pos = sql.find("HAVING").unwrap();
        let group_by_pos = sql.find("GROUP BY").unwrap();
        assert!(
            having_pos > group_by_pos,
            "HAVING must appear after GROUP BY: {sql}"
        );
    }

    /// Grouped path: a `function_aggregate` select item whose statistical aggregate
    /// takes an expression argument declines the WHOLE grouped detection, so the
    /// request routes to the Tier 1b qualified single-table wrapper and Exasol
    /// computes the statistic over its rows.
    ///
    /// Measured 2026-07-31 against the Docker Exasol container: `SELECT MOD(id, 4),
    /// STDDEV(score + id) FROM MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4)` is PUSHED by
    /// Exasol and fails with `sqlCode 22002`, `grouped partial aggregate SQL error:
    /// Schema error: No field named .`
    #[test]
    fn grouped_stat_aggregate_over_expression_argument_declines() {
        let req = make_group_by_request_with_types(
            serde_json::json!([mod_item("ID", 4)]),
            serde_json::json!([
                mod_item("ID", 4),
                agg_item_expr("STDDEV", mod_item("SCORE", 4), false),
            ]),
            serde_json::json!([decimal_type(9, 0), {"type": "double"}]),
        );
        assert!(
            detect_group_by_aggregates(&req).is_none(),
            "a grouped STDDEV over an expression argument must decline the grouped \
             partial/merge path"
        );
    }

    /// Grouped scalar-over-aggregate path: a select item WRAPPING a statistical
    /// aggregate over an expression argument (`SQRT(STDDEV(<expr>))`) does not
    /// classify, which declines the whole grouped detection and routes the request
    /// to the qualified single-table wrapper.
    ///
    /// Measured 2026-07-31: `SELECT MOD(id, 4), SQRT(STDDEV(score + id)) FROM
    /// MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4)` is PUSHED by Exasol as a
    /// scalar-over-aggregate — the merge wrapper renders — and fails with `sqlCode
    /// 22002`, `grouped partial aggregate SQL error: Schema error: No field named .`
    #[test]
    fn scalar_over_stat_aggregate_with_expression_argument_declines() {
        let sqrt_over_stat = serde_json::json!({
            "type": "function_scalar",
            "name": "SQRT",
            "arguments": [agg_item_expr("STDDEV", mod_item("SCORE", 4), false)],
        });
        assert!(
            classify_scalar_over_aggregate(&sqrt_over_stat).is_none(),
            "a scalar wrapping a stat aggregate over an expression must not classify"
        );

        let req = make_group_by_request_with_types(
            serde_json::json!([mod_item("ID", 4)]),
            serde_json::json!([mod_item("ID", 4), sqrt_over_stat]),
            serde_json::json!([decimal_type(9, 0), {"type": "double"}]),
        );
        assert!(
            detect_group_by_aggregates(&req).is_none(),
            "an unclassifiable scalar-over-aggregate item must decline the whole \
             grouped detection"
        );
    }

    /// HAVING path: a HAVING comparing a statistical aggregate over an expression
    /// argument does not render over the merge wrapper, so `classify_request_shape`
    /// routes the request to the qualified single-table wrapper rather than emit a
    /// HAVING over a partial column no scan produces.
    ///
    /// The `plans` slot here is the shape the pre-decline parse produced — a
    /// statistical kind carrying neither a source column nor a rendered argument.
    /// Matching that slot is what the decline now prevents; the realistic route,
    /// where the select list carries the same shape, is declined earlier by
    /// `grouped_stat_aggregate_over_expression_argument_declines`.
    #[test]
    fn having_over_stat_aggregate_with_expression_argument_declines() {
        let having = serde_json::json!({
            "type": "predicate_greater",
            "left": agg_item_expr("STDDEV", mod_item("SCORE", 4), false),
            "right": {"type": "literal_double", "value": 5.0},
        });
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevSamp,
            column: None,
            arg_expr: None,
        }];
        assert!(
            render_having_over_merge(&having, &plans).is_none(),
            "a HAVING over a stat aggregate with an expression argument must not \
             render over the merge wrapper"
        );
    }
}

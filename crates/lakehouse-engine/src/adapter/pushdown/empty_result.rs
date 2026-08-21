use crate::scan::spec::{AggKind, AggregatePlan, ProjectionItem};
use crate::types::mapping::exasol_type_from_json;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::GroupedSelectItem;
use super::grouped_agg::{group_key_exasol_types, select_item_index};
use super::request_shape::{RequestShape, classify_request_shape};
use super::scalar_over_agg::{classify_scalar_over_aggregate, render_scalar_over_merge};
use super::single_group_agg::SingleGroupItem;
use super::support::{cast_to_declared_type, declared_select_type, emits_ident};

/// Build the shape-correct empty-result response for a fully-pruned file list.
///
/// Routing goes through the SAME shared [`classify_request_shape`] the non-empty
/// dispatcher uses, so the empty and non-empty positional column shapes are
/// identical by construction — the 3-tier priority (grouped → single-group → row
/// scan), the `validate_agg_col_types` numeric gates, and the grouped HAVING
/// merge-render — whose failure routes to `GroupByWrapper` rather than erroring —
/// all live in the classifier, never re-derived here. Each arm renders only its own
/// empty shape:
/// - `Grouped` → zero rows in the full grouped output shape (`empty_grouped_sql`);
/// - `GroupByWrapper` → a zero-row result typed from `selectListDataTypes`
///   (`empty_select_list_typed_sql`), falling back to the full-row empty shape when
///   `selectListDataTypes` is absent or empty;
/// - `SingleGroupAgg` → one shape-correct empty aggregate row (`empty_agg_sql`);
/// - `RowScan` → a typed empty projection (`empty_pushdown_sql`), or — when
///   `projection_widened` — the same `selectListDataTypes` zero-row shape as
///   `GroupByWrapper`.
///
/// `projection_widened` is `project_columns`'s widening signal for the
/// `proj_cols`/`proj_types` pair: `true` means they are the full base row rather
/// than one item per select-list item (#196).
///
/// No scan or distinct-merge UDF is referenced: with zero files there is nothing to
/// scan or merge, and a zero-row result already satisfies any HAVING/ORDER BY/LIMIT.
pub(super) fn empty_result_sql(
    pushdown_req: &Json,
    proj_cols: &[ProjectionItem],
    proj_types: &[String],
    projection_widened: bool,
    col_types: &[(String, String)],
) -> Result<Json, UdfError> {
    match classify_request_shape(pushdown_req, col_types) {
        // A zero-row result satisfies any HAVING, so the classifier's `having` is
        // deliberately ignored on the empty path.
        RequestShape::Grouped { detection, .. } => {
            let group_key_types = group_key_exasol_types(
                pushdown_req,
                &detection.group_keys,
                &detection.select_items,
            );
            // Per-plan declared types, aligned 1:1 with `detection.plans` (includes
            // aggregates nested inside a scalar-over-aggregate item) — the same
            // aligned source the non-empty grouped path uses.
            Ok(empty_grouped_sql(
                &group_key_types,
                &detection.plan_types,
                &detection.select_items,
            ))
        }
        // The non-empty path routes such a request to the qualified single-table
        // wrapper whose output columns ARE the `selectList` items. Mirror that shape
        // with a zero-row result typed from `selectListDataTypes`, so the empty and
        // non-empty column shapes never diverge (never a full-row `04000` mismatch).
        // When `selectListDataTypes` is absent or empty this falls back to the
        // full-row empty shape, byte-for-byte with the pre-refactor behaviour.
        RequestShape::GroupByWrapper => Ok(empty_select_list_typed_sql(pushdown_req)
            .unwrap_or_else(|| empty_pushdown_sql(proj_cols, proj_types))),
        RequestShape::SingleGroupAgg { items } => {
            Ok(empty_agg_sql(&items, pushdown_req, col_types))
        }
        // A widened derived projection is the full base row, so the non-empty path
        // routes it to the qualified single-table wrapper whose output columns ARE
        // the `selectList` items (#196). Mirror that shape here for the same reason
        // the `GroupByWrapper` arm above does: emitting the full base row instead
        // would diverge from the non-empty column shape and trip Exasol's positional
        // `04000` check.
        RequestShape::RowScan if projection_widened => {
            Ok(empty_select_list_typed_sql(pushdown_req)
                .unwrap_or_else(|| empty_pushdown_sql(proj_cols, proj_types)))
        }
        RequestShape::RowScan => Ok(empty_pushdown_sql(proj_cols, proj_types)),
    }
}

/// A zero-row result whose columns are `CAST(NULL AS <ty>)` for each
/// `selectListDataTypes` entry, in order — the empty-result shape matching the
/// grouped qualified-wrapper fallback (whose output columns are the `selectList`
/// items). `None` when `selectListDataTypes` is absent or empty (the caller then
/// falls back to the full-row empty shape).
fn empty_select_list_typed_sql(pushdown_req: &Json) -> Option<Json> {
    let types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;
    let items: Vec<String> = types
        .iter()
        .map(|dt| format!("CAST(NULL AS {})", exasol_type_from_json(dt)))
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    Some(serde_json::json!({"type": "pushdown", "sql": sql}))
}

/// The empty-result literal for an aggregate evaluated over zero input rows.
///
/// The COUNT family yields `0`; every other kind yields `NULL` — single-node SQL
/// semantics over zero rows, mirroring the zero-count NULL guard (ADR-008).
fn empty_agg_literal(kind: &AggKind) -> &'static str {
    match kind {
        AggKind::Count | AggKind::CountCol => "0",
        AggKind::Sum
        | AggKind::Min
        | AggKind::Max
        | AggKind::Avg
        | AggKind::VarPop
        | AggKind::VarSamp
        | AggKind::StddevPop
        | AggKind::StddevSamp => "NULL",
    }
}

/// The Exasol type an absent nested aggregate's `NULL` carries: its argument
/// column's own type, or `DOUBLE PRECISION` when the argument is an expression
/// (or `COUNT(*)`). The value is NULL either way — the type exists only to make
/// the enclosing scalar's argument well-typed, and Exasol converts implicitly
/// between the numeric and character domains.
fn nested_absent_agg_type(plan: &AggregatePlan, col_types: &[(String, String)]) -> String {
    plan.column
        .as_deref()
        .and_then(|column| {
            col_types
                .iter()
                .find(|(name, _)| name == column)
                .map(|(_, ty)| ty.clone())
        })
        .unwrap_or_else(|| "DOUBLE PRECISION".to_string())
}

/// The empty-result literal for a scalar/arithmetic node wrapping one or more
/// aggregates, evaluated over zero input rows: each nested aggregate's own
/// `empty_agg_literal` is substituted into the scalar structure through the same
/// `render_scalar_over_merge` mechanism the non-empty merge uses, so e.g.
/// `ROUND(COUNT(*), 2)` yields `ROUND(0, 2)` rather than a bare `NULL`.
///
/// An absent value is substituted as a TYPED null: Exasol rejects an untyped
/// `NULL` as a scalar-function argument outright — `ROUND(NULL, 2)` fails with
/// `Feature not supported: Round with wrong type` (SQL state `0A000`,
/// live-captured), as does `FLOOR(NULL)` — while any cast makes the argument
/// well-typed, and the result is NULL either way.
fn empty_scalar_over_aggregate_literal(node: &Json, col_types: &[(String, String)]) -> String {
    let plans = classify_scalar_over_aggregate(node)
        .expect("a SingleGroupItem::ScalarOverAggregate node was already classified at detection");
    let zeros: Vec<String> = plans
        .iter()
        .map(|plan| match empty_agg_literal(&plan.kind) {
            "NULL" => format!("CAST(NULL AS {})", nested_absent_agg_type(plan, col_types)),
            zero => zero.to_string(),
        })
        .collect();
    render_scalar_over_merge(node, &plans, &zeros)
        .expect("a classified scalar-over-aggregate node must render over its own zero values")
}

/// Build the single-group aggregate empty-result response: exactly one row whose
/// columns are each select-list item's zero-row literal cast to its OWN declared
/// result type — looked up at the item's own `selectList` index, never through a
/// filtered/compacted type list, since a `ScalarOverAggregate` item shifts every
/// later index in such a list. A `COUNT(DISTINCT ...)` item yields `0` (no merge
/// UDF and no fan-out: with zero files there is nothing to scan or deduplicate);
/// an ordinary aggregate yields its per-`AggKind` empty literal; a scalar node
/// wrapping aggregates yields that structure rendered over each nested
/// aggregate's own empty literal (`empty_scalar_over_aggregate_literal`, which
/// reads `col_types` to type each absent nested value), cast to its own
/// `declared_type` field. `FROM DUAL` alone already yields one row, so no `WHERE`
/// is emitted.
///
/// The cast decision mirrors `cast_merge_items` (cast when a declared type is
/// present and not the `VARCHAR(2000000)` default) so the empty column types can
/// never drift from the non-empty single-group shape.
fn empty_agg_sql(
    items: &[SingleGroupItem],
    pushdown_req: &Json,
    col_types: &[(String, String)],
) -> Json {
    let literals: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(i, item)| match item {
            SingleGroupItem::Distinct(_) => {
                cast_to_declared_type("0", Some(declared_select_type(pushdown_req, i).as_str()))
            }
            SingleGroupItem::Aggregate(plan) => cast_to_declared_type(
                empty_agg_literal(&plan.kind),
                Some(declared_select_type(pushdown_req, i).as_str()),
            ),
            SingleGroupItem::ScalarOverAggregate {
                node,
                declared_type,
                ..
            } => cast_to_declared_type(
                &empty_scalar_over_aggregate_literal(node, col_types),
                Some(declared_type.as_str()),
            ),
        })
        .collect();
    let sql = format!("SELECT {} FROM DUAL", literals.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

/// Build the grouped aggregate empty-result response: zero rows
/// (`FROM DUAL WHERE 1=0`) whose columns are the full grouped output shape —
/// group-key, merged-aggregate, and constant-projection columns assembled in the
/// user's select-list order via `select_items`, exactly as the non-empty grouped
/// merge assembles its outer SELECT.
///
/// Group-key and aggregate columns are `CAST(NULL AS <declared-type>)` (types from
/// `group_key_exasol_types` / `GroupedAggregateDetection::plan_types`); a constant projection
/// reuses its already-rendered, type-cast expression. A zero-row result satisfies
/// any HAVING / ORDER BY / LIMIT, so none of those need rendering.
fn empty_grouped_sql(
    group_key_types: &[String],
    aggregate_types: &[String],
    select_items: &[GroupedSelectItem],
) -> Json {
    let mut ordered = select_items.to_vec();
    ordered.sort_by_key(select_item_index);
    let items: Vec<String> = ordered
        .iter()
        .filter_map(|item| match item {
            GroupedSelectItem::GroupKey { group_key_slot, .. } => group_key_types
                .get(*group_key_slot)
                .map(|ty| format!("CAST(NULL AS {ty})")),
            GroupedSelectItem::Aggregate { plan_slot, .. } => aggregate_types
                .get(*plan_slot)
                .map(|ty| format!("CAST(NULL AS {ty})")),
            GroupedSelectItem::Constant { projection, .. } => Some(projection.clone()),
            // A scalar-over-aggregate column is NULL over zero rows and goes through
            // the shared `cast_to_declared_type`, so — unlike the GroupKey/Aggregate
            // arms above, which cast unconditionally — it emits a bare NULL when the
            // item's declared type is the VARCHAR(2000000) default.
            GroupedSelectItem::ScalarOverAggregate { declared_type, .. } => {
                Some(cast_to_declared_type("NULL", Some(declared_type)))
            }
        })
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

/// Build a pushdown response with an empty result (no matching files).
fn empty_pushdown_sql(proj_cols: &[ProjectionItem], proj_types: &[String]) -> Json {
    let items: Vec<String> = proj_cols
        .iter()
        .zip(proj_types.iter())
        .enumerate()
        .map(|(i, (item, ty))| format!("CAST(NULL AS {ty}) AS {}", emits_ident(item, i)))
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

#[cfg(test)]
#[path = "empty_result_tests.rs"]
mod tests;

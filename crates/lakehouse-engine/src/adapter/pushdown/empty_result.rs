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

/// Routes through the same [`classify_request_shape`] as the non-empty dispatcher,
/// so empty and non-empty positional column shapes cannot diverge.
///
/// `projection_widened` means `proj_cols`/`proj_types` are the full base row rather
/// than one item per select-list item (#196).
pub(super) fn empty_result_sql(
    pushdown_req: &Json,
    proj_cols: &[ProjectionItem],
    proj_types: &[String],
    projection_widened: bool,
    col_types: &[(String, String)],
) -> Result<Json, UdfError> {
    match classify_request_shape(pushdown_req, col_types) {
        RequestShape::Grouped { detection, .. } => {
            let group_key_types = group_key_exasol_types(
                pushdown_req,
                &detection.group_keys,
                &detection.select_items,
            );
            Ok(empty_grouped_sql(
                &group_key_types,
                &detection.plan_types,
                &detection.select_items,
            ))
        }
        // Mirrors the non-empty wrapper's `selectList`-shaped output; a full-row shape
        // would trip Exasol's positional `04000` check.
        RequestShape::GroupByWrapper => Ok(empty_select_list_typed_sql(pushdown_req)
            .unwrap_or_else(|| empty_pushdown_sql(proj_cols, proj_types))),
        RequestShape::SingleGroupAgg { items } => {
            Ok(empty_agg_sql(&items, pushdown_req, col_types))
        }
        // Widened projection: same reasoning as the `GroupByWrapper` arm (#196).
        RequestShape::RowScan if projection_widened => {
            Ok(empty_select_list_typed_sql(pushdown_req)
                .unwrap_or_else(|| empty_pushdown_sql(proj_cols, proj_types)))
        }
        RequestShape::RowScan => Ok(empty_pushdown_sql(proj_cols, proj_types)),
    }
}

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

/// Single-node SQL semantics over zero rows, mirroring the zero-count NULL guard (ADR-008).
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

/// The type exists only to make the enclosing scalar's argument well-typed; the
/// value is NULL either way.
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

/// Exasol rejects an untyped `NULL` scalar-function argument (`ROUND(NULL, 2)` fails
/// with SQL state `0A000`), so an absent nested aggregate is substituted as a typed null.
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

/// Each item's type is looked up at its own `selectList` index, never a compacted
/// list, since a `ScalarOverAggregate` item shifts later indices. The cast rule
/// mirrors `cast_merge_items` so empty and non-empty types cannot drift.
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
            GroupedSelectItem::ScalarOverAggregate { declared_type, .. } => {
                Some(cast_to_declared_type("NULL", Some(declared_type)))
            }
        })
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

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

use crate::scan::spec::ProjectionItem;
use crate::types::mapping::exasol_type_from_json;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::GroupedSelectItem;
use super::grouped_agg::{group_key_exasol_types, select_item_index};
use super::joins::EmptyRelation;
use super::request_shape::{RequestShape, classify_request_shape};
use super::support::{cast_to_declared_type, emits_ident};

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
    relation: &EmptyRelation,
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
        RequestShape::SingleGroupAgg { .. } | RequestShape::SingleGroupWrapper => {
            relation.pushdown(pushdown_req)
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

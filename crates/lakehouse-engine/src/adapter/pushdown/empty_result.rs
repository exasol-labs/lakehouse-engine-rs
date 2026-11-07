use crate::types::exasol_type::ExasolType;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::GroupedSelectItem;
use super::grouped_agg::select_item_index;
use super::joins::EmptyRelation;
use super::request_shape::{RequestShape, classify_request_shape};
use super::support::{
    ColumnTypes, ProjectedColumn, PushdownRequest, cast_to_declared_type, emits_ident,
};

/// Routes through the same [`classify_request_shape`] as the non-empty dispatcher,
/// so empty and non-empty positional column shapes cannot diverge.
///
/// `projection_widened` means `projection` is the full base row rather
/// than one item per select-list item (#196).
pub(super) fn empty_result_sql(
    pushdown: &PushdownRequest,
    projection: &[ProjectedColumn],
    projection_widened: bool,
    col_types: &ColumnTypes,
    relation: &EmptyRelation,
) -> Result<Json, UdfError> {
    match classify_request_shape(pushdown, col_types) {
        RequestShape::Grouped { detection, .. } => Ok(empty_grouped_sql(
            &detection.group_key_types,
            &detection.plan_types,
            &detection.select_items,
        )),
        // Mirrors the non-empty wrapper's `selectList`-shaped output; a full-row shape
        // would trip Exasol's positional `04000` check. Both arms carry a non-empty
        // `selectList`, so `select_types()` holds one type per item.
        RequestShape::GroupByWrapper => Ok(empty_select_list_typed_sql(pushdown.select_types())),
        RequestShape::SingleGroupAgg { .. } | RequestShape::SingleGroupWrapper => {
            relation.pushdown(pushdown)
        }
        // Widened projection (#196).
        RequestShape::RowScan if projection_widened => {
            Ok(empty_select_list_typed_sql(pushdown.select_types()))
        }
        RequestShape::RowScan => Ok(empty_pushdown_sql(projection)),
    }
}

fn empty_select_list_typed_sql(select_types: &[ExasolType]) -> Json {
    let items: Vec<String> = select_types
        .iter()
        .map(|ty| format!("CAST(NULL AS {ty})"))
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

fn empty_grouped_sql(
    group_key_types: &[ExasolType],
    aggregate_types: &[ExasolType],
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
                Some(cast_to_declared_type("NULL", *declared_type))
            }
        })
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

fn empty_pushdown_sql(projection: &[ProjectedColumn]) -> Json {
    let items: Vec<String> = projection
        .iter()
        .enumerate()
        .map(|(i, column)| {
            format!(
                "CAST(NULL AS {}) AS {}",
                column.exa_type,
                emits_ident(&column.item, i)
            )
        })
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

#[cfg(test)]
#[path = "empty_result_tests.rs"]
mod tests;

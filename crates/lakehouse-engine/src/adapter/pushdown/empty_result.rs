use crate::types::exasol_type::ExasolType;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use vs_expression::SqlFragment;

use super::joins::EmptyRelation;
use super::request_shape::{RequestShape, classify_request_shape};
use super::support::{
    ProjectedColumn, PushdownRequest, ScanTranslator, emits_ident, output_select_list,
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
    scan: ScanTranslator<'_>,
    relation: &EmptyRelation,
) -> Result<Json, UdfError> {
    match classify_request_shape(pushdown, scan) {
        // Mirrors the non-empty merge's and wrapper's `selectList`-shaped output; a full-row shape
        // would trip Exasol's positional `04000` check. Both arms carry a non-empty
        // `selectList`, so `select_types()` holds one type per item.
        RequestShape::Grouped { .. } | RequestShape::GroupByWrapper => {
            Ok(empty_select_list_typed_sql(pushdown.select_types()))
        }
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
    let nulls = select_types.iter().map(|_| SqlFragment::new("NULL", None));
    let items = output_select_list(nulls, select_types).unwrap_or_default();
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

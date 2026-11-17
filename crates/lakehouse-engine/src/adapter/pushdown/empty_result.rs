use crate::types::exasol_type::ExasolType;
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::joins::EmptyRelation;
use super::request_shape::{RequestShape, classify_request_shape};
use super::support::{
    ProjectedColumn, PushdownRequest, RowProjection, ScanTranslator, emits_ident, pushdown_response,
};

/// Routes through the same [`classify_request_shape`] as the non-empty dispatcher, so empty and
/// non-empty column shapes cannot diverge. `bound` is `pushdown` as `scan` binds its columns.
pub(super) fn empty_result_sql(
    pushdown: &PushdownRequest,
    bound: &PushdownRequest,
    projection: &RowProjection,
    scan: ScanTranslator<'_>,
    relation: &EmptyRelation,
) -> Result<Json, UdfError> {
    let sql = match classify_request_shape(bound, scan) {
        // Mirrors the non-empty merge's and wrapper's `selectList`-shaped output; a full-row shape
        // would trip Exasol's positional `04000` check.
        RequestShape::Grouped { .. } | RequestShape::GroupByWrapper => {
            empty_select_list_typed_sql(pushdown.select_types())
        }
        RequestShape::SingleGroupAgg { .. } | RequestShape::SingleGroupWrapper => {
            relation.pushdown(pushdown)?
        }
        RequestShape::RowScan => match projection {
            RowProjection::Wrapped => empty_select_list_typed_sql(pushdown.select_types()),
            RowProjection::Direct(projection) => empty_pushdown_sql(projection),
        },
    };
    Ok(pushdown_response(sql))
}

fn empty_select_list_typed_sql(select_types: &[ExasolType]) -> String {
    let items: Vec<String> = select_types
        .iter()
        .map(|ty| format!("CAST(NULL AS {ty})"))
        .collect();
    format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "))
}

fn empty_pushdown_sql(projection: &[ProjectedColumn]) -> String {
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
    format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "))
}

#[cfg(test)]
#[path = "empty_result_tests.rs"]
mod tests;

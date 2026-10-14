use std::collections::HashSet;

use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::RefusedColumn;
use super::support::collect_all_column_names;
use crate::scan::spec::ProjectionItem;

/// Columns are collected by one blind recursive walk over the whole `request`,
/// so a later pushdown capability is gated automatically; pass the OUTERMOST
/// request value.
///
/// `emitted_projection` is `Some` only for a `SELECT *` projection, which the walk
/// would miss. A widened aggregate projection must be `None`: it is a synthetic
/// placeholder, and unioning it would refuse every aggregate (even `COUNT(*)`)
/// over a table with a refused column.
pub(super) fn ensure_no_refused_column_referenced(
    request: &Json,
    emitted_projection: Option<&[ProjectionItem]>,
    refused: &[RefusedColumn],
) -> Result<(), UdfError> {
    if refused.is_empty() {
        return Ok(());
    }

    let mut touched: HashSet<String> = HashSet::new();
    collect_all_column_names(request, &mut touched);
    if let Some(projection) = emitted_projection {
        touched.extend(projection.iter().filter_map(|item| match item {
            ProjectionItem::Column(name) => Some(name.to_uppercase()),
            ProjectionItem::Expr { .. } => None,
        }));
    }

    ensure_no_touched_column_is_refused(&touched, refused)
}

pub(super) fn ensure_no_touched_column_is_refused(
    touched: &HashSet<String>,
    refused: &[RefusedColumn],
) -> Result<(), UdfError> {
    let reasons: Vec<&str> = refused
        .iter()
        .filter(|column| touched.contains(&column.column_name.to_uppercase()))
        .map(|column| column.reason.as_str())
        .collect();
    if reasons.is_empty() {
        return Ok(());
    }
    Err(UdfError::User(format!(
        "pushdown request reads or emits column(s) this engine cannot render: {}",
        reasons.join("; ")
    )))
}

#[cfg(test)]
#[path = "refused_columns_tests.rs"]
mod tests;

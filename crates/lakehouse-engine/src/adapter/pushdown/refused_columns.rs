use std::collections::HashSet;

use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::RefusedColumn;
use super::support::{OutputContract, collect_all_column_names};

/// Columns are collected by one blind recursive walk over the whole `request`, so a later
/// pushdown capability is gated automatically. `SELECT *` names no column, so it charges every one.
pub(super) fn ensure_no_refused_column_referenced(
    request: &Json,
    output: OutputContract,
    refused: &[RefusedColumn],
) -> Result<(), UdfError> {
    if refused.is_empty() {
        return Ok(());
    }

    let mut touched: HashSet<String> = HashSet::new();
    collect_all_column_names(request, &mut touched);
    match output {
        OutputContract::WholeRow => {
            touched.extend(
                refused
                    .iter()
                    .map(|column| column.column_name.to_uppercase()),
            );
        }
        OutputContract::AnyOneColumn | OutputContract::Items => {}
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

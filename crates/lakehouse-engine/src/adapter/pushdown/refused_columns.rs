use std::collections::HashSet;

use exasol_udf_sdk::error::UdfError;

use super::RefusedColumn;
use super::request::{Clause, OutputContract, PushdownRequest};
use super::support::collect_all_column_names;

/// Columns are collected by one walk over every clause of the request, so a later pushdown
/// capability is gated automatically. `SELECT *` names no column, so it charges every one.
pub(super) fn ensure_no_refused_column_referenced(
    pushdown: &PushdownRequest,
    refused: &[RefusedColumn],
) -> Result<(), UdfError> {
    if refused.is_empty() {
        return Ok(());
    }

    let mut touched: HashSet<String> = HashSet::new();
    for tree in pushdown.expressions(&Clause::ALL) {
        collect_all_column_names(tree, &mut touched);
    }
    match pushdown.output() {
        OutputContract::WholeRow => {
            touched.extend(
                refused
                    .iter()
                    .map(|column| column.column_name.to_uppercase()),
            );
        }
        OutputContract::AnyOneColumn | OutputContract::Items(_) => {}
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

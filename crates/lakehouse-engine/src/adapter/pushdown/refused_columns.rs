//! The refused-column gate: refuses a pushdown request that reads or emits a
//! column the table's format reader declined to map to an Arrow tag.

use std::collections::HashSet;

use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use super::RefusedColumn;
use super::support::collect_all_column_names;
use crate::scan::spec::ProjectionItem;

/// Refuses `request` when it reads or emits any column in `refused`, naming
/// every one it touches and the reason that column's reader gave.
///
/// The columns a request touches are collected by ONE blind recursive walk over
/// the whole `request` rather than from an enumeration of clauses, so a pushdown
/// capability added later — a new predicate shape, a new aggregate-argument
/// position — is gated the day it lands instead of routing a refused column
/// straight past this check. Pass the OUTERMOST request value for that reason.
///
/// `emitted_projection` is `Some` only for a projection the request genuinely
/// emits without naming its columns anywhere — a `SELECT *`, which the walk alone
/// would miss. A WIDENED projection is `None`: that one is a synthetic full-row
/// placeholder `build_dispatch_sql` never reads (an aggregate's own referenced
/// columns live in its `arguments`, which the walk already reaches), so unioning
/// it would refuse every aggregate query over a table carrying a refused column,
/// `COUNT(*)` included. One argument rather than a projection plus a widening
/// flag, so the combination that re-introduces that over-refusal cannot be
/// expressed at a call site.
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

/// Refuses when `touched` names any column in `refused`, in ONE error carrying
/// each matched column's own reason in schema order.
///
/// Split from the request walk above so a caller holding a NARROWER touched set —
/// the join path, which charges each column reference to the side that must answer
/// for it — refuses through the same matching rule and the same message.
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

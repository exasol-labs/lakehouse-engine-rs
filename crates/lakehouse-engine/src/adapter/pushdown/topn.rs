use crate::scan::spec::{LogicalField, ProjectionItem, SortKey, render_ordered};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;
use vs_expression::render_expression_exasol_safe;

use super::support::{
    collect_all_column_names, emits_ident, extract_limit, extract_offset, render_limit_offset,
};

/// Bare `column` nodes only: per-shard bounded top-N is eligible for bare columns
/// only. Expression sort keys use [`parse_sort_flags`] instead.
pub(super) fn parse_sort_key_element(element: &Json) -> Option<SortKey> {
    let expr = element.get("expression")?;
    if expr.get("type").and_then(|t| t.as_str()) != Some("column") {
        return None;
    }
    let column = expr
        .get("name")
        .and_then(|n| n.as_str())
        .map(|s| s.to_uppercase())?;
    let ascending = element.get("isAscending").and_then(|b| b.as_bool())?;
    let nulls_last = element.get("nullsLast").and_then(|b| b.as_bool())?;
    Some(SortKey {
        column,
        ascending,
        nulls_last,
    })
}

/// `None` when either flag is absent: a defaulted direction or NULL placement would
/// be a wrong order.
pub(super) fn parse_sort_flags(element: &Json) -> Option<(bool, bool)> {
    let ascending = element.get("isAscending").and_then(|b| b.as_bool())?;
    let nulls_last = element.get("nullsLast").and_then(|b| b.as_bool())?;
    Some((ascending, nulls_last))
}

#[derive(Debug)]
pub(super) enum ParsedSortKey {
    Column(SortKey),
    /// Every referenced base column must be emitted, hidden, for the wrapper's
    /// `ORDER BY` to bind against (#198).
    Expression {
        rendered: String,
        columns: Vec<String>,
        ascending: bool,
        nulls_last: bool,
    },
    /// Kept in the parsed list rather than dropped so a caller can see an element was
    /// lost and decline instead of returning an under-ordered result.
    Unrenderable,
}

impl ParsedSortKey {
    fn referenced_columns(&self) -> &[String] {
        match self {
            Self::Column(key) => std::slice::from_ref(&key.column),
            Self::Expression { columns, .. } => columns,
            Self::Unrenderable => &[],
        }
    }

    fn render_order_by_element(&self) -> Option<String> {
        match self {
            Self::Column(key) => Some(key.render_order_by_element()),
            Self::Expression {
                rendered,
                ascending,
                nulls_last,
                ..
            } => Some(render_ordered(rendered, *ascending, *nulls_last)),
            Self::Unrenderable => None,
        }
    }
}

/// Rendered in the Exasol dialect because the outer wrapper is parsed by Exasol,
/// never inside a DataFusion `ScanSpec`.
fn parse_declined_sort_key(element: &Json) -> ParsedSortKey {
    if let Some(key) = parse_sort_key_element(element) {
        return ParsedSortKey::Column(key);
    }
    let (Some((ascending, nulls_last)), Some(expr)) =
        (parse_sort_flags(element), element.get("expression"))
    else {
        return ParsedSortKey::Unrenderable;
    };
    let Some(rendered) = render_expression_exasol_safe(expr) else {
        return ParsedSortKey::Unrenderable;
    };
    let mut names = std::collections::HashSet::new();
    collect_all_column_names(expr, &mut names);
    let mut columns: Vec<String> = names.into_iter().collect();
    // Sorted so the hidden columns' EMITS order (and `_LH_PROJ_{i}` aliases) is
    // deterministic across processes.
    columns.sort();
    ParsedSortKey::Expression {
        rendered,
        columns,
        ascending,
        nulls_last,
    }
}

/// Exasol does not re-apply a delegated `ORDER BY`, so the declined paths must
/// reproduce the global sort. Yields exactly one key per element, including
/// `Unrenderable`, so callers can detect a partially rendered clause.
pub(super) fn parse_order_by_keys(pushdown_req: &Json) -> Vec<ParsedSortKey> {
    pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .map(|elements| elements.iter().map(parse_declined_sort_key).collect())
        .unwrap_or_default()
}

/// Exasol does not re-apply a delegated `ORDER BY`, so returning SQL built from the
/// surviving keys would silently answer a different query. Any unrenderable element
/// declines with a hard error; there is no native re-plan to fall back to.
pub(super) fn ensure_every_sort_key_renders(keys: &[ParsedSortKey]) -> Result<(), UdfError> {
    let positions = keys
        .iter()
        .enumerate()
        .filter(|(_, key)| matches!(key, ParsedSortKey::Unrenderable))
        .map(|(i, _)| (i + 1).to_string())
        .collect::<Vec<_>>();
    if positions.is_empty() {
        return Ok(());
    }
    Err(UdfError::User(format!(
        "ORDER BY pushdown declined: sort key at clause position {} could not be rendered \
         for the declined row-scan wrapper; this is a hard error, not a native re-plan",
        positions.join(", ")
    )))
}

/// Guards (any failure returns `None`):
/// - a non-zero offset declines: a per-shard `LIMIT n OFFSET m` skips each shard's
///   own first m rows (#191). A zero offset matches, since Exasol normalises it away.
/// - every sort key must be a projected bare column whose type does not need the
///   JSON-fallback VARCHAR cast: the scan emits the JSON string but sorts by the
///   native value, so the outer merge would re-rank lexicographically and corrupt
///   the global top-N.
pub(super) fn detect_topn(
    request: &Json,
    pushdown_req: &Json,
    proj_cols: &[ProjectionItem],
    logical_schema: &[LogicalField],
) -> Option<Vec<SortKey>> {
    extract_limit(pushdown_req)?;
    if extract_offset(pushdown_req) != 0 {
        return None;
    }

    // Top-N over grouped results is out of scope.
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by") {
        return None;
    }
    if pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return None;
    }

    if pushdown_req
        .get("having")
        .filter(|h| !h.is_null())
        .is_some()
    {
        return None;
    }

    let table_count = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .map(|t| t.len())
        .unwrap_or(0);
    if table_count != 1 {
        return None;
    }

    let elements = pushdown_req.get("orderBy").and_then(|v| v.as_array())?;
    if elements.is_empty() {
        return None;
    }
    let mut keys = Vec::with_capacity(elements.len());
    for element in elements {
        let key = parse_sort_key_element(element)?;
        let projected = proj_cols
            .iter()
            .any(|p| matches!(p, ProjectionItem::Column(c) if *c == key.column));
        if !projected {
            return None;
        }
        // See the JSON-fallback guard above; an unknown column declines defensively.
        let arrow_type = logical_schema
            .iter()
            .find(|f| f.name.to_uppercase() == key.column)
            .map(|f| crate::types::mapping::arrow_type_from_tag(&f.arrow_type))?;
        if crate::types::mapping::needs_json_fallback(&arrow_type) {
            return None;
        }
        keys.push(key);
    }
    Some(keys)
}

/// Appended columns are hidden: emitted by the scan, dropped by the wrapper's
/// visible select list, so the result arity matches what Exasol expects
/// (`sqlCode 04000`, #225).
///
/// Append-only keeps every original index, so positional `_LH_PROJ_{i}` aliases
/// stay aligned. Each column is appended at most once (a duplicate EMITS identifier
/// is an error). `proj_cols` and `proj_types` stay in lockstep.
pub(super) fn extend_projection_with_sort_keys(
    proj_cols: &mut Vec<ProjectionItem>,
    proj_types: &mut Vec<String>,
    keys: &[ParsedSortKey],
    col_types: &[(String, String)],
) {
    for column in keys.iter().flat_map(ParsedSortKey::referenced_columns) {
        let already_emitted = proj_cols
            .iter()
            .any(|p| matches!(p, ProjectionItem::Column(c) if c == column));
        if already_emitted {
            continue;
        }
        let Some((name, exa_type)) = col_types.iter().find(|(name, _)| name == column) else {
            continue;
        };
        proj_cols.push(ProjectionItem::Column(name.clone()));
        proj_types.push(exa_type.clone());
    }
}

/// Exasol does not re-apply a delegated `ORDER BY`, so the adapter must reproduce the
/// global sort. The window is rendered here, not in the fan-out, since a per-shard
/// `OFFSET` does not compose (#191); Exasol rejects `OFFSET` without `ORDER BY`.
///
/// Returns `sql` unchanged when no key renders (callers must guarantee one does;
/// the broadcast caller `debug_assert`s on it). `visible_count == 0` falls back to
/// `SELECT *`, since `SELECT  FROM (…)` is invalid.
pub(super) fn wrap_declined_order_by(
    sql: &str,
    proj_cols: &[ProjectionItem],
    visible_count: usize,
    keys: &[ParsedSortKey],
    limit: Option<u64>,
    offset: u64,
) -> String {
    let order_by = keys
        .iter()
        .filter_map(ParsedSortKey::render_order_by_element)
        .collect::<Vec<_>>()
        .join(", ");
    if order_by.is_empty() {
        return sql.to_string();
    }
    let visible_list = if visible_count == 0 {
        "*".to_string()
    } else {
        proj_cols
            .iter()
            .take(visible_count)
            .enumerate()
            .map(|(i, item)| emits_ident(item, i))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "SELECT {visible_list} FROM ({sql}) ORDER BY {order_by}{}",
        render_limit_offset(limit, offset)
    )
}

#[cfg(test)]
#[path = "topn_tests.rs"]
mod tests;

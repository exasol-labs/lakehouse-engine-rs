use crate::scan::spec::{ColumnRef, LogicalField, ProjectionItem, SortKey, render_ordered};
use serde_json::Value as Json;
use vs_expression::render_expression_exasol;

use super::support::{
    AggregationType, Emitted, ProjectedColumn, PushdownRequest, aggregation_type,
    collect_all_column_names, column_node_ref, emits_ident, extract_limit, extract_offset,
    render_limit_offset,
};
use exasol_udf_sdk::error::UdfError;

/// Bare `column` nodes only: per-shard bounded top-N is eligible for bare columns
/// only. Expression sort keys use [`sort_flags`] instead.
///
/// A node's `tableAlias` qualifies the key, so a broadcast join's key keeps the leg its
/// qualification stamped; the single-table paths strip every alias first, so their keys stay
/// bare.
pub(super) fn parse_sort_key_element(element: &Json) -> Option<SortKey> {
    let column = column_node_ref(element.get("expression")?)?;
    let (ascending, nulls_last) = sort_flags(element);
    Some(SortKey {
        column,
        ascending,
        nulls_last,
    })
}

/// `(ascending, nulls_last)`: Exasol states both flags on every `orderBy` element.
pub(super) fn sort_flags(element: &Json) -> (bool, bool) {
    let flag = |key: &str| {
        element[key]
            .as_bool()
            .unwrap_or_else(|| panic!("Exasol states {key} on every orderBy element"))
    };
    (flag("isAscending"), flag("nullsLast"))
}

#[derive(Debug, Clone, PartialEq)]
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
}

impl ParsedSortKey {
    fn referenced_columns(&self) -> Vec<&str> {
        match self {
            Self::Column(key) => vec![key.column.name.as_str()],
            Self::Expression { columns, .. } => columns.iter().map(String::as_str).collect(),
        }
    }

    fn render_order_by_element(&self) -> String {
        match self {
            Self::Column(key) => key.render_order_by_element(),
            Self::Expression {
                rendered,
                ascending,
                nulls_last,
                ..
            } => render_ordered(rendered, *ascending, *nulls_last),
        }
    }
}

/// Rendered in the Exasol dialect because the outer wrapper is parsed by Exasol,
/// never inside a DataFusion `ScanSpec`.
fn parse_declined_sort_key(element: &Json) -> ParsedSortKey {
    if let Some(key) = parse_sort_key_element(element) {
        return ParsedSortKey::Column(key);
    }
    let (ascending, nulls_last) = sort_flags(element);
    let expr = &element["expression"];
    let rendered = render_expression_exasol(expr);
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
/// reproduce the global sort, one key per element.
pub(super) fn parse_order_by_keys(pushdown_req: &Json) -> Vec<ParsedSortKey> {
    pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .map(|elements| elements.iter().map(parse_declined_sort_key).collect())
        .unwrap_or_default()
}

/// A key equal to a select-list item sorts by the column the scan emits for it, so the wrapper
/// needs none of the item's base columns and Exasol does not compute the item again.
/// `projection` must hold one column per select-list item, as the row-scan path does.
pub(super) fn sort_by_select_items(
    keys: &mut [ParsedSortKey],
    pushdown: &PushdownRequest,
    projection: &[ProjectedColumn],
) {
    let items = pushdown.select_items();
    let elements = pushdown.json().get("orderBy").and_then(Json::as_array);
    let (Some(items), Some(elements)) = (items, elements) else {
        return;
    };
    if items.len() != projection.len() {
        return;
    }
    for (key, element) in keys.iter_mut().zip(elements) {
        let ParsedSortKey::Expression {
            ascending,
            nulls_last,
            ..
        } = *key
        else {
            continue;
        };
        let Some(index) = items
            .iter()
            .position(|item| Some(item) == element.get("expression"))
        else {
            continue;
        };
        *key = ParsedSortKey::Expression {
            rendered: emits_ident(&projection[index].item, index),
            columns: Vec::new(),
            ascending,
            nulls_last,
        };
    }
}

/// Guards (any failure returns `None`):
/// - a non-zero offset declines: a per-shard `LIMIT n OFFSET m` skips each shard's
///   own first m rows (#191). A zero offset matches, since Exasol normalises it away.
/// - every sort key must be a projected bare column whose type does not need the
///   JSON-fallback VARCHAR cast: the scan emits the JSON string but sorts by the
///   native value, so the outer merge would re-rank lexicographically and corrupt
///   the global top-N.
pub(super) fn detect_topn(
    pushdown_req: &Json,
    proj_cols: &[ProjectionItem],
    logical_schema: &[LogicalField],
) -> Option<Vec<SortKey>> {
    extract_limit(pushdown_req)?;
    if extract_offset(pushdown_req) != 0 {
        return None;
    }

    // Top-N over grouped results is out of scope.
    if aggregation_type(pushdown_req) == Some(AggregationType::GroupBy) {
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
            .find(|f| f.name.to_uppercase() == key.column.name)
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
/// is an error).
pub(super) fn extend_projection_with_sort_keys(
    projection: &mut Vec<ProjectedColumn>,
    keys: &[ParsedSortKey],
    emitted: &Emitted,
) -> Result<(), UdfError> {
    for column in keys.iter().flat_map(ParsedSortKey::referenced_columns) {
        let already_emitted = projection.iter().any(|projected| {
            matches!(&projected.item, ProjectionItem::Column(c) if c.relation.is_none() && c.name == column)
        });
        if already_emitted {
            continue;
        }
        projection.push(emitted.project(&ColumnRef::from(column))?);
    }
    Ok(())
}

/// Exasol does not re-apply a delegated `ORDER BY`, so the adapter must reproduce the
/// global sort. The window is rendered here, not in the fan-out, since a per-shard
/// `OFFSET` does not compose (#191); Exasol rejects `OFFSET` without `ORDER BY`.
///
/// `keys` is never empty: both callers wrap only a non-empty `orderBy`. `visible_count == 0`
/// falls back to `SELECT *`, since `SELECT  FROM (…)` is invalid.
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
        .map(ParsedSortKey::render_order_by_element)
        .collect::<Vec<_>>()
        .join(", ");
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

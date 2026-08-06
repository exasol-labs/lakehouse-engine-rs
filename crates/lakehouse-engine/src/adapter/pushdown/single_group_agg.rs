//! Single-group aggregate detection and shared aggregate-item parsing.
//!
//! Extracted verbatim from the former flat `pushdown.rs`. `detect_aggregates`
//! is the single-group entry point; `parse_agg_item` is the shared parsing
//! primitive consumed by `grouped_agg` (hence `pub(super)`).

use crate::scan::spec::{AggKind, AggregatePlan};
use serde_json::Value as Json;
use vs_expression::render_expression;

/// One resolved single-group select-list item, in select-list order.
///
/// An ordinary aggregate ([`SingleGroupItem::Aggregate`]) is computed node-locally
/// as a partial result and merged by the outer wrapper's aggregate expression. A
/// `COUNT(DISTINCT ...)` ([`SingleGroupItem::Distinct`]) is NOT an aggregate
/// partial: it is decomposed into its own DISTINCT row-scan fan-out counted by an
/// outer Exasol-native `COUNT(DISTINCT "V")` — see
/// `vs-adapter/pushdown-planning-count-distinct`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingleGroupItem {
    /// An ordinary aggregate served by the shared per-shard partial-aggregate scan.
    Aggregate(AggregatePlan),
    /// A `COUNT(DISTINCT col|expr)` served by its own DISTINCT row-scan fan-out.
    Distinct(DistinctCount),
}

/// A single-group `COUNT(DISTINCT col)` / `COUNT(DISTINCT expr)` descriptor.
///
/// Exactly one of `column` (bare-column fast path) or `arg_expr` (a rendered
/// DataFusion SQL fragment) is populated, mirroring [`AggregatePlan`]. It carries
/// no `AggKind`: the distinct count is realized as a DISTINCT row-scan fan-out over
/// this argument (`CommonScanSpec::distinct`), not as an aggregate partial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctCount {
    pub column: Option<String>,
    pub arg_expr: Option<String>,
}

/// Inspect the pushdown request's `selectList` and return one resolved
/// [`SingleGroupItem`] per item, in order, if every select-list item is a
/// supported single-group aggregate.
///
/// Returns `None` (fall back to row scan) when any of the following hold:
/// - `groupBy` is present and non-empty (GROUP BY not supported)
/// - any select item has `distinct: true` OTHER than a `COUNT(DISTINCT ...)`
///   (single-group `COUNT(DISTINCT col)` / `COUNT(DISTINCT expr)` is accepted as a
///   [`SingleGroupItem::Distinct`] fan-out; DISTINCT SUM/AVG/etc. still decline)
/// - any select item is not one of COUNT(*), COUNT(col)/COUNT(expr),
///   SUM/MIN/MAX/AVG (bare column or renderable expression), or the
///   STDDEV/VARIANCE family
/// - the select list is absent or empty
pub fn detect_aggregates(pushdown_req: &Json) -> Option<Vec<SingleGroupItem>> {
    // Reject GROUP BY.
    if pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return None;
    }

    let list = pushdown_req.get("selectList").and_then(|v| v.as_array())?;

    if list.is_empty() {
        return None;
    }

    let mut items = Vec::with_capacity(list.len());
    for item in list {
        // Every item must be a function_aggregate.
        if item.get("type").and_then(|t| t.as_str()) != Some("function_aggregate") {
            return None;
        }
        // A single-group COUNT(DISTINCT ...) becomes a DISTINCT row-scan fan-out;
        // every OTHER distinct aggregate declines via parse_agg_item.
        let resolved = match parse_count_distinct(item) {
            Some(distinct) => SingleGroupItem::Distinct(distinct),
            None => SingleGroupItem::Aggregate(parse_agg_item(item)?),
        };
        items.push(resolved);
    }

    Some(items)
}

/// The ordinary (non-distinct) aggregate plans among `items`, in order.
///
/// These drive the shared per-shard partial-aggregate scan and
/// [`validate_agg_col_types`](super::validate_agg_col_types); the distinct items
/// are handled separately as DISTINCT row-scan fan-outs.
///
/// `pub` (not `pub(super)`): this is the boundary that lets callers outside the
/// `pushdown` module (e.g. integration tests) obtain a nameable `Vec<AggregatePlan>`
/// from the opaque [`SingleGroupItem`] returned by [`detect_aggregates`] without
/// ever naming `SingleGroupItem` itself.
pub fn ordinary_plans(items: &[SingleGroupItem]) -> Vec<AggregatePlan> {
    items
        .iter()
        .filter_map(|item| match item {
            SingleGroupItem::Aggregate(plan) => Some(plan.clone()),
            SingleGroupItem::Distinct(_) => None,
        })
        .collect()
}

/// Whether any item is a `COUNT(DISTINCT ...)` needing its own row-scan fan-out.
pub(super) fn has_distinct(items: &[SingleGroupItem]) -> bool {
    items
        .iter()
        .any(|item| matches!(item, SingleGroupItem::Distinct(_)))
}

/// Whether the select list is EXACTLY one `COUNT(DISTINCT <bare column>)` and nothing
/// else — the ONLY shape that fans out to a dedicated DISTINCT row-scan counted by a
/// native `COUNT(DISTINCT "V")` (Case 1). The lone distinct must have a bare-column
/// argument (`dc.column.is_some()`): only a source column has a known exact Exasol
/// type to declare for the per-shard `"V"` value column, so cross-shard dedup stays
/// exact. A lone `COUNT(DISTINCT <expression>)` (`dc.column` is `None`, `dc.arg_expr`
/// set) is deliberately EXCLUDED — it would otherwise have to declare `"V"` as
/// `VARCHAR(2000000)` and rely on the expression's native→`Utf8` cast being injective,
/// which can silently undercount (e.g. two distinct timestamps that print identically
/// after string truncation). Any non-lone distinct shape (more than one distinct, or a
/// distinct alongside an ordinary SUM/MIN/MAX/COUNT/AVG aggregate — Case 2/3) is
/// likewise excluded. Every excluded shape declines the fan-out and routes to the
/// qualified single-table wrapper (`has_distinct && !is_lone_count_distinct` in
/// `mod.rs`), where Exasol evaluates any expression and the DISTINCT natively over
/// exact-typed base columns — and where no composition of several scalar-subquery
/// emitting-UDF calls in one select list would compile anyway (`sqlCode 04000`,
/// "emitting function in expression").
pub(super) fn is_lone_count_distinct(items: &[SingleGroupItem]) -> bool {
    matches!(items, [SingleGroupItem::Distinct(dc)] if dc.column.is_some())
}

/// Extract the column name (uppercase) from the first argument of an aggregate function.
fn column_from_first_arg(args: Option<&Vec<Json>>) -> Option<String> {
    args.and_then(|a| a.first()).and_then(|arg| {
        if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
            arg.get("name")
                .and_then(|n| n.as_str())
                .map(|s| s.to_uppercase())
        } else {
            None
        }
    })
}

/// Resolve an aggregate's single argument into either a bare-column name (the
/// fast path, populating `column`) or a rendered DataFusion SQL fragment
/// (populating `arg_expr`, via `vs_expression::render_expression` — the same
/// seam GROUP BY keys use).
///
/// Returns:
/// - `Some((Some(col), None))` when the argument is a bare `column` node — the
///   bare-column fast path, so the pre-existing exact-type MIN/MAX column
///   lookups keep working.
/// - `Some((None, Some(sql)))` when the argument is any other expression the VS
///   translator can render (e.g. `LENGTH(L_COMMENT)`).
/// - `None` when there is no argument, or the argument cannot be rendered — the
///   caller then declines the aggregate pushdown and falls back to row scanning.
fn arg_column_or_expr(args: Option<&Vec<Json>>) -> Option<(Option<String>, Option<String>)> {
    let arg = args.and_then(|a| a.first())?;
    if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
        return arg
            .get("name")
            .and_then(|n| n.as_str())
            .map(|s| (Some(s.to_uppercase()), None));
    }
    render_expression(arg).ok().map(|sql| (None, Some(sql)))
}

/// Parse a single-group `COUNT(DISTINCT ...)` select-list item into a
/// [`DistinctCount`] fan-out descriptor.
///
/// Handles both `COUNT(DISTINCT col)` (bare-column fast path) and
/// `COUNT(DISTINCT expr)` (rendered argument), mirroring how `COUNT(col)` /
/// `COUNT(expr)` are resolved. Returns `None` when the item is not a distinct
/// `COUNT`, or when its argument cannot be resolved to a column or rendered
/// expression — the single-group caller then defers to [`parse_agg_item`]
/// (which declines every other `distinct: true` item), so grouped
/// `COUNT(DISTINCT)` and other distinct aggregates still fall back to row scan.
fn parse_count_distinct(item: &Json) -> Option<DistinctCount> {
    if item.get("distinct").and_then(|d| d.as_bool()) != Some(true) {
        return None;
    }
    let fn_name = item
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_uppercase();
    if fn_name != "COUNT" {
        return None;
    }
    let args = item.get("arguments").and_then(|a| a.as_array());
    let (column, arg_expr) = arg_column_or_expr(args)?;
    Some(DistinctCount { column, arg_expr })
}

/// Function names resolved through the expression-capable argument path
/// ([`arg_column_or_expr`]): a bare column takes the fast path, any other
/// renderable expression populates `arg_expr`.
const EXPR_CAPABLE_AGG_KINDS: &[(&str, AggKind)] = &[
    ("SUM", AggKind::Sum),
    ("MIN", AggKind::Min),
    ("MAX", AggKind::Max),
    ("AVG", AggKind::Avg),
];

/// STDDEV/VARIANCE family function names, resolved through the bare-column-only
/// argument path ([`column_from_first_arg`]).
const STAT_AGG_KINDS: &[(&str, AggKind)] = &[
    ("STDDEV", AggKind::StddevSamp),
    ("STDDEV_SAMP", AggKind::StddevSamp),
    ("STDDEV_POP", AggKind::StddevPop),
    ("VARIANCE", AggKind::VarSamp),
    ("VAR_SAMP", AggKind::VarSamp),
    ("VAR_POP", AggKind::VarPop),
];

/// Parse a single `function_aggregate` select-list item into an `AggregatePlan`.
///
/// Returns `None` when the item uses `distinct: true` (single-group
/// `COUNT(DISTINCT)` is handled by [`parse_count_distinct`] before this is
/// called; every other distinct — and grouped `COUNT(DISTINCT)` — declines
/// here), when the function name is not one of COUNT, SUM, MIN, MAX, AVG, the
/// STDDEV/VARIANCE family, when a COUNT/SUM/MIN/MAX/AVG argument is a scalar
/// expression the VS translator cannot render, or when a STDDEV/VARIANCE
/// argument is anything but a bare `column`.
///
/// For COUNT/SUM/MIN/MAX/AVG a bare `column` argument takes the fast path
/// (`column` populated, `arg_expr` None); any other renderable expression is
/// carried in `arg_expr` (`column` None).
///
/// The STDDEV/VARIANCE family is bare-column-only: its (cnt, sum, sum_sq)
/// decomposition has no rendered-argument form, so an expression argument
/// declines rather than yielding a plan with neither `column` nor `arg_expr` —
/// a shape that passes type validation on the declared-type default, is given
/// three `EMITS` columns, and then fails inside the scan on an argument that
/// names no field. Declining instead routes the request to the row-scan or
/// qualified-wrapper fallback, where Exasol computes the statistic natively.
///
/// Every plan this returns therefore carries an argument — `column` or
/// `arg_expr` — except `AggKind::Count`, which is `COUNT(*)` and has none.
///
/// The caller must verify `item.type == "function_aggregate"` before calling.
pub(super) fn parse_agg_item(item: &Json) -> Option<AggregatePlan> {
    if item.get("distinct").and_then(|d| d.as_bool()) == Some(true) {
        return None;
    }

    let fn_name = item
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_uppercase();

    let args = item.get("arguments").and_then(|a| a.as_array());

    if fn_name == "COUNT" {
        return Some(match args.and_then(|a| a.first()) {
            // COUNT(*) — no argument: count every row.
            None => AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            // COUNT(col) fast path or COUNT(expr) rendered argument. An argument
            // that renders to neither a bare column nor a translatable expression
            // declines the whole aggregate pushdown (row-scan fallback).
            Some(_) => {
                let (column, arg_expr) = arg_column_or_expr(args)?;
                AggregatePlan {
                    kind: AggKind::CountCol,
                    column,
                    arg_expr,
                }
            }
        });
    }

    if let Some((_, kind)) = EXPR_CAPABLE_AGG_KINDS
        .iter()
        .find(|(name, _)| *name == fn_name)
    {
        let (column, arg_expr) = arg_column_or_expr(args)?;
        return Some(AggregatePlan {
            kind: kind.clone(),
            column,
            arg_expr,
        });
    }

    if let Some((_, kind)) = STAT_AGG_KINDS.iter().find(|(name, _)| *name == fn_name) {
        return Some(AggregatePlan {
            kind: kind.clone(),
            column: Some(column_from_first_arg(args)?),
            arg_expr: None,
        });
    }

    None
}

#[cfg(test)]
#[path = "single_group_agg_tests.rs"]
mod tests;

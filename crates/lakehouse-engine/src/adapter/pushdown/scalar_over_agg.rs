//! Aggregate decomposition primitives, shared by the single-group and the GROUP BY
//! aggregate planners: `function_aggregate` item parsing, the per-`AggKind`
//! partial-to-merge re-aggregation formulas, and scalar-over-aggregate rewriting.
//!
//! A `selectList` item that wraps aggregates in scalar/arithmetic structure (e.g.
//! `ROUND(SUM(x) / COUNT(*), 2)`) is decomposed the same way for both planners: the
//! nested aggregates become per-shard `PARTIAL_*` columns, and the surrounding
//! structure is rendered ONCE over the outer merge wrapper.
//!
//! This module names NEITHER planner, and both name it — that is what keeps the two
//! from drifting apart on any of these primitives, and what keeps the two planners
//! independently readable: neither has to be understood to change the other.

use crate::scan::spec::{AggKind, AggregatePlan, PartialAggColumn, partial_column_name};
use serde_json::Value as Json;
use vs_expression::{render_expression, render_expression_exasol};

use super::support::{cast_to_declared_type, quote_ident};

/// The declared type a plan slot gets when it is reached ONLY through a nested
/// aggregate, which no `selectListDataTypes` entry names directly.
///
/// The single owner of that answer for BOTH aggregate planners. It is numeric rather
/// than the `VARCHAR(2000000)` "Exasol declared nothing" placeholder because the
/// per-plan type list also types the scan's `EMITS` clause: an expression-argument
/// MIN/MAX has no source column to fall back to, so a character type there would turn
/// the merge's `MIN`/`MAX` into a lexicographic extremum of a numeric expression.
pub(super) const NESTED_AGGREGATE_PLAN_TYPE: &str = "DOUBLE PRECISION";

/// Fold an aggregate plan into the shared `plans`/`plan_types` lists, deduplicating
/// by `AggregatePlan` equality (kind + argument) so an aggregate used more than once
/// across the select list — bare AND nested inside a scalar — collapses to ONE
/// `PARTIAL_*` column (decision-log [4]). Returns the plan's slot.
///
/// `declared` is `Some` for a top-level `function_aggregate` select item (its
/// authoritative `selectListDataTypes` type) and `None` for an aggregate seen only
/// nested inside a scalar, which takes [`NESTED_AGGREGATE_PLAN_TYPE`]. A `Some`
/// declared type always wins: it overwrites a slot that a nested occurrence created
/// with the default, so a bare aggregate's output CAST uses the type Exasol declared
/// for it regardless of select-list order.
pub(super) fn fold_aggregate_plan(
    plans: &mut Vec<AggregatePlan>,
    plan_types: &mut Vec<String>,
    plan: AggregatePlan,
    declared: Option<String>,
) -> usize {
    match plans.iter().position(|p| *p == plan) {
        Some(slot) => {
            if let Some(ty) = declared {
                plan_types[slot] = ty;
            }
            slot
        }
        None => {
            let slot = plans.len();
            plans.push(plan);
            plan_types.push(declared.unwrap_or_else(|| NESTED_AGGREGATE_PLAN_TYPE.to_string()));
            slot
        }
    }
}

/// Sentinel `column` name substituted for the i-th nested aggregate while rendering
/// a scalar-over-aggregate node through the `vs-expression` translator. Distinctive
/// and already uppercase so it survives the translator's `column` uppercasing and
/// cannot collide with a real column; the rendered token is later string-replaced
/// with the aggregate's merged `PARTIAL_*` expression.
fn agg_sentinel_name(i: usize) -> String {
    format!("__LH_AGG_MERGE_{i}__")
}

/// The exact SQL token `vs-expression` emits for the i-th aggregate sentinel column
/// (a quoted identifier), used as the string-replacement target.
pub(super) fn agg_sentinel_token(i: usize) -> String {
    quote_ident(&agg_sentinel_name(i))
}

/// Build the sentinel `column` node for the i-th nested aggregate.
pub(super) fn sentinel_column_node(i: usize) -> Json {
    serde_json::json!({ "type": "column", "name": agg_sentinel_name(i) })
}

/// Deep-clone `node`, replacing every nested `function_aggregate` subtree with a
/// sentinel `column` node (`__LH_AGG_MERGE_{i}__`) and collecting the original
/// aggregate nodes in sentinel order. Recursion STOPS at a `function_aggregate`
/// (its arguments are subsumed into the aggregate and rewritten wholesale), so a
/// `column` inside an aggregate (e.g. inside `SUM(CASE … col …)`) is never treated
/// as a residual. `residual_column` is set when a bare `column` appears OUTSIDE any
/// aggregate — the outer merge wrapper exposes only `GK_*`/`PARTIAL_*` columns, so
/// such a node cannot be rendered there and disqualifies the scalar-over-aggregate
/// classification (the request routes to the qualified fallback instead).
pub(super) fn sentinelize_aggregates(
    node: &Json,
    aggregates: &mut Vec<Json>,
    residual_column: &mut bool,
) -> Json {
    match node {
        Json::Object(map) => match map.get("type").and_then(|t| t.as_str()) {
            Some("function_aggregate") => {
                let i = aggregates.len();
                aggregates.push(node.clone());
                sentinel_column_node(i)
            }
            kind => {
                if kind == Some("column") {
                    *residual_column = true;
                }
                let mut out = serde_json::Map::with_capacity(map.len());
                for (key, value) in map {
                    out.insert(
                        key.clone(),
                        sentinelize_aggregates(value, aggregates, residual_column),
                    );
                }
                Json::Object(out)
            }
        },
        Json::Array(items) => Json::Array(
            items
                .iter()
                .map(|v| sentinelize_aggregates(v, aggregates, residual_column))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Classify a `selectList` item as a scalar-over-aggregate: a scalar/arithmetic node
/// wrapping one or more nested `function_aggregate` nodes, every one decomposable via
/// `parse_agg_item` (so `DISTINCT`, an unsupported function, or an untranslatable
/// argument declines), with no bare source `column` outside those aggregates and a
/// residual structure the `vs-expression` translator can render. Returns the nested
/// plans in encounter order, or `None` to decline (→ qualified fallback).
pub(super) fn classify_scalar_over_aggregate(node: &Json) -> Option<Vec<AggregatePlan>> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    // Not a scalar-over-aggregate: no aggregate to decompose, or a source column the
    // outer merge wrapper cannot reference.
    if aggregates.is_empty() || residual_column {
        return None;
    }
    // The residual scalar/arithmetic structure (with aggregates sentinelized) must be
    // renderable by the translator — otherwise the outer wrapper cannot be built.
    render_expression(&sentinel_tree).ok()?;
    aggregates.iter().map(parse_agg_item).collect()
}

/// Render a scalar/arithmetic node over the OUTER merge wrapper: every nested
/// `function_aggregate` is rewritten to its merged `PARTIAL_*` expression (matched to
/// `plans` by `AggregatePlan` equality, then taken from `merged` at that slot), and
/// the surrounding scalar/arithmetic structure is rendered verbatim by the
/// `vs-expression` translator. This is the one merge-rewrite path shared by the
/// single-group select list, the grouped select list, AND a scalar-over-aggregate
/// inside a HAVING (decision-log [2]).
///
/// `merged` is the caller's per-plan merge expressions, positionally aligned with
/// `plans`; taking them as a parameter is what keeps this module independent of
/// either aggregate planner.
///
/// It reuses the translator by SUBSTITUTION rather than re-implementing its scalar
/// arms: each aggregate subtree is replaced with a distinctive sentinel `column`,
/// the tree is rendered once, then each sentinel token is string-replaced with the
/// aggregate's merged expression. This inherits every scalar/arithmetic node type,
/// operator string, and parenthesization the translator supports with zero risk of
/// drifting from it. Returns `None` if the structure cannot be rendered or a nested
/// aggregate is not among `plans` (cannot be merged).
pub(super) fn render_scalar_over_merge(
    node: &Json,
    plans: &[AggregatePlan],
    merged: &[String],
) -> Option<String> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    // Exasol dialect: this SQL is spliced verbatim into the OUTER merge wrapper,
    // which Exasol's own core engine parses — so a character CAST target is
    // rendered length-qualified: `VARCHAR(n)` for a VARCHAR target, `CHAR(n)`/
    // `CHAR(n) ASCII` for a CHAR target — unlike the DataFusion-side
    // renderability check in `classify_scalar_over_aggregate`, which
    // deliberately keeps bare `VARCHAR`.
    let mut sql = render_expression_exasol(&sentinel_tree).ok()?;
    for (i, agg) in aggregates.iter().enumerate() {
        let plan = parse_agg_item(agg)?;
        let slot = plans.iter().position(|p| *p == plan)?;
        sql = sql.replace(&agg_sentinel_token(i), merged.get(slot)?);
    }
    Some(sql)
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
pub(super) fn arg_column_or_expr(
    args: Option<&Vec<Json>>,
) -> Option<(Option<String>, Option<String>)> {
    let arg = args.and_then(|a| a.first())?;
    if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
        return arg
            .get("name")
            .and_then(|n| n.as_str())
            .map(|s| (Some(s.to_uppercase()), None));
    }
    render_expression(arg).ok().map(|sql| (None, Some(sql)))
}

/// Parse a single `function_aggregate` select-list item into an `AggregatePlan`.
///
/// Returns `None` when the item uses `distinct: true` (single-group
/// `COUNT(DISTINCT)` is handled by `parse_count_distinct` before this is
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

/// The three König–Huygens SQL fragments shared by all four statistical merge
/// formulas, rendered over the partial columns of the aggregate at ordinal `i`.
///
/// Held as one value so [`merge_select_items`]' four statistical arms compose
/// them by name instead of re-inlining the text: a variance is
/// `numer / pop_denom` or `numer / samp_denom`, and a standard deviation is
/// [`stddev_of`] applied to either. `numer` carries its own outer parentheses,
/// so no composition adds any.
struct StatMergeFragments {
    numer: String,
    pop_denom: String,
    samp_denom: String,
}

impl StatMergeFragments {
    fn for_ordinal(i: usize) -> Self {
        let cnt = partial_column_name(PartialAggColumn::StatCnt, i);
        let sum = partial_column_name(PartialAggColumn::StatSum, i);
        let sumsq = partial_column_name(PartialAggColumn::StatSumSq, i);
        let pop_denom = format!(r#"NULLIF(SUM("{cnt}"), 0)"#);
        let samp_denom =
            format!(r#"CASE WHEN SUM("{cnt}") <= 1 THEN NULL ELSE SUM("{cnt}") - 1 END"#);
        let numer = format!(r#"(SUM("{sumsq}") - SUM("{sum}") * SUM("{sum}") / {pop_denom})"#);
        Self {
            numer,
            pop_denom,
            samp_denom,
        }
    }
}

/// Render the NULL-preserving square root of a variance expression.
///
/// Adds exactly one parenthesis pair — around the `IS NULL` subject — and none
/// around the `GREATEST` argument; that is the nesting the merge SQL is pinned
/// to. See [`merge_select_items`] for why the `IS NULL` test cannot be folded
/// into `GREATEST`.
fn stddev_of(var: &str) -> String {
    format!("CASE WHEN ({var}) IS NULL THEN NULL ELSE SQRT(GREATEST(0.0, {var})) END")
}

/// Build the outer merge SELECT items following the COLUMN CONTRACT.
///
/// Every partial column name comes from [`partial_column_name`], so a rename can
/// never land here without also landing in the scan's aliases and the `EMITS`
/// clause; this function owns only the re-aggregation formula per [`AggKind`].
///
/// AVG uses `SUM(sum) / NULLIF(SUM(cnt), 0)` — the NULLIF guard ensures division
/// by zero yields NULL rather than an error (Exasol: `x / NULL = NULL`).
///
/// STDDEV/VARIANCE sufficient-statistics reconstruction (König–Huygens identity):
///   numer    = SUM(sumsq) - SUM(sum)² / NULLIF(SUM(cnt), 0)
///   var_pop  = numer / NULLIF(SUM(cnt), 0)          [NULL when cnt = 0]
///   var_samp = numer / (SUM(cnt) - 1)               [NULL when cnt ≤ 1, via CASE]
///
///   stddev_pop/samp = CASE WHEN var IS NULL THEN NULL
///                          ELSE SQRT(GREATEST(0.0, var)) END
///
///   The CASE guard is required because Exasol's `GREATEST(0.0, NULL) = 0.0`
///   (returns the max of non-NULL inputs; only returns NULL if ALL inputs are NULL),
///   so a bare `SQRT(GREATEST(0.0, NULL))` would yield `0.0` instead of NULL for
///   empty tables (N=0, pop) and single-row groups (N=1, samp).
///   The GREATEST(0.0, …) inside the ELSE branch guards against tiny-negative
///   float rounding artifacts that would otherwise cause SQRT to error.
pub(super) fn merge_select_items(aggregates: &[AggregatePlan]) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .map(|(i, plan)| match plan.kind {
            AggKind::Count => {
                let count = partial_column_name(PartialAggColumn::CountStar, i);
                format!(r#"SUM("{count}")"#)
            }
            AggKind::CountCol => {
                let count = partial_column_name(PartialAggColumn::CountArg, i);
                format!(r#"SUM("{count}")"#)
            }
            AggKind::Sum => {
                let sum = partial_column_name(PartialAggColumn::Sum, i);
                format!(r#"SUM("{sum}")"#)
            }
            AggKind::Min => {
                let min = partial_column_name(PartialAggColumn::Min, i);
                format!(r#"MIN("{min}")"#)
            }
            AggKind::Max => {
                let max = partial_column_name(PartialAggColumn::Max, i);
                format!(r#"MAX("{max}")"#)
            }
            AggKind::Avg => {
                let sum = partial_column_name(PartialAggColumn::AvgSum, i);
                let cnt = partial_column_name(PartialAggColumn::AvgCnt, i);
                format!(r#"SUM("{sum}") / NULLIF(SUM("{cnt}"), 0)"#)
            }
            AggKind::VarPop => {
                let stat = StatMergeFragments::for_ordinal(i);
                format!("{} / {}", stat.numer, stat.pop_denom)
            }
            AggKind::VarSamp => {
                let stat = StatMergeFragments::for_ordinal(i);
                format!("{} / {}", stat.numer, stat.samp_denom)
            }
            AggKind::StddevPop => {
                let stat = StatMergeFragments::for_ordinal(i);
                stddev_of(&format!("{} / {}", stat.numer, stat.pop_denom))
            }
            AggKind::StddevSamp => {
                let stat = StatMergeFragments::for_ordinal(i);
                stddev_of(&format!("{} / {}", stat.numer, stat.samp_denom))
            }
        })
        .collect()
}

/// Build the outer merge SELECT items, each cast to its Exasol-declared result type.
///
/// The merge expression (e.g. `SUM("PARTIAL_count_0")` over DECIMAL(20,0) partials →
/// DECIMAL(31,0)) must match the type Exasol declared for that select-list column
/// (COUNT(score) → DECIMAL(18,0)); Exasol strictly validates the pushdown output
/// column types. When no declared type is available (or it is VARCHAR(2000000)),
/// the merge expression is emitted uncast.
pub(super) fn cast_merge_items(
    aggregates: &[AggregatePlan],
    aggregate_types: &[String],
) -> Vec<String> {
    merge_select_items(aggregates)
        .into_iter()
        .enumerate()
        .map(|(i, expr)| cast_to_declared_type(&expr, aggregate_types.get(i).map(String::as_str)))
        .collect()
}

#[cfg(test)]
#[path = "scalar_over_agg_tests.rs"]
mod tests;

//! Aggregate decomposition primitives shared by the single-group and GROUP BY
//! planners. This module names neither planner, so the two cannot drift apart.

use crate::scan::spec::{AggKind, AggregatePlan, PartialAggColumn, partial_column_name};
use serde_json::Value as Json;
use vs_expression::{render_expression, render_expression_exasol};

use super::support::{cast_to_declared_type, quote_ident};

/// Declared type of a plan slot reached only through a nested aggregate. Numeric,
/// not `VARCHAR`, because the per-plan type also types the scan's `EMITS`: a
/// character type would make an expression-argument MIN/MAX lexicographic.
pub(super) const NESTED_AGGREGATE_PLAN_TYPE: &str = "DOUBLE PRECISION";

/// Deduplicates by `AggregatePlan` equality so an aggregate used bare and nested
/// collapses to one `PARTIAL_*` column (decision-log [4]). A `Some` declared type
/// always overwrites a slot a nested occurrence created with the default.
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

/// Already uppercase so it survives the translator's column uppercasing, and
/// distinctive so it cannot collide with a real column.
fn agg_sentinel_name(i: usize) -> String {
    format!("__LH_AGG_MERGE_{i}__")
}

pub(super) fn agg_sentinel_token(i: usize) -> String {
    quote_ident(&agg_sentinel_name(i))
}

pub(super) fn sentinel_column_node(i: usize) -> Json {
    serde_json::json!({ "type": "column", "name": agg_sentinel_name(i) })
}

/// Recursion stops at a `function_aggregate`, so a column inside an aggregate is
/// not residual. `residual_column` flags a bare column outside any aggregate: the
/// merge wrapper exposes only `GK_*`/`PARTIAL_*`, so such a node cannot render there.
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

pub(super) fn classify_scalar_over_aggregate(node: &Json) -> Option<Vec<AggregatePlan>> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    if aggregates.is_empty() || residual_column {
        return None;
    }
    render_expression(&sentinel_tree).ok()?;
    aggregates.iter().map(parse_agg_item).collect()
}

/// Reuses the `vs-expression` translator by substitution (sentinel columns, then
/// string-replace with `merged` at each plan's slot) so it cannot drift from the
/// translator's scalar rendering. Shared with HAVING rendering (decision-log [2]).
pub(super) fn render_scalar_over_merge(
    node: &Json,
    plans: &[AggregatePlan],
    merged: &[String],
) -> Option<String> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    // Exasol dialect: spliced into SQL that Exasol itself parses, so character CAST
    // targets are length-qualified (the DataFusion-side check keeps bare `VARCHAR`).
    let mut sql = render_expression_exasol(&sentinel_tree).ok()?;
    for (i, agg) in aggregates.iter().enumerate() {
        let plan = parse_agg_item(agg)?;
        let slot = plans.iter().position(|p| *p == plan)?;
        sql = sql.replace(&agg_sentinel_token(i), merged.get(slot)?);
    }
    Some(sql)
}

const EXPR_CAPABLE_AGG_KINDS: &[(&str, AggKind)] = &[
    ("SUM", AggKind::Sum),
    ("MIN", AggKind::Min),
    ("MAX", AggKind::Max),
    ("AVG", AggKind::Avg),
];

const STAT_AGG_KINDS: &[(&str, AggKind)] = &[
    ("STDDEV", AggKind::StddevSamp),
    ("STDDEV_SAMP", AggKind::StddevSamp),
    ("STDDEV_POP", AggKind::StddevPop),
    ("VARIANCE", AggKind::VarSamp),
    ("VAR_SAMP", AggKind::VarSamp),
    ("VAR_POP", AggKind::VarPop),
];

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

/// `None` when there is no argument or it cannot be rendered.
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

/// `None` for any `distinct: true` item, an unsupported function, or an
/// unrenderable argument. STDDEV/VARIANCE accept only a bare column: their
/// (cnt, sum, sum_sq) decomposition has no rendered-argument form, and a plan with
/// no argument would fail inside the scan.
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
            None => AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
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

/// The `IS NULL` guard is redundant with `GREATEST`'s own NULL propagation but is
/// kept because golden fixtures pin this SQL byte-for-byte.
fn stddev_of(var: &str) -> String {
    format!("CASE WHEN ({var}) IS NULL THEN NULL ELSE SQRT(GREATEST(0.0, {var})) END")
}

/// Partial column names come only from [`partial_column_name`], so the scan aliases,
/// `EMITS`, and this merge cannot disagree. STDDEV/VARIANCE use the König–Huygens
/// identity; `GREATEST(0.0, …)` guards SQRT against tiny negative float rounding.
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

/// Exasol strictly validates pushdown output column types, so each merge expression
/// is cast to its declared type (uncast when none or the `VARCHAR(2000000)` default).
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

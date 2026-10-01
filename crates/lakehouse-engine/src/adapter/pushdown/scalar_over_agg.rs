//! Aggregate decomposition primitives shared by the single-group and GROUP BY
//! planners. This module names neither planner, so the two cannot drift apart.

use crate::scan::spec::{AggKind, AggregatePlan, PartialAggColumn, partial_column_name};
use crate::types::mapping::{ExaTypeClass, classify_exa_type, exasol_type_from_json};
use serde_json::Value as Json;
use vs_expression::{render_expression, render_expression_exasol, scalar_fn_returns_character};

use super::support::{cast_to_declared_type, quote_ident};

/// Declared type of a nested-only plan slot whose argument is numeric or untyped.
/// Numeric, not `VARCHAR`, because the per-plan type also types the scan's `EMITS`: a
/// character type would make a numeric expression-argument MIN/MAX lexicographic.
pub(super) const NESTED_AGGREGATE_PLAN_TYPE: &str = "DOUBLE PRECISION";

/// A character value with no declared length.
const CHARACTER_PARTIAL_TYPE: &str = "VARCHAR(2000000)";

/// Deduplicates by `AggregatePlan` equality so an aggregate used bare and nested
/// collapses to one `PARTIAL_*` column (decision-log [4]). A `Some` declared type
/// always overwrites a slot a nested occurrence created.
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

/// A nested occurrence types only a slot it creates, so it never overwrites the
/// declared type of a top-level occurrence, whichever comes first.
pub(super) fn fold_nested_aggregate_plan(
    plans: &mut Vec<AggregatePlan>,
    plan_types: &mut Vec<String>,
    plan: AggregatePlan,
    partial_type: String,
) {
    if !plans.contains(&plan) {
        fold_aggregate_plan(plans, plan_types, plan, Some(partial_type));
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

fn decomposable_aggregates(node: &Json) -> Option<Vec<Json>> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    if aggregates.is_empty() || residual_column {
        return None;
    }
    render_expression(&sentinel_tree).ok()?;
    Some(aggregates)
}

pub(super) fn classify_scalar_over_aggregate(node: &Json) -> Option<Vec<AggregatePlan>> {
    decomposable_aggregates(node)?
        .iter()
        .map(parse_agg_item)
        .collect()
}

/// [`classify_scalar_over_aggregate`] with each nested plan paired with the type its
/// partial is emitted as when no top-level occurrence declares one.
pub(super) fn classify_typed_scalar_over_aggregate(
    node: &Json,
    col_types: &[(String, String)],
) -> Option<Vec<(AggregatePlan, String)>> {
    decomposable_aggregates(node)?
        .iter()
        .map(|agg| Some((parse_agg_item(agg)?, nested_partial_type(agg, col_types))))
        .collect()
}

/// A MIN/MAX partial carries its argument's own type, so a character, temporal, or
/// boolean argument must not take the numeric default: the merge would compare text
/// as numbers, or fail to cast the value (#227). Every other kind's partial is numeric.
fn nested_partial_type(aggregate: &Json, col_types: &[(String, String)]) -> String {
    let is_min_max = aggregate
        .get("name")
        .and_then(|n| n.as_str())
        .is_some_and(|n| n.eq_ignore_ascii_case("MIN") || n.eq_ignore_ascii_case("MAX"));
    node_arguments(aggregate)
        .first()
        .filter(|_| is_min_max)
        .and_then(|arg| non_numeric_type(arg, col_types))
        .unwrap_or_else(|| NESTED_AGGREGATE_PLAN_TYPE.to_string())
}

fn node_arguments(node: &Json) -> &[Json] {
    node.get("arguments")
        .and_then(|a| a.as_array())
        .map_or(&[], Vec::as_slice)
}

/// `None` for a numeric expression or one whose type the node does not determine,
/// either of which keeps the numeric default.
fn non_numeric_type(node: &Json, col_types: &[(String, String)]) -> Option<String> {
    match node.get("type").and_then(|t| t.as_str())? {
        "column" => {
            let name = node.get("name").and_then(|n| n.as_str())?.to_uppercase();
            let (_, ty) = col_types.iter().find(|(n, _)| *n == name)?;
            Some(ty.clone()).filter(|ty| is_non_numeric_partial_type(ty))
        }
        "function_scalar_cast" => cast_target_type(node),
        "literal_string" => Some(CHARACTER_PARTIAL_TYPE.to_string()),
        "literal_date" => Some("DATE".to_string()),
        "literal_timestamp" => Some("TIMESTAMP".to_string()),
        "literal_bool" => Some("BOOLEAN".to_string()),
        kind if kind.starts_with("predicate_") => Some("BOOLEAN".to_string()),
        "function_scalar_case" => unified_type(node.get("results")?.as_array()?, col_types),
        "function_scalar" => scalar_function_type(node, col_types),
        _ => None,
    }
}

fn cast_target_type(node: &Json) -> Option<String> {
    Some(exasol_type_from_json(node.get("dataType")?)).filter(|ty| is_non_numeric_partial_type(ty))
}

fn scalar_function_type(node: &Json, col_types: &[(String, String)]) -> Option<String> {
    let name = node.get("name").and_then(|n| n.as_str())?.to_uppercase();
    if scalar_fn_returns_character(&name) {
        return Some(CHARACTER_PARTIAL_TYPE.to_string());
    }
    let args = node_arguments(node);
    let operand_type = |index: usize| {
        args.get(index)
            .and_then(|arg| non_numeric_type(arg, col_types))
    };
    let temporal_operand_type =
        |index: usize| operand_type(index).filter(|ty| is_temporal_type(ty));
    match name.as_str() {
        "REGEXP_LIKE" => Some("BOOLEAN".to_string()),
        "TO_DATE" => Some("DATE".to_string()),
        "TO_TIMESTAMP" => Some("TIMESTAMP".to_string()),
        "CAST" => cast_target_type(node),
        "NULLIF" => operand_type(0),
        "DATE_TRUNC" => temporal_operand_type(1),
        "ROUND" | "TRUNC" => temporal_operand_type(0),
        "GREATEST" | "LEAST" => unified_type(args, col_types),
        // Arguments interleave [cond, result, ...] with an optional trailing ELSE.
        "CASE" => {
            let trailing_else = args.last().filter(|_| args.len() % 2 == 1);
            unified_type(
                args.iter().skip(1).step_by(2).chain(trailing_else),
                col_types,
            )
        }
        _ => None,
    }
}

/// Branches of differing character types take the unbounded character type, since
/// Exasol widens them to one; any other disagreement is left undetermined.
fn unified_type<'a>(
    operands: impl IntoIterator<Item = &'a Json>,
    col_types: &[(String, String)],
) -> Option<String> {
    let types: Vec<String> = operands
        .into_iter()
        .filter_map(|operand| non_numeric_type(operand, col_types))
        .collect();
    let first = types.first()?;
    if types.iter().all(|ty| ty == first) {
        return Some(first.clone());
    }
    types
        .iter()
        .any(|ty| is_character_type(ty))
        .then(|| CHARACTER_PARTIAL_TYPE.to_string())
}

fn is_character_type(ty: &str) -> bool {
    classify_exa_type(ty) == ExaTypeClass::Character
}

fn is_temporal_type(ty: &str) -> bool {
    ty == "DATE" || ty.starts_with("TIMESTAMP")
}

/// Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as an `EMITS` type (sqlCode 22002).
fn is_non_numeric_partial_type(ty: &str) -> bool {
    (is_character_type(ty) || is_temporal_type(ty) || ty == "BOOLEAN")
        && ty != "TIMESTAMP WITH LOCAL TIME ZONE"
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

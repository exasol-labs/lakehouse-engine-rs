//! Aggregate decomposition primitives shared by the single-group and GROUP BY
//! planners. This module names neither planner, so the two cannot drift apart.

use crate::scan::spec::{AggKind, AggregatePlan, PartialAggColumn, partial_column_name};
use serde_json::Value as Json;
use vs_expression::{
    ColumnNode, SqlFragment, exact_expression_type, render_exasol_fragment,
    render_expression_exasol,
};

use super::support::{ColumnTypes, ScanTranslator, quote_ident};
use crate::scan::spec::ColumnRef;
use crate::types::exasol_type::{ExasolType, MAX_DECIMAL_PRECISION};

/// Deduplicates by `AggregatePlan` equality so an aggregate used bare and nested
/// collapses to one `PARTIAL_*` column (decision-log [4]). A `Some` declared type
/// always overwrites the slot's type.
pub(super) fn fold_aggregate_plan(
    plans: &mut Vec<AggregatePlan>,
    plan_types: &mut Vec<Option<ExasolType>>,
    plan: AggregatePlan,
    declared: Option<ExasolType>,
) -> usize {
    match plans.iter().position(|p| *p == plan) {
        Some(slot) => {
            if declared.is_some() {
                plan_types[slot] = declared;
            }
            slot
        }
        None => {
            let slot = plans.len();
            plans.push(plan);
            plan_types.push(declared);
            slot
        }
    }
}

/// A slot a top-level item declares keeps that type; a nested-only slot takes
/// [`nested_aggregate_type`], so the outcome does not depend on select-list order.
pub(super) fn fold_nested_plans(
    plans: &mut Vec<AggregatePlan>,
    plan_types: &mut Vec<Option<ExasolType>>,
    nested: &[(AggregatePlan, Json)],
    scan: ScanTranslator<'_>,
) {
    for (plan, agg) in nested {
        let slot = fold_aggregate_plan(plans, plan_types, plan.clone(), None);
        if plan_types[slot].is_none() {
            plan_types[slot] = nested_aggregate_type(agg, plan, scan);
        }
    }
}

/// Exasol's result type of an aggregate the select list does not type. `None` when it depends on
/// an argument without an exact type: a guessed partial type breaks the merge (#476).
pub(super) fn nested_aggregate_type(
    agg: &Json,
    plan: &AggregatePlan,
    scan: ScanTranslator<'_>,
) -> Option<ExasolType> {
    match plan.kind {
        AggKind::Count | AggKind::CountCol => Some(ExasolType::decimal(18, 0)),
        AggKind::Sum | AggKind::Min | AggKind::Max => {
            let arg = agg.get("arguments")?.as_array()?.first()?;
            let arg_type = exact_expression_type(arg, scan.columns())?;
            if plan.kind == AggKind::Sum {
                sum_result_type(arg_type)
            } else {
                Some(arg_type)
            }
        }
        _ => Some(ExasolType::DoublePrecision),
    }
}

/// Exasol types `SUM` over `DECIMAL(p,s)` as `DECIMAL(min(p+11, 36), s)`.
fn sum_result_type(arg_type: ExasolType) -> Option<ExasolType> {
    match arg_type {
        ExasolType::DoublePrecision => Some(arg_type),
        ExasolType::Decimal(decimal) => Some(ExasolType::decimal(
            (decimal.precision() + 11).min(MAX_DECIMAL_PRECISION),
            decimal.scale(),
        )),
        _ => None,
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
    ColumnNode::to_json(&agg_sentinel_name(i), None, None)
}

/// Copies `node`, replacing each object subtree for which `substitute` returns `Some`; the walk
/// does not descend into a replaced subtree.
pub(super) fn substitute_subtrees(
    node: &Json,
    substitute: &mut impl FnMut(&Json) -> Option<Json>,
) -> Json {
    match node {
        Json::Object(map) => substitute(node).unwrap_or_else(|| {
            Json::Object(
                map.iter()
                    .map(|(key, value)| (key.clone(), substitute_subtrees(value, substitute)))
                    .collect(),
            )
        }),
        Json::Array(items) => Json::Array(
            items
                .iter()
                .map(|value| substitute_subtrees(value, substitute))
                .collect(),
        ),
        other => other.clone(),
    }
}

pub(super) fn is_aggregate_node(node: &Json) -> bool {
    node.get("type").and_then(Json::as_str) == Some("function_aggregate")
}

/// A column inside an aggregate is not residual. `residual_column` flags a bare column outside
/// any aggregate: the merge wrapper exposes only `GK_*`/`PARTIAL_*`, so it cannot render there.
pub(super) fn sentinelize_aggregates(
    node: &Json,
    aggregates: &mut Vec<Json>,
    residual_column: &mut bool,
) -> Json {
    substitute_subtrees(node, &mut |subtree| {
        if is_aggregate_node(subtree) {
            aggregates.push(subtree.clone());
            return Some(sentinel_column_node(aggregates.len() - 1));
        }
        if ColumnNode::parse(subtree).is_some() {
            *residual_column = true;
        }
        None
    })
}

/// The scalar runs in Exasol over the merged aggregates, so it is checked in the Exasol dialect.
/// Each plan comes with its aggregate node, which [`nested_aggregate_type`] types.
pub(super) fn classify_scalar_over_aggregate(
    node: &Json,
    scan: ScanTranslator<'_>,
) -> Option<Vec<(AggregatePlan, Json)>> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    if aggregates.is_empty() || residual_column {
        return None;
    }
    render_expression_exasol(&sentinel_tree).ok()?;
    aggregates
        .into_iter()
        .map(|agg| Some((parse_agg_item(&agg, scan)?, agg)))
        .collect()
}

/// Renders through the translator with sentinel columns, then substitutes each `merged` slot, so
/// it cannot drift from the translator; the type comes from the scalar's own CAST.
pub(super) fn render_scalar_over_merge(
    node: &Json,
    plans: &[AggregatePlan],
    merged: &[String],
    scan: ScanTranslator<'_>,
) -> Option<SqlFragment> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    // Exasol dialect: spliced into SQL that Exasol itself parses, so character CAST
    // targets are length-qualified.
    let SqlFragment { mut sql, ty } =
        render_exasol_fragment(&sentinel_tree, scan.columns()).ok()?;
    for (i, agg) in aggregates.iter().enumerate() {
        let plan = parse_agg_item(agg, scan)?;
        let slot = plans.iter().position(|p| *p == plan)?;
        sql = sql.replace(&agg_sentinel_token(i), merged.get(slot)?);
    }
    Some(SqlFragment::new(sql, ty))
}

const EXPR_CAPABLE_AGG_KINDS: &[(&str, AggKind)] = &[
    ("SUM", AggKind::Sum),
    ("MIN", AggKind::Min),
    ("MAX", AggKind::Max),
    ("AVG", AggKind::Avg),
];

const STAT_AGG_KINDS: &[(&str, AggKind)] = &[
    ("STDDEV", AggKind::Stddev),
    ("STDDEV_SAMP", AggKind::StddevSamp),
    ("STDDEV_POP", AggKind::StddevPop),
    ("VARIANCE", AggKind::Variance),
    ("VAR_SAMP", AggKind::VarSamp),
    ("VAR_POP", AggKind::VarPop),
];

fn column_from_first_arg(args: Option<&Vec<Json>>) -> Option<String> {
    ColumnNode::parse(args?.first()?).map(|column| column.upper_name())
}

/// `None` when there is no argument or it cannot be rendered.
pub(super) fn arg_column_or_expr(
    args: Option<&Vec<Json>>,
    scan: ScanTranslator<'_>,
) -> Option<(Option<String>, Option<String>)> {
    let arg = args.and_then(|a| a.first())?;
    if let Some(column) = ColumnNode::parse(arg) {
        return Some((Some(column.upper_name()), None));
    }
    scan.expression(arg).map(|arg| (None, Some(arg.sql)))
}

/// `None` for any `distinct: true` item, an unsupported function, or an
/// unrenderable argument. STDDEV/VARIANCE accept only a bare column: their
/// (cnt, mean, m2) decomposition has no rendered-argument form, and a plan with
/// no argument would fail inside the scan.
pub(super) fn parse_agg_item(item: &Json, scan: ScanTranslator<'_>) -> Option<AggregatePlan> {
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
                let (column, arg_expr) = arg_column_or_expr(args, scan)?;
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
        let (column, arg_expr) = arg_column_or_expr(args, scan)?;
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

fn stat_grand_mean_name(i: usize) -> String {
    format!("STAT_GMEAN_{i}")
}

/// Adds each stat aggregate's grand mean over the partial rows of its group. Shifting by one
/// shard's mean keeps it exact when all shard means are equal, which `SUM(n * mean) / N` is not.
pub(super) fn with_stat_grand_means(
    fan_out: &str,
    aggregates: &[AggregatePlan],
    partition_keys: &[String],
) -> String {
    let stats: Vec<usize> = (0..aggregates.len())
        .filter(|&i| aggregates[i].kind.is_stat())
        .collect();
    if stats.is_empty() {
        return fan_out.to_string();
    }
    let window = if partition_keys.is_empty() {
        "OVER ()".to_string()
    } else {
        format!("OVER (PARTITION BY {})", partition_keys.join(", "))
    };
    let mut refs = Vec::new();
    let mut grand_means = Vec::new();
    for i in stats {
        let cnt = partial_column_name(PartialAggColumn::StatCnt, i);
        let mean = partial_column_name(PartialAggColumn::StatMean, i);
        let shift = format!("STAT_REF_{i}");
        refs.push(format!(r#"MIN("{mean}") {window} AS "{shift}""#));
        grand_means.push(format!(
            r#""{shift}" + SUM("{cnt}" * ("{mean}" - "{shift}")) {window} / NULLIF(SUM("{cnt}") {window}, 0) AS "{}""#,
            stat_grand_mean_name(i)
        ));
    }
    format!(
        "SELECT q.*, {} FROM (SELECT p.*, {} FROM ({fan_out}) p) q",
        grand_means.join(", "),
        refs.join(", ")
    )
}

/// Chan's parallel merge of (cnt, mean, m2). Every term is non-negative, so SQRT needs no guard;
/// expanding the deviation term into `Σ n·mean² - N·g²` would bring back the cancellation.
fn stat_merge(kind: &AggKind, i: usize) -> String {
    let cnt = partial_column_name(PartialAggColumn::StatCnt, i);
    let mean = partial_column_name(PartialAggColumn::StatMean, i);
    let m2 = partial_column_name(PartialAggColumn::StatM2, i);
    let gmean = stat_grand_mean_name(i);
    let n = format!(r#"SUM("{cnt}")"#);
    let m2 = format!(
        r#"(SUM("{m2}") + SUM("{cnt}" * ("{mean}" - "{gmean}") * ("{mean}" - "{gmean}")))"#
    );
    let var_pop = format!("{m2} / NULLIF({n}, 0)");
    let var_samp = format!("{m2} / CASE WHEN {n} <= 1 THEN NULL ELSE {n} - 1 END");
    let variance = format!("CASE WHEN {n} = 1 THEN 0 ELSE {m2} / ({n} - 1) END");
    match kind {
        AggKind::VarPop => var_pop,
        AggKind::VarSamp => var_samp,
        AggKind::Variance => variance,
        AggKind::StddevPop => format!("SQRT({var_pop})"),
        AggKind::StddevSamp => format!("SQRT({var_samp})"),
        AggKind::Stddev => format!("SQRT({variance})"),
        other => unreachable!("{other:?} is not a stat aggregate"),
    }
}

/// Partial column names come only from [`partial_column_name`], so the scan aliases,
/// `EMITS`, and this merge cannot disagree. Stat merges read the grand means that
/// [`with_stat_grand_means`] adds to the FROM.
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
            AggKind::VarPop
            | AggKind::VarSamp
            | AggKind::Variance
            | AggKind::StddevPop
            | AggKind::StddevSamp
            | AggKind::Stddev => stat_merge(&plan.kind, i),
        })
        .collect()
}

/// Deduplicated aggregate plans, each with its native type and its merge expression, typed once
/// per request.
#[derive(Debug, Clone)]
pub struct TypedPlans {
    pub plans: Vec<AggregatePlan>,
    /// Aligned with `plans`; also types the scan's `EMITS`.
    pub types: Vec<ExasolType>,
    /// Aligned with `plans`, cast before any outer scalar applies so `SUM(a) / 2` stays exact.
    pub merged: Vec<String>,
}

impl TypedPlans {
    /// `None` when any slot has no exact type: a guessed partial type breaks the merge (#476).
    pub(super) fn new(
        plans: Vec<AggregatePlan>,
        types: Vec<Option<ExasolType>>,
        col_types: &ColumnTypes,
    ) -> Option<Self> {
        let types: Vec<ExasolType> = types.into_iter().collect::<Option<_>>()?;
        let merged = cast_merge_items(&plans, &types, col_types);
        Some(Self {
            plans,
            types,
            merged,
        })
    }
}

/// Each merged aggregate in its native type, so an outer scalar, HAVING, or the output sees the
/// type Exasol would (#476).
pub(super) fn cast_merge_items(
    aggregates: &[AggregatePlan],
    aggregate_types: &[ExasolType],
    col_types: &ColumnTypes,
) -> Vec<String> {
    debug_assert_eq!(aggregates.len(), aggregate_types.len());
    merge_select_items(aggregates)
        .into_iter()
        .zip(aggregates)
        .zip(aggregate_types)
        .map(|((sql, plan), &native)| {
            SqlFragment::new(sql, merged_type(plan, native, col_types)).as_type(native)
        })
        .collect()
}

/// The type of a merge expression as written: MIN and MAX keep the type their partial column is
/// emitted in. `None` for the rest, whose `SUM` or arithmetic Exasol types by its own rules.
fn merged_type(
    plan: &AggregatePlan,
    native: ExasolType,
    col_types: &ColumnTypes,
) -> Option<ExasolType> {
    match plan.kind.partial_columns() {
        [column @ (PartialAggColumn::Min | PartialAggColumn::Max)] => {
            Some(partial_column_type(plan, *column, col_types, Some(native)))
        }
        _ => None,
    }
}

const PARTIAL_COUNT_TYPE: ExasolType = ExasolType::decimal(20, 0);

/// The type the scan emits `column` of `plan` in. Counts are DECIMAL(20,0); SUM widens
/// DECIMAL(p,s) to DECIMAL(36,s) against overflow; AVG and stat sums are DOUBLE PRECISION
/// (reconstructed in floating point); MIN/MAX use the argument's exact type. `declared` is the
/// sole type source for an expression-argument aggregate, which has no source column.
pub(super) fn partial_column_type(
    plan: &AggregatePlan,
    column: PartialAggColumn,
    col_types: &ColumnTypes,
    declared: Option<ExasolType>,
) -> ExasolType {
    let argument_type = || {
        col_type_for(
            plan.column.as_deref(),
            plan.arg_expr.as_deref(),
            col_types,
            declared,
        )
    };
    match column {
        PartialAggColumn::CountStar
        | PartialAggColumn::CountArg
        | PartialAggColumn::AvgCnt
        | PartialAggColumn::StatCnt => PARTIAL_COUNT_TYPE,
        PartialAggColumn::AvgSum | PartialAggColumn::StatMean | PartialAggColumn::StatM2 => {
            ExasolType::DoublePrecision
        }
        PartialAggColumn::Sum => sum_emit_type(argument_type()),
        PartialAggColumn::Min | PartialAggColumn::Max => argument_type(),
    }
}

/// An expression-argument aggregate has no source column, so it takes `declared`.
pub(super) fn col_type_for(
    column: Option<&str>,
    arg_expr: Option<&str>,
    col_types: &ColumnTypes,
    declared: Option<ExasolType>,
) -> ExasolType {
    if column.is_none()
        && arg_expr.is_some()
        && let Some(ty) = declared
    {
        return ty;
    }
    column
        .and_then(|col| col_types.get(&ColumnRef::from(col)))
        .map_or(ExasolType::DoublePrecision, |column_type| {
            column_type.exa_type
        })
}

fn sum_emit_type(col_ty: ExasolType) -> ExasolType {
    match col_ty {
        ExasolType::Decimal(decimal) => ExasolType::decimal(MAX_DECIMAL_PRECISION, decimal.scale()),
        _ => ExasolType::DoublePrecision,
    }
}

#[cfg(test)]
#[path = "scalar_over_agg_tests.rs"]
mod tests;

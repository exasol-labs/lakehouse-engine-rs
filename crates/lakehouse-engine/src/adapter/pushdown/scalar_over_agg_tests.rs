use super::*;
use crate::adapter::pushdown::test_support::{
    AGGREGATED, EXASOL_TYPE_SPELLINGS, agg_item, agg_plan, ast, col, count_star, float_div,
    round_of, scalar_fn, typed_scan,
};
use serde_json::Value as Json;

/// The translator these node-level units take: the aggregate fixtures' columns and the key a
/// grouped caller counts.
fn scan() -> ScanTranslator<'static> {
    typed_scan(
        &[
            AGGREGATED.as_slice(),
            &[("L_ORDERKEY", ExasolType::decimal(20, 0))],
        ]
        .concat(),
    )
}

fn sum_x() -> Json {
    agg_item("SUM", Some("X"), false)
}

fn sum_over_count() -> Json {
    float_div(sum_x(), count_star())
}

fn plan_of(node: &Json) -> AggregatePlan {
    agg_plan(node, scan()).expect("test fixture aggregate must parse to a plan")
}

fn classify(node: &Json) -> Option<Vec<(AggregatePlan, Expr)>> {
    classify_scalar_over_aggregate(&ast(node), scan())
}

/// The SQL `node` renders over the merge wrapper, `None` when it declines.
fn render(node: &Json, plans: &[AggregatePlan], merged: &[&str]) -> Option<String> {
    let merged: Vec<String> = merged.iter().map(|sql| sql.to_string()).collect();
    render_scalar_over_merge(&ast(node), plans, &merged, scan()).map(|rendered| rendered.sql)
}

#[test]
fn fold_collapses_structurally_equal_plans_into_one_slot() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    let first = fold_aggregate_plan(&mut plans, &mut types, plan_of(&sum_x()), None);
    let second = fold_aggregate_plan(&mut plans, &mut types, plan_of(&sum_x()), None);

    assert_eq!(first, second, "the same aggregate must reuse its slot");
    assert_eq!(
        plans.len(),
        1,
        "one PARTIAL_* column, not one per occurrence"
    );
    assert_eq!(types.len(), plans.len(), "types stay aligned with plans");
}

#[test]
fn fold_keeps_structurally_different_aggregates_in_separate_slots() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    let sum = fold_aggregate_plan(&mut plans, &mut types, plan_of(&sum_x()), None);
    let count = fold_aggregate_plan(&mut plans, &mut types, plan_of(&count_star()), None);

    assert_eq!((sum, count), (0, 1));
    assert_eq!(plans.len(), 2);
}

#[test]
fn fold_leaves_a_nested_only_occurrence_untyped() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    fold_aggregate_plan(&mut plans, &mut types, plan_of(&sum_x()), None);

    assert_eq!(types, vec![None]);
}

#[test]
fn fold_declared_type_overwrites_a_slot_created_by_a_nested_occurrence() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    fold_aggregate_plan(&mut plans, &mut types, plan_of(&sum_x()), None);
    let slot = fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&sum_x()),
        Some(ExasolType::decimal(18, 2)),
    );

    assert_eq!(slot, 0);
    assert_eq!(
        types,
        vec![Some(ExasolType::decimal(18, 2))],
        "a top-level occurrence's declared type must fill the slot a nested occurrence left untyped"
    );
}

#[test]
fn fold_keeps_a_declared_type_when_a_nested_occurrence_follows_it() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&sum_x()),
        Some(ExasolType::decimal(18, 2)),
    );
    fold_aggregate_plan(&mut plans, &mut types, plan_of(&sum_x()), None);

    assert_eq!(types, vec![Some(ExasolType::decimal(18, 2))]);
}

#[test]
fn sentinelize_replaces_each_aggregate_and_collects_it_in_encounter_order() {
    let node = round_of(sum_over_count(), 2);
    let mut aggregates = Vec::new();
    let mut residual = false;

    let tree = sentinelize_aggregates(&ast(&node), &mut aggregates, &mut residual);

    assert!(!residual, "no bare column sits outside the aggregates");
    assert_eq!(aggregates, vec![ast(&sum_x()), ast(&count_star())]);
    let quotient = tree.children()[0];
    assert_eq!(
        quotient.children(),
        [&sentinel_column_node(0), &sentinel_column_node(1)]
    );
}

#[test]
fn sentinelize_does_not_treat_a_column_inside_an_aggregate_as_residual() {
    let mut aggregates = Vec::new();
    let mut residual = false;

    sentinelize_aggregates(&ast(&round_of(sum_x(), 2)), &mut aggregates, &mut residual);

    assert!(!residual);
}

#[test]
fn sentinelize_flags_a_bare_column_outside_any_aggregate() {
    let node = scalar_fn("ADD", [sum_x(), col("Y")]);
    let mut aggregates = Vec::new();
    let mut residual = false;

    sentinelize_aggregates(&ast(&node), &mut aggregates, &mut residual);

    assert!(
        residual,
        "the outer merge wrapper cannot reference a source column"
    );
}

#[test]
fn classify_returns_the_nested_plans_in_encounter_order() {
    let node = round_of(sum_over_count(), 2);

    let plans = classify(&node)
        .expect("ROUND(SUM(x) / COUNT(*), 2) must classify as a scalar-over-aggregate");

    assert_eq!(
        plans,
        vec![
            (plan_of(&sum_x()), ast(&sum_x())),
            (plan_of(&count_star()), ast(&count_star())),
        ]
    );
}

#[test]
fn classify_declines_a_node_with_no_nested_aggregate() {
    assert!(
        classify(&round_of(col("X"), 2)).is_none(),
        "a plain scalar over a column is not a scalar-over-aggregate"
    );
}

#[test]
fn classify_accepts_a_bare_aggregate_as_its_own_single_plan() {
    let plans = classify(&sum_x()).expect("a bare aggregate is structurally decomposable");

    assert_eq!(plans, vec![(plan_of(&sum_x()), ast(&sum_x()))]);
}

#[test]
fn classify_declines_a_distinct_inner_aggregate() {
    assert!(
        classify(&round_of(agg_item("SUM", Some("X"), true), 2)).is_none(),
        "an undecomposable DISTINCT inner aggregate must decline the whole item"
    );
}

#[test]
fn classify_declines_a_residual_column_outside_the_aggregate() {
    let node = scalar_fn("ADD", [sum_x(), col("Y")]);

    assert!(classify(&node).is_none());
}

#[test]
fn render_substitutes_the_callers_merged_expressions_by_plan_slot() {
    let node = round_of(sum_over_count(), 2);
    let plans = vec![plan_of(&count_star()), plan_of(&sum_x())];

    let sql = render(&node, &plans, &["<MERGED_COUNT>", "<MERGED_SUM>"])
        .expect("a classified scalar-over-aggregate must render over the merge wrapper");

    assert_eq!(sql, "ROUND((<MERGED_SUM> / <MERGED_COUNT>), 2)");
}

#[test]
fn render_leaves_no_sentinel_token_behind() {
    let node = round_of(sum_x(), 2);
    let plans = vec![plan_of(&sum_x())];

    let sql =
        render(&node, &plans, &["<MERGED_SUM>"]).expect("a single-aggregate item must render");

    assert!(
        !sql.contains(&agg_sentinel_token(0)),
        "unsubstituted: {sql}"
    );
}

#[test]
fn render_declines_an_aggregate_absent_from_the_plans() {
    let node = round_of(sum_x(), 2);
    let plans = vec![plan_of(&count_star())];

    assert!(
        render(&node, &plans, &["<MERGED_COUNT>"]).is_none(),
        "an aggregate with no merge slot cannot be rendered"
    );
}

#[test]
fn render_declines_when_the_merged_list_is_shorter_than_the_matched_slot() {
    let node = round_of(sum_x(), 2);
    let plans = vec![plan_of(&count_star()), plan_of(&sum_x())];

    assert!(
        render(&node, &plans, &["<MERGED_COUNT>"]).is_none(),
        "a merged list not aligned with plans must decline, not panic"
    );
}

#[test]
fn scalar_over_agg_primitives_serve_both_planners_with_no_planner_dependency() {
    let node = round_of(sum_over_count(), 2);
    let nested = classify(&node)
        .expect("ROUND(SUM(X) / COUNT(*), 2) must classify as scalar-over-aggregate");

    let mut grouped_plans = vec![plan_of(&agg_item("COUNT", Some("L_ORDERKEY"), false))];
    let mut grouped_types = vec![Some(ExasolType::decimal(18, 0))];
    for (plan, _) in &nested {
        fold_aggregate_plan(&mut grouped_plans, &mut grouped_types, plan.clone(), None);
    }

    let mut single_group_plans: Vec<AggregatePlan> = Vec::new();
    let mut single_group_types: Vec<Option<ExasolType>> = Vec::new();
    for (plan, _) in &nested {
        fold_aggregate_plan(
            &mut single_group_plans,
            &mut single_group_types,
            plan.clone(),
            None,
        );
    }

    assert_eq!(
        grouped_plans.len(),
        3,
        "the grouped-shaped caller's pre-existing COUNT(L_ORDERKEY) stays its own slot, \
         plus one fresh slot each for the nested SUM(X) and COUNT(*)"
    );
    assert_eq!(
        single_group_plans.len(),
        2,
        "the single-group-shaped caller has no pre-existing slot, so only the nested \
         SUM(X) and COUNT(*) get slots"
    );

    let sum = r#"SUM("PARTIAL_sum_1")"#;
    let count = r#"SUM("PARTIAL_count_2")"#;
    let grouped_sql = render(
        &node,
        &grouped_plans,
        &[r#"SUM("PARTIAL_count_l_orderkey_0")"#, sum, count],
    )
    .expect("the grouped-shaped caller must render");
    let single_group_sql = render(&node, &single_group_plans, &[sum, count])
        .expect("the single-group-shaped caller must render");

    assert_eq!(
        grouped_sql, single_group_sql,
        "the SAME node, matched against differently-shaped `plans` lists but given \
         the same merged expression for its own matched slot, must render byte-identical \
         SQL regardless of which planner drives it"
    );
}

#[test]
fn sum_widens_a_decimal_to_precision_36_keeps_a_double_and_types_nothing_else() {
    for exa_type in EXASOL_TYPE_SPELLINGS {
        let expected = match exa_type {
            ExasolType::Decimal(decimal) => Some(ExasolType::decimal(36, decimal.scale())),
            ExasolType::DoublePrecision => Some(ExasolType::DoublePrecision),
            ExasolType::Boolean
            | ExasolType::Date
            | ExasolType::Timestamp { .. }
            | ExasolType::TimestampWithLocalTimeZone { .. }
            | ExasolType::IntervalYearToMonth { .. }
            | ExasolType::IntervalDayToSecond { .. }
            | ExasolType::Geometry { .. }
            | ExasolType::Hashtype { .. }
            | ExasolType::Char { .. }
            | ExasolType::Varchar { .. } => None,
        };
        assert_eq!(sum_emit_type(exa_type), expected, "{exa_type}");
    }
}

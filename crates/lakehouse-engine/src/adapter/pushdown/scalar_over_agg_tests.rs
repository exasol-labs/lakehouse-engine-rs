use super::*;
use crate::adapter::pushdown::test_support::{
    EXASOL_TYPE_SPELLINGS, agg_item, agg_item_expr, cast_to, col, column_types, count_star,
    float_div, num_lit, round_of, scalar_fn,
};

fn sum_over_count() -> Json {
    float_div(agg_item("SUM", Some("X"), false), count_star())
}

fn plan_of(node: &Json) -> AggregatePlan {
    parse_agg_item(node, &ColumnTypes::default())
        .expect("test fixture aggregate must parse to a plan")
}

#[test]
fn fold_collapses_structurally_equal_plans_into_one_slot() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    let first = fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg_item("SUM", Some("X"), false)),
        None,
    );
    let second = fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg_item("SUM", Some("X"), false)),
        None,
    );

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

    let sum = fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg_item("SUM", Some("X"), false)),
        None,
    );
    let count = fold_aggregate_plan(&mut plans, &mut types, plan_of(&count_star()), None);

    assert_eq!((sum, count), (0, 1));
    assert_eq!(plans.len(), 2);
}

#[test]
fn fold_leaves_a_nested_only_occurrence_untyped() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg_item("SUM", Some("X"), false)),
        None,
    );

    assert_eq!(types, vec![None]);
}

#[test]
fn fold_declared_type_overwrites_a_slot_created_by_a_nested_occurrence() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg_item("SUM", Some("X"), false)),
        None,
    );
    let slot = fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg_item("SUM", Some("X"), false)),
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
        plan_of(&agg_item("SUM", Some("X"), false)),
        Some(ExasolType::decimal(18, 2)),
    );
    fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg_item("SUM", Some("X"), false)),
        None,
    );

    assert_eq!(types, vec![Some(ExasolType::decimal(18, 2))]);
}

#[test]
fn sentinelize_replaces_each_aggregate_and_collects_it_in_encounter_order() {
    let node = round_of(sum_over_count(), 2);
    let mut aggregates = Vec::new();
    let mut residual = false;

    let tree = sentinelize_aggregates(&node, &mut aggregates, &mut residual);

    assert!(!residual, "no bare column sits outside the aggregates");
    assert_eq!(
        aggregates,
        vec![agg_item("SUM", Some("X"), false), count_star()]
    );
    assert_eq!(
        tree["arguments"][0]["arguments"][0],
        sentinel_column_node(0)
    );
    assert_eq!(
        tree["arguments"][0]["arguments"][1],
        sentinel_column_node(1)
    );
}

#[test]
fn sentinelize_does_not_treat_a_column_inside_an_aggregate_as_residual() {
    let mut aggregates = Vec::new();
    let mut residual = false;

    sentinelize_aggregates(
        &round_of(agg_item("SUM", Some("X"), false), 2),
        &mut aggregates,
        &mut residual,
    );

    assert!(!residual);
}

#[test]
fn sentinelize_flags_a_bare_column_outside_any_aggregate() {
    let node = scalar_fn("ADD", [agg_item("SUM", Some("X"), false), col("Y")]);
    let mut aggregates = Vec::new();
    let mut residual = false;

    sentinelize_aggregates(&node, &mut aggregates, &mut residual);

    assert!(
        residual,
        "the outer merge wrapper cannot reference a source column"
    );
}

#[test]
fn classify_returns_the_nested_plans_in_encounter_order() {
    let node = round_of(sum_over_count(), 2);

    let plans = classify_scalar_over_aggregate(&node, &ColumnTypes::default())
        .expect("ROUND(SUM(x) / COUNT(*), 2) must classify as a scalar-over-aggregate");

    assert_eq!(
        plans,
        vec![
            (
                plan_of(&agg_item("SUM", Some("X"), false)),
                agg_item("SUM", Some("X"), false)
            ),
            (plan_of(&count_star()), count_star()),
        ]
    );
}

#[test]
fn classify_declines_a_node_with_no_nested_aggregate() {
    assert!(
        classify_scalar_over_aggregate(&round_of(col("X"), 2), &ColumnTypes::default()).is_none(),
        "a plain scalar over a column is not a scalar-over-aggregate"
    );
}

#[test]
fn classify_accepts_a_bare_aggregate_as_its_own_single_plan() {
    let plans =
        classify_scalar_over_aggregate(&agg_item("SUM", Some("X"), false), &ColumnTypes::default())
            .expect("a bare aggregate is structurally decomposable");

    assert_eq!(
        plans,
        vec![(
            plan_of(&agg_item("SUM", Some("X"), false)),
            agg_item("SUM", Some("X"), false)
        )]
    );
}

#[test]
fn classify_declines_a_distinct_inner_aggregate() {
    assert!(
        classify_scalar_over_aggregate(
            &round_of(agg_item("SUM", Some("X"), true), 2),
            &ColumnTypes::default()
        )
        .is_none(),
        "an undecomposable DISTINCT inner aggregate must decline the whole item"
    );
}

#[test]
fn classify_declines_a_residual_column_outside_the_aggregate() {
    let node = scalar_fn("ADD", [agg_item("SUM", Some("X"), false), col("Y")]);

    assert!(classify_scalar_over_aggregate(&node, &ColumnTypes::default()).is_none());
}

#[test]
fn render_substitutes_the_callers_merged_expressions_by_plan_slot() {
    let node = round_of(sum_over_count(), 2);
    let plans = vec![
        plan_of(&count_star()),
        plan_of(&agg_item("SUM", Some("X"), false)),
    ];
    let merged = vec!["<MERGED_COUNT>".to_string(), "<MERGED_SUM>".to_string()];

    let sql = render_scalar_over_merge(&node, &plans, &merged, &ColumnTypes::default())
        .expect("a classified scalar-over-aggregate must render over the merge wrapper")
        .sql;

    assert_eq!(sql, "ROUND((<MERGED_SUM> / <MERGED_COUNT>), 2)");
}

#[test]
fn render_leaves_no_sentinel_token_behind() {
    let node = round_of(agg_item("SUM", Some("X"), false), 2);
    let plans = vec![plan_of(&agg_item("SUM", Some("X"), false))];

    let sql = render_scalar_over_merge(
        &node,
        &plans,
        &["<MERGED_SUM>".to_string()],
        &ColumnTypes::default(),
    )
    .expect("a single-aggregate item must render")
    .sql;

    assert!(
        !sql.contains(&agg_sentinel_token(0)),
        "unsubstituted: {sql}"
    );
}

#[test]
fn render_declines_an_aggregate_absent_from_the_plans() {
    let node = round_of(agg_item("SUM", Some("X"), false), 2);
    let plans = vec![plan_of(&count_star())];

    assert!(
        render_scalar_over_merge(
            &node,
            &plans,
            &["<MERGED_COUNT>".to_string()],
            &ColumnTypes::default()
        )
        .is_none(),
        "an aggregate with no merge slot cannot be rendered"
    );
}

#[test]
fn render_declines_when_the_merged_list_is_shorter_than_the_matched_slot() {
    let node = round_of(agg_item("SUM", Some("X"), false), 2);
    let plans = vec![
        plan_of(&count_star()),
        plan_of(&agg_item("SUM", Some("X"), false)),
    ];

    assert!(
        render_scalar_over_merge(
            &node,
            &plans,
            &["<MERGED_COUNT>".to_string()],
            &ColumnTypes::default()
        )
        .is_none(),
        "a merged list not aligned with plans must decline, not panic"
    );
}

#[test]
fn scalar_over_agg_primitives_serve_both_planners_with_no_planner_dependency() {
    let node = round_of(sum_over_count(), 2);
    let nested = classify_scalar_over_aggregate(&node, &ColumnTypes::default())
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

    let grouped_merged = vec![
        "SUM(\"PARTIAL_count_l_orderkey_0\")".to_string(),
        "SUM(\"PARTIAL_sum_1\")".to_string(),
        "SUM(\"PARTIAL_count_2\")".to_string(),
    ];
    let single_group_merged = vec![
        "SUM(\"PARTIAL_sum_1\")".to_string(),
        "SUM(\"PARTIAL_count_2\")".to_string(),
    ];

    let grouped_sql = render_scalar_over_merge(
        &node,
        &grouped_plans,
        &grouped_merged,
        &ColumnTypes::default(),
    )
    .expect("the grouped-shaped caller must render")
    .sql;
    let single_group_sql = render_scalar_over_merge(
        &node,
        &single_group_plans,
        &single_group_merged,
        &ColumnTypes::default(),
    )
    .expect("the single-group-shaped caller must render")
    .sql;

    assert_eq!(
        grouped_sql, single_group_sql,
        "the SAME node, matched against differently-shaped `plans` lists but given \
         the same merged expression for its own matched slot, must render byte-identical \
         SQL regardless of which planner drives it"
    );
}

#[test]
fn sum_widens_a_decimal_to_precision_36_and_every_other_variant_to_double() {
    for exa_type in EXASOL_TYPE_SPELLINGS {
        let expected = match exa_type {
            ExasolType::Decimal(decimal) => ExasolType::decimal(36, decimal.scale()),
            ExasolType::Boolean
            | ExasolType::DoublePrecision
            | ExasolType::Date
            | ExasolType::Timestamp { .. }
            | ExasolType::TimestampWithLocalTimeZone { .. }
            | ExasolType::IntervalYearToMonth { .. }
            | ExasolType::IntervalDayToSecond { .. }
            | ExasolType::Geometry { .. }
            | ExasolType::Hashtype { .. }
            | ExasolType::Char { .. }
            | ExasolType::Varchar { .. } => ExasolType::DoublePrecision,
        };
        assert_eq!(sum_emit_type(exa_type), expected, "{exa_type}");
    }
}

fn nested_type_of(agg_node: &Json, col_types: &ColumnTypes) -> Option<ExasolType> {
    let plan = parse_agg_item(agg_node, col_types).expect("test aggregate must parse to a plan");
    nested_aggregate_type(agg_node, &plan, col_types)
}

#[test]
fn nested_aggregate_type_is_exasols_result_type_of_the_aggregate() {
    let col_types = column_types(&[("X", ExasolType::decimal(9, 2))]);
    let cases = [
        ("COUNT(*)", count_star(), ExasolType::decimal(18, 0)),
        (
            "SUM over DECIMAL(9,2) widens by 11 digits",
            agg_item("SUM", Some("X"), false),
            ExasolType::decimal(20, 2),
        ),
        (
            "MAX over a CAST takes the CAST's type, not the column's",
            agg_item_expr("MAX", cast_to(ExasolType::decimal(5, 1), col("X")), false),
            ExasolType::decimal(5, 1),
        ),
    ];
    for (label, agg_node, expected) in cases {
        assert_eq!(
            nested_type_of(&agg_node, &col_types),
            Some(expected),
            "{label}"
        );
    }
}

#[test]
fn nested_aggregate_type_declines_an_argument_without_an_exact_type() {
    let col_types = column_types(&[("X", ExasolType::decimal(9, 2))]);
    let max_of_expression = agg_item_expr("MAX", scalar_fn("ADD", [col("X"), num_lit(1)]), false);

    assert_eq!(nested_type_of(&max_of_expression, &col_types), None);
}

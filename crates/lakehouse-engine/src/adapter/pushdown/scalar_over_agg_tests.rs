use super::*;

fn agg(name: &str, col: &str, distinct: bool) -> Json {
    serde_json::json!({
        "type": "function_aggregate", "name": name, "distinct": distinct,
        "arguments": [{"type": "column", "name": col}]
    })
}

fn count_star() -> Json {
    serde_json::json!({"type": "function_aggregate", "name": "COUNT", "arguments": []})
}

fn binary(name: &str, left: Json, right: Json) -> Json {
    serde_json::json!({
        "type": "function_scalar", "name": name, "arguments": [left, right]
    })
}

fn sum_over_count() -> Json {
    binary("FLOAT_DIV", agg("SUM", "X", false), count_star())
}

fn round_of(inner: Json, digits: i64) -> Json {
    serde_json::json!({
        "type": "function_scalar", "name": "ROUND",
        "arguments": [inner, {"type": "literal_exactnumeric", "value": digits}]
    })
}

fn plan_of(node: &Json) -> AggregatePlan {
    parse_agg_item(node).expect("test fixture aggregate must parse to a plan")
}

#[test]
fn fold_collapses_structurally_equal_plans_into_one_slot() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    let first = fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg("SUM", "X", false)),
        None,
    );
    let second = fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg("SUM", "X", false)),
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
        plan_of(&agg("SUM", "X", false)),
        None,
    );
    let count = fold_aggregate_plan(&mut plans, &mut types, plan_of(&count_star()), None);

    assert_eq!((sum, count), (0, 1));
    assert_eq!(plans.len(), 2);
}

#[test]
fn fold_defaults_a_nested_only_occurrence_to_double_precision() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg("SUM", "X", false)),
        None,
    );

    assert_eq!(types, vec!["DOUBLE PRECISION".to_string()]);
}

#[test]
fn fold_declared_type_overwrites_a_slot_created_by_a_nested_occurrence() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg("SUM", "X", false)),
        None,
    );
    let slot = fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg("SUM", "X", false)),
        Some("DECIMAL(18,2)".to_string()),
    );

    assert_eq!(slot, 0);
    assert_eq!(
        types,
        vec!["DECIMAL(18,2)".to_string()],
        "a top-level occurrence's declared type must win over the nested default"
    );
}

#[test]
fn fold_keeps_a_declared_type_when_a_nested_occurrence_follows_it() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg("SUM", "X", false)),
        Some("DECIMAL(18,2)".to_string()),
    );
    fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg("SUM", "X", false)),
        None,
    );

    assert_eq!(types, vec!["DECIMAL(18,2)".to_string()]);
}

#[test]
fn sentinelize_replaces_each_aggregate_and_collects_it_in_encounter_order() {
    let node = round_of(sum_over_count(), 2);
    let mut aggregates = Vec::new();
    let mut residual = false;

    let tree = sentinelize_aggregates(&node, &mut aggregates, &mut residual);

    assert!(!residual, "no bare column sits outside the aggregates");
    assert_eq!(aggregates, vec![agg("SUM", "X", false), count_star()]);
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
        &round_of(agg("SUM", "X", false), 2),
        &mut aggregates,
        &mut residual,
    );

    assert!(!residual);
}

#[test]
fn sentinelize_flags_a_bare_column_outside_any_aggregate() {
    let node = binary(
        "ADD",
        agg("SUM", "X", false),
        serde_json::json!({"type": "column", "name": "Y"}),
    );
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

    let plans = classify_scalar_over_aggregate(&node)
        .expect("ROUND(SUM(x) / COUNT(*), 2) must classify as a scalar-over-aggregate");

    assert_eq!(
        plans,
        vec![plan_of(&agg("SUM", "X", false)), plan_of(&count_star())]
    );
}

#[test]
fn classify_declines_a_node_with_no_nested_aggregate() {
    assert!(
        classify_scalar_over_aggregate(&round_of(
            serde_json::json!({"type": "column", "name": "X"}),
            2
        ))
        .is_none(),
        "a plain scalar over a column is not a scalar-over-aggregate"
    );
}

/// Classification is purely structural: a bare aggregate satisfies it (one nested
/// aggregate, no residual column). Routing a top-level aggregate to the plain
/// aggregate path is the caller's decision, made before consulting this function —
/// so this test pins the structural answer, not a routing rule.
#[test]
fn classify_accepts_a_bare_aggregate_as_its_own_single_plan() {
    let plans = classify_scalar_over_aggregate(&agg("SUM", "X", false))
        .expect("a bare aggregate is structurally decomposable");

    assert_eq!(plans, vec![plan_of(&agg("SUM", "X", false))]);
}

#[test]
fn classify_declines_a_distinct_inner_aggregate() {
    assert!(
        classify_scalar_over_aggregate(&round_of(agg("SUM", "X", true), 2)).is_none(),
        "an undecomposable DISTINCT inner aggregate must decline the whole item"
    );
}

#[test]
fn classify_declines_a_residual_column_outside_the_aggregate() {
    let node = binary(
        "ADD",
        agg("SUM", "X", false),
        serde_json::json!({"type": "column", "name": "Y"}),
    );

    assert!(classify_scalar_over_aggregate(&node).is_none());
}

/// The one owner of the decomposition mechanism serves both aggregate planners
/// without naming either: the merged `PARTIAL_*` expressions arrive as a
/// parameter, so this module renders whatever merge shape its caller owns.
#[test]
fn render_substitutes_the_callers_merged_expressions_by_plan_slot() {
    let node = round_of(sum_over_count(), 2);
    let plans = vec![plan_of(&count_star()), plan_of(&agg("SUM", "X", false))];
    let merged = vec!["<MERGED_COUNT>".to_string(), "<MERGED_SUM>".to_string()];

    let sql = render_scalar_over_merge(&node, &plans, &merged)
        .expect("a classified scalar-over-aggregate must render over the merge wrapper");

    assert_eq!(sql, "ROUND((<MERGED_SUM> / <MERGED_COUNT>), 2)");
}

#[test]
fn render_leaves_no_sentinel_token_behind() {
    let node = round_of(agg("SUM", "X", false), 2);
    let plans = vec![plan_of(&agg("SUM", "X", false))];

    let sql = render_scalar_over_merge(&node, &plans, &["<MERGED_SUM>".to_string()])
        .expect("a single-aggregate item must render");

    assert!(
        !sql.contains(&agg_sentinel_token(0)),
        "unsubstituted: {sql}"
    );
}

#[test]
fn render_declines_an_aggregate_absent_from_the_plans() {
    let node = round_of(agg("SUM", "X", false), 2);
    let plans = vec![plan_of(&count_star())];

    assert!(
        render_scalar_over_merge(&node, &plans, &["<MERGED_COUNT>".to_string()]).is_none(),
        "an aggregate with no merge slot cannot be rendered"
    );
}

#[test]
fn render_declines_when_the_merged_list_is_shorter_than_the_matched_slot() {
    let node = round_of(agg("SUM", "X", false), 2);
    let plans = vec![plan_of(&count_star()), plan_of(&agg("SUM", "X", false))];

    assert!(
        render_scalar_over_merge(&node, &plans, &["<MERGED_COUNT>".to_string()]).is_none(),
        "a merged list not aligned with plans must decline, not panic"
    );
}

/// The GROUP BY planner and the single-group planner drive `fold_aggregate_plan`
/// and `render_scalar_over_merge` from structurally different starting states — the
/// grouped planner's `plans` list already carries an EARLIER, unrelated bare
/// aggregate from a preceding select-list item (`COUNT(L_ORDERKEY)`), while the
/// single-group planner's starts empty. Both must fold and render the SAME
/// scalar-over-aggregate node identically once each caller supplies the merged
/// expression for its OWN matched slot — proving the shared primitives carry no
/// assumption about which planner, or which starting `plans` shape, drives them.
#[test]
fn scalar_over_agg_primitives_serve_both_planners_with_no_planner_dependency() {
    let node = round_of(sum_over_count(), 2);
    let nested = classify_scalar_over_aggregate(&node)
        .expect("ROUND(SUM(X) / COUNT(*), 2) must classify as scalar-over-aggregate");

    let mut grouped_plans = vec![plan_of(&agg("COUNT", "L_ORDERKEY", false))];
    let mut grouped_types = vec!["DECIMAL(18,0)".to_string()];
    for plan in &nested {
        fold_aggregate_plan(&mut grouped_plans, &mut grouped_types, plan.clone(), None);
    }

    let mut single_group_plans: Vec<AggregatePlan> = Vec::new();
    let mut single_group_types: Vec<String> = Vec::new();
    for plan in &nested {
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

    let grouped_sql = render_scalar_over_merge(&node, &grouped_plans, &grouped_merged)
        .expect("the grouped-shaped caller must render");
    let single_group_sql =
        render_scalar_over_merge(&node, &single_group_plans, &single_group_merged)
            .expect("the single-group-shaped caller must render");

    assert_eq!(
        grouped_sql, single_group_sql,
        "the SAME node, matched against differently-shaped `plans` lists but given \
         the same merged expression for its own matched slot, must render byte-identical \
         SQL regardless of which planner drives it"
    );
}

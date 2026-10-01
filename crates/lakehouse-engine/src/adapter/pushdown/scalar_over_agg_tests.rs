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

fn column(name: &str) -> Json {
    serde_json::json!({"type": "column", "name": name})
}

fn scalar(name: &str, arguments: Vec<Json>) -> Json {
    serde_json::json!({"type": "function_scalar", "name": name, "arguments": arguments})
}

fn cast_to(arg: Json, data_type: Json) -> Json {
    serde_json::json!({"type": "function_scalar_cast", "dataType": data_type, "arguments": [arg]})
}

fn string_literal(value: &str) -> Json {
    serde_json::json!({"type": "literal_string", "value": value})
}

fn predicate(kind: &str, left: Json, right: Json) -> Json {
    serde_json::json!({"type": kind, "left": left, "right": right})
}

fn agg_over(name: &str, arg: Json) -> Json {
    serde_json::json!({"type": "function_aggregate", "name": name, "arguments": [arg]})
}

fn probe_col_types() -> Vec<(String, String)> {
    [
        ("ID", "DECIMAL(20,0)"),
        ("C_DOUBLE", "DOUBLE PRECISION"),
        ("C_VARCHAR", "VARCHAR(2000000) UTF8"),
        ("C_CHAR", "CHAR(3) ASCII"),
        ("C_DATE", "DATE"),
        ("C_TS", "TIMESTAMP(6)"),
        ("C_BOOL", "BOOLEAN"),
        ("C_TSTZ", "TIMESTAMP WITH LOCAL TIME ZONE"),
    ]
    .iter()
    .map(|(name, ty)| (name.to_string(), ty.to_string()))
    .collect()
}

/// The partial type the typed classifier gives the one aggregate `agg`.
fn partial_type_of(agg: Json) -> String {
    let typed = classify_typed_scalar_over_aggregate(&round_of(agg, 2), &probe_col_types())
        .expect("a scalar over one aggregate must classify");
    assert_eq!(typed.len(), 1, "one nested aggregate: {typed:?}");
    typed[0].1.clone()
}

#[test]
fn typed_classify_pairs_each_nested_plan_with_its_partial_type_in_encounter_order() {
    let node = binary(
        "CONCAT",
        agg_over("MAX", scalar("UPPER", vec![column("ID")])),
        agg_over("SUM", column("ID")),
    );

    let typed = classify_typed_scalar_over_aggregate(&node, &probe_col_types())
        .expect("a CONCAT over two aggregates must classify");

    assert_eq!(
        typed,
        vec![
            (
                plan_of(&agg_over("MAX", scalar("UPPER", vec![column("ID")]))),
                "VARCHAR(2000000)".to_string()
            ),
            (
                plan_of(&agg_over("SUM", column("ID"))),
                "DOUBLE PRECISION".to_string()
            ),
        ]
    );
}

#[test]
fn typed_classify_declines_what_the_untyped_classify_declines() {
    let residual = binary("ADD", agg("SUM", "X", false), column("Y"));

    assert!(classify_typed_scalar_over_aggregate(&residual, &probe_col_types()).is_none());
}

#[test]
fn nested_min_max_over_a_character_argument_takes_a_character_partial_type() {
    let cases = [
        (scalar("UPPER", vec![column("ID")]), "VARCHAR(2000000)"),
        (
            scalar(
                "SUBSTR",
                vec![
                    column("C_VARCHAR"),
                    serde_json::json!({"type": "literal_exactnumeric", "value": 1}),
                ],
            ),
            "VARCHAR(2000000)",
        ),
        (
            scalar("CONCAT", vec![column("ID"), string_literal("-")]),
            "VARCHAR(2000000)",
        ),
        (
            cast_to(
                column("ID"),
                serde_json::json!({"type": "VARCHAR", "size": 20}),
            ),
            "VARCHAR(20)",
        ),
        (
            cast_to(column("ID"), serde_json::json!({"type": "CHAR", "size": 5})),
            "CHAR(5)",
        ),
        (
            scalar(
                "CASE",
                vec![
                    predicate("predicate_equal", column("ID"), column("ID")),
                    column("C_VARCHAR"),
                ],
            ),
            "VARCHAR(2000000) UTF8",
        ),
        (
            scalar("GREATEST", vec![column("C_CHAR"), column("C_VARCHAR")]),
            "VARCHAR(2000000)",
        ),
        (
            scalar("NULLIF", vec![column("C_CHAR"), string_literal("x")]),
            "CHAR(3) ASCII",
        ),
        (
            scalar(
                "CASE",
                vec![
                    predicate("predicate_equal", column("ID"), column("ID")),
                    column("ID"),
                    column("C_VARCHAR"),
                ],
            ),
            "VARCHAR(2000000) UTF8",
        ),
        (string_literal("a"), "VARCHAR(2000000)"),
        (
            scalar("LEAST", vec![column("C_CHAR"), column("C_CHAR")]),
            "CHAR(3) ASCII",
        ),
    ];
    for (arg, expected) in cases {
        for kind in ["MAX", "MIN"] {
            assert_eq!(
                partial_type_of(agg_over(kind, arg.clone())),
                expected,
                "{kind} over {arg}"
            );
        }
    }
}

#[test]
fn nested_min_max_over_a_temporal_or_boolean_argument_takes_that_partial_type() {
    let date_type = serde_json::json!({"type": "DATE"});
    let simple_case = serde_json::json!({
        "type": "function_scalar_case",
        "arguments": [{"type": "literal_bool", "value": true}],
        "results": [column("C_TS"), {"type": "literal_null"}],
    });
    let cases = [
        (cast_to(column("C_TS"), date_type), "DATE"),
        (
            scalar("DATE_TRUNC", vec![string_literal("month"), column("C_TS")]),
            "TIMESTAMP(6)",
        ),
        (
            scalar("TO_DATE", vec![column("C_VARCHAR"), string_literal("YYYY")]),
            "DATE",
        ),
        (
            scalar("TO_TIMESTAMP", vec![column("C_VARCHAR")]),
            "TIMESTAMP",
        ),
        (scalar("TRUNC", vec![column("C_DATE")]), "DATE"),
        (simple_case, "TIMESTAMP(6)"),
        (
            predicate("predicate_greater", column("ID"), column("ID")),
            "BOOLEAN",
        ),
        (column("C_BOOL"), "BOOLEAN"),
        (
            serde_json::json!({"type": "literal_date", "value": "2024-01-01"}),
            "DATE",
        ),
        (
            serde_json::json!({"type": "literal_timestamp", "value": "2024-01-01 00:00:00"}),
            "TIMESTAMP",
        ),
        (
            serde_json::json!({"type": "literal_bool", "value": true}),
            "BOOLEAN",
        ),
        (
            scalar(
                "REGEXP_LIKE",
                vec![column("C_VARCHAR"), string_literal("a")],
            ),
            "BOOLEAN",
        ),
        (scalar("ROUND", vec![column("C_TS")]), "TIMESTAMP(6)"),
    ];
    for (arg, expected) in cases {
        assert_eq!(
            partial_type_of(agg_over("MAX", arg.clone())),
            expected,
            "MAX over {arg}"
        );
    }
}

#[test]
fn nested_aggregate_keeps_the_numeric_default_when_its_argument_is_numeric_or_untyped() {
    let numeric_default = NESTED_AGGREGATE_PLAN_TYPE;
    let cases = [
        agg_over("MAX", binary("ADD", column("ID"), column("ID"))),
        agg_over("MIN", scalar("ROUND", vec![column("C_DOUBLE")])),
        agg_over("MAX", scalar("LENGTH", vec![column("C_VARCHAR")])),
        agg_over(
            "MAX",
            cast_to(
                column("C_VARCHAR"),
                serde_json::json!({"type": "DECIMAL", "precision": 9, "scale": 2}),
            ),
        ),
        agg_over("MAX", column("UNKNOWN_COLUMN")),
        agg_over("MAX", scalar("ABS", vec![column("UNKNOWN_COLUMN")])),
        agg_over("SUM", scalar("UPPER", vec![column("ID")])),
        agg_over("AVG", column("C_DATE")),
        agg_over("MAX", column("C_TSTZ")),
        agg_over(
            "MAX",
            scalar("GREATEST", vec![column("C_DATE"), column("C_TS")]),
        ),
    ];
    for aggregate in cases {
        assert_eq!(
            partial_type_of(aggregate.clone()),
            numeric_default,
            "{aggregate}"
        );
    }
}

#[test]
fn fold_nested_types_a_new_slot_with_its_partial_type() {
    let mut plans = Vec::new();
    let mut types = Vec::new();

    fold_nested_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg("MAX", "X", false)),
        "VARCHAR(2000000)".to_string(),
    );

    assert_eq!(plans.len(), 1);
    assert_eq!(types, vec!["VARCHAR(2000000)".to_string()]);
}

#[test]
fn fold_nested_never_overwrites_an_existing_slot_type() {
    let mut plans = Vec::new();
    let mut types = Vec::new();
    fold_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg("MAX", "X", false)),
        Some("VARCHAR(20)".to_string()),
    );

    fold_nested_aggregate_plan(
        &mut plans,
        &mut types,
        plan_of(&agg("MAX", "X", false)),
        "VARCHAR(2000000)".to_string(),
    );

    assert_eq!(plans.len(), 1);
    assert_eq!(
        types,
        vec!["VARCHAR(20)".to_string()],
        "a declared type from a top-level occurrence must win"
    );
}

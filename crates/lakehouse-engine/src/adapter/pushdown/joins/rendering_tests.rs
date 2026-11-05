use super::super::super::support::collect_all_column_names;
use super::super::planning::{JoinLeaf, involved_table};
use super::super::sql_builders::{
    JoinScanRequestConfig, build_broadcast_join_sql, build_n_scan_join_sql, build_side_fan_out_sql,
    render_broadcast_join,
};
use super::super::tests::{
    broadcast_projection, detected_join, equi_condition, join_request, legs_from_leaves,
    resolved_side, two_scan_tuning,
};
use super::*;
use crate::adapter::pushdown::ProjectedColumn;
use crate::adapter::pushdown::support::apply_type_rewrites;
use crate::adapter::pushdown::test_support::*;
use crate::scan::spec::ColumnRef;
use crate::types::exasol_type::ExasolType;
use vs_expression::{render_df_filter_safe, render_expression_safe};

/// One unaliased leg per name, in FROM-tree order.
fn legs_of(names: &[&str]) -> JoinLegs {
    let leaves: Vec<JoinLeaf> = names
        .iter()
        .map(|name| JoinLeaf {
            table_name: (*name).to_string(),
            table_alias: None,
            table_identifier: format!("lh.{}", name.to_lowercase()),
        })
        .collect();
    legs_from_leaves(leaves)
}

fn customer_orders_legs() -> JoinLegs {
    legs_of(&["CUSTOMER", "ORDERS"])
}

fn self_join_legs() -> JoinLegs {
    legs_from_leaves(vec![
        JoinLeaf {
            table_name: "FACT_ORDERS".to_string(),
            table_alias: Some("A".to_string()),
            table_identifier: "lh.fact_orders".to_string(),
        },
        JoinLeaf {
            table_name: "FACT_ORDERS".to_string(),
            table_alias: Some("B".to_string()),
            table_identifier: "lh.fact_orders".to_string(),
        },
    ])
}

#[test]
fn join_condition_renders_via_translator() {
    assert_eq!(
        render_expression_safe(&equi_condition()).as_deref(),
        Some(r#"("C_CUSTKEY" = "O_CUSTKEY")"#),
        "the equi-condition must render to a bare-name DataFusion boolean expr"
    );

    let request = join_request(Json::Null, equi_condition());
    let detected = detected_join(&request);
    let rendered = render_broadcast_join(
        &parsed_pushdown(&request),
        &detected,
        &request_sides(&request, &detected),
    )
    .expect("renderable join")
    .expect("an equi-join must render, not decline");
    assert_eq!(
        rendered.condition, r#"("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY")"#,
        "each operand is qualified by its own leg's alias"
    );
}

#[test]
fn join_where_filter_spanning_both_sides_renders() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            predicate_equal(table_col("CUSTOMER", "C_NAME"), str_lit("ACME")),
            {"type": "predicate_greater",
             "left": table_col("ORDERS", "O_ORDERDATE"),
             "right": str_lit("1995-01-01")},
        ],
    });

    let detected = detected_join(&request);
    let rendered = render_broadcast_join(
        &parsed_pushdown(&request),
        &detected,
        &request_sides(&request, &detected),
    )
    .expect("renderable join")
    .expect("must render");
    let filter = rendered
        .filter
        .expect("a cross-side WHERE filter must render");
    assert!(
        filter.contains(r#""LHS_T0"."C_NAME""#),
        "the left-side column must appear qualified by its leg in the rendered filter: {filter}"
    );
    assert!(
        filter.contains(r#""LHS_T1"."O_ORDERDATE""#),
        "the right-side column must appear qualified by its leg in the rendered filter: {filter}"
    );
    assert!(
        filter.contains("AND"),
        "the conjunction of both sides must render: {filter}"
    );
}

#[test]
fn join_projection_emits_attribute_each_side_owning_type() {
    let request = join_request(Json::Null, equi_condition());
    let (projection, types, _widened) = broadcast_projection(&request);

    assert_eq!(
        projection,
        vec![
            ProjectionItem::Column(ColumnRef::qualified("LHS_T0", "C_NAME")),
            ProjectionItem::Column(ColumnRef::qualified("LHS_T1", "O_ORDERDATE")),
        ],
        "projection spans both tables in select-list order, each column qualified by its leg"
    );
    assert_eq!(
        types,
        vec![ExasolType::varchar(100), ExasolType::Date],
        "each column's EMITS type comes from the side that owns it"
    );
}

#[test]
fn join_projection_resolves_cast_node_to_expr_not_full_row_fallback() {
    let mut request = join_request(Json::Null, equi_condition());
    request = selecting(
        request,
        &[(
            cast_to(ExasolType::WIDEST_VARCHAR, table_col("CUSTOMER", "C_NAME")),
            ExasolType::WIDEST_VARCHAR,
        )],
    );

    let (projection, _types, _widened) = broadcast_projection(&request);

    assert_eq!(
        projection.len(),
        1,
        "a function_scalar_cast select-list item must not fall back to the two-table \
         full base row: {projection:?}"
    );
    assert!(
        matches!(projection[0], ProjectionItem::Expr { .. }),
        "a rendered CAST expression must be an Expr projection item, not a bare Column: \
         {projection:?}"
    );
}

#[test]
fn join_projection_string_fn_coerces_decimal_and_declines_unrenderable_arity() {
    let request = join_request(Json::Null, equi_condition());

    let mut coerce_request = request.clone();
    coerce_request = selecting(
        coerce_request,
        &[(
            serde_json::json!({
                "type": "function_scalar",
                "name": "UPPER",
                "arguments": [table_col("CUSTOMER", "C_CUSTKEY")]
            }),
            ExasolType::WIDEST_VARCHAR,
        )],
    );
    let (projection, _types, _widened) = broadcast_projection(&coerce_request);
    assert_eq!(
        projection.len(),
        1,
        "UPPER(C_CUSTKEY) must project a single expression, not the full two-table \
         row: {projection:?}"
    );
    let ProjectionItem::Expr { expr } = &projection[0] else {
        panic!("must be a rendered expression, not a bare column: {projection:?}");
    };
    assert!(
        expr.contains(
            r#"upper(regexp_replace(regexp_replace(CAST("LHS_T0"."C_CUSTKEY" AS VARCHAR)"#
        ),
        "UPPER's DECIMAL argument must render through the trimmed decimal-to-string \
         form: {expr}"
    );

    let mut decline_request = request.clone();
    decline_request = selecting(
        decline_request,
        &[(
            serde_json::json!({
                "type": "function_scalar",
                "name": "INSTR",
                "arguments": [
                    table_col("CUSTOMER", "C_NAME"),
                    str_lit("b"),
                    num_lit(3)
                ]
            }),
            ExasolType::decimal(18, 0),
        )],
    );
    let (projection, _types, _widened) = broadcast_projection(&decline_request);
    let expected_full_row_len = involved_table(&decline_request, "CUSTOMER")
        .unwrap()
        .types
        .len()
        + involved_table(&decline_request, "ORDERS")
            .unwrap()
            .types
            .len();
    assert_eq!(
        projection.len(),
        expected_full_row_len,
        "the arity-decline INSTR must fall back to the full projection over BOTH \
         joined tables' columns, not a truncated strpos: {projection:?}"
    );
}

#[test]
fn join_projection_like_guard_reaches_join_select_list() {
    let request = join_request(Json::Null, equi_condition());

    let mut string_request = request.clone();
    string_request = selecting(
        string_request,
        &[(
            serde_json::json!({
                "type": "predicate_like",
                "expression": table_col("CUSTOMER", "C_NAME"),
                "pattern": str_lit("A%")
            }),
            ExasolType::Boolean,
        )],
    );
    let (projection, _types, widened) = broadcast_projection(&string_request);
    assert!(
        !widened,
        "a VARCHAR subject must keep the broadcast projection, not widen to the \
         full row: {projection:?}"
    );
    assert_eq!(
        projection.len(),
        1,
        "C_NAME LIKE 'A%' must project a single expression, not the full two-table \
         row: {projection:?}"
    );
    let ProjectionItem::Expr { expr } = &projection[0] else {
        panic!("must be a rendered expression, not a bare column: {projection:?}");
    };
    assert!(
        expr.contains(r#""LHS_T0"."C_NAME""#) && expr.contains("LIKE"),
        "the VARCHAR subject must render as a LIKE expression over C_NAME: {expr}"
    );

    let mut decline_request = request.clone();
    decline_request = selecting(
        decline_request,
        &[(
            serde_json::json!({
                "type": "predicate_like",
                "expression": table_col("CUSTOMER", "C_CUSTKEY"),
                "pattern": str_lit("1%")
            }),
            ExasolType::Boolean,
        )],
    );
    let (projection, _types, widened) = broadcast_projection(&decline_request);
    assert!(
        widened,
        "the widening flag is what declines the broadcast join to the N-scan \
         fallback; a DECIMAL-subject LIKE must set it: \
         {projection:?}"
    );
    let expected_full_row_len = involved_table(&decline_request, "CUSTOMER")
        .unwrap()
        .types
        .len()
        + involved_table(&decline_request, "ORDERS")
            .unwrap()
            .types
            .len();
    assert_eq!(
        projection.len(),
        expected_full_row_len,
        "a DECIMAL-subject LIKE must fall back to the full projection over BOTH \
         joined tables' columns, not an unguarded LIKE: {projection:?}"
    );
    assert!(
        projection
            .iter()
            .all(|item| matches!(item, ProjectionItem::Column(_))),
        "the fallback projection must be bare columns, not a same-length vector \
         of rendered Expr items: {projection:?}"
    );
}

#[test]
fn leg_local_filter_attributes_conjuncts_to_owning_leg() {
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            predicate_equal(table_col("CUSTOMER", "C_NAME"), str_lit("ACME")),
            {"type": "predicate_greater",
             "left": table_col("ORDERS", "O_ORDERDATE"),
             "right": str_lit("1995-01-01")},
        ],
    });

    let legs = customer_orders_legs();
    let cust = render_df_filter_safe(
        &leg_local_filter(&filter, &legs, 0).expect("a CUSTOMER-local conjunct exists"),
    )
    .expect("renders");
    assert!(
        cust.contains("C_NAME") && !cust.contains("O_ORDERDATE"),
        "CUSTOMER side-local filter must carry only C_NAME: {cust}"
    );

    let ord = render_df_filter_safe(
        &leg_local_filter(&filter, &legs, 1).expect("an ORDERS-local conjunct exists"),
    )
    .expect("renders");
    assert!(
        ord.contains("O_ORDERDATE") && !ord.contains("C_NAME"),
        "ORDERS side-local filter must carry only O_ORDERDATE: {ord}"
    );
}

#[test]
fn leg_local_filter_withholds_cross_leg_and_or_conjuncts() {
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            predicate_equal(table_col("CUSTOMER", "C_CUSTKEY"), table_col("ORDERS", "O_CUSTKEY")),
            predicate_equal(table_col("CUSTOMER", "C_NAME"), str_lit("ACME")),
        ],
    });
    let legs = customer_orders_legs();
    let cust = render_df_filter_safe(
        &leg_local_filter(&filter, &legs, 0).expect("CUSTOMER-local conjunct present"),
    )
    .expect("renders");
    assert!(
        cust.contains("C_NAME") && !cust.contains("O_CUSTKEY"),
        "the cross-table conjunct must NOT be pushed to CUSTOMER: {cust}"
    );
    assert!(
        leg_local_filter(&filter, &legs, 1).is_none(),
        "ORDERS is only referenced by the cross-leg conjunct, so nothing is leg-local to it"
    );

    let or_filter = serde_json::json!({
        "type": "predicate_or",
        "expressions": [
            predicate_equal(table_col("CUSTOMER", "C_NAME"), str_lit("ACME")),
            {"type": "predicate_greater",
             "left": table_col("ORDERS", "O_ORDERDATE"),
             "right": str_lit("1995-01-01")},
        ],
    });
    assert!(leg_local_filter(&or_filter, &legs, 0).is_none());
    assert!(leg_local_filter(&or_filter, &legs, 1).is_none());

    let one_side_or = serde_json::json!({
        "type": "predicate_or",
        "expressions": [
            predicate_equal(table_col("CUSTOMER", "C_NAME"), str_lit("ACME")),
            predicate_equal(table_col("CUSTOMER", "C_NAME"), str_lit("GLOBEX")),
        ],
    });
    assert!(
        leg_local_filter(&one_side_or, &legs, 0).is_some(),
        "an OR over one leg alone is leg-local and prunable"
    );
    assert!(leg_local_filter(&one_side_or, &legs, 1).is_none());
}

#[test]
fn leg_local_filter_handles_a_single_conjunct() {
    let single = predicate_equal(table_col("CUSTOMER", "C_NAME"), str_lit("ACME"));
    let legs = customer_orders_legs();
    assert!(leg_local_filter(&single, &legs, 0).is_some());
    assert!(leg_local_filter(&single, &legs, 1).is_none());
}

#[test]
fn leg_local_filter_attributes_shared_column_by_leg_not_name() {
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_greater",
             "left": table_col("EVENTS", "ID"),
             "right": num_lit(5)},
            predicate_equal(table_col("LABELS", "LABEL"), str_lit("x")),
        ],
    });

    let legs = legs_of(&["EVENTS", "LABELS"]);
    let events = render_df_filter_safe(
        &leg_local_filter(&filter, &legs, 0).expect("EVENTS.ID conjunct is leg-local"),
    )
    .expect("renders");
    assert!(
        events.contains("ID") && events.contains('5'),
        "EVENTS side-local filter must carry the ID > 5 predicate: {events}"
    );

    let labels = render_df_filter_safe(
        &leg_local_filter(&filter, &legs, 1).expect("LABELS.LABEL conjunct is leg-local"),
    )
    .expect("renders");
    assert!(
        labels.contains("LABEL") && !labels.contains('5'),
        "the EVENTS.ID predicate must NOT be applied to LABELS despite the shared name: {labels}"
    );
}

#[test]
fn leg_local_conjunct_reaches_only_its_own_occurrence_leg() {
    let filter = serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            {"type": "predicate_greater",
             "left": aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"),
             "right": num_lit(5)},
            predicate_equal(aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"), num_lit(7)),
        ],
    });
    let legs = self_join_legs();

    let occurrence_a = render_df_filter_safe(
        &leg_local_filter(&filter, &legs, 0).expect("occurrence A's conjunct is leg-local"),
    )
    .expect("renders");
    assert!(
        occurrence_a.contains('5') && !occurrence_a.contains('7'),
        "occurrence A's leg-local filter must carry only its own conjunct: {occurrence_a}"
    );

    let occurrence_b = render_df_filter_safe(
        &leg_local_filter(&filter, &legs, 1).expect("occurrence B's conjunct is leg-local"),
    )
    .expect("renders");
    assert!(
        occurrence_b.contains('7') && !occurrence_b.contains('5'),
        "occurrence B's leg-local filter must NOT receive occurrence A's conjunct despite the \
         shared table name: {occurrence_b}"
    );
}

fn orders_local_rendering_conjunct() -> Json {
    serde_json::json!({
        "type": "predicate_greater",
        "left": table_col("ORDERS", "O_ORDERDATE"),
        "right": str_lit("1995-01-01")
    })
}

/// DataFusion refuses a two-argument `SECOND` while Exasol renders it.
fn orders_local_declined_conjunct() -> Json {
    serde_json::json!({
        "type": "predicate_greater",
        "left": {
            "type": "function_scalar",
            "name": "SECOND",
            "arguments": [
                table_col("ORDERS", "O_ORDERDATE"),
                num_lit(3)
            ]
        },
        "right": num_lit(1)
    })
}

fn orders_local_rendering_and_declined_filter() -> Json {
    serde_json::json!({
        "type": "predicate_and",
        "expressions": [
            orders_local_rendering_conjunct(),
            orders_local_declined_conjunct(),
        ],
    })
}

#[test]
fn declined_side_local_conjunct_partitions_to_residual() {
    let filter = orders_local_rendering_and_declined_filter();
    let rendering = orders_local_rendering_conjunct();
    let declined = orders_local_declined_conjunct();
    assert!(
        !datafusion_renderable(&declined) && datafusion_renderable(&rendering),
        "precondition: exactly one of the two conjuncts declines for DataFusion"
    );

    assert_eq!(
        declined_only(&filter),
        Some(declined.clone()),
        "declined_only must keep exactly the conjunct DataFusion cannot express"
    );
    assert_eq!(
        renderable_only(&filter),
        Some(rendering),
        "renderable_only must keep exactly its complement — the two are exact halves"
    );
    assert!(
        render_expression_exasol_safe(&declined).is_some(),
        "the residual conjunct must render in the Exasol dialect, or the outer \
         WHERE could not apply it either"
    );
    assert_eq!(
        customer_orders_legs().conjunct_leg(&declined),
        Some(1),
        "attribution is unchanged — only the RENDER declines, so the screen is \
         the sole reason this conjunct becomes residual"
    );
}

#[test]
fn rendering_side_local_conjunct_still_reaches_its_leg() {
    let filter = orders_local_rendering_and_declined_filter();
    let leg_eligible = renderable_only(&filter).expect("the rendering conjunct survives");

    let leg = render_df_filter_safe(
        &leg_local_filter(&leg_eligible, &customer_orders_legs(), 1)
            .expect("still ORDERS-leg-local"),
    )
    .expect("a DataFusion-renderable leg filter renders");

    assert!(
        leg.contains("'1995-01-01'") && !leg.contains("SECOND"),
        "the rendering conjunct must reach the leg and the declined one must not: {leg}"
    );
}

#[test]
fn join_side_pruning_input_unchanged_when_df_render_declines() {
    let filter = orders_local_rendering_and_declined_filter();

    let legs = customer_orders_legs();
    let pruning = leg_local_filter(&filter, &legs, 1).expect("both conjuncts are ORDERS-leg-local");
    let mut pruning_conjuncts = Vec::new();
    flatten_conjuncts(&pruning, &mut pruning_conjuncts);
    assert_eq!(
        pruning_conjuncts.len(),
        2,
        "pruning must still receive BOTH leg-local conjuncts: {pruning}"
    );
    assert!(
        pruning_conjuncts.iter().any(|c| !datafusion_renderable(c)),
        "the declined conjunct must still be in the pruning input: {pruning}"
    );

    let leg = leg_local_filter(
        &renderable_only(&filter).expect("the rendering conjunct survives"),
        &legs,
        1,
    )
    .expect("the rendering conjunct is still ORDERS-leg-local");
    let mut leg_conjuncts = Vec::new();
    flatten_conjuncts(&leg, &mut leg_conjuncts);
    assert_eq!(
        leg_conjuncts.len(),
        1,
        "the screened leg filter must carry only the rendering conjunct: {leg}"
    );
    assert!(
        leg_conjuncts.iter().all(|c| datafusion_renderable(c)),
        "the screened leg filter must omit the declined conjunct: {leg}"
    );
}

fn customer_col_types() -> Vec<(String, ExasolType)> {
    involved_table(&join_request(Json::Null, equi_condition()), "CUSTOMER")
        .unwrap()
        .types
}

fn orders_col_types() -> Vec<(String, ExasolType)> {
    involved_table(&join_request(Json::Null, equi_condition()), "ORDERS")
        .unwrap()
        .types
}

fn like_over(column: &str, table: &str, pattern: &str) -> Json {
    serde_json::json!({
        "type": "predicate_like",
        "expression": table_col(table, column),
        "pattern": str_lit(pattern)
    })
}

fn and_of(conjuncts: Vec<Json>) -> Json {
    serde_json::json!({"type": "predicate_and", "expressions": conjuncts})
}

fn conjunct_count(filter: Option<&Json>) -> usize {
    filter.map_or(0, |f| {
        let mut out = Vec::new();
        flatten_conjuncts(f, &mut out);
        out.len()
    })
}

#[test]
fn type_screened_leg_filter_pushes_whole_accepted_set_rewritten() {
    let filter = and_of(vec![
        like_over("O_ORDERDATE", "ORDERS", "1995%"),
        orders_local_rendering_conjunct(),
    ]);

    let (leg, declined) = type_screened_leg_filter(&filter, &column_types(&orders_col_types()));

    assert!(
        declined.is_none(),
        "no conjunct declines, so nothing may be handed to the outer wrapper: {declined:?}"
    );
    let leg = leg.expect("an all-accepted side-local set must reach its leg");
    assert_eq!(
        conjunct_count(Some(&leg)),
        2,
        "both accepted conjuncts must reach the leg: {leg}"
    );
    let rendered = render_df_filter_safe(&leg).expect("the rewritten set must render");
    assert!(
        rendered.contains(r#"CAST("O_ORDERDATE" AS VARCHAR)"#) && rendered.contains("LIKE"),
        "the DATE LIKE subject must reach the leg CAST to VARCHAR, not bare: {rendered}"
    );
}

#[test]
fn type_screened_leg_filter_declines_whole_set_when_no_conjunct_survives() {
    let decimal_like = like_over("O_CUSTKEY", "ORDERS", "1%");
    let filter = and_of(vec![decimal_like.clone(), orders_local_declined_conjunct()]);

    let (leg, declined) = type_screened_leg_filter(&filter, &column_types(&orders_col_types()));

    assert!(
        leg.is_none(),
        "no conjunct may be pushed when every one of them declines: {leg:?}"
    );
    let declined = declined.expect("a fully declined side-local set must go residual");
    let mut conjuncts = Vec::new();
    flatten_conjuncts(&declined, &mut conjuncts);
    assert_eq!(
        conjuncts.len(),
        2,
        "both declined conjuncts must reach the outer wrapper: {declined}"
    );
    assert!(
        conjuncts.contains(&&decimal_like),
        "the DECIMAL LIKE must be carried RAW so the Exasol dialect can render it: \
         {declined}"
    );
}

#[test]
fn type_screened_leg_filter_partition_is_total_and_fails_closed() {
    let date_like = like_over("O_ORDERDATE", "ORDERS", "1995%");
    let decimal_like = like_over("O_CUSTKEY", "ORDERS", "1%");
    let mixed = and_of(vec![
        date_like.clone(),
        decimal_like.clone(),
        orders_local_rendering_conjunct(),
    ]);
    let cases = [
        (
            "all accepted",
            and_of(vec![date_like, orders_local_rendering_conjunct()]),
        ),
        (
            "all declined",
            and_of(vec![decimal_like.clone(), orders_local_declined_conjunct()]),
        ),
        ("mixed", mixed.clone()),
    ];

    for (label, filter) in cases {
        let input = conjunct_count(Some(&filter));
        let (leg, declined) = type_screened_leg_filter(&filter, &column_types(&orders_col_types()));
        assert_eq!(
            conjunct_count(leg.as_ref()) + conjunct_count(declined.as_ref()),
            input,
            "{label}: every conjunct must land in exactly one half — none dropped, \
             none double-applied"
        );
        if leg.is_none() {
            assert_eq!(
                conjunct_count(declined.as_ref()),
                input,
                "{label}: fail closed — with no leg half the declined half must carry \
                 the WHOLE side-local set"
            );
        }
    }

    let (leg, declined) = type_screened_leg_filter(&mixed, &column_types(&orders_col_types()));
    let leg_sql = render_df_filter_safe(&leg.expect("the two accepted conjuncts form a leg"))
        .expect("the rewritten accepted set must render for DataFusion");
    assert!(
        leg_sql.contains(r#"CAST("O_ORDERDATE" AS VARCHAR)"#) && !leg_sql.contains("O_CUSTKEY"),
        "one type-declining conjunct must not forfeit its side's other pushable \
         conjuncts: {leg_sql}"
    );
    assert_eq!(
        declined,
        Some(decimal_like),
        "the declined half must be exactly the type-declined conjunct, RAW"
    );
}

#[test]
fn type_screened_leg_filter_declines_type_accepted_but_unrenderable_rewrite() {
    let col_types = customer_col_types();
    let filter = serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "function_scalar", "name": "SECOND", "arguments": [
            {"type": "function_scalar_cast", "name": "CAST",
             "dataType": {"type": "VARCHAR", "size": 40},
             "arguments": [
                 table_col("CUSTOMER", "C_CUSTKEY")]},
            num_lit(3)
        ]},
        "right": num_lit(1)
    });

    let rewritten = apply_type_rewrites(&filter, &column_types(&col_types))
        .expect("precondition: the type pipeline ACCEPTS this tree");
    assert!(
        rewritten.to_string().contains("decimal_to_varchar_exasol"),
        "precondition: the pipeline REWRITES the DECIMAL stringification: {rewritten}"
    );
    assert!(
        !datafusion_renderable(&rewritten),
        "precondition: the REWRITTEN tree still does not render for DataFusion"
    );

    let (leg, declined) = type_screened_leg_filter(&filter, &column_types(&col_types));

    assert!(
        leg.is_none(),
        "a rewrite the DataFusion dialect cannot render must not reach the leg: {leg:?}"
    );
    assert_eq!(
        declined,
        Some(filter),
        "it must reach the outer wrapper RAW — with the plain CAST, not the \
         DataFusion-only rewrite the Exasol dialect has no node for"
    );
}

#[test]
fn type_screened_leg_filter_uses_owning_side_types_for_shared_column_name() {
    let request = serde_json::json!({
        "involvedTables": [
            {"name": "DATED", "columns": [
                {"name": "SHARED_KEY", "dataType": {"type": "date"}}]},
            {"name": "NUMBERED", "columns": [
                {"name": "SHARED_KEY",
                 "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
        ]
    });
    let filter = like_over("SHARED_KEY", "DATED", "1995%");

    let (date_leg, date_declined) = type_screened_leg_filter(
        &filter,
        &column_types(&involved_table(&request, "DATED").unwrap().types),
    );
    let date_sql = render_df_filter_safe(
        &date_leg.expect("the DATE side accepts the LIKE through the CAST rewrite"),
    )
    .expect("the CAST-rewrapped LIKE renders for DataFusion");
    assert!(
        date_sql.contains(r#"CAST("SHARED_KEY" AS VARCHAR)"#),
        "the DATE side must push the CAST-rewritten LIKE into its leg: {date_sql}"
    );
    assert!(
        date_declined.is_none(),
        "the DATE side declines nothing: {date_declined:?}"
    );

    let (numeric_leg, numeric_declined) = type_screened_leg_filter(
        &filter,
        &column_types(&involved_table(&request, "NUMBERED").unwrap().types),
    );
    assert!(
        numeric_leg.is_none(),
        "the DECIMAL side has no safe rewrite, so nothing may reach its leg: \
         {numeric_leg:?}"
    );
    assert_eq!(
        numeric_declined,
        Some(filter),
        "the DECIMAL side must hand the SAME conjunct to the outer wrapper RAW"
    );
}

#[test]
fn referenced_leg_columns_narrows_to_used_columns() {
    let pushdown_req = pushdown_request("CUSTOMER", &[("C_NAME", ExasolType::WIDEST_VARCHAR)])
        .filter(predicate_equal(
            table_col("CUSTOMER", "C_ADDRESS"),
            str_lit("z"),
        ))
        .column(table_col("CUSTOMER", "C_NAME"))
        .build_pushdown();
    let condition = predicate_equal(
        table_col("CUSTOMER", "C_CUSTKEY"),
        table_col("ORDERS", "O_CUSTKEY"),
    );
    let full = vec![
        ("C_CUSTKEY".to_string(), ExasolType::decimal(20, 0)),
        ("C_NAME".to_string(), ExasolType::varchar(100)),
        ("C_ADDRESS".to_string(), ExasolType::varchar(100)),
        ("C_PHONE".to_string(), ExasolType::varchar(20)),
    ];
    let narrowed = referenced_leg_columns(
        pushdown_req.json(),
        &condition,
        &customer_orders_legs(),
        0,
        &column_types(&full),
    );
    let names: Vec<&str> = narrowed
        .iter()
        .map(|(column, _)| column.name.as_str())
        .collect();
    assert_eq!(
        names,
        vec!["C_CUSTKEY", "C_NAME", "C_ADDRESS"],
        "narrows to condition + select + filter columns, in full-column order, dropping C_PHONE"
    );
    assert_eq!(
        narrowed
            .iter()
            .nth(1)
            .map(|(column, column_type)| (column.name.as_str(), column_type.exa_type)),
        Some(("C_NAME", ExasolType::varchar(100)))
    );
}

#[test]
fn referenced_leg_columns_keeps_all_when_select_list_absent() {
    let condition = predicate_equal(
        table_col("CUSTOMER", "C_CUSTKEY"),
        table_col("ORDERS", "O_CUSTKEY"),
    );
    let full = vec![
        ("C_CUSTKEY".to_string(), ExasolType::decimal(20, 0)),
        ("C_NAME".to_string(), ExasolType::varchar(100)),
    ];
    let narrowed = referenced_leg_columns(
        &serde_json::json!({}),
        &condition,
        &customer_orders_legs(),
        0,
        &column_types(&full),
    );
    assert_eq!(
        narrowed,
        column_types(&full),
        "an absent select list ⇒ SELECT *, keep every column"
    );
}

#[test]
fn referenced_leg_columns_keeps_all_when_narrowing_empty() {
    let pushdown_req = pushdown_request("CUSTOMER", &[("C_NAME", ExasolType::WIDEST_VARCHAR)])
        .column(table_col("CUSTOMER", "C_NAME"))
        .build_pushdown();
    let condition = equi_condition();
    let full = vec![
        ("L_ORDERKEY".to_string(), ExasolType::decimal(20, 0)),
        ("L_QUANTITY".to_string(), ExasolType::decimal(18, 2)),
    ];
    let narrowed = referenced_leg_columns(
        pushdown_req.json(),
        &condition,
        &legs_of(&["CUSTOMER", "ORDERS", "LINEITEM"]),
        2,
        &column_types(&full),
    );
    assert_eq!(
        narrowed,
        column_types(&full),
        "no clause references a LINEITEM column ⇒ keep every column rather than \
         emit a zero-column leg"
    );
}

#[test]
fn column_collectors_keep_divergent_case_folding() {
    let expr = table_col("CUSTOMER", "straße");

    let mut unicode_folded = std::collections::HashSet::new();
    collect_all_column_names(&expr, &mut unicode_folded);
    assert_eq!(
        unicode_folded,
        std::collections::HashSet::from(["STRASSE".to_string()]),
        "collect_all_column_names folds ß to SS via Unicode to_uppercase"
    );

    let ascii_folded = possible_side_column_names(&expr, "CUSTOMER");
    assert_eq!(
        ascii_folded,
        std::collections::HashSet::from(["STRAßE".to_string()]),
        "the joins module's walks leave ß untouched via to_ascii_uppercase"
    );

    assert_ne!(
        unicode_folded, ascii_folded,
        "the two folds are NOT interchangeable and MUST NOT be unified"
    );
}

#[test]
fn side_fan_out_pushes_bare_side_local_filter_into_common_blob() {
    let side = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]);
    let cols = vec![
        ("O_CUSTKEY".to_string(), ExasolType::decimal(20, 0)),
        ("O_ORDERDATE".to_string(), ExasolType::Date),
    ];
    let filter = serde_json::json!({
        "type": "predicate_greater",
        "left": aliased_col("FACT_ORDERS", "O", "O_ORDERDATE"),
        "right": str_lit("1995-01-01")
    });

    let sql_with = build_side_fan_out_sql(
        &side,
        &column_types(&cols),
        Some(&filter),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("selecting the wire storage must succeed");
    let common = common_arg_literal(&sql_with);
    assert!(
        common.contains("\"filter\"") && common.contains("O_ORDERDATE"),
        "the side-local filter must be pushed into the fan-out common blob: {common}"
    );
    assert!(
        !common.contains(r#"\"O\".\"O_ORDERDATE\""#) && !common.contains(r#""O"."O_ORDERDATE""#),
        "the fan-out filter MUST be bare (alias stripped), never alias-qualified: {common}"
    );

    let sql_without = build_side_fan_out_sql(
        &side,
        &column_types(&cols),
        None,
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("selecting the wire storage must succeed");
    let common_none = common_arg_literal(&sql_without);
    assert!(
        !common_none.contains("\"filter\""),
        "no side-local filter ⇒ no filter field in the common blob: {common_none}"
    );
}

#[test]
fn side_fan_out_routes_through_distributor_scalar_scan_no_wrapper() {
    let side = resolved_side(
        "ORDERS",
        vec![("s3://w/o-0.parquet", 100), ("s3://w/o-1.parquet", 100)],
    );
    let cols = vec![("O_CUSTKEY".to_string(), ExasolType::decimal(20, 0))];
    let tuning = JoinScanRequestConfig {
        cluster_nodes: 2,
        parallelism_factor: 1,
        ..two_scan_tuning()
    };
    let sql = build_side_fan_out_sql(
        &side,
        &column_types(&cols),
        None,
        &tuning,
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("selecting the wire storage must succeed");

    assert!(
        !sql.contains("SELECT * FROM ("),
        "the leg must not use a SELECT * materialization wrapper: {sql}"
    );
    assert!(
        sql.starts_with("SELECT SCAN("),
        "the leg is the outer ungrouped scalar scan itself: {sql}"
    );
    assert!(
        sql.contains("DISTRIBUTE(files) FROM (VALUES")
            && sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
        "the leg's fan-out GROUP BY shard_key must live in the distributor: {sql}"
    );
}

#[test]
fn broadcast_fact_side_uses_distributor_scalar_scan() {
    let fact = resolved_side(
        "LINEITEM",
        vec![("s3://w/l-0.parquet", 1000), ("s3://w/l-1.parquet", 1000)],
    );
    let dimension = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 10)]);
    let sides = broadcast_sides(fact, dimension, 1);
    let rendered = rendered_join(
        r#""LHS_T0"."L_ORDERKEY" = "LHS_T1"."O_ORDERKEY""#,
        &[ProjectedColumn {
            item: ProjectionItem::Column(ColumnRef::qualified("LHS_T0", "L_ORDERKEY")),
            exa_type: ExasolType::decimal(20, 0),
        }],
    );
    let tuning = JoinScanRequestConfig {
        cluster_nodes: 2,
        parallelism_factor: 1,
        ..two_scan_tuning()
    };
    let sql = build_broadcast_join_sql(&sides, &rendered, &tuning, "SCAN", "DISTRIBUTE")
        .expect("selecting the wire storage must succeed")
        .expect("an unbounded broadcast join must build");

    assert!(
        !sql.contains("SELECT * FROM ("),
        "the broadcast fact side must not use a SELECT * wrapper: {sql}"
    );
    assert!(
        sql.starts_with("SELECT SCAN("),
        "the fact side is the outer ungrouped scalar scan itself: {sql}"
    );
    assert!(
        sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
        "the fact side fans out via the nested shard_key distributor: {sql}"
    );
}

#[test]
fn render_broadcast_join_replaces_native_table_alias_in_filter_with_leg_alias() {
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = serde_json::json!({
        "type": "predicate_greater",
        "left": aliased_col("ORDERS", "O", "O_ORDERDATE"),
        "right": str_lit("1995-01-01")
    });
    let detected = detected_join(&request);
    let rendered = render_broadcast_join(
        &parsed_pushdown(&request),
        &detected,
        &request_sides(&request, &detected),
    )
    .expect("renders")
    .expect("an equi-join renders");
    let filter = rendered.filter.expect("filter renders");
    assert!(
        filter.contains(r#""LHS_T1"."O_ORDERDATE""#),
        "the filter must reference the column on its leg's relation: {filter}"
    );
    assert!(
        !filter.contains(r#""O"."O_ORDERDATE""#),
        "Exasol's native tableAlias names no relation of the scan's join: {filter}"
    );
}

#[test]
fn render_broadcast_join_replaces_native_table_alias_in_condition_with_leg_alias() {
    let condition = predicate_equal(
        aliased_col("CUSTOMER", "C", "C_CUSTKEY"),
        aliased_col("ORDERS", "O", "O_CUSTKEY"),
    );
    let request = join_request(Json::Null, condition);
    let detected = detected_join(&request);
    let rendered = render_broadcast_join(
        &parsed_pushdown(&request),
        &detected,
        &request_sides(&request, &detected),
    )
    .expect("renderable join")
    .expect("an equi-join must render, not decline");
    assert_eq!(
        rendered.condition,
        r#"("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY")"#
    );
}

#[test]
fn render_broadcast_join_replaces_native_table_alias_in_projection_with_leg_alias() {
    let mut request = join_request(Json::Null, equi_condition());
    request = selecting(
        request,
        &[(
            serde_json::json!({"type": "function_scalar", "name": "MULT", "arguments": [
                aliased_col("ORDERS", "O", "O_CUSTKEY"),
                num_lit(2)
            ]}),
            ExasolType::WIDEST_VARCHAR,
        )],
    );
    let detected = detected_join(&request);
    let rendered = render_broadcast_join(
        &parsed_pushdown(&request),
        &detected,
        &request_sides(&request, &detected),
    )
    .expect("renders")
    .expect("an equi-join renders");
    assert_eq!(rendered.projection.len(), 1, "{:?}", rendered.projection);
    match &rendered.projection[0].item {
        ProjectionItem::Expr { expr } => {
            assert_eq!(expr, r#"("LHS_T1"."O_CUSTKEY" * 2)"#);
        }
        other => panic!("expected an Expr projection item, got {other:?}"),
    }
}

#[test]
fn unified_join_prunes_and_narrows_each_leg() {
    let request = with_identifier_notes(
        pushdown_request(
            "CUSTOMER",
            &[
                ("C_CUSTKEY", ExasolType::decimal(20, 0)),
                ("C_NAME", ExasolType::varchar(100)),
                ("C_ADDRESS", ExasolType::varchar(100)),
            ],
        )
        .table(
            "ORDERS",
            &[
                ("O_CUSTKEY", ExasolType::decimal(20, 0)),
                ("O_ORDERDATE", ExasolType::Date),
                ("O_TOTALPRICE", ExasolType::decimal(20, 2)),
            ],
        )
        .field("type", serde_json::json!("select"))
        .field("from", customer_orders_join())
        .filter(serde_json::json!({"type": "predicate_and", "expressions": [
                    predicate_equal(table_col("CUSTOMER", "C_NAME"), str_lit("ACME")),
                    {"type": "predicate_greater",
                     "left": table_col("ORDERS", "O_ORDERDATE"),
                     "right": str_lit("1995-01-01")}]}))
        .column(table_col("CUSTOMER", "C_NAME"))
        .column(table_col("ORDERS", "O_ORDERDATE"))
        .request_field("schemaMetadataInfo", serde_json::json!({"properties": {}}))
        .build()
        .0,
        &[("CUSTOMER", "lh.customer"), ("ORDERS", "lh.orders")],
    );

    let detected = detected_join(&request);
    let sides = vec![
        resolved_side("CUSTOMER", vec![("s3://w/c.parquet", 10)]),
        resolved_side("ORDERS", vec![("s3://w/o.parquet", 100)]),
    ];
    let sql = build_n_scan_join_sql(
        &parsed_pushdown(&request),
        &detected,
        &request_typed(&request, &sides),
        &two_scan_tuning(),
        "SCAN",
        "DISTRIBUTE",
    )
    .expect("unified wrapper must build");

    assert!(
        !sql.contains("C_ADDRESS"),
        "an unreferenced CUSTOMER column must be narrowed out of the fan-out: {sql}"
    );
    assert!(
        !sql.contains("O_TOTALPRICE"),
        "an unreferenced ORDERS column must be narrowed out of the fan-out: {sql}"
    );

    assert_eq!(
        sql.matches("\"filter\"").count(),
        2,
        "both fan-out legs must carry a side-local ScanSpec.filter: {sql}"
    );

    assert!(
        sql.contains("'ACME'") && sql.contains("'1995-01-01'"),
        "each leg's side-local conjunct must be pushed into its fan-out: {sql}"
    );
    assert!(
        !sql.contains(" WHERE "),
        "no cross-table residual conjunct remains, so the outer WHERE is omitted: {sql}"
    );
    assert!(
        sql.contains(r#"ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
        "the equi-condition attaches to the join point's ON clause: {sql}"
    );
}

/// Scenario: A filter on a binary column selects what native Exasol selects over the hex text
#[test]
fn both_join_paths_compare_a_hex_literal_on_a_binary_column_by_its_bytes() {
    let c_name = [nullable_field("c_name", "binary")];
    let filter = predicate_equal(table_col("CUSTOMER", "C_NAME"), str_lit("0a1b"));
    let mut request = join_request(Json::Null, equi_condition());
    request["pushdownRequest"]["filter"] = filter.clone();
    let detected = detected_join(&request);
    let mut sides = request_sides(&request, &detected);
    sides[0].column_types = declared_column_types(
        &involved_table(&request, "CUSTOMER").unwrap().types,
        &c_name,
    );

    let rendered = render_broadcast_join(&parsed_pushdown(&request), &detected, &sides)
        .expect("renderable join")
        .expect("must render");
    assert_eq!(
        rendered.filter.as_deref(),
        Some(r#"("LHS_T0"."_LH_BYTES_C_NAME" = X'0a1b')"#)
    );

    let (leg, declined) = type_screened_leg_filter(&filter, &sides[0].column_types);
    assert!(declined.is_none(), "{declined:?}");
    assert_eq!(
        render_df_filter_safe(&leg.expect("the leg takes the comparison")).as_deref(),
        Some(r#"("_LH_BYTES_C_NAME" = X'0a1b')"#)
    );
}

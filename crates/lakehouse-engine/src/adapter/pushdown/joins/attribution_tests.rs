use super::super::planning::involved_table;
use super::*;
use crate::adapter::pushdown::support::involved_columns;
use crate::adapter::pushdown::test_support::{
    PushdownRequestBuilder, aliased_col, assert_mentions, col, customer_orders_join, not, num_lit,
    order_by_element, pd, predicate_equal, pushdown_request, scalar_fn, str_lit, table_col,
    user_message,
};
use crate::types::exasol_type::ExasolType;

impl super::JoinLegs {
    fn resolve(&self, node: &Json) -> ColumnLeg {
        self.resolve_column(ColumnNode::parse(node).expect("a named column node"))
    }
}

fn leaf(table_name: &str, table_alias: Option<&str>) -> JoinLeaf {
    JoinLeaf {
        table_name: table_name.to_string(),
        table_alias: table_alias.map(str::to_string),
        table_identifier: format!("lh.{}", table_name.to_ascii_lowercase()),
    }
}

/// `FROM FACT_ORDERS a JOIN FACT_ORDERS b` (#361's shape).
fn self_join_legs() -> JoinLegs {
    JoinLegs::from_leaves(&[
        leaf("FACT_ORDERS", Some("A")),
        leaf("FACT_ORDERS", Some("B")),
    ])
}

fn two_table_legs() -> JoinLegs {
    JoinLegs::from_leaves(&[leaf("CUSTOMER", Some("C")), leaf("ORDERS", Some("O"))])
}

#[test]
fn exact_table_name_and_alias_pair_selects_its_own_leg() {
    let legs = self_join_legs();

    let first = legs.resolve(&aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"));
    let second = legs.resolve(&aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"));

    assert_eq!(first, ColumnLeg::Leg(0));
    assert_eq!(second, ColumnLeg::Leg(1));
}

#[test]
fn absent_alias_is_a_distinct_leg_key() {
    let legs = JoinLegs::from_leaves(&[leaf("FACT_ORDERS", None), leaf("FACT_ORDERS", Some("B"))]);

    let unaliased = legs.resolve(&table_col("FACT_ORDERS", "O_ORDERKEY"));
    let aliased = legs.resolve(&aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"));

    assert_eq!(unaliased, ColumnLeg::Leg(0));
    assert_eq!(aliased, ColumnLeg::Leg(1));
}

#[test]
fn single_leg_table_name_resolves_without_consulting_an_alias() {
    let legs = two_table_legs();

    let no_alias = legs.resolve(&table_col("CUSTOMER", "C_CUSTKEY"));
    let foreign_alias = legs.resolve(&aliased_col("ORDERS", "STALE", "O_CUSTKEY"));

    assert_eq!(no_alias, ColumnLeg::Leg(0));
    assert_eq!(foreign_alias, ColumnLeg::Leg(1));
}

#[test]
fn alias_matching_is_verbatim_not_case_folded() {
    let legs = JoinLegs::from_leaves(&[
        leaf("FACT_ORDERS", Some("myAlias")),
        leaf("FACT_ORDERS", Some("B")),
    ]);

    let verbatim = legs.resolve(&aliased_col("FACT_ORDERS", "myAlias", "O_ORDERKEY"));
    let folded = legs.resolve(&aliased_col("FACT_ORDERS", "MYALIAS", "O_ORDERKEY"));

    assert_eq!(verbatim, ColumnLeg::Leg(0));
    assert_eq!(folded, ColumnLeg::Unattributable);
}

#[test]
fn a_column_no_leg_declares_fails_a_join_and_stays_bare_in_a_single_scan() {
    let legs = self_join_legs();
    let single = JoinLegs::for_single_scan(&serde_json::json!({
        "involvedTables": [{ "name": "FACT_ORDERS" }],
    }));
    let foreign = aliased_col("DIM_CUSTOMER", "C", "C_CUSTKEY");

    assert_eq!(legs.resolve(&foreign), ColumnLeg::NoLeg);
    assert_eq!(legs.resolve(&col("X")), ColumnLeg::NoLeg);
    legs.qualify(&foreign)
        .expect_err("binding it to either leg could return the other leg's values");
    assert_eq!(
        single
            .qualify(&col("X"))
            .expect("a single scan binds its one table"),
        col("X")
    );
}

#[test]
fn unattributable_column_is_reported_rather_than_resolved() {
    let legs = self_join_legs();

    let no_alias = legs.resolve(&table_col("FACT_ORDERS", "O_ORDERKEY"));
    let unknown_alias = legs.resolve(&aliased_col("FACT_ORDERS", "Z", "O_ORDERKEY"));

    assert_eq!(no_alias, ColumnLeg::Unattributable);
    assert_eq!(unknown_alias, ColumnLeg::Unattributable);
}

#[test]
fn qualify_tags_each_self_join_occurrence_with_its_own_leg_alias() {
    let legs = self_join_legs();
    let condition = predicate_equal(
        aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"),
        aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"),
    );

    let qualified = legs.qualify(&condition).expect("both occurrences resolve");

    assert_eq!(
        qualified["left"]["tableAlias"],
        Json::String("LHS_T0".into())
    );
    assert_eq!(
        qualified["right"]["tableAlias"],
        Json::String("LHS_T1".into())
    );
}

#[test]
fn qualify_overwrites_the_request_alias_and_leaves_the_input_untouched() {
    let legs = two_table_legs();
    let expr = aliased_col("ORDERS", "O", "O_CUSTKEY");

    let qualified = legs.qualify(&expr).expect("resolves to its one leg");

    assert_eq!(qualified["tableAlias"], Json::String("LHS_T1".into()));
    assert_eq!(expr["tableAlias"], Json::String("O".into()));
}

#[test]
fn qualify_reaches_columns_nested_in_arrays_and_function_nodes() {
    let legs = self_join_legs();
    let expr = scalar_fn("UPPER", [aliased_col("FACT_ORDERS", "B", "O_COMMENT")]);

    let qualified = legs.qualify(&expr).expect("the nested occurrence resolves");

    assert_eq!(
        qualified["arguments"][0]["tableAlias"],
        Json::String("LHS_T1".into())
    );
}

#[test]
fn qualify_fails_on_an_unattributable_column_naming_it() {
    let legs = self_join_legs();
    let expr = not(table_col("FACT_ORDERS", "O_ORDERKEY"));

    let err = legs.qualify(&expr).expect_err("no leg key matches");

    let message = err.to_string();
    assert!(message.contains("O_ORDERKEY"), "{message}");
    assert!(message.contains("FACT_ORDERS"), "{message}");
}

#[test]
fn legs_referenced_reports_every_leg_a_tree_touches() {
    let legs = self_join_legs();
    let condition = predicate_equal(
        aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"),
        aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"),
    );

    let referenced = legs.legs_referenced(&condition);

    assert_eq!(referenced.legs, BTreeSet::from([0, 1]));
    assert!(!referenced.has_unattributed);
    assert!(referenced.any_column);
}

#[test]
fn legs_referenced_flags_an_unattributed_column() {
    let legs = self_join_legs();
    let conjunct = predicate_equal(table_col("FACT_ORDERS", "O_ORDERKEY"), num_lit("7"));

    let referenced = legs.legs_referenced(&conjunct);

    assert!(referenced.has_unattributed);
    assert!(referenced.any_column);
    assert!(referenced.legs.is_empty());
}

#[test]
fn legs_referenced_flags_a_column_free_expression() {
    let legs = self_join_legs();

    let referenced = legs.legs_referenced(&predicate_equal(num_lit("1"), num_lit("1")));

    assert!(!referenced.any_column);
    assert!(!referenced.has_unattributed);
    assert!(referenced.legs.is_empty());
}

#[test]
fn attachment_leg_is_the_highest_leg_a_cross_leg_condition_references() {
    let legs = JoinLegs::from_leaves(&[
        leaf("FACT_ORDERS", Some("A")),
        leaf("FACT_ORDERS", Some("B")),
        leaf("FACT_ORDERS", Some("C")),
    ]);
    let condition = predicate_equal(
        aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"),
        aliased_col("FACT_ORDERS", "C", "O_ORDERKEY"),
    );

    assert_eq!(legs.attachment_leg(&condition), Some(2));
}

#[test]
fn attachment_leg_is_the_only_leg_a_single_leg_condition_references() {
    let legs = self_join_legs();
    let condition = predicate_equal(aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"), num_lit("7"));

    assert_eq!(legs.attachment_leg(&condition), Some(1));
}

#[test]
fn attachment_leg_is_none_for_a_column_free_condition() {
    let legs = self_join_legs();

    assert_eq!(
        legs.attachment_leg(&predicate_equal(num_lit("1"), num_lit("1"))),
        None
    );
}

#[test]
fn attachment_leg_is_none_when_any_column_is_unattributed() {
    let legs = self_join_legs();
    let condition = predicate_equal(
        aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"),
        aliased_col("FACT_ORDERS", "Z", "O_ORDERKEY"),
    );

    assert_eq!(legs.attachment_leg(&condition), None);
}

#[test]
fn conjunct_leg_is_the_single_leg_every_column_resolves_to() {
    let legs = self_join_legs();
    let conjunct = predicate_equal(aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"), num_lit("7"));

    assert_eq!(legs.conjunct_leg(&conjunct), Some(1));
}

#[test]
fn conjunct_leg_is_none_for_a_cross_leg_unattributed_or_column_free_conjunct() {
    let legs = self_join_legs();
    let cross_leg = predicate_equal(
        aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"),
        aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"),
    );
    let unattributed = predicate_equal(table_col("FACT_ORDERS", "O_ORDERKEY"), num_lit("7"));
    let foreign = predicate_equal(aliased_col("DIM_CUSTOMER", "C", "C_CUSTKEY"), num_lit("7"));

    assert_eq!(legs.conjunct_leg(&cross_leg), None);
    assert_eq!(legs.conjunct_leg(&unattributed), None);
    assert_eq!(legs.conjunct_leg(&foreign), None);
    assert_eq!(
        legs.conjunct_leg(&predicate_equal(num_lit("1"), num_lit("1"))),
        None
    );
}

#[test]
fn single_scan_binding_maps_every_involved_table_onto_leg_zero() {
    let request = serde_json::json!({
        "involvedTables": [{ "name": "FACT_ORDERS" }, { "name": "DIM_CUSTOMER" }],
    });
    let legs = JoinLegs::for_single_scan(&request);

    let first = legs.resolve(&aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"));
    let second = legs.resolve(&table_col("DIM_CUSTOMER", "C_CUSTKEY"));
    let outsider = legs.resolve(&table_col("SOMETHING_ELSE", "X"));
    let qualified = legs
        .qualify(&aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"))
        .expect("the single scan can never be unattributable");

    assert_eq!(legs.leg_count(), 1);
    assert_eq!(first, ColumnLeg::Leg(0));
    assert_eq!(second, ColumnLeg::Leg(0));
    assert_eq!(outsider, ColumnLeg::NoLeg);
    assert_eq!(qualified["tableAlias"], Json::String("LHS_T0".into()));
}

#[test]
fn repeated_involved_table_still_collapses_onto_leg_zero() {
    let request = serde_json::json!({
        "involvedTables": [{ "name": "FACT_ORDERS" }, { "name": "FACT_ORDERS" }],
    });
    let legs = JoinLegs::for_single_scan(&request);

    let resolved = legs.resolve(&aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"));

    assert_eq!(resolved, ColumnLeg::Leg(0));
}

#[test]
fn single_scan_binding_without_involved_tables_qualifies_nothing() {
    let legs = JoinLegs::for_single_scan(&serde_json::json!({}));
    let expr = aliased_col("FACT_ORDERS", "A", "O_ORDERKEY");

    assert_eq!(legs.resolve(&expr), ColumnLeg::NoLeg);
    assert_eq!(legs.qualify(&expr).expect("no leg is not a failure"), expr);
}

#[test]
fn leg_alias_is_the_wrapper_subquery_alias_of_that_leg() {
    let legs = JoinLegs::from_leaves(&[
        leaf("FACT_ORDERS", Some("A")),
        leaf("FACT_ORDERS", Some("B")),
        leaf("DIM_CUSTOMER", Some("C")),
    ]);

    assert_eq!(legs.leg_count(), 3);
    assert_eq!(legs.leg_alias(0), "LHS_T0");
    assert_eq!(legs.leg_alias(1), "LHS_T1");
    assert_eq!(legs.leg_alias(2), "LHS_T2");
}

/// [`JoinLegs::ensure_columns_declared`] as `handle_pushdown` runs it over `request`'s one table.
fn single_scan_gate(request: &Json) -> Result<(), UdfError> {
    let declared = involved_columns(request, |tables| tables.first()).unwrap();
    JoinLegs::for_single_scan(request).ensure_columns_declared(&pd(request), &[&declared])
}

/// [`JoinLegs::ensure_columns_declared`] as `plan_join` runs it, each leg declaring its table's
/// columns.
fn join_gate(request: &Json, leaves: &[JoinLeaf]) -> Result<(), UdfError> {
    let declared: Vec<InvolvedColumns<'_>> = leaves
        .iter()
        .map(|leaf| involved_table(request, &leaf.table_name).unwrap())
        .collect();
    JoinLegs::from_leaves(leaves)
        .ensure_columns_declared(&pd(request), &declared.iter().collect::<Vec<_>>())
}

fn id_name_table() -> PushdownRequestBuilder {
    pushdown_request(
        "T",
        &[
            ("ID", ExasolType::decimal(18, 0)),
            ("NAME", ExasolType::varchar(100)),
        ],
    )
}

fn customer_orders() -> (PushdownRequestBuilder, [JoinLeaf; 2]) {
    let request = pushdown_request(
        "CUSTOMER",
        &[
            ("C_CUSTKEY", ExasolType::decimal(20, 0)),
            ("C_NAME", ExasolType::varchar(100)),
        ],
    )
    .table(
        "ORDERS",
        &[
            ("O_ORDERKEY", ExasolType::decimal(20, 0)),
            ("O_CUSTKEY", ExasolType::decimal(20, 0)),
        ],
    )
    .field("from", customer_orders_join());
    (request, [leaf("CUSTOMER", None), leaf("ORDERS", None)])
}

#[test]
fn a_single_scan_whose_every_column_is_declared_passes() {
    let request = id_name_table()
        .column(table_col("T", "ID"))
        .filter(predicate_equal(table_col("T", "NAME"), str_lit("x")))
        .group_by(vec![table_col("T", "ID")])
        .order_by(serde_json::json!([order_by_element(
            table_col("T", "ID"),
            true,
            true
        )]))
        .build()
        .0;

    single_scan_gate(&request).expect("every column is declared");
}

/// Scenario: A column the request does not declare fails the request
#[test]
fn a_single_scan_column_its_table_does_not_declare_fails_the_request() {
    let missing = || table_col("T", "MISSING");
    let selecting_id = || id_name_table().column(table_col("T", "ID"));
    let cases = [
        (
            "selectList",
            id_name_table().item(missing(), ExasolType::decimal(18, 0)),
        ),
        (
            "filter",
            selecting_id().filter(predicate_equal(missing(), num_lit(1))),
        ),
        ("groupBy", selecting_id().group_by(vec![missing()])),
        (
            "orderBy",
            selecting_id().order_by(serde_json::json!([order_by_element(missing(), true, true)])),
        ),
    ];
    for (clause, request) in cases {
        let error = single_scan_gate(&request.build().0)
            .expect_err(&format!("an undeclared column in {clause} fails"));
        assert_mentions(&user_message(error), &["MISSING"]);
    }
}

#[test]
fn a_single_scan_column_without_a_table_name_binds_the_one_table() {
    let declared = id_name_table().column(col("ID")).build().0;
    let undeclared = id_name_table()
        .item(col("MISSING"), ExasolType::decimal(18, 0))
        .build()
        .0;

    single_scan_gate(&declared).expect("the one table declares ID");
    assert_mentions(
        &user_message(single_scan_gate(&undeclared).expect_err("the one table lacks MISSING")),
        &["MISSING"],
    );
}

#[test]
fn a_join_column_declared_on_its_own_leg_passes() {
    let (request, leaves) = customer_orders();
    let request = request
        .column(table_col("CUSTOMER", "C_NAME"))
        .column(table_col("ORDERS", "O_ORDERKEY"))
        .build()
        .0;

    join_gate(&request, &leaves).expect("each column is declared on the leg it names");
}

/// Scenario: A column the request does not declare fails the request
#[test]
fn a_join_column_that_binds_no_declaring_leg_fails_the_request() {
    let (request, leaves) = customer_orders();
    let cases = [
        (
            "a leg that does not declare it",
            request.clone().item(
                table_col("CUSTOMER", "O_CUSTKEY"),
                ExasolType::decimal(20, 0),
            ),
            "O_CUSTKEY",
        ),
        (
            "a table the query does not read",
            request
                .clone()
                .item(table_col("NOPE", "C_NAME"), ExasolType::varchar(100)),
            "C_NAME",
        ),
        (
            "no tableName",
            request.column(col("C_CUSTKEY")),
            "C_CUSTKEY",
        ),
    ];
    for (binding, request, column) in cases {
        let error = join_gate(&request.build().0, &leaves)
            .expect_err(&format!("a column naming {binding} fails"));
        assert_mentions(&user_message(error), &[column]);
    }
}

/// Scenario: A column the request does not declare fails the request
/// Scenario: A column reference no leg key matches fails loudly
#[test]
fn a_self_join_column_whose_alias_matches_no_occurrence_fails_the_request() {
    let request = pushdown_request("FACT_ORDERS", &[("O_ORDERKEY", ExasolType::decimal(20, 0))])
        .column(aliased_col("FACT_ORDERS", "C", "O_ORDERKEY"))
        .build()
        .0;
    let leaves = [
        leaf("FACT_ORDERS", Some("A")),
        leaf("FACT_ORDERS", Some("B")),
    ];

    let error = join_gate(&request, &leaves).expect_err("alias C names neither occurrence");
    assert_mentions(&user_message(error), &["O_ORDERKEY", "FACT_ORDERS"]);
}

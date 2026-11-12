use super::*;
use crate::adapter::pushdown::test_support::{
    PushdownRequestBuilder, aliased_col, assert_mentions, col, customer_orders_join, inner_join,
    num_lit, order_by_element, predicate_equal, pushdown_request, request_check, scalar_fn,
    table_col, table_ref_as, user_message,
};
use crate::types::exasol_type::ExasolType;

impl super::JoinLegs {
    fn resolve(&self, node: &Json) -> Option<usize> {
        self.leg_of(ColumnNode::parse(node).expect("a named column node"))
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

    assert_eq!(first, Some(0));
    assert_eq!(second, Some(1));
}

#[test]
fn absent_alias_is_a_distinct_leg_key() {
    let legs = JoinLegs::from_leaves(&[leaf("FACT_ORDERS", None), leaf("FACT_ORDERS", Some("B"))]);

    let unaliased = legs.resolve(&table_col("FACT_ORDERS", "O_ORDERKEY"));
    let aliased = legs.resolve(&aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"));

    assert_eq!(unaliased, Some(0));
    assert_eq!(aliased, Some(1));
}

#[test]
fn single_leg_table_name_resolves_without_consulting_an_alias() {
    let legs = two_table_legs();

    let no_alias = legs.resolve(&table_col("CUSTOMER", "C_CUSTKEY"));
    let foreign_alias = legs.resolve(&aliased_col("ORDERS", "STALE", "O_CUSTKEY"));

    assert_eq!(no_alias, Some(0));
    assert_eq!(foreign_alias, Some(1));
}

#[test]
fn alias_matching_is_verbatim_not_case_folded() {
    let legs = JoinLegs::from_leaves(&[
        leaf("FACT_ORDERS", Some("myAlias")),
        leaf("FACT_ORDERS", Some("B")),
    ]);

    let verbatim = legs.resolve(&aliased_col("FACT_ORDERS", "myAlias", "O_ORDERKEY"));
    let folded = legs.resolve(&aliased_col("FACT_ORDERS", "MYALIAS", "O_ORDERKEY"));

    assert_eq!(verbatim, Some(0));
    assert_eq!(folded, None);
}

#[test]
fn qualify_tags_each_self_join_occurrence_with_its_own_leg_alias() {
    let legs = self_join_legs();
    let condition = predicate_equal(
        aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"),
        aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"),
    );

    let qualified = legs.qualify(&condition);

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

    let qualified = legs.qualify(&expr);

    assert_eq!(qualified["tableAlias"], Json::String("LHS_T1".into()));
    assert_eq!(expr["tableAlias"], Json::String("O".into()));
}

#[test]
fn qualify_reaches_columns_nested_in_arrays_and_function_nodes() {
    let legs = self_join_legs();
    let expr = scalar_fn("UPPER", [aliased_col("FACT_ORDERS", "B", "O_COMMENT")]);

    let qualified = legs.qualify(&expr);

    assert_eq!(
        qualified["arguments"][0]["tableAlias"],
        Json::String("LHS_T1".into())
    );
}

#[test]
fn legs_referenced_reports_every_leg_a_tree_touches() {
    let legs = self_join_legs();
    let condition = predicate_equal(
        aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"),
        aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"),
    );

    assert_eq!(legs.legs_referenced(&condition), BTreeSet::from([0, 1]));
    assert_eq!(
        legs.legs_referenced(&predicate_equal(num_lit("1"), num_lit("1"))),
        BTreeSet::new()
    );
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
fn conjunct_leg_is_the_single_leg_every_column_resolves_to() {
    let legs = self_join_legs();
    let conjunct = predicate_equal(aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"), num_lit("7"));

    assert_eq!(legs.conjunct_leg(&conjunct), Some(1));
}

#[test]
fn conjunct_leg_is_none_for_a_cross_leg_or_column_free_conjunct() {
    let legs = self_join_legs();
    let cross_leg = predicate_equal(
        aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"),
        aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"),
    );

    assert_eq!(legs.conjunct_leg(&cross_leg), None);
    assert_eq!(
        legs.conjunct_leg(&predicate_equal(num_lit("1"), num_lit("1"))),
        None
    );
}

#[test]
fn leg_alias_is_the_wrapper_subquery_alias_of_that_leg() {
    let legs = JoinLegs::from_leaves(&[
        leaf("FACT_ORDERS", Some("A")),
        leaf("FACT_ORDERS", Some("B")),
        leaf("DIM_CUSTOMER", Some("C")),
    ]);

    assert_eq!(legs.leg_alias(0), "LHS_T0");
    assert_eq!(legs.leg_alias(1), "LHS_T1");
    assert_eq!(legs.leg_alias(2), "LHS_T2");
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

fn customer_orders() -> PushdownRequestBuilder {
    pushdown_request(
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
    .field("from", customer_orders_join())
}

/// `FROM FACT_ORDERS a JOIN FACT_ORDERS b ON a.O_ORDERKEY = b.O_ORDERKEY`.
fn fact_orders_self_join() -> PushdownRequestBuilder {
    pushdown_request("FACT_ORDERS", &[("O_ORDERKEY", ExasolType::decimal(20, 0))]).field(
        "from",
        inner_join(
            table_ref_as("FACT_ORDERS", Some("A")),
            table_ref_as("FACT_ORDERS", Some("B")),
            predicate_equal(
                aliased_col("FACT_ORDERS", "A", "O_ORDERKEY"),
                aliased_col("FACT_ORDERS", "B", "O_ORDERKEY"),
            ),
        ),
    )
}

/// Scenario: A column the request does not declare fails the request
#[test]
fn a_column_the_request_does_not_declare_fails_the_request() {
    let missing = || table_col("T", "MISSING");
    let selecting_id = || id_name_table().column(table_col("T", "ID"));
    let mut untagged = selecting_id().build_unchecked().0;
    untagged["pushdownRequest"]["selectList"][0]
        .as_object_mut()
        .expect("a column node")
        .remove("tableName");
    let join = customer_orders();
    let unchecked = |request: PushdownRequestBuilder| request.build_unchecked().0;
    let cases: [(&str, Json, &[&str]); 10] = [
        (
            "an undeclared column in selectList",
            unchecked(id_name_table().item(missing(), ExasolType::decimal(18, 0))),
            &["MISSING"],
        ),
        (
            "an undeclared column in filter",
            unchecked(selecting_id().filter(predicate_equal(missing(), num_lit(1)))),
            &["MISSING"],
        ),
        (
            "an undeclared column in groupBy",
            unchecked(selecting_id().group_by(vec![missing()])),
            &["MISSING"],
        ),
        (
            "an undeclared column in orderBy",
            unchecked(selecting_id().order_by(serde_json::json!([order_by_element(
                missing(),
                true,
                true
            )]))),
            &["MISSING"],
        ),
        (
            "a tableName naming another table",
            unchecked(id_name_table().item(table_col("OTHER", "ID"), ExasolType::decimal(18, 0))),
            &["ID", "OTHER"],
        ),
        (
            "a join leg that does not declare it",
            unchecked(join.clone().item(
                table_col("CUSTOMER", "O_CUSTKEY"),
                ExasolType::decimal(20, 0),
            )),
            &["O_CUSTKEY"],
        ),
        (
            "a table the join does not read",
            unchecked(
                join.clone()
                    .item(table_col("NOPE", "C_NAME"), ExasolType::varchar(100)),
            ),
            &["C_NAME"],
        ),
        (
            "a join column with no tableName",
            unchecked(join.column(col("C_CUSTKEY"))),
            &["C_CUSTKEY"],
        ),
        (
            "a self-join alias that names no occurrence",
            unchecked(fact_orders_self_join().column(aliased_col(
                "FACT_ORDERS",
                "C",
                "O_ORDERKEY",
            ))),
            &["O_ORDERKEY", "FACT_ORDERS"],
        ),
        ("a single-table column with no tableName", untagged, &["ID"]),
    ];
    for (case, request, mentioned) in cases {
        let error = request_check(&request).expect_err(&format!("{case} fails"));
        assert_mentions(&user_message(error), mentioned);
    }
}

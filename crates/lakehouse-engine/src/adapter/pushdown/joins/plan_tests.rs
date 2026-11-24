use super::super::tests::{customer_orders_legs, customer_orders_request, equi_condition};
use super::*;
use crate::adapter::pushdown::test_support::*;
use serde_json::{Value as Json, json};

#[test]
fn a_broadcast_pair_takes_the_chain_position_of_its_lowest_leg() {
    let plan = JoinPlan::broadcasting(4, &LegGroup::pair(1, 3));

    assert_eq!(
        plan.fan_outs(),
        &[LegGroup::plain(0), LegGroup::pair(1, 3), LegGroup::plain(2)]
    );
    assert_eq!(
        (0..4).map(|leg| plan.position(leg)).collect::<Vec<_>>(),
        [0, 1, 2, 1]
    );
}

#[test]
fn the_n_scan_holds_one_plain_fan_out_per_leg() {
    let plan = JoinPlan::n_scan(3);

    assert!(plan.fan_outs().iter().all(LegGroup::is_plain));
    assert_eq!(
        (0..3).map(|leg| plan.position(leg)).collect::<Vec<_>>(),
        [0, 1, 2]
    );
}

#[test]
fn a_two_leg_broadcast_serves_only_a_window_it_can_bind() {
    let ordered = |key: Json| {
        customer_orders_request(json!({}), equi_condition())
            .column(table_col("CUSTOMER", "C_NAME"))
            .order_by(json!([order_by_element(key, true, true)]))
    };
    let other_leg = || ordered(table_col("ORDERS", "O_CUSTKEY"));
    let declined = declined_second_predicate(table_col("ORDERS", "O_ORDERDATE"));
    // (case, request, legs, whether the broadcast serves its window)
    let cases = [
        (
            "a projected sort key",
            ordered(table_col("CUSTOMER", "C_NAME")),
            2,
            true,
        ),
        ("a sort key of the other leg", other_leg(), 2, false),
        (
            "beside a conjunct left for Exasol",
            other_leg().filter(declined),
            2,
            false,
        ),
        (
            "an aggregation",
            other_leg().group_by(vec![table_col("CUSTOMER", "C_NAME")]),
            2,
            true,
        ),
        ("three legs", other_leg(), 3, true),
    ];
    for (case, request, leg_count, serves) in cases {
        let pushdown = parsed_pushdown(&request.build().0);
        assert_eq!(
            broadcast_serves_window(&pushdown, &customer_orders_legs(), leg_count),
            serves,
            "{case}"
        );
    }
}

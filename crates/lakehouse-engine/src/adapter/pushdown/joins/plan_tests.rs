use super::super::tests::{customer_orders_legs, customer_orders_request, equi_condition};
use super::*;
use crate::adapter::pushdown::test_support::*;
use serde_json::json;

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

fn ordered_request(
    select: &[(&str, &str)],
    key: (&str, &str),
    extra: &[(&str, Json)],
) -> PushdownRequest {
    let mut builder = customer_orders_request(json!({}), equi_condition()).field(
        "orderBy",
        json!([{
            "type": "order_by_element",
            "expression": table_col(key.0, key.1),
            "isAscending": true,
            "nullsLast": true,
        }]),
    );
    for (table, column) in select {
        builder = builder.column(table_col(table, column));
    }
    for (name, value) in extra {
        builder = builder.field(name, value.clone());
    }
    parsed_pushdown(&builder.build().0)
}

#[test]
fn a_two_leg_broadcast_serves_only_a_window_it_can_bind() {
    let legs = customer_orders_legs();
    let projected = ordered_request(&[("CUSTOMER", "C_NAME")], ("CUSTOMER", "C_NAME"), &[]);
    let other_leg = ordered_request(&[("CUSTOMER", "C_NAME")], ("ORDERS", "O_CUSTKEY"), &[]);
    let grouped = ordered_request(
        &[("CUSTOMER", "C_NAME")],
        ("ORDERS", "O_CUSTKEY"),
        &[
            ("aggregationType", json!("group_by")),
            ("groupBy", json!([table_col("CUSTOMER", "C_NAME")])),
        ],
    );

    assert!(broadcast_serves_window(&projected, &legs, 2));
    assert!(!broadcast_serves_window(&other_leg, &legs, 2));
    assert!(broadcast_serves_window(&grouped, &legs, 2));
    assert!(broadcast_serves_window(&other_leg, &legs, 3));
}

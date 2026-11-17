use super::super::tests::{customer_orders_legs, self_join_legs};
use super::*;
use crate::adapter::pushdown::test_support::*;

const UNBOUNDED: Option<NScanReason> = Some(NScanReason::UnboundedDimensionColumn);

/// An aggregating join within budget whose aggregation reads `read` of dimension leg
/// `dimension_leg`.
fn aggregating<'a>(
    legs: &'a JoinLegs,
    dimension_leg: usize,
    filter: Option<&'a Json>,
    read: &'a ColumnTypes,
) -> BroadcastFacts<'a> {
    BroadcastFacts {
        dimension_bytes: 10,
        max_bytes: 100,
        legs,
        dimension_leg,
        filter,
        aggregation_reads: Some(read),
    }
}

#[test]
fn the_dimension_bytes_are_judged_against_the_budget_inclusively() {
    let legs = customer_orders_legs();
    let text = column_types(&[("C_NAME", ExasolType::WIDEST_VARCHAR)]);
    let cases = [
        ("an empty dimension", 0, None, None),
        ("at the budget", 100, None, None),
        ("one byte over", 101, None, Some(NScanReason::OverBudget)),
        (
            "over the budget and reading unbounded text",
            101,
            Some(&text),
            Some(NScanReason::OverBudget),
        ),
    ];
    for (case, dimension_bytes, aggregation_reads, reason) in cases {
        let facts = BroadcastFacts {
            dimension_bytes,
            max_bytes: 100,
            legs: &legs,
            dimension_leg: 0,
            filter: None,
            aggregation_reads,
        };
        assert_eq!(n_scan_reason(&facts), reason, "{case}");
    }
}

#[test]
fn an_aggregation_must_read_only_width_bounded_dimension_columns() {
    let customer = |name: &str| table_col("CUSTOMER", name);
    let text = [("C_NAME", ExasolType::WIDEST_VARCHAR)];
    let fixed = [
        ("C_KEY", ExasolType::decimal(18, 0)),
        ("C_SCORE", ExasolType::DoublePrecision),
        ("C_SINCE", ExasolType::Date),
        ("C_SEEN", ExasolType::BARE_TIMESTAMP),
        ("C_ACTIVE", ExasolType::Boolean),
    ];
    let named = string_equal("CUSTOMER", "C_NAME", "ACME");
    let order_date = compare(
        "predicate_greater",
        table_col("ORDERS", "O_ORDERDATE"),
        date_lit("1995-01-01"),
    );
    let cases = [
        ("fixed-width types", column_types(&fixed), None, None),
        ("no column read", column_types(NO_COLUMNS), None, None),
        ("unfiltered VARCHAR", column_types(&text), None, UNBOUNDED),
        (
            "unfiltered CHAR",
            column_types(&[("C_NAME", ExasolType::char(10))]),
            None,
            UNBOUNDED,
        ),
        (
            "an interval",
            column_types(&[("C_TERM", ExasolType::IntervalYearToMonth { precision: 2 })]),
            None,
            UNBOUNDED,
        ),
        (
            "col = literal",
            column_types(&text),
            Some(named.clone()),
            None,
        ),
        (
            "literal = col",
            column_types(&text),
            Some(predicate_equal(str_lit("ACME"), customer("C_NAME"))),
            None,
        ),
        (
            "IN literals",
            column_types(&text),
            Some(in_list(
                customer("C_NAME"),
                [str_lit("ACME"), str_lit("GLOBEX")],
            )),
            None,
        ),
        (
            "a literal conjunct beside another",
            column_types(&text),
            Some(and([order_date, named.clone()])),
            None,
        ),
        (
            "an OR of literals",
            column_types(&text),
            Some(or([named, string_equal("CUSTOMER", "C_NAME", "GLOBEX")])),
            UNBOUNDED,
        ),
        (
            "an IN list holding a column",
            column_types(&text),
            Some(in_list(
                customer("C_NAME"),
                [str_lit("ACME"), customer("C_CITY")],
            )),
            UNBOUNDED,
        ),
        (
            "a column on both sides",
            column_types(&text),
            Some(predicate_equal(
                customer("C_NAME"),
                table_col("ORDERS", "O_COMMENT"),
            )),
            UNBOUNDED,
        ),
        (
            "a range",
            column_types(&text),
            Some(compare("predicate_less", customer("C_NAME"), str_lit("B"))),
            UNBOUNDED,
        ),
        (
            "a literal on another column",
            column_types(&text),
            Some(string_equal("CUSTOMER", "C_CITY", "BERLIN")),
            UNBOUNDED,
        ),
    ];
    let legs = customer_orders_legs();
    for (case, read, filter, reason) in cases {
        let facts = aggregating(&legs, 0, filter.as_ref(), &read);
        assert_eq!(n_scan_reason(&facts), reason, "{case}");
    }
}

#[test]
fn a_literal_condition_bounds_only_its_own_self_join_leg() {
    let read = column_types(&[("O_COMMENT", ExasolType::WIDEST_VARCHAR)]);
    let filter = predicate_equal(
        aliased_col("FACT_ORDERS", "A", "O_COMMENT"),
        str_lit("rush"),
    );
    let legs = self_join_legs();
    assert_eq!(
        n_scan_reason(&aggregating(&legs, 0, Some(&filter), &read)),
        None
    );
    assert_eq!(
        n_scan_reason(&aggregating(&legs, 1, Some(&filter), &read)),
        UNBOUNDED
    );
}

use super::super::tests::{customer_orders_legs, legs_of, self_join_legs};
use super::*;
use crate::adapter::pushdown::test_support::*;

/// A join graph over owned inputs. Every conjunct renders unless listed in `declined`.
struct Graph {
    legs: JoinLegs,
    bytes: Vec<u64>,
    columns: Vec<ColumnTypes>,
    conditions: Vec<Json>,
    conjuncts: Vec<Json>,
    declined: Vec<Json>,
    above: Vec<Json>,
    aggregates: bool,
}

impl Graph {
    /// CUSTOMER (leg 0, 10 bytes) joined to ORDERS (leg 1, 100 bytes) on their customer key,
    /// grouping by `read`, the dimension's only columns.
    fn aggregating_customer_orders(read: ColumnTypes) -> Self {
        let read_columns = read
            .iter()
            .map(|(column, _)| table_col("CUSTOMER", &column.name))
            .collect();
        Self {
            legs: customer_orders_legs(),
            bytes: vec![10, 100],
            columns: vec![read, column_types(NO_COLUMNS)],
            conditions: vec![predicate_equal(
                table_col("CUSTOMER", "C_CUSTKEY"),
                table_col("ORDERS", "O_CUSTKEY"),
            )],
            conjuncts: Vec::new(),
            declined: Vec::new(),
            above: read_columns,
            aggregates: true,
        }
    }

    fn filtered(mut self, filter: Option<Json>) -> Self {
        self.conjuncts = filter
            .map(|filter| flatten_conjuncts(&filter).into_iter().cloned().collect())
            .unwrap_or_default();
        self
    }

    fn grouping(&self) -> Vec<LegGroup> {
        let columns: Vec<&ColumnTypes> = self.columns.iter().collect();
        let conjuncts: Vec<WhereConjunct<'_>> = self
            .conjuncts
            .iter()
            .map(|tree| WhereConjunct {
                tree,
                renders: !self.declined.contains(tree),
            })
            .collect();
        let above: Vec<&Json> = self.above.iter().collect();
        group_legs(&JoinGraph {
            legs: &self.legs,
            bytes: &self.bytes,
            columns: &columns,
            conditions: &self.conditions,
            conjuncts: &conjuncts,
            above: &above,
            output: OutputContract::Items,
            aggregates: self.aggregates,
            max_bytes: 100,
        })
    }

    /// Whether the one candidate pair is broadcast.
    fn broadcast(&self) -> bool {
        !self.grouping().is_empty()
    }
}

#[test]
fn the_dimension_bytes_are_judged_against_the_budget_inclusively() {
    let text = || column_types(&[("C_NAME", ExasolType::WIDEST_VARCHAR)]);
    let cases = [
        ("an empty dimension", 0, column_types(NO_COLUMNS), true),
        ("at the budget", 100, column_types(NO_COLUMNS), true),
        ("one byte over", 101, column_types(NO_COLUMNS), false),
        (
            "over the budget and reading unbounded text",
            101,
            text(),
            false,
        ),
    ];
    for (case, dimension_bytes, read, broadcast) in cases {
        let mut graph = Graph::aggregating_customer_orders(read);
        graph.bytes = vec![dimension_bytes, 1000];
        assert_eq!(graph.broadcast(), broadcast, "{case}");
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
        ("fixed-width types", column_types(&fixed), None, true),
        ("no column read", column_types(NO_COLUMNS), None, true),
        ("unfiltered VARCHAR", column_types(&text), None, false),
        (
            "unfiltered CHAR",
            column_types(&[("C_NAME", ExasolType::char(10))]),
            None,
            false,
        ),
        (
            "an interval",
            column_types(&[("C_TERM", ExasolType::IntervalYearToMonth { precision: 2 })]),
            None,
            false,
        ),
        (
            "col = literal",
            column_types(&text),
            Some(named.clone()),
            true,
        ),
        (
            "literal = col",
            column_types(&text),
            Some(predicate_equal(str_lit("ACME"), customer("C_NAME"))),
            true,
        ),
        (
            "IN literals",
            column_types(&text),
            Some(in_list(
                customer("C_NAME"),
                [str_lit("ACME"), str_lit("GLOBEX")],
            )),
            true,
        ),
        (
            "a literal conjunct beside another",
            column_types(&text),
            Some(and([order_date, named.clone()])),
            true,
        ),
        (
            "an OR of literals",
            column_types(&text),
            Some(or([named, string_equal("CUSTOMER", "C_NAME", "GLOBEX")])),
            false,
        ),
        (
            "an IN list holding a column",
            column_types(&text),
            Some(in_list(
                customer("C_NAME"),
                [str_lit("ACME"), customer("C_CITY")],
            )),
            false,
        ),
        (
            "a column on both sides",
            column_types(&text),
            Some(predicate_equal(
                customer("C_NAME"),
                table_col("ORDERS", "O_COMMENT"),
            )),
            false,
        ),
        (
            "a range",
            column_types(&text),
            Some(compare("predicate_less", customer("C_NAME"), str_lit("B"))),
            false,
        ),
        (
            "a literal on another column",
            column_types(&text),
            Some(string_equal("CUSTOMER", "C_CITY", "BERLIN")),
            false,
        ),
    ];
    for (case, read, filter, broadcast) in cases {
        let graph = Graph::aggregating_customer_orders(read).filtered(filter);
        assert_eq!(graph.broadcast(), broadcast, "{case}");
    }
}

#[test]
fn a_literal_condition_bounds_only_its_own_self_join_leg() {
    let comment = column_types(&[("O_COMMENT", ExasolType::WIDEST_VARCHAR)]);
    let filter = predicate_equal(
        aliased_col("FACT_ORDERS", "A", "O_COMMENT"),
        str_lit("rush"),
    );
    let self_join = |dimension_leg: usize| {
        let alias = ["A", "B"][dimension_leg];
        let mut bytes = vec![100, 100];
        bytes[1 - dimension_leg] = 1000;
        Graph {
            legs: self_join_legs(),
            bytes,
            columns: vec![comment.narrowed(|_| true), comment.narrowed(|_| true)],
            conditions: vec![predicate_equal(
                aliased_col("FACT_ORDERS", "A", "O_KEY"),
                aliased_col("FACT_ORDERS", "B", "O_KEY"),
            )],
            conjuncts: vec![filter.clone()],
            declined: Vec::new(),
            above: vec![aliased_col("FACT_ORDERS", alias, "O_COMMENT")],
            aggregates: true,
        }
    };

    assert!(self_join(0).broadcast());
    assert!(!self_join(1).broadcast());
}

#[test]
fn a_two_leg_row_scan_reads_unbounded_text_only_once_a_conjunct_is_left_for_exasol() {
    let text = || column_types(&[("C_NAME", ExasolType::WIDEST_VARCHAR)]);
    let declined = compare(
        "predicate_like",
        table_col("CUSTOMER", "C_NAME"),
        str_lit("A%"),
    );
    let mut row_scan = Graph::aggregating_customer_orders(text());
    row_scan.aggregates = false;
    assert!(row_scan.broadcast());

    let mut declining = Graph::aggregating_customer_orders(text()).filtered(Some(declined.clone()));
    declining.aggregates = false;
    declining.declined = vec![declined];
    assert!(!declining.broadcast());

    // Exasol applies a declined literal after the fan-out has emitted the column, so it bounds
    // nothing.
    let literal = string_equal("CUSTOMER", "C_NAME", "x");
    let mut declined_literal =
        Graph::aggregating_customer_orders(text()).filtered(Some(literal.clone()));
    declined_literal.declined = vec![literal];
    assert!(!declined_literal.broadcast());
}

/// LINEITEM (leg 0) joined to PART (leg 1) and SUPPLIER (leg 2) on its own keys.
fn star(bytes: [u64; 3], part_filter: Option<Json>) -> Graph {
    let lineitem = |name: &str| table_col("LINEITEM", name);
    Graph {
        legs: legs_of(&["LINEITEM", "PART", "SUPPLIER"]),
        bytes: bytes.to_vec(),
        columns: vec![
            column_types(&[("L_PARTKEY", ExasolType::decimal(18, 0))]),
            column_types(&[
                ("P_PARTKEY", ExasolType::decimal(18, 0)),
                ("P_BRAND", ExasolType::WIDEST_VARCHAR),
            ]),
            column_types(&[("S_SUPPKEY", ExasolType::decimal(18, 0))]),
        ],
        conditions: vec![
            predicate_equal(lineitem("L_PARTKEY"), table_col("PART", "P_PARTKEY")),
            predicate_equal(lineitem("L_SUPPKEY"), table_col("SUPPLIER", "S_SUPPKEY")),
        ],
        conjuncts: part_filter.into_iter().collect(),
        declined: Vec::new(),
        above: vec![lineitem("L_PARTKEY")],
        aggregates: false,
    }
}

#[test]
fn ranks_candidate_pairs_by_filter_then_fact_bytes_then_dimension_bytes_then_order() {
    let brand = string_equal("PART", "P_BRAND", "Brand#23");
    let ranked = |graph: &Graph| graph.grouping();

    assert_eq!(
        ranked(&star([90, 50, 10], Some(brand.clone()))),
        [LegGroup::pair(0, 1), LegGroup::pair(0, 2)],
        "the filtered dimension ranks first, though the other is smaller"
    );
    let mut declined = star([90, 50, 10], Some(brand.clone()));
    declined.declined = vec![brand];
    assert_eq!(
        ranked(&declined),
        [LegGroup::pair(0, 2)],
        "a filter the scan declines neither ranks nor bounds the text column Exasol reads over PART"
    );
    assert_eq!(
        ranked(&star([90, 50, 10], None)),
        [LegGroup::pair(0, 2), LegGroup::pair(0, 1)],
        "without a filter the smaller dimension ranks first"
    );
    assert_eq!(
        ranked(&star([90, 10, 10], None)),
        [LegGroup::pair(0, 1), LegGroup::pair(0, 2)],
        "a tie on every byte count takes FROM-tree order"
    );

    let mut chain = star([90, 50, 10], None);
    chain.conditions[1] = predicate_equal(
        table_col("PART", "P_SUPPKEY"),
        table_col("SUPPLIER", "S_SUPPKEY"),
    );
    assert_eq!(
        ranked(&chain),
        [LegGroup::pair(0, 1), LegGroup::pair(1, 2)],
        "the pair with the larger fact ranks first"
    );
}

#[test]
fn no_pair_is_broadcast_whose_dimension_is_over_budget_or_unbounded() {
    let mut graph = star([500, 50, 1000], None);
    graph.above.push(table_col("PART", "P_BRAND"));

    assert_eq!(
        graph.grouping(),
        [],
        "PART reads an unbounded text column, and LINEITEM, SUPPLIER's dimension, is over budget"
    );
}

#[test]
fn the_width_rule_applies_to_three_legs_without_aggregation() {
    let mut reading_brand = star([90, 50, 10], None);
    reading_brand.above.push(table_col("PART", "P_BRAND"));
    let mut bounded = star(
        [90, 50, 10],
        Some(string_equal("PART", "P_BRAND", "Brand#23")),
    );
    bounded.above.push(table_col("PART", "P_BRAND"));

    assert_eq!(reading_brand.grouping(), [LegGroup::pair(0, 2)]);
    assert_eq!(
        bounded.grouping(),
        [LegGroup::pair(0, 1), LegGroup::pair(0, 2)]
    );
}

#[test]
fn a_join_condition_left_for_exasol_counts_as_a_dimension_read() {
    let mut graph = star([90, 50, 10], None);
    graph.conditions.push(predicate_equal(
        table_col("PART", "P_BRAND"),
        table_col("SUPPLIER", "S_SUPPKEY"),
    ));

    assert_eq!(
        graph.grouping(),
        [LegGroup::pair(0, 2), LegGroup::pair(1, 2)]
    );
}

#[test]
fn the_smaller_leg_is_the_dimension_and_a_tie_makes_the_first_leg_it() {
    let roles = |bytes: [u64; 2]| {
        let mut graph = Graph::aggregating_customer_orders(column_types(NO_COLUMNS));
        graph.bytes = bytes.to_vec();
        graph.grouping()
    };

    assert_eq!(roles([10, 100]), [LegGroup::pair(1, 0)]);
    assert_eq!(roles([100, 10]), [LegGroup::pair(0, 1)]);
    assert_eq!(roles([0, 0]), [LegGroup::pair(1, 0)], "an empty tie");
    assert_eq!(roles([50, 50]), [LegGroup::pair(1, 0)]);
}

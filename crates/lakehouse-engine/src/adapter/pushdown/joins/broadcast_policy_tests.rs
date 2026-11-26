use super::super::tests::{customer_orders_legs, legs_of, self_join_legs};
use super::*;
use crate::adapter::pushdown::test_support::*;
use serde_json::Value as Json;
use vs_expression::Expr;

/// A join graph over owned inputs. Every conjunct renders unless listed in `declined`.
struct Graph {
    legs: JoinLegs,
    bytes: Vec<u64>,
    columns: Vec<ColumnTypes>,
    conditions: Vec<Expr>,
    conjuncts: Vec<Expr>,
    declined: Vec<Expr>,
    above: Vec<Expr>,
    aggregates: bool,
}

impl Graph {
    /// A row scan of `legs` with no WHERE, reading no column above the join.
    fn new(
        legs: JoinLegs,
        bytes: &[u64],
        columns: Vec<ColumnTypes>,
        conditions: Vec<Json>,
    ) -> Self {
        Self {
            legs,
            bytes: bytes.to_vec(),
            columns,
            conditions: conditions.iter().map(ast).collect(),
            conjuncts: Vec::new(),
            declined: Vec::new(),
            above: Vec::new(),
            aggregates: false,
        }
    }

    /// CUSTOMER (leg 0, 10 bytes) joined to ORDERS (leg 1, 100 bytes) on their customer key,
    /// grouping by `read`, the dimension's only columns.
    fn aggregating_customer_orders(read: ColumnTypes) -> Self {
        let read_columns: Vec<Json> = read
            .iter()
            .map(|(column, _)| table_col("CUSTOMER", &column.name))
            .collect();
        let condition = predicate_equal(
            table_col("CUSTOMER", "C_CUSTKEY"),
            table_col("ORDERS", "O_CUSTKEY"),
        );
        let columns = vec![read, column_types(NO_COLUMNS)];
        Self {
            aggregates: true,
            ..Self::new(customer_orders_legs(), &[10, 100], columns, vec![condition])
        }
        .reading(read_columns)
    }

    fn filtered(mut self, filter: Option<Json>) -> Self {
        self.conjuncts = filter
            .map(|filter| ast(&filter).conjuncts().into_iter().cloned().collect())
            .unwrap_or_default();
        self
    }

    /// The graph with `conjunct` in its WHERE, a conjunct the scan declines.
    fn declining(mut self, conjunct: Json) -> Self {
        let conjunct = ast(&conjunct);
        self.conjuncts.push(conjunct.clone());
        self.declined.push(conjunct);
        self
    }

    /// The graph whose clauses above the join also read `columns`.
    fn reading(mut self, columns: impl IntoIterator<Item = Json>) -> Self {
        self.above
            .extend(columns.into_iter().map(|column| ast(&column)));
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
        let above: Vec<&Expr> = self.above.iter().collect();
        group_legs(&JoinGraph {
            legs: &self.legs,
            bytes: &self.bytes,
            columns: &columns,
            conditions: &self.conditions,
            conjuncts: &conjuncts,
            above: &above,
            output: &OutputContract::Items(self.above.clone()),
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
        let mut bytes = [100, 100];
        bytes[1 - dimension_leg] = 1000;
        let condition = predicate_equal(
            aliased_col("FACT_ORDERS", "A", "O_KEY"),
            aliased_col("FACT_ORDERS", "B", "O_KEY"),
        );
        let columns = vec![comment.narrowed(|_| true), comment.narrowed(|_| true)];
        let alias = ["A", "B"][dimension_leg];
        Graph {
            aggregates: true,
            ..Graph::new(self_join_legs(), &bytes, columns, vec![condition])
        }
        .filtered(Some(filter.clone()))
        .reading([aliased_col("FACT_ORDERS", alias, "O_COMMENT")])
    };

    assert!(self_join(0).broadcast());
    assert!(!self_join(1).broadcast());
}

#[test]
fn a_two_leg_row_scan_reads_unbounded_text_only_once_a_conjunct_is_left_for_exasol() {
    let text = || column_types(&[("C_NAME", ExasolType::WIDEST_VARCHAR)]);
    let row_scan = || Graph {
        aggregates: false,
        ..Graph::aggregating_customer_orders(text())
    };
    let declined = like(table_col("CUSTOMER", "C_NAME"), "A%");
    // Exasol applies a declined literal after the fan-out has emitted the column, so it bounds
    // nothing.
    let literal = string_equal("CUSTOMER", "C_NAME", "x");
    let cases = [
        ("a row scan", row_scan(), true),
        ("a declined conjunct", row_scan().declining(declined), false),
        (
            "a declined literal",
            Graph::aggregating_customer_orders(text()).declining(literal),
            false,
        ),
    ];
    for (case, graph, broadcast) in cases {
        assert_eq!(graph.broadcast(), broadcast, "{case}");
    }
}

/// LINEITEM (leg 0) joined to PART (leg 1) and SUPPLIER (leg 2) on its own keys.
fn star(bytes: [u64; 3]) -> Graph {
    let lineitem = |name: &str| table_col("LINEITEM", name);
    let columns = vec![
        column_types(&[("L_PARTKEY", ExasolType::decimal(18, 0))]),
        column_types(&[
            ("P_PARTKEY", ExasolType::decimal(18, 0)),
            ("P_BRAND", ExasolType::WIDEST_VARCHAR),
        ]),
        column_types(&[("S_SUPPKEY", ExasolType::decimal(18, 0))]),
    ];
    let conditions = vec![
        predicate_equal(lineitem("L_PARTKEY"), table_col("PART", "P_PARTKEY")),
        predicate_equal(lineitem("L_SUPPKEY"), table_col("SUPPLIER", "S_SUPPKEY")),
    ];
    Graph::new(
        legs_of(&["LINEITEM", "PART", "SUPPLIER"]),
        &bytes,
        columns,
        conditions,
    )
    .reading([lineitem("L_PARTKEY")])
}

/// The eligible pairs of a three-leg join, in rank order.
#[test]
fn ranks_candidate_pairs_by_filter_then_fact_bytes_then_dimension_bytes_then_order() {
    let brand = || string_equal("PART", "P_BRAND", "Brand#23");
    let reading_brand = || star([90, 50, 10]).reading([table_col("PART", "P_BRAND")]);
    let mut chain = star([90, 50, 10]);
    chain.conditions[1] = ast(&predicate_equal(
        table_col("PART", "P_SUPPKEY"),
        table_col("SUPPLIER", "S_SUPPKEY"),
    ));
    let mut cross_dimension = star([90, 50, 10]);
    cross_dimension.conditions.push(ast(&predicate_equal(
        table_col("PART", "P_BRAND"),
        table_col("SUPPLIER", "S_SUPPKEY"),
    )));
    let cases = [
        (
            "the filtered dimension ranks first, though the other is smaller",
            star([90, 50, 10]).filtered(Some(brand())),
            vec![LegGroup::pair(0, 1), LegGroup::pair(0, 2)],
        ),
        (
            "a filter the scan declines neither ranks nor bounds the text column Exasol reads over PART",
            star([90, 50, 10]).declining(brand()),
            vec![LegGroup::pair(0, 2)],
        ),
        (
            "without a filter the smaller dimension ranks first",
            star([90, 50, 10]),
            vec![LegGroup::pair(0, 2), LegGroup::pair(0, 1)],
        ),
        (
            "a tie on every byte count takes FROM-tree order",
            star([90, 10, 10]),
            vec![LegGroup::pair(0, 1), LegGroup::pair(0, 2)],
        ),
        (
            "the pair with the larger fact ranks first",
            chain,
            vec![LegGroup::pair(0, 1), LegGroup::pair(1, 2)],
        ),
        (
            "the width rule applies to three legs without aggregation",
            reading_brand(),
            vec![LegGroup::pair(0, 2)],
        ),
        (
            "a literal condition bounds the text column",
            reading_brand().filtered(Some(brand())),
            vec![LegGroup::pair(0, 1), LegGroup::pair(0, 2)],
        ),
        (
            "a join condition left for Exasol counts as a dimension read",
            cross_dimension,
            vec![LegGroup::pair(0, 2), LegGroup::pair(1, 2)],
        ),
        (
            "PART reads unbounded text, and LINEITEM, SUPPLIER's dimension, is over budget",
            Graph {
                bytes: vec![500, 50, 1000],
                ..reading_brand()
            },
            vec![],
        ),
    ];
    for (case, graph, ranked) in cases {
        assert_eq!(graph.grouping(), ranked, "{case}");
    }
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

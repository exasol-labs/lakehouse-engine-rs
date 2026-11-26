use super::*;
use crate::test_support::*;

fn column_names(expr: &Expr) -> Vec<&str> {
    expr.columns()
        .into_iter()
        .map(|column| column.name.as_str())
        .collect()
}

#[test]
fn columns_lists_every_column_in_the_order_sql_writes_them() {
    // A CASE lists its basis, each WHEN before its THEN, and the ELSE last.
    let expr = ast(&case_of(
        col("BASIS"),
        [in_list(col("SUBJECT"), [col("VALUE")]), col("WHEN2")],
        [
            scalar_fn("ADD", [table_col("T", "A"), num_lit(1)]),
            col("THEN2"),
            col("ELSE"),
        ],
    ));

    assert_eq!(
        column_names(&expr),
        ["BASIS", "SUBJECT", "VALUE", "A", "WHEN2", "THEN2", "ELSE"]
    );
}

#[test]
fn columns_reaches_a_json_value_default() {
    let expr = ast(&json_value_as(
        col("DOC"),
        "$.a",
        ExasolType::WIDEST_VARCHAR,
        json_default(col("FALLBACK")),
        json_behavior("NULL"),
    ));

    assert_eq!(column_names(&expr), ["DOC", "FALLBACK"]);
}

#[test]
fn strip_table_alias_removes_every_alias_and_keeps_table_names() {
    let expr = ast(&scalar_fn(
        "ADD",
        [aliased_col("FACT_ORDERS", "O", "O_ORDERDATE"), num_lit(1)],
    ));

    assert_eq!(
        expr.strip_table_alias(),
        ast(&scalar_fn(
            "ADD",
            [table_col("FACT_ORDERS", "O_ORDERDATE"), num_lit(1)]
        ))
    );
}

#[test]
fn substitute_does_not_descend_into_a_replaced_subtree() {
    let expr = ast(&scalar_fn(
        "ADD",
        [agg_item("SUM", Some("A"), false), col("B")],
    ));
    let mut seen = Vec::new();

    let replaced = expr.substitute(&mut |node| {
        if let Expr::Column(column) = node {
            seen.push(column.name.clone());
        }
        node.is_aggregate()
            .then(|| Expr::Column(Column::new("SENTINEL")))
    });

    assert_eq!(seen, ["B"], "the aggregate's own column is never visited");
    assert_eq!(
        replaced,
        ast(&scalar_fn("ADD", [col("SENTINEL"), col("B")]))
    );
}

#[test]
fn conjuncts_flatten_nested_ands_and_keep_ors_whole() {
    let a = predicate_equal(col("A"), num_lit(1));
    let b = predicate_equal(col("B"), num_lit(2));
    let c = or([a.clone(), b.clone()]);
    let expr = ast(&and([a.clone(), and([b.clone(), c.clone()])]));

    let conjuncts: Vec<Expr> = expr.conjuncts().into_iter().cloned().collect();

    assert_eq!(conjuncts, [a, b, c].iter().map(ast).collect::<Vec<_>>());
}

#[test]
fn conjoin_returns_none_one_conjunct_or_their_and() {
    let a = ast(&predicate_equal(col("A"), num_lit(1)));
    let b = ast(&predicate_equal(col("B"), num_lit(2)));

    assert_eq!(Expr::conjoin(Vec::new()), None);
    assert_eq!(Expr::conjoin(vec![a.clone()]), Some(a.clone()));
    assert_eq!(
        Expr::conjoin(vec![a.clone(), b.clone()]),
        Some(Expr::And(vec![a, b]))
    );
}

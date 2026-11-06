use super::*;
use crate::test_support::*;
use serde_json::json;

#[test]
fn a_column_node_without_a_name_does_not_parse() {
    assert_eq!(ColumnNode::parse(&json!({"type": "column"})), None);
}

#[test]
fn walk_column_nodes_visits_every_nested_column_node_once() {
    let expr = serde_json::json!({
        "type": "function_scalar",
        "name": "PLUS",
        "arguments": [
            table_col("T", "A"),
            num_lit(1)
        ],
        "case": {
            "type": "case",
            "results": [
                col("B"),
                num_lit(2)
            ]
        },
        "predicate": predicate_equal(
            col("C"),
            json!({"type": "column", "name": "D", "nested": col("E")}),
        )
    });

    let mut visited = Vec::new();
    walk_column_nodes(&expr, &mut |column| visited.push(column.name));
    visited.sort();

    assert_eq!(
        visited,
        vec!["A", "B", "C", "D", "E"],
        "every column node must fire exactly once, including one nested inside another column node"
    );
}

#[test]
fn walk_column_nodes_never_invokes_callback_for_a_non_container_root() {
    let mut invocations: usize = 0;

    walk_column_nodes(&serde_json::Value::Null, &mut |_| invocations += 1);
    walk_column_nodes(&serde_json::json!("REGION"), &mut |_| invocations += 1);
    walk_column_nodes(&serde_json::json!(7), &mut |_| invocations += 1);
    walk_column_nodes(&serde_json::json!({}), &mut |_| invocations += 1);

    assert_eq!(
        invocations, 0,
        "a null, scalar, or empty-object root must be a no-op: groupBy/orderBy/selectList reach walk_column_nodes unguarded"
    );
}

#[test]
fn strip_table_alias_removes_alias_preserves_table_name_and_name_recursively() {
    let expr = serde_json::json!({
        "type": "function_scalar",
        "name": "PLUS",
        "tableAlias": "O",
        "arguments": [
            aliased_col("FACT_ORDERS", "O", "O_ORDERDATE"),
            num_lit(1)
        ]
    });

    let stripped = strip_table_alias(&expr);

    assert_eq!(
        stripped,
        scalar_fn(
            "PLUS",
            [table_col("FACT_ORDERS", "O_ORDERDATE"), num_lit(1)]
        ),
        "every tableAlias key must be gone at every depth, while name/tableName survive"
    );
}

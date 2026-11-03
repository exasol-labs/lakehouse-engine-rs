use super::*;
use serde_json::json;

#[test]
fn parses_only_a_named_column_node() {
    let node = json!({"type": "column", "name": "a", "tableName": "T", "tableAlias": "X"});
    assert_eq!(
        ColumnNode::parse(&node),
        Some(ColumnNode {
            name: "a",
            table_name: Some("T"),
            table_alias: Some("X"),
        })
    );
    assert_eq!(ColumnNode::parse(&json!({"type": "column"})), None);
    assert_eq!(
        ColumnNode::parse(&json!({"type": "table", "name": "T"})),
        None
    );
}

#[test]
fn rewriting_aliases_touches_only_column_nodes() {
    let tree = json!({
        "type": "predicate_equal",
        "tableAlias": "KEEP",
        "left": {"type": "column", "name": "A", "tableAlias": "X"},
        "right": {"type": "column", "name": "B"},
    });
    let rewritten = rewrite_table_aliases(&tree, &mut |column| {
        Ok::<_, ()>((column.name == "B").then(|| "R".to_string()))
    })
    .unwrap();
    assert_eq!(
        rewritten,
        json!({
            "type": "predicate_equal",
            "tableAlias": "KEEP",
            "left": {"type": "column", "name": "A"},
            "right": {"type": "column", "name": "B", "tableAlias": "R"},
        })
    );
}

#[test]
fn walk_column_nodes_visits_every_nested_column_node_once() {
    let expr = serde_json::json!({
        "type": "function_scalar",
        "name": "PLUS",
        "arguments": [
            {"type": "column", "name": "A", "tableName": "T"},
            {"type": "literal_exactnumeric", "value": 1}
        ],
        "case": {
            "type": "case",
            "results": [
                {"type": "column", "name": "B"},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        },
        "predicate": {
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C"},
            "right": {
                "type": "column",
                "name": "D",
                "nested": {"type": "column", "name": "E"}
            }
        }
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
            {"type": "column", "name": "O_ORDERDATE", "tableName": "FACT_ORDERS", "tableAlias": "O"},
            {"type": "literal_exactnumeric", "value": 1}
        ]
    });

    let stripped = strip_table_alias(&expr);

    assert_eq!(
        stripped,
        serde_json::json!({
            "type": "function_scalar",
            "name": "PLUS",
            "arguments": [
                {"type": "column", "name": "O_ORDERDATE", "tableName": "FACT_ORDERS"},
                {"type": "literal_exactnumeric", "value": 1}
            ]
        }),
        "every tableAlias key must be gone at every depth, while name/tableName survive"
    );
}

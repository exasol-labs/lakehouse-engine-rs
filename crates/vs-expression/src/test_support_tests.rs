//! Builders for Exasol pushdown-request JSON nodes: columns, literals, and predicates. Dependent
//! crates reach them through the `test-support` feature.

use crate::ColumnNode;
use serde_json::{Map, Value as Json, json};

pub fn col(name: &str) -> Json {
    ColumnNode::to_json(name, None, None)
}

pub fn table_column(table: &str, alias: Option<&str>, name: &str) -> Json {
    ColumnNode::to_json(name, Some(table), alias)
}

pub fn table_col(table: &str, name: &str) -> Json {
    table_column(table, None, name)
}

pub fn aliased_col(table: &str, alias: &str, name: &str) -> Json {
    table_column(table, Some(alias), name)
}

/// A column node qualified by its relation alias only, as a rewritten join leg writes it.
pub fn alias_col(alias: &str, name: &str) -> Json {
    ColumnNode::to_json(name, None, Some(alias))
}

pub fn lit(kind: &str, value: impl Into<Json>) -> Json {
    node(kind, [("value", value.into())])
}

pub fn num_lit(value: impl Into<Json>) -> Json {
    lit("literal_exactnumeric", value)
}

pub fn str_lit(value: &str) -> Json {
    lit("literal_string", value)
}

pub fn bool_lit(value: bool) -> Json {
    lit("literal_bool", value)
}

pub fn null_lit() -> Json {
    json!({"type": "literal_null"})
}

pub fn date_lit(value: &str) -> Json {
    lit("literal_date", value)
}

pub fn timestamp_lit(value: &str) -> Json {
    lit("literal_timestamp", value)
}

/// A hex literal retyped as bytes, as the binary-filter rewrite writes it.
pub fn binary_lit(hex: &str) -> Json {
    lit("literal_binary", hex)
}

/// [`binary_lit`] for a fixed-length binary column `width` bytes wide.
pub fn fixed_binary_lit(hex: &str, width: usize) -> Json {
    node(
        "literal_binary",
        [("value", hex.into()), ("width", width.into())],
    )
}

pub fn compare(kind: &str, left: Json, right: Json) -> Json {
    node(kind, [("left", left), ("right", right)])
}

pub fn predicate_equal(left: Json, right: Json) -> Json {
    compare("predicate_equal", left, right)
}

/// `table.name = 'value'` as Exasol sends a single-table filter: its column node names the table.
pub fn string_equal(table: &str, name: &str, value: &str) -> Json {
    predicate_equal(table_col(table, name), str_lit(value))
}

pub fn not(expression: Json) -> Json {
    node("predicate_not", [("expression", expression)])
}

pub fn and(expressions: impl IntoIterator<Item = Json>) -> Json {
    node("predicate_and", [("expressions", array(expressions))])
}

pub fn or(expressions: impl IntoIterator<Item = Json>) -> Json {
    node("predicate_or", [("expressions", array(expressions))])
}

pub fn is_null(expression: Json) -> Json {
    node("predicate_is_null", [("expression", expression)])
}

pub fn is_not_null(expression: Json) -> Json {
    node("predicate_is_not_null", [("expression", expression)])
}

pub fn in_list(expression: Json, arguments: impl IntoIterator<Item = Json>) -> Json {
    node(
        "predicate_in_constlist",
        [("expression", expression), ("arguments", array(arguments))],
    )
}

pub fn between(expression: Json, low: Json, high: Json) -> Json {
    node(
        "predicate_between",
        [("expression", expression), ("left", low), ("right", high)],
    )
}

pub fn like(expression: Json, pattern: &str) -> Json {
    node(
        "predicate_like",
        [("expression", expression), ("pattern", str_lit(pattern))],
    )
}

/// Moves `fields` into a node of type `kind`. `json!` would clone every child it is handed,
/// which nested builders repeat at each level.
fn node<const N: usize>(kind: &str, fields: [(&str, Json); N]) -> Json {
    let mut map = Map::from_iter([("type".to_string(), Json::from(kind))]);
    map.extend(fields.map(|(key, value)| (key.to_string(), value)));
    Json::Object(map)
}

fn array(nodes: impl IntoIterator<Item = Json>) -> Json {
    Json::Array(nodes.into_iter().collect())
}

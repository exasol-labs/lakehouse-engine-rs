//! Builders for Exasol pushdown-request JSON nodes: columns, literals, and predicates. Dependent
//! crates reach them through the `test-support` feature.

use crate::ColumnNode;
use serde_json::{Value as Json, json};

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
    json!({"type": kind, "value": value.into()})
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

pub fn compare(kind: &str, left: Json, right: Json) -> Json {
    json!({"type": kind, "left": left, "right": right})
}

pub fn predicate_equal(left: Json, right: Json) -> Json {
    compare("predicate_equal", left, right)
}

/// `name = 'value'`, the common shape of a partition-equality filter.
pub fn string_equal(name: &str, value: &str) -> Json {
    predicate_equal(col(name), str_lit(value))
}

pub fn not(expression: Json) -> Json {
    json!({"type": "predicate_not", "expression": expression})
}

pub fn and(expressions: impl IntoIterator<Item = Json>) -> Json {
    let expressions: Vec<Json> = expressions.into_iter().collect();
    json!({"type": "predicate_and", "expressions": expressions})
}

pub fn or(expressions: impl IntoIterator<Item = Json>) -> Json {
    let expressions: Vec<Json> = expressions.into_iter().collect();
    json!({"type": "predicate_or", "expressions": expressions})
}

pub fn is_null(expression: Json) -> Json {
    json!({"type": "predicate_is_null", "expression": expression})
}

pub fn is_not_null(expression: Json) -> Json {
    json!({"type": "predicate_is_not_null", "expression": expression})
}

pub fn in_list(expression: Json, arguments: impl IntoIterator<Item = Json>) -> Json {
    let arguments: Vec<Json> = arguments.into_iter().collect();
    json!({"type": "predicate_in_constlist", "expression": expression, "arguments": arguments})
}

pub fn between(expression: Json, low: Json, high: Json) -> Json {
    json!({"type": "predicate_between", "expression": expression, "left": low, "right": high})
}

pub fn like(expression: Json, pattern: &str) -> Json {
    json!({"type": "predicate_like", "expression": expression, "pattern": str_lit(pattern)})
}

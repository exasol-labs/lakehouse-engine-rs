//! Builders for Exasol pushdown-request JSON nodes: columns, literals, predicates, scalar and
//! aggregate functions, and CAST target types.

use crate::ColumnNode;
use serde_json::{Value as Json, json};

pub(crate) fn col(name: &str) -> Json {
    ColumnNode::to_json(name, None, None)
}

pub(crate) fn table_col(table: &str, name: &str) -> Json {
    ColumnNode::to_json(name, Some(table), None)
}

pub(crate) fn aliased_col(table: &str, alias: &str, name: &str) -> Json {
    ColumnNode::to_json(name, Some(table), Some(alias))
}

/// A column that carries a `tableAlias` but no `tableName`.
pub(crate) fn alias_col(alias: &str, name: &str) -> Json {
    ColumnNode::to_json(name, None, Some(alias))
}

pub(crate) fn lit(kind: &str, value: impl Into<Json>) -> Json {
    json!({"type": kind, "value": value.into()})
}

pub(crate) fn num_lit(value: impl Into<Json>) -> Json {
    lit("literal_exactnumeric", value)
}

pub(crate) fn str_lit(value: &str) -> Json {
    lit("literal_string", value)
}

pub(crate) fn bool_lit(value: bool) -> Json {
    lit("literal_bool", value)
}

pub(crate) fn null_lit() -> Json {
    json!({"type": "literal_null"})
}

pub(crate) fn double_lit(value: impl Into<Json>) -> Json {
    lit("literal_double", value)
}

pub(crate) fn date_lit(value: impl Into<Json>) -> Json {
    lit("literal_date", value)
}

pub(crate) fn timestamp_lit(value: impl Into<Json>) -> Json {
    lit("literal_timestamp", value)
}

pub(crate) fn timestamp_utc_lit(value: impl Into<Json>) -> Json {
    lit("literal_timestamp_utc", value)
}

pub(crate) fn binary_lit(value: &str) -> Json {
    lit("literal_binary", value)
}

pub(crate) fn compare(kind: &str, left: Json, right: Json) -> Json {
    json!({"type": kind, "left": left, "right": right})
}

pub(crate) fn predicate_equal(left: Json, right: Json) -> Json {
    compare("predicate_equal", left, right)
}

pub(crate) fn predicate_greater(left: Json, right: Json) -> Json {
    compare("predicate_greater", left, right)
}

pub(crate) fn predicate_less(left: Json, right: Json) -> Json {
    compare("predicate_less", left, right)
}

pub(crate) fn predicate_and(expressions: impl IntoIterator<Item = Json>) -> Json {
    json!({"type": "predicate_and", "expressions": collect(expressions)})
}

pub(crate) fn predicate_or(expressions: impl IntoIterator<Item = Json>) -> Json {
    json!({"type": "predicate_or", "expressions": collect(expressions)})
}

pub(crate) fn predicate_not(expression: Json) -> Json {
    json!({"type": "predicate_not", "expression": expression})
}

pub(crate) fn predicate_is_null(expression: Json) -> Json {
    json!({"type": "predicate_is_null", "expression": expression})
}

pub(crate) fn predicate_in(expression: Json, arguments: impl IntoIterator<Item = Json>) -> Json {
    json!({
        "type": "predicate_in_constlist",
        "expression": expression,
        "arguments": collect(arguments)
    })
}

pub(crate) fn predicate_like(expression: Json, pattern: Json) -> Json {
    json!({"type": "predicate_like", "expression": expression, "pattern": pattern})
}

pub(crate) fn scalar_fn(name: &str, arguments: impl IntoIterator<Item = Json>) -> Json {
    json!({"type": "function_scalar", "name": name, "arguments": collect(arguments)})
}

pub(crate) fn cast(argument: Json, data_type: Json) -> Json {
    json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [argument],
        "dataType": data_type
    })
}

pub(crate) fn extract(field: &str, argument: Json) -> Json {
    json!({
        "type": "function_scalar_extract",
        "name": "EXTRACT",
        "toExtract": field,
        "arguments": [argument]
    })
}

/// A `CASE` without a basis: `arguments` are the WHEN conditions, a surplus result is the ELSE.
pub(crate) fn searched_case(
    arguments: impl IntoIterator<Item = Json>,
    results: impl IntoIterator<Item = Json>,
) -> Json {
    json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": collect(arguments),
        "results": collect(results)
    })
}

/// A `CASE <basis>`: `arguments` are the WHEN values, a surplus result is the ELSE.
pub(crate) fn simple_case(
    basis: Json,
    arguments: impl IntoIterator<Item = Json>,
    results: impl IntoIterator<Item = Json>,
) -> Json {
    let mut node = searched_case(arguments, results);
    node["basis"] = basis;
    node
}

pub(crate) fn aggregate(name: &str, arguments: impl IntoIterator<Item = Json>) -> Json {
    json!({
        "type": "function_aggregate",
        "name": name,
        "arguments": collect(arguments),
        "distinct": false
    })
}

pub(crate) fn distinct_aggregate(name: &str, arguments: impl IntoIterator<Item = Json>) -> Json {
    let mut node = aggregate(name, arguments);
    node["distinct"] = Json::Bool(true);
    node
}

pub(crate) fn decimal_to_varchar(arguments: impl IntoIterator<Item = Json>) -> Json {
    json!({"type": "decimal_to_varchar_exasol", "arguments": collect(arguments)})
}

/// A CAST target with no attribute, such as `DOUBLE` or a sizeless `VARCHAR`.
pub(crate) fn data_type(kind: &str) -> Json {
    json!({"type": kind})
}

pub(crate) fn varchar_type(size: u64) -> Json {
    json!({"type": "VARCHAR", "size": size})
}

pub(crate) fn char_type(size: u64) -> Json {
    json!({"type": "CHAR", "size": size, "characterSet": "ASCII"})
}

pub(crate) fn decimal_type(precision: u64, scale: u64) -> Json {
    json!({"type": "DECIMAL", "precision": precision, "scale": scale})
}

pub(crate) fn timestamp_type(precision: u64) -> Json {
    json!({"type": "TIMESTAMP", "fractionalSecondsPrecision": precision})
}

fn collect(items: impl IntoIterator<Item = Json>) -> Vec<Json> {
    items.into_iter().collect()
}

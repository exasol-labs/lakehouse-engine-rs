//! Builders for Exasol pushdown-request JSON nodes: columns, literals, predicates, scalar
//! functions, CAST, EXTRACT, CASE, and aggregates. Dependent crates enable `test-support`.

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

/// A column node as Exasol sends it, with the column's 0-based position in its virtual table.
pub fn numbered_col(table: &str, column_nr: u32, name: &str) -> Json {
    let mut column = table_col(table, name);
    column["columnNr"] = column_nr.into();
    column
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

pub fn scalar_fn(name: &str, arguments: impl IntoIterator<Item = Json>) -> Json {
    node(
        "function_scalar",
        [("name", name.into()), ("arguments", array(arguments))],
    )
}

/// `CAST(argument AS data_type)` with a raw `dataType` node, which may be one Exasol never sends.
pub fn cast_json(data_type: Json, argument: Json) -> Json {
    node(
        "function_scalar_cast",
        [
            ("name", "CAST".into()),
            ("arguments", array([argument])),
            ("dataType", data_type),
        ],
    )
}

/// `EXTRACT(field FROM argument)`.
pub fn extract(field: &str, argument: Json) -> Json {
    node(
        "function_scalar_extract",
        [
            ("name", "EXTRACT".into()),
            ("toExtract", field.into()),
            ("arguments", array([argument])),
        ],
    )
}

/// Searched `CASE WHEN conditions[i] THEN results[i] ... END`; one extra result is the ELSE.
pub fn case_when(
    conditions: impl IntoIterator<Item = Json>,
    results: impl IntoIterator<Item = Json>,
) -> Json {
    node(
        "function_scalar_case",
        [
            ("name", "CASE".into()),
            ("arguments", array(conditions)),
            ("results", array(results)),
        ],
    )
}

/// Simple `CASE basis WHEN values[i] THEN results[i] ... END`; one extra result is the ELSE.
pub fn case_of(
    basis: Json,
    values: impl IntoIterator<Item = Json>,
    results: impl IntoIterator<Item = Json>,
) -> Json {
    node(
        "function_scalar_case",
        [
            ("name", "CASE".into()),
            ("basis", basis),
            ("arguments", array(values)),
            ("results", array(results)),
        ],
    )
}

/// An aggregate over `column`, or over no argument (`COUNT(*)`) when `column` is `None`.
pub fn agg_item(name: &str, column: Option<&str>, distinct: bool) -> Json {
    aggregate(name, column.map(col), distinct)
}

/// An aggregate over one argument expression.
pub fn agg_item_expr(name: &str, argument: Json, distinct: bool) -> Json {
    aggregate(name, Some(argument), distinct)
}

/// `COUNT(*)`.
pub fn count_star() -> Json {
    agg_item("COUNT", None, false)
}

/// Exasol writes `arguments` only when there is one (never for `COUNT(*)`) and `distinct` only
/// for a DISTINCT aggregate.
fn aggregate(name: &str, argument: Option<Json>, distinct: bool) -> Json {
    let mut aggregate = node("function_aggregate", [("name", name.into())]);
    if let Some(argument) = argument {
        aggregate["arguments"] = array([argument]);
    }
    if distinct {
        aggregate["distinct"] = true.into();
    }
    aggregate
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

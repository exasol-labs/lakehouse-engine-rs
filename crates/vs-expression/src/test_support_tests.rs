use crate::ColumnNode;
use serde_json::{Value as Json, json};

pub(crate) fn col(name: &str) -> Json {
    ColumnNode::named(name).to_json()
}

pub(crate) fn table_col(table: &str, name: &str) -> Json {
    ColumnNode {
        table_name: Some(table),
        ..ColumnNode::named(name)
    }
    .to_json()
}

pub(crate) fn aliased_col(table: &str, alias: &str, name: &str) -> Json {
    ColumnNode {
        name,
        table_name: Some(table),
        table_alias: Some(alias),
    }
    .to_json()
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

pub(crate) fn predicate_equal(left: Json, right: Json) -> Json {
    json!({"type": "predicate_equal", "left": left, "right": right})
}

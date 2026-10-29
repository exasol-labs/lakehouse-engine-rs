use arrow::datatypes::{DataType, Field};

use super::*;

#[test]
fn render_column_keeps_a_non_nested_incompatible_column_cast_unchanged() {
    assert_eq!(
        render_column("PAYLOAD", Some(&DataType::Binary)),
        "CAST(\"PAYLOAD\" AS VARCHAR)"
    );
}

#[test]
fn render_column_diverts_a_nested_column_to_the_json_render_function() {
    let list_type = DataType::List(std::sync::Arc::new(Field::new(
        "item",
        DataType::Utf8,
        true,
    )));

    assert_eq!(
        render_column("TAGS", Some(&list_type)),
        format!("{NESTED_JSON_RENDER_UDF_NAME}(\"TAGS\")"),
        "a nested column must be routed through the JSON render function, not CAST"
    );
}

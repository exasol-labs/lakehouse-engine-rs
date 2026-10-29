use arrow::datatypes::{DataType, Field};

use super::*;

#[test]
fn render_column_keeps_a_non_nested_incompatible_column_cast_unchanged() {
    assert_eq!(
        render_column(
            "PAYLOAD",
            Some(&DataType::Time64(arrow::datatypes::TimeUnit::Microsecond))
        ),
        "CAST(\"PAYLOAD\" AS VARCHAR)"
    );
}

#[test]
fn a_binary_column_is_aliased_to_its_hex_text_and_exposes_its_raw_bytes() {
    let schema = DFSchema::try_from(arrow::datatypes::Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("Digest", DataType::FixedSizeBinary(16), true),
    ]))
    .unwrap();

    assert_eq!(
        build_alias_items(&schema),
        vec![
            "\"id\" AS \"ID\"".to_string(),
            format!(
                "{BINARY_HEX_UDF_NAME}(\"Digest\", 'DIGEST') AS \"DIGEST\", \"Digest\" AS \
                 \"_LH_BYTES_DIGEST\""
            ),
        ]
    );
    assert_eq!(
        render_column("DIGEST", Some(&DataType::FixedSizeBinary(16))),
        "\"DIGEST\"",
        "the alias already holds the hex text, so the projection takes it as is"
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

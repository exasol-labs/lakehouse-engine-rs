use delta_kernel::schema::{MetadataValue, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;

use super::*;

/// Verbatim `schemaString` from the `cdf-column-mapping-name-mode` fixture's first commit
/// (`scripts/unity/fixtures/cdf-column-mapping-name-mode/_delta_log/00000000000000000000.json`):
/// `name`-mode column mapping with `col-<uuid>` physical names.
const CDF_COLUMN_MAPPING_NAME_MODE_SCHEMA: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-80396d42-d765-483e-b86e-7ac1e13ef88c"}},{"name":"name","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-ed3e45cf-632b-4a07-bb22-d9f4693bbaa1"}},{"name":"value","type":"double","nullable":true,"metadata":{"delta.columnMapping.id":3,"delta.columnMapping.physicalName":"col-95e13b58-72f1-4d26-8390-49469180a8a2"}}]}"#;

/// The first two fields (in declared order) of the `stats-all-types` fixture's schema
/// (`scripts/unity/fixtures/stats-all-types/_delta_log/00000000000000000000.json`): `byte_col`
/// has no Arrow type tag in this engine's vocabulary, followed by a mapped `int_col`.
const STATS_ALL_TYPES_SCHEMA: &str = r#"{"type":"struct","fields":[{"name":"byte_col","type":"byte","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-042fadad-0f76-466d-a94d-e1159c2a9ed6"}},{"name":"int_col","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":3,"delta.columnMapping.physicalName":"col-e0571b78-2d49-4109-9add-f2c54d6ea29e"}}]}"#;

fn parse_schema(json: &str) -> StructType {
    serde_json::from_str(json).expect("fixture schemaString must parse as a Delta StructType")
}

fn user_message(err: UdfError) -> String {
    match err {
        UdfError::User(message) => message,
        other => panic!("expected UdfError::User, got {other:?}"),
    }
}

/// A field annotated the way the Delta protocol requires under `id`/`name` mode:
/// both a column-mapping id and a physical name.
fn mapped_field(name: &str, id: i64, physical_name: &str) -> StructField {
    StructField::not_null(name, DataType::INTEGER).with_metadata([
        ("delta.columnMapping.id", MetadataValue::Number(id)),
        (
            "delta.columnMapping.physicalName",
            MetadataValue::String(physical_name.to_string()),
        ),
    ])
}

// Scenario Coverage (add-delta-table-planning): `build_delta_table_schema` is `pub(super)`, so
// this scenario is reached as a crate-internal unit test rather than `tests/delta_log_replay.rs`.
#[test]
fn replay_carries_name_mode_column_mapping_and_physical_names() {
    let schema = parse_schema(CDF_COLUMN_MAPPING_NAME_MODE_SCHEMA);

    let (logical_fields, table_spec) =
        build_delta_table_schema(&schema, ColumnMappingMode::Name, Vec::new())
            .expect("boolean/long/string/double/... all map to an Arrow tag");

    assert_eq!(table_spec.column_mapping_mode, DeltaColumnMappingMode::Name);
    assert_eq!(
        table_spec.columns,
        vec![
            DeltaColumnMapping {
                logical_name: "id".to_string(),
                physical_name: "col-80396d42-d765-483e-b86e-7ac1e13ef88c".to_string(),
                physical_id: 1,
            },
            DeltaColumnMapping {
                logical_name: "name".to_string(),
                physical_name: "col-ed3e45cf-632b-4a07-bb22-d9f4693bbaa1".to_string(),
                physical_id: 2,
            },
            DeltaColumnMapping {
                logical_name: "value".to_string(),
                physical_name: "col-95e13b58-72f1-4d26-8390-49469180a8a2".to_string(),
                physical_id: 3,
            },
        ]
    );
    assert!(table_spec.partition_columns.is_empty());

    assert_eq!(logical_fields.len(), 3);
    assert_eq!(logical_fields[0].field_id, 1);
    assert_eq!(logical_fields[0].name, "id");
    assert_eq!(logical_fields[0].arrow_type, "int64");
    assert!(logical_fields[0].nullable);
    assert_eq!(logical_fields[1].arrow_type, "utf8");
    assert_eq!(logical_fields[2].arrow_type, "float64");
}

#[test]
fn absent_column_mapping_mode_defaults_to_none_with_physical_name_equal_to_logical_name() {
    let schema = StructType::try_new([StructField::not_null("plain_col", DataType::INTEGER)])
        .expect("single-field schema is valid");

    let (logical_fields, table_spec) = build_delta_table_schema(
        &schema,
        ColumnMappingMode::None,
        vec!["plain_col".to_string()],
    )
    .expect("integer maps to an Arrow tag");

    assert_eq!(table_spec.column_mapping_mode, DeltaColumnMappingMode::None);
    assert_eq!(table_spec.columns.len(), 1);
    assert_eq!(table_spec.columns[0].physical_name, "plain_col");
    assert_eq!(table_spec.columns[0].physical_id, 1);
    assert_eq!(logical_fields[0].field_id, 1);
    assert_eq!(logical_fields[0].arrow_type, "int32");
    assert_eq!(table_spec.partition_columns, vec!["plain_col".to_string()]);
}

/// Under `id` mode the ANNOTATED id reaches both the logical schema and the column
/// mapping — neither column's id is its ordinal position, so an assigned id is never
/// silently overwritten by the position it happens to sit at.
#[test]
fn explicit_column_mapping_id_wins_over_ordinal_position_when_present() {
    let schema = StructType::try_new([
        mapped_field("a", 7, "col-a"),
        mapped_field("b", 99, "col-b"),
    ])
    .unwrap();

    let (logical_fields, table_spec) =
        build_delta_table_schema(&schema, ColumnMappingMode::Id, Vec::new()).unwrap();

    assert_eq!(logical_fields[0].field_id, 7);
    assert_eq!(logical_fields[1].field_id, 99);
    assert_eq!(table_spec.columns[0].physical_id, 7);
    assert_eq!(table_spec.columns[1].physical_id, 99);
    assert_eq!(table_spec.columns[0].physical_name, "col-a");
    assert_eq!(table_spec.columns[1].physical_name, "col-b");
}

/// Under `none` mode a residual column-mapping annotation is INERT — the Delta
/// protocol resolves every physical name to its logical one there, and `delta_kernel`
/// documents the same read tolerance — so the id stays the field's 1-based ordinal.
/// Honouring the annotation would put ordinals and assigned ids in one namespace,
/// where an annotated id can collide with an unannotated sibling's ordinal.
#[test]
fn none_mode_uses_the_ordinal_position_even_when_a_column_mapping_id_is_annotated() {
    let schema = StructType::try_new([
        StructField::not_null("a", DataType::INTEGER),
        mapped_field("b", 1, "col-b"),
    ])
    .unwrap();

    let (logical_fields, table_spec) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new()).unwrap();

    assert_eq!(logical_fields[0].field_id, 1);
    assert_eq!(
        logical_fields[1].field_id, 2,
        "the annotated id 1 would collide with the first column's ordinal id"
    );
    assert_eq!(table_spec.columns[1].physical_id, 2);
    assert_eq!(table_spec.columns[1].physical_name, "b");
}

/// Under `id`/`name` mode the Delta protocol REQUIRES every field to carry a
/// `delta.columnMapping.id`, and nothing on the read path validates that. Substituting
/// the ordinal position would hand the scan an id that can collide with a sibling
/// column's assigned one, so an absent (or non-numeric) annotation is refused.
#[test]
fn id_mode_column_without_a_column_mapping_id_is_refused_naming_the_column() {
    let unannotated_id = StructField::not_null("b", DataType::INTEGER).with_metadata([(
        "delta.columnMapping.physicalName",
        MetadataValue::String("col-b".to_string()),
    )]);
    let schema = StructType::try_new([mapped_field("a", 1, "col-a"), unannotated_id]).unwrap();

    for (mode, rendered_mode) in [
        (ColumnMappingMode::Id, "Id"),
        (ColumnMappingMode::Name, "Name"),
    ] {
        let err = build_delta_table_schema(&schema, mode, Vec::new())
            .expect_err("a column carrying no column-mapping id must be refused");
        let message = user_message(err);

        assert!(message.contains("'b'"), "message was: {message}");
        assert!(
            message.contains("delta.columnMapping.id"),
            "message was: {message}"
        );
        assert!(message.contains(rendered_mode), "message was: {message}");
    }
}

/// A `delta.columnMapping.id` outside `i32` is refused rather than truncated or
/// replaced by an ordinal: the wire field-id is an `i32`, and the Delta protocol
/// restricts the annotation to a 32-bit non-negative integer, so an out-of-range value
/// describes no column this engine can bind.
#[test]
fn id_mode_column_with_an_out_of_range_column_mapping_id_is_refused_naming_the_column() {
    let oversized = i64::from(i32::MAX) + 1;
    let schema = StructType::try_new([mapped_field("a", oversized, "col-a")]).unwrap();

    let err = build_delta_table_schema(&schema, ColumnMappingMode::Id, Vec::new())
        .expect_err("an id outside i32 must be refused");
    let message = user_message(err);

    assert!(message.contains("'a'"), "message was: {message}");
    assert!(
        message.contains(&oversized.to_string()),
        "message was: {message}"
    );
    assert!(
        message.contains("delta.columnMapping.id"),
        "message was: {message}"
    );
}

/// Under `id`/`name` mode the physical name is the ONLY name the writer used in the
/// Parquet file, so an absent — or present but non-string — annotation is refused.
/// Falling back to the logical name would have the scan read a column the writer
/// never wrote.
#[test]
fn id_mode_column_without_a_physical_name_is_refused_naming_the_column() {
    let absent = vec![("delta.columnMapping.id", MetadataValue::Number(1))];
    let not_a_string = vec![
        ("delta.columnMapping.id", MetadataValue::Number(1)),
        ("delta.columnMapping.physicalName", MetadataValue::Number(7)),
    ];

    for metadata in [absent, not_a_string] {
        let schema = StructType::try_new([
            StructField::not_null("a", DataType::INTEGER).with_metadata(metadata)
        ])
        .unwrap();

        let err = build_delta_table_schema(&schema, ColumnMappingMode::Name, Vec::new())
            .expect_err("a column carrying no usable physical name must be refused");
        let message = user_message(err);

        assert!(message.contains("'a'"), "message was: {message}");
        assert!(
            message.contains("delta.columnMapping.physicalName"),
            "message was: {message}"
        );
        assert!(message.contains("Name"), "message was: {message}");
    }
}

#[test]
fn partition_columns_are_threaded_through_verbatim_and_in_order() {
    let schema = StructType::try_new([StructField::not_null("region", DataType::STRING)]).unwrap();
    let partition_columns = vec!["region".to_string(), "day".to_string()];

    let (_, table_spec) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, partition_columns.clone())
            .unwrap();

    assert_eq!(table_spec.partition_columns, partition_columns);
}

// Scenario Coverage (add-delta-table-planning): `build_delta_table_schema` is `pub(super)`, so
// this scenario is reached as a crate-internal unit test rather than `tests/delta_log_replay.rs`.
#[test]
fn unmapped_delta_type_is_refused_naming_the_column_and_issue_322() {
    let schema = parse_schema(STATS_ALL_TYPES_SCHEMA);

    let err = build_delta_table_schema(&schema, ColumnMappingMode::Name, Vec::new())
        .expect_err("byte has no Arrow type tag in this engine's vocabulary");
    let message = user_message(err);

    assert!(message.contains("byte_col"), "message was: {message}");
    assert!(message.contains("byte"), "message was: {message}");
    assert!(message.contains("#322"), "message was: {message}");
}

#[test]
fn struct_array_and_map_columns_are_refused_not_widened_to_json() {
    for (name, type_json) in [
        (
            "array_col",
            r#"{"type":"array","elementType":"integer","containsNull":true}"#,
        ),
        (
            "map_col",
            r#"{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}"#,
        ),
        (
            "nested_struct",
            r#"{"type":"struct","fields":[{"name":"inner","type":"integer","nullable":true,"metadata":{}}]}"#,
        ),
        ("variant_col", r#""variant""#),
        ("binary_col", r#""binary""#),
    ] {
        let schema_json = format!(
            r#"{{"type":"struct","fields":[{{"name":"{name}","type":{type_json},"nullable":true,"metadata":{{}}}}]}}"#
        );
        let schema = parse_schema(&schema_json);

        let err = build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .err()
            .unwrap_or_else(|| panic!("{name} has no Arrow type tag in this engine's vocabulary"));
        let message = user_message(err);

        assert!(message.contains(name), "message was: {message}");
        assert!(message.contains("#322"), "message was: {message}");
    }
}

#[test]
fn decimal_within_exasol_domain_maps_to_decimal128_tag() {
    let schema = StructType::try_new([StructField::not_null(
        "decimal_col",
        DataType::decimal(10, 2).unwrap(),
    )])
    .unwrap();

    let (logical_fields, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("decimal(10,2) is within Exasol's 1..=36 precision domain");

    assert_eq!(logical_fields[0].arrow_type, "decimal128(10,2)");
}

#[test]
fn decimal_outside_exasol_domain_is_refused_citing_issue_322() {
    let schema = StructType::try_new([StructField::not_null(
        "big_decimal_col",
        DataType::decimal(38, 10).unwrap(),
    )])
    .unwrap();

    let err = build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
        .expect_err("precision 38 exceeds Exasol's 36-digit DECIMAL domain");
    let message = user_message(err);

    assert!(
        message.contains("big_decimal_col"),
        "message was: {message}"
    );
    assert!(message.contains("#322"), "message was: {message}");
}

#[test]
fn nullability_is_carried_from_the_delta_schema() {
    let schema = StructType::try_new([
        StructField::nullable("nullable_col", DataType::STRING),
        StructField::not_null("required_col", DataType::STRING),
    ])
    .unwrap();

    let (logical_fields, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new()).unwrap();

    assert!(logical_fields[0].nullable);
    assert!(!logical_fields[1].nullable);
}

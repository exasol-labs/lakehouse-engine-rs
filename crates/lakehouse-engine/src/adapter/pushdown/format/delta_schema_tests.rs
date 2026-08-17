use delta_kernel::schema::{ArrayType, MapType, MetadataValue, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;

use super::*;

/// Verbatim `schemaString` from the `cdf-column-mapping-name-mode` fixture's first commit
/// (`scripts/unity/fixtures/cdf-column-mapping-name-mode/_delta_log/00000000000000000000.json`):
/// `name`-mode column mapping with `col-<uuid>` physical names.
const CDF_COLUMN_MAPPING_NAME_MODE_SCHEMA: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-80396d42-d765-483e-b86e-7ac1e13ef88c"}},{"name":"name","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-ed3e45cf-632b-4a07-bb22-d9f4693bbaa1"}},{"name":"value","type":"double","nullable":true,"metadata":{"delta.columnMapping.id":3,"delta.columnMapping.physicalName":"col-95e13b58-72f1-4d26-8390-49469180a8a2"}}]}"#;

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

/// Every logical field's binding key rendered as `<logical name>=<key>`, so one expected string
/// asserts BOTH which key a column carries and that it carries no second one — a field populating
/// both renders as `BOTH(...)` rather than passing a one-sided assertion.
fn binding_keys(fields: &[LogicalField]) -> String {
    fields
        .iter()
        .map(
            |field| match (field.field_id, field.physical_name.as_deref()) {
                (Some(id), None) => format!("{}=id({id})", field.name),
                (None, Some(physical_name)) => format!("{}=name({physical_name})", field.name),
                (None, None) => format!("{}=identity", field.name),
                (Some(id), Some(physical_name)) => {
                    format!("{}=BOTH(id({id}),name({physical_name}))", field.name)
                }
            },
        )
        .collect::<Vec<_>>()
        .join(", ")
}

fn array_of(element_type: DataType) -> DataType {
    ArrayType::new(element_type, true).into()
}

/// The refusal reason a single-column table of `data_type` produces. A single refused column
/// never fails `build_delta_table_schema` by itself — only the reader's whole-table guard does
/// that, once it sees an empty successful schema — so this reads the reason off the refused list.
fn refusal_message(column_name: &str, data_type: DataType) -> String {
    let schema = StructType::try_new([StructField::nullable(column_name, data_type)]).unwrap();
    let (logical_fields, _, refused_columns) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("a refused column type never fails build_delta_table_schema itself");
    assert!(
        logical_fields.is_empty(),
        "the refused column must carry no LogicalField"
    );
    assert_eq!(
        refused_columns.len(),
        1,
        "expected exactly one refused column"
    );
    refused_columns[0].reason.clone()
}

// Scenario Coverage (refactor-neutralize-scan-spec): `build_delta_table_schema` is `pub(super)`,
// so this scenario is reached as a crate-internal unit test rather than `tests/delta_log_replay.rs`.
#[test]
fn each_column_mapping_mode_selects_its_own_binding_key() {
    let schema = StructType::try_new([
        mapped_field("a", 7, "col-a"),
        mapped_field("b", 99, "col-b"),
    ])
    .unwrap();

    let (id_mode, _, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::Id, Vec::new()).unwrap();
    let (name_mode, _, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::Name, Vec::new()).unwrap();
    let (none_mode, _, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new()).unwrap();

    assert_eq!(
        binding_keys(&id_mode),
        "a=id(7), b=id(99)",
        "an annotated id is never replaced by the column's ordinal position"
    );
    assert_eq!(binding_keys(&name_mode), "a=name(col-a), b=name(col-b)");
    assert_eq!(binding_keys(&none_mode), "a=identity, b=identity");
}

/// The `name`-mode fixture's columns each bind by their `col-<uuid>` physical name, and their
/// Delta types reach the Arrow tags the logical schema declares.
#[test]
fn name_mode_fixture_columns_bind_by_their_declared_physical_name() {
    let schema = parse_schema(CDF_COLUMN_MAPPING_NAME_MODE_SCHEMA);

    let (logical_fields, partition_columns, refused_columns) =
        build_delta_table_schema(&schema, ColumnMappingMode::Name, Vec::new())
            .expect("boolean/long/string/double/... all map to an Arrow tag");
    assert!(refused_columns.is_empty());

    assert_eq!(
        binding_keys(&logical_fields),
        "id=name(col-80396d42-d765-483e-b86e-7ac1e13ef88c), \
         name=name(col-ed3e45cf-632b-4a07-bb22-d9f4693bbaa1), \
         value=name(col-95e13b58-72f1-4d26-8390-49469180a8a2)"
    );
    assert!(partition_columns.is_empty());

    assert_eq!(logical_fields[0].arrow_type, "int64");
    assert!(logical_fields[0].nullable);
    assert_eq!(logical_fields[1].arrow_type, "utf8");
    assert_eq!(logical_fields[2].arrow_type, "float64");
}

/// Under `none` mode a residual column-mapping annotation is INERT — the Delta protocol
/// resolves every physical name to its logical one there, and `delta_kernel` documents the
/// same read tolerance — so the column binds by its own logical name and carries neither key.
/// Honouring the annotation would hand the scan a key its unannotated siblings cannot offer.
/// Also pins the Delta `INTEGER` -> `int32` Arrow tag mapping.
#[test]
fn none_mode_ignores_a_residual_column_mapping_annotation() {
    let schema = StructType::try_new([
        StructField::not_null("a", DataType::INTEGER),
        mapped_field("b", 1, "col-b"),
    ])
    .unwrap();

    let (logical_fields, _, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new()).unwrap();

    assert_eq!(binding_keys(&logical_fields), "a=identity, b=identity");
    assert_eq!(
        logical_fields[0].arrow_type, "int32",
        "a Delta INTEGER column maps to the int32 Arrow tag"
    );
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

// Scenario Coverage (delta-type-mapping): A Delta type whose Arrow form cannot be rendered
// faithfully is refused by name
/// A refused column's binding key is never looked up, even when that lookup would itself fail:
/// under `id` mode `bad_binary` carries no `delta.columnMapping.id` at all, which would refuse
/// the whole call on a MISSING ANNOTATION if binding-key resolution ran before type
/// classification. Because classification runs first, `bad_binary` is refused for its `binary`
/// type, its malformed binding key is never read, and the mappable `id` column is unaffected.
#[test]
fn a_refused_columns_binding_key_is_never_looked_up_even_when_it_would_itself_fail() {
    let mappable = mapped_field("id", 1, "col-id");
    let refused_with_no_binding_annotation = StructField::nullable("bad_binary", DataType::BINARY);
    let schema = StructType::try_new([mappable, refused_with_no_binding_annotation]).unwrap();

    let (logical_fields, _, refused_columns) =
        build_delta_table_schema(&schema, ColumnMappingMode::Id, Vec::new()).expect(
            "a refused column with no column-mapping annotation must not fail the whole call",
        );

    assert_eq!(
        logical_fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id"],
        "only the mappable column carries a LogicalField"
    );
    assert_eq!(logical_fields[0].field_id, Some(1));

    assert_eq!(refused_columns.len(), 1);
    assert_eq!(refused_columns[0].column_name, "bad_binary");
    assert!(refused_columns[0].reason.contains("binary"));
}

#[test]
fn partition_columns_are_threaded_through_verbatim_and_in_order() {
    let schema = StructType::try_new([StructField::not_null("region", DataType::STRING)]).unwrap();
    let partition_columns = vec!["region".to_string(), "day".to_string()];

    let (_, carried_partition_columns, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, partition_columns.clone())
            .unwrap();

    assert_eq!(carried_partition_columns, partition_columns);
}

// Scenario Coverage (delta-type-mapping): Every Delta type Exasol represents natively maps to
// its own Arrow tag
#[test]
fn every_natively_representable_delta_type_maps_to_its_own_arrow_tag() {
    let schema = StructType::try_new([
        StructField::not_null("boolean_col", DataType::BOOLEAN),
        StructField::not_null("byte_col", DataType::BYTE),
        StructField::not_null("short_col", DataType::SHORT),
        StructField::not_null("integer_col", DataType::INTEGER),
        StructField::not_null("long_col", DataType::LONG),
        StructField::not_null("float_col", DataType::FLOAT),
        StructField::not_null("double_col", DataType::DOUBLE),
        StructField::not_null("string_col", DataType::STRING),
        StructField::not_null("date_col", DataType::DATE),
        StructField::not_null("timestamp_col", DataType::TIMESTAMP),
        StructField::not_null("timestamp_ntz_col", DataType::TIMESTAMP_NTZ),
        StructField::not_null("decimal_col", DataType::decimal(10, 2).unwrap()),
    ])
    .unwrap();

    let (logical_fields, _, refused_columns) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("every column here is in the native set");
    assert!(refused_columns.is_empty());

    let tags: Vec<(&str, &str)> = logical_fields
        .iter()
        .map(|field| (field.name.as_str(), field.arrow_type.as_str()))
        .collect();
    assert_eq!(
        tags,
        vec![
            ("boolean_col", "bool"),
            ("byte_col", "int32"),
            ("short_col", "int32"),
            ("integer_col", "int32"),
            ("long_col", "int64"),
            ("float_col", "float32"),
            ("double_col", "float64"),
            ("string_col", "utf8"),
            ("date_col", "date32"),
            ("timestamp_col", "timestamptz_us"),
            ("timestamp_ntz_col", "timestamp_us"),
            ("decimal_col", "decimal128(10,2)"),
        ]
    );
}

// Scenario Coverage (delta-type-mapping): A Delta type whose Arrow form cannot be rendered
// faithfully is refused by name
#[test]
fn binary_struct_map_and_variant_are_refused_with_their_own_reason_citing_350() {
    for (name, type_json, delta_type_name, cites_350) in [
        ("binary_col", r#""binary""#, "binary", true),
        (
            "nested_struct",
            r#"{"type":"struct","fields":[{"name":"inner","type":"integer","nullable":true,"metadata":{}}]}"#,
            "struct",
            true,
        ),
        (
            "map_col",
            r#"{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}"#,
            "map",
            true,
        ),
        ("variant_col", r#""variant""#, "variant", false),
    ] {
        let schema_json = format!(
            r#"{{"type":"struct","fields":[{{"name":"{name}","type":{type_json},"nullable":true,"metadata":{{}}}}]}}"#
        );
        let schema = parse_schema(&schema_json);

        let (logical_fields, _, refused_columns) =
            build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
                .unwrap_or_else(|_| panic!("a refused column type never fails the whole call"));

        assert!(
            logical_fields.is_empty(),
            "{name} is in the refused set and must carry no LogicalField"
        );
        assert_eq!(refused_columns.len(), 1);
        assert_eq!(refused_columns[0].column_name, name);
        let message = &refused_columns[0].reason;

        assert!(message.contains(name), "message was: {message}");
        assert!(
            message.contains(delta_type_name),
            "message must name the Delta type {delta_type_name}, was: {message}"
        );
        assert_eq!(
            message.contains("#350"),
            cites_350,
            "message was: {message}"
        );
        assert!(!message.contains("#322"), "message was: {message}");
    }
}

#[test]
fn decimal_within_exasol_domain_maps_to_decimal128_tag() {
    let schema = StructType::try_new([StructField::not_null(
        "decimal_col",
        DataType::decimal(10, 2).unwrap(),
    )])
    .unwrap();

    let (logical_fields, _, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("decimal(10,2) is within Exasol's 1..=36 precision domain");

    assert_eq!(logical_fields[0].arrow_type, "decimal128(10,2)");
}

// Scenario Coverage (delta-type-mapping): A Delta type Exasol cannot represent natively is
// surfaced as a VARCHAR rendering
#[test]
fn decimal_outside_exasol_domain_maps_to_utf8() {
    let schema = StructType::try_new([StructField::not_null(
        "big_decimal_col",
        DataType::decimal(38, 10).unwrap(),
    )])
    .unwrap();

    let (logical_fields, _, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("precision 38 exceeds Exasol's domain but is still text-rendered, not refused");

    assert_eq!(logical_fields[0].arrow_type, "utf8");
}

// Scenario Coverage (delta-type-mapping): A Delta type Exasol cannot represent natively is
// surfaced as a VARCHAR rendering
#[test]
fn void_and_interval_types_are_tagged_utf8() {
    let schema = StructType::try_new([
        StructField::nullable("void_col", DataType::VOID),
        StructField::not_null("interval_year_month_col", DataType::INTERVAL_YEAR_MONTH),
        StructField::not_null("interval_day_time_col", DataType::INTERVAL_DAY_TIME),
    ])
    .unwrap();

    let (logical_fields, _, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("void and both interval types are in the text-rendered set");

    for field in &logical_fields {
        assert_eq!(field.arrow_type, "utf8", "field was: {}", field.name);
    }
}

#[test]
fn nullability_is_carried_from_the_delta_schema() {
    let schema = StructType::try_new([
        StructField::nullable("nullable_col", DataType::STRING),
        StructField::not_null("required_col", DataType::STRING),
    ])
    .unwrap();

    let (logical_fields, _, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new()).unwrap();

    assert!(logical_fields[0].nullable);
    assert!(!logical_fields[1].nullable);
}

// Scenario Coverage (delta-type-mapping): A Delta type Exasol cannot represent natively is
// surfaced as a VARCHAR rendering
#[test]
fn a_type_exasol_cannot_represent_is_tagged_utf8_including_a_recursive_array() {
    let schema = StructType::try_new([
        StructField::not_null("array_col", array_of(DataType::INTEGER)),
        StructField::not_null("nested_array_col", array_of(array_of(DataType::INTEGER))),
        StructField::not_null(
            "deeply_nested_array_col",
            array_of(array_of(array_of(DataType::STRING))),
        ),
        StructField::not_null(
            "array_of_out_of_domain_decimal_col",
            array_of(DataType::decimal(38, 10).unwrap()),
        ),
        StructField::not_null(
            "nested_array_of_void_col",
            array_of(array_of(DataType::VOID)),
        ),
    ])
    .unwrap();

    let (logical_fields, _, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("every element type here is native or text-rendered, at every nesting depth");

    let tags: Vec<(&str, &str)> = logical_fields
        .iter()
        .map(|field| (field.name.as_str(), field.arrow_type.as_str()))
        .collect();
    assert_eq!(
        tags,
        vec![
            ("array_col", "utf8"),
            ("nested_array_col", "utf8"),
            ("deeply_nested_array_col", "utf8"),
            ("array_of_out_of_domain_decimal_col", "utf8"),
            ("nested_array_of_void_col", "utf8"),
        ]
    );
}

// Scenario Coverage (delta-type-mapping): A Delta type whose Arrow form cannot be rendered
// faithfully is refused by name
#[test]
fn an_array_inherits_its_element_types_refusal_at_any_nesting_depth() {
    let populated_struct = DataType::from(
        StructType::try_new([StructField::nullable("inner", DataType::INTEGER)]).unwrap(),
    );
    let map = DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true));

    for (element_type, delta_type_name, cites_350) in [
        (DataType::BINARY, "binary", true),
        (populated_struct, "struct", true),
        (map, "map", true),
        (DataType::unshredded_variant(), "variant", false),
    ] {
        let bare = refusal_message("col", element_type.clone());
        assert!(bare.contains("'col'"), "message was: {bare}");
        assert!(bare.contains(delta_type_name), "message was: {bare}");

        let mut nested = element_type;
        for depth in 1..=3 {
            nested = array_of(nested);
            let message = refusal_message("col", nested.clone());

            assert!(
                message.contains(&bare),
                "an array of {delta_type_name} nested {depth} deep must carry the element's own \
                 reason: {message}"
            );
            assert!(
                message.contains(&format!("'{nested}'")),
                "the refusal must name the column's OWN declared type, never only its \
                 element's: {message}"
            );
            assert_eq!(
                message.contains("#350"),
                cites_350,
                "message was: {message}"
            );
            assert!(!message.contains("#322"), "message was: {message}");
        }
    }
}

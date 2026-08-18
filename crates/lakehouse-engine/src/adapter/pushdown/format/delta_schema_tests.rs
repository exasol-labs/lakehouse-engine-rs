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

// Scenario Coverage (delta-type-mapping): Every recorded Delta type change is validated, and an
// unsupported one refuses its column
#[test]
fn a_field_with_no_type_changes_metadata_parses_to_an_empty_list() {
    let field = StructField::not_null("plain", DataType::INTEGER);

    let changes = recorded_type_changes(&field).expect("an unannotated field parses cleanly");

    assert!(changes.is_empty());
}

/// A field carrying OTHER metadata but no `delta.typeChanges` key still parses to an empty list —
/// the key's absence, not the field's overall metadata shape, decides.
#[test]
fn a_field_with_other_metadata_but_no_type_changes_key_parses_to_an_empty_list() {
    let field = mapped_field("a", 1, "col-a");

    let changes = recorded_type_changes(&field).expect("no delta.typeChanges key at all");

    assert!(changes.is_empty());
}

/// Verbatim shape of one entry from the vendored `type-widening` fixture's commit 2
/// `schemaString`, `tableVersion` included — Delta 3.2-era clients still write the superseded RFC
/// key on every entry, and it must be ignored rather than refused.
#[test]
fn parses_fromtype_totype_ignoring_the_superseded_tableversion_key() {
    let field = StructField::nullable("byte_long", DataType::LONG).with_metadata([(
        "delta.typeChanges",
        MetadataValue::Other(serde_json::json!([
            {"toType": "long", "fromType": "byte", "tableVersion": 2}
        ])),
    )]);

    let changes = recorded_type_changes(&field).expect("a well-formed entry must parse");

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].from_type, "byte");
    assert_eq!(changes[0].to_type, "long");
}

#[test]
fn parses_multiple_entries_and_ignores_an_optional_field_path() {
    let field = StructField::nullable("m", DataType::STRING).with_metadata([(
        "delta.typeChanges",
        MetadataValue::Other(serde_json::json!([
            {"fromType": "byte", "toType": "long"},
            {"fromType": "integer", "toType": "long", "fieldPath": "value"}
        ])),
    )]);

    let changes = recorded_type_changes(&field).expect("both entries must parse");

    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0], type_change("byte", "long"));
    assert_eq!(changes[1], type_change("integer", "long"));
}

#[test]
fn a_non_list_type_changes_value_is_refused_naming_the_column() {
    let field = StructField::nullable("bad", DataType::LONG).with_metadata([(
        "delta.typeChanges",
        MetadataValue::String("not-a-list".to_string()),
    )]);

    let err = recorded_type_changes(&field).expect_err("a non-list value must be refused");
    let message = user_message(err);

    assert!(message.contains("'bad'"), "message was: {message}");
    assert!(
        message.contains("delta.typeChanges"),
        "message was: {message}"
    );
}

#[test]
fn a_non_object_entry_is_refused_naming_the_column() {
    let field = StructField::nullable("bad", DataType::LONG).with_metadata([(
        "delta.typeChanges",
        MetadataValue::Other(serde_json::json!(["not-an-object"])),
    )]);

    let err = recorded_type_changes(&field).expect_err("a non-object entry must be refused");
    let message = user_message(err);

    assert!(message.contains("'bad'"), "message was: {message}");
}

#[test]
fn an_entry_missing_or_misshaping_fromtype_or_totype_is_refused_naming_the_column() {
    let missing_from = StructField::nullable("bad", DataType::LONG).with_metadata([(
        "delta.typeChanges",
        MetadataValue::Other(serde_json::json!([{"toType": "long"}])),
    )]);
    let missing_to = StructField::nullable("bad", DataType::LONG).with_metadata([(
        "delta.typeChanges",
        MetadataValue::Other(serde_json::json!([{"fromType": "byte"}])),
    )]);
    let non_string_from = StructField::nullable("bad", DataType::LONG).with_metadata([(
        "delta.typeChanges",
        MetadataValue::Other(serde_json::json!([{"fromType": 1, "toType": "long"}])),
    )]);

    for field in [missing_from, missing_to, non_string_from] {
        let err = recorded_type_changes(&field).expect_err("a malformed entry must be refused");
        let message = user_message(err);
        assert!(message.contains("'bad'"), "message was: {message}");
    }
}

#[test]
fn a_non_string_field_path_is_refused_naming_the_column() {
    let field = StructField::nullable("bad", DataType::LONG).with_metadata([(
        "delta.typeChanges",
        MetadataValue::Other(serde_json::json!([
            {"fromType": "byte", "toType": "long", "fieldPath": 7}
        ])),
    )]);

    let err = recorded_type_changes(&field).expect_err("a non-string fieldPath must be refused");
    let message = user_message(err);

    assert!(message.contains("'bad'"), "message was: {message}");
    assert!(message.contains("fieldPath"), "message was: {message}");
}

fn type_change(from_type: &str, to_type: &str) -> RecordedTypeChange {
    RecordedTypeChange {
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
    }
}

/// Every `fromType`/`toType` pair the Delta protocol's § Type Widening lists, spelled as the Delta
/// schema spells them. `decimal(10,2)` -> `decimal(10,2)` is the `k1 = k2 = 0` corner the
/// protocol's own formula admits.
const PROTOCOL_SUPPORTED_PAIRS: &[(&str, &str)] = &[
    ("byte", "short"),
    ("byte", "integer"),
    ("byte", "long"),
    ("short", "integer"),
    ("short", "long"),
    ("integer", "long"),
    ("float", "double"),
    ("byte", "double"),
    ("short", "double"),
    ("integer", "double"),
    ("date", "timestamp_ntz"),
    ("decimal(10,2)", "decimal(10,2)"),
    ("decimal(10,2)", "decimal(20,2)"),
    ("decimal(10,2)", "decimal(20,5)"),
    ("decimal(10,1)", "decimal(12,3)"),
    ("byte", "decimal(10,0)"),
    ("short", "decimal(12,2)"),
    ("integer", "decimal(11,1)"),
    ("long", "decimal(20,0)"),
    ("long", "decimal(21,1)"),
];

#[test]
fn every_pair_the_protocol_lists_is_supported() {
    let refused: Vec<&(&str, &str)> = PROTOCOL_SUPPORTED_PAIRS
        .iter()
        .filter(|(from_type, to_type)| !is_supported_type_change(&type_change(from_type, to_type)))
        .collect();

    assert!(
        refused.is_empty(),
        "pairs the protocol lists but this engine refused: {refused:?}"
    );
}

/// The floating-point bullet names `Byte`, `Short` or `Int` and omits `Long`, which is lossy above
/// 2^53. arrow-cast performs the cast regardless — `scan/type_relaxation_tests.rs` pins that — so
/// castability is no evidence of protocol support.
#[test]
fn long_to_double_is_refused_because_the_protocol_omits_it() {
    assert!(!is_supported_type_change(&type_change("long", "double")));
}

/// `decimal(10,1)` -> `decimal(11,3)` grows BOTH precision and scale, so the `P' >= P && S' >= S`
/// paraphrase accepts it; the protocol's `k1 >= k2 >= 0` refuses it because the integral digit
/// count shrinks from 9 to 8.
#[test]
fn a_decimal_target_is_checked_as_k1_ge_k2_ge_0_not_as_precision_and_scale_both_growing() {
    assert!(!is_supported_type_change(&type_change(
        "decimal(10,1)",
        "decimal(11,3)"
    )));
    assert!(is_supported_type_change(&type_change(
        "decimal(10,1)",
        "decimal(12,3)"
    )));
}

#[test]
fn a_decimal_target_narrowing_precision_or_scale_is_refused() {
    for (from_type, to_type) in [
        ("decimal(10,2)", "decimal(9,2)"),
        ("decimal(10,2)", "decimal(10,1)"),
        ("decimal(20,5)", "decimal(10,2)"),
    ] {
        assert!(
            !is_supported_type_change(&type_change(from_type, to_type)),
            "{from_type} -> {to_type} must be refused"
        );
    }
}

/// `Byte`, `Short`, and `Int` are all stored as `INT32`, so the protocol's integral-to-decimal
/// target is `Decimal(10 + k1, k2)` for all three and `Decimal(20 + k1, k2)` for `Long` — never a
/// target derived from the declared source type's own narrower range.
#[test]
fn an_integral_source_is_checked_against_the_protocols_int32_and_int64_decimal_bases() {
    for (from_type, to_type) in [
        ("byte", "decimal(4,1)"),
        ("short", "decimal(6,1)"),
        ("integer", "decimal(10,1)"),
        ("long", "decimal(20,1)"),
    ] {
        assert!(
            !is_supported_type_change(&type_change(from_type, to_type)),
            "{from_type} -> {to_type} must be refused"
        );
    }
}

#[test]
fn a_narrowing_or_unrelated_pair_is_refused() {
    for (from_type, to_type) in [
        ("long", "integer"),
        ("integer", "byte"),
        ("double", "float"),
        ("timestamp_ntz", "date"),
        ("date", "timestamp"),
        ("string", "long"),
        ("integer", "string"),
        ("boolean", "integer"),
    ] {
        assert!(
            !is_supported_type_change(&type_change(from_type, to_type)),
            "{from_type} -> {to_type} must be refused"
        );
    }
}

/// A name no Delta primitive spelling matches — a nested type, another format's spelling, or a
/// decimal outside the type's own domain — is one more pair the protocol's list does not contain.
#[test]
fn a_type_name_that_is_not_a_delta_primitive_is_refused() {
    for (from_type, to_type) in [
        ("struct", "long"),
        ("byte", "int"),
        ("byte", "decimal(0,0)"),
        ("byte", "decimal(4,9)"),
        ("", "long"),
    ] {
        assert!(
            !is_supported_type_change(&type_change(from_type, to_type)),
            "{from_type} -> {to_type} must be refused"
        );
    }
}

/// An entry carrying a `fieldPath` is validated by its pair ALONE: the path names a map key/value
/// or an array element and is never parsed.
#[test]
fn an_entry_carrying_a_field_path_is_validated_by_its_pair_alone() {
    let supported = type_change("byte", "long");
    let unsupported = type_change("long", "double");

    assert!(is_supported_type_change(&supported));
    assert!(!is_supported_type_change(&unsupported));
}

// Scenario Coverage (delta-type-mapping): Every recorded Delta type change is validated, and an
// unsupported one refuses its column
#[test]
fn a_field_carrying_an_unsupported_recorded_type_change_is_refused_naming_both_types() {
    let field = StructField::nullable("value", DataType::DOUBLE).with_metadata([(
        "delta.typeChanges",
        MetadataValue::Other(serde_json::json!([
            {"fromType": "long", "toType": "double"}
        ])),
    )]);
    let schema = StructType::try_new([field]).unwrap();

    let (logical_fields, _, refused_columns) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("an unsupported recorded type change refuses the column, not the whole call");

    assert!(
        logical_fields.is_empty(),
        "value must carry no LogicalField once its recorded type change is unsupported"
    );
    assert_eq!(refused_columns.len(), 1);
    assert_eq!(refused_columns[0].column_name, "value");
    let message = &refused_columns[0].reason;
    assert!(message.contains("value"), "message was: {message}");
    assert!(message.contains("long"), "message was: {message}");
    assert!(message.contains("double"), "message was: {message}");
}

#[test]
fn a_field_whose_recorded_type_changes_are_all_supported_plans_normally() {
    let field = StructField::nullable("value", DataType::LONG).with_metadata([(
        "delta.typeChanges",
        MetadataValue::Other(serde_json::json!([
            {"fromType": "byte", "toType": "long"}
        ])),
    )]);
    let schema = StructType::try_new([field]).unwrap();

    let (logical_fields, _, refused_columns) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("a supported recorded type change must not fail the call");

    assert!(
        refused_columns.is_empty(),
        "a supported recorded type change must not refuse the column"
    );
    assert_eq!(logical_fields.len(), 1);
    assert_eq!(logical_fields[0].name, "value");
}

use delta_kernel::schema::{ArrayType, MapType, MetadataValue, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;

use super::*;

/// Verbatim `schemaString` from the `cdf-column-mapping-name-mode` fixture's first commit
/// (`scripts/unity/fixtures/cdf-column-mapping-name-mode/_delta_log/00000000000000000000.json`):
/// `name`-mode column mapping with `col-<uuid>` physical names.
const CDF_COLUMN_MAPPING_NAME_MODE_SCHEMA: &str = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-80396d42-d765-483e-b86e-7ac1e13ef88c"}},{"name":"name","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-ed3e45cf-632b-4a07-bb22-d9f4693bbaa1"}},{"name":"value","type":"double","nullable":true,"metadata":{"delta.columnMapping.id":3,"delta.columnMapping.physicalName":"col-95e13b58-72f1-4d26-8390-49469180a8a2"}}]}"#;

/// Verbatim `nested_struct` field from the vendored `stats-all-types` fixture's `schemaString`
/// (`scripts/unity/fixtures/stats-all-types/_delta_log/00000000000000000000.json`): `name`-mode
/// column mapping, with a `col-<uuid>` physical name on every INNER field as well as on the column.
const STATS_ALL_TYPES_NESTED_STRUCT_SCHEMA: &str = r#"{"type":"struct","fields":[{"name":"nested_struct","type":{"type":"struct","fields":[{"name":"inner_int","type":"integer","nullable":true,"metadata":{"delta.columnMapping.id":17,"delta.columnMapping.physicalName":"col-7f2f94cf-7082-430c-bba7-852bc6c5215e"}},{"name":"inner_string","type":"string","nullable":true,"metadata":{"delta.columnMapping.id":18,"delta.columnMapping.physicalName":"col-26fcfd6b-04c7-4772-8bdf-04ac9425f06e"}},{"name":"inner_double","type":"double","nullable":true,"metadata":{"delta.columnMapping.id":19,"delta.columnMapping.physicalName":"col-92dcf16d-d249-48a9-afb8-93deeaf7ce23"}}]},"nullable":true,"metadata":{"delta.columnMapping.id":16,"delta.columnMapping.physicalName":"col-481c7590-d3b8-4e9c-b40e-7b7128a972f4"}}]}"#;

const INNER_INT_PHYSICAL_NAME: &str = "col-7f2f94cf-7082-430c-bba7-852bc6c5215e";
const INNER_STRING_PHYSICAL_NAME: &str = "col-26fcfd6b-04c7-4772-8bdf-04ac9425f06e";
const INNER_DOUBLE_PHYSICAL_NAME: &str = "col-92dcf16d-d249-48a9-afb8-93deeaf7ce23";

fn parse_schema(json: &str) -> StructType {
    serde_json::from_str(json).expect("fixture schemaString must parse as a Delta StructType")
}

fn user_message(err: UdfError) -> String {
    match err {
        UdfError::User(message) => message,
        other => panic!("expected UdfError::User, got {other:?}"),
    }
}

/// `field` annotated the way the Delta protocol requires under `id`/`name` mode: both a
/// column-mapping id and a physical name.
fn annotated(field: StructField, id: i64, physical_name: &str) -> StructField {
    field.with_metadata([
        ("delta.columnMapping.id", MetadataValue::Number(id)),
        (
            "delta.columnMapping.physicalName",
            MetadataValue::String(physical_name.to_string()),
        ),
    ])
}

/// An annotated `integer` field, the shape most binding-key assertions need.
fn mapped_field(name: &str, id: i64, physical_name: &str) -> StructField {
    annotated(
        StructField::not_null(name, DataType::INTEGER),
        id,
        physical_name,
    )
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

fn map_of(key_type: DataType, value_type: DataType) -> DataType {
    MapType::new(key_type, value_type, true).into()
}

fn struct_of(fields: impl IntoIterator<Item = StructField>) -> DataType {
    StructType::try_new(fields)
        .expect("a test fixture's struct fields are distinct by construction")
        .into()
}

/// One expected primitive nested field: its logical name and the ONE binding key its mode selects.
fn nested_field(name: &str, field_id: Option<i32>, physical_name: Option<&str>) -> NestedField {
    NestedField {
        field_id,
        name: name.to_string(),
        physical_name: physical_name.map(str::to_string),
        nested: None,
    }
}

/// `field` carrying `entries` as its `delta.typeChanges` annotation.
fn with_type_changes(field: StructField, entries: serde_json::Value) -> StructField {
    field.with_metadata([("delta.typeChanges", MetadataValue::Other(entries))])
}

/// The single logical field a one-column table resolves to under `mode`.
fn only_logical_field(schema: &StructType, mode: ColumnMappingMode) -> LogicalField {
    let (logical_fields, _, refused_columns) = build_delta_table_schema(schema, mode, Vec::new())
        .expect("this fixture's only column is mappable");
    assert!(
        refused_columns.is_empty(),
        "expected no refusal, got: {refused_columns:?}"
    );
    assert_eq!(logical_fields.len(), 1);
    logical_fields.into_iter().next().unwrap()
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
/// A container is tagged `utf8` exactly when every one of its members is itself renderable, at
/// every nesting depth and through every member position — an `array`'s element, a `struct`'s
/// field, and a `map`'s key and value alike.
#[test]
fn containers_classify_recursively_by_renderability() {
    let schema = StructType::try_new([
        StructField::not_null("array_col", array_of(DataType::INTEGER)),
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
        StructField::not_null(
            "struct_col",
            struct_of([StructField::nullable("inner", DataType::INTEGER)]),
        ),
        StructField::not_null(
            "struct_of_void_col",
            struct_of([StructField::nullable("v", DataType::VOID)]),
        ),
        StructField::not_null(
            "struct_of_out_of_domain_decimal_col",
            struct_of([StructField::nullable(
                "d",
                DataType::decimal(38, 10).unwrap(),
            )]),
        ),
        StructField::not_null("map_col", map_of(DataType::STRING, DataType::INTEGER)),
        StructField::not_null(
            "map_of_int_key_col",
            map_of(DataType::INTEGER, DataType::STRING),
        ),
        StructField::not_null(
            "array_of_struct_col",
            array_of(struct_of([StructField::nullable("a", DataType::INTEGER)])),
        ),
        StructField::not_null(
            "map_of_array_col",
            map_of(DataType::STRING, array_of(DataType::INTEGER)),
        ),
        StructField::not_null(
            "struct_of_map_col",
            struct_of([StructField::nullable(
                "m",
                map_of(DataType::STRING, array_of(DataType::INTEGER)),
            )]),
        ),
    ])
    .unwrap();

    let (logical_fields, _, refused_columns) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("every member type here is native, text-rendered, or a container of those");

    assert!(
        refused_columns.is_empty(),
        "no container of renderable members may be refused, got: {refused_columns:?}"
    );
    let tags: Vec<(&str, &str)> = logical_fields
        .iter()
        .map(|field| (field.name.as_str(), field.arrow_type.as_str()))
        .collect();
    assert_eq!(
        tags,
        vec![
            ("array_col", "utf8"),
            ("deeply_nested_array_col", "utf8"),
            ("array_of_out_of_domain_decimal_col", "utf8"),
            ("nested_array_of_void_col", "utf8"),
            ("struct_col", "utf8"),
            ("struct_of_void_col", "utf8"),
            ("struct_of_out_of_domain_decimal_col", "utf8"),
            ("map_col", "utf8"),
            ("map_of_int_key_col", "utf8"),
            ("array_of_struct_col", "utf8"),
            ("map_of_array_col", "utf8"),
            ("struct_of_map_col", "utf8"),
        ]
    );
}

// Scenario Coverage (delta-type-mapping): A Delta type whose Arrow form cannot be rendered
// faithfully is refused by name
/// `binary` and `variant` are the whole refused set, and a container joins it exactly when one of
/// its members is in it — at any depth, through any member position. One composer states every
/// container refusal: the column's OWN declared type, the offending member's path, and that
/// member's own cause, so no operator is told the column has a member's type.
#[test]
fn refused_set_is_binary_variant_and_containers_of_them() {
    let variant = DataType::unshredded_variant();
    let binary_field = || StructField::nullable("b", DataType::BINARY);

    for (declared_type, member_path, refused_type) in [
        (DataType::BINARY, None, "binary"),
        (variant.clone(), None, "variant"),
        (array_of(DataType::BINARY), Some("col.element"), "binary"),
        (
            array_of(array_of(DataType::BINARY)),
            Some("col.element.element"),
            "binary",
        ),
        (struct_of([binary_field()]), Some("col.b"), "binary"),
        (
            struct_of([
                StructField::nullable("ok", DataType::INTEGER),
                StructField::nullable("v", variant.clone()),
            ]),
            Some("col.v"),
            "variant",
        ),
        (
            map_of(DataType::BINARY, DataType::STRING),
            Some("col.key"),
            "binary",
        ),
        (
            map_of(DataType::STRING, DataType::BINARY),
            Some("col.value"),
            "binary",
        ),
        (
            array_of(struct_of([binary_field()])),
            Some("col.element.b"),
            "binary",
        ),
        (
            struct_of([StructField::nullable("inner", struct_of([binary_field()]))]),
            Some("col.inner.b"),
            "binary",
        ),
        (
            map_of(DataType::STRING, array_of(struct_of([binary_field()]))),
            Some("col.value.element.b"),
            "binary",
        ),
    ] {
        let message = refusal_message("col", declared_type.clone());

        assert!(
            message.contains("Delta column 'col'"),
            "message was: {message}"
        );
        assert!(
            message.contains(&format!("has type '{declared_type}'")),
            "the refusal must name the column's OWN declared type: {message}"
        );
        assert!(
            message.contains(&format!("type '{refused_type}'")),
            "the refusal must name the refused type {refused_type}: {message}"
        );
        match member_path {
            Some(member_path) => assert!(
                message.contains(&format!("whose member '{member_path}'")),
                "the refusal must name the offending member's path {member_path}: {message}"
            ),
            None => assert!(
                !message.contains("whose member"),
                "a column refused for its OWN type names no member: {message}"
            ),
        }
        assert_eq!(
            message.contains("#351"),
            refused_type == "binary",
            "binary's reason cites the issue that owns it, and variant's cites none: {message}"
        );
        assert!(!message.contains("#350"), "message was: {message}");
        assert!(!message.contains("#322"), "message was: {message}");
    }
}

// Scenario Coverage (delta-type-mapping): A Delta type whose Arrow form cannot be rendered
// faithfully is refused by name
/// The nested counterpart of `a_refused_columns_binding_key_is_never_looked_up_even_when_it_would
/// _itself_fail`: a column refused for one member's TYPE must never instead FAIL the call for a
/// sibling member's missing column-mapping annotation, which under `id` mode the whole nested tree
/// would otherwise be checked for.
#[test]
fn a_refused_containers_nested_binding_key_is_never_looked_up() {
    let refused_member_beside_an_unannotated_one = annotated(
        StructField::nullable(
            "keep",
            struct_of([
                StructField::nullable("unannotated", DataType::INTEGER),
                StructField::nullable("b", DataType::BINARY),
            ]),
        ),
        1,
        "col-keep",
    );
    let schema = StructType::try_new([refused_member_beside_an_unannotated_one]).unwrap();

    let (logical_fields, _, refused_columns) =
        build_delta_table_schema(&schema, ColumnMappingMode::Id, Vec::new()).expect(
            "a column refused for a member's type must not fail the call for a sibling member's \
             missing annotation",
        );

    assert!(logical_fields.is_empty());
    assert_eq!(refused_columns.len(), 1);
    assert_eq!(refused_columns[0].column_name, "keep");
    assert!(
        refused_columns[0].reason.contains("'keep.b'"),
        "message was: {}",
        refused_columns[0].reason
    );
}

// Scenario Coverage (delta-type-mapping): Every recorded Delta type change is validated, and an
// unsupported one refuses its column
#[test]
fn a_field_with_no_type_changes_metadata_parses_to_an_empty_list() {
    let field = StructField::not_null("plain", DataType::INTEGER);

    let changes = parsed_type_changes(&field).expect("an unannotated field parses cleanly");

    assert!(changes.is_empty());
}

/// A field carrying OTHER metadata but no `delta.typeChanges` key still parses to an empty list —
/// the key's absence, not the field's overall metadata shape, decides.
#[test]
fn a_field_with_other_metadata_but_no_type_changes_key_parses_to_an_empty_list() {
    let field = mapped_field("a", 1, "col-a");

    let changes = parsed_type_changes(&field).expect("no delta.typeChanges key at all");

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

    let changes = parsed_type_changes(&field).expect("a well-formed entry must parse");

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].from_type, "byte");
    assert_eq!(changes[0].to_type, "long");
}

/// An entry's `fieldPath` is RETAINED: the refusal reports it so an operator can locate the change
/// inside a nested tree, even though it is never an input to the supported-pair check.
#[test]
fn parses_multiple_entries_and_retains_an_optional_field_path() {
    let field = StructField::nullable("m", DataType::STRING).with_metadata([(
        "delta.typeChanges",
        MetadataValue::Other(serde_json::json!([
            {"fromType": "byte", "toType": "long"},
            {"fromType": "integer", "toType": "long", "fieldPath": "value"}
        ])),
    )]);

    let changes = parsed_type_changes(&field).expect("both entries must parse");

    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0], type_change("byte", "long"));
    assert_eq!(changes[1], type_change_at("integer", "long", "value"));
}

#[test]
fn a_non_list_type_changes_value_is_refused_naming_the_column() {
    let field = StructField::nullable("bad", DataType::LONG).with_metadata([(
        "delta.typeChanges",
        MetadataValue::String("not-a-list".to_string()),
    )]);

    let err = parsed_type_changes(&field).expect_err("a non-list value must be refused");
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

    let err = parsed_type_changes(&field).expect_err("a non-object entry must be refused");
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
        let err = parsed_type_changes(&field).expect_err("a malformed entry must be refused");
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

    let err = parsed_type_changes(&field).expect_err("a non-string fieldPath must be refused");
    let message = user_message(err);

    assert!(message.contains("'bad'"), "message was: {message}");
    assert!(message.contains("fieldPath"), "message was: {message}");
}

fn type_change(from_type: &str, to_type: &str) -> RecordedTypeChange {
    RecordedTypeChange {
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
        field_path: None,
    }
}

/// A recorded entry carrying a `fieldPath`, which the protocol writes when the change applies to a
/// map key/value or an array element rather than to the annotated field itself.
fn type_change_at(from_type: &str, to_type: &str, field_path: &str) -> RecordedTypeChange {
    RecordedTypeChange {
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
        field_path: Some(field_path.to_string()),
    }
}

/// The recorded entries `field` carries, read at the path a top-level column of its name occupies.
fn parsed_type_changes(field: &StructField) -> Result<Vec<RecordedTypeChange>, UdfError> {
    recorded_type_changes(field, &FieldPath::column(field.name()))
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

/// An entry carrying a `fieldPath` is validated by its pair ALONE: the path is retained for the
/// refusal to report and never interpreted, coerced into a type decision, or resolved against the
/// schema.
#[test]
fn an_entry_carrying_a_field_path_is_validated_by_its_pair_alone() {
    let supported = type_change_at("byte", "long", "value");
    let unsupported = type_change_at("long", "double", "element");

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

// Scenario Coverage (delta-type-mapping): Every recorded Delta type change is validated, and an
// unsupported one refuses its column
/// Every recorded entry is validated at EVERY nesting depth, and the refusal reports the
/// structural path from the top-level column down to the annotated field, composed with that
/// entry's own `fieldPath` when it carries one.
#[test]
fn nested_type_changes_are_validated_and_refuse_with_a_composed_path() {
    let long_to_double = || serde_json::json!([{"fromType": "long", "toType": "double"}]);
    let long_to_double_on_a_value =
        || serde_json::json!([{"fromType": "long", "toType": "double", "fieldPath": "value"}]);

    for (column_name, data_type, expected_path) in [
        (
            "payload",
            struct_of([with_type_changes(
                StructField::nullable("inner", DataType::LONG),
                long_to_double(),
            )]),
            "payload.inner",
        ),
        (
            "payload",
            struct_of([with_type_changes(
                StructField::nullable("attrs", map_of(DataType::STRING, DataType::LONG)),
                long_to_double_on_a_value(),
            )]),
            "payload.attrs.value",
        ),
        (
            "rows",
            array_of(struct_of([with_type_changes(
                StructField::nullable("v", DataType::LONG),
                long_to_double(),
            )])),
            "rows.element.v",
        ),
        (
            "deep",
            struct_of([StructField::nullable(
                "mid",
                struct_of([with_type_changes(
                    StructField::nullable("leaf", DataType::LONG),
                    long_to_double(),
                )]),
            )]),
            "deep.mid.leaf",
        ),
    ] {
        let schema = StructType::try_new([StructField::nullable(column_name, data_type)]).unwrap();

        let (logical_fields, _, refused_columns) =
            build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new()).expect(
                "an unsupported recorded type change refuses the column, not the whole call",
            );

        assert!(logical_fields.is_empty());
        assert_eq!(refused_columns.len(), 1);
        assert_eq!(refused_columns[0].column_name, column_name);
        let message = &refused_columns[0].reason;
        assert!(
            message.contains(&format!("whose member '{expected_path}'")),
            "the refusal must locate the annotated field: {message}"
        );
        assert!(
            message.contains("delta.typeChanges"),
            "message was: {message}"
        );
        assert!(message.contains("'long'"), "message was: {message}");
        assert!(message.contains("'double'"), "message was: {message}");
    }
}

/// The `fieldPath` an entry on the TOP-LEVEL field carries is reported too: the annotation sits on
/// the column, but the change it records applies to the column's map value.
#[test]
fn a_top_level_entrys_own_field_path_is_reported_below_the_column() {
    let schema = StructType::try_new([with_type_changes(
        StructField::nullable("m", map_of(DataType::STRING, DataType::LONG)),
        serde_json::json!([{"fromType": "long", "toType": "double", "fieldPath": "value"}]),
    )])
    .unwrap();

    let (_, _, refused_columns) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new()).unwrap();

    assert_eq!(refused_columns.len(), 1);
    assert!(
        refused_columns[0].reason.contains("whose member 'm.value'"),
        "message was: {}",
        refused_columns[0].reason
    );
}

/// A SUPPORTED nested change refuses nothing, and a MALFORMED nested annotation surfaces the same
/// `UdfError` a malformed top-level one does rather than being skipped for sitting in a container.
#[test]
fn a_nested_annotation_is_supported_or_malformed_by_the_same_rules_as_a_top_level_one() {
    let supported = StructType::try_new([StructField::nullable(
        "payload",
        struct_of([with_type_changes(
            StructField::nullable("inner", DataType::LONG),
            serde_json::json!([{"fromType": "byte", "toType": "long"}]),
        )]),
    )])
    .unwrap();
    let malformed = StructType::try_new([StructField::nullable(
        "payload",
        struct_of([with_type_changes(
            StructField::nullable("inner", DataType::LONG),
            serde_json::json!([{"toType": "long"}]),
        )]),
    )])
    .unwrap();

    let (logical_fields, _, refused_columns) =
        build_delta_table_schema(&supported, ColumnMappingMode::None, Vec::new())
            .expect("a supported nested change must not fail the call");
    assert!(refused_columns.is_empty());
    assert_eq!(logical_fields.len(), 1);

    let err = build_delta_table_schema(&malformed, ColumnMappingMode::None, Vec::new())
        .expect_err("a malformed nested annotation must surface as a UdfError");
    let message = user_message(err);
    assert!(
        message.contains("'payload.inner'"),
        "message was: {message}"
    );
    assert!(message.contains("fromType"), "message was: {message}");
}

// Scenario Coverage (delta-type-mapping): Every nested field's logical name and binding key reach
// the scan
/// The vendored `stats-all-types` fixture's `nested_struct` is the pairing the JSON renderer keys
/// by: each inner field's LOGICAL name plus the ONE binding key the mode in force selects, and
/// never a `col-`-prefixed physical name in the `name` slot.
#[test]
fn nested_descriptor_carries_logical_names_and_mode_selected_binding_keys() {
    let schema = parse_schema(STATS_ALL_TYPES_NESTED_STRUCT_SCHEMA);

    for (mode, expected_fields) in [
        (
            ColumnMappingMode::Name,
            vec![
                nested_field("inner_int", None, Some(INNER_INT_PHYSICAL_NAME)),
                nested_field("inner_string", None, Some(INNER_STRING_PHYSICAL_NAME)),
                nested_field("inner_double", None, Some(INNER_DOUBLE_PHYSICAL_NAME)),
            ],
        ),
        (
            ColumnMappingMode::Id,
            vec![
                nested_field("inner_int", Some(17), None),
                nested_field("inner_string", Some(18), None),
                nested_field("inner_double", Some(19), None),
            ],
        ),
        (
            ColumnMappingMode::None,
            vec![
                nested_field("inner_int", None, None),
                nested_field("inner_string", None, None),
                nested_field("inner_double", None, None),
            ],
        ),
    ] {
        let field = only_logical_field(&schema, mode);

        assert_eq!(field.arrow_type, "utf8", "mode was: {mode:?}");
        assert_eq!(
            field.nested,
            Some(NestedMembers::Struct {
                fields: expected_fields
            }),
            "mode was: {mode:?}"
        );
    }
}

// Scenario Coverage (delta-type-mapping): Every nested field's logical name and binding key reach
// the scan
/// A list's element and a map's key and value are POSITIONAL: the descriptor records them without
/// a name or a binding key, and records them at all only when the member is itself a container.
/// A primitive column carries no descriptor whatsoever.
#[test]
fn positional_members_carry_no_name_and_a_primitive_column_carries_no_descriptor() {
    let schema = StructType::try_new([
        StructField::nullable("primitive_col", DataType::LONG),
        StructField::nullable("array_col", array_of(DataType::INTEGER)),
        StructField::nullable("map_col", map_of(DataType::STRING, DataType::INTEGER)),
        StructField::nullable(
            "array_of_struct_col",
            array_of(struct_of([StructField::nullable("a", DataType::INTEGER)])),
        ),
        StructField::nullable(
            "map_of_array_col",
            map_of(DataType::STRING, array_of(DataType::INTEGER)),
        ),
    ])
    .unwrap();

    let (logical_fields, _, _) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new()).unwrap();

    let descriptors: Vec<(&str, Option<&NestedMembers>)> = logical_fields
        .iter()
        .map(|field| (field.name.as_str(), field.nested.as_ref()))
        .collect();
    assert_eq!(
        descriptors,
        vec![
            ("primitive_col", None),
            ("array_col", Some(&NestedMembers::List { element: None })),
            (
                "map_col",
                Some(&NestedMembers::Map {
                    key: None,
                    value: None
                })
            ),
            (
                "array_of_struct_col",
                Some(&NestedMembers::List {
                    element: Some(Box::new(NestedMembers::Struct {
                        fields: vec![nested_field("a", None, None)]
                    }))
                })
            ),
            (
                "map_of_array_col",
                Some(&NestedMembers::Map {
                    key: None,
                    value: Some(Box::new(NestedMembers::List { element: None }))
                })
            ),
        ]
    );
}

// Scenario Coverage (delta-type-mapping): Every nested field's logical name and binding key reach
// the scan
/// A nested field whose physical identity the writer never wrote cannot be bound, so it is refused
/// at the depth it is missing, by the same rule and the same message a missing top-level
/// annotation already uses.
#[test]
fn a_nested_field_missing_its_modes_annotation_is_refused_naming_its_path() {
    let schema = StructType::try_new([annotated(
        StructField::nullable(
            "payload",
            struct_of([StructField::nullable("inner", DataType::INTEGER)]),
        ),
        1,
        "col-payload",
    )])
    .unwrap();

    for (mode, expected_key) in [
        (ColumnMappingMode::Id, "delta.columnMapping.id"),
        (ColumnMappingMode::Name, "delta.columnMapping.physicalName"),
    ] {
        let err = build_delta_table_schema(&schema, mode, Vec::new())
            .expect_err("an unannotated nested field must be refused");
        let message = user_message(err);

        assert!(
            message.contains("'payload.inner'"),
            "message was: {message}"
        );
        assert!(message.contains(expected_key), "message was: {message}");
    }
}

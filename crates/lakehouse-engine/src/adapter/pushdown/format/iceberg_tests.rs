use super::super::super::test_support::{
    BINARY_ICEBERG_LAST_COLUMN_ID, RecordingCatalog, binary_iceberg_fields, binary_refused,
    declared_logical_schema, glue_catalog_table, involved_notes, load_table_body,
    load_table_body_notes, load_table_body_with_columns, object_endpoint, resolve_from_notes,
    sample_storage, sigv4_creds, user_message,
};
use super::super::ResolvedScan;
use super::*;
use crate::adapter::column_notes::TableNotes;
use iceberg::spec::{DataContentType, DataFileFormat};
use lakehouse_catalog::{ConnectionCreds, StorageBackend, TableFormat};

fn name_mapped_load_table_body() -> Json {
    load_table_body(serde_json::json!({
        "last-column-id": 2,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "label", "required": false, "type": "string"}
        ]}],
        "properties": {
            "schema.name-mapping.default":
                "[{\"field-id\":1,\"names\":[\"id\"]},{\"field-id\":2,\"names\":[\"label\"]}]"
        }
    }))
}

/// Plans `table` through a loopback REST catalog that answers every request with `body`, from the
/// notes CREATE recorded when the table's current schema was `body`'s.
async fn resolve_rest(
    body: String,
    table: &str,
    creds: &ConnectionCreds,
    storage: &StorageBackend,
    filter_json: Option<&Json>,
) -> Result<ResolvedScan, UdfError> {
    let notes = load_table_body_notes(&serde_json::from_str(&body).expect("a JSON body"));
    let catalog = RecordingCatalog::spawn(move |_target| (200, body.clone())).await;
    let session = CatalogSession::resolve(&catalog.uri, &creds.warehouse, creds)
        .await
        .expect("the SigV4 path resolves a session without contacting the catalog");
    let catalog_props = CatalogProps {
        warehouse: creds.warehouse.clone(),
        table: table.into(),
    };
    let reader = IcebergFormatReader {
        metadata: IcebergMetadataSource::RestLoadTable {
            session: &session,
            catalog_props: &catalog_props,
        },
        connection: ConnectionStorage {
            storage,
            creds,
            allow_http: true,
        },
    };
    resolve_from_notes(&reader, &involved_notes("T", &notes), filter_json).await
}

/// Scenario: The Iceberg format reader owns file resolution and binds columns by field-id
#[tokio::test]
async fn iceberg_reader_owns_resolution_and_keeps_its_encoding() {
    let storage = sample_storage();
    let body = name_mapped_load_table_body().to_string();
    let resolved = resolve_rest(body, "db.t", &sigv4_creds(), &storage, None)
        .await
        .expect("the reader must resolve the loopback catalog's table");

    assert_eq!(resolved.table_root, "s3://bucket/db/t");
    assert!(
        resolved.files.is_empty(),
        "a snapshotless table has no files"
    );
    assert_eq!(
        resolved.effective_storage, storage,
        "no vending is requested, so the effective storage is the static one"
    );
    assert_eq!(
        resolved.logical_schema.len(),
        2,
        "the fixture's two-field schema must reach the resolved scan"
    );
    assert!(
        resolved
            .logical_schema
            .iter()
            .all(|field| field.field_id.is_some() && field.physical_name.is_none()),
        "every Iceberg logical field must carry a field-id and no declared physical name, \
         so its encoding gains no key: {:?}",
        resolved.logical_schema
    );
    assert_eq!(
        resolved.name_mapping,
        vec![
            NameMappingEntry {
                names: vec!["id".to_string()],
                field_id: 1,
                fields: Vec::new(),
            },
            NameMappingEntry {
                names: vec!["label".to_string()],
                field_id: 2,
                fields: Vec::new(),
            },
        ],
        "the fixture's name-mapping property must reach the resolved scan"
    );
    assert!(
        resolved.partition_columns.is_empty(),
        "an Iceberg scan must carry no partition columns, so its encoding stays byte-identical"
    );
}

#[test]
fn classify_accepts_parquet_data_parquet_positional_delete_and_puffin_deletion_vector() {
    assert!(
        classify_planned_file(DataContentType::Data, DataFileFormat::Parquet).is_ok(),
        "Parquet data file must be supported"
    );
    assert!(
        classify_planned_file(DataContentType::PositionDeletes, DataFileFormat::Parquet).is_ok(),
        "Parquet positional delete must be supported"
    );
    assert!(
        classify_planned_file(DataContentType::PositionDeletes, DataFileFormat::Puffin).is_ok(),
        "Puffin deletion vector must be supported"
    );
}

#[test]
fn classify_rejects_equality_deletes() {
    for fmt in [
        DataFileFormat::Parquet,
        DataFileFormat::Avro,
        DataFileFormat::Orc,
    ] {
        assert_eq!(
            classify_planned_file(DataContentType::EqualityDeletes, fmt),
            Err(UnsupportedDeleteMechanism::EqualityDelete),
            "equality delete ({fmt:?}) must fail loud"
        );
    }
}

#[test]
fn classify_rejects_orc_and_avro_data_and_delete_files() {
    assert_eq!(
        classify_planned_file(DataContentType::Data, DataFileFormat::Orc),
        Err(UnsupportedDeleteMechanism::OrcDataFile),
    );
    assert_eq!(
        classify_planned_file(DataContentType::Data, DataFileFormat::Avro),
        Err(UnsupportedDeleteMechanism::AvroDataFile),
    );
    assert_eq!(
        classify_planned_file(DataContentType::PositionDeletes, DataFileFormat::Orc),
        Err(UnsupportedDeleteMechanism::OrcDeleteFile),
    );
    assert_eq!(
        classify_planned_file(DataContentType::PositionDeletes, DataFileFormat::Avro),
        Err(UnsupportedDeleteMechanism::AvroDeleteFile),
    );
}

#[test]
fn unsupported_delete_error_names_mechanism_and_redacts() {
    let err = unsupported_delete_error(
        UnsupportedDeleteMechanism::EqualityDelete,
        "db.mor_eq_table",
    );
    let msg = user_message(err);
    assert!(
        msg.contains("Iceberg equality deletes"),
        "error must name the mechanism: {msg}"
    );
    assert!(
        msg.contains("db.mor_eq_table"),
        "error must name the offending table: {msg}"
    );
    assert!(
        !msg.contains("access_key"),
        "must not leak access_key: {msg}"
    );
    assert!(
        !msg.contains("secret_key"),
        "must not leak secret_key: {msg}"
    );
}

#[test]
fn plan_errors_redact_the_literal_azure_secret_values() {
    let account_key = "Zm9vYmFyYmF6cXV1eGNvcmdlc2VjcmV0QUNDT1VOVEtFWT09";
    let sas_permissions = "sp=racwdlmeop";
    let sas_token = format!(
        "sv=2024-11-04&ss=bf&srt=sco&{sas_permissions}&se=2026-12-31T23:59:59Z&sig=aB3%2FxQ7"
    );
    let raw = format!(
        "AuthenticationFailed: Server failed to authenticate the request. \
         String to sign used was: {account_key}. \
         Request URL: https://acct.dfs.core.windows.net/c/meta/snap.avro?{sas_token}"
    );
    let secrets = [account_key, sas_token.as_str()];

    let surfaced = format!(
        "failed to plan Iceberg files for 'ns.tbl': {}",
        redact_error_text(&raw, &secrets)
    );

    assert!(
        !surfaced.contains(account_key),
        "account key value must not survive: {surfaced}"
    );
    assert!(
        !surfaced.contains(&sas_token),
        "SAS token value must not survive: {surfaced}"
    );
    assert!(
        !surfaced.contains(sas_permissions),
        "the SAS token's permission field must not survive either: {surfaced}"
    );
    assert!(
        surfaced.contains("failed to plan Iceberg files for 'ns.tbl'"),
        "the actionable context must be preserved: {surfaced}"
    );
}

/// A missing local file, not a closed port: object-store reads retry a refused connection with
/// seconds of backoff, a not-found read is never retried.
#[tokio::test]
async fn an_unreadable_manifest_list_fails_planning_naming_its_cause() {
    let body = load_table_body(serde_json::json!({
        "last-sequence-number": 1,
        "current-snapshot-id": 1,
        "snapshots": [{
            "snapshot-id": 1,
            "sequence-number": 1,
            "timestamp-ms": 0,
            "manifest-list": "file:///lakehouse-engine-missing-manifest-list/snap-1.avro",
            "summary": {"operation": "append"},
            "schema-id": 0
        }]
    }));
    let table = iceberg::table::Table::builder()
        .identifier(iceberg::TableIdent::from_strs(["db", "t"]).expect("identifier"))
        .file_io(iceberg::io::FileIO::new_with_fs())
        .runtime(iceberg::Runtime::try_current().expect("runtime"))
        .metadata(
            serde_json::from_value::<iceberg::spec::TableMetadata>(body["metadata"].clone())
                .expect("metadata"),
        )
        .build()
        .expect("table");

    let error = plan_file_tasks(table, "db.t", None, &[])
        .await
        .expect_err("an unreadable manifest list must fail planning");

    let text = error.to_string();
    assert!(
        text.contains("Iceberg file") && text.contains("No such file or directory"),
        "{text}"
    );
}

#[test]
fn iceberg_delete_mechanism_maps_position_equality_and_the_data_sentinel() {
    let mechanism =
        |path: &str, content, format| iceberg_delete_mechanism(&delete_file(path, content, format));
    assert_eq!(
        mechanism(
            "d0.parquet",
            DataContentType::PositionDeletes,
            DataFileFormat::Parquet
        ),
        DeleteMechanism::IcebergPositionalDelete {
            path: "d0.parquet".into(),
            size: 50,
        }
    );
    assert_eq!(
        mechanism(
            "d1.parquet",
            DataContentType::EqualityDeletes,
            DataFileFormat::Parquet
        ),
        DeleteMechanism::IcebergEqualityDelete {
            path: "d1.parquet".into(),
            size: 50,
        }
    );
    assert_eq!(
        mechanism(
            "d3.puffin",
            DataContentType::PositionDeletes,
            DataFileFormat::Puffin
        ),
        DeleteMechanism::IcebergPuffinDeletionVector {
            path: "d3.puffin".into(),
            size: 50,
            content_offset: 4,
            content_size_in_bytes: 40,
            record_count: 2,
        },
        "a Puffin position delete is a deletion vector carrying its blob range and cardinality"
    );
    assert_eq!(
        mechanism("d2.parquet", DataContentType::Data, DataFileFormat::Parquet),
        DeleteMechanism::IcebergEqualityDelete {
            path: "d2.parquet".into(),
            size: 50,
        },
        "the Data sentinel must map to a non-positional mechanism"
    );
}

fn scan_task(
    format: DataFileFormat,
    deletes: Vec<iceberg::scan::FileScanTaskDeleteFile>,
) -> iceberg::scan::FileScanTask {
    iceberg::scan::FileScanTask::builder()
        .with_file_size_in_bytes(100)
        .with_start(0)
        .with_length(100)
        .with_data_file_path("s3://bucket/t/data-0".to_string())
        .with_data_file_format(format)
        .with_schema(std::sync::Arc::new(
            iceberg::spec::Schema::builder()
                .build()
                .expect("an empty schema"),
        ))
        .with_project_field_ids(vec![])
        .with_deletes(deletes)
        .with_case_sensitive(true)
        .build()
        .expect("a scan task")
}

fn delete_file(
    path: &str,
    content: DataContentType,
    format: DataFileFormat,
) -> iceberg::scan::FileScanTaskDeleteFile {
    let is_dv = format == DataFileFormat::Puffin;
    iceberg::scan::FileScanTaskDeleteFile::builder()
        .with_file_path(path.to_string())
        .with_file_size_in_bytes(50)
        .with_file_type(content)
        .with_file_format(format)
        .with_partition_spec_id(0)
        .with_referenced_data_file(is_dv.then(|| "s3://bucket/t/data-0".to_string()))
        .with_content_offset(is_dv.then_some(4))
        .with_content_size_in_bytes(is_dv.then_some(40))
        .with_record_count(is_dv.then_some(2))
        .build()
        .expect("a delete file")
}

/// Scenario: Delete files and deletion vectors are preserved into the scan spec
#[test]
fn planned_file_entry_carries_parquet_positional_deletes_and_deletion_vectors() {
    let task = scan_task(
        DataFileFormat::Parquet,
        vec![
            delete_file(
                "s3://bucket/t/del-0",
                DataContentType::PositionDeletes,
                DataFileFormat::Parquet,
            ),
            delete_file(
                "s3://bucket/t/dv-0",
                DataContentType::PositionDeletes,
                DataFileFormat::Puffin,
            ),
        ],
    );

    let entry = planned_file_entry(&task).expect("Parquet data with supported deletes");

    assert_eq!(
        (entry.path.as_str(), entry.size),
        ("s3://bucket/t/data-0", 100)
    );
    assert_eq!(
        entry.deletes,
        vec![
            DeleteMechanism::IcebergPositionalDelete {
                path: "s3://bucket/t/del-0".into(),
                size: 50,
            },
            DeleteMechanism::IcebergPuffinDeletionVector {
                path: "s3://bucket/t/dv-0".into(),
                size: 50,
                content_offset: 4,
                content_size_in_bytes: 40,
                record_count: 2,
            },
        ]
    );
}

#[test]
fn planned_file_entry_refuses_an_unsupported_data_or_applied_delete_file() {
    let positional = delete_file(
        "s3://bucket/t/del-0",
        DataContentType::PositionDeletes,
        DataFileFormat::Parquet,
    );
    let cases = [
        (
            scan_task(DataFileFormat::Orc, vec![]),
            UnsupportedDeleteMechanism::OrcDataFile,
        ),
        (
            scan_task(
                DataFileFormat::Parquet,
                vec![
                    positional,
                    delete_file(
                        "s3://bucket/t/eq-0",
                        DataContentType::EqualityDeletes,
                        DataFileFormat::Parquet,
                    ),
                ],
            ),
            UnsupportedDeleteMechanism::EqualityDelete,
        ),
    ];

    for (task, expected) in cases {
        assert_eq!(
            planned_file_entry(&task).map(|_| ()),
            Err(expected),
            "{task:?}"
        );
    }
}

fn identity_partitioned_schema() -> std::sync::Arc<iceberg::spec::Schema> {
    use iceberg::spec::{NestedField, PrimitiveType, StructType, Type};
    let city = NestedField::optional(4, "city", Type::Primitive(PrimitiveType::String));
    std::sync::Arc::new(
        iceberg::spec::Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(
                    3,
                    "address",
                    Type::Struct(StructType::new(vec![city.into()])),
                )
                .into(),
                NestedField::optional(5, "token", Type::Primitive(PrimitiveType::Uuid)).into(),
            ])
            .build()
            .expect("a partitioned schema"),
    )
}

/// The logical fields a declaration of [`identity_partitioned_schema`] carries: the `uuid` column
/// is refused at plan time, so no logical field stands for it.
fn identity_partitioned_logical_fields() -> Vec<LogicalField> {
    let field = |field_id, name: &str, arrow_type: &str, nullable| LogicalField {
        field_id: Some(field_id),
        name: name.to_string(),
        arrow_type: arrow_type.to_string(),
        nullable,
        initial_default: None,
        nested: None,
        physical_name: None,
    };
    vec![
        field(1, "id", "int64", false),
        field(2, "region", "utf8", true),
        field(3, "address", "utf8", true),
    ]
}

fn identity_partition_spec(
    spec_id: i32,
    fields: &[(&str, &str, iceberg::spec::Transform)],
) -> std::sync::Arc<iceberg::spec::PartitionSpec> {
    let builder = fields.iter().fold(
        iceberg::spec::PartitionSpec::builder(identity_partitioned_schema()).with_spec_id(spec_id),
        |builder, (source, name, transform)| {
            builder
                .add_partition_field(*source, *name, *transform)
                .expect("a valid partition field")
        },
    );
    std::sync::Arc::new(builder.build().expect("a partition spec"))
}

fn partitioned_scan_task(
    spec: &std::sync::Arc<iceberg::spec::PartitionSpec>,
    partition: Vec<Option<iceberg::spec::Literal>>,
) -> iceberg::scan::FileScanTask {
    iceberg::scan::FileScanTask::builder()
        .with_file_size_in_bytes(100)
        .with_start(0)
        .with_length(100)
        .with_data_file_path("s3://bucket/t/data-0".to_string())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(identity_partitioned_schema())
        .with_project_field_ids(vec![])
        .with_case_sensitive(true)
        .with_partition(Some(partition.into_iter().collect()))
        .with_partition_spec(Some(std::sync::Arc::clone(spec)))
        .build()
        .expect("a partitioned scan task")
}

fn identity_values_of(
    task: &iceberg::scan::FileScanTask,
) -> std::collections::BTreeMap<String, Option<String>> {
    identity_partition_values(task, &identity_partitioned_logical_fields(), "db.t")
        .expect("a readable data file")
}

/// Scenario: The VS carries each data file's identity partition values from the file's own partition spec
#[test]
fn identity_values_come_from_each_files_own_spec() {
    use iceberg::spec::{Literal, Transform};
    let identity_spec = identity_partition_spec(
        0,
        &[
            ("region", "region", Transform::Identity),
            ("id", "id_bucket", Transform::Bucket(16)),
            ("address.city", "city", Transform::Identity),
        ],
    );
    let evolved_spec = identity_partition_spec(
        1,
        &[
            ("region", "region_trunc", Transform::Truncate(2)),
            ("id", "id_bucket", Transform::Bucket(16)),
        ],
    );

    let imported = partitioned_scan_task(
        &identity_spec,
        vec![
            Some(Literal::string("eu")),
            Some(Literal::int(3)),
            Some(Literal::string("paris")),
        ],
    );
    let null_region =
        partitioned_scan_task(&identity_spec, vec![None, Some(Literal::int(5)), None]);
    let evolved = partitioned_scan_task(
        &evolved_spec,
        vec![Some(Literal::string("eu")), Some(Literal::int(7))],
    );

    assert_eq!(
        identity_values_of(&imported),
        std::collections::BTreeMap::from([("region".to_string(), Some("eu".to_string()))]),
        "only the identity field on a top-level logical column carries a value: the bucket \
         field does not reproduce `id`, and `city` is nested"
    );
    assert_eq!(
        identity_values_of(&null_region),
        std::collections::BTreeMap::from([("region".to_string(), None)]),
        "a null in the partition struct is an explicit absent value"
    );
    assert!(
        identity_values_of(&evolved).is_empty(),
        "a file of the evolved spec carries no identity field on `region`, so it carries no \
         value, although the other spec has one"
    );
}

/// The value a Parquet column holding `file_value` reads as at `declared`, through the cast the
/// scan's expression adapter inserts.
fn read_at_declared_type(
    file_value: &arrow::array::ArrayRef,
    declared: &arrow::datatypes::DataType,
) -> datafusion::scalar::ScalarValue {
    let read = datafusion::logical_expr::ColumnarValue::Array(std::sync::Arc::clone(file_value))
        .cast_to(declared, None)
        .and_then(|value| value.into_array(1))
        .expect("the file type reads at the declared type");
    datafusion::scalar::ScalarValue::try_from_array(&read, 0).expect("one value")
}

/// Scenario: Every admitted identity value survives the scan-spec round trip at the column's declared type
#[test]
fn identity_value_round_trips_at_declared_type_for_every_admitted_type() {
    use arrow::array::{
        ArrayRef, Decimal128Array, Float32Array, Int32Array, Time64MicrosecondArray,
        TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use iceberg::spec::{Literal, PrimitiveLiteral as L, PrimitiveType as T, Transform};
    use std::sync::Arc;

    let micros = 1_709_296_496_000_001_i64;
    let nanos = 1_709_296_496_000_000_123_i64;
    // The E2E types fixture covers every type Spark writes; these are the ones it cannot.
    let cases: Vec<(L, T, &str, ArrayRef)> = vec![
        (
            L::Long(45_296_000_001),
            T::Time,
            "utf8",
            Arc::new(Time64MicrosecondArray::from(vec![45_296_000_001])),
        ),
        (
            L::Long(micros),
            T::Timestamp,
            "timestamp_us",
            Arc::new(TimestampMicrosecondArray::from(vec![micros])),
        ),
        (
            L::Long(nanos),
            T::TimestampNs,
            "timestamp_ns",
            Arc::new(TimestampNanosecondArray::from(vec![nanos])),
        ),
        (
            L::Long(micros),
            T::Timestamptz,
            "timestamptz_us",
            Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC")),
        ),
        (
            L::Long(nanos),
            T::TimestamptzNs,
            "timestamptz_ns",
            Arc::new(TimestampNanosecondArray::from(vec![nanos]).with_timezone("UTC")),
        ),
        (
            L::Int(-7),
            T::Long,
            "int64",
            Arc::new(Int32Array::from(vec![-7])),
        ),
        (
            L::Float(1.1_f32.into()),
            T::Double,
            "float64",
            Arc::new(Float32Array::from(vec![1.1_f32])),
        ),
        (
            L::Int128(1234),
            T::Decimal {
                precision: 18,
                scale: 2,
            },
            "decimal128(18,2)",
            Arc::new(
                Decimal128Array::from(vec![1234])
                    .with_precision_and_scale(9, 2)
                    .expect("a valid decimal type"),
            ),
        ),
    ];

    for (value, source_type, tag, file_value) in cases {
        let declared = crate::types::mapping::arrow_type_from_tag(tag);
        let rendered = render_identity_value(&Literal::Primitive(value.clone()), &source_type, tag)
            .unwrap_or_else(|e| panic!("{source_type} {value:?} must render at {tag}: {e}"));
        let converted = crate::scan::partition_scalar(&rendered, &declared)
            .unwrap_or_else(|e| panic!("'{rendered}' must convert back to {tag}: {e}"));

        assert_eq!(
            converted,
            read_at_declared_type(&file_value, &declared),
            "a {source_type} value {value:?} rendered as '{rendered}' must read as a physical \
             {} column does at {tag}",
            file_value.data_type()
        );
    }

    let empty = render_identity_value(&Literal::string(""), &T::String, "utf8")
        .expect("an empty string renders");
    assert_eq!(
        crate::scan::non_null_partition_value(Some(&empty)),
        None,
        "an empty string reads as NULL, as Exasol stores an empty VARCHAR"
    );

    let refused = partitioned_scan_task(
        &identity_partition_spec(0, &[("token", "token", Transform::Identity)]),
        vec![Some(Literal::Primitive(L::UInt128(7)))],
    );
    assert!(
        identity_values_of(&refused).is_empty(),
        "a uuid column is refused at plan time, so no logical field takes its value"
    );
}

#[test]
fn an_unreadable_identity_value_fails_planning_naming_the_column() {
    use iceberg::spec::{Literal, Transform};
    let spec = identity_partition_spec(0, &[("id", "id", Transform::Identity)]);
    let narrowed = LogicalField {
        arrow_type: "int32".to_string(),
        ..identity_partitioned_logical_fields()[0].clone()
    };
    let cases = [
        (Literal::long(i64::MAX), narrowed, "Int32"),
        (
            Literal::string("x"),
            identity_partitioned_logical_fields()[0].clone(),
            "long",
        ),
        (
            Literal::Struct(iceberg::spec::Struct::from_iter([Some(Literal::long(1))])),
            identity_partitioned_logical_fields()[0].clone(),
            "non-primitive",
        ),
    ];

    for (literal, field, cause) in cases {
        let task = partitioned_scan_task(&spec, vec![Some(literal)]);
        let message = user_message(
            identity_partition_values(&task, &[field], "db.t")
                .expect_err("an unreadable value must never be dropped or truncated"),
        );
        assert!(
            message.contains("db.t") && message.contains("'id'") && message.contains(cause),
            "{message}"
        );
    }
}

#[test]
fn resolves_name_mapping_entries_at_every_depth() {
    let raw = r#"
    [
        { "field-id": 1, "names": ["id", "record_id"] },
        {
            "field-id": 3,
            "names": ["location"],
            "fields": [
                { "field-id": 4, "names": ["latitude", "lat"] },
                { "names": ["schema_only_child"], "fields": [{ "field-id": 9, "names": ["x"] }] }
            ]
        },
        { "names": ["schema_only_no_field_id"] }
    ]
    "#;

    let entries = parse_name_mapping(Some(raw)).expect("valid name-mapping JSON must parse");

    let names = |names: &[&str]| names.iter().map(ToString::to_string).collect();
    assert_eq!(
        entries,
        vec![
            NameMappingEntry {
                names: names(&["id", "record_id"]),
                field_id: 1,
                fields: Vec::new(),
            },
            NameMappingEntry {
                names: names(&["location"]),
                field_id: 3,
                fields: vec![NameMappingEntry {
                    names: names(&["latitude", "lat"]),
                    field_id: 4,
                    fields: Vec::new(),
                }],
            },
        ],
        "entries keep the mapping's shape at every depth; an id-less entry drops with its children"
    );
}

#[test]
fn absent_name_mapping_is_empty() {
    assert_eq!(
        parse_name_mapping(None).expect("absent property must not error"),
        Vec::new()
    );
}

#[test]
fn malformed_name_mapping_errors_cleanly() {
    let err = parse_name_mapping(Some("{ not valid json mapping shape"))
        .expect_err("malformed name-mapping JSON must error");

    let msg = user_message(err);
    assert!(
        msg.contains(iceberg::spec::TableProperties::PROPERTY_DEFAULT_NAME_MAPPING),
        "error must name the offending property: {msg}"
    );
    assert!(
        !msg.contains("access_key") && !msg.contains("secret_key"),
        "error must not leak credentials: {msg}"
    );
}

/// `location` present but empty: an omitted key fails deserialization before the guard.
async fn resolve_against_locationless_catalog(
    creds: &ConnectionCreds,
) -> Result<ResolvedScan, UdfError> {
    let body = load_table_body(serde_json::json!({"location": ""})).to_string();
    resolve_rest(body, "db.t", creds, &sample_storage(), None).await
}

#[tokio::test]
async fn absent_table_location_errors_on_both_vended_and_static_paths() {
    let static_creds = sigv4_creds();
    let vended_creds = ConnectionCreds {
        use_vended_credentials: true,
        ..sigv4_creds()
    };

    let vended_err = resolve_against_locationless_catalog(&vended_creds)
        .await
        .expect_err("vended path must reject a loadTable response with an empty location");
    let static_err = resolve_against_locationless_catalog(&static_creds)
        .await
        .expect_err("static path must reject a loadTable response with an empty location");

    let vended_message = user_message(vended_err);
    let static_message = user_message(static_err);

    for (path, message) in [("vended", &vended_message), ("static", &static_message)] {
        assert!(
            message.contains("loadTable"),
            "{path}-path error must name the response the location is absent from: {message}"
        );
        assert!(
            message.contains("location"),
            "{path}-path error must name the absent location field: {message}"
        );
        assert!(
            message.contains("db.t"),
            "{path}-path error must name the table whose loadTable response was malformed: \
             {message}"
        );
        assert!(
            !message.contains("storage backend cannot be resolved"),
            "{path}-path error must not frame the failure as a vended-storage-backend \
             resolution failure: {message}"
        );
    }
    assert_eq!(
        vended_message, static_message,
        "both paths must surface the SAME absent-location error — the vended-credential \
         flag must not change how a malformed catalog response is diagnosed"
    );
}

/// All values distinct, so which source placed the resolved store is readable off the result.
const CONNECTION_ENDPOINT: &str = "https://connection-store.example.com";
const CONNECTION_REGION: &str = "eu-central-1";
const VENDED_ENDPOINT: &str = "https://vended-store.example.com";
const VENDED_REGION: &str = "us-west-2";
const VENDED_ACCESS_KEY: &str = "vended-access-key";
const VENDED_SECRET_KEY: &str = "vended-secret-key";
const VENDED_SESSION_TOKEN: &str = "vended-session-token";

#[tokio::test]
async fn vended_addressing_prefers_the_connection_endpoint_and_region() {
    let creds = ConnectionCreds {
        use_vended_credentials: true,
        endpoint: CONNECTION_ENDPOINT.into(),
        region: CONNECTION_REGION.into(),
        ..sigv4_creds()
    };
    let mut body = load_table_body(serde_json::json!({}));
    body["config"] = serde_json::json!({
        "s3.access-key-id": VENDED_ACCESS_KEY,
        "s3.secret-access-key": VENDED_SECRET_KEY,
        "s3.session-token": VENDED_SESSION_TOKEN,
        "client.region": VENDED_REGION,
        "s3.endpoint": VENDED_ENDPOINT
    });

    let resolved = resolve_rest(body.to_string(), "db.t", &creds, &sample_storage(), None)
        .await
        .expect("a vended key pair over a snapshotless s3:// table must resolve a backend");

    let StorageBackend::S3(props) = resolved.effective_storage else {
        panic!("an s3:// table location must resolve an S3 backend");
    };

    assert_eq!(
        props.endpoint, CONNECTION_ENDPOINT,
        "the CONNECTION's endpoint must place the store, not the vended {VENDED_ENDPOINT}"
    );
    assert_eq!(
        props.region, CONNECTION_REGION,
        "the CONNECTION's region must place the store, not the vended {VENDED_REGION}"
    );
    assert_eq!(
        props.access_key, VENDED_ACCESS_KEY,
        "the access key must come from the catalog alone: the CONNECTION reaches this \
         resolution as an ADDRESS, and must never supply a storage credential"
    );
    assert_eq!(
        props.secret_key, VENDED_SECRET_KEY,
        "the secret key must come from the catalog alone: the CONNECTION reaches this \
         resolution as an ADDRESS, and must never supply a storage credential"
    );
    assert_eq!(
        props.session_token.as_deref(),
        Some(VENDED_SESSION_TOKEN),
        "the vended session token must reach the effective storage the scan reads with"
    );
}

fn metadata_with_schema_history(
    schemas: Json,
    current_schema_id: i32,
) -> iceberg::spec::TableMetadata {
    let body = load_table_body(serde_json::json!({
        "format-version": 3,
        "next-row-id": 0,
        "last-column-id": 9,
        "current-schema-id": current_schema_id,
        "schemas": schemas,
    }));
    serde_json::from_value(body["metadata"].clone())
        .expect("synthetic Iceberg table metadata must deserialize")
}

#[test]
fn date_to_timestamp_promotion_is_refused_naming_table_column_both_types_and_the_issue() {
    let metadata = metadata_with_schema_history(
        serde_json::json!([
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "event_day", "required": false, "type": "date"}
            ]},
            {"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "event_day", "required": false, "type": "timestamp"}
            ]}
        ]),
        1,
    );

    let err = refuse_date_promotion(&metadata, "db.promoted")
        .expect_err("a recorded date -> timestamp promotion must be refused");

    let msg = user_message(err);
    assert!(
        msg.contains("db.promoted"),
        "error must name the table: {msg}"
    );
    assert!(
        msg.contains("event_day"),
        "error must name the column: {msg}"
    );
    assert!(
        msg.contains("'date'"),
        "error must name the earlier Iceberg type: {msg}"
    );
    assert!(
        msg.contains("'timestamp'"),
        "error must name the current Iceberg type: {msg}"
    );
    assert!(
        msg.contains("#355"),
        "error must cite the tracked issue: {msg}"
    );
    assert!(
        !msg.contains("access_key") && !msg.contains("secret_key"),
        "error must not leak credentials: {msg}"
    );
}

#[test]
fn date_to_timestamp_ns_promotion_is_refused() {
    let metadata = metadata_with_schema_history(
        serde_json::json!([
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "event_day", "required": false, "type": "date"}
            ]},
            {"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "event_day", "required": false, "type": "timestamp_ns"}
            ]}
        ]),
        1,
    );

    let err = refuse_date_promotion(&metadata, "db.promoted_ns")
        .expect_err("a recorded date -> timestamp_ns promotion must be refused");

    let msg = user_message(err);
    assert!(
        msg.contains("'timestamp_ns'"),
        "error must name the current Iceberg type: {msg}"
    );
    assert!(
        msg.contains("'date'") && msg.contains("#355"),
        "error must name the earlier type and cite the issue: {msg}"
    );
}

#[test]
fn a_date_column_that_was_never_promoted_is_not_refused() {
    let metadata = metadata_with_schema_history(
        serde_json::json!([
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "event_day", "required": false, "type": "date"},
                {"id": 2, "name": "seen_at", "required": false, "type": "timestamp"}
            ]},
            {"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "event_day", "required": false, "type": "date"},
                {"id": 2, "name": "seen_at", "required": false, "type": "timestamp"},
                {"id": 3, "name": "label", "required": false, "type": "string"}
            ]}
        ]),
        1,
    );

    refuse_date_promotion(&metadata, "db.unpromoted")
        .expect("a date column that was never promoted must plan normally");
}

#[test]
fn a_date_promotion_nested_inside_a_struct_is_refused_by_its_full_name() {
    let metadata = metadata_with_schema_history(
        serde_json::json!([
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "payload", "required": false, "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 2, "name": "stamped", "required": false, "type": "date"}
                    ]
                }}
            ]},
            {"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "payload", "required": false, "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 2, "name": "stamped", "required": false, "type": "timestamp"}
                    ]
                }}
            ]}
        ]),
        1,
    );

    let err = refuse_date_promotion(&metadata, "db.nested")
        .expect_err("a date promotion on a nested field must be refused");

    let msg = user_message(err);
    assert!(
        msg.contains("payload.stamped"),
        "error must name the nested column by its full path: {msg}"
    );
}

#[test]
fn an_int_to_long_promotion_history_plans_normally() {
    let metadata = metadata_with_schema_history(
        serde_json::json!([
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "amount", "required": false, "type": "int"}
            ]},
            {"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "amount", "required": false, "type": "long"}
            ]}
        ]),
        1,
    );

    refuse_date_promotion(&metadata, "db.int_to_long")
        .expect("an int -> long promotion must plan normally");
}

#[test]
fn a_float_to_double_promotion_history_plans_normally() {
    let metadata = metadata_with_schema_history(
        serde_json::json!([
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "reading", "required": false, "type": "float"}
            ]},
            {"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "reading", "required": false, "type": "double"}
            ]}
        ]),
        1,
    );

    refuse_date_promotion(&metadata, "db.float_to_double")
        .expect("a float -> double promotion must plan normally");
}

#[test]
fn a_decimal_precision_widening_history_plans_normally() {
    let metadata = metadata_with_schema_history(
        serde_json::json!([
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "price", "required": false, "type": "decimal(10,2)"}
            ]},
            {"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "price", "required": false, "type": "decimal(20,2)"}
            ]}
        ]),
        1,
    );

    refuse_date_promotion(&metadata, "db.decimal_widening")
        .expect("a decimal precision widening must plan normally");
}

fn assert_promotion_refusal_names_table_column_and_issue(err: UdfError) {
    let msg = user_message(err);
    for fragment in ["db.promoted", "event_day", "#355"] {
        assert!(
            msg.contains(fragment),
            "error must name the table and column and cite the issue: {msg}"
        );
    }
}

/// No snapshot, so reaching the refusal proves `resolve_scan` invokes it without a manifest read.
#[tokio::test]
async fn a_promoted_date_table_is_refused_filtered_or_not_and_from_either_metadata_source() {
    let body = load_table_body(serde_json::json!({
        "format-version": 3,
        "next-row-id": 0,
        "last-column-id": 2,
        "current-schema-id": 1,
        "schemas": [
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "event_day", "required": false, "type": "date"}
            ]},
            {"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "event_day", "required": false, "type": "timestamp"}
            ]}
        ],
    }));
    let filter = serde_json::json!({"op": "eq", "column": "id", "value": 1});
    for filter_json in [None, Some(&filter)] {
        let err = resolve_rest(
            body.to_string(),
            "db.promoted",
            &sigv4_creds(),
            &sample_storage(),
            filter_json,
        )
        .await
        .expect_err("a recorded date -> timestamp promotion is refused, filtered or not");
        assert_promotion_refusal_names_table_column_and_issue(err);
    }

    let metadata_file = "db/promoted/metadata/v1.json";
    let storage = object_endpoint(
        "bucket",
        vec![(metadata_file.into(), body["metadata"].to_string())],
    )
    .await;
    let promoted = resolve_metadata_file(
        &glue_iceberg_table("promoted", Some(&format!("s3://bucket/{metadata_file}"))),
        &TableNotes::default(),
        &storage,
    )
    .await
    .expect_err("the date-promotion refusal applies to a metadata-file table");
    assert_promotion_refusal_names_table_column_and_issue(promoted);
}

fn glue_iceberg_table(name: &str, metadata_location: Option<&str>) -> CatalogTable {
    CatalogTable {
        metadata_location: metadata_location.map(str::to_string),
        ..glue_catalog_table(
            "db",
            name,
            TableFormat::Iceberg,
            &format!("s3://bucket/glue-declared/{name}"),
            &[("glue_only", "binary")],
        )
    }
}

async fn resolve_metadata_file(
    table: &CatalogTable,
    notes: &TableNotes<'_>,
    storage: &StorageBackend,
) -> Result<ResolvedScan, UdfError> {
    let reader = IcebergFormatReader {
        metadata: IcebergMetadataSource::MetadataFile { table },
        connection: ConnectionStorage {
            storage,
            creds: &sigv4_creds(),
            allow_http: true,
        },
    };
    resolve_from_notes(&reader, notes, None).await
}

/// Scenario: A Glue Iceberg table is planned from its metadata location by the one Iceberg planner
#[tokio::test]
async fn a_glue_iceberg_table_is_planned_from_its_metadata_file() {
    let body = name_mapped_load_table_body();
    let storage = object_endpoint(
        "bucket",
        vec![("db/t/metadata/v1.json".into(), body["metadata"].to_string())],
    )
    .await;
    let from_rest = resolve_rest(body.to_string(), "db.t", &sigv4_creds(), &storage, None)
        .await
        .expect("the REST planner resolves the table");

    let notes = load_table_body_notes(&body);
    let from_file = resolve_metadata_file(
        &glue_iceberg_table("t", Some("s3://bucket/db/t/metadata/v1.json")),
        &involved_notes("T", &notes),
        &storage,
    )
    .await
    .expect("the metadata file plans the table");

    assert_eq!(
        from_file, from_rest,
        "the one Iceberg planner plans the same metadata alike from either source"
    );
    assert_eq!(from_file.table_root, "s3://bucket/db/t");
    assert!(
        from_file
            .logical_schema
            .iter()
            .all(|field| field.name != "glue_only")
            && from_file.refused_columns.is_empty(),
        "the metadata file, not Glue's column copy, is the schema authority"
    );
}

#[tokio::test]
async fn an_unreadable_or_unset_metadata_location_fails_naming_the_table() {
    let storage = object_endpoint("bucket", Vec::new()).await;

    let missing = user_message(
        resolve_metadata_file(
            &glue_iceberg_table("gone", Some("s3://bucket/db/gone/metadata/v9.json")),
            &TableNotes::default(),
            &storage,
        )
        .await
        .expect_err("an unreadable metadata file fails the plan"),
    );
    let unset = user_message(
        resolve_metadata_file(
            &glue_iceberg_table("unset", None),
            &TableNotes::default(),
            &storage,
        )
        .await
        .expect_err("a table without a metadata location fails the plan"),
    );

    assert!(
        missing.contains("db.gone") && missing.contains("s3://bucket/db/gone/metadata/v9.json"),
        "{missing}"
    );
    assert!(unset.contains("db.unset"), "{unset}");
    for message in [&missing, &unset] {
        assert!(!message.contains("minioadmin"), "{message}");
    }
}

#[tokio::test]
async fn a_readable_iceberg_promotion_plans_normally_and_carries_the_current_type() {
    let body = load_table_body(serde_json::json!({
        "last-column-id": 3,
        "current-schema-id": 1,
        "schemas": [
            {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "amount", "required": false, "type": "int"},
                {"id": 2, "name": "reading", "required": false, "type": "float"},
                {"id": 3, "name": "price", "required": false, "type": "decimal(10,2)"}
            ]},
            {"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "amount", "required": false, "type": "long"},
                {"id": 2, "name": "reading", "required": false, "type": "double"},
                {"id": 3, "name": "price", "required": false, "type": "decimal(20,2)"}
            ]}
        ],
    }));

    let resolved = resolve_rest(
        body.to_string(),
        "db.promoted_numerics",
        &sigv4_creds(),
        &sample_storage(),
        None,
    )
    .await
    .expect("int -> long, float -> double and decimal precision widening must all plan normally");

    let field = |field_id: i32, name: &str, arrow_type: &str| LogicalField {
        field_id: Some(field_id),
        name: name.to_string(),
        arrow_type: arrow_type.to_string(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    };
    assert_eq!(
        resolved.logical_schema,
        vec![
            field(1, "amount", "int64"),
            field(2, "reading", "float64"),
            field(3, "price", "decimal128(20,2)"),
        ],
        "each promoted column must carry its CURRENT type against its original field id"
    );
    assert!(
        resolved.refused_columns.is_empty(),
        "a promotion this engine reads refuses no column: {:?}",
        resolved.refused_columns
    );
}

async fn resolve_table_with_columns(
    fields: Json,
    last_column_id: i32,
) -> Result<ResolvedScan, UdfError> {
    let body = load_table_body_with_columns(fields, last_column_id);
    resolve_rest(body, "db.t", &sigv4_creds(), &sample_storage(), None).await
}

/// Scenario: A binary column is refused on every catalog-declared format at every depth
#[tokio::test]
async fn iceberg_binary_fixed_and_uuid_columns_are_refused_and_an_all_binary_table_as_a_whole() {
    let resolved =
        resolve_table_with_columns(binary_iceberg_fields(), BINARY_ICEBERG_LAST_COLUMN_ID)
            .await
            .expect("a table with a mappable column plans, refusing only its binary columns");

    assert_eq!(
        resolved
            .logical_schema
            .iter()
            .map(|field| (field.name.as_str(), field.field_id))
            .collect::<Vec<_>>(),
        vec![("id", Some(1)), ("name", Some(13))],
        "every refused column is dropped from the logical schema, and nothing else"
    );
    let refused = |column, member, declared| binary_refused("Iceberg", column, member, declared);
    assert_eq!(
        resolved.refused_columns,
        vec![
            refused("b", None, "binary"),
            refused("f", None, "fixed(16)"),
            refused("u", None, "uuid"),
            refused("s", Some("s.x"), "binary"),
            refused("l", Some("l.element"), "fixed(4)"),
            refused("m", Some("m.value"), "uuid"),
        ],
        "each binary column is refused by name, naming its declared type, its member path, and \
         issue #351"
    );

    let every_column_binary = serde_json::json!([
        {"id": 1, "name": "b", "required": false, "type": "binary"},
        {"id": 2, "name": "u", "required": false, "type": "uuid"}
    ]);
    let message = resolve_table_with_columns(every_column_binary, 2)
        .await
        .expect_err("a table with no mappable column cannot be scanned")
        .to_string();
    assert!(
        message.contains("virtual table 'T' has no mappable column"),
        "{message}"
    );
}

#[test]
fn nested_iceberg_fields_stay_utf8_tagged_and_carry_a_nested_descriptor() {
    use iceberg::spec::{
        ListType, MapType, NestedField as IcebergField, PrimitiveType, Schema, StructType, Type,
    };

    let struct_ty = Type::Struct(StructType::new(vec![]));
    let list_ty = Type::List(ListType::new(
        IcebergField::list_element(100, Type::Primitive(PrimitiveType::String), true).into(),
    ));
    let map_ty = Type::Map(MapType::new(
        IcebergField::map_key_element(101, Type::Primitive(PrimitiveType::Int)).into(),
        IcebergField::map_value_element(102, Type::Primitive(PrimitiveType::String), true).into(),
    ));
    for ty in [&struct_ty, &list_ty, &map_ty] {
        assert_eq!(
            crate::types::mapping::iceberg_type_to_arrow(ty),
            arrow::datatypes::DataType::Utf8,
            "{ty:?} must stay Utf8-tagged"
        );
    }

    let address = Type::Struct(StructType::new(vec![
        IcebergField::required(3, "street", Type::Primitive(PrimitiveType::String)).into(),
        IcebergField::required(4, "city", Type::Primitive(PrimitiveType::String)).into(),
    ]));
    let inner_struct = Type::Struct(StructType::new(vec![
        IcebergField::required(10, "a", Type::Primitive(PrimitiveType::String)).into(),
    ]));
    let items = Type::List(ListType::new(
        IcebergField::list_element(9, inner_struct, true).into(),
    ));
    let tags = Type::Map(MapType::new(
        IcebergField::map_key_element(6, Type::Primitive(PrimitiveType::Int)).into(),
        IcebergField::map_value_element(7, Type::Primitive(PrimitiveType::String), true).into(),
    ));

    let schema = Schema::builder()
        .with_fields(vec![
            IcebergField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            IcebergField::required(2, "address", address).into(),
            IcebergField::required(5, "tags", tags).into(),
            IcebergField::required(8, "items", items).into(),
        ])
        .build()
        .expect("well-formed nested schema builds");

    let logical = declared_logical_schema(&schema);

    assert_eq!(logical[0].arrow_type, "int32");
    assert_eq!(
        logical[0].nested, None,
        "a primitive field carries no nested descriptor"
    );

    assert_eq!(logical[1].arrow_type, "utf8", "struct stays Utf8-tagged");
    assert_eq!(
        logical[1].nested,
        Some(NestedMembers::Struct {
            fields: vec![
                NestedField {
                    field_id: Some(3),
                    name: "street".to_string(),
                    physical_name: None,
                    nested: None,
                },
                NestedField {
                    field_id: Some(4),
                    name: "city".to_string(),
                    physical_name: None,
                    nested: None,
                },
            ],
        }),
        "each struct field carries its real Iceberg field-id and logical name"
    );

    assert_eq!(logical[2].arrow_type, "utf8", "map stays Utf8-tagged");
    assert_eq!(
        logical[2].nested,
        Some(NestedMembers::Map {
            key: None,
            value: None
        }),
        "a map of primitives carries no key/value descriptor"
    );

    assert_eq!(logical[3].arrow_type, "utf8", "list stays Utf8-tagged");
    assert_eq!(
        logical[3].nested,
        Some(NestedMembers::List {
            element: Some(Box::new(NestedMembers::Struct {
                fields: vec![NestedField {
                    field_id: Some(10),
                    name: "a".to_string(),
                    physical_name: None,
                    nested: None,
                }],
            })),
        }),
        "a list of structs recurses into the element's own struct descriptor"
    );
}

fn long_schema(fields: &[(i32, &str, bool)]) -> iceberg::spec::Schema {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    iceberg::spec::Schema::builder()
        .with_fields(fields.iter().map(|&(id, name, required)| {
            std::sync::Arc::new(NestedField::new(
                id,
                name,
                Type::Primitive(PrimitiveType::Long),
                required,
            ))
        }))
        .build()
        .expect("a valid schema")
}

#[test]
fn relaxed_nullability_is_matched_by_declared_field_id() {
    let declared = DeclaredSchema {
        table: "ORDERS".into(),
        logical_schema: declared_logical_schema(&long_schema(&[
            (2, "qty", true),
            (3, "note", false),
        ])),
        partition_columns: vec![],
        refused_columns: vec![],
    };

    let tightened_and_renamed =
        long_schema(&[(2, "amount", true), (3, "note", true), (4, "qty", false)]);
    refuse_relaxed_nullability(&tightened_and_renamed, &declared).expect(
        "field id 2 is still required, the optional `qty` is field id 4, and a tightened \
         column loses no row",
    );

    let relaxed = long_schema(&[(2, "amount", false), (3, "note", true)]);
    let message = user_message(
        refuse_relaxed_nullability(&relaxed, &declared)
            .expect_err("a stale non-nullable declaration would drop NULL rows silently"),
    );
    for fragment in ["'QTY'", "'ORDERS'", "ALTER VIRTUAL SCHEMA ... REFRESH"] {
        assert!(message.contains(fragment), "{message}");
    }
}

#[test]
fn iceberg_v3_types_without_an_exasol_rendering_are_refused_at_declaration() {
    use iceberg::spec::{
        GeographyType, GeometryType, ListType, NestedField, PrimitiveType, Type, VariantType,
    };

    let refusal = |field: NestedField| declaration(&field).refused.expect("must be refused");
    let primitive = |pt| NestedField::optional(1, "c", Type::Primitive(pt));

    assert_eq!(
        refusal(NestedField::optional(1, "v", Type::Variant(VariantType))),
        format!("Iceberg column 'v' {}", variant_cause())
    );
    assert_eq!(
        refusal(primitive(PrimitiveType::Geometry(
            GeometryType::new(None).unwrap()
        ))),
        format!("Iceberg column 'c' {}", binary_cause("geometry"))
    );
    assert_eq!(
        refusal(primitive(
            PrimitiveType::Geography(GeographyType::default())
        )),
        format!("Iceberg column 'c' {}", binary_cause("geography"))
    );
    assert!(
        refusal(primitive(PrimitiveType::Unknown))
            .starts_with("Iceberg column 'c' has type 'unknown'")
    );

    let list_of_variant = NestedField::optional(
        1,
        "l",
        Type::List(ListType::new(
            NestedField::list_element(2, Type::Variant(VariantType), false).into(),
        )),
    );
    assert!(
        refusal(list_of_variant)
            .starts_with("Iceberg column 'l', whose member 'l.element' has type 'variant'")
    );
}

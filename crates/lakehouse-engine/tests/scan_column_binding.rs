//! Column-binding order in `bind_columns`: embedded `PARQUET:field_id` -> declared
//! `physical_name` -> `name_mapping` -> identity.

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::ExaType;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, NameMappingEntry, NestedField, NestedMembers,
    ProjectionItem, ScanSpec, ScanStorage, StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{
    ResolvedScanStorage, run_raw_scan_with_session, session_config_for_spec,
};
use object_store::local::LocalFileSystem;
use parquet::arrow::ArrowWriter;

mod scan_fixture;

fn dummy_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://localhost:9000".into(),
        region: "us-east-1".into(),
        access_key: "k".into(),
        secret_key: "s".into(),
        allow_http: true,
        ..Default::default()
    })
}

/// No column carries `PARQUET:field_id`. The first column holds `0..rows`, the rest `10 * id`.
fn write_local_parquet(
    dir: &std::path::Path,
    relative: &str,
    columns: &[(&str, bool)],
    rows: i64,
) -> String {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, nullable)| Field::new(*name, DataType::Int64, *nullable))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let ids: Vec<i64> = (0..rows).collect();
    let arrays: Vec<Arc<dyn Array>> = columns
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let values: Vec<i64> = if index == 0 {
                ids.clone()
            } else {
                ids.iter().map(|i| i * 10).collect()
            };
            Arc::new(Int64Array::from(values)) as _
        })
        .collect();
    let batch = RecordBatch::try_new(schema, arrays).expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn raw_scan_spec(
    file_url: String,
    file_size: u64,
    projection: Vec<&str>,
    logical_schema: Vec<LogicalField>,
    name_mapping: Vec<NameMappingEntry>,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            projection: projection
                .into_iter()
                .map(|name| ProjectionItem::Column(name.to_string()))
                .collect(),
            logical_schema,
            name_mapping,
            storage: ScanStorage::Inline(dummy_storage()),
            df_batch_size: 64,
            ..Default::default()
        },
        files: vec![FileEntry::new(file_url, file_size)],
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

async fn run_scan(
    spec: &ScanSpec,
    register_url: &str,
    emits: &[ExaType],
) -> Result<Vec<RecordBatch>, UdfError> {
    let session = datafusion::execution::context::SessionContext::new_with_config(
        session_config_for_spec(spec),
    );
    session.runtime_env().register_object_store(
        &url::Url::parse(register_url).expect("register url"),
        Arc::new(LocalFileSystem::new()),
    );
    let mut ctx = scan_fixture::BatchCapturingCtx::declaring(TestContext::scalar(vec![]), emits);
    let mut timers = PhaseTimers::start();
    let storage = ResolvedScanStorage::from_backends(dummy_storage(), None);
    run_raw_scan_with_session(&mut ctx, &session, spec, &storage, &mut timers).await?;
    Ok(ctx.into_batches())
}

fn int64_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column(batch.schema().index_of(name).expect("column present"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column")
}

/// Scenario: a declared `physical_name` binds its column and wins over a `name_mapping` entry for another field
#[test]
fn declared_physical_name_binds_the_renamed_physical_column() {
    let dir = std::env::temp_dir().join(format!(
        "lh_column_binding_physical_name_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let rows = 20;
    let file_url = write_local_parquet(
        &dir,
        "physical_name.parquet",
        &[("id", false), ("col-abc", true)],
        rows,
    );
    let file_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    let logical_schema = vec![
        LogicalField {
            field_id: None,
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: None,
            name: "amount".to_string(),
            arrow_type: "int64".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: Some("col-abc".to_string()),
        },
        LogicalField {
            field_id: Some(7),
            name: "other".to_string(),
            arrow_type: "int64".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ];
    let name_mapping = vec![NameMappingEntry {
        name: "col-abc".to_string(),
        field_id: 7,
    }];
    let spec = raw_scan_spec(
        file_url.clone(),
        file_size,
        vec!["ID", "AMOUNT", "OTHER"],
        logical_schema,
        name_mapping,
    );

    let batches = block_on(run_scan(&spec, &file_url, &vec![ExaType::Int64; 3]))
        .expect("raw scan must succeed");
    let mut row_count = 0;
    for batch in &batches {
        let id_values = int64_column(batch, "ID");
        let amounts = int64_column(batch, "AMOUNT");
        let others = int64_column(batch, "OTHER");
        for i in 0..batch.num_rows() {
            let id = id_values.value(i);
            assert!(
                !amounts.is_null(i),
                "row {id}: AMOUNT must bind to col-abc via the declared physical_name, never NULL"
            );
            assert_eq!(
                amounts.value(i),
                id * 10,
                "row {id}: AMOUNT must carry the real physical value"
            );
            assert!(
                others.is_null(i),
                "row {id}: OTHER must stay unbound — name_mapping must NOT also claim col-abc"
            );
        }
        row_count += id_values.len();
    }
    assert_eq!(row_count, rows as usize, "row count");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: identity-bound fields bind by name and keep NULL-fill, default, and required-error semantics
#[test]
fn identity_bound_fields_bind_by_name_and_keep_the_default_fill_semantics() {
    let dir =
        std::env::temp_dir().join(format!("lh_column_binding_identity_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let rows = 15;
    let file_url = write_local_parquet(&dir, "identity.parquet", &[("id", false)], rows);
    let file_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    let id_field = || LogicalField {
        field_id: None,
        name: "id".to_string(),
        arrow_type: "int64".to_string(),
        nullable: false,
        initial_default: None,
        nested: None,
        physical_name: None,
    };

    let logical_schema = vec![
        id_field(),
        LogicalField {
            field_id: None,
            name: "extra_nullable".to_string(),
            arrow_type: "int64".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: None,
            name: "extra_default".to_string(),
            arrow_type: "int64".to_string(),
            nullable: true,
            initial_default: Some("42".to_string()),
            nested: None,
            physical_name: None,
        },
    ];
    let spec = raw_scan_spec(
        file_url.clone(),
        file_size,
        vec!["ID", "EXTRA_NULLABLE", "EXTRA_DEFAULT"],
        logical_schema,
        vec![],
    );
    let batches = block_on(run_scan(&spec, &file_url, &vec![ExaType::Int64; 3]))
        .expect("raw scan must succeed");
    let mut row_count = 0;
    for batch in &batches {
        let ids = int64_column(batch, "ID");
        let nullable = int64_column(batch, "EXTRA_NULLABLE");
        let defaulted = int64_column(batch, "EXTRA_DEFAULT");
        for i in 0..batch.num_rows() {
            let id = ids.value(i);
            assert!(
                nullable.is_null(i),
                "row {id}: an absent nullable identity-bound field with no default must NULL-fill"
            );
            assert!(
                !defaulted.is_null(i),
                "row {id}: an absent identity-bound field with an initial_default must not NULL-fill"
            );
            assert_eq!(
                defaulted.value(i),
                42,
                "row {id}: an absent identity-bound field must substitute its initial_default"
            );
        }
        row_count += ids.len();
    }
    assert_eq!(row_count, rows as usize, "row count");

    let required_missing_schema = vec![
        id_field(),
        LogicalField {
            field_id: None,
            name: "mandatory".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ];
    let error_spec = raw_scan_spec(
        file_url.clone(),
        file_size,
        vec!["ID", "MANDATORY"],
        required_missing_schema,
        vec![],
    );
    let err = block_on(run_scan(&error_spec, &file_url, &[]))
        .expect_err("an absent required identity-bound field with no default must error");
    let text = err.to_string();
    assert!(
        text.contains("mandatory"),
        "error must name the missing required column: {text}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: nested list/struct columns render as JSON while a primitive column in the same scan passes through
#[test]
fn mixed_column_parquet_file_emits_json_for_populated_list_and_struct() {
    use arrow::array::{ArrayRef, ListBuilder, StringArray, StringBuilder, StructArray};
    use arrow::datatypes::Fields;

    let dir = std::env::temp_dir().join(format!("lh_column_binding_mixed_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    tags_builder.values().append_value("a");
    tags_builder.values().append_value("b");
    tags_builder.append(true);
    let tags = tags_builder.finish();

    let addr = StructArray::try_new(
        Fields::from(vec![
            Arc::new(Field::new("street", DataType::Utf8, true)),
            Arc::new(Field::new("city", DataType::Utf8, true)),
        ]),
        vec![
            Arc::new(StringArray::from(vec![Some("Main St")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("Berlin")])) as ArrayRef,
        ],
        None,
    )
    .expect("struct array");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
        Field::new("tags", tags.data_type().clone(), true),
        Field::new("addr", addr.data_type().clone(), true),
    ]));
    let path = dir.join("mixed.parquet");
    {
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0i64])),
                Arc::new(Int64Array::from(vec![250i64])),
                Arc::new(tags),
                Arc::new(addr),
            ],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    let file_url = url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string();
    let file_size = std::fs::metadata(&path).expect("stat parquet").len();

    let identity_field =
        |name: &str, arrow_type: &str, nested: Option<NestedMembers>| LogicalField {
            field_id: None,
            name: name.to_string(),
            arrow_type: arrow_type.to_string(),
            nullable: true,
            initial_default: None,
            nested,
            physical_name: None,
        };
    let logical_schema = vec![
        LogicalField {
            nullable: false,
            ..identity_field("id", "int64", None)
        },
        identity_field("amount", "int64", None),
        identity_field("tags", "utf8", Some(NestedMembers::List { element: None })),
        identity_field(
            "addr",
            "utf8",
            Some(NestedMembers::Struct {
                fields: vec![
                    NestedField {
                        field_id: None,
                        name: "street".to_string(),
                        physical_name: None,
                        nested: None,
                    },
                    NestedField {
                        field_id: None,
                        name: "city".to_string(),
                        physical_name: None,
                        nested: None,
                    },
                ],
            }),
        ),
    ];
    let spec = raw_scan_spec(
        file_url.clone(),
        file_size,
        vec!["ID", "AMOUNT", "TAGS", "ADDR"],
        logical_schema,
        vec![],
    );

    let batches = block_on(run_scan(
        &spec,
        &file_url,
        &[
            ExaType::Int64,
            ExaType::Int64,
            scan_fixture::varchar(),
            scan_fixture::varchar(),
        ],
    ))
    .expect("mixed-column scan must succeed");
    assert_eq!(
        1,
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        "row count"
    );
    let batch = &batches[0];

    let amounts = int64_column(batch, "AMOUNT");
    assert_eq!(
        amounts.value(0),
        250,
        "the ordinary primitive column must pass through the scan unaffected"
    );

    let tags_index = batch.schema().index_of("TAGS").expect("TAGS present");
    let rendered_tags = batch
        .column(tags_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("TAGS must render as Utf8 JSON");
    let parsed_tags: serde_json::Value =
        serde_json::from_str(rendered_tags.value(0)).expect("TAGS must be valid JSON");
    assert_eq!(parsed_tags, serde_json::json!(["a", "b"]));

    let addr_index = batch.schema().index_of("ADDR").expect("ADDR present");
    let rendered_addr = batch
        .column(addr_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("ADDR must render as Utf8 JSON");
    let parsed_addr: serde_json::Value =
        serde_json::from_str(rendered_addr.value(0)).expect("ADDR must be valid JSON");
    assert_eq!(
        parsed_addr,
        serde_json::json!({"street": "Main St", "city": "Berlin"})
    );

    let _ = std::fs::remove_dir_all(&dir);
}

use super::*;
use std::collections::HashMap;

/// Built field-by-field since these tests need no storage backend; the Parquet format still
/// comes from the production decision.
fn scan_table_over(files: Vec<FileEntry>) -> ParquetScanTable {
    let field_id_resolution = FieldIdResolution {
        name_mapping: Vec::new(),
        declared_physical_names: HashMap::new(),
        defaults: HashMap::new(),
        nested_members: HashMap::new(),
    };
    ParquetScanTable {
        object_store_url: ObjectStoreUrl::parse("memory://").unwrap(),
        schema: PartitionedScanSchema::split(Arc::new(arrow::datatypes::Schema::empty()), &[])
            .expect("an empty schema declares no partition column"),
        use_field_id_adapter: false,
        format: Arc::new(scan_table_parquet_format(&field_id_resolution)),
        field_id_resolution,
        files,
        table_root: "memory:///db/t".to_string(),
        secrets: Vec::new(),
        delete_path_read_limiter: Arc::new(Semaphore::new(1)),
    }
}

fn scan_table_partitioned_by(
    files: Vec<FileEntry>,
    declared: SchemaRef,
    partition_columns: &[String],
) -> ParquetScanTable {
    ParquetScanTable {
        schema: PartitionedScanSchema::split(declared, partition_columns)
            .expect("every partition column is declared"),
        ..scan_table_over(files)
    }
}

fn partitioned_declared_schema() -> SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("part", DataType::Int32, true),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

fn entry_in_partition(path: &str, part: Option<&str>) -> FileEntry {
    FileEntry::with_partition_values(
        path,
        1,
        std::collections::BTreeMap::from([("part".to_string(), part.map(str::to_string))]),
    )
}

fn memory_session() -> datafusion::execution::session_state::SessionState {
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new();
    ctx.register_object_store(
        &url::Url::parse("memory://").unwrap(),
        Arc::new(object_store::memory::InMemory::new()),
    );
    ctx.state()
}

#[test]
fn the_provider_reports_the_declared_schema_while_scanning_the_split_one() {
    let declared = partitioned_declared_schema();
    let table = scan_table_partitioned_by(
        vec![entry_in_partition("data/a.parquet", Some("1"))],
        Arc::clone(&declared),
        &["part".to_string()],
    );

    assert_eq!(TableProvider::schema(&table).as_ref(), declared.as_ref());

    let scan_schema = table.schema.file_source_schema();
    let file_names: Vec<&str> = scan_schema
        .file_schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        file_names,
        ["id", "payload"],
        "the partition column is not read from any data file"
    );
    assert_eq!(
        table.schema.remap_projection(None),
        Some(vec![0, 2, 1]),
        "declared order is restored by the projection alone"
    );
}

#[test]
fn each_partitioned_file_carries_its_own_logged_partition_values() {
    let table = scan_table_partitioned_by(
        vec![
            entry_in_partition("data/a.parquet", Some("1")),
            entry_in_partition("data/b.parquet", Some("2")),
            entry_in_partition("data/default.parquet", None),
        ],
        partitioned_declared_schema(),
        &["part".to_string()],
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let files = rt
        .block_on(async { table.partitioned_files(&memory_session()).await })
        .expect("no delete needs reading");

    let values: Vec<&datafusion::scalar::ScalarValue> = files
        .iter()
        .map(|f| {
            assert_eq!(
                f.partition_values.len(),
                1,
                "one value per partition column"
            );
            &f.partition_values[0]
        })
        .collect();
    assert_eq!(
        values,
        vec![
            &datafusion::scalar::ScalarValue::Int32(Some(1)),
            &datafusion::scalar::ScalarValue::Int32(Some(2)),
            &datafusion::scalar::ScalarValue::Int32(None),
        ]
    );
}

#[test]
fn an_unpartitioned_scan_attaches_no_partition_values() {
    let table = scan_table_over(vec![FileEntry::with_deletes(
        "data/a.parquet".to_string(),
        1,
        Vec::new(),
    )]);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let files = rt
        .block_on(async { table.partitioned_files(&memory_session()).await })
        .expect("no delete needs reading");

    assert!(files[0].partition_values.is_empty());
}

#[test]
fn an_unrepresentable_partition_value_fails_the_scan() {
    let table = scan_table_partitioned_by(
        vec![entry_in_partition("data/a.parquet", Some("not-a-number"))],
        partitioned_declared_schema(),
        &["part".to_string()],
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = rt
        .block_on(async { table.partitioned_files(&memory_session()).await })
        .expect_err("an unrepresentable partition value must never reach the query");

    let message = error.to_string();
    assert!(message.contains("part"), "{message}");
    assert!(message.contains("Int32"), "{message}");
    assert!(message.contains("not-a-number"), "{message}");
}

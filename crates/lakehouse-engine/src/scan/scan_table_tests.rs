use super::*;
use crate::scan::spec::DeleteMechanism;
use datafusion::execution::session_state::SessionState;
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
        secrets: Arc::from([]),
        delete_path_read_limiter: Arc::new(Semaphore::new(1)),
        delete_state_reservations: Default::default(),
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

fn parquet_bytes(batch: arrow::array::RecordBatch, rows_per_row_group: usize) -> Vec<u8> {
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_row_group))
        .build();
    let mut buf = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

/// A 1000-row data file in ten row groups whose every other row is deleted, in a session whose
/// memory pool holds `pool_bytes`.
fn session_with_deleted_rows(pool_bytes: usize) -> (ParquetScanTable, SessionState) {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use object_store::{ObjectStoreExt, PutPayload};

    let data = parquet_bytes(
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from_iter_values(0..1000)) as _)])
            .unwrap(),
        100,
    );
    let deleted: Vec<i64> = (0..1000).step_by(2).collect();
    let deletes = parquet_bytes(
        RecordBatch::try_from_iter([
            (
                "file_path",
                Arc::new(StringArray::from(vec![
                    "memory:///db/t/data/a.parquet";
                    deleted.len()
                ])) as _,
            ),
            ("pos", Arc::new(Int64Array::from(deleted)) as _),
        ])
        .unwrap(),
        1000,
    );

    let table = scan_table_over(vec![FileEntry::with_deletes(
        "data/a.parquet".to_string(),
        data.len() as u64,
        vec![DeleteMechanism::IcebergPositionalDelete {
            path: "deletes/d.parquet".to_string(),
            size: deletes.len() as u64,
        }],
    )]);
    let store = object_store::memory::InMemory::new();
    futures::executor::block_on(async {
        store
            .put(&"db/t/data/a.parquet".into(), PutPayload::from(data))
            .await
            .unwrap();
        store
            .put(&"db/t/deletes/d.parquet".into(), PutPayload::from(deletes))
            .await
            .unwrap();
    });
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_bytes)))
        .build_arc()
        .unwrap();
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    ctx.register_object_store(&url::Url::parse("memory://").unwrap(), Arc::new(store));
    (table, ctx.state())
}

/// Scenario: Row-position delete state is reserved in the memory pool
#[test]
fn delete_state_is_reserved_in_the_memory_pool_while_the_provider_lives() {
    let (table, state) = session_with_deleted_rows(1024 * 1024);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(table.partitioned_files(&state)).unwrap();
    let pool = &state.runtime_env().memory_pool;
    assert!(
        pool.reserved() >= 1000 * size_of::<parquet::arrow::arrow_reader::RowSelector>(),
        "the access plans' selectors are reserved: {} bytes",
        pool.reserved()
    );

    drop(table);
    assert_eq!(
        pool.reserved(),
        0,
        "dropping the provider releases the reservation"
    );
}

/// Scenario: Row-position delete state is reserved in the memory pool
#[test]
fn delete_state_beyond_the_memory_pool_fails_the_scan_cleanly() {
    let (table, state) = session_with_deleted_rows(4096);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let error = rt
        .block_on(table.partitioned_files(&state))
        .expect_err("the delete state outgrows a 4 KiB pool");

    assert!(
        error.to_string().contains("does not fit the memory pool"),
        "{error}"
    );
}

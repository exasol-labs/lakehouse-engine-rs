//! `schema.name-mapping.default` field-id resolution for Parquet columns without field-ids.

use crate::scan_fixture;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::ExaType;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, NameMappingEntry, ScanSpec, ScanStorage,
    StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{run_raw_scan_with_session, session_config_for_spec};
use object_store::local::LocalFileSystem;
use parquet::arrow::ArrowWriter;

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

/// Neither column carries `PARQUET:field_id`. `id` holds `0..rows`, the other `10 * id`.
fn write_local_parquet_two_int_cols(
    dir: &std::path::Path,
    relative: &str,
    id_col_name: &str,
    other_col_name: &str,
    rows: i64,
) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new(id_col_name, DataType::Int64, false),
        Field::new(other_col_name, DataType::Int64, true),
    ]));
    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let ids: Vec<i64> = (0..rows).collect();
    let others: Vec<i64> = (0..rows).map(|i| i * 10).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(others)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn name_mapping_spec(
    file_url: String,
    file_size: u64,
    logical_schema: Vec<LogicalField>,
    name_mapping: Vec<NameMappingEntry>,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NEW_COL".into()],
            logical_schema,
            name_mapping,
            storage: ScanStorage::Inline(dummy_storage()),
            df_batch_size: 64,
            ..Default::default()
        },
        files: vec![FileEntry::new(file_url, file_size)],
    }
}

/// `new_col` is nullable so an unresolved binding NULL-fills silently, making "never NULL" the proof.
fn logical_schema() -> Vec<LogicalField> {
    vec![
        LogicalField {
            field_id: Some(1),
            name: "id".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: Some(2),
            name: "new_col".to_string(),
            arrow_type: "int64".to_string(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ]
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

async fn run_scan(spec: &ScanSpec, register_url: &str) -> Result<Vec<RecordBatch>, String> {
    let session = datafusion::execution::context::SessionContext::new_with_config(
        session_config_for_spec(spec),
    );
    session.runtime_env().register_object_store(
        &url::Url::parse(register_url).expect("register url"),
        Arc::new(LocalFileSystem::new()),
    );
    let mut ctx = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(vec![]),
        &[ExaType::Int64, ExaType::Int64],
    );
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(
        &mut ctx,
        &session,
        spec,
        &scan_fixture::resolved_storage(spec),
        &mut timers,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(ctx.into_batches())
}

fn mapping_entry(name: &str, field_id: i32) -> NameMappingEntry {
    NameMappingEntry {
        names: vec![name.to_string()],
        field_id,
        fields: Vec::new(),
    }
}

fn id_to_new_col(batches: &[RecordBatch]) -> HashMap<i64, Option<i64>> {
    let mut out = HashMap::new();
    for b in batches {
        assert_eq!(
            b.schema().field(1).name(),
            "NEW_COL",
            "the renamed column must be emitted under its CURRENT logical name"
        );
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id col");
        let values = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("new_col col");
        for i in 0..b.num_rows() {
            let value = if values.is_null(i) {
                None
            } else {
                Some(values.value(i))
            };
            out.insert(ids.value(i), value);
        }
    }
    out
}

#[test]
fn name_mapping_resolves_no_field_id_column() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let rows = 30;
    let file_url = write_local_parquet_two_int_cols(dir, "renamed.parquet", "id", "old_col", rows);
    let file_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    let spec = name_mapping_spec(
        file_url.clone(),
        file_size,
        logical_schema(),
        vec![mapping_entry("id", 1), mapping_entry("old_col", 2)],
    );

    let batches = block_on(run_scan(&spec, &file_url)).expect("raw scan must succeed");
    let by_id = id_to_new_col(&batches);
    assert_eq!(by_id.len(), rows as usize, "row count");

    for (id, value) in &by_id {
        assert_eq!(
            *value,
            Some(id * 10),
            "row {id}: NEW_COL must carry the real file value via name-mapping resolution, never NULL"
        );
    }
}

/// Scenario: A column is never bound by name to a field-id-bound logical field
#[test]
fn a_same_named_column_the_mapping_omits_reads_null() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let rows = 30;
    let file_url = write_local_parquet_two_int_cols(dir, "omitted.parquet", "id", "new_col", rows);
    let file_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    let spec = name_mapping_spec(
        file_url.clone(),
        file_size,
        logical_schema(),
        vec![mapping_entry("id", 1)],
    );

    let batches = block_on(run_scan(&spec, &file_url)).expect("raw scan must succeed");
    let by_id = id_to_new_col(&batches);
    assert_eq!(by_id.len(), rows as usize, "row count");
    assert!(
        by_id.values().all(Option::is_none),
        "NEW_COL must read NULL, never the file's same-named column the mapping omits"
    );
}

/// Scenario: A column is never bound by name to a field-id-bound logical field
#[test]
fn a_file_without_field_ids_or_a_mapping_fails_a_required_column() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let file_url = write_local_parquet_two_int_cols(dir, "unmapped.parquet", "id", "new_col", 3);
    let file_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    let spec = name_mapping_spec(file_url.clone(), file_size, logical_schema(), vec![]);

    let error = block_on(run_scan(&spec, &file_url))
        .expect_err("the required `id` must not bind the file's same-named column");
    assert!(error.contains("'id'"), "{error}");
}

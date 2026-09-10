use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::test_support::TestContext;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, ProjectionItem, ScanSpec, ScanStorage, StorageBackend,
    StorageProps,
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

fn write_local_parquet(dir: &std::path::Path, relative: &str, rows: &[(i64, &str)]) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let names: Vec<&str> = rows.iter().map(|(_, name)| *name).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn identity_field(name: &str, arrow_type: &str, nullable: bool) -> LogicalField {
    LogicalField {
        field_id: None,
        name: name.to_string(),
        arrow_type: arrow_type.to_string(),
        nullable,
        initial_default: None,
        nested: None,
        physical_name: None,
    }
}

fn raw_scan_spec(
    file_url: String,
    file_size: u64,
    projection: Vec<ProjectionItem>,
    filter: Option<String>,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            projection,
            filter,
            logical_schema: vec![
                identity_field("id", "int64", false),
                identity_field("name", "utf8", false),
            ],
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

async fn run_scan(spec: &ScanSpec, register_url: &str) -> Result<Vec<RecordBatch>, UdfError> {
    let session = datafusion::execution::context::SessionContext::new_with_config(
        session_config_for_spec(spec),
    );
    session.runtime_env().register_object_store(
        &url::Url::parse(register_url).expect("register url"),
        Arc::new(LocalFileSystem::new()),
    );
    let mut ctx = scan_fixture::BatchCapturingCtx::new(TestContext::scalar(vec![]));
    let mut timers = PhaseTimers::start();
    let storage = ResolvedScanStorage::from_backends(dummy_storage(), None);
    run_raw_scan_with_session(&mut ctx, &session, spec, &storage, &mut timers).await?;
    Ok(ctx.into_batches())
}

fn int64_column(batch: &RecordBatch, index: usize) -> &Int64Array {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column")
}

fn string_value(batch: &RecordBatch, index: usize, row: usize) -> String {
    let column = batch.column(index);
    if let Some(v) = column
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
    {
        return v.value(row).to_string();
    }
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column (Utf8 or Utf8View)")
        .value(row)
        .to_string()
}

fn rows_for_test(dir: &std::path::Path, relative: &str) -> (String, u64) {
    let rows = [(0i64, "event-01"), (1i64, "event-02"), (2i64, "other-03")];
    let file_url = write_local_parquet(dir, relative, &rows);
    let file_size = std::fs::metadata(
        url::Url::parse(&file_url)
            .expect("parse file url")
            .to_file_path()
            .expect("file url to path"),
    )
    .expect("stat parquet")
    .len();
    (file_url, file_size)
}

#[test]
fn substr_expression_in_select_list_evaluates() {
    let dir = std::env::temp_dir().join(format!("lh_substr_select_list_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (file_url, file_size) = rows_for_test(&dir, "select_list.parquet");

    let spec = raw_scan_spec(
        file_url.clone(),
        file_size,
        vec![
            ProjectionItem::Column("ID".into()),
            ProjectionItem::Expr {
                expr: r#"substr("NAME", 1, 5)"#.into(),
            },
        ],
        None,
    );

    let batches = block_on(run_scan(&spec, &file_url)).expect("raw scan must succeed");
    let expected: std::collections::HashMap<i64, &str> = [(0, "event"), (1, "event"), (2, "other")]
        .into_iter()
        .collect();
    let mut seen = 0;
    for batch in &batches {
        assert_eq!(batch.num_columns(), 2, "projection must emit 2 columns");
        let ids = int64_column(batch, 0);
        for row in 0..batch.num_rows() {
            let id = ids.value(row);
            let substr = string_value(batch, 1, row);
            assert_eq!(
                substr, expected[&id],
                "row {id}: substr(\"NAME\", 1, 5) must evaluate to the expected substring"
            );
            seen += 1;
        }
    }
    assert_eq!(seen, 3, "all rows must be seen");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn substr_expression_in_filter_selects_matching_rows() {
    let dir = std::env::temp_dir().join(format!("lh_substr_filter_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (file_url, file_size) = rows_for_test(&dir, "filter.parquet");

    let spec = raw_scan_spec(
        file_url.clone(),
        file_size,
        vec![
            ProjectionItem::Column("ID".into()),
            ProjectionItem::Column("NAME".into()),
        ],
        Some(r#"substr("NAME", 1, 5) = 'event'"#.into()),
    );

    let batches = block_on(run_scan(&spec, &file_url)).expect("raw scan must succeed");
    let mut matched_ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let ids = int64_column(batch, 0);
        for row in 0..batch.num_rows() {
            let id = ids.value(row);
            let name = string_value(batch, 1, row);
            assert!(
                name.starts_with("event"),
                "row {id}: filter must select only rows whose NAME starts with 'event', got {name}"
            );
            matched_ids.push(id);
        }
    }
    matched_ids.sort_unstable();
    assert_eq!(
        matched_ids,
        vec![0, 1],
        "filter must select exactly the matching rows (id 2 is 'other-03' and must be excluded)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn left_expression_still_plans_alongside_substr() {
    let dir = std::env::temp_dir().join(format!("lh_substr_left_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (file_url, file_size) = rows_for_test(&dir, "left.parquet");

    let spec = raw_scan_spec(
        file_url.clone(),
        file_size,
        vec![
            ProjectionItem::Column("ID".into()),
            ProjectionItem::Expr {
                expr: r#"substr("NAME", 1, 5)"#.into(),
            },
            ProjectionItem::Expr {
                expr: r#"left("NAME", 5)"#.into(),
            },
        ],
        None,
    );

    let batches = block_on(run_scan(&spec, &file_url)).expect("raw scan must succeed");
    let mut seen = 0;
    for batch in &batches {
        assert_eq!(batch.num_columns(), 3, "projection must emit 3 columns");
        for row in 0..batch.num_rows() {
            let substr_value = string_value(batch, 1, row);
            let left_value = string_value(batch, 2, row);
            assert_eq!(
                left_value, substr_value,
                "left(\"NAME\", 5) must plan and evaluate to the same leftmost \
                 5 characters as substr(\"NAME\", 1, 5)"
            );
            seen += 1;
        }
    }
    assert_eq!(seen, 3, "all rows must be seen");

    let _ = std::fs::remove_dir_all(&dir);
}

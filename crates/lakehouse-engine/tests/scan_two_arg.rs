mod scan_fixture;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::{ExaType, Value};
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, DeleteMechanism, FileEntry, ScanSpec, ScanStorage, StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{read_scan_spec, run_raw_scan_with_session, session_config_for_spec};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;

/// Iceberg reserved field-ids; duplicated because the engine's constants are `pub(crate)`.
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

fn write_local_parquet(dir: &std::path::Path, rows: i64, row_group: usize) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let path = dir.join("two_arg_data.parquet");
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let ids: Vec<i64> = (0..rows).collect();
    let names: Vec<String> = (0..rows).map(|i| format!("row-{i}")).collect();
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

fn id_name_as_decimal() -> Vec<ExaType> {
    vec![scan_fixture::decimal(20, 0), scan_fixture::varchar()]
}

fn id_name_as_int64() -> Vec<ExaType> {
    vec![ExaType::Int64, scan_fixture::varchar()]
}

fn scan_spec(file_url: String) -> ScanSpec {
    let size = std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(&file_url))
        .map(|m| m.len())
        .unwrap_or(0);
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            filter: Some("\"ID\" >= 10".into()),
            storage: ScanStorage::Inline(StorageBackend::S3(StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "k".into(),
                secret_key: "s".into(),
                allow_http: true,
                ..Default::default()
            })),
            df_batch_size: 64,
            ..Default::default()
        },
        files: vec![FileEntry::new(file_url, size)],
    }
}

async fn run_with_spec(spec: &ScanSpec, emits: &[ExaType]) -> Vec<RecordBatch> {
    let mut ctx = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(vec![Value::String(spec.to_json())]),
        emits,
    );
    let session = SessionContext::new_with_config(session_config_for_spec(spec));
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(
        &mut ctx,
        &session,
        spec,
        &scan_fixture::resolved_storage(spec),
        &mut timers,
    )
    .await
    .expect("raw scan must succeed");
    ctx.into_batches()
}

async fn run_two_arg(common_json: &str, files_json: &str, emits: &[ExaType]) -> Vec<RecordBatch> {
    let mut ctx = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(vec![
            Value::String(common_json.to_string()),
            Value::String(files_json.to_string()),
        ]),
        emits,
    );
    let spec = read_scan_spec(&ctx).expect("reconstitute spec from two args");
    let session = SessionContext::new_with_config(session_config_for_spec(&spec));
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(
        &mut ctx,
        &session,
        &spec,
        &scan_fixture::resolved_storage(&spec),
        &mut timers,
    )
    .await
    .expect("raw scan must succeed");
    ctx.into_batches()
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn ids_of(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id col");
        for i in 0..b.num_rows() {
            out.push(ids.value(i));
        }
    }
    out.sort_unstable();
    out
}

fn file_size(file_url: &str) -> u64 {
    std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(file_url))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn write_delete_parquet(dir: &std::path::Path, relative: &str, entries: &[(&str, i64)]) -> String {
    let field_id_meta =
        |id: i32| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false)
            .with_metadata(field_id_meta(FIELD_ID_POSITIONAL_DELETE_FILE_PATH)),
        Field::new("pos", DataType::Int64, false)
            .with_metadata(field_id_meta(FIELD_ID_POSITIONAL_DELETE_POS)),
    ]));
    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let paths: Vec<&str> = entries.iter().map(|(p, _)| *p).collect();
    let positions: Vec<i64> = entries.iter().map(|(_, pos)| *pos).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(paths)),
            Arc::new(Int64Array::from(positions)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn spec_for_files(files: Vec<FileEntry>) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            storage: ScanStorage::Inline(StorageBackend::S3(StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "k".into(),
                secret_key: "s".into(),
                allow_http: true,
                ..Default::default()
            })),
            df_batch_size: 64,
            ..Default::default()
        },
        files,
    }
}

/// Scenario: the two-argument path emits rows identical to the single-argument path
#[test]
fn scan_registers_only_assigned_files_two_arg() {
    let dir = std::env::temp_dir().join(format!("lh_two_arg_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file_url = write_local_parquet(&dir, 200, 64);
    let spec = scan_spec(file_url);

    let common_json = spec.to_common_json();
    let files_json = ScanSpec::files_json(&spec.files);
    assert!(
        !common_json.contains("\"files\""),
        "common blob must not carry a files key: {common_json}"
    );

    let reconstituted =
        ScanSpec::from_parts_json(&common_json, &files_json).expect("from_parts_json");
    assert_eq!(
        reconstituted, spec,
        "two-arg reconstitution must equal spec"
    );

    let single = block_on(run_with_spec(&spec, &id_name_as_decimal()));
    let two_arg = block_on(run_two_arg(
        &common_json,
        &files_json,
        &id_name_as_decimal(),
    ));

    assert_eq!(total_rows(&single), 190, "single-arg row count");
    assert_eq!(
        total_rows(&two_arg),
        190,
        "two-arg row count must match the filtered file contents"
    );

    assert_eq!(
        two_arg.len(),
        single.len(),
        "two-arg and single-arg must emit the same number of batches"
    );
    assert_eq!(
        two_arg, single,
        "two-arg reconstitution must emit rows identical to the pre-split single-arg path"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a SQL NULL in either argument is a user error
#[test]
fn two_arg_null_in_either_argument_is_user_error() {
    let files_json = ScanSpec::files_json(&[FileEntry::new("s3://w/f0.parquet", 0)]);
    let common_json = scan_spec("s3://w/f0.parquet".into()).to_common_json();

    let ctx = TestContext::scalar(vec![Value::Null, Value::String(files_json.clone())]);
    let err = read_scan_spec(&ctx).expect_err("NULL common must error");
    assert!(
        matches!(err, UdfError::User(ref m) if m.contains("common") && m.contains("NULL")),
        "NULL common must be a user error naming the common arg: {err:?}"
    );

    let ctx = TestContext::scalar(vec![Value::String(common_json), Value::Null]);
    let err = read_scan_spec(&ctx).expect_err("NULL files must error");
    assert!(
        matches!(err, UdfError::User(ref m) if m.contains("files") && m.contains("NULL")),
        "NULL files must be a user error naming the files arg: {err:?}"
    );
}

/// Scenario: only the assigned file is scanned; an unassigned sibling file never leaks in
#[test]
fn scan_registers_assigned_files_via_parquet_provider() {
    let dir = std::env::temp_dir().join(format!("lh_provider_{}", std::process::id()));
    let assigned_dir = dir.join("assigned");
    let decoy_dir = dir.join("decoy");
    std::fs::create_dir_all(&assigned_dir).unwrap();
    std::fs::create_dir_all(&decoy_dir).unwrap();

    let assigned_url = write_local_parquet(&assigned_dir, 30, 8);
    let decoy_url = write_local_parquet(&decoy_dir, 500, 64);
    let _ = &decoy_url;

    let entry = FileEntry::new(assigned_url.clone(), file_size(&assigned_url));
    let spec = spec_for_files(vec![entry]);
    let common_json = spec.to_common_json();
    let files_json = ScanSpec::files_json(&spec.files);

    let rows = block_on(run_two_arg(&common_json, &files_json, &id_name_as_int64()));
    let ids = ids_of(&rows);

    assert_eq!(
        total_rows(&rows),
        30,
        "only the assigned file's 30 rows must be scanned, not the decoy's 500"
    );
    assert_eq!(
        ids,
        (0..30).collect::<Vec<_>>(),
        "no decoy id (>= 10_000 range would be impossible here since decoy ids are 0..500, \
         so any leak would inflate the count/ids beyond the assigned file's own range): {ids:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: positional-delete refs survive the two-argument split and are enforced
#[test]
fn spec_reconstitutes_with_delete_entries() {
    let dir = std::env::temp_dir().join(format!("lh_recon_del_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let data_url = write_local_parquet(&dir, 20, 8);
    let delete_url =
        write_delete_parquet(&dir, "deletes.parquet", &[(&data_url, 2), (&data_url, 9)]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        file_size(&data_url),
        vec![DeleteMechanism::IcebergPositionalDelete {
            path: delete_url.clone(),
            size: file_size(&delete_url),
        }],
    );
    let spec = spec_for_files(vec![entry]);

    let common_json = spec.to_common_json();
    let files_json = ScanSpec::files_json(&spec.files);
    assert!(
        files_json.contains("deletes.parquet"),
        "per-shard files JSON must carry the delete file: {files_json}"
    );

    let reconstituted =
        ScanSpec::from_parts_json(&common_json, &files_json).expect("from_parts_json");
    assert_eq!(
        reconstituted, spec,
        "two-arg reconstitution must equal the delete-carrying spec"
    );
    assert_eq!(reconstituted.files[0].deletes.len(), 1);
    assert!(matches!(
        reconstituted.files[0].deletes[0],
        DeleteMechanism::IcebergPositionalDelete { .. }
    ));

    let rows = block_on(run_two_arg(&common_json, &files_json, &id_name_as_int64()));
    assert_eq!(total_rows(&rows), 18, "2 of 20 rows deleted");
    let ids = ids_of(&rows);
    assert!(!ids.contains(&2), "position 2 must be deleted: {ids:?}");
    assert!(!ids.contains(&9), "position 9 must be deleted: {ids:?}");

    let _ = std::fs::remove_dir_all(&dir);
}

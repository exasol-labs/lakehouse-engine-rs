mod scan_fixture;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::ExaType;
use futures::stream::BoxStream;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, DeleteMechanism, FileEntry, JoinSpec, JoinType, LogicalField, ScanSpec,
    ScanStorage, StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{
    build_join_physical_plan, build_raw_scan_physical_plan, register_files,
    run_join_scan_with_session, run_raw_scan_with_session, session_config_for_spec,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use url::Url;

/// Iceberg reserved field-ids; duplicated because the engine's constants are `pub(crate)`.
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

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

fn local_file_size(file_url: &str) -> u64 {
    let path = Url::parse(file_url)
        .expect("valid file URL")
        .to_file_path()
        .expect("file:// URL");
    std::fs::metadata(path).expect("stat local parquet").len()
}

fn write_data_parquet(dir: &Path, relative: &str, ids: &[i64], row_group: usize) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let names: Vec<String> = ids.iter().map(|id| format!("row-{id}")).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn write_delete_parquet(dir: &Path, relative: &str, entries: &[(&str, i64)]) -> String {
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
    Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn delete_ref(abs_url: &str) -> DeleteMechanism {
    DeleteMechanism::IcebergPositionalDelete {
        path: abs_url.to_string(),
        size: local_file_size(abs_url),
    }
}

fn scan_spec(files: Vec<FileEntry>, filter: Option<String>, limit: Option<u64>) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            filter,
            limit,
            storage: ScanStorage::Inline(dummy_storage()),
            df_batch_size: 64,
            ..Default::default()
        },
        files,
    }
}

/// Field-ids are assigned sequentially from 1.
fn logical_fields(fields: &[(&str, &str)]) -> Vec<LogicalField> {
    fields
        .iter()
        .enumerate()
        .map(|(i, (name, arrow_type))| LogicalField {
            field_id: Some((i + 1) as i32),
            name: (*name).to_string(),
            arrow_type: (*arrow_type).to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        })
        .collect()
}

/// Request-count assertions must use this, not [`scan_spec`]: an empty `logical_schema`
/// triggers schema inference, which fetches the first file's footer and skews GET counts.
fn scan_spec_with_logical_schema(
    files: Vec<FileEntry>,
    filter: Option<String>,
    limit: Option<u64>,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            filter,
            limit,
            storage: ScanStorage::Inline(dummy_storage()),
            df_batch_size: 64,
            logical_schema: logical_fields(&[("id", "int64"), ("name", "utf8")]),
            ..Default::default()
        },
        files,
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

async fn try_run_scan_with_store(
    spec: &ScanSpec,
    register_url: &str,
    store: Arc<dyn ObjectStore>,
    emits: &[ExaType],
) -> Result<Vec<RecordBatch>, UdfError> {
    let session = SessionContext::new_with_config(session_config_for_spec(spec));
    session
        .runtime_env()
        .register_object_store(&Url::parse(register_url).expect("register url"), store);
    let mut ctx = scan_fixture::BatchCapturingCtx::declaring(TestContext::scalar(vec![]), emits);
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(
        &mut ctx,
        &session,
        spec,
        &scan_fixture::resolved_storage(spec),
        &mut timers,
    )
    .await?;
    Ok(ctx.into_batches())
}

fn run_scan(spec: &ScanSpec, register_url: &str) -> Vec<RecordBatch> {
    block_on(try_run_scan_with_store(
        spec,
        register_url,
        Arc::new(LocalFileSystem::new()),
        &id_name_emits(),
    ))
    .expect("raw scan must succeed")
}

fn id_name_emits() -> Vec<ExaType> {
    vec![ExaType::Int64, scan_fixture::varchar()]
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

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("lh_pos_del_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Scenario: a file-granularity positional-delete file removes exactly its flagged positions
#[test]
fn scan_applies_file_granularity_positional_deletes() {
    let dir = temp_dir("file_gran");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..20).collect::<Vec<_>>(), 8);
    let delete_url =
        write_delete_parquet(&dir, "deletes.parquet", &[(&data_url, 3), (&data_url, 7)]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    assert_eq!(total_rows(&rows), 18, "18 rows survive after 2 deletes");
    let ids = ids_of(&rows);
    assert!(!ids.contains(&3), "position 3 must be deleted: {ids:?}");
    assert!(!ids.contains(&7), "position 7 must be deleted: {ids:?}");
    assert_eq!(
        ids,
        (0..20).filter(|i| *i != 3 && *i != 7).collect::<Vec<_>>()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a partition-granularity delete file applies to each data file only by `file_path`
#[test]
fn scan_filters_partition_delete_by_file_path() {
    let dir = temp_dir("partition_gran");
    let f0 = write_data_parquet(&dir, "p0/data.parquet", &(100..110).collect::<Vec<_>>(), 4);
    let f1 = write_data_parquet(&dir, "p1/data.parquet", &(200..210).collect::<Vec<_>>(), 4);
    let delete_url = write_delete_parquet(
        &dir,
        "shared_delete.parquet",
        &[(&f0, 2), (&f0, 5), (&f1, 1)],
    );
    let shared_delete = delete_ref(&delete_url);

    let entries = vec![
        FileEntry::with_deletes(
            f0.clone(),
            local_file_size(&f0),
            vec![shared_delete.clone()],
        ),
        FileEntry::with_deletes(f1.clone(), local_file_size(&f1), vec![shared_delete]),
    ];
    let spec = scan_spec(entries, None, None);
    let rows = run_scan(&spec, &f0);

    assert_eq!(total_rows(&rows), 17, "20 rows - 3 deleted = 17");
    let ids = ids_of(&rows);
    for missing in [102, 105, 201] {
        assert!(
            !ids.contains(&missing),
            "id {missing} must be deleted by the shared partition delete file: {ids:?}"
        );
    }
    assert!(ids.contains(&200), "f1's other rows must survive: {ids:?}");
    assert!(ids.contains(&100), "f0's other rows must survive: {ids:?}");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: multiple positional-delete files on one data file are unioned
#[test]
fn scan_unions_multiple_delete_files() {
    let dir = temp_dir("union");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..20).collect::<Vec<_>>(), 8);
    let delete_a = write_delete_parquet(&dir, "del_a.parquet", &[(&data_url, 1), (&data_url, 4)]);
    let delete_b = write_delete_parquet(&dir, "del_b.parquet", &[(&data_url, 4), (&data_url, 9)]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_a), delete_ref(&delete_b)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    assert_eq!(
        total_rows(&rows),
        17,
        "17 rows survive after the union of 2 delete files"
    );
    let ids = ids_of(&rows);
    for missing in [1, 4, 9] {
        assert!(
            !ids.contains(&missing),
            "id {missing} must be deleted: {ids:?}"
        );
    }
    assert_eq!(
        ids,
        (0..20)
            .filter(|i| ![1, 4, 9].contains(i))
            .collect::<Vec<_>>()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a delete file flagging every row yields zero rows for that file
#[test]
fn scan_fully_deleted_file_yields_no_rows() {
    let dir = temp_dir("fully_deleted");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..5).collect::<Vec<_>>(), 8);
    let delete_url = write_delete_parquet(
        &dir,
        "deletes.parquet",
        &[
            (&data_url, 0),
            (&data_url, 1),
            (&data_url, 2),
            (&data_url, 3),
            (&data_url, 4),
        ],
    );

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    assert_eq!(
        total_rows(&rows),
        0,
        "a fully-deleted file must yield no rows"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: positional deletes compose with filter pushdown + row-group pruning and with LIMIT
#[test]
fn scan_deletes_compose_with_pushdown_and_pruning() {
    let dir = temp_dir("compose");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..100).collect::<Vec<_>>(), 16);
    let delete_url = write_delete_parquet(
        &dir,
        "deletes.parquet",
        &[(&data_url, 5), (&data_url, 50), (&data_url, 95)],
    );
    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_url)],
    );

    let filter_spec = scan_spec(vec![entry.clone()], Some("\"ID\" >= 60".to_string()), None);
    let filter_rows = run_scan(&filter_spec, &data_url);
    let expected_filtered: Vec<i64> = (60..100).filter(|id| *id != 95).collect();
    assert_eq!(
        ids_of(&filter_rows),
        expected_filtered,
        "filter pushdown + row-group pruning must compose with the delete (only 95 was in-range)"
    );

    let limit_spec = scan_spec(vec![entry], None, Some(10));
    let limit_rows = run_scan(&limit_spec, &data_url);
    assert_eq!(
        ids_of(&limit_rows),
        vec![0, 1, 2, 3, 4, 6, 7, 8, 9, 10],
        "LIMIT pushdown must count only post-delete rows (position 5 is deleted)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: an equality delete is rejected with a mechanism-naming error before the file is opened
#[test]
fn scan_rejects_unapplicable_delete_file() {
    let dir = temp_dir("unapplicable");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);

    let bogus_delete = DeleteMechanism::IcebergEqualityDelete {
        path: format!("{}/does-not-need-to-exist.parquet", dir.to_string_lossy()),
        size: 10,
    };
    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![bogus_delete],
    );
    let spec = scan_spec(vec![entry], None, None);

    let err = block_on(try_run_scan_with_store(
        &spec,
        &data_url,
        Arc::new(LocalFileSystem::new()),
        &id_name_emits(),
    ))
    .expect_err("an equality delete must be rejected, not applied");
    let msg = err.to_string();
    assert!(
        msg.contains("equality delete"),
        "error must name the unsupported mechanism: {msg}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a Puffin deletion vector is rejected with a mechanism-naming error before opening
#[test]
fn scan_rejects_puffin_deletion_vector() {
    let dir = temp_dir("puffin_dv");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);

    let bogus_delete = DeleteMechanism::IcebergPuffinDeletionVector {
        path: format!("{}/does-not-need-to-exist.puffin", dir.to_string_lossy()),
        size: 10,
    };
    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![bogus_delete],
    );
    let spec = scan_spec(vec![entry], None, None);

    let err = block_on(try_run_scan_with_store(
        &spec,
        &data_url,
        Arc::new(LocalFileSystem::new()),
        &id_name_emits(),
    ))
    .expect_err("a Puffin deletion vector must be rejected, not applied");
    let msg = err.to_string();
    assert!(
        msg.contains("Puffin deletion vector"),
        "error must name the unsupported mechanism: {msg}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a negative positional-delete `pos` is rejected rather than wrapping to a huge index
#[test]
fn scan_rejects_negative_positional_delete() {
    let dir = temp_dir("neg_pos");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);
    let delete_url = write_delete_parquet(&dir, "delete.parquet", &[(data_url.as_str(), -1)]);
    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);

    let err = block_on(try_run_scan_with_store(
        &spec,
        &data_url,
        Arc::new(LocalFileSystem::new()),
        &id_name_emits(),
    ))
    .expect_err("a negative pos must be rejected, not silently dropped");
    // secret_key is "s", so redaction strips every "s" from the message.
    let msg = err.to_string();
    assert!(
        msg.contains("negative") && msg.contains("(-1)"),
        "error must name the malformed negative position: {msg}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a spec whose files span more than one object-store root is rejected at registration
#[test]
fn scan_rejects_mixed_object_store_roots() {
    let dir = temp_dir("mixed_roots");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);
    let local = FileEntry::new(data_url.clone(), local_file_size(&data_url));
    let foreign = FileEntry::new("s3://other-bucket/part-0.parquet", 10);
    let spec = scan_spec(vec![local, foreign], None, None);

    let err = block_on(try_run_scan_with_store(
        &spec,
        &data_url,
        Arc::new(LocalFileSystem::new()),
        &id_name_emits(),
    ))
    .expect_err("a spec mixing object-store roots must be rejected");
    assert!(
        err.to_string().contains("mixes object-store roots"),
        "error must explain the mixed-root rejection: {err}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a data file with no delete files scans unchanged
#[test]
fn scan_delete_free_file_unchanged() {
    let dir = temp_dir("delete_free");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);

    let entry = FileEntry::new(data_url.clone(), local_file_size(&data_url));
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    assert_eq!(
        total_rows(&rows),
        10,
        "no rows must be dropped when there are no deletes"
    );
    assert_eq!(ids_of(&rows), (0..10).collect::<Vec<_>>());

    let _ = std::fs::remove_dir_all(&dir);
}

/// Records every non-HEAD `get` by location.
#[derive(Debug)]
struct TrackingStore {
    inner: Arc<dyn ObjectStore>,
    gets: Arc<std::sync::Mutex<Vec<ObjectStorePath>>>,
    calls: Arc<AtomicUsize>,
    concurrency: Option<ConcurrencyProbe>,
}

/// Peak-concurrency counter over probed reads, with a fixed delay forcing deterministic overlap.
#[derive(Debug)]
struct ConcurrencyProbe {
    needles: Vec<String>,
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    delay: Duration,
}

impl ConcurrencyProbe {
    fn is_probed_read(&self, location: &ObjectStorePath) -> bool {
        let path = location.as_ref();
        self.needles.iter().any(|n| path.contains(n.as_str()))
    }
}

/// Decrements on drop so a cancelled read (fired timeout) never leaks an in-flight count.
struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl std::fmt::Display for TrackingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TrackingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for TrackingStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !options.head {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.gets.lock().unwrap().push(location.clone());
        }
        // For data-file needles the peak is only meaningful when the plan is built but never
        // executed: execution re-reads data files without holding a permit.
        if !options.head
            && let Some(probe) = &self.concurrency
            && probe.is_probed_read(location)
        {
            let now = probe.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            probe.peak.fetch_max(now, Ordering::SeqCst);
            let _guard = InFlightGuard {
                in_flight: Arc::clone(&probe.in_flight),
            };
            tokio::time::sleep(probe.delay).await;
            return self.inner.get_opts(location, options).await;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectStorePath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectStorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Scenario: delete files are read through the same registered (credentialed) store as data files
#[test]
fn scan_reads_delete_files_with_vended_credentials() {
    let dir = temp_dir("vended_creds");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..20).collect::<Vec<_>>(), 8);
    let delete_url =
        write_delete_parquet(&dir, "deletes.parquet", &[(&data_url, 2), (&data_url, 6)]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);

    let calls = Arc::new(AtomicUsize::new(0));
    let gets = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracking_store = Arc::new(TrackingStore {
        inner: Arc::new(LocalFileSystem::new()),
        gets: Arc::clone(&gets),
        calls: Arc::clone(&calls),
        concurrency: None,
    });

    let rows = block_on(try_run_scan_with_store(
        &spec,
        &data_url,
        tracking_store,
        &id_name_emits(),
    ))
    .expect("raw scan must succeed via the tracking (credentialed) store");

    assert_eq!(total_rows(&rows), 18, "2 deletes applied");

    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "both the data file and the delete file must be fetched via the registered store (got {} calls)",
        calls.load(Ordering::SeqCst)
    );
    let recorded = gets.lock().unwrap();
    let delete_needle = file_needle(&delete_url);
    assert!(
        recorded
            .iter()
            .any(|p| p.as_ref().contains(delete_needle.as_str())),
        "the delete file must be fetched through the registered (credentialed) store: {recorded:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

fn count_gets_matching(gets: &std::sync::Mutex<Vec<ObjectStorePath>>, needle: &str) -> usize {
    gets.lock()
        .unwrap()
        .iter()
        .filter(|p| p.as_ref().contains(needle))
        .count()
}

fn run_scan_tracked(
    spec: &ScanSpec,
    register_url: &str,
) -> (
    Vec<RecordBatch>,
    Arc<std::sync::Mutex<Vec<ObjectStorePath>>>,
) {
    let gets = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracking_store = Arc::new(TrackingStore {
        inner: Arc::new(LocalFileSystem::new()),
        gets: Arc::clone(&gets),
        calls: Arc::new(AtomicUsize::new(0)),
        concurrency: None,
    });
    let rows = block_on(try_run_scan_with_store(
        spec,
        register_url,
        tracking_store,
        &id_name_emits(),
    ))
    .expect("raw scan must succeed");
    (rows, gets)
}

/// Scenario: a delete file shared by two data files is read once per shard
#[test]
fn scan_reads_shared_delete_file_once_per_shard() {
    // One Parquet open issues several range GETs, so this compares against a one-referencer
    // baseline instead of asserting a magic total.
    let dir = temp_dir("shared_delete_once");
    let f0 = write_data_parquet(&dir, "p0/data.parquet", &(100..110).collect::<Vec<_>>(), 4);
    let f1 = write_data_parquet(&dir, "p1/data.parquet", &(200..210).collect::<Vec<_>>(), 4);
    let delete_url = write_delete_parquet(
        &dir,
        "shared_delete.parquet",
        &[(&f0, 2), (&f0, 5), (&f1, 1)],
    );
    let shared_delete = delete_ref(&delete_url);
    let delete_filename = file_needle(&delete_url);

    let solo_entries = vec![FileEntry::with_deletes(
        f0.clone(),
        local_file_size(&f0),
        vec![shared_delete.clone()],
    )];
    let solo_spec = scan_spec(solo_entries, None, None);
    let (_solo_rows, solo_gets) = run_scan_tracked(&solo_spec, &f0);
    let solo_delete_reads = count_gets_matching(&solo_gets, &delete_filename);
    assert!(
        solo_delete_reads > 0,
        "the solo baseline scan must actually fetch the delete file's body"
    );

    let shared_entries = vec![
        FileEntry::with_deletes(
            f0.clone(),
            local_file_size(&f0),
            vec![shared_delete.clone()],
        ),
        FileEntry::with_deletes(f1.clone(), local_file_size(&f1), vec![shared_delete]),
    ];
    let shared_spec = scan_spec(shared_entries, None, None);
    let (shared_rows, shared_gets) = run_scan_tracked(&shared_spec, &f0);
    let shared_delete_reads = count_gets_matching(&shared_gets, &delete_filename);

    assert_eq!(
        shared_delete_reads, solo_delete_reads,
        "the shared delete file must be read exactly once per shard regardless of \
         referencing data-file count: solo (1 referencer) = {solo_delete_reads} non-HEAD \
         get_opts, shared (2 referencers) = {shared_delete_reads}"
    );

    assert_eq!(total_rows(&shared_rows), 17, "20 rows - 3 deleted = 17");
    let ids = ids_of(&shared_rows);
    for missing in [102, 105, 201] {
        assert!(
            !ids.contains(&missing),
            "id {missing} must be deleted by the shared partition delete file: {ids:?}"
        );
    }
    assert!(ids.contains(&100), "f0's other rows must survive: {ids:?}");
    assert!(ids.contains(&200), "f1's other rows must survive: {ids:?}");

    let _ = std::fs::remove_dir_all(&dir);
}

/// A current-thread runtime fires timers only once it parks, so every read admitted in one
/// scheduling wave bumps the peak before any delay elapses.
const DELETE_READ_DELAY: Duration = Duration::from_millis(50);

/// A deadlocked limiter fails here rather than hanging CI.
const DELETE_READ_TIMEOUT: Duration = Duration::from_secs(30);

fn tracking_store_with_probe(needles: Vec<String>) -> (Arc<TrackingStore>, Arc<AtomicUsize>) {
    let peak = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(TrackingStore {
        inner: Arc::new(LocalFileSystem::new()),
        gets: Arc::new(std::sync::Mutex::new(Vec::new())),
        calls: Arc::new(AtomicUsize::new(0)),
        concurrency: Some(ConcurrencyProbe {
            needles,
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak: Arc::clone(&peak),
            delay: DELETE_READ_DELAY,
        }),
    });
    (store, peak)
}

/// Scenario: concurrent delete-file reads peak at exactly the connection budget
#[test]
fn scan_delete_reads_bounded_by_connection_budget() {
    const BUDGET: usize = 3;
    const UNIQUE_DELETES: usize = 6;

    let dir = temp_dir("bounded_budget");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..12).collect::<Vec<_>>(), 4);

    let mut delete_refs = Vec::with_capacity(UNIQUE_DELETES);
    let mut needles = Vec::with_capacity(UNIQUE_DELETES);
    for i in 0..UNIQUE_DELETES {
        let name = format!("del_{i}.parquet");
        let url = write_delete_parquet(&dir, &name, &[(&data_url, i as i64)]);
        delete_refs.push(delete_ref(&url));
        needles.push(name);
    }

    let entry = FileEntry::with_deletes(data_url.clone(), local_file_size(&data_url), delete_refs);
    let mut spec = scan_spec(vec![entry], None, None);
    spec.common.s3_max_connections = BUDGET;

    let (store, peak) = tracking_store_with_probe(needles);
    let rows = block_on(async {
        tokio::time::timeout(
            DELETE_READ_TIMEOUT,
            try_run_scan_with_store(&spec, &data_url, store, &id_name_emits()),
        )
        .await
        .expect("bounded delete-read fan-out must finish within the timeout, not hang")
        .expect("raw scan must succeed")
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        BUDGET,
        "concurrent delete-file reads must peak at EXACTLY the connection budget ({BUDGET}): \
         a lower peak means the fan-out was not exercised, a higher peak means the bound leaked"
    );

    assert_eq!(total_rows(&rows), 6, "6 of 12 rows deleted");
    assert_eq!(ids_of(&rows), (6..12).collect::<Vec<_>>());

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a connection budget of 1 serializes delete-file reads
#[test]
fn scan_delete_reads_serial_when_budget_is_one() {
    const UNIQUE_DELETES: usize = 4;

    let dir = temp_dir("serial_budget");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 4);

    let mut delete_refs = Vec::with_capacity(UNIQUE_DELETES);
    let mut needles = Vec::with_capacity(UNIQUE_DELETES);
    for i in 0..UNIQUE_DELETES {
        let name = format!("del_{i}.parquet");
        let url = write_delete_parquet(&dir, &name, &[(&data_url, i as i64)]);
        delete_refs.push(delete_ref(&url));
        needles.push(name);
    }

    let entry = FileEntry::with_deletes(data_url.clone(), local_file_size(&data_url), delete_refs);
    let mut spec = scan_spec(vec![entry], None, None);
    spec.common.s3_max_connections = 1;

    let (store, peak) = tracking_store_with_probe(needles);
    let rows = block_on(async {
        tokio::time::timeout(
            DELETE_READ_TIMEOUT,
            try_run_scan_with_store(&spec, &data_url, store, &id_name_emits()),
        )
        .await
        .expect("serial delete reads must finish within the timeout, not hang")
        .expect("raw scan must succeed")
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "a budget of 1 must serialize delete-file reads (peak in-flight == 1)"
    );

    assert_eq!(total_rows(&rows), 6, "4 of 10 rows deleted");
    assert_eq!(ids_of(&rows), (4..10).collect::<Vec<_>>());

    let _ = std::fs::remove_dir_all(&dir);
}

/// Join sides must use disjoint column names: the join path relies on the VS guarantee.
fn write_keyed_parquet(
    dir: &Path,
    relative: &str,
    key_col: &str,
    data_col: &str,
    keys: &[i64],
    row_group: usize,
) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new(key_col, DataType::Int64, false),
        Field::new(data_col, DataType::Utf8, false),
    ]));
    let path = dir.join(relative);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let data: Vec<String> = keys.iter().map(|k| format!("{data_col}-{k}")).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(StringArray::from(data)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

/// Scenario: both join sides' delete-file reads share one connection budget
#[test]
fn scan_delete_reads_bounded_across_join_sides() {
    // N=3 with 2 delete files per side: a per-side semaphore would peak at 4. Delete positions
    // are disjoint per side so a dropped dimension-side delete shows up in the row count.
    const BUDGET: usize = 3;

    let dir = temp_dir("join_shared_budget");
    let orders_url = write_keyed_parquet(
        &dir,
        "orders.parquet",
        "o_key",
        "o_data",
        &(0..8).collect::<Vec<_>>(),
        4,
    );
    let customer_url = write_keyed_parquet(
        &dir,
        "customer.parquet",
        "c_key",
        "c_data",
        &(0..8).collect::<Vec<_>>(),
        4,
    );

    let mut needles = Vec::new();
    let mut fact_deletes = Vec::new();
    for i in 0..2 {
        let name = format!("fact_del_{i}.parquet");
        let url = write_delete_parquet(&dir, &name, &[(&orders_url, i as i64)]);
        fact_deletes.push(delete_ref(&url));
        needles.push(name);
    }
    let mut dim_deletes = Vec::new();
    for i in 0..2 {
        let name = format!("dim_del_{i}.parquet");
        let url = write_delete_parquet(&dir, &name, &[(&customer_url, (2 + i) as i64)]);
        dim_deletes.push(delete_ref(&url));
        needles.push(name);
    }

    let fact_entry = FileEntry::with_deletes(
        orders_url.clone(),
        local_file_size(&orders_url),
        fact_deletes,
    );
    let dim_entry = FileEntry::with_deletes(
        customer_url.clone(),
        local_file_size(&customer_url),
        dim_deletes,
    );

    let mut spec = scan_spec(vec![fact_entry], None, None);
    spec.common.projection = vec!["O_KEY".into(), "C_DATA".into()];
    spec.common.s3_max_connections = BUDGET;
    spec.common.join = Some(JoinSpec {
        table_root: String::new(),
        files: vec![dim_entry],
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"C_KEY\" = \"O_KEY\"".into(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage: ScanStorage::Inline(dummy_storage()),
    });

    let (store, peak) = tracking_store_with_probe(needles);
    let rows = block_on(async {
        let mut ctx = scan_fixture::BatchCapturingCtx::declaring(
            TestContext::scalar(vec![]),
            &[ExaType::Int64, scan_fixture::varchar()],
        );
        let mut config = session_config_for_spec(&spec);
        // A single-core runner would otherwise serialize the leaves and pass vacuously.
        config.options_mut().execution.planning_concurrency = 2;
        let session = SessionContext::new_with_config(config);
        session
            .runtime_env()
            .register_object_store(&Url::parse(&orders_url).expect("register url"), store);
        let mut timers = PhaseTimers::start();
        tokio::time::timeout(
            DELETE_READ_TIMEOUT,
            run_join_scan_with_session(
                &mut ctx,
                &session,
                &spec,
                &scan_fixture::resolved_storage(&spec),
                &mut timers,
            ),
        )
        .await
        .expect("join delete-read fan-out must finish within the timeout, not hang")
        .expect("join scan must succeed");
        ctx.into_batches()
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        BUDGET,
        "delete-file reads across BOTH join sides must peak at EXACTLY the shared budget \
         ({BUDGET}): a peak of {BUDGET} proves one shared limiter caps both sides; a peak above \
         {BUDGET} (up to 4) would mean each provider built its own size-{BUDGET} semaphore"
    );

    // Fact {2..7} ∩ dimension {0,1,4..7} = {4..7}.
    assert_eq!(
        total_rows(&rows),
        4,
        "inner join over post-delete rows (fact keys 2..8, dimension keys 0,1,4..8) \
         yields 4 rows — this would be 6 if the dimension side's own delete file \
         were never applied"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Sums bytes of non-HEAD reads matching `needle`: skipped row groups transfer no column data.
#[derive(Debug)]
struct RangeBytesStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    matched_bytes: Arc<AtomicUsize>,
}

impl std::fmt::Display for RangeBytesStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeBytesStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RangeBytesStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let matches = !options.head && location.as_ref().contains(self.needle.as_str());
        let result = self.inner.get_opts(location, options).await?;
        if matches {
            let len = (result.range.end - result.range.start) as usize;
            self.matched_bytes.fetch_add(len, Ordering::SeqCst);
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectStorePath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectStorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn file_needle(abs_url: &str) -> String {
    let path = Url::parse(abs_url)
        .expect("valid file URL")
        .to_file_path()
        .expect("file:// URL");
    path.file_name()
        .expect("file has a name")
        .to_string_lossy()
        .to_string()
}

/// Pass `None` for `truncate_length` to match parquet-java, which does not truncate stats.
fn write_delete_parquet_shaped(
    dir: &Path,
    relative: &str,
    entries: &[(&str, i64)],
    row_group_size: usize,
    statistics_enabled: bool,
    truncate_length: Option<usize>,
) -> String {
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
    let stats_level = if statistics_enabled {
        EnabledStatistics::Chunk
    } else {
        EnabledStatistics::None
    };
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_size))
        .set_statistics_enabled(stats_level)
        .set_statistics_truncate_length(truncate_length)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
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
    Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

/// Sorted by `(file_path, pos)` as Iceberg requires.
fn one_row_group_per_file_entries(files: [&str; 3], rows_per_file: usize) -> Vec<(String, i64)> {
    let mut entries = Vec::with_capacity(files.len() * rows_per_file);
    for path in files {
        for pos in 0..rows_per_file {
            entries.push((path.to_string(), pos as i64));
        }
    }
    entries
}

/// Scenario: delete-file row groups for unassigned data files are pruned by `file_path` stats
#[test]
fn scan_prunes_delete_row_groups_by_file_path() {
    // Large enough that row-group data dwarfs the footer bytes the counter also captures.
    const ROWS_PER_FILE: usize = 500;

    let dir = temp_dir("row_group_pruning");
    let f1 = write_data_parquet(&dir, "f1.parquet", &(0..5).collect::<Vec<_>>(), 8);
    let f2 = write_data_parquet(&dir, "f2.parquet", &(1000..1005).collect::<Vec<_>>(), 8);
    let f3 = write_data_parquet(&dir, "f3.parquet", &(2000..2005).collect::<Vec<_>>(), 8);

    let entries = one_row_group_per_file_entries([&f1, &f2, &f3], ROWS_PER_FILE);
    let entry_refs: Vec<(&str, i64)> = entries.iter().map(|(p, pos)| (p.as_str(), *pos)).collect();
    let delete_url = write_delete_parquet_shaped(
        &dir,
        "shared_delete.parquet",
        &entry_refs,
        ROWS_PER_FILE,
        true,
        None,
    );
    let shared_delete = delete_ref(&delete_url);
    let needle = file_needle(&delete_url);

    let pruned_entries = vec![FileEntry::with_deletes(
        f2.clone(),
        local_file_size(&f2),
        vec![shared_delete.clone()],
    )];
    let pruned_spec = scan_spec(pruned_entries, None, None);
    let pruned_bytes = Arc::new(AtomicUsize::new(0));
    let pruned_store = Arc::new(RangeBytesStore {
        inner: Arc::new(LocalFileSystem::new()),
        needle: needle.clone(),
        matched_bytes: Arc::clone(&pruned_bytes),
    });
    let pruned_rows = block_on(try_run_scan_with_store(
        &pruned_spec,
        &f2,
        pruned_store,
        &id_name_emits(),
    ))
    .expect("pruned scan must succeed");

    let full_entries = vec![
        FileEntry::with_deletes(
            f1.clone(),
            local_file_size(&f1),
            vec![shared_delete.clone()],
        ),
        FileEntry::with_deletes(
            f2.clone(),
            local_file_size(&f2),
            vec![shared_delete.clone()],
        ),
        FileEntry::with_deletes(f3.clone(), local_file_size(&f3), vec![shared_delete]),
    ];
    let full_spec = scan_spec(full_entries, None, None);
    let full_bytes = Arc::new(AtomicUsize::new(0));
    let full_store = Arc::new(RangeBytesStore {
        inner: Arc::new(LocalFileSystem::new()),
        needle,
        matched_bytes: Arc::clone(&full_bytes),
    });
    block_on(try_run_scan_with_store(
        &full_spec,
        &f1,
        full_store,
        &id_name_emits(),
    ))
    .expect("full scan must succeed");

    let pruned_total = pruned_bytes.load(Ordering::SeqCst);
    let full_total = full_bytes.load(Ordering::SeqCst);
    assert!(
        pruned_total > 0,
        "the pruned scan must still fetch its own assigned file's row group"
    );
    assert!(
        pruned_total < full_total,
        "assigning only 1 of 3 files must decode fewer delete-file bytes than assigning all 3: \
         pruned={pruned_total} full={full_total}"
    );
    assert!(
        pruned_total * 2 < full_total,
        "pruning 2 of 3 disjoint row groups should cut delete-file bytes by more than half: \
         pruned={pruned_total} full={full_total}"
    );

    assert_eq!(
        total_rows(&pruned_rows),
        0,
        "all of f2's rows must be deleted, pruning notwithstanding"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: truncated `file_path` statistics never prune the assigned file's own row group
#[test]
fn scan_prunes_delete_row_groups_with_truncated_statistics() {
    const ROWS_PER_FILE: usize = 20;

    let dir = temp_dir("truncated_stats");
    let long_prefix =
        "warehouse/analytics/very/long/namespace/orders/table/data/nested/deeply/for/truncation";
    assert!(
        long_prefix.len() > 64,
        "shared prefix must exceed the 64-byte statistics-truncation length"
    );

    let f_a = write_data_parquet(
        &dir,
        &format!("{long_prefix}/part-00001-order-events.parquet"),
        &(0..5).collect::<Vec<_>>(),
        8,
    );
    let f_b = write_data_parquet(
        &dir,
        &format!("{long_prefix}/part-00002-order-events.parquet"),
        &(1000..1005).collect::<Vec<_>>(),
        8,
    );
    let f_c = write_data_parquet(
        &dir,
        &format!("{long_prefix}/part-00003-order-events.parquet"),
        &(2000..2005).collect::<Vec<_>>(),
        8,
    );

    let entries = one_row_group_per_file_entries([&f_a, &f_b, &f_c], ROWS_PER_FILE);
    let entry_refs: Vec<(&str, i64)> = entries.iter().map(|(p, pos)| (p.as_str(), *pos)).collect();
    let delete_url = write_delete_parquet_shaped(
        &dir,
        "truncated_delete.parquet",
        &entry_refs,
        ROWS_PER_FILE,
        true,
        Some(64),
    );

    let entry = FileEntry::with_deletes(
        f_b.clone(),
        local_file_size(&f_b),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &f_b);

    assert_eq!(
        total_rows(&rows),
        0,
        "truncated file_path statistics must not cause the assigned file's own row group to be \
         wrongly pruned: expected all 5 of f_b's rows deleted, got {} surviving",
        total_rows(&rows)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: delete-file row groups without `file_path` statistics are all decoded, never pruned
#[test]
fn scan_decodes_all_row_groups_when_file_path_statistics_absent() {
    const ROWS_PER_FILE: usize = 100;

    let dir = temp_dir("no_stats");
    let f1 = write_data_parquet(&dir, "f1.parquet", &(0..5).collect::<Vec<_>>(), 8);
    let f2 = write_data_parquet(&dir, "f2.parquet", &(1000..1005).collect::<Vec<_>>(), 8);
    let f3 = write_data_parquet(&dir, "f3.parquet", &(2000..2005).collect::<Vec<_>>(), 8);

    let entries = one_row_group_per_file_entries([&f1, &f2, &f3], ROWS_PER_FILE);
    let entry_refs: Vec<(&str, i64)> = entries.iter().map(|(p, pos)| (p.as_str(), *pos)).collect();
    let delete_url = write_delete_parquet_shaped(
        &dir,
        "no_stats_delete.parquet",
        &entry_refs,
        ROWS_PER_FILE,
        false,
        None,
    );
    let delete_total_size = local_file_size(&delete_url);
    let needle = file_needle(&delete_url);

    let entry = FileEntry::with_deletes(
        f2.clone(),
        local_file_size(&f2),
        vec![delete_ref(&delete_url)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let bytes = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(RangeBytesStore {
        inner: Arc::new(LocalFileSystem::new()),
        needle,
        matched_bytes: Arc::clone(&bytes),
    });
    let rows = block_on(try_run_scan_with_store(&spec, &f2, store, &id_name_emits()))
        .expect("unpruned scan must succeed");

    let fetched = bytes.load(Ordering::SeqCst);
    assert!(
        (fetched as f64) >= delete_total_size as f64 * 0.9,
        "with no file_path statistics every row group must be decoded (no pruning possible): \
         fetched {fetched} bytes of a {delete_total_size}-byte delete file"
    );

    assert_eq!(
        total_rows(&rows),
        0,
        "all of f2's rows must be deleted despite absent statistics"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

fn collect_leaf_execs(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Arc<dyn ExecutionPlan>>) {
    let children = plan.children();
    if children.is_empty() {
        out.push(Arc::clone(plan));
    } else {
        for child in children {
            collect_leaf_execs(child, out);
        }
    }
}

fn leaf_file_scan_config(
    plan: &Arc<dyn ExecutionPlan>,
) -> datafusion::datasource::physical_plan::FileScanConfig {
    let mut leaves = Vec::new();
    collect_leaf_execs(plan, &mut leaves);
    assert_eq!(leaves.len(), 1, "raw scan plan must have exactly one leaf");
    let (file_scan_config, _parquet_source) = leaves[0]
        .downcast_ref::<DataSourceExec>()
        .expect("leaf must be a DataSourceExec")
        .downcast_to_file_source::<ParquetSource>()
        .expect("leaf must be backed by a ParquetSource");
    file_scan_config.clone()
}

/// Scenario: concurrent footer fetches peak at exactly the connection budget
#[test]
fn scan_footer_fetches_bounded_by_connection_budget() {
    // The plan is built but never executed: execute-time reads of the needled data files hold no
    // permit and would inflate the peak.
    const BUDGET: usize = 3;
    const DATA_FILES: usize = 6;

    let dir = temp_dir("footer_bounded_budget");

    let mut entries = Vec::with_capacity(DATA_FILES);
    let mut needles = Vec::with_capacity(DATA_FILES);
    let mut data_urls = Vec::with_capacity(DATA_FILES);
    for i in 0..DATA_FILES {
        let data_name = format!("data_{i}.parquet");
        let data_url = write_data_parquet(&dir, &data_name, &(0..10).collect::<Vec<_>>(), 4);
        let delete_url = write_delete_parquet(&dir, &format!("del_{i}.parquet"), &[(&data_url, 0)]);
        entries.push(FileEntry::with_deletes(
            data_url.clone(),
            local_file_size(&data_url),
            vec![delete_ref(&delete_url)],
        ));
        needles.push(file_needle(&data_url));
        data_urls.push(data_url);
    }

    let mut spec = scan_spec_with_logical_schema(entries, None, None);
    spec.common.s3_max_connections = BUDGET;

    let (store, peak) = tracking_store_with_probe(needles.clone());

    let plan = block_on(async {
        let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
        ctx.runtime_env()
            .register_object_store(&Url::parse(&data_urls[0]).expect("register url"), store);
        register_files(
            &ctx,
            "scan_target",
            &spec,
            &scan_fixture::resolved_storage(&spec),
        )
        .await
        .expect("register_files must succeed");
        tokio::time::timeout(
            DELETE_READ_TIMEOUT,
            build_raw_scan_physical_plan(&ctx, &spec),
        )
        .await
        .expect("plan construction must finish within the timeout, not hang")
        .expect("physical plan must build")
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        BUDGET,
        "concurrent footer fetches must peak at EXACTLY the connection budget ({BUDGET}): \
         a lower peak means the fan-out never ran, a higher peak means the bound leaked"
    );

    let file_scan_config = leaf_file_scan_config(&plan);
    assert_eq!(
        file_scan_config.file_groups.len(),
        1,
        "a single-shard raw scan must produce exactly one file group"
    );
    let group_locations: Vec<String> = file_scan_config.file_groups[0]
        .iter()
        .map(|f| f.object_meta.location.as_ref().to_string())
        .collect();
    assert_eq!(
        group_locations.len(),
        DATA_FILES,
        "the file group must list every assigned data file"
    );
    for (i, needle) in needles.iter().enumerate() {
        assert!(
            group_locations[i].contains(needle.as_str()),
            "file group entry {i} must be {needle} (spec order), got {}: {group_locations:?}",
            group_locations[i]
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a mixed shard fetches footers only for delete-carrying files, once each
#[test]
fn scan_mixed_shard_fetches_footers_only_for_delete_carrying_files() {
    // The plan is never executed so execute-time reads do not contaminate the counts.
    let dir = temp_dir("mixed_shard_footers");

    let free_a = write_data_parquet(&dir, "free_a.parquet", &(0..10).collect::<Vec<_>>(), 4);
    let carrying_b =
        write_data_parquet(&dir, "carrying_b.parquet", &(0..10).collect::<Vec<_>>(), 4);
    let del_b = write_delete_parquet(&dir, "del_b.parquet", &[(&carrying_b, 0)]);
    let free_c = write_data_parquet(&dir, "free_c.parquet", &(0..10).collect::<Vec<_>>(), 4);
    let carrying_d =
        write_data_parquet(&dir, "carrying_d.parquet", &(0..10).collect::<Vec<_>>(), 4);
    let del_d = write_delete_parquet(&dir, "del_d.parquet", &[(&carrying_d, 0)]);

    let entries = vec![
        FileEntry::new(free_a.clone(), local_file_size(&free_a)),
        FileEntry::with_deletes(
            carrying_b.clone(),
            local_file_size(&carrying_b),
            vec![delete_ref(&del_b)],
        ),
        FileEntry::new(free_c.clone(), local_file_size(&free_c)),
        FileEntry::with_deletes(
            carrying_d.clone(),
            local_file_size(&carrying_d),
            vec![delete_ref(&del_d)],
        ),
    ];
    let spec_order = [
        file_needle(&free_a),
        file_needle(&carrying_b),
        file_needle(&free_c),
        file_needle(&carrying_d),
    ];
    let delete_carrying_needles = [file_needle(&carrying_b), file_needle(&carrying_d)];
    let delete_free_needles = [file_needle(&free_a), file_needle(&free_c)];

    let spec = scan_spec_with_logical_schema(entries, None, None);

    let gets = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = Arc::new(TrackingStore {
        inner: Arc::new(LocalFileSystem::new()),
        gets: Arc::clone(&gets),
        calls: Arc::new(AtomicUsize::new(0)),
        concurrency: None,
    });

    let plan = block_on(async {
        let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
        ctx.runtime_env()
            .register_object_store(&Url::parse(&free_a).expect("register url"), store);
        register_files(
            &ctx,
            "scan_target",
            &spec,
            &scan_fixture::resolved_storage(&spec),
        )
        .await
        .expect("register_files must succeed");
        build_raw_scan_physical_plan(&ctx, &spec)
            .await
            .expect("physical plan must build")
    });

    for needle in &delete_free_needles {
        assert_eq!(
            count_gets_matching(&gets, needle.as_str()),
            0,
            "a delete-free file must cost no footer fetch of its own: {needle}"
        );
    }
    for needle in &delete_carrying_needles {
        assert_eq!(
            count_gets_matching(&gets, needle.as_str()),
            1,
            "a delete-carrying file's footer must be fetched exactly once: {needle}"
        );
    }

    let file_scan_config = leaf_file_scan_config(&plan);
    assert_eq!(
        file_scan_config.file_groups.len(),
        1,
        "a single-shard raw scan must produce exactly one file group"
    );
    let group = &file_scan_config.file_groups[0];
    assert_eq!(
        group.iter().count(),
        spec_order.len(),
        "the file group must list every assigned file"
    );
    for (i, (partitioned, needle)) in group.iter().zip(spec_order.iter()).enumerate() {
        let location = partitioned.object_meta.location.as_ref();
        assert!(
            location.contains(needle.as_str()),
            "file group entry {i} must be {needle} (spec order), got {location}"
        );
        let has_access_plan = partitioned.extension::<ParquetAccessPlan>().is_some();
        let expect_access_plan = delete_carrying_needles.contains(needle);
        assert_eq!(
            has_access_plan, expect_access_plan,
            "file group entry {i} ({needle}) access-plan presence must match its delete-carrying status"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: both join sides' footer fetches share one connection budget
#[test]
fn scan_footer_fetches_bounded_across_join_sides() {
    // N=3 with 2 files per side: a per-side semaphore would peak at 4, sequential planning at 2.
    // The plan is never executed (execute-time reads hold no permit), and both sides need a
    // `logical_schema` so schema inference does not add an unpermitted GET.
    const BUDGET: usize = 3;
    const FILES_PER_SIDE: usize = 2;

    let dir = temp_dir("join_footer_shared_budget");

    let mut needles = Vec::with_capacity(2 * FILES_PER_SIDE);
    let mut fact_entries = Vec::with_capacity(FILES_PER_SIDE);
    let mut dim_entries = Vec::with_capacity(FILES_PER_SIDE);
    let mut first_data_url = None;
    for i in 0..FILES_PER_SIDE {
        let keys: Vec<i64> = ((i as i64) * 8..(i as i64) * 8 + 8).collect();
        let orders_url = write_keyed_parquet(
            &dir,
            &format!("orders_{i}.parquet"),
            "o_key",
            "o_data",
            &keys,
            4,
        );
        let customer_url = write_keyed_parquet(
            &dir,
            &format!("customer_{i}.parquet"),
            "c_key",
            "c_data",
            &keys,
            4,
        );
        let fact_delete_url =
            write_delete_parquet(&dir, &format!("fact_del_{i}.parquet"), &[(&orders_url, 0)]);
        let dim_delete_url =
            write_delete_parquet(&dir, &format!("dim_del_{i}.parquet"), &[(&customer_url, 0)]);

        needles.push(file_needle(&orders_url));
        needles.push(file_needle(&customer_url));
        first_data_url.get_or_insert_with(|| orders_url.clone());
        fact_entries.push(FileEntry::with_deletes(
            orders_url.clone(),
            local_file_size(&orders_url),
            vec![delete_ref(&fact_delete_url)],
        ));
        dim_entries.push(FileEntry::with_deletes(
            customer_url.clone(),
            local_file_size(&customer_url),
            vec![delete_ref(&dim_delete_url)],
        ));
    }
    let register_url = first_data_url.expect("at least one data file per side");

    let mut spec = scan_spec_with_logical_schema(fact_entries, None, None);
    spec.common.projection = vec!["O_KEY".into(), "C_DATA".into()];
    spec.common.logical_schema = logical_fields(&[("o_key", "int64"), ("o_data", "utf8")]);
    spec.common.s3_max_connections = BUDGET;
    spec.common.join = Some(JoinSpec {
        table_root: String::new(),
        files: dim_entries,
        logical_schema: logical_fields(&[("c_key", "int64"), ("c_data", "utf8")]),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: "\"C_KEY\" = \"O_KEY\"".into(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage: ScanStorage::Inline(dummy_storage()),
    });

    let (store, peak) = tracking_store_with_probe(needles);

    block_on(async {
        let mut config = session_config_for_spec(&spec);
        // A single-core runner would otherwise serialize the leaves and pass vacuously.
        config.options_mut().execution.planning_concurrency = 2;
        let session = SessionContext::new_with_config(config);
        session
            .runtime_env()
            .register_object_store(&Url::parse(&register_url).expect("register url"), store);
        tokio::time::timeout(
            DELETE_READ_TIMEOUT,
            build_join_physical_plan(&session, &spec, &scan_fixture::resolved_storage(&spec)),
        )
        .await
        .expect("join plan construction must finish within the timeout, not hang")
        .expect("join physical plan must build");
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        BUDGET,
        "data-file footer fetches across BOTH join sides must peak at EXACTLY the shared budget \
         ({BUDGET}): a peak of {BUDGET} proves one shared limiter caps both sides' Phase B, a \
         lower peak means the fan-out never ran, and a peak above {BUDGET} (up to 4) would mean \
         each provider built its own size-{BUDGET} semaphore"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

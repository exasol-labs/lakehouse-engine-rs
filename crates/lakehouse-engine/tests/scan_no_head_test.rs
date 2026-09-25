mod scan_fixture;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::ExaType;
use futures::StreamExt;
use futures::stream::BoxStream;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, DeleteMechanism, FileEntry, LogicalField, ScanSpec, ScanStorage,
    StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{
    build_raw_scan_physical_plan, register_files, run_raw_scan_with_session,
    session_config_for_spec,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use url::Url;

/// Iceberg reserved field-ids; duplicated because the engine's constants are `pub(crate)`.
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

#[derive(Clone, Debug, Default)]
struct HeadCounts {
    from_spec: Arc<AtomicUsize>,
    to_inner: Arc<AtomicUsize>,
}

impl HeadCounts {
    fn served_from_spec(&self) -> usize {
        self.from_spec.load(Ordering::SeqCst)
    }
    fn forwarded_to_inner(&self) -> usize {
        self.to_inner.load(Ordering::SeqCst)
    }
}

/// Mirrors the production `SpecSizedObjectStore`: a `head` for a location in `sizes` is
/// answered without inner I/O. `object_store` dispatches `head` as `get_opts(head: true)`.
#[derive(Debug)]
struct CountingHeadStore {
    inner: Arc<dyn ObjectStore>,
    sizes: HashMap<ObjectStorePath, u64>,
    counts: HeadCounts,
}

impl CountingHeadStore {
    fn new(
        inner: Arc<dyn ObjectStore>,
        sizes: HashMap<ObjectStorePath, u64>,
        counts: HeadCounts,
    ) -> Self {
        Self {
            inner,
            sizes,
            counts,
        }
    }
}

impl std::fmt::Display for CountingHeadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingHeadStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingHeadStore {
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
        if options.head {
            if let Some(&size) = self.sizes.get(location) {
                self.counts.from_spec.fetch_add(1, Ordering::SeqCst);
                let meta = ObjectMeta {
                    location: location.clone(),
                    last_modified: Utc.timestamp_nanos(0),
                    size,
                    e_tag: None,
                    version: None,
                };
                return Ok(GetResult {
                    payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
                    meta,
                    range: 0..0,
                    attributes: object_store::Attributes::default(),
                });
            }
            self.counts.to_inner.fetch_add(1, Ordering::SeqCst);
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

fn raw_spec(files: Vec<(String, u64)>, table_root: String) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root,
            projection: vec!["ID".into(), "NAME".into()],
            storage: ScanStorage::Inline(dummy_storage()),
            df_batch_size: 64,
            ..Default::default()
        },
        files: files.into_iter().map(FileEntry::from).collect(),
    }
}

/// Request-count assertions must use this, not [`raw_spec`]: an empty `logical_schema`
/// triggers schema inference, which pre-caches the first file's footer and hides
/// Phase B's real round-trips.
fn raw_spec_with_logical_schema(files: Vec<(String, u64)>, table_root: String) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root,
            projection: vec!["ID".into(), "NAME".into()],
            storage: ScanStorage::Inline(dummy_storage()),
            df_batch_size: 64,
            logical_schema: vec![
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
                    name: "name".to_string(),
                    arrow_type: "utf8".to_string(),
                    nullable: false,
                    initial_default: None,
                    nested: None,
                    physical_name: None,
                },
            ],
            ..Default::default()
        },
        files: files.into_iter().map(FileEntry::from).collect(),
    }
}

fn wide_logical_fields(columns: usize) -> Vec<LogicalField> {
    (0..columns)
        .map(|i| LogicalField {
            field_id: Some((i + 1) as i32),
            name: match i {
                0 => "id".to_string(),
                1 => "name".to_string(),
                _ => format!("c{i}"),
            },
            arrow_type: if i == 1 { "utf8" } else { "int64" }.to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        })
        .collect()
}

/// The wide logical schema makes the cached footer large while the two-column projection
/// keeps execute-time reads cheap.
fn raw_spec_with_wide_logical_schema(
    files: Vec<(String, u64)>,
    table_root: String,
    columns: usize,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root,
            projection: vec!["ID".into(), "NAME".into()],
            storage: ScanStorage::Inline(dummy_storage()),
            df_batch_size: 64,
            logical_schema: wide_logical_fields(columns),
            ..Default::default()
        },
        files: files.into_iter().map(FileEntry::from).collect(),
    }
}

fn write_local_parquet(dir: &std::path::Path, relative: &str, rows: i64) -> String {
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
        .set_max_row_group_row_count(Some(64))
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

/// `columns × row_groups` sets the cached footer's `memory_size()`, which the session
/// `FileMetadataCache` charges against its limit; columns past `id`/`name` are padding.
fn write_wide_local_parquet(
    dir: &std::path::Path,
    relative: &str,
    columns: usize,
    row_groups: usize,
    rows_per_row_group: usize,
) -> String {
    assert!(
        columns >= 2,
        "the fixture's first two columns are id + name"
    );
    let fields: Vec<Field> = (0..columns)
        .map(|i| match i {
            0 => Field::new("id", DataType::Int64, false),
            1 => Field::new("name", DataType::Utf8, false),
            _ => Field::new(format!("c{i}"), DataType::Int64, false),
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let path = dir.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dir");
    }
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_row_group))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");

    let rows = (row_groups * rows_per_row_group) as i64;
    let ids: Vec<i64> = (0..rows).collect();
    let names: Vec<String> = (0..rows).map(|i| format!("row-{i}")).collect();
    let mut arrays: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(names)),
    ];
    for i in 2..columns {
        arrays.push(Arc::new(Int64Array::from(
            (0..rows).map(|r| r + i as i64).collect::<Vec<i64>>(),
        )));
    }
    let batch = RecordBatch::try_new(schema, arrays).expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

/// Must match the key production `build_spec_size_index` uses.
fn head_key(abs_file_url: &str) -> ObjectStorePath {
    ListingTableUrl::parse(abs_file_url)
        .expect("listing url")
        .prefix()
        .clone()
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

/// `(location, is_head, range)`.
type LoggedRequest = (ObjectStorePath, bool, Option<GetRange>);

/// Logs every request and answers HEADs from `sizes` without inner I/O.
#[derive(Debug)]
struct RequestLoggingStore {
    inner: Arc<dyn ObjectStore>,
    sizes: HashMap<ObjectStorePath, u64>,
    log: Arc<std::sync::Mutex<Vec<LoggedRequest>>>,
}

impl std::fmt::Display for RequestLoggingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RequestLoggingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RequestLoggingStore {
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
        self.log
            .lock()
            .unwrap()
            .push((location.clone(), options.head, options.range.clone()));
        if options.head
            && let Some(&size) = self.sizes.get(location)
        {
            let meta = ObjectMeta {
                location: location.clone(),
                last_modified: Utc.timestamp_nanos(0),
                size,
                e_tag: None,
                version: None,
            };
            return Ok(GetResult {
                payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
                meta,
                range: 0..0,
                attributes: object_store::Attributes::default(),
            });
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

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

async fn run_scan_with_store(
    spec: &ScanSpec,
    register_url: &str,
    store: Arc<dyn ObjectStore>,
) -> Vec<RecordBatch> {
    run_scan_capturing_session(spec, register_url, store)
        .await
        .0
}

/// Returns the session too: the `FileMetadataCache` is per-session.
async fn run_scan_capturing_session(
    spec: &ScanSpec,
    register_url: &str,
    store: Arc<dyn ObjectStore>,
) -> (Vec<RecordBatch>, SessionContext) {
    let session = SessionContext::new_with_config(session_config_for_spec(spec));
    session
        .runtime_env()
        .register_object_store(&Url::parse(register_url).expect("register url"), store);
    let mut ctx = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(vec![]),
        &[ExaType::Int64, scan_fixture::varchar()],
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
    .expect("raw scan must succeed");
    (ctx.into_batches(), session)
}

fn rows_of(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id col");
        let names = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name col");
        for i in 0..b.num_rows() {
            out.push((ids.value(i), names.value(i).to_string()));
        }
    }
    out
}

/// Scenario: a spec-supplied file size serves the per-file HEAD with no store request and identical rows
#[test]
fn scan_uses_spec_size_and_issues_no_head() {
    let dir = std::env::temp_dir().join(format!("lh_no_head_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file_url = write_local_parquet(&dir, "size_data.parquet", 200);
    let real_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    let discover_counts = HeadCounts::default();
    let discover_store = Arc::new(CountingHeadStore::new(
        Arc::new(LocalFileSystem::new()),
        HashMap::new(),
        discover_counts.clone(),
    ));
    let discover_spec = raw_spec(vec![(file_url.clone(), real_size)], String::new());
    let discovered = block_on(run_scan_with_store(
        &discover_spec,
        &file_url,
        discover_store,
    ));
    assert!(
        discover_counts.forwarded_to_inner() >= 1,
        "discovery scan must issue at least one HEAD to the inner store (got {})",
        discover_counts.forwarded_to_inner()
    );
    assert_eq!(
        discover_counts.served_from_spec(),
        0,
        "discovery scan must not answer any HEAD from a spec size"
    );

    let spec_counts = HeadCounts::default();
    let mut sizes = HashMap::new();
    sizes.insert(head_key(&file_url), real_size);
    let spec_store = Arc::new(CountingHeadStore::new(
        Arc::new(LocalFileSystem::new()),
        sizes,
        spec_counts.clone(),
    ));
    let spec_spec = raw_spec(vec![(file_url.clone(), real_size)], String::new());
    let via_spec = block_on(run_scan_with_store(&spec_spec, &file_url, spec_store));

    assert!(
        spec_counts.served_from_spec() >= 1,
        "spec-size scan must answer the per-file HEAD from the spec size (got {})",
        spec_counts.served_from_spec()
    );
    assert_eq!(
        spec_counts.forwarded_to_inner(),
        0,
        "spec-size scan must issue NO HEAD to the wrapped store (got {})",
        spec_counts.forwarded_to_inner()
    );

    let discovered_rows = rows_of(&discovered);
    let spec_rows = rows_of(&via_spec);
    assert_eq!(discovered_rows.len(), 200, "row count");
    assert_eq!(
        spec_rows, discovered_rows,
        "spec-size scan must return rows identical to the discovery-based scan"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a relative entry joined onto `table_root` resolves to the same file as the absolute entry
#[test]
fn relative_and_absolute_entries_resolve_to_same_files() {
    let dir = std::env::temp_dir().join(format!("lh_rel_abs_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let abs_url = write_local_parquet(&dir, "data/f.parquet", 150);
    let real_size = std::fs::metadata(abs_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();
    let table_root = url::Url::from_file_path(&dir).expect("dir url").to_string();

    let abs_spec = raw_spec(vec![(abs_url.clone(), real_size)], String::new());
    let rel_spec = raw_spec(
        vec![("data/f.parquet".to_string(), real_size)],
        table_root.clone(),
    );

    assert_eq!(
        head_key(&abs_url),
        head_key(&format!(
            "{}/data/f.parquet",
            table_root.strip_suffix('/').unwrap_or(&table_root)
        )),
        "relative entry must reconstruct to the same absolute file"
    );

    let abs_rows = rows_of(&block_on(run_scan_with_store(
        &abs_spec,
        &abs_url,
        Arc::new(LocalFileSystem::new()),
    )));
    let rel_rows = rows_of(&block_on(run_scan_with_store(
        &rel_spec,
        &abs_url,
        Arc::new(LocalFileSystem::new()),
    )));

    assert_eq!(abs_rows.len(), 150, "row count");
    assert_eq!(
        rel_rows, abs_rows,
        "relative-entry + table_root scan must return the same rows as the absolute-entry scan"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a delete-carrying scan issues no HEAD for its positional-delete file
#[test]
fn scan_issues_no_head_for_delete_files() {
    let dir = std::env::temp_dir().join(format!("lh_no_head_del_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let data_url = write_local_parquet(&dir, "data.parquet", 40);
    let data_size = std::fs::metadata(data_url.strip_prefix("file://").unwrap())
        .expect("stat data parquet")
        .len();
    let delete_url = write_delete_parquet(&dir, "deletes.parquet", &[(&data_url, 3)]);
    let delete_size = std::fs::metadata(delete_url.strip_prefix("file://").unwrap())
        .expect("stat delete parquet")
        .len();

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        data_size,
        vec![DeleteMechanism::IcebergPositionalDelete {
            path: delete_url.clone(),
            size: delete_size,
        }],
    );
    let spec = raw_spec(vec![], String::new());
    let mut spec = spec;
    spec.files = vec![entry];

    let mut sizes = HashMap::new();
    sizes.insert(head_key(&data_url), data_size);
    sizes.insert(head_key(&delete_url), delete_size);
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = Arc::new(RequestLoggingStore {
        inner: Arc::new(LocalFileSystem::new()),
        sizes,
        log: Arc::clone(&log),
    });

    let rows = block_on(run_scan_with_store(&spec, &data_url, store));
    assert_eq!(rows_of(&rows).len(), 39, "1 row deleted out of 40");

    let recorded = log.lock().unwrap();
    let delete_key = head_key(&delete_url);
    let delete_head_calls = recorded
        .iter()
        .filter(|(loc, head, _)| *head && *loc == delete_key)
        .count();
    assert_eq!(
        delete_head_calls, 0,
        "the positional-delete file must never receive an object-store HEAD: {recorded:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: attaching a positional-delete file adds no GET against the data file's own footer
#[test]
fn scan_reads_footer_via_range_get_once() {
    let dir = std::env::temp_dir().join(format!("lh_footer_once_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let baseline_url = write_local_parquet(&dir, "baseline/data.parquet", 200);
    let baseline_size = std::fs::metadata(baseline_url.strip_prefix("file://").unwrap())
        .expect("stat baseline parquet")
        .len();
    let delta_url = write_local_parquet(&dir, "delta/data.parquet", 200);
    let delta_size = std::fs::metadata(delta_url.strip_prefix("file://").unwrap())
        .expect("stat delta parquet")
        .len();
    // One deleted position, so every row group is still opened as in the baseline.
    let delete_url = write_delete_parquet(&dir, "delta/deletes.parquet", &[(&delta_url, 5)]);
    let delete_size = std::fs::metadata(delete_url.strip_prefix("file://").unwrap())
        .expect("stat delete parquet")
        .len();

    let baseline_entry = FileEntry::new(baseline_url.clone(), baseline_size);
    let delta_entry = FileEntry::with_deletes(
        delta_url.clone(),
        delta_size,
        vec![DeleteMechanism::IcebergPositionalDelete {
            path: delete_url.clone(),
            size: delete_size,
        }],
    );

    let mut baseline_spec = raw_spec_with_logical_schema(vec![], String::new());
    baseline_spec.files = vec![baseline_entry];
    let mut delta_spec = raw_spec_with_logical_schema(vec![], String::new());
    delta_spec.files = vec![delta_entry];

    let baseline_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let baseline_store = Arc::new(RequestLoggingStore {
        inner: Arc::new(LocalFileSystem::new()),
        sizes: HashMap::from([(head_key(&baseline_url), baseline_size)]),
        log: Arc::clone(&baseline_log),
    });
    let baseline_rows = block_on(run_scan_with_store(
        &baseline_spec,
        &baseline_url,
        baseline_store,
    ));
    assert_eq!(
        rows_of(&baseline_rows).len(),
        200,
        "baseline has no deletes"
    );

    let delta_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let delta_store = Arc::new(RequestLoggingStore {
        inner: Arc::new(LocalFileSystem::new()),
        sizes: HashMap::from([
            (head_key(&delta_url), delta_size),
            (head_key(&delete_url), delete_size),
        ]),
        log: Arc::clone(&delta_log),
    });
    let delta_rows = block_on(run_scan_with_store(&delta_spec, &delta_url, delta_store));
    assert_eq!(rows_of(&delta_rows).len(), 199, "1 row deleted out of 200");

    let baseline_data_calls: Vec<Option<GetRange>> = baseline_log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, head, _)| !head)
        .map(|(_, _, range)| range.clone())
        .collect();
    let delta_data_calls: Vec<Option<GetRange>> = delta_log
        .lock()
        .unwrap()
        .iter()
        .filter(|(loc, head, _)| !head && *loc == head_key(&delta_url))
        .map(|(_, _, range)| range.clone())
        .collect();
    assert_eq!(
        delta_data_calls, baseline_data_calls,
        "attaching a positional delete must not add any extra GET against the data file's own \
         footer/content (shared FileMetadataCache => footer parsed once): baseline={baseline_data_calls:?} delta={delta_data_calls:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: plan construction fetches a delete-carrying data file's footer with one bounded suffix GET
#[test]
fn scan_access_plan_footer_fetch_is_one_range_get() {
    // More than one request means Phase B lost the metadata size hint or `PageIndexPolicy::Skip`.
    let dir = std::env::temp_dir().join(format!("lh_access_plan_footer_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let data_url = write_local_parquet(&dir, "data.parquet", 200);
    let data_size = std::fs::metadata(data_url.strip_prefix("file://").unwrap())
        .expect("stat data parquet")
        .len();
    let delete_url = write_delete_parquet(&dir, "deletes.parquet", &[(&data_url, 5)]);
    let delete_size = std::fs::metadata(delete_url.strip_prefix("file://").unwrap())
        .expect("stat delete parquet")
        .len();

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        data_size,
        vec![DeleteMechanism::IcebergPositionalDelete {
            path: delete_url.clone(),
            size: delete_size,
        }],
    );
    let mut spec = raw_spec_with_logical_schema(vec![], String::new());
    spec.files = vec![entry];

    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = Arc::new(RequestLoggingStore {
        inner: Arc::new(LocalFileSystem::new()),
        sizes: HashMap::from([
            (head_key(&data_url), data_size),
            (head_key(&delete_url), delete_size),
        ]),
        log: Arc::clone(&log),
    });

    let session = SessionContext::new_with_config(session_config_for_spec(&spec));
    session
        .runtime_env()
        .register_object_store(&Url::parse(&data_url).expect("register url"), store);

    block_on(async {
        register_files(
            &session,
            "scan_target",
            &spec,
            &scan_fixture::resolved_storage(&spec),
        )
        .await
        .expect("register_files must succeed on the delete-carrying data file");
        // Never execute: the opener's reads would contaminate the log.
        build_raw_scan_physical_plan(&session, &spec)
            .await
            .expect("physical plan must build");
    });

    let data_key = head_key(&data_url);
    let data_calls: Vec<Option<GetRange>> = log
        .lock()
        .unwrap()
        .iter()
        .filter(|(loc, head, _)| !head && *loc == data_key)
        .map(|(_, _, range)| range.clone())
        .collect();

    assert_eq!(
        data_calls.len(),
        1,
        "plan construction alone must fetch the delete-carrying data file's footer with \
         EXACTLY ONE non-HEAD request, not an unhinted probe-then-metadata(-then-page-index) \
         sequence: {data_calls:?}"
    );
    match &data_calls[0] {
        Some(GetRange::Bounded(range)) => {
            assert_eq!(
                range.end, data_size,
                "the single footer fetch must be a suffix range ending at the file size"
            );
            assert!(
                range.end - range.start > 8,
                "must be the hinted suffix range, not the 8-byte footer-length probe: {range:?}"
            );
        }
        other => panic!("expected a bounded suffix range for the footer fetch, got {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: footer reuse holds at shard scale with the metadata cache near its eviction limit (#165)
#[test]
fn scan_footer_reuse_holds_at_shard_scale() {
    // The fixture is calibrated (decision-log [6]) so 22 footers of 64 columns × 64 row groups
    // fill ~78% of `DEFAULT_METADATA_CACHE_LIMIT`; at 29 files the cache evicts and the GET
    // equality fails. The band assertion catches drift in the limit or `memory_size()`.
    use datafusion::execution::cache::cache_manager::DEFAULT_METADATA_CACHE_LIMIT;
    use std::collections::HashSet;

    const COLUMNS: usize = 64;
    const ROW_GROUPS: usize = 64;
    const ROWS_PER_ROW_GROUP: usize = 4;
    const SHARD_FILE_COUNT: usize = 22;
    const BAND_LOW_PERCENT: usize = 70;
    const BAND_HIGH_PERCENT: usize = 90;

    let dir = std::env::temp_dir().join(format!("lh_footer_shard_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let stat = |url: &str| {
        std::fs::metadata(url.strip_prefix("file://").unwrap())
            .expect("stat fixture parquet")
            .len()
    };

    let mut data_files: Vec<(String, u64)> = Vec::with_capacity(SHARD_FILE_COUNT);
    let mut delete_files: Vec<(String, u64)> = Vec::with_capacity(SHARD_FILE_COUNT);
    for i in 0..SHARD_FILE_COUNT {
        let data_url = write_wide_local_parquet(
            &dir,
            &format!("data/f{i}.parquet"),
            COLUMNS,
            ROW_GROUPS,
            ROWS_PER_ROW_GROUP,
        );
        // One position only, so no row group is fully deleted and both runs open every group.
        let delete_url =
            write_delete_parquet(&dir, &format!("deletes/d{i}.parquet"), &[(&data_url, 0)]);
        data_files.push((data_url.clone(), stat(&data_url)));
        delete_files.push((delete_url.clone(), stat(&delete_url)));
    }

    let sizes: HashMap<ObjectStorePath, u64> = data_files
        .iter()
        .chain(delete_files.iter())
        .map(|(url, size)| (head_key(url), *size))
        .collect();
    let data_keys: HashSet<ObjectStorePath> =
        data_files.iter().map(|(url, _)| head_key(url)).collect();
    let register_url = data_files[0].0.clone();
    let rows_per_file = ROW_GROUPS * ROWS_PER_ROW_GROUP;

    let data_file_gets = |log: &Arc<std::sync::Mutex<Vec<LoggedRequest>>>| {
        log.lock()
            .unwrap()
            .iter()
            .filter(|(loc, head, _)| !head && data_keys.contains(loc))
            .count()
    };

    let mut free_spec = raw_spec_with_wide_logical_schema(vec![], String::new(), COLUMNS);
    free_spec.files = data_files
        .iter()
        .map(|(url, size)| FileEntry::new(url.clone(), *size))
        .collect();
    let free_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let free_store = Arc::new(RequestLoggingStore {
        inner: Arc::new(LocalFileSystem::new()),
        sizes: sizes.clone(),
        log: Arc::clone(&free_log),
    });
    let free_rows = block_on(run_scan_with_store(&free_spec, &register_url, free_store));
    assert_eq!(
        rows_of(&free_rows).len(),
        SHARD_FILE_COUNT * rows_per_file,
        "the delete-free run must return every fixture row"
    );

    let mut delta_spec = raw_spec_with_wide_logical_schema(vec![], String::new(), COLUMNS);
    delta_spec.files = data_files
        .iter()
        .zip(delete_files.iter())
        .map(|((data_url, data_size), (delete_url, delete_size))| {
            FileEntry::with_deletes(
                data_url.clone(),
                *data_size,
                vec![DeleteMechanism::IcebergPositionalDelete {
                    path: delete_url.clone(),
                    size: *delete_size,
                }],
            )
        })
        .collect();
    let delta_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let delta_store = Arc::new(RequestLoggingStore {
        inner: Arc::new(LocalFileSystem::new()),
        sizes,
        log: Arc::clone(&delta_log),
    });
    let (delta_rows, delta_session) = block_on(run_scan_capturing_session(
        &delta_spec,
        &register_url,
        delta_store,
    ));
    assert_eq!(
        rows_of(&delta_rows).len(),
        SHARD_FILE_COUNT * (rows_per_file - 1),
        "one position deleted per data file"
    );

    let entries = delta_session
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache()
        .list_entries();
    let cached: Vec<(usize, usize)> = data_keys
        .iter()
        .filter_map(|key| entries.get(key).map(|e| (e.size_bytes, e.hits)))
        .collect();
    let aggregate: usize = cached.iter().map(|(size, _)| size).sum();
    let per_entry_bytes = cached.first().map(|(size, _)| *size).unwrap_or(0);

    assert_eq!(
        data_file_gets(&delta_log),
        data_file_gets(&free_log),
        "a delete-carrying shard must issue the SAME number of non-HEAD data-file GETs as the \
         delete-free shard over the same {SHARD_FILE_COUNT} files: any excess is a footer that \
         access-plan construction cached and the opener could not read back"
    );

    assert_eq!(
        cached.len(),
        SHARD_FILE_COUNT,
        "every data-file footer access-plan construction cached must still be cached after the \
         scan; a missing one was evicted (or never admitted) and re-fetched"
    );
    assert!(
        cached.iter().all(|(_, hits)| *hits >= 1),
        "each cached footer must have been READ BACK at least once — the opener's own lookup; \
         a zero-hit entry means the opener missed and re-`put` it: {cached:?}"
    );

    let band_low = DEFAULT_METADATA_CACHE_LIMIT * BAND_LOW_PERCENT / 100;
    let band_high = DEFAULT_METADATA_CACHE_LIMIT * BAND_HIGH_PERCENT / 100;
    assert!(
        aggregate >= band_low && aggregate <= band_high,
        "the fixture is calibrated so the {SHARD_FILE_COUNT} cached footers occupy \
         {BAND_LOW_PERCENT}-{BAND_HIGH_PERCENT}% of DEFAULT_METADATA_CACHE_LIMIT \
         ({DEFAULT_METADATA_CACHE_LIMIT} bytes) — near enough the eviction cliff for the reuse \
         assertion above to be able to fail for eviction. Measured {aggregate} bytes \
         ({per_entry_bytes} per entry × {SHARD_FILE_COUNT}), expected {band_low}..={band_high}. \
         Re-calibrate COLUMNS / ROW_GROUPS / SHARD_FILE_COUNT against a fresh `list_entries()` \
         measurement and record the new numbers in decision-log [6]"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

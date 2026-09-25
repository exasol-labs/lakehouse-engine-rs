//! A footer re-fetch from an evicted or never-admitted `FileMetadataCache` entry must be
//! countable via `footer_refetch_count`, and nothing else may count as one. The recorded
//! footer set is process-global, so both tests serialize on one lock.

mod scan_fixture;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::ExaType;
use futures::StreamExt;
use futures::stream::BoxStream;
use lakehouse_engine::scan::diagnostics::{
    OpenerCoverage, PhaseTimers, footer_refetch_count, reset_access_plan_cached_footers,
};
use lakehouse_engine::scan::spec::{
    CommonScanSpec, DeleteMechanism, FileEntry, LogicalField, ScanSpec, ScanStorage,
    StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{
    ResolvedScanStorage, run_raw_scan_with_session, run_scan_one, session_config_for_spec,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use url::Url;

/// Iceberg reserved field-ids; duplicated because the engine's constants are `pub(crate)`.
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

/// Below any footer's `memory_size()`, so `put` declines every entry: no reliance on LRU order.
const TINY_CACHE_LIMIT_BYTES: usize = 100;

type LoggedRequest = (ObjectStorePath, bool, Option<GetRange>);

/// Logs every request and answers HEADs from a size map with no inner I/O.
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

/// A non-empty logical schema avoids schema inference, which would fetch and cache the footer
/// before Phase B and make the request counts vacuous.
fn raw_spec_with_logical_schema(table_root: String) -> ScanSpec {
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
        files: Vec::new(),
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
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
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

fn data_key(abs_file_url: &str) -> ObjectStorePath {
    use datafusion::datasource::listing::ListingTableUrl;
    ListingTableUrl::parse(abs_file_url)
        .expect("listing url")
        .prefix()
        .clone()
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

/// Counts bounded suffix GETs ending at `size` and wider than the 8-byte footer-length probe:
/// the shape `fetch_metadata` issues under this scan's hint configuration.
fn footer_shaped_get_count(
    log: &Arc<std::sync::Mutex<Vec<LoggedRequest>>>,
    key: &ObjectStorePath,
    size: u64,
) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|(loc, head, range)| {
            !head
                && loc == key
                && matches!(range, Some(GetRange::Bounded(r)) if r.end == size && r.end - r.start > 8)
        })
        .count()
}

/// The footer record is a process-global set; poison is recovered so a failing test reports
/// its own assertion rather than a poison panic in its sibling.
fn serialize_footer_record() -> std::sync::MutexGuard<'static, ()> {
    static FOOTER_RECORD: std::sync::Mutex<()> = std::sync::Mutex::new(());
    FOOTER_RECORD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Scenario: a metadata-cache eviction that re-fetches a footer is observable
#[test]
fn scan_footer_refetch_is_observable_when_the_cache_evicts() {
    // The three runs share one function so they cannot interleave over the process-global record.
    let _serialized = serialize_footer_record();
    reset_access_plan_cached_footers();

    let dir = std::env::temp_dir().join(format!("lh_footer_refetch_{}", std::process::id()));
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
    let mut spec = raw_spec_with_logical_schema(String::new());
    spec.files = vec![entry];

    let data_key_path = data_key(&data_url);
    let sizes: HashMap<ObjectStorePath, u64> = HashMap::from([
        (data_key_path.clone(), data_size),
        (data_key(&delete_url), delete_size),
    ]);

    // Run 1: the tiny cache limit forces a re-fetch.
    let evict_runtime = RuntimeEnvBuilder::new()
        .with_metadata_cache_limit(TINY_CACHE_LIMIT_BYTES)
        .build_arc()
        .expect("build runtime env with tiny metadata-cache limit");
    let evict_session =
        SessionContext::new_with_config_rt(session_config_for_spec(&spec), evict_runtime);
    let evict_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let evict_store = Arc::new(RequestLoggingStore {
        inner: Arc::new(LocalFileSystem::new()),
        sizes: sizes.clone(),
        log: Arc::clone(&evict_log),
    });
    evict_session
        .runtime_env()
        .register_object_store(&Url::parse(&data_url).expect("register url"), evict_store);
    let mut evict_ctx = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(vec![]),
        &[ExaType::Int64, scan_fixture::varchar()],
    );
    let mut evict_timers = PhaseTimers::start();
    block_on(run_raw_scan_with_session(
        &mut evict_ctx,
        &evict_session,
        &spec,
        &scan_fixture::resolved_storage(&spec),
        &mut evict_timers,
    ))
    .expect("delete-carrying scan must succeed even when the metadata cache evicts");
    assert_eq!(
        evict_ctx
            .batches()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        39,
        "1 row deleted out of 40"
    );

    let evict_entries = evict_session
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache()
        .list_entries();
    assert!(
        footer_refetch_count(&evict_entries, OpenerCoverage::EveryAssignedFile) >= 1,
        "a footer the tiny-limit cache could never admit must count as at least one re-fetch"
    );
    assert_eq!(
        footer_shaped_get_count(&evict_log, &data_key_path, data_size),
        2,
        "with no working cache, Phase B's own footer fetch and the opener's later footer fetch \
         must each issue their own hinted range GET against the data file: {:?}",
        evict_log.lock().unwrap()
    );

    // Run 2: default cache limit; the observable must not fire on every delete-carrying scan.
    let default_session = SessionContext::new_with_config(session_config_for_spec(&spec));
    let default_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let default_store = Arc::new(RequestLoggingStore {
        inner: Arc::new(LocalFileSystem::new()),
        sizes,
        log: Arc::clone(&default_log),
    });
    default_session
        .runtime_env()
        .register_object_store(&Url::parse(&data_url).expect("register url"), default_store);
    let mut default_ctx = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(vec![]),
        &[ExaType::Int64, scan_fixture::varchar()],
    );
    let mut default_timers = PhaseTimers::start();
    block_on(run_raw_scan_with_session(
        &mut default_ctx,
        &default_session,
        &spec,
        &scan_fixture::resolved_storage(&spec),
        &mut default_timers,
    ))
    .expect("delete-carrying scan must succeed under the default cache limit");
    assert_eq!(
        default_ctx
            .batches()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        39,
        "1 row deleted out of 40"
    );

    let default_entries = default_session
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache()
        .list_entries();
    assert_eq!(
        footer_refetch_count(&default_entries, OpenerCoverage::EveryAssignedFile),
        0,
        "under the default cache limit the opener must read the footer back from the cache, \
         not re-fetch it — a nonzero count here means the observable fires on every scan"
    );
    assert_eq!(
        footer_shaped_get_count(&default_log, &data_key_path, data_size),
        1,
        "under the default cache limit access-plan construction's footer fetch must be the ONLY \
         footer-shaped GET against the data file: {:?}",
        default_log.lock().unwrap()
    );

    // Run 3: `LIMIT 1` leaves later footers unopened (`hits == 0`) though none was re-fetched.
    reset_access_plan_cached_footers();
    let limit_urls: Vec<String> = (0..4)
        .map(|i| write_local_parquet(&dir, &format!("limit_data_{i}.parquet"), 40))
        .collect();
    let limit_sizes: Vec<u64> = limit_urls
        .iter()
        .map(|u| {
            std::fs::metadata(u.strip_prefix("file://").unwrap())
                .expect("stat data parquet")
                .len()
        })
        .collect();
    let limit_delete_entries: Vec<(&str, i64)> =
        limit_urls.iter().map(|u| (u.as_str(), 3i64)).collect();
    let limit_delete_url =
        write_delete_parquet(&dir, "limit_deletes.parquet", &limit_delete_entries);
    let limit_delete_size = std::fs::metadata(limit_delete_url.strip_prefix("file://").unwrap())
        .expect("stat delete parquet")
        .len();
    let mut limit_spec = raw_spec_with_logical_schema(String::new());
    limit_spec.common.limit = Some(1);
    limit_spec.files = limit_urls
        .iter()
        .zip(&limit_sizes)
        .map(|(url, size)| {
            FileEntry::with_deletes(
                url.clone(),
                *size,
                vec![DeleteMechanism::IcebergPositionalDelete {
                    path: limit_delete_url.clone(),
                    size: limit_delete_size,
                }],
            )
        })
        .collect();
    let limit_session = SessionContext::new_with_config(session_config_for_spec(&limit_spec));
    let mut limit_ctx = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(vec![]),
        &[ExaType::Int64, scan_fixture::varchar()],
    );
    let mut limit_timers = PhaseTimers::start();
    block_on(run_raw_scan_with_session(
        &mut limit_ctx,
        &limit_session,
        &limit_spec,
        &scan_fixture::resolved_storage(&limit_spec),
        &mut limit_timers,
    ))
    .expect("delete-carrying scan with a pushed LIMIT must succeed");
    assert_eq!(
        limit_ctx
            .batches()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        1,
        "the pushed LIMIT 1 must reach the scan"
    );
    let limit_entries = limit_session
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache()
        .list_entries();
    assert!(
        footer_refetch_count(&limit_entries, OpenerCoverage::EveryAssignedFile) > 0,
        "premise: at least one recorded footer must sit at `hits == 0` because the pushed LIMIT \
         kept the opener from ever opening its file — without that this run asserts nothing"
    );
    assert_eq!(
        footer_refetch_count(&limit_entries, OpenerCoverage::MayStopEarly),
        0,
        "a footer the opener never opened was fetched once, not twice: a scan shape that can stop \
         early must report ZERO re-fetches, not one per unopened file"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: the invocation-start reset keeps a pooled process from reporting an earlier invocation's footers
#[test]
fn scan_dispatch_resets_the_footer_record_between_invocations() {
    let _serialized = serialize_footer_record();
    reset_access_plan_cached_footers();

    let dir = std::env::temp_dir().join(format!("lh_footer_reset_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let data_a = write_local_parquet(&dir, "reset_data_a.parquet", 40);
    let data_b = write_local_parquet(&dir, "reset_data_b.parquet", 40);
    let delete_a = write_delete_parquet(&dir, "reset_deletes_a.parquet", &[(&data_a, 3)]);
    let delete_b = write_delete_parquet(&dir, "reset_deletes_b.parquet", &[(&data_b, 7)]);
    let file_size = |url: &str| {
        std::fs::metadata(url.strip_prefix("file://").unwrap())
            .expect("stat parquet")
            .len()
    };
    let sizes: HashMap<ObjectStorePath, u64> = [&data_a, &data_b, &delete_a, &delete_b]
        .iter()
        .map(|url| (data_key(url), file_size(url)))
        .collect();

    // Captured before `run_scan_one` drops it; the `Arc`'d cache outlives the drop.
    let captured: std::sync::Mutex<Vec<SessionContext>> = std::sync::Mutex::new(Vec::new());
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store_url = Url::parse(&data_a).expect("register url");
    let build_session =
        |spec: &ScanSpec, _storage: &ResolvedScanStorage, _memory_limit_bytes: u64| {
            let session = SessionContext::new_with_config(session_config_for_spec(spec));
            session.runtime_env().register_object_store(
                &store_url,
                Arc::new(RequestLoggingStore {
                    inner: Arc::new(LocalFileSystem::new()),
                    sizes: sizes.clone(),
                    log: Arc::clone(&log),
                }),
            );
            captured.lock().unwrap().push(session.clone());
            Ok(session)
        };

    let spec_for = |data_url: &str, delete_url: &str| {
        let mut spec = raw_spec_with_logical_schema(String::new());
        spec.files = vec![FileEntry::with_deletes(
            data_url.to_string(),
            file_size(data_url),
            vec![DeleteMechanism::IcebergPositionalDelete {
                path: delete_url.to_string(),
                size: file_size(delete_url),
            }],
        )];
        spec
    };

    let mut ctx_a = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(vec![]),
        &[ExaType::Int64, scan_fixture::varchar()],
    );
    block_on(run_scan_one(
        &mut ctx_a,
        spec_for(&data_a, &delete_a),
        &scan_fixture::resolved_storage(&spec_for(&data_a, &delete_a)),
        &build_session,
    ))
    .expect("invocation 1 over file A must succeed");
    assert_eq!(
        ctx_a.batches().iter().map(|b| b.num_rows()).sum::<usize>(),
        39,
        "1 row deleted out of 40 in file A"
    );

    let mut ctx_b = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(vec![]),
        &[ExaType::Int64, scan_fixture::varchar()],
    );
    block_on(run_scan_one(
        &mut ctx_b,
        spec_for(&data_b, &delete_b),
        &scan_fixture::resolved_storage(&spec_for(&data_b, &delete_b)),
        &build_session,
    ))
    .expect("invocation 2 over file B must succeed");
    assert_eq!(
        ctx_b.batches().iter().map(|b| b.num_rows()).sum::<usize>(),
        39,
        "1 row deleted out of 40 in file B"
    );

    let sessions = captured.lock().unwrap();
    assert_eq!(
        sessions.len(),
        2,
        "each `run_scan_one` call must build exactly one session"
    );
    let data_b_key = data_key(&data_b);
    let data_b_size = file_size(&data_b);
    assert_eq!(
        footer_shaped_get_count(&log, &data_b_key, data_b_size),
        1,
        "invocation 2's own footer must stay cached, so a nonzero count below can only come from \
         invocation 1's record: {:?}",
        log.lock().unwrap()
    );
    let entries = sessions[1]
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache()
        .list_entries();
    assert_eq!(
        footer_refetch_count(&entries, OpenerCoverage::EveryAssignedFile),
        0,
        "invocation 2 must report only its OWN footers: file A's recorded path is absent from \
         this session's cache, so without the invocation-start reset it counts as a phantom \
         re-fetch"
    );

    drop(sessions);
    let _ = std::fs::remove_dir_all(&dir);
}

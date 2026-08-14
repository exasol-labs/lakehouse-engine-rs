//! Host integration tests for the metadata-cache footer re-fetch observable
//! (task 1.7b): a re-fetch caused by an evicted (or never-admitted) session
//! `FileMetadataCache` entry must be countable via `footer_refetch_count`, not
//! silent — and nothing else may be counted as one.
//!
//! Kept in its OWN file, holding TWO test functions that between them cover
//! three things: the eviction signal itself, the limit-pushdown FALSE POSITIVE
//! (a footer the opener never opened is not a re-fetch), and the
//! invocation-start reset of the recorded set in `run_scan_dispatch`.
//! `scan::diagnostics` records access-plan-cached footer paths in a
//! PROCESS-GLOBAL set (there is no per-session handle for it), so both tests
//! take `serialize_footer_record`'s lock for their whole body and clear the set
//! on entry — no sibling test may run against that global concurrently, and no
//! foreign leftovers may reach a count.
//!
//! The miss is forced deterministically rather than by scale: the first run
//! builds its session with a metadata-cache limit of a few bytes
//! (`RuntimeEnvBuilder::with_metadata_cache_limit`), well under any real
//! Parquet footer's `memory_size()`, so `DefaultFilesMetadataCacheState::put`
//! declines every entry outright — the deterministic "never admitted" half of
//! "evicted, or never admitted" (`datafusion-execution-54.1.0/src/cache/
//! file_metadata_cache.rs:69-73`). The second run re-runs the identical scan on
//! a fresh session with the DEFAULT cache limit, and the third pushes a
//! `LIMIT 1` over four delete-carrying files so the opener provably leaves
//! footers unopened.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use futures::StreamExt;
use futures::stream::BoxStream;
use lakehouse_engine::scan::diagnostics::{
    OpenerCoverage, PhaseTimers, footer_refetch_count, reset_access_plan_cached_footers,
};
use lakehouse_engine::scan::spec::{
    CommonScanSpec, DeleteMechanism, FileEntry, LogicalField, ScanSpec, StorageBackend,
    StorageProps,
};
use lakehouse_engine::scan::{run_raw_scan_with_session, run_scan_one, session_config_for_spec};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use url::Url;

/// Iceberg reserved field-ids for a positional-delete file's `file_path`/`pos`
/// columns (mirrors `scan::positional_deletes`'s private constants; duplicated
/// here since this integration test cannot import a `pub(crate)` item — same
/// duplication `scan_no_head_test.rs` already carries).
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

/// Metadata-cache limit forced small enough that `DefaultFilesMetadataCacheState
/// ::put` declines every footer entry outright (`value_size > memory_limit`),
/// well under any real Parquet footer's `memory_size()` — deterministic, no
/// reliance on LRU eviction ordering.
const TINY_CACHE_LIMIT_BYTES: usize = 100;

/// A fake `UdfContext` serving one input row and decoding every emitted Arrow
/// IPC batch — the same capture pattern `scan_no_head_test.rs`'s `FakeCtx` uses.
struct FakeCtx {
    served: bool,
    emitted: Vec<RecordBatch>,
}

impl FakeCtx {
    fn new() -> Self {
        Self {
            served: false,
            emitted: Vec::new(),
        }
    }
}

impl UdfContext for FakeCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&Value, UdfError> {
        Err(UdfError::User("FakeCtx has no input columns".into()))
    }
    fn get_string(&self, _col: usize) -> Result<Option<&str>, UdfError> {
        Ok(None)
    }
    fn emit(&mut self, _values: &[Value]) -> Result<(), UdfError> {
        Err(UdfError::User("raw path must use emit_batch".into()))
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        if self.served {
            Ok(false)
        } else {
            self.served = true;
            Ok(true)
        }
    }
    fn debug_level(&self) -> tracing::Level {
        tracing::Level::INFO
    }
    fn emit_record_batch_ipc(&mut self, ipc: &[u8]) -> Result<(), UdfError> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;
        let reader = StreamReader::try_new(Cursor::new(ipc), None)
            .map_err(|e| UdfError::User(format!("ipc decode: {e}")))?;
        for batch in reader {
            let batch = batch.map_err(|e| UdfError::User(format!("ipc batch: {e}")))?;
            self.emitted.push(batch);
        }
        Ok(())
    }
}

/// One logged request: the location, whether it was a HEAD, and the byte
/// range requested (if any).
type LoggedRequest = (ObjectStorePath, bool, Option<GetRange>);

/// An [`ObjectStore`] decorator that records the location of every request
/// (HEAD or GET, with its byte range) into a shared log and answers every HEAD
/// from a caller-supplied size map with NO inner I/O — mirrors
/// `scan_no_head_test.rs`'s `RequestLoggingStore`.
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

/// Storage props are never dialed for a local `file://` scan; a placeholder
/// keeps the spec well-formed.
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

/// Build a raw-scan `ScanSpec` with a non-empty `common.logical_schema`
/// matching `write_local_parquet`'s fixture (`id` field-id 1 `int64`, `name`
/// field-id 2 `utf8`) — load-bearing exactly as `scan_no_head_test.rs`'s
/// `raw_spec_with_logical_schema` documents: an empty logical schema would
/// route registration through `ParquetFormat::infer_schema`, which fetches and
/// caches the footer BEFORE Phase B ever runs, making this test's request
/// counts vacuous.
fn raw_spec_with_logical_schema(table_root: String) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root,
            projection: vec!["ID".into(), "NAME".into()],
            storage: dummy_storage(),
            df_batch_size: 64,
            logical_schema: vec![
                LogicalField {
                    field_id: Some(1),
                    name: "id".to_string(),
                    arrow_type: "int64".to_string(),
                    nullable: false,
                    initial_default: None,
                    physical_name: None,
                },
                LogicalField {
                    field_id: Some(2),
                    name: "name".to_string(),
                    arrow_type: "utf8".to_string(),
                    nullable: false,
                    initial_default: None,
                    physical_name: None,
                },
            ],
            ..Default::default()
        },
        files: Vec::new(),
    }
}

/// Write a local Parquet at `dir/relative` (creating parent dirs) with `rows`
/// rows across small row groups. Returns the file's absolute `file://` URL.
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

/// Write a local positional-delete Parquet at `dir/relative`: `file_path`/`pos`
/// columns tagged with the Iceberg reserved field-ids, one row per
/// `(referenced_file_abs_url, position)` entry. Returns the file's absolute
/// `file://` URL.
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

/// The object-store `Path` DataFusion resolves an exact-file URL to — the same
/// key production request logging keys HEAD/GET calls by.
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

/// Count the non-HEAD `get_opts` calls in `log` against `key` whose range is a
/// bounded suffix ending at `size` and wider than the 8-byte footer-length
/// probe — the shape `DFParquetMetadata::fetch_metadata` issues under the
/// hint/page-index-skip configuration this scan uses, whether served from the
/// cache or fetched fresh (mirrors the shape asserted in
/// `scan_no_head_test.rs::scan_access_plan_footer_fetch_is_one_range_get`).
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

/// Serialize the two tests in this binary and hand back the guard they hold for
/// their whole body. `scan::diagnostics` keeps the record of access-plan-cached
/// footer paths in a PROCESS-GLOBAL set with no per-session handle, so on
/// cargo's default parallel test threads each test would see the other's
/// recorded paths. The workspace carries no `serial_test` dev-dependency and
/// this file adds none — one `std::sync::Mutex` is the whole mechanism. A
/// poisoned lock is recovered from rather than propagated, so a failing test
/// reports its own assertion instead of a misleading poison panic in its
/// sibling.
fn serialize_footer_record() -> std::sync::MutexGuard<'static, ()> {
    static FOOTER_RECORD: std::sync::Mutex<()> = std::sync::Mutex::new(());
    FOOTER_RECORD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Scenario (memory-and-credentials): a metadata-cache eviction that re-fetches
/// a footer is observable — `footer_refetch_count` reports at least one
/// re-fetch when the cache cannot retain the footer access-plan construction
/// cached, reports exactly zero for the identical scan under the default cache
/// limit, and reports zero for a scan the opener could not finish opening.
///
/// All three runs live in this ONE function, in this order, so they cannot
/// interleave over the process-global record. Run 2 is the control that the
/// observable reports a LOST footer rather than firing on every
/// delete-carrying scan: its own session retains the footer its own
/// access-plan construction cached, so its count is computed purely from its
/// own entries. It says nothing about the invocation-start reset — it rescans
/// the SAME file, so a stale record would be indistinguishable from a fresh
/// one; `scan_dispatch_resets_the_footer_record_between_invocations` covers the
/// reset. Run 3 is the control against the opposite failure, a count that fires
/// on footers nothing ever re-fetched.
#[test]
fn scan_footer_refetch_is_observable_when_the_cache_evicts() {
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

    // Run 1 — a cache limit far below any real footer's memory_size() means
    // `put` declines every entry outright: Phase B's own fetch and the
    // opener's later fetch are each a fresh, uncached hinted request.
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
    let mut evict_ctx = FakeCtx::new();
    let mut evict_timers = PhaseTimers::start();
    block_on(run_raw_scan_with_session(
        &mut evict_ctx,
        &evict_session,
        &spec,
        &mut evict_timers,
    ))
    .expect("delete-carrying scan must succeed even when the metadata cache evicts");
    assert_eq!(
        evict_ctx
            .emitted
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

    // Run 2 — the IDENTICAL scan on a fresh session with the DEFAULT cache
    // limit. This session's own Phase B call re-records the same data-file
    // path, and this session's own cache retains it, so the check below is
    // computed purely from THIS run's entries — proving the observable
    // reports a lost footer rather than firing on every delete-carrying scan.
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
    let mut default_ctx = FakeCtx::new();
    let mut default_timers = PhaseTimers::start();
    block_on(run_raw_scan_with_session(
        &mut default_ctx,
        &default_session,
        &spec,
        &mut default_timers,
    ))
    .expect("delete-carrying scan must succeed under the default cache limit");
    assert_eq!(
        default_ctx
            .emitted
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

    // Run 3 — the limit-pushdown control: a pushed `LIMIT 1` over FOUR
    // delete-carrying data files. Access-plan construction fetches, caches and
    // records all four footers, but the opener stops the stream once the row
    // budget is spent and never opens the later files, leaving their entries at
    // `hits == 0` although nothing was evicted and no footer was fetched twice.
    // The record is reset first because the two runs above left this run's
    // predecessor path in the process-global set — exactly what
    // `run_scan_dispatch` does at every real invocation start.
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
    let mut limit_ctx = FakeCtx::new();
    let mut limit_timers = PhaseTimers::start();
    block_on(run_raw_scan_with_session(
        &mut limit_ctx,
        &limit_session,
        &limit_spec,
        &mut limit_timers,
    ))
    .expect("delete-carrying scan with a pushed LIMIT must succeed");
    assert_eq!(
        limit_ctx
            .emitted
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

/// Scenario (memory-and-credentials): the invocation-start reset of the
/// process-global footer record is what keeps a pooled UDF process from
/// reporting an earlier invocation's footers against a later invocation's cache.
///
/// Two sequential [`run_scan_one`] calls — the public entry that routes through
/// the private `run_scan_dispatch`, which is where the reset lives — scan
/// DIFFERENT data files, each with its own one-position delete file, each on its
/// own default-cache-limit session. Invocation 2's cache can only ever hold file
/// B's footer, so invocation 1's recorded path A is absent from it and counts as
/// an unconditional re-fetch unless the reset cleared the record first. That is
/// what the two runs of the test above cannot show: they scan the SAME file, so
/// a stale record is indistinguishable from a fresh one.
///
/// `run_scan_one` drops its session before returning, so the injected
/// `build_session` closure stashes a clone: `SessionContext` is cheaply
/// cloneable and its `RuntimeEnv` — carrying the `FileMetadataCache` — is an
/// `Arc`, so the cache outlives that drop and stays readable here.
///
/// Verified to exercise the reset rather than merely pass: with the
/// `diagnostics::reset_access_plan_cached_footers()` call in `run_scan_dispatch`
/// (`crates/lakehouse-engine/src/scan/mod.rs`) commented out, this test FAILS —
/// invocation 2 reports 1 re-fetch, file A's stale path; with the call restored
/// it PASSES.
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

    // Every invocation's session is captured here before `run_scan_one` drops
    // it, so the assertions below read invocation 2's own metadata cache.
    let captured: std::sync::Mutex<Vec<SessionContext>> = std::sync::Mutex::new(Vec::new());
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store_url = Url::parse(&data_a).expect("register url");
    let build_session = |spec: &ScanSpec, _memory_limit_bytes: u64| {
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

    let mut ctx_a = FakeCtx::new();
    block_on(run_scan_one(
        &mut ctx_a,
        spec_for(&data_a, &delete_a),
        &build_session,
    ))
    .expect("invocation 1 over file A must succeed");
    assert_eq!(
        ctx_a.emitted.iter().map(|b| b.num_rows()).sum::<usize>(),
        39,
        "1 row deleted out of 40 in file A"
    );

    let mut ctx_b = FakeCtx::new();
    block_on(run_scan_one(
        &mut ctx_b,
        spec_for(&data_b, &delete_b),
        &build_session,
    ))
    .expect("invocation 2 over file B must succeed");
    assert_eq!(
        ctx_b.emitted.iter().map(|b| b.num_rows()).sum::<usize>(),
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

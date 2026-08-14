//! Host integration tests for the reshaped `(path, size)` scan payload (task 5.1).
//!
//! Two behaviours are exercised end-to-end against a local `file://` Parquet (no
//! S3 / MinIO stack), driving the production raw-scan pipeline
//! ([`run_raw_scan_with_session`] → `build_dataframe` → `register_files`):
//!
//! 1. `scan_uses_spec_size_and_issues_no_head` — a scan whose spec carries
//!    CALLER-SUPPLIED file sizes returns rows identical to a scan that discovers
//!    the size from the store, AND its per-file metadata (`head`) lookup is
//!    satisfied from the spec size without ever reaching the wrapped store. The
//!    proof reuses the exact production mechanism the S3 wrapper uses in
//!    `scan/mod.rs`: `head` dispatches through `object_store` to
//!    `get_opts(head: true)`, which a size-carrying [`ObjectStore`] decorator
//!    intercepts. A `HashMap`-empty decorator (the discovery baseline) forwards
//!    the same `head` to the inner store; the two scans must yield identical rows.
//!
//! 2. `relative_and_absolute_entries_resolve_to_same_files` — a relative
//!    `(path, size)` entry joined onto `table_root` reconstitutes to the same
//!    absolute file as the equivalent absolute entry, and both scans emit the
//!    same rows.
//!
//! The no-network-HEAD behaviour of the production S3 wrapper is additionally
//! unit-covered in `scan/mod.rs` (`SpecSizedObjectStore::get_opts`).

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
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use futures::StreamExt;
use futures::stream::BoxStream;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, DeleteMechanism, FileEntry, LogicalField, ScanSpec, StorageBackend,
    StorageProps,
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

/// Iceberg reserved field-ids for a positional-delete file's `file_path`/`pos`
/// columns (mirrors `scan::positional_deletes`'s private constants; duplicated
/// here since this integration test cannot import a `pub(crate)` item).
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

/// A fake `UdfContext` serving one input row and decoding every emitted Arrow IPC
/// batch — the same capture pattern the sibling two-arg integration test uses.
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

/// Shared HEAD-lookup counters observed from outside a registered store.
#[derive(Clone, Debug, Default)]
struct HeadCounts {
    /// `head` requests satisfied from the caller-supplied size map (no inner I/O).
    from_spec: Arc<AtomicUsize>,
    /// `head` requests forwarded to the wrapped store.
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

/// An [`ObjectStore`] decorator that mirrors the production `SpecSizedObjectStore`
/// mechanism: a `head` (dispatched by `object_store` as `get_opts(head: true)`)
/// whose location is present in `sizes` is answered from the spec size with no I/O
/// to the inner store; every other operation delegates. When `sizes` is empty it
/// forwards all `head`s to the inner store — the discovery baseline. Both branches
/// increment the corresponding [`HeadCounts`] so a test can prove which path ran.
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

/// Storage props are never dialed for a local `file://` scan; a placeholder keeps
/// the spec well-formed.
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

/// Build a raw-scan `ScanSpec` over `files` (already `(path, size)` shaped) with
/// the given `table_root`. Row scan (no aggregates/group keys), projecting id+name
/// so the output ordering is deterministic.
fn raw_spec(files: Vec<(String, u64)>, table_root: String) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root,
            projection: vec!["ID".into(), "NAME".into()],
            storage: dummy_storage(),
            df_batch_size: 64,
            ..Default::default()
        },
        files: files.into_iter().map(FileEntry::from).collect(),
    }
}

/// Build a raw-scan `ScanSpec` exactly like [`raw_spec`], but with a non-empty
/// `common.logical_schema` matching the fixture Parquet this file writes
/// (`write_local_parquet`): `id` (field-id 1, `int64`, non-nullable) and `name`
/// (field-id 2, `utf8`, non-nullable).
///
/// This is NOT interchangeable with [`raw_spec`] for a request-count assertion.
/// `register_file_list` installs the field-id expression adapter only when
/// `logical_schema` is non-empty; an empty schema takes the
/// `ParquetFormat::infer_schema` fallback instead, which fetches and caches the
/// FIRST assigned file's footer before Phase B (`PositionalDeleteScanTable::
/// partitioned_files`) ever runs. Any later Phase-B request-count assertion then
/// observes a pure cache hit and holds regardless of how many round-trips that
/// fetch would actually cost — the exact gap decision-log [8] documents. Kept as
/// a separate helper rather than changing `raw_spec` itself: `raw_spec`'s other
/// three callers (`scan_uses_spec_size_and_issues_no_head`,
/// `relative_and_absolute_entries_resolve_to_same_files`,
/// `scan_issues_no_head_for_delete_files`) assert unrelated no-HEAD properties
/// that do not depend on which registration branch runs.
///
/// The fixture Parquet carries no Iceberg field-id metadata on `id`/`name`
/// themselves, so the installed adapter falls back to binding by name; that is
/// expected and changes no read, only which registration branch runs.
fn raw_spec_with_logical_schema(files: Vec<(String, u64)>, table_root: String) -> ScanSpec {
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
        files: files.into_iter().map(FileEntry::from).collect(),
    }
}

/// Logical fields for a `columns`-wide fixture written by
/// [`write_wide_local_parquet`]: `id` (field-id 1, `int64`), `name` (field-id 2,
/// `utf8`), then `c2 … c{columns-1}` (`int64`), field-ids assigned from 1 in
/// declaration order. Names are lowercase to match the physical Parquet, while
/// the spec's `projection` stays uppercase, exactly as
/// [`raw_spec_with_logical_schema`] already does.
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
            physical_name: None,
        })
        .collect()
}

/// Build a raw-scan `ScanSpec` over a `columns`-wide fixture: the
/// `common.logical_schema` names ALL `columns` (so `register_file_list` installs
/// the field-id adapter rather than taking the `ParquetFormat::infer_schema`
/// fallback — load-bearing for the same reason documented on
/// [`raw_spec_with_logical_schema`]), while `common.projection` stays at the two
/// columns [`rows_of`] decodes.
///
/// The asymmetry is deliberate: the wide logical schema is what makes the cached
/// `ParquetMetaData` entry large (one `ColumnChunkMetaData` per
/// `columns × row_groups`), and the narrow projection is what keeps the opener's
/// execute-time column reads cheap, so a footer-reuse assertion can be run at a
/// realistic metadata scale without also reading a realistic volume of data.
fn raw_spec_with_wide_logical_schema(
    files: Vec<(String, u64)>,
    table_root: String,
    columns: usize,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root,
            projection: vec!["ID".into(), "NAME".into()],
            storage: dummy_storage(),
            df_batch_size: 64,
            logical_schema: wide_logical_fields(columns),
            ..Default::default()
        },
        files: files.into_iter().map(FileEntry::from).collect(),
    }
}

/// Write a local Parquet at `dir/relative` (creating parent dirs) with `rows` rows
/// across small row groups. Returns the file's absolute `file://` URL.
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

/// Write a local Parquet at `dir/relative` whose FOOTER is deliberately wide:
/// `columns` columns × `row_groups` row groups × `rows_per_row_group` rows.
/// Returns the file's absolute `file://` URL.
///
/// `columns` and `row_groups` — not the row count — are the knobs that set the
/// size of the cached `ParquetMetaData` entry: the parsed footer holds one
/// `ColumnChunkMetaData` per `columns × row_groups`, and that product dominates
/// `ParquetMetaData::memory_size()`, which is exactly what the session
/// `FileMetadataCache` charges against its limit. `rows_per_row_group` is kept
/// small on purpose so a wide footer costs little actual data.
///
/// Column 0 is `id` (Int64) and column 1 is `name` (Utf8), matching
/// [`write_local_parquet`], so the `["ID", "NAME"]` projection and [`rows_of`]
/// apply unchanged; the remaining `columns - 2` are `cN` Int64 padding whose only
/// job is to widen the footer.
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

/// The object-store `Path` DataFusion passes to `head` for an exact-file URL — the
/// key production `build_spec_size_index` uses. Computed identically here.
fn head_key(abs_file_url: &str) -> ObjectStorePath {
    ListingTableUrl::parse(abs_file_url)
        .expect("listing url")
        .prefix()
        .clone()
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

/// One logged request: the location, whether it was a HEAD, and the byte
/// range requested (if any).
type LoggedRequest = (ObjectStorePath, bool, Option<GetRange>);

/// An [`ObjectStore`] decorator that records the location of every request
/// (HEAD or GET, with its byte range) into a shared log and answers every HEAD
/// from a caller-supplied size map with NO inner I/O — extending
/// [`CountingHeadStore`]'s head-interception with a full request log so a test
/// can assert exactly WHICH locations, and how many times each, were fetched.
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

/// Run the production raw scan exactly as [`run_scan_with_store`] does, but also
/// hand back the [`SessionContext`] it ran on, so a caller can interrogate that
/// session's `FileMetadataCache` (`runtime_env().cache_manager
/// .get_file_metadata_cache().list_entries()`) once the scan has finished. The
/// cache is per-session, so this is the only way to observe what the scan cached
/// and what survived to the opener.
async fn run_scan_capturing_session(
    spec: &ScanSpec,
    register_url: &str,
    store: Arc<dyn ObjectStore>,
) -> (Vec<RecordBatch>, SessionContext) {
    let session = SessionContext::new_with_config(session_config_for_spec(spec));
    session
        .runtime_env()
        .register_object_store(&Url::parse(register_url).expect("register url"), store);
    let mut ctx = FakeCtx::new();
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, spec, &mut timers)
        .await
        .expect("raw scan must succeed");
    (ctx.emitted, session)
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

/// A spec carrying the caller-supplied file size scans the same rows as a
/// discovery-based scan, and its per-file `head` is served from the spec size
/// without ever reaching the wrapped store (no network HEAD).
#[test]
fn scan_uses_spec_size_and_issues_no_head() {
    let dir = std::env::temp_dir().join(format!("lh_no_head_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file_url = write_local_parquet(&dir, "size_data.parquet", 200);
    let real_size = std::fs::metadata(file_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();

    // Discovery baseline: an empty size map forwards every `head` to the inner
    // LocalFileSystem, which reports the real size.
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

    // Spec-size path: the exact same size, but keyed into the store so the `head`
    // is served from the spec without touching the inner store.
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

    // The spec-size path produces identical, correct data.
    let discovered_rows = rows_of(&discovered);
    let spec_rows = rows_of(&via_spec);
    assert_eq!(discovered_rows.len(), 200, "row count");
    assert_eq!(
        spec_rows, discovered_rows,
        "spec-size scan must return rows identical to the discovery-based scan"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A relative `(path, size)` entry joined onto `table_root` resolves to the same
/// file as the equivalent absolute entry, and both scans emit the same rows.
#[test]
fn relative_and_absolute_entries_resolve_to_same_files() {
    let dir = std::env::temp_dir().join(format!("lh_rel_abs_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    // File lives under <dir>/data/f.parquet so the relative entry "data/f.parquet"
    // joins onto the <dir> table root to the same absolute URL.
    let abs_url = write_local_parquet(&dir, "data/f.parquet", 150);
    let real_size = std::fs::metadata(abs_url.strip_prefix("file://").unwrap())
        .expect("stat parquet")
        .len();
    let table_root = url::Url::from_file_path(&dir).expect("dir url").to_string();

    // Absolute entry, empty root (passthrough reconstruction).
    let abs_spec = raw_spec(vec![(abs_url.clone(), real_size)], String::new());
    // Relative entry joined onto table_root.
    let rel_spec = raw_spec(
        vec![("data/f.parquet".to_string(), real_size)],
        table_root.clone(),
    );

    // Sanity: both reconstitute to the identical absolute head-lookup key.
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

/// Scenario (file-metadata): a delete-carrying scan issues NO object-store HEAD
/// for its associated positional-delete file — the delete file's `ObjectMeta`
/// is built directly from the spec-supplied size (`DeleteMechanism`'s `size`), the
/// same no-HEAD mechanism `FileEntry::size` already gives data files.
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

/// Scenario (file-metadata / memory-creds): the shared session
/// `FileMetadataCache` (task 2.5) means attaching a positional-delete file to a
/// data file causes NO additional object-store GET against the DATA file
/// itself — the footer is parsed once (through the cache) and reused by both
/// the access-plan construction and the opener's own read, rather than being
/// fetched a second time.
///
/// Both specs MUST carry a non-empty `common.logical_schema`, built via
/// [`raw_spec_with_logical_schema`] rather than [`raw_spec`]. Without it,
/// `register_file_list` takes the `ParquetFormat::infer_schema` fallback, which
/// fetches and caches the delta file's footer BEFORE Phase B
/// (`PositionalDeleteScanTable::partitioned_files`) ever runs — so the
/// access-plan fetch this test cares about is then a pure cache hit, and the
/// range-equality assertion below holds regardless of how many round-trips
/// that fetch would actually have cost. Production always supplies a logical
/// schema, so Phase B is the FIRST reader of the footer there, making its
/// request shape fully load-bearing (decision-log [8]). The fixture Parquet
/// carries no Iceberg field-id metadata of its own, so the installed adapter
/// binds `id`/`name` by name fallback; that is expected and changes no read.
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
    // A single deleted position (never a whole row group) so every row group of
    // the delta file is opened identically to the baseline — isolating any
    // difference in call pattern to metadata/footer reads.
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

    // Every non-HEAD GET the delta scan issues AGAINST THE DATA FILE (i.e.
    // excluding the delete file's own, separately necessary, reads) must be
    // byte-range-identical to the baseline's data-file reads: the delete
    // file's associated access-plan construction reads the SAME footer through
    // the shared `FileMetadataCache` the opener uses, rather than fetching it a
    // second time from the object store.
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

/// Scenario (file-metadata): plan construction ALONE — before any row is
/// executed — fetches a delete-carrying data file's own Parquet footer with
/// EXACTLY ONE non-HEAD object-store request, and that request is a bounded
/// suffix range rather than the unhinted footer-length-probe shape.
///
/// Registers one delete-carrying data file through the production
/// `register_files` seam, with a non-empty `logical_schema` via
/// `raw_spec_with_logical_schema` (load-bearing for the same reason documented
/// on that helper), then calls `build_raw_scan_physical_plan` and STOPS: plan
/// construction runs Phase A (`collect_delete_positions`) and Phase B
/// (`partitioned_files`) and nothing else, so the request log at that point
/// holds only preparation reads — none of the opener's execute-time column
/// reads, which only happen once the returned plan is executed.
///
/// More than one non-HEAD request against the data file means Phase B lost
/// either the metadata size hint or the `PageIndexPolicy::Skip` and is back to
/// a probe-then-metadata(-then-page-index) sequence.
#[test]
fn scan_access_plan_footer_fetch_is_one_range_get() {
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
        register_files(&session, "scan_target", &spec)
            .await
            .expect("register_files must succeed on the delete-carrying data file");
        // STOP HERE: build the physical plan and go no further. Executing it
        // would add the opener's execute-time column reads to the same log,
        // which is the exact contamination this test exists to avoid.
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

/// Scenario (memory-and-credentials): the footer-reuse property holds at SHARD
/// SCALE, with the session `FileMetadataCache` loaded close to its eviction
/// cliff — the check issue #165 asks for in place of assuming the cache never
/// evicts.
///
/// Two scans run over the SAME `SHARD_FILE_COUNT` data files: one delete-free,
/// one where every file carries a one-position positional-delete file. The total
/// number of non-HEAD `get_opts` against the DATA-file locations must be EQUAL
/// between them. Equality is the proof: on the delete-carrying run the footer is
/// fetched by access-plan construction (Phase B) and must be READ BACK from the
/// cache by the opener, so a footer the cache failed to retain would show up as a
/// second GET the delete-free run never pays. The per-entry `hits >= 1` assertion
/// pins the same property from the cache's side.
///
/// # Why the fixture is wide rather than merely numerous
///
/// A cached entry is a parsed `ParquetMetaData` holding one
/// `ColumnChunkMetaData` per `columns × row_groups`, so `columns` and
/// `row_groups` — not the row count — set the entry size the cache charges
/// against its limit. A two-column, 64-row-group footer measures ~64 KB, so 64
/// of them occupy ~8% of the 50 MiB `DEFAULT_METADATA_CACHE_LIMIT`: the reuse
/// assertion would then be structurally unable to fail for eviction, the one
/// cause it is offered as the guard for. The shape below was CALIBRATED by
/// measurement instead (decision-log [6]): 64 columns × 64 row groups × 4 rows
/// per row group measures 1,865,550 bytes per entry, and
/// `SHARD_FILE_COUNT = 22` of them aggregate to 41,042,100 bytes — 78.3% of the
/// limit. Close enough to the cliff that seven more files, or a 22% smaller
/// limit, would evict; still far enough under it that the positive case is
/// expected to pass. Verified during calibration: at `SHARD_FILE_COUNT = 29`
/// (nominally 54,100,950 bytes) the cache evicts, and the delete-carrying run
/// pays 23 extra data-file GETs — so the equality assertion below genuinely does
/// fail for eviction rather than merely being offered as a guard against it.
///
/// # What this assertion is sensitive to
///
/// - `DEFAULT_METADATA_CACHE_LIMIT` (50 MiB). The band assertion below fails
///   loudly if the limit or `ParquetMetaData::memory_size()` changes, rather
///   than letting the fixture silently drift back under the cliff.
/// - LRU eviction on `put`. Phase B `put`s all `SHARD_FILE_COUNT` entries before
///   the opener reads any of them, so an aggregate over the limit evicts the
///   earliest-cached footers and the opener re-fetches them.
/// - An entry whose own `size_bytes` exceeds the WHOLE limit is silently never
///   cached at all — no eviction, no error, just a permanent miss. This test
///   cannot cover that case (it needs an aggregate UNDER the limit to have a
///   passing positive case); `scan_footer_refetch_observable.rs` covers it.
///
/// # Runtime cost
///
/// Deliberate, not accidental: the fixture writes 22 wide Parquet files (~20 MB)
/// and the two scans parse all 22 wide footers each, so this test costs seconds
/// rather than milliseconds. Wide footers are the whole point — a cheaper
/// fixture cannot reach the cliff this test exists to sit next to.
#[test]
fn scan_footer_reuse_holds_at_shard_scale() {
    use datafusion::execution::cache::cache_manager::DEFAULT_METADATA_CACHE_LIMIT;
    use std::collections::HashSet;

    /// Calibrated fixture shape: `COLUMNS × ROW_GROUPS` column chunks per footer.
    const COLUMNS: usize = 64;
    const ROW_GROUPS: usize = 64;
    const ROWS_PER_ROW_GROUP: usize = 4;
    /// K: the shard's delete-carrying data-file count, calibrated so the K
    /// cached footers occupy 70-90% of `DEFAULT_METADATA_CACHE_LIMIT`.
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
        // Position 0 only: one row of a 4-row row group, so no row group is ever
        // fully deleted and every row group is opened on BOTH runs — isolating
        // any difference in the request count to the footer fetch itself.
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

    // Run 1 — delete-free: the opener is the only reader of every footer.
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

    // Run 2 — every data file carries a one-position delete file, so access-plan
    // construction fetches and caches each footer and the opener must read it back.
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

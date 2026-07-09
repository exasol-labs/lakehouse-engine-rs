//! Scan-level no-container tests for Iceberg merge-on-read **Parquet positional
//! deletes** (Task 4.1).
//!
//! Every test here writes a data Parquet and a positional-delete Parquet to a
//! local temp directory (no S3 / MinIO, no Docker), hand-builds a `ScanSpec`
//! whose `FileEntry`s carry `DeleteFileRef`s, drives the production raw-scan
//! pipeline ([`run_raw_scan_with_session`] → `build_dataframe` →
//! `register_files` → `PositionalDeleteScanTable`), and asserts the deleted
//! rows are gone from the emitted output.
//!
//! Host-runnable: everything lives under `file://`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use futures::stream::BoxStream;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    DeleteFileContentType, DeleteFileRef, FileEntry, ScanSpec, StorageProps,
};
use lakehouse_engine::scan::{run_raw_scan_with_session, session_config_for_spec};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
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

/// A fake `UdfContext` serving one input row and decoding every emitted Arrow
/// IPC batch — the same capture pattern the sibling scan integration tests use.
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

/// Storage props are never dialed for a local `file://` scan; a placeholder
/// keeps the spec well-formed.
fn dummy_storage() -> StorageProps {
    StorageProps {
        endpoint: "http://localhost:9000".into(),
        region: "us-east-1".into(),
        access_key: "k".into(),
        secret_key: "s".into(),
        session_token: None,
        allow_http: true,
        path_style: true,
    }
}

/// Byte size of a local file, given its `file://` URL — robust to
/// URL-encoding (unlike a bare `strip_prefix("file://")`).
fn local_file_size(file_url: &str) -> u64 {
    let path = Url::parse(file_url)
        .expect("valid file URL")
        .to_file_path()
        .expect("file:// URL");
    std::fs::metadata(path).expect("stat local parquet").len()
}

/// Write a local data Parquet at `dir/relative` with an `id`/`name` row per
/// entry in `ids` (`name` is `row-<id>`), across small row groups so
/// multi-row-group deletes are exercised. Returns the file's absolute
/// `file://` URL.
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

/// Write a local positional-delete Parquet at `dir/relative`: `file_path`/`pos`
/// columns tagged with the Iceberg reserved field-ids, one row per
/// `(referenced_file_abs_url, position)` entry. Returns the file's absolute
/// `file://` URL.
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

/// A [`DeleteFileRef`] for the Parquet positional-delete file at `abs_url`.
fn delete_ref(abs_url: &str) -> DeleteFileRef {
    DeleteFileRef {
        path: abs_url.to_string(),
        size: local_file_size(abs_url),
        content_type: DeleteFileContentType::PositionDeletes,
    }
}

/// A row-scan `ScanSpec` over `files` (already absolute, `table_root` empty),
/// optionally pushing a filter and/or a limit.
fn scan_spec(files: Vec<FileEntry>, filter: Option<String>, limit: Option<u64>) -> ScanSpec {
    ScanSpec {
        table_root: String::new(),
        files,
        projection: vec!["ID".into(), "NAME".into()],
        filter,
        limit,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: Vec::new(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join: None,
        storage: dummy_storage(),
        df_target_partitions: 1,
        df_batch_size: 64,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

/// Run the production raw scan for `spec` against a session registering
/// `store` for every `register_url` scheme/authority. Returns the decoded
/// emitted batches, or the scan's error.
async fn try_run_scan_with_store(
    spec: &ScanSpec,
    register_url: &str,
    store: Arc<dyn ObjectStore>,
) -> Result<Vec<RecordBatch>, UdfError> {
    let session = SessionContext::new_with_config(session_config_for_spec(spec));
    session
        .runtime_env()
        .register_object_store(&Url::parse(register_url).expect("register url"), store);
    let mut ctx = FakeCtx::new();
    assert!(ctx.next().expect("next"), "one input row");
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, spec, &mut timers).await?;
    Ok(ctx.emitted)
}

/// Run the production raw scan over a plain `LocalFileSystem`, panicking on
/// scan failure (the happy-path helper used by every scenario except the
/// backstop-rejection test).
fn run_scan(spec: &ScanSpec, register_url: &str) -> Vec<RecordBatch> {
    block_on(try_run_scan_with_store(
        spec,
        register_url,
        Arc::new(LocalFileSystem::new()),
    ))
    .expect("raw scan must succeed")
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

/// Scenario: `write.delete.granularity=file` — a data file's OWN positional-delete
/// file removes exactly its flagged row positions.
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

/// Scenario: `write.delete.granularity=partition` — ONE delete file references
/// data files spanning multiple files; each data file's read is filtered to
/// only the delete rows whose `file_path` matches ITS own absolute URI.
#[test]
fn scan_filters_partition_delete_by_file_path() {
    let dir = temp_dir("partition_gran");
    let f0 = write_data_parquet(&dir, "p0/data.parquet", &(100..110).collect::<Vec<_>>(), 4);
    let f1 = write_data_parquet(&dir, "p1/data.parquet", &(200..210).collect::<Vec<_>>(), 4);
    // One shared partition-granularity delete file: 2 rows for f0, 1 row for f1.
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

    // f0 loses positions 2,5 -> ids 102,105; f1 loses position 1 -> id 201.
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

/// Scenario: multiple positional-delete files associated with the SAME data
/// file are unioned (including an overlapping position) rather than only the
/// last one applying.
#[test]
fn scan_unions_multiple_delete_files() {
    let dir = temp_dir("union");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..20).collect::<Vec<_>>(), 8);
    let delete_a = write_delete_parquet(&dir, "del_a.parquet", &[(&data_url, 1), (&data_url, 4)]);
    // Overlaps position 4 with delete_a; also deletes 9.
    let delete_b = write_delete_parquet(&dir, "del_b.parquet", &[(&data_url, 4), (&data_url, 9)]);

    let entry = FileEntry::with_deletes(
        data_url.clone(),
        local_file_size(&data_url),
        vec![delete_ref(&delete_a), delete_ref(&delete_b)],
    );
    let spec = scan_spec(vec![entry], None, None);
    let rows = run_scan(&spec, &data_url);

    // Union of {1,4} and {4,9} = {1,4,9}: exactly 3 rows removed, not 4.
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

/// Scenario: a delete file that flags EVERY row of its data file yields zero
/// rows for that file (rather than erroring or returning stale rows).
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

/// Scenario: positional deletes compose with projection/filter pushdown +
/// row-group pruning, and separately with LIMIT pushdown, rather than
/// disabling either — the base access plan (deletes) and the opener's own
/// pruning/limit intersect to the correct final row set in both cases.
///
/// Filter and LIMIT are exercised in SEPARATE sub-scans here rather than
/// combined in one query: combining a WHERE predicate with LIMIT in this
/// engine's scan pipeline currently mis-orders results even with NO deletes
/// involved at all (reproduced independently against the plain `ListingTable`
/// path via `build_raw_scan_physical_plan`, i.e. a pre-existing scan-execution
/// gap unrelated to positional-delete application — out of scope here; see
/// Task 4.2's plan-shape/pruning gate).
#[test]
fn scan_deletes_compose_with_pushdown_and_pruning() {
    let dir = temp_dir("compose");
    // Small row groups (16 rows) so a predicate can prune whole row groups
    // while the base access plan still carries the deletes.
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

    // Filter pushdown + row-group pruning: a predicate that prunes several
    // whole row groups (keeps only ids >= 60, spanning groups 3..6) still
    // composes correctly with the base delete access plan.
    let filter_spec = scan_spec(vec![entry.clone()], Some("\"ID\" >= 60".to_string()), None);
    let filter_rows = run_scan(&filter_spec, &data_url);
    let expected_filtered: Vec<i64> = (60..100).filter(|id| *id != 95).collect();
    assert_eq!(
        ids_of(&filter_rows),
        expected_filtered,
        "filter pushdown + row-group pruning must compose with the delete (only 95 was in-range)"
    );

    // LIMIT pushdown: the first N surviving (post-delete) rows in file order.
    let limit_spec = scan_spec(vec![entry], None, Some(10));
    let limit_rows = run_scan(&limit_spec, &data_url);
    assert_eq!(
        ids_of(&limit_rows),
        vec![0, 1, 2, 3, 4, 6, 7, 8, 9, 10],
        "LIMIT pushdown must count only post-delete rows (position 5 is deleted)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (backstop): an assigned delete file this engine cannot apply
/// (equality delete) is rejected with a clean, mechanism-naming error rather
/// than silently ignored or applied incorrectly. Because the read-time
/// backstop check runs BEFORE the delete file is opened, the referenced path
/// need not exist.
#[test]
fn scan_rejects_unapplicable_delete_file() {
    let dir = temp_dir("unapplicable");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);

    let bogus_delete = DeleteFileRef {
        path: format!("{}/does-not-need-to-exist.parquet", dir.to_string_lossy()),
        size: 10,
        content_type: DeleteFileContentType::EqualityDeletes,
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
    ))
    .expect_err("an equality delete must be rejected, not applied");
    let msg = err.to_string();
    assert!(
        msg.contains("equality delete"),
        "error must name the unsupported mechanism: {msg}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (fail loud): a malformed positional-delete file carrying a negative
/// `pos` is rejected with a clean error rather than silently dropped (casting a
/// negative to `u64` would wrap to a huge index and skip the delete).
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
    ))
    .expect_err("a negative pos must be rejected, not silently dropped");
    // NB: the `dummy_storage` secret_key is "s", so credential redaction strips
    // every "s" from the message — assert on tokens that survive it.
    let msg = err.to_string();
    assert!(
        msg.contains("negative") && msg.contains("(-1)"),
        "error must name the malformed negative position: {msg}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (fail loud): a spec whose files resolve to more than one
/// object-store root is rejected at registration. The scan registers a single
/// store (keyed by the first file); a file under a different scheme/host would
/// otherwise be read through the wrong store and fail confusingly.
#[test]
fn scan_rejects_mixed_object_store_roots() {
    let dir = temp_dir("mixed_roots");
    let data_url = write_data_parquet(&dir, "data.parquet", &(0..10).collect::<Vec<_>>(), 8);
    let local = FileEntry::new(data_url.clone(), local_file_size(&data_url));
    // A second data file under a DIFFERENT (s3://) root than the first (file://).
    let foreign = FileEntry::new("s3://other-bucket/part-0.parquet", 10);
    let spec = scan_spec(vec![local, foreign], None, None);

    let err = block_on(try_run_scan_with_store(
        &spec,
        &data_url,
        Arc::new(LocalFileSystem::new()),
    ))
    .expect_err("a spec mixing object-store roots must be rejected");
    assert!(
        err.to_string().contains("mixes object-store roots"),
        "error must explain the mixed-root rejection: {err}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a data file with NO associated delete files scans unchanged —
/// the unified `PositionalDeleteScanTable` path must not regress the
/// delete-free case.
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

/// An [`ObjectStore`] decorator that records every non-HEAD `get` it serves, by
/// location. Delegates everything to `inner` (a plain [`LocalFileSystem`]).
/// Used to prove the delete file is fetched through the SAME registered
/// object-store instance the data file uses — i.e. delete-file reads ride the
/// identical credentialed client the scan configures from `spec.storage`
/// rather than opening a separate, unauthenticated path.
#[derive(Debug)]
struct TrackingStore {
    inner: Arc<dyn ObjectStore>,
    gets: Arc<std::sync::Mutex<Vec<ObjectStorePath>>>,
    calls: Arc<AtomicUsize>,
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

/// Scenario (memory-creds): delete files are read through the SAME registered
/// object-store instance the scan configures for data-file access — modeling
/// "read with vended credentials" locally: a store standing in for a
/// credentialed client is registered ONCE for the scan, and both the data
/// file's read AND the associated delete file's read must flow through it (no
/// separate, unauthenticated object-store path for delete files).
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
    });

    let rows = block_on(try_run_scan_with_store(&spec, &data_url, tracking_store))
        .expect("raw scan must succeed via the tracking (credentialed) store");

    assert_eq!(total_rows(&rows), 18, "2 deletes applied");

    // The delete file's content was fetched via a `get` (not just a HEAD),
    // proving it went through the SAME registered store as the data file.
    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "both the data file and the delete file must be fetched via the registered store (got {} calls)",
        calls.load(Ordering::SeqCst)
    );
    let recorded = gets.lock().unwrap();
    let delete_path = Url::parse(&delete_url)
        .unwrap()
        .to_file_path()
        .unwrap()
        .to_string_lossy()
        .to_string();
    assert!(
        recorded.iter().any(|p| p.as_ref().contains(
            delete_path
                .trim_start_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or(&delete_path)
        )),
        "the delete file must be fetched through the registered (credentialed) store: {recorded:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

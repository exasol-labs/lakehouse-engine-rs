//! Integration tests (Task 2.3, plan `add-delta-scan-execution`) for **Delta deletion
//! vectors applied at scan time**, over the vendored `table-with-dv-small` fixture on a
//! local filesystem store.
//!
//! The fixture is real Delta writer output: a 10-row single-column (`value` int32)
//! Parquet file plus a 45-byte deletion-vector sidecar the table's second commit logs
//! against it (`storage=u`, cardinality 2). `_delta_log/00000000000000000001.json`'s
//! `DELETE ... WHERE value IN (0, 9)` deletes exactly the rows holding `value` 0 and 9,
//! which sit at row positions 0 and 9 — confirmed once against the real sidecar bytes
//! via `delta_kernel`'s own decoder, not assumed from the predicate text.
//!
//! Every test drives the production raw-scan pipeline (`register_files` /
//! `run_raw_scan_with_session` → `PositionalDeleteScanTable` →
//! `crate::scan::deletion_vectors`) against a temp-directory copy of the vendored
//! bytes, so nothing here mutates the checked-in fixture.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{Int32Array, Int64Array, StringArray};
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
    CommonScanSpec, DeleteMechanism, DeltaDeletionVectorStorage, FileEntry, ScanSpec,
    StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{register_files, run_raw_scan_with_session, session_config_for_spec};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use url::Url;

/// Iceberg reserved field-ids for a positional-delete file's `file_path`/`pos`
/// columns (mirrors `scan::positional_deletes`'s private constants; duplicated here
/// since this integration test cannot import a `pub(crate)` item).
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

/// The Delta `pathOrInlineDv` value `_delta_log/00000000000000000001.json` logs for the
/// vendored fixture's deletion vector (`storage=u`, UUID-relative).
const FIXTURE_LOGGED_PATH: &str = "vBn[lx{q8@P<9BNH/isA";
const FIXTURE_SIDECAR_NAME: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";
const FIXTURE_DATA_FILE_NAME: &str =
    "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet";
const FIXTURE_OFFSET: i32 = 1;
const FIXTURE_SIZE_IN_BYTES: i32 = 36;
const FIXTURE_CARDINALITY: i64 = 2;

/// The same vector's `magicNumber ++ bitmapData` (the sidecar body without its
/// container framing), Z85-encoded — exactly what an inline descriptor carries.
/// Decodes to the identical {0, 9} position set as the vendored sidecar (verified in
/// `crate::scan::deletion_vectors_tests`, which vendors the same fixture bytes).
const INLINE_PAYLOAD: &str = "^Bg9^0rr910000000000iXQKl0rr91000315c8Xg000r9";

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/unity/fixtures/table-with-dv-small")
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("lh_delta_dv_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn file_url(path: &Path) -> String {
    Url::from_file_path(path)
        .expect("absolute path")
        .to_string()
}

fn dir_url(path: &Path) -> String {
    let mut url = file_url(path);
    if !url.ends_with('/') {
        url.push('/');
    }
    url
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()))
        .len()
}

/// Copy the vendored fixture's data Parquet into `dir` under `name`, returning its
/// absolute size.
fn copy_fixture_data_file(dir: &Path, name: &str) -> (PathBuf, u64) {
    let dest = dir.join(name);
    std::fs::copy(fixture_dir().join(FIXTURE_DATA_FILE_NAME), &dest)
        .unwrap_or_else(|e| panic!("copy fixture data file: {e}"));
    let size = file_size(&dest);
    (dest, size)
}

/// Copy the vendored fixture's DV sidecar into `dir` under `name`.
fn copy_fixture_sidecar(dir: &Path, name: &str) -> PathBuf {
    let dest = dir.join(name);
    std::fs::copy(fixture_dir().join(FIXTURE_SIDECAR_NAME), &dest)
        .unwrap_or_else(|e| panic!("copy fixture sidecar: {e}"));
    dest
}

/// The [`DeleteMechanism::DeltaDeletionVector`] `_delta_log/...0001.json` logs for the
/// fixture: a UUID-relative vector resolved against whatever `table_root` the caller's
/// `ScanSpec` carries.
fn fixture_deletion_vector() -> DeleteMechanism {
    DeleteMechanism::DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage::UuidRelative,
        path_or_inline_dv: FIXTURE_LOGGED_PATH.to_string(),
        offset: Some(FIXTURE_OFFSET),
        size_in_bytes: FIXTURE_SIZE_IN_BYTES,
        cardinality: FIXTURE_CARDINALITY,
    }
}

/// An absolute-path deletion vector naming `sidecar_url` verbatim — bypasses table-root
/// reconstruction entirely, so its `table_root` never needs to resolve.
fn absolute_deletion_vector(sidecar_url: &str) -> DeleteMechanism {
    DeleteMechanism::DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage::AbsolutePath,
        path_or_inline_dv: sidecar_url.to_string(),
        offset: Some(FIXTURE_OFFSET),
        size_in_bytes: FIXTURE_SIZE_IN_BYTES,
        cardinality: FIXTURE_CARDINALITY,
    }
}

/// An inline deletion vector carrying its own bitmap payload — resolves no sidecar path
/// at all.
fn inline_deletion_vector() -> DeleteMechanism {
    DeleteMechanism::DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage::Inline,
        path_or_inline_dv: INLINE_PAYLOAD.to_string(),
        offset: None,
        size_in_bytes: FIXTURE_SIZE_IN_BYTES,
        cardinality: FIXTURE_CARDINALITY,
    }
}

/// Storage props are never dialed for a local `file://` scan; a placeholder keeps the
/// spec well-formed.
fn dummy_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://localhost:9000".into(),
        region: "us-east-1".into(),
        access_key: "k".into(),
        secret_key: "SECRETKEY".into(),
        allow_http: true,
        ..Default::default()
    })
}

/// A row-scan `ScanSpec` over the fixture's single `value` column, rooted at
/// `table_root` (needed to resolve a UUID-relative deletion vector), optionally
/// pushing a filter and/or a limit.
fn scan_spec(
    files: Vec<FileEntry>,
    table_root: &str,
    filter: Option<String>,
    limit: Option<u64>,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root: table_root.to_string(),
            projection: vec!["VALUE".into()],
            filter,
            limit,
            storage: dummy_storage(),
            df_batch_size: 64,
            ..Default::default()
        },
        files,
    }
}

/// A fake `UdfContext` serving one input row and decoding every emitted Arrow IPC
/// batch — the same capture pattern `scan_positional_deletes.rs` uses.
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

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

/// Run the production raw scan for `spec` against a session registering `store` for
/// `register_url`'s scheme/authority. Returns the decoded emitted batches, or the
/// scan's error.
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
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, spec, &mut timers).await?;
    Ok(ctx.emitted)
}

/// Run the production raw scan over a plain `LocalFileSystem`, panicking on scan
/// failure.
fn run_scan(spec: &ScanSpec, register_url: &str) -> Vec<RecordBatch> {
    block_on(try_run_scan_with_store(
        spec,
        register_url,
        Arc::new(LocalFileSystem::new()),
    ))
    .expect("raw scan must succeed")
}

fn values_of(batches: &[RecordBatch]) -> Vec<i32> {
    let mut out = Vec::new();
    for b in batches {
        let values = b
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("value col");
        for i in 0..b.num_rows() {
            out.push(values.value(i));
        }
    }
    out.sort_unstable();
    out
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Scenario: a UUID-relative deletion vector — the real vendored fixture, end to
/// end — removes exactly its flagged row positions: 10 physical rows yield 8, and
/// the two deleted values (0 and 9) are absent.
#[test]
fn uuid_relative_deletion_vector_removes_its_flagged_rows() {
    let dir = temp_dir("uuid_relative");
    let (data_path, data_size) = copy_fixture_data_file(&dir, FIXTURE_DATA_FILE_NAME);
    copy_fixture_sidecar(&dir, FIXTURE_SIDECAR_NAME);

    let entry = FileEntry::with_deletes(
        FIXTURE_DATA_FILE_NAME,
        data_size,
        vec![fixture_deletion_vector()],
    );
    let spec = scan_spec(vec![entry], &dir_url(&dir), None, None);
    let rows = run_scan(&spec, &file_url(&data_path));

    assert_eq!(total_rows(&rows), 8, "10 physical rows minus 2 deleted");
    let values = values_of(&rows);
    assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert!(!values.contains(&0), "deleted value 0 must be absent");
    assert!(!values.contains(&9), "deleted value 9 must be absent");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: an inline deletion vector is decoded from its own payload, with no
/// object-store access for it at all — a store that errors on any non-HEAD read of a
/// `.bin` sidecar still lets the scan succeed, because nothing ever asks it for one.
#[test]
fn inline_deletion_vector_decodes_without_object_store_access() {
    let dir = temp_dir("inline");
    let (data_path, data_size) = copy_fixture_data_file(&dir, FIXTURE_DATA_FILE_NAME);

    let entry = FileEntry::with_deletes(
        FIXTURE_DATA_FILE_NAME,
        data_size,
        vec![inline_deletion_vector()],
    );
    let spec = scan_spec(vec![entry], &dir_url(&dir), None, None);

    let store = Arc::new(RefusingReadStore::refusing(".bin"));
    let rows = block_on(try_run_scan_with_store(
        &spec,
        &file_url(&data_path),
        Arc::clone(&store) as Arc<dyn ObjectStore>,
    ))
    .expect("an inline vector must decode without ever reading a sidecar body");

    assert_eq!(total_rows(&rows), 8, "10 physical rows minus 2 deleted");
    assert_eq!(values_of(&rows), vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(
        store.refused_count(),
        0,
        "no sidecar read was even attempted for an inline vector"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: an absolute-path deletion vector is read verbatim — resolution never
/// joins it onto `table_root`. A deliberately wrong (non-existent) `table_root` proves
/// it: if the code tried to reconstruct a UUID-relative-style path against it, the
/// sidecar would not be found.
#[test]
fn absolute_path_deletion_vector_is_read_verbatim() {
    let dir = temp_dir("absolute_path");
    let (data_path, data_size) = copy_fixture_data_file(&dir, FIXTURE_DATA_FILE_NAME);
    let sidecar_path = copy_fixture_sidecar(&dir, FIXTURE_SIDECAR_NAME);

    // The data-file entry is itself absolute, so a decoy `table_root` cannot affect
    // ITS resolution either — isolating the assertion to the deletion vector's own
    // path handling.
    let entry = FileEntry::with_deletes(
        file_url(&data_path),
        data_size,
        vec![absolute_deletion_vector(&file_url(&sidecar_path))],
    );
    // A decoy table_root that shares no files with `dir` at all: if `AbsolutePath`
    // resolution joined onto it (like `UuidRelative` does), the sidecar would not be
    // found there.
    let decoy_root = "file:///nonexistent/decoy/root/";
    let spec = scan_spec(vec![entry], decoy_root, None, None);
    let rows = run_scan(&spec, &file_url(&data_path));

    assert_eq!(
        total_rows(&rows),
        8,
        "the absolute sidecar path was read verbatim"
    );
    assert_eq!(values_of(&rows), vec![1, 2, 3, 4, 5, 6, 7, 8]);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a deletion-vector file shared by several data files is fetched exactly
/// once per shard — an absolute-path vector naming the SAME sidecar from two distinct
/// data-file entries triggers only one non-HEAD read of it.
#[test]
fn shared_deletion_vector_file_is_fetched_once_per_shard() {
    let dir = temp_dir("shared_sidecar");
    let (data_a, size_a) = copy_fixture_data_file(&dir, "a.snappy.parquet");
    let (_data_b, size_b) = copy_fixture_data_file(&dir, "b.snappy.parquet");
    let sidecar_path = copy_fixture_sidecar(&dir, FIXTURE_SIDECAR_NAME);
    let shared_dv = absolute_deletion_vector(&file_url(&sidecar_path));

    let entries = vec![
        FileEntry::with_deletes("a.snappy.parquet", size_a, vec![shared_dv.clone()]),
        FileEntry::with_deletes("b.snappy.parquet", size_b, vec![shared_dv]),
    ];
    let spec = scan_spec(entries, &dir_url(&dir), None, None);

    let gets = Arc::new(std::sync::Mutex::new(Vec::new()));
    let tracking = Arc::new(TrackingStore {
        inner: Arc::new(LocalFileSystem::new()),
        gets: Arc::clone(&gets),
        concurrency: None,
    });
    let rows = block_on(try_run_scan_with_store(&spec, &file_url(&data_a), tracking))
        .expect("raw scan must succeed");

    assert_eq!(
        total_rows(&rows),
        16,
        "two 10-row files minus 2 deletes each"
    );

    let sidecar_reads = gets
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.as_ref().contains(FIXTURE_SIDECAR_NAME))
        .count();
    assert_eq!(
        sidecar_reads, 1,
        "a sidecar shared by two data files must be fetched exactly once for the shard"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Fixed per-read delay long enough that, on the tests' current-thread runtime, every
/// read admitted in one scheduling wave has bumped the peak counter before any timer
/// fires — deterministic, not a race on real I/O timing (mirrors
/// `scan_positional_deletes.rs`'s `DELETE_READ_DELAY`).
const DV_READ_DELAY: Duration = Duration::from_millis(50);
const DV_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Scenario (connection-concurrency bound): with a read budget of N and MORE than N
/// unique deletion-vector sidecars to fetch, the concurrent sidecar reads peak at
/// EXACTLY N — the shared instance-level semaphore admits N at a time and no more.
#[test]
fn deletion_vector_reads_stay_within_the_connection_budget() {
    const BUDGET: usize = 3;
    const UNIQUE_SIDECARS: usize = 6; // strictly greater than BUDGET

    let dir = temp_dir("bounded_budget");
    let mut entries = Vec::with_capacity(UNIQUE_SIDECARS);
    let mut needles = Vec::with_capacity(UNIQUE_SIDECARS);
    for i in 0..UNIQUE_SIDECARS {
        let data_name = format!("data_{i}.snappy.parquet");
        let (_data_path, size) = copy_fixture_data_file(&dir, &data_name);
        let sidecar_name = format!("sidecar_{i}.bin");
        let sidecar_path = copy_fixture_sidecar(&dir, &sidecar_name);
        entries.push(FileEntry::with_deletes(
            data_name,
            size,
            vec![absolute_deletion_vector(&file_url(&sidecar_path))],
        ));
        needles.push(sidecar_name);
    }

    let mut spec = scan_spec(entries, &dir_url(&dir), None, None);
    spec.common.s3_max_connections = BUDGET;

    let peak = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(TrackingStore {
        inner: Arc::new(LocalFileSystem::new()),
        gets: Arc::new(std::sync::Mutex::new(Vec::new())),
        concurrency: Some(ConcurrencyProbe {
            needles,
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak: Arc::clone(&peak),
            delay: DV_READ_DELAY,
        }),
    });

    let register_url = dir_url(&dir);
    let rows = block_on(async {
        tokio::time::timeout(
            DV_READ_TIMEOUT,
            try_run_scan_with_store(&spec, &register_url, store),
        )
        .await
        .expect("bounded deletion-vector-read fan-out must finish within the timeout, not hang")
        .expect("raw scan must succeed")
    });

    assert_eq!(
        peak.load(Ordering::SeqCst),
        BUDGET,
        "concurrent deletion-vector reads must peak at EXACTLY the connection budget \
         ({BUDGET}): a lower peak means the fan-out was not exercised, a higher peak means \
         the bound leaked"
    );
    assert_eq!(
        total_rows(&rows),
        UNIQUE_SIDECARS * 8,
        "every shard file loses exactly 2 of its 10 rows"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: deletion vectors compose with projection, filter, LIMIT, and
/// aggregation — the base access plan (deletes) and the opener's own pushdown
/// intersect to the correct final result in every case, rather than either
/// disabling the other.
#[test]
fn deletion_vectors_compose_with_projection_filter_limit_and_aggregation() {
    let dir = temp_dir("compose");
    let (data_path, data_size) = copy_fixture_data_file(&dir, FIXTURE_DATA_FILE_NAME);
    copy_fixture_sidecar(&dir, FIXTURE_SIDECAR_NAME);
    let table_root = dir_url(&dir);

    // Projection + filter pushdown: only the "VALUE" column is registered, and the
    // filter is evaluated over post-delete rows (1..=8), keeping 3..=8.
    let entry = FileEntry::with_deletes(
        FIXTURE_DATA_FILE_NAME,
        data_size,
        vec![fixture_deletion_vector()],
    );
    let filter_spec = scan_spec(
        vec![entry.clone()],
        &table_root,
        Some("\"VALUE\" > 2".to_string()),
        None,
    );
    let filter_rows = run_scan(&filter_spec, &file_url(&data_path));
    assert_eq!(
        values_of(&filter_rows),
        vec![3, 4, 5, 6, 7, 8],
        "filter pushdown must see only post-delete rows"
    );

    // LIMIT pushdown: the first N surviving (post-delete) rows in file order.
    let limit_spec = scan_spec(vec![entry], &table_root, None, Some(3));
    let limit_rows = run_scan(&limit_spec, &file_url(&data_path));
    assert_eq!(
        values_of(&limit_rows),
        vec![1, 2, 3],
        "LIMIT pushdown must count only post-delete rows (values 0 and 9 are deleted)"
    );

    // Aggregation: a COUNT(*) over the SAME registered production table
    // (`PositionalDeleteScanTable`) reflects only the 8 live rows — the deletion
    // vector's base access plan applies underneath any SQL run against it,
    // aggregation included.
    let agg_entry = FileEntry::with_deletes(
        FIXTURE_DATA_FILE_NAME,
        data_size,
        vec![fixture_deletion_vector()],
    );
    let agg_spec = scan_spec(vec![agg_entry], &table_root, None, None);
    let count = block_on(async {
        let session = SessionContext::new_with_config(session_config_for_spec(&agg_spec));
        session.runtime_env().register_object_store(
            &Url::parse(&file_url(&data_path)).expect("register url"),
            Arc::new(LocalFileSystem::new()),
        );
        register_files(&session, "scan_target", &agg_spec)
            .await
            .expect("register_files must succeed");
        let df = session
            .sql("SELECT COUNT(*) AS c FROM scan_target")
            .await
            .expect("aggregate SQL must plan");
        let batches = df.collect().await.expect("aggregate must execute");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column");
        column.value(0)
    });
    assert_eq!(
        count, 8,
        "COUNT(*) over the deletion-vector-carrying table must see only live rows"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a Delta data file carrying NO deletion vector scans unchanged — the
/// unified `PositionalDeleteScanTable` path must not regress the delete-free case for
/// a Delta-sourced file any more than for an Iceberg one.
#[test]
fn delta_file_without_a_deletion_vector_scans_unchanged() {
    let dir = temp_dir("delete_free");
    let (data_path, data_size) = copy_fixture_data_file(&dir, FIXTURE_DATA_FILE_NAME);

    let entry = FileEntry::new(FIXTURE_DATA_FILE_NAME, data_size);
    let spec = scan_spec(vec![entry], &dir_url(&dir), None, None);
    let rows = run_scan(&spec, &file_url(&data_path));

    assert_eq!(
        total_rows(&rows),
        10,
        "no rows must be dropped absent a deletion vector"
    );
    assert_eq!(values_of(&rows), (0..10).collect::<Vec<_>>());

    let _ = std::fs::remove_dir_all(&dir);
}

/// Write a local Iceberg-style positional-delete Parquet at `dir/relative`:
/// `file_path`/`pos` columns tagged with the Iceberg reserved field-ids. Returns the
/// file's absolute `file://` URL.
fn write_iceberg_delete_parquet(dir: &Path, relative: &str, entries: &[(&str, i64)]) -> String {
    let field_id_meta =
        |id: i32| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false)
            .with_metadata(field_id_meta(FIELD_ID_POSITIONAL_DELETE_FILE_PATH)),
        Field::new("pos", DataType::Int64, false)
            .with_metadata(field_id_meta(FIELD_ID_POSITIONAL_DELETE_POS)),
    ]));
    let path = dir.join(relative);
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
    file_url(&path)
}

/// Write a local single-column (`value` int32) data Parquet with `values` as its
/// rows. Returns the file's absolute `file://` URL.
fn write_iceberg_data_parquet(dir: &Path, relative: &str, values: &[i32]) -> String {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let path = dir.join(relative);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
        .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    file_url(&path)
}

/// Scenario: both delete mechanisms converge on one position map and one
/// access-plan pipeline — a shard mixing an Iceberg positional-delete file (on its
/// own data file) with a Delta deletion vector (the vendored fixture, on a
/// different data file) applies BOTH correctly in a single scan, end to end.
#[test]
fn mixed_iceberg_and_delta_shard_shares_one_position_map_and_limiter() {
    let dir = temp_dir("mixed_shard");

    // Iceberg leg: a 5-row data file with its own positional-delete file removing
    // positions 1 and 3.
    let iceberg_data_url =
        write_iceberg_data_parquet(&dir, "iceberg_data.parquet", &[100, 101, 102, 103, 104]);
    let iceberg_delete_url = write_iceberg_delete_parquet(
        &dir,
        "iceberg_deletes.parquet",
        &[(&iceberg_data_url, 1), (&iceberg_data_url, 3)],
    );
    let iceberg_delete_size = file_size(Path::new(
        iceberg_delete_url.strip_prefix("file://").unwrap(),
    ));

    // Delta leg: the vendored fixture, deletion vector removing values 0 and 9.
    let (delta_data_path, delta_data_size) = copy_fixture_data_file(&dir, FIXTURE_DATA_FILE_NAME);
    copy_fixture_sidecar(&dir, FIXTURE_SIDECAR_NAME);

    let entries = vec![
        FileEntry::with_deletes(
            iceberg_data_url.clone(),
            file_size(Path::new(iceberg_data_url.strip_prefix("file://").unwrap())),
            vec![DeleteMechanism::IcebergPositionalDelete {
                path: iceberg_delete_url,
                size: iceberg_delete_size,
            }],
        ),
        FileEntry::with_deletes(
            FIXTURE_DATA_FILE_NAME,
            delta_data_size,
            vec![fixture_deletion_vector()],
        ),
    ];
    let spec = scan_spec(entries, &dir_url(&dir), None, None);
    let rows = run_scan(&spec, &file_url(&delta_data_path));

    // Iceberg: 100,102,104 survive (101, 103 deleted). Delta: 1..8 survive (0, 9 deleted).
    let values = values_of(&rows);
    assert_eq!(
        total_rows(&rows),
        3 + 8,
        "3 Iceberg survivors + 8 Delta survivors"
    );
    for v in [100, 102, 104] {
        assert!(
            values.contains(&v),
            "Iceberg survivor {v} must be present: {values:?}"
        );
    }
    for v in [101, 103] {
        assert!(
            !values.contains(&v),
            "Iceberg-deleted {v} must be absent: {values:?}"
        );
    }
    for v in 1..=8 {
        assert!(
            values.contains(&v),
            "Delta survivor {v} must be present: {values:?}"
        );
    }
    for v in [0, 9] {
        assert!(
            !values.contains(&v),
            "Delta-deleted {v} must be absent: {values:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario (fail loud): every deletion-vector container the scan cannot trust —
/// wrong version byte, a stored size the log contradicts, a foreign magic, a broken
/// checksum, and a truncated container — is rejected with a clean error through the
/// FULL production scan pipeline, never a panic and never silently-emitted rows.
#[test]
fn malformed_deletion_vector_containers_fail_the_scan_without_panicking() {
    let sidecar_bytes = std::fs::read(fixture_dir().join(FIXTURE_SIDECAR_NAME)).unwrap();

    let corruptions: Vec<(&str, Vec<u8>)> = vec![
        ("a container version other than 1", {
            let mut b = sidecar_bytes.clone();
            b[0] = 2;
            b
        }),
        ("a stored size the log contradicts", {
            let mut b = sidecar_bytes.clone();
            b[4] = 0x23;
            b
        }),
        ("a foreign bitmap magic", {
            let mut b = sidecar_bytes.clone();
            b[5] = 0x00;
            b
        }),
        ("a broken CRC-32", {
            let mut b = sidecar_bytes.clone();
            let last = b.len() - 1;
            b[last] = 0x00;
            b
        }),
        (
            "a container truncated before its checksum",
            sidecar_bytes[..20].to_vec(),
        ),
    ];

    for (label, body) in corruptions {
        let dir = temp_dir("malformed");
        let (data_path, data_size) = copy_fixture_data_file(&dir, FIXTURE_DATA_FILE_NAME);
        std::fs::write(dir.join(FIXTURE_SIDECAR_NAME), &body).expect("write corrupted sidecar");

        let entry = FileEntry::with_deletes(
            FIXTURE_DATA_FILE_NAME,
            data_size,
            vec![fixture_deletion_vector()],
        );
        let spec = scan_spec(vec![entry], &dir_url(&dir), None, None);

        let session = SessionContext::new_with_config(session_config_for_spec(&spec));
        session.runtime_env().register_object_store(
            &Url::parse(&file_url(&data_path)).expect("register url"),
            Arc::new(LocalFileSystem::new()),
        );
        let mut ctx = FakeCtx::new();
        let mut timers = PhaseTimers::start();
        let err = block_on(run_raw_scan_with_session(
            &mut ctx,
            &session,
            &spec,
            &mut timers,
        ))
        .expect_err(&format!("{label} must be refused, not applied"));
        let msg = err.to_string();
        assert!(
            msg.contains("deletion vector"),
            "{label}: error must name the deletion vector: {msg}"
        );
        assert!(
            msg.contains(FIXTURE_DATA_FILE_NAME),
            "{label}: error must name the affected data file: {msg}"
        );
        assert!(
            ctx.emitted.is_empty(),
            "{label}: no batch may be emitted before the scan fails: {:?}",
            ctx.emitted
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// An [`ObjectStore`] that errors on any non-HEAD `get_opts` whose location contains
/// `needle`, delegating everything else to a plain [`LocalFileSystem`]. Proves a
/// scenario never even attempts a read it should not need — a miscounted-but-silent
/// read would fail the SCAN here, not just a follow-up assertion.
#[derive(Debug)]
struct RefusingReadStore {
    inner: LocalFileSystem,
    needle: String,
    refused: AtomicUsize,
}

impl RefusingReadStore {
    fn refusing(needle: &str) -> Self {
        Self {
            inner: LocalFileSystem::new(),
            needle: needle.to_string(),
            refused: AtomicUsize::new(0),
        }
    }

    fn refused_count(&self) -> usize {
        self.refused.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for RefusingReadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RefusingReadStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RefusingReadStore {
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
        if !options.head && location.as_ref().contains(self.needle.as_str()) {
            self.refused.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "RefusingReadStore",
                source: format!("refused: read of '{location}' was never expected").into(),
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

/// Instrumentation shared by the concurrency-bound test: an atomic peak-concurrency
/// counter over probed reads plus a fixed artificial delay forcing genuine overlap
/// without real I/O timing (mirrors `scan_positional_deletes.rs`'s `ConcurrencyProbe`).
#[derive(Debug)]
struct ConcurrencyProbe {
    /// Bare filenames identifying the sidecar reads to instrument.
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

struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// An [`ObjectStore`] decorator that records every non-HEAD `get` it serves, by
/// location, and optionally instruments a peak-concurrency probe (mirrors
/// `scan_positional_deletes.rs`'s `TrackingStore`).
#[derive(Debug)]
struct TrackingStore {
    inner: Arc<dyn ObjectStore>,
    gets: Arc<std::sync::Mutex<Vec<ObjectStorePath>>>,
    concurrency: Option<ConcurrencyProbe>,
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
            self.gets.lock().unwrap().push(location.clone());
        }
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

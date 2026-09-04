use std::sync::Arc;

use delta_kernel::scan::{Scan, StatsOptions};
use delta_kernel::{Expression, Predicate};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde_json::json;

use super::super::delta_predicate::to_delta_predicate;
use super::*;
use crate::scan::spec::{DeleteMechanism, DeltaDeletionVectorStorage};

/// The vendored fixture tables are read through a plain local-filesystem store,
/// which is the same injection the S3 path uses in production.
fn local_store() -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

fn fixture_root(table: &str) -> String {
    format!(
        "{}/../../scripts/unity/fixtures/{table}",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn replay_fixture(table: &str) -> Vec<FileEntry> {
    open_fixture(table)
        .active_files(None)
        .expect("fixture log replays")
}

fn replay_fixture_pruned(table: &str, predicate: Predicate) -> Vec<FileEntry> {
    open_fixture(table)
        .active_files(Some(Arc::new(predicate)))
        .expect("fixture log replays")
}

fn open_fixture(table: &str) -> DeltaSnapshot {
    DeltaSnapshot::open(local_store(), &fixture_root(table)).expect("fixture table opens")
}

impl super::DeltaSnapshot {
    fn files_surviving(&self, prune: Predicate) -> Vec<FileEntry> {
        self.active_files(Some(Arc::new(prune)))
            .expect("the probe scan replays")
    }

    /// Deliberately does NOT call `DeltaSnapshot::active_files` and MUST NOT be
    /// refactored to: `active_files` no longer exposes `StatsOptions`, and
    /// re-routing through it would make the two `disabling_stats_forfeits_*`
    /// tests assert nothing about the mechanism this plan removed.
    ///
    /// `replay_probe` reproduces `active_files`'s replay loop minus its path
    /// sort, so callers of this helper must assert on counts or on a
    /// sorted/set-based comparison, never on raw order.
    fn files_surviving_without_stats(&self, prune: Predicate) -> Vec<FileEntry> {
        self.replay_probe(
            self.snapshot
                .clone()
                .scan_builder()
                .with_predicate(Arc::new(prune))
                .with_stats(StatsOptions::none())
                .without_row_transforms()
                .build()
                .expect("the probe scan plans"),
        )
    }

    fn replay_probe(&self, scan: Scan) -> Vec<FileEntry> {
        let mut active = Vec::new();
        let mut listed_paths = HashSet::new();
        for replayed in scan
            .scan_metadata(&self.engine)
            .expect("the probe scan replays")
        {
            let (data, selected) = replayed
                .expect("the probe scan replays")
                .scan_files
                .into_parts();
            let batch =
                ArrowEngineData::try_from_engine_data(data).expect("the replayed probe log reads");
            append_active_files(
                batch.record_batch(),
                &selected,
                &mut listed_paths,
                &mut active,
            )
            .expect("the replayed probe log reads");
        }
        active
    }
}

fn id_above_three() -> Predicate {
    Predicate::gt(Expression::column(["id"]), Expression::literal(3i64))
}

fn letter_is_a() -> Predicate {
    Predicate::eq(Expression::column(["letter"]), Expression::literal("a"))
}

#[test]
fn disabling_stats_forfeits_range_skipping_and_keeps_every_file() {
    let files = open_fixture("multi-part-stats").files_surviving_without_stats(id_above_three());

    assert_eq!(files.len(), 5);
}

#[test]
fn disabling_stats_forfeits_partition_pruning_and_keeps_every_file() {
    let files = open_fixture("basic_partitioned").files_surviving_without_stats(letter_is_a());

    assert_eq!(files.len(), 6);
}

const SYNTHETIC_ROOT: &str = "memory:///synthetic_table";

const SYNTHETIC_PREAMBLE: &str = concat!(
    r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
    "\n",
    r#"{"metaData":{"id":"synthetic","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1}}"#,
    "\n",
);

/// A log written into an in-memory store, so an action no vendored fixture holds
/// is exercised without a fixture on disk. The first commit carries the protocol
/// and metadata every replay needs.
async fn synthetic_table(commits: &[&str]) -> Arc<dyn ObjectStore> {
    let store = Arc::new(InMemory::new());
    for (version, actions) in commits.iter().enumerate() {
        let commit = if version == 0 {
            format!("{SYNTHETIC_PREAMBLE}{actions}\n")
        } else {
            format!("{actions}\n")
        };
        store
            .put(
                &StorePath::from(format!("synthetic_table/_delta_log/{version:020}.json")),
                PutPayload::from(commit),
            )
            .await
            .expect("synthetic commit is stored");
    }
    store
}

/// A log whose metadata declares TWO partition columns in a non-alphabetical order
/// that is also not their schema order, which no vendored fixture carries and which
/// neither a sorted nor a schema-derived partition-column list could reproduce.
const SYNTHETIC_PARTITIONED_PREAMBLE: &str = concat!(
    r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
    "\n",
    r#"{"metaData":{"id":"synthetic-partitioned","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"zone\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"area\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["zone","area"],"configuration":{},"createdTime":1}}"#,
    "\n",
);

async fn synthetic_partitioned_table() -> Arc<dyn ObjectStore> {
    let store = Arc::new(InMemory::new());
    store
        .put(
            &StorePath::from("synthetic_table/_delta_log/00000000000000000000.json"),
            PutPayload::from(SYNTHETIC_PARTITIONED_PREAMBLE.to_string()),
        )
        .await
        .expect("synthetic commit is stored");
    store
}

const SYNTHETIC_COMMIT_ZERO: &str = "00000000000000000000.json";

const SYNTHETIC_ADD: &str = r#"{"add":{"path":"part-0.parquet","partitionValues":{},"size":100,"modificationTime":1,"dataChange":true}}"#;

/// A log declaring a reader feature this engine does not implement (`variantType`),
/// alongside an allow-listed one — so `delta_kernel` itself reads this log without
/// complaint and the refusal can only come from this engine's own gate.
const SYNTHETIC_REFUSED_PREAMBLE: &str = concat!(
    r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["variantType","deletionVectors"],"writerFeatures":["variantType","deletionVectors"]}}"#,
    "\n",
    r#"{"metaData":{"id":"synthetic-refused","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1}}"#,
    "\n",
);

async fn synthetic_refused_table() -> Arc<dyn ObjectStore> {
    let store = Arc::new(InMemory::new());
    store
        .put(
            &StorePath::from(format!(
                "synthetic_table/_delta_log/{SYNTHETIC_COMMIT_ZERO}"
            )),
            PutPayload::from(format!("{SYNTHETIC_REFUSED_PREAMBLE}{SYNTHETIC_ADD}\n")),
        )
        .await
        .expect("synthetic commit is stored");
    store
}

#[tokio::test]
async fn partition_columns_are_reported_in_the_order_the_metadata_declares_them() {
    let snapshot = DeltaSnapshot::open(synthetic_partitioned_table().await, SYNTHETIC_ROOT)
        .expect("synthetic table opens");

    assert_eq!(
        snapshot.partition_columns(),
        vec!["zone".to_string(), "area".to_string()],
        "the logged partition order is carried verbatim, neither sorted nor taken from \
         schema order"
    );
}

#[test]
fn a_table_declaring_no_partition_column_reports_an_empty_partition_list() {
    let snapshot = DeltaSnapshot::open(local_store(), &fixture_root("table-with-dv-small"))
        .expect("fixture table opens");

    assert!(
        snapshot.partition_columns().is_empty(),
        "an unpartitioned table declares no partition column"
    );
}

#[test]
fn column_mapping_mode_is_reported_from_the_tables_metadata() {
    for (table, expected) in [
        ("cdf-column-mapping-name-mode", ColumnMappingMode::Name),
        ("cdf-column-mapping-id-mode", ColumnMappingMode::Id),
        ("basic_partitioned", ColumnMappingMode::None),
    ] {
        let snapshot =
            DeltaSnapshot::open(local_store(), &fixture_root(table)).expect("fixture table opens");

        assert_eq!(
            snapshot.column_mapping_mode(),
            expected,
            "{table} is in {expected:?} column-mapping mode"
        );
    }
}

/// Scenario: A legacy reader version table passes the gate and keeps its column-mapping mode
#[test]
fn a_legacy_reader_version_table_passes_the_gate_and_keeps_its_column_mapping_mode() {
    let snapshot = DeltaSnapshot::open(local_store(), &fixture_root("cdf-column-mapping-id-mode"))
        .expect("minReaderVersion 2 with no readerFeatures list is readable");

    assert_eq!(snapshot.column_mapping_mode(), ColumnMappingMode::Id);
}

// Scenario Coverage (add-delta-table-planning): `open`/`active_files` are `pub(super)`, so this
// scenario is reached as a crate-internal unit test rather than `tests/delta_log_replay.rs`.
#[test]
fn replay_returns_only_the_files_active_at_the_current_version() {
    let files = replay_fixture("cdf-column-mapping-name-mode");

    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "part-00000-9bfc20cc-db37-4a34-aaff-9d5641169fad.c000.snappy.parquet",
            "part-00015-1238a68f-8818-47d5-868f-fd5c382d5d95-c000.snappy.parquet",
            "part-00015-af716d9b-f57a-4063-a732-623f9bd472d2-c000.snappy.parquet",
        ],
        "the two removed files are absent, the later-added file is present, and no \
         change-data file is listed"
    );
}

#[test]
fn replay_carries_each_active_files_path_verbatim_and_its_size() {
    let files = replay_fixture("basic_partitioned");

    let listed: Vec<(&str, u64)> = files.iter().map(|f| (f.path.as_str(), f.size)).collect();
    assert_eq!(
        listed,
        vec![
            (
                "letter=__HIVE_DEFAULT_PARTITION__/part-00000-8eb7f29a-e6a1-436e-a638-bbf0a7953f09.c000.snappy.parquet",
                751
            ),
            (
                "letter=a/part-00000-0dbe0cc5-e3bf-4fb0-b36a-b5fdd67fe843.c000.snappy.parquet",
                751
            ),
            (
                "letter=a/part-00000-a08d296a-d2c5-4a99-bea9-afcea42ba2e9.c000.snappy.parquet",
                751
            ),
            (
                "letter=b/part-00000-41954fb0-ef91-47e5-bd41-b75169c41c17.c000.snappy.parquet",
                751
            ),
            (
                "letter=c/part-00000-27a17b8f-be68-485c-9c49-70c742be30c0.c000.snappy.parquet",
                751
            ),
            (
                "letter=e/part-00000-847cf2d1-1247-4aa0-89ef-2f90c68ea51e.c000.snappy.parquet",
                750
            ),
        ],
        "every path is stored exactly as the log records it, resolved against no table root"
    );
}

// Scenario Coverage (add-delta-table-planning): `open`/`active_files` are `pub(super)`, so this
// scenario is reached as a crate-internal unit test rather than `tests/delta_log_replay.rs`.
#[test]
fn replay_carries_partition_values_and_an_explicit_null() {
    let files = replay_fixture("basic_partitioned");

    let carried: Vec<Option<Option<String>>> = files
        .iter()
        .map(|f| f.partition_values.get("letter").cloned())
        .collect();
    assert_eq!(
        carried,
        vec![
            Some(None),
            Some(Some("a".to_string())),
            Some(Some("a".to_string())),
            Some(Some("b".to_string())),
            Some(Some("c".to_string())),
            Some(Some("e".to_string())),
        ],
        "the Hive default-partition file carries an explicit absent value, not a literal"
    );
    for entry in &files {
        assert_eq!(
            entry.partition_values.len(),
            1,
            "one entry per partition column for {}",
            entry.path
        );
        assert!(
            !entry
                .partition_values
                .values()
                .any(|v| v.as_deref() == Some("__HIVE_DEFAULT_PARTITION__")),
            "the partition-directory literal is never carried as a value"
        );
    }
}

#[test]
fn replay_carries_no_partition_value_for_an_unpartitioned_table() {
    let files = replay_fixture("table-with-dv-small");

    assert!(
        files[0].partition_values.is_empty(),
        "an unpartitioned table logs an empty partitionValues map"
    );
}

// Scenario Coverage (add-delta-table-planning): `open`/`active_files` are `pub(super)`, so this
// scenario is reached as a crate-internal unit test rather than `tests/delta_log_replay.rs`.
#[test]
fn replay_carries_a_readded_files_deletion_vector_exactly_once() {
    let files = replay_fixture("table-with-dv-small");

    assert_eq!(
        files.len(),
        1,
        "the removed-and-re-added path yields exactly one entry, not one per `add`"
    );
    assert_eq!(
        files[0].path,
        "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
    );
    assert_eq!(
        files[0].deletes,
        vec![DeleteMechanism::DeltaDeletionVector {
            storage: DeltaDeletionVectorStorage::UuidRelative,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        }],
        "the re-added entry's deletion vector rides alone in the delete list, carried \
         verbatim, and not the earlier delete-free add's"
    );
}

#[test]
fn replay_carries_no_iceberg_delete_reference_on_any_entry() {
    for (table, active) in [("basic_partitioned", 6), ("table-with-dv-small", 1)] {
        let files = replay_fixture(table);
        assert_eq!(files.len(), active, "{table} has {active} active files");
        for entry in files {
            assert!(
                entry.deletes.iter().all(|mechanism| matches!(
                    mechanism,
                    DeleteMechanism::DeltaDeletionVector { .. }
                )),
                "a replayed Delta log names no Iceberg delete file, so a Delta deletion \
                 vector never reaches the Iceberg positional-delete reader ({table}/{})",
                entry.path
            );
        }
    }
}

#[test]
fn replay_carries_no_deletion_vector_for_a_delete_free_file() {
    let files = replay_fixture("basic_partitioned");

    for entry in &files {
        assert!(
            entry.deletes.is_empty(),
            "no `add` action in this log attaches a deletion vector ({})",
            entry.path
        );
    }
}

#[tokio::test]
async fn an_unrecognized_deletion_vector_storage_kind_is_refused() {
    let store = synthetic_table(&[
        r#"{"add":{"path":"part-0.parquet","partitionValues":{},"size":100,"modificationTime":1,"dataChange":true,"deletionVector":{"storageType":"x","pathOrInlineDv":"abc","sizeInBytes":36,"cardinality":2}}}"#,
    ])
    .await;

    let error = DeltaSnapshot::open(store, SYNTHETIC_ROOT)
        .expect("synthetic table opens")
        .active_files(None)
        .expect_err("an unknown storage kind must not reach the scan as an unread string");

    let text = error.to_string();
    assert!(
        text.contains("'x'") && text.contains("part-0.parquet"),
        "the refusal names the unknown kind and the file: {text}"
    );
}

#[tokio::test]
async fn a_negative_logged_file_size_is_refused() {
    let store = synthetic_table(&[
        r#"{"add":{"path":"part-0.parquet","partitionValues":{},"size":-1,"modificationTime":1,"dataChange":true}}"#,
    ])
    .await;

    let error = DeltaSnapshot::open(store, SYNTHETIC_ROOT)
        .expect("synthetic table opens")
        .active_files(None)
        .expect_err("a negative size must not wrap into a huge unsigned size");

    let text = error.to_string();
    assert!(
        text.contains("-1") && text.contains("part-0.parquet"),
        "the refusal names the size and the file: {text}"
    );
}

/// Scenario: A Delta table resolves its current version's active data files
#[test]
fn replay_reads_the_active_files_out_of_a_multi_part_checkpoint() {
    let files = replay_fixture("multi-part-stats");

    let listed: Vec<(&str, u64)> = files.iter().map(|f| (f.path.as_str(), f.size)).collect();
    assert_eq!(
        listed,
        vec![
            (
                "test%25file%25prefix-part-00000-2699f745-4b33-4eb9-b3cf-04f6af08307f-c000.snappy.parquet",
                761
            ),
            (
                "test%25file%25prefix-part-00000-323f4e76-58ff-48ce-bf0d-14d179e9bf0c-c000.snappy.parquet",
                761
            ),
            (
                "test%25file%25prefix-part-00000-743ccd8e-15b0-49f2-b0e2-aa0efbf148ae-c000.snappy.parquet",
                761
            ),
            (
                "test%25file%25prefix-part-00000-f98612d6-6213-41f1-a006-a11beb0bb544-c000.snappy.parquet",
                761
            ),
            (
                "test%25file%25prefix-part-00000-ff529603-203f-4a68-9ab1-d495e5c1c409-c000.snappy.parquet",
                760
            ),
        ],
        "the five files this table's parquet checkpoint holds, each path percent-encoded \
         exactly as logged"
    );
    for entry in &files {
        assert!(
            entry.deletes.is_empty(),
            "no `add` action in this checkpoint attaches a deletion vector ({})",
            entry.path
        );
        assert!(
            entry.partition_values.is_empty(),
            "an unpartitioned table logs no partition value ({})",
            entry.path
        );
    }
}

#[tokio::test]
async fn a_table_whose_every_file_was_removed_replays_to_no_file() {
    let store = synthetic_table(&[
        r#"{"add":{"path":"part-0.parquet","partitionValues":{},"size":100,"modificationTime":1,"dataChange":true}}"#,
        r#"{"remove":{"path":"part-0.parquet","deletionTimestamp":2,"dataChange":true,"partitionValues":{},"size":100}}"#,
    ])
    .await;

    let files = DeltaSnapshot::open(store, SYNTHETIC_ROOT)
        .expect("synthetic table opens")
        .active_files(None)
        .expect("a fully emptied table replays without error");

    assert!(
        files.is_empty(),
        "a file added at one version and removed at a later one is absent: {files:?}"
    );
}

#[test]
fn a_table_root_holding_no_delta_log_is_refused() {
    let error = DeltaSnapshot::open(local_store(), &fixture_root("PROVENANCE.md"))
        .err()
        .expect("a path that is no Delta table root must fail, never panic");

    assert!(
        error.to_string().contains("PROVENANCE.md"),
        "the refusal names the table root it was given: {error}"
    );
}

/// An [`ObjectStore`] decorator recording the location of every read it forwards,
/// so a test can hold a refusal to the object-store work it actually cost.
#[derive(Debug)]
struct ReadRecordingStore {
    inner: Arc<dyn ObjectStore>,
    reads: Arc<std::sync::Mutex<Vec<String>>>,
}

impl ReadRecordingStore {
    fn wrapping(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            reads: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    fn reads(&self) -> Vec<String> {
        self.reads.lock().expect("read log is not poisoned").clone()
    }
}

impl std::fmt::Display for ReadRecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadRecordingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ReadRecordingStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.reads
            .lock()
            .expect("read log is not poisoned")
            .push(format!("get {location}"));
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.reads
            .lock()
            .expect("read log is not poisoned")
            .push(format!("list {prefix:?}"));
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.reads
            .lock()
            .expect("read log is not poisoned")
            .push(format!("list_with_delimiter {prefix:?}"));
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Scenario: A reader feature outside the allow-list refuses the table before any log replay
///
/// The read count is the evidence: resolving the current version reads commit 0 once,
/// and `active_files` reads it a SECOND time. Holding the refused table to exactly one
/// read of that commit is therefore what pins the gate ahead of the replay.
#[tokio::test]
async fn an_unsupported_reader_feature_is_refused_before_any_schema_or_file_read() {
    let store = ReadRecordingStore::wrapping(synthetic_refused_table().await);

    let error = DeltaSnapshot::open(store.clone(), SYNTHETIC_ROOT)
        .err()
        .expect("a reader feature outside the allow-list must refuse the table");

    let message = error.to_string();
    assert!(
        message.contains("variantType"),
        "the refusal names the unsupported feature: {message}"
    );
    assert!(
        message.contains(SYNTHETIC_ROOT),
        "the refusal names the table root it was given: {message}"
    );
    assert!(
        !message.contains("deletionVectors"),
        "the allow-listed feature declared alongside it is not refused: {message}"
    );

    let reads = store.reads();
    assert_eq!(
        reads
            .iter()
            .filter(|read| read.ends_with(SYNTHETIC_COMMIT_ZERO))
            .count(),
        1,
        "the refusal costs the one commit read that resolving the current version already \
         needed; the second read of it is the log replay the gate exists to precede: {reads:?}"
    );
}

/// Scenario: The gate runs inside snapshot construction, so no resolution path can bypass it
///
/// Reaches the constructor directly, with no `DeltaFormatReader` in the call: a gate at the
/// reader's entry point instead would let this call return an ungated snapshot, and every
/// downstream step — schema, partition columns, column-mapping mode, active files — is
/// reachable only through the value the constructor hands back.
#[tokio::test]
async fn the_protocol_gate_runs_inside_snapshot_construction() {
    let refused = DeltaSnapshot::open(synthetic_refused_table().await, SYNTHETIC_ROOT);

    assert!(
        refused.is_err(),
        "construction itself refuses, so no caller obtains a snapshot whose protocol went \
         unchecked"
    );

    let gated = DeltaSnapshot::open(synthetic_table(&[SYNTHETIC_ADD]).await, SYNTHETIC_ROOT)
        .expect("an allow-listed protocol still yields a snapshot");

    assert_eq!(
        gated.active_files(None).expect("its log replays").len(),
        1,
        "the other outcome of construction is a gated snapshot that resolves unchanged"
    );
}

#[test]
fn every_shipped_fixture_whose_reader_features_are_allow_listed_still_resolves() {
    for table in [
        "table-with-dv-small",
        "multi-part-stats",
        "stats-all-types",
        "basic_partitioned",
        "cdf-column-mapping-id-mode",
        "cdf-column-mapping-name-mode",
        "type-widening",
    ] {
        let snapshot = DeltaSnapshot::open(local_store(), &fixture_root(table))
            .unwrap_or_else(|error| panic!("{table} has only allow-listed features: {error}"));

        snapshot
            .active_files(None)
            .unwrap_or_else(|error| panic!("{table}'s log replays past the gate: {error}"));
    }
}

/// Scenario: Every recorded Delta type change is validated, and an unsupported one refuses its
/// column
///
/// Over the vendored `type-widening` fixture, whose commit-2 `schemaString` records a
/// `delta.typeChanges` entry on every one of its thirteen columns. Eleven pairs are on the
/// protocol's supported list; `byte_decimal` and `short_decimal` derive a negative `k1` against the
/// protocol's fixed base-10 precision for a `Byte`/`Short`/`Int` source, so they fail
/// `k1 >= k2 >= 0` and are refused one column at a time while the other eleven stay queryable.
#[test]
fn an_unsupported_recorded_type_change_refuses_only_its_own_column() {
    const QUERYABLE_COLUMNS: [&str; 11] = [
        "byte_long",
        "int_long",
        "float_double",
        "byte_double",
        "short_double",
        "int_double",
        "decimal_decimal_same_scale",
        "decimal_decimal_greater_scale",
        "int_decimal",
        "long_decimal",
        "date_timestamp_ntz",
    ];
    const REFUSED_COLUMNS: [(&str, &str, &str); 2] = [
        ("byte_decimal", "byte", "decimal(4,1)"),
        ("short_decimal", "short", "decimal(6,1)"),
    ];

    let snapshot = DeltaSnapshot::open(local_store(), &fixture_root("type-widening"))
        .expect("the type-widening fixture declares only allow-listed reader features");

    let (logical_fields, _partition_columns, refused_columns) =
        super::super::delta_schema::build_delta_table_schema(
            &snapshot.schema(),
            snapshot.column_mapping_mode(),
            snapshot.partition_columns(),
        )
        .expect("an unsupported recorded change is answered as a refused column, never an error");

    let mapped: Vec<&str> = logical_fields
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    assert_eq!(
        mapped, QUERYABLE_COLUMNS,
        "the eleven columns whose recorded change the protocol supports stay queryable, in schema \
         order"
    );

    let refused: Vec<&str> = refused_columns
        .iter()
        .map(|column| column.column_name.as_str())
        .collect();
    assert_eq!(
        refused,
        REFUSED_COLUMNS.map(|(column_name, _, _)| column_name),
        "only the two columns whose recorded change derives a negative k1 are refused"
    );

    for (column, (column_name, from_type, to_type)) in refused_columns.iter().zip(REFUSED_COLUMNS) {
        let reason = &column.reason;
        assert!(
            reason.contains(&format!("'{column_name}'")),
            "the refusal names the column it scopes to: {reason}"
        );
        assert!(
            reason.contains(&format!("'{from_type}'")),
            "the refusal names the recorded source type: {reason}"
        );
        assert!(
            reason.contains(&format!("'{to_type}'")),
            "the refusal names the recorded target type: {reason}"
        );
    }
}

/// Scenario: A reader feature outside the allow-list refuses the table before any log replay
///
/// `unshredded-variant` declares `variantType-preview`, which this engine does not implement,
/// over the real vendored fixture log rather than a synthetic one.
#[test]
fn a_vendored_fixture_declaring_a_reader_feature_outside_the_allow_list_is_refused() {
    let table = "unshredded-variant";
    let unsupported_feature = "variantType-preview";
    let root = fixture_root(table);
    let error = DeltaSnapshot::open(local_store(), &root)
        .err()
        .unwrap_or_else(|| panic!("{table} declares a reader feature outside the allow-list"));

    let message = error.to_string();
    assert!(
        message.contains(unsupported_feature),
        "{table}'s refusal names its unsupported feature: {message}"
    );
    assert!(
        message.contains(&root),
        "{table}'s refusal names the table root it was given: {message}"
    );
}

/// The `name`-mode physical name the synthetic `void` table assigns its mappable
/// `integer` column — the one its data file actually carries.
const VOID_TABLE_VALUE_PHYSICAL_NAME: &str = "col-a0f31c92";

/// The `name`-mode physical name that table assigns its `void` column, which NO data
/// file carries: the Delta protocol requires a writer to omit a `void` column from
/// every data file, so its assigned physical name never reaches Parquet.
const VOID_TABLE_VOID_PHYSICAL_NAME: &str = "col-7b4e2d15";

/// A `name`-column-mapping table declaring one `integer` and one `void` column, written
/// to disk beside a real Parquet file carrying the `integer` column's physical name
/// alone, and answering the table root both the log read and the scan resolve against.
///
/// On disk rather than in memory because the scan reads the Parquet file itself.
fn write_void_column_table(dir: &std::path::Path) -> String {
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Schema};
    use parquet::arrow::ArrowWriter;

    std::fs::create_dir_all(dir.join("_delta_log")).expect("synthetic table directory");

    let data_path = dir.join("part-0.parquet");
    let physical_schema = Arc::new(Schema::new(vec![Field::new(
        VOID_TABLE_VALUE_PHYSICAL_NAME,
        DataType::Int32,
        true,
    )]));
    {
        let file = std::fs::File::create(&data_path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&physical_schema), None).expect("arrow writer");
        let batch = RecordBatch::try_new(
            physical_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }
    let size = std::fs::metadata(&data_path).expect("stat parquet").len();

    let schema_string = serde_json::json!({
        "type": "struct",
        "fields": [
            {
                "name": "value",
                "type": "integer",
                "nullable": true,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    "delta.columnMapping.physicalName": VOID_TABLE_VALUE_PHYSICAL_NAME,
                },
            },
            {
                "name": "void_col",
                "type": "void",
                "nullable": true,
                "metadata": {
                    "delta.columnMapping.id": 2,
                    "delta.columnMapping.physicalName": VOID_TABLE_VOID_PHYSICAL_NAME,
                },
            },
        ],
    })
    .to_string();
    let protocol = serde_json::json!({"protocol": {
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": ["columnMapping"],
        "writerFeatures": ["columnMapping"],
    }});
    let metadata = serde_json::json!({"metaData": {
        "id": "synthetic-void",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": schema_string,
        "partitionColumns": [],
        "configuration": {
            "delta.columnMapping.mode": "name",
            "delta.columnMapping.maxColumnId": "2",
        },
        "createdTime": 1,
    }});
    let add = serde_json::json!({"add": {
        "path": "part-0.parquet",
        "partitionValues": {},
        "size": size,
        "modificationTime": 1,
        "dataChange": true,
    }});
    std::fs::write(
        dir.join("_delta_log").join(SYNTHETIC_COMMIT_ZERO),
        format!("{protocol}\n{metadata}\n{add}\n"),
    )
    .expect("synthetic commit is stored");

    url::Url::from_directory_path(dir)
        .expect("an absolute table root")
        .to_string()
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime builds")
        .block_on(future)
}

/// Scenario: A Delta type Exasol cannot represent natively is surfaced as a VARCHAR rendering
///
/// Runs the whole read: log replay, schema build, and the production raw-scan pipeline
/// over the real Parquet file. `name` column mapping is the mode that makes the case
/// observable — the `void` column binds by an assigned physical name, and no data file
/// carries it, because the protocol requires writers to omit `void` columns entirely.
/// Reading it must reconstruct an all-NULL column rather than fail on the unmatched
/// name, while its mappable sibling still reads its real values.
#[test]
fn a_void_column_reads_as_all_null_under_name_column_mapping() {
    use super::super::delta_schema::build_delta_table_schema;
    use crate::scan::spec::{CommonScanSpec, ScanSpec};
    use crate::scan::{build_raw_scan_physical_plan, register_files, session_config_for_spec};
    use datafusion::execution::context::SessionContext;

    let dir = std::env::temp_dir().join("lh_delta_void_column");
    let _ = std::fs::remove_dir_all(&dir);
    let table_root = write_void_column_table(&dir);

    let snapshot = DeltaSnapshot::open(local_store(), &table_root)
        .expect("the void table opens past the gate");
    assert_eq!(
        snapshot.column_mapping_mode(),
        ColumnMappingMode::Name,
        "the case under test is the `name`-mode physical-name binding"
    );

    let (logical_schema, partition_columns, refused_columns) = build_delta_table_schema(
        &snapshot.schema(),
        snapshot.column_mapping_mode(),
        snapshot.partition_columns(),
    )
    .expect("the void table's schema builds");
    let files = snapshot
        .active_files(None)
        .expect("the void table's log replays");

    assert!(
        refused_columns.is_empty(),
        "a void column is mapped, not refused: {refused_columns:?}"
    );
    let void_field = logical_schema
        .iter()
        .find(|field| field.name == "void_col")
        .expect("the void column carries a logical field");
    assert_eq!(
        (
            void_field.arrow_type.as_str(),
            void_field.physical_name.as_deref()
        ),
        ("utf8", Some(VOID_TABLE_VOID_PHYSICAL_NAME)),
        "the void column is tagged utf8 and binds by the physical name no data file carries"
    );

    let spec = ScanSpec {
        common: CommonScanSpec {
            table_root,
            projection: vec!["VALUE".into(), "VOID_COL".into()],
            logical_schema,
            partition_columns,
            ..Default::default()
        },
        files,
    };

    let batches = block_on(async {
        let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
        // The fixture reads local files through the session's own store, so the
        // resolved pair only has to be the placeholder backend the spec's own
        // `Default` carries — `register_files` uses it for the per-side redaction
        // set, not to reach the local fixture.
        let storage = crate::scan::ResolvedScanStorage::from_backends(
            crate::scan::spec::StorageBackend::S3(Default::default()),
            None,
        );
        register_files(&ctx, "scan_target", &spec, &storage)
            .await
            .expect("the void table registers");
        let plan = build_raw_scan_physical_plan(&ctx, &spec)
            .await
            .expect("the void table plans");
        datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("a void column reads as NULL rather than failing the scan")
    });

    let mut values = Vec::new();
    for batch in &batches {
        let read_values = batch.column(0).as_primitive::<Int32Type>();
        let void_column = batch.column(1);
        for row in 0..batch.num_rows() {
            assert!(
                void_column.is_null(row),
                "every row of a void column reads NULL"
            );
            values.push(read_values.value(row));
        }
    }
    values.sort_unstable();
    assert_eq!(
        values,
        vec![1, 2, 3],
        "the mappable sibling column still reads its real values"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: Equality on a partition column prunes every file in a non-matching partition
#[test]
fn a_partition_equality_prunes_every_file_in_a_non_matching_partition() {
    let letter_a = replay_fixture_pruned("basic_partitioned", letter_is_a());
    let letter_a_paths: Vec<&str> = letter_a.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        letter_a_paths,
        vec![
            "letter=a/part-00000-0dbe0cc5-e3bf-4fb0-b36a-b5fdd67fe843.c000.snappy.parquet",
            "letter=a/part-00000-a08d296a-d2c5-4a99-bea9-afcea42ba2e9.c000.snappy.parquet",
        ]
    );

    let letter_z = replay_fixture_pruned(
        "basic_partitioned",
        Predicate::eq(Expression::column(["letter"]), Expression::literal("z")),
    );
    assert!(letter_z.is_empty());
}

/// Scenario: Equality on a partition column prunes every file in a non-matching partition
#[test]
fn an_is_null_partition_predicate_resolves_the_default_partition_file_alone() {
    let files = replay_fixture_pruned(
        "basic_partitioned",
        Predicate::is_null(Expression::column(["letter"])),
    );
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "letter=__HIVE_DEFAULT_PARTITION__/part-00000-8eb7f29a-e6a1-436e-a638-bbf0a7953f09.c000.snappy.parquet",
        ]
    );
}

/// Scenario: A range predicate prunes files whose min/max bounds exclude the value
#[test]
fn a_range_predicate_prunes_files_whose_logged_bounds_exclude_the_value() {
    let by_number = replay_fixture_pruned(
        "basic_partitioned",
        Predicate::le(Expression::column(["number"]), Expression::literal(2i64)),
    );
    let number_paths: Vec<&str> = by_number.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        number_paths,
        vec![
            "letter=a/part-00000-a08d296a-d2c5-4a99-bea9-afcea42ba2e9.c000.snappy.parquet",
            "letter=b/part-00000-41954fb0-ef91-47e5-bd41-b75169c41c17.c000.snappy.parquet",
        ]
    );

    let by_id = replay_fixture_pruned(
        "multi-part-stats",
        Predicate::le(Expression::column(["id"]), Expression::literal(2i64)),
    );
    let id_paths: Vec<&str> = by_id.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        id_paths,
        vec![
            "test%25file%25prefix-part-00000-2699f745-4b33-4eb9-b3cf-04f6af08307f-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-f98612d6-6213-41f1-a006-a11beb0bb544-c000.snappy.parquet",
        ]
    );
}

#[test]
fn an_equality_predicate_on_a_data_column_prunes_by_its_logged_bounds() {
    let by_id = replay_fixture_pruned(
        "multi-part-stats",
        Predicate::eq(Expression::column(["id"]), Expression::literal(3i64)),
    );
    let id_paths: Vec<&str> = by_id.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        id_paths,
        vec![
            "test%25file%25prefix-part-00000-ff529603-203f-4a68-9ab1-d495e5c1c409-c000.snappy.parquet"
        ]
    );

    let by_missing_id = replay_fixture_pruned(
        "multi-part-stats",
        Predicate::eq(Expression::column(["id"]), Expression::literal(99i64)),
    );
    assert!(by_missing_id.is_empty());

    let by_value = replay_fixture_pruned(
        "multi-part-stats",
        Predicate::eq(
            Expression::column(["value"]),
            Expression::literal("value_3"),
        ),
    );
    let value_paths: Vec<&str> = by_value.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        value_paths,
        vec![
            "test%25file%25prefix-part-00000-ff529603-203f-4a68-9ab1-d495e5c1c409-c000.snappy.parquet"
        ]
    );
}

const SYNTHETIC_BOOLEAN_PREAMBLE: &str = concat!(
    r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
    "\n",
    r#"{"metaData":{"id":"synthetic-boolean","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"flag\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1}}"#,
    "\n",
    r#"{"add":{"path":"part-0.parquet","partitionValues":{},"size":100,"modificationTime":1,"dataChange":true}}"#,
    "\n",
);

async fn synthetic_boolean_table() -> Arc<dyn ObjectStore> {
    let store = Arc::new(InMemory::new());
    store
        .put(
            &StorePath::from("synthetic_table/_delta_log/00000000000000000000.json"),
            PutPayload::from(SYNTHETIC_BOOLEAN_PREAMBLE.to_string()),
        )
        .await
        .expect("synthetic commit is stored");
    store
}

const SYNTHETIC_PARTIAL_STATS_PREAMBLE: &str = concat!(
    r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
    "\n",
    r#"{"metaData":{"id":"synthetic-partial-stats","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1}}"#,
    "\n",
    r#"{"add":{"path":"part-0.parquet","partitionValues":{},"size":100,"modificationTime":1,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":1},\"nullCount\":{\"id\":0}}"}}"#,
    "\n",
    r#"{"add":{"path":"part-1.parquet","partitionValues":{},"size":100,"modificationTime":1,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"id\":5},\"maxValues\":{\"id\":5},\"nullCount\":{\"id\":0}}"}}"#,
    "\n",
);

async fn synthetic_partial_stats_table() -> Arc<dyn ObjectStore> {
    let store = Arc::new(InMemory::new());
    store
        .put(
            &StorePath::from("synthetic_table/_delta_log/00000000000000000000.json"),
            PutPayload::from(SYNTHETIC_PARTIAL_STATS_PREAMBLE.to_string()),
        )
        .await
        .expect("synthetic commit is stored");
    store
}

/// Scenario: A predicate the kernel cannot evaluate keeps every file
#[tokio::test]
async fn a_predicate_over_a_statless_or_boolean_column_keeps_every_file() {
    let statless = DeltaSnapshot::open(synthetic_table(&[SYNTHETIC_ADD]).await, SYNTHETIC_ROOT)
        .expect("synthetic table opens")
        .active_files(Some(Arc::new(Predicate::gt(
            Expression::column(["value"]),
            Expression::literal(0i32),
        ))))
        .expect("a predicate over a statless column still replays");
    assert_eq!(statless.len(), 1);

    let boolean = DeltaSnapshot::open(synthetic_boolean_table().await, SYNTHETIC_ROOT)
        .expect("synthetic table opens")
        .active_files(Some(Arc::new(Predicate::eq(
            Expression::column(["flag"]),
            Expression::literal(true),
        ))))
        .expect("a boolean equality predicate still replays");
    assert_eq!(boolean.len(), 1);
}

/// Scenario: An untranslatable conjunct disables pruning for that conjunct only
#[tokio::test]
async fn a_partly_untranslatable_conjunction_still_prunes_by_its_translatable_half() {
    let predicate = Predicate::and(
        Predicate::gt(Expression::column(["id"]), Expression::literal(3i32)),
        Predicate::gt(Expression::column(["value"]), Expression::literal(100i32)),
    );

    let files = DeltaSnapshot::open(synthetic_partial_stats_table().await, SYNTHETIC_ROOT)
        .expect("synthetic table opens")
        .active_files(Some(Arc::new(predicate)))
        .expect("a conjunction mixing a usable and an unusable comparison still replays");

    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, vec!["part-1.parquet"]);
}

/// Scenario: A predicate the kernel cannot evaluate keeps every file
///
/// Column mapping is NOT one of the keep-all cases: under both `name` and
/// `id` column mapping the kernel resolves a logical predicate column to its
/// physical statistics path, so pruning stays live and does not degrade to
/// keep-all.
#[test]
fn pruning_under_column_mapping_records_its_observed_behavior() {
    let name_mode_all = replay_fixture("cdf-column-mapping-name-mode");
    let name_mode_pruned = replay_fixture_pruned(
        "cdf-column-mapping-name-mode",
        Predicate::eq(Expression::column(["id"]), Expression::literal(3i64)),
    );
    let id_mode_all = replay_fixture("cdf-column-mapping-id-mode");
    let id_mode_pruned = replay_fixture_pruned(
        "cdf-column-mapping-id-mode",
        Predicate::eq(Expression::column(["id"]), Expression::literal(1i64)),
    );
    let id_mode_unmatched = replay_fixture_pruned(
        "cdf-column-mapping-id-mode",
        Predicate::eq(Expression::column(["id"]), Expression::literal(99i64)),
    );

    let name_mode_paths: Vec<&str> = name_mode_pruned.iter().map(|f| f.path.as_str()).collect();
    let id_mode_paths: Vec<&str> = id_mode_pruned.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(name_mode_all.len(), 3);
    assert_eq!(id_mode_all.len(), 3);
    assert_eq!(
        name_mode_paths,
        vec!["part-00015-1238a68f-8818-47d5-868f-fd5c382d5d95-c000.snappy.parquet"]
    );
    assert_eq!(
        id_mode_paths,
        vec!["part-00005-a921c063-2ccf-43f3-94ed-016896b6df42-c000.snappy.parquet"]
    );
    assert!(id_mode_unmatched.is_empty());
}

/// Scenario: A range predicate prunes files whose min/max bounds exclude the value
#[test]
fn a_between_keeps_one_bound_when_the_other_fails_to_convert() {
    let schema = open_fixture("multi-part-stats").schema();
    let node = json!({
        "type": "predicate_between",
        "expression": {"type": "column", "name": "ID"},
        "left": {"type": "literal_exactnumeric", "value": 4},
        "right": {"type": "literal_string", "value": "ten"},
    });
    let predicate =
        to_delta_predicate(&node, &schema).expect("a BETWEEN must keep the bound it can translate");

    let files = replay_fixture_pruned("multi-part-stats", predicate);
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "test%25file%25prefix-part-00000-323f4e76-58ff-48ce-bf0d-14d179e9bf0c-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-743ccd8e-15b0-49f2-b0e2-aa0efbf148ae-c000.snappy.parquet",
        ]
    );
}

/// Scenario: An untranslatable branch of an OR disables pruning entirely
#[test]
fn an_or_with_an_untranslatable_branch_keeps_every_file() {
    let schema = open_fixture("multi-part-stats").schema();
    let node = json!({"type": "predicate_or", "expressions": [
        {"type": "predicate_lessequal", "left": {"type": "column", "name": "ID"}, "right": {"type": "literal_exactnumeric", "value": 2}},
        {"type": "predicate_notequal", "left": {"type": "column", "name": "ID"}, "right": {"type": "literal_exactnumeric", "value": 7}},
    ]});

    assert_eq!(to_delta_predicate(&node, &schema), None);

    let forfeited = replay_fixture("multi-part-stats");
    let paths: Vec<&str> = forfeited.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "test%25file%25prefix-part-00000-2699f745-4b33-4eb9-b3cf-04f6af08307f-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-323f4e76-58ff-48ce-bf0d-14d179e9bf0c-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-743ccd8e-15b0-49f2-b0e2-aa0efbf148ae-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-f98612d6-6213-41f1-a006-a11beb0bb544-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-ff529603-203f-4a68-9ab1-d495e5c1c409-c000.snappy.parquet",
        ]
    );
}

/// Scenario: An IN list prunes as an OR-chain of equalities and never as an empty junction
#[test]
fn an_in_list_prunes_to_the_union_of_its_element_files() {
    let files = replay_fixture_pruned(
        "multi-part-stats",
        Predicate::or_from([
            Predicate::eq(Expression::column(["id"]), Expression::literal(1i64)),
            Predicate::eq(Expression::column(["id"]), Expression::literal(2i64)),
        ]),
    );
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "test%25file%25prefix-part-00000-2699f745-4b33-4eb9-b3cf-04f6af08307f-c000.snappy.parquet",
            "test%25file%25prefix-part-00000-f98612d6-6213-41f1-a006-a11beb0bb544-c000.snappy.parquet",
        ]
    );
}

/// Scenario: Enabling the kernel's skipping surfaces no statistic to the engine or the wire
#[test]
fn the_stats_disabling_option_is_what_suppresses_pruning() {
    let pruned = open_fixture("multi-part-stats").files_surviving(id_above_three());
    let unpruned = open_fixture("multi-part-stats").files_surviving_without_stats(id_above_three());

    assert_eq!(pruned.len(), 2);
    assert_eq!(unpruned.len(), 5);
}

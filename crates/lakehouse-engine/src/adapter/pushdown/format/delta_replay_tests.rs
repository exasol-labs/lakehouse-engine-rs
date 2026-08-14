use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

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
    DeltaSnapshot::open(local_store(), &fixture_root(table))
        .expect("fixture table opens")
        .active_files()
        .expect("fixture log replays")
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
        .active_files()
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
        .active_files()
        .expect_err("a negative size must not wrap into a huge unsigned size");

    let text = error.to_string();
    assert!(
        text.contains("-1") && text.contains("part-0.parquet"),
        "the refusal names the size and the file: {text}"
    );
}

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
        .active_files()
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

use super::*;
use crate::adapter::parquet_directory::MergeMode;
use crate::adapter::tests::parquet_fixture::{
    directory_options, in_memory_store, nullable, parquet_bytes,
};
use crate::adapter::tests::recording_store::RecordingStore;
use arrow::datatypes::{Field, Fields};
use object_store::memory::InMemory;

const FOLD_NO_HIVE: DirectoryOptions = DirectoryOptions {
    merge_mode: MergeMode::FoldEveryFile,
    hive_partitioning: false,
};

/// Builds the client through the production constructor's own prefix derivation, so no test
/// injects a prefix the real `base_path` would not have produced.
fn test_client(
    store: Arc<dyn ObjectStore>,
    base_path: &str,
    options: DirectoryOptions,
) -> DirectStorageCatalogClient {
    DirectStorageCatalogClient::over_store(store, base_path, options)
        .expect("the fixture base path is a valid storage URI")
}

fn id_file() -> Vec<u8> {
    parquet_bytes(vec![nullable("id", DataType::Int64)], 0)
}

async fn id_files_at(keys: &[&str]) -> Arc<InMemory> {
    let data = id_file();
    let objects: Vec<(&str, &[u8])> = keys.iter().map(|key| (*key, data.as_slice())).collect();
    in_memory_store(&objects).await
}

#[test]
fn client_is_reachable_as_a_boxed_catalog_client() {
    fn assert_is_catalog_client<T: CatalogClient>() {}
    assert_is_catalog_client::<DirectStorageCatalogClient>();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let client: Box<dyn CatalogClient> =
        Box::new(test_client(store, "s3://bucket/lake", FOLD_NO_HIVE));
    drop(client);
}

#[tokio::test]
async fn load_table_returns_a_clear_error_naming_the_direct_storage_kind() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let client = test_client(store, "s3://bucket/lake", FOLD_NO_HIVE);

    let ident = CatalogTableIdent {
        namespace: Vec::new(),
        name: "orders".to_string(),
    };
    let err = client
        .load_table(&ident)
        .await
        .expect_err("load_table is unreachable on this kind's happy path");

    let message = err.to_string();
    assert!(
        message.contains("direct-storage"),
        "must name the direct-storage kind: {message}"
    );
    assert!(
        message.contains("orders"),
        "must name the requested table: {message}"
    );
    assert!(
        message.contains("load_table"),
        "must state a direct-storage table is not loaded through load_table: {message}"
    );
}

#[tokio::test]
async fn first_level_directories_are_the_tables() {
    let inner = id_files_at(&[
        "lake/orders/part-0.parquet",
        "lake/events/part-0.parquet",
        "lake/orders/2026/part-1.parquet",
        "lake/notes.parquet",
    ])
    .await;

    let client = test_client(inner, "s3://bucket/lake", FOLD_NO_HIVE);
    let listing = client.list_tables(&[]).await.expect("enumeration succeeds");

    let mut names: Vec<&str> = listing
        .tables
        .iter()
        .map(|t| t.ident.name.as_str())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["events", "orders"],
        "a loose file and a nested directory contribute no table of their own"
    );

    for table in &listing.tables {
        assert!(
            table.ident.namespace.is_empty(),
            "a direct-storage identifier carries an empty namespace"
        );
        assert_eq!(table.format, TableFormat::Parquet);
        assert_eq!(table.vended_credential_key, None);
        assert_eq!(
            table.storage_location,
            Some(format!("s3://bucket/lake/{}", table.ident.name)),
            "the storage location is the base path followed by the directory name"
        );
    }
}

#[tokio::test]
async fn columns_and_files_come_from_the_shared_seam() {
    let nested = Fields::from(vec![Field::new("x", DataType::Int32, true)]);
    let data = parquet_bytes(
        vec![
            nullable("id", DataType::Int64),
            nullable("tags", DataType::Struct(nested)),
        ],
        0,
    );
    let inner = in_memory_store(&[
        ("lake/orders/part-0.parquet", &data),
        ("lake/orders/2026/part-1.parquet", &data),
        ("lake/orders/region=us/part-2.parquet", &data),
        // Objects the shared seam's own filter excludes: a marker file and a hidden Parquet file.
        ("lake/orders/_SUCCESS", b""),
        ("lake/orders/.hidden.parquet", b""),
    ])
    .await;

    let client = test_client(
        inner,
        "s3://bucket/lake",
        directory_options(MergeMode::FoldEveryFile, true),
    );
    let listing = client.list_tables(&[]).await.expect("enumeration succeeds");

    assert_eq!(listing.tables.len(), 1);
    let table = &listing.tables[0];
    let names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["id", "tags", "region"],
        "folded columns, then partition columns"
    );
    assert_eq!(
        table.columns[0].source_type,
        ColumnSourceType::Parquet("int64".to_string())
    );
    assert_eq!(
        table.columns[2].source_type,
        ColumnSourceType::Parquet("utf8".to_string()),
        "partition column is utf8"
    );
    assert_eq!(
        table.columns[1].source_type,
        ColumnSourceType::Parquet("utf8".to_string()),
        "a nested column is substituted to the JSON-string tag before it is rendered"
    );
}

#[tokio::test]
async fn directory_with_no_data_file_is_skipped_with_a_neutral_reason() {
    let inner = in_memory_store(&[
        ("lake/orders/part-0.parquet", &id_file()),
        ("lake/empty/_SUCCESS", b""),
    ])
    .await;

    let client = test_client(inner, "s3://bucket/lake", FOLD_NO_HIVE);
    let listing = client
        .list_tables(&[])
        .await
        .expect("createVirtualSchema completes with the remaining tables");

    assert_eq!(listing.tables.len(), 1);
    assert_eq!(listing.tables[0].ident.name, "orders");

    assert_eq!(listing.skipped.len(), 1);
    assert_eq!(listing.skipped[0].ident.name, "empty");
    assert_eq!(listing.skipped[0].reason, SkipReason::NoDataFile);
}

#[tokio::test]
async fn one_admission_limited_store_serves_the_whole_call() {
    let inner = id_files_at(&["lake/orders/part-0.parquet", "lake/events/part-0.parquet"]).await;

    let recording = RecordingStore::wrapping(inner);
    let client = test_client(
        Arc::clone(&recording) as Arc<dyn ObjectStore>,
        "s3://bucket/lake",
        FOLD_NO_HIVE,
    );

    let listing = client.list_tables(&[]).await.expect("enumeration succeeds");
    assert_eq!(listing.tables.len(), 2);

    let prefixes = recording.listed_prefixes();
    assert!(
        prefixes.iter().any(|p| p == "lake"),
        "the client's own top-level listing must reach the one recorded store: {prefixes:?}"
    );
    assert!(
        prefixes.iter().any(|p| p == "lake/orders"),
        "orders' file listing must reach the SAME recorded store: {prefixes:?}"
    );
    assert!(
        prefixes.iter().any(|p| p == "lake/events"),
        "events' file listing must reach the SAME recorded store: {prefixes:?}"
    );
}

#[tokio::test]
async fn new_derives_the_store_prefix_from_the_base_path() {
    let inner = id_files_at(&["lake/finance/orders/part-0.parquet"]).await;

    let recording = RecordingStore::wrapping(inner);
    let client = test_client(
        Arc::clone(&recording) as Arc<dyn ObjectStore>,
        "s3://bucket/lake/finance",
        FOLD_NO_HIVE,
    );

    let listing = client.list_tables(&[]).await.expect("enumeration succeeds");

    let names: Vec<&str> = listing
        .tables
        .iter()
        .map(|t| t.ident.name.as_str())
        .collect();
    assert_eq!(
        names,
        vec!["orders"],
        "the base path's own subtree is what the client enumerates"
    );

    let prefixes = recording.listed_prefixes();
    assert_eq!(
        prefixes.first().map(String::as_str),
        Some("lake/finance"),
        "the top-level listing runs under the prefix DERIVED from the base path — the bucket \
         dropped, everything below it kept: {prefixes:?}"
    );
}

#[tokio::test]
async fn merge_mode_selects_every_footer_or_exactly_one() {
    let files = ["lake/orders/part-0.parquet", "lake/orders/part-1.parquet"];

    for (merge_mode, expected) in [
        (MergeMode::SampleOneFile, &files[..1]),
        (MergeMode::FoldEveryFile, &files[..]),
    ] {
        let recording = RecordingStore::wrapping(id_files_at(&files).await);
        let client = test_client(
            Arc::clone(&recording) as Arc<dyn ObjectStore>,
            "s3://bucket/lake",
            directory_options(merge_mode, false),
        );

        let listing = client.list_tables(&[]).await.expect("enumeration succeeds");
        assert_eq!(listing.tables.len(), 1, "{merge_mode:?}");

        assert_eq!(
            recording.files_read(),
            expected,
            "{merge_mode:?} must open exactly these footers"
        );
    }
}

#[tokio::test]
async fn a_non_utc_timezone_column_is_declared_at_its_normalized_tag() {
    let declared = DataType::Timestamp(
        arrow::datatypes::TimeUnit::Microsecond,
        Some("America/New_York".into()),
    );
    let inner = in_memory_store(&[(
        "lake/events/part-0.parquet",
        &parquet_bytes(vec![nullable("occurred_at", declared.clone())], 0),
    )])
    .await;

    let directory = resolve_parquet_directory(
        &(Arc::clone(&inner) as Arc<dyn ObjectStore>),
        &StorePath::from("lake/events"),
        FOLD_NO_HIVE,
        &|_| true,
    )
    .await
    .expect("the fixture directory folds");
    assert_eq!(
        directory.schema.field(0).data_type(),
        &declared,
        "the fixture must reach enumeration carrying its ORIGINAL timezone label, or this test \
         exercises nothing"
    );

    let client = test_client(inner, "s3://bucket/lake", FOLD_NO_HIVE);
    let listing = client.list_tables(&[]).await.expect(
        "a legal timezone-aware Parquet column must enumerate, not fail the whole virtual schema",
    );

    assert_eq!(
        listing.tables[0].columns[0].source_type,
        ColumnSourceType::Parquet("timestamptz_us".to_string()),
        "the tag vocabulary discards WHICH timezone the file declared, by design; the column is \
         declared at that normalized tag — the same one the plan path's logical_schema renders"
    );
}

#[tokio::test]
async fn hive_partitioning_reaches_the_seam_on_enumeration() {
    let inner: Arc<dyn ObjectStore> = id_files_at(&["lake/sales/year=2026/part-0.parquet"]).await;

    for (hive_partitioning, expected) in [(false, vec!["id"]), (true, vec!["id", "year"])] {
        let listing = test_client(
            Arc::clone(&inner),
            "s3://bucket/lake",
            directory_options(MergeMode::FoldEveryFile, hive_partitioning),
        )
        .list_tables(&[])
        .await
        .expect("enumeration succeeds");
        assert_eq!(
            listing.tables[0]
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<&str>>(),
            expected,
            "HIVE_PARTITIONING = {hive_partitioning}"
        );
    }
}

use super::*;
use crate::adapter::parquet_directory::MergeMode;
use arrow::array::new_empty_array;
use arrow::datatypes::{Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use std::collections::BTreeMap;

fn nullable(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

/// A Parquet file declaring `fields` and holding zero rows: the declaration survives the batch's
/// own validation with no value needed per Arrow type.
fn parquet_bytes(fields: Vec<Field>) -> Vec<u8> {
    let schema = Arc::new(Schema::new(fields));
    let columns: Vec<arrow::array::ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| new_empty_array(field.data_type()))
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .expect("the fixture batch matches its own schema");

    let mut bytes = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut bytes, schema, None).expect("the fixture schema is writable");
    writer.write(&batch).expect("the fixture batch is writable");
    writer.close().expect("the fixture file closes");
    bytes
}

/// Builds the client through the production constructor's own prefix derivation, so no test
/// injects a prefix the real `base_path` would not have produced.
fn test_client(
    store: Arc<dyn ObjectStore>,
    base_path: &str,
    merge_mode: MergeMode,
) -> DirectStorageCatalogClient {
    DirectStorageCatalogClient::over_store(
        store,
        base_path,
        DirectoryOptions {
            merge_mode,
            hive_partitioning: false,
        },
    )
    .expect("the fixture base path is a valid storage URI")
}

fn keep_all(_: &BTreeMap<String, Option<String>>) -> bool {
    true
}

/// An [`ObjectStore`] decorator recording every `list`/`list_with_delimiter` prefix and `get`
/// location, so a test can confirm every table reached the SAME store and which footers were read.
#[derive(Debug)]
struct RecordingStore {
    inner: Arc<dyn ObjectStore>,
    prefixes: std::sync::Mutex<Vec<String>>,
    fetched: std::sync::Mutex<Vec<String>>,
}

impl RecordingStore {
    fn wrapping(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            prefixes: std::sync::Mutex::new(Vec::new()),
            fetched: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn prefixes_seen(&self) -> Vec<String> {
        self.prefixes
            .lock()
            .expect("read log is not poisoned")
            .clone()
    }

    /// The DISTINCT object locations any `get` reached, sorted — a footer read costs a variable
    /// number of ranged requests per file, so only the set of files opened is stable.
    fn files_fetched(&self) -> Vec<String> {
        let mut seen = self
            .fetched
            .lock()
            .expect("read log is not poisoned")
            .clone();
        seen.sort();
        seen.dedup();
        seen
    }

    fn record(&self, prefix: Option<&StorePath>) {
        self.prefixes
            .lock()
            .expect("read log is not poisoned")
            .push(prefix.map(ToString::to_string).unwrap_or_default());
    }
}

impl std::fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecordingStore {
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
        self.fetched
            .lock()
            .expect("read log is not poisoned")
            .push(location.to_string());
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
        self.record(prefix);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.record(prefix);
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

#[test]
fn client_is_reachable_as_a_boxed_catalog_client() {
    fn assert_is_catalog_client<T: CatalogClient>() {}
    assert_is_catalog_client::<DirectStorageCatalogClient>();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let client: Box<dyn CatalogClient> = Box::new(test_client(
        store,
        "s3://bucket/lake",
        MergeMode::FoldEveryFile,
    ));
    drop(client);
}

#[tokio::test]
async fn load_table_returns_a_clear_error_naming_the_direct_storage_kind() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let client = test_client(store, "s3://bucket/lake", MergeMode::FoldEveryFile);

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
    let inner = Arc::new(InMemory::new());
    for key in [
        "lake/orders/part-0.parquet",
        "lake/events/part-0.parquet",
        "lake/orders/2026/part-1.parquet",
        "lake/notes.parquet",
    ] {
        inner
            .put(
                &StorePath::from(key),
                PutPayload::from(parquet_bytes(vec![nullable("id", DataType::Int64)])),
            )
            .await
            .expect("the in-memory store accepts the fixture object");
    }

    let client = test_client(inner, "s3://bucket/lake", MergeMode::FoldEveryFile);
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
    let inner = Arc::new(InMemory::new());
    for key in [
        "lake/orders/part-0.parquet",
        "lake/orders/2026/part-1.parquet",
        "lake/orders/region=us/part-2.parquet",
    ] {
        inner
            .put(
                &StorePath::from(key),
                PutPayload::from(parquet_bytes(vec![
                    nullable("id", DataType::Int64),
                    nullable("tags", DataType::Struct(nested.clone())),
                ])),
            )
            .await
            .expect("the in-memory store accepts the fixture object");
    }
    // Objects the shared seam's own filter excludes: a marker file and a hidden Parquet file.
    for key in ["lake/orders/_SUCCESS", "lake/orders/.hidden.parquet"] {
        inner
            .put(&StorePath::from(key), PutPayload::from(Vec::<u8>::new()))
            .await
            .expect("the in-memory store accepts the fixture object");
    }

    let client = DirectStorageCatalogClient::over_store(
        inner,
        "s3://bucket/lake",
        DirectoryOptions {
            merge_mode: MergeMode::FoldEveryFile,
            hive_partitioning: true,
        },
    )
    .expect("the fixture base path is a valid storage URI");
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
    let inner = Arc::new(InMemory::new());
    inner
        .put(
            &StorePath::from("lake/orders/part-0.parquet"),
            PutPayload::from(parquet_bytes(vec![nullable("id", DataType::Int64)])),
        )
        .await
        .expect("the in-memory store accepts the fixture object");
    inner
        .put(
            &StorePath::from("lake/empty/_SUCCESS"),
            PutPayload::from(Vec::<u8>::new()),
        )
        .await
        .expect("the in-memory store accepts the fixture object");

    let client = test_client(inner, "s3://bucket/lake", MergeMode::FoldEveryFile);
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
    let inner = Arc::new(InMemory::new());
    for key in ["lake/orders/part-0.parquet", "lake/events/part-0.parquet"] {
        inner
            .put(
                &StorePath::from(key),
                PutPayload::from(parquet_bytes(vec![nullable("id", DataType::Int64)])),
            )
            .await
            .expect("the in-memory store accepts the fixture object");
    }

    let recording = RecordingStore::wrapping(inner);
    let client = test_client(
        Arc::clone(&recording) as Arc<dyn ObjectStore>,
        "s3://bucket/lake",
        MergeMode::FoldEveryFile,
    );

    let listing = client.list_tables(&[]).await.expect("enumeration succeeds");
    assert_eq!(listing.tables.len(), 2);

    let prefixes = recording.prefixes_seen();
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
    let inner = Arc::new(InMemory::new());
    inner
        .put(
            &StorePath::from("lake/finance/orders/part-0.parquet"),
            PutPayload::from(parquet_bytes(vec![nullable("id", DataType::Int64)])),
        )
        .await
        .expect("the in-memory store accepts the fixture object");

    let recording = RecordingStore::wrapping(inner);
    let client = test_client(
        Arc::clone(&recording) as Arc<dyn ObjectStore>,
        "s3://bucket/lake/finance",
        MergeMode::FoldEveryFile,
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

    let prefixes = recording.prefixes_seen();
    assert_eq!(
        prefixes.first().map(String::as_str),
        Some("lake/finance"),
        "the top-level listing runs under the prefix DERIVED from the base path — the bucket \
         dropped, everything below it kept: {prefixes:?}"
    );
}

#[tokio::test]
async fn merge_schema_true_and_false_select_every_footer_or_exactly_one() {
    let files = [
        "lake/orders/part-0.parquet".to_string(),
        "lake/orders/part-1.parquet".to_string(),
    ];

    for (merge_schema, expected) in [(false, vec![files[0].clone()]), (true, files.to_vec())] {
        let inner = Arc::new(InMemory::new());
        for key in &files {
            inner
                .put(
                    &StorePath::from(key.as_str()),
                    PutPayload::from(parquet_bytes(vec![nullable("id", DataType::Int64)])),
                )
                .await
                .expect("the in-memory store accepts the fixture object");
        }

        let recording = RecordingStore::wrapping(inner);
        let client = test_client(
            Arc::clone(&recording) as Arc<dyn ObjectStore>,
            "s3://bucket/lake",
            MergeMode::for_merge_schema(merge_schema),
        );

        let listing = client.list_tables(&[]).await.expect("enumeration succeeds");
        assert_eq!(listing.tables.len(), 1, "MERGE_SCHEMA={merge_schema}");

        assert_eq!(
            recording.files_fetched(),
            expected,
            "MERGE_SCHEMA={merge_schema} must open exactly these footers"
        );
    }
}

#[tokio::test]
async fn a_non_utc_timezone_column_is_declared_at_its_normalized_tag() {
    let declared = DataType::Timestamp(
        arrow::datatypes::TimeUnit::Microsecond,
        Some("America/New_York".into()),
    );
    let inner = Arc::new(InMemory::new());
    inner
        .put(
            &StorePath::from("lake/events/part-0.parquet"),
            PutPayload::from(parquet_bytes(vec![nullable(
                "occurred_at",
                declared.clone(),
            )])),
        )
        .await
        .expect("the in-memory store accepts the fixture object");

    let directory = resolve_parquet_directory(
        &(Arc::clone(&inner) as Arc<dyn ObjectStore>),
        &StorePath::from("lake/events"),
        DirectoryOptions {
            merge_mode: MergeMode::FoldEveryFile,
            hive_partitioning: false,
        },
        &keep_all,
    )
    .await
    .expect("the fixture directory folds");
    assert_eq!(
        directory.schema.field(0).data_type(),
        &declared,
        "the fixture must reach enumeration carrying its ORIGINAL timezone label, or this test \
         exercises nothing"
    );

    let client = test_client(inner, "s3://bucket/lake", MergeMode::FoldEveryFile);
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
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    inner
        .put(
            &StorePath::from("lake/sales/year=2026/part-0.parquet"),
            PutPayload::from(parquet_bytes(vec![nullable("id", DataType::Int64)])),
        )
        .await
        .expect("the in-memory store accepts the fixture object");

    let off = DirectStorageCatalogClient::over_store(
        Arc::clone(&inner),
        "s3://bucket/lake",
        DirectoryOptions {
            merge_mode: MergeMode::FoldEveryFile,
            hive_partitioning: false,
        },
    )
    .expect("the fixture base path is a valid storage URI")
    .list_tables(&[])
    .await
    .expect("enumeration succeeds");
    assert_eq!(
        off.tables[0]
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<&str>>(),
        vec!["id"],
        "HIVE_PARTITIONING = FALSE declares no partition column"
    );

    let on = DirectStorageCatalogClient::over_store(
        inner,
        "s3://bucket/lake",
        DirectoryOptions {
            merge_mode: MergeMode::FoldEveryFile,
            hive_partitioning: true,
        },
    )
    .expect("the fixture base path is a valid storage URI")
    .list_tables(&[])
    .await
    .expect("enumeration succeeds");
    assert_eq!(
        on.tables[0]
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<&str>>(),
        vec!["id", "year"],
        "HIVE_PARTITIONING = TRUE declares the partition column"
    );
}

use super::*;
use crate::adapter::tests::parquet_fixture::{
    directory_options, in_memory_store, nullable, parquet_bytes, parquet_footer_bytes,
    parquet_schema_footer_bytes, values,
};
use crate::types::mapping::arrow_to_exasol_type;
use arrow::datatypes::{Fields, TimeUnit};
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStoreExt, PutPayload};

/// The prefix every fixture is written under, so a test also pins that objects OUTSIDE it are
/// never listed.
const TABLE_ROOT: &str = "warehouse/direct/events";

/// Hands back the inner store's listing REVERSED (so a test can't pass by luck against
/// [`InMemory`](object_store::memory::InMemory)'s already-sorted order) and records every read,
/// HEAD vs. GET distinctly, and every listing, recursive vs. delimited.
#[derive(Debug)]
struct ReversedListingStore {
    inner: Arc<dyn ObjectStore>,
    reads: Arc<std::sync::Mutex<Vec<String>>>,
    listings: Arc<std::sync::Mutex<Vec<String>>>,
}

impl ReversedListingStore {
    fn wrapping(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            reads: Arc::new(std::sync::Mutex::new(Vec::new())),
            listings: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    fn reads(&self) -> Vec<String> {
        self.reads.lock().expect("read log is not poisoned").clone()
    }

    fn listings(&self) -> Vec<String> {
        self.listings
            .lock()
            .expect("listing log is not poisoned")
            .clone()
    }

    fn record_listing(&self, kind: &str, prefix: Option<&StorePath>) {
        self.listings
            .lock()
            .expect("listing log is not poisoned")
            .push(format!("{kind} {}", prefix.map_or("", StorePath::as_ref)));
    }

    fn files_read(&self) -> Vec<String> {
        let mut paths: Vec<String> = self
            .reads()
            .into_iter()
            .filter_map(|read| read.strip_prefix("get ").map(str::to_string))
            .collect();
        paths.sort();
        paths.dedup();
        paths
    }
}

impl std::fmt::Display for ReversedListingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReversedListingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ReversedListingStore {
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
        let verb = if options.head { "head" } else { "get" };
        self.reads
            .lock()
            .expect("read log is not poisoned")
            .push(format!("{verb} {location}"));
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
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.record_listing("recursive", prefix);
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            match inner.list(prefix.as_ref()).try_collect::<Vec<_>>().await {
                Ok(mut metas) => {
                    metas.reverse();
                    metas.into_iter().map(Ok).collect::<Vec<_>>()
                }
                Err(error) => vec![Err(error)],
            }
        })
        .flat_map(futures::stream::iter)
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.record_listing("delimited", prefix);
        let mut listed = self.inner.list_with_delimiter(prefix).await?;
        listed.objects.reverse();
        Ok(listed)
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

fn under_root(key: &str) -> String {
    format!("{TABLE_ROOT}/{key}")
}

fn rooted(keys: &[&str]) -> Vec<String> {
    keys.iter().map(|key| under_root(key)).collect()
}

/// A store holding each object at its key below [`TABLE_ROOT`], seen through the reversing,
/// read-recording decorator.
async fn store_holding(objects: &[(&str, &[u8])]) -> Arc<ReversedListingStore> {
    let keys: Vec<String> = objects.iter().map(|(key, _)| under_root(key)).collect();
    let rooted: Vec<(&str, &[u8])> = keys
        .iter()
        .zip(objects)
        .map(|(key, (_, bytes))| (key.as_str(), *bytes))
        .collect();
    ReversedListingStore::wrapping(in_memory_store(&rooted).await)
}

async fn store_with(keys: &[&str], data: &[u8]) -> Arc<ReversedListingStore> {
    let objects: Vec<(&str, &[u8])> = keys.iter().map(|key| (*key, data)).collect();
    store_holding(&objects).await
}

async fn resolve(
    probe: &Arc<ReversedListingStore>,
    merge_mode: MergeMode,
    keep: &PartitionKeepPredicate,
) -> Result<ParquetDirectory, UdfError> {
    resolve_parquet_directory(
        &(Arc::clone(probe) as Arc<dyn ObjectStore>),
        &root(),
        directory_options(merge_mode, true),
        keep,
    )
    .await
}

fn assert_mentions(error: &UdfError, expected: &[&str]) {
    let message = error.to_string();
    for fragment in expected {
        assert!(
            message.contains(fragment),
            "'{fragment}' missing from: {message}"
        );
    }
}

fn root() -> StorePath {
    StorePath::from(TABLE_ROOT)
}

fn column_types(schema: &SchemaRef) -> Vec<(String, DataType)> {
    schema
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect()
}

fn paths(directory: &ParquetDirectory) -> Vec<String> {
    directory
        .files
        .iter()
        .map(|file| file.path.as_ref().to_string())
        .collect()
}

fn keep_year_2026(values: &BTreeMap<String, Option<String>>) -> bool {
    values.get("year").and_then(|value| value.as_deref()) == Some("2026")
}

fn file_at<'a>(directory: &'a ParquetDirectory, suffix: &str) -> &'a ParquetFile {
    directory
        .files
        .iter()
        .find(|file| file.path.as_ref().ends_with(suffix))
        .unwrap_or_else(|| panic!("fixture file '{suffix}' is listed: {:?}", paths(directory)))
}

#[tokio::test]
async fn one_seam_returns_files_sizes_schema_and_footers() {
    let first = parquet_bytes(vec![nullable("id", DataType::Int32)], 3);
    let second = parquet_bytes(vec![nullable("id", DataType::Int64)], 5);
    let probe = store_holding(&[("a.parquet", &first), ("b.parquet", &second)]).await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("a prefix of readable Parquet files resolves");

    assert_eq!(
        paths(&directory),
        rooted(&["a.parquet", "b.parquet"]),
        "one call answers the data-file list"
    );
    assert_eq!(
        directory
            .files
            .iter()
            .map(|file| file.size)
            .collect::<Vec<u64>>(),
        vec![first.len() as u64, second.len() as u64],
        "each file's byte size comes back with it"
    );
    assert_eq!(
        column_types(&directory.schema),
        vec![("id".to_string(), DataType::Int64)],
        "the same call answers the folded schema"
    );

    let rows: Vec<i64> = directory
        .files
        .iter()
        .map(|file| {
            file.footer
                .as_ref()
                .expect("the fold-every-file mode read every footer")
                .file_metadata()
                .num_rows()
        })
        .collect();
    assert_eq!(
        rows,
        vec![3, 5],
        "the per-file metadata is the PARSED footer rather than the schema alone, so a later \
         consumer pruning files from footer statistics re-reads no footer"
    );
    assert!(
        directory.files.iter().all(|file| file
            .footer
            .as_ref()
            .is_some_and(|footer| footer.num_row_groups() == 1)),
        "the parsed footer carries the file's row groups, which is what statistics pruning needs"
    );
}

#[tokio::test]
async fn listing_is_recursive_filtered_and_deterministic() {
    let data = parquet_bytes(vec![nullable("id", DataType::Int32)], 1);
    let probe = store_holding(&[
        ("p1.parquet", &data),
        ("a/p2.parquet", &data),
        ("a/b/p3.parquet", &data),
        ("_SUCCESS", b""),
        ("_metadata", b""),
        ("p1.parquet.crc", b""),
        ("_staging/p4.parquet", &data),
        (".hidden/p5.parquet", &data),
    ])
    .await;
    probe
        .put(
            &StorePath::from("warehouse/direct/other/p6.parquet"),
            PutPayload::from(data.clone()),
        )
        .await
        .expect("the in-memory store accepts the fixture object");

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("the prefix resolves");

    assert_eq!(
        paths(&directory),
        rooted(&["a/b/p3.parquet", "a/p2.parquet", "p1.parquet"]),
        "recursion reaches unlimited depth, only '*.parquet' objects qualify, a segment beginning \
         '_' or '.' excludes the object below it, and the order does not depend on the store's \
         own listing order, which this store reverses"
    );
    assert!(
        directory
            .files
            .iter()
            .all(|file| file.size == data.len() as u64),
        "each size is carried from the listing response"
    );
    assert!(
        !probe.reads().iter().any(|read| read.starts_with("head ")),
        "no consumer issues a HEAD for a size the listing already reported: {:?}",
        probe.reads()
    );
}

#[tokio::test]
async fn directory_segments_follow_the_key_value_rule_and_decode_values() {
    let data = parquet_bytes(vec![nullable("id", DataType::Int32)], 1);
    let probe = store_with(
        &[
            "year=2024/month=01/p1.parquet",
            "plain/p2.parquet",
            "region=a%2Fb/p3.parquet",
            "month=__HIVE_DEFAULT_PARTITION__/p4.parquet",
            "flag=/p5.parquet",
            "year=2020/year=2021/p6.parquet",
            "x=1.parquet",
            "=x/p7.parquet",
            "v=%FF/p8.parquet",
        ],
        &data,
    )
    .await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("the prefix resolves");

    for (file, key, expected, rule) in [
        (
            "p1.parquet",
            "year",
            Some(Some("2024")),
            "a key=value segment declares the key",
        ),
        (
            "p1.parquet",
            "month",
            Some(Some("01")),
            "every such segment declares its key",
        ),
        (
            "p3.parquet",
            "region",
            Some(Some("a/b")),
            "the VALUE is percent-decoded",
        ),
        (
            "p4.parquet",
            "month",
            Some(None),
            "__HIVE_DEFAULT_PARTITION__ reads NULL",
        ),
        (
            "p5.parquet",
            "flag",
            Some(None),
            "an empty value reads NULL",
        ),
        (
            "p6.parquet",
            "year",
            Some(Some("2021")),
            "a repeated key takes its deepest value",
        ),
        (
            "p7.parquet",
            "",
            None,
            "a segment with an empty key declares nothing",
        ),
        (
            "p8.parquet",
            "v",
            Some(Some("%FF")),
            "a non-UTF-8 value keeps its raw text",
        ),
    ] {
        assert_eq!(
            file_at(&directory, file)
                .partition_values
                .get(key)
                .map(Option::as_deref),
            expected,
            "{file} '{key}': {rule}"
        );
    }
    assert!(
        !directory
            .partition_columns
            .iter()
            .any(|key| key == "x" || key.is_empty()),
        "neither a file NAME matching key=value nor an empty key declares a partition column: {:?}",
        directory.partition_columns
    );
}

#[tokio::test]
async fn declared_keys_are_the_ordered_union_and_fill_every_file() {
    let data = parquet_bytes(vec![nullable("id", DataType::Int32)], 1);
    let probe = store_with(&["A/p.parquet", "year=2026/p.parquet"], &data).await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("the prefix resolves");

    assert_eq!(directory.partition_columns, vec!["year".to_string()]);
    assert_eq!(
        column_types(&directory.schema).last(),
        Some(&("year".to_string(), DataType::Utf8)),
        "partition column appended as nullable Utf8"
    );
    assert_eq!(
        file_at(&directory, "A/p.parquet")
            .partition_values
            .get("year"),
        Some(&None),
        "a declared key missing from the path reads NULL"
    );
    assert_eq!(
        file_at(&directory, "year=2026/p.parquet")
            .partition_values
            .get("year"),
        Some(&Some("2026".to_string()))
    );
}

#[tokio::test]
async fn sample_mode_declares_only_the_sampled_files_keys() {
    let data = parquet_bytes(vec![nullable("id", DataType::Int32)], 1);
    let probe = store_with(&["a/p.parquet", "year=2026/p.parquet"], &data).await;

    let directory = resolve(&probe, MergeMode::SampleOneFile, &|_| true)
        .await
        .expect("the prefix resolves");

    assert!(
        directory.partition_columns.is_empty(),
        "only the sampled file declares keys: {:?}",
        directory.partition_columns
    );
    assert!(
        file_at(&directory, "year=2026/p.parquet")
            .partition_values
            .is_empty(),
        "an unsampled file's key is ignored"
    );
}

#[tokio::test]
async fn two_declared_keys_folding_to_the_same_name_fail_naming_both_spellings() {
    let data = parquet_bytes(vec![nullable("id", DataType::Int32)], 1);
    let cases: [(&str, &PartitionKeepPredicate); 2] = [
        ("Year=2026/p1.parquet", &|_| true),
        // The collision is checked on the unfiltered listing, so pruning one spelling still fails.
        ("Year=2025/p1.parquet", &keep_year_2026),
    ];
    for (upper, keep) in cases {
        let lower = "year=2026/p2.parquet";
        let probe = store_with(&[upper, lower], &data).await;

        let error = resolve(&probe, MergeMode::FoldEveryFile, keep)
            .await
            .err()
            .unwrap_or_else(|| panic!("{upper}: case-colliding keys must fail"));

        assert_mentions(
            &error,
            &["Year", "year", &under_root(upper), &under_root(lower)],
        );
    }
}

#[tokio::test]
async fn a_key_folding_onto_a_column_drops_the_column_and_keeps_the_key() {
    let stored_k = parquet_bytes(vec![nullable("K", DataType::Int32)], 1);
    let probe = store_with(&["k=1/p1.parquet", "k=2/p2.parquet"], &stored_k).await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("the key overrides the stored column");

    assert_eq!(
        column_types(&directory.schema),
        vec![("k".to_string(), DataType::Utf8)],
        "K is declared once, as the partition column"
    );
    assert_eq!(
        file_at(&directory, "p1.parquet").partition_values.get("k"),
        Some(&Some("1".to_string()))
    );
}

#[tokio::test]
async fn a_file_missing_the_colliding_keys_segment_fails_the_fold_naming_it() {
    let stored_k = parquet_bytes(vec![nullable("K", DataType::Int32)], 1);
    let stored_n = parquet_bytes(vec![nullable("n", DataType::Int32)], 1);
    for (merge_mode, objects, missing) in [
        (
            MergeMode::FoldEveryFile,
            [("k=1/p1.parquet", &stored_k), ("p2.parquet", &stored_k)],
            "p2.parquet",
        ),
        (
            MergeMode::SampleOneFile,
            [("a.parquet", &stored_k), ("k=1/b.parquet", &stored_n)],
            "a.parquet",
        ),
    ] {
        let objects = objects.map(|(key, bytes)| (key, bytes.as_slice()));
        let probe = store_holding(&objects).await;

        let error = resolve(&probe, merge_mode, &|_| true)
            .await
            .err()
            .unwrap_or_else(|| panic!("{merge_mode:?}: '{missing}' stores K without a k= segment"));

        assert_mentions(&error, &["K", "k", &under_root(missing)]);
    }
}

#[tokio::test]
async fn a_default_or_empty_key_segment_counts_as_carrying_the_colliding_key() {
    let stored_k = parquet_bytes(vec![nullable("K", DataType::Int32)], 1);
    let probe = store_with(
        &[
            "k=1/p1.parquet",
            "k=__HIVE_DEFAULT_PARTITION__/p2.parquet",
            "k=/p3.parquet",
        ],
        &stored_k,
    )
    .await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("a NULL-valued segment still overrides the stored column");

    assert_eq!(
        column_types(&directory.schema),
        vec![("k".to_string(), DataType::Utf8)],
        "K is declared exactly once, as the partition column"
    );
    for suffix in ["p2.parquet", "p3.parquet"] {
        assert_eq!(
            file_at(&directory, suffix).partition_values.get("k"),
            Some(&None),
            "'{suffix}' carries the segment and reads K as NULL"
        );
    }
}

#[tokio::test]
async fn hive_partitioning_off_parses_no_segment_and_checks_no_collision() {
    let probe = store_holding(&[
        (
            "Year=2026/p1.parquet",
            &parquet_bytes(vec![nullable("id", DataType::Int32)], 1),
        ),
        (
            "year=2026/p2.parquet",
            &parquet_bytes(vec![nullable("K", DataType::Int32)], 1),
        ),
    ])
    .await;

    let directory = resolve_parquet_directory(
        &(probe as Arc<dyn ObjectStore>),
        &root(),
        directory_options(MergeMode::FoldEveryFile, false),
        &|_| true,
    )
    .await
    .expect("no keys, so no collision check");

    assert!(directory.partition_columns.is_empty());
    assert!(
        directory
            .files
            .iter()
            .all(|file| file.partition_values.is_empty()),
        "every file entry carries an empty partition-value map"
    );
    assert_eq!(
        column_types(&directory.schema),
        vec![
            ("id".to_string(), DataType::Int32),
            ("K".to_string(), DataType::Int32),
        ],
        "key=value directories are plain directories"
    );
}

#[tokio::test]
async fn a_keep_predicate_narrows_files_before_any_footer_is_read() {
    let data = parquet_bytes(vec![nullable("id", DataType::Int32)], 1);
    let probe = store_with(&["year=2026/p1.parquet", "year=2025/p2.parquet"], &data).await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &keep_year_2026)
        .await
        .expect("the prefix resolves");

    assert_eq!(
        directory.partition_columns,
        vec!["year".to_string()],
        "keys come from the unfiltered listing"
    );
    assert_eq!(paths(&directory), rooted(&["year=2026/p1.parquet"]));
    assert_eq!(
        probe.files_read(),
        rooted(&["year=2026/p1.parquet"]),
        "a rejected file costs no footer read"
    );
}

#[tokio::test]
async fn sample_mode_reads_the_first_unfiltered_footer_even_when_keep_rejects_it() {
    let probe = store_holding(&[
        (
            "year=2025/p1.parquet",
            &parquet_bytes(vec![nullable("a", DataType::Int32)], 1),
        ),
        (
            "year=2026/p2.parquet",
            &parquet_bytes(vec![nullable("b", DataType::Int32)], 1),
        ),
    ])
    .await;

    let directory = resolve(&probe, MergeMode::SampleOneFile, &keep_year_2026)
        .await
        .expect("the prefix resolves");

    assert_eq!(
        paths(&directory),
        rooted(&["year=2026/p2.parquet"]),
        "the rejected sampled file is not returned"
    );
    assert_eq!(
        probe.files_read(),
        rooted(&["year=2025/p1.parquet"]),
        "the sampled footer is the first UNFILTERED file's, kept or not"
    );
    assert_eq!(
        column_types(&directory.schema),
        vec![
            ("a".to_string(), DataType::Int32),
            ("year".to_string(), DataType::Utf8),
        ],
        "the schema comes from the sampled file's footer"
    );
    assert!(
        file_at(&directory, "p2.parquet").footer.is_none(),
        "the rejected sampled file's footer attaches to no kept file"
    );
}

#[tokio::test]
async fn fold_widens_pairwise_in_listing_order_to_the_widest_reachable_type() {
    let probe = store_holding(&[
        (
            "a.parquet",
            &parquet_bytes(
                vec![
                    nullable("n", DataType::Int8),
                    nullable("f", DataType::Float32),
                    nullable("d", DataType::Decimal128(10, 2)),
                ],
                1,
            ),
        ),
        (
            "b.parquet",
            &parquet_bytes(
                vec![
                    nullable("n", DataType::Int32),
                    nullable("f", DataType::Float64),
                    nullable("d", DataType::Decimal128(12, 2)),
                ],
                1,
            ),
        ),
        (
            "c.parquet",
            &parquet_bytes(vec![nullable("n", DataType::Int64)], 1),
        ),
    ])
    .await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("every pair is a recorded relaxation");

    assert_eq!(
        column_types(&directory.schema),
        vec![
            ("n".to_string(), DataType::Int64),
            ("f".to_string(), DataType::Float64),
            ("d".to_string(), DataType::Decimal128(12, 2)),
        ],
        "each column resolves to the widest type successive supported widenings reach \
         (n: Int8 -> Int32 -> Int64)"
    );
}

#[tokio::test]
async fn a_pair_no_widening_rule_covers_fails_naming_the_column_both_types_and_both_files() {
    let probe = store_holding(&[
        (
            "a.parquet",
            &parquet_bytes(vec![nullable("n", DataType::Int32)], 1),
        ),
        (
            "b.parquet",
            &parquet_bytes(vec![nullable("n", DataType::Utf8)], 1),
        ),
    ])
    .await;

    let error = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .err()
        .expect("no supported pair covers Int32 and Utf8, so the fold must fail rather than guess");

    assert_mentions(
        &error,
        &[
            "n",
            "Int32",
            "Utf8",
            &under_root("a.parquet"),
            &under_root("b.parquet"),
        ],
    );
}

#[tokio::test]
async fn merge_mode_selects_every_footer_or_the_first() {
    let int32 = parquet_bytes(vec![nullable("n", DataType::Int32)], 1);
    let int64 = parquet_bytes(vec![nullable("n", DataType::Int64)], 1);
    let objects: [(&str, &[u8]); 3] = [
        ("a.parquet", &int32),
        ("b.parquet", &int64),
        ("c.parquet", &int64),
    ];

    let every = store_holding(&objects).await;
    let folded = resolve(&every, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("the prefix resolves");
    let one = store_holding(&objects).await;
    let sampled = resolve(&one, MergeMode::SampleOneFile, &|_| true)
        .await
        .expect("the prefix resolves");

    assert_eq!(
        one.files_read(),
        rooted(&["a.parquet"]),
        "the sample-one-file mode reads EXACTLY one footer, the FIRST in the deterministic order, \
         which this store's reversed listing would otherwise make the last"
    );
    assert_eq!(
        every.files_read().len(),
        3,
        "the other mode reads every one"
    );
    assert_eq!(
        paths(&sampled),
        paths(&folded),
        "the mode narrows which footers are read, never which files are scanned"
    );
    assert_eq!(
        column_types(&sampled.schema),
        vec![("n".to_string(), DataType::Int32)],
        "the sampled schema is the first file's own declaration"
    );
    assert_eq!(
        column_types(&folded.schema),
        vec![("n".to_string(), DataType::Int64)],
        "the folded schema is the widened one, so the mode is observable where footers differ"
    );
    assert_eq!(
        sampled
            .files
            .iter()
            .map(|file| file.footer.is_some())
            .collect::<Vec<bool>>(),
        vec![true, false, false],
        "metadata is PAIRED with the files whose footers were read and ABSENT for the rest, so a \
         consumer reads presence per file rather than indexing positionally"
    );
    assert!(
        folded.files.iter().all(|file| file.footer.is_some()),
        "under the fold-every-file mode every listed file carries its parsed footer"
    );

    let single = store_with(&["a.parquet"], &int32).await;
    for merge_mode in [MergeMode::FoldEveryFile, MergeMode::SampleOneFile] {
        let directory = resolve(&single, merge_mode, &|_| true)
            .await
            .expect("the prefix resolves");
        assert_eq!(
            column_types(&directory.schema),
            vec![("n".to_string(), DataType::Int32)],
            "{merge_mode:?}: a single-file prefix declares that file's own schema under both modes"
        );
    }
}

#[tokio::test]
async fn folded_columns_are_the_nullable_union_in_first_appearance_order() {
    let probe = store_holding(&[
        (
            "a.parquet",
            &parquet_bytes(
                vec![
                    Field::new("A", DataType::Int32, false),
                    nullable("B", DataType::Utf8),
                ],
                0,
            ),
        ),
        (
            "b.parquet",
            &parquet_bytes(
                vec![
                    nullable("A", DataType::Int32),
                    nullable("C", DataType::Utf8),
                ],
                0,
            ),
        ),
    ])
    .await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("the prefix resolves");

    assert_eq!(
        directory
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<String>>(),
        vec!["A", "B", "C"],
        "the folded column set is the union, ordered by first appearance in the listing order"
    );
    assert!(
        directory
            .schema
            .fields()
            .iter()
            .all(|field| field.is_nullable()),
        "a column absent from one file is filled with NULL for that file's rows, so a column \
         declared required in the file that carries it is still declared nullable here"
    );
}

#[tokio::test]
async fn two_names_equal_after_the_uppercase_fold_fail_the_fold() {
    let probe = store_holding(&[
        (
            "a.parquet",
            &parquet_bytes(vec![nullable("id", DataType::Int32)], 0),
        ),
        (
            "b.parquet",
            &parquet_bytes(vec![nullable("ID", DataType::Int32)], 0),
        ),
    ])
    .await;

    let error = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .err()
        .expect("the declaration would advertise a duplicate column name");

    assert_mentions(
        &error,
        &[
            "id",
            "ID",
            &under_root("a.parquet"),
            &under_root("b.parquet"),
        ],
    );
}

#[tokio::test]
async fn nested_and_unrepresentable_types_fold_to_the_string_declaration() {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Int32, true),
        ])),
        false,
    );
    let probe = store_holding(&[(
        "a.parquet",
        &parquet_bytes(
            vec![
                nullable(
                    "s",
                    DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int32, true)])),
                ),
                nullable(
                    "l",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                ),
                nullable("m", DataType::Map(Arc::new(entries), false)),
                nullable("b", DataType::Binary),
                nullable("t", DataType::Time64(TimeUnit::Microsecond)),
            ],
            0,
        ),
    )])
    .await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("a nested column is folded rather than refused");

    assert_eq!(
        directory.schema.fields().len(),
        5,
        "every declared column reaches the caller rather than being dropped by the fold"
    );
    for field in directory.schema.fields() {
        assert!(
            matches!(
                field.data_type(),
                DataType::Struct(_)
                    | DataType::List(_)
                    | DataType::Map(_, _)
                    | DataType::Binary
                    | DataType::Time64(_)
            ),
            "the seam returns each column's own Arrow type and adds NO classification of its own: \
             {field:?}"
        );
        assert_eq!(
            arrow_to_exasol_type(field.data_type()),
            "VARCHAR(2000000)",
            "the engine's ONE existing Arrow classifier is what declares the column, so the \
             declared Exasol type and the logical Arrow tag stay in lockstep"
        );
    }
}

#[tokio::test]
async fn a_prefix_holding_no_data_file_answers_an_empty_list_and_schema() {
    let probe = store_holding(&[("_SUCCESS", b"")]).await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("an empty prefix is answered rather than raised as a panic");

    assert!(
        directory.files.is_empty(),
        "no object qualifies as a data file"
    );
    assert_eq!(
        directory.schema.fields().len(),
        0,
        "whether an empty prefix is a table at all is the caller's decision, not this module's"
    );
    assert!(
        probe.files_read().is_empty(),
        "no footer is read when no file qualifies"
    );
}

#[test]
fn the_store_prefix_is_the_percent_decoded_path_below_the_store_root() {
    for (uri, expected) in [
        ("s3://warehouse/direct/events", "direct/events"),
        ("s3://warehouse", ""),
        ("s3://warehouse/", ""),
        ("s3a://warehouse/direct/events/", "direct/events"),
        ("s3://warehouse/direct/my%20events", "direct/my events"),
        (
            "abfss://container@account.dfs.core.windows.net/direct/events",
            "direct/events",
        ),
    ] {
        let prefix = store_prefix(uri).unwrap_or_else(|e| panic!("'{uri}' names a prefix: {e}"));
        assert_eq!(prefix.as_ref(), expected, "for '{uri}'");
    }

    let err = store_prefix("warehouse/direct").expect_err("a schemeless value names no store");
    assert!(
        err.to_string().contains("warehouse/direct"),
        "the refusal must name the value it rejected: {err}"
    );
}

/// Scenario: The file pattern selects the listing depth and the file-name rule
#[tokio::test]
async fn file_pattern_selects_listing_depth_and_file_name_rule() {
    let body: &[u8] = b"a non-empty object";
    let probe = store_holding(&[
        ("a.parquet", body),
        ("b", body),
        ("sub/c.parquet", body),
        ("sub/d", body),
        ("_SUCCESS", body),
        (".hidden", body),
        ("e.parquet", b""),
    ])
    .await;
    let store = Arc::clone(&probe) as Arc<dyn ObjectStore>;

    for (pattern, expected) in [
        (
            FilePattern::ParquetAtAnyDepth,
            vec!["a.parquet", "sub/c.parquet"],
        ),
        (FilePattern::AnyDirectChild, vec!["a.parquet", "b"]),
    ] {
        let files = list_location_files(&store, &root(), pattern)
            .await
            .unwrap_or_else(|e| panic!("the prefix lists under {pattern:?}: {e}"));

        assert_eq!(
            files
                .iter()
                .map(|file| file.path.as_ref().to_string())
                .collect::<Vec<_>>(),
            rooted(&expected),
            "{pattern:?}: a '_' or '.' segment and a zero-length object are never data files, \
             and the order does not depend on the store's own listing order"
        );
        assert!(
            files.iter().all(|file| file.size == body.len() as u64),
            "{pattern:?}: each size is carried from the listing response"
        );
    }
    assert_eq!(
        probe.listings(),
        vec![
            format!("recursive {TABLE_ROOT}"),
            format!("delimited {TABLE_ROOT}"),
        ],
        "only the any-depth pattern lists recursively; the direct-child one lists the direct \
         children alone"
    );
    assert!(
        probe.reads().is_empty(),
        "the listing reads no object: {:?}",
        probe.reads()
    );
}

/// Scenario: A catalog-registered location is listed by its raw object key
#[tokio::test]
async fn a_raw_key_location_is_listed_without_percent_decoding() {
    let location = "s3://bucket/tbl/p_str=a b%2Fc/";
    let body: &[u8] = b"a non-empty object";
    let store = in_memory_store(&[("tbl/p_str=a b%2Fc/f1", body), ("tbl/p_str=a b/c/f2", body)])
        .await as Arc<dyn ObjectStore>;

    let (store_root, prefix) =
        raw_location_prefix(location).expect("an S3 location names a store root and a key");
    let files = list_location_files(&store, &prefix, FilePattern::AnyDirectChild)
        .await
        .expect("the raw key lists");

    assert_eq!(
        (store_root.as_str(), prefix.as_ref()),
        ("s3://bucket", "tbl/p_str=a b%2Fc"),
        "the key is taken verbatim, with no percent-decoding"
    );
    assert_eq!(
        files
            .iter()
            .map(|file| file.path.as_ref().to_string())
            .collect::<Vec<_>>(),
        vec!["tbl/p_str=a b%2Fc/f1"],
        "the literal key lists, never the key its decoding would name"
    );
    assert_eq!(
        store_prefix(location)
            .expect("the location also parses as a URL")
            .as_ref(),
        "tbl/p_str=a b/c",
        "a direct-storage or Unity storage URI is still decoded as a URL"
    );
    for (other, expected_root, expected_key) in [
        ("s3a://bucket/tbl/", "s3://bucket", "tbl"),
        ("S3://bucket/tbl", "s3://bucket", "tbl"),
        ("s3://bucket", "s3://bucket", ""),
        ("s3://bucket/", "s3://bucket", ""),
        ("s3://bucket/a#b/c?d", "s3://bucket", "a#b/c?d"),
    ] {
        let (root, key) =
            raw_location_prefix(other).unwrap_or_else(|e| panic!("'{other}' names a key: {e}"));
        assert_eq!(
            (root.as_str(), key.as_ref()),
            (expected_root, expected_key),
            "for '{other}'"
        );
    }
}

#[test]
fn a_location_naming_no_scheme_bucket_or_valid_key_is_refused_naming_it() {
    for (location, cause) in [
        ("bucket/tbl", "no scheme"),
        ("://bucket/tbl", "no scheme"),
        ("s3:///tbl", "no bucket"),
        ("s3://bucket/tbl//part", "empty path segment"),
    ] {
        let error = raw_location_prefix(location)
            .expect_err("a location that names no raw key must be refused");
        assert_mentions(&error, &[location, cause]);
    }
}

/// Scenario: The listing answer serves a caller that declares its own partition columns
#[tokio::test]
async fn listing_answer_fills_caller_declared_partition_columns_and_reads_no_footer() {
    let probe = store_with(
        &[
            "year=2024/region=eu/a.parquet",
            "Year=2025/b.parquet",
            "other=x/c.parquet",
            "year=1999/YEAR=2000/year=2001/d.parquet",
            "e.parquet",
            "_SUCCESS",
        ],
        b"not a parquet file",
    )
    .await;
    let store = Arc::clone(&probe) as Arc<dyn ObjectStore>;
    let declared = ["year".to_string(), "region".to_string()];

    let files = list_parquet_files(&store, &root(), &declared, &|_| true)
        .await
        .expect("a listing that reads no footer succeeds over unreadable bodies");

    let listed: Vec<_> = files
        .iter()
        .map(|file| (file.path.to_string(), file.partition_values.clone()))
        .collect();
    assert_eq!(
        listed,
        vec![
            (
                under_root("Year=2025/b.parquet"),
                values(&[("year", Some("2025")), ("region", None)]),
            ),
            (
                under_root("e.parquet"),
                values(&[("year", None), ("region", None)]),
            ),
            (
                under_root("other=x/c.parquet"),
                values(&[("year", None), ("region", None)]),
            ),
            (
                under_root("year=1999/YEAR=2000/year=2001/d.parquet"),
                values(&[("year", Some("2001")), ("region", None)]),
            ),
            (
                under_root("year=2024/region=eu/a.parquet"),
                values(&[("year", Some("2024")), ("region", Some("eu"))]),
            ),
        ],
        "every declared column is keyed by the caller's spelling and filled from the deepest \
         segment equal to it under the uppercase fold; an undeclared segment contributes nothing"
    );
    assert!(
        files.iter().all(|file| file.footer.is_none()) && probe.files_read().is_empty(),
        "the listing answer reads no object: {:?}",
        probe.reads()
    );

    let kept = list_parquet_files(&store, &root(), &declared, &|values| {
        values.get("region").and_then(|value| value.as_deref()) == Some("eu")
    })
    .await
    .expect("a narrowing keep predicate resolves");
    assert_eq!(
        kept.iter()
            .map(|file| file.path.as_ref().to_string())
            .collect::<Vec<_>>(),
        rooted(&["year=2024/region=eu/a.parquet"]),
        "the keep predicate runs on the filled values before the files are returned"
    );
}

fn binary_column(column: &str, member_path: Option<&str>, declared: &str) -> BinaryColumn {
    BinaryColumn {
        column: column.to_string(),
        member_path: member_path.map(str::to_string),
        declared: declared.to_string(),
        nested_enum: declared == "enum",
    }
}

/// Scenario: A direct-storage unannotated BYTE_ARRAY column is refused
#[tokio::test]
async fn every_footer_leaf_is_classified_by_its_parquet_annotation() {
    let footer = parquet_footer_bytes(
        "message classified {
            OPTIONAL INT32 id;
            OPTIONAL BYTE_ARRAY name (STRING);
            OPTIONAL BYTE_ARRAY legacy_utf8 (UTF8);
            OPTIONAL BYTE_ARRAY doc_json (JSON);
            OPTIONAL BYTE_ARRAY kind (ENUM);
            OPTIONAL BYTE_ARRAY price (DECIMAL(10,2));
            OPTIONAL FIXED_LEN_BYTE_ARRAY (8) amount (DECIMAL(18,2));
            OPTIONAL FIXED_LEN_BYTE_ARRAY (2) half (FLOAT16);
            OPTIONAL FIXED_LEN_BYTE_ARRAY (12) span (INTERVAL);
            OPTIONAL BYTE_ARRAY legacy_name;
            OPTIONAL BYTE_ARRAY doc_bson (BSON);
            OPTIONAL BYTE_ARRAY shape (GEOMETRY);
            OPTIONAL BYTE_ARRAY area (GEOGRAPHY);
            OPTIONAL FIXED_LEN_BYTE_ARRAY (16) uid (UUID);
            OPTIONAL FIXED_LEN_BYTE_ARRAY (20) digest;
            OPTIONAL GROUP s {
                OPTIONAL BYTE_ARRAY ok (STRING);
                OPTIONAL BYTE_ARRAY k (ENUM);
                OPTIONAL BYTE_ARRAY raw;
            }
            OPTIONAL GROUP texts (LIST) {
                REPEATED GROUP list {
                    OPTIONAL BYTE_ARRAY element (STRING);
                }
            }
            OPTIONAL GROUP l (LIST) {
                REPEATED GROUP list {
                    OPTIONAL BYTE_ARRAY element;
                }
            }
            OPTIONAL GROUP m (MAP) {
                REPEATED GROUP key_value {
                    REQUIRED BYTE_ARRAY key (STRING);
                    OPTIONAL FIXED_LEN_BYTE_ARRAY (4) value;
                }
            }
            REPEATED BYTE_ARRAY tags (ENUM);
        }",
    );
    let probe = store_holding(&[("a.parquet", &footer)]).await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("every annotation of the fixture folds to an Arrow type");

    assert_eq!(
        directory.binary_columns,
        vec![
            binary_column("legacy_name", None, "binary"),
            binary_column("doc_bson", None, "bson"),
            binary_column("shape", None, "geometry"),
            binary_column("area", None, "geography"),
            binary_column("uid", None, "uuid"),
            binary_column("digest", None, "fixed(20)"),
            binary_column("s", Some("s.k"), "enum"),
            binary_column("l", Some("l.list.element"), "binary"),
            binary_column("m", Some("m.key_value.value"), "fixed(4)"),
            binary_column("tags", None, "enum"),
        ],
        "a STRING, JSON, top-level ENUM, DECIMAL, FLOAT16, or INTERVAL leaf is not binary; every \
         other byte-array leaf is, named in the file's terms, with the column's first such member"
    );
    assert_eq!(
        directory.schema.fields().len(),
        20,
        "classifying the leaves drops no column from the folded schema"
    );
}

/// Scenario: A direct-storage ENUM column reads as text
#[tokio::test]
async fn a_converted_type_decides_a_leaf_that_carries_no_logical_type() {
    use parquet::basic::Repetition;
    use parquet::schema::types::Type as SchemaType;

    let leaf = |name: &'static str, physical: PhysicalType, converted: ConvertedType| {
        SchemaType::primitive_type_builder(name, physical)
            .with_repetition(Repetition::OPTIONAL)
            .with_converted_type(converted)
    };
    let fields = vec![
        leaf("kind", PhysicalType::BYTE_ARRAY, ConvertedType::ENUM).build(),
        leaf("doc_bson", PhysicalType::BYTE_ARRAY, ConvertedType::BSON).build(),
        leaf("doc_json", PhysicalType::BYTE_ARRAY, ConvertedType::JSON).build(),
        leaf("price", PhysicalType::BYTE_ARRAY, ConvertedType::DECIMAL)
            .with_precision(10)
            .with_scale(2)
            .build(),
        leaf(
            "amount",
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            ConvertedType::DECIMAL,
        )
        .with_length(8)
        .with_precision(18)
        .with_scale(2)
        .build(),
        SchemaType::group_type_builder("s")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![Arc::new(
                leaf("k", PhysicalType::BYTE_ARRAY, ConvertedType::ENUM)
                    .build()
                    .expect("a converted ENUM byte array is valid"),
            )])
            .build(),
    ];
    let schema = SchemaType::group_type_builder("converted")
        .with_fields(
            fields
                .into_iter()
                .map(|field| Arc::new(field.expect("each fixture leaf is a valid Parquet type")))
                .collect(),
        )
        .build()
        .expect("the fixture schema is valid");
    let footer = parquet_schema_footer_bytes(schema);
    let probe = store_holding(&[("a.parquet", &footer)]).await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("every converted type of the fixture folds to an Arrow type");

    assert_eq!(
        directory.binary_columns,
        vec![
            binary_column("doc_bson", None, "bson"),
            binary_column("s", Some("s.k"), "enum"),
        ],
        "a legacy writer's converted type classifies the leaf exactly as its logical type would"
    );
}

/// Scenario: A direct-storage unannotated BYTE_ARRAY column is refused
#[tokio::test]
async fn an_embedded_arrow_schema_binary_leaf_declares_binary() {
    let probe = store_holding(&[(
        "a.parquet",
        &parquet_bytes(
            vec![
                nullable("id", DataType::Int32),
                nullable("text", DataType::Utf8View),
                nullable("large", DataType::LargeBinary),
                nullable("view", DataType::BinaryView),
            ],
            1,
        ),
    )])
    .await;

    let directory = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("the Arrow-written file folds");

    assert_eq!(
        column_types(&directory.schema),
        vec![
            ("id".to_string(), DataType::Int32),
            ("text".to_string(), DataType::Utf8View),
            ("large".to_string(), DataType::LargeBinary),
            ("view".to_string(), DataType::BinaryView),
        ],
        "the embedded Arrow schema still folds each column to its own Arrow type"
    );
    assert_eq!(
        directory.binary_columns,
        vec![
            binary_column("large", None, "binary"),
            binary_column("view", None, "binary"),
        ],
        "the Parquet leaf under an Arrow LargeBinary or BinaryView is an unannotated BYTE_ARRAY"
    );
}

/// Scenario: A direct-storage unannotated BYTE_ARRAY column is refused
#[tokio::test]
async fn each_column_keeps_the_first_binary_leaf_any_read_footer_declares() {
    let first = parquet_footer_bytes(
        "message first {
            OPTIONAL INT32 id;
            OPTIONAL BYTE_ARRAY payload;
            OPTIONAL BYTE_ARRAY region;
        }",
    );
    let second = parquet_footer_bytes(
        "message second {
            OPTIONAL INT32 id;
            OPTIONAL BYTE_ARRAY payload (BSON);
            OPTIONAL BYTE_ARRAY extra (GEOMETRY);
        }",
    );
    let probe = store_holding(&[
        ("region=eu/a.parquet", &first),
        ("region=us/b.parquet", &second),
    ])
    .await;

    let folded = resolve(&probe, MergeMode::FoldEveryFile, &|_| true)
        .await
        .expect("both footers fold");
    let sampled = resolve(&probe, MergeMode::SampleOneFile, &|_| true)
        .await
        .expect("the first footer folds");

    assert_eq!(
        folded.binary_columns,
        vec![
            binary_column("payload", None, "binary"),
            binary_column("extra", None, "geometry"),
        ],
        "the first listed file names a column's type, a later file adds its own columns, and a \
         stored column dropped for a partition key is never refused"
    );
    assert_eq!(
        sampled.binary_columns,
        vec![binary_column("payload", None, "binary")],
        "sampling one footer classifies that footer alone"
    );
}

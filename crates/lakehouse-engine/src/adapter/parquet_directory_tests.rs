use super::*;
use crate::types::mapping::arrow_to_exasol_type;
use arrow::array::new_empty_array;
use arrow::datatypes::{Fields, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::{ObjectMeta, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

/// The prefix every fixture is written under, so a test also pins that objects OUTSIDE it are
/// never listed.
const TABLE_ROOT: &str = "warehouse/direct/events";

/// Hands back the inner store's listing REVERSED (so a test can't pass by luck against
/// [`InMemory`]'s already-sorted order) and records every read, HEAD vs. GET distinctly.
#[derive(Debug)]
struct ReversedListingStore {
    inner: Arc<dyn ObjectStore>,
    reads: Arc<std::sync::Mutex<Vec<String>>>,
}

impl ReversedListingStore {
    fn wrapping(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            reads: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    fn reads(&self) -> Vec<String> {
        self.reads.lock().expect("read log is not poisoned").clone()
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

/// A store holding the given objects, seen through the reversing, read-recording decorator.
async fn store_holding(objects: Vec<(&str, Vec<u8>)>) -> Arc<ReversedListingStore> {
    let inner = Arc::new(InMemory::new());
    for (key, bytes) in objects {
        inner
            .put(&StorePath::from(key), PutPayload::from(bytes))
            .await
            .expect("the in-memory store accepts the fixture object");
    }
    ReversedListingStore::wrapping(inner)
}

fn store_of(probe: &Arc<ReversedListingStore>) -> Arc<dyn ObjectStore> {
    Arc::clone(probe) as Arc<dyn ObjectStore>
}

/// A Parquet file declaring `fields` and holding `rows` rows of nulls (0 rows for a non-nullable
/// field, since a null value would fail the batch's own validation).
fn parquet_bytes(fields: Vec<Field>, rows: usize) -> Vec<u8> {
    let schema = Arc::new(Schema::new(fields));
    let columns: Vec<arrow::array::ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            if rows == 0 {
                new_empty_array(field.data_type())
            } else {
                arrow::array::new_null_array(field.data_type(), rows)
            }
        })
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

fn nullable(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
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

#[tokio::test]
async fn one_seam_returns_files_sizes_schema_and_footers() {
    let first = parquet_bytes(vec![nullable("id", DataType::Int32)], 3);
    let second = parquet_bytes(vec![nullable("id", DataType::Int64)], 5);
    let probe = store_holding(vec![
        (&format!("{TABLE_ROOT}/a.parquet"), first.clone()),
        (&format!("{TABLE_ROOT}/b.parquet"), second.clone()),
    ])
    .await;

    let directory = resolve_parquet_directory(&store_of(&probe), &root(), MergeMode::FoldEveryFile)
        .await
        .expect("a prefix of readable Parquet files resolves");

    assert_eq!(
        paths(&directory),
        vec![
            format!("{TABLE_ROOT}/a.parquet"),
            format!("{TABLE_ROOT}/b.parquet")
        ],
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
    let probe = store_holding(vec![
        (&format!("{TABLE_ROOT}/p1.parquet"), data.clone()),
        (&format!("{TABLE_ROOT}/a/p2.parquet"), data.clone()),
        (&format!("{TABLE_ROOT}/a/b/p3.parquet"), data.clone()),
        (&format!("{TABLE_ROOT}/_SUCCESS"), b"".to_vec()),
        (&format!("{TABLE_ROOT}/_metadata"), b"".to_vec()),
        (&format!("{TABLE_ROOT}/p1.parquet.crc"), b"".to_vec()),
        (&format!("{TABLE_ROOT}/_staging/p4.parquet"), data.clone()),
        (&format!("{TABLE_ROOT}/.hidden/p5.parquet"), data.clone()),
        ("warehouse/direct/other/p6.parquet", data.clone()),
    ])
    .await;

    let directory = resolve_parquet_directory(&store_of(&probe), &root(), MergeMode::FoldEveryFile)
        .await
        .expect("the prefix resolves");

    assert_eq!(
        paths(&directory),
        vec![
            format!("{TABLE_ROOT}/a/b/p3.parquet"),
            format!("{TABLE_ROOT}/a/p2.parquet"),
            format!("{TABLE_ROOT}/p1.parquet"),
        ],
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
async fn key_value_path_segments_are_split_into_a_per_file_map() {
    let data = parquet_bytes(vec![nullable("id", DataType::Int32)], 1);
    let probe = store_holding(vec![
        (
            &format!("{TABLE_ROOT}/year=2024/month=01/p1.parquet"),
            data.clone(),
        ),
        (&format!("{TABLE_ROOT}/plain/p2.parquet"), data.clone()),
    ])
    .await;

    let directory = resolve_parquet_directory(&store_of(&probe), &root(), MergeMode::FoldEveryFile)
        .await
        .expect("the prefix resolves");

    assert_eq!(
        directory.files[1].partition_segments,
        HashMap::from([
            ("year".to_string(), "2024".to_string()),
            ("month".to_string(), "01".to_string()),
        ]),
        "every 'key=value' segment below the prefix is split out for the file that carries it"
    );
    assert!(
        directory.files[0].partition_segments.is_empty(),
        "a plain directory segment contributes no entry"
    );
}

#[tokio::test]
async fn fold_widens_only_across_supported_pairs_and_names_conflicts() {
    let probe = store_holding(vec![
        (
            &format!("{TABLE_ROOT}/a.parquet"),
            parquet_bytes(
                vec![
                    nullable("n", DataType::Int32),
                    nullable("f", DataType::Float32),
                    nullable("d", DataType::Decimal128(10, 2)),
                ],
                1,
            ),
        ),
        (
            &format!("{TABLE_ROOT}/b.parquet"),
            parquet_bytes(
                vec![
                    nullable("n", DataType::Int64),
                    nullable("f", DataType::Float64),
                    nullable("d", DataType::Decimal128(12, 2)),
                ],
                1,
            ),
        ),
    ])
    .await;

    let directory = resolve_parquet_directory(&store_of(&probe), &root(), MergeMode::FoldEveryFile)
        .await
        .expect("every pair is a recorded relaxation");

    assert_eq!(
        column_types(&directory.schema),
        vec![
            ("n".to_string(), DataType::Int64),
            ("f".to_string(), DataType::Float64),
            ("d".to_string(), DataType::Decimal128(12, 2)),
        ],
        "each column resolves to the WIDER member of its pair"
    );
    assert_eq!(
        probe.files_read(),
        vec![
            format!("{TABLE_ROOT}/a.parquet"),
            format!("{TABLE_ROOT}/b.parquet")
        ],
        "every listed file's footer is read"
    );
}

#[tokio::test]
async fn a_column_folds_pairwise_in_listing_order_to_the_widest_reachable_type() {
    let probe = store_holding(vec![
        (
            &format!("{TABLE_ROOT}/a.parquet"),
            parquet_bytes(vec![nullable("n", DataType::Int8)], 1),
        ),
        (
            &format!("{TABLE_ROOT}/b.parquet"),
            parquet_bytes(vec![nullable("n", DataType::Int32)], 1),
        ),
        (
            &format!("{TABLE_ROOT}/c.parquet"),
            parquet_bytes(vec![nullable("n", DataType::Int64)], 1),
        ),
    ])
    .await;

    let directory = resolve_parquet_directory(&store_of(&probe), &root(), MergeMode::FoldEveryFile)
        .await
        .expect("Int8 widens to Int32 and Int32 to Int64");

    assert_eq!(
        column_types(&directory.schema),
        vec![("n".to_string(), DataType::Int64)],
        "a column appearing at three types resolves to the widest one successive supported \
         widenings reach"
    );
}

#[tokio::test]
async fn a_pair_no_widening_rule_covers_fails_naming_the_column_both_types_and_both_files() {
    let probe = store_holding(vec![
        (
            &format!("{TABLE_ROOT}/a.parquet"),
            parquet_bytes(vec![nullable("n", DataType::Int32)], 1),
        ),
        (
            &format!("{TABLE_ROOT}/b.parquet"),
            parquet_bytes(vec![nullable("n", DataType::Utf8)], 1),
        ),
    ])
    .await;

    let error = resolve_parquet_directory(&store_of(&probe), &root(), MergeMode::FoldEveryFile)
        .await
        .err()
        .expect("no supported pair covers Int32 and Utf8, so the fold must fail rather than guess")
        .to_string();

    for expected in [
        "n",
        "Int32",
        "Utf8",
        &format!("{TABLE_ROOT}/a.parquet"),
        &format!("{TABLE_ROOT}/b.parquet"),
    ] {
        assert!(
            error.contains(expected),
            "the refusal must name the column, BOTH types, and BOTH file paths so an operator \
             locates the files without listing the prefix; '{expected}' is missing from: {error}"
        );
    }
}

#[tokio::test]
async fn merge_mode_selects_every_footer_or_the_first() {
    let objects = [
        (
            format!("{TABLE_ROOT}/a.parquet"),
            parquet_bytes(vec![nullable("n", DataType::Int32)], 1),
        ),
        (
            format!("{TABLE_ROOT}/b.parquet"),
            parquet_bytes(vec![nullable("n", DataType::Int64)], 1),
        ),
        (
            format!("{TABLE_ROOT}/c.parquet"),
            parquet_bytes(vec![nullable("n", DataType::Int64)], 1),
        ),
    ];
    let borrowed = || {
        objects
            .iter()
            .map(|(key, bytes)| (key.as_str(), bytes.clone()))
            .collect::<Vec<(&str, Vec<u8>)>>()
    };

    let every = store_holding(borrowed()).await;
    let folded = resolve_parquet_directory(&store_of(&every), &root(), MergeMode::FoldEveryFile)
        .await
        .expect("the prefix resolves");

    let one = store_holding(borrowed()).await;
    let sampled = resolve_parquet_directory(&store_of(&one), &root(), MergeMode::SampleOneFile)
        .await
        .expect("the prefix resolves");

    assert_eq!(
        one.files_read(),
        vec![format!("{TABLE_ROOT}/a.parquet")],
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
}

#[tokio::test]
async fn a_single_file_prefix_folds_identically_under_both_modes() {
    let object = [(
        format!("{TABLE_ROOT}/a.parquet"),
        parquet_bytes(vec![nullable("n", DataType::Int32)], 1),
    )];
    let borrowed = || vec![(object[0].0.as_str(), object[0].1.clone())];

    let folded = resolve_parquet_directory(
        &store_of(&store_holding(borrowed()).await),
        &root(),
        MergeMode::FoldEveryFile,
    )
    .await
    .expect("the prefix resolves");
    let sampled = resolve_parquet_directory(
        &store_of(&store_holding(borrowed()).await),
        &root(),
        MergeMode::SampleOneFile,
    )
    .await
    .expect("the prefix resolves");

    assert_eq!(
        column_types(&folded.schema),
        vec![("n".to_string(), DataType::Int32)],
        "the single file's own declaration is the folded schema"
    );
    assert_eq!(
        column_types(&folded.schema),
        column_types(&sampled.schema),
        "the mode is observable only where footers differ"
    );
}

#[tokio::test]
async fn folded_columns_are_the_nullable_union_in_first_appearance_order() {
    let probe = store_holding(vec![
        (
            &format!("{TABLE_ROOT}/a.parquet"),
            parquet_bytes(
                vec![
                    Field::new("A", DataType::Int32, false),
                    nullable("B", DataType::Utf8),
                ],
                0,
            ),
        ),
        (
            &format!("{TABLE_ROOT}/b.parquet"),
            parquet_bytes(
                vec![
                    nullable("A", DataType::Int32),
                    nullable("C", DataType::Utf8),
                ],
                0,
            ),
        ),
    ])
    .await;

    let directory = resolve_parquet_directory(&store_of(&probe), &root(), MergeMode::FoldEveryFile)
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
    let probe = store_holding(vec![
        (
            &format!("{TABLE_ROOT}/a.parquet"),
            parquet_bytes(vec![nullable("id", DataType::Int32)], 0),
        ),
        (
            &format!("{TABLE_ROOT}/b.parquet"),
            parquet_bytes(vec![nullable("ID", DataType::Int32)], 0),
        ),
    ])
    .await;

    let error = resolve_parquet_directory(&store_of(&probe), &root(), MergeMode::FoldEveryFile)
        .await
        .err()
        .expect("the declaration would advertise a duplicate column name")
        .to_string();

    for expected in [
        "id",
        "ID",
        &format!("{TABLE_ROOT}/a.parquet"),
        &format!("{TABLE_ROOT}/b.parquet"),
    ] {
        assert!(
            error.contains(expected),
            "the refusal names both spellings and the files that carry them; '{expected}' is \
             missing from: {error}"
        );
    }
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
    let probe = store_holding(vec![(
        &format!("{TABLE_ROOT}/a.parquet"),
        parquet_bytes(
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

    let directory = resolve_parquet_directory(&store_of(&probe), &root(), MergeMode::FoldEveryFile)
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
    let probe = store_holding(vec![(&format!("{TABLE_ROOT}/_SUCCESS"), b"".to_vec())]).await;

    let directory = resolve_parquet_directory(&store_of(&probe), &root(), MergeMode::FoldEveryFile)
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

/// Percent-DECODED, since an object store addresses a key by its decoded name; an `abfss://`
/// URI's userinfo (the container) is the store's own scope, not part of the key.
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

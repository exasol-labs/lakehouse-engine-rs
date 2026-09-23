use super::*;
use crate::adapter::parquet_directory::MergeMode;
use crate::adapter::pushdown::test_support::sample_storage;
use crate::scan::spec::reconstruct_abs_uri;
use arrow::array::new_null_array;
use arrow::datatypes::{Field, Fields, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingTableUrl;
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use std::collections::BTreeMap;

/// The CONNECTION address plus the namespace property, joined by the resolver.
const TABLE_ROOT: &str = "s3://warehouse/direct/events";

/// Bytes no Parquet reader accepts, so resolving a directory that reads such a file's footer
/// fails: a successful resolution proves that footer was never read.
const NOT_PARQUET: &[u8] = b"not a parquet file";

/// One row of nulls is enough for the footer to declare `fields` without needing a value per Arrow type.
fn parquet_bytes(fields: Vec<Field>) -> Vec<u8> {
    let schema = Arc::new(ArrowSchema::new(fields));
    let columns: Vec<arrow::array::ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| new_null_array(field.data_type(), 1))
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

/// Keys are stored verbatim, so a key holding `%`, `#`, or `?` is exactly the object the listing
/// returns.
async fn store_holding(objects: Vec<(&str, Vec<u8>)>) -> Arc<dyn ObjectStore> {
    let store = InMemory::new();
    for (key, bytes) in objects {
        store
            .put(
                &StorePath::parse(key).expect("the fixture key is a valid store path"),
                PutPayload::from(bytes),
            )
            .await
            .expect("the in-memory store accepts the fixture object");
    }
    Arc::new(store)
}

fn hive(merge_mode: MergeMode) -> DirectoryOptions {
    DirectoryOptions {
        merge_mode,
        hive_partitioning: true,
    }
}

async fn try_resolve(
    store: &Arc<dyn ObjectStore>,
    options: DirectoryOptions,
    filter_json: Option<&Json>,
    declared_columns: &[(String, String)],
) -> Result<ResolvedScan, UdfError> {
    let storage = sample_storage();
    ParquetFormatReader {
        store,
        table_root: TABLE_ROOT,
        options,
        declared_columns,
        storage: &storage,
    }
    .resolve_scan(filter_json)
    .await
}

async fn resolve(
    store: &Arc<dyn ObjectStore>,
    merge_mode: MergeMode,
    filter_json: Option<&Json>,
) -> ResolvedScan {
    try_resolve(store, hive(merge_mode), filter_json, &[])
        .await
        .expect("a directory of readable Parquet files resolves a scan")
}

fn column_names(scan: &ResolvedScan) -> Vec<&str> {
    scan.logical_schema
        .iter()
        .map(|field| field.name.as_str())
        .collect()
}

fn file_paths(scan: &ResolvedScan) -> Vec<&str> {
    scan.files.iter().map(|file| file.path.as_str()).collect()
}

fn year_equals(value: &str) -> Json {
    serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "YEAR"},
        "right": {"type": "literal_string", "value": value},
    })
}

fn year_greater_than(value: &str) -> Json {
    serde_json::json!({
        "type": "predicate_greater",
        "left": {"type": "column", "name": "YEAR"},
        "right": {"type": "literal_string", "value": value},
    })
}

fn declared(columns: &[(&str, &str)]) -> Vec<(String, String)> {
    columns
        .iter()
        .map(|(name, exasol_type)| (name.to_string(), exasol_type.to_string()))
        .collect()
}

/// Two data files at different depths/schemas, plus two objects the listing must skip (a hidden segment, a sibling table's directory).
async fn two_depth_directory() -> Arc<dyn ObjectStore> {
    store_holding(vec![
        (
            "direct/events/part-0.parquet",
            parquet_bytes(vec![
                nullable("id", DataType::Int32),
                nullable("name", DataType::Utf8),
            ]),
        ),
        (
            "direct/events/day=2/part-1.parquet",
            parquet_bytes(vec![
                nullable("id", DataType::Int64),
                nullable("extra", DataType::Utf8),
            ]),
        ),
        (
            "direct/events/_staging/part-9.parquet",
            parquet_bytes(vec![nullable("hidden", DataType::Utf8)]),
        ),
        (
            "direct/other/part-0.parquet",
            parquet_bytes(vec![nullable("other", DataType::Utf8)]),
        ),
    ])
    .await
}

#[tokio::test]
async fn resolved_scan_carries_identity_bound_fields_and_no_deletes() {
    let store = two_depth_directory().await;

    let scan = resolve(&store, MergeMode::FoldEveryFile, None).await;

    assert_eq!(
        file_paths(&scan),
        vec!["day=2/part-1.parquet", "part-0.parquet"],
        "every data file under the root, encoded relative to it, and nothing else"
    );
    assert_eq!(
        scan.files
            .iter()
            .map(|file| file.partition_values.clone())
            .collect::<Vec<_>>(),
        vec![
            BTreeMap::from([("day".to_string(), Some("2".to_string()))]),
            BTreeMap::from([("day".to_string(), None)]),
        ],
        "each entry carries the seam's partition values, a segment-less file's key unset"
    );
    for file in &scan.files {
        assert!(
            file.deletes.is_empty(),
            "a raw Parquet directory declares no delete mechanism: {file:?}"
        );
        assert!(
            file.size > 0,
            "each entry carries the byte size the listing reported: {file:?}"
        );
    }
    for field in &scan.logical_schema {
        assert_eq!(
            field.field_id, None,
            "an identity-bound field carries no field-id, and no ordinal is synthesized: {field:?}"
        );
        assert_eq!(
            field.physical_name, None,
            "an identity-bound field declares no physical name: {field:?}"
        );
        assert!(
            field.nullable,
            "a column absent from one file is NULL for its rows, so every field is nullable: \
             {field:?}"
        );
        assert_eq!(
            field.initial_default, None,
            "a raw directory records no initial default: {field:?}"
        );
    }
    assert_eq!(
        column_names(&scan),
        vec!["id", "extra", "name", "day"],
        "the folded union in first-appearance order over the listing order, then the partition \
         columns"
    );
    assert_eq!(
        scan.partition_columns,
        vec!["day".to_string()],
        "the scan carries the seam's partition columns"
    );
    assert!(
        scan.name_mapping.is_empty(),
        "a raw directory declares no name mapping"
    );
    assert!(
        scan.refused_columns.is_empty(),
        "every folded Parquet type reaches a declared Exasol type, so no column is refused"
    );
    assert_eq!(
        scan.table_root, TABLE_ROOT,
        "the composed directory root travels once per fan-out"
    );
    assert_eq!(
        scan.effective_storage,
        sample_storage(),
        "this kind reaches no credential-vending catalog, so the static backend is the \
         effective one"
    );
}

#[tokio::test]
async fn file_entry_paths_round_trip_to_the_listed_object() {
    let keys = [
        "direct/events/region=a%2Fb/p%25.parquet",
        "direct/events/c#d/p#.parquet",
        "direct/events/e?f/p?.parquet",
        "direct/events/g h/p q.parquet",
        "direct/events/é/ü.parquet",
        "direct/events/plain/p.parquet",
    ];
    let data = parquet_bytes(vec![nullable("id", DataType::Int64)]);
    let store = store_holding(keys.iter().map(|key| (*key, data.clone())).collect()).await;

    let scan = resolve(&store, MergeMode::FoldEveryFile, None).await;

    let mut scanned: Vec<StorePath> = scan
        .files
        .iter()
        .map(|file| {
            let uri = reconstruct_abs_uri(&file.path, &scan.table_root);
            ListingTableUrl::parse(&uri)
                .unwrap_or_else(|e| panic!("the scan parses the entry URI '{uri}': {e}"))
                .prefix()
                .clone()
        })
        .collect();
    let mut listed: Vec<StorePath> = keys
        .iter()
        .map(|key| StorePath::parse(key).expect("the fixture key is a valid store path"))
        .collect();
    scanned.sort();
    listed.sort();
    assert_eq!(
        scanned, listed,
        "each entry must address at scan time exactly the object the listing returned"
    );
    assert!(
        file_paths(&scan).contains(&"plain/p.parquet"),
        "a path holding no character the URL parse mangles stays byte-identical: {:?}",
        file_paths(&scan)
    );
    assert!(
        file_paths(&scan).contains(&"region=a%252Fb/p%2525.parquet"),
        "a key's own '%' is encoded, never decoded to the '/' its partition value reads as: {:?}",
        file_paths(&scan)
    );
}

// merge_mode narrows which footers are read, never which files are scanned or filtered.
#[tokio::test]
async fn plan_reads_selected_footers_and_lists_every_file() {
    let store = two_depth_directory().await;
    let filter = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "ID"},
        "right": {"type": "literal_exactnumeric", "value": "1"},
    });

    let folded = resolve(&store, MergeMode::FoldEveryFile, Some(&filter)).await;
    let sampled = resolve(&store, MergeMode::SampleOneFile, Some(&filter)).await;

    assert_eq!(
        file_paths(&folded),
        file_paths(&sampled),
        "the merge mode selects footers, never files"
    );
    assert_eq!(
        file_paths(&folded),
        vec!["day=2/part-1.parquet", "part-0.parquet"],
        "a filter on no partition column narrows the rows the scan emits, never the files it \
         reads (#412)"
    );
    assert_eq!(
        column_names(&folded),
        vec!["id", "extra", "name", "day"],
        "folding every footer reaches the second listed file's own column"
    );
    assert_eq!(
        column_names(&sampled),
        vec!["id", "extra", "day"],
        "sampling one footer reads the FIRST listed file's declaration alone"
    );
    assert_eq!(
        folded.logical_schema[0].arrow_type, "int64",
        "folding widens the first file's int32 against the second file's int64"
    );
    assert_eq!(
        sampled.logical_schema[0].arrow_type, "int64",
        "the sampled file's own declared type is carried unwidened"
    );
}

#[tokio::test]
async fn a_partition_filter_prunes_files_before_their_footers_are_read() {
    let store = store_holding(vec![
        (
            "direct/events/year=2026/p1.parquet",
            parquet_bytes(vec![nullable("id", DataType::Int64)]),
        ),
        ("direct/events/year=2025/p2.parquet", NOT_PARQUET.to_vec()),
        ("direct/events/year=2024/p3.parquet", NOT_PARQUET.to_vec()),
    ])
    .await;
    let options = hive(MergeMode::FoldEveryFile);

    let unpruned = try_resolve(&store, options, None, &[]).await.expect_err(
        "reading a pruned file's footer fails, so a success below proves none was read",
    );
    assert!(
        unpruned
            .to_string()
            .contains("failed to read the Parquet footer"),
        "{unpruned}"
    );

    for filter in [year_equals("2026"), year_greater_than("2025")] {
        let scan = try_resolve(&store, options, Some(&filter), &[])
            .await
            .unwrap_or_else(|e| panic!("{filter}: a pruned file's footer must not be read: {e}"));
        assert_eq!(
            file_paths(&scan),
            vec!["year=2026/p1.parquet"],
            "{filter}: only the file whose partition value can satisfy the filter is kept"
        );
        assert_eq!(scan.partition_columns, vec!["year".to_string()]);
    }
}

#[tokio::test]
async fn a_predicate_keeping_no_file_reads_no_footer_and_errors_never() {
    let store = store_holding(vec![
        ("direct/events/year=2026/p1.parquet", NOT_PARQUET.to_vec()),
        ("direct/events/year=2025/p2.parquet", NOT_PARQUET.to_vec()),
    ])
    .await;
    let options = hive(MergeMode::FoldEveryFile);
    let columns = declared(&[("ID", "DECIMAL(20,0)"), ("YEAR", "VARCHAR(2000000) UTF8")]);

    try_resolve(&store, options, None, &columns)
        .await
        .expect_err("reading either file's footer fails, so a success below proves none was read");

    let scan = try_resolve(&store, options, Some(&year_equals("2099")), &columns)
        .await
        .expect("a predicate keeping no file resolves without reading any footer");

    assert!(scan.files.is_empty(), "no file can satisfy YEAR = '2099'");
    assert_eq!(
        column_names(&scan),
        vec!["year", "ID"],
        "the partition column comes from the unfiltered listing, and every other declared column \
         from the request, so the schema survives pruning every file"
    );
}

#[tokio::test]
async fn a_declared_column_absent_from_kept_files_is_added_as_a_null_field() {
    let store = store_holding(vec![
        (
            "direct/events/year=2026/p1.parquet",
            parquet_bytes(vec![
                nullable("id", DataType::Int64),
                nullable("discount", DataType::Float64),
            ]),
        ),
        (
            "direct/events/year=2025/p2.parquet",
            parquet_bytes(vec![nullable("id", DataType::Int64)]),
        ),
    ])
    .await;
    let columns = declared(&[
        ("ID", "DECIMAL(20,0)"),
        ("DISCOUNT", "DOUBLE PRECISION"),
        ("NOTE", "VARCHAR(2000000) UTF8"),
        ("PLACE", "GEOMETRY"),
        ("YEAR", "VARCHAR(2000000) UTF8"),
    ]);
    let options = hive(MergeMode::FoldEveryFile);

    let pruned = try_resolve(&store, options, Some(&year_equals("2025")), &columns)
        .await
        .expect("the kept file resolves a scan");
    let unpruned = try_resolve(&store, options, None, &columns)
        .await
        .expect("both files resolve a scan");

    assert_eq!(
        column_names(&pruned),
        vec!["id", "year", "DISCOUNT", "NOTE", "PLACE"],
        "a declared column no kept file carries is appended once; one the fold or the partition \
         columns already carry, under any case, is not repeated"
    );
    assert_eq!(
        column_names(&unpruned),
        vec!["id", "discount", "year", "NOTE", "PLACE"],
        "a column a kept file carries keeps its footer-derived field"
    );
    let added = &pruned.logical_schema[2..];
    assert_eq!(
        added
            .iter()
            .map(|field| field.arrow_type.as_str())
            .collect::<Vec<_>>(),
        vec!["float64", "utf8", "utf8"],
        "typed from the declared Exasol type, a string-family or unmapped type as utf8"
    );
    for field in added {
        assert!(
            field.nullable,
            "every scanned file reads it as NULL: {field:?}"
        );
        assert_eq!(field.field_id, None, "no binding key: {field:?}");
        assert_eq!(field.physical_name, None, "no binding key: {field:?}");
        assert_eq!(field.nested, None, "no nested descriptor: {field:?}");
        assert_eq!(field.initial_default, None, "{field:?}");
    }
}

#[tokio::test]
async fn a_nested_column_declares_the_string_tag_and_an_identity_bound_descriptor() {
    let inner = Fields::from(vec![
        nullable("a", DataType::Int32),
        nullable("b", DataType::Utf8),
    ]);
    let store = store_holding(vec![(
        "direct/events/part-0.parquet",
        parquet_bytes(vec![
            nullable("point", DataType::Struct(inner)),
            nullable(
                "tags",
                DataType::List(Arc::new(nullable("item", DataType::Utf8))),
            ),
        ]),
    )])
    .await;

    let scan = resolve(&store, MergeMode::FoldEveryFile, None).await;

    let point = &scan.logical_schema[0];
    assert_eq!(
        point.arrow_type, "utf8",
        "a nested column is declared as the JSON string"
    );
    assert_eq!(
        point.nested,
        Some(NestedMembers::Struct {
            fields: vec![
                NestedField {
                    field_id: None,
                    name: "a".into(),
                    physical_name: None,
                    nested: None,
                },
                NestedField {
                    field_id: None,
                    name: "b".into(),
                    physical_name: None,
                    nested: None,
                },
            ],
        }),
        "each member names itself and carries NO binding key"
    );
    let tags = &scan.logical_schema[1];
    assert_eq!(tags.arrow_type, "utf8");
    assert_eq!(
        tags.nested,
        Some(NestedMembers::List { element: None }),
        "a list's element is positional, so a primitive element names no member"
    );
}

// An empty prefix resolves an empty scan, not an error — whether it's a table was decided at create time.
#[tokio::test]
async fn a_directory_holding_no_data_file_resolves_an_empty_scan() {
    let store = store_holding(vec![(
        "direct/events/_delta_log/00000000000000000000.json",
        b"{}".to_vec(),
    )])
    .await;

    let scan = resolve(&store, MergeMode::FoldEveryFile, None).await;

    assert!(scan.files.is_empty());
    assert!(scan.logical_schema.is_empty());
    assert_eq!(scan.table_root, TABLE_ROOT);
}

// Planning must render the same normalized timestamptz_* tag as enumeration, or CREATE VIRTUAL SCHEMA could accept a column that planning then refuses.
#[tokio::test]
async fn a_non_utc_timezone_column_plans_at_its_normalized_tag() {
    let store = store_holding(vec![(
        "direct/events/part-0.parquet",
        parquet_bytes(vec![nullable(
            "occurred_at",
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("America/New_York".into()),
            ),
        )]),
    )])
    .await;

    let scan = resolve(&store, MergeMode::FoldEveryFile, None).await;

    assert_eq!(
        scan.logical_schema[0].arrow_type, "timestamptz_us",
        "the plan path renders the tz-aware tag, the same one enumeration declares"
    );
}

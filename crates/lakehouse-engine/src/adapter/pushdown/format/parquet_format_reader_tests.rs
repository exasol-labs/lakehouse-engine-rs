use super::*;
use crate::adapter::pushdown::test_support::{sample_storage, unauthenticated_creds};
use arrow::array::new_null_array;
use arrow::datatypes::{Field, Fields, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

/// The CONNECTION address plus the namespace property, joined by the resolver: the
/// bucket is the store's own scope, so everything below it is a store-relative key.
const TABLE_ROOT: &str = "s3://warehouse/direct/events";

/// A Parquet file declaring `fields` and holding one row of nulls, so the footer
/// carries the declaration without needing a value per Arrow type.
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

async fn store_holding(objects: Vec<(&str, Vec<u8>)>) -> Arc<dyn ObjectStore> {
    let store = InMemory::new();
    for (key, bytes) in objects {
        store
            .put(&StorePath::from(key), PutPayload::from(bytes))
            .await
            .expect("the in-memory store accepts the fixture object");
    }
    Arc::new(store)
}

/// Resolve `TABLE_ROOT` through the reader the third scan source selects.
async fn resolve(
    store: &Arc<dyn ObjectStore>,
    merge_mode: MergeMode,
    filter_json: Option<&Json>,
) -> ResolvedScan {
    let storage = sample_storage();
    let creds = unauthenticated_creds();
    let connection = ConnectionStorage {
        storage: &storage,
        creds: &creds,
        allow_http: true,
    };
    ParquetFormatReader::new(store, TABLE_ROOT, merge_mode, &connection)
        .resolve_scan(filter_json)
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

/// Two data files at two depths, declaring different column sets, plus two objects
/// the listing must never reach: one under a hidden segment, one in a sibling table's
/// directory.
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

/// Scenario: A direct-storage table resolves its files and schema through the shared
/// seam.
///
/// Every field of the resolved scan is pinned at once, because the scenario states
/// them as one shape: the file entries carry a path and a LISTED size with no delete
/// mechanism and no partition value, every logical field binds by IDENTITY, the
/// partition columns and the name mapping are empty, the storage is the CONNECTION's
/// own, the table root is the composed directory root, and no column is refused.
#[tokio::test]
async fn resolved_scan_carries_identity_bound_fields_and_no_deletes() {
    let store = two_depth_directory().await;

    let scan = resolve(&store, MergeMode::FoldEveryFile, None).await;

    assert_eq!(
        file_paths(&scan),
        vec!["day=2/part-1.parquet", "part-0.parquet"],
        "every data file under the root, encoded relative to it, and nothing else"
    );
    for file in &scan.files {
        assert!(
            file.deletes.is_empty(),
            "a raw Parquet directory declares no delete mechanism: {file:?}"
        );
        assert!(
            file.partition_values.is_empty(),
            "a raw Parquet directory declares no partition value: {file:?}"
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
        vec!["id", "extra", "name"],
        "the folded union in first-appearance order over the listing order"
    );
    assert!(
        scan.partition_columns.is_empty(),
        "a raw directory declares no partition column"
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

/// Scenario: Every plan-time footer is read and the resulting cost is stated.
///
/// The merge mode narrows which FOOTERS are read, never which FILES are scanned, and
/// a filter narrows neither: this kind has no manifest and no catalog statistics to
/// prune from. Sampling one footer is what makes the read set observable — the second
/// file's own column is absent from the declaration exactly when its footer went
/// unread.
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
        "a filter narrows the rows the scan emits, never the files it reads (#408, #412)"
    );
    assert_eq!(
        column_names(&folded),
        vec!["id", "extra", "name"],
        "folding every footer reaches the second listed file's own column"
    );
    assert_eq!(
        column_names(&sampled),
        vec!["id", "extra"],
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

/// Scenario: A nested or unrepresentable Parquet type folds to the JSON string
/// declaration.
///
/// The planning half: a struct, list, or map column is declared with the string
/// Arrow tag AND carries the nested member descriptor the JSON renderer is selected
/// by. Without the descriptor such a column would reach the cast path, where no
/// string kernel exists. Every member binds by identity, matching the top level.
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

/// Scenario: A direct-storage table resolves its files and schema through the shared
/// seam.
///
/// A prefix holding no data file resolves an empty scan rather than an error: whether
/// that directory is a table at all was decided at create time, not here.
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

/// Scenario: A timezone-aware column plans at the same normalized tag the enumeration path
/// declares it at.
///
/// The tag vocabulary renders every tz-aware timestamp as `timestamptz_*`, discarding which
/// timezone the file declared. Both paths must therefore answer `timestamptz_us` for the same
/// footer: an enumeration that refused this column while planning accepted it would fail
/// `CREATE VIRTUAL SCHEMA` over a directory every query could otherwise read.
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

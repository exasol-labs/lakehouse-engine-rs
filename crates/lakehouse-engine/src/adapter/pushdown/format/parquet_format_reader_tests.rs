use super::super::{RefusedColumn, ResolvedScan};
use super::*;
use crate::adapter::column_notes::TableNotes;
use crate::adapter::parquet_directory::MergeMode;
use crate::adapter::pushdown::test_support::filter_json::{column, compare, equal, number};
use crate::adapter::pushdown::test_support::{binary_refused, resolve_from_notes, sample_storage};
use crate::adapter::tests::parquet_fixture::{
    directory_options, enumerated_notes, in_memory_store, nullable, parquet_bytes,
    parquet_footer_bytes,
};
use crate::scan::spec::{NestedField, NestedMembers, reconstruct_abs_uri};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::datasource::listing::ListingTableUrl;
use std::collections::BTreeMap;

const BASE_PATH: &str = "s3://warehouse/direct";
const TABLE_ROOT: &str = "s3://warehouse/direct/events";

/// Unreadable as Parquet: a successful resolution proves its footer was never read.
type Notes = Vec<(String, Option<String>)>;

/// One row of nulls declares `fields` without needing a value per Arrow type.
fn parquet(fields: Vec<Field>) -> Vec<u8> {
    parquet_bytes(fields, 1)
}

async fn store_holding(objects: &[(&str, &[u8])]) -> Arc<dyn ObjectStore> {
    in_memory_store(objects).await
}

/// The notes CREATE records for the `events` table under `merge_mode`.
async fn enumerate(store: &Arc<dyn ObjectStore>, merge_mode: MergeMode) -> Notes {
    enumerated_notes(
        store,
        BASE_PATH,
        "events",
        directory_options(merge_mode, true),
    )
    .await
}

async fn try_resolve(
    store: &Arc<dyn ObjectStore>,
    notes: &Notes,
    filter_json: Option<&Json>,
) -> Result<ResolvedScan, UdfError> {
    let storage = sample_storage();
    let table_notes = TableNotes {
        table: "EVENTS",
        columns: notes
            .iter()
            .map(|(name, note)| (name.as_str(), note.as_deref()))
            .collect(),
    };
    let reader = ParquetFormatReader {
        store,
        table_root: TABLE_ROOT,
        storage: &storage,
    };
    resolve_from_notes(&reader, &table_notes, filter_json).await
}

/// Enumerates, then plans against the notes that enumeration recorded.
async fn resolve(
    store: &Arc<dyn ObjectStore>,
    merge_mode: MergeMode,
    filter_json: Option<&Json>,
) -> ResolvedScan {
    let notes = enumerate(store, merge_mode).await;
    try_resolve(store, &notes, filter_json)
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

/// Plus two objects the listing must skip: a hidden segment and a sibling table's directory.
async fn two_depth_directory() -> Arc<dyn ObjectStore> {
    store_holding(&[
        (
            "direct/events/part-0.parquet",
            &parquet(vec![
                nullable("id", DataType::Int32),
                nullable("name", DataType::Utf8),
            ]),
        ),
        (
            "direct/events/day=2/part-1.parquet",
            &parquet(vec![
                nullable("id", DataType::Int64),
                nullable("extra", DataType::Utf8),
            ]),
        ),
        (
            "direct/events/_staging/part-9.parquet",
            &parquet(vec![nullable("hidden", DataType::Utf8)]),
        ),
        (
            "direct/other/part-0.parquet",
            &parquet(vec![nullable("other", DataType::Utf8)]),
        ),
    ])
    .await
}

/// Scenario: A direct-storage table resolves its files and schema through the shared seam
#[tokio::test]
async fn direct_storage_scan_takes_its_schema_from_notes_and_files_from_the_listing() {
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
        "a file without the partition segment carries the key unset"
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
        "folded union in listing order, then the partition columns"
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
    let data = parquet(vec![nullable("id", DataType::Int64)]);
    let objects: Vec<(&str, &[u8])> = keys.iter().map(|key| (*key, data.as_slice())).collect();
    let store = store_holding(&objects).await;

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
        "each entry must resolve to the listed object"
    );
    assert!(
        file_paths(&scan).contains(&"plain/p.parquet"),
        "a plain path stays byte-identical: {:?}",
        file_paths(&scan)
    );
    assert!(
        file_paths(&scan).contains(&"region=a%252Fb/p%2525.parquet"),
        "a key's '%' is encoded, not decoded: {:?}",
        file_paths(&scan)
    );
}

#[tokio::test]
async fn merge_mode_decides_the_declared_columns_at_enumeration() {
    let store = two_depth_directory().await;

    let folded = resolve(&store, MergeMode::FoldEveryFile, None).await;
    let sampled = resolve(&store, MergeMode::SampleOneFile, None).await;

    assert_eq!(
        file_paths(&folded),
        file_paths(&sampled),
        "the merge mode selects footers, never files"
    );
    assert_eq!(
        column_names(&folded),
        vec!["id", "extra", "name", "day"],
        "folding every footer reaches the second listed file's own column"
    );
    assert_eq!(
        column_names(&sampled),
        vec!["id", "extra", "day"],
        "sampling one footer declares the FIRST listed file's columns alone"
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

/// Scenario: Direct storage passes every partition column to the one predicate as a string
#[tokio::test]
async fn a_numeric_literal_prunes_no_direct_storage_file() {
    let data = parquet(vec![nullable("id", DataType::Int64)]);
    let store = store_holding(&[
        ("direct/events/year=2024/p1.parquet", &data),
        ("direct/events/year=2025/p2.parquet", &data),
    ])
    .await;

    let numeric = compare("predicate_equal", column("YEAR"), number("2024"));
    let unpruned = resolve(&store, MergeMode::FoldEveryFile, Some(&numeric)).await;
    let pruned = resolve(
        &store,
        MergeMode::FoldEveryFile,
        Some(&equal("YEAR", "2024")),
    )
    .await;

    assert_eq!(
        file_paths(&unpruned),
        vec!["year=2024/p1.parquet", "year=2025/p2.parquet"],
        "a numeric literal against a string partition column prunes no file"
    );
    assert_eq!(
        unpruned
            .logical_schema
            .iter()
            .find(|field| field.name == "year")
            .map(|field| field.arrow_type.as_str()),
        Some("utf8"),
        "direct storage declares its partition column a string"
    );
    assert_eq!(
        file_paths(&pruned),
        vec!["year=2024/p1.parquet"],
        "a string literal still prunes through the same predicate"
    );
}

/// Scenario: A declared column absent from every kept file reads NULL
#[tokio::test]
async fn a_column_only_pruned_files_carry_reads_null() {
    let store = store_holding(&[
        (
            "direct/events/year=2026/p1.parquet",
            &parquet(vec![
                nullable("id", DataType::Int64),
                nullable("discount", DataType::Float64),
            ]),
        ),
        (
            "direct/events/year=2025/p2.parquet",
            &parquet(vec![nullable("id", DataType::Int64)]),
        ),
    ])
    .await;
    let notes = enumerate(&store, MergeMode::FoldEveryFile).await;

    let pruned = try_resolve(&store, &notes, Some(&equal("YEAR", "2025")))
        .await
        .expect("the kept file resolves a scan");

    assert_eq!(file_paths(&pruned), vec!["year=2025/p2.parquet"]);
    assert_eq!(
        column_names(&pruned),
        vec!["id", "discount", "year"],
        "the declaration comes from the notes, never from whichever files pruning keeps"
    );
    let discount = &pruned.logical_schema[1];
    assert_eq!(discount.arrow_type, "float64");
    assert!(discount.nullable, "{discount:?}");
    assert_eq!(discount.field_id, None, "no binding key: {discount:?}");
    assert_eq!(discount.physical_name, None, "no binding key: {discount:?}");
}

#[tokio::test]
async fn a_nested_column_declares_the_string_tag_and_an_identity_bound_descriptor() {
    let inner = Fields::from(vec![
        nullable("a", DataType::Int32),
        nullable("b", DataType::Utf8),
    ]);
    let store = store_holding(&[(
        "direct/events/part-0.parquet",
        &parquet(vec![
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

#[tokio::test]
async fn a_non_utc_timezone_column_plans_at_its_normalized_tag() {
    let store = store_holding(&[(
        "direct/events/part-0.parquet",
        &parquet(vec![nullable(
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
        "the plan carries the tz-aware tag that enumeration recorded in the column's note"
    );
}

async fn resolve_footer(message: &str) -> Result<ResolvedScan, UdfError> {
    let store = store_holding(&[(
        "direct/events/part-0.parquet",
        &parquet_footer_bytes(message),
    )])
    .await;
    let notes = enumerate(&store, MergeMode::FoldEveryFile).await;
    try_resolve(&store, &notes, None).await
}

/// Scenario: A direct-storage unannotated BYTE_ARRAY column is refused
/// Scenario: A direct-storage ENUM column reads as text
/// Scenario: A direct-storage UUID or unannotated fixed-length column is refused
#[tokio::test]
async fn each_binary_column_is_refused_by_name_and_an_enum_column_reads_as_text() {
    let scan = resolve_footer(
        "message events {
            OPTIONAL INT32 id;
            OPTIONAL BYTE_ARRAY legacy_name;
            OPTIONAL BYTE_ARRAY name (STRING);
            OPTIONAL BYTE_ARRAY kind (ENUM);
            OPTIONAL GROUP s {
                OPTIONAL BYTE_ARRAY raw;
            }
            OPTIONAL GROUP e {
                OPTIONAL BYTE_ARRAY k (ENUM);
            }
            OPTIONAL BYTE_ARRAY doc (BSON);
            OPTIONAL FIXED_LEN_BYTE_ARRAY (16) uid (UUID);
            OPTIONAL FIXED_LEN_BYTE_ARRAY (16) digest;
        }",
    )
    .await
    .expect("a directory with a mappable column plans, refusing only its binary columns");

    assert_eq!(
        column_names(&scan),
        vec!["id", "name", "kind"],
        "a refused column stays declared in its note but is dropped from the logical schema"
    );
    assert_eq!(
        scan.logical_schema[2].arrow_type, "utf8",
        "a top-level ENUM is read as its UTF-8 text"
    );
    let refused =
        |column, member_path, declared| binary_refused("Parquet", column, member_path, declared);
    let nested_enum = refused("e", Some("e.k"), "enum");
    assert_eq!(
        scan.refused_columns,
        vec![
            refused("legacy_name", None, "binary"),
            refused("s", Some("s.raw"), "binary"),
            RefusedColumn {
                reason: format!(
                    "{}; a Parquet ENUM is read as text only as a top-level, non-repeated column",
                    nested_enum.reason
                ),
                ..nested_enum
            },
            refused("doc", None, "bson"),
            refused("uid", None, "uuid"),
            refused("digest", None, "fixed(16)"),
        ]
    );

    let error = resolve_footer(
        "message events {
            OPTIONAL BYTE_ARRAY payload;
            OPTIONAL FIXED_LEN_BYTE_ARRAY (16) uid (UUID);
        }",
    )
    .await
    .expect_err("a directory with no mappable column cannot be scanned");
    assert!(
        error
            .to_string()
            .contains("virtual table 'EVENTS' has no mappable column"),
        "{error}"
    );
}

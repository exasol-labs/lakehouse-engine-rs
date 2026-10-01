use super::super::binary_cause;
use super::*;
use crate::adapter::parquet_directory::{
    FilePattern, MergeMode, list_location_files, raw_location_prefix,
};
use crate::adapter::pushdown::test_support::filter_json::{column, compare, equal, number, string};
use crate::adapter::pushdown::test_support::sample_storage;
use crate::adapter::tests::parquet_fixture::{
    directory_options, in_memory_store, nullable, parquet_bytes, parquet_footer_bytes,
};
use crate::scan::spec::reconstruct_abs_uri;
use arrow::datatypes::Fields;
use datafusion::datasource::listing::ListingTableUrl;
use std::collections::BTreeMap;

const TABLE_ROOT: &str = "s3://warehouse/direct/events";

/// Unreadable as Parquet: a successful resolution proves its footer was never read.
const NOT_PARQUET: &[u8] = b"not a parquet file";

/// One row of nulls declares `fields` without needing a value per Arrow type.
fn parquet(fields: Vec<Field>) -> Vec<u8> {
    parquet_bytes(fields, 1)
}

async fn store_holding(objects: &[(&str, &[u8])]) -> Arc<dyn ObjectStore> {
    in_memory_store(objects).await
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
    try_resolve(store, directory_options(merge_mode, true), filter_json, &[])
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

fn declared(columns: &[(&str, &str)]) -> Vec<(String, String)> {
    columns
        .iter()
        .map(|(name, exasol_type)| (name.to_string(), exasol_type.to_string()))
        .collect()
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

/// Scenario: A catalog-registered location is listed by its raw object key
#[tokio::test]
async fn a_raw_key_file_path_resolves_back_to_its_object_key() {
    let table_root = "s3://bucket/tbl";
    let inside = "tbl/p_str=a b%2Fc/f1";
    let outside = "elsewhere/p_str=a b%2Fc/f1";
    let store = in_memory_store(&[(inside, NOT_PARQUET), (outside, NOT_PARQUET)]).await
        as Arc<dyn ObjectStore>;
    let (store_root, table_prefix) =
        raw_location_prefix(table_root).expect("the table location names a raw key");

    for key in [inside, outside] {
        let (partition_location, _) = key.rsplit_once('/').expect("the key has a directory");
        let (_, partition_prefix) =
            raw_location_prefix(&format!("s3://bucket/{partition_location}"))
                .expect("the partition location names a raw key");
        let [listed] = list_location_files(&store, &partition_prefix, FilePattern::AnyDirectChild)
            .await
            .expect("the partition location lists")
            .try_into()
            .unwrap_or_else(|files: Vec<_>| panic!("one file under '{key}', got {}", files.len()));

        let entry = file_entry(
            ParquetFile {
                path: listed.path,
                size: listed.size,
                partition_values: BTreeMap::new(),
                footer: None,
            },
            &table_prefix,
            &store_root,
        );
        let uri = reconstruct_abs_uri(&entry.path, table_root);
        let scanned = ListingTableUrl::parse(&uri)
            .unwrap_or_else(|e| panic!("the scan parses the entry URI '{uri}': {e}"))
            .prefix()
            .clone();

        assert_eq!(
            scanned,
            StorePath::parse(key).expect("the fixture key is a valid store path"),
            "'{}' must resolve to the listed object '{key}'",
            entry.path
        );
    }
}

#[tokio::test]
async fn plan_reads_selected_footers_and_lists_every_file() {
    let store = two_depth_directory().await;
    let filter = compare("predicate_equal", column("ID"), number("1"));

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
        "a non-partition filter never prunes files (#412)"
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
    let store = store_holding(&[
        (
            "direct/events/year=2026/p1.parquet",
            &parquet(vec![nullable("id", DataType::Int64)]),
        ),
        ("direct/events/year=2025/p2.parquet", NOT_PARQUET),
        ("direct/events/year=2024/p3.parquet", NOT_PARQUET),
    ])
    .await;
    let options = directory_options(MergeMode::FoldEveryFile, true);
    let columns = declared(&[("ID", "DECIMAL(20,0)"), ("YEAR", "VARCHAR(2000000) UTF8")]);

    let unpruned = try_resolve(&store, options, None, &columns)
        .await
        .expect_err("an unpruned scan reads the unreadable footers");
    assert!(
        unpruned
            .to_string()
            .contains("failed to read the Parquet footer"),
        "{unpruned}"
    );

    for (filter, kept, columns_after) in [
        (
            equal("YEAR", "2026"),
            vec!["year=2026/p1.parquet"],
            vec!["id", "year"],
        ),
        (
            compare("predicate_greater", column("YEAR"), string("2025")),
            vec!["year=2026/p1.parquet"],
            vec!["id", "year"],
        ),
        // Keeping no file still resolves, and the schema survives pruning every file.
        (equal("YEAR", "2099"), vec![], vec!["year", "ID"]),
    ] {
        let scan = try_resolve(&store, options, Some(&filter), &columns)
            .await
            .unwrap_or_else(|e| panic!("{filter}: a pruned file's footer must not be read: {e}"));
        assert_eq!(
            file_paths(&scan),
            kept,
            "{filter}: only the file whose partition value can satisfy the filter is kept"
        );
        assert_eq!(scan.partition_columns, vec!["year".to_string()]);
        assert_eq!(column_names(&scan), columns_after, "{filter}");
    }
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

#[tokio::test]
async fn a_declared_column_absent_from_kept_files_is_added_as_a_null_field() {
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
    let columns = declared(&[
        ("ID", "DECIMAL(20,0)"),
        ("DISCOUNT", "DOUBLE PRECISION"),
        ("NOTE", "VARCHAR(2000000) UTF8"),
        ("PLACE", "GEOMETRY"),
        ("YEAR", "VARCHAR(2000000) UTF8"),
    ]);
    let options = directory_options(MergeMode::FoldEveryFile, true);

    let pruned = try_resolve(&store, options, Some(&equal("YEAR", "2025")), &columns)
        .await
        .expect("the kept file resolves a scan");
    let unpruned = try_resolve(&store, options, None, &columns)
        .await
        .expect("both files resolve a scan");

    assert_eq!(
        column_names(&pruned),
        vec!["id", "year", "DISCOUNT", "NOTE", "PLACE"],
        "only declared columns absent from the fold and partition columns are appended"
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
        "typed from the declared Exasol type, unmapped types as utf8"
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
async fn a_directory_holding_no_data_file_resolves_an_empty_scan() {
    let store =
        store_holding(&[("direct/events/_delta_log/00000000000000000000.json", b"{}")]).await;

    let scan = resolve(&store, MergeMode::FoldEveryFile, None).await;

    assert!(scan.files.is_empty());
    assert!(scan.logical_schema.is_empty());
    assert_eq!(scan.table_root, TABLE_ROOT);
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
        "the plan path renders the tz-aware tag, the same one enumeration declares"
    );
}

async fn resolve_footer(
    message: &str,
    declared_columns: &[(String, String)],
) -> Result<ResolvedScan, UdfError> {
    let store = store_holding(&[(
        "direct/events/part-0.parquet",
        &parquet_footer_bytes(message),
    )])
    .await;
    try_resolve(
        &store,
        directory_options(MergeMode::FoldEveryFile, true),
        None,
        declared_columns,
    )
    .await
}

fn refused(column: &str, member_path: Option<&str>, declared: &str) -> RefusedColumn {
    let subject = match member_path {
        Some(member_path) => format!("Parquet column '{column}', whose member '{member_path}'"),
        None => format!("Parquet column '{column}'"),
    };
    RefusedColumn {
        column_name: column.to_string(),
        reason: format!("{subject} {}", binary_cause(declared)),
    }
}

/// Scenario: A direct-storage unannotated BYTE_ARRAY column is refused
#[tokio::test]
async fn unannotated_bson_and_geospatial_byte_arrays_are_refused_naming_their_type() {
    let varchar = "VARCHAR(2000000) UTF8";
    let listed = declared(&[
        ("ID", "DECIMAL(10,0)"),
        ("LEGACY_NAME", varchar),
        ("NAME", varchar),
        ("S", varchar),
        ("DOC", varchar),
        ("SHAPE", varchar),
        ("AREA", varchar),
    ]);

    let scan = resolve_footer(
        "message events {
            OPTIONAL INT32 id;
            OPTIONAL BYTE_ARRAY legacy_name;
            OPTIONAL BYTE_ARRAY name (STRING);
            OPTIONAL GROUP s {
                OPTIONAL BYTE_ARRAY raw;
            }
            OPTIONAL BYTE_ARRAY doc (BSON);
            OPTIONAL BYTE_ARRAY shape (GEOMETRY);
            OPTIONAL BYTE_ARRAY area (GEOGRAPHY);
        }",
        &listed,
    )
    .await
    .expect("a directory with a mappable column plans, refusing only its binary columns");

    assert_eq!(
        column_names(&scan),
        vec!["id", "name"],
        "a refused column is dropped, and its listed declaration never re-adds it as a NULL column"
    );
    assert_eq!(
        scan.refused_columns,
        vec![
            refused("legacy_name", None, "binary"),
            refused("s", Some("s.raw"), "binary"),
            refused("doc", None, "bson"),
            refused("shape", None, "geometry"),
            refused("area", None, "geography"),
        ]
    );
}

/// Scenario: A direct-storage ENUM column reads as text
#[tokio::test]
async fn an_enum_column_reads_as_text_and_a_nested_enum_member_is_refused() {
    let scan = resolve_footer(
        "message events {
            OPTIONAL INT32 id;
            OPTIONAL BYTE_ARRAY kind (ENUM);
            OPTIONAL GROUP s {
                OPTIONAL BYTE_ARRAY k (ENUM);
            }
        }",
        &[],
    )
    .await
    .expect("the ENUM column is mappable");

    assert_eq!(column_names(&scan), vec!["id", "kind"]);
    assert_eq!(
        scan.logical_schema[1].arrow_type, "utf8",
        "a top-level ENUM is read as its UTF-8 text"
    );
    let nested = refused("s", Some("s.k"), "enum");
    assert_eq!(
        scan.refused_columns,
        vec![RefusedColumn {
            reason: format!(
                "{}; a Parquet ENUM is read as text only as a top-level, non-repeated column",
                nested.reason
            ),
            ..nested
        }],
        "the nested ENUM member is refused, stating where an ENUM does read as text"
    );
}

/// Scenario: A direct-storage UUID or unannotated fixed-length column is refused
#[tokio::test]
async fn uuid_and_unannotated_fixed_len_columns_are_refused_naming_their_type() {
    let scan = resolve_footer(
        "message events {
            OPTIONAL INT32 id;
            OPTIONAL FIXED_LEN_BYTE_ARRAY (16) uid (UUID);
            OPTIONAL FIXED_LEN_BYTE_ARRAY (16) digest;
        }",
        &[],
    )
    .await
    .expect("the id column is mappable");

    assert_eq!(column_names(&scan), vec!["id"]);
    assert_eq!(
        scan.refused_columns,
        vec![
            refused("uid", None, "uuid"),
            refused("digest", None, "fixed(16)"),
        ]
    );
}

/// Scenario: A direct-storage unannotated BYTE_ARRAY column is refused
#[tokio::test]
async fn a_directory_whose_every_column_is_binary_is_refused_as_a_whole() {
    let error = resolve_footer(
        "message events {
            OPTIONAL BYTE_ARRAY payload;
            OPTIONAL FIXED_LEN_BYTE_ARRAY (16) uid (UUID);
        }",
        &[],
    )
    .await
    .expect_err("a directory with no mappable column cannot be scanned");

    let message = error.to_string();
    for fragment in [
        "Direct storage table has no mappable column",
        "'payload'",
        "'uid'",
        "#351",
    ] {
        assert!(
            message.contains(fragment),
            "'{fragment}' missing from: {message}"
        );
    }
}

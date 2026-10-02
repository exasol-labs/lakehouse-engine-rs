use std::collections::BTreeMap;

use super::super::ConnectionStorage;
use super::super::delta_schema::build_delta_table_schema;
use super::*;
use crate::adapter::pushdown::test_support::filter_json::{
    and, column, compare, equal, number, or,
};
use crate::adapter::pushdown::test_support::{
    GlueEndpoint, ObjectEndpoint, SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY, SIGV4_SECRET_KEY,
    UNREACHABLE_CATALOG, closed_port_storage, glue_catalog_table, object_endpoint, sample_storage,
    sigv4_creds, unauthenticated_creds, user_message,
};
use crate::adapter::tests::parquet_fixture::{in_memory_store, values};
use crate::adapter::tests::recording_store::RecordingStore;
use crate::scan::spec::reconstruct_abs_uri;
use crate::scan::test_support::column_binding_for;
use crate::tests::hive_type_cases::{
    UNRECOGNIZED_HIVE_TYPES, binary_hive_types, nested_hive_types, primitive_hive_types,
};
use arrow::array::{BinaryArray, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use delta_kernel::schema::DataType as DeltaType;
use lakehouse_catalog::{
    CatalogColumn, CatalogTableIdent, CatalogTableType, ColumnSourceType, ConnectionCreds,
    TableFormat, UnityCatalogSession,
};
use serde_json::json;

const TABLE_NAME: &str = "cat.sch.sales";

const TABLE_PREFIX: &str = "unity/sales";

const TABLE_ROOT: &str = "s3://bucket/unity/sales";

const GLUE_TABLE_ROOT: &str = "s3://bucket/glue/orders";

/// No Parquet reader parses this, so a plan-time footer read would fail the resolution.
const UNREADABLE_BODY: &str = "not a parquet file";

const STATIC_SECRET: &str = "minioadmin";

fn raw_column(name: &str, type_json: Option<String>) -> CatalogColumn {
    CatalogColumn {
        name: name.to_string(),
        source_type: ColumnSourceType::Unity {
            type_name: "UNUSED".to_string(),
            precision: 0,
            scale: 0,
            type_json,
        },
    }
}

/// Each `(name, spark_type)` column is declared NOT NULL.
fn sales_table(columns: &[(&str, &str)], partition_columns: &[&str]) -> CatalogTable {
    let columns = columns
        .iter()
        .map(|(name, spark_type)| {
            let field =
                json!({"name": name, "type": spark_type, "nullable": false, "metadata": {}});
            raw_column(name, Some(field.to_string()))
        })
        .collect();
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: vec!["cat".into(), "sch".into()],
            name: "sales".into(),
        },
        table_type: CatalogTableType::Table,
        storage_location: Some(TABLE_ROOT.to_string()),
        format: TableFormat::Parquet,
        vended_credential_key: Some("table-sales".to_string()),
        partition_columns: partition_columns.iter().map(|c| c.to_string()).collect(),
        metadata_location: None,
        columns,
    }
}

fn id_table() -> CatalogTable {
    sales_table(&[("id", "long")], &[])
}

fn partitioned_table() -> CatalogTable {
    sales_table(
        &[("id", "long"), ("region", "string"), ("year", "integer")],
        &["year", "region"],
    )
}

/// Each key holds [`UNREADABLE_BODY`].
async fn served_storage<K: ToString>(keys: impl IntoIterator<Item = K>) -> ObjectEndpoint {
    let objects = keys
        .into_iter()
        .map(|key| (key.to_string(), UNREADABLE_BODY.to_string()));
    ObjectEndpoint::spawn("bucket", objects.collect()).await
}

fn under_table<'k>(keys: &'k [&str]) -> impl Iterator<Item = String> + 'k {
    keys.iter().map(|key| format!("{TABLE_PREFIX}/{key}"))
}

async fn resolve_with(
    table: &CatalogTable,
    storage: &StorageBackend,
    use_vended_credentials: bool,
    filter: Option<&Json>,
) -> Result<ResolvedScan, String> {
    let creds = ConnectionCreds {
        use_vended_credentials,
        ..unauthenticated_creds()
    };
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let connection = ConnectionStorage {
        storage,
        creds: &creds,
        allow_http: true,
    };
    let reader = CatalogParquetFormatReader {
        table,
        files: ParquetFileSource::TableDirectory(UnityTableStorage::new(
            &session,
            table,
            &connection,
        )),
    };
    user_outcome(reader.resolve_scan(filter).await)
}

fn user_outcome(outcome: Result<ResolvedScan, UdfError>) -> Result<ResolvedScan, String> {
    outcome.map_err(user_message)
}

async fn resolve(table: &CatalogTable, keys: &[&str]) -> ResolvedScan {
    resolve_with(
        table,
        &served_storage(under_table(keys)).await.storage,
        false,
        None,
    )
    .await
    .expect("the Unity Parquet table resolves")
}

async fn refusal(table: &CatalogTable, storage: StorageBackend, vended: bool) -> String {
    resolve_with(table, &storage, vended, None)
        .await
        .expect_err("resolution must fail, never answer a scan")
}

fn file_paths(scan: &ResolvedScan) -> Vec<&str> {
    scan.files.iter().map(|file| file.path.as_str()).collect()
}

fn identity_bound(name: &str, arrow_type: &str) -> LogicalField {
    LogicalField {
        field_id: None,
        name: name.to_string(),
        arrow_type: arrow_type.to_string(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    }
}

/// Scenario: A Unity Parquet table lists its files through the shared directory seam and reads no footer
#[tokio::test]
async fn files_are_listed_through_the_seam_and_no_footer_is_read() {
    let under_table_keys = [
        "part-0.parquet",
        "a/b/part-2.parquet",
        "_SUCCESS",
        "part-4.snappy",
    ];
    let sibling = format!("{TABLE_PREFIX}_archive/part-9.parquet");
    let storage = served_storage(under_table(&under_table_keys).chain([sibling]))
        .await
        .storage;

    let scan = resolve_with(&id_table(), &storage, false, None)
        .await
        .expect("a listing over unreadable bodies resolves when no footer is read");

    assert_eq!(
        file_paths(&scan),
        vec!["a/b/part-2.parquet", "part-0.parquet"]
    );
    assert!(
        scan.files
            .iter()
            .all(|file| file.size == UNREADABLE_BODY.len() as u64)
    );
    assert_eq!(scan.table_root, TABLE_ROOT);
    assert_eq!(scan.effective_storage, storage);
}

#[tokio::test]
async fn logical_schema_is_the_catalog_column_list_and_undescribed_columns_are_refused() {
    let mut table = sales_table(&[("id", "long"), ("payload", "binary")], &[]);
    let drifted =
        json!({"name": "customerid", "type": "string", "nullable": false, "metadata": {}});
    table.columns.extend([
        raw_column("CustomerId", Some(drifted.to_string())),
        raw_column("note", None),
        raw_column("broken", Some("{}".to_string())),
    ]);

    let scan = resolve(&table, &["part-0.parquet"]).await;

    assert_eq!(
        scan.logical_schema,
        vec![
            identity_bound("id", "int64"),
            identity_bound("CustomerId", "utf8")
        ]
    );
    let refused: Vec<(&str, bool)> = scan
        .refused_columns
        .iter()
        .map(|refused| {
            let undescribed = refused.reason.contains("type_json");
            (refused.column_name.as_str(), undescribed)
        })
        .collect();
    assert_eq!(
        refused,
        vec![("payload", false), ("note", true), ("broken", true)]
    );
}

#[tokio::test]
async fn an_unplannable_catalog_schema_fails_the_plan() {
    let mut no_mappable = sales_table(&[("payload", "binary")], &[]);
    no_mappable.columns.push(raw_column("note", None));
    let cases = [
        (
            no_mappable,
            vec![
                "Unity Parquet table has no mappable column",
                "'payload'",
                "'note'",
            ],
        ),
        (
            sales_table(&[("id", "long"), ("ID", "long")], &[]),
            vec![TABLE_NAME, "do not form one Spark schema"],
        ),
        (
            sales_table(&[("id", "long"), ("shard", "binary")], &["shard"]),
            vec![TABLE_NAME, "'shard'", "partition column"],
        ),
    ];

    for (table, fragments) in cases {
        let message = refusal(&table, sample_storage(), false).await;
        for fragment in fragments {
            assert!(
                message.contains(fragment),
                "'{fragment}' missing: {message}"
            );
        }
    }
}

#[tokio::test]
async fn file_columns_bind_under_the_case_fold_and_the_shared_cast_rules() {
    let table = sales_table(&[("CustomerId", "long"), ("amount", "integer")], &[]);
    let scan = resolve(&table, &["part-0.parquet"]).await;
    let (logical, factory) = column_binding_for(&scan.logical_schema, &scan.table_root);
    let physical = Arc::new(Schema::new(vec![
        Field::new("region", ArrowType::Utf8, true),
        Field::new("customerid", ArrowType::Int32, true),
        Field::new("amount", ArrowType::Float64, true),
    ]));
    let adapter = factory
        .create(logical, physical)
        .expect("a refused column fails only a rewrite that references it");

    let widened = adapter
        .rewrite(Arc::new(Column::new("CustomerId", 0)))
        .expect("a drifted, narrower file column in the widening set binds");
    let bound = widened
        .downcast_ref::<CastExpr>()
        .and_then(|cast| cast.expr().downcast_ref::<Column>())
        .expect("the narrower int column is cast to the declared long");
    assert_eq!((bound.name(), bound.index()), ("customerid", 1));

    let message = adapter
        .rewrite(Arc::new(Column::new("amount", 1)) as Arc<dyn PhysicalExpr>)
        .expect_err("a double file column under an int declaration is refused")
        .to_string();
    for fragment in ["'amount'", TABLE_ROOT, "Float64", "Int32"] {
        assert!(
            message.contains(fragment),
            "'{fragment}' missing: {message}"
        );
    }
}

/// Scenario: Partition columns come from the catalog and partition values from the file paths
#[tokio::test]
async fn partition_columns_come_from_partition_index_and_values_from_paths() {
    let scan = resolve(
        &partitioned_table(),
        &[
            "YEAR=2025/region=__HIVE_DEFAULT_PARTITION__/c.parquet",
            "d.parquet",
            "year=2024/region=eu/a.parquet",
        ],
    )
    .await;

    assert_eq!(scan.partition_columns, vec!["year", "region"]);
    assert_eq!(
        scan.files
            .iter()
            .map(|file| file.partition_values.clone())
            .collect::<Vec<_>>(),
        vec![
            values(&[("year", Some("2025")), ("region", None)]),
            values(&[("year", None), ("region", None)]),
            values(&[("year", Some("2024")), ("region", Some("eu"))]),
        ]
    );
}

/// Scenario: A predicate on a partition column prunes files under the column's declared type
#[tokio::test]
async fn a_partition_predicate_prunes_under_the_declared_type() {
    let every_file = [
        "year=2024/region=eu/a.parquet",
        "year=2024/region=us/b.parquet",
        "year=2025/region=eu/c.parquet",
    ];
    let storage = served_storage(under_table(&every_file)).await.storage;
    let cases = [
        (equal("REGION", "eu"), vec![every_file[0], every_file[2]]),
        (
            compare("predicate_equal", column("YEAR"), number("2024")),
            vec![every_file[0], every_file[1]],
        ),
        (
            or(vec![
                equal("REGION", "us"),
                compare("predicate_less", number("10"), column("AMOUNT")),
            ]),
            every_file.to_vec(),
        ),
    ];

    for (filter, expected) in cases {
        let scan = resolve_with(&partitioned_table(), &storage, false, Some(&filter))
            .await
            .expect("a filtered request resolves");
        assert_eq!(file_paths(&scan), expected, "{filter}");
    }
}

#[tokio::test]
async fn storage_is_resolved_through_the_shared_unity_path() {
    let no_location = CatalogTable {
        storage_location: None,
        ..id_table()
    };
    let no_vending_key = CatalogTable {
        vended_credential_key: None,
        ..id_table()
    };

    let empty_static = refusal(&no_location, sample_storage(), false).await;
    let empty_vended = refusal(&no_location, sample_storage(), true).await;
    let no_key = refusal(&no_vending_key, sample_storage(), true).await;
    let failed_listing = refusal(&id_table(), closed_port_storage(), false).await;

    assert_eq!(empty_static, empty_vended);
    assert!(
        empty_static.contains(TABLE_NAME) && empty_static.contains("EMPTY storage location"),
        "{empty_static}"
    );
    assert!(
        no_key.contains(TABLE_NAME)
            && no_key.contains("vend")
            && !no_key.contains("failed to list"),
        "a listing proves a static-credential fallback: {no_key}"
    );
    assert!(
        failed_listing.contains("failed to list") && failed_listing.contains(TABLE_PREFIX),
        "{failed_listing}"
    );
    for message in [&empty_static, &no_key, &failed_listing] {
        for secret in [STATIC_SECRET, SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY] {
            assert!(!message.contains(secret), "{message}");
        }
    }
}

const GLUE_TABLE_NAME: &str = "sales.orders";

fn glue_table(columns: &[(&str, &str)], partition_columns: &[&str]) -> CatalogTable {
    CatalogTable {
        partition_columns: partition_columns.iter().map(|c| c.to_string()).collect(),
        ..glue_catalog_table(
            "sales",
            "orders",
            TableFormat::Parquet,
            GLUE_TABLE_ROOT,
            columns,
        )
    }
}

fn delta_classification(columns: &[(String, DeltaType)]) -> Vec<LogicalField> {
    let schema = StructType::try_new(
        columns
            .iter()
            .map(|(name, spark_type)| StructField::nullable(name, spark_type.clone())),
    )
    .expect("distinct column names");
    let (logical_schema, _, refused) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("an annotation-free schema classifies");
    assert!(refused.is_empty(), "{refused:?}");
    logical_schema
}

/// Scenario: Every Hive primitive type maps to its Spark type
/// Scenario: Nested Hive types parse recursively and render as JSON text
/// Scenario: A binary, unrecognized, or malformed Hive type refuses only its column
#[test]
fn glue_columns_are_classified_by_the_spark_type_classifier_and_refused_only_by_their_type() {
    let typed: Vec<(String, &str, DeltaType)> = primitive_hive_types()
        .map(|(hive_type, spark, _)| (hive_type, spark))
        .into_iter()
        .chain(nested_hive_types())
        .enumerate()
        .map(|(index, (hive_type, spark))| (format!("c{index}"), hive_type, spark))
        .collect();
    let refused_types: Vec<&str> = binary_hive_types()
        .map(|(hive_type, _)| hive_type)
        .into_iter()
        .chain(UNRECOGNIZED_HIVE_TYPES)
        .collect();
    let refused_names: Vec<String> = (0..refused_types.len()).map(|i| format!("r{i}")).collect();
    let columns: Vec<(&str, &str)> = typed
        .iter()
        .map(|(name, hive_type, _)| (name.as_str(), *hive_type))
        .chain(
            refused_names
                .iter()
                .map(String::as_str)
                .zip(refused_types.clone()),
        )
        .collect();

    let Ok(schema) = catalog_schema(&glue_table(&columns, &[]), "Glue") else {
        panic!("a refused column never fails the table");
    };

    let spark: Vec<(String, DeltaType)> = typed
        .iter()
        .map(|(name, _, spark)| (name.clone(), spark.clone()))
        .collect();
    assert_eq!(schema.logical_schema, delta_classification(&spark));
    assert!(
        schema.logical_schema.iter().all(|field| field.nullable
            && field.field_id.is_none()
            && field.physical_name.is_none())
    );
    let field_of = |hive_type: &str| {
        let index = typed.iter().position(|(_, typed, _)| *typed == hive_type);
        &schema.logical_schema[index.expect("a typed fixture column")]
    };
    assert_eq!(field_of("decimal(10,2)").arrow_type, "decimal128(10,2)");
    assert_eq!(
        field_of("DECIMAL( 38 , 10 )").arrow_type,
        "utf8",
        "precision 38 is outside Exasol's DECIMAL domain"
    );
    for (hive_type, _) in nested_hive_types() {
        let field = field_of(hive_type);
        assert!(
            field.arrow_type == "utf8" && field.nested.is_some(),
            "{field:?}"
        );
    }

    let binary = ["has type 'binary'", "#351"].map(str::to_string);
    let expected_fragments = [
        binary.to_vec(),
        [binary.to_vec(), vec!["whose member 'r1.b'".to_string()]].concat(),
    ]
    .into_iter()
    .chain(UNRECOGNIZED_HIVE_TYPES.map(|hive_type| vec![format!(": Hive type '{hive_type}'")]));
    assert_eq!(schema.refused_columns.len(), refused_types.len());
    for ((refused, name), fragments) in schema
        .refused_columns
        .iter()
        .zip(&refused_names)
        .zip(expected_fragments)
    {
        assert_eq!(&refused.column_name, name);
        let reason = &refused.reason;
        assert!(
            reason.starts_with(&format!("Glue column '{name}'")),
            "{reason}"
        );
        for fragment in fragments {
            assert!(reason.contains(&fragment), "'{fragment}' missing: {reason}");
        }
    }

    let every_column_refused = glue_table(&[("b", "binary"), ("u", "uniontype<int>")], &[]);
    let error = user_message(
        catalog_schema(&every_column_refused, "Glue")
            .err()
            .expect("a table whose every column is refused must be refused as a whole"),
    );
    assert!(
        error.starts_with("Glue table has no mappable column") && error.contains("'b'"),
        "{error}"
    );
}

const PARQUET_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";

const ORC_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";

fn partitions_page(partitions: &[(&[&str], &str, &str)]) -> Json {
    let partitions: Vec<Json> = partitions
        .iter()
        .map(|(values, location, input_format)| {
            json!({
                "Values": values,
                "StorageDescriptor": {"Location": location, "InputFormat": input_format}
            })
        })
        .collect();
    json!({ "Partitions": partitions })
}

async fn resolve_glue(
    table: &CatalogTable,
    glue_address: &str,
    storage: &StorageBackend,
    filter: Option<&Json>,
) -> Result<ResolvedScan, String> {
    let session = GlueCatalogSession::new(glue_address, storage.clone(), sigv4_creds())
        .expect("the CONNECTION region signs the session");
    let reader = CatalogParquetFormatReader {
        table,
        files: ParquetFileSource::GluePartitions {
            session: &session,
            storage,
        },
    };
    user_outcome(reader.resolve_scan(filter).await)
}

fn scanned_key(entry: &FileEntry, table_root: &str) -> String {
    let uri = reconstruct_abs_uri(&entry.path, table_root);
    ListingTableUrl::parse(&uri)
        .unwrap_or_else(|error| panic!("the scan parses '{uri}': {error}"))
        .prefix()
        .to_string()
}

fn scanned_files(scan: &ResolvedScan) -> Vec<(String, BTreeMap<String, Option<String>>)> {
    scan.files
        .iter()
        .map(|entry| {
            (
                scanned_key(entry, &scan.table_root),
                entry.partition_values.clone(),
            )
        })
        .collect()
}

/// Scenario: A Glue Parquet table is planned by the shared catalog-declared Parquet reader
#[tokio::test]
async fn glue_parquet_table_is_planned_by_the_shared_reader_with_nullable_catalog_columns() {
    let storage = served_storage(["glue/orders/part-0"]).await.storage;
    let glue = glue_table(&[("id", "bigint"), ("CustomerId", "string")], &[]);
    let unity = sales_table(&[("id", "long"), ("CustomerId", "string")], &[]);

    let glue_scan = resolve_glue(&glue, UNREACHABLE_CATALOG, &storage, None)
        .await
        .expect("an unpartitioned table issues no Glue request and reads no footer");
    let unity_scan = resolve(&unity, &["part-0.parquet"]).await;

    assert_eq!(glue_scan.logical_schema, unity_scan.logical_schema);
    assert_eq!(
        glue_scan.logical_schema,
        vec![
            identity_bound("id", "int64"),
            identity_bound("CustomerId", "utf8")
        ]
    );
    assert_eq!(file_paths(&glue_scan), vec!["part-0"]);
    assert_eq!(glue_scan.table_root, GLUE_TABLE_ROOT);
    assert_eq!(
        glue_scan.effective_storage, storage,
        "the CONNECTION's static storage"
    );
    assert!(glue_scan.partition_columns.is_empty());
    assert!(glue_scan.name_mapping.is_empty() && glue_scan.refused_columns.is_empty());
}

/// Scenario: A Glue location's data files are its direct children of any name
#[tokio::test]
async fn a_glue_location_reads_direct_children_of_any_name() {
    let children = |location: &'static str| {
        [
            "20240101_abc",
            "part-0.snappy.parquet",
            "_SUCCESS",
            ".hidden",
            "nested/x.parquet",
        ]
        .map(|name| (format!("{location}/{name}"), UNREADABLE_BODY.to_string()))
        .into_iter()
        .chain([(format!("{location}/empty"), String::new())])
    };
    let objects = children("glue/orders/p=1").chain(children("glue/flat"));
    let storage = object_endpoint("bucket", objects.collect()).await;
    let glue = GlueEndpoint::spawn(|_| {
        (
            200,
            partitions_page(&[(&["1"], "s3://bucket/glue/orders/p=1/", PARQUET_INPUT_FORMAT)]),
        )
    })
    .await;
    let partitioned_table = glue_table(&[("id", "bigint"), ("p", "int")], &["p"]);
    let flat_table = CatalogTable {
        storage_location: Some("s3://bucket/glue/flat".to_string()),
        ..glue_table(&[("id", "bigint")], &[])
    };

    let partitioned = resolve_glue(&partitioned_table, &glue.address, &storage, None)
        .await
        .expect("the partition location lists");
    let flat = resolve_glue(&flat_table, UNREACHABLE_CATALOG, &storage, None)
        .await
        .expect("the table location lists");

    assert_eq!(
        file_paths(&partitioned),
        vec!["p=1/20240101_abc", "p=1/part-0.snappy.parquet"]
    );
    assert_eq!(
        file_paths(&flat),
        vec!["20240101_abc", "part-0.snappy.parquet"]
    );
}

/// Scenario: Each kept partition's location is listed and its files carry the partition's Glue values
/// Scenario: A catalog-registered location is listed by its raw object key
#[tokio::test]
async fn each_kept_partition_is_listed_and_carries_its_glue_values() {
    let storage = served_storage([
        "glue/orders/p_str=__HIVE_DEFAULT_PARTITION__/f",
        "glue/orders/custom_dir/f",
        "glue/orders/p_str=a b%2Fc/f",
        "elsewhere/p_str=a b%2Fc/f",
        "glue/orders/p_str=path_value/f",
    ])
    .await
    .storage;
    let glue = GlueEndpoint::spawn(|_| {
        let page = partitions_page(&[
            (
                &["__HIVE_DEFAULT_PARTITION__"],
                "s3://bucket/glue/orders/p_str=__HIVE_DEFAULT_PARTITION__/",
                PARQUET_INPUT_FORMAT,
            ),
            (
                &["no_segment"],
                "s3://bucket/glue/orders/custom_dir",
                PARQUET_INPUT_FORMAT,
            ),
            (
                &["a b/c"],
                "s3://bucket/glue/orders/p_str=a b%2Fc/",
                PARQUET_INPUT_FORMAT,
            ),
            (
                &["outside"],
                "s3://bucket/elsewhere/p_str=a b%2Fc",
                PARQUET_INPUT_FORMAT,
            ),
            (
                &["glue_value"],
                "s3a://bucket/glue/orders/p_str=path_value",
                PARQUET_INPUT_FORMAT,
            ),
        ]);
        (200, page)
    })
    .await;
    let table = CatalogTable {
        storage_location: Some("s3a://bucket/glue/orders".to_string()),
        ..glue_table(&[("id", "bigint"), ("p_str", "string")], &["p_str"])
    };

    let scan = resolve_glue(&table, &glue.address, &storage, None)
        .await
        .expect("every partition is a same-bucket Parquet location");

    assert_eq!(
        scan.table_root, "s3://bucket/glue/orders",
        "s3a reads as s3"
    );
    assert_eq!(scan.partition_columns, vec!["p_str"]);
    let mut expected = vec![
        ("elsewhere/p_str=a b%2Fc/f", Some("outside")),
        ("glue/orders/custom_dir/f", Some("no_segment")),
        ("glue/orders/p_str=__HIVE_DEFAULT_PARTITION__/f", None),
        ("glue/orders/p_str=a b%2Fc/f", Some("a b/c")),
        ("glue/orders/p_str=path_value/f", Some("glue_value")),
    ];
    expected.sort_by_key(|(key, _)| *key);
    let mut scanned = scanned_files(&scan);
    scanned.sort_by(|(left, _), (right, _)| left.cmp(right));
    assert_eq!(
        scanned,
        expected
            .into_iter()
            .map(|(key, value)| (key.to_string(), values(&[("p_str", value)])))
            .collect::<Vec<_>>()
    );
    let outside = scan
        .files
        .iter()
        .find(|entry| entry.partition_values["p_str"].as_deref() == Some("outside"))
        .expect("the out-of-root partition's file");
    assert!(
        outside.path.starts_with("s3://bucket/"),
        "a file outside the table location is absolute: {}",
        outside.path
    );
    assert!(
        scan.files
            .iter()
            .filter(|entry| *entry != outside)
            .all(|entry| !entry.path.contains("://")),
        "every file inside the table location, s3a-addressed or not, is relative: {:?}",
        file_paths(&scan)
    );
    let requests = glue.bodies_of("GetPartitions");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["DatabaseName"], "sales");
    assert_eq!(requests[0]["TableName"], "orders");
}

fn partition(
    values: &[(&str, Option<&str>)],
    location: &str,
    format: PartitionFormat,
) -> CatalogPartition {
    CatalogPartition {
        values: values
            .iter()
            .map(|(key, value)| (key.to_string(), value.map(str::to_string)))
            .collect(),
        location: location.to_string(),
        format,
    }
}

fn keep_under(
    filter: &Json,
    declared: &[(&str, ArrowType)],
) -> impl Fn(&BTreeMap<String, Option<String>>) -> bool + Send + Sync + use<> {
    let declared: Vec<(String, ArrowType)> = declared
        .iter()
        .map(|(name, arrow_type)| (name.to_string(), arrow_type.clone()))
        .collect();
    let predicate = PartitionPredicate::from_filter(Some(filter), &declared);
    move |values| predicate.keeps(values)
}

fn keep_every_partition(_: &BTreeMap<String, Option<String>>) -> bool {
    true
}

async fn recording_store<K: AsRef<str>>(keys: &[K]) -> Arc<RecordingStore> {
    let objects: Vec<(&str, &[u8])> = keys
        .iter()
        .map(|key| (key.as_ref(), b"rows".as_slice()))
        .collect();
    RecordingStore::wrapping(in_memory_store(&objects).await)
}

async fn planned_keys(
    store: Arc<RecordingStore>,
    partitions: Vec<CatalogPartition>,
    keep: &PartitionKeepPredicate,
) -> Result<Vec<String>, String> {
    let store: Arc<dyn ObjectStore> = store;
    let (store_root, table_prefix) =
        raw_location_prefix(GLUE_TABLE_ROOT).expect("the Glue table root names a raw key");
    plan_glue_partitions(&store, &store_root, &table_prefix, partitions, keep)
        .await
        .map(|files| {
            files
                .iter()
                .map(|entry| scanned_key(entry, GLUE_TABLE_ROOT))
                .collect()
        })
        .map_err(|error| error.to_string())
}

/// Scenario: A kept partition the reader cannot read faithfully fails the query naming it
#[tokio::test]
async fn a_kept_orc_or_foreign_bucket_partition_fails_naming_it_and_a_pruned_one_does_not() {
    let parquet = partition(
        &[("p_int", Some("1"))],
        "s3://bucket/glue/orders/p_int=1",
        PartitionFormat::Parquet,
    );
    let orc = partition(
        &[("p_int", Some("9"))],
        "s3://bucket/glue/orders/p_int=9",
        PartitionFormat::Unsupported {
            input_format: ORC_INPUT_FORMAT.to_string(),
        },
    );
    let foreign = partition(
        &[("p_int", Some("5"))],
        "s3://other-bucket/glue/orders/p_int=5",
        PartitionFormat::Parquet,
    );
    let objects = ["glue/orders/p_int=1/f"];
    let below_five = keep_under(
        &compare("predicate_less", column("P_INT"), number("5")),
        &[("p_int", ArrowType::Int32)],
    );

    let kept_orc = planned_keys(
        recording_store(&objects).await,
        vec![parquet.clone(), orc.clone()],
        &keep_every_partition,
    )
    .await
    .expect_err("a kept ORC partition fails the query");
    let kept_foreign = planned_keys(
        recording_store(&objects).await,
        vec![parquet.clone(), foreign.clone()],
        &keep_every_partition,
    )
    .await
    .expect_err("a kept partition in another bucket fails the query");
    let pruned = planned_keys(
        recording_store(&objects).await,
        vec![parquet, orc, foreign],
        &below_five,
    )
    .await
    .expect("a pruned partition contributes no row, so it cannot fail the query");

    for fragment in [
        "p_int=9",
        "s3://bucket/glue/orders/p_int=9",
        ORC_INPUT_FORMAT,
    ] {
        assert!(
            kept_orc.contains(fragment),
            "'{fragment}' missing: {kept_orc}"
        );
    }
    for fragment in [
        "p_int=5",
        "s3://other-bucket/glue/orders/p_int=5",
        "bucket 's3://other-bucket'",
    ] {
        assert!(
            kept_foreign.contains(fragment),
            "'{fragment}' missing: {kept_foreign}"
        );
    }
    assert_eq!(pruned, vec!["glue/orders/p_int=1/f"]);
}

/// Scenario: A partition predicate prunes partitions before their locations are listed
#[tokio::test]
async fn glue_partitions_are_pruned_on_glue_values_before_listing() {
    let filter = and(vec![
        compare("predicate_equal", column("P_INT"), number("1")),
        compare(
            "predicate_lessequal",
            json!({"type": "literal_date", "value": "2024-01-01"}),
            column("P_DATE"),
        ),
    ]);
    let rows = [
        ("1", "2024-01-01", "a", true),
        ("1", "2023-12-31", "b", false),
        ("2", "2024-06-01", "c", false),
        ("1", "2024-06-01", "d", true),
    ];
    let location = |p_str: &str| format!("glue/orders/p_str={p_str}");
    let page_rows: Vec<(Vec<&str>, String)> = rows
        .iter()
        .map(|(p_int, p_date, p_str, _)| {
            (
                vec![*p_int, *p_date, *p_str],
                format!("s3://bucket/{}", location(p_str)),
            )
        })
        .collect();
    let page = partitions_page(
        &page_rows
            .iter()
            .map(|(values, location)| (values.as_slice(), location.as_str(), PARQUET_INPUT_FORMAT))
            .collect::<Vec<_>>(),
    );
    let kept: Vec<String> = rows
        .iter()
        .filter(|(_, _, _, kept)| *kept)
        .map(|(_, _, p_str, _)| location(p_str))
        .collect();
    let objects =
        served_storage(rows.map(|(_, _, p_str, _)| format!("{}/f", location(p_str)))).await;
    let glue = GlueEndpoint::spawn(move |_| (200, page.clone())).await;
    let table = glue_table(
        &[
            ("id", "bigint"),
            ("p_int", "int"),
            ("p_date", "date"),
            ("p_str", "string"),
        ],
        &["p_int", "p_date", "p_str"],
    );

    let scan = resolve_glue(&table, &glue.address, &objects.storage, Some(&filter))
        .await
        .expect("a filtered Glue table resolves");

    assert_eq!(
        scanned_files(&scan)
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>(),
        kept.iter()
            .map(|location| format!("{location}/f"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        objects.listed_prefixes(),
        kept.iter()
            .map(|location| format!("{location}/"))
            .collect::<Vec<_>>(),
        "only the kept partitions' locations are listed"
    );
    let requests = glue.bodies_of("GetPartitions");
    assert!(
        requests.iter().all(|body| body.get("Expression").is_none()),
        "the predicate is never sent to Glue: {requests:?}"
    );
}

#[tokio::test]
async fn kept_partitions_are_listed_concurrently_within_the_table_store_budget() {
    let keys: Vec<String> = (0..DEFAULT_S3_MAX_CONNECTIONS * 3)
        .map(|index| format!("glue/orders/p={index}/f"))
        .collect();
    let store = recording_store(&keys).await;
    let partitions = (0..keys.len())
        .map(|index| {
            let value = index.to_string();
            partition(
                &[("p", Some(value.as_str()))],
                &format!("s3://bucket/glue/orders/p={index}"),
                PartitionFormat::Parquet,
            )
        })
        .collect();

    let planned = planned_keys(store.clone(), partitions, &keep_every_partition)
        .await
        .expect("every partition lists");

    assert_eq!(planned.len(), keys.len());
    let peak = store.peak_delimited_listings();
    assert!(
        (2..=DEFAULT_S3_MAX_CONNECTIONS).contains(&peak),
        "listings overlap within the store's connection budget, peak {peak}"
    );
    assert!(
        store
            .listings()
            .iter()
            .all(|listing| listing.starts_with("delimited ")),
        "a Glue location is listed only by delimiter, never recursively"
    );
}

#[tokio::test]
async fn glue_planning_failures_name_the_table_and_carry_no_credential() {
    let flat = glue_table(&[("id", "bigint")], &[]);
    let no_location = CatalogTable {
        storage_location: Some("  ".to_string()),
        ..flat.clone()
    };

    let empty = resolve_glue(&no_location, UNREACHABLE_CATALOG, &sample_storage(), None)
        .await
        .expect_err("an empty location fails before any request");
    let failed_listing = resolve_glue(&flat, UNREACHABLE_CATALOG, &closed_port_storage(), None)
        .await
        .expect_err("an unreachable store fails the listing");

    assert!(
        empty.contains(&format!(
            "the Glue metadata for table {GLUE_TABLE_NAME} carries an EMPTY storage location"
        )),
        "{empty}"
    );
    assert!(
        failed_listing.contains("failed to list") && failed_listing.contains("glue/orders"),
        "{failed_listing}"
    );
    for message in [&empty, &failed_listing] {
        for secret in [
            STATIC_SECRET,
            SENTINEL_ACCESS_KEY,
            SENTINEL_SECRET_KEY,
            SIGV4_SECRET_KEY,
        ] {
            assert!(!message.contains(secret), "{message}");
        }
    }
}

/// Scenario: A catalog string column over binary file data reads as text
#[tokio::test]
async fn a_catalog_string_column_over_unannotated_byte_array_reads_as_text() {
    let storage = served_storage(["glue/orders/part-0"]).await.storage;
    let table = glue_table(&[("id", "bigint"), ("name", "string")], &[]);
    let scan = resolve_glue(&table, UNREACHABLE_CATALOG, &storage, None)
        .await
        .expect("the catalog declares no binary column");
    assert!(
        scan.refused_columns.is_empty(),
        "a catalog-declared source refuses only by its declared type"
    );
    let (logical, factory) = column_binding_for(&scan.logical_schema, &scan.table_root);
    let physical = Arc::new(Schema::new(vec![Field::new(
        "name",
        ArrowType::Binary,
        true,
    )]));
    let name = factory
        .create(logical, Arc::clone(&physical))
        .expect("the binding builds")
        .rewrite(Arc::new(Column::new("name", 1)))
        .expect("an unannotated BYTE_ARRAY under a declared string is admitted");
    let read = |bytes: &[u8]| {
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(BinaryArray::from_iter_values([bytes]))],
        )
        .expect("a one-row file batch");
        name.evaluate(&batch)
            .and_then(|value| value.into_array(batch.num_rows()))
    };

    let text = read(b"alice").expect("valid UTF-8 reads as text");
    let invalid = read(&[0xff, 0xfe]);

    assert_eq!(
        text.as_any()
            .downcast_ref::<StringArray>()
            .map(|array| array.value(0)),
        Some("alice")
    );
    assert!(
        invalid.is_err(),
        "bytes that are not valid UTF-8 fail the read, never an altered value: {invalid:?}"
    );
}

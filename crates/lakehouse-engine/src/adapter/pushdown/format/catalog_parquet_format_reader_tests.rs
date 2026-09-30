use super::super::ConnectionStorage;
use super::super::delta_schema::build_delta_table_schema;
use super::*;
use crate::adapter::pushdown::test_support::filter_json::{
    and, column, compare, equal, number, or,
};
use crate::adapter::pushdown::test_support::{
    GlueEndpoint, SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY, closed_port_storage, object_endpoint,
    sample_storage, unauthenticated_creds,
};
use crate::adapter::tests::parquet_fixture::{in_memory_store, values};
use crate::scan::spec::reconstruct_abs_uri;
use crate::scan::test_support::column_binding_for;
use arrow::array::{BinaryArray, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use delta_kernel::schema::{ArrayType, DataType as DeltaType, MapType};
use futures::stream::BoxStream;
use lakehouse_catalog::{
    CatalogColumn, CatalogTableIdent, CatalogTableType, ColumnSourceType, ConnectionCreds,
    TableFormat, UnityCatalogSession,
};
use object_store::PutPayload;
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A credential request here fails with a transport error, distinct from every asserted refusal.
const UNREACHABLE_CATALOG: &str = "http://127.0.0.1:1";

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

/// `keys` sit below the table prefix; `siblings` are full object keys.
async fn served_storage(keys: &[&str], siblings: &[&str]) -> StorageBackend {
    let under_table = keys.iter().map(|key| format!("{TABLE_PREFIX}/{key}"));
    let objects = under_table.chain(siblings.iter().map(|key| key.to_string()));
    object_endpoint(
        "bucket",
        objects
            .map(|key| (key, UNREADABLE_BODY.to_string()))
            .collect(),
    )
    .await
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
    outcome.map_err(|error| match error {
        UdfError::User(message) => message,
        other => panic!("every refusal must be a user error, got {other:?}"),
    })
}

async fn resolve(table: &CatalogTable, keys: &[&str]) -> ResolvedScan {
    resolve_with(table, &served_storage(keys, &[]).await, false, None)
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
    let under_table = [
        "part-0.parquet",
        "a/b/part-2.parquet",
        "_SUCCESS",
        "part-4.snappy",
    ];
    let sibling = format!("{TABLE_PREFIX}_archive/part-9.parquet");
    let storage = served_storage(&under_table, &[sibling.as_str()]).await;

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
    let storage = served_storage(&every_file, &[]).await;
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

fn glue_column(name: &str, hive_type: &str) -> CatalogColumn {
    CatalogColumn {
        name: name.to_string(),
        source_type: ColumnSourceType::Glue {
            hive_type: hive_type.to_string(),
        },
    }
}

fn glue_table(columns: &[(&str, &str)], partition_columns: &[&str]) -> CatalogTable {
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: vec!["sales".into()],
            name: "orders".into(),
        },
        table_type: CatalogTableType::Table,
        storage_location: Some(GLUE_TABLE_ROOT.to_string()),
        format: TableFormat::Parquet,
        vended_credential_key: None,
        partition_columns: partition_columns.iter().map(|c| c.to_string()).collect(),
        metadata_location: None,
        columns: columns
            .iter()
            .map(|(name, hive_type)| glue_column(name, hive_type))
            .collect(),
    }
}

fn delta_classification(columns: &[(&str, DeltaType)]) -> Vec<LogicalField> {
    let schema = StructType::try_new(
        columns
            .iter()
            .map(|(name, spark_type)| StructField::nullable(*name, spark_type.clone())),
    )
    .expect("distinct column names");
    let (logical_schema, _, refused) =
        build_delta_table_schema(&schema, ColumnMappingMode::None, Vec::new())
            .expect("an annotation-free schema classifies");
    assert!(refused.is_empty(), "{refused:?}");
    logical_schema
}

fn decimal(precision: u8, scale: u8) -> DeltaType {
    DeltaType::decimal(precision, scale).expect("a valid Spark decimal")
}

/// Scenario: Every Hive primitive type maps to its Spark type
#[test]
fn glue_columns_are_classified_by_the_spark_type_classifier() {
    let columns = [
        ("c_tinyint", "tinyint", DeltaType::BYTE),
        ("c_smallint", "smallint", DeltaType::SHORT),
        ("c_int", "int", DeltaType::INTEGER),
        ("c_integer", "integer", DeltaType::INTEGER),
        ("c_bigint", "bigint", DeltaType::LONG),
        ("c_float", "float", DeltaType::FLOAT),
        ("c_double", "double", DeltaType::DOUBLE),
        ("c_boolean", "boolean", DeltaType::BOOLEAN),
        ("c_string", "string", DeltaType::STRING),
        ("c_varchar", "varchar(10)", DeltaType::STRING),
        ("c_char", "char(5)", DeltaType::STRING),
        ("c_date", "date", DeltaType::DATE),
        ("c_timestamp", "timestamp", DeltaType::TIMESTAMP_NTZ),
        ("c_decimal", "decimal", decimal(10, 0)),
        ("c_decimal_10_2", "decimal(10,2)", decimal(10, 2)),
        ("c_decimal_38_10", "DECIMAL( 38 , 10 )", decimal(38, 10)),
    ];
    let table = glue_table(&columns.clone().map(|(name, hive, _)| (name, hive)), &[]);

    let schema = catalog_schema(&table, "Glue").expect("every Hive primitive classifies");

    assert_eq!(
        schema.logical_schema,
        delta_classification(&columns.map(|(name, _, spark)| (name, spark)))
    );
    assert!(schema.refused_columns.is_empty());
    assert!(
        schema.logical_schema.iter().all(|field| field.nullable
            && field.field_id.is_none()
            && field.physical_name.is_none())
    );
    let tag = |name: &str| {
        schema
            .logical_schema
            .iter()
            .find(|field| field.name == name)
            .map(|field| field.arrow_type.as_str())
    };
    assert_eq!(tag("c_decimal_10_2"), Some("decimal128(10,2)"));
    assert_eq!(
        tag("c_decimal_38_10"),
        Some("utf8"),
        "precision 38 is outside Exasol's DECIMAL domain"
    );
}

/// Scenario: Nested Hive types parse recursively and render as JSON text
#[test]
fn glue_nested_columns_carry_the_string_tag_and_a_nested_descriptor() {
    let nullable_struct = |fields: Vec<StructField>| {
        DeltaType::Struct(Box::new(
            StructType::try_new(fields).expect("distinct members"),
        ))
    };
    let columns = [
        (
            "c_array",
            "array<int>",
            DeltaType::Array(Box::new(ArrayType::new(DeltaType::INTEGER, true))),
        ),
        (
            "c_map",
            "map<varchar(1),int>",
            DeltaType::Map(Box::new(MapType::new(
                DeltaType::STRING,
                DeltaType::INTEGER,
                true,
            ))),
        ),
        (
            "c_struct",
            "struct<x:int,y:string>",
            nullable_struct(vec![
                StructField::nullable("x", DeltaType::INTEGER),
                StructField::nullable("y", DeltaType::STRING),
            ]),
        ),
        (
            "c_array_of_struct",
            "array<struct<a:decimal(5,2)>>",
            DeltaType::Array(Box::new(ArrayType::new(
                nullable_struct(vec![StructField::nullable("a", decimal(5, 2))]),
                true,
            ))),
        ),
    ];
    let table = glue_table(&columns.clone().map(|(name, hive, _)| (name, hive)), &[]);

    let schema = catalog_schema(&table, "Glue").expect("every nested Hive type classifies");

    assert_eq!(
        schema.logical_schema,
        delta_classification(&columns.map(|(name, _, spark)| (name, spark)))
    );
    assert!(
        schema
            .logical_schema
            .iter()
            .all(|field| field.arrow_type == "utf8" && field.nested.is_some()),
        "{:?}",
        schema.logical_schema
    );
}

/// Scenario: A binary, unrecognized, or malformed Hive type refuses only its column
#[test]
fn binary_unrecognized_and_malformed_glue_types_refuse_only_their_column() {
    let table = glue_table(
        &[
            ("id", "int"),
            ("b", "binary"),
            ("s", "struct<b:binary>"),
            ("u", "uniontype<int,string>"),
            ("i", "interval_day_time"),
            ("m", "map<int>"),
            ("e", ""),
        ],
        &[],
    );

    let schema = catalog_schema(&table, "Glue").expect("a refused column never fails the table");

    assert_eq!(
        schema
            .logical_schema
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id"]
    );
    let reasons: Vec<(&str, &str)> = schema
        .refused_columns
        .iter()
        .map(|refused| (refused.column_name.as_str(), refused.reason.as_str()))
        .collect();
    assert_eq!(
        reasons.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
        vec!["b", "s", "u", "i", "m", "e"]
    );
    let expected_fragments: [&[&str]; 6] = [
        &["Glue column 'b'", "has type 'binary'", "#351"],
        &[
            "Glue column 's'",
            "whose member 's.b'",
            "has type 'binary'",
            "#351",
        ],
        &["Glue column 'u'", "'uniontype<int,string>'"],
        &["Glue column 'i'", "'interval_day_time'"],
        &["Glue column 'm'", "'map<int>'"],
        &["Glue column 'e'", "Hive type ''"],
    ];
    for ((name, reason), fragments) in reasons.iter().zip(expected_fragments) {
        for fragment in fragments {
            assert!(
                reason.contains(fragment),
                "{name}: '{fragment}' missing: {reason}"
            );
        }
    }

    let every_column_refused = glue_table(&[("b", "binary"), ("u", "uniontype<int>")], &[]);
    let error = match catalog_schema(&every_column_refused, "Glue") {
        Err(UdfError::User(message)) => message,
        Err(other) => panic!("a whole-table refusal is a user error, got {other:?}"),
        Ok(_) => panic!("a table whose every column is refused must be refused as a whole"),
    };
    assert!(
        error.starts_with("Glue table has no mappable column") && error.contains("'b'"),
        "{error}"
    );
}

const PARQUET_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";

const ORC_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";

const GLUE_SECRET_KEY: &str = "glue-signing-secret";

fn glue_creds() -> ConnectionCreds {
    ConnectionCreds {
        region: "us-east-1".into(),
        access_key: "glue-signing-access".into(),
        secret_key: GLUE_SECRET_KEY.into(),
        use_sigv4: true,
        ..ConnectionCreds::default()
    }
}

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
    let session = GlueCatalogSession::new(glue_address, storage.clone(), glue_creds())
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

async fn glue_storage(objects: &[(&str, &str)]) -> StorageBackend {
    object_endpoint(
        "bucket",
        objects
            .iter()
            .map(|(key, body)| (key.to_string(), body.to_string()))
            .collect(),
    )
    .await
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
    let storage = glue_storage(&[("glue/orders/part-0", UNREADABLE_BODY)]).await;
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
        .map(|name| (format!("{location}/{name}"), UNREADABLE_BODY))
        .into_iter()
        .chain([(format!("{location}/empty"), "")])
    };
    let objects: Vec<(String, &str)> = children("glue/orders/p=1")
        .chain(children("glue/flat"))
        .collect();
    let storage = glue_storage(
        &objects
            .iter()
            .map(|(key, body)| (key.as_str(), *body))
            .collect::<Vec<_>>(),
    )
    .await;
    let glue = GlueEndpoint::spawn(|_| {
        partitions_page(&[(&["1"], "s3://bucket/glue/orders/p=1/", PARQUET_INPUT_FORMAT)])
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
#[tokio::test]
async fn each_kept_partition_is_listed_and_carries_its_glue_values() {
    let storage = glue_storage(&[
        (
            "glue/orders/p_str=__HIVE_DEFAULT_PARTITION__/f",
            UNREADABLE_BODY,
        ),
        ("glue/orders/custom_dir/f", UNREADABLE_BODY),
        ("glue/orders/p_str=a b%2Fc/f", UNREADABLE_BODY),
        ("elsewhere/p_str=outside/f", UNREADABLE_BODY),
        ("glue/orders/p_str=path_value/f", UNREADABLE_BODY),
    ])
    .await;
    let glue = GlueEndpoint::spawn(|_| {
        partitions_page(&[
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
                "s3://bucket/elsewhere/p_str=outside",
                PARQUET_INPUT_FORMAT,
            ),
            (
                &["glue_value"],
                "s3a://bucket/glue/orders/p_str=path_value",
                PARQUET_INPUT_FORMAT,
            ),
        ])
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
        ("elsewhere/p_str=outside/f", Some("outside")),
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

#[derive(Debug)]
struct ListingGauge {
    inner: Arc<InMemory>,
    listed: Mutex<Vec<String>>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

impl ListingGauge {
    async fn over(objects: &[(&str, &[u8])]) -> Arc<Self> {
        Arc::new(Self {
            inner: in_memory_store(objects).await,
            listed: Mutex::default(),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })
    }

    fn listed(&self) -> Vec<String> {
        let mut listed = self.listed.lock().expect("listing log").clone();
        listed.sort();
        listed
    }
}

impl std::fmt::Display for ListingGauge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListingGauge({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ListingGauge {
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
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StorePath>>,
    ) -> BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        _prefix: Option<&StorePath>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        panic!("a Glue location is listed only by delimiter, never recursively")
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.listed
            .lock()
            .expect("listing log")
            .push(prefix.map(ToString::to_string).unwrap_or_default());
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::task::yield_now().await;
        let listing = self.inner.list_with_delimiter(prefix).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        listing
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

fn partition(
    values: &[(&str, Option<&str>)],
    location: &str,
    input_format: &str,
) -> CatalogPartition {
    CatalogPartition {
        values: values
            .iter()
            .map(|(key, value)| (key.to_string(), value.map(str::to_string)))
            .collect(),
        location: location.to_string(),
        format: (input_format == PARQUET_INPUT_FORMAT).then_some(TableFormat::Parquet),
        input_format: input_format.to_string(),
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

async fn planned_keys(
    store: Arc<ListingGauge>,
    partitions: Vec<CatalogPartition>,
    keep: &PartitionKeepPredicate,
) -> Result<Vec<String>, String> {
    let store: Arc<dyn ObjectStore> = store;
    plan_glue_partitions(&store, GLUE_TABLE_ROOT, partitions, keep)
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
        PARQUET_INPUT_FORMAT,
    );
    let orc = partition(
        &[("p_int", Some("9"))],
        "s3://bucket/glue/orders/p_int=9",
        ORC_INPUT_FORMAT,
    );
    let foreign = partition(
        &[("p_int", Some("5"))],
        "s3://other-bucket/glue/orders/p_int=5",
        PARQUET_INPUT_FORMAT,
    );
    let objects: &[(&str, &[u8])] = &[("glue/orders/p_int=1/f", b"rows")];
    let below_five = keep_under(
        &compare("predicate_less", column("P_INT"), number("5")),
        &[("p_int", ArrowType::Int32)],
    );

    let kept_orc = planned_keys(
        ListingGauge::over(objects).await,
        vec![parquet.clone(), orc.clone()],
        &keep_every_partition,
    )
    .await
    .expect_err("a kept ORC partition fails the query");
    let kept_foreign = planned_keys(
        ListingGauge::over(objects).await,
        vec![parquet.clone(), foreign.clone()],
        &keep_every_partition,
    )
    .await
    .expect_err("a kept partition in another bucket fails the query");
    let pruned = planned_keys(
        ListingGauge::over(objects).await,
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
    let keys: Vec<String> = rows
        .iter()
        .map(|(_, _, p_str, _)| format!("{}/f", location(p_str)))
        .collect();
    let kept_keys: Vec<String> = rows
        .iter()
        .filter(|(_, _, _, kept)| *kept)
        .map(|(_, _, p_str, _)| format!("{}/f", location(p_str)))
        .collect();
    let storage = glue_storage(
        &keys
            .iter()
            .map(|key| (key.as_str(), UNREADABLE_BODY))
            .collect::<Vec<_>>(),
    )
    .await;
    let glue = GlueEndpoint::spawn(move |_| page.clone()).await;
    let table = glue_table(
        &[
            ("id", "bigint"),
            ("p_int", "int"),
            ("p_date", "date"),
            ("p_str", "string"),
        ],
        &["p_int", "p_date", "p_str"],
    );

    let scan = resolve_glue(&table, &glue.address, &storage, Some(&filter))
        .await
        .expect("a filtered Glue table resolves");

    assert_eq!(
        scanned_files(&scan)
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>(),
        kept_keys
    );
    let requests = glue.bodies_of("GetPartitions");
    assert!(
        requests.iter().all(|body| body.get("Expression").is_none()),
        "the predicate is never sent to Glue: {requests:?}"
    );

    let gauge = ListingGauge::over(
        &keys
            .iter()
            .map(|key| (key.as_str(), b"rows".as_slice()))
            .collect::<Vec<_>>(),
    )
    .await;
    let partitions: Vec<CatalogPartition> = rows
        .iter()
        .map(|(p_int, p_date, p_str, _)| {
            partition(
                &[
                    ("p_int", Some(*p_int)),
                    ("p_date", Some(*p_date)),
                    ("p_str", Some(*p_str)),
                ],
                &format!("s3://bucket/{}", location(p_str)),
                PARQUET_INPUT_FORMAT,
            )
        })
        .collect();
    let keep = keep_under(
        &filter,
        &[
            ("p_int", ArrowType::Int32),
            ("p_date", ArrowType::Date32),
            ("p_str", ArrowType::Utf8),
        ],
    );
    planned_keys(gauge.clone(), partitions, &keep)
        .await
        .expect("the kept partitions list");
    assert_eq!(
        gauge.listed(),
        vec![location("a"), location("d")],
        "only the kept partitions' locations are listed"
    );
}

#[tokio::test]
async fn kept_partitions_are_listed_concurrently_within_the_table_store_budget() {
    let keys: Vec<String> = (0..DEFAULT_S3_MAX_CONNECTIONS * 3)
        .map(|index| format!("glue/orders/p={index}/f"))
        .collect();
    let gauge = ListingGauge::over(
        &keys
            .iter()
            .map(|key| (key.as_str(), b"rows".as_slice()))
            .collect::<Vec<_>>(),
    )
    .await;
    let partitions = (0..keys.len())
        .map(|index| {
            let value = index.to_string();
            partition(
                &[("p", Some(value.as_str()))],
                &format!("s3://bucket/glue/orders/p={index}"),
                PARQUET_INPUT_FORMAT,
            )
        })
        .collect();

    let planned = planned_keys(gauge.clone(), partitions, &keep_every_partition)
        .await
        .expect("every partition lists");

    assert_eq!(planned.len(), keys.len());
    let peak = gauge.peak.load(Ordering::SeqCst);
    assert!(
        (2..=DEFAULT_S3_MAX_CONNECTIONS).contains(&peak),
        "listings overlap within the store's connection budget, peak {peak}"
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
            GLUE_SECRET_KEY,
        ] {
            assert!(!message.contains(secret), "{message}");
        }
    }
}

/// Scenario: A catalog string column over binary file data reads as text
#[tokio::test]
async fn a_catalog_string_column_over_unannotated_byte_array_reads_as_text() {
    let storage = glue_storage(&[("glue/orders/part-0", UNREADABLE_BODY)]).await;
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

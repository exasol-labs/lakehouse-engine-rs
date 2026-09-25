use super::*;
use crate::adapter::pushdown::test_support::filter_json::{column, compare, equal, number, or};
use crate::adapter::pushdown::test_support::{
    object_endpoint, sample_storage, unauthenticated_creds,
};
use crate::scan::spec::{NestedField, NestedMembers, StorageProps};
use crate::scan::test_support::column_binding_for;
use arrow::datatypes::{DataType as ArrowType, Field, Schema};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use lakehouse_catalog::{
    CatalogColumn, CatalogTableIdent, CatalogTableType, ColumnSourceType, ConnectionCreds,
    TableFormat,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A credential request here fails with a transport error, distinct from every asserted refusal.
const UNREACHABLE_CATALOG: &str = "http://127.0.0.1:1";

const TABLE_NAME: &str = "cat.sch.sales";

const BUCKET: &str = "bucket";

const TABLE_PREFIX: &str = "unity/sales";

const TABLE_ROOT: &str = "s3://bucket/unity/sales";

/// No Parquet reader parses this, so a plan-time footer read would fail the resolution.
const UNREADABLE_BODY: &str = "not a parquet file";

const STATIC_SECRET: &str = "minioadmin";

const SENTINEL_ACCESS_KEY: &str = "AKIA-SENTINEL-ACCESS-0001";

const SENTINEL_SECRET_KEY: &str = "sentinel-secret-value-0002";

fn spark_field_json(name: &str, spark_type: Json) -> String {
    json!({"name": name, "type": spark_type, "nullable": false, "metadata": {}}).to_string()
}

fn catalog_column_with(name: &str, type_json: Option<String>) -> CatalogColumn {
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

/// Declared NOT NULL.
fn catalog_column(name: &str, spark_type: Json) -> CatalogColumn {
    catalog_column_with(name, Some(spark_field_json(name, spark_type)))
}

fn sales_table(columns: Vec<CatalogColumn>, partition_columns: &[&str]) -> CatalogTable {
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: vec!["cat".into(), "sch".into()],
            name: "sales".into(),
        },
        table_type: CatalogTableType::Table,
        storage_location: Some(TABLE_ROOT.to_string()),
        format: TableFormat::Parquet,
        vended_credential_key: Some("table-sales".to_string()),
        partition_columns: partition_columns
            .iter()
            .map(|name| name.to_string())
            .collect(),
        columns,
    }
}

fn id_table() -> CatalogTable {
    sales_table(vec![catalog_column("id", json!("long"))], &[])
}

/// `year` (`integer`) and `region` (`string`) partition the table, declared after `id` but
/// partitioned `year` first, as their `partition_index` orders them.
fn partitioned_table() -> CatalogTable {
    sales_table(
        vec![
            catalog_column("id", json!("long")),
            catalog_column("region", json!("string")),
            catalog_column("year", json!("integer")),
        ],
        &["year", "region"],
    )
}

async fn served_storage(keys: &[&str]) -> StorageBackend {
    object_endpoint(
        BUCKET,
        keys.iter()
            .map(|key| (format!("{TABLE_PREFIX}/{key}"), UNREADABLE_BODY.to_string()))
            .collect(),
    )
    .await
}

async fn resolve_with(
    table: &CatalogTable,
    storage: &StorageBackend,
    filter: Option<&Json>,
) -> Result<ResolvedScan, UdfError> {
    let creds = unauthenticated_creds();
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let connection = ConnectionStorage {
        storage,
        creds: &creds,
        allow_http: true,
    };
    UnityParquetFormatReader::new(&session, table, &connection)
        .resolve_scan(filter)
        .await
}

async fn resolve(table: &CatalogTable, keys: &[&str]) -> ResolvedScan {
    resolve_with(table, &served_storage(keys).await, None)
        .await
        .expect("the Unity Parquet table resolves")
}

async fn resolution_error(table: &CatalogTable) -> String {
    let error = resolve_with(table, &served_storage(&["part-0.parquet"]).await, None)
        .await
        .expect_err("resolution must fail, never answer a scan");
    user_message(error)
}

fn user_message(error: UdfError) -> String {
    match error {
        UdfError::User(message) => message,
        other => panic!("every refusal must be a user error, got {other:?}"),
    }
}

fn file_paths(scan: &ResolvedScan) -> Vec<&str> {
    scan.files.iter().map(|file| file.path.as_str()).collect()
}

fn values(pairs: &[(&str, Option<&str>)]) -> BTreeMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.map(str::to_string)))
        .collect()
}

fn identity_bound(name: &str, arrow_type: &str, nested: Option<NestedMembers>) -> LogicalField {
    LogicalField {
        field_id: None,
        name: name.to_string(),
        arrow_type: arrow_type.to_string(),
        nullable: true,
        initial_default: None,
        nested,
        physical_name: None,
    }
}

fn member(name: &str) -> NestedField {
    NestedField {
        field_id: None,
        name: name.to_string(),
        physical_name: None,
        nested: None,
    }
}

/// Scenario: A Unity Parquet table lists its files through the shared directory seam and reads no footer
#[tokio::test]
async fn files_are_listed_through_the_seam_and_no_footer_is_read() {
    let under_table = [
        "part-0.parquet",
        "a/part-1.parquet",
        "a/b/part-2.parquet",
        "_SUCCESS",
        "_temporary/part-3.parquet",
        ".hidden.parquet",
        "part-4.snappy",
    ];
    let mut objects: Vec<(String, String)> = under_table
        .iter()
        .map(|key| (format!("{TABLE_PREFIX}/{key}"), UNREADABLE_BODY.to_string()))
        .collect();
    objects.push((
        format!("{TABLE_PREFIX}_archive/part-9.parquet"),
        UNREADABLE_BODY.to_string(),
    ));
    let storage = object_endpoint(BUCKET, objects).await;

    let scan = resolve_with(&id_table(), &storage, None)
        .await
        .expect("a listing over unreadable bodies resolves when no footer is read");

    assert_eq!(
        file_paths(&scan),
        vec!["a/b/part-2.parquet", "a/part-1.parquet", "part-0.parquet"],
        "the seam's data-file rule selects the files below the table root, at every depth, and no \
         sibling whose name merely starts with the root's"
    );
    assert!(
        scan.files
            .iter()
            .all(|file| file.size == UNREADABLE_BODY.len() as u64
                && file.deletes.is_empty()
                && file.partition_values.is_empty()),
        "each entry carries its listed size and no delete mechanism: {:?}",
        scan.files
    );
    assert_eq!(scan.table_root, TABLE_ROOT);
    assert!(
        scan.name_mapping.is_empty(),
        "no field-id mapping is carried"
    );
    assert_eq!(
        scan.effective_storage, storage,
        "vending disabled, the files are read through the CONNECTION's static backend"
    );
}

/// Scenario: The logical schema is the catalog's declared column list
#[tokio::test]
async fn logical_schema_is_the_catalog_column_list_classified_by_the_spark_type_classifier() {
    let address = json!({"type": "struct", "fields": [
        {"name": "street", "type": "string", "nullable": true, "metadata": {}},
        {"name": "zip", "type": "integer", "nullable": true, "metadata": {}},
    ]});
    let table = sales_table(
        vec![
            catalog_column("id", json!("long")),
            catalog_column("address", address),
            catalog_column("payload", json!("binary")),
            catalog_column_with(
                "CustomerId",
                Some(spark_field_json("customerid", json!("string"))),
            ),
        ],
        &[],
    );

    let scan = resolve(&table, &["part-0.parquet"]).await;

    assert_eq!(
        scan.logical_schema,
        vec![
            identity_bound("id", "int64", None),
            identity_bound(
                "address",
                "utf8",
                Some(NestedMembers::Struct {
                    fields: vec![member("street"), member("zip")],
                }),
            ),
            identity_bound("CustomerId", "utf8", None),
        ],
        "one identity-bound, nullable field per mappable catalog column, in catalog order and \
         named as the catalog declares it"
    );
    assert_eq!(
        scan.refused_columns
            .iter()
            .map(|refused| refused.column_name.as_str())
            .collect::<Vec<_>>(),
        vec!["payload"],
        "the binary column is refused by name"
    );
    assert!(
        scan.refused_columns[0].reason.contains("binary"),
        "the refusal states its type: {:?}",
        scan.refused_columns
    );
}

/// Scenario: A Unity Parquet column with no usable type descriptor is refused, and nullability always follows the file
#[tokio::test]
async fn a_column_without_a_readable_type_json_is_refused_by_name() {
    let table = sales_table(
        vec![
            catalog_column("id", json!("long")),
            catalog_column_with("note", None),
            catalog_column_with(
                "broken",
                Some(json!({"name": "broken", "type": "long"}).to_string()),
            ),
        ],
        &[],
    );

    let scan = resolve(&table, &["part-0.parquet"]).await;

    assert_eq!(
        scan.logical_schema,
        vec![identity_bound("id", "int64", None)]
    );
    let refused: Vec<(&str, &str)> = scan
        .refused_columns
        .iter()
        .map(|refused| (refused.column_name.as_str(), refused.reason.as_str()))
        .collect();
    assert_eq!(
        refused.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
        vec!["note", "broken"]
    );
    assert!(
        refused
            .iter()
            .all(|(_, reason)| reason.contains("type_json")),
        "each reason names the missing or unreadable descriptor: {refused:?}"
    );
}

/// Scenario: A table whose every column is refused is refused as a whole, exactly as a Delta table is
#[tokio::test]
async fn a_table_whose_every_column_is_refused_is_refused_as_a_whole() {
    let table = sales_table(
        vec![
            catalog_column("payload", json!("binary")),
            catalog_column_with("note", None),
        ],
        &[],
    );

    let message = resolution_error(&table).await;

    assert!(
        message.starts_with("Unity Parquet table has no mappable column; every column is refused"),
        "message was: {message}"
    );
    assert!(
        message.contains("'payload'") && message.contains("'note'"),
        "message was: {message}"
    );
}

/// Scenario: The logical schema is the catalog's declared column list
#[tokio::test]
async fn catalog_columns_equal_ignoring_letter_case_fail_the_plan() {
    let table = sales_table(
        vec![
            catalog_column("id", json!("long")),
            catalog_column("ID", json!("long")),
        ],
        &[],
    );

    let message = resolution_error(&table).await;

    assert!(
        message.contains(TABLE_NAME) && message.contains("do not form one Spark schema"),
        "columns equal ignoring letter case must refuse the table by name: {message}"
    );
}

/// Scenario: A data-file column whose name differs only in letter case binds to its catalog column
#[tokio::test]
async fn a_case_drifted_file_column_binds_to_its_catalog_column() {
    let table = sales_table(vec![catalog_column("CustomerId", json!("long"))], &[]);
    let scan = resolve(&table, &["part-0.parquet"]).await;
    let (logical, factory) = column_binding_for(&scan.logical_schema, &scan.table_root);
    let physical = Arc::new(Schema::new(vec![
        Field::new("region", ArrowType::Utf8, true),
        Field::new("customerid", ArrowType::Int64, true),
    ]));

    let adapter = factory
        .create(logical, physical)
        .expect("the drifted file binds");
    let bound = adapter
        .rewrite(Arc::new(Column::new("CustomerId", 0)))
        .expect("the catalog column rewrites against the drifted file");

    let column = bound
        .downcast_ref::<Column>()
        .expect("an identical type binds with no cast");
    assert_eq!(
        (column.name(), column.index()),
        ("customerid", 1),
        "the catalog column binds the file column equal to it under the case fold"
    );
}

/// Scenario: The scan applies its shared cast and admission rules, unchanged, to a Unity Parquet column
#[tokio::test]
async fn a_file_type_outside_the_widening_set_is_refused_naming_the_column() {
    let table = sales_table(
        vec![
            catalog_column("id", json!("long")),
            catalog_column("amount", json!("integer")),
        ],
        &[],
    );
    let scan = resolve(&table, &["part-0.parquet"]).await;
    let (logical, factory) = column_binding_for(&scan.logical_schema, &scan.table_root);
    let physical = Arc::new(Schema::new(vec![
        Field::new("id", ArrowType::Int32, true),
        Field::new("amount", ArrowType::Float64, true),
    ]));

    let adapter = factory
        .create(logical, physical)
        .expect("a refused column fails only a rewrite that references it");

    let widened = adapter
        .rewrite(Arc::new(Column::new("id", 0)))
        .expect("a narrower file type in the widening set is admitted");
    assert!(
        widened.downcast_ref::<CastExpr>().is_some(),
        "the narrower int column is cast to the declared long: {widened}"
    );
    let message = adapter
        .rewrite(Arc::new(Column::new("amount", 1)) as Arc<dyn PhysicalExpr>)
        .expect_err("a double file column under an int declaration is refused")
        .to_string();
    for fragment in ["'amount'", TABLE_ROOT, "Float64", "Int32"] {
        assert!(
            message.contains(fragment),
            "'{fragment}' missing from: {message}"
        );
    }
}

/// Scenario: Partition columns come from the catalog and partition values from the file paths
#[tokio::test]
async fn partition_columns_come_from_partition_index_and_values_from_paths() {
    let scan = resolve(
        &partitioned_table(),
        &[
            "year=2024/region=eu/a.parquet",
            "year=2024/region=us/b.parquet",
            "year=2025/region=__HIVE_DEFAULT_PARTITION__/c.parquet",
            "d.parquet",
            "other=x/e.parquet",
            "YEAR=2026/Region=apac/f.parquet",
        ],
    )
    .await;

    assert_eq!(
        scan.partition_columns,
        vec!["year".to_string(), "region".to_string()],
        "the catalog's partition order, not its column order"
    );
    assert_eq!(
        scan.logical_schema,
        vec![
            identity_bound("id", "int64", None),
            identity_bound("region", "utf8", None),
            identity_bound("year", "int32", None),
        ],
        "each partition column keeps its declared type"
    );
    let listed: Vec<(&str, BTreeMap<String, Option<String>>)> = scan
        .files
        .iter()
        .map(|file| (file.path.as_str(), file.partition_values.clone()))
        .collect();
    assert_eq!(
        listed,
        vec![
            (
                "YEAR=2026/Region=apac/f.parquet",
                values(&[("year", Some("2026")), ("region", Some("apac"))]),
            ),
            ("d.parquet", values(&[("year", None), ("region", None)]),),
            (
                "other=x/e.parquet",
                values(&[("year", None), ("region", None)]),
            ),
            (
                "year=2024/region=eu/a.parquet",
                values(&[("year", Some("2024")), ("region", Some("eu"))]),
            ),
            (
                "year=2024/region=us/b.parquet",
                values(&[("year", Some("2024")), ("region", Some("us"))]),
            ),
            (
                "year=2025/region=__HIVE_DEFAULT_PARTITION__/c.parquet",
                values(&[("year", Some("2025")), ("region", None)]),
            ),
        ],
        "values come from each file's own segments, fold-matched and keyed by the catalog \
         spelling; a default, absent, or undeclared segment reads NULL or nothing"
    );
}

/// Scenario: A partition column the type classification refuses fails the plan
#[tokio::test]
async fn a_refused_partition_column_fails_the_plan() {
    let table = sales_table(
        vec![
            catalog_column("id", json!("long")),
            catalog_column("shard", json!("binary")),
        ],
        &["shard"],
    );

    let message = resolution_error(&table).await;

    assert!(
        message.contains("'shard'") && message.contains(TABLE_NAME),
        "the refusal names the partition column and its table: {message}"
    );
    assert!(
        message.contains("partition column"),
        "the refusal states why a column refusal fails the plan: {message}"
    );
}

/// Scenario: A predicate on a string partition column prunes files and no other predicate does
#[tokio::test]
async fn only_string_partition_columns_prune_files() {
    let storage = served_storage(&[
        "year=2024/region=eu/a.parquet",
        "year=2024/region=us/b.parquet",
        "year=2025/region=eu/c.parquet",
    ])
    .await;
    let every_file = vec![
        "year=2024/region=eu/a.parquet",
        "year=2024/region=us/b.parquet",
        "year=2025/region=eu/c.parquet",
    ];
    let cases = [
        (
            "a string partition column",
            equal("REGION", "eu"),
            vec![
                "year=2024/region=eu/a.parquet",
                "year=2025/region=eu/c.parquet",
            ],
        ),
        (
            "an integer partition column against a string literal",
            equal("YEAR", "2024"),
            every_file.clone(),
        ),
        (
            "an integer partition column against a number",
            compare("predicate_equal", column("YEAR"), number("2024")),
            every_file.clone(),
        ),
        (
            "an OR whose other branch reads a data column",
            or(vec![
                equal("REGION", "us"),
                compare("predicate_less", number("10"), column("AMOUNT")),
            ]),
            every_file.clone(),
        ),
    ];

    for (shape, filter, expected) in cases {
        let scan = resolve_with(&partitioned_table(), &storage, Some(&filter))
            .await
            .expect("a filtered request resolves");
        assert_eq!(file_paths(&scan), expected, "{shape}");
    }
}

async fn storage_refusal(table: &CatalogTable, use_vended_credentials: bool) -> String {
    let creds = ConnectionCreds {
        use_vended_credentials,
        ..unauthenticated_creds()
    };
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let storage = sample_storage();
    let connection = ConnectionStorage {
        storage: &storage,
        creds: &creds,
        allow_http: true,
    };
    let error = UnityParquetFormatReader::new(&session, table, &connection)
        .resolve_scan(None)
        .await
        .expect_err("resolution must fail, never answer a scan");
    user_message(error)
}

/// Scenario: Storage is resolved through the table's own catalog exactly as for a Delta table
#[tokio::test]
async fn vending_without_a_vending_key_errors_and_never_falls_back_to_static() {
    for absent_key in [None, Some("")] {
        let table = CatalogTable {
            vended_credential_key: absent_key.map(str::to_string),
            ..id_table()
        };

        let message = storage_refusal(&table, true).await;

        assert!(
            message.contains(TABLE_NAME) && message.contains("vend"),
            "the refusal names the table whose vending key is missing: {message}"
        );
        assert!(
            !message.contains("failed to list"),
            "a listing proves the static credential was used as a fallback: {message}"
        );
        assert!(
            !message.contains(STATIC_SECRET),
            "no error may carry a credential value: {message}"
        );
    }
}

/// Scenario: Storage is resolved through the table's own catalog exactly as for a Delta table
#[tokio::test]
async fn a_failed_listing_reports_no_static_credential_value() {
    let storage = StorageBackend::S3(StorageProps {
        endpoint: "http://127.0.0.1:1".into(),
        region: "us-east-1".into(),
        access_key: SENTINEL_ACCESS_KEY.into(),
        secret_key: SENTINEL_SECRET_KEY.into(),
        allow_http: true,
        path_style: true,
        ..Default::default()
    });

    let message = user_message(
        resolve_with(&id_table(), &storage, None)
            .await
            .expect_err("a listing against a closed port must fail, never answer a scan"),
    );

    assert!(
        message.contains("failed to list") && message.contains(TABLE_PREFIX),
        "the refusal must name the prefix it could not list: {message}"
    );
    for sentinel in [SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY] {
        assert!(
            !message.contains(sentinel),
            "no error may carry a credential it listed through: {message}"
        );
    }
}

/// Scenario: An empty storage location is refused with one text under both credential modes
#[tokio::test]
async fn empty_storage_location_errors_identically_under_both_credential_modes() {
    let mut messages = Vec::new();
    for location in [None, Some(""), Some("   ")] {
        for vending_key in [None, Some("table-sales")] {
            for use_vended_credentials in [false, true] {
                let table = CatalogTable {
                    storage_location: location.map(str::to_string),
                    vended_credential_key: vending_key.map(str::to_string),
                    ..id_table()
                };
                messages.push(storage_refusal(&table, use_vended_credentials).await);
            }
        }
    }

    let expected = &messages[0];
    assert!(
        messages.iter().all(|message| message == expected),
        "one text for every credential mode and vending key: {messages:?}"
    );
    assert!(
        expected.contains(TABLE_NAME) && expected.contains("EMPTY storage location"),
        "the refusal names the table and the empty location: {expected}"
    );
}

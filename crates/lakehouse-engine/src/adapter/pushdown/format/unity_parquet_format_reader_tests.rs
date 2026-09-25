use super::*;
use crate::adapter::pushdown::test_support::filter_json::{column, compare, equal, number, or};
use crate::adapter::pushdown::test_support::{
    SENTINEL_ACCESS_KEY, SENTINEL_SECRET_KEY, closed_port_storage, object_endpoint, sample_storage,
    unauthenticated_creds,
};
use crate::adapter::tests::parquet_fixture::values;
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
use std::sync::Arc;

/// A credential request here fails with a transport error, distinct from every asserted refusal.
const UNREACHABLE_CATALOG: &str = "http://127.0.0.1:1";

const TABLE_NAME: &str = "cat.sch.sales";

const TABLE_PREFIX: &str = "unity/sales";

const TABLE_ROOT: &str = "s3://bucket/unity/sales";

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
    UnityParquetFormatReader::new(&session, table, &connection)
        .resolve_scan(filter)
        .await
        .map_err(|error| match error {
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

/// Scenario: The logical schema is the catalog's declared column list
/// Scenario: A Unity Parquet column with no usable type descriptor is refused, and nullability always follows the file
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

/// Scenario: A table whose every column is refused is refused as a whole, exactly as a Delta table is
/// Scenario: A partition column the type classification refuses fails the plan
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

/// Scenario: A data-file column whose name differs only in letter case binds to its catalog column
/// Scenario: The scan applies its shared cast and admission rules, unchanged, to a Unity Parquet column
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

/// Scenario: A predicate on a string partition column prunes files and no other predicate does
#[tokio::test]
async fn only_string_partition_columns_prune_files() {
    let every_file = [
        "year=2024/region=eu/a.parquet",
        "year=2024/region=us/b.parquet",
        "year=2025/region=eu/c.parquet",
    ];
    let storage = served_storage(&every_file, &[]).await;
    let cases = [
        (equal("REGION", "eu"), vec![every_file[0], every_file[2]]),
        (equal("YEAR", "2024"), every_file.to_vec()),
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

/// Scenario: Storage is resolved through the table's own catalog exactly as for a Delta table
/// Scenario: An empty storage location is refused with one text under both credential modes
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

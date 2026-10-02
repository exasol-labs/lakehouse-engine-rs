use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use iceberg::spec::{PrimitiveType, TableMetadata, Type};

use super::*;
use crate::glue::routing::PARQUET_INPUT_FORMAT;
use crate::glue::source::{GluePartition, SourceFuture};
use crate::{StorageProps, TableFormat};

use crate::test_support::ORC_INPUT_FORMAT;
const ACCESS_KEY: &str = "AKIDGLUECONNECTION01";
const SECRET_KEY: &str = "GLUE_SECRET_KEY_SENTINEL";
const SESSION_TOKEN: &str = "GLUE_SESSION_TOKEN_SENTINEL";
const STORAGE_SECRET_KEY: &str = "STORAGE_SECRET_KEY_SENTINEL";
const DATABASE: &str = "sales";

/// An in-memory Glue database. `failure`, when set, answers every Glue call.
#[derive(Default)]
struct FakeGlue {
    tables: Vec<GlueTable>,
    partitions: Vec<GluePartition>,
    metadata: HashMap<String, TableMetadata>,
    failure: Option<GlueFailure>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeGlue {
    fn record(&self, call: String) -> Result<(), GlueFailure> {
        self.calls.lock().expect("calls").push(call);
        self.failure.clone().map_or(Ok(()), Err)
    }
}

impl GlueSource for FakeGlue {
    fn tables<'a>(
        &'a self,
        database: &'a str,
    ) -> SourceFuture<'a, Result<Vec<GlueTable>, GlueFailure>> {
        Box::pin(async move {
            self.record(format!("tables {database}"))?;
            Ok(self.tables.clone())
        })
    }

    fn table<'a>(
        &'a self,
        database: &'a str,
        name: &'a str,
    ) -> SourceFuture<'a, Result<Option<GlueTable>, GlueFailure>> {
        Box::pin(async move {
            self.record(format!("table {database}.{name}"))?;
            Ok(self.tables.iter().find(|table| table.name == name).cloned())
        })
    }

    fn partitions<'a>(
        &'a self,
        database: &'a str,
        table: &'a str,
    ) -> SourceFuture<'a, Result<Vec<GluePartition>, GlueFailure>> {
        Box::pin(async move {
            self.record(format!("partitions {database}.{table}"))?;
            Ok(self.partitions.clone())
        })
    }

    fn iceberg_metadata<'a>(
        &'a self,
        location: &'a str,
        table_name: &'a str,
    ) -> SourceFuture<'a, Result<TableMetadata, UdfError>> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("calls")
                .push(format!("metadata {location}"));
            self.metadata.get(location).cloned().ok_or_else(|| {
                UdfError::User(format!(
                    "no object at {location} of table {table_name} for key {STORAGE_SECRET_KEY}"
                ))
            })
        })
    }
}

fn session(fake: FakeGlue) -> GlueCatalogSession {
    GlueCatalogSession {
        source: Box::new(fake),
        secrets: [ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, STORAGE_SECRET_KEY]
            .map(str::to_string)
            .to_vec(),
    }
}

fn ident(name: &str) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: vec![DATABASE.to_string()],
        name: name.to_string(),
    }
}

fn column(name: &str, hive_type: &str) -> GlueColumn {
    GlueColumn {
        name: name.to_string(),
        hive_type: hive_type.to_string(),
    }
}

fn hive_table(name: &str, input_format: &str) -> GlueTable {
    GlueTable {
        name: name.to_string(),
        table_type: Some("EXTERNAL_TABLE".to_string()),
        input_format: Some(input_format.to_string()),
        location: Some(format!("s3://bucket/{name}/")),
        columns: vec![column("id", "bigint")],
        ..GlueTable::default()
    }
}

fn metadata_location(name: &str) -> String {
    format!("s3://bucket/{name}/metadata/00001.metadata.json")
}

fn iceberg_table(name: &str, table_type: &str) -> GlueTable {
    GlueTable {
        name: name.to_string(),
        table_type: Some("EXTERNAL_TABLE".to_string()),
        parameters: HashMap::from([
            ("table_type".to_string(), table_type.to_string()),
            ("metadata_location".to_string(), metadata_location(name)),
        ]),
        location: Some(format!("s3://bucket/{name}/")),
        columns: vec![column("glue_only", "string")],
        ..GlueTable::default()
    }
}

fn iceberg_metadata(name: &str) -> TableMetadata {
    serde_json::from_value(serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000001",
        "location": format!("s3://bucket/{name}"),
        "last-sequence-number": 0,
        "last-updated-ms": 0,
        "last-column-id": 2,
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": [
            {"id": 1, "name": "order_id", "required": true, "type": "long"},
            {"id": 2, "name": "placed_on", "required": false, "type": "date"}
        ]}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0
    }))
    .expect("valid metadata")
}

fn glue_columns(pairs: &[(&str, &str)]) -> Vec<CatalogColumn> {
    pairs
        .iter()
        .map(|(name, hive_type)| CatalogColumn {
            name: name.to_string(),
            source_type: ColumnSourceType::Glue {
                hive_type: hive_type.to_string(),
            },
        })
        .collect()
}

fn user_message(error: UdfError) -> String {
    match error {
        UdfError::User(message) => message,
        other => panic!("expected a user error, got {other:?}"),
    }
}

fn assert_no_credential(text: &str) {
    for secret in [ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, STORAGE_SECRET_KEY] {
        assert!(
            !text.contains(secret),
            "the error must not contain a credential value: {text}"
        );
    }
}

async fn listing_error(failure: GlueFailure) -> String {
    let fake = FakeGlue {
        failure: Some(failure),
        ..FakeGlue::default()
    };
    user_message(
        session(fake)
            .list_tables(&[DATABASE.to_string()])
            .await
            .expect_err("the Glue call fails"),
    )
}

/// Scenario: The client routes a table by its declared table type before its storage descriptor
#[tokio::test]
async fn the_listing_admits_each_routed_table_with_its_format_and_records_a_skip() {
    let fake = FakeGlue {
        tables: vec![
            iceberg_table("orders", "iceberg"),
            hive_table("events", PARQUET_INPUT_FORMAT),
            hive_table("events_orc", ORC_INPUT_FORMAT),
        ],
        metadata: HashMap::from([(metadata_location("orders"), iceberg_metadata("orders"))]),
        ..FakeGlue::default()
    };

    let listing = session(fake)
        .list_tables(&[DATABASE.to_string()])
        .await
        .expect("the listing succeeds");

    let admitted: Vec<(&str, TableFormat)> = listing
        .tables
        .iter()
        .map(|table| (table.ident.name.as_str(), table.format))
        .collect();
    assert_eq!(
        admitted,
        [
            ("orders", TableFormat::Iceberg),
            ("events", TableFormat::Parquet)
        ]
    );
    assert_eq!(
        listing.skipped,
        [SkippedTable {
            ident: ident("events_orc"),
            reason: SkipReason::NotPlannableGlueTable {
                detail: format!("InputFormat={ORC_INPUT_FORMAT}"),
            },
        }]
    );
}

/// Scenario: An Iceberg table takes its columns from its current metadata file
#[tokio::test]
async fn an_iceberg_table_lists_its_metadata_columns_and_plans_without_reading_the_file() {
    let location = metadata_location("orders");
    let calls = Arc::default();
    let fake = FakeGlue {
        tables: vec![iceberg_table("orders", "ICEBERG")],
        metadata: HashMap::from([(location.clone(), iceberg_metadata("orders"))]),
        calls: Arc::clone(&calls),
        ..FakeGlue::default()
    };
    let session = session(fake);

    let listed = session
        .load_table(&ident("orders"))
        .await
        .expect("the listing load reads the metadata file");
    let planned = session
        .load_table_for_planning(&ident("orders"))
        .await
        .expect("the planning load succeeds");

    let columns: Vec<(&str, &ColumnSourceType)> = listed
        .columns
        .iter()
        .map(|column| (column.name.as_str(), &column.source_type))
        .collect();
    assert_eq!(
        columns,
        [
            (
                "order_id",
                &ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Long))
            ),
            (
                "placed_on",
                &ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Date))
            ),
        ],
        "the metadata file's schema, never the Glue column"
    );
    assert_eq!(listed.metadata_location.as_deref(), Some(location.as_str()));
    assert_eq!(
        planned,
        CatalogTable {
            ident: ident("orders"),
            table_type: CatalogTableType::Table,
            storage_location: Some("s3://bucket/orders".to_string()),
            format: TableFormat::Iceberg,
            vended_credential_key: None,
            partition_columns: Vec::new(),
            columns: Vec::new(),
            metadata_location: Some(location.clone()),
        }
    );
    assert_eq!(
        *calls.lock().expect("calls"),
        [
            "table sales.orders".to_string(),
            format!("metadata {location}"),
            "table sales.orders".to_string(),
        ],
        "only the listing load reads the metadata file"
    );
}

#[tokio::test]
async fn an_unreadable_metadata_file_fails_the_listing_naming_the_table() {
    let fake = FakeGlue {
        tables: vec![iceberg_table("orders", "ICEBERG")],
        ..FakeGlue::default()
    };

    let message = user_message(
        session(fake)
            .list_tables(&[DATABASE.to_string()])
            .await
            .expect_err("the metadata file is missing"),
    );

    assert!(message.contains("sales.orders"), "{message}");
    assert!(message.contains(&metadata_location("orders")), "{message}");
    assert_no_credential(&message);
}

/// Scenario: A Parquet table declares its Glue columns and partition keys
#[tokio::test]
async fn a_parquet_table_declares_storage_columns_then_partition_keys() {
    let events = GlueTable {
        location: Some("s3://bucket/warehouse/events/".to_string()),
        columns: vec![
            column("id", "bigint"),
            column("payload", "struct<x:int,y:string>"),
        ],
        partition_keys: vec![
            column("p_int", "int"),
            column("p_date", "date"),
            column("p_str", "string"),
        ],
        ..hive_table("events", PARQUET_INPUT_FORMAT)
    };
    let session = session(FakeGlue {
        tables: vec![events],
        ..FakeGlue::default()
    });

    let listing = session
        .list_tables(&[DATABASE.to_string()])
        .await
        .expect("the listing succeeds");
    let planned = session
        .load_table_for_planning(&ident("events"))
        .await
        .expect("a Parquet table plans");

    let expected = CatalogTable {
        ident: ident("events"),
        table_type: CatalogTableType::Table,
        storage_location: Some("s3://bucket/warehouse/events".to_string()),
        format: TableFormat::Parquet,
        vended_credential_key: None,
        partition_columns: vec![
            "p_int".to_string(),
            "p_date".to_string(),
            "p_str".to_string(),
        ],
        columns: glue_columns(&[
            ("id", "bigint"),
            ("payload", "struct<x:int,y:string>"),
            ("p_int", "int"),
            ("p_date", "date"),
            ("p_str", "string"),
        ]),
        metadata_location: None,
    };
    assert_eq!(planned, expected);
    assert_eq!(listing.tables, [expected]);
}

#[tokio::test]
async fn a_partition_that_does_not_match_the_keys_fails_naming_its_location() {
    let fake = FakeGlue {
        partitions: vec![GluePartition {
            values: vec!["1".to_string()],
            location: Some(format!("s3://bucket/{SECRET_KEY}/p_int=1/")),
            input_format: None,
        }],
        ..FakeGlue::default()
    };

    let message = user_message(
        session(fake)
            .partitions(&parquet_table(&["p_int", "p_str"]))
            .await
            .expect_err("one value for two keys fails"),
    );

    assert!(message.contains("p_int=1"), "{message}");
    assert_no_credential(&message);
}

#[tokio::test]
async fn an_unpartitioned_table_reads_as_one_parquet_partition_at_its_location() {
    let calls = Arc::default();
    let session = session(FakeGlue {
        calls: Arc::clone(&calls),
        ..FakeGlue::default()
    });

    let partitions = session
        .partitions(&parquet_table(&[]))
        .await
        .expect("an unpartitioned table needs no GetPartitions");

    assert_eq!(
        partitions,
        [CatalogPartition {
            values: BTreeMap::new(),
            location: "s3://bucket/events".to_string(),
            format: PartitionFormat::Parquet,
        }]
    );
    assert!(
        calls.lock().expect("calls").is_empty(),
        "no Glue call is issued"
    );
}

fn parquet_table(partition_columns: &[&str]) -> CatalogTable {
    CatalogTable {
        ident: ident("events"),
        table_type: CatalogTableType::Table,
        storage_location: Some("s3://bucket/events".to_string()),
        format: TableFormat::Parquet,
        vended_credential_key: None,
        partition_columns: partition_columns
            .iter()
            .map(|column| column.to_string())
            .collect(),
        columns: Vec::new(),
        metadata_location: None,
    }
}

/// Scenario: A Glue NAMESPACE names exactly one database
#[tokio::test]
async fn a_namespace_naming_more_or_less_than_one_database_fails_before_any_glue_call() {
    for namespace in [
        vec!["sales".to_string(), "eu".to_string()],
        Vec::new(),
        vec![String::new()],
    ] {
        let calls = Arc::default();
        let session = session(FakeGlue {
            calls: Arc::clone(&calls),
            ..FakeGlue::default()
        });

        let message = user_message(
            session
                .list_tables(&namespace)
                .await
                .expect_err("only one database segment is accepted"),
        );

        assert!(message.contains("exactly one database"), "{message}");
        assert!(calls.lock().expect("calls").is_empty(), "{namespace:?}");
    }
}

/// Scenario: A failed Glue call names the operation, the subject, and the cause
#[tokio::test]
async fn a_failed_glue_call_names_the_operation_the_subject_and_the_cause() {
    let echoed = format!("request by {ACCESS_KEY} with {SECRET_KEY} and {SESSION_TOKEN} denied");
    let cases = [
        (
            GlueFailure::NotFound {
                message: "Database sales not found.".to_string(),
            },
            "does not exist",
        ),
        (
            GlueFailure::Service {
                code: "AccessDeniedException".to_string(),
                message: echoed,
            },
            "AccessDeniedException",
        ),
        (
            GlueFailure::Uncoded {
                status: 503,
                message: "Service Unavailable".to_string(),
            },
            "HTTP 503",
        ),
        (GlueFailure::TimedOut { seconds: 30 }, "within 30 seconds"),
        (
            GlueFailure::Request("dispatch failure".to_string()),
            "dispatch failure",
        ),
    ];

    for (failure, cause) in cases {
        let message = listing_error(failure).await;

        assert!(message.contains("GetTables"), "{message}");
        assert!(message.contains("database 'sales'"), "{message}");
        assert!(message.contains(cause), "{message}");
        assert_no_credential(&message);
    }
}

/// Scenario: A Glue table resolves from its recorded identifier and fails loud when it is no longer plannable
#[tokio::test]
async fn a_dropped_or_no_longer_plannable_table_fails_the_planning_load_naming_it() {
    let dropped = session(FakeGlue {
        failure: Some(GlueFailure::NotFound {
            message: "Table orders not found.".to_string(),
        }),
        ..FakeGlue::default()
    });
    let reregistered = session(FakeGlue {
        tables: vec![hive_table("orders", ORC_INPUT_FORMAT)],
        ..FakeGlue::default()
    });

    let dropped = user_message(
        dropped
            .load_table_for_planning(&ident("orders"))
            .await
            .expect_err("a dropped table cannot be planned"),
    );
    let reregistered = user_message(
        reregistered
            .load_table_for_planning(&ident("orders"))
            .await
            .expect_err("an ORC table cannot be planned"),
    );

    assert!(dropped.contains("GetTable"), "{dropped}");
    assert!(dropped.contains("sales.orders"), "{dropped}");
    assert!(dropped.contains("does not exist"), "{dropped}");
    assert!(reregistered.contains("sales.orders"), "{reregistered}");
    assert!(
        reregistered.contains(&format!("InputFormat={ORC_INPUT_FORMAT}")),
        "{reregistered}"
    );
}

#[test]
fn a_session_without_a_signing_region_is_refused() {
    let creds = ConnectionCreds {
        access_key: ACCESS_KEY.to_string(),
        secret_key: SECRET_KEY.to_string(),
        use_sigv4: true,
        ..ConnectionCreds::default()
    };

    let result = GlueCatalogSession::new(
        "http://127.0.0.1:1",
        StorageBackend::S3(StorageProps::default()),
        creds,
    );

    assert!(result.is_err(), "no signing region, no session");
}

#[test]
fn a_recorded_glue_identifier_splits_at_the_first_dot_and_refuses_an_empty_part() {
    let ident = parse_glue_table_ident("sales.orders").expect("database.table");
    assert_eq!(ident.namespace, vec!["sales".to_string()]);
    assert_eq!(ident.name, "orders");

    let dotted =
        parse_glue_table_ident("sales.orders.v2").expect("database.table with a dotted table");
    assert_eq!(dotted.namespace, vec!["sales".to_string()]);
    assert_eq!(dotted.name, "orders.v2");

    for malformed in ["orders", "sales.", ".orders", " .orders", ""] {
        let err = parse_glue_table_ident(malformed)
            .expect_err("an identifier missing its database or table must be refused");
        assert!(
            err.to_string().contains("database.table"),
            "{malformed:?}: {err}"
        );
    }
}

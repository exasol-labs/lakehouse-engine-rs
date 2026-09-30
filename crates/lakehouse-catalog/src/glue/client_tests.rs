use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use aws_sdk_glue::config::timeout::TimeoutConfig;
use aws_sdk_glue::primitives::{DateTime, DateTimeFormat};
use iceberg::spec::{PrimitiveType, Type};
use serde_json::{Value, json};

use super::*;
use crate::glue::mock_glue::{self, MockGlue, MockResponse, RecordedRequest};
use crate::glue::routing::PARQUET_INPUT_FORMAT;
use crate::{
    CatalogColumn, CatalogTableType, ColumnSourceType, SkipReason, SkippedTable, StorageProps,
    TableFormat,
};

const ORC_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
const ACCESS_KEY: &str = "AKIDGLUECONNECTION01";
const SECRET_KEY: &str = "GLUE_SECRET_KEY_SENTINEL";
const SESSION_TOKEN: &str = "GLUE_SESSION_TOKEN_SENTINEL";
const STORAGE_SECRET_KEY: &str = "STORAGE_SECRET_KEY_SENTINEL";
const REGION: &str = "eu-west-1";
const DATABASE: &str = "sales";

fn glue_creds(warehouse: &str) -> ConnectionCreds {
    ConnectionCreds {
        warehouse: warehouse.to_string(),
        region: REGION.to_string(),
        access_key: ACCESS_KEY.to_string(),
        secret_key: SECRET_KEY.to_string(),
        session_token: Some(SESSION_TOKEN.to_string()),
        use_sigv4: true,
        ..ConnectionCreds::default()
    }
}

fn storage(mock: &MockGlue) -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: mock.address.clone(),
        region: "us-east-1".to_string(),
        access_key: "STORAGE_ACCESS_KEY".to_string(),
        secret_key: STORAGE_SECRET_KEY.to_string(),
        path_style: true,
        ..StorageProps::default()
    })
}

fn session(mock: &MockGlue) -> GlueCatalogSession {
    session_with_warehouse(mock, "")
}

fn session_with_warehouse(mock: &MockGlue, warehouse: &str) -> GlueCatalogSession {
    GlueCatalogSession::new(&mock.address, storage(mock), glue_creds(warehouse))
        .expect("a session with a signing region")
}

/// Keeps the production retry and timeout policy but backs off in milliseconds, so a
/// test that exhausts every attempt does not sleep for seconds.
fn fast_retry_session(mock: &MockGlue) -> GlueCatalogSession {
    let creds = glue_creds("");
    let config = glue_config(&mock.address, &creds, REGION.to_string())
        .retry_config(glue_retry_config().with_initial_backoff(Duration::from_millis(1)))
        .build();
    GlueCatalogSession::from_config(config, storage(mock), &creds)
}

fn ident(name: &str) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: vec![DATABASE.to_string()],
        name: name.to_string(),
    }
}

fn database() -> Vec<String> {
    vec![DATABASE.to_string()]
}

fn columns(pairs: &[(&str, &str)]) -> Value {
    Value::Array(
        pairs
            .iter()
            .map(|(name, hive_type)| json!({ "Name": name, "Type": hive_type }))
            .collect(),
    )
}

fn hive_table(name: &str, input_format: &str) -> Value {
    json!({
        "Name": name,
        "DatabaseName": DATABASE,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": columns(&[("id", "bigint")]),
            "Location": format!("s3://bucket/{name}/"),
            "InputFormat": input_format,
        },
    })
}

fn metadata_path(name: &str) -> String {
    format!("/bucket/{name}/metadata/00001.metadata.json")
}

fn metadata_location(name: &str) -> String {
    format!("s3:/{}", metadata_path(name))
}

fn iceberg_table(name: &str, table_type: &str) -> Value {
    json!({
        "Name": name,
        "DatabaseName": DATABASE,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "table_type": table_type,
            "metadata_location": metadata_location(name),
        },
        "StorageDescriptor": {
            "Columns": [
                { "Name": "glue_only", "Type": "string", "Parameters": { "iceberg.field.id": "7" } }
            ],
            "Location": format!("s3://bucket/{name}/"),
        },
    })
}

fn iceberg_metadata(name: &str) -> Vec<u8> {
    json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000001",
        "location": format!("s3://bucket/{name}"),
        "last-sequence-number": 0,
        "last-updated-ms": 0,
        "last-column-id": 2,
        "current-schema-id": 0,
        "schemas": [{
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "Name", "required": false, "type": "string"}
            ]
        }],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0
    })
    .to_string()
    .into_bytes()
}

fn iceberg_columns() -> Vec<CatalogColumn> {
    vec![
        CatalogColumn {
            name: "id".to_string(),
            source_type: ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Long)),
        },
        CatalogColumn {
            name: "Name".to_string(),
            source_type: ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::String)),
        },
    ]
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

fn tables_page(tables: Vec<Value>) -> MockResponse {
    MockResponse::json(json!({ "TableList": tables }))
}

fn catalog_of(tables: Vec<Value>) -> impl Fn(&RecordedRequest, usize) -> MockResponse {
    move |request, _| match request.operation() {
        Some("GetTables") => tables_page(tables.clone()),
        Some("GetTable") => {
            let name = request.json()["Name"].clone();
            match tables.iter().find(|table| table["Name"] == name) {
                Some(table) => MockResponse::json(json!({ "Table": table })),
                None => MockResponse::error(
                    400,
                    "EntityNotFoundException",
                    &format!("Table {name} not found."),
                ),
            }
        }
        None if request.method == "GET" => {
            let name = request
                .path
                .trim_start_matches("/bucket/")
                .split('/')
                .next()
                .unwrap_or("");
            MockResponse::object(iceberg_metadata(name))
        }
        other => MockResponse::error(400, "InvalidInputException", &format!("unmocked {other:?}")),
    }
}

fn skipped(name: &str, detail: &str) -> SkippedTable {
    SkippedTable {
        ident: ident(name),
        reason: SkipReason::NotPlannableGlueTable {
            detail: detail.to_string(),
        },
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

fn user_message(error: UdfError) -> String {
    match error {
        UdfError::User(message) => message,
        other => panic!("expected a user error, got {other:?}"),
    }
}

/// Scenario: The client routes a table by its declared table type before its storage descriptor
#[tokio::test]
async fn list_tables_routes_each_registration_to_its_reader_or_a_skip() {
    let mut athena_delta = hive_table(
        "athena_delta",
        "org.apache.hadoop.mapred.SequenceFileInputFormat",
    );
    athena_delta["Parameters"] = json!({ "table_type": "delta" });
    let mut symlink = hive_table(
        "symlink",
        "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat",
    );
    symlink["StorageDescriptor"]["SerdeInfo"] = json!({
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    });
    let view = json!({
        "Name": "a_view",
        "DatabaseName": DATABASE,
        "TableType": "VIRTUAL_VIEW",
        "ViewOriginalText": "/* Presto View */",
    });
    let mock = mock_glue::spawn(catalog_of(vec![
        iceberg_table("iceberg_upper", "ICEBERG"),
        iceberg_table("iceberg_lower", "iceberg"),
        athena_delta,
        hive_table("hive_parquet", PARQUET_INPUT_FORMAT),
        hive_table("hive_orc", ORC_INPUT_FORMAT),
        hive_table("hive_text", "org.apache.hadoop.mapred.TextInputFormat"),
        hive_table(
            "hive_avro",
            "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
        ),
        symlink,
        view,
    ]))
    .await;

    let listing = session(&mock)
        .list_tables(&database())
        .await
        .expect("the listing succeeds");

    let admitted: Vec<(&str, TableFormat)> = listing
        .tables
        .iter()
        .map(|table| (table.ident.name.as_str(), table.format))
        .collect();
    assert_eq!(
        admitted,
        vec![
            ("iceberg_upper", TableFormat::Iceberg),
            ("iceberg_lower", TableFormat::Iceberg),
            ("hive_parquet", TableFormat::Parquet),
        ]
    );
    assert_eq!(
        listing.skipped,
        vec![
            skipped("athena_delta", "table_type=delta"),
            skipped("hive_orc", &format!("InputFormat={ORC_INPUT_FORMAT}")),
            skipped(
                "hive_text",
                "InputFormat=org.apache.hadoop.mapred.TextInputFormat"
            ),
            skipped(
                "hive_avro",
                "InputFormat=org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"
            ),
            skipped(
                "symlink",
                "InputFormat=org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
            ),
            skipped("a_view", "TableType=VIRTUAL_VIEW"),
        ]
    );
}

/// Scenario: An Iceberg table takes its columns from its current metadata file
#[tokio::test]
async fn iceberg_columns_come_from_the_metadata_file_and_the_planning_load_reads_none() {
    let mock = mock_glue::spawn(catalog_of(vec![iceberg_table("orders", "ICEBERG")])).await;
    let glue = session(&mock);

    let listing = glue
        .list_tables(&database())
        .await
        .expect("the listing succeeds");
    let storage_reads_after_listing = mock.storage_requests().len();
    let planned = glue
        .load_table_for_planning(&ident("orders"))
        .await
        .expect("the planning load succeeds");

    assert_eq!(
        listing.tables,
        vec![CatalogTable {
            ident: ident("orders"),
            table_type: CatalogTableType::Table,
            storage_location: Some("s3://bucket/orders".to_string()),
            format: TableFormat::Iceberg,
            vended_credential_key: None,
            partition_columns: Vec::new(),
            columns: iceberg_columns(),
            metadata_location: Some(metadata_location("orders")),
        }],
        "the listing takes its columns from the metadata file, never the Glue columns"
    );
    assert_eq!(
        mock.storage_requests()
            .iter()
            .map(|request| request.path.as_str())
            .collect::<Vec<_>>(),
        vec![metadata_path("orders")],
        "the listing reads the metadata file once"
    );
    assert_eq!(
        mock.storage_requests().len(),
        storage_reads_after_listing,
        "the planning load reads no metadata file"
    );
    assert_eq!(planned.metadata_location, Some(metadata_location("orders")));
    assert_eq!(planned.format, TableFormat::Iceberg);
    assert!(
        planned.columns.is_empty(),
        "the planning load reads no Glue column: {:?}",
        planned.columns
    );
}

#[tokio::test]
async fn catalog_client_load_table_performs_the_listing_load_for_one_table() {
    let mock = mock_glue::spawn(catalog_of(vec![
        iceberg_table("orders", "ICEBERG"),
        hive_table("hive_orc", ORC_INPUT_FORMAT),
    ]))
    .await;
    let glue = session(&mock);

    let loaded = glue
        .load_table(&ident("orders"))
        .await
        .expect("load succeeds");
    let refused = glue.load_table(&ident("hive_orc")).await;

    assert_eq!(loaded.columns, iceberg_columns());
    assert_eq!(loaded.metadata_location, Some(metadata_location("orders")));
    let message = user_message(refused.expect_err("an ORC table is not loadable"));
    assert!(message.contains("sales.hive_orc"), "{message}");
    assert!(message.contains(ORC_INPUT_FORMAT), "{message}");
}

#[tokio::test]
async fn an_iceberg_table_without_metadata_location_is_skipped() {
    let mut no_pointer = iceberg_table("no_pointer", "ICEBERG");
    no_pointer["Parameters"] = json!({ "table_type": "ICEBERG" });
    let mut empty_pointer = iceberg_table("empty_pointer", "ICEBERG");
    empty_pointer["Parameters"]["metadata_location"] = json!("");
    let mock = mock_glue::spawn(catalog_of(vec![no_pointer, empty_pointer])).await;

    let listing = session(&mock)
        .list_tables(&database())
        .await
        .expect("the listing succeeds");

    assert!(listing.tables.is_empty());
    let names: Vec<&str> = listing
        .skipped
        .iter()
        .map(|entry| entry.ident.name.as_str())
        .collect();
    assert_eq!(names, vec!["no_pointer", "empty_pointer"]);
    for entry in &listing.skipped {
        let SkipReason::NotPlannableGlueTable { detail } = &entry.reason else {
            panic!("expected a Glue skip, got {:?}", entry.reason);
        };
        assert!(detail.contains("metadata_location"), "{detail}");
    }
    assert!(
        mock.storage_requests().is_empty(),
        "a skipped table's metadata is never read"
    );
}

#[tokio::test]
async fn an_unreadable_metadata_file_fails_the_listing_naming_the_table() {
    let tables = vec![iceberg_table("orders", "ICEBERG")];
    let mock = mock_glue::spawn(move |request, _| match request.operation() {
        Some("GetTables") => tables_page(tables.clone()),
        _ => MockResponse::error(403, "AccessDenied", "Access Denied"),
    })
    .await;

    let error = session(&mock)
        .list_tables(&database())
        .await
        .expect_err("an unreadable metadata file fails the listing");

    let message = user_message(error);
    assert!(message.contains("sales.orders"), "{message}");
    assert!(message.contains(&metadata_location("orders")), "{message}");
    assert_no_credential(&message);
}

#[tokio::test]
async fn iceberg_metadata_files_are_each_read_once_in_listing_order() {
    let names: Vec<String> = (0..40).map(|index| format!("t{index:02}")).collect();
    let tables: Vec<Value> = names
        .iter()
        .map(|name| iceberg_table(name, "ICEBERG"))
        .collect();
    let mock = mock_glue::spawn(catalog_of(tables)).await;

    let listing = session(&mock)
        .list_tables(&database())
        .await
        .expect("the listing succeeds");

    assert_eq!(
        listing
            .tables
            .iter()
            .map(|table| table.ident.name.clone())
            .collect::<Vec<_>>(),
        names,
        "the listing keeps the Glue order"
    );
    assert_eq!(mock.storage_requests().len(), names.len());
}

/// Scenario: A Parquet table declares its Glue columns and partition keys
#[tokio::test]
async fn parquet_table_declares_storage_columns_then_partition_keys() {
    let mut table = hive_table("events", PARQUET_INPUT_FORMAT);
    table["StorageDescriptor"]["Columns"] =
        columns(&[("id", "bigint"), ("payload", "struct<x:int,y:string>")]);
    table["StorageDescriptor"]["Location"] = json!("s3://bucket/warehouse/events/");
    table["PartitionKeys"] = columns(&[("p_int", "int"), ("p_date", "date"), ("p_str", "string")]);
    let mock = mock_glue::spawn(catalog_of(vec![table])).await;
    let glue = session(&mock);

    let listing = glue
        .list_tables(&database())
        .await
        .expect("listing succeeds");
    let planned = glue
        .load_table_for_planning(&ident("events"))
        .await
        .expect("planning load succeeds");

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
    assert_eq!(listing.tables, vec![expected.clone()]);
    assert_eq!(planned, expected);
}

#[tokio::test]
async fn a_projection_enabled_table_is_skipped() {
    let mut projected = hive_table("projected", PARQUET_INPUT_FORMAT);
    projected["Parameters"] = json!({ "projection.enabled": "TRUE" });
    let mock = mock_glue::spawn(catalog_of(vec![projected])).await;

    let listing = session(&mock)
        .list_tables(&database())
        .await
        .expect("listing succeeds");

    assert!(listing.tables.is_empty());
    let [entry] = listing.skipped.as_slice() else {
        panic!("expected one skip, got {:?}", listing.skipped);
    };
    let SkipReason::NotPlannableGlueTable { detail } = &entry.reason else {
        panic!("expected a Glue skip, got {:?}", entry.reason);
    };
    assert!(detail.contains("partition projection"), "{detail}");
    assert!(detail.contains("zero rows"), "{detail}");
}

fn partition(values: &[&str], location: &str, input_format: Option<&str>) -> Value {
    let mut descriptor = json!({ "Location": location });
    if let Some(input_format) = input_format {
        descriptor["InputFormat"] = json!(input_format);
    }
    json!({ "Values": values, "StorageDescriptor": descriptor })
}

fn partition_keys() -> Vec<String> {
    vec![
        "p_int".to_string(),
        "p_date".to_string(),
        "p_str".to_string(),
    ]
}

fn values(pairs: &[(&str, Option<&str>)]) -> BTreeMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.map(str::to_string)))
        .collect()
}

/// Scenario: Partitions carry their Glue values, location, and format
#[tokio::test]
async fn partitions_carry_glue_values_the_default_partition_as_null_and_their_own_format() {
    let page = json!({ "Partitions": [
        partition(&["1", "2024-01-01", "a"], "s3://bucket/events/p_int=1/p_date=2024-01-01/p_str=a/", Some(PARQUET_INPUT_FORMAT)),
        partition(&["__HIVE_DEFAULT_PARTITION__", "2024-01-02", "b"], "s3://bucket/events/p_int=__HIVE_DEFAULT_PARTITION__/", Some(PARQUET_INPUT_FORMAT)),
        partition(&["3", "2024-01-03", "c"], "s3://other/elsewhere", None),
        partition(&["9", "2024-01-09", "z"], "s3://bucket/events/p_int=9/", Some(ORC_INPUT_FORMAT)),
    ]});
    let mock = mock_glue::spawn(move |_, _| MockResponse::json(page.clone())).await;

    let partitions = session(&mock)
        .partitions(&ident("events"), &partition_keys())
        .await
        .expect("partitions load");

    assert_eq!(
        partitions,
        vec![
            CatalogPartition {
                values: values(&[
                    ("p_int", Some("1")),
                    ("p_date", Some("2024-01-01")),
                    ("p_str", Some("a"))
                ]),
                location: "s3://bucket/events/p_int=1/p_date=2024-01-01/p_str=a".to_string(),
                format: Some(TableFormat::Parquet),
                input_format: PARQUET_INPUT_FORMAT.to_string(),
            },
            CatalogPartition {
                values: values(&[
                    ("p_int", None),
                    ("p_date", Some("2024-01-02")),
                    ("p_str", Some("b"))
                ]),
                location: "s3://bucket/events/p_int=__HIVE_DEFAULT_PARTITION__".to_string(),
                format: Some(TableFormat::Parquet),
                input_format: PARQUET_INPUT_FORMAT.to_string(),
            },
            CatalogPartition {
                values: values(&[
                    ("p_int", Some("3")),
                    ("p_date", Some("2024-01-03")),
                    ("p_str", Some("c"))
                ]),
                location: "s3://other/elsewhere".to_string(),
                format: Some(TableFormat::Parquet),
                input_format: PARQUET_INPUT_FORMAT.to_string(),
            },
            CatalogPartition {
                values: values(&[
                    ("p_int", Some("9")),
                    ("p_date", Some("2024-01-09")),
                    ("p_str", Some("z"))
                ]),
                location: "s3://bucket/events/p_int=9".to_string(),
                format: None,
                input_format: ORC_INPUT_FORMAT.to_string(),
            },
        ]
    );
    let [request] = mock
        .requests_for("GetPartitions")
        .try_into()
        .expect("one page");
    assert_eq!(request.json()["DatabaseName"], json!(DATABASE));
    assert_eq!(request.json()["TableName"], json!("events"));
}

#[tokio::test]
async fn a_partition_value_count_mismatch_fails_naming_its_location() {
    let page = json!({ "Partitions": [
        partition(&["1", "2024-01-01"], "s3://bucket/events/p_int=1/", Some(PARQUET_INPUT_FORMAT)),
    ]});
    let mock = mock_glue::spawn(move |_, _| MockResponse::json(page.clone())).await;

    let error = session(&mock)
        .partitions(&ident("events"), &partition_keys())
        .await
        .expect_err("two values for three keys fail");

    let message = user_message(error);
    assert!(message.contains("s3://bucket/events/p_int=1/"), "{message}");
    assert!(message.contains("sales.events"), "{message}");
}

#[tokio::test]
async fn a_partition_without_a_location_fails_naming_its_values() {
    let page = json!({ "Partitions": [
        { "Values": ["1", "2024-01-01", "a"], "StorageDescriptor": { "InputFormat": PARQUET_INPUT_FORMAT } },
    ]});
    let mock = mock_glue::spawn(move |_, _| MockResponse::json(page.clone())).await;

    let error = session(&mock)
        .partitions(&ident("events"), &partition_keys())
        .await
        .expect_err("a partition without a location fails");

    let message = user_message(error);
    assert!(message.contains("sales.events"), "{message}");
    assert!(message.contains("2024-01-01"), "{message}");
}

fn next_token(request: &RecordedRequest) -> Option<String> {
    request.json()["NextToken"].as_str().map(str::to_string)
}

/// Scenario: Every listing follows its continuation tokens and stops on an empty page
#[tokio::test]
async fn listings_follow_tokens_and_stop_on_an_empty_page() {
    let mock = mock_glue::spawn(|request, attempt| match (request.operation(), attempt) {
        (Some("GetTables"), 0) => MockResponse::json(json!({
            "TableList": [hive_table("a", PARQUET_INPUT_FORMAT), hive_table("b", PARQUET_INPUT_FORMAT)],
            "NextToken": "tables-1",
        })),
        (Some("GetTables"), 1) => MockResponse::json(json!({
            "TableList": [hive_table("c", PARQUET_INPUT_FORMAT)],
            "NextToken": "tables-2",
        })),
        (Some("GetTables"), _) => MockResponse::json(json!({ "TableList": [], "NextToken": "tables-3" })),
        (Some("GetPartitions"), 0) => MockResponse::json(json!({
            "Partitions": [partition(&["1"], "s3://bucket/t/p=1", None)],
            "NextToken": "partitions-1",
        })),
        (Some("GetPartitions"), 1) => MockResponse::json(json!({
            "Partitions": [partition(&["2"], "s3://bucket/t/p=2", None)],
            "NextToken": "",
        })),
        (other, _) => MockResponse::error(400, "InvalidInputException", &format!("{other:?}")),
    })
    .await;
    let glue = session(&mock);

    let listing = glue
        .list_tables(&database())
        .await
        .expect("listing succeeds");
    let partitions = glue
        .partitions(&ident("t"), &["p".to_string()])
        .await
        .expect("partitions load");

    assert_eq!(
        listing
            .tables
            .iter()
            .map(|table| table.ident.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c"],
        "a short page with a token does not end the listing"
    );
    assert_eq!(
        mock.requests_for("GetTables")
            .iter()
            .map(next_token)
            .collect::<Vec<_>>(),
        vec![
            None,
            Some("tables-1".to_string()),
            Some("tables-2".to_string())
        ],
        "the empty page ends the listing although it carries a token"
    );
    assert_eq!(
        partitions.len(),
        2,
        "an empty token ends the partition listing"
    );
    let partition_requests = mock.requests_for("GetPartitions");
    assert_eq!(
        partition_requests
            .iter()
            .map(next_token)
            .collect::<Vec<_>>(),
        vec![None, Some("partitions-1".to_string())]
    );
    for request in &partition_requests {
        assert_eq!(request.json()["ExcludeColumnSchema"], json!(true));
    }
}

/// Scenario: The CatalogId is sent only when the CONNECTION names one, and the NAMESPACE names one database
#[tokio::test]
async fn catalog_id_is_sent_only_for_a_non_empty_warehouse() {
    let table = hive_table("t", PARQUET_INPUT_FORMAT);
    let mock = mock_glue::spawn(move |request, _| match request.operation() {
        Some("GetTables") => tables_page(vec![table.clone()]),
        Some("GetTable") => MockResponse::json(json!({ "Table": table })),
        _ => MockResponse::json(json!({ "Partitions": [] })),
    })
    .await;

    for warehouse in ["", "   ", "123456789012"] {
        let glue = session_with_warehouse(&mock, warehouse);
        glue.list_tables(&database())
            .await
            .expect("listing succeeds");
        glue.load_table_for_planning(&ident("t"))
            .await
            .expect("planning load succeeds");
        glue.partitions(&ident("t"), &[])
            .await
            .expect("partitions load");
    }

    let requests = mock.requests();
    assert_eq!(requests.len(), 9);
    for (index, request) in requests.iter().enumerate() {
        let body = request.json();
        if index < 6 {
            assert!(
                body.get("CatalogId").is_none(),
                "{:?} without a warehouse must send no CatalogId: {body}",
                request.operation()
            );
        } else {
            assert_eq!(
                body["CatalogId"],
                json!("123456789012"),
                "{:?}",
                request.operation()
            );
        }
        assert_eq!(body["DatabaseName"], json!(DATABASE));
    }
}

#[tokio::test]
async fn a_multi_segment_namespace_is_refused() {
    let mock = mock_glue::spawn(catalog_of(Vec::new())).await;
    let glue = session(&mock);

    for namespace in [
        vec!["sales".to_string(), "eu".to_string()],
        Vec::new(),
        vec![String::new()],
    ] {
        let error = glue
            .list_tables(&namespace)
            .await
            .expect_err("only one database segment is accepted");
        let message = user_message(error);
        assert!(message.contains("exactly one database"), "{message}");
    }
    assert!(
        mock.requests().is_empty(),
        "no refused namespace reaches Glue"
    );
}

/// Scenario: Service errors are classified by their error code, not their HTTP status
#[tokio::test]
async fn errors_are_classified_by_code_and_name_the_operation() {
    let echoed = format!("request by {ACCESS_KEY} with {SECRET_KEY} and {SESSION_TOKEN} denied");
    let cases: Vec<(u16, &str, String, bool)> = vec![
        (
            400,
            "EntityNotFoundException",
            "Database sales not found.".to_string(),
            true,
        ),
        (404, "AccessDeniedException", echoed.clone(), false),
        (400, "AccessDeniedException", echoed, false),
        (
            400,
            "InvalidInputException",
            "Invalid database name".to_string(),
            false,
        ),
    ];

    for (status, code, service_message, missing) in cases {
        let answer = service_message.clone();
        let mock = mock_glue::spawn(move |_, _| MockResponse::error(status, code, &answer)).await;

        let error = session(&mock)
            .list_tables(&database())
            .await
            .expect_err("a service error fails the listing");

        let message = user_message(error);
        assert!(message.contains("GetTables"), "{message}");
        assert!(message.contains(code), "{message}");
        assert!(message.contains("database 'sales'"), "{message}");
        assert_eq!(
            message.contains("does not exist"),
            missing,
            "HTTP {status} {code} is classified by its code: {message}"
        );
        assert_no_credential(&message);
        if !service_message.contains(SECRET_KEY) {
            assert!(message.contains(&service_message), "{message}");
        }
    }
}

/// Scenario: Throttling and server errors are retried within a bounded time
#[tokio::test]
async fn throttling_and_503_are_retried_until_success() {
    for failure in ["throttling", "503"] {
        let mock = mock_glue::spawn(move |_, attempt| match (failure, attempt) {
            ("throttling", 0 | 1) => {
                MockResponse::error(400, "ThrottlingException", "Rate exceeded")
            }
            ("503", 0 | 1) => MockResponse::status(503),
            _ => tables_page(vec![hive_table("t", PARQUET_INPUT_FORMAT)]),
        })
        .await;

        let listing = fast_retry_session(&mock).list_tables(&database()).await;

        assert!(
            listing.is_ok(),
            "{failure} twice then success is retried: {:?}",
            listing.err()
        );
        assert_eq!(mock.requests_for("GetTables").len(), 3, "{failure}");
    }
}

#[tokio::test]
async fn persistent_503_fails_naming_the_http_status() {
    let mock = mock_glue::spawn(|_, _| MockResponse::status(503)).await;

    let error = fast_retry_session(&mock)
        .list_tables(&database())
        .await
        .expect_err("a persistent 503 fails");

    let message = user_message(error);
    assert!(message.contains("GetTables"), "{message}");
    assert!(message.contains("HTTP 503"), "{message}");
}

#[test]
fn every_signing_time_rejection_names_the_clock() {
    assert!(is_signing_time_rejection("RequestTimeTooSkewed", ""));
    assert!(is_signing_time_rejection("RequestExpired", ""));
    assert!(is_signing_time_rejection(
        "InvalidSignatureException",
        "Signature not yet current: x"
    ));
    assert!(!is_signing_time_rejection(
        "InvalidSignatureException",
        "The request signature we calculated does not match"
    ));
}

#[tokio::test]
async fn an_operation_outliving_its_timeout_fails_naming_the_deadline() {
    let mock = mock_glue::spawn(|_, _| MockResponse::hang()).await;
    let creds = glue_creds("");
    let config = glue_config(&mock.address, &creds, REGION.to_string())
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(Duration::from_millis(200))
                .build(),
        )
        .build();
    let glue = GlueCatalogSession::from_config(config, storage(&mock), &creds);

    let error = glue
        .list_tables(&database())
        .await
        .expect_err("a hung call fails at its deadline");

    let message = user_message(error);
    assert!(message.contains("GetTables"), "{message}");
    assert!(message.contains("did not complete within"), "{message}");
}

#[tokio::test]
async fn persistent_throttling_fails_naming_the_operation() {
    let mock =
        mock_glue::spawn(|_, _| MockResponse::error(400, "ThrottlingException", "Rate exceeded"))
            .await;

    let error = fast_retry_session(&mock)
        .list_tables(&database())
        .await
        .expect_err("persistent throttling fails");

    let message = user_message(error);
    assert!(message.contains("GetTables"), "{message}");
    assert!(message.contains("ThrottlingException"), "{message}");
    assert_eq!(
        mock.requests_for("GetTables").len(),
        MAX_ATTEMPTS as usize,
        "every attempt of the call is spent"
    );
}

fn amz_date_seconds(request: &RecordedRequest) -> i64 {
    let basic = request.header("x-amz-date").expect("a signed request");
    let extended = format!(
        "{}-{}-{}T{}:{}:{}Z",
        &basic[0..4],
        &basic[4..6],
        &basic[6..8],
        &basic[9..11],
        &basic[11..13],
        &basic[13..15]
    );
    DateTime::from_str(&extended, DateTimeFormat::DateTime)
        .expect("an ISO timestamp")
        .secs()
}

/// Scenario: A clock-skew signing failure names the clock
#[tokio::test]
async fn a_persistent_signing_time_rejection_names_the_clock() {
    let service_now = DateTime::from(SystemTime::now() + Duration::from_secs(3600))
        .fmt(DateTimeFormat::HttpDate)
        .expect("an HTTP date");
    let mock = mock_glue::spawn(move |_, _| {
        MockResponse::error(
            400,
            "InvalidSignatureException",
            "Signature expired: 20260101T000000Z is now earlier than 20260101T005500Z (20260101T010000Z - 5 min.)",
        )
        .with_date(service_now.clone())
    })
    .await;

    let error = fast_retry_session(&mock)
        .list_tables(&database())
        .await
        .expect_err("a persistent signing-time rejection fails");

    let message = user_message(error);
    assert!(message.contains("clock"), "{message}");
    assert!(message.contains("AWS time"), "{message}");
    assert!(
        !message.to_lowercase().contains("credential"),
        "a clock error must not read as a credential error: {message}"
    );
    let attempts = mock.requests_for("GetTables");
    assert!(attempts.len() > 1, "the SDK retries a skewed signature");
    let first = amz_date_seconds(&attempts[0]);
    let last = amz_date_seconds(attempts.last().expect("a retry"));
    assert!(
        last - first >= 3000,
        "the retry signs with the service time: first {first}, last {last}"
    );
}

#[tokio::test]
async fn requests_are_signed_for_the_signing_region_with_the_session_token() {
    let mock = mock_glue::spawn(catalog_of(Vec::new())).await;

    session(&mock)
        .list_tables(&database())
        .await
        .expect("listing succeeds");

    let [request] = mock.requests_for("GetTables").try_into().expect("one call");
    let authorization = request.header("authorization").expect("a signed request");
    assert!(
        authorization.contains(&format!("Credential={ACCESS_KEY}/")),
        "{authorization}"
    );
    assert!(
        authorization.contains(&format!("/{REGION}/glue/aws4_request")),
        "{authorization}"
    );
    assert_eq!(request.header("x-amz-security-token"), Some(SESSION_TOKEN));
    assert_eq!(request.method, "POST");
}

#[test]
fn a_session_without_a_signing_region_is_refused() {
    let creds = ConnectionCreds {
        region: String::new(),
        ..glue_creds("")
    };

    let result = GlueCatalogSession::new(
        "http://127.0.0.1:1",
        StorageBackend::S3(StorageProps::default()),
        creds,
    );

    assert!(result.is_err(), "no signing region, no session");
}

/// Scenario: A Glue table resolves from its recorded identifier and fails loud when it is no longer plannable
#[tokio::test]
async fn the_planning_load_fails_for_a_dropped_or_no_longer_plannable_table() {
    let mock = mock_glue::spawn(catalog_of(vec![hive_table("orders_orc", ORC_INPUT_FORMAT)])).await;
    let glue = session(&mock);

    let dropped = user_message(
        glue.load_table_for_planning(&ident("orders"))
            .await
            .expect_err("a dropped table fails"),
    );
    let reregistered = user_message(
        glue.load_table_for_planning(&ident("orders_orc"))
            .await
            .expect_err("an ORC table fails"),
    );

    assert!(dropped.contains("sales.orders"), "{dropped}");
    assert!(dropped.contains("does not exist"), "{dropped}");
    assert!(dropped.contains("GetTable"), "{dropped}");
    assert!(dropped.contains("EntityNotFoundException"), "{dropped}");
    assert!(reregistered.contains("sales.orders_orc"), "{reregistered}");
    assert!(
        reregistered.contains(&format!("InputFormat={ORC_INPUT_FORMAT}")),
        "{reregistered}"
    );
    assert_no_credential(&dropped);
    assert_no_credential(&reregistered);
    let requested: Vec<Value> = mock
        .requests_for("GetTable")
        .iter()
        .map(|request| request.json()["Name"].clone())
        .collect();
    assert_eq!(requested, vec![json!("orders"), json!("orders_orc")]);
}

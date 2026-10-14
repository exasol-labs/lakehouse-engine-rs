use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_glue::config::timeout::TimeoutConfig;
use iceberg::spec::{PrimitiveType, Type};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::*;
use crate::StorageProps;
use crate::glue::routing::PARQUET_INPUT_FORMAT;
use crate::test_support::{ORC_INPUT_FORMAT, header_value, request_body, spawn_server};

const ACCESS_KEY: &str = "AKIDGLUECONNECTION01";
const SECRET_KEY: &str = "GLUE_SECRET_KEY_SENTINEL";
const SESSION_TOKEN: &str = "GLUE_SESSION_TOKEN_SENTINEL";
const STORAGE_SECRET_KEY: &str = "STORAGE_SECRET_KEY_SENTINEL";
const DATABASE: &str = "sales";

type Requests = Arc<Mutex<Vec<String>>>;
type Reply = (u16, Value);

/// The Glue operation a recorded request names, or `GET <path>` for a storage read.
fn label(request: &str) -> String {
    match header_value(request, "x-amz-target") {
        Some(target) => target.trim_start_matches("AWSGlue.").to_string(),
        None => request.split(' ').take(2).collect::<Vec<_>>().join(" "),
    }
}

fn labels(requests: &Requests) -> Vec<String> {
    requests.lock().unwrap().iter().map(|r| label(r)).collect()
}

/// Glue and the CONNECTION's storage behind one loopback address; `respond` answers each
/// request from its [`label`] and JSON body.
async fn fake_glue(
    respond: impl Fn(&str, &Value) -> Reply + Send + 'static,
) -> (GlueCatalogSession, Requests) {
    let (address, requests) = spawn_server("application/x-amz-json-1.1", move |request| {
        let body = serde_json::from_str(request_body(request)).unwrap_or(Value::Null);
        let (status, reply) = respond(&label(request), &body);
        (status, reply.to_string())
    })
    .await;
    (session_at(&address), requests)
}

/// Keeps the production retry policy but backs off for milliseconds and gives up after a
/// second, so a failing call stays fast.
fn session_at(address: &str) -> GlueCatalogSession {
    let creds = ConnectionCreds {
        region: "eu-west-1".into(),
        access_key: ACCESS_KEY.into(),
        secret_key: SECRET_KEY.into(),
        session_token: Some(SESSION_TOKEN.into()),
        ..ConnectionCreds::default()
    };
    let storage = StorageBackend::S3(StorageProps {
        endpoint: address.into(),
        region: "eu-west-1".into(),
        access_key: ACCESS_KEY.into(),
        secret_key: STORAGE_SECRET_KEY.into(),
        path_style: true,
        ..StorageProps::default()
    });
    let mut session = GlueCatalogSession::new(address, storage, creds).expect("a signing region");
    let config = session.client.config();
    let retry = config.retry_config().expect("a retry policy").clone();
    session.client = aws_sdk_glue::Client::from_conf(
        config
            .to_builder()
            .retry_config(retry.with_initial_backoff(Duration::from_millis(1)))
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(1))
                    .build(),
            )
            .build(),
    );
    session
}

fn ok(body: Value) -> Reply {
    (200, body)
}

fn service_error(code: &str, message: &str) -> Reply {
    (400, json!({"__type": code, "Message": message}))
}

fn unexpected(label: &str) -> Reply {
    service_error("UnexpectedRequest", label)
}

fn table(name: &str, parameters: Value, input_format: Option<&str>) -> Value {
    let mut table = json!({
        "Name": name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": parameters,
        "StorageDescriptor": {
            "Location": format!("s3://bucket/{name}/"),
            "Columns": [{"Name": "glue_only", "Type": "string"}],
        },
    });
    if let Some(input_format) = input_format {
        table["StorageDescriptor"]["InputFormat"] = json!(input_format);
    }
    table
}

fn metadata_location(name: &str) -> String {
    format!("s3://bucket/{name}/metadata/00001.metadata.json")
}

fn iceberg_table(name: &str, table_type: &str) -> Value {
    let parameters =
        json!({"table_type": table_type, "metadata_location": metadata_location(name)});
    table(name, parameters, None)
}

fn iceberg_metadata() -> Value {
    json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000001",
        "location": "s3://bucket/orders",
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
    })
}

fn partition(values: &[&str], location: &str, input_format: Option<&str>) -> Value {
    let descriptor = json!({"Location": location, "InputFormat": input_format});
    json!({"Values": values, "StorageDescriptor": descriptor})
}

fn ident(name: &str) -> CatalogTableIdent {
    CatalogTableIdent {
        namespace: vec![DATABASE.to_string()],
        name: name.to_string(),
    }
}

fn parquet_table(partition_columns: &[&str]) -> CatalogTable {
    CatalogTable {
        ident: ident("events"),
        table_type: CatalogTableType::Table,
        storage_location: Some("s3://bucket/events".to_string()),
        format: TableFormat::Parquet,
        vended_credential_key: None,
        partition_columns: partition_columns.iter().map(|c| c.to_string()).collect(),
        columns: Vec::new(),
        metadata_location: None,
    }
}

fn user_message(error: UdfError) -> String {
    match error {
        UdfError::User(message) => message,
        other => panic!("expected a user error, got {other:?}"),
    }
}

async fn listing_error(session: &GlueCatalogSession) -> String {
    let error = session.list_tables(&[DATABASE.to_string()]).await;
    user_message(error.expect_err("the Glue call fails"))
}

fn assert_no_credential(text: &str) {
    for secret in [ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, STORAGE_SECRET_KEY] {
        assert!(!text.contains(secret), "a credential leaked: {text}");
    }
}

/// Scenario: The client routes a table by its declared table type before its storage descriptor
#[tokio::test]
async fn the_listing_routes_each_table_by_its_table_type_before_its_input_format() {
    const SEQUENCE: &str = "org.apache.hadoop.mapred.SequenceFileInputFormat";
    const TEXT: &str = "org.apache.hadoop.mapred.TextInputFormat";
    const SYMLINK: &str = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat";
    const PARQUET: Option<&str> = Some(PARQUET_INPUT_FORMAT);
    let lowercase = PARQUET_INPUT_FORMAT.to_lowercase();
    let unlocated = "table_type=ICEBERG with an absent or empty metadata_location".to_string();
    let skip_format = |format: &str| format!("InputFormat={format}");
    let iceberg = |kind: &str| json!({"table_type": kind, "metadata_location": "s3://b/m.json"});
    let delta = json!({"table_type": "delta"});
    let cases = [
        ("ice_upper", iceberg("ICEBERG"), None, "Iceberg".to_string()),
        ("ice_lower", iceberg("iceberg"), None, "Iceberg".to_string()),
        ("events", json!({}), PARQUET, "Parquet".to_string()),
        (
            "unprojected",
            json!({"projection.enabled": "false"}),
            PARQUET,
            "Parquet".to_string(),
        ),
        (
            "athena_delta",
            delta.clone(),
            Some(SEQUENCE),
            "table_type=delta".to_string(),
        ),
        (
            "delta_on_parquet",
            delta,
            PARQUET,
            "table_type=delta".to_string(),
        ),
        (
            "ice_unlocated",
            json!({"table_type": "ICEBERG"}),
            None,
            unlocated.clone(),
        ),
        (
            "ice_blank",
            json!({"table_type": "ICEBERG", "metadata_location": " "}),
            None,
            unlocated,
        ),
        (
            "orc",
            json!({}),
            Some(ORC_INPUT_FORMAT),
            skip_format(ORC_INPUT_FORMAT),
        ),
        ("text", json!({}), Some(TEXT), skip_format(TEXT)),
        ("symlink", json!({}), Some(SYMLINK), skip_format(SYMLINK)),
        (
            "lowercase",
            json!({}),
            Some(lowercase.as_str()),
            skip_format(&lowercase),
        ),
        ("unformatted", json!({}), None, skip_format("absent")),
        (
            "view",
            iceberg("ICEBERG"),
            PARQUET,
            "TableType=VIRTUAL_VIEW".to_string(),
        ),
        (
            "projected",
            json!({"projection.enabled": "TRUE"}),
            PARQUET,
            "projection.enabled=TRUE: partition projection registers no partition in Glue, so \
             reading the table would return zero rows"
                .to_string(),
        ),
    ];
    let tables: Vec<Value> = cases
        .iter()
        .map(|(name, parameters, input_format, _)| {
            let mut table = table(name, parameters.clone(), *input_format);
            if *name == "view" {
                table["TableType"] = json!("VIRTUAL_VIEW");
            }
            table
        })
        .collect();
    let (session, _) = fake_glue(move |label, _| match label {
        "GetTables" => ok(json!({ "TableList": tables })),
        _ => ok(iceberg_metadata()),
    })
    .await;

    let listing = session
        .list_tables(&[DATABASE.to_string()])
        .await
        .expect("the listing succeeds");

    let admitted = listing
        .tables
        .iter()
        .map(|table| (table.ident.name.clone(), format!("{:?}", table.format)));
    let skipped = listing.skipped.iter().map(|skipped| match &skipped.reason {
        SkipReason::NotPlannableGlueTable { detail } => {
            (skipped.ident.name.clone(), detail.clone())
        }
        other => panic!("a Glue skip names its Glue value: {other:?}"),
    });
    let expected = cases.map(|(name, _, _, outcome)| (name.to_string(), outcome));
    assert_eq!(
        admitted.chain(skipped).collect::<BTreeMap<_, _>>(),
        BTreeMap::from(expected)
    );
}

/// Scenario: An Iceberg table takes its columns from its current metadata file
#[tokio::test]
async fn an_iceberg_table_lists_its_metadata_columns_and_plans_without_reading_the_file() {
    let orders = iceberg_table("orders", "ICEBERG");
    let (session, requests) = fake_glue(move |label, _| match label {
        "GetTables" => ok(json!({ "TableList": [orders] })),
        "GetTable" => ok(json!({ "Table": orders })),
        _ => ok(iceberg_metadata()),
    })
    .await;

    let listed = session
        .list_tables(&[DATABASE.to_string()])
        .await
        .expect("the listing reads the metadata file");
    let planned = session
        .load_table(&ident("orders"))
        .await
        .expect("the planning load succeeds");

    let [listed] = listed.tables.as_slice() else {
        panic!("one table: {listed:?}");
    };
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
    let location = metadata_location("orders");
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
            metadata_location: Some(location),
        }
    );
    assert_eq!(
        labels(&requests),
        [
            "GetTables",
            "GET /bucket/orders/metadata/00001.metadata.json",
            "GetTable"
        ],
        "only the listing reads the metadata file"
    );
}

#[tokio::test]
async fn an_unreadable_metadata_file_fails_the_listing_naming_the_table() {
    let (session, _) = fake_glue(|label, _| match label {
        "GetTables" => ok(json!({ "TableList": [iceberg_table("orders", "ICEBERG")] })),
        _ => (404, json!({})),
    })
    .await;

    let message = listing_error(&session).await;

    assert!(message.contains("sales.orders"), "{message}");
    assert!(message.contains(&metadata_location("orders")), "{message}");
    assert_no_credential(&message);
}

/// Scenario: A Parquet table declares its Glue columns and partition keys
#[tokio::test]
async fn a_parquet_table_declares_storage_columns_then_partition_keys() {
    let mut events = table("events", json!({}), Some(PARQUET_INPUT_FORMAT));
    events["StorageDescriptor"]["Location"] = json!("s3://bucket/warehouse/events/");
    events["StorageDescriptor"]["Columns"] = json!([
        {"Name": "id", "Type": "bigint"},
        {"Name": "payload", "Type": "struct<x:int,y:string>"},
    ]);
    events["PartitionKeys"] = json!([
        {"Name": "p_int", "Type": "int"},
        {"Name": "p_date", "Type": "date"},
        {"Name": "p_str", "Type": "string"},
    ]);
    let (session, _) = fake_glue(move |label, _| match label {
        "GetTables" => ok(json!({ "TableList": [events] })),
        _ => ok(json!({ "Table": events })),
    })
    .await;

    let listing = session
        .list_tables(&[DATABASE.to_string()])
        .await
        .expect("the listing succeeds");
    let planned = session
        .load_table(&ident("events"))
        .await
        .expect("a Parquet table plans");

    let columns = [
        ("id", "bigint"),
        ("payload", "struct<x:int,y:string>"),
        ("p_int", "int"),
        ("p_date", "date"),
        ("p_str", "string"),
    ];
    let expected = CatalogTable {
        ident: ident("events"),
        table_type: CatalogTableType::Table,
        storage_location: Some("s3://bucket/warehouse/events".to_string()),
        format: TableFormat::Parquet,
        vended_credential_key: None,
        partition_columns: vec!["p_int".into(), "p_date".into(), "p_str".into()],
        columns: columns
            .map(|(name, hive_type)| CatalogColumn {
                name: name.into(),
                source_type: ColumnSourceType::Glue {
                    hive_type: hive_type.into(),
                },
            })
            .to_vec(),
        metadata_location: None,
    };
    assert_eq!(planned, expected);
    assert_eq!(listing.tables, [expected]);
}

/// Scenario: Partitions carry their Glue values, location, and format
#[tokio::test]
async fn partitions_carry_glue_values_the_default_partition_as_null_and_their_own_format() {
    let parquet = Some(PARQUET_INPUT_FORMAT);
    let page = [
        partition(
            &["1", "2024-01-01", "a"],
            "s3://bucket/events/p_int=7/p=z/",
            parquet,
        ),
        partition(
            &["__HIVE_DEFAULT_PARTITION__", "2024-01-02", "b"],
            "s3://bucket/d/",
            parquet,
        ),
        partition(&["3", "2024-01-03", "c"], "s3://other/elsewhere", None),
        partition(
            &["9", "2024-01-09", "z"],
            "s3://bucket/events/p_int=9/",
            Some(ORC_INPUT_FORMAT),
        ),
    ];
    let (session, requests) = fake_glue(move |label, _| match label {
        "GetPartitions" => ok(json!({ "Partitions": page })),
        other => unexpected(other),
    })
    .await;

    let partitions = session
        .partitions(&parquet_table(&["p_int", "p_date", "p_str"]))
        .await
        .expect("well-formed partitions convert");
    let unpartitioned = session
        .partitions(&parquet_table(&[]))
        .await
        .expect("an unpartitioned table needs no GetPartitions");

    let expected = |values: [Option<&str>; 3], location: &str, format| CatalogPartition {
        values: ["p_int", "p_date", "p_str"]
            .into_iter()
            .zip(values)
            .map(|(key, value)| (key.to_string(), value.map(str::to_string)))
            .collect(),
        location: location.to_string(),
        format,
    };
    assert_eq!(
        partitions,
        [
            expected(
                [Some("1"), Some("2024-01-01"), Some("a")],
                "s3://bucket/events/p_int=7/p=z",
                PartitionFormat::Parquet
            ),
            expected(
                [None, Some("2024-01-02"), Some("b")],
                "s3://bucket/d",
                PartitionFormat::Parquet
            ),
            expected(
                [Some("3"), Some("2024-01-03"), Some("c")],
                "s3://other/elsewhere",
                PartitionFormat::Parquet
            ),
            expected(
                [Some("9"), Some("2024-01-09"), Some("z")],
                "s3://bucket/events/p_int=9",
                PartitionFormat::Unsupported {
                    input_format: ORC_INPUT_FORMAT.to_string()
                },
            ),
        ]
    );
    assert_eq!(
        unpartitioned,
        [CatalogPartition {
            values: BTreeMap::new(),
            location: "s3://bucket/events".to_string(),
            format: PartitionFormat::Parquet,
        }]
    );
    let requests = requests.lock().unwrap();
    let [request] = requests.as_slice() else {
        panic!("the unpartitioned table issues no GetPartitions: {requests:?}");
    };
    let body: Value = serde_json::from_str(request_body(request)).expect("a JSON request");
    assert_eq!(body["ExcludeColumnSchema"], true);
}

#[tokio::test]
async fn a_malformed_partition_fails_naming_its_location_or_values() {
    let cases = [
        (
            partition(&["1"], &format!("s3://bucket/{SECRET_KEY}/p_int=1/"), None),
            "p_int=1",
        ),
        (json!({ "Values": ["1", "2024-01-01"] }), "2024-01-01"),
    ];

    for (malformed, named) in cases {
        let (session, _) = fake_glue(move |_, _| ok(json!({ "Partitions": [malformed] }))).await;

        let message = user_message(
            session
                .partitions(&parquet_table(&["p_int", "p_date"]))
                .await
                .expect_err("a malformed partition fails"),
        );

        assert!(message.contains(named), "{message}");
        assert_no_credential(&message);
    }
}

/// Scenario: Every listing follows its continuation tokens and stops on an empty page
#[tokio::test]
async fn a_listing_follows_a_non_empty_token_and_stops_on_an_empty_token_or_page() {
    let (session, requests) =
        fake_glue(
            |label, request| match (label, request["NextToken"].as_str()) {
                ("GetTables", None) => ok(json!({
                    "TableList": [table("events", json!({}), Some(PARQUET_INPUT_FORMAT))],
                    "NextToken": "tables-2",
                })),
                ("GetTables", Some("tables-2")) => {
                    ok(json!({"TableList": [], "NextToken": "tables-3"}))
                }
                ("GetPartitions", None) => ok(json!({
                    "Partitions": [partition(&["1"], "s3://bucket/events/p=1", None)],
                    "NextToken": "partitions-2",
                })),
                ("GetPartitions", Some("partitions-2")) => ok(json!({
                    "Partitions": [partition(&["2"], "s3://bucket/events/p=2", None)],
                    "NextToken": "",
                })),
                (other, _) => unexpected(other),
            },
        )
        .await;

    let listing = session
        .list_tables(&[DATABASE.to_string()])
        .await
        .expect("the listing succeeds");
    let partitions = session
        .partitions(&parquet_table(&["p"]))
        .await
        .expect("the partitions list");

    assert_eq!(listing.tables.len(), 1);
    assert_eq!(partitions.len(), 2, "a short page with a token continues");
    assert_eq!(
        labels(&requests),
        ["GetTables", "GetTables", "GetPartitions", "GetPartitions"],
        "an empty page or an empty token ends a listing"
    );
}

/// Scenario: A Glue NAMESPACE names exactly one database
#[tokio::test]
async fn a_namespace_naming_more_or_less_than_one_database_fails_before_any_glue_call() {
    let (session, requests) = fake_glue(|label, _| unexpected(label)).await;

    for namespace in [
        vec!["sales".to_string(), "eu".to_string()],
        Vec::new(),
        vec![String::new()],
    ] {
        let message = user_message(
            session
                .list_tables(&namespace)
                .await
                .expect_err("only one database segment is accepted"),
        );

        assert!(message.contains("exactly one database"), "{message}");
    }
    assert!(
        requests.lock().unwrap().is_empty(),
        "no Glue call is issued"
    );
}

/// Scenario: A failed Glue call names the operation, the subject, and the cause
/// Scenario: A clock-skew signing failure names the clock
/// Scenario: Every Glue call retries within a bounded time
#[tokio::test]
async fn a_failed_glue_call_names_the_operation_the_subject_and_the_cause() {
    let echoed = format!("request by {ACCESS_KEY} with {SECRET_KEY} and {SESSION_TOKEN} denied");
    let clock = "clock differs from AWS time";
    let mismatch = "The request signature we calculated does not match";
    let replies = [
        (
            service_error("EntityNotFoundException", "Database sales not found."),
            "does not exist",
        ),
        (
            service_error("AccessDeniedException", &echoed),
            "AccessDeniedException: request by",
        ),
        (
            (
                404,
                json!({"__type": "AccessDeniedException", "Message": "no"}),
            ),
            "AccessDeniedException: no",
        ),
        (
            service_error("InvalidInputException", "bad name"),
            "InvalidInputException: bad name",
        ),
        ((503, json!({})), "HTTP 503"),
        (service_error("RequestTimeTooSkewed", ""), clock),
        (service_error("RequestExpired", ""), clock),
        (
            service_error(
                "InvalidSignatureException",
                "Signature expired: 20260101T000000Z is now earlier than 20260101T005500Z",
            ),
            clock,
        ),
        (
            service_error("InvalidSignatureException", "Signature not yet current: x"),
            clock,
        ),
        (
            service_error("InvalidSignatureException", mismatch),
            mismatch,
        ),
    ];
    for (reply, cause) in replies {
        let status = reply.0;
        let (session, requests) = fake_glue(move |_, _| reply.clone()).await;

        let message = listing_error(&session).await;

        assert!(message.contains("GetTables failed"), "{message}");
        assert!(message.contains("database 'sales'"), "{message}");
        assert!(message.contains(cause), "{message}");
        assert_eq!(message.contains(clock), cause == clock, "{message}");
        assert!(!message.to_lowercase().contains("credential"), "{message}");
        assert_no_credential(&message);
        if status == 503 {
            assert_eq!(
                requests.lock().unwrap().len(),
                5,
                "a transient failure takes 5 attempts"
            );
        }
    }

    let unanswered = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let refused = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let refused_address = format!("http://{}", refused.local_addr().expect("local_addr"));
    drop(refused);
    for (address, cause) in [
        (
            format!("http://{}", unanswered.local_addr().expect("local_addr")),
            "within 30 seconds",
        ),
        (
            refused_address,
            "GetTables request for database 'sales' failed: ",
        ),
    ] {
        let message = listing_error(&session_at(&address)).await;

        assert!(message.contains("database 'sales'"), "{message}");
        assert!(message.contains(cause), "{message}");
        assert_no_credential(&message);
    }
}

/// Scenario: A Glue table resolves from its recorded identifier and fails loud when it is no longer plannable
#[tokio::test]
async fn a_dropped_or_no_longer_plannable_table_fails_the_planning_load_naming_it() {
    let (dropped, _) =
        fake_glue(|_, _| service_error("EntityNotFoundException", "Table orders not found.")).await;
    let (reregistered, _) = fake_glue(|_, _| {
        ok(json!({ "Table": table("orders", json!({}), Some(ORC_INPUT_FORMAT)) }))
    })
    .await;

    let dropped = user_message(
        dropped
            .load_table(&ident("orders"))
            .await
            .expect_err("a dropped table cannot be planned"),
    );
    let reregistered = user_message(
        reregistered
            .load_table(&ident("orders"))
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

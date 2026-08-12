//! Contract tests for the native Unity Catalog REST client: listing, single-table
//! load, pagination, credential-safe failures, and the OSS/Databricks request
//! shape. Every request is served by an in-process mock; no live network.

use super::*;
use crate::test_support::base_creds;
use crate::unity::mock_server::spawn;
use crate::{CatalogClient, CatalogTableIdent, CatalogTableType, ColumnSourceType};
use exasol_udf_sdk::error::UdfError;

const PAT_SENTINEL: &str = "PAT_SECRET_SENTINEL_VALUE";

fn tables_page_body() -> String {
    r#"{"tables":[
        {"name":"orders","catalog_name":"cat","schema_name":"sch","full_name":"cat.sch.orders","table_type":"MANAGED","data_source_format":"DELTA","storage_location":"s3://bucket/orders","table_id":"uuid-1","columns":[
            {"name":"id","type_name":"LONG","type_precision":0,"type_scale":0,"position":0},
            {"name":"amount","type_name":"DECIMAL","type_precision":10,"type_scale":2,"position":1}
        ]},
        {"name":"orders_summary","catalog_name":"cat","schema_name":"sch","full_name":"cat.sch.orders_summary","table_type":"VIEW","data_source_format":null,"columns":[
            {"name":"total","type_name":"DOUBLE"}
        ]}
    ]}"#
    .to_string()
}

fn single_table_body() -> String {
    r#"{"name":"orders","catalog_name":"cat","schema_name":"sch","full_name":"cat.sch.orders","table_type":"MANAGED","data_source_format":"DELTA","storage_location":"s3://bucket/orders","table_id":"uuid-1","columns":[
        {"name":"id","type_name":"LONG"},
        {"name":"amount","type_name":"DECIMAL","type_precision":10,"type_scale":2}
    ]}"#
    .to_string()
}

fn empty_tables_body() -> String {
    r#"{"tables":[]}"#.to_string()
}

#[tokio::test]
async fn lists_tables_in_catalog_schema() {
    let server = spawn(|_req| (200, tables_page_body())).await;
    let session = UnityCatalogSession::new(&server.base_url, base_creds());

    let listing = session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect("list failed");

    assert_eq!(
        listing
            .tables
            .iter()
            .map(|table| table.ident.name.as_str())
            .collect::<Vec<_>>(),
        vec!["orders"],
        "the VIEW entry is routed to skipped, not returned"
    );

    let orders = &listing.tables[0];
    assert_eq!(
        orders.ident.namespace,
        vec!["cat".to_string(), "sch".to_string()]
    );
    assert_eq!(orders.table_type, CatalogTableType::Table);
    assert_eq!(
        orders.storage_location.as_deref(),
        Some("s3://bucket/orders")
    );
    assert_eq!(
        orders
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "amount"]
    );
    assert_eq!(
        orders.columns[1].source_type,
        ColumnSourceType::Unity {
            type_name: "DECIMAL".to_string(),
            precision: 10,
            scale: 2,
        }
    );

    assert_eq!(
        listing.skipped,
        vec![SkippedTable {
            ident: CatalogTableIdent {
                namespace: vec!["cat".to_string(), "sch".to_string()],
                name: "orders_summary".to_string(),
            },
            reason: SkipReason::NotDeltaBaseTable {
                detail: "table_type=VIEW".to_string(),
            },
        }],
        "the VIEW entry is skipped with its table_type reason"
    );

    let requests = server.requests();
    assert_eq!(
        requests.len(),
        1,
        "one list request, no per-table get-table"
    );
    assert!(
        requests[0]
            .target
            .starts_with("/api/2.1/unity-catalog/tables?"),
        "addresses the standard tables endpoint: {}",
        requests[0].target
    );
    assert!(requests[0].target.contains("catalog_name=cat"));
    assert!(requests[0].target.contains("schema_name=sch"));
    assert!(
        !requests[0].target.contains("omit_columns"),
        "must not set omit_columns: {}",
        requests[0].target
    );
}

#[tokio::test]
async fn includes_managed_and_external_delta_base_tables() {
    let body = r#"{"tables":[
        {"name":"orders","table_type":"MANAGED","data_source_format":"DELTA","storage_location":"s3://bucket/orders","columns":[]},
        {"name":"external_orders","table_type":"EXTERNAL","data_source_format":"DELTA","storage_location":"s3://bucket/external_orders","columns":[]},
        {"name":"orders_clone","table_type":"MANAGED","data_source_format":"DELTA","storage_location":"s3://bucket/orders_clone","columns":[]}
    ]}"#
    .to_string();
    let server = spawn(move |_req| (200, body.clone())).await;
    let session = UnityCatalogSession::new(&server.base_url, base_creds());

    let listing = session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect("list failed");

    assert!(
        listing.skipped.is_empty(),
        "every MANAGED/EXTERNAL DELTA entry admits as a Delta base table"
    );
    assert_eq!(
        listing
            .tables
            .iter()
            .map(|table| table.ident.name.as_str())
            .collect::<Vec<_>>(),
        vec!["orders", "external_orders", "orders_clone"],
        "the MANAGED, EXTERNAL, and shallow-clone-shaped entries are all returned"
    );
    for table in &listing.tables {
        assert_eq!(table.table_type, CatalogTableType::Table);
    }
}

#[tokio::test]
async fn skips_view_non_delta_and_other_type_with_reason() {
    let body = r#"{"tables":[
        {"name":"orders_summary","table_type":"VIEW","data_source_format":null,"columns":[]},
        {"name":"legacy_orders","table_type":"MANAGED","data_source_format":"ICEBERG","columns":[]},
        {"name":"streaming_orders","table_type":"STREAMING_TABLE","data_source_format":"DELTA","columns":[]}
    ]}"#
    .to_string();
    let server = spawn(move |_req| (200, body.clone())).await;
    let session = UnityCatalogSession::new(&server.base_url, base_creds());

    let listing = session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect("list failed");

    assert!(
        listing.tables.is_empty(),
        "none of the VIEW, non-DELTA, or other-type entries are Delta base tables"
    );
    assert_eq!(
        listing.skipped,
        vec![
            SkippedTable {
                ident: CatalogTableIdent {
                    namespace: vec!["cat".to_string(), "sch".to_string()],
                    name: "orders_summary".to_string(),
                },
                reason: SkipReason::NotDeltaBaseTable {
                    detail: "table_type=VIEW".to_string(),
                },
            },
            SkippedTable {
                ident: CatalogTableIdent {
                    namespace: vec!["cat".to_string(), "sch".to_string()],
                    name: "legacy_orders".to_string(),
                },
                reason: SkipReason::NotDeltaBaseTable {
                    detail: "data_source_format=ICEBERG".to_string(),
                },
            },
            SkippedTable {
                ident: CatalogTableIdent {
                    namespace: vec!["cat".to_string(), "sch".to_string()],
                    name: "streaming_orders".to_string(),
                },
                reason: SkipReason::NotDeltaBaseTable {
                    detail: "table_type=STREAMING_TABLE".to_string(),
                },
            },
        ]
    );
}

#[tokio::test]
async fn loads_table_metadata_with_columns() {
    let server = spawn(|_req| (200, single_table_body())).await;
    let session = UnityCatalogSession::new(&server.base_url, base_creds());
    let ident = CatalogTableIdent {
        namespace: vec!["cat".to_string(), "sch".to_string()],
        name: "orders".to_string(),
    };

    let table = session.load_table(&ident).await.expect("load failed");

    assert_eq!(table.ident, ident);
    assert_eq!(
        table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "amount"]
    );

    let requests = server.requests();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(
        requests[0].target,
        "/api/2.1/unity-catalog/tables/cat.sch.orders"
    );
}

#[tokio::test]
async fn loads_table_percent_encodes_reserved_full_name_segments() {
    let server = spawn(|_req| (200, single_table_body())).await;
    let session = UnityCatalogSession::new(&server.base_url, base_creds());
    let ident = CatalogTableIdent {
        namespace: vec!["cat".to_string(), "sch".to_string()],
        name: "weird/name #1".to_string(),
    };

    session.load_table(&ident).await.expect("load failed");

    let requests = server.requests();
    assert_eq!(
        requests[0].target, "/api/2.1/unity-catalog/tables/cat.sch.weird%2Fname%20%231",
        "reserved characters in the dotted full name are percent-encoded within the one path segment, dots kept literal"
    );
}

#[tokio::test]
async fn follows_pagination_across_pages() {
    let server = spawn(|req| {
        if req.target.contains("page_token=") {
            (
                200,
                r#"{"tables":[{"name":"t2","table_type":"MANAGED","data_source_format":"DELTA","storage_location":"s3://b/t2","columns":[]}]}"#
                    .to_string(),
            )
        } else {
            (
                200,
                r#"{"tables":[{"name":"t1","table_type":"MANAGED","data_source_format":"DELTA","storage_location":"s3://b/t1","columns":[]}],"next_page_token":"PAGE2"}"#
                    .to_string(),
            )
        }
    })
    .await;
    let session = UnityCatalogSession::new(&server.base_url, base_creds());

    let listing = session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect("list failed");

    assert_eq!(
        listing
            .tables
            .iter()
            .map(|table| table.ident.name.as_str())
            .collect::<Vec<_>>(),
        vec!["t1", "t2"],
        "every page's entries are returned in page order"
    );
    let requests = server.requests();
    assert_eq!(requests.len(), 2, "one request per page");
    assert!(!requests[0].target.contains("page_token="));
    assert!(requests[1].target.contains("page_token=PAGE2"));
}

#[tokio::test]
async fn request_failure_is_credential_safe_error() {
    // The mock echoes the bearer it received into its error body, so the test
    // proves the token is stripped from the surfaced error.
    let server = spawn(|req| {
        (
            500,
            format!(
                "internal error saw {}",
                req.authorization.clone().unwrap_or_default()
            ),
        )
    })
    .await;
    let mut creds = base_creds();
    creds.token = Some(PAT_SENTINEL.to_string());
    let session = UnityCatalogSession::new(&server.base_url, creds);

    let err = session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect_err("a 500 must surface an error");

    let UdfError::User(msg) = err else {
        panic!("expected a UdfError::User variant");
    };
    assert!(
        msg.contains("list tables"),
        "error names the request kind: {msg}"
    );
    assert!(msg.contains("500"), "error carries the status: {msg}");
    assert!(
        !msg.contains(PAT_SENTINEL),
        "the bearer token must be stripped from the error: {msg}"
    );
}

#[tokio::test]
async fn identical_request_shape_oss_and_databricks() {
    let oss = spawn(|_req| (200, empty_tables_body())).await;
    let databricks = spawn(|_req| (200, empty_tables_body())).await;

    // OSS with auth disabled; Databricks-managed reached with a PAT.
    let oss_session = UnityCatalogSession::new(&oss.base_url, base_creds());
    let mut databricks_creds = base_creds();
    databricks_creds.token = Some("dbx-pat".to_string());
    let databricks_session = UnityCatalogSession::new(&databricks.base_url, databricks_creds);

    oss_session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect("oss list failed");
    databricks_session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect("databricks list failed");

    let oss_reqs = oss.requests();
    let databricks_reqs = databricks.requests();
    assert_eq!(oss_reqs[0].method, databricks_reqs[0].method);
    assert_eq!(
        oss_reqs[0].target, databricks_reqs[0].target,
        "request shape must be identical across OSS and Databricks"
    );
    assert_eq!(
        oss_reqs[0].authorization, None,
        "the OSS auth-off session sends no Authorization header"
    );
    assert_eq!(
        databricks_reqs[0].authorization.as_deref(),
        Some("Bearer dbx-pat"),
        "only the resolved auth strategy differs"
    );
}

#[tokio::test]
async fn posts_temporary_table_credentials() {
    let server = spawn(|_req| {
        (
            200,
            r#"{"aws_temp_credentials":{"access_key_id":"AK","secret_access_key":"SK","session_token":"ST"}}"#
                .to_string(),
        )
    })
    .await;
    let session = UnityCatalogSession::new(&server.base_url, base_creds());

    let vended = session
        .temporary_table_credentials("table-uuid-1", "READ")
        .await
        .expect("post failed");

    assert_eq!(
        vended
            .aws_temp_credentials
            .expect("aws credentials")
            .access_key_id,
        "AK"
    );
    let requests = server.requests();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(
        requests[0].target,
        "/api/2.1/unity-catalog/temporary-table-credentials"
    );
    assert!(
        requests[0].body.contains("table-uuid-1"),
        "body carries the table_id: {}",
        requests[0].body
    );
    assert!(
        requests[0].body.contains("READ"),
        "body carries the operation: {}",
        requests[0].body
    );
}

#[test]
fn delta_base_skip_reason_admits_a_table_with_delta_format() {
    assert_eq!(delta_base_skip_reason("MANAGED", Some("DELTA")), None);
}

#[test]
fn delta_base_skip_reason_type_wins_over_format_for_a_view_even_when_delta() {
    assert_eq!(
        delta_base_skip_reason("VIEW", Some("DELTA")),
        Some(SkipReason::NotDeltaBaseTable {
            detail: "table_type=VIEW".to_string()
        })
    );
}

#[test]
fn delta_base_skip_reason_type_wins_over_format_for_other_even_when_delta() {
    assert_eq!(
        delta_base_skip_reason("STREAMING_TABLE", Some("DELTA")),
        Some(SkipReason::NotDeltaBaseTable {
            detail: "table_type=STREAMING_TABLE".to_string()
        })
    );
}

/// The detail must name the spelling the catalog actually sent, for every
/// disqualifying `table_type` — including one the neutral mapping folds onto
/// `View`, whose raw spelling would otherwise be lost.
#[test]
fn delta_base_skip_reason_names_the_raw_table_type_it_was_handed() {
    for raw in [
        "VIEW",
        "MATERIALIZED_VIEW",
        "STREAMING_TABLE",
        "FOREIGN",
        "MANAGED_SHALLOW_CLONE",
    ] {
        assert_eq!(
            delta_base_skip_reason(raw, Some("DELTA")),
            Some(SkipReason::NotDeltaBaseTable {
                detail: format!("table_type={raw}")
            }),
            "raw table_type {raw} must be reported by its own spelling"
        );
    }
}

#[test]
fn delta_base_skip_reason_reports_a_non_delta_format_verbatim() {
    assert_eq!(
        delta_base_skip_reason("MANAGED", Some("ICEBERG")),
        Some(SkipReason::NotDeltaBaseTable {
            detail: "data_source_format=ICEBERG".to_string()
        })
    );
    assert_eq!(
        delta_base_skip_reason("EXTERNAL", Some("CSV")),
        Some(SkipReason::NotDeltaBaseTable {
            detail: "data_source_format=CSV".to_string()
        })
    );
}

#[test]
fn delta_base_skip_reason_reports_an_absent_format() {
    assert_eq!(
        delta_base_skip_reason("MANAGED", None),
        Some(SkipReason::NotDeltaBaseTable {
            detail: "data_source_format=absent".to_string()
        })
    );
}

#[test]
fn delta_base_skip_reason_rejects_a_lowercase_or_mixed_case_delta_spelling() {
    assert_eq!(
        delta_base_skip_reason("MANAGED", Some("delta")),
        Some(SkipReason::NotDeltaBaseTable {
            detail: "data_source_format=delta".to_string()
        })
    );
    assert_eq!(
        delta_base_skip_reason("MANAGED", Some("Delta")),
        Some(SkipReason::NotDeltaBaseTable {
            detail: "data_source_format=Delta".to_string()
        })
    );
}

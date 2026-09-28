use super::*;
use crate::test_support::*;
use iceberg::spec::{PrimitiveType, Type};
use std::sync::{Arc, Mutex};

struct FixedCatalogClient;

impl FixedCatalogClient {
    fn iceberg_table(namespace: Vec<String>) -> CatalogTable {
        CatalogTable {
            ident: CatalogTableIdent {
                namespace,
                name: "orders".to_string(),
            },
            table_type: CatalogTableType::Table,
            storage_location: Some("s3://warehouse/orders".to_string()),
            format: TableFormat::Iceberg,
            vended_credential_key: None,
            partition_columns: Vec::new(),
            columns: vec![CatalogColumn {
                name: "order_id".to_string(),
                source_type: ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Long)),
            }],
        }
    }

    fn unity_delta_table(namespace: Vec<String>) -> CatalogTable {
        CatalogTable {
            ident: CatalogTableIdent {
                namespace,
                name: "payments".to_string(),
            },
            table_type: CatalogTableType::Table,
            storage_location: Some("s3://warehouse/payments".to_string()),
            format: TableFormat::Delta,
            vended_credential_key: Some("payments-uuid".to_string()),
            partition_columns: Vec::new(),
            columns: vec![CatalogColumn {
                name: "total".to_string(),
                source_type: ColumnSourceType::Unity {
                    type_name: "DECIMAL".to_string(),
                    precision: 10,
                    scale: 2,
                    type_json: None,
                },
            }],
        }
    }
}

impl CatalogClient for FixedCatalogClient {
    fn list_tables(
        &self,
        namespace: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>> {
        let namespace = namespace.to_vec();
        Box::pin(async move {
            Ok(CatalogListing {
                tables: vec![
                    Self::iceberg_table(namespace.clone()),
                    Self::unity_delta_table(namespace.clone()),
                ],
                skipped: vec![
                    SkippedTable {
                        ident: CatalogTableIdent {
                            namespace: namespace.clone(),
                            name: "not_a_table".to_string(),
                        },
                        reason: SkipReason::NotLoadableIcebergTable,
                    },
                    SkippedTable {
                        ident: CatalogTableIdent {
                            namespace,
                            name: "orders_summary".to_string(),
                        },
                        reason: SkipReason::NotDeltaBaseTable {
                            detail: "table_type=VIEW".to_string(),
                        },
                    },
                ],
            })
        })
    }

    fn load_table(
        &self,
        ident: &CatalogTableIdent,
    ) -> Pin<Box<dyn Future<Output = Result<CatalogTable, UdfError>> + Send + '_>> {
        let ident = ident.clone();
        Box::pin(async move {
            let mut table = Self::iceberg_table(ident.namespace);
            table.ident.name = ident.name;
            Ok(table)
        })
    }

    fn load_tables<'a>(
        &'a self,
        idents: &'a [CatalogTableIdent],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let mut listing = CatalogListing::default();
            for ident in idents {
                listing.tables.push(self.load_table(ident).await?);
            }
            Ok(listing)
        })
    }
}

fn boxed_client() -> Box<dyn CatalogClient> {
    Box::new(FixedCatalogClient)
}

#[tokio::test]
async fn boxed_client_lists_neutral_tables_and_skipped_entries_with_reasons() {
    let client = boxed_client();

    let listing = client
        .list_tables(&["prod".to_string()])
        .await
        .expect("listing failed");

    assert_eq!(
        listing
            .tables
            .iter()
            .map(|table| table.ident.name.as_str())
            .collect::<Vec<_>>(),
        vec!["orders", "payments"]
    );
    assert_eq!(
        listing.skipped,
        vec![
            SkippedTable {
                ident: CatalogTableIdent {
                    namespace: vec!["prod".to_string()],
                    name: "not_a_table".to_string(),
                },
                reason: SkipReason::NotLoadableIcebergTable,
            },
            SkippedTable {
                ident: CatalogTableIdent {
                    namespace: vec!["prod".to_string()],
                    name: "orders_summary".to_string(),
                },
                reason: SkipReason::NotDeltaBaseTable {
                    detail: "table_type=VIEW".to_string(),
                },
            },
        ]
    );
}

#[tokio::test]
async fn namespace_segments_reach_the_client_unjoined() {
    let namespace = vec!["prod.eu".to_string(), "finance".to_string()];
    let client = boxed_client();

    let listing = client
        .list_tables(&namespace)
        .await
        .expect("listing failed");

    for table in &listing.tables {
        assert_eq!(table.ident.namespace, namespace);
    }
}

#[tokio::test]
async fn boxed_client_loads_one_table_by_segmented_identifier() {
    let ident = CatalogTableIdent {
        namespace: vec!["prod".to_string(), "finance".to_string()],
        name: "orders".to_string(),
    };
    let client = boxed_client();

    let table = client.load_table(&ident).await.expect("load failed");

    assert_eq!(table.ident, ident);
    assert_eq!(
        table.storage_location.as_deref(),
        Some("s3://warehouse/orders")
    );
}

#[test]
fn a_boxed_catalog_client_is_send_and_sync() {
    fn requires_send_sync<T: Send + Sync + ?Sized>() {}

    requires_send_sync::<dyn CatalogClient>();
    requires_send_sync::<Box<dyn CatalogClient>>();
}

#[tokio::test]
async fn an_iceberg_column_carries_its_iceberg_source_type() {
    let listing = boxed_client()
        .list_tables(&["prod".to_string()])
        .await
        .expect("listing failed");

    let column = &listing.tables[0].columns[0];

    assert_eq!(column.name, "order_id");
    assert_eq!(
        column.source_type,
        ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Long))
    );
}

#[tokio::test]
async fn a_unity_decimal_column_carries_its_precision_and_scale() {
    let listing = boxed_client()
        .list_tables(&["prod".to_string()])
        .await
        .expect("listing failed");

    match &listing.tables[1].columns[0].source_type {
        ColumnSourceType::Unity {
            type_name,
            precision,
            scale,
            ..
        } => assert_eq!((type_name.as_str(), *precision, *scale), ("DECIMAL", 10, 2)),
        ColumnSourceType::Iceberg(ty) => panic!("expected a Unity source type, got iceberg {ty}"),
        ColumnSourceType::Parquet(tag) => {
            panic!("expected a Unity source type, got a Parquet tag {tag}")
        }
    }
}

#[derive(Default)]
struct RequestLog {
    oauth_grants: usize,
    load_table_names: Vec<String>,
    load_table_queries: Vec<String>,
    delegation_headers: usize,
    in_flight_loads: usize,
    max_in_flight_loads: usize,
}

/// Every connection is served concurrently, and each loadTable answer is delayed so
/// overlapping loads register in `max_in_flight_loads`.
async fn spawn_mock_catalog(
    namespace: &[&str],
    tables: &[&str],
    not_loadable: &[&str],
    server_error: &[&str],
) -> (String, Arc<Mutex<RequestLog>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let uri = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let log = Arc::new(Mutex::new(RequestLog::default()));
    let not_loadable: Vec<String> = not_loadable.iter().map(|s| s.to_string()).collect();
    let server_error: Vec<String> = server_error.iter().map(|s| s.to_string()).collect();
    let list_tables_body = list_tables_page(namespace, tables, None);

    let server_log = log.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let server_log = server_log.clone();
            let not_loadable = not_loadable.clone();
            let server_error = server_error.clone();
            let list_tables_body = list_tables_body.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                let request = String::from_utf8_lossy(&buf[..n]).into_owned();
                let request_line = request.lines().next().unwrap_or("");
                let mut fields = request_line.split_whitespace();
                let method = fields.next().unwrap_or("");
                let path = fields.next().unwrap_or("");
                let (path_no_query, query) = path.split_once('?').unwrap_or((path, ""));

                let (status, body) = if method == "POST" {
                    server_log.lock().unwrap().oauth_grants += 1;
                    (
                        "200 OK",
                        r#"{"access_token":"mock-access-token","token_type":"bearer","expires_in":3600}"#
                            .to_string(),
                    )
                } else if path_no_query.ends_with("/v1/config") {
                    ("200 OK", r#"{"overrides":{},"defaults":{}}"#.to_string())
                } else if path_no_query.ends_with("/namespaces") {
                    ("200 OK", r#"{"namespaces":[]}"#.to_string())
                } else if path_no_query.ends_with("/tables") {
                    ("200 OK", list_tables_body.clone())
                } else if let Some(offset) = path_no_query.find("/tables/") {
                    let table = &path_no_query[offset + "/tables/".len()..];
                    {
                        let mut log = server_log.lock().unwrap();
                        log.load_table_names.push(table.to_string());
                        log.load_table_queries.push(query.to_string());
                        if request.lines().any(|line| {
                            line.to_ascii_lowercase()
                                .starts_with("x-iceberg-access-delegation:")
                        }) {
                            log.delegation_headers += 1;
                        }
                        log.in_flight_loads += 1;
                        log.max_in_flight_loads = log.max_in_flight_loads.max(log.in_flight_loads);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                    server_log.lock().unwrap().in_flight_loads -= 1;
                    if server_error.iter().any(|t| t == table) {
                        ("500 Internal Server Error", String::new())
                    } else if not_loadable.iter().any(|t| t == table) {
                        (
                            "404 Not Found",
                            r#"{"error":"not an iceberg table"}"#.to_string(),
                        )
                    } else {
                        ("200 OK", load_table_body(table))
                    }
                } else {
                    ("500 Internal Server Error", String::new())
                };

                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });

    (uri, log)
}

fn oauth_creds() -> ConnectionCreds {
    let mut creds = base_creds();
    creds.client_id = Some("mock-client-id".into());
    creds.client_secret = Some("mock-client-secret".into());
    creds
}

#[tokio::test]
async fn empty_load_batch_builds_no_session_and_no_grant() {
    let client = IcebergRestCatalogClient::new("http://127.0.0.1:1".into(), oauth_creds());

    let listing = client
        .load_tables(&[])
        .await
        .expect("an empty ident batch must resolve with no session build and no grant");

    assert!(
        listing.tables.is_empty(),
        "an empty batch resolves no table"
    );
    assert!(listing.skipped.is_empty(), "an empty batch skips nothing");
}

#[tokio::test]
async fn list_tables_over_empty_namespace_lists_nothing() {
    let (uri, log) = spawn_mock_catalog(&["sales"], &[], &[], &[]).await;
    let client = IcebergRestCatalogClient::new(uri, oauth_creds());

    let listing = client
        .list_tables(&["sales".to_string()])
        .await
        .expect("an empty namespace must list nothing");

    assert!(listing.tables.is_empty());
    assert!(listing.skipped.is_empty());
    assert_eq!(log.lock().unwrap().oauth_grants, 1);
}

#[tokio::test]
async fn enumeration_builds_exactly_one_session() {
    let (uri, log) =
        spawn_mock_catalog(&["sales"], &["orders", "customers", "returns"], &[], &[]).await;
    let client = IcebergRestCatalogClient::new(uri, oauth_creds());

    let listing = client
        .list_tables(&["sales".to_string()])
        .await
        .expect("list_tables failed");

    assert_eq!(listing.tables.len(), 3, "every enumerated table resolves");
    assert!(listing.skipped.is_empty());
    for table in &listing.tables {
        let column_names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            column_names,
            vec!["id", "name"],
            "columns resolve in schema order and original case for {}",
            table.ident.name
        );
    }
    let log = log.lock().unwrap();
    assert_eq!(
        log.oauth_grants, 1,
        "one session serves the listing and every load; per-table sessions would grant four times"
    );
    assert!(
        log.max_in_flight_loads >= 2,
        "table loads must overlap, not run one at a time: {}",
        log.max_in_flight_loads
    );
    assert_eq!(
        log.load_table_queries,
        vec!["snapshots=refs"; 3],
        "enumeration reads only the current schema, so it leaves unreferenced snapshots out"
    );
}

#[tokio::test]
async fn enumeration_requests_no_storage_credentials_even_when_vending_is_on() {
    let (uri, log) = spawn_mock_catalog(&["sales"], &["orders"], &[], &[]).await;
    let mut creds = creds_no_auth();
    creds.use_vended_credentials = true;
    let client = IcebergRestCatalogClient::new(uri, creds);

    client
        .list_tables(&["sales".to_string()])
        .await
        .expect("list_tables failed");

    let log = log.lock().unwrap();
    assert_eq!(log.load_table_names, vec!["orders"]);
    assert_eq!(
        log.delegation_headers, 0,
        "createVirtualSchema never uses vended credentials, so it must not ask the catalog to mint them"
    );
}

#[tokio::test]
async fn load_tables_resolves_only_the_named_tables_and_skips_a_vanished_one() {
    let (uri, log) = spawn_mock_catalog(&["sales"], &["orders", "customers"], &["gone"], &[]).await;
    let client = IcebergRestCatalogClient::new(uri, oauth_creds());
    let ident = |name: &str| CatalogTableIdent {
        namespace: vec!["sales".to_string()],
        name: name.to_string(),
    };

    let listing = client
        .load_tables(&[ident("orders"), ident("gone")])
        .await
        .expect("load_tables failed");

    assert_eq!(
        listing
            .tables
            .iter()
            .map(|t| t.ident.name.as_str())
            .collect::<Vec<_>>(),
        ["orders"]
    );
    assert_eq!(
        listing.skipped,
        vec![SkippedTable {
            ident: ident("gone"),
            reason: SkipReason::NotLoadableIcebergTable,
        }]
    );
    let log = log.lock().unwrap();
    assert_eq!(log.oauth_grants, 1);
    assert_eq!(
        log.load_table_names,
        vec!["orders", "gone"],
        "no listing request precedes the loads"
    );
}

#[tokio::test]
async fn iceberg_client_tags_every_table_iceberg_with_no_vending_key() {
    let (uri, _log) = spawn_mock_catalog(&["sales"], &["orders", "customers"], &[], &[]).await;
    let client = IcebergRestCatalogClient::new(uri, creds_no_auth());

    let listing = client
        .list_tables(&["sales".to_string()])
        .await
        .expect("list_tables failed");
    let loaded = client
        .load_table(&CatalogTableIdent {
            namespace: vec!["sales".to_string()],
            name: "orders".to_string(),
        })
        .await
        .expect("load_table failed");

    for table in listing.tables.iter().chain(std::iter::once(&loaded)) {
        assert_eq!(
            table.format,
            TableFormat::Iceberg,
            "{} must carry the Iceberg format tag",
            table.ident.name
        );
        assert_eq!(
            table.vended_credential_key, None,
            "{} must carry no vending key",
            table.ident.name
        );
    }
}

#[tokio::test]
async fn unloadable_table_is_reported_skipped_not_failed() {
    let (uri, _log) =
        spawn_mock_catalog(&["prod"], &["orders", "hive_events"], &["hive_events"], &[]).await;
    let client = IcebergRestCatalogClient::new(uri, creds_no_auth());

    let listing = client
        .list_tables(&["prod".to_string()])
        .await
        .expect("a skipped non-Iceberg table must not fail the batch");

    assert_eq!(
        listing
            .tables
            .iter()
            .map(|t| t.ident.name.as_str())
            .collect::<Vec<_>>(),
        vec!["orders"],
        "only the loadable table resolves"
    );
    assert_eq!(
        listing.skipped,
        vec![SkippedTable {
            ident: CatalogTableIdent {
                namespace: vec!["prod".to_string()],
                name: "hive_events".to_string(),
            },
            reason: SkipReason::NotLoadableIcebergTable,
        }],
        "the 404 table is reported skipped, verbatim"
    );
}

#[tokio::test]
async fn non_404_load_failure_aborts_the_batch_naming_the_table() {
    let (uri, _log) =
        spawn_mock_catalog(&["prod"], &["orders", "hive_events"], &[], &["hive_events"]).await;
    let client = IcebergRestCatalogClient::new(uri, creds_no_auth());

    let err = client
        .list_tables(&["prod".to_string()])
        .await
        .expect_err("a 500 on loadTable is a catalog fault and must abort enumeration");

    assert!(
        err.to_string()
            .starts_with("failed to load table 'prod.hive_events': catalog returned HTTP 500"),
        "{err}"
    );
}

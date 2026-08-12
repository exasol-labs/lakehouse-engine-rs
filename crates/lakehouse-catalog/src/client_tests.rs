//! Contract tests for the shared catalog-client trait and its catalog-neutral
//! metadata types.
//!
//! Covers catalog-crate-structure scenario:
//! "One shared catalog-client trait and its neutral types become the crate's
//! operation surface" -- every operation here is driven through a
//! `Box<dyn CatalogClient>`, so the boxed-future signature's trait-object
//! compatibility is what is under test, not a concrete client's behavior.

use super::*;
use crate::test_support::*;
use iceberg::spec::{PrimitiveType, Type};
use std::sync::{Arc, Mutex};

/// A client serving fixed metadata whose identifiers are built from the
/// namespace segments it was handed, so a test can observe what the trait passed
/// through. Stands in for both catalog kinds: it lists one Iceberg-sourced and
/// one Unity-sourced base table, plus one skipped entry per `SkipReason`
/// variant. Every listed shape is one a production client can produce — no
/// client puts a non-`Table` entry in `tables`.
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
            columns: vec![CatalogColumn {
                name: "total".to_string(),
                source_type: ColumnSourceType::Unity {
                    type_name: "DECIMAL".to_string(),
                    precision: 10,
                    scale: 2,
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
        // Own the segments before the future is built: the returned future is
        // bound to `&self`, not to the caller's slice borrow.
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
    // A segment may itself contain the dot separator, so a pre-joined identifier
    // could not be re-split back into these two segments.
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
    // The engine holds the client across the UDF's async boundary, so dropping
    // either bound must be a build failure here rather than there.
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

    // Matching an Iceberg column and a Unity column in distinct arms is what the
    // engine's single type-mapping home relies on.
    match &listing.tables[1].columns[0].source_type {
        ColumnSourceType::Unity {
            type_name,
            precision,
            scale,
        } => assert_eq!((type_name.as_str(), *precision, *scale), ("DECIMAL", 10, 2)),
        ColumnSourceType::Iceberg(ty) => panic!("expected a Unity source type, got iceberg {ty}"),
    }
}

// ---------------------------------------------------------------------------
// IcebergRestCatalogClient — the one-session, empty-batch, and skip guarantees
// migrated from the deleted engine schema loop.
// ---------------------------------------------------------------------------

/// A record of the session-scoped work the mock catalog served, so a test can
/// prove exactly one session (one OAuth grant) served a whole multi-table batch.
#[derive(Default)]
struct RequestLog {
    oauth_grants: usize,
    load_table_names: Vec<String>,
}

/// Spawn a minimal Iceberg REST catalog on a fresh local port, so the PUBLIC
/// `CatalogClient::list_tables` (enumerate-then-load) can be driven end-to-end.
///
/// Serves the namespace enumeration `list_tables` (`GET .../namespaces/{ns}/tables`)
/// with the `tables` identifiers in `namespace`, and reports no child namespaces
/// (`GET .../namespaces?parent=`) so the recursion terminates. Every enumerated
/// table then loads: one in `server_error` answers `loadTable` with HTTP 500 (a
/// catalog fault — NOT the "not an Iceberg table" signal), one in `not_loadable`
/// answers `loadTable` with HTTP 404 (the catalog's "not a loadable Iceberg
/// table" signal), and every other answers with a two-column (`id` long, `name`
/// string) result. Returns the catalog URI and the shared log.
///
/// Responses close the connection so the pooled `reqwest` client opens a fresh
/// connection per request, letting a single-threaded accept loop serve the whole
/// sequential request stream in order.
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
    let list_tables_body = list_tables_response(namespace, tables);

    let server_log = log.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut buf = vec![0u8; 8192];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            if n == 0 {
                continue;
            }
            let request = String::from_utf8_lossy(&buf[..n]);
            let request_line = request.lines().next().unwrap_or("");
            let mut fields = request_line.split_whitespace();
            let method = fields.next().unwrap_or("");
            let path = fields.next().unwrap_or("");
            let path_no_query = path.split(['?', '#']).next().unwrap_or("");

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
                // No child namespaces: the enumeration recursion terminates here.
                ("200 OK", r#"{"namespaces":[]}"#.to_string())
            } else if path_no_query.ends_with("/tables") {
                // The namespace `list_tables` enumeration endpoint.
                ("200 OK", list_tables_body.clone())
            } else if let Some(offset) = path_no_query.find("/tables/") {
                let table = &path_no_query[offset + "/tables/".len()..];
                server_log
                    .lock()
                    .unwrap()
                    .load_table_names
                    .push(table.to_string());
                if server_error.iter().any(|t| t == table) {
                    // A catalog fault (unreachable/broken), NOT "not an Iceberg
                    // table" — enumeration must abort, not skip.
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
        }
    });

    (uri, log)
}

/// A `loadTable` wire body for `table` carrying a two-column schema, so the
/// resolved `CatalogColumn`s can be asserted in schema order and original case.
fn load_table_body(table: &str) -> String {
    serde_json::json!({
        "metadata-location": format!("s3://bucket/{table}/metadata/v1.json"),
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": format!("s3://bucket/{table}"),
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "name", "required": false, "type": "string"}
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0
        }
    })
    .to_string()
}

/// A `listTables` wire body: the `tables` names as `TableIdent`s under
/// `namespace`, so the enumeration step of `list_tables` returns exactly these
/// identifiers.
fn list_tables_response(namespace: &[&str], tables: &[&str]) -> String {
    let identifiers: Vec<serde_json::Value> = tables
        .iter()
        .map(|name| serde_json::json!({ "namespace": namespace, "name": name }))
        .collect();
    serde_json::json!({ "identifiers": identifiers }).to_string()
}

fn oauth_creds() -> ConnectionCreds {
    let mut creds = base_creds();
    creds.client_id = Some("mock-client-id".into());
    creds.client_secret = Some("mock-client-secret".into());
    creds
}

/// The resolve-path corner of the one-session guarantee: `resolve_listing` over
/// an EMPTY identifier batch builds NO `CatalogSession` and performs NO OAuth
/// grant, proven by succeeding under OAuth credentials against an UNREACHABLE
/// catalog — a built session would fail to connect. Distinct from the public
/// `list_tables` empty case below, which must still reach the catalog to discover
/// the namespace holds no table.
#[tokio::test]
async fn empty_namespace_builds_no_session_and_no_grant() {
    let client =
        IcebergRestCatalogClient::new("http://127.0.0.1:1".into(), static_backend(), oauth_creds());

    let listing = client
        .resolve_listing(&[])
        .await
        .expect("an empty ident batch must resolve with no session build and no grant");

    assert!(
        listing.tables.is_empty(),
        "an empty batch resolves no table"
    );
    assert!(listing.skipped.is_empty(), "an empty batch skips nothing");
}

/// The PUBLIC `list_tables` over a namespace the catalog reports as holding no
/// table lists nothing. Under OAuth this costs exactly ONE grant — the
/// enumeration's own `credential` exchange — and no second grant: the empty load
/// batch builds no `CatalogSession`. A per-table or empty-batch session build
/// would push the count above one.
#[tokio::test]
async fn list_tables_over_empty_namespace_lists_nothing() {
    let (uri, log) = spawn_mock_catalog(&["sales"], &[], &[], &[]).await;
    let client = IcebergRestCatalogClient::new(uri, static_backend(), oauth_creds());

    let listing = client
        .list_tables(&["sales".to_string()])
        .await
        .expect("an empty namespace must list nothing");

    assert!(listing.tables.is_empty());
    assert!(listing.skipped.is_empty());
    assert_eq!(
        log.lock().unwrap().oauth_grants,
        1,
        "only the enumeration grant; the empty load batch adds no session grant"
    );
}

/// Drives the PUBLIC `list_tables` end-to-end: three tables enumerate, then load.
/// Under OAuth that is exactly TWO grants — one for the enumeration, one for the
/// whole load batch's single reused `CatalogSession` — never one grant per table
/// (which would be four). Every table resolves its columns in schema order and
/// original case.
#[tokio::test]
async fn enumeration_builds_exactly_one_session() {
    let (uri, log) =
        spawn_mock_catalog(&["sales"], &["orders", "customers", "returns"], &[], &[]).await;
    let client = IcebergRestCatalogClient::new(uri, static_backend(), oauth_creds());

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
    assert_eq!(
        log.lock().unwrap().oauth_grants,
        2,
        "one enumeration grant + one load-batch grant (the single reused session); per-table loads would be four"
    );
}

/// Driving the PUBLIC `list_tables`: a table the catalog reports as not a
/// loadable Iceberg table (HTTP 404 on load) is routed into `skipped` and does
/// NOT abort the batch; the loadable table still resolves.
#[tokio::test]
async fn unloadable_table_is_reported_skipped_not_failed() {
    let (uri, _log) =
        spawn_mock_catalog(&["prod"], &["orders", "hive_events"], &["hive_events"], &[]).await;
    let client = IcebergRestCatalogClient::new(uri, static_backend(), creds_no_auth());

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

/// Driving the PUBLIC `list_tables`: a non-404 `loadTable` failure (HTTP 500 —
/// an unreachable/broken catalog, NOT "not an Iceberg table") aborts the whole
/// enumeration with `Err`. The batch must never come back a short listing that
/// looks complete: a table silently vanishing behind a catalog fault is the
/// failure this guards. Guards `is_not_loadable_iceberg_table`'s non-404 branch
/// against being widened to "any load failure = skip".
#[tokio::test]
async fn non_404_load_failure_aborts_the_batch() {
    let (uri, _log) =
        spawn_mock_catalog(&["prod"], &["orders", "hive_events"], &[], &["hive_events"]).await;
    let client = IcebergRestCatalogClient::new(uri, static_backend(), creds_no_auth());

    let result = client.list_tables(&["prod".to_string()]).await;

    assert!(
        result.is_err(),
        "a 500 on loadTable is a catalog fault and must abort enumeration, \
         not be routed into a skipped entry: {result:?}"
    );
}

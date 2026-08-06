use super::*;
use crate::test_support::*;

/// Scenario: single-level namespace — returns (NamespaceIdent::new("mydb"), "mytable").
#[test]
fn parse_table_ident_splits_namespace_table() {
    let (ns, tbl) = parse_table_ident("mydb.mytable").unwrap();
    let levels: &[String] = &ns;
    assert_eq!(levels, &["mydb".to_string()]);
    assert_eq!(tbl, "mytable");
}

#[test]
fn parse_table_ident_errors_on_no_dot() {
    let err = parse_table_ident("notable").unwrap_err();
    assert!(err.to_string().contains("namespace.table"));
}

/// Scenario: Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent.
/// "prod.finance.orders" → NamespaceIdent(["prod","finance"]), "orders".
#[test]
fn parse_table_ident_handles_multilevel_namespace() {
    let (ns, tbl) = parse_table_ident("prod.finance.orders").unwrap();
    let levels: &[String] = &ns;
    assert_eq!(
        levels,
        &["prod".to_string(), "finance".to_string()],
        "namespace must have two levels"
    );
    assert_eq!(tbl, "orders", "table name is the trailing segment");

    // Three-level namespace + table.
    let (ns3, tbl3) = parse_table_ident("prod.finance.eu.orders").unwrap();
    let levels3: &[String] = &ns3;
    assert_eq!(
        levels3,
        &["prod".to_string(), "finance".to_string(), "eu".to_string()],
        "namespace must have three levels"
    );
    assert_eq!(tbl3, "orders");
}

/// Scenario: end-to-end — `list_namespace_tables`'s SigV4 enumeration path
/// (`list_in_namespace_signed` / `build_list_tables_url`) signs its
/// `list_tables` request against the derived `catalogs/{account-id}` prefix,
/// not the bare warehouse. This path bypasses `resolve_load_table_prefix`
/// entirely (it is `create-virtual-schema`'s namespace-enumeration path), so
/// it needs its own proof that `glue_catalog_prefix` reached it too.
///
/// A local HTTP server captures the raw request line of the `list_tables`
/// GET. Any follow-up `list_namespaces?parent=` request (child-namespace
/// recursion) is left unanswered — `list_in_namespace_signed` treats that as
/// a flat catalog (no children) and returns, matching AWS Glue's actual
/// behavior of rejecting nested-namespace listing.
#[tokio::test]
async fn list_tables_signed_url_carries_catalogs_prefix() {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let port = addr.port();

    let captured_request_line: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let captured = captured_request_line.clone();

    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            if n == 0 {
                continue;
            }
            let request = String::from_utf8_lossy(&buf[..n]).to_string();
            let request_line = request.lines().next().unwrap_or("").to_string();

            // Only the list_tables request (never the list_namespaces
            // recursion request) gets a reply — an AWS Glue-shaped flat
            // catalog. See `list_in_namespace_signed`'s "ponytail" fallback.
            if !request_line.contains("?parent=") {
                *captured.lock().unwrap() = Some(request_line);
                let body = r#"{"identifiers":[]}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        }
    });

    let catalog_uri = format!("http://127.0.0.1:{port}");
    let storage = static_backend();
    let mut creds = base_creds();
    creds.use_sigv4 = true;
    creds.warehouse = "123456789012".into();

    let result = list_namespace_tables(&catalog_uri, &["db".to_string()], &storage, &creds).await;

    assert!(
        result.is_ok(),
        "expected the enumeration to succeed: {:?}",
        result.err()
    );

    let request_line = captured_request_line
        .lock()
        .unwrap()
        .clone()
        .expect("the list_tables request must have been captured");
    assert!(
        request_line.contains("/v1/catalogs/123456789012/namespaces/db/tables"),
        "signed list_tables URL must carry the derived catalogs/{{account-id}} prefix: {request_line}"
    );
    assert!(
        !request_line.contains("/v1/123456789012/namespaces"),
        "signed list_tables URL must NOT use the bare warehouse as the prefix: {request_line}"
    );
}

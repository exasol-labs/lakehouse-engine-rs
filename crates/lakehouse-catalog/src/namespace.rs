//! Namespace enumeration for `createVirtualSchema`, plus the Iceberg identifier
//! parsing every catalog request needs.
//!
//! Moved verbatim from the engine's `adapter/pushdown/namespace.rs`.
//! `parse_table_ident` arrived ahead of the rest because the `loadTable` GET
//! this crate owns calls it.

use crate::redaction::redact_credentials;
use crate::session::{build_rest_catalog, glue_catalog_prefix};
use crate::{CatalogProps, ConnectionCreds, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

/// Parse a fully-qualified Iceberg identifier into `(NamespaceIdent, table_name)`.
///
/// The trailing `.`-delimited segment is the table name; all preceding segments form the
/// namespace. Supports any number of namespace levels:
/// - `"db.table"` → `(NamespaceIdent(["db"]), "table")`
/// - `"prod.finance.orders"` → `(NamespaceIdent(["prod","finance"]), "orders")`
///
/// Returns an error when the input contains no `.` (a bare table name with no namespace).
pub fn parse_table_ident(qualified: &str) -> Result<(NamespaceIdent, String), UdfError> {
    let mut parts: Vec<&str> = qualified.split('.').collect();
    if parts.len() < 2 {
        return Err(UdfError::User(format!(
            "table property must be 'namespace.table', got: '{qualified}'"
        )));
    }
    let table_name = parts.pop().unwrap().to_string();
    let ns_ident = NamespaceIdent::from_vec(parts.iter().map(|s| s.to_string()).collect())
        .map_err(|e| UdfError::User(format!("invalid namespace in '{qualified}': {e}")))?;
    Ok((ns_ident, table_name))
}

// ---------------------------------------------------------------------------
// Namespace enumeration (createVirtualSchema)
// ---------------------------------------------------------------------------

/// Enumerate every `TableIdent` in the configured namespace and all descendants.
///
/// Branches on `creds.use_sigv4`: unsigned path uses `RestCatalog::list_namespaces`
/// and `list_tables`; signed path issues SigV4-signed GETs directly against the
/// `catalogs/{warehouse}` prefix derived by `glue_catalog_prefix` (AWS Glue's
/// required REST prefix format).
///
/// The configured namespace is passed as split segments (e.g. `["prod","finance"]`).
/// Credentials NEVER appear in returned errors.
pub async fn list_namespace_tables(
    catalog_uri: &str,
    configured_ns: &[String],
    storage: &StorageBackend,
    creds: &ConnectionCreds,
) -> Result<Vec<TableIdent>, UdfError> {
    let ns_ident = NamespaceIdent::from_vec(configured_ns.to_vec()).map_err(|e| {
        UdfError::User(format!(
            "invalid ICEBERG_NAMESPACE '{}': {}",
            configured_ns.join("."),
            e
        ))
    })?;

    if creds.use_sigv4 {
        let prefix = glue_catalog_prefix(&creds.warehouse);
        list_in_namespace_signed(catalog_uri, &ns_ident, &prefix, creds).await
    } else {
        list_namespace_tables_unsigned(catalog_uri, &ns_ident, &creds.warehouse, storage, creds)
            .await
    }
}

/// Enumerate tables using the unsigned `RestCatalog` path.
///
/// Recursively lists all direct-child namespaces of `parent`, collecting tables at
/// every level. `list_namespaces(parent)` returns only direct children.
async fn list_namespace_tables_unsigned(
    catalog_uri: &str,
    parent: &NamespaceIdent,
    warehouse: &str,
    storage: &StorageBackend,
    creds: &ConnectionCreds,
) -> Result<Vec<TableIdent>, UdfError> {
    // Build a temporary CatalogProps with an empty table to construct the RestCatalog.
    let dummy_catalog = CatalogProps {
        warehouse: warehouse.to_string(),
        table: String::new(),
    };
    let catalog = build_rest_catalog(catalog_uri, &dummy_catalog, storage, creds).await?;
    list_in_namespace_unsigned(&catalog, parent).await
}

/// Recursively collect tables in `ns` and all descendant namespaces using an unsigned catalog.
fn list_in_namespace_unsigned<'a>(
    catalog: &'a iceberg_catalog_rest::RestCatalog,
    ns: &'a NamespaceIdent,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<TableIdent>, UdfError>> + Send + 'a>,
> {
    Box::pin(async move {
        let mut all: Vec<TableIdent> = Vec::new();

        // Tables directly in this namespace.
        let tables = catalog.list_tables(ns).await.map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to list tables in namespace '{}': {}",
                ns.join("."),
                redact_credentials(&e.to_string())
            ))
        })?;
        all.extend(tables);

        // Recurse into direct child namespaces.
        let children = catalog
            .list_namespaces(Some(ns))
            .await
            .map_err(|e: iceberg::Error| {
                UdfError::User(format!(
                    "failed to list namespaces under '{}': {}",
                    ns.join("."),
                    redact_credentials(&e.to_string())
                ))
            })?;

        for child in children {
            let child_tables = list_in_namespace_unsigned(catalog, &child).await?;
            all.extend(child_tables);
        }

        Ok(all)
    })
}

/// Build the `list_namespaces` URL for a given parent namespace.
///
/// `GET {catalog_uri}/v1/{warehouse?}/namespaces?parent={ns_url}`
fn build_list_namespaces_url(
    catalog_uri: &str,
    warehouse: &str,
    parent: &NamespaceIdent,
) -> String {
    let ns_url = parent.to_url_string();
    if warehouse.is_empty() {
        format!("{catalog_uri}/v1/namespaces?parent={ns_url}")
    } else {
        format!("{catalog_uri}/v1/{warehouse}/namespaces?parent={ns_url}")
    }
}

/// Build the `list_tables` URL for a given namespace.
///
/// `GET {catalog_uri}/v1/{warehouse?}/namespaces/{ns_url}/tables`
fn build_list_tables_url(catalog_uri: &str, warehouse: &str, ns: &NamespaceIdent) -> String {
    let ns_url = ns.to_url_string();
    if warehouse.is_empty() {
        format!("{catalog_uri}/v1/namespaces/{ns_url}/tables")
    } else {
        format!("{catalog_uri}/v1/{warehouse}/namespaces/{ns_url}/tables")
    }
}

/// Sign and execute a GET request, returning the response body as JSON.
///
/// Credential values NEVER appear in returned errors.
async fn signed_get_json(
    url: &str,
    creds: &ConnectionCreds,
) -> Result<serde_json::Value, UdfError> {
    let client = reqwest::Client::new();
    let request = client
        .get(url)
        .header("accept", "application/json")
        .build()
        .map_err(|e| UdfError::User(format!("failed to build catalog request: {e}")))?;

    let signed = crate::sigv4::sign_request(
        request,
        &creds.access_key,
        &creds.secret_key,
        creds.session_token.as_deref(),
        &creds.region,
        "glue",
    )
    .map_err(|e| {
        UdfError::User(format!(
            "failed to sign catalog request: {}",
            redact_credentials(&e.to_string())
        ))
    })?;

    let response = client.execute(signed).await.map_err(|e| {
        UdfError::User(format!(
            "catalog request failed: {}",
            redact_credentials(&e.to_string())
        ))
    })?;

    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "(unreadable body)".into());
        return Err(UdfError::User(format!(
            "catalog returned HTTP {}: {}",
            status.as_u16(),
            redact_credentials(&body)
        )));
    }

    response.json::<serde_json::Value>().await.map_err(|e| {
        UdfError::User(format!(
            "failed to parse catalog response: {}",
            redact_credentials(&e.to_string())
        ))
    })
}

/// Recursively collect tables in `ns` and all descendants using SigV4-signed GETs
/// (mirrors the SigV4 arm of `load_table_any_auth`). Credential values NEVER
/// appear in errors.
fn list_in_namespace_signed<'a>(
    catalog_uri: &'a str,
    ns: &'a NamespaceIdent,
    warehouse: &'a str,
    creds: &'a ConnectionCreds,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<TableIdent>, UdfError>> + Send + 'a>,
> {
    Box::pin(async move {
        use iceberg_catalog_rest::{ListNamespaceResponse, ListTablesResponse};

        let mut all: Vec<TableIdent> = Vec::new();

        // List tables in this namespace.
        let tables_url = build_list_tables_url(catalog_uri, warehouse, ns);
        let tables_json = signed_get_json(&tables_url, creds).await.map_err(|e| {
            UdfError::User(format!(
                "failed to list tables in namespace '{}': {}",
                ns.join("."),
                redact_credentials(&e.to_string())
            ))
        })?;
        let tables_response: ListTablesResponse =
            serde_json::from_value(tables_json).map_err(|e| {
                UdfError::User(format!(
                    "failed to parse list-tables response for namespace '{}': {}",
                    ns.join("."),
                    redact_credentials(&e.to_string())
                ))
            })?;
        all.extend(tables_response.identifiers);

        // List child namespaces and recurse. Best-effort: flat catalogs (e.g. AWS
        // Glue) reject nested-namespace listing with HTTP 400 "does not support
        // multipart namespace" — treat any failure here as "no children" and return
        // the tables already collected from this namespace.
        // ponytail: swallows ALL child-listing errors, not just the flat-catalog 400;
        // on a genuinely nested catalog a transient error would silently skip a
        // subtree. Upgrade path: branch on catalog capability from GET /v1/config.
        let ns_url = build_list_namespaces_url(catalog_uri, warehouse, ns);
        let ns_json = match signed_get_json(&ns_url, creds).await {
            Ok(j) => j,
            Err(_) => return Ok(all),
        };
        let ns_response: ListNamespaceResponse = match serde_json::from_value(ns_json) {
            Ok(r) => r,
            Err(_) => return Ok(all),
        };

        for child in ns_response.namespaces {
            let child_tables =
                list_in_namespace_signed(catalog_uri, &child, warehouse, creds).await?;
            all.extend(child_tables);
        }

        Ok(all)
    })
}

#[cfg(test)]
mod tests {
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

        let result =
            list_namespace_tables(&catalog_uri, &["db".to_string()], &storage, &creds).await;

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
}

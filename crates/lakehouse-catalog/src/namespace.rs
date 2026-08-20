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
///
/// Crate-private: `IcebergRestCatalogClient::list_tables` is its only caller — the
/// engine reaches enumeration through the `CatalogClient` trait, not this function.
pub(crate) async fn list_namespace_tables(
    catalog_uri: &str,
    configured_ns: &[String],
    storage: &StorageBackend,
    creds: &ConnectionCreds,
) -> Result<Vec<TableIdent>, UdfError> {
    let ns_ident = NamespaceIdent::from_vec(configured_ns.to_vec()).map_err(|e| {
        UdfError::User(format!(
            "invalid namespace '{}': {}",
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
#[path = "namespace_tests.rs"]
mod tests;

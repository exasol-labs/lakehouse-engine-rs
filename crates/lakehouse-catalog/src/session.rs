use crate::auth::{CatalogAuth, resolve_catalog_auth};
use crate::catalog_http::{CatalogHttp, CatalogRequestError};
use crate::iceberg_io::authed_get_json;
use crate::namespace::parse_table_ident;
use crate::redaction::redact_credentials;
use crate::{CatalogProps, ConnectionCreds};
use exasol_udf_sdk::error::UdfError;
use iceberg::NamespaceIdent;
use iceberg::spec::TableMetadata;
use iceberg_catalog_rest::LoadTableResult;

/// `prefix` is the already-resolved REST prefix, not the raw CONNECTION warehouse. It is
/// inserted verbatim (not URL-encoded) so a multi-segment prefix like Glue's
/// `catalogs/{account-id}` keeps its `/`.
pub(crate) fn catalog_url(catalog_uri: &str, prefix: &str, path: &str) -> String {
    let base = catalog_uri.trim_end_matches('/');
    if prefix.is_empty() {
        format!("{base}/v1/{path}")
    } else {
        format!("{base}/v1/{prefix}/{path}")
    }
}

fn build_load_table_url(catalog_uri: &str, prefix: &str, ns: &str, table_name: &str) -> String {
    catalog_url(
        catalog_uri,
        prefix,
        &format!("namespaces/{ns}/tables/{table_name}?snapshots=refs"),
    )
}

/// AWS Glue requires the REST prefix `catalogs/{catalogId}`, while the
/// user-facing `warehouse` is the bare account id. SigV4 is exclusively the Glue
/// path, so callers apply this unconditionally on SigV4.
pub(crate) fn glue_catalog_prefix(warehouse: &str) -> String {
    format!("catalogs/{warehouse}")
}

/// Per the Iceberg REST spec `overrides` take precedence over `defaults`, and
/// `prefix` may be served in either: Databricks uses `overrides`, Lakekeeper
/// `defaults`.
fn prefix_from_config(config: &serde_json::Value) -> String {
    let read = |map: &str| {
        config
            .get(map)
            .and_then(|m| m.get("prefix"))
            .and_then(|p| p.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    };
    read("overrides")
        .or_else(|| read("defaults"))
        .unwrap_or_default()
}

/// A config without a prefix yields an EMPTY prefix, never the warehouse: a warehouse like
/// `s3://…` as a path segment gives HTTP 400.
async fn resolve_load_table_prefix(
    http: &CatalogHttp,
    catalog_uri: &str,
    warehouse: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> Result<String, UdfError> {
    if let CatalogAuth::Sigv4 { .. } = auth {
        return Ok(glue_catalog_prefix(warehouse));
    }
    let encoded_warehouse: String =
        url::form_urlencoded::byte_serialize(warehouse.as_bytes()).collect();
    let config_url = catalog_url(
        catalog_uri,
        "",
        &format!("config?warehouse={encoded_warehouse}"),
    );
    authed_get_json::<serde_json::Value>(http, &config_url, auth, false, creds)
        .await
        .map(|config| prefix_from_config(&config))
        .map_err(|e| {
            UdfError::User(format!(
                "failed to read the catalog config for warehouse '{warehouse}': {}",
                redact_credentials(&e.to_string())
            ))
        })
}

/// Catalog HTTP state resolved once per query and reused across every table's
/// `loadTable` GET, so the OAuth grant and config lookup run once per query.
pub struct CatalogSession {
    http: CatalogHttp,
    catalog_uri: String,
    auth: CatalogAuth,
    prefix: String,
}

impl CatalogSession {
    /// A failed OAuth2 grant or config lookup is an error. Credential values never appear in any
    /// returned error.
    pub async fn resolve(
        catalog_uri: &str,
        warehouse: &str,
        creds: &ConnectionCreds,
    ) -> Result<CatalogSession, UdfError> {
        let http = CatalogHttp::new();
        let auth = resolve_catalog_auth(&http, catalog_uri, creds).await?;
        let prefix = resolve_load_table_prefix(&http, catalog_uri, warehouse, &auth, creds).await?;
        Ok(CatalogSession {
            http,
            catalog_uri: catalog_uri.to_string(),
            auth,
            prefix,
        })
    }

    pub(crate) fn http(&self) -> &CatalogHttp {
        &self.http
    }

    pub(crate) fn catalog_uri(&self) -> &str {
        &self.catalog_uri
    }

    pub(crate) fn auth(&self) -> &CatalogAuth {
        &self.auth
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// Self-issued because `RestCatalog::load_table` drops the response
/// `config`/`storage_credentials` needed for vended credentials. Sends
/// `X-Iceberg-Access-Delegation: vended-credentials` only when
/// `creds.use_vended_credentials`. Credential values never appear in the error.
pub async fn load_table_any_auth(
    session: &CatalogSession,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
) -> Result<LoadTableResult, UdfError> {
    let (ns_ident, table_name) = parse_table_ident(&catalog.table)?;
    load_table(
        session,
        &ns_ident,
        &table_name,
        creds.use_vended_credentials,
        creds,
    )
    .await
    .map_err(UdfError::from)
}

/// The enumeration reads only the current schema, so it asks for no delegated access.
pub(crate) async fn load_table_schema_metadata(
    session: &CatalogSession,
    namespace: &NamespaceIdent,
    table_name: &str,
    creds: &ConnectionCreds,
) -> Result<TableMetadata, CatalogRequestError> {
    load_table(session, namespace, table_name, false, creds)
        .await
        .map(|result| result.metadata)
}

/// Names the table in every failure, so the enumeration load and `load_table_any_auth` both
/// report which table could not be loaded. Asks for `snapshots=refs`: both callers read only the
/// current snapshot, which the `main` branch always references.
async fn load_table(
    session: &CatalogSession,
    namespace: &NamespaceIdent,
    table_name: &str,
    send_access_delegation: bool,
    creds: &ConnectionCreds,
) -> Result<LoadTableResult, CatalogRequestError> {
    let url = build_load_table_url(
        &session.catalog_uri,
        &session.prefix,
        &namespace.to_url_string(),
        table_name,
    );
    let identifier = format!("{}.{table_name}", namespace.join("."));
    authed_get_json::<LoadTableResult>(
        &session.http,
        &url,
        &session.auth,
        send_access_delegation,
        creds,
    )
    .await
    .map_err(|error| error.named(&identifier))
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;

//! `load_table_any_auth` lives here because it reads `CatalogSession`'s private
//! fields, keeping `CatalogAuth` unreachable from outside this crate.

use crate::auth::{CatalogAuth, resolve_catalog_auth};
use crate::iceberg_io::authed_get_json;
use crate::namespace::parse_table_ident;
use crate::{CatalogProps, ConnectionCreds};
use exasol_udf_sdk::error::UdfError;
use iceberg::NamespaceIdent;
use iceberg::spec::TableMetadata;
use iceberg_catalog_rest::LoadTableResult;

/// `warehouse` is the already-resolved REST prefix, not the raw CONNECTION
/// warehouse. It is inserted verbatim (not URL-encoded) so a multi-segment prefix
/// like Glue's `catalogs/{account-id}` keeps its `/`.
fn build_load_table_url(catalog_uri: &str, warehouse: &str, ns: &str, table_name: &str) -> String {
    let base = format!("{catalog_uri}/v1");
    if warehouse.is_empty() {
        format!("{base}/namespaces/{ns}/tables/{table_name}")
    } else {
        format!("{base}/{warehouse}/namespaces/{ns}/tables/{table_name}")
    }
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

/// A missing prefix or unreachable config endpoint yields an EMPTY prefix, never
/// the warehouse: a warehouse like `s3://…` as a path segment gives HTTP 400.
async fn resolve_load_table_prefix(
    client: &reqwest::Client,
    catalog_uri: &str,
    warehouse: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> String {
    if let CatalogAuth::Sigv4 { .. } = auth {
        return glue_catalog_prefix(warehouse);
    }
    let encoded_warehouse: String =
        url::form_urlencoded::byte_serialize(warehouse.as_bytes()).collect();
    let config_url = format!(
        "{}/v1/config?warehouse={encoded_warehouse}",
        catalog_uri.trim_end_matches('/')
    );
    match authed_get_json::<serde_json::Value>(client, &config_url, auth, false, creds).await {
        Ok(config) => prefix_from_config(&config),
        Err(_) => String::new(),
    }
}

/// Catalog HTTP state resolved once per query and reused across every table's
/// `loadTable` GET, so the OAuth grant and config lookup run once per query.
pub struct CatalogSession {
    client: reqwest::Client,
    catalog_uri: String,
    auth: CatalogAuth,
    prefix: String,
}

impl CatalogSession {
    /// A failed OAuth2 grant is an error; a failed config lookup yields an empty
    /// prefix. Credential values never appear in any returned error.
    pub async fn resolve(
        catalog_uri: &str,
        warehouse: &str,
        creds: &ConnectionCreds,
    ) -> Result<CatalogSession, UdfError> {
        let client = reqwest::Client::new();
        let auth = resolve_catalog_auth(&client, catalog_uri, creds).await?;
        let prefix = resolve_load_table_prefix(&client, catalog_uri, warehouse, &auth, creds).await;
        Ok(CatalogSession {
            client,
            catalog_uri: catalog_uri.to_string(),
            auth,
            prefix,
        })
    }

    pub(crate) fn client(&self) -> &reqwest::Client {
        &self.client
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
    let ns_url = ns_ident.to_url_string();
    let url = build_load_table_url(&session.catalog_uri, &session.prefix, &ns_url, &table_name);

    authed_get_json::<LoadTableResult>(
        &session.client,
        &url,
        &session.auth,
        creds.use_vended_credentials,
        creds,
    )
    .await
}

/// The enumeration reads only the current schema, so it asks for no delegated access and for
/// only the referenced snapshots (Iceberg REST `loadTable` `snapshots=refs`).
pub(crate) async fn load_table_schema_metadata(
    session: &CatalogSession,
    namespace: &NamespaceIdent,
    table_name: &str,
    creds: &ConnectionCreds,
) -> Result<TableMetadata, UdfError> {
    let url = build_load_table_url(
        &session.catalog_uri,
        &session.prefix,
        &namespace.to_url_string(),
        table_name,
    );
    let url = format!("{url}?snapshots=refs");
    authed_get_json::<LoadTableResult>(&session.client, &url, &session.auth, false, creds)
        .await
        .map(|result| result.metadata)
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;

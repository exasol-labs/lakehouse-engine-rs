//! Catalog HTTP state resolved once per request. The table loads live here
//! because they read `CatalogSession`'s private fields, keeping `CatalogAuth`
//! unreachable from outside this crate.

use crate::auth::{CatalogAuth, resolve_catalog_auth};
use crate::http::build_client;
use crate::iceberg_io::authed_get_json;
use crate::namespace::parse_table_ident;
use crate::{CatalogProps, ConnectionCreds};
use exasol_udf_sdk::error::UdfError;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{ListNamespaceResponse, ListTablesResponse, LoadTableResult};
use serde::de::DeserializeOwned;

/// `prefix` is the already-resolved REST prefix, not the raw CONNECTION
/// warehouse. It is inserted verbatim (not URL-encoded) so a multi-segment prefix
/// like Glue's `catalogs/{account-id}` keeps its `/`.
fn build_rest_url(catalog_uri: &str, prefix: &str, path: &str, query: &[(&str, &str)]) -> String {
    let base = format!("{}/v1", catalog_uri.trim_end_matches('/'));
    let mut url = if prefix.is_empty() {
        format!("{base}/{path}")
    } else {
        format!("{base}/{prefix}/{path}")
    };
    if !query.is_empty() {
        url.push('?');
        url.push_str(
            &url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(query)
                .finish(),
        );
    }
    url
}

fn build_load_table_url(
    catalog_uri: &str,
    prefix: &str,
    ns: &str,
    table_name: &str,
    query: &[(&str, &str)],
) -> String {
    build_rest_url(
        catalog_uri,
        prefix,
        &format!("namespaces/{ns}/tables/{table_name}"),
        query,
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

/// A missing prefix yields an EMPTY prefix, never the warehouse: a warehouse like
/// `s3://…` as a path segment gives HTTP 400.
async fn resolve_load_table_prefix(
    client: &reqwest::Client,
    catalog_uri: &str,
    warehouse: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> Result<String, UdfError> {
    if let CatalogAuth::Sigv4 { .. } = auth {
        return Ok(glue_catalog_prefix(warehouse));
    }
    let config_url = build_rest_url(catalog_uri, "", "config", &[("warehouse", warehouse)]);
    let config = authed_get_json::<serde_json::Value>(client, &config_url, auth, false, creds)
        .await
        .map_err(|e| UdfError::User(format!("failed to read the catalog config: {e}")))?;
    Ok(prefix_from_config(&config))
}

/// Catalog HTTP state resolved once per request and reused across every catalog
/// GET, so the OAuth grant and config lookup run once per request.
pub struct CatalogSession {
    client: reqwest::Client,
    catalog_uri: String,
    auth: CatalogAuth,
    prefix: String,
}

impl CatalogSession {
    /// A failed OAuth2 grant or config lookup is an error. Credential values
    /// never appear in any returned error.
    pub async fn resolve(
        catalog_uri: &str,
        warehouse: &str,
        creds: &ConnectionCreds,
    ) -> Result<CatalogSession, UdfError> {
        let client = build_client();
        let auth = resolve_catalog_auth(&client, catalog_uri, creds).await?;
        let prefix =
            resolve_load_table_prefix(&client, catalog_uri, warehouse, &auth, creds).await?;
        Ok(CatalogSession {
            client,
            catalog_uri: catalog_uri.to_string(),
            auth,
            prefix,
        })
    }

    /// AWS Glue has no nested namespaces, and SigV4 is exclusively the Glue path.
    pub(crate) fn is_flat(&self) -> bool {
        matches!(self.auth, CatalogAuth::Sigv4 { .. })
    }

    pub(crate) async fn list_tables(
        &self,
        ns: &NamespaceIdent,
        creds: &ConnectionCreds,
    ) -> Result<Vec<TableIdent>, UdfError> {
        let path = format!("namespaces/{}/tables", ns.to_url_string());
        self.paged(&path, &[], creds, |page: ListTablesResponse| {
            (page.identifiers, page.next_page_token)
        })
        .await
        .map_err(|e| {
            UdfError::User(format!(
                "failed to list tables in namespace '{}': {e}",
                ns.join(".")
            ))
        })
    }

    pub(crate) async fn list_namespaces(
        &self,
        parent: &NamespaceIdent,
        creds: &ConnectionCreds,
    ) -> Result<Vec<NamespaceIdent>, UdfError> {
        let parent_url = parent.to_url_string();
        self.paged(
            "namespaces",
            &[("parent", &parent_url)],
            creds,
            |page: ListNamespaceResponse| (page.namespaces, page.next_page_token),
        )
        .await
        .map_err(|e| {
            UdfError::User(format!(
                "failed to list namespaces under '{}': {e}",
                parent.join(".")
            ))
        })
    }

    async fn paged<P: DeserializeOwned, T>(
        &self,
        path: &str,
        query: &[(&str, &str)],
        creds: &ConnectionCreds,
        split: impl Fn(P) -> (Vec<T>, Option<String>),
    ) -> Result<Vec<T>, UdfError> {
        let mut items = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut params = query.to_vec();
            if let Some(token) = page_token.as_deref() {
                params.push(("pageToken", token));
            }
            let url = build_rest_url(&self.catalog_uri, &self.prefix, path, &params);
            let page: P = authed_get_json(&self.client, &url, &self.auth, false, creds).await?;
            let (page_items, next) = split(page);
            items.extend(page_items);
            match next {
                Some(token) if !token.is_empty() => page_token = Some(token),
                _ => return Ok(items),
            }
        }
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
    load_table(session, catalog, creds, creds.use_vended_credentials, &[]).await
}

/// Enumeration reads only the current schema, so it requests no storage
/// credentials and leaves the unreferenced snapshot history out of the response.
pub(crate) async fn load_table_schema(
    session: &CatalogSession,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
) -> Result<LoadTableResult, UdfError> {
    load_table(session, catalog, creds, false, &[("snapshots", "refs")]).await
}

async fn load_table(
    session: &CatalogSession,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
    send_access_delegation: bool,
    query: &[(&str, &str)],
) -> Result<LoadTableResult, UdfError> {
    let (ns_ident, table_name) = parse_table_ident(&catalog.table)?;
    let url = build_load_table_url(
        &session.catalog_uri,
        &session.prefix,
        &ns_ident.to_url_string(),
        &table_name,
        query,
    );
    authed_get_json::<LoadTableResult>(
        &session.client,
        &url,
        &session.auth,
        send_access_delegation,
        creds,
    )
    .await
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;

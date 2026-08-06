//! The per-query catalog session: the auth strategy and `/v1/config` prefix
//! resolved once, the `loadTable` GET issued on them, the `loadTable` URL
//! builder, and the `RestCatalog` constructor.
//!
//! Moved verbatim from the engine's `adapter/pushdown/credentials.rs`.
//! `load_table_any_auth` lives here rather than beside the other I/O primitives
//! because it reads `CatalogSession`'s private fields, which is what keeps those
//! fields private and `CatalogAuth` unreachable from outside this crate.

use crate::auth::{
    CatalogAuth, inject_catalog_auth_props, redact_catalog_auth_error, resolve_catalog_auth,
};
use crate::iceberg_io::authed_get_json;
use crate::namespace::parse_table_ident;
use crate::{CatalogProps, ConnectionCreds, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use std::collections::HashMap;

/// Build a RestCatalog for the unsigned namespace-enumeration path.
///
/// No `StorageFactory` is set: the only caller lists tables/namespaces (pure
/// REST), which never builds a `FileIO`. Credentials flow through `props` via
/// [`StorageBackend::catalog_storage_props`] and never appear in SQL or errors.
pub(crate) async fn build_rest_catalog(
    catalog_uri: &str,
    catalog: &CatalogProps,
    storage: &StorageBackend,
    creds: &ConnectionCreds,
) -> Result<RestCatalog, UdfError> {
    let mut props = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_uri.to_string());
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        catalog.warehouse.clone(),
    );
    props.extend(storage.catalog_storage_props());

    inject_catalog_auth_props(&mut props, creds);

    RestCatalogBuilder::default()
        .load("lakehouse", props)
        .await
        .map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to connect to Iceberg catalog: {}",
                redact_catalog_auth_error(&e.to_string(), creds)
            ))
        })
}

/// Build the `loadTable` REST URL matching iceberg-catalog-rest's `table_endpoint` pattern:
/// `{catalog_uri}/v1/{prefix?}/namespaces/{ns_url}/tables/{table_name}`
///
/// The `warehouse` parameter is the already-resolved URL prefix string (matching
/// `props["prefix"]` in the iceberg-catalog-rest config map); the name is historical —
/// the caller passes the resolved prefix, not a raw connection warehouse.
/// `resolve_load_table_prefix` produces it upstream: for SigV4/Glue the derived
/// `catalogs/{account-id}` segment (via `glue_catalog_prefix`), for Databricks-style
/// catalogs the `overrides.prefix` fetched from `GET {catalog_uri}/v1/config?warehouse=…`,
/// and for plain REST catalogs typically empty. When empty, the prefix is omitted and the
/// URL reduces to `{catalog_uri}/v1/namespaces/{ns}/tables/{table}`.
///
/// The prefix is inserted verbatim — no URL-encoding — so a multi-segment prefix such as
/// the Glue `catalogs/{account-id}` form keeps its `/` literal, and any reserved characters
/// pass through unchanged. This is a low-level, format-agnostic builder: it inserts whatever
/// prefix string it is given and does not interpret its shape. Non-ASCII prefixes are not
/// URL-encoded here.
fn build_load_table_url(catalog_uri: &str, warehouse: &str, ns: &str, table_name: &str) -> String {
    let base = format!("{catalog_uri}/v1");
    if warehouse.is_empty() {
        format!("{base}/namespaces/{ns}/tables/{table_name}")
    } else {
        format!("{base}/{warehouse}/namespaces/{ns}/tables/{table_name}")
    }
}

/// Derive the AWS Glue Iceberg REST catalog prefix path segment from a
/// bare-account-id `warehouse` value.
///
/// AWS Glue's Iceberg REST catalog requires the REST prefix in the form
/// `catalogs/{catalogId}` — the bare AWS account id is the correct
/// user-facing `warehouse` value (standard Iceberg clients derive
/// `catalogs/{account-id}` internally). This is a Glue-proprietary
/// convention: `CatalogAuth::Sigv4` is exclusively the Glue path today, so
/// this derivation is applied unconditionally here rather than gated on a
/// separate auth check.
///
/// Crate-private: `resolve_load_table_prefix` and `namespace`'s signed
/// enumeration path are the only callers.
pub(crate) fn glue_catalog_prefix(warehouse: &str) -> String {
    format!("catalogs/{warehouse}")
}

/// Read the per-warehouse routing `prefix` from a `GET /v1/config` response.
///
/// Per the Iceberg REST spec a client merges the server's `defaults` (base) and
/// `overrides` (higher precedence) config maps; the `prefix` property may be
/// served in EITHER. Databricks-style catalogs place it in `overrides`, while
/// Lakekeeper serves the per-warehouse UUID prefix in `defaults`. Prefer
/// `overrides.prefix` (spec merge precedence), then fall back to `defaults.prefix`;
/// EMPTY when neither carries a non-empty value (the plain-REST case, including
/// `apache/iceberg-rest-fixture`).
///
/// Reading only `overrides.prefix` (the former behaviour) yielded an empty prefix
/// against Lakekeeper, producing a malformed `loadTable` URL missing the required
/// warehouse segment → HTTP 404.
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

/// Resolve the `loadTable` URL prefix from the catalog config endpoint.
///
/// `GET {catalog_uri}/v1/config?warehouse=<warehouse>` → merged `prefix` (see
/// [`prefix_from_config`] for the `overrides`-then-`defaults` precedence).
/// Databricks-style endpoints return a `prefix` that must address the table
/// instead of the raw warehouse; Lakekeeper returns a per-warehouse UUID prefix;
/// plain REST catalogs (including `apache/iceberg-rest-fixture`) typically omit
/// the prefix. When the config endpoint returns no `prefix` (or cannot be
/// contacted), the prefix is EMPTY — not the warehouse — so `build_load_table_url`
/// produces the standard-REST URL `/v1/namespaces/{ns}/tables/{table}` with no
/// extra segment. Inserting the warehouse as a path segment would yield a
/// malformed URL (e.g. `/v1/s3://warehouse//namespaces/…` → HTTP 400).
///
/// The SigV4/Glue path short-circuits immediately: the prefix is derived from
/// the warehouse via `glue_catalog_prefix` (`catalogs/{warehouse}`, AWS Glue's
/// required REST prefix format) — no config round-trip.
async fn resolve_load_table_prefix(
    client: &reqwest::Client,
    catalog_uri: &str,
    warehouse: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> String {
    // SigV4/Glue: the prefix is derived from the warehouse — no /v1/config round-trip.
    if let CatalogAuth::Sigv4 = auth {
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

/// The catalog HTTP state resolved once per query: one pooled `reqwest` client,
/// the catalog URI, the resolved catalog-auth strategy, and the `/v1/config` URL
/// prefix.
///
/// The auth strategy and prefix are catalog-scoped, not table-scoped, so a single
/// resolution is built once (by [`CatalogSession::resolve`]) before file
/// resolution and reused across every table's `loadTable` GET — collapsing the
/// per-table OAuth grant, config lookup, and cold client into one of each per
/// query. `reqwest::Client` is `Arc`-backed, so the one client shares its
/// per-host connection pool across every catalog request.
///
/// Published by `lakehouse-catalog` and never re-exported on the engine's
/// pushdown façade. Its fields are private, so the crate-private `CatalogAuth`
/// type never leaks through the public interface and no consumer can re-derive
/// the auth strategy or the prefix.
pub struct CatalogSession {
    client: reqwest::Client,
    catalog_uri: String,
    auth: CatalogAuth,
    prefix: String,
}

impl CatalogSession {
    /// Resolve the per-query catalog HTTP state once: construct a single pooled
    /// `reqwest` client, resolve the catalog-auth strategy on it (running the
    /// OAuth2 client-credentials grant exactly once on the OAuth path), then
    /// resolve the `/v1/config` prefix on the same client (exactly once).
    ///
    /// A failed config lookup yields an EMPTY prefix, never a hard build error —
    /// the swallow lives in `resolve_load_table_prefix`, which returns `String`,
    /// so a config failure cannot fail session construction. A failed OAuth2
    /// grant DOES propagate, matching the pre-refactor per-table behaviour.
    ///
    /// Credential values never appear in any returned error: the grant and every
    /// config request route through their own redaction closures unchanged.
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
}

/// Self-issue a `loadTable` GET on the query's `CatalogSession` and deserialize
/// the raw `LoadTableResult`.
///
/// Auth-mode-agnostic: the session already carries the resolved catalog-auth
/// strategy (SigV4 signing, a static/OAuth2-derived bearer token, or no auth) and
/// the `/v1/config` prefix, so this issues ONLY the per-table GET — it never
/// re-derives auth or the prefix. The returned `LoadTableResult` feeds BOTH file
/// planning AND vended-credential extraction, so vending works on every mode.
/// `iceberg-catalog-rest` 0.10.0's `RestCatalog::load_table` returns only a
/// `Table` and drops the response `config`/`storage_credentials`, which is why
/// this self-issued GET is required.
///
/// Sends `X-Iceberg-Access-Delegation: vended-credentials` ONLY when
/// `creds.use_vended_credentials`, keeping the no-vending request byte-identical
/// to the pre-feature shape on every mode.
///
/// Credential values (signing keys, bearer/OAuth2 tokens, vended STS, client
/// secret) NEVER appear in the returned error.
pub async fn load_table_any_auth(
    session: &CatalogSession,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
) -> Result<iceberg_catalog_rest::LoadTableResult, UdfError> {
    let (ns_ident, table_name) = parse_table_ident(&catalog.table)?;
    let ns_url = ns_ident.to_url_string();
    let url = build_load_table_url(&session.catalog_uri, &session.prefix, &ns_url, &table_name);

    authed_get_json::<iceberg_catalog_rest::LoadTableResult>(
        &session.client,
        &url,
        &session.auth,
        creds.use_vended_credentials,
        creds,
    )
    .await
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;

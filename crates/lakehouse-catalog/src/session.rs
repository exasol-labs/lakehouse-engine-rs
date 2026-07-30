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
use crate::{CatalogProps, ConnectionCreds, StorageProps};
use exasol_udf_sdk::error::UdfError;
use iceberg::CatalogBuilder;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use std::collections::HashMap;
use std::sync::Arc;

/// Build a RestCatalog configured to read/write data files through the S3
/// (MinIO) storage factory.
///
/// iceberg 0.10.0 requires an explicit `StorageFactory`; the S3 config keys are
/// supplied in the same props map passed to `load`. Credentials live only in
/// this map and never appear in returned SQL or error strings.
///
/// Crate-private: `namespace`'s unsigned enumeration path is the only caller.
pub(crate) async fn build_rest_catalog(
    catalog_uri: &str,
    catalog: &CatalogProps,
    storage: &StorageProps,
    creds: &ConnectionCreds,
) -> Result<RestCatalog, UdfError> {
    let mut props = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_uri.to_string());
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        catalog.warehouse.clone(),
    );
    if !storage.endpoint.is_empty() {
        props.insert(S3_ENDPOINT.to_string(), storage.endpoint.clone());
    }
    if !storage.region.is_empty() {
        props.insert(S3_REGION.to_string(), storage.region.clone());
    }
    if !storage.access_key.is_empty() {
        props.insert(S3_ACCESS_KEY_ID.to_string(), storage.access_key.clone());
    }
    if !storage.secret_key.is_empty() {
        props.insert(S3_SECRET_ACCESS_KEY.to_string(), storage.secret_key.clone());
    }
    if let Some(token) = &storage.session_token {
        props.insert(S3_SESSION_TOKEN.to_string(), token.clone());
    }
    props.insert(
        S3_PATH_STYLE_ACCESS.to_string(),
        storage.path_style.to_string(),
    );

    inject_catalog_auth_props(&mut props, creds);

    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
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
mod tests {
    use super::*;
    use crate::test_support::*;

    // ---------------------------------------------------------------------------
    // Task 3.3 / 3.4 — SigV4 wiring: loadTable URL construction
    // ---------------------------------------------------------------------------

    /// Scenario: build_load_table_url produces the iceberg-catalog-rest
    /// `table_endpoint` path ({uri}/v1/{warehouse}/namespaces/{ns}/tables/{table}).
    #[test]
    fn build_load_table_url_with_warehouse_prefix() {
        let url = build_load_table_url(
            "https://glue.us-east-1.amazonaws.com/iceberg",
            "123456789012",
            "db",
            "events",
        );
        assert_eq!(
            url,
            "https://glue.us-east-1.amazonaws.com/iceberg/v1/123456789012/namespaces/db/tables/events",
            "URL must follow {{uri}}/v1/{{warehouse}}/namespaces/{{ns}}/tables/{{table}} pattern"
        );
    }

    /// Scenario: build_load_table_url omits the warehouse prefix when empty.
    #[test]
    fn build_load_table_url_without_warehouse() {
        let url = build_load_table_url("https://rest.example.com", "", "db", "events");
        assert_eq!(
            url, "https://rest.example.com/v1/namespaces/db/tables/events",
            "URL must omit prefix when warehouse is empty"
        );
    }

    /// Scenario: build_load_table_url inserts an already-resolved prefix verbatim,
    /// with no URL-encoding of reserved characters or path separators.
    ///
    /// This low-level builder inserts whatever prefix string it is given exactly as-is:
    /// reserved characters (`:`) and internal separators (`/`) are NOT percent-encoded.
    /// The invariant guards the derived Glue `catalogs/{account-id}` prefix — whose `/`
    /// must stay literal — against a future URL-encoding refactor regressing it silently.
    /// (`build_load_table_url_with_warehouse_prefix` exercises only an all-digit prefix,
    /// which URL-encoding would leave unchanged, so it cannot catch such a regression.)
    #[test]
    fn build_load_table_url_inserts_prefix_verbatim_without_encoding() {
        let prefix = "raw:prefix/extra";
        let url = build_load_table_url(
            "https://glue.us-east-1.amazonaws.com/iceberg",
            prefix,
            "mydb",
            "orders",
        );
        assert_eq!(
            url,
            format!(
                "https://glue.us-east-1.amazonaws.com/iceberg/v1/{prefix}/namespaces/mydb/tables/orders"
            ),
            "prefix must be inserted verbatim — `:` and `/` left unencoded"
        );
    }

    /// Scenario: glue_catalog_prefix derives the `catalogs/{warehouse}` segment
    /// AWS Glue's Iceberg REST catalog requires as its prefix path segment.
    #[test]
    fn glue_catalog_prefix_derives_catalogs_segment() {
        assert_eq!(
            glue_catalog_prefix("123456789012"),
            "catalogs/123456789012",
            "Glue prefix must be catalogs/{{warehouse}}"
        );
    }

    /// Scenario: end-to-end — the `catalogs/{account-id}` prefix `glue_catalog_prefix`
    /// derives flows through `build_load_table_url` into the actual `loadTable` URL,
    /// landing in the `{uri}/v1/{prefix}/namespaces/{ns}/tables/{table}` slot.
    #[test]
    fn build_load_table_url_glue_carries_catalogs_prefix() {
        let prefix = glue_catalog_prefix("123456789012");
        let url = build_load_table_url(
            "https://glue.us-east-1.amazonaws.com/iceberg",
            &prefix,
            "db",
            "events",
        );
        assert_eq!(
            url,
            "https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/123456789012/namespaces/db/tables/events",
            "derived catalogs/{{account-id}} prefix must appear verbatim in the loadTable URL: {url}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 6 (add-lakekeeper-e2e) — `/v1/config` prefix location. Surfaced as a
    // genuine interop gap against a real Lakekeeper 0.13.1 (defaults.prefix).
    // ---------------------------------------------------------------------------

    /// Databricks-style catalogs serve the routing prefix in `overrides` — it wins.
    #[test]
    fn prefix_from_config_prefers_overrides() {
        let config = serde_json::json!({
            "overrides": {"prefix": "over-prefix"},
            "defaults": {"prefix": "def-prefix"}
        });
        assert_eq!(prefix_from_config(&config), "over-prefix");
    }

    /// Lakekeeper serves the per-warehouse prefix in `defaults`; with no
    /// `overrides.prefix` the adapter must fall back to it (the fixed gap).
    #[test]
    fn prefix_from_config_falls_back_to_defaults() {
        let config = serde_json::json!({
            "overrides": {"uri": "http://localhost:28181/catalog"},
            "defaults": {"prefix": "530164b8-8697-11f1-939b-239086e9948e", "rest-page-size": "100"}
        });
        assert_eq!(
            prefix_from_config(&config),
            "530164b8-8697-11f1-939b-239086e9948e",
            "Lakekeeper's defaults.prefix must be honoured when overrides.prefix is absent"
        );
    }

    /// Plain REST catalogs (fixture) omit the prefix entirely → empty.
    #[test]
    fn prefix_from_config_empty_when_absent() {
        assert_eq!(prefix_from_config(&serde_json::json!({})), "");
        assert_eq!(
            prefix_from_config(&serde_json::json!({"overrides": {}, "defaults": {}})),
            ""
        );
        // An empty-string prefix in either map is treated as absent.
        assert_eq!(
            prefix_from_config(&serde_json::json!({"defaults": {"prefix": ""}})),
            ""
        );
    }

    // ---------------------------------------------------------------------------
    // R1 — SigV4 skips /v1/config round-trip, derives the catalogs/{account-id} prefix
    // ---------------------------------------------------------------------------

    /// Scenario: The SigV4 path short-circuits `resolve_load_table_prefix` and
    /// returns the derived `catalogs/{warehouse}` prefix (AWS Glue's required
    /// REST prefix form), even when the catalog server would return a DIFFERENT
    /// prefix.
    ///
    /// A local HTTP server is started that responds with `overrides.prefix` =
    /// `"server-returned-prefix"`. For non-SigV4, that prefix would be used.
    /// For SigV4, the function must return `catalogs/{warehouse}` WITHOUT
    /// contacting the server — proved by the contrast with the paired non-SigV4
    /// test `non_sigv4_config_prefix_resolution_uses_config_endpoint`. `warehouse`
    /// is the bare AWS account id (the documented input shape) rather than an
    /// ARN — an ARN-shaped warehouse is not a supported input.
    #[tokio::test]
    async fn sigv4_resolve_prefix_derives_catalogs_segment() {
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Bind a local server that returns a DIFFERENT prefix. If SigV4 contacted
        // it, the result would differ from the derived catalogs/{warehouse} prefix.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let port = addr.port();

        let server_prefix = "server-returned-prefix-SHOULD-NOT-BE-USED";

        tokio::spawn(async move {
            // Accept and reply — but the SigV4 path must never connect.
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let _n = stream.read(&mut buf).await.unwrap_or(0);
                let body = format!(r#"{{"overrides":{{"prefix":"{server_prefix}"}}}}"#);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let catalog_uri = format!("http://127.0.0.1:{port}");
        let warehouse = "123456789012";

        let mut creds = base_creds();
        creds.use_sigv4 = true;
        let auth = CatalogAuth::Sigv4;

        let client = reqwest::Client::new();
        let result =
            resolve_load_table_prefix(&client, &catalog_uri, warehouse, &auth, &creds).await;

        assert_eq!(
            result,
            format!("catalogs/{warehouse}"),
            "SigV4 path must return the derived catalogs/{{warehouse}} prefix, \
             ignoring the server-side overrides.prefix"
        );
        assert_ne!(
            result, server_prefix,
            "SigV4 path must NOT use the server-returned prefix"
        );
    }

    /// Scenario: A non-SigV4 path that hits a local HTTP server returning an
    /// `overrides.prefix` different from the warehouse uses that resolved prefix.
    ///
    /// This confirms that the config-endpoint round-trip IS performed for non-SigV4
    /// modes, contrasting with `sigv4_skips_config_prefix_lookup_uses_warehouse_directly`.
    #[tokio::test]
    async fn non_sigv4_config_prefix_resolution_uses_config_endpoint() {
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Bind to a random local port to serve the /v1/config response.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let port = addr.port();

        let resolved_prefix = "resolved-prefix-from-config";

        // Spawn a minimal HTTP/1.1 server returning overrides.prefix.
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 4096];
            let _n = stream.read(&mut buf).await.expect("read");

            let body = format!(r#"{{"overrides":{{"prefix":"{resolved_prefix}"}}}}"#);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.expect("write");
        });

        let catalog_uri = format!("http://127.0.0.1:{port}");
        let creds = creds_no_auth();
        let auth = CatalogAuth::None;

        let client = reqwest::Client::new();
        let result =
            resolve_load_table_prefix(&client, &catalog_uri, "original-warehouse", &auth, &creds)
                .await;

        assert_eq!(
            result, resolved_prefix,
            "non-SigV4 path must use the prefix from /v1/config overrides"
        );
    }

    // ---------------------------------------------------------------------------
    // R4 — non-SigV4 no-override fallback yields EMPTY prefix (not warehouse)
    // ---------------------------------------------------------------------------

    /// Scenario: A non-SigV4 path whose catalog returns a config body with NO
    /// `overrides.prefix` (e.g. `apache/iceberg-rest-fixture`, plain REST) must
    /// resolve to EMPTY STRING — never to the warehouse value.
    ///
    /// If the warehouse were used as the fallback, `build_load_table_url` would
    /// insert it as a URL path segment and produce
    /// `/v1/s3://warehouse//namespaces/…` → HTTP 400 ("Ambiguous URI empty
    /// segment"). An empty prefix causes `build_load_table_url` to emit the
    /// standard-REST form `/v1/namespaces/{ns}/tables/{table}` with no prefix
    /// segment.
    #[tokio::test]
    async fn non_sigv4_no_config_prefix_yields_empty_not_warehouse() {
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Serve a config body that contains NO overrides.prefix.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let port = addr.port();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 4096];
            let _n = stream.read(&mut buf).await.expect("read");

            // Config body with no overrides.prefix — matches iceberg-rest-fixture behaviour.
            let body = r#"{"overrides":{}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.expect("write");
        });

        let catalog_uri = format!("http://127.0.0.1:{port}");
        let warehouse = "s3://warehouse";
        let creds = creds_no_auth();
        let auth = CatalogAuth::None;

        let client = reqwest::Client::new();
        let result =
            resolve_load_table_prefix(&client, &catalog_uri, warehouse, &auth, &creds).await;

        assert_eq!(
            result, "",
            "non-SigV4 no-override path must return empty string, not the warehouse"
        );
        assert_ne!(
            result, warehouse,
            "warehouse must NOT be used as the URL prefix for non-SigV4 no-override path"
        );

        // Also verify build_load_table_url produces the correct no-prefix URL.
        let url = build_load_table_url(&catalog_uri, &result, "e2e_lakehouse", "events");
        assert!(
            url.contains("/v1/namespaces/e2e_lakehouse/tables/events"),
            "URL must not contain a warehouse path segment: {url}"
        );
        assert!(
            !url.contains("s3://"),
            "URL must not contain the warehouse s3:// URI as a path segment: {url}"
        );
    }

    // ---------------------------------------------------------------------------
    // Group C — CatalogSession::resolve
    // ---------------------------------------------------------------------------

    /// Scenario: `CatalogSession::resolve` on the SigV4 path never contacts the
    /// `/v1/config` endpoint (mirroring `resolve_load_table_prefix`'s short-circuit)
    /// and carries `catalog_uri` verbatim, so it needs no live server at all.
    #[tokio::test]
    async fn catalog_session_resolve_sigv4_no_config_roundtrip() {
        let catalog_uri = "https://glue.us-east-1.amazonaws.com/iceberg";
        let warehouse = "123456789012";

        let mut creds = base_creds();
        creds.use_sigv4 = true;

        let session = CatalogSession::resolve(catalog_uri, warehouse, &creds)
            .await
            .expect("sigv4 session resolution must not fail without any network access");

        assert!(
            matches!(session.auth, CatalogAuth::Sigv4),
            "sigv4 creds must resolve to CatalogAuth::Sigv4"
        );
        assert_eq!(
            session.prefix,
            glue_catalog_prefix(warehouse),
            "sigv4 prefix must be derived from the warehouse, with no /v1/config round-trip"
        );
        assert_eq!(
            session.catalog_uri, catalog_uri,
            "catalog_uri must be carried verbatim"
        );
    }
}

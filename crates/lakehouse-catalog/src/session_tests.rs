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
    let result = resolve_load_table_prefix(&client, &catalog_uri, warehouse, &auth, &creds).await;

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
        resolve_load_table_prefix(&client, &catalog_uri, "original-warehouse", &auth, &creds).await;

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
    let result = resolve_load_table_prefix(&client, &catalog_uri, warehouse, &auth, &creds).await;

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

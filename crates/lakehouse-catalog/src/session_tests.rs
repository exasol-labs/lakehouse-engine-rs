use super::*;
use crate::expiring::EpochClock;
use crate::test_support::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
        "https://glue.us-east-1.amazonaws.com/iceberg/v1/123456789012/namespaces/db/tables/events?snapshots=refs",
        "URL must follow {{uri}}/v1/{{warehouse}}/namespaces/{{ns}}/tables/{{table}} pattern"
    );
}

#[test]
fn the_table_routes_join_a_multi_level_namespace_with_the_unit_separator() {
    let table = TableIdent::from_strs(["sales", "eu", "orders"]).unwrap();

    assert_eq!(
        namespace_tables_path(&table.namespace),
        "namespaces/sales\u{1f}eu/tables"
    );
    assert_eq!(
        table_ident_path(&table),
        "namespaces/sales\u{1f}eu/tables/orders"
    );
}

/// Scenario: A pushdown's loadTable asks only for the referenced snapshots
#[test]
fn build_load_table_url_without_warehouse() {
    let url = build_load_table_url("https://rest.example.com", "", "db", "events");
    assert_eq!(
        url, "https://rest.example.com/v1/namespaces/db/tables/events?snapshots=refs",
        "URL must omit prefix when warehouse is empty"
    );
}

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
            "https://glue.us-east-1.amazonaws.com/iceberg/v1/{prefix}/namespaces/mydb/tables/orders?snapshots=refs"
        ),
        "prefix must be inserted verbatim — `:` and `/` left unencoded"
    );
}

#[test]
fn glue_catalog_prefix_derives_catalogs_segment() {
    assert_eq!(
        glue_catalog_prefix("123456789012"),
        "catalogs/123456789012",
        "Glue prefix must be catalogs/{{warehouse}}"
    );
}

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
        "https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/123456789012/namespaces/db/tables/events?snapshots=refs",
        "derived catalogs/{{account-id}} prefix must appear verbatim in the loadTable URL: {url}"
    );
}

#[test]
fn prefix_from_config_prefers_overrides() {
    let config = serde_json::json!({
        "overrides": {"prefix": "over-prefix"},
        "defaults": {"prefix": "def-prefix"}
    });
    assert_eq!(prefix_from_config(&config), "over-prefix");
}

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

#[test]
fn prefix_from_config_empty_when_absent() {
    assert_eq!(prefix_from_config(&serde_json::json!({})), "");
    assert_eq!(
        prefix_from_config(&serde_json::json!({"overrides": {}, "defaults": {}})),
        ""
    );
    assert_eq!(
        prefix_from_config(&serde_json::json!({"defaults": {"prefix": ""}})),
        ""
    );
}

#[tokio::test]
async fn sigv4_resolve_prefix_derives_catalogs_segment() {
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let port = addr.port();

    let server_prefix = "server-returned-prefix-SHOULD-NOT-BE-USED";

    tokio::spawn(async move {
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
    let auth = CatalogAuth::Sigv4 {
        region: "us-east-1".into(),
    };

    let http = CatalogHttp::new();
    let result = resolve_served_config(&http, &catalog_uri, warehouse, &auth, &creds)
        .await
        .expect("prefix")
        .prefix;

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

#[tokio::test]
async fn non_sigv4_config_prefix_resolution_uses_config_endpoint() {
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let port = addr.port();

    let resolved_prefix = "resolved-prefix-from-config";

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

    let http = CatalogHttp::new();
    let result = resolve_served_config(&http, &catalog_uri, "original-warehouse", &auth, &creds)
        .await
        .expect("prefix")
        .prefix;

    assert_eq!(
        result, resolved_prefix,
        "non-SigV4 path must use the prefix from /v1/config overrides"
    );
}

#[tokio::test]
async fn non_sigv4_no_config_prefix_yields_empty_not_warehouse() {
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let port = addr.port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 4096];
        let _n = stream.read(&mut buf).await.expect("read");

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

    let http = CatalogHttp::new();
    let result = resolve_served_config(&http, &catalog_uri, warehouse, &auth, &creds)
        .await
        .expect("prefix")
        .prefix;

    assert_eq!(
        result, "",
        "non-SigV4 no-override path must return empty string, not the warehouse"
    );
    assert_ne!(
        result, warehouse,
        "warehouse must NOT be used as the URL prefix for non-SigV4 no-override path"
    );

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

/// A minimal `LoadTableResult` a `loadTable` GET deserializes.
const LOAD_TABLE_BODY: &str = r#"{
  "metadata-location": "s3://warehouse/db/events/metadata/v1.json",
  "metadata": {
    "format-version": 2,
    "table-uuid": "00000000-0000-0000-0000-000000000001",
    "location": "s3://warehouse/db/events",
    "last-sequence-number": 0,
    "last-updated-ms": 0,
    "last-column-id": 0,
    "current-schema-id": 0,
    "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "last-partition-id": 0,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0
  },
  "config": {}
}"#;

/// An empty `list_tables` and `list_namespaces` page, ending the enumeration after one level.
const EMPTY_LISTING: &str = r#"{"identifiers":[],"namespaces":[]}"#;

/// Scenario: Session credentials sign every SigV4 catalog request
#[tokio::test]
async fn assumed_session_signs_load_table_and_namespace_enumeration() {
    const WAREHOUSE: &str = "123456789012";
    const BASE_AK: &str = "AKIABASEIDENTITY";

    for vending in [false, true] {
        let (sts, _sts_heads) = spawn_recording_sts(200, ASSUME_ROLE_RESPONSE).await;
        let (load_table_uri, load_table_heads) = spawn_recording_catalog(LOAD_TABLE_BODY).await;
        let (enumeration_uri, enumeration_heads) = spawn_recording_catalog(EMPTY_LISTING).await;
        let stated = ConnectionCreds {
            warehouse: WAREHOUSE.into(),
            access_key: BASE_AK.into(),
            use_sigv4: true,
            use_vended_credentials: vending,
            aws_assume_role_arn: Some("arn:aws:iam::123456789012:role/lakehouse-reader".into()),
            aws_sts_endpoint: Some(sts),
            ..base_creds()
        };

        let session_creds = crate::resolve_aws_identity(stated, &load_table_uri, true)
            .await
            .expect("the stub answers a well-formed AssumeRoleResponse");
        let session = CatalogSession::resolve(&load_table_uri, WAREHOUSE, None, &session_creds)
            .await
            .expect("a SigV4 session resolves without any network access");
        let catalog = CatalogProps {
            warehouse: WAREHOUSE.into(),
            table: "db.events".into(),
        };
        load_table_any_auth(&session, &catalog, &session_creds)
            .await
            .expect("the session-signed loadTable must succeed");
        let enumeration_session =
            CatalogSession::resolve(&enumeration_uri, WAREHOUSE, None, &session_creds)
                .await
                .expect("a SigV4 session resolves without any network access");
        crate::namespace::list_namespace_tables(
            &enumeration_session,
            &["db".to_string()],
            &session_creds,
        )
        .await
        .expect("the session-signed enumeration must succeed");

        let load_table_heads = load_table_heads.lock().unwrap();
        let [load_table] = load_table_heads.as_slice() else {
            panic!(
                "exactly one loadTable request, got {}",
                load_table_heads.len()
            );
        };
        let enumeration_heads = enumeration_heads.lock().unwrap();
        assert!(
            !enumeration_heads.is_empty(),
            "the enumeration must send a request"
        );
        for head in std::iter::once(load_table).chain(enumeration_heads.iter()) {
            let authorization = authorization_header(head).expect("every request must be signed");
            assert!(
                authorization.contains(&format!("Credential={SESSION_AK}/"))
                    && authorization.contains("/glue/aws4_request"),
                "signed by the session key for Glue: {authorization}"
            );
            assert!(
                !authorization.contains(BASE_AK),
                "never signed by the base key pair: {authorization}"
            );
            assert_eq!(
                header_value(head, "x-amz-security-token"),
                Some(SESSION_TOKEN)
            );
            let request_line = head.lines().next().unwrap_or_default();
            assert!(
                request_line.contains(&format!("/v1/catalogs/{WAREHOUSE}/")),
                "the catalogs/<warehouse> prefix is unchanged by the role: {request_line}"
            );
        }
        assert_eq!(
            header_value(load_table, "x-iceberg-access-delegation"),
            vending.then_some("vended-credentials"),
            "a role leaves the access-delegation header to use_vended_credentials={vending}"
        );
    }
}

#[tokio::test]
async fn catalog_session_resolve_sigv4_no_config_roundtrip() {
    let catalog_uri = "https://glue.us-east-1.amazonaws.com/iceberg";
    let warehouse = "123456789012";

    let mut creds = base_creds();
    creds.use_sigv4 = true;
    creds.region = String::new();

    let session = CatalogSession::resolve(catalog_uri, warehouse, None, &creds)
        .await
        .expect("sigv4 session resolution must not fail without any network access");

    assert!(
        matches!(&session.auth, CatalogAuth::Sigv4 { region } if region == "us-east-1"),
        "sigv4 creds must resolve to CatalogAuth::Sigv4 carrying the endpoint's region"
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

/// Scenario: A failed config lookup fails the request
#[tokio::test]
async fn a_failed_config_lookup_fails_the_session() {
    let (uri, requests) = spawn_server("application/json", |_| {
        (404, r#"{"error":"no such warehouse"}"#.to_string())
    })
    .await;

    let Err(UdfError::User(msg)) =
        CatalogSession::resolve(&uri, "wrong-wh", None, &creds_no_auth()).await
    else {
        panic!("a 404 config answer must fail the session");
    };

    assert!(
        msg.starts_with("failed to read the catalog config for warehouse 'wrong-wh': ")
            && msg.contains("HTTP 404"),
        "{msg}"
    );
    let requests = requests.lock().unwrap();
    let [request] = requests.as_slice() else {
        panic!("one config request, got {}", requests.len());
    };
    assert!(
        request.starts_with("GET /v1/config?warehouse=wrong-wh "),
        "{request}"
    );
}

#[test]
fn catalog_urls_ignore_a_trailing_slash_on_the_address() {
    assert_eq!(
        build_load_table_url("http://host/catalog/", "wh", "db", "t"),
        "http://host/catalog/v1/wh/namespaces/db/tables/t?snapshots=refs"
    );
    assert_eq!(
        catalog_url("http://host/catalog/", "", "namespaces"),
        "http://host/catalog/v1/namespaces"
    );
}

/// A session built directly, bypassing `resolve`'s own catalog round-trips, so a test can target
/// only the request under study.
fn session_over(http: CatalogHttp, catalog_uri: &str) -> CatalogSession {
    CatalogSession {
        http,
        catalog_uri: catalog_uri.to_string(),
        auth: CatalogAuth::None,
        prefix: String::new(),
        endpoints: None,
        creds: creds_no_auth(),
    }
}

/// Scenario: A failed loadTable names the table it loads
#[tokio::test]
async fn a_failed_load_table_names_the_table() {
    const IDENT: &str = "sales.eu.orders";
    let catalog = CatalogProps {
        warehouse: "wh".into(),
        table: IDENT.into(),
    };
    let creds = creds_no_auth();

    let (uri_500, _) = spawn_server("application/json", |_| (500, "boom".into())).await;
    let session = session_over(CatalogHttp::new(), &uri_500);
    let message = load_table_any_auth(&session, &catalog, &creds)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        message.contains(IDENT) && message.contains("HTTP 500"),
        "{message}"
    );

    let (uri_404, _) = spawn_server("application/json", |_| (404, "not found".into())).await;
    let session = session_over(CatalogHttp::new(), &uri_404);
    let message = load_table_any_auth(&session, &catalog, &creds)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        message.contains(IDENT) && message.contains("HTTP 404"),
        "{message}"
    );

    let (uri_bad_json, _) = spawn_server("application/json", |_| (200, "not json".into())).await;
    let session = session_over(CatalogHttp::new(), &uri_bad_json);
    let message = load_table_any_auth(&session, &catalog, &creds)
        .await
        .unwrap_err()
        .to_string();
    assert!(message.contains(IDENT), "{message}");

    let session = session_over(CatalogHttp::with_policy(SHORT_POLICY), CLOSED_PORT_URI);
    let message = load_table_any_auth(&session, &catalog, &creds)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        message.contains(IDENT) && message.contains(CLOSED_PORT_CAUSE),
        "{message}"
    );

    let stalling_uri = spawn_header_stalling_server().await;
    let session = session_over(CatalogHttp::with_policy(SHORT_POLICY), &stalling_uri);
    let message = load_table_any_auth(&session, &catalog, &creds)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        message.contains(IDENT) && message.contains(TIMED_OUT_CAUSE),
        "{message}"
    );
}

/// Scenario: Every catalog request kind is bounded and retried
#[tokio::test]
async fn a_resent_sigv4_load_table_carries_its_first_signature() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let answered = AtomicUsize::new(0);
    let (uri, requests) = spawn_server("application/json", move |_| {
        match answered.fetch_add(1, Ordering::SeqCst) {
            0 => (503, "{\"error\":\"busy\"}".into()),
            _ => (200, LOAD_TABLE_BODY.into()),
        }
    })
    .await;
    let mut creds = base_creds();
    creds.use_sigv4 = true;
    creds.warehouse = "123456789012".into();
    let session = CatalogSession::resolve(&uri, &creds.warehouse, None, &creds)
        .await
        .expect("a SigV4 session resolves without any network access");
    let catalog = CatalogProps {
        warehouse: creds.warehouse.clone(),
        table: "db.events".into(),
    };

    load_table_any_auth(&session, &catalog, &creds)
        .await
        .expect("the retried loadTable must succeed");

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        authorization_header(&requests[0]),
        authorization_header(&requests[1]),
        "the re-sent request keeps its first signature"
    );
    assert_eq!(
        header_value(&requests[0], "x-amz-date"),
        header_value(&requests[1], "x-amz-date"),
        "the re-sent request keeps its first x-amz-date"
    );
}

/// Scenario: Every catalog request kind is bounded and retried
#[tokio::test]
async fn every_iceberg_rest_request_kind_succeeds_after_one_503() {
    use crate::client::{CatalogClient, CatalogTableIdent, IcebergRestCatalogClient};
    use std::sync::atomic::{AtomicUsize, Ordering};

    // OAuth2 grant and /v1/config: both run during `CatalogSession::resolve`.
    let oauth_hits = AtomicUsize::new(0);
    let config_hits = AtomicUsize::new(0);
    let (grant_uri, grant_requests) = spawn_server("application/json", move |request| {
        if request.starts_with("POST") {
            return match oauth_hits.fetch_add(1, Ordering::SeqCst) {
                0 => (503, "{\"error\":\"busy\"}".into()),
                _ => (200, format!(r#"{{"access_token":"{OAUTH_ACCESS_TOKEN}"}}"#)),
            };
        }
        match config_hits.fetch_add(1, Ordering::SeqCst) {
            0 => (503, "{\"error\":\"busy\"}".into()),
            _ => (200, r#"{"overrides":{},"defaults":{}}"#.into()),
        }
    })
    .await;
    let mut oauth_creds = creds_no_auth();
    oauth_creds.client_id = Some("client-id".into());
    oauth_creds.client_secret = Some(CLIENT_SECRET.into());

    let oauth_session = CatalogSession::resolve(&grant_uri, "wh", None, &oauth_creds)
        .await
        .expect("a 503 on the grant and the config lookup is retried to success");
    assert_eq!(oauth_session.auth.held_bearers(), [OAUTH_ACCESS_TOKEN]);
    let posts: Vec<String> = grant_requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.starts_with("POST"))
        .cloned()
        .collect();
    assert_eq!(posts.len(), 2);
    assert_eq!(request_body(&posts[0]), request_body(&posts[1]));
    assert!(request_body(&posts[0]).contains("grant_type=client_credentials"));

    let table_hits = AtomicUsize::new(0);
    let ns_hits = AtomicUsize::new(0);
    let (listing_uri, _) = spawn_server("application/json", move |request| {
        if request.starts_with("GET /v1/config") {
            (200, r#"{"overrides":{},"defaults":{}}"#.into())
        } else if request.contains("?parent=") {
            match ns_hits.fetch_add(1, Ordering::SeqCst) {
                0 => (503, "{\"error\":\"busy\"}".into()),
                _ => (200, EMPTY_LISTING.into()),
            }
        } else {
            match table_hits.fetch_add(1, Ordering::SeqCst) {
                0 => (503, "{\"error\":\"busy\"}".into()),
                _ => (
                    200,
                    r#"{"identifiers":[{"namespace":["db"],"name":"orders"}]}"#.into(),
                ),
            }
        }
    })
    .await;
    let listing_session = CatalogSession::resolve(&listing_uri, "wh", None, &creds_no_auth())
        .await
        .expect("the config lookup succeeds");
    let tables = crate::namespace::list_namespace_tables(
        &listing_session,
        &["db".to_string()],
        &creds_no_auth(),
    )
    .await
    .expect("a 503 on the listing is retried to success");
    assert_eq!(tables.len(), 1);

    let load_hits = AtomicUsize::new(0);
    let (enum_uri, _) = spawn_server("application/json", move |request| {
        if request.starts_with("GET /v1/config") {
            (200, r#"{"overrides":{},"defaults":{}}"#.into())
        } else {
            match load_hits.fetch_add(1, Ordering::SeqCst) {
                0 => (503, "{\"error\":\"busy\"}".into()),
                _ => (200, LOAD_TABLE_BODY.into()),
            }
        }
    })
    .await;
    let client = IcebergRestCatalogClient::new(enum_uri, creds_no_auth(), None);
    let ident = CatalogTableIdent {
        namespace: vec!["db".into()],
        name: "orders".into(),
    };
    let listing = client
        .load_listed_tables(std::slice::from_ref(&ident))
        .await
        .expect("a 503 on the enumeration load is retried to success");
    assert_eq!(listing.tables.len(), 1);
    assert_eq!(listing.tables[0].ident.name, "orders");
    assert!(listing.skipped.is_empty());

    let pushdown_hits = AtomicUsize::new(0);
    let (pushdown_uri, _) = spawn_server("application/json", move |request| {
        if request.starts_with("GET /v1/config") {
            (200, r#"{"overrides":{},"defaults":{}}"#.into())
        } else {
            match pushdown_hits.fetch_add(1, Ordering::SeqCst) {
                0 => (503, "{\"error\":\"busy\"}".into()),
                _ => (200, LOAD_TABLE_BODY.into()),
            }
        }
    })
    .await;
    let pushdown_session = CatalogSession::resolve(&pushdown_uri, "wh", None, &creds_no_auth())
        .await
        .expect("the config lookup succeeds");
    let catalog = CatalogProps {
        warehouse: "wh".into(),
        table: "db.events".into(),
    };
    load_table_any_auth(&pushdown_session, &catalog, &creds_no_auth())
        .await
        .expect("a 503 on the pushdown load is retried to success");
}

const CREDENTIALS_ROUTE: &str =
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials";

fn table_t() -> TableIdent {
    TableIdent::from_strs(["db", "t"]).expect("a valid identifier")
}

/// [`session_over`] with a REST prefix, the given auth, and CONNECTION credentials.
fn prefixed_session(auth: CatalogAuth, creds: ConnectionCreds) -> CatalogSession {
    CatalogSession {
        prefix: "wh-prefix".into(),
        auth,
        creds,
        ..session_over(CatalogHttp::new(), "https://catalog.example.com/catalog")
    }
}

#[tokio::test]
async fn the_request_method_builds_an_authorized_prefixed_request() {
    let body = serde_json::json!({"name": "t"});
    for (vends, sent_body) in [(true, Some(&body)), (false, None)] {
        let creds = ConnectionCreds {
            use_vended_credentials: vends,
            ..creds_no_auth()
        };
        let session = prefixed_session(CatalogAuth::Bearer(BEARER_TOK.into()), creds);

        let request = session
            .build_request(Method::POST, "namespaces/db/tables", sent_body)
            .await
            .expect("the request builds");

        assert_eq!(request.method(), Method::POST);
        assert_eq!(
            request.url().as_str(),
            "https://catalog.example.com/catalog/v1/wh-prefix/namespaces/db/tables"
        );
        let header = |name: &str| {
            request
                .headers()
                .get(name)
                .map(|value| value.to_str().unwrap().to_string())
        };
        assert_eq!(
            header("authorization"),
            Some(format!("Bearer {BEARER_TOK}"))
        );
        assert_eq!(
            header("x-iceberg-access-delegation").as_deref(),
            vends.then_some("vended-credentials"),
            "vends={vends}"
        );
        assert_eq!(
            header("content-type").as_deref(),
            sent_body.map(|_| "application/json"),
            "vends={vends}"
        );
        let sent = request.body().and_then(|body| body.as_bytes());
        assert_eq!(
            sent.map(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).unwrap()),
            sent_body.cloned()
        );
    }
}

/// Scenario: A request that changes catalog state is sent once after a transient failure
#[tokio::test]
async fn the_request_method_returns_a_whole_answer_and_repeats_only_a_safe_request() {
    let gateway_page = "x".repeat(100 * 1024);
    for (safety, attempts, status) in [(RepeatSafety::Unsafe, 1, 504), (RepeatSafety::Safe, 2, 200)]
    {
        let answered = std::sync::atomic::AtomicUsize::new(0);
        let page = gateway_page.clone();
        let (uri, requests) = spawn_server("text/html", move |_| {
            match answered.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                0 => (504, page.clone()),
                _ => (200, "{}".into()),
            }
        })
        .await;
        let session = session_over(CatalogHttp::with_policy(SHORT_POLICY), &uri);

        let answer = session
            .post(
                "namespaces",
                &serde_json::json!({"namespace": ["db"]}),
                safety,
            )
            .await
            .unwrap();

        assert_eq!(answer.status, status, "{safety:?}");
        assert_eq!(requests.lock().unwrap().len(), attempts, "{safety:?}");
        if safety == RepeatSafety::Unsafe {
            assert_eq!(answer.body, gateway_page.as_bytes(), "no 64 KiB cap");
        }
    }
}

#[tokio::test]
async fn a_send_without_an_answer_tells_whether_the_catalog_may_have_applied_it() {
    let refused = session_over(CatalogHttp::with_policy(SHORT_POLICY), CLOSED_PORT_URI);
    let stalling = session_over(
        CatalogHttp::with_policy(SHORT_POLICY),
        &spawn_header_stalling_server().await,
    );

    let body = serde_json::json!({"namespace": ["db"]});
    let unsent = refused
        .post("namespaces", &body, RepeatSafety::Unsafe)
        .await
        .err()
        .expect("a refused connection fails");
    let unknown = stalling
        .post("namespaces", &body, RepeatSafety::Unsafe)
        .await
        .err()
        .expect("a stalled answer fails");

    assert!(matches!(unsent, CatalogSendError::Unsent(_)), "{unsent:?}");
    assert!(
        matches!(unknown, CatalogSendError::OutcomeUnknown(_)),
        "{unknown:?}"
    );
    let unsent = UdfError::from(unsent).to_string();
    let unknown = UdfError::from(unknown).to_string();
    assert!(
        unsent.starts_with("the catalog request POST ") && unsent.contains(CLOSED_PORT_CAUSE),
        "{unsent}"
    );
    assert!(unknown.contains(TIMED_OUT_CAUSE), "{unknown}");
}

#[test]
fn a_quoted_body_is_stripped_of_the_session_secrets_and_cut() {
    let creds = ConnectionCreds {
        client_secret: Some(CLIENT_SECRET.into()),
        ..creds_no_auth()
    };
    let session = prefixed_session(CatalogAuth::Bearer(BEARER_TOK.into()), creds);
    let body = format!(
        "{BEARER_TOK} {CLIENT_SECRET} {}",
        "x".repeat(crate::catalog_http::MAX_QUOTED_BODY_BYTES)
    );

    let quoted = session.quote(body.as_bytes());

    assert!(!quoted.contains(BEARER_TOK), "{quoted}");
    assert!(!quoted.contains(CLIENT_SECRET), "{quoted}");
    assert!(quoted.contains("truncated"), "{quoted}");
}

#[test]
fn a_table_exists_on_a_2xx_answer_and_is_absent_on_404() {
    for (status, existence) in [
        (200, Some(true)),
        (204, Some(true)),
        (404, Some(false)),
        (403, None),
        (409, None),
        (500, None),
    ] {
        assert_eq!(table_existence(status), existence, "HTTP {status}");
    }
}

#[test]
fn offered_endpoints_are_the_routes_the_config_lists() {
    let listing = serde_json::json!({"endpoints": [
        "get /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
        "not-a-route",
    ]});
    for (config, offered) in [
        (listing, true),
        (
            serde_json::json!({"endpoints": ["GET /v1/{prefix}/namespaces"]}),
            false,
        ),
        (serde_json::json!({"defaults": {}}), false),
    ] {
        let session = CatalogSession {
            endpoints: endpoints_from_config(&config),
            ..session_over(CatalogHttp::new(), "https://catalog.example.com")
        };

        assert_eq!(
            session.offers_endpoint(CREDENTIALS_ROUTE),
            offered,
            "{config}"
        );
        assert!(!session.offers_endpoint("not-a-route"), "{config}");
    }
}

const CREDENTIALS_ANSWER: &str = r#"{"storage-credentials": [
  {"prefix": "s3://bucket/", "config": {"s3.access-key-id": "WIDE_AK", "s3.secret-access-key": "WIDE_SK"}},
  {"prefix": "S3://bucket/db/t", "config": {"s3.access-key-id": "TABLE_AK", "s3.secret-access-key": "TABLE_SK"}}
]}"#;

#[test]
fn a_credentials_answer_yields_the_longest_covering_entry() {
    let credentials = read_credentials(CREDENTIALS_ANSWER.as_bytes(), "s3://bucket/db/t/data")
        .expect("an entry covers the location");

    assert_eq!(credentials["s3.access-key-id"], "TABLE_AK");
}

#[test]
fn a_credentials_answer_that_covers_nothing_or_is_malformed_fails_quoting_none_of_it() {
    for (body, location, cause) in [
        (
            CREDENTIALS_ANSWER,
            "s3://other/db/t",
            "covers s3://other/db/t",
        ),
        (
            r#"{"storage-credentials": "WIDE_AK"}"#,
            "s3://bucket/db/t",
            "invalid JSON at line 1",
        ),
    ] {
        let error = read_credentials(body.as_bytes(), location).unwrap_err();

        assert!(error.contains(cause), "{error}");
        assert!(
            !error.contains("WIDE_AK") && !error.contains("TABLE_AK"),
            "{error}"
        );
    }
}

/// The answers of a catalog that is briefly unavailable once for each request kind, then serves a
/// table and its credentials.
fn busy_once_catalog() -> impl Fn(&str) -> (u16, String) + Send + 'static {
    let seen = Mutex::new(std::collections::HashSet::new());
    move |request: &str| {
        let line = request.lines().next().unwrap_or_default().to_string();
        if seen.lock().unwrap().insert(line.clone()) {
            return (503, "{\"error\":\"busy\"}".into());
        }
        match line.split(' ').nth(1).unwrap_or_default() {
            path if path.ends_with("/credentials") => (200, CREDENTIALS_ANSWER.into()),
            path if path.ends_with("?snapshots=all") => (200, LOAD_TABLE_BODY.into()),
            _ => (204, String::new()),
        }
    }
}

#[tokio::test]
async fn every_session_read_is_repeated_after_a_transient_failure() {
    let (uri, requests) = spawn_server("application/json", busy_once_catalog()).await;
    let session = CatalogSession {
        creds: ConnectionCreds {
            use_vended_credentials: true,
            ..creds_no_auth()
        },
        ..session_over(CatalogHttp::with_policy(SHORT_POLICY), &uri)
    };
    let table = table_t();

    let exists = session.table_exists(&table).await.expect("HEAD succeeds");
    let loaded = session
        .load_table_with_all_snapshots(&table)
        .await
        .expect("the load succeeds");
    let credentials = session
        .load_credentials(&table, "s3://bucket/db/t")
        .await
        .expect("loadCredentials succeeds");

    assert!(exists);
    assert_eq!(loaded.metadata.location(), "s3://warehouse/db/events");
    assert_eq!(credentials.access_key_id, "TABLE_AK");
    let requests = requests.lock().unwrap();
    let lines: Vec<&str> = requests
        .iter()
        .map(|request| request.lines().next().unwrap_or_default())
        .collect();
    assert_eq!(
        lines,
        [
            "HEAD /v1/namespaces/db/tables/t HTTP/1.1",
            "HEAD /v1/namespaces/db/tables/t HTTP/1.1",
            "GET /v1/namespaces/db/tables/t?snapshots=all HTTP/1.1",
            "GET /v1/namespaces/db/tables/t?snapshots=all HTTP/1.1",
            "GET /v1/namespaces/db/tables/t/credentials HTTP/1.1",
            "GET /v1/namespaces/db/tables/t/credentials HTTP/1.1",
        ]
    );
    assert!(requests.iter().all(
        |request| header_value(request, "x-iceberg-access-delegation")
            == Some("vended-credentials")
    ));
}

#[tokio::test]
async fn a_session_read_that_stays_refused_names_the_table_and_the_status() {
    let (uri, _) = spawn_server("application/json", |_| (503, "{}".into())).await;
    let session = session_over(CatalogHttp::with_policy(SHORT_POLICY), &uri);
    let table = table_t();

    let existence = session.table_exists(&table).await.unwrap_err().to_string();
    let credentials = session
        .load_credentials(&table, "s3://bucket/db/t")
        .await
        .unwrap_err()
        .to_string();

    assert!(
        existence.contains("'db.t'") && existence.contains("HTTP 503"),
        "{existence}"
    );
    assert!(
        credentials.contains("'db.t'") && credentials.contains("HTTP 503"),
        "{credentials}"
    );
}

/// A token endpoint that answers grant N with `tok-N`, stating `expires_in` when given, and a
/// catalog that answers every other request with `answer`.
fn granting_catalog(
    expires_in: Option<u64>,
    answer: (u16, &'static str),
) -> impl Fn(&str) -> (u16, String) + Send + 'static {
    let grants = std::sync::atomic::AtomicUsize::new(0);
    move |request: &str| {
        if !request.starts_with("POST /v1/oauth/tokens") {
            return (answer.0, answer.1.into());
        }
        let grant = grants.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let lifetime = expires_in.map_or(String::new(), |secs| format!(r#","expires_in":{secs}"#));
        (
            200,
            format!(r#"{{"access_token":"tok-{grant}"{lifetime}}}"#),
        )
    }
}

/// A session in OAuth2 mode whose first grant ran at the time `clock` reads.
async fn oauth_session(catalog_uri: &str, clock: EpochClock) -> CatalogSession {
    let creds = ConnectionCreds {
        client_id: Some("client-id".into()),
        client_secret: Some(CLIENT_SECRET.into()),
        ..creds_no_auth()
    };
    let http = CatalogHttp::new();
    let auth = resolve_catalog_auth(&http, catalog_uri, &creds, clock)
        .await
        .expect("the first grant succeeds");
    CatalogSession {
        auth,
        creds,
        ..session_over(http, catalog_uri)
    }
}

/// The bearer of every request other than a grant, in order.
fn catalog_bearers(requests: &Mutex<Vec<String>>) -> Vec<String> {
    requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| !request.starts_with("POST /v1/oauth/tokens"))
        .map(|request| {
            authorization_header(request)
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

/// Scenario: An OAuth2 token that reaches its renewal point is renewed once
#[tokio::test]
async fn a_token_at_its_renewal_point_is_granted_anew_once_for_the_requests_after_it() {
    let (uri, requests) =
        spawn_server("application/json", granting_catalog(Some(120), (204, ""))).await;
    let time = Arc::new(AtomicU64::new(0));
    let session = oauth_session(&uri, clock_reading(&time)).await;
    let table = table_t();

    session.table_exists(&table).await.unwrap();
    time.store(61_000, Ordering::SeqCst);
    let (first, second) = tokio::join!(session.table_exists(&table), session.table_exists(&table));
    session.table_exists(&table).await.unwrap();

    assert!(first.unwrap() && second.unwrap());
    assert_eq!(
        catalog_bearers(&requests),
        [
            "Bearer tok-1",
            "Bearer tok-2",
            "Bearer tok-2",
            "Bearer tok-2"
        ]
    );
    assert_eq!(session.auth.held_bearers(), ["tok-1", "tok-2"]);
}

/// Scenario: An OAuth2 token that reaches its renewal point is renewed once
#[tokio::test]
async fn a_token_granted_without_expires_in_is_kept_for_the_life_of_the_session() {
    let (uri, requests) = spawn_server("application/json", granting_catalog(None, (204, ""))).await;
    let time = Arc::new(AtomicU64::new(0));
    let session = oauth_session(&uri, clock_reading(&time)).await;

    time.store(u64::MAX, Ordering::SeqCst);
    session.table_exists(&table_t()).await.unwrap();

    assert_eq!(catalog_bearers(&requests), ["Bearer tok-1"]);
}

/// Scenario: An OAuth2 token that reaches its renewal point is renewed once
#[tokio::test]
async fn no_token_the_session_held_appears_in_an_error() {
    let (uri, _) = spawn_server(
        "application/json",
        granting_catalog(Some(120), (401, "rejected tok-1 and tok-2")),
    )
    .await;
    let time = Arc::new(AtomicU64::new(0));
    let session = oauth_session(&uri, clock_reading(&time)).await;

    time.store(61_000, Ordering::SeqCst);
    let message = session
        .load_table_with_all_snapshots(&table_t())
        .await
        .unwrap_err()
        .to_string();

    assert!(message.contains("HTTP 401"), "{message}");
    assert!(
        !message.contains("tok-1") && !message.contains("tok-2"),
        "{message}"
    );
    assert_eq!(session.auth.held_bearers(), ["tok-1", "tok-2"]);
}

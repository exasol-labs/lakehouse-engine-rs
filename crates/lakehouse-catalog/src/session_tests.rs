use super::*;
use crate::test_support::*;

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

#[test]
fn build_load_table_url_without_warehouse() {
    let url = build_load_table_url("https://rest.example.com", "", "db", "events");
    assert_eq!(
        url, "https://rest.example.com/v1/namespaces/db/tables/events",
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
            "https://glue.us-east-1.amazonaws.com/iceberg/v1/{prefix}/namespaces/mydb/tables/orders"
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
        "https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/123456789012/namespaces/db/tables/events",
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
    let result = resolve_load_table_prefix(&http, &catalog_uri, warehouse, &auth, &creds)
        .await
        .expect("prefix");

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
    let result =
        resolve_load_table_prefix(&http, &catalog_uri, "original-warehouse", &auth, &creds)
            .await
            .expect("prefix");

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
    let result = resolve_load_table_prefix(&http, &catalog_uri, warehouse, &auth, &creds)
        .await
        .expect("prefix");

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
        let session = CatalogSession::resolve(&load_table_uri, WAREHOUSE, &session_creds)
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
            CatalogSession::resolve(&enumeration_uri, WAREHOUSE, &session_creds)
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

    let session = CatalogSession::resolve(catalog_uri, warehouse, &creds)
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
        CatalogSession::resolve(&uri, "wrong-wh", &creds_no_auth()).await
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
        "http://host/catalog/v1/wh/namespaces/db/tables/t"
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

    let session = session_over(CatalogHttp::new(), CLOSED_PORT_URI);
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
        message.contains(IDENT) && message.contains(SHORT_POLICY_SPENT_BOUND),
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
    let session = CatalogSession::resolve(&uri, &creds.warehouse, &creds)
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

    let oauth_session = CatalogSession::resolve(&grant_uri, "wh", &oauth_creds)
        .await
        .expect("a 503 on the grant and the config lookup is retried to success");
    assert!(matches!(oauth_session.auth, CatalogAuth::Bearer(ref t) if t == OAUTH_ACCESS_TOKEN));
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
    let listing_session = CatalogSession::resolve(&listing_uri, "wh", &creds_no_auth())
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
    let client = IcebergRestCatalogClient::new(enum_uri, creds_no_auth());
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
    let pushdown_session = CatalogSession::resolve(&pushdown_uri, "wh", &creds_no_auth())
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

use super::*;
use crate::test_support::*;
use std::sync::{Arc, Mutex};

#[test]
fn disabled_sigv4_produces_no_auth_header_in_request() {
    let client = reqwest::Client::new();
    let request = client
        .get("https://minio.local:9000/iceberg/v1/namespaces/db/tables/events")
        .build()
        .expect("valid request");

    assert!(
        request.headers().get("authorization").is_none(),
        "unsigned path: no Authorization header expected"
    );
    assert!(
        request.headers().get("x-amz-date").is_none(),
        "unsigned path: no x-amz-date header expected"
    );
}

#[test]
fn signed_request_does_not_leak_keys_in_headers() {
    let secret = "wJalrXUtnFEMI_EXAMPLE_KEY";
    let client = reqwest::Client::new();
    let request = client
        .get("https://glue.us-east-1.amazonaws.com/iceberg/v1/123/namespaces/db/tables/t")
        .build()
        .expect("valid request");

    let signed =
        crate::sigv4::sign_request(request, "AKIDEXAMPLE", secret, None, "us-east-1", "glue")
            .expect("signing must succeed");

    for (name, value) in signed.headers().iter() {
        let v = value.to_str().unwrap_or("");
        assert!(
            !v.contains(secret),
            "secret key must not appear in signed header '{name}': {v}"
        );
    }
}

/// Scenario: Static bearer token is attached to unsigned catalog requests.
#[test]
fn bearer_token_attached_to_load_table_request() {
    let client = reqwest::Client::new();
    let url = "https://catalog.example.com/v1/namespaces/db/tables/t";

    let request = client
        .get(url)
        .header("accept", "application/json")
        .bearer_auth(BEARER_TOK)
        .build()
        .expect("valid request");

    let auth_header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    assert!(
        auth_header.starts_with("Bearer "),
        "authorization header must start with 'Bearer ': {auth_header}"
    );
    assert!(
        auth_header.contains(BEARER_TOK),
        "bearer token must appear in the authorization header"
    );

    assert!(
        request.headers().get("x-amz-date").is_none(),
        "bearer-auth must not set x-amz-date"
    );
    assert!(
        request.headers().get("x-amz-security-token").is_none(),
        "bearer-auth must not set x-amz-security-token"
    );
}

#[test]
fn no_auth_load_table_sends_no_authorization() {
    let client = reqwest::Client::new();
    let request = client
        .get("https://catalog.example.com/v1/namespaces/db/tables/t")
        .header("accept", "application/json")
        .build()
        .expect("valid request");

    assert!(
        request.headers().get("authorization").is_none(),
        "no-auth path must not set Authorization header"
    );
    assert!(
        request.headers().get("x-amz-date").is_none(),
        "no-auth path must not set x-amz-date"
    );
}

#[tokio::test]
async fn catalog_error_message_uses_http_status_prefix() {
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const NOT_ICEBERG_BODY: &str =
        "NoSuchIcebergTableException: Input table is not an iceberg table";

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let port = addr.port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 4096];
        let _ = stream.read(&mut buf).await.expect("read");
        let response = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            NOT_ICEBERG_BODY.len(),
            NOT_ICEBERG_BODY
        );
        stream.write_all(response.as_bytes()).await.expect("write");
    });

    let url = format!("http://127.0.0.1:{port}/v1/warehouse/namespaces/db/tables/hive_table");
    let creds = creds_no_auth();
    let client = reqwest::Client::new();
    let err =
        authed_get_json::<serde_json::Value>(&client, &url, &CatalogAuth::None, false, &creds)
            .await
            .expect_err("a 404 response must surface as an error");

    let msg = err.to_string();
    assert!(
        msg.starts_with("catalog returned HTTP 404: "),
        "the classifier's load-bearing prefix must be emitted verbatim, got: {msg}"
    );
    assert!(
        msg.contains(NOT_ICEBERG_BODY),
        "the (credential-free) response body must follow the status prefix, got: {msg}"
    );
}

#[tokio::test]
async fn sigv4_request_is_signed_for_the_carried_region() {
    let (catalog_uri, heads) = spawn_recording_catalog("{}").await;
    let mut creds = creds_no_auth();
    creds.use_sigv4 = true;
    creds.region = String::new();
    let auth = CatalogAuth::Sigv4 {
        region: "eu-west-1".into(),
    };
    let url = format!("{catalog_uri}/v1/catalogs/123456789012/namespaces/db/tables/t");

    authed_get_json::<serde_json::Value>(&reqwest::Client::new(), &url, &auth, false, &creds)
        .await
        .expect("the signed request must reach the stub and succeed");

    let heads = heads.lock().unwrap();
    let [head] = heads.as_slice() else {
        panic!(
            "exactly one catalog request must be sent, got {}",
            heads.len()
        );
    };
    let authorization = authorization_header(head).expect("the request must be signed");
    assert!(
        authorization.contains("/eu-west-1/glue/aws4_request"),
        "the credential scope must name the carried region: {authorization}"
    );
}

#[tokio::test]
async fn load_table_error_redacts_session_bearer_and_static_secrets() {
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let port = addr.port();

    let body = format!("error: secret={CLIENT_SECRET} bearer={OAUTH_ACCESS_TOKEN}");

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 4096];
        let _ = stream.read(&mut buf).await.expect("read");
        let response = format!(
            "HTTP/1.1 401 Unauthorized\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.expect("write");
    });

    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/v1/namespaces/db/tables/t");
    let mut creds = creds_no_auth();
    creds.client_secret = Some(CLIENT_SECRET.into());
    let auth = CatalogAuth::Bearer(OAUTH_ACCESS_TOKEN.to_string());

    let err = authed_get_json::<serde_json::Value>(&client, &url, &auth, false, &creds)
        .await
        .expect_err("a 401 response must surface as an error");

    let msg = err.to_string();
    assert!(
        msg.starts_with("catalog returned HTTP 401: "),
        "the redaction closure must have run against the real 401 body, got: {msg}"
    );
    assert!(
        !msg.contains(CLIENT_SECRET),
        "static client_secret must not appear in error: {msg}"
    );
    assert!(
        !msg.contains(OAUTH_ACCESS_TOKEN),
        "live session bearer token must not appear in error: {msg}"
    );
}

async fn post_to(
    status: u16,
    body: String,
) -> (Result<PostAnswer, UdfError>, Arc<Mutex<Vec<String>>>) {
    let (uri, requests) = spawn_server("application/json", move |_| (status, body.clone())).await;
    let auth = CatalogAuth::Bearer(BEARER_TOK.into());
    let answer = authed_post_json(
        &reqwest::Client::new(),
        &format!("{uri}/check"),
        &serde_json::json!({"k": 1}),
        &auth,
        &base_creds(),
        Duration::from_secs(5),
    )
    .await;
    (answer, requests)
}

#[tokio::test]
async fn a_post_sends_its_json_body_with_the_bearer_and_returns_the_answer() {
    let (answer, requests) = post_to(200, r#"{"ok":true}"#.into()).await;

    assert!(
        matches!(answer, Ok(PostAnswer::Accepted { status: 200, ref body }) if body == r#"{"ok":true}"#)
    );
    let request = requests.lock().unwrap()[0].to_lowercase();
    assert!(request.starts_with("post /check"), "{request}");
    assert!(request.contains(&format!("bearer {}", BEARER_TOK.to_lowercase())));
    assert!(request.ends_with(r#"{"k":1}"#), "{request}");
}

#[tokio::test]
async fn a_refusal_is_a_value_whose_quoted_body_is_redacted_and_cut() {
    let echoed = format!("{BEARER_TOK} {}", "x".repeat(MAX_QUOTED_BODY_BYTES * 2));

    let (answer, _) = post_to(403, echoed).await;

    let Ok(PostAnswer::Refused { status: 403, body }) = answer else {
        panic!("a non-2xx answer must be a refusal");
    };
    assert!(!body.contains(BEARER_TOK), "{body}");
    assert!(body.contains("truncated"), "{body}");
}

#[tokio::test]
async fn an_answer_over_the_read_limit_is_an_error() {
    let (answer, _) = post_to(200, "x".repeat(MAX_ANSWER_BYTES + 1)).await;

    let Err(UdfError::User(message)) = answer else {
        panic!("an oversized answer must be an error");
    };
    assert!(message.contains("exceeds"), "{message}");
}

#[tokio::test]
async fn a_post_to_a_closed_port_is_an_error() {
    let auth = CatalogAuth::None;

    let answer = authed_post_json(
        &reqwest::Client::new(),
        "http://127.0.0.1:1/check",
        &serde_json::json!({}),
        &auth,
        &base_creds(),
        Duration::from_secs(5),
    )
    .await;

    assert!(matches!(answer, Err(UdfError::User(m)) if m.contains("catalog request failed")));
}

#[tokio::test]
async fn a_catalog_that_never_answers_fails_within_the_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/check", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _held = listener.accept().await;
        std::future::pending::<()>().await;
    });

    let answer = authed_post_json(
        &reqwest::Client::new(),
        &url,
        &serde_json::json!({}),
        &CatalogAuth::None,
        &base_creds(),
        Duration::from_millis(200),
    )
    .await;

    assert!(matches!(answer, Err(UdfError::User(m)) if m.contains("200ms")));
}

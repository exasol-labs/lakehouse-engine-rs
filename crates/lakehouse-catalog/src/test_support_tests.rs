use crate::{ConnectionCreds, StorageBackend, StorageProps};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub(crate) fn base_creds() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "warehouse".into(),
        endpoint: "http://minio:9000".into(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        session_token: None,
        path_style: Some(true),
        use_sigv4: false,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}

pub(crate) fn s3_payload(backend: StorageBackend) -> StorageProps {
    match backend {
        StorageBackend::S3(props) => props,
        StorageBackend::Adls { .. } => panic!("s3_payload is S3-only"),
    }
}

pub(crate) const STATIC_AK: &str = "STATIC_AK_SENTINEL";
pub(crate) const STATIC_SK: &str = "STATIC_SK_SENTINEL";
pub(crate) const BEARER_TOK: &str = "BEARER_TOKEN_SENTINEL_VALUE";
pub(crate) const CLIENT_SECRET: &str = "CLIENT_SECRET_SENTINEL_VALUE";
pub(crate) const OAUTH_ACCESS_TOKEN: &str = "OAUTH_OBTAINED_ACCESS_TOKEN";

pub(crate) fn creds_no_auth() -> ConnectionCreds {
    ConnectionCreds {
        warehouse: "warehouse".into(),
        endpoint: "https://s3.amazonaws.com".into(),
        region: "us-east-1".into(),
        access_key: STATIC_AK.into(),
        secret_key: STATIC_SK.into(),
        session_token: None,
        path_style: Some(false),
        use_sigv4: false,
        use_vended_credentials: false,
        token: None,
        client_id: None,
        client_secret: None,
        oauth2_server_uri: None,
        scope: None,
        account_name: None,
        account_key: None,
        sas_token: None,
    }
}

/// Answers every request with HTTP 200 and `body`, recording each request head in order.
pub(crate) async fn spawn_recording_catalog(
    body: &'static str,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let base_uri = format!("http://{}", listener.local_addr().expect("local_addr"));
    let heads = Arc::new(Mutex::new(Vec::new()));
    let recorded = heads.clone();

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let head = read_request_head(&mut stream).await;
            recorded.lock().unwrap().push(head);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });

    (base_uri, heads)
}

type Routes = HashMap<String, Vec<(u16, String)>>;

pub(crate) struct RoutingCatalog {
    pub(crate) base_uri: String,
    pub(crate) heads: Arc<Mutex<Vec<String>>>,
    pub(crate) max_in_flight: Arc<AtomicUsize>,
}

impl RoutingCatalog {
    pub(crate) fn request_lines(&self) -> Vec<String> {
        self.heads
            .lock()
            .unwrap()
            .iter()
            .map(|head| head.lines().next().unwrap_or_default().to_string())
            .collect()
    }
}

/// Answers each request target (path plus query) with the next of its listed
/// `(status, body)` responses, repeating the last; an unlisted target gets 404.
/// Every connection is served concurrently after `delay`, so overlap is observable.
pub(crate) async fn spawn_routing_catalog(
    routes: Vec<(&str, Vec<(u16, String)>)>,
    delay: Duration,
) -> RoutingCatalog {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let base_uri = format!("http://{}", listener.local_addr().expect("local_addr"));
    let heads = Arc::new(Mutex::new(Vec::new()));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let routes: Arc<Mutex<Routes>> = Arc::new(Mutex::new(
        routes
            .into_iter()
            .map(|(target, responses)| (target.to_string(), responses))
            .collect(),
    ));

    let recorded = heads.clone();
    let peak = max_in_flight.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let recorded = recorded.clone();
            let peak = peak.clone();
            let in_flight = in_flight.clone();
            let routes = routes.clone();
            tokio::spawn(async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                let head = read_request_head(&mut stream).await;
                let target = head
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or_default()
                    .to_string();
                recorded.lock().unwrap().push(head);
                let (status, body) = {
                    let mut routes = routes.lock().unwrap();
                    match routes.get_mut(&target) {
                        Some(responses) if responses.len() > 1 => responses.remove(0),
                        Some(responses) => responses[0].clone(),
                        None => (404, r#"{"error":"no route"}"#.to_string()),
                    }
                };
                tokio::time::sleep(delay).await;
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });

    RoutingCatalog {
        base_uri,
        heads,
        max_in_flight,
    }
}

pub(crate) fn list_tables_page(
    namespace: &[&str],
    tables: &[&str],
    next_page_token: Option<&str>,
) -> String {
    let identifiers: Vec<serde_json::Value> = tables
        .iter()
        .map(|name| serde_json::json!({ "namespace": namespace, "name": name }))
        .collect();
    let mut page = serde_json::json!({ "identifiers": identifiers });
    if let Some(token) = next_page_token {
        page["next-page-token"] = serde_json::json!(token);
    }
    page.to_string()
}

pub(crate) fn list_namespaces_page(
    namespaces: &[&[&str]],
    next_page_token: Option<&str>,
) -> String {
    let mut page = serde_json::json!({ "namespaces": namespaces });
    if let Some(token) = next_page_token {
        page["next-page-token"] = serde_json::json!(token);
    }
    page.to_string()
}

pub(crate) fn load_table_body(table: &str) -> String {
    serde_json::json!({
        "metadata-location": format!("s3://bucket/{table}/metadata/v1.json"),
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": format!("s3://bucket/{table}"),
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "name", "required": false, "type": "string"}
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0
        }
    })
    .to_string()
}

async fn read_request_head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8_lossy(&head).into_owned()
}

pub(crate) fn authorization_header(head: &str) -> Option<&str> {
    head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("authorization")
            .then(|| value.trim())
    })
}

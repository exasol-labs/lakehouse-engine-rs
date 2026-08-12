//! A minimal in-process HTTP/1.1 server for the Unity Catalog client and auth
//! tests. It records every request it receives and answers each from a
//! caller-supplied responder, so a test asserts the exact request shape and
//! scripts a status/body per request without any live network.
//!
//! Consumers: `unity::client::tests`, `unity::auth::tests`.

use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// One request the mock server received: the request line's method and target
/// (path plus query), the `Authorization` header if present, and the body.
pub(crate) struct RecordedRequest {
    pub method: String,
    pub target: String,
    pub authorization: Option<String>,
    pub body: String,
}

/// A running mock server: its base URL and every request it has served so far.
pub(crate) struct MockServer {
    pub base_url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl MockServer {
    pub(crate) fn requests(&self) -> std::sync::MutexGuard<'_, Vec<RecordedRequest>> {
        self.requests.lock().unwrap()
    }
}

/// Spawn a server answering each request from `responder`, which returns the
/// `(status_code, body)` to send back. Every request is recorded before the
/// responder runs. Responses close the connection so the pooled `reqwest` client
/// opens a fresh connection per request, letting a single accept loop serve the
/// whole sequential request stream in order.
pub(crate) async fn spawn<F>(responder: F) -> MockServer
where
    F: Fn(&RecordedRequest) -> (u16, String) + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let base_url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let responder = Arc::new(responder);

    let server_requests = requests.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut buf = vec![0u8; 16384];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            if n == 0 {
                continue;
            }
            let raw = String::from_utf8_lossy(&buf[..n]).to_string();
            let recorded = parse_request(&raw);
            let (status, body) = responder(&recorded);
            server_requests.lock().unwrap().push(recorded);
            let reason = if (200..300).contains(&status) {
                "OK"
            } else {
                "ERROR"
            };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });

    MockServer { base_url, requests }
}

fn parse_request(raw: &str) -> RecordedRequest {
    let request_line = raw.lines().next().unwrap_or("");
    let mut fields = request_line.split_whitespace();
    let method = fields.next().unwrap_or("").to_string();
    let target = fields.next().unwrap_or("").to_string();
    let mut authorization = None;
    for line in raw.lines() {
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("authorization")
        {
            authorization = Some(value.trim().to_string());
        }
    }
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .unwrap_or("")
        .to_string();
    RecordedRequest {
        method,
        target,
        authorization,
        body,
    }
}

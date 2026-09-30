use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TARGET_PREFIX: &str = "AWSGlue.";
const JSON_1_1: &str = "application/x-amz-json-1.1";

#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    pub method: String,
    pub path: String,
    headers: Vec<(String, String)>,
    pub body: String,
}

impl RecordedRequest {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// The Glue operation named by `X-Amz-Target`; `None` for a storage request.
    pub(crate) fn operation(&self) -> Option<&str> {
        self.header("x-amz-target")
            .and_then(|target| target.strip_prefix(TARGET_PREFIX))
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).expect("a Glue request body is JSON")
    }
}

pub(crate) struct MockResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
    date: Option<String>,
    hang: bool,
}

impl MockResponse {
    pub(crate) fn json(body: serde_json::Value) -> Self {
        Self {
            status: 200,
            content_type: JSON_1_1,
            body: body.to_string().into_bytes(),
            date: None,
            hang: false,
        }
    }

    /// A Glue JSON 1.1 service error: `__type` carries the error code.
    pub(crate) fn error(status: u16, code: &str, message: &str) -> Self {
        Self {
            status,
            content_type: JSON_1_1,
            body: serde_json::json!({ "__type": code, "message": message })
                .to_string()
                .into_bytes(),
            date: None,
            hang: false,
        }
    }

    pub(crate) fn status(status: u16) -> Self {
        Self {
            status,
            content_type: JSON_1_1,
            body: Vec::new(),
            date: None,
            hang: false,
        }
    }

    pub(crate) fn object(bytes: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type: "application/octet-stream",
            body: bytes,
            date: None,
            hang: false,
        }
    }

    /// Holds the connection open without answering, so a client deadline can expire.
    pub(crate) fn hang() -> Self {
        Self {
            hang: true,
            ..Self::status(200)
        }
    }

    pub(crate) fn with_date(mut self, http_date: String) -> Self {
        self.date = Some(http_date);
        self
    }
}

pub(crate) struct MockGlue {
    pub address: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl MockGlue {
    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub(crate) fn requests_for(&self, operation: &str) -> Vec<RecordedRequest> {
        self.requests()
            .into_iter()
            .filter(|request| request.operation() == Some(operation))
            .collect()
    }

    pub(crate) fn storage_requests(&self) -> Vec<RecordedRequest> {
        self.requests()
            .into_iter()
            .filter(|request| request.operation().is_none())
            .collect()
    }
}

/// `attempt` counts the earlier requests for the same Glue operation, so a responder can
/// fail a call a fixed number of times. Every response closes its connection.
pub(crate) async fn spawn<F>(responder: F) -> MockGlue
where
    F: Fn(&RecordedRequest, usize) -> MockResponse + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let address = format!("http://{}", listener.local_addr().expect("local_addr"));
    let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let responder = Arc::new(responder);

    let recorded = requests.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let recorded = recorded.clone();
            let responder = responder.clone();
            tokio::spawn(async move { serve(stream, &recorded, &*responder).await });
        }
    });

    MockGlue { address, requests }
}

async fn serve<F>(mut stream: TcpStream, recorded: &Mutex<Vec<RecordedRequest>>, responder: &F)
where
    F: Fn(&RecordedRequest, usize) -> MockResponse,
{
    let Some(request) = read_request(&mut stream).await else {
        return;
    };
    let response = {
        let mut log = recorded.lock().unwrap();
        let attempt = log
            .iter()
            .filter(|earlier| earlier.operation() == request.operation())
            .count();
        let response = responder(&request, attempt);
        log.push(request);
        response
    };
    if response.hang {
        std::future::pending::<()>().await;
    }
    let mut head = format!(
        "HTTP/1.1 {} MOCK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        response.content_type,
        response.body.len()
    );
    if let Some(date) = &response.date {
        head.push_str(&format!("Date: {date}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(&response.body).await;
    let _ = stream.shutdown().await;
}

async fn read_request(stream: &mut TcpStream) -> Option<RecordedRequest> {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(position) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            break position;
        }
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        raw.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&raw[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let path = request_line.next()?.to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect();
    let content_length: usize = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or(0);
    let mut body = raw[head_end + 4..].to_vec();
    while body.len() < content_length {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    Some(RecordedRequest {
        method,
        path,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

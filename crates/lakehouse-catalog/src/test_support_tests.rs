use crate::catalog_http::RetryPolicy;
use crate::{ConnectionCreds, StorageBackend, StorageProps};
use std::error::Error;
use std::fmt;
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
        ..Default::default()
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
/// A closed loopback port; its cause sits below reqwest's own `error sending request`.
pub(crate) const CLOSED_PORT_URI: &str = "http://127.0.0.1:1";
pub(crate) const CLOSED_PORT_CAUSE: &str = "Connection refused";
pub(crate) const ORC_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";

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
        ..Default::default()
    }
}

/// Answers every request with HTTP 200 and `body`, recording each request in order.
pub(crate) async fn spawn_recording_catalog(
    body: &'static str,
) -> (String, Arc<Mutex<Vec<String>>>) {
    spawn_server("application/json", move |_| (200, body.into())).await
}

/// Answers every request with HTTP `status` and the XML `body`, recording each request in order.
pub(crate) async fn spawn_recording_sts(
    status: u16,
    body: impl Into<String>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let body = body.into();
    spawn_server("text/xml", move |_| (status, body.clone())).await
}

/// Answers each request, head and body, with the status and body `respond` returns for it,
/// recording each request in order.
pub(crate) async fn spawn_server(
    content_type: &'static str,
    respond: impl Fn(&str) -> (u16, String) + Send + 'static,
) -> (String, Arc<Mutex<Vec<String>>>) {
    spawn_server_with_headers(content_type, move |request| {
        let (status, body) = respond(request);
        (status, Vec::new(), body)
    })
    .await
}

/// [`spawn_server`], where `respond` also returns extra response headers, such as `Retry-After`.
pub(crate) async fn spawn_server_with_headers(
    content_type: &'static str,
    respond: impl Fn(&str) -> (u16, Vec<(&'static str, String)>, String) + Send + 'static,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let base_uri = format!("http://{}", listener.local_addr().expect("local_addr"));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let request = read_request(&mut stream).await;
            let (status, headers, body) = respond(&request);
            recorded.lock().unwrap().push(request);
            let reason = reqwest::StatusCode::from_u16(status)
                .ok()
                .and_then(|code| code.canonical_reason())
                .unwrap_or("Stub");
            let extra: String = headers
                .iter()
                .map(|(name, value)| format!("{name}: {value}\r\n"))
                .collect();
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\n{extra}\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });

    (base_uri, requests)
}

/// Answers each request with HTTP 200 headers announcing 100 body bytes, sends 10 of them, and then
/// holds the connection open without sending more.
pub(crate) async fn spawn_header_stalling_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let base_uri = format!("http://{}", listener.local_addr().expect("local_addr"));

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                read_request(&mut stream).await;
                let head = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                            Content-Length: 100\r\n\r\n{\"partial\"";
                let _ = stream.write_all(head.as_bytes()).await;
                std::future::pending::<()>().await;
                drop(stream);
            });
        }
    });

    base_uri
}

/// The catalog policy shrunk so a test of a timeout or the backoff runs in about two seconds.
pub(crate) const SHORT_POLICY: RetryPolicy = RetryPolicy {
    attempts: 5,
    connect_timeout: Duration::from_millis(100),
    read_timeout: Duration::from_millis(300),
    retry_window: Duration::from_secs(2),
    backoff_base: Duration::from_millis(10),
};

/// reqwest's text for a request or body read that reached its read timeout.
pub(crate) const TIMED_OUT_CAUSE: &str = "operation timed out";

pub(crate) async fn read_request(stream: &mut TcpStream) -> String {
    let mut request = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&request[..end]);
            let length =
                header_value(&head, "content-length").map_or(0, |n| n.parse().unwrap_or(0));
            if request.len() >= end + 4 + length {
                break;
            }
        }
        let read = stream.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8_lossy(&request).into_owned()
}

pub(crate) fn authorization_header(head: &str) -> Option<&str> {
    header_value(head, "authorization")
}

/// The value of header `name` in a recorded request, matched
/// case-insensitively, or `None` when the request did not carry it.
pub(crate) fn header_value<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request
        .lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .find_map(|line| {
            let (header, value) = line.split_once(':')?;
            header.eq_ignore_ascii_case(name).then(|| value.trim())
        })
}

/// The body of a recorded request.
pub(crate) fn request_body(request: &str) -> &str {
    request.split_once("\r\n\r\n").map_or("", |(_, body)| body)
}

/// One layer of a synthetic error chain: its text and the cause below it.
#[derive(Debug)]
pub(crate) struct Layer(
    pub(crate) &'static str,
    pub(crate) Option<Box<dyn Error + Send + Sync>>,
);

impl fmt::Display for Layer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Error for Layer {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.1.as_deref().map(|e| e as &(dyn Error + 'static))
    }
}

pub(crate) fn layer(text: &'static str, cause: impl Error + Send + Sync + 'static) -> Layer {
    Layer(text, Some(Box::new(cause)))
}

/// The session credentials [`ASSUME_ROLE_RESPONSE`] carries.
pub(crate) const SESSION_AK: &str = "ASIASESSIONKEYSENTINEL";
pub(crate) const SESSION_SK: &str = "session-secret-access-key-sentinel";
pub(crate) const SESSION_TOKEN: &str = "session-token-sentinel";

/// A well-formed AWS STS `AssumeRoleResponse` carrying [`SESSION_AK`],
/// [`SESSION_SK`], and [`SESSION_TOKEN`], in the sample response's shape and namespace.
pub(crate) const ASSUME_ROLE_RESPONSE: &str = r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/lakehouse-reader/lakehouse-engine</Arn>
      <AssumedRoleId>AROA3XFRBF535PLBIFPI4:lakehouse-engine</AssumedRoleId>
    </AssumedRoleUser>
    <Credentials>
      <AccessKeyId>ASIASESSIONKEYSENTINEL</AccessKeyId>
      <SecretAccessKey>session-secret-access-key-sentinel</SecretAccessKey>
      <SessionToken>session-token-sentinel</SessionToken>
      <Expiration>2026-09-28T13:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleResult>
  <ResponseMetadata>
    <RequestId>c6104cbe-af31-11e0-8154-cbc7ccf896c7</RequestId>
  </ResponseMetadata>
</AssumeRoleResponse>"#;

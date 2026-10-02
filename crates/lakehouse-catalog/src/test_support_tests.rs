use crate::{ConnectionCreds, StorageBackend, StorageProps};
use std::sync::{Arc, Mutex};
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

pub(crate) fn static_storage() -> StorageProps {
    StorageProps {
        endpoint: "https://s3.amazonaws.com".into(),
        region: "us-east-1".into(),
        access_key: "STATIC_AK_SENTINEL".into(),
        secret_key: "STATIC_SK_SENTINEL".into(),
        path_style: false,
        ..Default::default()
    }
}

pub(crate) fn static_backend() -> StorageBackend {
    StorageBackend::S3(static_storage())
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
        ..Default::default()
    }
}

/// Answers every request with HTTP 200 and `body`, recording each request head in order.
pub(crate) async fn spawn_recording_catalog(
    body: &'static str,
) -> (String, Arc<Mutex<Vec<String>>>) {
    spawn_recording_server(200, "application/json", body.into()).await
}

/// Answers every request with HTTP `status` and the XML `body`, recording each request head as [`spawn_recording_catalog`] does.
pub(crate) async fn spawn_recording_sts(
    status: u16,
    body: impl Into<String>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    spawn_recording_server(status, "text/xml", body.into()).await
}

async fn spawn_recording_server(
    status: u16,
    content_type: &'static str,
    body: String,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let base_uri = format!("http://{}", listener.local_addr().expect("local_addr"));
    let heads = Arc::new(Mutex::new(Vec::new()));
    let recorded = heads.clone();
    let reason = reqwest::StatusCode::from_u16(status)
        .ok()
        .and_then(|code| code.canonical_reason())
        .unwrap_or("Stub");

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let head = read_request_head(&mut stream).await;
            recorded.lock().unwrap().push(head);
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });

    (base_uri, heads)
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
    header_value(head, "authorization")
}

/// The value of header `name` in a recorded request head, matched
/// case-insensitively, or `None` when the request did not carry it.
pub(crate) fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().skip(1).find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(name).then(|| value.trim())
    })
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

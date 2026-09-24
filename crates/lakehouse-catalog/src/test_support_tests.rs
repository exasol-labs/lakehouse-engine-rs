//! Test-only fixtures reached by two or more of this crate's test modules —
//! the single home the `vs-adapter/pushdown-module-structure` rule requires
//! for a test helper reachable from multiple submodules. Each fixture's doc
//! comment names its consumers; a fixture reached by only one module lives
//! in that module's own `mod tests` instead.

use crate::{ConnectionCreds, StorageBackend, StorageProps};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A baseline `ConnectionCreds` with no catalog auth (all auth fields `None`).
/// Individual tests set only the auth fields under test.
///
/// Consumers: `auth`, `namespace`, `session`, `storage`.
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

/// Static storage with the sentinel keys `STATIC_AK_SENTINEL` / `STATIC_SK_SENTINEL`
/// (matching the credentials-cluster test sentinels below).
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

/// [`static_storage`] wrapped in the `S3` backend variant, for call sites taking
/// `&StorageBackend` rather than `&StorageProps`.
pub(crate) fn static_backend() -> StorageBackend {
    StorageBackend::S3(static_storage())
}

/// Unwrap a `StorageBackend`'s `S3` payload — the test-only inverse of
/// `StorageBackend::S3(..)`, so a test asserting directly on `StorageProps`
/// fields against a `resolve_vended_storage` return value can stay unchanged
/// below the unwrap.
///
/// Consumers: `storage`, `vended`.
pub(crate) fn s3_payload(backend: StorageBackend) -> StorageProps {
    match backend {
        StorageBackend::S3(props) => props,
        StorageBackend::Adls { .. } => panic!("s3_payload is S3-only"),
    }
}

// --- Shared sentinels ---
/// Consumers: `creds_no_auth` (below) and `vended`.
pub(crate) const STATIC_AK: &str = "STATIC_AK_SENTINEL";
/// Consumers: `creds_no_auth` (below) and `vended`.
pub(crate) const STATIC_SK: &str = "STATIC_SK_SENTINEL";
/// Consumers: `auth`, `iceberg_io`.
pub(crate) const BEARER_TOK: &str = "BEARER_TOKEN_SENTINEL_VALUE";
/// Consumers: `auth`, `iceberg_io`, `vended`.
pub(crate) const CLIENT_SECRET: &str = "CLIENT_SECRET_SENTINEL_VALUE";
/// Consumers: `auth`, `iceberg_io`.
pub(crate) const OAUTH_ACCESS_TOKEN: &str = "OAUTH_OBTAINED_ACCESS_TOKEN";

/// A `ConnectionCreds` with no auth, no vending — the no-op baseline.
///
/// Consumers: `auth`, `creds`, `iceberg_io`, `session`.
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

/// A loopback catalog stub answering every request with HTTP 200 and `body`,
/// recording each request's head (request line plus headers) in arrival order.
/// Returns the stub's base URI and the recorded heads, so a test can assert on
/// exactly what a signing path sent — or that it sent nothing.
///
/// Consumers: `iceberg_io`, `namespace`.
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

/// The `Authorization` header value of a request head recorded by
/// [`spawn_recording_catalog`], with the header name matched case-insensitively.
///
/// Consumers: `iceberg_io`, `namespace`.
pub(crate) fn authorization_header(head: &str) -> Option<&str> {
    head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("authorization")
            .then(|| value.trim())
    })
}

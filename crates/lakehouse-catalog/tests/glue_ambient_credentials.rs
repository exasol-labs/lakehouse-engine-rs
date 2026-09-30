//! Its own binary with one test, because the test sets process-wide AWS variables.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use lakehouse_catalog::{
    CatalogClient, ConnectionCreds, GlueCatalogSession, StorageBackend, StorageProps,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CONNECTION_ACCESS_KEY: &str = "AKIDCONNECTIONKEY001";
const CONNECTION_SECRET_KEY: &str = "connection-secret-key";
const CONNECTION_REGION: &str = "eu-central-1";
const AMBIENT_ACCESS_KEY: &str = "AKIDAMBIENTENVKEY001";
const PROFILE_ACCESS_KEY: &str = "AKIDAMBIENTPROFILE01";
const AMBIENT_REGION: &str = "ap-south-1";

/// Scenario: The client signs only with the CONNECTION's credentials
#[test]
fn requests_are_signed_with_the_connection_key_not_the_environment() {
    let profile = write_ambient_profile();
    // SAFETY: this binary holds one test, and no other thread runs before the runtime starts.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", AMBIENT_ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "ambient-secret-key");
        std::env::set_var("AWS_REGION", AMBIENT_REGION);
        std::env::set_var("AWS_CONFIG_FILE", &profile);
        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &profile);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");

    let heads = runtime.block_on(list_one_database());
    let _ = std::fs::remove_file(&profile);

    let [head] = heads.as_slice() else {
        panic!("expected one GetTables call, got {}", heads.len());
    };
    let signed_date = header(head, "x-amz-date").expect("a signed request");
    let authorization = header(head, "authorization").expect("an Authorization header");
    let expected = format!(
        "Credential={CONNECTION_ACCESS_KEY}/{}/{CONNECTION_REGION}/glue/aws4_request",
        &signed_date[..8]
    );
    assert!(
        authorization.contains(&expected),
        "expected {expected} in {authorization}"
    );
    for ambient in [AMBIENT_ACCESS_KEY, PROFILE_ACCESS_KEY, AMBIENT_REGION] {
        assert!(!authorization.contains(ambient), "{authorization}");
    }
}

fn write_ambient_profile() -> PathBuf {
    let path =
        std::env::temp_dir().join(format!("glue-ambient-profile-{}.ini", std::process::id()));
    std::fs::write(
        &path,
        format!(
            "[default]\naws_access_key_id = {PROFILE_ACCESS_KEY}\n\
             aws_secret_access_key = profile-secret-key\nregion = {AMBIENT_REGION}\n"
        ),
    )
    .expect("write the ambient profile");
    path
}

async fn list_one_database() -> Vec<String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let address = format!("http://{}", listener.local_addr().expect("local_addr"));
    let heads = Arc::new(Mutex::new(Vec::new()));
    let recorded = heads.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let head = read_head(&mut stream).await;
            recorded.lock().unwrap().push(head);
            let body = r#"{"TableList":[]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.1\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });
    let creds = ConnectionCreds {
        region: CONNECTION_REGION.to_string(),
        access_key: CONNECTION_ACCESS_KEY.to_string(),
        secret_key: CONNECTION_SECRET_KEY.to_string(),
        use_sigv4: true,
        ..ConnectionCreds::default()
    };
    let session =
        GlueCatalogSession::new(&address, StorageBackend::S3(StorageProps::default()), creds)
            .expect("a session");

    session
        .list_tables(&["sales".to_string()])
        .await
        .expect("the listing succeeds");

    heads.lock().unwrap().clone()
}

/// Reads the head and the `Content-Length` body, so the client never sees a reset.
async fn read_head(stream: &mut TcpStream) -> String {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(end) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            break end;
        }
        let read = stream.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            return String::from_utf8_lossy(&raw).into_owned();
        }
        raw.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&raw[..head_end]).into_owned();
    let body_length: usize = header(&head, "content-length")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let mut received = raw.len() - head_end - 4;
    while received < body_length {
        let read = stream.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            break;
        }
        received += read;
    }
    head
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim().eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

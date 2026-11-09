//! The test helpers that dependent crates enable through the `test-support` feature: a fake HTTP
//! server and a movable epoch clock.

use crate::EpochClock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Answers each request, head and body, with the status and body `respond` returns for it,
/// recording each request in order. Returns the server's base URI and the recorded requests.
pub async fn spawn_server(
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
pub async fn spawn_server_with_headers(
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

/// The body of a recorded request.
pub fn request_body(request: &str) -> &str {
    request.split_once("\r\n\r\n").map_or("", |(_, body)| body)
}

/// A clock that reads `time`, so a test moves it by storing a new epoch millisecond.
pub fn clock_reading(time: &Arc<AtomicU64>) -> EpochClock {
    let time = time.clone();
    Arc::new(move || time.load(Ordering::SeqCst))
}

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

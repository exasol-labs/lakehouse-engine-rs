use super::*;
use crate::test_support::*;
use std::future::Future;
use std::io::{Error as IoError, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::UNIX_EPOCH;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// rustls 0.23's text for a TLS peer that closes without `close_notify` (`src/conn.rs`).
const UNEXPECTED_EOF_MESSAGE: &str = "peer closed connection without sending TLS close_notify: \
     https://docs.rs/rustls/latest/rustls/manual/_03_howto/index.html#unexpected-eof";

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

/// `error sending request` over hyper's connection error over `cause`, as reqwest nests them.
fn hyper_connection_error(cause: IoError) -> impl Error + 'static {
    layer(
        "error sending request",
        layer(
            "client error (SendRequest)",
            layer("connection error", cause),
        ),
    )
}

#[test]
fn only_408_429_502_503_504_are_transient_statuses() {
    let transient: Vec<u16> = (100..600)
        .filter(|&code| is_transient_status(StatusCode::from_u16(code).unwrap()))
        .collect();

    assert_eq!(transient, [408, 429, 502, 503, 504]);
}

#[test]
fn retry_after_reads_delay_seconds_and_http_dates() {
    let imf_fixdate = "Sun, 06 Nov 1994 08:49:37 GMT";
    let stated = UNIX_EPOCH + Duration::from_secs(784_111_777);
    let before = stated - Duration::from_secs(10);

    assert_eq!(retry_after_wait("1", before), Some(Duration::from_secs(1)));
    assert_eq!(retry_after_wait("0", before), Some(Duration::ZERO));
    assert_eq!(
        retry_after_wait(" 120 ", before),
        Some(Duration::from_secs(120))
    );
    assert_eq!(
        retry_after_wait("99999999999999999999999", before),
        Some(Duration::MAX)
    );
    assert_eq!(
        retry_after_wait(imf_fixdate, before),
        Some(Duration::from_secs(10))
    );
    assert_eq!(retry_after_wait(imf_fixdate, stated), Some(Duration::ZERO));
    assert_eq!(
        retry_after_wait(imf_fixdate, stated + Duration::from_secs(10)),
        Some(Duration::ZERO)
    );
    for ignored in [
        "Sunday, 06-Nov-94 08:49:37 GMT",
        "Sun Nov  6 08:49:37 1994",
        "",
        "soon",
        "+1",
        "-1",
        "1.5",
        "1s",
    ] {
        assert_eq!(retry_after_wait(ignored, before), None, "{ignored:?}");
    }
}

#[test]
fn the_backoff_wait_doubles_within_its_jitter_range() {
    let base = ms(250);
    let range = |retry| {
        (
            backoff_wait(base, retry, 0),
            backoff_wait(base, retry, u32::MAX),
        )
    };

    assert_eq!(range(1), (ms(125), ms(250)));
    assert_eq!(range(2), (ms(250), ms(500)));
    assert_eq!(range(3), (ms(500), ms(1000)));
    assert_eq!(range(4), (ms(1000), ms(2000)));
    let midway = backoff_wait(base, 1, u32::MAX / 2);
    assert!(ms(125) < midway && midway < ms(250), "{midway:?}");
}

/// Scenario: Retry-After sets the wait, and a wait that leaves no time for an attempt ends the retries
#[test]
fn a_retry_waits_only_when_an_attempt_still_fits_after_its_wait() {
    let backoff = ms(200);
    let left = ms(1000);
    let room = ms(100);
    let wait = |requested, backoff, left| chosen_wait(requested, backoff, left, room);

    assert_eq!(wait(None, backoff, left), Some(backoff));
    assert_eq!(wait(Some(ms(500)), backoff, left), Some(ms(500)));
    assert_eq!(
        wait(Some(Duration::ZERO), backoff, left),
        Some(Duration::ZERO)
    );
    assert_eq!(wait(Some(left - room), backoff, left), Some(left - room));
    let just_past = left - room + Duration::from_nanos(1);
    assert_eq!(wait(Some(just_past), backoff, left), None);
    assert_eq!(wait(Some(Duration::MAX), backoff, left), None);
    assert_eq!(wait(None, backoff, backoff + room / 2), None);
    assert_eq!(wait(None, left, left), None);
}

#[test]
fn only_refused_reset_aborted_broken_and_eof_connections_are_transient_causes() {
    let transient = |kind| is_transient_cause_chain(&hyper_connection_error(IoError::from(kind)));

    for kind in [
        ErrorKind::ConnectionRefused,
        ErrorKind::ConnectionReset,
        ErrorKind::ConnectionAborted,
        ErrorKind::BrokenPipe,
        ErrorKind::UnexpectedEof,
    ] {
        assert!(transient(kind), "{kind:?}");
    }
    for kind in [
        ErrorKind::TimedOut,
        ErrorKind::NotConnected,
        ErrorKind::InvalidData,
        ErrorKind::Other,
    ] {
        assert!(!transient(kind), "{kind:?}");
    }
}

/// Scenario: A transient failure is retried with exponential backoff and jitter
#[test]
fn a_tls_close_without_close_notify_is_transient() {
    let close = IoError::new(ErrorKind::UnexpectedEof, UNEXPECTED_EOF_MESSAGE);
    let tls_failure = rustls::Error::AlertReceived(rustls::AlertDescription::DecodeError);
    let close_with_tls_failure = IoError::new(
        ErrorKind::UnexpectedEof,
        layer(UNEXPECTED_EOF_MESSAGE, tls_failure),
    );

    assert!(is_transient_cause_chain(&hyper_connection_error(close)));
    assert!(!is_transient_cause_chain(&hyper_connection_error(
        close_with_tls_failure
    )));
}

#[test]
fn the_catalog_policy_is_five_attempts_ten_second_connect_thirty_second_budget() {
    let policy = CatalogHttp::new().policy;

    assert_eq!(
        policy,
        RetryPolicy {
            attempts: 5,
            connect_timeout: Duration::from_secs(10),
            bound: Duration::from_secs(30),
            backoff_base: ms(250),
        }
    );
}

#[test]
fn a_spent_bound_states_the_policy_bound_and_no_cause() {
    let render = |bound| CatalogHttpError(Failure::BoundSpent(bound)).render(|_| unreachable!());

    assert_eq!(
        render(Duration::from_secs(30)),
        "the catalog sent no complete answer within 30 seconds, retries included"
    );
    assert_eq!(
        render(ms(1500)),
        "the catalog sent no complete answer within 1.5 seconds, retries included"
    );
}

/// A loopback listener that hands each accepted connection and its 0-based index to `serve`.
async fn spawn_listener<F, Fut>(serve: F) -> (SocketAddr, Arc<AtomicUsize>)
where
    F: Fn(TcpStream, usize) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let address = listener.local_addr().expect("local_addr");
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let index = counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(serve(stream, index));
        }
    });
    (address, accepted)
}

async fn hold_open(stream: TcpStream) {
    std::future::pending::<()>().await;
    drop(stream);
}

/// Reads the request, then closes with a TCP reset instead of an orderly close.
async fn reset_after_request(mut stream: TcpStream) {
    read_request(&mut stream).await;
    stream.set_zero_linger().expect("set SO_LINGER to zero");
}

async fn answer_ok(mut stream: TcpStream) {
    read_request(&mut stream).await;
    let answer = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
    let _ = stream.write_all(answer.as_bytes()).await;
}

/// Answers whatever arrives, a TLS ClientHello included, in plain HTTP.
async fn answer_in_plain_http(mut stream: TcpStream) {
    let mut hello = [0u8; 1024];
    let _ = stream.read(&mut hello).await;
    let answer = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let _ = stream.write_all(answer.as_bytes()).await;
}

/// Reads the first bytes of the TLS handshake, then resets the connection.
async fn reset_during_handshake(mut stream: TcpStream) {
    let mut hello = [0u8; 1024];
    let _ = stream.read(&mut hello).await;
    stream.set_zero_linger().expect("set SO_LINGER to zero");
}

/// Reads the request and closes the connection in order, before any status line.
async fn close_after_request(mut stream: TcpStream) {
    read_request(&mut stream).await;
}

/// A listener whose accept queue is full, so the kernel drops every further connection's SYN.
/// The returned values hold the queue full and must outlive the test's requests. The SYN drop
/// holds on Linux but not on macOS, so this helper and its callers are Linux-only.
#[cfg(target_os = "linux")]
async fn spawn_full_backlog_listener() -> (SocketAddr, TcpListener, Vec<TcpStream>) {
    let socket = tokio::net::TcpSocket::new_v4().expect("a TCP socket");
    socket
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("bind a loopback port");
    let listener = socket.listen(0).expect("listen with no backlog");
    let address = listener.local_addr().expect("local_addr");
    let mut queued = Vec::new();
    while queued.len() < 64 {
        match tokio::time::timeout(ms(100), TcpStream::connect(address)).await {
            Ok(Ok(stream)) => queued.push(stream),
            _ => return (address, listener, queued),
        }
    }
    panic!("the accept queue of a listen(0) socket never filled");
}

fn get(http: &CatalogHttp, url: &str) -> Request {
    http.get(url)
        .bearer_auth(BEARER_TOK)
        .build()
        .expect("a valid request")
}

fn identity(text: &str) -> String {
    text.to_owned()
}

/// Scenario: A catalog request that gets no complete answer fails within 30 seconds
#[tokio::test]
async fn a_catalog_that_never_answers_fails_within_the_budget() {
    let (address, accepted) = spawn_listener(|stream, _| hold_open(stream)).await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);
    let started = Instant::now();

    let error = http
        .send(get(&http, &format!("http://{address}/")))
        .await
        .unwrap_err();

    let elapsed = started.elapsed();
    assert!(
        SHORT_POLICY.bound <= elapsed && elapsed < SHORT_POLICY.bound + ms(500),
        "{elapsed:?}"
    );
    assert_eq!(error.render(identity), SHORT_POLICY_SPENT_BOUND);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

/// Scenario: A catalog request that gets no complete answer fails within 30 seconds
#[tokio::test]
async fn a_catalog_that_stalls_after_the_headers_fails_within_the_budget() {
    let base_uri = spawn_header_stalling_server().await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);
    let started = Instant::now();

    let error = http.send(get(&http, &base_uri)).await.unwrap_err();

    let elapsed = started.elapsed();
    assert!(
        SHORT_POLICY.bound <= elapsed && elapsed < SHORT_POLICY.bound + ms(500),
        "{elapsed:?}"
    );
    assert_eq!(error.render(identity), SHORT_POLICY_SPENT_BOUND);
    assert_eq!(error.status(), None);
}

/// Scenario: A catalog request that gets no complete answer fails within 30 seconds
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_connection_that_never_completes_ends_at_the_connect_timeout() {
    let (address, _listener, _queued) = spawn_full_backlog_listener().await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);
    let started = Instant::now();

    let error = http
        .send(get(&http, &format!("http://{address}/")))
        .await
        .unwrap_err();

    let elapsed = started.elapsed();
    let five_connect_timeouts = SHORT_POLICY.connect_timeout * SHORT_POLICY.attempts;
    assert!(
        five_connect_timeouts <= elapsed && elapsed < SHORT_POLICY.bound,
        "{elapsed:?}"
    );
    assert!(error.is_transient(), "{}", error.render(identity));
}

#[tokio::test]
async fn a_capped_read_keeps_the_first_bytes_and_reports_the_cut() {
    let (base_uri, _) = spawn_server("text/plain", |request| {
        let body = if request.starts_with("GET /long") {
            "x".repeat(100)
        } else {
            "y".repeat(10)
        };
        (200, body)
    })
    .await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);

    let long = http
        .send_capped(get(&http, &format!("{base_uri}/long")), 10)
        .await
        .unwrap();
    let exact = http
        .send_capped(get(&http, &format!("{base_uri}/exact")), 10)
        .await
        .unwrap();

    assert_eq!((long.body, long.cut), (b"x".repeat(10), true));
    assert_eq!((exact.body, exact.cut), (b"y".repeat(10), false));
}

#[tokio::test]
async fn a_body_that_breaks_off_fails_once_and_keeps_the_answer_status() {
    let (address, accepted) = spawn_listener(|mut stream, _| async move {
        read_request(&mut stream).await;
        let head = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 100\r\n\r\n0123456789";
        let _ = stream.write_all(head.as_bytes()).await;
    })
    .await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);

    let error = http
        .send(get(&http, &format!("http://{address}/")))
        .await
        .unwrap_err();

    assert_eq!(error.status(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_ne!(error.render(identity), SHORT_POLICY_SPENT_BOUND);
    assert!(
        error.render(identity).contains("HTTP 500"),
        "{}",
        error.render(identity)
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

/// Scenario: A transient failure is retried with exponential backoff and jitter
#[tokio::test]
async fn transient_statuses_are_retried_until_an_answer_succeeds() {
    for status in [408, 429, 502, 503, 504] {
        let answered = AtomicUsize::new(0);
        let (base_uri, requests) = spawn_server("application/json", move |_| {
            match answered.fetch_add(1, Ordering::SeqCst) {
                0 => (status, "{\"error\":\"busy\"}".into()),
                _ => (200, "{\"ok\":true}".into()),
            }
        })
        .await;
        let http = CatalogHttp::with_policy(SHORT_POLICY);
        let request = http
            .post(format!("{base_uri}/v1/oauth/tokens"))
            .header("x-catalog-probe", "first")
            .bearer_auth(BEARER_TOK)
            .body("grant_type=client_credentials")
            .build()
            .unwrap();

        let answer = http.send(request).await.unwrap();

        assert_eq!(answer.status, StatusCode::OK, "after HTTP {status}");
        assert_eq!(answer.body, b"{\"ok\":true}");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2, "after HTTP {status}");
        assert_eq!(requests[0], requests[1], "after HTTP {status}");
    }
}

/// Scenario: Retry-After sets the wait, and a wait that leaves no time for an attempt ends the retries
#[tokio::test]
async fn a_retry_after_wait_is_honored() {
    let answered = AtomicUsize::new(0);
    let (base_uri, requests) =
        spawn_server_with_headers("application/json", move |_| {
            match answered.fetch_add(1, Ordering::SeqCst) {
                0 => (503, vec![("Retry-After", "1".into())], String::new()),
                _ => (200, Vec::new(), "{}".into()),
            }
        })
        .await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);
    let started = Instant::now();

    let answer = http.send(get(&http, &base_uri)).await.unwrap();

    assert_eq!(answer.status, StatusCode::OK);
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(requests.lock().unwrap().len(), 2);
}

/// Scenario: Retry-After sets the wait, and a wait that leaves no time for an attempt ends the retries
#[tokio::test]
async fn a_wait_past_the_budget_ends_the_retries_at_once() {
    let (base_uri, requests) = spawn_server_with_headers("application/json", |_| {
        (
            503,
            vec![("Retry-After", "5".into())],
            "{\"error\":\"busy\"}".into(),
        )
    })
    .await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);
    let started = Instant::now();

    let answer = http.send(get(&http, &base_uri)).await.unwrap();

    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(answer.body, b"{\"error\":\"busy\"}");
    assert!(started.elapsed() < ms(500), "{:?}", started.elapsed());
    assert_eq!(requests.lock().unwrap().len(), 1);
}

/// Scenario: Retry-After sets the wait, and a wait that leaves no time for an attempt ends the retries
#[tokio::test]
async fn a_wait_that_leaves_less_than_a_connect_timeout_keeps_the_answer_at_hand() {
    let (base_uri, requests) = spawn_server_with_headers("application/json", |_| {
        (
            503,
            vec![("Retry-After", "1".into())],
            "{\"error\":\"busy\"}".into(),
        )
    })
    .await;
    let http = CatalogHttp::with_policy(RetryPolicy {
        connect_timeout: ms(600),
        bound: ms(1500),
        ..SHORT_POLICY
    });
    let started = Instant::now();

    let answer = http.send(get(&http, &base_uri)).await.unwrap();

    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(started.elapsed() < ms(500), "{:?}", started.elapsed());
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_refused_json_answer_quotes_its_body_redacted_and_cut() {
    let echoed = format!("{BEARER_TOK} {}", "x".repeat(MAX_QUOTED_BODY_BYTES * 2));
    let (base_uri, _) = spawn_server("text/html", move |_| (500, echoed.clone())).await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);

    let error = http
        .send_json::<serde_json::Value>(get(&http, &base_uri), "catalog request", |msg| {
            msg.replace(BEARER_TOK, "***")
        })
        .await
        .unwrap_err();

    let message = error.to_string();
    let suffix = format!("... (truncated to {MAX_QUOTED_BODY_BYTES} bytes)");
    assert!(
        message.starts_with("catalog request failed with HTTP 500: *** x"),
        "{message}"
    );
    assert!(message.ends_with(&suffix), "{message}");
    assert!(!message.contains(BEARER_TOK), "{message}");
    assert!(
        message.len() < MAX_QUOTED_BODY_BYTES + 100,
        "{}",
        message.len()
    );
}

/// Scenario: A failure that is not transient fails the request at the first attempt
#[tokio::test]
async fn a_non_transient_answer_is_sent_once() {
    for status in [400, 401, 403, 404, 409, 419, 500] {
        let (base_uri, requests) = spawn_server("application/json", move |_| {
            (status, format!("{{\"code\":{status}}}"))
        })
        .await;
        let http = CatalogHttp::with_policy(SHORT_POLICY);

        let answer = http.send(get(&http, &base_uri)).await.unwrap();

        assert_eq!(answer.status.as_u16(), status);
        assert_eq!(answer.body, format!("{{\"code\":{status}}}").into_bytes());
        assert_eq!(requests.lock().unwrap().len(), 1, "HTTP {status}");
    }
}

/// Scenario: A transient failure is retried with exponential backoff and jitter
/// Scenario: Retries that run out fail with the last failure
#[tokio::test]
async fn a_refused_connection_is_retried_until_attempts_run_out() {
    let http = CatalogHttp::with_policy(SHORT_POLICY);
    let single = CatalogHttp::with_policy(RetryPolicy {
        attempts: 1,
        ..SHORT_POLICY
    });
    let started = Instant::now();

    let error = http.send(get(&http, CLOSED_PORT_URI)).await.unwrap_err();

    let elapsed = started.elapsed();
    let four_shortest_waits = ms(5 + 10 + 20 + 40);
    assert!(elapsed >= four_shortest_waits, "{elapsed:?}");
    let single_error = single
        .send(get(&single, CLOSED_PORT_URI))
        .await
        .unwrap_err();
    assert_eq!(error.render(identity), single_error.render(identity));
    assert!(
        error.render(identity).contains(CLOSED_PORT_CAUSE),
        "{}",
        error.render(identity)
    );
}

/// Scenario: A transient failure is retried with exponential backoff and jitter
#[tokio::test]
async fn a_connection_reset_after_the_request_is_retried() {
    let (address, accepted) = spawn_listener(|stream, index| async move {
        match index {
            0 => reset_after_request(stream).await,
            _ => answer_ok(stream).await,
        }
    })
    .await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);

    let answer = http
        .send(get(&http, &format!("http://{address}/")))
        .await
        .unwrap();

    assert_eq!(
        (answer.status, answer.body),
        (StatusCode::OK, b"ok".to_vec())
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
}

/// Scenario: A transient failure is retried with exponential backoff and jitter
#[tokio::test]
async fn a_connection_closed_cleanly_after_the_request_is_retried() {
    let (address, accepted) = spawn_listener(|stream, index| async move {
        match index {
            0 => close_after_request(stream).await,
            _ => answer_ok(stream).await,
        }
    })
    .await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);

    let answer = http
        .send(get(&http, &format!("http://{address}/")))
        .await
        .unwrap();

    assert_eq!(
        (answer.status, answer.body),
        (StatusCode::OK, b"ok".to_vec())
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
}

/// Scenario: A transient failure is retried with exponential backoff and jitter
#[tokio::test]
async fn an_http2_connection_closed_cleanly_is_transient() {
    let (closing, _) = spawn_listener(|stream, _| close_after_request(stream)).await;
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();

    let error = client
        .get(format!("http://{closing}/"))
        .send()
        .await
        .unwrap_err();

    assert!(
        is_transient_transport_failure(&error),
        "{}",
        error_chain(&error)
    );
}

/// Scenario: A failure that is not transient fails the request at the first attempt
#[tokio::test]
async fn a_tls_client_against_a_plain_http_listener_connects_once() {
    let (address, accepted) = spawn_listener(|stream, _| answer_in_plain_http(stream)).await;
    let http = CatalogHttp::with_policy(SHORT_POLICY);

    let error = http
        .send(get(&http, &format!("https://{address}/")))
        .await
        .unwrap_err();

    let text = error.render(identity);
    assert!(
        text.contains("the server did not answer with TLS"),
        "{text}"
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

/// Scenario: A failure that is not transient fails the request at the first attempt
#[tokio::test]
async fn an_unresolvable_host_fails_at_the_first_attempt() {
    let shortest_first_wait = ms(2500);
    let http = CatalogHttp::with_policy(RetryPolicy {
        backoff_base: shortest_first_wait * 2,
        ..CATALOG_POLICY
    });
    let started = Instant::now();

    let error = http
        .send(get(&http, "http://catalog.invalid/"))
        .await
        .unwrap_err();

    assert!(
        started.elapsed() < shortest_first_wait,
        "{:?}",
        started.elapsed()
    );
    assert!(
        error.render(identity).contains("dns error"),
        "{}",
        error.render(identity)
    );
}

/// Scenario: A transient failure is retried with exponential backoff and jitter
/// Scenario: A failure that is not transient fails the request at the first attempt
#[tokio::test]
async fn transport_failures_are_classified_by_their_typed_cause() {
    let client = reqwest::Client::builder()
        .connect_timeout(SHORT_POLICY.connect_timeout)
        .build()
        .unwrap();
    let failure = |url: String| {
        let client = client.clone();
        async move { client.get(&url).send().await.expect_err(&url) }
    };
    let (reset, _) = spawn_listener(|stream, _| reset_after_request(stream)).await;
    let (tls_reset, _) = spawn_listener(|stream, _| reset_during_handshake(stream)).await;
    let (plain, _) = spawn_listener(|stream, _| answer_in_plain_http(stream)).await;
    let (closing, _) = spawn_listener(|stream, _| close_after_request(stream)).await;

    let transient = [
        CLOSED_PORT_URI.to_owned(),
        format!("http://{reset}/"),
        format!("https://{tls_reset}/"),
        format!("http://{closing}/"),
    ];
    let final_ = [
        "http://catalog.invalid/".to_owned(),
        format!("https://{plain}/"),
    ];

    for url in transient {
        let error = failure(url.clone()).await;
        assert!(
            is_transient_transport_failure(&error),
            "{url}: {}",
            error_chain(&error)
        );
    }
    for url in final_ {
        let error = failure(url.clone()).await;
        assert!(
            !is_transient_transport_failure(&error),
            "{url}: {}",
            error_chain(&error)
        );
    }
}

use super::*;
use crate::test_support::*;
use std::time::Instant;
use tokio::net::TcpListener;

const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/lakehouse-reader";
const EXTERNAL_ID: &str = "external-id-sentinel";
const BASE_AK: &str = "AKIABASEKEYSENTINEL";
const BASE_SK: &str = "base-secret-key-sentinel";
const BASE_SESSION_TOKEN: &str = "base-session-token-sentinel";
const STATED_REGION: &str = "eu-central-1";
const PLAIN_CATALOG_URI: &str = "http://iceberg-rest:8181";
const SHORT_TIMEOUT: Duration = Duration::from_millis(200);

fn role_creds(sts_endpoint: &str) -> ConnectionCreds {
    ConnectionCreds {
        access_key: BASE_AK.into(),
        secret_key: BASE_SK.into(),
        session_token: Some(BASE_SESSION_TOKEN.into()),
        region: STATED_REGION.into(),
        aws_assume_role_arn: Some(ROLE_ARN.into()),
        aws_external_id: Some(EXTERNAL_ID.into()),
        aws_sts_endpoint: Some(sts_endpoint.into()),
        ..creds_no_auth()
    }
}

async fn assume(creds: ConnectionCreds) -> Result<ConnectionCreds, UdfError> {
    resolve_aws_identity(creds, PLAIN_CATALOG_URI, true).await
}

async fn assume_error(creds: ConnectionCreds, timeout: Duration) -> String {
    match resolve_aws_identity_within(creds, PLAIN_CATALOG_URI, true, timeout).await {
        Err(UdfError::User(msg)) => msg,
        Err(other) => panic!("a failed AssumeRole must be a user error, got {other:?}"),
        Ok(_) => panic!("the AssumeRole must fail"),
    }
}

/// Both credential sets hold the same value in every field, the `Debug`-redacted ones included.
fn assert_same_creds(actual: &ConnectionCreds, expected: &ConnectionCreds) {
    assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    let redacted = |c: &ConnectionCreds| {
        (
            c.access_key.clone(),
            c.secret_key.clone(),
            c.session_token.clone(),
            c.token.clone(),
            c.client_secret.clone(),
            c.account_key.clone(),
            c.sas_token.clone(),
            c.aws_external_id.clone(),
        )
    };
    assert_eq!(redacted(actual), redacted(expected));
}

/// Scenario: a plaintext STS endpoint is used only with ALLOW_HTTP, and a non-http(s) endpoint is refused, both before any request.
#[tokio::test]
async fn plaintext_sts_endpoint_requires_allow_http() {
    let (sts, heads) = spawn_recording_sts(200, ASSUME_ROLE_RESPONSE).await;

    let Err(UdfError::User(refused)) =
        resolve_aws_identity(role_creds(&sts), PLAIN_CATALOG_URI, false).await
    else {
        panic!("an http STS endpoint without ALLOW_HTTP must be refused as a user error");
    };
    assert!(
        refused.contains("aws_sts_endpoint") && refused.contains("ALLOW_HTTP"),
        "the refusal must name aws_sts_endpoint and ALLOW_HTTP: {refused}"
    );
    assert!(heads.lock().unwrap().is_empty(), "no request may be sent");

    resolve_aws_identity(role_creds(&sts), PLAIN_CATALOG_URI, true)
        .await
        .expect("ALLOW_HTTP consents to the plaintext endpoint");
    assert_eq!(heads.lock().unwrap().len(), 1);

    for unsupported in ["ftp://sts.example.com", "sts.example.com"] {
        let Err(UdfError::User(msg)) =
            resolve_aws_identity(role_creds(unsupported), PLAIN_CATALOG_URI, true).await
        else {
            panic!("{unsupported} must be refused as a user error");
        };
        assert!(
            msg.contains("aws_sts_endpoint"),
            "the refusal of {unsupported} must name aws_sts_endpoint: {msg}"
        );
    }
}

/// Scenario: an unanswered AssumeRole times out after the client timeout, naming the STS endpoint host; production waits 30 seconds.
#[tokio::test]
async fn sts_request_times_out_naming_the_endpoint_host() {
    let silent = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", silent.local_addr().expect("local_addr"));
    let started = Instant::now();

    let msg = assume_error(role_creds(&endpoint), SHORT_TIMEOUT).await;

    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the injected timeout must bound the wait"
    );
    assert!(msg.contains("timed out"), "must state the timeout: {msg}");
    assert!(
        msg.contains("127.0.0.1"),
        "must name the endpoint host: {msg}"
    );
    assert_eq!(STS_TIMEOUT, Duration::from_secs(30));
    drop(silent);
}

/// Scenario: a credential set naming no role is returned unchanged, with no STS request and no endpoint gate.
#[tokio::test]
async fn no_role_identity_is_returned_unchanged_without_a_request() {
    let (sts, heads) = spawn_recording_sts(200, ASSUME_ROLE_RESPONSE).await;

    for unstated in [None, Some(String::new())] {
        let stated = ConnectionCreds {
            aws_assume_role_arn: unstated,
            ..role_creds(&sts)
        };

        let resolved = resolve_aws_identity(stated.clone(), PLAIN_CATALOG_URI, false)
            .await
            .expect("a set naming no role resolves without STS");

        assert_same_creds(&resolved, &stated);
    }
    assert!(
        heads.lock().unwrap().is_empty(),
        "no STS request may be sent"
    );
}

/// Scenario: the resolved identity replaces only the access key, secret key, and session token; the role and vending flag survive.
#[tokio::test]
async fn resolved_identity_replaces_only_the_key_triple() {
    let (sts, _heads) = spawn_recording_sts(200, ASSUME_ROLE_RESPONSE).await;
    let stated = ConnectionCreds {
        warehouse: "123456789012".into(),
        endpoint: "http://minio:9000".into(),
        path_style: Some(true),
        use_sigv4: true,
        use_vended_credentials: true,
        account_name: Some("account".into()),
        ..role_creds(&sts)
    };

    let resolved = assume(stated.clone())
        .await
        .expect("the AssumeRole must succeed");

    let expected = ConnectionCreds {
        access_key: SESSION_AK.into(),
        secret_key: SESSION_SK.into(),
        session_token: Some(SESSION_TOKEN.into()),
        ..stated
    };
    assert_same_creds(&resolved, &expected);
}

/// Scenario: a denied, refused, or timed-out AssumeRole names the role and the STS host, carries STS's code and message, and no credential or query.
#[tokio::test]
async fn assume_role_failure_is_credential_safe_for_denial_refusal_and_timeout() {
    let denial = format!(
        r#"<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Sender</Type>
    <Code>AccessDenied</Code>
    <Message>User is not authorized to perform: sts:AssumeRole (echo {BASE_SK} {BASE_SESSION_TOKEN} {EXTERNAL_ID})</Message>
  </Error>
  <RequestId>c6104cbe-af31-11e0-8154-cbc7ccf896c7</RequestId>
</ErrorResponse>"#
    );
    let (denying, _heads) = spawn_recording_sts(403, denial).await;
    let denied = assume_error(role_creds(&denying), STS_TIMEOUT).await;
    for detail in [
        "403",
        "AccessDenied",
        "is not authorized to perform: sts:AssumeRole",
    ] {
        assert!(
            denied.contains(detail),
            "the denial must carry {detail}: {denied}"
        );
    }

    let (failing, _heads) = spawn_recording_sts(500, format!("{BASE_SK} plain body")).await;
    let failed = assume_error(role_creds(&failing), STS_TIMEOUT).await;
    assert!(
        failed.contains("500"),
        "must carry the HTTP status: {failed}"
    );
    assert!(
        !failed.contains("plain body"),
        "a non-XML body is never echoed: {failed}"
    );

    let closed = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let refused_endpoint = format!("http://{}", closed.local_addr().expect("local_addr"));
    drop(closed);
    let refused = assume_error(role_creds(&refused_endpoint), STS_TIMEOUT).await;

    let silent = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let silent_endpoint = format!("http://{}", silent.local_addr().expect("local_addr"));
    let timed_out = assume_error(role_creds(&silent_endpoint), SHORT_TIMEOUT).await;
    drop(silent);

    for (case, msg) in [
        ("denial", &denied),
        ("server error", &failed),
        ("refusal", &refused),
        ("timeout", &timed_out),
    ] {
        assert!(msg.contains(ROLE_ARN), "{case} must name the role: {msg}");
        assert!(
            msg.contains("127.0.0.1"),
            "{case} must name the STS host: {msg}"
        );
        for secret in [
            BASE_SK,
            BASE_SESSION_TOKEN,
            EXTERNAL_ID,
            SESSION_SK,
            SESSION_TOKEN,
        ] {
            assert!(!msg.contains(secret), "{case} leaked {secret}: {msg}");
        }
        for query in ["Action=", "RoleSessionName", "ExternalId="] {
            assert!(
                !msg.contains(query),
                "{case} leaked the query string: {msg}"
            );
        }
    }
}

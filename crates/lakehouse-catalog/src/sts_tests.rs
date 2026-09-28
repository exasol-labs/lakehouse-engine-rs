use super::*;
use crate::test_support::*;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};
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

fn only_head(heads: &Arc<Mutex<Vec<String>>>) -> String {
    let heads = heads.lock().unwrap();
    let [head] = heads.as_slice() else {
        panic!("exactly one STS request must be sent, got {}", heads.len());
    };
    head.clone()
}

fn request_target(head: &str) -> &str {
    head.lines()
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .expect("a recorded head starts with its request line")
}

fn decoded_query(head: &str) -> Vec<(String, String)> {
    let query = request_target(head).split_once('?').map_or("", |(_, q)| q);
    let mut pairs: Vec<(String, String)> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    pairs.sort();
    pairs
}

fn expected_query(external_id: Option<&str>) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = [
        ("Action", "AssumeRole"),
        ("Version", "2011-06-15"),
        ("RoleArn", ROLE_ARN),
        ("RoleSessionName", "lakehouse-engine"),
    ]
    .into_iter()
    .chain(external_id.map(|id| ("ExternalId", id)))
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect();
    pairs.sort();
    pairs
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

/// The value after `label` in a SigV4 `Authorization` header, up to the next `,`.
fn authorization_field<'a>(authorization: &'a str, label: &str) -> &'a str {
    let start = authorization
        .find(label)
        .unwrap_or_else(|| panic!("the Authorization header must carry {label}"))
        + label.len();
    let rest = &authorization[start..];
    rest.split(',').next().unwrap_or(rest).trim()
}

/// SHA-256 of `bytes` in lowercase hex, read from the `x-amz-content-sha256`
/// header aws-sigv4 computes over a payload: this borrows only its hash
/// primitive, never its query canonicalization, which the caller recomputes.
fn sha256_hex(bytes: &[u8]) -> String {
    let identity: Identity = Credentials::new("AK", "SK", None, None, "hash").into();
    let mut settings = SigningSettings::default();
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("sts")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .expect("every signing parameter is set")
        .into();
    let request = SignableRequest::new(
        "POST",
        "https://hash.invalid/",
        std::iter::empty(),
        SignableBody::Bytes(bytes),
    )
    .expect("a static request is signable");
    let (instructions, _) = sign(request, &params).expect("signing").into_parts();
    instructions
        .headers()
        .find_map(|(name, value)| (name == "x-amz-content-sha256").then(|| value.to_string()))
        .expect("aws-sigv4 emits the payload hash header")
}

fn hex_decode(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("hex digit pair"))
        .collect()
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    hex_decode(&v4::calculate_signature(key, data.as_bytes()))
}

/// The SigV4 signature a server recomputes from the request as RECEIVED: the
/// raw query pairs sorted but never re-encoded. It equals the sent signature
/// only when the sent query already is the canonical query the signer signed.
fn signature_recomputed_from_the_received_request(head: &str, secret_key: &str) -> String {
    let authorization = authorization_header(head).expect("the request must be signed");
    let credential = authorization_field(authorization, "Credential=");
    let signed_headers = authorization_field(authorization, "SignedHeaders=");
    let scope = credential
        .split_once('/')
        .map(|(_, scope)| scope)
        .expect("Credential=<key id>/<scope>");
    let [date, region, service, _terminator] = scope.split('/').collect::<Vec<_>>()[..] else {
        panic!("the credential scope has four parts: {scope}");
    };

    let (path, raw_query) = request_target(head)
        .split_once('?')
        .expect("the AssumeRole request carries a query");
    let mut raw_pairs: Vec<(&str, &str)> = raw_query
        .split('&')
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .collect();
    raw_pairs.sort();
    let canonical_query = raw_pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical_headers: String = signed_headers
        .split(';')
        .map(|name| {
            let value = header_value(head, name)
                .unwrap_or_else(|| panic!("the signed header {name} must be sent"));
            format!("{name}:{value}\n")
        })
        .collect();
    let payload_hash =
        header_value(head, "x-amz-content-sha256").expect("the payload hash is sent");
    let canonical_request = format!(
        "GET\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let amz_date = header_value(head, "x-amz-date").expect("the signing time is sent");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let date_key = hmac(format!("AWS4{secret_key}").as_bytes(), date);
    let region_key = hmac(&date_key, region);
    let service_key = hmac(&region_key, service);
    let signing_key = hmac(&service_key, "aws4_request");
    v4::calculate_signature(signing_key, string_to_sign.as_bytes())
}

/// Scenario: the AssumeRole request is a signed GET carrying the role, the fixed session name, and the stated external id, never a duration.
#[tokio::test]
async fn assume_role_request_is_a_signed_get_carrying_role_and_external_id() {
    let (sts, heads) = spawn_recording_sts(200, ASSUME_ROLE_RESPONSE).await;

    assume(role_creds(&sts))
        .await
        .expect("the stub answers a well-formed AssumeRoleResponse");

    let head = only_head(&heads);
    assert!(head.starts_with("GET /?"), "must be a GET: {head}");
    assert_eq!(decoded_query(&head), expected_query(Some(EXTERNAL_ID)));
    let authorization = authorization_header(&head).expect("the request must be signed");
    assert!(
        authorization.contains(&format!("Credential={BASE_AK}/")),
        "signed by the base access key: {authorization}"
    );
    assert!(
        authorization.contains(&format!("/{STATED_REGION}/sts/aws4_request")),
        "signed for service sts in the STS region: {authorization}"
    );
    assert_eq!(
        header_value(&head, "x-amz-security-token"),
        Some(BASE_SESSION_TOKEN),
        "the base session token rides as x-amz-security-token"
    );
}

/// Scenario: a role without an external id sends no ExternalId parameter, and a base identity without a session token sends no security token.
#[tokio::test]
async fn a_role_without_an_external_id_sends_no_external_id_parameter() {
    for unstated in [None, Some(String::new())] {
        let (sts, heads) = spawn_recording_sts(200, ASSUME_ROLE_RESPONSE).await;
        let creds = ConnectionCreds {
            aws_external_id: unstated.clone(),
            session_token: None,
            ..role_creds(&sts)
        };

        assume(creds).await.expect("the AssumeRole must succeed");

        let head = only_head(&heads);
        assert_eq!(
            decoded_query(&head),
            expected_query(None),
            "external id {unstated:?} must send no ExternalId"
        );
        assert_eq!(header_value(&head, "x-amz-security-token"), None);
    }
}

/// Scenario: every character the ExternalId pattern admits is encoded identically in the sent query and the signed canonical query.
#[tokio::test]
async fn external_id_characters_survive_signing() {
    const PATTERN_CHARACTERS: &str = "a+b=c,d.e@f:g/h-i_J9";
    let (sts, heads) = spawn_recording_sts(200, ASSUME_ROLE_RESPONSE).await;
    let creds = ConnectionCreds {
        aws_external_id: Some(PATTERN_CHARACTERS.into()),
        ..role_creds(&sts)
    };

    assume(creds).await.expect("the AssumeRole must succeed");

    let head = only_head(&heads);
    assert_eq!(
        decoded_query(&head),
        expected_query(Some(PATTERN_CHARACTERS))
    );
    let authorization = authorization_header(&head).expect("the request must be signed");
    assert_eq!(
        signature_recomputed_from_the_received_request(&head, BASE_SK),
        authorization_field(authorization, "Signature="),
        "the received query must be the canonical query that was signed: {head}"
    );
}

/// Scenario: the STS endpoint is the stated override, else the regional endpoint of the signing region, else the global endpoint signed for us-east-1.
#[test]
fn sts_endpoint_prefers_override_then_regional_then_global() {
    const GLUE_URI: &str = "https://glue.eu-west-1.amazonaws.com/iceberg";
    let unrouted = ConnectionCreds {
        region: String::new(),
        ..creds_no_auth()
    };
    let cases = [
        (
            ConnectionCreds {
                aws_sts_endpoint: Some("https://vpce-1.sts.eu-west-1.vpce.amazonaws.com".into()),
                ..unrouted.clone()
            },
            GLUE_URI,
            "https://vpce-1.sts.eu-west-1.vpce.amazonaws.com/",
            "eu-west-1",
        ),
        (
            ConnectionCreds {
                aws_sts_endpoint: Some("https://sts.example.internal".into()),
                ..unrouted.clone()
            },
            PLAIN_CATALOG_URI,
            "https://sts.example.internal/",
            "us-east-1",
        ),
        (
            ConnectionCreds {
                region: "ap-south-1".into(),
                ..unrouted.clone()
            },
            GLUE_URI,
            "https://sts.eu-west-1.amazonaws.com/",
            "eu-west-1",
        ),
        (
            ConnectionCreds {
                region: "ap-south-1".into(),
                ..unrouted.clone()
            },
            PLAIN_CATALOG_URI,
            "https://sts.ap-south-1.amazonaws.com/",
            "ap-south-1",
        ),
        (
            unrouted.clone(),
            PLAIN_CATALOG_URI,
            "https://sts.amazonaws.com/",
            "us-east-1",
        ),
    ];

    for (creds, catalog_uri, url, region) in cases {
        let endpoint = StsEndpoint::resolve(&creds, catalog_uri, false)
            .unwrap_or_else(|e| panic!("{url} must resolve: {e}"));
        assert_eq!(endpoint.url.as_str(), url);
        assert_eq!(endpoint.region, region, "signing region for {url}");
    }
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

/// Scenario: a region that cannot form an STS host is refused, naming aws_sts_endpoint, before any request.
#[tokio::test]
async fn an_unroutable_region_without_an_sts_endpoint_is_an_error_naming_aws_sts_endpoint() {
    let (sts, heads) = spawn_recording_sts(200, ASSUME_ROLE_RESPONSE).await;
    let creds = ConnectionCreds {
        region: "eu west 1".into(),
        aws_sts_endpoint: None,
        ..role_creds(&sts)
    };

    let Err(UdfError::User(msg)) = resolve_aws_identity(creds, PLAIN_CATALOG_URI, true).await
    else {
        panic!("a region that forms no STS host must be refused as a user error");
    };

    assert!(
        msg.contains("does not form a valid STS endpoint host") && msg.contains("aws_sts_endpoint"),
        "the refusal must name the unroutable region and aws_sts_endpoint: {msg}"
    );
    assert!(heads.lock().unwrap().is_empty(), "no request may be sent");
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

/// Scenario: the session credentials are read from AssumeRoleResult/Credentials with surrounding whitespace removed and entity references decoded.
#[tokio::test]
async fn assume_role_response_yields_trimmed_decoded_session_credentials() {
    const PADDED_ENTITIES: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>
        ASIADECODEDKEY
      </AccessKeyId>
      <SecretAccessKey>  secret&amp;with&lt;entities&#43;/&gt;  </SecretAccessKey>
      <SessionToken>
        token&#x2F;with&quot;references&apos;
      </SessionToken>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;
    let (sts, _heads) = spawn_recording_sts(200, PADDED_ENTITIES).await;

    let resolved = assume(role_creds(&sts))
        .await
        .expect("a padded response parses");

    assert_eq!(resolved.access_key, "ASIADECODEDKEY");
    assert_eq!(resolved.secret_key, "secret&with<entities+/>");
    assert_eq!(
        resolved.session_token.as_deref(),
        Some("token/with\"references'")
    );
}

/// Scenario: an absent or empty AccessKeyId, SecretAccessKey, or SessionToken is an error naming the element, never a fallback to the base identity.
#[tokio::test]
async fn assume_role_response_missing_an_element_is_an_error_naming_it() {
    for (element, value) in [
        ("AccessKeyId", SESSION_AK),
        ("SecretAccessKey", SESSION_SK),
        ("SessionToken", SESSION_TOKEN),
    ] {
        let present = format!("<{element}>{value}</{element}>");
        let absent = ASSUME_ROLE_RESPONSE.replace(&present, "");
        let empty = ASSUME_ROLE_RESPONSE.replace(&present, &format!("<{element}>  </{element}>"));
        for (shape, body) in [("absent", absent), ("empty", empty)] {
            let (sts, _heads) = spawn_recording_sts(200, body).await;

            let msg = assume_error(role_creds(&sts), STS_TIMEOUT).await;

            assert!(
                msg.contains(element),
                "an {shape} {element} must be named: {msg}"
            );
            for session_value in [SESSION_AK, SESSION_SK, SESSION_TOKEN] {
                assert!(
                    !msg.contains(session_value),
                    "{session_value} leaked: {msg}"
                );
            }
        }
    }

    let no_credentials = ASSUME_ROLE_RESPONSE.replace(
        ASSUME_ROLE_RESPONSE
            .split("<Credentials>")
            .nth(1)
            .and_then(|rest| rest.split("</Credentials>").next())
            .expect("the fixture carries a Credentials element"),
        "",
    );
    let (sts, _heads) = spawn_recording_sts(200, no_credentials).await;
    let msg = assume_error(role_creds(&sts), STS_TIMEOUT).await;
    assert!(
        msg.contains("AccessKeyId"),
        "an empty Credentials must name AccessKeyId: {msg}"
    );
}

/// Scenario: a 2xx body that is not a well-formed AssumeRoleResponse document is an error stating so, without the body text.
#[tokio::test]
async fn a_malformed_assume_role_body_is_an_error_without_the_body() {
    const SENTINEL: &str = "BODY_SENTINEL_VALUE";
    let wrong_root = ASSUME_ROLE_RESPONSE
        .replace("AssumeRoleResponse", "GetSessionTokenResponse")
        .replace(SESSION_SK, SENTINEL);
    for body in [
        format!("<AssumeRoleResponse><AssumeRoleResult><Credentials><AccessKeyId>{SENTINEL}"),
        format!("{SENTINEL} is not XML"),
        format!(r#"{{"AccessKeyId":"{SENTINEL}"}}"#),
        wrong_root,
        String::new(),
    ] {
        let (sts, _heads) = spawn_recording_sts(200, body.clone()).await;

        let msg = assume_error(role_creds(&sts), STS_TIMEOUT).await;

        assert!(
            msg.contains("not a well-formed AssumeRoleResponse"),
            "body {body:?} must be reported as malformed: {msg}"
        );
        assert!(!msg.contains(SENTINEL), "the body leaked: {msg}");
    }
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

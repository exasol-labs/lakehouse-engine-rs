use super::*;
use crate::redaction::{redact_credentials, redact_secret_values};
use crate::test_support::*;

/// The four REST-catalog auth prop keys, for negative assertions.
const AUTH_PROP_KEYS: [&str; 4] = [
    REST_CATALOG_PROP_TOKEN,
    REST_CATALOG_PROP_CREDENTIAL,
    REST_CATALOG_PROP_OAUTH2_SERVER_URI,
    REST_CATALOG_PROP_SCOPE,
];

// ---------------------------------------------------------------------------
// Task 5 — catalog-auth prop injection (inject_catalog_auth_props)
//
// The pure prop-map seam is tested directly: `inject_catalog_auth_props`
// mutates a `HashMap<String,String>` from the resolved `ConnectionCreds`,
// which is exactly what `build_rest_catalog` does before the async
// `RestCatalogBuilder::load`. Asserting against the map needs no network I/O.
// ---------------------------------------------------------------------------

/// Scenario: Static bearer token is attached to unsigned catalog requests.
///
/// A token-only config injects `"token"` and NONE of
/// `"credential"`/`"oauth2-server-uri"`/`"scope"` — the token mode never
/// consults the OAuth2 endpoint/scope.
#[test]
fn build_rest_catalog_sets_token_prop() {
    let mut creds = base_creds();
    creds.token = Some("bearer-secret-123".into());
    // oauth2_server_uri / scope present but irrelevant: token mode ignores them.
    creds.oauth2_server_uri = Some("https://auth.example/token".into());
    creds.scope = Some("catalog".into());

    let mut props = HashMap::new();
    inject_catalog_auth_props(&mut props, &creds);

    assert_eq!(
        props.get(REST_CATALOG_PROP_TOKEN).map(String::as_str),
        Some("bearer-secret-123"),
        "token mode must set the token prop"
    );
    assert!(
        !props.contains_key(REST_CATALOG_PROP_CREDENTIAL),
        "token mode must NOT set credential"
    );
    assert!(
        !props.contains_key(REST_CATALOG_PROP_OAUTH2_SERVER_URI),
        "token mode must NOT set oauth2-server-uri (never consulted)"
    );
    assert!(
        !props.contains_key(REST_CATALOG_PROP_SCOPE),
        "token mode must NOT set scope (never consulted)"
    );
}

/// An empty-string token (`Some("")`) is treated as ABSENT, not present:
/// the empty-vs-absent distinction must not inject a blank `"token"` prop.
#[test]
fn build_rest_catalog_empty_token_injects_nothing() {
    let mut creds = base_creds();
    creds.token = Some(String::new());

    let mut props = HashMap::new();
    inject_catalog_auth_props(&mut props, &creds);

    for key in AUTH_PROP_KEYS {
        assert!(
            !props.contains_key(key),
            "empty-string token must inject no auth prop, but {key} was set"
        );
    }
}

/// Scenario: OAuth2 client credentials drive the catalog client-credentials grant.
///
/// OAuth config sets `"credential"` = `"id:secret"`; includes
/// `"oauth2-server-uri"`/`"scope"` ONLY when supplied (here: both supplied),
/// and NEVER sets `"token"`.
#[test]
fn build_rest_catalog_sets_credential_and_oauth_props() {
    // (a) Both oauth2_server_uri and scope supplied → both injected.
    let mut creds = base_creds();
    creds.client_id = Some("client-abc".into());
    creds.client_secret = Some("secret-xyz".into());
    creds.oauth2_server_uri = Some("https://auth.example/token".into());
    creds.scope = Some("catalog-read".into());

    let mut props = HashMap::new();
    inject_catalog_auth_props(&mut props, &creds);

    assert_eq!(
        props.get(REST_CATALOG_PROP_CREDENTIAL).map(String::as_str),
        Some("client-abc:secret-xyz"),
        "credential must be the colon-joined client_id:client_secret"
    );
    assert_eq!(
        props
            .get(REST_CATALOG_PROP_OAUTH2_SERVER_URI)
            .map(String::as_str),
        Some("https://auth.example/token"),
        "oauth2-server-uri must be set when supplied"
    );
    assert_eq!(
        props.get(REST_CATALOG_PROP_SCOPE).map(String::as_str),
        Some("catalog-read"),
        "scope must be set when supplied"
    );
    assert!(
        !props.contains_key(REST_CATALOG_PROP_TOKEN),
        "OAuth mode must NEVER set token"
    );

    // (b) Neither oauth2_server_uri nor scope supplied → omitted (catalog defaults).
    let mut creds = base_creds();
    creds.client_id = Some("client-abc".into());
    creds.client_secret = Some("secret-xyz".into());

    let mut props = HashMap::new();
    inject_catalog_auth_props(&mut props, &creds);

    assert_eq!(
        props.get(REST_CATALOG_PROP_CREDENTIAL).map(String::as_str),
        Some("client-abc:secret-xyz"),
        "credential still set when oauth2-server-uri/scope omitted"
    );
    assert!(
        !props.contains_key(REST_CATALOG_PROP_OAUTH2_SERVER_URI),
        "oauth2-server-uri must be omitted when not supplied (catalog defaults)"
    );
    assert!(
        !props.contains_key(REST_CATALOG_PROP_SCOPE),
        "scope must be omitted when not supplied (catalog defaults)"
    );
    assert!(
        !props.contains_key(REST_CATALOG_PROP_TOKEN),
        "OAuth mode must NEVER set token"
    );

    // (c) Incomplete client credentials (only client_id, empty secret) must NOT
    //     enter the credential branch (guards the non_empty filter + the
    //     all-or-nothing pair requirement).
    let mut creds = base_creds();
    creds.client_id = Some("client-abc".into());
    creds.client_secret = Some(String::new());

    let mut props = HashMap::new();
    inject_catalog_auth_props(&mut props, &creds);

    for key in AUTH_PROP_KEYS {
        assert!(
            !props.contains_key(key),
            "incomplete client credentials must inject no auth prop, but {key} was set"
        );
    }
}

/// Scenario: No catalog auth props are set when neither token nor OAuth
/// credentials are supplied — the prop map is shape-identical to before.
#[test]
fn build_rest_catalog_no_auth_props_when_no_auth() {
    let creds = base_creds();

    let mut props = HashMap::new();
    inject_catalog_auth_props(&mut props, &creds);

    assert!(
        props.is_empty(),
        "no-auth config must inject nothing into the props map: {props:?}"
    );
    for key in AUTH_PROP_KEYS {
        assert!(
            !props.contains_key(key),
            "no-auth config must not set {key}"
        );
    }
}

/// Scenario: OAuth2 client credentials drive the catalog client-credentials
/// grant — the grant POSTs form fields to the token endpoint and returns the
/// `access_token`.
///
/// This test spins up a minimal local HTTP server that verifies the form
/// fields (`grant_type`, `client_id`, `client_secret`, `scope`) and returns a
/// mock `access_token`. We then call `oauth2_client_credentials_grant` against
/// this server and assert the returned token matches.
#[tokio::test]
async fn oauth2_grant_built_from_client_credentials() {
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // Bind to a random port on localhost.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let port = addr.port();

    // Build creds pointing at our local server.
    let catalog_uri = format!("http://127.0.0.1:{port}");
    let mut creds = creds_no_auth();
    creds.client_id = Some("my-client-id".into());
    creds.client_secret = Some(CLIENT_SECRET.into());
    creds.scope = Some("catalog-read".into());

    // Spawn a minimal HTTP/1.1 server that reads the POST and replies.
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.expect("read");
        let request = String::from_utf8_lossy(&buf[..n]).to_string();

        // Verify the form fields are present in the request body.
        assert!(
            request.contains("grant_type=client_credentials"),
            "grant_type must be client_credentials"
        );
        assert!(
            request.contains("client_id=my-client-id"),
            "client_id must be present"
        );
        // client_secret and scope must be in the body.
        let has_secret = request.contains(CLIENT_SECRET);
        let has_scope = request.contains("scope=catalog-read");
        // Reply with a valid token response.
        let body = format!(
            r#"{{"access_token":"{OAUTH_ACCESS_TOKEN}","token_type":"Bearer","expires_in":3600}}"#
        );
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.expect("write");
        // Return these for the test to check after the call.
        assert!(has_secret, "client_secret must be in POST body");
        assert!(has_scope, "scope must be in POST body when supplied");
    });

    let client = reqwest::Client::new();
    let token = oauth2_client_credentials_grant(&client, &catalog_uri, &creds)
        .await
        .expect("grant must succeed");

    assert_eq!(
        token, OAUTH_ACCESS_TOKEN,
        "returned access_token must match server response"
    );
}

/// Scenario: the grant refuses the `token` + complete-pair shape `validate_creds`
/// rule 6 rejects, because it reads the mode named by `supplied_catalog_auth`
/// instead of re-deriving pair completeness from the two fields.
///
/// The catalog URI is a closed loopback port, so a grant that re-derived
/// completeness would fail with a transport error rather than this refusal —
/// which is what makes the assertion distinguish the two.
#[tokio::test]
async fn oauth2_grant_errors_for_the_validation_rejected_shape() {
    let mut creds = creds_no_auth();
    creds.token = Some(BEARER_TOK.into());
    creds.client_id = Some("client-id-sentinel".into());
    creds.client_secret = Some(CLIENT_SECRET.into());

    let client = reqwest::Client::new();
    let err = oauth2_client_credentials_grant(&client, "http://127.0.0.1:1", &creds)
        .await
        .expect_err("a validation-rejected token+pair shape must not reach the token endpoint");

    let UdfError::User(msg) = err else {
        panic!("an unresolvable auth mode must surface as a user error, got {err:?}");
    };
    assert!(
        msg.contains("complete client_id/client_secret pair"),
        "the grant must refuse on the mode owner's answer, not on a transport error: {msg}"
    );
    assert!(
        !msg.contains(CLIENT_SECRET),
        "the refusal must not leak the client secret: {msg}"
    );
}

// ---------------------------------------------------------------------------
// R3 — redact_catalog_auth_error strips client_id, oauth2_server_uri, scope
// ---------------------------------------------------------------------------

/// Scenario: `redact_catalog_auth_error` strips `client_id`, `oauth2_server_uri`,
/// and `scope` from error messages so the no-leak guarantee matches the doc comment.
#[test]
fn redact_catalog_auth_error_strips_client_id_oauth_uri_scope() {
    const CLIENT_ID_SENTINEL: &str = "MY_CLIENT_ID_SENTINEL";
    const OAUTH_URI_SENTINEL: &str = "https://auth-server-sentinel.example/token";
    const SCOPE_SENTINEL: &str = "MY_SCOPE_SENTINEL_VALUE";

    let mut creds = creds_no_auth();
    creds.client_id = Some(CLIENT_ID_SENTINEL.into());
    creds.oauth2_server_uri = Some(OAUTH_URI_SENTINEL.into());
    creds.scope = Some(SCOPE_SENTINEL.into());

    // Construct an error message that echoes all three values.
    let raw = format!(
        "catalog error: client_id={CLIENT_ID_SENTINEL} uri={OAUTH_URI_SENTINEL} scope={SCOPE_SENTINEL}"
    );

    let redacted = redact_catalog_auth_error(&raw, &creds);

    assert!(
        !redacted.contains(CLIENT_ID_SENTINEL),
        "client_id must be redacted: {redacted}"
    );
    assert!(
        !redacted.contains(OAUTH_URI_SENTINEL),
        "oauth2_server_uri must be redacted: {redacted}"
    );
    assert!(
        !redacted.contains(SCOPE_SENTINEL),
        "scope must be redacted: {redacted}"
    );
}

/// `redact_catalog_auth_error` routes through `redact_error_text`, which runs
/// the value pass BEFORE the label pass — not the inverted composition this
/// site used before. A token shaped like a SAS value (carrying its own `sig=`
/// label) proves the ordering: the inverted composition would let the label
/// pass mangle the token's `sig=` portion first, leaving the value pass unable
/// to match the (now-broken) literal and leaking the signature tail.
#[test]
fn auth_error_sites_apply_the_value_pass_first() {
    const SAS_SHAPED_TOKEN: &str = "sv=2023-11-03&sp=rwdlacx&sig=SIG_VALUE";

    let mut creds = creds_no_auth();
    creds.token = Some(SAS_SHAPED_TOKEN.into());

    let raw = format!("upstream error echoed {SAS_SHAPED_TOKEN}");
    let redacted = redact_catalog_auth_error(&raw, &creds);

    assert!(
        !redacted.contains(SAS_SHAPED_TOKEN),
        "the whole token literal must be gone: {redacted}"
    );
    assert!(
        !redacted.contains("sp=rwdlacx"),
        "the permission field preceding `sig=` must not survive a label-first mangling: \
         {redacted}"
    );

    let inverted = redact_secret_values(&redact_credentials(&raw), &[SAS_SHAPED_TOKEN]);
    assert!(
        inverted.contains("sp=rwdlacx"),
        "the inverted composition is expected to leak the permission field, pinning that \
         `redact_catalog_auth_error` no longer uses it: {inverted}"
    );
}

// ---------------------------------------------------------------------------
// Group C — resolve_catalog_auth precedence
// ---------------------------------------------------------------------------

/// Scenario: `resolve_catalog_auth` selects exactly one auth strategy per
/// mutually-exclusive credential shape, on the non-network branches only
/// (`use_sigv4` and the no-client-credentials paths never contact the network).
#[tokio::test]
async fn resolve_catalog_auth_selects_one_strategy_per_non_network_shape() {
    let client = reqwest::Client::new();

    // SigV4 shape → Sigv4.
    let mut sigv4_creds = creds_no_auth();
    sigv4_creds.use_sigv4 = true;
    let auth = resolve_catalog_auth(&client, "https://catalog.example.com", &sigv4_creds)
        .await
        .expect("sigv4 resolution must not fail");
    assert!(
        matches!(auth, CatalogAuth::Sigv4),
        "use_sigv4 must resolve to CatalogAuth::Sigv4"
    );

    // The OAuth2 client-credentials grant shape is the network branch and is
    // exercised elsewhere; the remaining non-network shapes follow.

    // Bearer-token shape (no SigV4, no OAuth client credentials) → Bearer.
    let mut bearer_creds = creds_no_auth();
    bearer_creds.token = Some(BEARER_TOK.into());
    let auth = resolve_catalog_auth(&client, "https://catalog.example.com", &bearer_creds)
        .await
        .expect("bearer resolution must not fail");
    match auth {
        CatalogAuth::Bearer(token) => assert_eq!(
            token, BEARER_TOK,
            "static token must be carried into CatalogAuth::Bearer verbatim"
        ),
        _ => panic!("a non-empty static token must resolve to CatalogAuth::Bearer"),
    }

    // No-auth shape → None.
    let no_auth_creds = creds_no_auth();
    let auth = resolve_catalog_auth(&client, "https://catalog.example.com", &no_auth_creds)
        .await
        .expect("no-auth resolution must not fail");
    assert!(
        matches!(auth, CatalogAuth::None),
        "no auth fields supplied must resolve to CatalogAuth::None"
    );
}

/// Scenario: `token` supplied alongside a complete `client_id`/`client_secret`
/// pair is a shape `validate_creds` rejects (rule 6) before any catalog
/// session exists — `supplied_catalog_auth` classifies it `Unauthenticated`,
/// so `resolve_catalog_auth` must resolve to `CatalogAuth::None` without
/// attempting the OAuth2 grant. Nothing listens on the loopback port used
/// here, so a network attempt fails fast and deterministically instead of
/// hanging or depending on external DNS.
#[tokio::test]
async fn resolve_catalog_auth_is_unauthenticated_for_the_validation_rejected_shape() {
    let client = reqwest::Client::new();
    let mut creds = creds_no_auth();
    creds.token = Some(BEARER_TOK.into());
    creds.client_id = Some("client-id-sentinel".into());
    creds.client_secret = Some(CLIENT_SECRET.into());

    let auth = resolve_catalog_auth(&client, "http://127.0.0.1:1", &creds)
        .await
        .expect(
            "a validation-rejected token+pair shape must resolve without contacting the network",
        );

    assert!(
        matches!(auth, CatalogAuth::None),
        "a validation-rejected token+pair shape must classify as unauthenticated"
    );
}

/// Scenario: same validation-rejected token+pair shape as above —
/// `inject_catalog_auth_props` must inject none of the four REST-catalog
/// auth props for it.
#[test]
fn inject_catalog_auth_props_injects_nothing_for_the_validation_rejected_shape() {
    let mut creds = creds_no_auth();
    creds.token = Some(BEARER_TOK.into());
    creds.client_id = Some("client-id-sentinel".into());
    creds.client_secret = Some(CLIENT_SECRET.into());

    let mut props = HashMap::new();
    inject_catalog_auth_props(&mut props, &creds);

    for key in AUTH_PROP_KEYS {
        assert!(
            !props.contains_key(key),
            "a validation-rejected token+pair shape must inject no auth prop, but {key} was set"
        );
    }
}

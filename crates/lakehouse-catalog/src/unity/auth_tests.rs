//! Tests for the Unity Catalog authentication strategy: PAT verbatim bearer,
//! OAuth machine-to-machine mint/cache/refresh, the no-auth mode, and the
//! credential-safe grant failure. Grants are served by an in-process mock; the
//! refresh test drives an injected clock so it needs no real clock or sleep.

use super::*;
use crate::test_support::base_creds;
use crate::unity::mock_server::spawn;
use crate::{CatalogClient, UnityCatalogSession};
use exasol_udf_sdk::error::UdfError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MINTED_TOKEN: &str = "MINTED-ACCESS-TOKEN";
const OAUTH_SECRET_SENTINEL: &str = "OAUTH_CLIENT_SECRET_SENTINEL_VALUE";

#[tokio::test]
async fn pat_is_applied_as_bearer_verbatim() {
    let server = spawn(|_req| (200, r#"{"tables":[]}"#.to_string())).await;
    let mut creds = base_creds();
    creds.token = Some("pat-verbatim-123".to_string());
    let session = UnityCatalogSession::new(&server.base_url, creds);

    session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect("list failed");

    assert_eq!(
        server.requests()[0].authorization.as_deref(),
        Some("Bearer pat-verbatim-123"),
        "the PAT is applied as the bearer verbatim, with no token exchange"
    );
}

#[tokio::test]
async fn oauth_m2m_mints_bearer_via_client_credentials() {
    let server = spawn(|req| {
        if req.target.ends_with("/oidc/v1/token") {
            (
                200,
                format!(
                    r#"{{"access_token":"{MINTED_TOKEN}","token_type":"Bearer","expires_in":3600}}"#
                ),
            )
        } else {
            (200, r#"{"tables":[]}"#.to_string())
        }
    })
    .await;
    let mut creds = base_creds();
    creds.client_id = Some("client-abc".to_string());
    creds.client_secret = Some("secret-xyz".to_string());
    let session = UnityCatalogSession::new(&server.base_url, creds);

    session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect("list failed");

    let requests = server.requests();
    let token_req = requests
        .iter()
        .find(|req| req.target.ends_with("/oidc/v1/token"))
        .expect("a token request was issued");
    assert_eq!(token_req.method, "POST");
    assert!(
        token_req
            .authorization
            .as_deref()
            .unwrap_or_default()
            .starts_with("Basic "),
        "the grant carries HTTP Basic client_id:client_secret"
    );
    assert!(
        token_req.body.contains("grant_type=client_credentials"),
        "grant body: {}",
        token_req.body
    );
    assert!(
        token_req.body.contains("scope=all-apis"),
        "the scope defaults to all-apis: {}",
        token_req.body
    );

    let tables_req = requests
        .iter()
        .find(|req| req.target.contains("/tables"))
        .expect("a tables request was issued");
    assert_eq!(
        tables_req.authorization,
        Some(format!("Bearer {MINTED_TOKEN}")),
        "the minted token is applied as the request bearer"
    );
}

#[tokio::test]
async fn oauth_token_is_cached_and_refreshed_before_expiry() {
    let grants = Arc::new(AtomicUsize::new(0));
    let server_grants = grants.clone();
    let server = spawn(move |_req| {
        let mint = server_grants.fetch_add(1, Ordering::SeqCst) + 1;
        (
            200,
            format!(r#"{{"access_token":"tok-{mint}","token_type":"Bearer","expires_in":120}}"#),
        )
    })
    .await;

    // A clock the test advances by hand, so caching and refresh are observed
    // without a real clock or a sleep.
    let ticks = Arc::new(Mutex::new(Instant::now()));
    let clock_ticks = ticks.clone();
    let source = OAuthTokenSource {
        client: reqwest::Client::new(),
        token_url: format!("{}/oidc/v1/token", server.base_url),
        client_id: "client-abc".to_string(),
        client_secret: "secret-xyz".to_string(),
        scope: "all-apis".to_string(),
        cache: Mutex::new(None),
        clock: Arc::new(move || *clock_ticks.lock().unwrap()),
    };

    let first = source.bearer().await.expect("first mint");
    let reused = source.bearer().await.expect("cached reuse");
    assert_eq!(
        first, reused,
        "a fresh cached token is reused, not re-minted"
    );
    assert_eq!(
        grants.load(Ordering::SeqCst),
        1,
        "no second grant while the cached token is still valid"
    );

    // Expiry is 120s and the refresh skew is 60s, so the refresh point is +60s.
    *ticks.lock().unwrap() += Duration::from_secs(61);
    let refreshed = source.bearer().await.expect("refresh mint");
    assert_eq!(
        grants.load(Ordering::SeqCst),
        2,
        "the token is re-minted once it reaches its refresh point"
    );
    assert_ne!(first, refreshed, "the refreshed bearer is a fresh mint");
}

#[tokio::test]
async fn oauth_grant_missing_expires_in_is_a_clear_error() {
    // A token endpoint that omits expires_in gives the cache no lifetime: the
    // grant must fail loudly rather than mint a token that re-grants on every
    // request.
    let server = spawn(|_req| {
        (
            200,
            format!(r#"{{"access_token":"{MINTED_TOKEN}","token_type":"Bearer"}}"#),
        )
    })
    .await;
    let source = OAuthTokenSource {
        client: reqwest::Client::new(),
        token_url: format!("{}/oidc/v1/token", server.base_url),
        client_id: "client-abc".to_string(),
        client_secret: "secret-xyz".to_string(),
        scope: "all-apis".to_string(),
        cache: Mutex::new(None),
        clock: Arc::new(Instant::now),
    };

    let err = source
        .bearer()
        .await
        .expect_err("a grant response without expires_in must error");

    let UdfError::User(msg) = err else {
        panic!("expected a UdfError::User variant");
    };
    assert!(
        msg.contains("expires_in"),
        "the error names the missing field: {msg}"
    );
}

#[tokio::test]
async fn oauth_grant_zero_expires_in_is_a_clear_error() {
    let server = spawn(|_req| {
        (
            200,
            format!(r#"{{"access_token":"{MINTED_TOKEN}","token_type":"Bearer","expires_in":0}}"#),
        )
    })
    .await;
    let source = OAuthTokenSource {
        client: reqwest::Client::new(),
        token_url: format!("{}/oidc/v1/token", server.base_url),
        client_id: "client-abc".to_string(),
        client_secret: "secret-xyz".to_string(),
        scope: "all-apis".to_string(),
        cache: Mutex::new(None),
        clock: Arc::new(Instant::now),
    };

    let err = source
        .bearer()
        .await
        .expect_err("a grant response with expires_in=0 must error");

    let UdfError::User(msg) = err else {
        panic!("expected a UdfError::User variant");
    };
    assert!(
        msg.contains("expires_in"),
        "the error names the invalid field: {msg}"
    );
}

#[tokio::test]
async fn unauthenticated_mode_sends_no_authorization_header() {
    let server = spawn(|_req| (200, r#"{"tables":[]}"#.to_string())).await;
    // base_creds() supplies neither a token nor OAuth client credentials.
    let session = UnityCatalogSession::new(&server.base_url, base_creds());

    session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect("list failed");

    assert_eq!(
        server.requests()[0].authorization,
        None,
        "the unauthenticated mode sends no Authorization header"
    );
}

#[tokio::test]
async fn failed_oauth_grant_is_credential_safe_error() {
    let server = spawn(|req| {
        if req.target.ends_with("/oidc/v1/token") {
            // The 401 body echoes the client secret, proving it is stripped.
            (
                401,
                format!("invalid_client: bad secret {OAUTH_SECRET_SENTINEL}"),
            )
        } else {
            (200, r#"{"tables":[]}"#.to_string())
        }
    })
    .await;
    let mut creds = base_creds();
    creds.client_id = Some("client-abc".to_string());
    creds.client_secret = Some(OAUTH_SECRET_SENTINEL.to_string());
    let session = UnityCatalogSession::new(&server.base_url, creds);

    let err = session
        .list_tables(&["cat".to_string(), "sch".to_string()])
        .await
        .expect_err("a failed grant must surface an error");

    let UdfError::User(msg) = err else {
        panic!("expected a UdfError::User variant");
    };
    assert!(
        msg.contains("OAuth client-credentials grant failed"),
        "the error names the grant failure: {msg}"
    );
    assert!(
        !msg.contains(OAUTH_SECRET_SENTINEL),
        "the client secret must be stripped from the error: {msg}"
    );
}

/// Scenario: `token` supplied alongside a complete `client_id`/`client_secret`
/// pair is a shape `validate_creds` rejects (rule 6) before any catalog
/// session exists — `supplied_catalog_auth` classifies it `Unauthenticated`,
/// so `resolve_unity_auth` must resolve to `UnityAuth::None`. Synchronous and
/// infallible, so no network fixture is needed.
#[test]
fn resolve_unity_auth_is_unauthenticated_for_the_validation_rejected_shape() {
    let client = reqwest::Client::new();
    let mut creds = base_creds();
    creds.token = Some("pat-sentinel".into());
    creds.client_id = Some("client-id-sentinel".into());
    creds.client_secret = Some(OAUTH_SECRET_SENTINEL.into());

    let auth = resolve_unity_auth(&client, "https://unity.example.com", &creds);

    assert!(
        matches!(auth, UnityAuth::None),
        "a validation-rejected token+pair shape must classify as unauthenticated"
    );
}

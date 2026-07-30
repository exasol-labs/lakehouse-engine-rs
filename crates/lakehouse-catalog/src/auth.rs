//! REST-catalog authentication: the auth strategy resolved once per query, the
//! OAuth2 client-credentials grant, catalog-auth prop injection, and
//! auth-value redaction.
//!
//! Moved verbatim from the engine's `adapter/pushdown/credentials.rs`.
//! Credential values NEVER appear in any returned SQL string or error message —
//! every error site in this module routes through a redaction closure.

use crate::ConnectionCreds;
use crate::redaction::{redact_credentials, redact_secret_values};
use exasol_udf_sdk::error::UdfError;
use std::collections::HashMap;

/// REST-catalog auth property keys (literal strings, fixed by `iceberg-catalog-rest`
/// 0.10.0; the crate exports no constants for them). They flow through
/// `RestCatalogBuilder::load`, which copies every prop except `uri`/`warehouse`.
pub(crate) const REST_CATALOG_PROP_TOKEN: &str = "token";
pub(crate) const REST_CATALOG_PROP_CREDENTIAL: &str = "credential";
pub(crate) const REST_CATALOG_PROP_OAUTH2_SERVER_URI: &str = "oauth2-server-uri";
pub(crate) const REST_CATALOG_PROP_SCOPE: &str = "scope";

/// Inject catalog-auth props from the resolved credentials into the REST-catalog
/// props map. Three mutually exclusive modes:
///
/// * no `token` and no client credentials → inject nothing (no-auth, default).
/// * non-empty `token` → inject only `token` (the bearer header; the crate never
///   consults `oauth2-server-uri`/`scope` in this mode).
/// * non-empty `client_id` + `client_secret` → inject `credential` =
///   `"client_id:client_secret"`, plus `oauth2-server-uri` ONLY when a non-empty
///   `oauth2_server_uri` is supplied and `scope` ONLY when a non-empty `scope` is
///   supplied; never inject `token` in this mode.
///
/// Token and client-credentials are mutually exclusive by construction.
pub(crate) fn inject_catalog_auth_props(
    props: &mut HashMap<String, String>,
    creds: &ConnectionCreds,
) {
    let token = non_empty(&creds.token);
    let client_id = non_empty(&creds.client_id);
    let client_secret = non_empty(&creds.client_secret);

    if let (Some(id), Some(secret)) = (client_id, client_secret) {
        props.insert(
            REST_CATALOG_PROP_CREDENTIAL.to_string(),
            format!("{id}:{secret}"),
        );
        if let Some(uri) = non_empty(&creds.oauth2_server_uri) {
            props.insert(
                REST_CATALOG_PROP_OAUTH2_SERVER_URI.to_string(),
                uri.to_string(),
            );
        }
        if let Some(scope) = non_empty(&creds.scope) {
            props.insert(REST_CATALOG_PROP_SCOPE.to_string(), scope.to_string());
        }
    } else if let Some(token) = token {
        props.insert(REST_CATALOG_PROP_TOKEN.to_string(), token.to_string());
    }
}

/// Borrow the inner value of an `Option<String>` only when it is non-empty.
fn non_empty(field: &Option<String>) -> Option<&str> {
    field.as_deref().filter(|v| !v.is_empty())
}

/// Redact a catalog error that may have surfaced an auth value. Applies the
/// generic label/pattern redaction AND strips the literal `token`, `client_secret`,
/// `client_id`, `oauth2_server_uri`, and `scope` values so any auth field echoed
/// without a recognizable label can never leak.
pub(crate) fn redact_catalog_auth_error(msg: &str, creds: &ConnectionCreds) -> String {
    let mut secrets: Vec<String> = Vec::new();
    if let Some(token) = non_empty(&creds.token) {
        secrets.push(token.to_string());
    }
    if let Some(secret) = non_empty(&creds.client_secret) {
        // The joined `credential` ("<id>:<secret>") need not be pushed separately:
        // stripping the bare secret first already removes the only sensitive
        // portion, leaving the non-secret `id`.
        secrets.push(secret.to_string());
    }
    if let Some(id) = non_empty(&creds.client_id) {
        secrets.push(id.to_string());
    }
    if let Some(uri) = non_empty(&creds.oauth2_server_uri) {
        secrets.push(uri.to_string());
    }
    if let Some(scope) = non_empty(&creds.scope) {
        secrets.push(scope.to_string());
    }
    let secret_refs: Vec<&str> = secrets.iter().map(String::as_str).collect();
    redact_secret_values(&redact_credentials(msg), &secret_refs)
}

/// The catalog-auth strategy resolved once for a query, used to authenticate
/// every self-issued catalog HTTP request (the `loadTable` GET and the
/// `/v1/config` prefix lookup) identically.
///
/// Orthogonal to credential vending: this selects HOW a request is authenticated,
/// never WHETHER vended credentials are extracted.
pub(crate) enum CatalogAuth {
    /// AWS SigV4 request signing against the `glue` service.
    Sigv4,
    /// `Authorization: Bearer <token>` — either a static `token` or a token
    /// obtained from the OAuth2 client-credentials grant.
    Bearer(String),
    /// No `Authorization` header (no-auth catalog).
    None,
}

/// The OAuth2 token request body field name for the grant type.
const OAUTH2_GRANT_TYPE: &str = "client_credentials";

/// The default token endpoint path appended to the catalog URI when no explicit
/// `oauth2_server_uri` is supplied (the Iceberg REST catalog convention).
const OAUTH2_DEFAULT_TOKEN_PATH: &str = "/v1/oauth/tokens";

/// Perform the OAuth2 client-credentials grant and return the obtained access token.
///
/// Form-encodes `grant_type=client_credentials`, `client_id`, `client_secret`,
/// and the optional `scope`, POSTed to `creds.oauth2_server_uri` when supplied,
/// otherwise to the catalog default token endpoint (`{catalog_uri}/v1/oauth/tokens`).
///
/// `client_secret`, the request, and the obtained token NEVER appear in any
/// returned error: every error site strips the client secret AND the obtained
/// token via value-based redaction.
async fn oauth2_client_credentials_grant(
    client: &reqwest::Client,
    catalog_uri: &str,
    creds: &ConnectionCreds,
) -> Result<String, UdfError> {
    let client_id = non_empty(&creds.client_id).ok_or_else(|| {
        UdfError::User("OAuth2 grant requires client_id but none was resolved".into())
    })?;
    let client_secret = non_empty(&creds.client_secret).ok_or_else(|| {
        UdfError::User("OAuth2 grant requires client_secret but none was resolved".into())
    })?;

    let token_url = match non_empty(&creds.oauth2_server_uri) {
        Some(uri) => uri.to_string(),
        None => format!(
            "{}{OAUTH2_DEFAULT_TOKEN_PATH}",
            catalog_uri.trim_end_matches('/')
        ),
    };

    // Strip the client secret AND the obtained token from every error. The token
    // is not yet known at the point a transport/parse error is built, so it is
    // added to the redaction set after a successful parse before being returned.
    let redact_secret =
        |msg: &str| redact_secret_values(&redact_credentials(msg), &[client_secret]);

    let mut form: Vec<(&str, &str)> = vec![
        ("grant_type", OAUTH2_GRANT_TYPE),
        ("client_id", client_id),
        ("client_secret", client_secret),
    ];
    if let Some(scope) = non_empty(&creds.scope) {
        form.push(("scope", scope));
    }

    let response = client
        .post(&token_url)
        .header("accept", "application/json")
        .form(&form)
        .send()
        .await
        .map_err(|e| {
            UdfError::User(format!(
                "OAuth2 token request failed: {}",
                redact_secret(&e.to_string())
            ))
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "(unreadable body)".into());
        return Err(UdfError::User(format!(
            "OAuth2 token endpoint returned HTTP {}: {}",
            status.as_u16(),
            redact_secret(&body)
        )));
    }

    let body: serde_json::Value = response.json().await.map_err(|e| {
        UdfError::User(format!(
            "failed to parse OAuth2 token response: {}",
            redact_secret(&e.to_string())
        ))
    })?;

    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            UdfError::User(format!(
                "OAuth2 token response missing access_token: {}",
                redact_secret(&body.to_string())
            ))
        })?;

    Ok(access_token.to_string())
}

/// Resolve the catalog-auth strategy for a query from the resolved credentials.
///
/// Precedence mirrors `inject_catalog_auth_props` (SigV4 is mutually exclusive with
/// catalog auth, enforced upstream in `validate_creds`):
/// 1. `use_sigv4` → SigV4 signing.
/// 2. `client_id` + `client_secret` → OAuth2 client-credentials grant → bearer.
/// 3. non-empty `token` → static bearer.
/// 4. otherwise → no auth.
pub(crate) async fn resolve_catalog_auth(
    client: &reqwest::Client,
    catalog_uri: &str,
    creds: &ConnectionCreds,
) -> Result<CatalogAuth, UdfError> {
    if creds.use_sigv4 {
        return Ok(CatalogAuth::Sigv4);
    }
    if non_empty(&creds.client_id).is_some() && non_empty(&creds.client_secret).is_some() {
        let token = oauth2_client_credentials_grant(client, catalog_uri, creds).await?;
        return Ok(CatalogAuth::Bearer(token));
    }
    if let Some(token) = non_empty(&creds.token) {
        return Ok(CatalogAuth::Bearer(token.to_string()));
    }
    Ok(CatalogAuth::None)
}

#[cfg(test)]
mod tests {
    use super::*;
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

        // (c) Mutual exclusivity by construction: client-credentials present alongside
        //     a stray token → credential wins, token is never injected.
        let mut creds = base_creds();
        creds.client_id = Some("client-abc".into());
        creds.client_secret = Some("secret-xyz".into());
        creds.token = Some("stray-token".into());

        let mut props = HashMap::new();
        inject_catalog_auth_props(&mut props, &creds);

        assert!(
            props.contains_key(REST_CATALOG_PROP_CREDENTIAL),
            "credential must be set when client credentials present"
        );
        assert!(
            !props.contains_key(REST_CATALOG_PROP_TOKEN),
            "client-credentials mode must NOT inject token even if one is set"
        );

        // (d) Incomplete client credentials (only client_id, empty secret) must NOT
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

    // ---------------------------------------------------------------------------
    // Group C — resolve_catalog_auth precedence
    // ---------------------------------------------------------------------------

    /// Scenario: `resolve_catalog_auth` selects the auth strategy by the
    /// documented precedence, on the non-network branches only (`use_sigv4` and
    /// the no-client-credentials paths never contact the network).
    #[tokio::test]
    async fn resolve_catalog_auth_precedence_non_network_branches() {
        let client = reqwest::Client::new();

        // 1. use_sigv4 → Sigv4, regardless of any token also being set.
        let mut sigv4_creds = creds_no_auth();
        sigv4_creds.use_sigv4 = true;
        sigv4_creds.token = Some(BEARER_TOK.into());
        let auth = resolve_catalog_auth(&client, "https://catalog.example.com", &sigv4_creds)
            .await
            .expect("sigv4 resolution must not fail");
        assert!(
            matches!(auth, CatalogAuth::Sigv4),
            "use_sigv4 must take precedence and resolve to CatalogAuth::Sigv4"
        );

        // Precedence #2 (OAuth2 client-credentials grant) is the network branch and
        // is exercised elsewhere; the remaining non-network branches follow.

        // 3. Non-empty token, no SigV4, no OAuth client credentials → Bearer.
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

        // 4. No auth supplied at all → None.
        let no_auth_creds = creds_no_auth();
        let auth = resolve_catalog_auth(&client, "https://catalog.example.com", &no_auth_creds)
            .await
            .expect("no-auth resolution must not fail");
        assert!(
            matches!(auth, CatalogAuth::None),
            "no auth fields supplied must resolve to CatalogAuth::None"
        );
    }
}

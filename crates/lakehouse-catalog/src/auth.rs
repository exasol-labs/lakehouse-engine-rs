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
#[path = "auth_tests.rs"]
mod tests;

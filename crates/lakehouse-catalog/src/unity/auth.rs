//! The authentication strategy a `UnityCatalogSession` applies to every Unity
//! Catalog REST request: a static personal-access-token bearer, a Databricks
//! OAuth machine-to-machine client-credentials grant with a minted/cached/
//! refreshed bearer, or no authentication for an OSS server whose auth is off.
//!
//! Every mode terminates in an `Authorization: Bearer` header or no header, so
//! only a token's origin and lifecycle differ. The resolved bearer, the OAuth
//! client secret, and the minted access token NEVER appear in any returned
//! error: every grant error site strips them.

use crate::ConnectionCreds;
use crate::redaction::redact_error_text;
use exasol_udf_sdk::error::UdfError;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Refresh a minted OAuth token this many seconds before its stated expiry, so a
/// request never carries a bearer that expires in flight. The grant returns no
/// refresh token, so the only renewal is a fresh mint.
const OAUTH_REFRESH_SKEW_SECS: u64 = 60;

/// The default OAuth scope for the Databricks client-credentials grant.
const OAUTH_DEFAULT_SCOPE: &str = "all-apis";

/// How a request is authenticated: no header, a static PAT bearer, or an OAuth
/// machine-to-machine bearer minted and cached by [`OAuthTokenSource`].
pub(crate) enum UnityAuth {
    None,
    Pat(String),
    OAuth(OAuthTokenSource),
}

impl UnityAuth {
    /// Apply the resolved strategy to `builder`, returning the request builder
    /// and the bearer token it now carries (if any), so the caller can strip that
    /// live token from any error it surfaces even when the token was minted and
    /// is not present in the CONNECTION credentials.
    pub(crate) async fn apply(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> Result<(reqwest::RequestBuilder, Option<String>), UdfError> {
        match self {
            UnityAuth::None => Ok((builder, None)),
            UnityAuth::Pat(token) => Ok((builder.bearer_auth(token), Some(token.clone()))),
            UnityAuth::OAuth(source) => {
                let token = source.bearer().await?;
                Ok((builder.bearer_auth(&token), Some(token)))
            }
        }
    }
}

/// A minted bearer and the instant at which it should be refreshed (its stated
/// expiry minus the skew), so the hot path is a single instant comparison.
struct CachedToken {
    token: String,
    refresh_at: Instant,
}

/// The monotonic clock the token cache reads, injected so the refresh decision is
/// testable without a real clock.
type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// Mints, caches, and refreshes an OAuth machine-to-machine bearer via the
/// client-credentials grant. One source per session, so a whole enumeration reuses
/// a single minted token rather than re-granting per request.
pub(crate) struct OAuthTokenSource {
    client: reqwest::Client,
    token_url: String,
    client_id: String,
    client_secret: String,
    scope: String,
    cache: Mutex<Option<CachedToken>>,
    clock: Clock,
}

impl OAuthTokenSource {
    /// Return a valid bearer: reuse the cached token while it is still fresh, and
    /// mint a new one once the cached token has reached its refresh point.
    pub(crate) async fn bearer(&self) -> Result<String, UdfError> {
        {
            // The guard is released before any `.await`, so the returned future
            // stays `Send` across the mint.
            let cache = self.cache.lock().unwrap();
            if let Some(cached) = cache.as_ref()
                && (self.clock)() < cached.refresh_at
            {
                return Ok(cached.token.clone());
            }
        }
        let (token, expires_in) = self.mint().await?;
        let refresh_at = (self.clock)()
            + Duration::from_secs(expires_in.saturating_sub(OAUTH_REFRESH_SKEW_SECS));
        *self.cache.lock().unwrap() = Some(CachedToken {
            token: token.clone(),
            refresh_at,
        });
        Ok(token)
    }

    /// Perform the client-credentials grant and return the minted token and its
    /// `expires_in` seconds. HTTP Basic `client_id:client_secret`, body
    /// `grant_type=client_credentials&scope=<scope>`. The client secret and any
    /// partial token material are stripped from every returned error.
    async fn mint(&self) -> Result<(String, u64), UdfError> {
        let redact = |msg: &str| redact_error_text(msg, &[self.client_secret.as_str()]);
        let form = [
            ("grant_type", "client_credentials"),
            ("scope", self.scope.as_str()),
        ];
        let response = self
            .client
            .post(&self.token_url)
            .basic_auth(&self.client_id, Some(&self.client_secret))
            .header("accept", "application/json")
            .form(&form)
            .send()
            .await
            .map_err(|e| {
                UdfError::User(format!(
                    "Unity Catalog OAuth client-credentials grant failed: {}",
                    redact(&e.to_string())
                ))
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "(unreadable body)".into());
            return Err(UdfError::User(format!(
                "Unity Catalog OAuth client-credentials grant failed with HTTP {}: {}",
                status.as_u16(),
                redact(&body)
            )));
        }
        let parsed: OAuthTokenResponse = response.json().await.map_err(|e| {
            UdfError::User(format!(
                "Unity Catalog OAuth client-credentials grant returned an unparseable response: {}",
                redact(&e.to_string())
            ))
        })?;
        if parsed.access_token.is_empty() {
            return Err(UdfError::User(
                "Unity Catalog OAuth client-credentials grant returned no access_token".into(),
            ));
        }
        // An absent or zero `expires_in` gives the cache no lifetime to reason
        // about: refreshing at `now` would re-mint on every request. Reject it as
        // a grant error, mirroring the empty-`access_token` guard above, rather
        // than silently defeating the cache.
        let expires_in = parsed.expires_in.filter(|&secs| secs > 0).ok_or_else(|| {
            UdfError::User(
                "Unity Catalog OAuth client-credentials grant returned no usable expires_in".into(),
            )
        })?;
        Ok((parsed.access_token, expires_in))
    }
}

#[derive(Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

/// Resolve the authentication strategy from the CONNECTION credentials without a
/// new credential field: a non-empty `token` selects the personal-access-token
/// bearer, a `client_id`/`client_secret` pair selects the OAuth grant, and
/// neither selects the unauthenticated mode.
///
/// Synchronous by design: the OAuth grant is deferred to the first request, so
/// building a session issues no request and an empty enumeration mints no token.
pub(crate) fn resolve_unity_auth(
    client: &reqwest::Client,
    address: &str,
    creds: &ConnectionCreds,
) -> UnityAuth {
    if let Some(token) = non_empty(&creds.token) {
        return UnityAuth::Pat(token.to_string());
    }
    if let (Some(id), Some(secret)) = (non_empty(&creds.client_id), non_empty(&creds.client_secret))
    {
        let token_url = match non_empty(&creds.oauth2_server_uri) {
            Some(uri) => uri.to_string(),
            None => format!("{}/oidc/v1/token", address.trim_end_matches('/')),
        };
        let scope = non_empty(&creds.scope)
            .unwrap_or(OAUTH_DEFAULT_SCOPE)
            .to_string();
        return UnityAuth::OAuth(OAuthTokenSource {
            client: client.clone(),
            token_url,
            client_id: id.to_string(),
            client_secret: secret.to_string(),
            scope,
            cache: Mutex::new(None),
            clock: Arc::new(Instant::now),
        });
    }
    UnityAuth::None
}

fn non_empty(field: &Option<String>) -> Option<&str> {
    field.as_deref().filter(|value| !value.is_empty())
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;

//! The resolved bearer, the OAuth client secret, and the minted access token
//! must never appear in any returned error.

use crate::ConnectionCreds;
use crate::creds::{SuppliedCatalogAuth, non_empty};
use crate::redaction::redact_error_text;
use exasol_udf_sdk::error::UdfError;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Refresh this early so a bearer never expires in flight; the grant returns no
/// refresh token, so renewal is a fresh mint.
const OAUTH_REFRESH_SKEW_SECS: u64 = 60;

const OAUTH_DEFAULT_SCOPE: &str = "all-apis";

pub(crate) enum UnityAuth {
    None,
    Pat(String),
    OAuth(OAuthTokenSource),
}

impl UnityAuth {
    /// Also returns the bearer so callers can redact a minted token that is not
    /// in the CONNECTION credentials.
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

struct CachedToken {
    token: String,
    refresh_at: Instant,
}

/// Injected so the refresh decision is testable without a real clock.
type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// One source per session, so a whole enumeration reuses one minted token.
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
    pub(crate) async fn bearer(&self) -> Result<String, UdfError> {
        {
            // Guard dropped before `.await` so the future stays `Send`.
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
        // A zero/absent lifetime would re-mint on every request.
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

/// Synchronous by design: the OAuth grant is deferred to the first request, so
/// building a session issues no request.
pub(crate) fn resolve_unity_auth(
    client: &reqwest::Client,
    address: &str,
    creds: &ConnectionCreds,
) -> UnityAuth {
    match creds.supplied_catalog_auth() {
        SuppliedCatalogAuth::Unauthenticated => UnityAuth::None,
        SuppliedCatalogAuth::StaticToken(token) => UnityAuth::Pat(token.to_string()),
        SuppliedCatalogAuth::ClientCredentials {
            client_id,
            client_secret,
        } => {
            let token_url = match non_empty(&creds.oauth2_server_uri) {
                Some(uri) => uri.to_string(),
                None => format!("{}/oidc/v1/token", address.trim_end_matches('/')),
            };
            let scope = non_empty(&creds.scope)
                .unwrap_or(OAUTH_DEFAULT_SCOPE)
                .to_string();
            UnityAuth::OAuth(OAuthTokenSource {
                client: client.clone(),
                token_url,
                client_id: client_id.to_string(),
                client_secret: client_secret.to_string(),
                scope,
                cache: Mutex::new(None),
                clock: Arc::new(Instant::now),
            })
        }
    }
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;

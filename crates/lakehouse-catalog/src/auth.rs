//! REST-catalog authentication. Credential values never appear in any returned
//! string or error: every error site routes through redaction.

use crate::ConnectionCreds;
use crate::catalog_http::{CatalogHttp, RepeatSafety};
use crate::creds::{SuppliedCatalogAuth, non_empty};
use crate::error_chain::error_chain;
use crate::expiring::{EpochClock, ExpiringCredential, Lease};
use crate::redaction::redact_error_text;
use crate::sigv4::required_signing_region;
use exasol_udf_sdk::error::UdfError;
use std::time::Duration;

/// Renew an OAuth2 token this early, so a bearer never expires in flight; the grant returns no
/// refresh token, so renewal is a fresh grant.
pub(crate) const OAUTH_REFRESH_SKEW_SECS: u64 = 60;

/// Strips the literal auth values too, so a value echoed without a recognizable
/// label cannot leak. The bearers go first, so a field value one contains cannot split it.
pub(crate) fn redact_catalog_auth_error(
    msg: &str,
    creds: &ConnectionCreds,
    bearers: &[String],
) -> String {
    let fields = [
        creds.token.as_deref(),
        // Stripping the bare secret also covers the joined `<id>:<secret>` credential.
        creds.client_secret.as_deref(),
        creds.client_id.as_deref(),
        creds.oauth2_server_uri.as_deref(),
        creds.scope.as_deref(),
    ];
    let secrets: Vec<&str> = bearers
        .iter()
        .map(String::as_str)
        .chain(fields.into_iter().flatten())
        .collect();
    redact_error_text(msg, &secrets)
}

/// Resolved once per session; authenticates every self-issued catalog request
/// identically. Orthogonal to credential vending.
pub(crate) enum CatalogAuth {
    Sigv4 { region: String },
    Bearer(String),
    OAuth2(Box<OAuth2Token>),
    None,
}

impl CatalogAuth {
    /// The bearer token a request carries now, after an OAuth2 token that reached its renewal
    /// point was granted anew; `None` when requests carry no bearer.
    pub(crate) async fn bearer(&self) -> Result<Option<String>, UdfError> {
        match self {
            Self::Bearer(token) => Ok(Some(token.clone())),
            Self::OAuth2(token) => token.bearer().await.map(Some),
            Self::Sigv4 { .. } | Self::None => Ok(None),
        }
    }

    /// Every bearer token this auth has held, so an error can be stripped of each one.
    pub(crate) fn held_bearers(&self) -> Vec<String> {
        match self {
            Self::Bearer(token) => vec![token.clone()],
            Self::OAuth2(token) => token.token.held_values(),
            Self::Sigv4 { .. } | Self::None => Vec::new(),
        }
    }
}

/// An OAuth2 client-credentials token that is granted anew once it reaches its renewal point, so
/// one session outlives its tokens and no caller rebuilds the session to renew one.
pub(crate) struct OAuth2Token {
    grant: ClientCredentialsGrant,
    token: ExpiringCredential<String>,
}

impl OAuth2Token {
    /// Runs the session's first grant, so a refused grant fails the session before any other
    /// catalog request.
    async fn granted(grant: ClientCredentialsGrant, clock: EpochClock) -> Result<Self, UdfError> {
        let first = grant.request(clock()).await?;
        let skew = Duration::from_secs(OAUTH_REFRESH_SKEW_SECS);
        Ok(Self {
            grant,
            token: ExpiringCredential::seeded(first, skew, clock),
        })
    }

    async fn bearer(&self) -> Result<String, UdfError> {
        self.token.get(|now_ms| self.grant.request(now_ms)).await
    }
}

const OAUTH2_GRANT_TYPE: &str = "client_credentials";

/// Iceberg REST catalog convention, used when no `oauth2_server_uri` is supplied.
const OAUTH2_DEFAULT_TOKEN_PATH: &str = "/v1/oauth/tokens";

/// One session's client-credentials grant, kept so the session can run it again.
struct ClientCredentialsGrant {
    http: CatalogHttp,
    token_url: String,
    client_id: String,
    client_secret: String,
    scope: Option<String>,
}

impl ClientCredentialsGrant {
    /// A token whose answer states no `expires_in` is kept for the life of the session, because
    /// the Iceberg REST token answer requires only `access_token`.
    async fn request(&self, now_ms: u64) -> Result<Lease<String>, UdfError> {
        let redact_secret = |msg: &str| redact_error_text(msg, &[self.client_secret.as_str()]);
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", OAUTH2_GRANT_TYPE),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
        ];
        if let Some(scope) = &self.scope {
            form.push(("scope", scope));
        }

        let request = self
            .http
            .post(&self.token_url)
            .header("accept", "application/json")
            .form(&form)
            .build()
            .map_err(|e| {
                UdfError::User(format!(
                    "failed to build OAuth2 token request: {}",
                    redact_secret(&error_chain(&e))
                ))
            })?;

        let body: serde_json::Value = self
            .http
            .send_json(
                request,
                RepeatSafety::Safe,
                "OAuth2 token request",
                redact_secret,
            )
            .await?;

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
        let expires_at_ms = body
            .get("expires_in")
            .and_then(serde_json::Value::as_u64)
            .map(|secs| now_ms.saturating_add(secs.saturating_mul(1000)));

        Ok(Lease {
            value: access_token.to_string(),
            expires_at_ms,
        })
    }
}

/// SigV4 and token/OAuth never co-occur (`validate_creds` rule 4). SigV4 is
/// refused when neither the credentials nor `catalog_uri` yield a signing region. An OAuth2
/// token's expiry is read on `clock`.
pub(crate) async fn resolve_catalog_auth(
    http: &CatalogHttp,
    catalog_uri: &str,
    creds: &ConnectionCreds,
    clock: EpochClock,
) -> Result<CatalogAuth, UdfError> {
    if creds.use_sigv4 {
        return Ok(CatalogAuth::Sigv4 {
            region: required_signing_region(creds, catalog_uri)?,
        });
    }
    match creds.supplied_catalog_auth() {
        SuppliedCatalogAuth::Unauthenticated => Ok(CatalogAuth::None),
        SuppliedCatalogAuth::StaticToken(token) => Ok(CatalogAuth::Bearer(token.to_string())),
        SuppliedCatalogAuth::ClientCredentials {
            client_id,
            client_secret,
        } => {
            let token_url = match non_empty(&creds.oauth2_server_uri) {
                Some(uri) => uri.to_string(),
                None => format!(
                    "{}{OAUTH2_DEFAULT_TOKEN_PATH}",
                    catalog_uri.trim_end_matches('/')
                ),
            };
            let grant = ClientCredentialsGrant {
                http: http.clone(),
                token_url,
                client_id: client_id.to_string(),
                client_secret: client_secret.to_string(),
                scope: non_empty(&creds.scope).map(str::to_string),
            };
            let token = OAuth2Token::granted(grant, clock).await?;
            Ok(CatalogAuth::OAuth2(Box::new(token)))
        }
    }
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;

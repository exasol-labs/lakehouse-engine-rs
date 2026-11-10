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
/// label cannot leak. The bearer goes first, so a field value it contains cannot split it.
pub(crate) fn redact_catalog_auth_error(
    msg: &str,
    creds: &ConnectionCreds,
    bearer: Option<&str>,
) -> String {
    let secrets: Vec<&str> = [
        bearer,
        creds.token.as_deref(),
        // Stripping the bare secret also covers the joined `<id>:<secret>` credential.
        creds.client_secret.as_deref(),
        creds.client_id.as_deref(),
        creds.oauth2_server_uri.as_deref(),
        creds.scope.as_deref(),
    ]
    .into_iter()
    .flatten()
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
    /// Puts the bearer on `builder`, after an OAuth2 token that reached its renewal point was
    /// granted anew, and returns the token the request carries, so the caller redacts exactly it.
    /// A request that carries no bearer returns `None`.
    pub(crate) async fn apply(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> Result<(reqwest::RequestBuilder, Option<String>), UdfError> {
        let bearer = match self {
            Self::Bearer(token) => Some(token.clone()),
            Self::OAuth2(token) => Some(token.bearer().await?),
            Self::Sigv4 { .. } | Self::None => None,
        };
        Ok(match bearer {
            Some(token) => (builder.bearer_auth(&token), Some(token)),
            None => (builder, None),
        })
    }
}

/// How a grant presents the client credentials to the token endpoint.
#[derive(Clone, Copy)]
pub(crate) enum ClientAuthentication {
    /// `client_id` and `client_secret` in the form body, as the Iceberg REST OAuth2 endpoint reads.
    Form,
    /// HTTP Basic, as the Unity Catalog token endpoint reads.
    Basic,
}

/// An OAuth2 client-credentials grant, kept so its token can be granted again.
pub(crate) struct ClientCredentialsGrant {
    pub(crate) http: CatalogHttp,
    pub(crate) token_url: String,
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    pub(crate) scope: Option<String>,
    pub(crate) authentication: ClientAuthentication,
    /// Names the grant in errors.
    pub(crate) label: &'static str,
    /// A token answer without a usable `expires_in` is an error when set, because the token could
    /// never be renewed; otherwise the token is kept for the life of the session, since the
    /// Iceberg REST token answer requires only `access_token`.
    pub(crate) require_expiry: bool,
}

/// Why a grant answer yields no lease.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum GrantAnswerError {
    NoAccessToken,
    NoUsableExpiry,
}

/// Reads a grant answer into a lease whose expiry is on the clock that read `now_ms`. A missing
/// or zero `expires_in` is no lifetime: the token is kept for the session, or refused when the
/// grant requires one.
pub(crate) fn lease_from_grant_answer(
    answer: &serde_json::Value,
    now_ms: u64,
    require_expiry: bool,
) -> Result<Lease<String>, GrantAnswerError> {
    let access_token = answer
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or(GrantAnswerError::NoAccessToken)?;
    let expires_at_ms = answer
        .get("expires_in")
        .and_then(serde_json::Value::as_u64)
        .filter(|&secs| secs > 0)
        .map(|secs| now_ms.saturating_add(secs.saturating_mul(1000)));
    if require_expiry && expires_at_ms.is_none() {
        return Err(GrantAnswerError::NoUsableExpiry);
    }
    Ok(Lease {
        value: access_token.to_string(),
        expires_at_ms,
    })
}

impl ClientCredentialsGrant {
    async fn request(&self, now_ms: u64) -> Result<Lease<String>, UdfError> {
        let label = self.label;
        let redact_secret = |msg: &str| redact_error_text(msg, &[self.client_secret.as_str()]);
        let mut form: Vec<(&str, &str)> = vec![("grant_type", OAUTH2_GRANT_TYPE)];
        if matches!(self.authentication, ClientAuthentication::Form) {
            form.push(("client_id", &self.client_id));
            form.push(("client_secret", &self.client_secret));
        }
        if let Some(scope) = &self.scope {
            form.push(("scope", scope));
        }

        let mut builder = self
            .http
            .post(&self.token_url)
            .header("accept", "application/json")
            .form(&form);
        if matches!(self.authentication, ClientAuthentication::Basic) {
            builder = builder.basic_auth(&self.client_id, Some(&self.client_secret));
        }
        let request = builder.build().map_err(|e| {
            UdfError::User(format!(
                "failed to build {label} request: {}",
                redact_secret(&error_chain(&e))
            ))
        })?;

        let body: serde_json::Value = self
            .http
            .send_json(request, RepeatSafety::Safe, label, redact_secret)
            .await?;

        lease_from_grant_answer(&body, now_ms, self.require_expiry).map_err(|error| {
            UdfError::User(match error {
                GrantAnswerError::NoAccessToken => format!(
                    "{label} response missing access_token: {}",
                    redact_secret(&body.to_string())
                ),
                GrantAnswerError::NoUsableExpiry => {
                    format!("{label} response has no usable expires_in")
                }
            })
        })
    }
}

/// An OAuth2 client-credentials token that is granted anew once it reaches its renewal point, so
/// one session outlives its tokens and no caller rebuilds the session to renew one.
pub(crate) struct OAuth2Token {
    grant: ClientCredentialsGrant,
    token: ExpiringCredential<String>,
}

impl OAuth2Token {
    /// Holds no token, so the first [`Self::bearer`] runs the grant.
    pub(crate) fn lazy(grant: ClientCredentialsGrant, clock: EpochClock) -> Self {
        Self {
            grant,
            token: ExpiringCredential::new(Duration::from_secs(OAUTH_REFRESH_SKEW_SECS), clock),
        }
    }

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

    pub(crate) async fn bearer(&self) -> Result<String, UdfError> {
        self.token.get(|now_ms| self.grant.request(now_ms)).await
    }
}

const OAUTH2_GRANT_TYPE: &str = "client_credentials";

/// Iceberg REST catalog convention, used when no `oauth2_server_uri` is supplied.
const OAUTH2_DEFAULT_TOKEN_PATH: &str = "/v1/oauth/tokens";

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
                authentication: ClientAuthentication::Form,
                label: "OAuth2 token request",
                require_expiry: false,
            };
            let token = OAuth2Token::granted(grant, clock).await?;
            Ok(CatalogAuth::OAuth2(Box::new(token)))
        }
    }
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;

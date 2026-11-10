//! The resolved bearer, the OAuth client secret, and the minted access token
//! must never appear in any returned error.

use crate::ConnectionCreds;
use crate::auth::{ClientAuthentication, ClientCredentialsGrant, OAuth2Token};
use crate::catalog_http::CatalogHttp;
use crate::creds::{SuppliedCatalogAuth, non_empty};
use crate::expiring::system_clock;
use exasol_udf_sdk::error::UdfError;

const OAUTH_DEFAULT_SCOPE: &str = "all-apis";

const GRANT: &str = "Unity Catalog OAuth client-credentials grant";

pub(crate) enum UnityAuth {
    None,
    Pat(String),
    OAuth(Box<OAuth2Token>),
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

/// Synchronous by design: the OAuth grant is deferred to the first request, so
/// building a session issues no request.
pub(crate) fn resolve_unity_auth(
    http: &CatalogHttp,
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
            let grant = ClientCredentialsGrant {
                http: http.clone(),
                token_url,
                client_id: client_id.to_string(),
                client_secret: client_secret.to_string(),
                scope: Some(scope),
                authentication: ClientAuthentication::Basic,
                label: GRANT,
                require_expiry: true,
            };
            UnityAuth::OAuth(Box::new(OAuth2Token::lazy(grant, system_clock())))
        }
    }
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;

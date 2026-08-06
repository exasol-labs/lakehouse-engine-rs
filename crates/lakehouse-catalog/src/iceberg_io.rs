//! The authenticated catalog `GET` the catalog layer's REST access is built on:
//! it applies the resolved auth strategy to the request and deserializes the JSON
//! body.
//!
//! Moved verbatim from the engine's `adapter/pushdown/credentials.rs`.
//! Credential values NEVER appear in any returned error — every error site
//! routes through a redaction closure.

use crate::ConnectionCreds;
use crate::auth::{CatalogAuth, redact_catalog_auth_error};
use crate::redaction::redact_secret_values;
use exasol_udf_sdk::error::UdfError;

/// Build and authenticate a `GET` request against `url`, applying the resolved
/// catalog-auth strategy and (when vending) the access-delegation header, then
/// execute it and deserialize the JSON body into `T`.
///
/// Credential values NEVER appear in the returned error.
pub(crate) async fn authed_get_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    auth: &CatalogAuth,
    send_access_delegation: bool,
    creds: &ConnectionCreds,
) -> Result<T, UdfError> {
    // Redact static catalog-auth secrets AND the live bearer token (which, for the
    // OAuth2 mode, is the grant-obtained access token and is NOT present in
    // `creds`). Every error site below routes through this closure.
    let redact = |msg: &str| {
        let base = redact_catalog_auth_error(msg, creds);
        match auth {
            CatalogAuth::Bearer(token) => redact_secret_values(&base, &[token.as_str()]),
            CatalogAuth::Sigv4 | CatalogAuth::None => base,
        }
    };

    let mut builder = client.get(url).header("accept", "application/json");
    if send_access_delegation {
        builder = builder.header("X-Iceberg-Access-Delegation", "vended-credentials");
    }
    if let CatalogAuth::Bearer(token) = auth {
        builder = builder.bearer_auth(token);
    }
    let request = builder.build().map_err(|e| {
        UdfError::User(format!(
            "failed to build catalog request: {}",
            redact(&e.to_string())
        ))
    })?;

    let request = match auth {
        CatalogAuth::Sigv4 => crate::sigv4::sign_request(
            request,
            &creds.access_key,
            &creds.secret_key,
            creds.session_token.as_deref(),
            &creds.region,
            "glue",
        )
        .map_err(|e| {
            UdfError::User(format!(
                "failed to sign catalog request: {}",
                redact(&e.to_string())
            ))
        })?,
        CatalogAuth::Bearer(_) | CatalogAuth::None => request,
    };

    let response = client.execute(request).await.map_err(|e| {
        UdfError::User(format!(
            "catalog request failed: {}",
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
            "catalog returned HTTP {}: {}",
            status.as_u16(),
            redact(&body)
        )));
    }

    response.json::<T>().await.map_err(|e| {
        UdfError::User(format!(
            "failed to parse catalog response: {}",
            redact(&e.to_string())
        ))
    })
}

#[cfg(test)]
#[path = "iceberg_io_tests.rs"]
mod tests;

//! REST-catalog authentication. Credential values never appear in any returned
//! string or error: every error site routes through redaction.

use crate::ConnectionCreds;
use crate::creds::{SuppliedCatalogAuth, non_empty};
use crate::redaction::redact_error_text;
use crate::sigv4::required_signing_region;
use exasol_udf_sdk::error::UdfError;
use std::collections::HashMap;

/// Fixed by `iceberg-catalog-rest` 0.10.0, which exports no constants for them.
pub(crate) const REST_CATALOG_PROP_TOKEN: &str = "token";
pub(crate) const REST_CATALOG_PROP_CREDENTIAL: &str = "credential";
pub(crate) const REST_CATALOG_PROP_OAUTH2_SERVER_URI: &str = "oauth2-server-uri";
pub(crate) const REST_CATALOG_PROP_SCOPE: &str = "scope";

/// Token and client-credentials never co-occur: `validate_creds` rule 6 rejects a
/// CONNECTION supplying both.
pub(crate) fn inject_catalog_auth_props(
    props: &mut HashMap<String, String>,
    creds: &ConnectionCreds,
) {
    match creds.supplied_catalog_auth() {
        SuppliedCatalogAuth::Unauthenticated => {}
        SuppliedCatalogAuth::StaticToken(token) => {
            props.insert(REST_CATALOG_PROP_TOKEN.to_string(), token.to_string());
        }
        SuppliedCatalogAuth::ClientCredentials {
            client_id,
            client_secret,
        } => {
            props.insert(
                REST_CATALOG_PROP_CREDENTIAL.to_string(),
                format!("{client_id}:{client_secret}"),
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
        }
    }
}

/// Strips the literal auth values too, so a value echoed without a recognizable
/// label cannot leak.
pub(crate) fn redact_catalog_auth_error(msg: &str, creds: &ConnectionCreds) -> String {
    let mut secrets: Vec<String> = Vec::new();
    if let Some(token) = non_empty(&creds.token) {
        secrets.push(token.to_string());
    }
    if let Some(secret) = non_empty(&creds.client_secret) {
        // Stripping the bare secret also covers the joined `<id>:<secret>` credential.
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
    redact_error_text(msg, &secret_refs)
}

/// Resolved once per query; authenticates every self-issued catalog request
/// identically. Orthogonal to credential vending.
pub(crate) enum CatalogAuth {
    Sigv4 { region: String },
    Bearer(String),
    None,
}

const OAUTH2_GRANT_TYPE: &str = "client_credentials";

/// Iceberg REST catalog convention, used when no `oauth2_server_uri` is supplied.
const OAUTH2_DEFAULT_TOKEN_PATH: &str = "/v1/oauth/tokens";

async fn oauth2_client_credentials_grant(
    client: &reqwest::Client,
    catalog_uri: &str,
    creds: &ConnectionCreds,
) -> Result<String, UdfError> {
    let SuppliedCatalogAuth::ClientCredentials {
        client_id,
        client_secret,
    } = creds.supplied_catalog_auth()
    else {
        return Err(UdfError::User(
            "OAuth2 grant requires a complete client_id/client_secret pair but none was resolved"
                .into(),
        ));
    };

    let token_url = match non_empty(&creds.oauth2_server_uri) {
        Some(uri) => uri.to_string(),
        None => format!(
            "{}{OAUTH2_DEFAULT_TOKEN_PATH}",
            catalog_uri.trim_end_matches('/')
        ),
    };

    let redact_secret = |msg: &str| redact_error_text(msg, &[client_secret]);

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

/// SigV4 and token/OAuth never co-occur (`validate_creds` rule 4). SigV4 is
/// refused when neither the credentials nor `catalog_uri` yield a signing region.
pub(crate) async fn resolve_catalog_auth(
    client: &reqwest::Client,
    catalog_uri: &str,
    creds: &ConnectionCreds,
) -> Result<CatalogAuth, UdfError> {
    if creds.use_sigv4 {
        return Ok(CatalogAuth::Sigv4 {
            region: required_signing_region(creds, catalog_uri)?,
        });
    }
    match creds.supplied_catalog_auth() {
        SuppliedCatalogAuth::Unauthenticated => Ok(CatalogAuth::None),
        SuppliedCatalogAuth::StaticToken(token) => Ok(CatalogAuth::Bearer(token.to_string())),
        SuppliedCatalogAuth::ClientCredentials { .. } => Ok(CatalogAuth::Bearer(
            oauth2_client_credentials_grant(client, catalog_uri, creds).await?,
        )),
    }
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;

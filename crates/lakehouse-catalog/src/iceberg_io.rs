//! The authenticated catalog `GET` and `POST` and the metastore-pointed metadata-file read.
//! Credential values never appear in any returned error.

use crate::auth::{CatalogAuth, redact_catalog_auth_error};
use crate::catalog_http::{CatalogHttp, CatalogRequestError, RepeatSafety, quote_body};
use crate::redaction::redact_error_text;
use crate::{ConnectionCreds, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use iceberg::spec::TableMetadata;

/// A batch-check answer is small, so a larger 2xx answer is refused.
pub(crate) const MAX_ANSWER_BYTES: usize = 64 * 1024;

/// An OAuth2 grant-obtained bearer token is not in `creds`, so every one the session held is
/// redacted explicitly.
pub(crate) fn redact_catalog_text(
    msg: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> String {
    redact_catalog_auth_error(msg, creds, &auth.held_bearers())
}

/// Renews an OAuth2 token that reached its renewal point before the request carries it.
pub(crate) async fn build_signed_request(
    builder: reqwest::RequestBuilder,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> Result<reqwest::Request, UdfError> {
    let redact = |msg: &str| redact_catalog_text(msg, auth, creds);
    let builder = match auth.bearer().await? {
        Some(token) => builder.bearer_auth(token),
        None => builder,
    };
    let request = builder.build().map_err(|e| {
        UdfError::User(format!(
            "failed to build catalog request: {}",
            redact(&e.to_string())
        ))
    })?;

    match auth {
        CatalogAuth::Sigv4 { region } => crate::sigv4::sign_request(
            request,
            &creds.access_key,
            &creds.secret_key,
            creds.session_token.as_deref(),
            region,
            "glue",
        )
        .map_err(|e| {
            UdfError::User(format!(
                "failed to sign catalog request: {}",
                redact(&e.to_string())
            ))
        }),
        CatalogAuth::Bearer(_) | CatalogAuth::OAuth2(_) | CatalogAuth::None => Ok(request),
    }
}

/// Asks the catalog to vend storage credentials with its answer.
pub(crate) fn delegating_access(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    builder.header("X-Iceberg-Access-Delegation", "vended-credentials")
}

pub(crate) async fn authed_get_json<T: serde::de::DeserializeOwned>(
    http: &CatalogHttp,
    url: &str,
    auth: &CatalogAuth,
    send_access_delegation: bool,
    creds: &ConnectionCreds,
) -> Result<T, CatalogRequestError> {
    let mut builder = http.get(url).header("accept", "application/json");
    if send_access_delegation {
        builder = delegating_access(builder);
    }
    let request = build_signed_request(builder, auth, creds)
        .await
        .map_err(|e| CatalogRequestError::without_status(e.to_string()))?;
    http.send_json(request, RepeatSafety::Safe, "catalog request", |msg| {
        redact_catalog_text(msg, auth, creds)
    })
    .await
}

/// A non-2xx answer is a value, not an error, so the caller maps its status to its own failure.
pub(crate) enum PostAnswer {
    /// Raw body; show it only through [`quote_catalog_body`].
    Accepted { status: u16, body: String },
    /// The body of a non-2xx answer, already quoted by [`quote_catalog_body`].
    Refused { status: u16, body: String },
}

pub(crate) async fn authed_post_json(
    http: &CatalogHttp,
    url: &str,
    payload: &impl serde::Serialize,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> Result<PostAnswer, UdfError> {
    let body = serde_json::to_vec(payload)
        .map_err(|e| UdfError::User(format!("failed to serialize catalog request: {e}")))?;
    let builder = http
        .post(url)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .body(body);
    let request = build_signed_request(builder, auth, creds).await?;
    let answer = http
        .send_capped(request, RepeatSafety::Safe, MAX_ANSWER_BYTES)
        .await
        .map_err(|error| {
            UdfError::User(format!(
                "catalog request failed: {}",
                error.render(|msg| redact_catalog_text(msg, auth, creds))
            ))
        })?;

    let code = answer.status.as_u16();
    let text = String::from_utf8_lossy(&answer.body).into_owned();
    match (answer.status.is_success(), answer.cut) {
        (true, false) => Ok(PostAnswer::Accepted {
            status: code,
            body: text,
        }),
        (true, true) => Err(UdfError::User(format!(
            "the catalog's HTTP {code} answer exceeds {MAX_ANSWER_BYTES} bytes: {}",
            quote_catalog_body(&text, auth, creds)
        ))),
        (false, _) => Ok(PostAnswer::Refused {
            status: code,
            body: quote_catalog_body(&text, auth, creds),
        }),
    }
}

/// `body` as an error may show it: redacted, then cut by [`quote_body`].
pub(crate) fn quote_catalog_body(
    body: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> String {
    quote_body(body.as_bytes(), |msg| redact_catalog_text(msg, auth, creds))
}

/// Reads the `metadata.json` a metastore points to (Iceberg § Metastore Tables) through `storage`.
pub async fn read_iceberg_metadata_file(
    storage: &StorageBackend,
    location: &str,
    table_name: &str,
) -> Result<TableMetadata, UdfError> {
    TableMetadata::read_from(&storage.file_io(), location)
        .await
        .map_err(|error| {
            let message = format!(
                "failed to read the Iceberg metadata file '{location}' of table '{table_name}': \
                 {error}"
            );
            UdfError::User(redact_error_text(&message, &storage.secret_values()))
        })
}

#[cfg(test)]
#[path = "iceberg_io_tests.rs"]
mod tests;

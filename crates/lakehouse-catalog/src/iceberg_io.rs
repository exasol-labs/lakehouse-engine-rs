//! The authenticated catalog `GET` and `POST` and the metastore-pointed metadata-file read.
//! Credential values never appear in any returned error.

use crate::auth::{CatalogAuth, redact_catalog_auth_error};
use crate::catalog_http::CatalogHttp;
use crate::redaction::{redact_error_text, redact_secret_values};
use crate::{ConnectionCreds, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use iceberg::spec::TableMetadata;

/// A batch-check answer is small, so a larger 2xx answer is refused.
pub(crate) const MAX_ANSWER_BYTES: usize = 64 * 1024;
/// The most of an answer body an error quotes. The quote is cut only after the whole read is
/// redacted, so a secret that the cut splits was already replaced.
pub(crate) const MAX_QUOTED_BODY_BYTES: usize = 4096;

/// The OAuth2 grant-obtained bearer token is not in `creds`, so it is redacted explicitly.
fn redact_catalog_text(msg: &str, auth: &CatalogAuth, creds: &ConnectionCreds) -> String {
    let base = redact_catalog_auth_error(msg, creds);
    match auth {
        CatalogAuth::Bearer(token) => redact_secret_values(&base, &[token.as_str()]),
        CatalogAuth::Sigv4 { .. } | CatalogAuth::None => base,
    }
}

fn build_signed_request(
    builder: reqwest::RequestBuilder,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> Result<reqwest::Request, UdfError> {
    let redact = |msg: &str| redact_catalog_text(msg, auth, creds);
    let builder = match auth {
        CatalogAuth::Bearer(token) => builder.bearer_auth(token),
        CatalogAuth::Sigv4 { .. } | CatalogAuth::None => builder,
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
        CatalogAuth::Bearer(_) | CatalogAuth::None => Ok(request),
    }
}

/// `authed_get_json`'s failure: the redacted message, and the HTTP status when the answer carried
/// one, so a caller can tell a missing table from every other failure without parsing the message
/// text.
#[derive(Debug)]
pub(crate) struct CatalogGetError {
    status: Option<reqwest::StatusCode>,
    message: String,
}

impl CatalogGetError {
    fn new(status: Option<reqwest::StatusCode>, message: String) -> Self {
        Self { status, message }
    }

    pub(crate) fn without_status(message: String) -> Self {
        Self::new(None, message)
    }

    /// A 404 answer: the catalog has no loadable Iceberg table under this name.
    pub(crate) fn is_not_found(&self) -> bool {
        self.status == Some(reqwest::StatusCode::NOT_FOUND)
    }

    /// Prefixes the message with the table's dotted identifier, for every kind of failure.
    pub(crate) fn named(self, identifier: &str) -> Self {
        Self::new(
            self.status,
            format!("failed to load table '{identifier}': {}", self.message),
        )
    }
}

impl std::fmt::Display for CatalogGetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<CatalogGetError> for UdfError {
    fn from(error: CatalogGetError) -> Self {
        UdfError::User(error.message)
    }
}

pub(crate) async fn authed_get_json<T: serde::de::DeserializeOwned>(
    http: &CatalogHttp,
    url: &str,
    auth: &CatalogAuth,
    send_access_delegation: bool,
    creds: &ConnectionCreds,
) -> Result<T, CatalogGetError> {
    let mut builder = http.get(url).header("accept", "application/json");
    if send_access_delegation {
        builder = builder.header("X-Iceberg-Access-Delegation", "vended-credentials");
    }
    let request = build_signed_request(builder, auth, creds)
        .map_err(|e| CatalogGetError::without_status(e.to_string()))?;
    let answer = http.send(request).await.map_err(|error| {
        CatalogGetError::new(
            error.status(),
            format!(
                "catalog request failed: {}",
                error.render(|msg| redact_catalog_text(msg, auth, creds))
            ),
        )
    })?;

    if !answer.status.is_success() {
        let body = String::from_utf8_lossy(&answer.body).into_owned();
        return Err(CatalogGetError::new(
            Some(answer.status),
            format!(
                "catalog returned HTTP {}: {}",
                answer.status.as_u16(),
                redact_catalog_text(&body, auth, creds)
            ),
        ));
    }

    serde_json::from_slice::<T>(&answer.body).map_err(|e| {
        CatalogGetError::without_status(format!(
            "failed to parse catalog response: {}",
            redact_catalog_text(&e.to_string(), auth, creds)
        ))
    })
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
    let request = build_signed_request(builder, auth, creds)?;
    let answer = http
        .send_capped(request, MAX_ANSWER_BYTES)
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

/// `body` as an error may show it: redacted, then cut to [`MAX_QUOTED_BODY_BYTES`].
pub(crate) fn quote_catalog_body(
    body: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> String {
    let redacted = redact_catalog_text(body, auth, creds);
    if redacted.len() <= MAX_QUOTED_BODY_BYTES {
        return redacted;
    }
    let cut = redacted.floor_char_boundary(MAX_QUOTED_BODY_BYTES);
    format!(
        "{}... (truncated to {MAX_QUOTED_BODY_BYTES} bytes)",
        &redacted[..cut]
    )
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

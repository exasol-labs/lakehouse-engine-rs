//! The authenticated catalog `GET` and `POST` and the metastore-pointed metadata-file read.
//! Credential values never appear in any returned error.

use crate::auth::{CatalogAuth, redact_catalog_auth_error};
use crate::redaction::{redact_error_text, redact_secret_values};
use crate::{ConnectionCreds, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use iceberg::spec::TableMetadata;
use std::time::Duration;

/// The most of a catalog `POST` answer read. A batch-check answer holds one small result per
/// table, so a larger 2xx answer is refused, and an error quotes only its redacted start.
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

async fn execute_request(
    client: &reqwest::Client,
    request: reqwest::Request,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> Result<reqwest::Response, UdfError> {
    let deadline = request.timeout().copied();
    client.execute(request).await.map_err(|e| {
        UdfError::User(format!(
            "catalog request failed: {}",
            describe_transport_error(&e, deadline, auth, creds)
        ))
    })
}

/// reqwest's own text for an expired deadline names neither the deadline nor the timeout.
fn describe_transport_error(
    error: &reqwest::Error,
    deadline: Option<Duration>,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> String {
    match deadline {
        Some(deadline) if error.is_timeout() => {
            format!("the catalog sent no complete answer within {deadline:?}")
        }
        _ => redact_catalog_text(&error.to_string(), auth, creds),
    }
}

pub(crate) async fn authed_get_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    auth: &CatalogAuth,
    send_access_delegation: bool,
    creds: &ConnectionCreds,
) -> Result<T, UdfError> {
    let mut builder = client.get(url).header("accept", "application/json");
    if send_access_delegation {
        builder = builder.header("X-Iceberg-Access-Delegation", "vended-credentials");
    }
    let request = build_signed_request(builder, auth, creds)?;
    let response = execute_request(client, request, auth, creds).await?;

    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "(unreadable body)".into());
        return Err(UdfError::User(format!(
            "catalog returned HTTP {}: {}",
            status.as_u16(),
            redact_catalog_text(&body, auth, creds)
        )));
    }

    response.json::<T>().await.map_err(|e| {
        UdfError::User(format!(
            "failed to parse catalog response: {}",
            redact_catalog_text(&e.to_string(), auth, creds)
        ))
    })
}

/// How a catalog answered a `POST`. A non-2xx answer is a value, not an error, so a caller
/// can read the status and map it to its own failure.
pub(crate) enum PostAnswer {
    /// The raw body of a 2xx answer. A caller that fails to parse it shows it only through
    /// [`quote_catalog_body`].
    Accepted { status: u16, body: String },
    /// The body of a non-2xx answer, already quoted by [`quote_catalog_body`].
    Refused { status: u16, body: String },
}

/// Sends `payload` and reads the answer within `deadline`, because a catalog that accepts the
/// connection and never answers would otherwise hold the request forever.
pub(crate) async fn authed_post_json(
    client: &reqwest::Client,
    url: &str,
    payload: &impl serde::Serialize,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
    deadline: Duration,
) -> Result<PostAnswer, UdfError> {
    let body = serde_json::to_vec(payload)
        .map_err(|e| UdfError::User(format!("failed to serialize catalog request: {e}")))?;
    let builder = client
        .post(url)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .body(body);
    let mut request = build_signed_request(builder, auth, creds)?;
    // reqwest applies a request's timeout until the last body byte, not only to the headers.
    *request.timeout_mut() = Some(deadline);
    let mut response = execute_request(client, request, auth, creds).await?;

    let status = response.status();
    let code = status.as_u16();
    let answer = read_capped_body(&mut response).await.map_err(|e| {
        UdfError::User(format!(
            "failed to read the catalog's HTTP {code} answer: {}",
            describe_transport_error(&e, Some(deadline), auth, creds)
        ))
    })?;
    match (status.is_success(), answer.cut) {
        (true, false) => Ok(PostAnswer::Accepted {
            status: code,
            body: answer.text,
        }),
        (true, true) => Err(UdfError::User(format!(
            "the catalog's HTTP {code} answer exceeds {MAX_ANSWER_BYTES} bytes: {}",
            quote_catalog_body(&answer.text, auth, creds)
        ))),
        (false, _) => Ok(PostAnswer::Refused {
            status: code,
            body: quote_catalog_body(&answer.text, auth, creds),
        }),
    }
}

/// An answer body read up to [`MAX_ANSWER_BYTES`]; `cut` when the body holds more.
struct CappedBody {
    text: String,
    cut: bool,
}

async fn read_capped_body(response: &mut reqwest::Response) -> Result<CappedBody, reqwest::Error> {
    let mut bytes = Vec::new();
    let mut cut = false;
    while let Some(chunk) = response.chunk().await? {
        let room = MAX_ANSWER_BYTES - bytes.len();
        if chunk.len() > room {
            bytes.extend_from_slice(&chunk[..room]);
            cut = true;
            break;
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(CappedBody {
        text: String::from_utf8_lossy(&bytes).into_owned(),
        cut,
    })
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

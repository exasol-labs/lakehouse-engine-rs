//! The authenticated catalog `GET` and `POST` and the metastore-pointed metadata-file read.
//! Credential values never appear in any returned error.

use crate::auth::{CatalogAuth, redact_catalog_auth_error};
use crate::redaction::{redact_error_text, redact_secret_values};
use crate::{ConnectionCreds, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use iceberg::spec::TableMetadata;

/// The OAuth2 grant-obtained bearer token is not in `creds`, so it is redacted explicitly.
pub(crate) fn redact_catalog_text(
    msg: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> String {
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
    client.execute(request).await.map_err(|e| {
        UdfError::User(format!(
            "catalog request failed: {}",
            redact_catalog_text(&e.to_string(), auth, creds)
        ))
    })
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
    /// The raw body of a 2xx answer. A caller that fails to parse it redacts it itself.
    Accepted { status: u16, body: String },
    /// The body of a non-2xx answer, already redacted.
    Refused { status: u16, body: String },
}

pub(crate) async fn authed_post_json(
    client: &reqwest::Client,
    url: &str,
    payload: &impl serde::Serialize,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> Result<PostAnswer, UdfError> {
    let body = serde_json::to_vec(payload)
        .map_err(|e| UdfError::User(format!("failed to serialize catalog request: {e}")))?;
    let builder = client
        .post(url)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .body(body);
    let request = build_signed_request(builder, auth, creds)?;
    let response = execute_request(client, request, auth, creds).await?;

    let status = response.status();
    let body = response.text().await.map_err(|e| {
        UdfError::User(format!(
            "failed to read the catalog's HTTP {} answer: {}",
            status.as_u16(),
            redact_catalog_text(&e.to_string(), auth, creds)
        ))
    })?;
    if status.is_success() {
        Ok(PostAnswer::Accepted {
            status: status.as_u16(),
            body,
        })
    } else {
        Ok(PostAnswer::Refused {
            status: status.as_u16(),
            body: redact_catalog_text(&body, auth, creds),
        })
    }
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

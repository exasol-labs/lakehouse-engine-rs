//! The authenticated catalog `GET` and the metastore-pointed metadata-file read. Credential
//! values never appear in any returned error.

use crate::auth::{CatalogAuth, redact_catalog_auth_error};
use crate::redaction::{redact_error_text, redact_secret_values};
use crate::{ConnectionCreds, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use iceberg::spec::TableMetadata;

pub(crate) async fn authed_get_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    auth: &CatalogAuth,
    send_access_delegation: bool,
    creds: &ConnectionCreds,
) -> Result<T, UdfError> {
    // The OAuth2 grant-obtained bearer token is not in `creds`, so redact it explicitly.
    let redact = |msg: &str| {
        let base = redact_catalog_auth_error(msg, creds);
        match auth {
            CatalogAuth::Bearer(token) => redact_secret_values(&base, &[token.as_str()]),
            CatalogAuth::Sigv4 { .. } | CatalogAuth::None => base,
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

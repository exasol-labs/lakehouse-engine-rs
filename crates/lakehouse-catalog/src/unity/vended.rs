//! Unity Catalog credential vending: the temporary-table-credentials response and
//! the selector that terminates it in a [`StorageBackend`].
//!
//! `resolve_uc_vended_storage` is a THIRD backend-selection site beside the two
//! the Storage Backend Enum defines, reading a DISJOINT input — the Unity Catalog
//! temporary-table-credentials response, distinct from the Iceberg REST
//! `loadTable` response. It shares only the scheme-to-variant-kind classification
//! with the Iceberg vended selector; each selector constructs its OWN
//! `StorageBackend` variant from its own credential family, so the single-home
//! rule and the probe's requirement that every variant name appear in this
//! selector's own source are both satisfiable. It is admitted and unit-tested but
//! wired into no selector dispatch here — Delta scan execution reaches it in
//! #319/#320.
//!
//! Vended secret values NEVER appear in any returned error or Debug output.

use serde::Deserialize;

use crate::storage::{VendedBackendKind, classify_vended_scheme};
use crate::{AdlsCred, StorageBackend, StorageProps};
use exasol_udf_sdk::error::UdfError;

/// The Unity Catalog temporary-table-credentials response: exactly one credential
/// family keyed by the storage backend.
///
/// Public because [`resolve_uc_vended_storage`] reads it and the scan path
/// (#319/#320) posts for it; its wire fields are the contract the selector reads.
/// Its `Debug` redacts every secret, so it can be logged without leaking.
#[derive(Debug, Clone, Deserialize)]
pub struct TemporaryTableCredentials {
    #[serde(default)]
    pub aws_temp_credentials: Option<AwsTempCredentials>,
    #[serde(default)]
    pub azure_user_delegation_sas: Option<AzureUserDelegationSas>,
    #[serde(default)]
    pub gcp_oauth_token: Option<GcpOauthToken>,
}

/// Vended S3 temporary credentials (dynamic STS on Databricks, static keys on the
/// OSS fixture). `endpoint` is present only when the deployment vends one (an
/// OSS/MinIO object store); absent for AWS STS credentials, whose store is the
/// AWS default.
#[derive(Clone, Deserialize)]
pub struct AwsTempCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
}

/// A vended Azure user-delegation shared-access-signature.
#[derive(Clone, Deserialize)]
pub struct AzureUserDelegationSas {
    pub sas_token: String,
}

/// A vended Google Cloud Storage OAuth token. Modeled so the response shape is
/// complete, but Google Cloud Storage is not a supported `StorageBackend`, so a
/// `gs://` location is rejected by scheme before this is read.
#[derive(Clone, Deserialize)]
pub struct GcpOauthToken {
    #[serde(default)]
    pub oauth_token: String,
}

impl std::fmt::Debug for AwsTempCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsTempCredentials")
            .field("access_key_id", &"[redacted]")
            .field("secret_access_key", &"[redacted]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[redacted]"),
            )
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl std::fmt::Debug for AzureUserDelegationSas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureUserDelegationSas")
            .field("sas_token", &"[redacted]")
            .finish()
    }
}

impl std::fmt::Debug for GcpOauthToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcpOauthToken")
            .field("oauth_token", &"[redacted]")
            .finish()
    }
}

/// Resolve the storage backend a Unity Catalog table's vended credentials
/// describe, from the vended response, the table's storage location, and the
/// operator's `ALLOW_HTTP` consent.
///
/// The backend variant comes from the storage location's URI scheme ALONE —
/// through the one scheme-to-variant-kind home the Iceberg vended selector also
/// uses — and never from a CONNECTION-derived value; its signature carries no
/// `warehouse`, `region`, or existing `StorageBackend`, so the three selectors'
/// input disjointness is enforced by the signature. Vended secret values never
/// appear in the returned error.
pub fn resolve_uc_vended_storage(
    vended: &TemporaryTableCredentials,
    storage_location: &str,
    allow_http: bool,
) -> Result<StorageBackend, UdfError> {
    let scheme = scheme_of(storage_location);
    match classify_vended_scheme(&scheme) {
        Some(VendedBackendKind::S3) => s3_from_vended(vended, storage_location, allow_http),
        Some(VendedBackendKind::Adls) => adls_from_vended(vended, storage_location),
        None => Err(UdfError::User(format!(
            "the Unity Catalog vended table location {storage_location} names no storage backend \
             this engine can read: expected an s3://, s3a://, abfss://, or abfs:// scheme"
        ))),
    }
}

/// The URI scheme of `location`, lowercased per RFC 3986 §3.1, or empty when the
/// location carries none.
fn scheme_of(location: &str) -> String {
    location
        .split_once("://")
        .map_or(String::new(), |(scheme, _)| scheme.to_ascii_lowercase())
}

/// The S3 backend the vended `aws_temp_credentials` describe. No CONNECTION
/// `endpoint`/`region` is read: the endpoint stays empty unless the response vends
/// one, and a vended plaintext `http` endpoint is honored only under `ALLOW_HTTP`,
/// matching the Iceberg vended selector's plaintext consent gate.
fn s3_from_vended(
    vended: &TemporaryTableCredentials,
    location: &str,
    allow_http: bool,
) -> Result<StorageBackend, UdfError> {
    let aws = vended
        .aws_temp_credentials
        .as_ref()
        .filter(|creds| !creds.access_key_id.is_empty() && !creds.secret_access_key.is_empty())
        .ok_or_else(|| {
            UdfError::User(format!(
                "the Unity Catalog returned no usable S3 credential for the table location \
                 {location}: its scheme selects the S3 backend but the temporary-credentials \
                 response carried no aws_temp_credentials"
            ))
        })?;
    let endpoint = aws
        .endpoint
        .as_deref()
        .filter(|endpoint| !endpoint.is_empty());
    if !allow_http
        && let Some(endpoint) = endpoint
        && endpoint
            .split_once("://")
            .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("http"))
    {
        return Err(UdfError::User(format!(
            "the Unity Catalog vended the plaintext endpoint {endpoint} for table location \
             {location}, but the ALLOW_HTTP virtual-schema property is false: a catalog cannot move \
             vended credentials onto plaintext transport without the operator's consent"
        )));
    }
    Ok(StorageBackend::S3(StorageProps {
        endpoint: endpoint.unwrap_or_default().to_string(),
        region: String::new(),
        access_key: aws.access_key_id.clone(),
        secret_key: aws.secret_access_key.clone(),
        session_token: aws.session_token.clone().filter(|token| !token.is_empty()),
        allow_http,
        path_style: endpoint.is_some(),
    }))
}

/// The ADLS backend the vended `azure_user_delegation_sas` describes, with the
/// account name recovered from the storage location's host so the SAS and account
/// stay consistent. No CONNECTION `account_name`/`account_key`/`sas_token` is read.
fn adls_from_vended(
    vended: &TemporaryTableCredentials,
    location: &str,
) -> Result<StorageBackend, UdfError> {
    let sas = vended
        .azure_user_delegation_sas
        .as_ref()
        .map(|sas| sas.sas_token.as_str())
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            UdfError::User(format!(
                "the Unity Catalog returned no usable ADLS credential for the table location \
                 {location}: its scheme selects the ADLS backend but the temporary-credentials \
                 response carried no azure_user_delegation_sas"
            ))
        })?;
    let host = location_host(location);
    let account_name = host
        .split('.')
        .next()
        .filter(|label| !label.is_empty())
        .ok_or_else(|| {
            UdfError::User(format!(
                "the Unity Catalog table location {location} carries no ADLS account name in its \
                 host '{host}': expected the <account> of <account>.dfs.core.windows.net"
            ))
        })?;
    Ok(StorageBackend::Adls {
        account_name: account_name.to_string(),
        cred: AdlsCred::Sas(sas.to_string()),
    })
}

/// The storage host of a table location: its authority segment, read after any
/// `<container>@` userinfo.
fn location_host(location: &str) -> &str {
    let after_scheme = location
        .split_once("://")
        .map_or(location, |(_, rest)| rest);
    let authority = after_scheme
        .split_once('/')
        .map_or(after_scheme, |(authority, _)| authority);
    authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host)
}

#[cfg(test)]
#[path = "vended_tests.rs"]
mod tests;

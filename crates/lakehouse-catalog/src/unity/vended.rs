//! Unity Catalog credential vending: the temporary-table-credentials response and
//! the selector that terminates it in a [`StorageBackend`].
//!
//! `resolve_uc_vended_storage` is a THIRD backend-selection site beside the two
//! the Storage Backend Enum defines, reading a DISJOINT input — the Unity Catalog
//! temporary-table-credentials response, distinct from the Iceberg REST
//! `loadTable` response. This module reads that wire shape ONLY — it reduces the
//! response to the neutral `VendedS3`/SAS values and hands them to the shared
//! policy and construction in `storage`, which is what applies the consent gates
//! and builds the `StorageBackend` variant, so neither selector names a variant
//! itself; the probe instead requires every `VendedBackendKind` to be dispatched
//! from this selector's own source. It is admitted and unit-tested but wired into
//! no selector dispatch here — Delta scan execution reaches it in #319/#320.
//!
//! Vended secret values NEVER appear in any returned error or Debug output.

use serde::Deserialize;

use crate::StorageBackend;
use crate::storage::{
    StaticStoreAddress, VendedBackendKind, VendedS3, adls_backend, classify_vended_scheme,
    s3_backend, scheme_of,
};
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

/// Vended S3 temporary credentials: dynamic STS on Databricks, and — since the
/// fixture harness now mints a real MinIO STS session and injects it — dynamic
/// STS on the OSS fixture too, never a static key. `endpoint` is present only
/// when the deployment vends one (an OSS/MinIO object store); absent for AWS STS
/// credentials, whose store is the AWS default.
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
/// describe, from the vended response, the table's storage location, the
/// operator's `ALLOW_HTTP` consent, and the CONNECTION's store address.
///
/// The backend variant comes from the storage location's URI scheme ALONE —
/// through the one scheme-to-variant-kind home the Iceberg vended selector also
/// uses — and never from a CONNECTION-derived value; its signature carries no
/// `warehouse`, no credential, and no existing `StorageBackend`, so the three
/// selectors' input disjointness is enforced by the signature. Credentials come
/// from the vended response ALONE; only the store address — `endpoint`/`region` —
/// may cross over from the CONNECTION, independently per field, through
/// `address`. Vended secret values never appear in the returned error.
pub fn resolve_uc_vended_storage(
    vended: &TemporaryTableCredentials,
    storage_location: &str,
    allow_http: bool,
    address: &StaticStoreAddress,
) -> Result<StorageBackend, UdfError> {
    let scheme = scheme_of(storage_location);
    match classify_vended_scheme(&scheme) {
        Some(VendedBackendKind::S3) => {
            let vended_s3 = uc_vended_s3(vended, storage_location)?;
            s3_backend(vended_s3, storage_location, allow_http, address)
        }
        Some(VendedBackendKind::Adls) => {
            let sas = uc_vended_sas(vended, storage_location)?;
            adls_backend(sas, storage_location, allow_http)
        }
        None => Err(UdfError::User(format!(
            "the Unity Catalog vended table location {storage_location} names no storage backend \
             this engine can read: expected an s3://, s3a://, abfss://, or abfs:// scheme"
        ))),
    }
}

/// The neutral S3 credential and address values the vended `aws_temp_credentials`
/// describe, before the shared plaintext-transport and address-resolution policy
/// in `storage::s3_backend` runs. Unity's wire shape carries no `region` and no
/// explicit path-style-access field, so both are left unset for the shared
/// derivation to resolve.
fn uc_vended_s3(vended: &TemporaryTableCredentials, location: &str) -> Result<VendedS3, UdfError> {
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
    Ok(VendedS3 {
        access_key: aws.access_key_id.clone(),
        secret_key: aws.secret_access_key.clone(),
        session_token: aws.session_token.clone().filter(|token| !token.is_empty()),
        region: None,
        endpoint: aws.endpoint.clone().filter(|endpoint| !endpoint.is_empty()),
        path_style: None,
    })
}

/// The neutral SAS token the vended `azure_user_delegation_sas` describes, before
/// the shared `abfs://` consent gate and account-name derivation in
/// `storage::adls_backend` run.
fn uc_vended_sas(vended: &TemporaryTableCredentials, location: &str) -> Result<String, UdfError> {
    vended
        .azure_user_delegation_sas
        .as_ref()
        .map(|sas| sas.sas_token.as_str())
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            UdfError::User(format!(
                "the Unity Catalog returned no usable ADLS credential for the table location \
                 {location}: its scheme selects the ADLS backend but the temporary-credentials \
                 response carried no azure_user_delegation_sas"
            ))
        })
}

#[cfg(test)]
#[path = "vended_tests.rs"]
mod tests;

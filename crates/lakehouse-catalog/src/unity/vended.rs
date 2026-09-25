//! Reduces the Unity temporary-table-credentials response to neutral values; the
//! consent gates and `StorageBackend` construction live in `storage`.
//!
//! Vended secret values must never appear in any returned error or Debug output.

use serde::Deserialize;

use crate::StorageBackend;
use crate::storage::{
    StaticStoreAddress, VendedBackendKind, VendedS3, adls_backend, classify_vended_scheme,
    s3_backend, scheme_of,
};
use exasol_udf_sdk::error::UdfError;

/// Carries exactly one credential family. `Debug` redacts every secret.
#[derive(Debug, Clone, Deserialize)]
pub struct TemporaryTableCredentials {
    #[serde(default)]
    pub aws_temp_credentials: Option<AwsTempCredentials>,
    #[serde(default)]
    pub azure_user_delegation_sas: Option<AzureUserDelegationSas>,
    #[serde(default)]
    pub gcp_oauth_token: Option<GcpOauthToken>,
}

/// `endpoint` is vended only by OSS/MinIO deployments; absent means the AWS default.
#[derive(Clone, Deserialize)]
pub struct AwsTempCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
}

#[derive(Clone, Deserialize)]
pub struct AzureUserDelegationSas {
    pub sas_token: String,
}

/// Never read: `gs://` is not a supported backend and is rejected by scheme first.
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

/// The backend variant comes from the location's URI scheme alone and credentials
/// from the vended response alone; only `endpoint`/`region` may fall back to the
/// CONNECTION via `address`, per field.
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

/// Unity's wire shape carries no `region` or path-style field, so both stay unset
/// for `storage::s3_backend` to derive.
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

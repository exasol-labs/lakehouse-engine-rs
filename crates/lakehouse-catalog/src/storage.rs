//! Object-storage backend and credentials. Sole owner of the iceberg storage config
//! keys ([`StorageBackend::catalog_storage_props`]) and of the vended-storage policy
//! both catalog kinds share, so neither can drift. Credential values never appear in
//! a returned error.

use crate::{ConnectionCreds, StorageProps};
use exasol_udf_sdk::error::UdfError;
use iceberg::io::{
    ADLS_ACCOUNT_KEY, ADLS_ACCOUNT_NAME, ADLS_SAS_TOKEN, FileIOBuilder, S3_ACCESS_KEY_ID,
    S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// An enum rather than two `Option`s: `MicrosoftAzureBuilder::build()` silently prefers
/// the account key when both are set.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdlsCred {
    AccountKey(String),
    Sas(String),
}

impl std::fmt::Debug for AdlsCred {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AccountKey(_) => f.debug_tuple("AccountKey").field(&"[redacted]").finish(),
            Self::Sas(_) => f.debug_tuple("Sas").field(&"[redacted]").finish(),
        }
    }
}

/// Externally tagged (`{"s3": {…}}`), never untagged: trial deserialization would
/// resolve a malformed credentials payload to whichever variant happens to parse.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    S3(StorageProps),
    Adls {
        /// Used only by the iceberg `FileIO` manifest-read path; the DataFusion scan
        /// derives the account from each file URI's host instead.
        account_name: String,
        cred: AdlsCred,
    },
}

impl std::fmt::Debug for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::S3(props) => f.debug_tuple("S3").field(props).finish(),
            Self::Adls { account_name, cred } => f
                .debug_struct("Adls")
                .field("account_name", account_name)
                .field("cred", cred)
                .finish(),
        }
    }
}

impl StorageBackend {
    /// Non-empty secret values, for value-based error redaction.
    pub fn secret_values(&self) -> Vec<&str> {
        match self {
            Self::S3(storage) => storage.secret_values(),
            Self::Adls { cred, .. } => {
                let value = match cred {
                    AdlsCred::AccountKey(key) => key.as_str(),
                    AdlsCred::Sas(sas) => sas.as_str(),
                };
                if value.is_empty() {
                    Vec::new()
                } else {
                    vec![value]
                }
            }
        }
    }

    pub(crate) fn catalog_storage_props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        match self {
            Self::S3(storage) => {
                for (key, value) in [
                    (S3_ENDPOINT, &storage.endpoint),
                    (S3_REGION, &storage.region),
                    (S3_ACCESS_KEY_ID, &storage.access_key),
                    (S3_SECRET_ACCESS_KEY, &storage.secret_key),
                ] {
                    if !value.is_empty() {
                        props.insert(key.to_string(), value.clone());
                    }
                }
                if let Some(token) = &storage.session_token {
                    props.insert(S3_SESSION_TOKEN.to_string(), token.clone());
                }
                props.insert(
                    S3_PATH_STYLE_ACCESS.to_string(),
                    storage.path_style.to_string(),
                );
            }
            Self::Adls { account_name, cred } => {
                if !account_name.is_empty() {
                    props.insert(ADLS_ACCOUNT_NAME.to_string(), account_name.clone());
                }
                let (key, value) = match cred {
                    AdlsCred::AccountKey(key) => (ADLS_ACCOUNT_KEY, key),
                    AdlsCred::Sas(sas) => (ADLS_SAS_TOKEN, sas),
                };
                if !value.is_empty() {
                    props.insert(key.to_string(), value.clone());
                }
            }
        }
        props
    }

    pub fn file_io(&self) -> iceberg::io::FileIO {
        let factory = match self {
            Self::S3(_) => OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            },
            Self::Adls { .. } => OpenDalStorageFactory::Azdls,
        };
        FileIOBuilder::new(Arc::new(factory))
            .with_props(self.catalog_storage_props())
            .build()
    }

    /// Azure accepts only `abfss`, never plaintext `abfs`.
    pub fn addresses_scheme(&self, scheme: &str) -> bool {
        match self {
            Self::S3(_) => matches!(scheme, "s3" | "s3a"),
            Self::Adls { .. } => scheme == "abfss",
        }
    }
}

pub(crate) enum VendedBackendKind {
    S3,
    Adls,
}

/// Expects an already-lowercased scheme.
pub(crate) fn classify_vended_scheme(scheme: &str) -> Option<VendedBackendKind> {
    match scheme {
        "s3" | "s3a" => Some(VendedBackendKind::S3),
        "abfs" | "abfss" => Some(VendedBackendKind::Adls),
        _ => None,
    }
}

/// Lowercased per RFC 3986 §3.1; empty when the location carries none.
pub fn scheme_of(location: &str) -> String {
    location
        .split_once("://")
        .map_or(String::new(), |(scheme, _)| scheme.to_ascii_lowercase())
}

/// The authority after any `<container>@` userinfo; for ADLS, the host vended SAS keys
/// are suffixed with.
pub(crate) fn location_host(location: &str) -> &str {
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

/// Never case-folded: iceberg-storage-opendal's `adls.account-name` guard compares it
/// byte-for-byte against the account parsed from each file URI.
fn adls_account_name(location: &str) -> Result<&str, UdfError> {
    let host = location_host(location);
    host.split('.')
        .next()
        .filter(|label| !label.is_empty())
        .ok_or_else(|| {
            UdfError::User(format!(
                "vended credentials were requested for table location {location}, but its storage \
             host '{host}' carries no leading label to read an ADLS account name from: expected \
             the <account> of <account>.dfs.core.windows.net"
            ))
        })
}

/// The CONNECTION addressing a vended resolution may use. Deliberately cannot carry a
/// credential, so static keys never reach the vended path.
#[derive(Debug, Default)]
pub struct StaticStoreAddress {
    endpoint: String,
    region: String,
    path_style: Option<bool>,
}

impl StaticStoreAddress {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn path_style(&self) -> Option<bool> {
        self.path_style
    }
}

impl From<&ConnectionCreds> for StaticStoreAddress {
    fn from(creds: &ConnectionCreds) -> Self {
        Self {
            endpoint: creds.endpoint.clone(),
            region: creds.region.clone(),
            path_style: creds.path_style,
        }
    }
}

/// Carries live credentials, so it deliberately derives no `Debug`.
pub(crate) struct VendedS3 {
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    pub(crate) session_token: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) path_style: Option<bool>,
}

/// The plaintext gate reads the RESOLVED endpoint, since either source can name one.
/// An empty endpoint and region is valid: Databricks vends neither, and AWS's default
/// chain places the store. `path_style` falls back to "an endpoint resolved" because
/// `register_side_store` drops the endpoint when `path_style` is false.
pub(crate) fn s3_backend(
    vended: VendedS3,
    location: &str,
    allow_http: bool,
    address: &StaticStoreAddress,
) -> Result<StorageBackend, UdfError> {
    let endpoint = resolved_address_field(address.endpoint(), vended.endpoint.as_deref());
    let region = resolved_address_field(address.region(), vended.region.as_deref());

    if !allow_http
        && endpoint
            .split_once("://")
            .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("http"))
    {
        return Err(UdfError::User(format!(
            "the plaintext endpoint {endpoint} resolves the store address for table location \
             {location}, but the ALLOW_HTTP virtual-schema property is false: vended credentials \
             cannot move onto plaintext transport without the operator's consent, whether the \
             catalog vended that endpoint or the CONNECTION configured it"
        )));
    }

    let path_style = address
        .path_style()
        .or(vended.path_style)
        .unwrap_or(!endpoint.is_empty());

    Ok(StorageBackend::S3(StorageProps {
        endpoint,
        region,
        access_key: vended.access_key,
        secret_key: vended.secret_key,
        session_token: vended.session_token,
        allow_http,
        path_style,
    }))
}

fn resolved_address_field(connection: &str, vended: Option<&str>) -> String {
    if !connection.is_empty() {
        return connection.to_string();
    }
    vended.unwrap_or_default().to_string()
}

/// The `abfs://` gate lives in the construction so no selector can reach a backend
/// ungated.
pub(crate) fn adls_backend(
    sas: String,
    location: &str,
    allow_http: bool,
) -> Result<StorageBackend, UdfError> {
    if scheme_of(location) == "abfs" && !allow_http {
        return Err(UdfError::User(format!(
            "vended credentials were requested for the plaintext table location {location}, but \
             the ALLOW_HTTP virtual-schema property is false: abfs:// names plaintext transport, \
             and this engine has no plaintext Azure path — it would silently read the location \
             over HTTPS instead, so honouring it requires the operator's explicit ALLOW_HTTP \
             acknowledgement rather than a silent scheme upgrade"
        )));
    }
    Ok(StorageBackend::Adls {
        account_name: adls_account_name(location)?.to_string(),
        cred: AdlsCred::Sas(sas),
    })
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;

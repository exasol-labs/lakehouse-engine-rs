//! Object-storage backend and credentials. Sole owner of how a backend configures an object_store
//! client and of the vended-storage policy both catalog kinds share, so neither can drift.
//! Credential values never appear in a returned error.

use crate::dns::{adls_store_builder, caching_client_options, s3_store_builder};
use crate::object_storage::object_file_io;
use crate::{ConnectionCreds, StorageProps};
use exasol_udf_sdk::error::UdfError;
use iceberg::io::FileIO;
use object_store::aws::AmazonS3Builder;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::{ClientOptions, CredentialProvider, ObjectStore};
use serde::{Deserialize, Serialize};
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
        /// Used only by the iceberg `FileIO`, which refuses a path of another account; the
        /// DataFusion scan derives the account from each file URI's host instead.
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

    /// The FileIO that signs with the stored credentials. An ADLS FileIO addresses only the
    /// stored account, so a path of another account never receives its credential.
    pub fn file_io(&self) -> FileIO {
        let backend = self.clone();
        object_file_io(Arc::new(move |root: &str| match &backend {
            Self::S3(storage) => {
                let builder = s3_builder(storage, location_host(root), caching_client_options());
                Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
            }
            Self::Adls { account_name, cred } => {
                let account = location_host(root).split('.').next().unwrap_or_default();
                if account != account_name {
                    return Err(object_store::Error::Generic {
                        store: "MicrosoftAzure",
                        source: format!(
                            "the storage account {account} of {root} is not the account \
                             {account_name} the credential belongs to"
                        )
                        .into(),
                    });
                }
                let builder = adls_builder(root, cred, caching_client_options());
                Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
            }
        }))
    }

    /// [`Self::file_io`] whose S3 requests sign with the credential `credentials` gives for each
    /// request, so a writer's credentials can be renewed while it runs. Fails on ADLS storage.
    pub fn file_io_signed_by(
        &self,
        credentials: Arc<dyn CredentialProvider<Credential = object_store::aws::AwsCredential>>,
    ) -> Result<FileIO, UdfError> {
        let Self::S3(storage) = self else {
            return Err(UdfError::User(
                "an AWS credential source signs only S3 storage requests, but the storage is ADLS"
                    .into(),
            ));
        };
        let storage = storage.clone();
        Ok(object_file_io(Arc::new(move |root: &str| {
            let builder = s3_address(&storage, location_host(root), caching_client_options())
                .with_credentials(credentials.clone());
            Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
        })))
    }

    /// Azure accepts only `abfss`, never plaintext `abfs`.
    pub fn addresses_scheme(&self, scheme: &str) -> bool {
        match self {
            Self::S3(_) => matches!(scheme, "s3" | "s3a"),
            Self::Adls { .. } => scheme == "abfss",
        }
    }
}

/// The S3 client builder of `bucket`, signing with the stored keys, or unsigned when none are
/// stored. `options` replace the builder's whole `ClientOptions`.
pub fn s3_builder(storage: &StorageProps, bucket: &str, options: ClientOptions) -> AmazonS3Builder {
    let builder = s3_address(storage, bucket, options);
    // No credentials reads a public bucket unsigned (#590): an empty key is still signed and
    // rejected, and unset keys make object_store fall back to ambient instance credentials.
    if storage.access_key.is_empty()
        && storage.secret_key.is_empty()
        && storage.session_token.is_none()
    {
        return builder.with_skip_signature(true);
    }
    let builder = builder
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key);
    match &storage.session_token {
        Some(token) => builder.with_token(token),
        None => builder,
    }
}

fn s3_address(storage: &StorageProps, bucket: &str, options: ClientOptions) -> AmazonS3Builder {
    // `with_client_options` REPLACES the whole `ClientOptions`, so it must precede
    // `with_allow_http`; reversed, `allow_http` is silently dropped (breaking SeaweedFS).
    let builder = s3_store_builder()
        .with_bucket_name(bucket)
        .with_region(&storage.region)
        .with_client_options(options)
        .with_allow_http(storage.allow_http);
    // Real AWS S3 must NOT get an endpoint: object_store derives the virtual-hosted URL
    // from the region, and a regional endpoint without the bucket yields 403.
    if storage.path_style {
        builder
            .with_endpoint(&storage.endpoint)
            .with_virtual_hosted_style_request(false)
    } else {
        builder
    }
}

/// The ADLS Gen2 client builder of the container `url` names, signing with `cred`. `options`
/// replace the builder's whole `ClientOptions`.
pub fn adls_builder(url: &str, cred: &AdlsCred, options: ClientOptions) -> MicrosoftAzureBuilder {
    let builder = adls_store_builder()
        .with_url(url)
        .with_client_options(options);
    match cred {
        AdlsCred::AccountKey(key) => builder.with_access_key(key),
        AdlsCred::Sas(sas) => builder.with_config(AzureConfigKey::SasKey, sas),
    }
}

/// The backend a vended table location selects by its scheme.
pub enum VendedBackendKind {
    S3,
    Adls,
}

/// Expects an already-lowercased scheme.
pub fn classify_vended_scheme(scheme: &str) -> Option<VendedBackendKind> {
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

/// Never case-folded: [`StorageBackend::file_io`] compares it byte-for-byte against the account
/// of each path.
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

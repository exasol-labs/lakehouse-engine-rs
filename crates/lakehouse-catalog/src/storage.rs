//! Which object-storage backend a scan reads its data files through, and the
//! credentials for it.
//!
//! [`StorageBackend`] is declared here, in the crate that PRODUCES storage
//! credentials — a `loadTable` response vends them — so one type backs one serde
//! wire contract for every consumer downstream.
//!
//! This module is also the single owner of the mapping from a backend's
//! credentials to iceberg's storage config keys: [`StorageBackend::catalog_storage_props`]
//! is the one place those keys are named, and both the REST-catalog props map and
//! the `FileIO` are configured from it, so neither can drift from the other.
//!
//! It is, third, the single home for vended-storage POLICY and CONSTRUCTION: the
//! `abfs://` and plaintext-endpoint consent gates, the CONNECTION-wins
//! store-address rule, and the two `StorageBackend` constructions both catalog
//! kinds share. It lives here because this enum's own module already owns which
//! module may name a variant, so the Iceberg and Unity Catalog vended selectors
//! fork only on how a value is read off the wire, never on what makes it
//! acceptable.
//!
//! Credential values NEVER appear in any returned error;
//! [`StorageBackend::secret_values`] is what the redaction sites strip against.

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

/// Exactly one Azure ADLS Gen2 credential — a shared account key or an inline
/// shared-access-signature (SAS) token, never both and never neither.
///
/// A two-`Option` shape would let both be set at once, and `object_store`'s
/// `MicrosoftAzureBuilder::build()` silently prefers the access key over the SAS
/// when both are present — resolving that contradiction without telling anyone.
/// This enum makes the ambiguous shape unrepresentable instead: `validate_creds`
/// is then the one place a caller supplying both is told so, rather than the
/// object-store builder picking silently.
///
/// `snake_case` variant keys match the outer [`StorageBackend`]'s lowercase wire
/// convention and the `account_key`/`sas_token` vocabulary already used by the
/// CONNECTION string and `adls.*` iceberg config keys.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdlsCred {
    /// A shared storage-account key.
    AccountKey(String),
    /// An inline shared-access-signature token.
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

/// The object-storage backend a scan reads its data files through, carrying that
/// backend's connection properties and credentials.
///
/// Externally tagged (serde's default) with a lowercase variant key, so an S3
/// backend is `{"s3": {…}}` on the wire. Externally tagged rather than untagged
/// because this is a credentials path: untagged selects a variant by trial
/// deserialization, which resolves a malformed or ambiguous payload to whichever
/// variant happens to parse instead of rejecting it.
///
/// Wrapping [`StorageProps`] rather than inlining its fields keeps that struct —
/// its `Default`, its `secret_values`, and its serde field contract — the single
/// S3 credential type, so an added backend is a new variant beside it rather than
/// an edit to it. `Adls` has no equivalent pre-existing struct to protect, so it
/// carries its two fields inline instead of wrapping a symmetry-only type.
///
/// `Debug` is a manual impl below, not derived: it delegates to each variant's
/// own redacting `Debug` (`StorageProps`'s and `AdlsCred`'s), so this enum
/// never needs to know which of its fields are secret itself.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// S3-compatible object storage (AWS S3, MinIO).
    S3(StorageProps),
    /// Azure Data Lake Storage Gen2, reached via `abfss://`.
    Adls {
        /// Configures the iceberg `FileIO` manifest-read path via
        /// [`Self::catalog_storage_props`] — an `account_name` that disagrees
        /// with the credential's actual account surfaces as an auth failure
        /// there, at plan time (manifest read), before any scan runs. The
        /// DataFusion scan path does NOT read this field — it derives the
        /// account from the host of the side's own file URIs via
        /// `MicrosoftAzureBuilder::with_url`
        /// (`crates/lakehouse-engine/src/scan/object_store.rs`) instead.
        account_name: String,
        /// Exactly one Azure credential for that account.
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
    /// The non-empty secret values this backend's credentials contain.
    ///
    /// Used for value-based error redaction: any error string containing one of
    /// these literal values has it stripped before the error is surfaced.
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

    /// This backend's iceberg storage config keys, as the props map both the REST
    /// catalog `load` call and [`Self::file_io`] are configured from.
    ///
    /// Crate-private because naming an iceberg config key is this module's
    /// decision alone: a caller outside the crate holding the map could re-derive
    /// the same keys and drift from it.
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

    /// A `FileIO` the iceberg `Table` reads manifest files through, configured
    /// from this backend's credentials.
    ///
    /// Used by the signed path to give the `Table` a way to read manifest files
    /// after we have fetched and deserialized the `LoadTableResult`.
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
}

/// The storage-backend KIND a vended table location's URI scheme selects — the
/// single scheme-to-variant-kind classification both vended selectors share. It
/// names ONLY which kind the scheme selects and constructs no [`StorageBackend`],
/// so each vended selector builds its own variant from its own credential family.
pub(crate) enum VendedBackendKind {
    S3,
    Adls,
}

/// Classify a vended table location's (already-lowercased) URI scheme into the
/// storage-backend kind it selects, or `None` when the scheme names no supported
/// backend: `s3`/`s3a` select S3 and `abfs`/`abfss` select ADLS, in this one home
/// rather than duplicated across the vended selectors.
pub(crate) fn classify_vended_scheme(scheme: &str) -> Option<VendedBackendKind> {
    match scheme {
        "s3" | "s3a" => Some(VendedBackendKind::S3),
        "abfs" | "abfss" => Some(VendedBackendKind::Adls),
        _ => None,
    }
}

/// The URI scheme of a vended table location, lowercased per RFC 3986 §3.1, or
/// empty when the location carries none.
pub(crate) fn scheme_of(location: &str) -> String {
    location
        .split_once("://")
        .map_or(String::new(), |(scheme, _)| scheme.to_ascii_lowercase())
}

/// The storage host of a table location: its authority segment, read after any
/// `<container>@` userinfo. For ADLS that is the `<account>.dfs.core.windows.net`
/// the vended SAS keys are suffixed with, so one reading serves both the SAS
/// selection and the account name.
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

/// The ADLS account name the storage host of `location` names: the first
/// dot-separated label of [`location_host`]'s result for it, the `<account>` of
/// `<account>.dfs.core.windows.net`. The host is derived here rather than taken
/// as a second argument, so it cannot be paired with a location it was not read
/// from — a refusal always names a host the location actually has. Both vended
/// selectors read the account name from the same host their SAS selection
/// matched, so a disagreeing account name can never desynchronise from the SAS
/// it travels with, and share this one refusal text, which names neither catalog
/// kind.
///
/// The label is read from the host byte-exactly, never case-folded: the
/// downstream `adls.account-name` wrong-account guard compares it byte-for-byte
/// against the account parsed out of each file URI
/// (`iceberg-storage-opendal-0.10.0/src/azdls.rs:165`), so case-folding it here
/// would fire that guard on the very locations it was derived from.
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

/// The store address a vended resolution may take from the CONNECTION: an S3
/// endpoint and a region, and nothing else.
///
/// Under vending the catalog's response is the sole source of CREDENTIALS, while
/// ADDRESSING may still come from the CONNECTION. Handing the selectors
/// `&ConnectionCreds` to express that would put every static credential back
/// within their reach, so the parameter is narrowed to a type that CANNOT carry
/// one. Both fields are private, which leaves [`Default`] and the single
/// [`From<&ConnectionCreds>`] conversion below as the only constructions
/// reachable outside this module — widening what crosses over is then an edit to
/// that one conversion rather than a field a distant call site can set.
#[derive(Debug, Default)]
pub struct StaticStoreAddress {
    endpoint: String,
    region: String,
}

impl StaticStoreAddress {
    /// The CONNECTION's S3 endpoint, empty when it configured none.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The CONNECTION's S3 region, empty when it configured none.
    pub fn region(&self) -> &str {
        &self.region
    }
}

impl From<&ConnectionCreds> for StaticStoreAddress {
    fn from(creds: &ConnectionCreds) -> Self {
        Self {
            endpoint: creds.endpoint.clone(),
            region: creds.region.clone(),
        }
    }
}

/// The S3 credential and address values a vended response yields, in the neutral
/// shape both catalog kinds reduce to before the shared policy runs.
///
/// The two wire shapes genuinely differ — a flat map of Iceberg REST config keys,
/// and Unity Catalog's typed `aws_temp_credentials` — but what makes the
/// resulting values acceptable does not, so the fork stops at this type.
/// `path_style` is an `Option` because "the response stated no
/// `s3.path-style-access`" is a third state the derivation branches on, distinct
/// from a stated `false`. Carries live credentials, so it deliberately derives no
/// `Debug`.
pub(crate) struct VendedS3 {
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    pub(crate) session_token: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) path_style: Option<bool>,
}

/// The S3 backend a vended credential family describes for `location`, addressed
/// by the CONNECTION-wins rule and refused when the resolved endpoint names
/// plaintext transport the operator has not consented to.
///
/// Credentials come from `vended` ALONE; only the ADDRESS may cross over from the
/// CONNECTION, independently per field. The gate reads the RESOLVED endpoint
/// rather than the vended one because either source can name plaintext transport,
/// and the one that wins the address rule is the one the scan actually reads
/// through.
///
/// An address that resolves both fields empty is a SUCCESS, not a refusal: AWS's
/// default credential and region chain places the store from the ambient
/// environment at read time, and a real Databricks AWS response vends a key pair
/// with no endpoint and no region at all — rejecting it at plan time would refuse
/// a legal table.
///
/// `path_style` falls back to whether an endpoint resolved at all, because
/// `register_side_store` treats it as the gate on whether `endpoint` reaches
/// `AmazonS3Builder` — an endpoint beside `path_style: false` would be silently
/// dropped for a virtual-hosted host derived from the region, which is the wrong
/// store rather than a plan-time error. A value the response states still wins.
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

    let path_style = vended.path_style.unwrap_or(!endpoint.is_empty());

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

/// The ADLS backend a vended SAS describes for `location`, refused when the
/// location names plaintext transport the operator has not consented to.
///
/// The gate lives inside the construction rather than at the scheme
/// classification so that reaching a backend is what enforces it: a selector
/// that classifies and then constructs cannot end up ungated by omitting a match
/// arm, which is exactly how the two selectors drifted apart.
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

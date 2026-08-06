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
//! Credential values NEVER appear in any returned error;
//! [`StorageBackend::secret_values`] is what the redaction sites strip against.

use crate::StorageProps;
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;

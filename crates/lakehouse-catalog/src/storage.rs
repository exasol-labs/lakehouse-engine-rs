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
    FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
    S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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
/// an edit to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// S3-compatible object storage (AWS S3, MinIO).
    S3(StorageProps),
}

impl StorageBackend {
    /// The non-empty secret values this backend's credentials contain.
    ///
    /// Used for value-based error redaction: any error string containing one of
    /// these literal values has it stripped before the error is surfaced.
    pub fn secret_values(&self) -> Vec<&str> {
        match self {
            Self::S3(storage) => storage.secret_values(),
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
        };
        FileIOBuilder::new(Arc::new(factory))
            .with_props(self.catalog_storage_props())
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENDPOINT: &str = "http://minio.local:9000";
    const REGION: &str = "us-east-1";
    const ACCESS_KEY: &str = "AKIDEXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI_EXAMPLE_KEY";
    const SESSION_TOKEN: &str = "FwoGZXIvYXdzEXAMPLE_TOKEN";

    /// Every connection field populated, a session token present, and
    /// `allow_http` deliberately ON — the latter is NOT an iceberg storage config
    /// key, so an exact map comparison against this fixture pins its absence.
    fn populated_backend() -> StorageBackend {
        StorageBackend::S3(StorageProps {
            endpoint: ENDPOINT.into(),
            region: REGION.into(),
            access_key: ACCESS_KEY.into(),
            secret_key: SECRET_KEY.into(),
            session_token: Some(SESSION_TOKEN.into()),
            allow_http: true,
            path_style: true,
        })
    }

    /// The exact six-key iceberg config map [`populated_backend`] must produce.
    fn expected_populated_config() -> HashMap<String, String> {
        HashMap::from([
            (S3_ENDPOINT.to_string(), ENDPOINT.to_string()),
            (S3_REGION.to_string(), REGION.to_string()),
            (S3_ACCESS_KEY_ID.to_string(), ACCESS_KEY.to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), SECRET_KEY.to_string()),
            (S3_SESSION_TOKEN.to_string(), SESSION_TOKEN.to_string()),
            (S3_PATH_STYLE_ACCESS.to_string(), "true".to_string()),
        ])
    }

    #[test]
    fn catalog_storage_props_emits_every_populated_s3_key_and_nothing_else() {
        assert_eq!(
            populated_backend().catalog_storage_props(),
            expected_populated_config()
        );
    }

    #[test]
    fn catalog_storage_props_omits_empty_connection_fields_and_an_absent_token() {
        let backend = StorageBackend::S3(StorageProps::default());

        assert_eq!(
            backend.catalog_storage_props(),
            HashMap::from([(S3_PATH_STYLE_ACCESS.to_string(), "true".to_string())]),
            "only path-style access is unconditional; every empty credential field \
             and the absent session token must be left out"
        );
    }

    /// A `Some("")` session token is gated on presence, NOT on being non-empty —
    /// unlike the four connection fields. Preserved verbatim from the pre-refactor
    /// `if let Some(token)` so the props map stays byte-identical.
    #[test]
    fn catalog_storage_props_emits_a_present_but_empty_session_token() {
        let backend = StorageBackend::S3(StorageProps {
            session_token: Some(String::new()),
            path_style: false,
            ..StorageProps::default()
        });

        assert_eq!(
            backend.catalog_storage_props(),
            HashMap::from([
                (S3_SESSION_TOKEN.to_string(), String::new()),
                (S3_PATH_STYLE_ACCESS.to_string(), "false".to_string()),
            ])
        );
    }

    #[test]
    fn file_io_is_configured_from_exactly_the_catalog_storage_props() {
        assert_eq!(
            populated_backend().file_io().config().props(),
            &expected_populated_config()
        );
    }

    #[test]
    fn secret_values_are_the_wrapped_props_secret_values() {
        let props = StorageProps {
            access_key: ACCESS_KEY.into(),
            secret_key: SECRET_KEY.into(),
            session_token: Some(SESSION_TOKEN.into()),
            ..StorageProps::default()
        };

        assert_eq!(
            StorageBackend::S3(props.clone()).secret_values(),
            props.secret_values()
        );
    }

    #[test]
    fn s3_serializes_under_a_lowercase_externally_tagged_variant_key() {
        assert_eq!(
            serde_json::to_value(populated_backend()).expect("backend serializes"),
            serde_json::json!({
                "s3": {
                    "endpoint": ENDPOINT,
                    "region": REGION,
                    "access_key": ACCESS_KEY,
                    "secret_key": SECRET_KEY,
                    "session_token": SESSION_TOKEN,
                    "allow_http": true,
                    "path_style": true,
                }
            })
        );
    }

    #[test]
    fn s3_round_trips_through_its_tagged_encoding() {
        let backend = populated_backend();
        let encoded = serde_json::to_string(&backend).expect("backend serializes");

        assert_eq!(
            serde_json::from_str::<StorageBackend>(&encoded).expect("backend deserializes"),
            backend
        );
    }

    /// The externally-tagged decision, asserted from the decode side: a bare
    /// (untagged) props object and an unknown variant key must both be rejected
    /// rather than resolved by trial deserialization.
    #[test]
    fn only_the_lowercase_s3_variant_key_decodes() {
        for payload in [
            r#"{"endpoint":"","region":"","access_key":"","secret_key":""}"#,
            r#"{"S3":{"endpoint":"","region":"","access_key":"","secret_key":""}}"#,
            r#"{"azure":{"endpoint":"","region":"","access_key":"","secret_key":""}}"#,
        ] {
            assert!(
                serde_json::from_str::<StorageBackend>(payload).is_err(),
                "payload must not decode to a storage backend: {payload}"
            );
        }
    }
}

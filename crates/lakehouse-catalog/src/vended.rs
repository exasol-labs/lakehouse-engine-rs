//! Iceberg REST vended-storage resolution: derive a table's effective scan
//! storage from what its `loadTable` response vends for that table's own
//! location, taking no CONNECTION-supplied credential — only the store address
//! may cross over.
//!
//! [`resolve_vended_storage`] is the whole public surface; everything below it is a
//! private step. This module reads the Iceberg REST wire shape and nothing else:
//! what makes the values it reads acceptable is the shared policy in `storage`.

use crate::StorageBackend;
use crate::storage::{
    StaticStoreAddress, VendedBackendKind, VendedS3, adls_backend, classify_vended_scheme,
    location_host, s3_backend, scheme_of,
};
use exasol_udf_sdk::error::UdfError;
use std::borrow::Cow;
use std::collections::HashMap;

/// Vended ADLS SAS keys are host-suffixed (`adls.sas-token.<host>`) — the iceberg Java
/// `AzureProperties` convention, not a key the Iceberg REST spec enumerates. The reader
/// downstream understands only the flat `adls.sas-token`, so the suffix is recovered
/// here and travels no further.
const VENDED_SAS_TOKEN_KEY_PREFIX: &str = "adls.sas-token.";

/// The effective scan storage for the table `anchor` locates, built from what
/// `result` vends for that location.
///
/// `anchor` must be the table's OWN location — the same value
/// `storage_credentials[*].prefix` is matched against — so that a catalog URI is
/// refused as an unsupported scheme rather than silently falling through to the
/// flat `config` map.
///
/// CREDENTIALS come from the response alone — `address` is narrowed to a type that
/// cannot carry one, so no static key can reach this path. ADDRESSING is the one
/// thing that may cross over: an `endpoint` or `region` the CONNECTION configures
/// wins over the vended value, independently per field, and an address neither side
/// supplies is left for AWS's own default chain to place.
pub fn resolve_vended_storage(
    result: &iceberg_catalog_rest::LoadTableResult,
    anchor: &str,
    allow_http: bool,
    address: &StaticStoreAddress,
) -> Result<StorageBackend, UdfError> {
    let scheme = scheme_of(anchor);
    let vended = select_credential_source(result, anchor);
    match classify_vended_scheme(&scheme) {
        Some(VendedBackendKind::S3) => s3_backend(
            iceberg_vended_s3(vended, anchor)?,
            anchor,
            allow_http,
            address,
        ),
        Some(VendedBackendKind::Adls) => {
            adls_backend(iceberg_vended_sas(vended, anchor)?, anchor, allow_http)
        }
        None => Err(UdfError::User(format!(
            "vended credentials were requested, but the table location {anchor} names no storage \
             backend this engine can read: expected an s3://, s3a://, abfss://, or abfs:// scheme"
        ))),
    }
}

/// The one credential source that applies to `location`, per the Iceberg REST
/// spec: the `storage_credentials` entry whose non-empty `prefix` is the longest
/// prefix of `location`, else the flat `config` map.
///
/// Selecting once and reading all six vended values from the result is what makes
/// a matched entry authoritative for the whole credential set: a key the entry
/// omits reads as absent rather than falling back to the flat map, so the two
/// sources never mix.
///
/// Both sides are compared with the SCHEME lowercased, because
/// [`resolve_vended_storage`] accepts a case-variant anchor scheme (RFC 3986 §3.1) —
/// so a response spelling `location` and a `prefix` scheme differently would
/// otherwise miss the entry and silently read the flat map instead.
fn select_credential_source<'a>(
    result: &'a iceberg_catalog_rest::LoadTableResult,
    location: &str,
) -> &'a HashMap<String, String> {
    let location = lowercase_scheme(location);
    result
        .storage_credentials
        .as_ref()
        .and_then(|credentials| {
            credentials
                .iter()
                .filter(|entry| {
                    !entry.prefix.is_empty()
                        && location.starts_with(lowercase_scheme(&entry.prefix).as_ref())
                })
                .max_by_key(|entry| entry.prefix.len())
        })
        .map_or(&result.config, |entry| &entry.config)
}

/// `uri` with its URI scheme lowercased and everything after `://` verbatim. Only the
/// scheme is folded: RFC 3986 §3.1 makes it case-insensitive, while a bucket, container,
/// or object key is case-sensitive — two S3 buckets differing only in case are two
/// buckets.
fn lowercase_scheme(uri: &str) -> Cow<'_, str> {
    match uri.split_once("://") {
        Some((scheme, rest)) if scheme.bytes().any(|byte| byte.is_ascii_uppercase()) => {
            Cow::Owned(format!("{}://{rest}", scheme.to_ascii_lowercase()))
        }
        _ => Cow::Borrowed(uri),
    }
}

/// The neutral S3 values the selected credential source vends.
///
/// The key pair is required: with no static payload underneath, a source that omits
/// it satisfies nothing. Every ADDRESSING value is optional here, because what
/// places the store is the shared address rule rather than this response alone.
fn iceberg_vended_s3(vended: &HashMap<String, String>, anchor: &str) -> Result<VendedS3, UdfError> {
    Ok(VendedS3 {
        access_key: required_vended_value(vended, "s3.access-key-id", anchor)?,
        secret_key: required_vended_value(vended, "s3.secret-access-key", anchor)?,
        session_token: vended_config_value(vended, "s3.session-token"),
        region: vended_config_value(vended, "client.region"),
        endpoint: vended_config_value(vended, "s3.endpoint"),
        path_style: vended_config_value(vended, "s3.path-style-access")
            .and_then(|value| value.parse::<bool>().ok()),
    })
}

/// A vended value the resolved backend cannot be built without, reported when the
/// selected source omits it or spells it empty rather than substituted from static
/// config.
fn required_vended_value(
    vended: &HashMap<String, String>,
    key: &str,
    anchor: &str,
) -> Result<String, UdfError> {
    vended_config_value(vended, key).ok_or_else(|| {
        UdfError::User(format!(
            "vended credentials were requested for table location {anchor}, but the catalog \
             returned none: the selected credential source carries no non-empty {key} value"
        ))
    })
}

/// The vended SAS for the anchor's own storage host, read from the host-suffixed key
/// the catalog minted it under, matched case-insensitively per RFC 3986 §3.2.2 and
/// broken toward the smallest key when only case-variant spellings are vended. A SAS
/// is account-scoped, so one minted for another host is as unusable as none.
fn iceberg_vended_sas(vended: &HashMap<String, String>, anchor: &str) -> Result<String, UdfError> {
    let host = location_host(anchor);
    vended_config_value(vended, &format!("{VENDED_SAS_TOKEN_KEY_PREFIX}{host}"))
        .or_else(|| {
            vended
                .iter()
                .filter(|(key, value)| {
                    !value.is_empty()
                        && key
                            .strip_prefix(VENDED_SAS_TOKEN_KEY_PREFIX)
                            .is_some_and(|key_host| key_host.eq_ignore_ascii_case(host))
                })
                .min_by(|(left, _), (right, _)| left.cmp(right))
                .map(|(_, value)| value.clone())
        })
        .ok_or_else(|| {
            UdfError::User(format!(
                "vended credentials were requested for storage host {host} (table location \
                 {anchor}), but the catalog returned none: the selected credential source carries \
                 no non-empty {VENDED_SAS_TOKEN_KEY_PREFIX}{host} key in any casing of that host"
            ))
        })
}

fn vended_config_value(vended: &HashMap<String, String>, key: &str) -> Option<String> {
    vended.get(key).filter(|s| !s.is_empty()).cloned()
}

#[cfg(test)]
#[path = "vended_tests.rs"]
mod tests;

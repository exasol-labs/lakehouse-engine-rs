//! Iceberg REST vended-storage resolution. Only the store address may cross over from
//! the CONNECTION, never a credential; acceptance policy lives in `storage`.

use crate::StorageBackend;
use crate::storage::{
    StaticStoreAddress, VendedBackendKind, VendedS3, adls_backend, classify_vended_scheme,
    location_host, s3_backend, scheme_of,
};
use exasol_udf_sdk::error::UdfError;
use std::borrow::Cow;
use std::collections::HashMap;

/// Host-suffixed per the iceberg Java `AzureProperties` convention, not the Iceberg REST
/// spec; downstream readers only understand the flat `adls.sas-token`.
const VENDED_SAS_TOKEN_KEY_PREFIX: &str = "adls.sas-token.";

/// `anchor` must be the table's OWN location (what `storage_credentials[*].prefix` is
/// matched against), so a catalog URI is refused rather than falling through to the
/// flat `config` map. Credentials come from the response alone; a CONNECTION
/// `endpoint`/`region` wins per field, and an address neither side supplies is left to
/// AWS's default chain.
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

/// Per the Iceberg REST spec: the longest non-empty matching `prefix` entry, else the
/// flat `config` map. A matched entry is authoritative for the whole set; the two
/// sources never mix. Schemes are compared lowercased (RFC 3986 §3.1).
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

/// Only the scheme is folded (RFC 3986 §3.1); buckets and keys are case-sensitive.
fn lowercase_scheme(uri: &str) -> Cow<'_, str> {
    match uri.split_once("://") {
        Some((scheme, rest)) if scheme.bytes().any(|byte| byte.is_ascii_uppercase()) => {
            Cow::Owned(format!("{}://{rest}", scheme.to_ascii_lowercase()))
        }
        _ => Cow::Borrowed(uri),
    }
}

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

/// Host matched case-insensitively (RFC 3986 §3.2.2), ties broken toward the smallest
/// key. A SAS is account-scoped, so one for another host is as unusable as none.
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

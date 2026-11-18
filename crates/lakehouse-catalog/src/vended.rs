//! Iceberg REST vended-storage resolution. Only the store address may cross over from
//! the CONNECTION, never a credential; acceptance policy lives in `storage`.

use crate::StorageBackend;
use crate::storage::{
    StaticStoreAddress, VendedBackendKind, VendedS3, adls_backend, classify_vended_scheme,
    location_host, s3_backend, scheme_of,
};
use exasol_udf_sdk::error::UdfError;
use iceberg_catalog_rest::{LoadTableResult, StorageCredential};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

/// Host-suffixed per the iceberg Java `AzureProperties` convention, not the Iceberg REST
/// spec; downstream readers only understand the flat `adls.sas-token`.
const VENDED_SAS_TOKEN_KEY_PREFIX: &str = "adls.sas-token.";
/// Host-suffixed like the SAS key it states the expiry of.
const VENDED_SAS_EXPIRES_AT_MS_KEY_PREFIX: &str = "adls.sas-token-expires-at-ms.";
const S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
const S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
const S3_SESSION_TOKEN: &str = "s3.session-token";
const S3_ENDPOINT: &str = "s3.endpoint";
const S3_EXPIRES_AT_MS: &str = "s3.session-token-expires-at-ms";

/// The S3 credentials a catalog vends for one table location, for a writer that renews them
/// before they expire. The vended config keys stay known to this crate alone, and `Debug` shows
/// no value.
pub struct VendedS3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    /// The storage endpoint the catalog vends with the credentials, if any.
    pub endpoint: Option<String>,
    /// When the credentials expire, in epoch milliseconds; `None` when the catalog states none.
    pub expires_at_ms: Option<u64>,
}

impl fmt::Debug for VendedS3Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VendedS3Credentials")
            .finish_non_exhaustive()
    }
}

/// The ADLS SAS a catalog vends for one table location's host, for a writer that renews it before
/// it expires. `Debug` shows no value.
pub struct VendedAdlsSas {
    pub sas: String,
    /// When the SAS expires, in epoch milliseconds; `None` when the catalog states none.
    pub expires_at_ms: Option<u64>,
}

impl fmt::Debug for VendedAdlsSas {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VendedAdlsSas").finish_non_exhaustive()
    }
}

/// The vended credentials that apply to one table location, as a `loadTable` or `loadCredentials`
/// answer selects them. It carries credentials, so `Debug` shows none.
pub struct VendedCredentialSet(pub(crate) HashMap<String, String>);

impl VendedCredentialSet {
    /// The set `answer` vends for `location`, from the same credential source that
    /// [`resolve_vended_storage`] selects.
    pub fn of(answer: &LoadTableResult, location: &str) -> Self {
        Self(vended_credential_config(answer, location).clone())
    }

    /// The set read as S3 keys. A refusal names the missing or unreadable key, never a value.
    pub fn s3(&self, location: &str) -> Result<VendedS3Credentials, UdfError> {
        s3_credentials(&self.0, location)
    }

    /// The set read as the SAS for `location`'s host and its expiry, with the host match of
    /// [`resolve_vended_storage`]. A refusal names the missing or unreadable key, never a value.
    pub fn adls_sas(&self, location: &str) -> Result<VendedAdlsSas, UdfError> {
        adls_sas_credentials(&self.0, location)
    }
}

impl fmt::Debug for VendedCredentialSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VendedCredentialSet")
            .finish_non_exhaustive()
    }
}

/// `anchor` must be the table's OWN location (what `storage_credentials[*].prefix` is
/// matched against), so a catalog URI is refused rather than falling through to the
/// flat `config` map. Credentials come from the response alone; a CONNECTION
/// `endpoint`/`region` wins per field, and an address neither side supplies is left to
/// AWS's default chain.
pub fn resolve_vended_storage(
    result: &LoadTableResult,
    anchor: &str,
    allow_http: bool,
    address: &StaticStoreAddress,
) -> Result<StorageBackend, UdfError> {
    let scheme = scheme_of(anchor);
    let vended = vended_credential_config(result, anchor);
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

/// The vended storage config that applies to `location`, per the Iceberg REST spec: the covering
/// `storage-credentials` entry, else the flat `config` map. A matched entry is authoritative for
/// the whole set; the two sources never mix. It carries credentials, so never quote it.
pub(crate) fn vended_credential_config<'a>(
    result: &'a LoadTableResult,
    location: &str,
) -> &'a HashMap<String, String> {
    result
        .storage_credentials
        .as_deref()
        .and_then(|credentials| covering_credential(credentials, location))
        .unwrap_or(&result.config)
}

/// The config of the longest non-empty `prefix` entry that `location` starts with. Schemes are
/// compared lowercased (RFC 3986 §3.1).
pub(crate) fn covering_credential<'a>(
    credentials: &'a [StorageCredential],
    location: &str,
) -> Option<&'a HashMap<String, String>> {
    let location = lowercase_scheme(location);
    credentials
        .iter()
        .filter(|entry| {
            !entry.prefix.is_empty()
                && location.starts_with(lowercase_scheme(&entry.prefix).as_ref())
        })
        .max_by_key(|entry| entry.prefix.len())
        .map(|entry| &entry.config)
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
        access_key: required_vended_value(vended, S3_ACCESS_KEY_ID, anchor)?,
        secret_key: required_vended_value(vended, S3_SECRET_ACCESS_KEY, anchor)?,
        session_token: vended_config_value(vended, S3_SESSION_TOKEN),
        region: vended_config_value(vended, "client.region"),
        endpoint: vended_config_value(vended, S3_ENDPOINT),
        path_style: vended_config_value(vended, "s3.path-style-access")
            .and_then(|value| value.parse::<bool>().ok()),
    })
}

/// The scan's storage resolution ignores the expiry, so only this read refuses an unreadable one.
pub(crate) fn s3_credentials(
    vended: &HashMap<String, String>,
    anchor: &str,
) -> Result<VendedS3Credentials, UdfError> {
    let expires_at_ms = vended
        .get(S3_EXPIRES_AT_MS)
        .map(|value| expiry(value, S3_EXPIRES_AT_MS, anchor))
        .transpose()?;
    let s3 = iceberg_vended_s3(vended, anchor)?;
    Ok(VendedS3Credentials {
        access_key_id: s3.access_key,
        secret_access_key: s3.secret_key,
        session_token: s3.session_token,
        endpoint: s3.endpoint,
        expires_at_ms,
    })
}

/// The scan's storage resolution ignores the expiry, so only this read refuses an unreadable one.
/// The expiry key carries the host spelling the SAS key was selected by.
fn adls_sas_credentials(
    vended: &HashMap<String, String>,
    anchor: &str,
) -> Result<VendedAdlsSas, UdfError> {
    let (host, sas) = selected_sas(vended, anchor)?;
    let expiry_key = format!("{VENDED_SAS_EXPIRES_AT_MS_KEY_PREFIX}{host}");
    let expires_at_ms = vended
        .get(&expiry_key)
        .map(|value| expiry(value, &expiry_key, anchor))
        .transpose()?;
    Ok(VendedAdlsSas {
        sas: sas.clone(),
        expires_at_ms,
    })
}

/// An expiry that is present but empty is unreadable, not absent.
fn expiry(value: &str, key: &str, anchor: &str) -> Result<u64, UdfError> {
    value.parse::<u64>().map_err(|_| {
        UdfError::User(format!(
            "the catalog vended credentials for table location {anchor} whose {key} is no epoch \
             millisecond"
        ))
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
    selected_sas(vended, anchor).map(|(_, sas)| sas.clone())
}

/// The first non-empty SAS among the location host's key spellings, with the spelling.
fn selected_sas<'a>(
    vended: &'a HashMap<String, String>,
    anchor: &'a str,
) -> Result<(&'a str, &'a String), UdfError> {
    let host = location_host(anchor);
    sas_key_hosts(vended, host)
        .into_iter()
        .find_map(|suffix| {
            vended
                .get(&format!("{VENDED_SAS_TOKEN_KEY_PREFIX}{suffix}"))
                .filter(|sas| !sas.is_empty())
                .map(|sas| (suffix, sas))
        })
        .ok_or_else(|| {
            UdfError::User(format!(
                "vended credentials were requested for storage host {host} (table location \
                 {anchor}), but the catalog returned none: the selected credential source carries \
                 no non-empty {VENDED_SAS_TOKEN_KEY_PREFIX}{host} key in any casing of that host"
            ))
        })
}

/// The host spellings of the SAS keys: `host` itself first when present, then every case variant
/// (RFC 3986 §3.2.2), smallest key first.
fn sas_key_hosts<'a>(vended: &'a HashMap<String, String>, host: &'a str) -> Vec<&'a str> {
    let mut variants: Vec<&str> = vended
        .keys()
        .filter_map(|key| key.strip_prefix(VENDED_SAS_TOKEN_KEY_PREFIX))
        .filter(|key_host| *key_host != host && key_host.eq_ignore_ascii_case(host))
        .collect();
    variants.sort_unstable();
    let exact = vended
        .contains_key(&format!("{VENDED_SAS_TOKEN_KEY_PREFIX}{host}"))
        .then_some(host);
    exact.into_iter().chain(variants).collect()
}

fn vended_config_value(vended: &HashMap<String, String>, key: &str) -> Option<String> {
    vended.get(key).filter(|s| !s.is_empty()).cloned()
}

#[cfg(test)]
#[path = "vended_tests.rs"]
mod tests;

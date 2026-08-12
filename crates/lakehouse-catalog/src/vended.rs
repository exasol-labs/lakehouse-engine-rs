//! Iceberg REST vended-storage resolution: derive a table's effective scan
//! storage from what its `loadTable` response vends for that table's own
//! location, with no CONNECTION-supplied storage value involved.
//!
//! [`resolve_vended_storage`] is the whole public surface; everything below it is a
//! private step.

use crate::storage::{VendedBackendKind, classify_vended_scheme};
use crate::{AdlsCred, StorageBackend, StorageProps};
use exasol_udf_sdk::error::UdfError;
use std::borrow::Cow;
use std::collections::HashMap;

/// Vended ADLS SAS keys are host-suffixed (`adls.sas-token.<host>`) — the iceberg Java
/// `AzureProperties` convention, not a key the Iceberg REST spec enumerates. The reader
/// downstream understands only the flat `adls.sas-token`, so the suffix is recovered
/// here and travels no further.
const VENDED_SAS_TOKEN_KEY_PREFIX: &str = "adls.sas-token.";

/// Resolve the effective scan storage for a table from its `loadTable` response, the
/// URI scheme of that table's own location, and the operator's `ALLOW_HTTP` consent.
///
/// Reads the catalog's response and nothing else: taking no [`StorageBackend`] and no
/// `ConnectionCreds` makes "no CONNECTION storage field is read under vending" a
/// property of this signature. The caller gates the call on `use_vended_credentials`.
///
/// The backend variant comes from `anchor`'s scheme ALONE, matched case-insensitively
/// per RFC 3986 §3.1: `s3`/`s3a` → S3, `abfss`/`abfs` → ADLS, anything else a
/// `UdfError::User`. The mapping is total over the scheme string rather than a match on
/// [`StorageBackend`], so a third enum variant still compiles here —
/// `catalog_public_surface.rs`'s source-level variant probe is the compensating gate.
///
/// Anything the response does not advertise is ABSENT; there is no static value
/// underneath to preserve it from. A vended `http://` endpoint and an `abfs://`
/// location both require `allow_http` (the engine has no plaintext Azure path and would
/// silently read `abfs://` over HTTPS).
///
/// `anchor` must be the table's own location — also what
/// `storage_credentials[*].prefix` matches against — so a catalog URI is rejected as an
/// unsupported scheme rather than silently selecting the flat `config` map.
pub fn resolve_vended_storage(
    result: &iceberg_catalog_rest::LoadTableResult,
    anchor: &str,
    allow_http: bool,
) -> Result<StorageBackend, UdfError> {
    let scheme = anchor
        .split_once("://")
        .map_or(String::new(), |(scheme, _)| scheme.to_ascii_lowercase());
    let vended = select_credential_source(result, anchor);
    match classify_vended_scheme(&scheme) {
        Some(VendedBackendKind::S3) => s3_backend_from_vended(vended, anchor, allow_http),
        Some(VendedBackendKind::Adls) if scheme != "abfs" || allow_http => {
            adls_backend_from_vended(vended, anchor)
        }
        Some(VendedBackendKind::Adls) => Err(UdfError::User(format!(
            "vended credentials were requested for the plaintext table location {anchor}, but the \
             ALLOW_HTTP virtual-schema property is false: abfs:// names plaintext transport, and \
             this engine has no plaintext Azure path — it would silently read the location over \
             HTTPS instead, so honouring it requires the operator's explicit ALLOW_HTTP \
             acknowledgement rather than a silent scheme upgrade"
        ))),
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

/// The S3 backend the vended credential source describes, built from that source ALONE.
/// The key pair is required, and so is at least one of `client.region` and
/// `s3.endpoint`: with no static payload underneath, those are the only values that can
/// place the store.
///
/// `path_style` defaults to whether an endpoint was vended, because
/// `register_side_store` treats it as the gate on whether `endpoint` reaches
/// `AmazonS3Builder` at all — a vended endpoint beside `path_style: false` would be
/// silently dropped for a virtual-hosted host derived from the region, which is the
/// wrong store rather than a plan-time error. A value the response does state still
/// wins.
fn s3_backend_from_vended(
    vended: &HashMap<String, String>,
    anchor: &str,
    allow_http: bool,
) -> Result<StorageBackend, UdfError> {
    let access_key = required_vended_value(vended, "s3.access-key-id", anchor)?;
    let secret_key = required_vended_value(vended, "s3.secret-access-key", anchor)?;
    let region = vended_config_value(vended, "client.region");
    let endpoint = vended_config_value(vended, "s3.endpoint");

    if region.is_none() && endpoint.is_none() {
        return Err(UdfError::User(format!(
            "vended credentials for table location {anchor} leave the store address undetermined: \
             the selected credential source carries neither a non-empty client.region nor a \
             non-empty s3.endpoint, and under vending no CONNECTION value can supply one"
        )));
    }
    if !allow_http
        && let Some(endpoint) = endpoint.as_deref()
        && endpoint
            .split_once("://")
            .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("http"))
    {
        return Err(UdfError::User(format!(
            "the catalog vended the plaintext endpoint {endpoint} for table location {anchor}, but \
             the ALLOW_HTTP virtual-schema property is false: a catalog cannot move vended \
             credentials onto plaintext transport without the operator's consent"
        )));
    }

    let path_style = vended_config_value(vended, "s3.path-style-access")
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(endpoint.is_some());

    Ok(StorageBackend::S3(StorageProps {
        endpoint: endpoint.unwrap_or_default(),
        region: region.unwrap_or_default(),
        access_key,
        secret_key,
        session_token: vended_config_value(vended, "s3.session-token"),
        allow_http,
        path_style,
    }))
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

/// The ADLS backend the vended credential source describes, selected by the anchor's
/// OWN storage host. Reading the account name and the SAS from that one host keeps the
/// pair consistent: `adls.account-name` is the downstream wrong-account guard, so an
/// account name disagreeing with the account the SAS was minted for would disarm it.
///
/// The host suffix is matched CASE-INSENSITIVELY per RFC 3986 §3.2.2, resolved
/// deterministically rather than by hash order: the anchor's exact spelling wins, and
/// among case-variant spellings the lexicographically smallest key does. The KEY LABEL
/// is still matched exactly, as the S3 arm matches its keys.
///
/// `account_name` is derived from the host VERBATIM: the guard it feeds compares it
/// byte-exactly against the account parsed out of each file URI
/// (`iceberg-storage-opendal-0.10.0/src/azdls.rs:165`), so a case-folded account name
/// would fire the guard on the very locations it was derived from.
fn adls_backend_from_vended(
    vended: &HashMap<String, String>,
    anchor: &str,
) -> Result<StorageBackend, UdfError> {
    let host = anchor_host(anchor);
    let account_name = host
        .split('.')
        .next()
        .filter(|label| !label.is_empty())
        .ok_or_else(|| {
            UdfError::User(format!(
                "vended credentials were requested for table location {anchor}, but its storage \
                 host '{host}' carries no leading label to read an ADLS account name from: \
                 expected the <account> of <account>.dfs.core.windows.net"
            ))
        })?;
    let sas = vended_config_value(vended, &format!("{VENDED_SAS_TOKEN_KEY_PREFIX}{host}"))
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
        })?;

    Ok(StorageBackend::Adls {
        account_name: account_name.to_string(),
        cred: AdlsCred::Sas(sas),
    })
}

/// The storage host of a table location: its authority segment, read after any
/// `<container>@` userinfo. For ADLS that is the `<account>.dfs.core.windows.net` the
/// vended SAS keys are suffixed with, so one reading serves both the SAS selection and
/// the account name.
fn anchor_host(anchor: &str) -> &str {
    let after_scheme = anchor.split_once("://").map_or(anchor, |(_, rest)| rest);
    let authority = after_scheme
        .split_once('/')
        .map_or(after_scheme, |(authority, _)| authority);
    authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host)
}

fn vended_config_value(vended: &HashMap<String, String>, key: &str) -> Option<String> {
    vended.get(key).filter(|s| !s.is_empty()).cloned()
}

#[cfg(test)]
#[path = "vended_tests.rs"]
mod tests;

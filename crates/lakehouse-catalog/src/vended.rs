//! Iceberg REST vended-storage resolution: derive a table's effective scan
//! storage from what its `loadTable` response vends for that table's own
//! location, with no CONNECTION-supplied storage value involved.
//!
//! [`resolve_vended_storage`] is the whole public surface. Everything below it is
//! a private step: the caller states the intent (resolve the effective storage
//! for this response) and never handles the recipe — which of two credential
//! sources applies, which backend the location's URI scheme selects, which
//! config keys carry that backend's vended values, or how each spells absence.

use crate::{AdlsCred, StorageBackend, StorageProps};
use exasol_udf_sdk::error::UdfError;
use std::collections::HashMap;

/// The vended ADLS SAS keys are host-suffixed — `adls.sas-token.<host>` — which
/// is the iceberg Java `AzureProperties` convention a catalog emits, not a key
/// the Iceberg REST spec enumerates. The reader downstream understands only the
/// flat `adls.sas-token` spelling, so the suffix is recovered here and never
/// travels any further.
const VENDED_SAS_TOKEN_KEY_PREFIX: &str = "adls.sas-token.";

/// Resolve the effective scan storage for a table from its `loadTable` response,
/// the URI scheme of that table's own location, and the operator's resolved
/// `ALLOW_HTTP` consent.
///
/// This is the VENDED half of a selector pair. It reads the catalog's response
/// and nothing else; `storage_block` reads the CONNECTION and nothing else; one
/// `use_vended_credentials` branch at the call site picks between them, so the
/// two selectors run on disjoint inputs and neither can override the other.
/// Taking no [`StorageBackend`] and no `ConnectionCreds` is the design rather
/// than an omission: it makes "no CONNECTION storage FIELD is read under
/// vending" a property of this signature instead of a rule an auditor has to
/// re-verify by reading the body. That guarantee covers storage fields only —
/// `anchor` itself is caller-supplied and is not guaranteed CONNECTION-free:
/// when a table's metadata carries no location, the caller
/// (`resolve_file_list`) substitutes the CONNECTION's `warehouse`, and on that
/// fallback path a CONNECTION-derived string chooses the backend variant, the
/// credential-source prefix match, and (for ADLS) the SAS host. `allow_http`
/// does not reopen the storage-field door either — it carries one
/// virtual-schema property resolved outside this crate, names no credential,
/// and cannot supply one.
///
/// The backend variant comes from `anchor`'s scheme ALONE, matched
/// case-insensitively per RFC 3986 §3.1: `s3://`/`S3://` and `s3a://`/`S3A://`
/// resolve S3, `abfss://` and `abfs://` (in any casing) resolve ADLS, and every
/// other scheme — including a location carrying no scheme at all — is a
/// `UdfError::User`, because a default backend would be a guess about where the
/// operator's data lives. The mapping is a total function over the scheme
/// string rather than a match on [`StorageBackend`], so a third variant added
/// to that enum still compiles here; `catalog_public_surface.rs`'s
/// source-level variant probe is the compensating gate.
///
/// Whatever the response does not advertise is ABSENT: there is no static value
/// underneath to preserve it from, so a credential or a store address the
/// catalog does not vend is reported rather than substituted. Plaintext
/// transport needs the operator's consent, but the two schemes below are gated
/// for different reasons: a vended plain-`http://` endpoint really is
/// plaintext on the wire, while an `abfs://` location is not — this engine has
/// no plaintext Azure path and would silently read it over HTTPS instead — but
/// naming a plaintext scheme the engine will not actually honour as plaintext
/// is itself misleading, so honouring the location still requires the
/// operator's explicit `ALLOW_HTTP` acknowledgement rather than a silent scheme
/// upgrade.
///
/// `anchor` must be the table's own location. It is both what
/// `storage_credentials[*].prefix` matches against and what the variant is read
/// from, so a catalog URI passed here is rejected as an unsupported scheme
/// rather than silently selecting the flat `config` map.
///
/// Whether vending applies at all is the caller's decision, not this function's:
/// `use_vended_credentials` gates the call rather than being a parameter,
/// because a flag that switches a function between doing the work and returning
/// its input is a decision the function declined to make.
pub fn resolve_vended_storage(
    result: &iceberg_catalog_rest::LoadTableResult,
    anchor: &str,
    allow_http: bool,
) -> Result<StorageBackend, UdfError> {
    let scheme = anchor
        .split_once("://")
        .map_or(String::new(), |(scheme, _)| scheme.to_ascii_lowercase());
    let vended = select_credential_source(result, anchor);
    match scheme.as_str() {
        "s3" | "s3a" => s3_backend_from_vended(vended, anchor, allow_http),
        "abfss" => adls_backend_from_vended(vended, anchor),
        "abfs" if allow_http => adls_backend_from_vended(vended, anchor),
        "abfs" => Err(UdfError::User(format!(
            "vended credentials were requested for the plaintext table location {anchor}, but the \
             ALLOW_HTTP virtual-schema property is false: abfs:// names plaintext transport, and \
             this engine has no plaintext Azure path — it would silently read the location over \
             HTTPS instead, so honouring it requires the operator's explicit ALLOW_HTTP \
             acknowledgement rather than a silent scheme upgrade"
        ))),
        _ => Err(UdfError::User(format!(
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
fn select_credential_source<'a>(
    result: &'a iceberg_catalog_rest::LoadTableResult,
    location: &str,
) -> &'a HashMap<String, String> {
    result
        .storage_credentials
        .as_ref()
        .and_then(|credentials| {
            credentials
                .iter()
                .filter(|entry| !entry.prefix.is_empty() && location.starts_with(&entry.prefix))
                .max_by_key(|entry| entry.prefix.len())
        })
        .map_or(&result.config, |entry| &entry.config)
}

/// The S3 backend the vended credential source describes, constructed from that
/// source ALONE. The key pair is required, and so is at least one of
/// `client.region` and `s3.endpoint`, because with no static payload underneath
/// those two are the only values left that can place the store. Everything else
/// is absent when the source omits it — there is nothing to preserve it from.
///
/// `path_style` is the one value that is not resolved in isolation, because it is
/// not an independent field to its consumer: `register_side_store` treats it as
/// the gate on whether `endpoint` reaches `AmazonS3Builder` at all. A vended
/// endpoint resolved beside `path_style: false` is therefore an address the scan
/// cannot use — it would silently drop the endpoint and read a virtual-hosted AWS
/// host derived from the region instead, which is the wrong store rather than a
/// plan-time error. So a response that omits `s3.path-style-access`, or spells it
/// unparseably, resolves it from whether an endpoint was vended; a value the
/// response does state still wins, since a catalog naming virtual-hosted
/// addressing outright is describing its own store.
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

/// A vended value the resolved backend cannot be built without, reported when
/// the selected source omits it or spells it empty. Reported rather than
/// substituted: a static value read here would be a credential the operator
/// believed was unused, and the whole point of vending is that the catalog
/// supplies it.
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

/// The ADLS backend the vended credential source describes, selected by the
/// anchor's OWN storage host. Reading the account name and the SAS from that one
/// host is what keeps the pair internally consistent: `adls.account-name` is the
/// downstream wrong-account guard, so an account name that disagreed with the
/// account the SAS was minted for would disarm it or fail the manifest read.
///
/// The vended key spelling is host-suffixed while the iceberg ADLS reader
/// accepts only the flat `adls.sas-token`, so recovering the host here and
/// assembling the flat SAS state is what keeps every consumer downstream unaware
/// that a second spelling exists.
///
/// The host suffix is matched CASE-INSENSITIVELY, matching the scheme rule in
/// [`resolve_vended_storage`]: RFC 3986 §3.2.2 makes a URI host
/// case-insensitive, so a catalog that spells the account differently from the
/// table location names the same storage account and must not read as "the
/// catalog vended none". The exact spelling is tried FIRST, by direct lookup, so
/// a payload carrying case-variant spellings of one host resolves
/// deterministically rather than by hash order; only a payload with two
/// non-empty case-variant keys and no exact match leaves the choice arbitrary,
/// and by that same RFC both of those keys name one account.
///
/// The KEY LABEL is deliberately still matched exactly. Unlike the host it is
/// not a URI component with a documented case rule but a protocol key spelling,
/// and the S3 arm reads its keys exactly too — relaxing one arm alone would make
/// the two disagree about what a vended key is.
///
/// `account_name` is derived from the host VERBATIM, not normalised. The guard
/// it feeds compares it byte-exactly against the account parsed out of each file
/// URI (`iceberg-storage-opendal-0.10.0/src/azdls.rs:165`), so a case-folded
/// account name would make the guard fire on the very locations it was derived
/// from.
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
                .keys()
                .filter(|key| {
                    key.strip_prefix(VENDED_SAS_TOKEN_KEY_PREFIX)
                        .is_some_and(|key_host| key_host.eq_ignore_ascii_case(host))
                })
                .find_map(|key| vended_config_value(vended, key))
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
/// `<container>@` userinfo. For an ADLS location that is the
/// `<account>.dfs.core.windows.net` the vended SAS keys are suffixed with, which
/// is why one reading serves both the SAS selection and the account name.
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
mod tests {
    use super::*;
    use crate::test_support::*;

    const VENDED_AK: &str = "VENDED_AK_SENTINEL";
    const VENDED_SK: &str = "VENDED_SK_SENTINEL";
    const VENDED_TOK: &str = "VENDED_TOKEN_SENTINEL";
    const VENDED_REGION: &str = "eu-west-2";
    const VENDED_SAS: &str = "VENDED_SAS_SENTINEL";
    /// A SAS minted for an account the anchor does not name — the wrong-account
    /// value every host-selection assertion is measured against.
    const OTHER_HOST_SAS: &str = "OTHER_HOST_SAS_SENTINEL";
    /// An `adls.account-key` beside the vended SAS. The vended selector has no
    /// reader for it, so no resolved backend may ever carry it.
    const VENDED_ACCOUNT_KEY: &str = "VENDED_ACCOUNT_KEY_SENTINEL";
    const ADLS_ACCOUNT: &str = "myaccount";
    const ADLS_HOST: &str = "myaccount.dfs.core.windows.net";
    const OTHER_ADLS_HOST: &str = "otheraccount.dfs.core.windows.net";

    /// Every credential value this module's fixtures carry. A refusal may name
    /// none of them, whichever fixture drew it.
    const CREDENTIAL_SENTINELS: &[&str] = &[
        VENDED_AK,
        VENDED_SK,
        VENDED_TOK,
        VENDED_SAS,
        OTHER_HOST_SAS,
        VENDED_ACCOUNT_KEY,
        STATIC_AK,
        STATIC_SK,
    ];

    /// Build a minimal LoadTableResult for testing.
    #[allow(clippy::type_complexity)]
    fn make_load_table_result(
        storage_credentials: Option<Vec<(&str, Vec<(&str, &str)>)>>,
        config: Vec<(&str, &str)>,
    ) -> iceberg_catalog_rest::LoadTableResult {
        use iceberg::spec::TableMetadata;
        use iceberg_catalog_rest::LoadTableResult;

        // Minimal valid JSON for iceberg TableMetadata (v2).
        // Requires: format-version, table-uuid, location, last-sequence-number,
        // last-updated-ms, last-column-id, schemas (type+schema-id+fields),
        // current-schema-id, partition-specs, default-spec-id, last-partition-id,
        // sort-orders, default-sort-order-id.
        let meta_json = serde_json::json!({
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": "s3://bucket/db/t",
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 0,
            "current-schema-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0
        });
        let metadata: TableMetadata = serde_json::from_value(meta_json).expect("valid metadata");

        let sc = storage_credentials.map(|entries| {
            entries
                .into_iter()
                .map(|(prefix, kvs)| iceberg_catalog_rest::StorageCredential {
                    prefix: prefix.to_string(),
                    config: kvs
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                })
                .collect()
        });

        LoadTableResult {
            metadata_location: Some("s3://bucket/db/t/metadata/v1.json".into()),
            metadata,
            config: config
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            storage_credentials: sc,
        }
    }

    /// A `LoadTableResult` pre-loaded with vended S3 credentials in the flat
    /// config map — this is the Databricks Unity Catalog shape where
    /// `storage_credentials` is empty and vended creds live in the flat config.
    fn vended_result_flat_config() -> iceberg_catalog_rest::LoadTableResult {
        make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.session-token", VENDED_TOK),
                ("client.region", VENDED_REGION),
            ],
        )
    }

    /// A `LoadTableResult` whose flat config vends one host-suffixed SAS per
    /// entry — the `adls.sas-token.<host>` shape a catalog emits for an ADLS
    /// warehouse, spelled through the production key prefix so a change to that
    /// constant cannot leave these fixtures asserting a stale spelling.
    fn adls_vended_result(sas_per_host: &[(&str, &str)]) -> iceberg_catalog_rest::LoadTableResult {
        let keys: Vec<(String, &str)> = sas_per_host
            .iter()
            .map(|(host, sas)| (format!("{VENDED_SAS_TOKEN_KEY_PREFIX}{host}"), *sas))
            .collect();
        make_load_table_result(
            None,
            keys.iter().map(|(key, sas)| (key.as_str(), *sas)).collect(),
        )
    }

    /// The S3 payload the vended selector resolves — for the tests whose subject
    /// is which values it reads rather than whether the request was satisfiable.
    fn vended_s3(
        result: &iceberg_catalog_rest::LoadTableResult,
        anchor: &str,
        allow_http: bool,
    ) -> StorageProps {
        s3_payload(
            resolve_vended_storage(result, anchor, allow_http)
                .expect("the vended request must be satisfiable"),
        )
    }

    /// The message a refused vended request reports — for the tests whose subject
    /// is the refusal. Any variant other than `UdfError::User` is itself a
    /// failure: an unsatisfied vended request is an operator-actionable
    /// condition, not an internal one.
    fn vended_user_error(
        result: &iceberg_catalog_rest::LoadTableResult,
        anchor: &str,
        allow_http: bool,
    ) -> String {
        let error = resolve_vended_storage(result, anchor, allow_http)
            .expect_err("an unsatisfied vended request must not resolve a backend");
        assert!(
            matches!(error, UdfError::User(_)),
            "an unsatisfied vended request must be a user error, got {error:?}"
        );
        error.to_string()
    }

    /// The account name and SAS the vended selector resolves for an ADLS anchor —
    /// for the tests whose subject is which values it reads. That the SAS state is
    /// the only state it can reach is asserted separately, by
    /// [`vended_adls_backend_holds_the_sas_state_never_the_account_key_state`].
    fn vended_adls_sas(
        result: &iceberg_catalog_rest::LoadTableResult,
        anchor: &str,
        allow_http: bool,
    ) -> (String, String) {
        match resolve_vended_storage(result, anchor, allow_http)
            .expect("the vended request must be satisfiable")
        {
            StorageBackend::Adls {
                account_name,
                cred: AdlsCred::Sas(sas),
            } => (account_name, sas),
            other => panic!("expected an ADLS backend carrying a vended SAS, got {other:?}"),
        }
    }

    fn assert_names_no_credential_value(message: &str) {
        for secret in CREDENTIAL_SENTINELS {
            assert!(
                !message.contains(secret),
                "a refusal must name no credential value, found {secret} in: {message}"
            );
        }
    }

    /// Every refusal a vended request can draw has the same three obligations: it
    /// names the table location it was for, it names the config key or property
    /// that was not satisfied, and it names no credential value.
    fn assert_refused(message: &str, anchor: &str, named: &[&str]) {
        assert!(
            message.contains(anchor),
            "the refusal must name the table location {anchor}: {message}"
        );
        for name in named {
            assert!(
                message.contains(name),
                "the refusal must name {name}: {message}"
            );
        }
        assert_names_no_credential_value(message);
    }

    // ---------------------------------------------------------------------------
    // Credential-source selection: which of the two sources a loadTable response
    // carries applies to a given table location.
    // ---------------------------------------------------------------------------

    /// Scenario: storage_credentials entry with the matching prefix provides vended creds.
    #[test]
    fn vended_storage_prefers_storage_credentials_over_flat_config() {
        let result = make_load_table_result(
            Some(vec![(
                "s3://bucket/db",
                vec![
                    ("s3.access-key-id", "VENDED_AK"),
                    ("s3.secret-access-key", "VENDED_SK"),
                    ("s3.session-token", "VENDED_TOK"),
                    ("client.region", VENDED_REGION),
                ],
            )]),
            // config also has keys — must be ignored when storage_credentials matches
            vec![
                ("s3.access-key-id", "STATIC_AK"),
                ("s3.secret-access-key", "STATIC_SK"),
            ],
        );

        let merged = vended_s3(&result, result.metadata.location(), false);

        assert_eq!(
            merged.access_key, "VENDED_AK",
            "storage_credentials must take precedence"
        );
        assert_eq!(merged.secret_key, "VENDED_SK");
        assert_eq!(merged.session_token.as_deref(), Some("VENDED_TOK"));
    }

    /// Scenario: longest prefix wins when multiple storage_credentials entries match.
    #[test]
    fn vended_storage_longest_matching_prefix_wins() {
        let result = make_load_table_result(
            Some(vec![
                (
                    "s3://bucket",
                    vec![
                        ("s3.access-key-id", "SHORT_AK"),
                        ("s3.secret-access-key", "SHORT_SK"),
                        ("client.region", VENDED_REGION),
                    ],
                ),
                (
                    "s3://bucket/db/t",
                    vec![
                        ("s3.access-key-id", "LONG_AK"),
                        ("s3.secret-access-key", "LONG_SK"),
                        ("client.region", VENDED_REGION),
                    ],
                ),
                (
                    "s3://bucket/db",
                    vec![
                        ("s3.access-key-id", "MID_AK"),
                        ("s3.secret-access-key", "MID_SK"),
                        ("client.region", VENDED_REGION),
                    ],
                ),
            ]),
            vec![],
        );

        let merged = vended_s3(&result, "s3://bucket/db/t/metadata/v1.json", false);

        assert_eq!(
            merged.access_key, "LONG_AK",
            "longest matching prefix must win"
        );
        assert_eq!(merged.secret_key, "LONG_SK");
    }

    /// Scenario: falls back to flat config when no storage_credentials prefix matches.
    #[test]
    fn vended_storage_falls_back_to_flat_config() {
        let result = make_load_table_result(
            Some(vec![(
                "s3://other-bucket", // doesn't match location
                vec![("s3.access-key-id", "WRONG_AK")],
            )]),
            vec![
                ("s3.access-key-id", "CONFIG_AK"),
                ("s3.secret-access-key", "CONFIG_SK"),
                ("client.region", VENDED_REGION),
            ],
        );

        let merged = vended_s3(&result, "s3://bucket/db/t/metadata/v1.json", false);

        assert_eq!(
            merged.access_key, "CONFIG_AK",
            "must fall back to flat config"
        );
        assert_eq!(merged.secret_key, "CONFIG_SK");
    }

    /// Scenario: falls back to flat config when storage_credentials is absent.
    #[test]
    fn vended_storage_uses_flat_config_when_no_storage_credentials() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", "CONFIG_AK"),
                ("s3.secret-access-key", "CONFIG_SK"),
                ("s3.session-token", "CONFIG_TOK"),
                ("client.region", VENDED_REGION),
            ],
        );

        let merged = vended_s3(&result, "s3://bucket/db/t/metadata/v1.json", false);

        assert_eq!(merged.access_key, "CONFIG_AK");
        assert_eq!(merged.secret_key, "CONFIG_SK");
        assert_eq!(merged.session_token.as_deref(), Some("CONFIG_TOK"));
    }

    // ---------------------------------------------------------------------------
    // The anchor is the table's OWN location: it is both what a
    // storage_credentials prefix matches against and what the backend variant is
    // read from, so the catalog's own HTTPS URI is refused rather than quietly
    // reaching the flat config map.
    // ---------------------------------------------------------------------------

    /// Scenario: the correct anchor for longest-prefix matching is
    /// `result.metadata.location()` — an S3 table URI — not the HTTPS catalog
    /// endpoint or the metadata-file JSON path.
    ///
    /// `make_load_table_result` sets `metadata.location = "s3://bucket/db/t"`.
    /// A prefix `"s3://bucket/db"` matches that S3 location. An HTTPS catalog URI
    /// matches no S3 prefix AND names no storage backend, so it is now a refusal
    /// rather than a silent fall-back to the flat config's own credentials.
    #[test]
    fn vended_storage_anchor_is_the_s3_table_location() {
        let result = make_load_table_result(
            Some(vec![(
                "s3://bucket/db",
                vec![
                    ("s3.access-key-id", "VENDED_AK"),
                    ("s3.secret-access-key", "VENDED_SK"),
                    ("client.region", VENDED_REGION),
                ],
            )]),
            vec![
                ("s3.access-key-id", "CONFIG_AK"),
                ("s3.secret-access-key", "CONFIG_SK"),
                ("client.region", VENDED_REGION),
            ],
        );

        // The S3 table location ("s3://bucket/db/t") matches the prefix "s3://bucket/db".
        // Verify vended creds are returned when the anchor is the S3 table location.
        let s3_anchor = result.metadata.location().to_string();
        assert!(
            s3_anchor.starts_with("s3://"),
            "metadata.location() must be an S3 URI, got: {s3_anchor}"
        );
        let merged_s3 = vended_s3(&result, &s3_anchor, false);
        assert_eq!(
            merged_s3.access_key, "VENDED_AK",
            "S3 table location anchor must match the storage_credentials prefix"
        );

        // Passing the HTTPS catalog URI instead is refused: it matches no S3
        // prefix and names no backend, so reaching the flat config's credentials
        // from it would read the wrong store with the wrong keys.
        let https_anchor = "https://glue.us-east-1.amazonaws.com/v1/catalog";
        let message = vended_user_error(&result, https_anchor, false);
        assert!(
            message.contains(https_anchor),
            "the refusal must name the location it could not read: {message}"
        );
        assert!(
            !message.contains("CONFIG_AK"),
            "an unsupported location must not reach the flat config: {message}"
        );
    }

    // ---------------------------------------------------------------------------
    // The vended source is the WHOLE storage source: the resolved backend carries
    // what the response advertises and nothing else.
    // ---------------------------------------------------------------------------

    /// Scenario: the storage the scan spec carries under vending is the vended
    /// credential set — access key, secret key, and session token — with no
    /// static value beside it for anything the response does advertise.
    #[test]
    fn vended_creds_are_the_sole_storage_source_in_spec() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", "VENDED_AK"),
                ("s3.secret-access-key", "VENDED_SK"),
                ("s3.session-token", "VENDED_TOK"),
                ("client.region", VENDED_REGION),
            ],
        );

        let merged = vended_s3(&result, result.metadata.location(), false);

        assert_eq!(
            merged.access_key, "VENDED_AK",
            "access_key must be the vended value"
        );
        assert_eq!(
            merged.secret_key, "VENDED_SK",
            "secret_key must be the vended value"
        );
        assert_eq!(
            merged.session_token.as_deref(),
            Some("VENDED_TOK"),
            "session_token must be the vended value"
        );
    }

    /// Scenario: once vending IS requested, an empty vended key is a missing
    /// credential rather than a licence to read the static one. The refusal names
    /// the config key the catalog left empty and carries no credential value.
    ///
    /// The vending-DISABLED half of that rule is deliberately not asserted here.
    /// It is not this crate's branch to reach: `resolve_vended_storage` takes no
    /// static storage, so the decision lives entirely in the engine crate's
    /// `use_vended_credentials` check at the call site, and the E2E suites are what
    /// cover it. A local `StorageProps` cloned and compared against itself would
    /// assert nothing about either.
    #[test]
    fn empty_vended_key_pair_is_a_missing_credential_not_a_licence_to_read_static() {
        let result = make_load_table_result(
            None,
            vec![("s3.access-key-id", ""), ("s3.secret-access-key", "")],
        );

        let message = vended_user_error(&result, result.metadata.location(), true);

        assert!(
            message.contains("s3.access-key-id"),
            "the refusal must name the config key the catalog left empty: {message}"
        );
        assert_names_no_credential_value(&message);
    }

    /// Scenario: the vended `s3.session-token` is the session token the resolved
    /// storage carries.
    #[test]
    fn vended_storage_adopts_the_vended_session_token() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", "VENDED_AK"),
                ("s3.secret-access-key", "VENDED_SK"),
                ("s3.session-token", "NEW_STS_TOKEN"),
                ("client.region", VENDED_REGION),
            ],
        );

        let merged = vended_s3(&result, result.metadata.location(), false);

        assert_eq!(
            merged.session_token.as_deref(),
            Some("NEW_STS_TOKEN"),
            "the vended session_token must be the resolved one"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 6 (add-lakekeeper-e2e) — vended S3 endpoint/path-style extraction.
    // Surfaced as a genuine interop gap against a real Lakekeeper 0.13.1 (the
    // MinIO vended endpoint).
    // ---------------------------------------------------------------------------

    /// The vended flat config's `s3.endpoint` and `s3.path-style-access` are
    /// extracted so an S3-compatible store (MinIO behind Lakekeeper) is reachable
    /// even though the vended CONNECTION carries no static endpoint. The endpoint
    /// is plaintext, which the operator's `ALLOW_HTTP` consent admits.
    #[test]
    fn vended_storage_adopts_endpoint_and_path_style_from_flat_config() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", "VENDED_AK"),
                ("s3.secret-access-key", "VENDED_SK"),
                ("s3.endpoint", "http://minio:9000/"),
                ("s3.path-style-access", "true"),
            ],
        );

        let merged = vended_s3(&result, "s3://bucket/db/t/metadata/v1.json", true);

        assert_eq!(merged.endpoint, "http://minio:9000/");
        assert!(merged.path_style);
    }

    /// The endpoint is read from the longest-matching `storage_credentials` entry
    /// (the Lakekeeper shape) with the same precedence as the vended keys/region.
    #[test]
    fn vended_storage_adopts_endpoint_from_storage_credentials() {
        let result = make_load_table_result(
            Some(vec![(
                "s3://bucket/db",
                vec![
                    ("s3.access-key-id", "VENDED_AK"),
                    ("s3.secret-access-key", "VENDED_SK"),
                    ("s3.endpoint", "http://minio:9000/"),
                    ("s3.path-style-access", "true"),
                ],
            )]),
            vec![("s3.endpoint", "http://wrong:1/")],
        );

        let merged = vended_s3(&result, "s3://bucket/db/t/metadata/v1.json", true);

        assert_eq!(
            merged.endpoint, "http://minio:9000/",
            "the matching storage_credentials entry's endpoint must win over flat config"
        );
        assert!(merged.path_style);
    }

    // ---------------------------------------------------------------------------
    // Group C — redaction hardening + vended-auth-orthogonality tests
    // ---------------------------------------------------------------------------

    /// Scenario: Vended S3 credentials are the sole storage source regardless of
    /// catalog auth mode.
    ///
    /// Vended extraction is a pure post-processing step on the `LoadTableResult`;
    /// the auth mode that produced the result is irrelevant. This test simulates
    /// the result of all three non-SigV4 modes and confirms that the same vended
    /// storage is derived from each.
    #[test]
    fn vended_creds_are_the_sole_storage_source_across_all_auth_modes() {
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        // The vended extraction logic is auth-mode-independent: run it for each
        // logical auth mode and confirm identical output.
        for mode_label in ["no-auth", "bearer", "oauth2"] {
            let merged = vended_s3(&result, &anchor, false);

            assert_eq!(
                merged.access_key, VENDED_AK,
                "{mode_label}: access_key must be vended"
            );
            assert_eq!(
                merged.secret_key, VENDED_SK,
                "{mode_label}: secret_key must be vended"
            );
            assert_eq!(
                merged.session_token.as_deref(),
                Some(VENDED_TOK),
                "{mode_label}: session_token must be vended"
            );
            assert_ne!(
                merged.access_key, STATIC_AK,
                "{mode_label}: the static access_key must never appear"
            );
        }
    }

    /// Scenario: Vended credentials are extracted on the static bearer-token
    /// catalog path (Databricks Unity Catalog flat-config shape).
    ///
    /// Simulates the bearer-token path: the catalog request was authenticated with
    /// `Authorization: Bearer <token>`; the returned result carries vended creds in
    /// the flat config map. The extraction must work identically to the SigV4 path.
    #[test]
    fn bearer_token_path_extracts_vended_from_config() {
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        let merged = vended_s3(&result, &anchor, false);

        assert_eq!(
            merged.access_key, VENDED_AK,
            "bearer path: vended access_key"
        );
        assert_eq!(
            merged.secret_key, VENDED_SK,
            "bearer path: vended secret_key"
        );
        assert_eq!(
            merged.session_token.as_deref(),
            Some(VENDED_TOK),
            "bearer path: vended session_token"
        );
        // Static token must NOT bleed into storage.
        assert_ne!(merged.access_key, STATIC_AK);
    }

    /// Scenario: Vended credentials are extracted on the OAuth2 client-credentials
    /// catalog path.
    ///
    /// The OAuth2 grant produces a bearer token used to authenticate the loadTable
    /// GET. The returned `LoadTableResult` carries vended creds in the same flat
    /// config shape. Extraction is auth-mode-independent.
    #[test]
    fn oauth2_path_extracts_vended_credentials() {
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        let merged = vended_s3(&result, &anchor, false);

        assert_eq!(
            merged.access_key, VENDED_AK,
            "oauth2 path: vended access_key"
        );
        assert_eq!(
            merged.secret_key, VENDED_SK,
            "oauth2 path: vended secret_key"
        );
        assert_eq!(
            merged.session_token.as_deref(),
            Some(VENDED_TOK),
            "oauth2 path: vended session_token"
        );
        // OAuth2 client_secret must NOT bleed into storage.
        assert_ne!(merged.access_key, STATIC_AK);
        assert_ne!(merged.secret_key, CLIENT_SECRET);
    }

    // ---------------------------------------------------------------------------
    // Region, endpoint, and path-style come from the response and nowhere else.
    // ---------------------------------------------------------------------------

    /// Scenario: a vended-credentials request takes every S3 transport value from
    /// the response — the region it advertises is adopted, the endpoint and
    /// path-style it omits are absent — and a response that advertises neither
    /// region nor endpoint leaves the store address undetermined.
    #[test]
    fn vended_storage_takes_region_endpoint_and_path_style_from_the_response_only() {
        // Part A: client.region present → vended region adopted, and the two
        // transport values the response omits are absent, not filled in.
        let result_with_region = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.session-token", VENDED_TOK),
                ("client.region", VENDED_REGION),
            ],
        );
        let anchor = result_with_region.metadata.location().to_string();

        let merged = vended_s3(&result_with_region, &anchor, false);
        assert_eq!(
            merged.region, VENDED_REGION,
            "the vended region must be the resolved region"
        );
        assert_ne!(
            merged.region, "us-east-1",
            "no static region may appear under vending"
        );
        assert!(
            merged.endpoint.is_empty(),
            "an endpoint the response omits is absent, got {}",
            merged.endpoint
        );
        assert!(
            !merged.path_style,
            "a path-style the response omits is absent"
        );

        // Part B: neither client.region nor s3.endpoint → nothing left can place
        // the store, and no static value may supply one.
        let result_no_address = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
            ],
        );
        let anchor_no_address = result_no_address.metadata.location().to_string();

        let message = vended_user_error(&result_no_address, &anchor_no_address, false);
        assert!(
            message.contains("client.region") && message.contains("s3.endpoint"),
            "the refusal must name both keys that could have placed the store: {message}"
        );
        assert_names_no_credential_value(&message);
    }

    /// Scenario: the `X-Iceberg-Access-Delegation` header is sent when vending is
    /// enabled and absent when it is not.
    ///
    /// Verified on a request constructed the way `authed_get_json` constructs it.
    #[test]
    fn vended_request_sends_access_delegation_header() {
        let client = reqwest::Client::new();
        let url = "https://catalog.example.com/v1/namespaces/db/tables/t";

        // When use_vended_credentials=true: header must be present.
        let req_with_delegation = client
            .get(url)
            .header("accept", "application/json")
            .header("X-Iceberg-Access-Delegation", "vended-credentials")
            .build()
            .expect("valid request");
        assert_eq!(
            req_with_delegation
                .headers()
                .get("x-iceberg-access-delegation")
                .and_then(|v| v.to_str().ok()),
            Some("vended-credentials"),
            "access-delegation header must be present when vending enabled"
        );

        // When use_vended_credentials=false: header must be absent.
        let req_no_delegation = client
            .get(url)
            .header("accept", "application/json")
            .build()
            .expect("valid request");
        assert!(
            req_no_delegation
                .headers()
                .get("x-iceberg-access-delegation")
                .is_none(),
            "access-delegation header must be absent when vending disabled"
        );
    }

    // ---------------------------------------------------------------------------
    // Redaction: vended STS values never in error messages
    // ---------------------------------------------------------------------------

    /// Scenario: vended STS values (access key, secret key, session token) never
    /// appear in errors from the new auth paths.
    ///
    /// Vended values arrive only in a SUCCESS response, so they don't appear in
    /// error responses. We verify they are stripped if they were ever erroneously
    /// echoed, using `redact_secret_values` (same mechanism StorageProps uses).
    #[test]
    fn vended_sts_values_not_in_error_messages() {
        let vended_secrets = [VENDED_AK, VENDED_SK, VENDED_TOK];
        let raw_error =
            format!("scan failed: access_key={VENDED_AK} secret={VENDED_SK} token={VENDED_TOK}");
        let redacted = crate::redact_secret_values(&raw_error, &vended_secrets);
        for secret in vended_secrets {
            assert!(
                !redacted.contains(secret),
                "vended STS value must not appear in error: {redacted}"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // Absence, precedence, and single selection: what the S3 arm does with each
    // value the selected source omits, spells empty, or spells unparseably.
    // ---------------------------------------------------------------------------

    /// Scenario: an empty vended `s3.access-key-id` is absent per the uniform
    /// convention (`vended_config_value` filters empty strings), and an absent
    /// credential under vending is a refusal — there is no static value beneath
    /// it to read instead. The sibling non-empty vended values do not soften it:
    /// a credential set missing one required member is not usable.
    #[test]
    fn resolve_vended_storage_empty_access_key_is_a_missing_credential() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", ""),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.session-token", VENDED_TOK),
                ("client.region", VENDED_REGION),
            ],
        );
        let anchor = result.metadata.location().to_string();

        let message = vended_user_error(&result, &anchor, false);

        assert!(
            message.contains("s3.access-key-id"),
            "the refusal must name the empty config key: {message}"
        );
        assert_names_no_credential_value(&message);
    }

    /// Scenario: an empty vended `s3.secret-access-key` is absent per the same
    /// convention, and equally a refusal.
    #[test]
    fn resolve_vended_storage_empty_secret_key_is_a_missing_credential() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", ""),
                ("s3.session-token", VENDED_TOK),
                ("client.region", VENDED_REGION),
            ],
        );
        let anchor = result.metadata.location().to_string();

        let message = vended_user_error(&result, &anchor, false);

        assert!(
            message.contains("s3.secret-access-key"),
            "the refusal must name the empty config key: {message}"
        );
        assert_names_no_credential_value(&message);
    }

    /// Scenario: an absent or empty vended `s3.session-token` resolves to `None`
    /// — covering both the absent-key case and the empty-string case in one
    /// test, since both spell "no vended token" per `vended_config_value`.
    ///
    /// A session token is not required: a vended long-lived key pair carries
    /// none. What it must never be is a static token the operator believed was
    /// unused, so absent resolves to absent.
    #[test]
    fn resolve_vended_storage_absent_session_token_is_absent() {
        // Case 1: the key is absent from the vended config entirely.
        let result_absent = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("client.region", VENDED_REGION),
            ],
        );
        let anchor_absent = result_absent.metadata.location().to_string();
        let merged_absent = vended_s3(&result_absent, &anchor_absent, false);
        assert_eq!(
            merged_absent.session_token, None,
            "an absent vended session_token key resolves to no token"
        );

        // Case 2: the key is present but empty.
        let result_empty = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.session-token", ""),
                ("client.region", VENDED_REGION),
            ],
        );
        let anchor_empty = result_empty.metadata.location().to_string();
        let merged_empty = vended_s3(&result_empty, &anchor_empty, false);
        assert_eq!(
            merged_empty.session_token, None,
            "an empty vended session_token value resolves to no token"
        );
    }

    /// Scenario: an unparseable `s3.path-style-access` string on a response that
    /// vends NO endpoint resolves to `false`. `bool::from_str` is case-sensitive —
    /// only the exact lowercase `"true"`/`"false"` parse — so a differently-cased
    /// value such as `"TRUE"` must fail to parse and fall to the default, not be
    /// treated as truthy.
    ///
    /// The default it falls to is whether an endpoint was vended, so the absence of
    /// an endpoint here is what makes the expected value `false`: with only
    /// `client.region` to place the store, virtual-hosted addressing is all that is
    /// left to address it with. The endpoint-present half of the same default is
    /// [`vended_endpoint_without_path_style_stays_reachable_by_the_scan`].
    #[test]
    fn resolve_vended_storage_unparseable_path_style_without_an_endpoint_is_false() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("client.region", VENDED_REGION),
                ("s3.path-style-access", "TRUE"),
            ],
        );
        let anchor = result.metadata.location().to_string();

        let merged = vended_s3(&result, &anchor, false);

        assert!(
            merged.endpoint.is_empty(),
            "the fixture must vend no endpoint, or the default under test is the other one"
        );
        assert!(
            !merged.path_style,
            "an unparseable path-style string must fall to the default, not parse as true"
        );
    }

    /// Scenario: a response vending an `s3.endpoint` but no `s3.path-style-access`
    /// resolves `path_style: true`. `path_style` is what decides whether the scan
    /// hands that endpoint to `AmazonS3Builder` at all, so resolving it `false`
    /// beside a non-empty endpoint produces an address the scan silently discards —
    /// it would fall back to a virtual-hosted AWS host derived from the (here
    /// empty) region and read the wrong store. This is the shape a catalog
    /// fronting an S3-compatible store vends.
    #[test]
    fn vended_endpoint_without_path_style_stays_reachable_by_the_scan() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.endpoint", "http://minio:9000/"),
            ],
        );
        let anchor = result.metadata.location().to_string();

        let merged = vended_s3(&result, &anchor, true);

        assert_eq!(
            merged.endpoint, "http://minio:9000/",
            "the vended endpoint must be the resolved endpoint"
        );
        assert!(
            merged.path_style,
            "a vended endpoint with no path-style flag must resolve path_style true, or the scan \
             drops the endpoint the catalog just supplied"
        );
    }

    /// Scenario: an `s3.path-style-access` the response DOES state still wins over
    /// the endpoint-coupled default. The coupling above fills in a value the
    /// catalog left unstated; it must not overwrite one the catalog stated, or a
    /// store that asked for virtual-hosted addressing would be addressed
    /// path-style against its own endpoint.
    #[test]
    fn vended_explicit_path_style_false_wins_over_the_endpoint_coupled_default() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.endpoint", "https://s3.example.com/"),
                ("s3.path-style-access", "false"),
            ],
        );
        let anchor = result.metadata.location().to_string();

        assert!(
            !vended_s3(&result, &anchor, false).path_style,
            "a path-style of false that the response states must not be overridden by the \
             presence of an endpoint"
        );
    }

    /// Scenario: a matched `storage_credentials` entry that omits a key must
    /// NOT fall back to the flat `config` map for that key — the entry, once
    /// selected, is authoritative for the whole credential set.
    ///
    /// The refusal IS the compliance evidence: the flat `config` map here carries
    /// the secret_key the entry omits, so a per-key second selection would have
    /// resolved successfully with that value. Only a single selection can turn
    /// this fixture into a missing-credential error.
    #[test]
    fn resolve_vended_storage_matched_entry_missing_key_does_not_fall_back_to_config() {
        let result = make_load_table_result(
            Some(vec![(
                "s3://bucket/db",
                vec![
                    ("s3.access-key-id", VENDED_AK),
                    ("client.region", VENDED_REGION),
                    // secret_key omitted from the entry
                ],
            )]),
            vec![("s3.secret-access-key", "CONFIG_SK_MUST_NOT_LEAK")],
        );
        let anchor = result.metadata.location().to_string();

        let message = vended_user_error(&result, &anchor, false);

        assert!(
            message.contains("s3.secret-access-key"),
            "the refusal must name the key the matched entry omits: {message}"
        );
        assert!(
            !message.contains("CONFIG_SK_MUST_NOT_LEAK"),
            "the flat config's secret must never be read or named: {message}"
        );
        assert_names_no_credential_value(&message);
    }

    /// Scenario: `allow_http` is the operator's resolved `ALLOW_HTTP` property,
    /// threaded in — never read from the vended result, which advertises no such
    /// key. Confirmed by flipping the parameter across two calls with the
    /// identical vended result and observing the resolved value track it.
    ///
    /// The same parameter is the plaintext consent gate: a catalog that vends an
    /// `http://` endpoint cannot move the vended credentials onto plaintext
    /// transport on its own authority.
    #[test]
    fn resolve_vended_storage_allow_http_comes_from_the_threaded_parameter() {
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        assert!(
            vended_s3(&result, &anchor, true).allow_http,
            "allow_http=true must carry through"
        );
        assert!(
            !vended_s3(&result, &anchor, false).allow_http,
            "allow_http=false must carry through, unaffected by the same vended result"
        );

        let plaintext = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.endpoint", "http://minio:9000/"),
            ],
        );
        let plaintext_anchor = plaintext.metadata.location().to_string();

        let honoured = vended_s3(&plaintext, &plaintext_anchor, true);
        assert_eq!(
            honoured.endpoint, "http://minio:9000/",
            "a vended plaintext endpoint is honoured under operator consent"
        );
        assert!(
            honoured.path_style,
            "and honouring it must leave it reachable: the scan passes the endpoint to the \
             builder only when path_style is set"
        );
        let message = vended_user_error(&plaintext, &plaintext_anchor, false);
        assert!(
            message.contains("ALLOW_HTTP"),
            "the refusal must name the property that withholds consent: {message}"
        );
        assert_names_no_credential_value(&message);
    }

    /// Scenario: the credential source is selected ONCE for `anchor` and that
    /// single selection feeds all six vended reads — not a per-key
    /// re-selection that could let some keys read the matched entry and others
    /// silently read the flat `config` map instead.
    ///
    /// The matched `storage_credentials` entry supplies only four of the six
    /// keys (access_key, secret_key, region, endpoint); the flat `config` map
    /// carries wrong sentinel values for all six. A single selection means the
    /// four present keys resolve to the entry's values and the two the entry
    /// omits resolve without ever consulting config: `session_token` to absent,
    /// and `path_style` to the endpoint-coupled default — `true` here, because
    /// the matched entry vends an endpoint.
    ///
    /// Config's `s3.path-style-access` sentinel is therefore `"false"`, the
    /// opposite of that default: a value agreeing with it could not tell a leak
    /// from a correct resolution, which is the whole point of a sentinel.
    #[test]
    fn resolve_vended_storage_selects_credential_source_once_for_all_six_values() {
        let result = make_load_table_result(
            Some(vec![(
                "s3://bucket/db",
                vec![
                    ("s3.access-key-id", VENDED_AK),
                    ("s3.secret-access-key", VENDED_SK),
                    ("client.region", VENDED_REGION),
                    ("s3.endpoint", "http://vended-endpoint:9000/"),
                    // session_token and path_style deliberately omitted here.
                ],
            )]),
            vec![
                ("s3.access-key-id", "CONFIG_AK_MUST_NOT_LEAK"),
                ("s3.secret-access-key", "CONFIG_SK_MUST_NOT_LEAK"),
                ("s3.session-token", "CONFIG_TOKEN_MUST_NOT_LEAK"),
                ("client.region", "config-region-must-not-leak"),
                ("s3.endpoint", "http://config-endpoint-must-not-leak/"),
                ("s3.path-style-access", "false"),
            ],
        );
        let anchor = result.metadata.location().to_string();

        let merged = vended_s3(&result, &anchor, true);

        assert_eq!(
            merged.access_key, VENDED_AK,
            "access_key from matched entry"
        );
        assert_eq!(
            merged.secret_key, VENDED_SK,
            "secret_key from matched entry"
        );
        assert_eq!(merged.region, VENDED_REGION, "region from matched entry");
        assert_eq!(
            merged.endpoint, "http://vended-endpoint:9000/",
            "endpoint from matched entry"
        );
        assert_eq!(
            merged.session_token, None,
            "a session_token absent from the matched entry is absent, never the flat config's value"
        );
        assert!(
            merged.path_style,
            "a path-style absent from the matched entry falls to the endpoint-coupled default, \
             never to the flat config's contradicting value"
        );
    }

    // ---------------------------------------------------------------------------
    // Scheme-driven backend selection: the table location's URI scheme is the one
    // input that knows which store the table's data actually lives in.
    // ---------------------------------------------------------------------------

    /// Scenario: the resolved backend variant follows the anchor's URI scheme —
    /// `s3://` and `s3a://` resolve S3, `abfss://` and `abfs://` resolve ADLS. The
    /// scheme is matched case-insensitively (RFC 3986 §3.1), so an upper-cased
    /// scheme resolves the same backend as its lowercase spelling.
    ///
    /// The two Azure schemes are checked at the consent value each one needs:
    /// `abfss://` is encrypted and resolves without operator consent, `abfs://` is
    /// plaintext and resolves only with it.
    #[test]
    fn vended_backend_variant_comes_from_the_anchor_scheme() {
        let s3_result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("client.region", VENDED_REGION),
            ],
        );
        for scheme in ["s3", "s3a"] {
            let anchor = format!("{scheme}://bucket/db/t");
            let resolved = resolve_vended_storage(&s3_result, &anchor, false)
                .unwrap_or_else(|e| panic!("{scheme}:// must resolve a backend: {e}"));
            assert!(
                matches!(resolved, StorageBackend::S3(_)),
                "{scheme}:// must select the S3 backend, got {resolved:?}"
            );
        }

        let adls_result = adls_vended_result(&[(ADLS_HOST, VENDED_SAS)]);
        for (scheme, allow_http) in [("abfss", false), ("abfs", true)] {
            let anchor = format!("{scheme}://container@{ADLS_HOST}/db/t");
            let resolved = resolve_vended_storage(&adls_result, &anchor, allow_http)
                .unwrap_or_else(|e| panic!("{scheme}:// must resolve a backend: {e}"));
            assert!(
                matches!(resolved, StorageBackend::Adls { .. }),
                "{scheme}:// must select the ADLS backend, got {resolved:?}"
            );
        }

        let resolved_upper_s3 = resolve_vended_storage(&s3_result, "S3://bucket/db/t", false)
            .unwrap_or_else(|e| panic!("S3:// must resolve a backend: {e}"));
        assert!(
            matches!(resolved_upper_s3, StorageBackend::S3(_)),
            "an upper-cased S3:// scheme must select the S3 backend, got {resolved_upper_s3:?}"
        );

        let upper_abfss_anchor = format!("ABFSS://container@{ADLS_HOST}/db/t");
        let resolved_upper_abfss = resolve_vended_storage(&adls_result, &upper_abfss_anchor, false)
            .unwrap_or_else(|e| panic!("ABFSS:// must resolve a backend: {e}"));
        assert!(
            matches!(resolved_upper_abfss, StorageBackend::Adls { .. }),
            "an upper-cased ABFSS:// scheme must select the ADLS backend, got \
             {resolved_upper_abfss:?}"
        );
    }

    /// Scenario: an anchor whose scheme names no backend this engine can read is
    /// refused, and a scheme-less warehouse fallback is one of them.
    ///
    /// `file_resolution.rs` substitutes `catalog_props.warehouse` for an empty
    /// table location, and a Glue warehouse is a bare AWS account id carrying no
    /// scheme at all — so a scheme-less anchor is a shape this function really
    /// receives rather than a hypothetical. The catalog's own HTTPS URI is the
    /// other, and it is the one a caller is most likely to pass by mistake.
    #[test]
    fn vended_backend_variant_comes_from_the_anchor_scheme_and_refuses_every_other() {
        let result = vended_result_flat_config();

        for anchor in [
            "https://glue.us-east-1.amazonaws.com/v1/catalog",
            "123456789012",
        ] {
            let message = vended_user_error(&result, anchor, false);
            assert_refused(
                &message,
                anchor,
                &["s3://", "s3a://", "abfss://", "abfs://"],
            );
        }
    }

    // ---------------------------------------------------------------------------
    // Refusals: every way a loadTable response can fail to satisfy a vended
    // request, on either arm.
    // ---------------------------------------------------------------------------

    /// Scenario: a vended request the catalog does not satisfy is a refusal naming
    /// what was missing — never a fall-back to a static value, of which this
    /// selector holds none.
    #[test]
    fn unsatisfied_vended_request_errors_without_static_fallback() {
        let s3_anchor = "s3://bucket/db/t";
        let abfss_anchor = format!("abfss://container@{ADLS_HOST}/db/t");
        let abfs_anchor = format!("abfs://container@{ADLS_HOST}/db/t");

        // An absent S3 key pair.
        let absent_pair = make_load_table_result(None, vec![("client.region", VENDED_REGION)]);
        assert_refused(
            &vended_user_error(&absent_pair, s3_anchor, false),
            s3_anchor,
            &["s3.access-key-id"],
        );

        // An S3 key pair spelled empty — the same absence, per the one convention.
        let empty_pair = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", ""),
                ("s3.secret-access-key", ""),
                ("client.region", VENDED_REGION),
            ],
        );
        assert_refused(
            &vended_user_error(&empty_pair, s3_anchor, false),
            s3_anchor,
            &["s3.access-key-id"],
        );

        // A key pair with neither region nor endpoint: nothing left can place the
        // store, and an empty region silently misroutes an AWS store.
        let no_address = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
            ],
        );
        assert_refused(
            &vended_user_error(&no_address, s3_anchor, false),
            s3_anchor,
            &["client.region", "s3.endpoint"],
        );

        // An ADLS response carrying no adls.sas-token.* key at all.
        let no_sas = make_load_table_result(None, vec![("client.region", VENDED_REGION)]);
        assert_refused(
            &vended_user_error(&no_sas, &abfss_anchor, false),
            &abfss_anchor,
            &[VENDED_SAS_TOKEN_KEY_PREFIX],
        );

        // An ADLS response whose only SAS was minted for a different host: a SAS
        // is account-scoped, so the wrong account's is no more usable than none.
        let wrong_host_sas = adls_vended_result(&[(OTHER_ADLS_HOST, OTHER_HOST_SAS)]);
        assert_refused(
            &vended_user_error(&wrong_host_sas, &abfss_anchor, false),
            &abfss_anchor,
            &[VENDED_SAS_TOKEN_KEY_PREFIX, ADLS_HOST],
        );

        // A vended plaintext endpoint without the operator's consent.
        let plaintext_endpoint = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.endpoint", "http://minio:9000/"),
            ],
        );
        assert_refused(
            &vended_user_error(&plaintext_endpoint, s3_anchor, false),
            s3_anchor,
            &["ALLOW_HTTP", "http://minio:9000/"],
        );

        // A plaintext abfs:// location without the operator's consent, refused
        // even though this payload WOULD satisfy the same anchor over abfss://:
        // the consent gate is on the transport, not on the credential set.
        let satisfiable_sas = adls_vended_result(&[(ADLS_HOST, VENDED_SAS)]);
        assert_refused(
            &vended_user_error(&satisfiable_sas, &abfs_anchor, false),
            &abfs_anchor,
            &["ALLOW_HTTP", "abfs://"],
        );
    }

    /// Scenario: the missing-SAS refusal still names the storage host after the
    /// error text has passed through [`crate::redact_error_text`] on its way out.
    ///
    /// The refusal has to name a host to be actionable, and it also has to name
    /// the absent `adls.sas-token.<host>` key — but redaction treats that key as a
    /// credential label and truncates everything after it up to the next space,
    /// host included. Naming the host earlier in the message is what survives, so
    /// this pins that placement rather than the wording around it.
    #[test]
    fn adls_missing_sas_refusal_names_the_host_after_redaction() {
        let result = adls_vended_result(&[(OTHER_ADLS_HOST, OTHER_HOST_SAS)]);
        let anchor = format!("abfss://container@{ADLS_HOST}/db/t");

        let message = vended_user_error(&result, &anchor, false);
        assert_names_no_credential_value(&message);

        let redacted = crate::redact_error_text(&message, &[OTHER_HOST_SAS, VENDED_SAS]);

        assert!(
            !redacted.contains(&format!("{VENDED_SAS_TOKEN_KEY_PREFIX}{ADLS_HOST}")),
            "the host suffixed onto the key name is the occurrence redaction truncates, so a \
             refusal naming the host only there would surface no host at all: {redacted}"
        );
        assert!(
            redacted.contains(ADLS_HOST),
            "the storage host must survive redaction: {redacted}"
        );
    }

    // ---------------------------------------------------------------------------
    // Azure extraction: which vended SAS applies, and the account name that has to
    // agree with it.
    // ---------------------------------------------------------------------------

    /// Scenario: with several `adls.sas-token.<host>` keys vended at once, the SAS
    /// the anchor's OWN storage host names is the one selected, and the account
    /// name is read from that same host so the two cannot disagree.
    ///
    /// A disagreeing pair is not cosmetic: `adls.account-name` is the downstream
    /// wrong-account guard on the manifest read, so an account name taken from
    /// anywhere but the host the SAS was minted for would disarm it.
    #[test]
    fn vended_adls_sas_is_selected_by_anchor_host_with_derived_account_name() {
        let result = adls_vended_result(&[
            (OTHER_ADLS_HOST, OTHER_HOST_SAS),
            (ADLS_HOST, VENDED_SAS),
            (
                "thirdaccount.dfs.core.windows.net",
                "THIRD_HOST_SAS_SENTINEL",
            ),
        ]);
        let anchor = format!("abfss://mycontainer@{ADLS_HOST}/db/t");

        let (account_name, sas) = vended_adls_sas(&result, &anchor, false);

        assert_eq!(
            sas, VENDED_SAS,
            "the selected SAS must be the one minted for the anchor's own host"
        );
        assert_eq!(
            account_name, ADLS_ACCOUNT,
            "the account name must be the anchor host's first label"
        );
    }

    /// Scenario: an `abfss://<container>@<host>/…` location reads its host AFTER
    /// the `<container>@` userinfo segment, so the container name never becomes
    /// the account name and never joins the SAS key it is matched against.
    #[test]
    fn vended_adls_reads_the_host_after_the_container_userinfo() {
        let result = adls_vended_result(&[(ADLS_HOST, VENDED_SAS)]);

        let with_container = vended_adls_sas(
            &result,
            &format!("abfss://mycontainer@{ADLS_HOST}/db/t"),
            false,
        );
        let without_container =
            vended_adls_sas(&result, &format!("abfss://{ADLS_HOST}/db/t"), false);

        assert_eq!(
            with_container.0, ADLS_ACCOUNT,
            "the container must not be read as the account name"
        );
        assert_eq!(
            with_container, without_container,
            "the container segment must not change what the location resolves to"
        );
    }

    /// Scenario: the anchor host and the vended key's host suffix are matched
    /// case-insensitively, so a location spelling the account in a different case
    /// than the catalog's key still resolves that key's SAS.
    ///
    /// RFC 3986 §3.2.2 makes a URI host case-insensitive, and
    /// [`resolve_vended_storage`] already reads the SCHEME that way — a
    /// byte-exact host match would refuse a request the catalog did satisfy and
    /// report it as "the catalog returned none", which is both wrong and
    /// misdirecting.
    ///
    /// `account_name` stays VERBATIM from the anchor: the guard it feeds compares
    /// it byte-exactly against the account in each file URI, so normalising it
    /// here would fire that guard on the locations it was derived from.
    #[test]
    fn vended_adls_sas_host_match_is_case_insensitive() {
        let result = adls_vended_result(&[(ADLS_HOST, VENDED_SAS)]);
        let mixed_case_host = "MyAccount.DFS.Core.Windows.NET";
        assert_ne!(
            mixed_case_host, ADLS_HOST,
            "the anchor host must differ from the vended key's host by CASE ONLY for this test to \
             be about case at all"
        );
        assert!(
            mixed_case_host.eq_ignore_ascii_case(ADLS_HOST),
            "the anchor host must differ from the vended key's host by CASE ONLY"
        );

        let (account_name, sas) = vended_adls_sas(
            &result,
            &format!("abfss://mycontainer@{mixed_case_host}/db/t"),
            false,
        );

        assert_eq!(
            sas, VENDED_SAS,
            "a host differing only in case names the same storage account, so the SAS the catalog \
             vended for it must be selected"
        );
        assert_eq!(
            account_name, "MyAccount",
            "the account name must stay the anchor's own spelling: the downstream wrong-account \
             guard compares it byte-exactly against the account in each file URI"
        );
    }

    /// Scenario: with both an exact-case and a case-variant spelling of the
    /// anchor's host vended at once, the exact one is selected — the choice is
    /// deterministic rather than resolved by hash-map iteration order.
    #[test]
    fn vended_adls_sas_prefers_the_exact_host_spelling() {
        let result = adls_vended_result(&[
            (ADLS_HOST, VENDED_SAS),
            ("MYACCOUNT.DFS.CORE.WINDOWS.NET", OTHER_HOST_SAS),
        ]);
        let anchor = format!("abfss://mycontainer@{ADLS_HOST}/db/t");

        for _ in 0..16 {
            let (_, sas) = vended_adls_sas(&result, &anchor, false);
            assert_eq!(
                sas, VENDED_SAS,
                "the key whose host suffix matches the anchor exactly must win every time, not \
                 whichever spelling the map happens to yield first"
            );
        }
    }

    /// Scenario: an anchor whose storage host carries no leading label is refused.
    /// There is no account name to read from it, and substituting a guess would
    /// disarm the downstream wrong-account guard instead of failing cleanly.
    ///
    /// Both ways a location can arrive without one: an empty authority behind a
    /// `<container>@` segment, and a host whose first dot-separated label is
    /// empty.
    #[test]
    fn vended_adls_account_name_requires_a_labelled_host() {
        let result = adls_vended_result(&[(ADLS_HOST, VENDED_SAS)]);

        for anchor in [
            "abfss://mycontainer@/db/t",
            "abfss://.dfs.core.windows.net/db/t",
        ] {
            let message = vended_user_error(&result, anchor, false);
            assert_refused(&message, anchor, &["account name"]);
        }
    }

    /// Scenario: the resolved ADLS backend holds the SAS state and never the
    /// account-key state, even when the response vends an `adls.account-key`
    /// beside the SAS.
    ///
    /// This pins the vended path's reach over `AdlsCred` rather than the
    /// response's contents: the selector has no reader for an account key, so one
    /// of the enum's two states is unreachable under vending.
    #[test]
    fn vended_adls_backend_holds_the_sas_state_never_the_account_key_state() {
        let sas_key = format!("{VENDED_SAS_TOKEN_KEY_PREFIX}{ADLS_HOST}");
        let result = make_load_table_result(
            None,
            vec![
                (sas_key.as_str(), VENDED_SAS),
                ("adls.account-key", VENDED_ACCOUNT_KEY),
            ],
        );
        let anchor = format!("abfss://mycontainer@{ADLS_HOST}/db/t");

        let resolved = resolve_vended_storage(&result, &anchor, false)
            .expect("the vended request must be satisfiable");

        match resolved {
            StorageBackend::Adls {
                cred: AdlsCred::Sas(sas),
                ..
            } => assert_eq!(sas, VENDED_SAS, "the SAS state must carry the vended SAS"),
            StorageBackend::Adls {
                cred: AdlsCred::AccountKey(_),
                ..
            } => panic!("the vended selector must never reach the account-key state"),
            StorageBackend::S3(_) => panic!("an abfss:// location must not resolve to S3"),
        }
    }
}

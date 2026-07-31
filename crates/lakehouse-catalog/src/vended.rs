//! Iceberg REST vended-storage resolution: read the S3 keys, region, endpoint,
//! and path-style a `loadTable` response vends for one table location, and
//! overlay them onto the statically configured storage.
//!
//! [`resolve_vended_storage`] is the whole public surface. Everything below it is
//! a private step: the caller states the intent (resolve the effective storage
//! for this response) and never handles the recipe — which of two credential
//! sources applies, which six config keys carry the vended values, or how each
//! spells absence.

use crate::{StorageBackend, StorageProps};
use std::collections::HashMap;

/// Resolve the effective scan storage for a table from its `loadTable` response.
///
/// For an `S3` backend, selects the vended credential source ONCE for `anchor`
/// and overlays every value it advertises onto `base`'s wrapped `StorageProps`,
/// returning a fresh [`StorageBackend::S3`]. A value the response does not
/// advertise keeps `base`'s, so a response carrying no vended credentials at
/// all resolves to `base` unchanged.
///
/// `anchor` must be the table's own S3 location — that is what
/// `storage_credentials[*].prefix` matches against. An HTTPS catalog URI can
/// never prefix-match an S3 prefix and would silently select the flat `config`
/// map instead.
///
/// For an `Adls` backend, `base` passes through unchanged: vended Azure SAS
/// credentials are a tracked exception (#276), so the effective backend is
/// already selected once at `parse_creds`/`storage_block` time and this
/// function does no Azure-specific extraction.
///
/// Whether vending applies at all is the caller's decision, not this function's:
/// `use_vended_credentials` gates the call rather than being a parameter,
/// because a flag that switches a function between doing the work and returning
/// its input is a decision the function declined to make.
pub fn resolve_vended_storage(
    result: &iceberg_catalog_rest::LoadTableResult,
    base: &StorageBackend,
    anchor: &str,
) -> StorageBackend {
    match base {
        StorageBackend::S3(props) => {
            let vended = select_credential_source(result, anchor);
            StorageBackend::S3(merge_vended_into_storage(props, vended))
        }
        StorageBackend::Adls { .. } => base.clone(),
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

fn merge_vended_into_storage(
    base: &StorageProps,
    vended: &HashMap<String, String>,
) -> StorageProps {
    StorageProps {
        endpoint: vended_config_value(vended, "s3.endpoint")
            .unwrap_or_else(|| base.endpoint.clone()),
        region: vended_config_value(vended, "client.region").unwrap_or_else(|| base.region.clone()),
        access_key: vended_config_value(vended, "s3.access-key-id")
            .unwrap_or_else(|| base.access_key.clone()),
        secret_key: vended_config_value(vended, "s3.secret-access-key")
            .unwrap_or_else(|| base.secret_key.clone()),
        session_token: vended_config_value(vended, "s3.session-token")
            .or_else(|| base.session_token.clone()),
        allow_http: base.allow_http,
        path_style: vended_config_value(vended, "s3.path-style-access")
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(base.path_style),
    }
}

fn vended_config_value(vended: &HashMap<String, String>, key: &str) -> Option<String> {
    vended.get(key).filter(|s| !s.is_empty()).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::AdlsCred;
    use crate::test_support::*;

    const VENDED_AK: &str = "VENDED_AK_SENTINEL";
    const VENDED_SK: &str = "VENDED_SK_SENTINEL";
    const VENDED_TOK: &str = "VENDED_TOKEN_SENTINEL";
    const VENDED_REGION: &str = "eu-west-2";

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

    // ---------------------------------------------------------------------------
    // Task 4.1 — Vended credential extraction from LoadTableResult
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
                ],
            )]),
            // config also has keys — must be ignored when storage_credentials matches
            vec![
                ("s3.access-key-id", "STATIC_AK"),
                ("s3.secret-access-key", "STATIC_SK"),
            ],
        );

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &static_backend(),
            result.metadata.location(),
        ));

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
                    ],
                ),
                (
                    "s3://bucket/db/t",
                    vec![
                        ("s3.access-key-id", "LONG_AK"),
                        ("s3.secret-access-key", "LONG_SK"),
                    ],
                ),
                (
                    "s3://bucket/db",
                    vec![
                        ("s3.access-key-id", "MID_AK"),
                        ("s3.secret-access-key", "MID_SK"),
                    ],
                ),
            ]),
            vec![],
        );

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &static_backend(),
            "s3://bucket/db/t/metadata/v1.json",
        ));

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
            ],
        );

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &static_backend(),
            "s3://bucket/db/t/metadata/v1.json",
        ));

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
            ],
        );

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &static_backend(),
            "s3://bucket/db/t/metadata/v1.json",
        ));

        assert_eq!(merged.access_key, "CONFIG_AK");
        assert_eq!(merged.secret_key, "CONFIG_SK");
        assert_eq!(merged.session_token.as_deref(), Some("CONFIG_TOK"));
    }

    // ---------------------------------------------------------------------------
    // R2 — vended-credential anchor must be the S3 table location, not the
    // HTTPS catalog URI or the metadata_location JSON path.
    // ---------------------------------------------------------------------------

    /// Scenario: the correct anchor for longest-prefix matching is
    /// `result.metadata.location()` — an S3 table URI — not the HTTPS catalog
    /// endpoint or the metadata-file JSON path.
    ///
    /// `make_load_table_result` sets `metadata.location = "s3://bucket/db/t"`.
    /// A prefix `"s3://bucket/db"` matches that S3 location.
    /// An HTTPS catalog URI such as `"https://glue.amazonaws.com/..."` would
    /// never match an S3 prefix, silently returning no vended creds.
    #[test]
    fn vended_storage_anchor_is_the_s3_table_location() {
        let result = make_load_table_result(
            Some(vec![(
                "s3://bucket/db",
                vec![
                    ("s3.access-key-id", "VENDED_AK"),
                    ("s3.secret-access-key", "VENDED_SK"),
                ],
            )]),
            vec![
                ("s3.access-key-id", "CONFIG_AK"),
                ("s3.secret-access-key", "CONFIG_SK"),
            ],
        );

        let base = static_backend();

        // The S3 table location ("s3://bucket/db/t") matches the prefix "s3://bucket/db".
        // Verify vended creds are returned when the anchor is the S3 table location.
        let s3_anchor = result.metadata.location().to_string();
        assert!(
            s3_anchor.starts_with("s3://"),
            "metadata.location() must be an S3 URI, got: {s3_anchor}"
        );
        let merged_s3 = s3_payload(resolve_vended_storage(&result, &base, &s3_anchor));
        assert_eq!(
            merged_s3.access_key, "VENDED_AK",
            "S3 table location anchor must match the storage_credentials prefix"
        );

        // If we mistakenly used the HTTPS catalog URI as the anchor, no prefix matches
        // and we fall back to the flat config — pin that failure mode here.
        let https_anchor = "https://glue.us-east-1.amazonaws.com/v1/catalog";
        let merged_https = s3_payload(resolve_vended_storage(&result, &base, https_anchor));
        assert_eq!(
            merged_https.access_key, "CONFIG_AK",
            "HTTPS URI must not match any S3 prefix, must fall back to flat config"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 4.2 — merge_vended_into_storage
    // ---------------------------------------------------------------------------

    /// Scenario: Vended S3 credentials from load_table override static credentials
    /// in the scan spec (access_key, secret_key, session_token). A vended source
    /// that advertises only the three S3 key fields leaves `endpoint`, `region`,
    /// `path_style`, and `allow_http` at their `base` values — this fixture's
    /// vended source advertises none of those four fields, so they fall through.
    #[test]
    fn vended_creds_override_static_in_spec() {
        let base = StorageProps {
            endpoint: "https://s3.amazonaws.com".into(),
            region: "us-east-1".into(),
            access_key: "STATIC_AK".into(),
            secret_key: "STATIC_SK".into(),
            session_token: Some("OLD_TOKEN".into()),
            path_style: false,
            ..Default::default()
        };

        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", "VENDED_AK"),
                ("s3.secret-access-key", "VENDED_SK"),
                ("s3.session-token", "VENDED_TOK"),
            ],
        );

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &StorageBackend::S3(base.clone()),
            result.metadata.location(),
        ));

        assert_eq!(
            merged.access_key, "VENDED_AK",
            "vended access_key must override static"
        );
        assert_eq!(
            merged.secret_key, "VENDED_SK",
            "vended secret_key must override static"
        );
        assert_eq!(
            merged.session_token.as_deref(),
            Some("VENDED_TOK"),
            "vended session_token must override static"
        );
        // endpoint, region, path_style, and allow_http fall through to base
        // because this fixture's vended source advertises none of them.
        assert_eq!(
            merged.endpoint, base.endpoint,
            "endpoint must be preserved from static"
        );
        assert_eq!(
            merged.region, base.region,
            "region must be preserved from static"
        );
        assert!(
            !merged.path_style,
            "path_style must be preserved from static"
        );
        assert!(
            !merged.allow_http,
            "allow_http must be preserved from static"
        );
    }

    /// Scenario: Static credentials are used for data files when vending is disabled.
    ///
    /// When use_vended_credentials=false, resolve_file_list returns the static storage
    /// unchanged. We test this via resolve_vended_storage with empty vended keys —
    /// the static keys must be preserved.
    #[test]
    fn vending_disabled_keeps_static_creds() {
        let base = StorageProps {
            endpoint: "http://minio:9000".into(),
            region: "us-east-1".into(),
            access_key: "STATIC_AK".into(),
            secret_key: "STATIC_SK".into(),
            allow_http: true,
            ..Default::default()
        };

        // Empty vended keys — falls back to static.
        let result = make_load_table_result(
            None,
            vec![("s3.access-key-id", ""), ("s3.secret-access-key", "")],
        );
        let merged = s3_payload(resolve_vended_storage(
            &result,
            &StorageBackend::S3(base.clone()),
            result.metadata.location(),
        ));

        assert_eq!(
            merged.access_key, "STATIC_AK",
            "empty vended access_key must keep static"
        );
        assert_eq!(
            merged.secret_key, "STATIC_SK",
            "empty vended secret_key must keep static"
        );
        assert_eq!(
            merged.session_token, None,
            "no session_token when both empty and static absent"
        );
        assert_eq!(merged.endpoint, base.endpoint);
        assert_eq!(merged.region, base.region);
        assert!(merged.path_style);
        assert!(merged.allow_http);
    }

    /// Scenario: vended session_token overrides an existing static session_token.
    #[test]
    fn vended_storage_session_token_overrides_static() {
        let base = StorageProps {
            endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            region: "us-east-1".into(),
            access_key: "STATIC_AK".into(),
            secret_key: "STATIC_SK".into(),
            session_token: Some("OLD_STS_TOKEN".into()),
            path_style: false,
            ..Default::default()
        };

        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", "VENDED_AK"),
                ("s3.secret-access-key", "VENDED_SK"),
                ("s3.session-token", "NEW_STS_TOKEN"),
            ],
        );

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &StorageBackend::S3(base),
            result.metadata.location(),
        ));

        assert_eq!(
            merged.session_token.as_deref(),
            Some("NEW_STS_TOKEN"),
            "new vended session_token must replace old static one"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 6 (add-lakekeeper-e2e) — vended S3 endpoint/path-style extraction.
    // Surfaced as a genuine interop gap against a real Lakekeeper 0.13.1 (the
    // MinIO vended endpoint).
    // ---------------------------------------------------------------------------

    /// The vended flat config's `s3.endpoint` and `s3.path-style-access` are
    /// extracted so an S3-compatible store (MinIO behind Lakekeeper) is reachable
    /// even though the vended CONNECTION carries no static endpoint.
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

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &static_backend(),
            "s3://bucket/db/t/metadata/v1.json",
        ));

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
                    ("s3.endpoint", "http://minio:9000/"),
                    ("s3.path-style-access", "true"),
                ],
            )]),
            vec![("s3.endpoint", "http://wrong:1/")],
        );

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &static_backend(),
            "s3://bucket/db/t/metadata/v1.json",
        ));

        assert_eq!(
            merged.endpoint, "http://minio:9000/",
            "the matching storage_credentials entry's endpoint must win over flat config"
        );
        assert!(merged.path_style);
    }

    /// AWS S3 omits `s3.endpoint`/`s3.path-style-access`; their absence preserves
    /// the static endpoint and path-style (Glue vended path unchanged).
    #[test]
    fn vended_storage_keeps_static_endpoint_and_path_style_when_absent() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", "VENDED_AK"),
                ("s3.secret-access-key", "VENDED_SK"),
            ],
        );
        let base = static_storage();

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &StorageBackend::S3(base.clone()),
            "s3://bucket/db/t/metadata/v1.json",
        ));

        assert_eq!(merged.endpoint, base.endpoint);
        assert_eq!(merged.path_style, base.path_style);
    }

    // ---------------------------------------------------------------------------
    // Group C — redaction hardening + vended-auth-orthogonality tests
    // (Tasks 3.1, 4.1, 4.2, 4.3, 4.4, 4.5)
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // Task 4.1 — Vending orthogonal to auth mode
    // ---------------------------------------------------------------------------

    /// Scenario: Unsigned catalog path is unchanged when SigV4 and vending are
    /// both disabled.
    ///
    /// When `use_vended_credentials=false`, the vended resolution is skipped
    /// entirely and the static credentials stay unchanged.
    #[test]
    fn no_vending_no_sigv4_uses_static_storage_unchanged() {
        let storage = static_storage();
        // Simulate the effective_storage derivation when use_vended_credentials=false:
        // static storage is returned as-is (no vended path entered).
        let effective = storage.clone();

        assert_eq!(effective.access_key, STATIC_AK, "access_key must be static");
        assert_eq!(effective.secret_key, STATIC_SK, "secret_key must be static");
        assert_eq!(effective.session_token, None, "no session_token");
        assert_eq!(effective.region, "us-east-1", "region must be static");
        assert_eq!(effective.endpoint, storage.endpoint, "endpoint preserved");
        assert!(!effective.path_style, "path_style preserved");
        assert!(!effective.allow_http, "allow_http preserved");

        // Also confirm that a loadTable result carrying vended creds does NOT
        // affect the storage when we skip vended extraction.
        let result = vended_result_flat_config();
        let if_applied = s3_payload(resolve_vended_storage(
            &result,
            &StorageBackend::S3(storage.clone()),
            "s3://bucket/db/t",
        ));
        // The keys are present in the result but we never apply them.
        assert_eq!(
            if_applied.access_key, VENDED_AK,
            "vended keys exist in result"
        );
        assert_eq!(
            if_applied.secret_key, VENDED_SK,
            "vended keys exist in result"
        );
        // The static storage remains unchanged.
        assert_eq!(
            storage.access_key, STATIC_AK,
            "static storage must be unchanged"
        );
    }

    /// Scenario: Vended S3 credentials override static credentials regardless
    /// of catalog auth mode.
    ///
    /// Vended extraction is a pure post-processing step on the `LoadTableResult`;
    /// the auth mode that produced the result is irrelevant. This test simulates
    /// the result of all three non-SigV4 modes and confirms that the same vended
    /// storage is derived from each.
    #[test]
    fn vended_overrides_static_across_all_auth_modes() {
        let storage = static_storage();
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        // The vended extraction logic is auth-mode-independent: run it for each
        // logical auth mode and confirm identical output.
        for mode_label in ["no-auth", "bearer", "oauth2"] {
            let merged = s3_payload(resolve_vended_storage(
                &result,
                &StorageBackend::S3(storage.clone()),
                &anchor,
            ));

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
                "{mode_label}: static access_key must be replaced"
            );
            // Static infrastructure fields are preserved.
            assert_eq!(
                merged.endpoint, storage.endpoint,
                "{mode_label}: endpoint preserved"
            );
            assert!(!merged.path_style, "{mode_label}: path_style preserved");
            assert!(!merged.allow_http, "{mode_label}: allow_http preserved");
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
        let storage = static_storage();
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &StorageBackend::S3(storage.clone()),
            &anchor,
        ));

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
        // Endpoint preserved.
        assert_eq!(merged.endpoint, storage.endpoint);
    }

    /// Scenario: Vended credentials are extracted on the OAuth2 client-credentials
    /// catalog path.
    ///
    /// The OAuth2 grant produces a bearer token used to authenticate the loadTable
    /// GET. The returned `LoadTableResult` carries vended creds in the same flat
    /// config shape. Extraction is auth-mode-independent.
    #[test]
    fn oauth2_path_extracts_vended_credentials() {
        let storage = static_backend();
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        let merged = s3_payload(resolve_vended_storage(&result, &storage, &anchor));

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

    /// Scenario: Static credentials are used for data files when vending is disabled.
    ///
    /// For each catalog-auth mode, when `use_vended_credentials=false` the
    /// effective storage equals the static storage unchanged.
    #[test]
    fn vending_disabled_uses_static_on_every_mode() {
        let storage = static_storage();
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        // When use_vended_credentials=false, the adapter skips extraction entirely
        // and clones static storage. Confirm that static storage is byte-identical
        // regardless of the auth mode used.
        for mode_label in ["no-auth", "bearer", "oauth2", "sigv4"] {
            // The vended extraction is NOT applied (use_vended_credentials=false).
            let effective = storage.clone();

            assert_eq!(
                effective.access_key, STATIC_AK,
                "{mode_label}: static access_key must not be replaced"
            );
            assert_eq!(
                effective.secret_key, STATIC_SK,
                "{mode_label}: static secret_key must not be replaced"
            );
            assert_eq!(
                effective.session_token, None,
                "{mode_label}: no vended session_token"
            );
            // Confirm the result has vended keys (but we ignored them).
            let if_applied = s3_payload(resolve_vended_storage(
                &result,
                &StorageBackend::S3(storage.clone()),
                &anchor,
            ));
            assert_eq!(
                if_applied.access_key, VENDED_AK,
                "{mode_label}: result has vended keys (not applied)"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // Task 4.3 — client.region from config overrides static region
    // ---------------------------------------------------------------------------

    /// Scenario: Vended-credentials request adopts the vended region from
    /// `client.region` in the loadTable response config.
    ///
    /// When `use_vended_credentials=true` AND the response carries `client.region`,
    /// the effective storage region is set to the vended value. When `client.region`
    /// is absent, the static region is preserved.
    #[test]
    fn vended_storage_adopts_region_from_flat_config() {
        let storage = static_storage();

        // Part A: client.region present → vended region adopted.
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

        let merged = s3_payload(resolve_vended_storage(
            &result_with_region,
            &StorageBackend::S3(storage.clone()),
            &anchor,
        ));
        assert_eq!(
            merged.region, VENDED_REGION,
            "vended region must override static region"
        );
        assert_ne!(merged.region, "us-east-1", "static region must be replaced");

        // Part B: client.region absent → static region preserved.
        let result_no_region = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
            ],
        );
        let anchor2 = result_no_region.metadata.location().to_string();
        let merged2 = s3_payload(resolve_vended_storage(
            &result_no_region,
            &StorageBackend::S3(storage.clone()),
            &anchor2,
        ));
        assert_eq!(
            merged2.region, "us-east-1",
            "static region must be preserved when client.region absent"
        );
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
    // Task 4.5 / 3.1 — Redaction: vended STS values never in error messages
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
    // resolve_vended_storage behavior-parity: the six absence and precedence
    // cases plus the single-selection guarantee.
    // ---------------------------------------------------------------------------

    /// Scenario: an empty vended `s3.access-key-id` is absent per the uniform
    /// convention (`vended_config_value` filters empty strings), so the static
    /// `base` access_key is preserved. Other vended values still apply, proving
    /// the empty key is treated individually, not as a signal to reject the
    /// whole vended source.
    #[test]
    fn resolve_vended_storage_empty_access_key_preserves_static() {
        let base = static_backend();
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", ""),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.session-token", VENDED_TOK),
            ],
        );
        let anchor = result.metadata.location().to_string();

        let merged = s3_payload(resolve_vended_storage(&result, &base, &anchor));

        assert_eq!(
            merged.access_key, STATIC_AK,
            "empty vended access_key must preserve the static value"
        );
        assert_eq!(
            merged.secret_key, VENDED_SK,
            "a sibling non-empty vended value must still apply"
        );
        assert_eq!(merged.session_token.as_deref(), Some(VENDED_TOK));
    }

    /// Scenario: an empty vended `s3.secret-access-key` is absent per the same
    /// convention, so the static `base` secret_key is preserved.
    #[test]
    fn resolve_vended_storage_empty_secret_key_preserves_static() {
        let base = static_backend();
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", ""),
                ("s3.session-token", VENDED_TOK),
            ],
        );
        let anchor = result.metadata.location().to_string();

        let merged = s3_payload(resolve_vended_storage(&result, &base, &anchor));

        assert_eq!(
            merged.secret_key, STATIC_SK,
            "empty vended secret_key must preserve the static value"
        );
        assert_eq!(merged.access_key, VENDED_AK);
        assert_eq!(merged.session_token.as_deref(), Some(VENDED_TOK));
    }

    /// Scenario: an absent or empty vended `s3.session-token` preserves whatever
    /// `base.session_token` already held — covering both the absent-key case and
    /// the empty-string case in one test, since both spell "no vended token" per
    /// `vended_config_value`.
    #[test]
    fn resolve_vended_storage_absent_session_token_preserves_static() {
        let mut base = static_storage();
        base.session_token = Some("STATIC_TOKEN_SENTINEL".into());

        // Case 1: the key is absent from the vended config entirely.
        let result_absent = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
            ],
        );
        let anchor_absent = result_absent.metadata.location().to_string();
        let merged_absent = s3_payload(resolve_vended_storage(
            &result_absent,
            &StorageBackend::S3(base.clone()),
            &anchor_absent,
        ));
        assert_eq!(
            merged_absent.session_token.as_deref(),
            Some("STATIC_TOKEN_SENTINEL"),
            "an absent vended session_token key must preserve the static token"
        );

        // Case 2: the key is present but empty.
        let result_empty = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.session-token", ""),
            ],
        );
        let anchor_empty = result_empty.metadata.location().to_string();
        let merged_empty = s3_payload(resolve_vended_storage(
            &result_empty,
            &StorageBackend::S3(base.clone()),
            &anchor_empty,
        ));
        assert_eq!(
            merged_empty.session_token.as_deref(),
            Some("STATIC_TOKEN_SENTINEL"),
            "an empty vended session_token value must preserve the static token"
        );
    }

    /// Scenario: an unparseable `s3.path-style-access` string preserves the
    /// static `path_style`. `bool::from_str` is case-sensitive — only the exact
    /// lowercase `"true"`/`"false"` parse — so a differently-cased value such as
    /// `"TRUE"` must fail to parse and fall through to `base`, not be treated as
    /// truthy.
    #[test]
    fn resolve_vended_storage_unparseable_path_style_preserves_static() {
        let mut base = static_storage();
        base.path_style = false;
        let result = make_load_table_result(None, vec![("s3.path-style-access", "TRUE")]);
        let anchor = result.metadata.location().to_string();

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &StorageBackend::S3(base),
            &anchor,
        ));

        assert!(
            !merged.path_style,
            "an unparseable path-style string must preserve the static value, not parse as true"
        );
    }

    /// Scenario: a matched `storage_credentials` entry that omits a key must
    /// NOT fall back to the flat `config` map for that key — the entry, once
    /// selected, is authoritative for the whole credential set. The flat
    /// `config` map here carries a different, wrong secret_key to prove that
    /// value never leaks into the result.
    #[test]
    fn resolve_vended_storage_matched_entry_missing_key_does_not_fall_back_to_config() {
        let base = static_backend();
        let result = make_load_table_result(
            Some(vec![(
                "s3://bucket/db",
                vec![("s3.access-key-id", VENDED_AK)], // secret_key omitted from the entry
            )]),
            vec![("s3.secret-access-key", "CONFIG_SK_MUST_NOT_LEAK")],
        );
        let anchor = result.metadata.location().to_string();

        let merged = s3_payload(resolve_vended_storage(&result, &base, &anchor));

        assert_eq!(
            merged.access_key, VENDED_AK,
            "the key present in the matched entry must apply"
        );
        assert_eq!(
            merged.secret_key, STATIC_SK,
            "a key missing from the matched entry must preserve static, never the flat config"
        );
    }

    /// Scenario: `allow_http` is always taken from `base`, never read from the
    /// vended result at all — there is no `allow_http` extraction function and
    /// none of the six vended config keys map to it. Confirmed by flipping
    /// `base.allow_http` across two calls with the identical vended result and
    /// observing the merged value track `base` every time.
    #[test]
    fn resolve_vended_storage_allow_http_always_from_base() {
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        let mut base_http = static_storage();
        base_http.allow_http = true;
        let merged_http = s3_payload(resolve_vended_storage(
            &result,
            &StorageBackend::S3(base_http),
            &anchor,
        ));
        assert!(
            merged_http.allow_http,
            "allow_http=true on base must carry through"
        );

        let mut base_https = static_storage();
        base_https.allow_http = false;
        let merged_https = s3_payload(resolve_vended_storage(
            &result,
            &StorageBackend::S3(base_https),
            &anchor,
        ));
        assert!(
            !merged_https.allow_http,
            "allow_http=false on base must carry through, unaffected by the same vended result"
        );
    }

    /// Scenario: the credential source is selected ONCE for `anchor` and that
    /// single selection feeds all six vended reads — not a per-key
    /// re-selection that could let some keys read the matched entry and others
    /// silently read the flat `config` map instead.
    ///
    /// The matched `storage_credentials` entry supplies only four of the six
    /// keys (access_key, secret_key, region, endpoint); the flat `config` map
    /// carries wrong sentinel values for all six. A single selection means the
    /// four present keys resolve to the entry's values and the two absent keys
    /// (session_token, path_style) fall through to `base` — never to config's
    /// wrong values, which would only be reachable through a second,
    /// independent selection per key.
    #[test]
    fn resolve_vended_storage_selects_credential_source_once_for_all_six_values() {
        let mut base = static_storage();
        base.session_token = Some("STATIC_TOKEN_SENTINEL".into());
        base.path_style = false;

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
                ("s3.path-style-access", "true"),
            ],
        );
        let anchor = result.metadata.location().to_string();

        let merged = s3_payload(resolve_vended_storage(
            &result,
            &StorageBackend::S3(base),
            &anchor,
        ));

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
            merged.session_token.as_deref(),
            Some("STATIC_TOKEN_SENTINEL"),
            "session_token absent from the matched entry must preserve static, not read config"
        );
        assert!(
            !merged.path_style,
            "path_style absent from the matched entry must preserve static, not read config"
        );
    }

    /// Scenario: an `Adls` backend is returned unchanged, byte-for-byte, from a
    /// `LoadTableResult` that carries vended S3 credentials. The effective
    /// backend is already selected once at `parse_creds`/`storage_block` time;
    /// vended Azure SAS credentials are a tracked exception (#276), so this
    /// function must not attempt any Azure-specific extraction or scheme
    /// switching — it must simply pass the `Adls` variant through.
    #[test]
    fn resolve_vended_storage_returns_an_adls_backend_unchanged() {
        let base = StorageBackend::Adls {
            account_name: "myaccount".into(),
            cred: AdlsCred::AccountKey("static-account-key".into()),
        };
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        let resolved = resolve_vended_storage(&result, &base, &anchor);

        assert_eq!(
            resolved, base,
            "an Adls backend must pass through unchanged"
        );
    }
}

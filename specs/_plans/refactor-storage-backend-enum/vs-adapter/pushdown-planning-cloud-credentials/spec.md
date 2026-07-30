# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode — embedding them into every per-shard scan spec.

## Background

<!-- DELTA:NEW -->
* This delta amends TWO clauses of the vended-consolidation scenario and nothing else. `vs-adapter/storage-backend-enum` (issue #274) changes what `resolve_vended_storage` takes and returns from `StorageProps` to `StorageBackend`; the sequence it owns, the single credential-source selection, the absence convention, and every resolved value are unchanged. Every other scenario of this feature is unchanged, and no Background bullet is superseded.
* The field-for-field guarantee is preserved by construction, not by re-verification: `select_credential_source` and `merge_vended_into_storage` keep their bodies verbatim inside the S3 arm, so the resolved payload is the same value the pre-refactor function returned for every response shape.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: One concept-level call resolves the effective scan storage from a loadTable response

* *GIVEN* the five-step vended sequence written out at its single call site — extract the STS key triple, merge it over the static storage, then conditionally override region, endpoint, and path-style — whose steps `extract_vended_keys`, `merge_vended_into_storage`, `extract_vended_region`, `extract_vended_endpoint`, `extract_vended_path_style`, `vended_config_value`, and `extract_s3_keys_from_config` are the mechanism, and whose two `pub` members sit on the pushdown façade because a probe test named them
* *WHEN* the planning layer resolves the effective storage for a table whose `loadTable` response has been fetched and for which `use_vended_credentials` is enabled
* *THEN* exactly ONE function, `resolve_vended_storage`, SHALL own the whole sequence, taking the `loadTable` response, the static storage backend, and the location anchor, and returning the effective `StorageBackend`
* *AND* `resolve_vended_storage` SHALL be the ONLY vended entry point reachable from outside the `lakehouse-catalog` crate; EVERY mechanism step SHALL be crate-private, and `extract_vended_keys` and `merge_vended_into_storage` SHALL NOT be reachable at `crate::adapter::pushdown::<name>` or from the `tests/` crate
* *AND* `extract_vended_keys`, `extract_vended_region`, `extract_vended_endpoint`, and `extract_vended_path_style` SHALL NOT survive as private functions either: each is a single-caller one-line read of one config key, so they SHALL be INLINED into `merge_vended_into_storage` as direct `vended_config_value(vended, "<key>")` calls, leaving `select_credential_source`, `merge_vended_into_storage`, and `vended_config_value` as the whole private mechanism
* *AND* the crate's own unit tests SHALL assert against the storage `resolve_vended_storage` returns, never against a mechanism step's tuple element or `Option` — a test that pins `extract_vended_keys`' signature re-creates one level down exactly the coupling that blocked this consolidation for two releases — and each such test SHALL be named for the observable storage outcome rather than for the step it used to call
* *AND* the credential-source selection — the longest `storage_credentials` entry whose non-empty `prefix` prefixes the location, else the flat `config` map — SHALL run EXACTLY ONCE per call and SHALL supply all six vended values, replacing the four independent selections the pre-consolidation code ran over the same response
* *AND* every vended value SHALL carry ONE absence convention, `Option`, where `None` means the key is absent OR its value is the empty string, applied uniformly to access key, secret key, session token, region, endpoint, and path-style, so no caller has to know that an empty string means "absent" for two of the six and `None` means it for the other four
* *AND* the returned `StorageBackend` MUST carry a payload field-for-field identical to the pre-consolidation `StorageProps` output for every response shape, including: an empty-string vended access key or secret key preserving the static one; an absent or empty `s3.session-token` preserving the static session token; an unparseable `s3.path-style-access` preserving the static `path_style`; a matched `storage_credentials` entry that omits a key preserving the static value WITHOUT falling back to the flat `config` map; and `allow_http` always taken from the static storage because no catalog vends it
* *AND* the resolved backend MUST be the SAME variant as the input backend, because vending overlays credential values onto a backend the caller already selected and never re-selects one
* *AND* the `use_vended_credentials` gate SHALL stay at the call site rather than becoming a parameter of `resolve_vended_storage`, because a boolean that switches a function between "do the work" and "return the input" is a decision the function declined to make
* *AND* the vended STS keys, the vended session token, and the catalog-auth secrets MUST NOT appear in any returned SQL string or error message
<!-- /DELTA:CHANGED -->

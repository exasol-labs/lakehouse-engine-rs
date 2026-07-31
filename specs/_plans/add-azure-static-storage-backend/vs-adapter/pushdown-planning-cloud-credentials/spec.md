# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode — embedding them into every per-shard scan spec.

## Background

<!-- DELTA:NEW -->
* **This delta adds ONE clause to the vended-consolidation scenario and nothing else.** `vs-adapter/storage-backend-enum` (issue #275) adds a second `StorageBackend` variant, which makes `resolve_vended_storage`'s `match` non-exhaustive. Every S3 behaviour of this feature is unchanged, no Background bullet is superseded, and no other scenario is touched.
* **TRACKED EXCEPTION — vended Azure credentials are deferred to issue #276 (slice D).** Slice C selects the backend at ONE site, `storage_block`, from the CONNECTION credential shape. An Azure table reached with `use_vended_credentials` enabled therefore reads with its STATIC credentials: the compile-forced `Adls` arm returns the caller's backend unchanged, and no vended SAS is extracted. This is a named, scoped gap, not a silent one. Issue #276 closes it by switching the variant from the table's location scheme after `loadTable` — the only place the scheme is known — and by extracting the vended SAS.
* **The Iceberg REST rule the deferral does not break.** `apache/iceberg` `open-api/rest-catalog-open-api.yaml` (main) states for `LoadTableResult`: "Credentials for ADLS / GCS / S3 / ... are provided through the `storage-credentials` field. Clients must first check whether the respective credentials exist in the `storage-credentials` field before checking the `config` for credentials." The passthrough arm reads NEITHER source for Azure rather than reading them in the wrong order, so the precedence rule is not violated — it is simply not yet exercised for ADLS. The longest-`prefix` selection rule (`StorageCredential.prefix`: "Clients should choose the most specific prefix (by selecting the longest prefix) if several credentials of the same type are available") is likewise untouched and stays implemented for S3.
* **Why a passthrough and not a rejection.** Returning the static backend unchanged is the reading that preserves the credentials the operator actually supplied; erroring on the combination would break an Azure CONNECTION that carries both static credentials and a `use_vended_credentials` flag left over from an S3 deployment, for no correctness gain.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: One concept-level call resolves the effective scan storage from a loadTable response

* *GIVEN* the five-step vended sequence written out at its single call site — extract the STS key triple, merge it over the static storage, then conditionally override region, endpoint, and path-style — whose steps `select_credential_source`, `merge_vended_into_storage`, and `vended_config_value` are the mechanism
* *WHEN* the planning layer resolves the effective storage for a table whose `loadTable` response has been fetched and for which `use_vended_credentials` is enabled
* *THEN* exactly ONE function, `resolve_vended_storage`, SHALL own the whole sequence, taking the `loadTable` response, the static storage backend, and the location anchor, and returning the effective `StorageBackend`
* *AND* `resolve_vended_storage` SHALL be the ONLY vended entry point reachable from outside the `lakehouse-catalog` crate, and EVERY mechanism step SHALL stay crate-private
* *AND* the credential-source selection — the longest `storage_credentials` entry whose non-empty `prefix` prefixes the location, else the flat `config` map — SHALL run EXACTLY ONCE per call and SHALL supply all six vended values
* *AND* every vended value SHALL carry ONE absence convention, `Option`, where `None` means the key is absent OR its value is the empty string, applied uniformly to access key, secret key, session token, region, endpoint, and path-style
* *AND* the returned `StorageBackend` MUST carry a payload field-for-field identical to the pre-consolidation `StorageProps` output for every response shape, including: an empty-string vended access key or secret key preserving the static one; an absent or empty `s3.session-token` preserving the static session token; an unparseable `s3.path-style-access` preserving the static `path_style`; a matched `storage_credentials` entry that omits a key preserving the static value WITHOUT falling back to the flat `config` map; and `allow_http` always taken from the static storage because no catalog vends it
* *AND* the resolved backend MUST be the SAME variant as the input backend, because vending overlays credential values onto a backend the caller already selected and never re-selects one
* *AND* the ADLS arm SHALL return the caller's backend UNCHANGED and MUST NOT read `storage_credentials` or `config` — a TRACKED EXCEPTION deferring vended Azure SAS to issue #276, which is the slice that switches the variant from the table location scheme; until then an Azure table reads with the static credentials the CONNECTION supplied, and this MUST NOT be implemented as a catch-all `_` arm, so a third backend is a build failure here rather than a silent second deferral
* *AND* the `use_vended_credentials` gate SHALL stay at the call site rather than becoming a parameter of `resolve_vended_storage`, because a boolean that switches a function between "do the work" and "return the input" is a decision the function declined to make
* *AND* the vended STS keys, the vended session token, the catalog-auth secrets, and any Azure account key or SAS token MUST NOT appear in any returned SQL string or error message
<!-- /DELTA:CHANGED -->

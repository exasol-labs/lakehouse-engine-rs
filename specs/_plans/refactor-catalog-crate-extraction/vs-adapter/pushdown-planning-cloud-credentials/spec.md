# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode — embedding them into every per-shard scan spec.

## Background

* SigV4 signing and credential vending are opt-in per CONNECTION (`use_sigv4`,
  `use_vended_credentials`); both default to false so existing MinIO/REST stacks
  behave exactly as before.
* On the SigV4/Glue path the adapter derives the REST catalog prefix by
  unconditionally prepending `catalogs/` to the configured bare-account-id
  `warehouse`, because AWS Glue's Iceberg REST catalog requires the prefix in the
  `catalogs/{catalogId}` form. The user-facing `warehouse` remains the bare AWS
  account id; the adapter appends `catalogs/` so the user never supplies it. This
  derivation extends the deliberate SigV4 `/v1/config` short-circuit already
  recorded in `specs/_decision/001-migrate-legacy-decision-log.md`; the
  `catalogs/{id}` shape is a Glue-proprietary convention, not an Apache Iceberg
  REST spec requirement. The derivation applies only to Glue — `CatalogAuth::Sigv4`
  is today exclusively the Glue path — and does NOT generalize to other SigV4-style
  catalogs such as S3 Tables (#123).
* **`use_vended_credentials` is orthogonal to catalog authentication.** Vended S3
  credential extraction is gated SOLELY on `use_vended_credentials`, never on the
  catalog-auth mode. It applies identically across all four auth modes: no-auth,
  static bearer `token`, OAuth2 client-credentials (`client_id` + `client_secret`),
  and AWS SigV4. The catalog-auth mode selects only how the table-load request is
  authenticated; it never gates whether vended creds are extracted.
* The adapter resolves the table once per query via a single self-issued
  `loadTable` GET whose authentication is chosen by the catalog-auth mode (SigV4
  signature | `Authorization: Bearer <token>` | OAuth2-grant-derived bearer |
  none). The raw `LoadTableResult` from that one request feeds BOTH Iceberg file
  planning AND vended-credential extraction. `iceberg-catalog-rest` 0.9.1's
  `RestCatalog::load_table` returns only a `Table` and drops the response
  `config`/`storage_credentials`, so it cannot surface vended creds — the
  self-issued GET is required for vending on every mode.
* When `use_vended_credentials` is false, no `loadTable` response field is read for
  credentials and the static storage credentials flow through unchanged on every
  auth mode.
* Credentials (signing keys, bearer tokens, OAuth2 client secrets, vended STS
  tokens) MUST NEVER appear in any returned SQL string or error message.
* See `vs-adapter/pushdown-planning` for the base pushdown planning scenarios and
  `vs-adapter/rest-catalog-oauth-auth` for the catalog-auth modes.
* Vended-credential resolution extends to an S3-compatible object store (MinIO
  behind Lakekeeper) whose endpoint is not the AWS default: the adapter adopts the
  vended `s3.endpoint` and `s3.path-style-access` in addition to the vended
  `client.region`, so a vended CONNECTION that carries no static S3 endpoint of its
  own can still reach the correct store. Surfaced as a genuine interop gap against a
  real Lakekeeper 0.13.1 + MinIO stack: the vended `loadTable` config carries
  `s3.endpoint` (e.g. `http://minio:9000/`) and `s3.path-style-access` (`true`), but
  the adapter previously preserved only the static endpoint/path-style, so the S3
  client defaulted to AWS and MinIO was unreachable. AWS S3 omits these config keys,
  so absence preserves the static values and the Glue vended path is unchanged.
* This delta adds ONE scenario: the five-step vended sequence collapses onto one concept-level entry point, `resolve_vended_storage`, published by the `lakehouse-catalog` crate (issue #204, absorbing issue #214). Every other scenario of this feature is unchanged, and the resolved `StorageProps` is field-for-field identical for every response shape, so no behavioral scenario needs editing.
* The consolidation is a DEDUPLICATION of the credential-source selection, not a change to it. The current sequence selects the source FOUR times — once inside `extract_vended_keys` and once per `vended_config_value` call for region, endpoint, and path-style — each time re-running the same longest-prefix match over the same `LoadTableResult`. One selection feeding all six values is the same answer computed once.
* Iceberg REST compliance, quoted from `apache/iceberg` `open-api/rest-catalog-open-api.yaml` (main). `StorageCredential.prefix`: "Indicates a storage location prefix where the credential is relevant. Clients should choose the most specific prefix (by selecting the longest prefix) if several credentials of the same type are available." `LoadTableResult`: "Credentials for ADLS / GCS / S3 / ... are provided through the `storage-credentials` field. Clients must first check whether the respective credentials exist in the `storage-credentials` field before checking the `config` for credentials." The enumerated AWS configuration keys are `client.region`, `s3.access-key-id`, `s3.secret-access-key`, and `s3.session-token`.
* Two readings of that normative text are deliberate and PRESERVED, not introduced, by the consolidation. First, "check `storage-credentials` before `config`" is read per CREDENTIAL SET, not per key: when a `storage_credentials` entry matches the table location, that entry is authoritative for all six values and a key it omits does NOT fall back to the flat `config` map. Second, `s3.endpoint` and `s3.path-style-access` are not in the spec's enumerated AWS list; they are read from the same maps under the spec's `additionalProperties: string` allowance, which is what makes an S3-compatible store reachable. Neither reading is a spec deviation, and neither changes here.
* Issue #214 is CLOSED AS SUBSUMED by this delta rather than executed first. Its defining constraint — land the consolidation with zero public-surface change — existed only because the pushdown façade was frozen; issue #204 redraws that façade in the same change, so executing #214 first would deliberately build an intermediate `pub(super)` shape whose only reason to exist is a constraint this plan deletes, and would keep `extract_vended_keys` and `merge_vended_into_storage` `pub` across one extra commit for no verification benefit.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: One concept-level call resolves the effective scan storage from a loadTable response

* *GIVEN* the five-step vended sequence written out at its single call site — extract the STS key triple, merge it over the static storage, then conditionally override region, endpoint, and path-style — whose steps `extract_vended_keys`, `merge_vended_into_storage`, `extract_vended_region`, `extract_vended_endpoint`, `extract_vended_path_style`, `vended_config_value`, and `extract_s3_keys_from_config` are the mechanism, and whose two `pub` members sit on the pushdown façade because a probe test named them
* *WHEN* the planning layer resolves the effective storage for a table whose `loadTable` response has been fetched and for which `use_vended_credentials` is enabled
* *THEN* exactly ONE function, `resolve_vended_storage`, SHALL own the whole sequence, taking the `loadTable` response, the static storage, and the location anchor, and returning the effective `StorageProps`
* *AND* `resolve_vended_storage` SHALL be the ONLY vended entry point reachable from outside the `lakehouse-catalog` crate; EVERY mechanism step SHALL be crate-private, and `extract_vended_keys` and `merge_vended_into_storage` SHALL NO LONGER be reachable at `crate::adapter::pushdown::<name>` or from the `tests/` crate
* *AND* `extract_vended_keys`, `extract_vended_region`, `extract_vended_endpoint`, and `extract_vended_path_style` SHALL NOT survive as private functions either: each is a single-caller one-line read of one config key, so they SHALL be INLINED into `merge_vended_into_storage` as direct `vended_config_value(vended, "<key>")` calls, leaving `select_credential_source`, `merge_vended_into_storage`, and `vended_config_value` as the whole private mechanism
* *AND* the crate's own unit tests SHALL assert against the `StorageProps` `resolve_vended_storage` returns, never against a mechanism step's tuple element or `Option` — a test that pins `extract_vended_keys`' signature re-creates one level down exactly the coupling that blocked this consolidation for two releases — and each such test SHALL be named for the observable storage outcome rather than for the step it used to call
* *AND* the credential-source selection — the longest `storage_credentials` entry whose non-empty `prefix` prefixes the location, else the flat `config` map — SHALL run EXACTLY ONCE per call and SHALL supply all six vended values, replacing today's four independent selections over the same response
* *AND* every vended value SHALL carry ONE absence convention, `Option`, where `None` means the key is absent OR its value is the empty string, applied uniformly to access key, secret key, session token, region, endpoint, and path-style, so no caller has to know that an empty string means "absent" for two of the six and `None` means it for the other four
* *AND* the returned `StorageProps` MUST be field-for-field identical to the pre-consolidation output for every response shape, including: an empty-string vended access key or secret key preserving the static one; an absent or empty `s3.session-token` preserving the static session token; an unparseable `s3.path-style-access` preserving the static `path_style`; a matched `storage_credentials` entry that omits a key preserving the static value WITHOUT falling back to the flat `config` map; and `allow_http` always taken from the static storage because no catalog vends it
* *AND* the `use_vended_credentials` gate SHALL stay at the call site rather than becoming a parameter of `resolve_vended_storage`, because a boolean that switches a function between "do the work" and "return the input" is a decision the function declined to make
* *AND* the vended STS keys, the vended session token, and the catalog-auth secrets MUST NOT appear in any returned SQL string or error message
<!-- /DELTA:NEW -->

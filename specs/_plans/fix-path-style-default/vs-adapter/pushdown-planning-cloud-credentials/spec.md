# Feature: Pushdown Planning — Cloud Credentials (SigV4 + Vended)

Resolves cloud credentials once in the pushdown planning layer: signs catalog requests with AWS SigV4 when enabled, and extracts short-lived vended S3 credentials from the `loadTable` response — orthogonally to the catalog-authentication mode — embedding them into every per-shard scan spec.

## Background

<!-- DELTA:NEW -->
* **This delta admits the CONNECTION's `path_style` into the CONNECTION-wins addressing rule (#130).** Changes behaviour only for a CONNECTION that STATES `path_style`; omitting it resolves the same value as before. AMENDS two clauses across two scenarios, SUPERSEDES one Background bullet.
* **SUPERSEDES "path_style does NOT participate in the CONNECTION-wins rule, and the reason is a type limitation".** Both premises removed by #130: the field is now `Option<bool>` (can express "unstated"), and the `true` default the bullet treated as shipped behaviour is the defect being fixed.
* **Three-step chain: CONNECTION stated value > vended `s3.path-style-access` > endpoint-presence derivation.** Same shape as `endpoint` and `region`. The derivation stays LAST (not deleted) because `register_side_store` gates `endpoint` on `path_style` (`object_store.rs:226-231`), so a vended endpoint nobody stated a style for must still reach the store.
* **`StaticStoreAddress` gains `path_style: Option<bool>` beside `endpoint` and `region`.** Non-`pub`, read through an accessor. The credential-disjointness probe is unchanged: `path_style` names none of the forbidden spellings.
* **Both vended selectors reach the new precedence through `s3_backend`.** Unity's `uc_vended_s3` already reduces to `path_style: None` into the same shared function, so the override reaches both catalog kinds with no second implementation.
* **Iceberg REST compliance unchanged.** `s3.path-style-access` appears nowhere in the spec; it is readable only under `additionalProperties: {type: string}`. Neither spec constrains client-side static-config precedence. Decision [9] records the evidence.
<!-- /DELTA:NEW -->

## Scenarios

### Scenario: Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set

* *GIVEN* a virtual schema whose CONNECTION credentials set `use_vended_credentials` to true
* *WHEN* the adapter issues the `loadTable` request to fetch vended credentials for an `s3://` table
* *THEN* the adapter SHALL send the `X-Iceberg-Access-Delegation: vended-credentials` request header so spec-compliant catalogs return vended credentials
* *AND* the adapter SHALL resolve the per-shard scan-spec storage `region` from the CONNECTION's `region` when that value is non-empty, and from the selected credential source's `client.region` otherwise; when NEITHER states one, the `region` SHALL be empty
* *AND* the adapter SHALL resolve the per-shard scan-spec storage `endpoint` from the CONNECTION's `endpoint` when that value is non-empty, and from the selected credential source's `s3.endpoint` otherwise; when NEITHER states one, the `endpoint` SHALL be empty
* *AND* the two SHALL be resolved INDEPENDENTLY, so a CONNECTION stating only a `region` beside a response stating only an `s3.endpoint` yields BOTH values rather than one source winning wholesale
* *AND* an S3 storage block whose `endpoint` and `region` are BOTH empty SHALL be produced successfully and MUST NOT be refused, because Databricks AWS vends short-lived credentials with no endpoint and no region field at all and the AWS default chain places that store
<!-- DELTA:CHANGED -->
* *AND* the adapter SHALL resolve `path_style` in THREE ordered steps: the CONNECTION's `path_style` when the CONNECTION STATES one, else the selected credential source's `s3.path-style-access` when the response states a value parseable as a boolean, else whether a store `endpoint` was RESOLVED at all under the two rules above — SUPERSEDING the recorded clause "the adapter MUST NOT read the CONNECTION's `path_style` here, because that field is a plain boolean with a `true` default and cannot express 'unstated'" (#130)
* *AND* "STATES one" SHALL mean the CONNECTION JSON carried a `path_style` key with a boolean value, so an ABSENT key SHALL fall through to the second step and a stated `false` SHALL WIN over a vended `true`
* *AND* the third step SHALL remain the LAST resort rather than being deleted, because `register_side_store` treats `path_style` as the gate on whether `endpoint` reaches the S3 builder
* *AND* `path_style` SHALL be resolved INDEPENDENTLY of `endpoint` and `region`
* *AND* a CONNECTION stating `path_style: false` beside a RESOLVED non-empty `endpoint` SHALL be honoured and MUST NOT be refused
<!-- /DELTA:CHANGED -->
* *AND* the vended region, endpoint, path-style, and keys SHALL be read from the SAME single credential source — the longest-matching `storage_credentials` entry's config, falling back to the flat `config` map only when no entry's non-empty `prefix` prefixes the location — so this delta changes what OVERRIDES a vended value and never which response map a vended value is read from

### Scenario: One concept-level call resolves the effective scan storage from a loadTable response

* *GIVEN* the vended sequence written out at its single call site — select the credential source for the location, then build the storage backend that source describes — whose steps `select_credential_source` and the shared per-backend construction functions are the mechanism
* *WHEN* the planning layer resolves the effective storage for a table whose `loadTable` response has been fetched and for which `use_vended_credentials` is enabled
* *THEN* exactly ONE function, `resolve_vended_storage`, SHALL own the whole sequence, taking the `loadTable` response, the location anchor, the resolved `ALLOW_HTTP` value, and the CONNECTION's configured store address, and returning `Result<StorageBackend, UdfError>`
* *AND* `resolve_vended_storage` MUST NOT take a storage backend, a `ConnectionCreds`, or any other value carrying a credential field as a parameter, so "no CONNECTION CREDENTIAL is read under vending" stays enforced by what its parameters CAN carry
<!-- DELTA:CHANGED -->
* *AND* the store-address parameter SHALL be a type declaring EXACTLY the CONNECTION's `endpoint`, `region`, and `path_style`, with exactly one conversion from `ConnectionCreds` declared beside it, and a source-level probe SHALL assert that its declaration names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password` — SUPERSEDING the recorded clause naming EXACTLY `endpoint` and `region` (#130)
* *AND* the type SHALL carry `path_style` as an OPTION of boolean, because the resolution chain branches on whether the CONNECTION stated a value
* *AND* every field of the type SHALL stay non-`pub` and SHALL be read through an accessor, and the existing probe asserting that non-`pub` property SHALL cover the added field without being rewritten to enumerate fields by name
<!-- /DELTA:CHANGED -->
* *AND* the `ALLOW_HTTP` parameter SHALL NOT be read as an exception to that rule: it carries one virtual-schema boolean resolved outside this crate, names no credential, and cannot supply one
* *AND* `resolve_vended_storage` SHALL be the ONLY Iceberg-path vended entry point reachable from outside the `lakehouse-catalog` crate, and EVERY mechanism step — the per-catalog wire extraction and the shared policy and construction functions alike — SHALL stay crate-private
* *AND* the credential-source selection — the longest `storage_credentials` entry whose non-empty `prefix` prefixes the location, else the flat `config` map — SHALL run EXACTLY ONCE per call, SHALL be the SAME scheme-agnostic selection for both backends, and SHALL supply every value the resolved backend carries
* *AND* a matched `storage_credentials` entry SHALL remain authoritative for the whole credential set: a key that entry omits MUST NOT fall back to the flat `config` map, because the Iceberg REST rule is read per credential SET rather than per key

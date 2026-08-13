# Plan: fix-vended-storage-shared-policy

## Summary

Move the vended-storage POLICY — the plaintext-transport consent gates, the S3 store-address rule, and both `StorageBackend` constructions — out of the two forked vended selectors into one shared home, and replace the "store address undetermined" hard error with a credentials/addressing split in which vended credentials stay vended-only and a CONNECTION-configured `endpoint`/`region` wins when set. Fixes issue #330: the Unity selector accepts plaintext `abfs://` with no operator consent, and can build an S3 store with no address at all.

## Design

### Context

`crates/lakehouse-catalog/src/vended.rs` and `crates/lakehouse-catalog/src/unity/vended.rs` resolve the SAME output (`StorageBackend`) from DIFFERENT wire inputs. Only `classify_vended_scheme` is shared; everything after it is forked. What was copied stayed consistent — scheme extraction, storage-host derivation, ADLS account-name derivation, the plaintext `http://` endpoint gate, the `path_style` default. What was not copied became a gap:

| Logic | Iceberg (`vended.rs`) | Unity (`unity/vended.rs`) |
|---|---|---|
| Scheme extraction | inline in `resolve_vended_storage` | `scheme_of` — copied |
| Storage host | `anchor_host` | `location_host` — byte-identical, renamed |
| ADLS account name from host | inline | inline — copied |
| Plaintext `http://` endpoint gate | present | present — copied |
| `path_style` default `endpoint.is_some()` | present | present — copied |
| **`abfs://` consent gate** | present | **absent → defect 1** |
| **Store-address check** | present | **absent → defect 2** |

Defect 1: `adls_from_vended` takes no `allow_http` parameter at all, so a plaintext `abfs://` location is always accepted and read over HTTPS — a silent scheme upgrade the operator never authorised.

Defect 2: `s3_from_vended` returns `StorageBackend::S3` with `region: String::new()` and an empty `endpoint` whenever the response vends no endpoint. Real Databricks AWS vends short-lived credentials with NO endpoint, and `AwsTempCredentials` carries no region field at all — exactly the state `s3_backend_from_vended`'s "store address undetermined" error rejects. Sharing the policy alone would therefore import an error that rejects legal Databricks tables.

Neither defect is live: reference search confirms `resolve_uc_vended_storage` has no production caller — only `unity/vended_tests.rs`, `tests/catalog_public_surface.rs`, and the crate-root re-exports. Delta scan execution reaches it in #319/#320, which is why this lands first: once that path is built on the forked selector, the supersede cost grows.

- **Goals** — one home for every vended policy rule both catalog kinds share; the `abfs://` gate unreachable-around; an empty S3 store address legal; the credential-vending guarantee preserved by a mechanism rather than by a signature that no longer carries it.
- **Non-Goals** — wiring `resolve_uc_vended_storage` into a scan path (#319/#320); extending the plaintext consent gate to the non-vended `storage_block` path; widening `ConnectionCreds.path_style` to an `Option<bool>`; changing `StorageProps`, `StorageBackend`, `AdlsCred`, or the scan-spec wire encoding; touching `select_credential_source`'s Iceberg REST longest-prefix rule.

### Decision

Fork on HOW a value is read off the wire. Do not fork on WHAT makes the resulting value acceptable.

#### Architecture

```
 Iceberg REST loadTable                    Unity temporary-table-credentials
 (flat HashMap<String,String>)             (typed TemporaryTableCredentials)
          │                                              │
          ▼                                              ▼
 vended.rs  EXTRACTION                     unity/vended.rs  EXTRACTION
 select_credential_source                  aws_temp_credentials
 iceberg_vended_s3 / _sas                  azure_user_delegation_sas
 (stays forked)                            (stays forked)
          │                                              │
          └──────────────► neutral values ◄──────────────┘
                    VendedS3 { access_key, secret_key,
                      session_token, region, endpoint, path_style }
                            or a SAS String
                                   │
                                   ▼
                     storage.rs  SHARED POLICY + CONSTRUCTION
                       scheme_of · location_host · adls_account_name
                       classify_vended_scheme  (unchanged)
                       s3_backend(...)   → plaintext-endpoint gate
                                         → CONNECTION-wins address rule
                                         → path_style derivation
                                         → StorageBackend::S3
                       adls_backend(...) → abfs:// consent gate
                                         → StorageBackend::Adls
                                   │
                                   ▼
                            StorageBackend
```

Each selector reduces to three steps: classify the location's scheme, read its own credential family off its own wire shape, hand the neutral values to the shared construction function. Neither selector names a `StorageBackend` variant afterwards, so the recorded exhaustive variant-naming module list SHRINKS from six to four.

#### Key interfaces

| Item | Home | Visibility |
|---|---|---|
| `StaticStoreAddress` — exactly two NON-`pub` fields (`endpoint`, `region`), `endpoint()`/`region()` accessors, `Default`, one `From<&ConnectionCreds>` | `storage.rs` | `pub` type with private fields, re-exported at crate root |
| `VendedS3 { access_key, secret_key, session_token, region: Option<String>, endpoint: Option<String>, path_style: Option<bool> }` | `storage.rs` | `pub(crate)` |
| `s3_backend(VendedS3, location: &str, allow_http: bool, address: &StaticStoreAddress) -> Result<StorageBackend, UdfError>` | `storage.rs` | `pub(crate)` |
| `adls_backend(sas: String, location: &str, allow_http: bool) -> Result<StorageBackend, UdfError>` | `storage.rs` | `pub(crate)` |
| `scheme_of(&str) -> String`, `location_host(&str) -> &str`, `adls_account_name(&str) -> Result<&str, UdfError>` | `storage.rs` | `pub(crate)` |
| `resolve_vended_storage(&LoadTableResult, anchor, allow_http, &StaticStoreAddress)` | `vended.rs` | `pub` (one new parameter) |
| `resolve_uc_vended_storage(&TemporaryTableCredentials, location, allow_http, &StaticStoreAddress)` | `unity/vended.rs` | `pub` (one new parameter) |

`lowercase_scheme` STAYS in `vended.rs`: it folds a whole URI's scheme for `select_credential_source`'s prefix match and is not the same function as `scheme_of`, which returns the scheme alone.

#### Address rule

For `endpoint` and `region` INDEPENDENTLY: the CONNECTION's value when non-empty, else the vended value, else empty. An S3 backend with both empty is returned successfully — the AWS default chain places it. `path_style` = the vended `s3.path-style-access` when the response states a parseable boolean, else whether an endpoint was RESOLVED at all. The plaintext gate applies to the RESOLVED endpoint whichever source supplied it.

#### Patterns

| Pattern | Where | Why |
|---|---|---|
| Extract–normalise–construct | selectors → `VendedS3` → `storage.rs` | Puts the fork at the wire boundary, where the two kinds genuinely differ, and nowhere else |
| Capability-narrowed parameter | `StaticStoreAddress` | A type that CANNOT carry a credential replaces a signature that merely did not |
| Single-owner conversion | `From<&ConnectionCreds> for StaticStoreAddress` | One decision, one place, for which CONNECTION fields may cross into vended resolution — enforced by the type's private fields, so a call site building it field-by-field does not compile rather than being caught by review |
| Three-level source probe | `catalog_public_surface.rs` | Replaces two mention sites that could drift with one construction check, per-selector dispatch checks, and a set-equality check binding them |

### Consequences

| Decision | Alternatives Considered | Rationale |
|---|---|---|
| Shared home is `storage.rs` | A new `vended_policy.rs` module | The enum's own module already owns "which module may name a variant"; putting construction there SHRINKS that list from six to four instead of adding a fifth entry. The issue names `storage.rs` explicitly. |
| Addressing arrives as `StaticStoreAddress` | Pass `&ConnectionCreds`; pass two bare `&str` params | `&ConnectionCreds` destroys the credential guarantee outright. Two adjacent `&str` params make an endpoint/region transposition compile silently. A named two-field type keeps the guarantee AND is the probe target. |
| `path_style` does not read the CONNECTION | Widen `ConnectionCreds.path_style` to `Option<bool>` | A plain `bool` defaulting to `true` cannot express "unstated", so admitting it would make the vended `s3.path-style-access` override unreachable. Widening the field ripples into the static path, whose `true` default is shipped behaviour. |
| Consent gates live INSIDE the construction functions | Gate at the classification step | A caller reaching a construction function directly is still gated. Gating at classification only protects callers that classify. |
| Probe repointed to three rules | Repoint to `storage.rs` alone | `storage.rs` alone would stop forcing each selector to dispatch every kind — weaker than today, and drift is what produced defect 1. |
| Shared error texts name no catalog kind | Keep per-selector texts | Two texts for one rule are two things to drift. Every existing assertion is on neutral tokens (the location, `ALLOW_HTTP`, the endpoint value), so a neutral text passes both suites. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/storage-backend-enum | CHANGED | `vs-adapter/storage-backend-enum/spec.md` |
| vs-adapter/unity-catalog-vended-credentials | CHANGED | `vs-adapter/unity-catalog-vended-credentials/spec.md` |
| vs-adapter/pushdown-planning-cloud-credentials | CHANGED | `vs-adapter/pushdown-planning-cloud-credentials/spec.md` |
| vs-adapter/catalog-crate-structure | CHANGED | `vs-adapter/catalog-crate-structure/spec.md` |
| vs-adapter/connection-credentials | CHANGED | `vs-adapter/connection-credentials/spec.md` |
| e2e-harness/cloud-e2e-harness | CHANGED | `e2e-harness/cloud-e2e-harness/spec.md` |
| e2e-harness/lakekeeper-e2e-harness | CHANGED | `e2e-harness/lakekeeper-e2e-harness/spec.md` |
| azure-e2e/azure-e2e-harness | CHANGED | `azure-e2e/azure-e2e-harness/spec.md` |

## Impact

Four operator-visible changes on the vended path, TWO of them breaking. The vending-DISABLED path is untouched.

**Breaking — a CONNECTION-configured store address now overrides a vended one.** A deployment that sets `use_vended_credentials: true` AND a non-empty `endpoint` or `region` in its CONNECTION previously had that CONNECTION value DISCARDED — the vended path read `s3.endpoint`/`client.region` only, so the two never competed; now the CONNECTION value wins. Operators relying on the vended endpoint must clear the CONNECTION's `endpoint`/`region`. The Lakekeeper and Azure vended fixtures are unaffected: their vended CONNECTIONs carry neither.

**Fixed — a vended response with no store address is no longer a plan-time failure.** A Databricks AWS or Unity Catalog table whose response vends a key pair but neither `client.region` nor `s3.endpoint` now resolves, falling back to the CONNECTION value or the AWS default chain. The "vended credentials … leave the store address undetermined" error is deleted.

**Fixed — a plaintext `abfs://` Unity Catalog location now requires `ALLOW_HTTP`.** Previously accepted silently and read over HTTPS. Latent today: that selector has no production caller until #319/#320.

**Breaking — a CONNECTION `endpoint` the vended path previously IGNORED now places the store, and a plaintext one is refused at plan time.** Today `s3_backend_from_vended` (`crates/lakehouse-catalog/src/vended.rs:114-168`) reads the endpoint from the response's `s3.endpoint` ALONE: the CONNECTION's `endpoint` is never read on the vended path, so it neither triggers the deleted store-address error nor falls through to the AWS default — it is discarded. Two configurations that work today therefore change. A CONNECTION carrying `use_vended_credentials: true` and a stale plaintext `endpoint` (`http://minio:9000`, the shape of every MinIO CONNECTION in this repo's fixtures) beside a response that vends an HTTPS `s3.endpoint` or a `client.region` becomes a plan-time `UdfError::User` while `ALLOW_HTTP` is false. Under `ALLOW_HTTP = 'true'` the same CONNECTION instead moves the store address off the vended endpoint onto the stale CONNECTION one and fails at read time. Operators must clear a CONNECTION `endpoint` they do not intend the vended scan to read through.

No wire-format change: the scan-spec `storage` encoding, `StorageProps`, `StorageBackend`, and `AdlsCred` are unedited, so every committed golden SQL and scan-spec JSON fixture passes without edits.

## Dependencies

None added; no dependency version changes. Ordering: this plan MUST land before #319/#320 (Delta scan execution), which is the first production caller of `resolve_uc_vended_storage`.

## Migration

| Current | New |
|---------|-----|
| `resolve_vended_storage(result, anchor, allow_http)` | `resolve_vended_storage(result, anchor, allow_http, &StaticStoreAddress::from(creds))` |
| `resolve_uc_vended_storage(vended, location, allow_http)` | `resolve_uc_vended_storage(vended, location, allow_http, address)` |
| Vended payload with no `client.region` and no `s3.endpoint` → `UdfError::User` | Resolves; address falls back to the CONNECTION, else empty (AWS default chain) |
| CONNECTION `endpoint` IGNORED under vending (never read; the vended `s3.endpoint` was the only source) | CONNECTION `endpoint` wins when non-empty; clear it to keep the vended endpoint |
| CONNECTION `region` IGNORED under vending (never read; the vended `client.region` was the only source) | CONNECTION `region` wins when non-empty; clear it to keep the vended region |
| `abfs://` Unity location accepted unconditionally | Requires `ALLOW_HTTP = 'true'` |

## Implementation Tasks

1. **Shared derivations (no behaviour change).**
   1.1 Add `scheme_of`, `location_host`, and `adls_account_name` to `storage.rs` as `pub(crate)`, moving the bodies of `vended.rs::anchor_host`, `unity/vended.rs::scheme_of`, and `unity/vended.rs::location_host` verbatim, and unifying the two ADLS account-name error texts into one neutral text naming the location and the host.
   1.2 Repoint `resolve_vended_storage`'s inline scheme computation, `adls_backend_from_vended`, `resolve_uc_vended_storage`, `s3_from_vended`, and `adls_from_vended` at the shared derivations; delete `vended.rs::anchor_host`, `unity/vended.rs::scheme_of`, and `unity/vended.rs::location_host`. Keep `vended.rs::lowercase_scheme`.
   1.3 Add `storage_tests.rs` unit tests for the three shared derivations, including the container-userinfo host case and the unlabelled-host account-name error.

2. **Shared construction and gates.**
   2.1 Add `pub(crate) struct VendedS3` and `pub(crate) fn adls_backend` to `storage.rs`, with the `abfs://` consent gate inside `adls_backend`, and one neutral refusal text naming the `abfs://` scheme, the location, and `ALLOW_HTTP`. [expert]
   2.2 Add `pub(crate) fn s3_backend` to `storage.rs` carrying the plaintext-endpoint gate, the address rule, and the `path_style` derivation; do NOT port the "store address undetermined" error. [expert]
   2.3 Add `pub struct StaticStoreAddress` with `Default` and `impl From<&ConnectionCreds>`, re-exported from `lib.rs`. Declare BOTH fields NON-`pub` with `pub fn endpoint(&self) -> &str` and `pub fn region(&self) -> &str` accessors, and have `s3_backend` read the address through those accessors, so outside the shared home `Default` and that ONE conversion are the only constructions the type admits and a field-by-field literal does not compile. [expert]
   2.4 Add `storage_tests.rs` unit tests for the address precedence matrix, the `path_style` composition matrix, the `abfs`/`abfss` gate, the plaintext-endpoint gate on a CONNECTION-supplied endpoint, and the both-empty-address success case.

3. **Repoint the Iceberg selector.** [expert]
   3.1 Extract `iceberg_vended_s3` and `iceberg_vended_sas` in `vended.rs` from the bodies of `s3_backend_from_vended` and `adls_backend_from_vended`, keeping the host-suffixed SAS selection and the required-key error texts byte-identical; delete both `*_from_vended` functions.
   3.2 Add the `StaticStoreAddress` parameter to `resolve_vended_storage`, move the `abfs` gate out of its `match` arm, and dispatch to `s3_backend`/`adls_backend`.
   3.3 Update `vended_tests.rs` per § Test Disposition.

4. **Repoint the Unity selector.**
   4.1 Extract `uc_vended_s3` and `uc_vended_sas` in `unity/vended.rs`, keeping the missing-credential error texts byte-identical; delete `s3_from_vended` and `adls_from_vended`.
   4.2 Add the `StaticStoreAddress` parameter to `resolve_uc_vended_storage` and dispatch to `s3_backend`/`adls_backend`; update its doc comment to state that credentials stay vended-only while addressing may come from the CONNECTION.
   4.3 Update `unity/vended_tests.rs` per § Test Disposition, adding the `abfs://` gate case and the both-empty-address success case.

5. **Repoint the source probes.** [expert]
   5.1 Generalise `storage_backend_variant_names()` into one extractor parameterised by enum name; add the `VendedBackendKind` extraction.
   5.2 Replace the two per-selector `StorageBackend`-variant probes with: one construction probe over `storage.rs`; one per-selector `VendedBackendKind`-dispatch probe each over `vended.rs` and `unity/vended.rs`; one set-equality probe over the two enums' extracted names.
   5.3 Add `static_store_address_is_reachable_and_declares_no_credential_field` — ONE test under ONE name, used unchanged in both § Verification rows below — constructing the type from the external vantage through `Default` and `From<&ConnectionCreds>` and asserting over `struct StaticStoreAddress`'s own declaration that it names no field spelled `access_key`, `secret_key`, `session_token`, `token`, `account_key`, `sas_token`, or `password`.
   5.4 Add `StaticStoreAddress` and its conversion to the `use` list and the arity-pin tests.
   5.5 Add `shared_vended_policy_steps_are_not_public`, asserting that no `CATALOG_SOURCES` file declares `pub fn s3_backend`, `pub fn adls_backend`, `pub fn scheme_of`, `pub fn location_host`, or `pub fn adls_account_name`, that none declares `pub struct VendedS3`, and that `lib.rs` re-exports none of the six. In the SAME task, correct `demoted_and_deleted_functions_are_not_declared_public`: `s3_backend_from_vended` is now a DELETED predecessor rather than a demoted mechanism step, so add its sibling `pub fn adls_backend_from_vended` beside it and rewrite the doc comment to state that the shared `s3_backend`/`adls_backend` in `storage.rs` replaced both and that their crate-privacy is asserted by `shared_vended_policy_steps_are_not_public`. Assert each shared step in exactly ONE of the two tests.
   5.6 Add `static_store_address_fields_are_not_public`, asserting from `storage.rs`'s production source that `struct StaticStoreAddress`'s declaration keeps BOTH fields non-`pub` — a later widening to `pub` fields would restore field-by-field construction outside the shared home and MUST fail a test rather than pass silently.

6. **Repoint the engine and E2E call sites.**
   6.1 `resolve_file_list` (`crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`) passes `&StaticStoreAddress::from(creds)`; update its doc comment's "reading no CONNECTION storage field" sentence to the credentials/addressing split.
   6.2 `probe_vended_credential` (`crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`) passes `&StaticStoreAddress::from(creds)`.
   6.3 `cloud_e2e_test.rs`: change the `client.region`/`s3.endpoint` assertion to a report, leaving the key-pair assertions hard.
   6.4 Add `vended_addressing_prefers_the_connection_endpoint_and_region` to `crates/lakehouse-engine/src/adapter/pushdown/file_resolution_tests.rs`, driving `resolve_file_list` at the layer that performs the change: a single-shot loopback catalog fake serving a `loadTable` response whose `location` is a non-empty `s3://` URI and whose metadata carries NO snapshot — `TableScanBuilder::build` returns an empty `TableScan` when `current_snapshot()` is `None`, so the call needs no object-store access — and whose `config` vends a key pair plus a `client.region` and `s3.endpoint` DIFFERENT from the CONNECTION's; call it with `use_vended_credentials: true` and a CONNECTION carrying a non-empty HTTPS `endpoint` and a non-empty `region`, and assert the returned effective `StorageBackend::S3` carries the CONNECTION's `endpoint` and `region` beside the VENDED key pair. Substituting `&StaticStoreAddress::default()` at `file_resolution.rs:262` MUST fail this test — no existing test or E2E suite can see that substitution, because both vended fixtures carry empty CONNECTIONs. [expert]

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3 |
| Group B | 2.1, 2.2, 2.3, 2.4 |
| Group C | 3.1, 3.2, 3.3 · 4.1, 4.2, 4.3 |
| Group D | 5.1, 5.2, 5.3, 5.4, 5.5, 5.6 · 6.1, 6.2, 6.3, 6.4 |

Sequential dependencies:
- Group A → Group B (the construction functions call the shared derivations)
- Group B → Group C (both selectors call the construction functions)
- Group C → Group D (the probes and call sites depend on the final signatures)

Intra-group order, stated for every group because three of the four are not flat:

- **Within Group A: 1.1 → 1.2 → 1.3.** Strictly sequential — 1.2 repoints at what 1.1 adds, and 1.3 tests it.
- **Within Group B: 2.3 → 2.1 · 2.2 → 2.4.** `StaticStoreAddress` lands FIRST because `s3_backend`'s signature names it; 2.1 and 2.2 are then independent of each other; 2.4 tests all three. 2.1, 2.2, and 2.3 all edit `storage.rs`, so they are never file-disjoint and this order is the one the expert set must execute in.
- **Within Group C** the two selectors are independent of each other and may run concurrently.
- **Within Group D** task 5 and task 6 are independent; inside task 5, 5.1 precedes 5.2 (it supplies the extractor), and inside task 6, 6.1 precedes 6.4 (the test drives the call site 6.1 repoints).

## Test Disposition

| Test | File | Disposition |
|---|---|---|
| `vended_storage_takes_region_endpoint_and_path_style_from_the_response_only` | `src/vended_tests.rs` | REWRITTEN and RENAMED. Part A keeps every assertion (vended region adopted, omitted endpoint empty, omitted path-style false) called with an EMPTY address, so it now proves vended addressing fills in while the CONNECTION is silent. Part B's store-address refusal is DELETED and replaced by an assertion that a both-empty address resolves successfully. |
| `unsatisfied_vended_request_errors_without_static_fallback` | `src/vended_tests.rs` | AMENDED: the "Neither region nor endpoint" block is DELETED. Every other block — absent key pair, empty-string key pair, absent SAS, wrong-host SAS, plaintext endpoint, plaintext `abfs://` — is UNCHANGED. |
| `resolve_vended_storage_unparseable_path_style_without_an_endpoint_is_false` | `src/vended_tests.rs` | MECHANICAL: empty address argument; assertions unchanged. |
| `vended_endpoint_without_path_style_stays_reachable_by_the_scan` | `src/vended_tests.rs` | MECHANICAL: empty address argument; assertions unchanged. |
| `vended_explicit_path_style_false_wins_over_the_endpoint_coupled_default` | `src/vended_tests.rs` | MECHANICAL: empty address argument; assertions unchanged. |
| `vended_s3`, `vended_user_error`, `vended_adls_sas` helpers | `src/vended_tests.rs` | MECHANICAL: gain an address parameter defaulting to empty, so every remaining test in the file keeps every assertion byte-identical. |
| All other tests in `src/vended_tests.rs` (33 tests) | `src/vended_tests.rs` | UNCHANGED assertions. The credential-source-selection tests — longest prefix, case-variant scheme, matched entry authoritative, flat-config fallback — MUST NOT weaken; they are this feature's Iceberg REST compliance evidence. |
| All six tests in `src/unity/vended_tests.rs` | `src/unity/vended_tests.rs` | MECHANICAL: empty address argument; assertions unchanged. `plaintext_endpoint_requires_allow_http` and `missing_matching_credential_is_error` assert only neutral tokens, so the shared neutral error texts satisfy them. |
| `vended_selector_source_names_every_storage_backend_variant`, `uc_vended_selector_source_names_every_storage_backend_variant` | `tests/catalog_public_surface.rs` | REPLACED by the four probes of task 5.2/5.3. The replacement is stronger, not weaker: construction, per-selector dispatch, and enum set-equality each fail independently. |
| `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend`, `resolve_uc_vended_storage_signature_takes_no_connection_value` | `tests/catalog_public_surface.rs` | AMENDED: gain the address argument; each gains an assertion that the parameter type carries no credential field. Neither may drop its existing arity pin. |
| `lakekeeper_vended_*` end-to-end tests | `tests/e2e_lakekeeper_test.rs` | UNCHANGED assertions plus the promoted empty-`region` assertion on the vended CONNECTION. This suite is the characterization gate. |
| `cloud_scan_reads_with_vended_credentials` | `tests/cloud_e2e_test.rs` | AMENDED: the `client.region`/`s3.endpoint` assertion becomes a report. The key-pair assertions stay hard. Env-gated; skips without AWS credentials. |
| `vended_addressing_prefers_the_connection_endpoint_and_region` | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution_tests.rs` | NEW (task 6.4). The only test at the layer that PERFORMS the change: it drives `resolve_file_list` with a non-empty CONNECTION `endpoint`/`region` against a response vending different ones, so substituting `&StaticStoreAddress::default()` at the production call site fails here and nowhere else. |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/lakehouse-catalog/src/vended.rs::anchor_host` | Replaced by the shared `storage.rs::location_host` |
| Function | `crates/lakehouse-catalog/src/vended.rs::s3_backend_from_vended` | Split into `iceberg_vended_s3` (extraction) + shared `s3_backend` (policy) |
| Function | `crates/lakehouse-catalog/src/vended.rs::adls_backend_from_vended` | Split into `iceberg_vended_sas` (extraction) + shared `adls_backend` (policy) |
| Function | `crates/lakehouse-catalog/src/unity/vended.rs::scheme_of` | Replaced by the shared `storage.rs::scheme_of` |
| Function | `crates/lakehouse-catalog/src/unity/vended.rs::location_host` | Byte-identical duplicate of `anchor_host`; replaced by the shared derivation |
| Function | `crates/lakehouse-catalog/src/unity/vended.rs::s3_from_vended` | Split into `uc_vended_s3` (extraction) + shared `s3_backend` (policy) |
| Function | `crates/lakehouse-catalog/src/unity/vended.rs::adls_from_vended` | Split into `uc_vended_sas` (extraction) + shared `adls_backend` (policy) |
| Error branch | `s3_backend_from_vended`'s "store address undetermined" `Err` | Superseded by the address rule; rejected legal Databricks AWS tables |
| Match guard | `resolve_vended_storage`'s `if scheme != "abfs" \|\| allow_http` arm and its `Err` arm | Moved into the shared `adls_backend` so both selectors are gated |
| Test | `vended_selector_source_names_every_storage_backend_variant` | Replaced by the repointed three-rule probe set |
| Test | `uc_vended_selector_source_names_every_storage_backend_variant` | Replaced by the repointed three-rule probe set |

`cargo clippy --workspace --all-targets -- -D warnings` is this crate's CI gate, so an item left without a caller is a build failure — that gate, not preference, forces each deletion in the same change.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| storage-backend-enum: Vended policy and construction move into the enum's own module | Unit | `crates/lakehouse-catalog/src/storage_tests.rs` | `shared_home_builds_both_backends_from_neutral_vended_values` |
| storage-backend-enum: The repointed source probe binds the shared home, both selectors, and both enums | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `shared_vended_home_constructs_every_storage_backend_variant`, `each_vended_selector_dispatches_every_vended_backend_kind`, `vended_kind_and_storage_backend_variant_sets_are_equal` |
| storage-backend-enum: The vended selectors take a store address that cannot carry a credential | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `static_store_address_is_reachable_and_declares_no_credential_field`, `static_store_address_fields_are_not_public` |
| unity-catalog-vended-credentials: An S3 vended response terminates in an S3 storage backend | Unit | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `s3_vended_response_terminates_in_s3_backend` |
| unity-catalog-vended-credentials: A vended plaintext endpoint is honored only with operator consent | Unit | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `plaintext_endpoint_requires_allow_http` |
| unity-catalog-vended-credentials: A plaintext abfs:// location is honored only with operator consent | Unit | `crates/lakehouse-catalog/src/unity/vended_tests.rs`, `crates/lakehouse-catalog/src/vended_tests.rs` | `abfs_location_requires_allow_http_on_the_unity_path`, `unsatisfied_vended_request_errors_without_static_fallback` |
| pushdown-planning-cloud-credentials: Vended S3 credentials are the sole storage source regardless of catalog auth mode | Unit | `crates/lakehouse-catalog/src/vended_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/file_resolution_tests.rs` | `vended_creds_are_the_sole_storage_source_in_spec`, `vended_creds_are_the_sole_storage_source_across_all_auth_modes`, `vended_addressing_prefers_the_connection_endpoint_and_region` |
| pushdown-planning-cloud-credentials: Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set | Unit | `crates/lakehouse-catalog/src/storage_tests.rs`, `crates/lakehouse-catalog/src/vended_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/file_resolution_tests.rs` | `store_address_resolves_endpoint_and_region_independently_with_the_connection_winning`, `path_style_composes_the_vended_override_with_the_resolved_endpoint`, `vended_request_sends_access_delegation_header`, `vended_addressing_prefers_the_connection_endpoint_and_region` |
| pushdown-planning-cloud-credentials: A vended-credentials request the catalog does not satisfy is a clear error | Unit | `crates/lakehouse-catalog/src/vended_tests.rs` | `unsatisfied_vended_request_errors_without_static_fallback`, `empty_vended_key_pair_is_a_missing_credential_not_a_licence_to_read_static` |
| pushdown-planning-cloud-credentials: One concept-level call resolves the effective scan storage from a loadTable response | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-catalog/src/vended_tests.rs` | `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend`, `resolve_vended_storage_selects_credential_source_once_for_all_six_values` |
| catalog-crate-structure: The vended store-address type extends the crate's public surface through an explicit reviewed edit | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `static_store_address_is_reachable_and_declares_no_credential_field`, `static_store_address_fields_are_not_public`, `shared_vended_policy_steps_are_not_public` |
| connection-credentials: Static storage credentials are ignored, not rejected, when vending is requested | Unit | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution_tests.rs`, `crates/lakehouse-catalog/src/vended_tests.rs` | `vended_addressing_prefers_the_connection_endpoint_and_region`, `vended_creds_are_the_sole_storage_source_in_spec` |
| cloud-e2e-harness: Vended credentials are exercised end to end against Glue | Integration (E2E, env-gated) | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_scan_reads_with_vended_credentials` |
| lakekeeper-e2e-harness: End-to-end scan over a vended-credential Lakekeeper warehouse returns correct rows | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_warehouse_scan_returns_rows` |
| azure-e2e-harness: no scenario changes | n/a | n/a | Background-only delta; existing coverage unchanged |

Every scenario is pure computation over strings, maps, and typed response structs with no I/O, so unit tests in the sibling `_tests.rs` files are the correct form per `/speq:code-guardrails`; the two E2E suites are the characterization gate for the behavioural change.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Shared policy + both selectors | `cargo test -p lakehouse-catalog` | 0 failures; the credential-source-selection tests all pass unedited |
| Public surface + repointed probes | `cargo test -p lakehouse-catalog --test catalog_public_surface` | 0 failures; every repointed and added probe passes — construction, per-selector dispatch, enum set-equality, credential-field absence, non-`pub` address fields, and the crate-privacy of the shared steps |
| Vended addressing end to end (MinIO, vended endpoint wins while the CONNECTION is silent) | `EXASOL_CONTAINER=lakehouse-engine-rs-2-exasol-1 make test-e2e 2>&1 \| tee /tmp/e2e.log; echo "exit=$?"` | Exit 0; the Lakekeeper vended-warehouse scan returns the seeded rows |
| Glue vended path (address may come from the CONNECTION) | `cargo test -p lakehouse-engine --test cloud_e2e_test -- --nocapture` | Skips without AWS credentials; with them, the scan returns rows and the vended `client.region`/`s3.endpoint` presence is REPORTED, not asserted |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `EXASOL_CONTAINER=lakehouse-engine-rs-2-exasol-1 make test-e2e` | 0 failures |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |

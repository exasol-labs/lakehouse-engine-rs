# Plan: change-vended-storage-resolution-scheme-driven

## Summary

Under `use_vended_credentials`, take the effective scan storage solely from the `loadTable` response and the backend variant solely from the table location's URI scheme. A vended request the catalog does not satisfy becomes a clear error, closing issue #276 and deliberately changing shipped S3 vended behaviour.

## Design

### Context

Issue #276 asked for one thing: an Azure SAS extractor beside the existing S3 one, "additive, static path and S3 unaffected". That framing does not survive contact with the code.

`storage_block` (`crates/lakehouse-engine/src/adapter/connection.rs:258`) selects the backend from the CONNECTION credential shape. A vended-only Azure CONNECTION supplies no `account_name`, no `account_key`, and no `sas_token` — so `storage_block` falls through to `S3`, and an Azure extractor placed in `resolve_vended_storage`'s existing `Adls` arm would be dead code for the primary vended use case. The scheme is the only input that knows an `abfss://` table is Azure, and the scheme is known only after `loadTable`.

Selecting the variant from the scheme forces the rest. Once the variant no longer follows the CONNECTION, keeping the CONNECTION's storage *values* would mean overlaying credentials from one source onto transport config from another, with a per-field precedence nobody declared. The shipped S3 arm already carries six such per-field preservation rules, and each one is a place a vended request silently reads a static credential.

The interview resolved this as one strict rule for both arms. `decision-log.md` carries the exchange.

- **Goals** — one rule for both backends; no CONNECTION storage field read under vending; the backend selected from the location scheme alone; a missing vended credential reported as a clear `UdfError::User`, never a silent fallback; the `#276` tracked exception discharged from every spec that cites it.
- **Non-Goals** — vended-SAS E2E against a live Lakekeeper ADLS warehouse, which stays in **issue #278** (slice F); a spec clause about the static-SAS TTL ceiling, declined twice in the interview; remote signing (`s3.remote-signing-enabled`), never implemented and unchanged here except that it now fails loud; Azurite / plain-HTTP Azure endpoints; any change to the vending-DISABLED path on any auth mode.

### Decision

Two total selectors, one per credential mode, on disjoint inputs, chosen by one boolean at one site.

#### Architecture

```
                       resolve_file_list  (the ONE decision point)
                                │
              use_vended_credentials ?
                    │                       │
                  false                   true
                    │                       │
      storage_block(creds, allow_http)   resolve_vended_storage(result, anchor, allow_http)
      reads: CONNECTION shape            reads: loadTable response + anchor scheme
                                         (allow_http = VS property, not CONNECTION)
      ── STATIC selector ──              ── VENDED selector ──
                    │                       │
                    └────────► StorageBackend ◄───┘
                                │
                    secret_values() / file_io() / scan spec
```

`resolve_vended_storage` loses its `base: &StorageBackend` parameter. That deletion is the design, not a tidy-up: with no CONNECTION-derived value in the signature, "no CONNECTION storage field is read under vending" is enforced by the compiler rather than by auditing the body. It gains a resolved `ALLOW_HTTP` boolean, which is a virtual-schema property rather than a CONNECTION field and so leaves that guarantee intact.

The vended selector, in full:

1. Read the scheme of `anchor`. `s3://`/`s3a://` → S3; `abfss://`/`abfs://` → ADLS; anything else, including a scheme-less `warehouse` fallback → `UdfError::User` naming the scheme.
2. Select the credential source ONCE with the existing `select_credential_source`, unchanged and already scheme-agnostic.
3. **S3 arm** — require a non-empty `s3.access-key-id` and `s3.secret-access-key`, else error. Read `s3.session-token`, `client.region`, `s3.endpoint`, `s3.path-style-access` from the same source; absent means absent. Require at least one of `client.region` and `s3.endpoint`, else error: nothing else can place the store. Take `allow_http` from the resolved `ALLOW_HTTP` virtual-schema property, and error when the vended endpoint is plain-`http://` while `ALLOW_HTTP` is false.
4. **ADLS arm** — recover `<host>` from each `adls.sas-token.<host>` key, select the one equal to the anchor's host (read after any `<container>@` userinfo), else error. Derive `account_name` from that host's first dot-separated label. Assemble `AdlsCred::Sas`. Error on an `abfs://` (plaintext) anchor when `ALLOW_HTTP` is false.

`ALLOW_HTTP` is threaded in as its own `bool` parameter. It is a virtual-schema property, not a CONNECTION field, so it does not reopen what deleting `base` closed — and it follows the convention already established for `s3_max_connections` and the DataFusion tuning knobs, which travel this same path as resolved scalars.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Selector pair on disjoint inputs | `storage_block` / `resolve_vended_storage` | One decision point survives while each mode reads only its own input; neither can override the other |
| Parameter deletion as invariant | `resolve_vended_storage`'s dropped `base` | Turns a body-level rule into a signature-level property |
| Operator consent for transport downgrade | `ALLOW_HTTP` gates a vended `http://` endpoint and an `abfs://` anchor | A catalog must not be able to move credentials to plaintext on its own authority; the property defaults to false |
| Total function over the scheme string | scheme → variant | The catch-all is REQUIRED, not forbidden: the match is over a scheme, not over `StorageBackend`, so a third variant compiles here and needs a source probe instead |
| Fail loud at plan time | every new error path, including a join whose sides differ | The alternative reads data through the wrong credentials or the wrong store |

#### Design-philosophy check

The new abstraction is a *narrower* interface over *more* hidden work, so the Quick Diagnostic passes on the axes it touches. `resolve_vended_storage` keeps three parameters but exchanges a whole `StorageBackend` for one boolean, drops six per-field absence conventions to none, and absorbs variant selection it previously delegated to its caller's caller. Calling it stays far easier than reimplementing it. The one boundary question — "is there exactly one module that owns each significant design decision?" — is answered explicitly rather than assumed: `decision-log.md` [1] argues why two selectors on disjoint inputs is one decision and not two, and names the alternatives rejected.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Strict rule on BOTH arms | Azure-only extractor, S3 untouched (issue #276's text) | Two rules on one credentials path is the ambiguity the change exists to remove; the interview chose uniform |
| Variant from the anchor scheme | Variant from the CONNECTION shape (shipped) | A vended-only Azure CONNECTION names no Azure field, so the shipped selector cannot reach the Azure arm at all |
| Delete the `base` parameter | Keep it and read only `allow_http`; add a `StorageBackend::allow_http()` accessor | Either leaves one CONNECTION-derived read under vending — the exact coupling the rule removes |
| Thread `allow_http: bool` as its own parameter | Derive it from the vended endpoint's scheme | The derivation is a security regression in the default configuration, where `ALLOW_HTTP` is absent and the shipped rule permits plaintext to no endpoint at all |
| Guard a join on variant + `account_name` only | Guard on full backend equality; leave the collapse unguarded | Full equality could break every vended join against a catalog minting per-table STS keys (unverified); unguarded reads an ADLS dimension through an S3 store |
| Require a vended region OR endpoint | Leave an absent region empty | An empty region silently misroutes an AWS store to a region-less URL; the interview's own principle is "requested but not satisfied is an error" |
| Static storage fields ignored, not rejected | Reject a CONNECTION carrying both | A SigV4 CONNECTION legitimately carries static keys for catalog signing; rejecting breaks that shape |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-cloud-credentials | CHANGED | `vs-adapter/pushdown-planning-cloud-credentials/spec.md` |
| vs-adapter/connection-credentials | CHANGED | `vs-adapter/connection-credentials/spec.md` |
| vs-adapter/storage-backend-enum | CHANGED | `vs-adapter/storage-backend-enum/spec.md` |
| e2e-harness/cloud-e2e-harness | CHANGED | `e2e-harness/cloud-e2e-harness/spec.md` |
| e2e-harness/lakekeeper-e2e-harness | CHANGED | `e2e-harness/lakekeeper-e2e-harness/spec.md` |

Features checked and deliberately NOT amended:

- **`datafusion-scan/scan-execution-memory-and-credentials`** — issue #274 already restated its two credential-passthrough scenarios backend-agnostically ("registers the object store the carried storage backend names"), so a carried ADLS backend holding a vended SAS travels the identical path.
- **`e2e-harness/azure-e2e-harness`** — its scenario sets `use_vended_credentials: false`, and its Background already assigns the vended sibling case to a later slice. That statement stays accurate under #278.
- **`vs-adapter/catalog-crate-structure`** — `resolve_vended_storage` keeps its name, its `pub` status, and its position as the crate's only vended entry point; every mechanism step stays crate-private.

## Impact

**Breaking, on one shipped path.** A CONNECTION that sets `use_vended_credentials` and relies on its static `endpoint`, `region`, `path_style`, `access_key`, `secret_key`, or `session_token` reaching the scan will stop working. The two in-repo vended paths land on opposite sides of that line:

- **Lakekeeper vended (MinIO) — unaffected.** Its CONNECTION already supplies an empty `endpoint`, `region`, `access_key`, and `secret_key`, so there was never a static value to preserve. Lakekeeper vends `s3.endpoint` (`http://minio:9000/`) and `s3.path-style-access` (`true`), which satisfies the address rule, and the harness's `ALLOW_HTTP = 'true'` (`crates/lakehouse-engine/tests/common/e2e_harness.rs:270`) satisfies the plaintext consent gate. This path is the change's characterization gate.
- **Glue vended (AWS) — at risk across the WHOLE vended credential set, and NOT verified.** `catalog_connection_password_vended` is `catalog_connection_password()` plus one flag, so that CONNECTION carries a static `access_key`, `secret_key`, AND `session_token` from the AWS env, plus a static `region` that `use_sigv4` requires (`crates/lakehouse-engine/tests/cloud_e2e_test.rs:139-159`). The shipped preservation rule preserves all four. **`cloud_scan_reads_with_vended_credentials` passing today is therefore compatible with Glue vending nothing at all** — the scan would read with the test's own static AWS keys, and the green test evidences neither the keys nor the address. Under the strict rule an absent vended key pair kills every Glue vended virtual schema at plan time. Four keys are at stake, not one: `s3.access-key-id`, `s3.secret-access-key`, the address (`client.region`, since Glue vends no `s3.endpoint`), and `s3.session-token` — whose absence beside a vended *temporary* key pair now yields `None` instead of the preserved static token and fails at read time rather than plan time. **None of this could be verified during planning**: the path needs live AWS credentials and the suite skips here. Task 4.2 makes all four falsifiable; if any assertion fails, the rule's premise is falsified and the decision returns to the interview rather than being patched around.
- **Databricks Unity Catalog — at risk, and no in-repo suite can observe it.** `specs/mission.md` Core Capability 7 makes Databricks-managed Iceberg first-class through this exact path, and `vended.rs`'s flat-`config` fixture is named for the Unity Catalog shape. A Unity Catalog response vending a key pair but neither `client.region` nor `s3.endpoint` now fails at plan time with the same clear address error. There is no `databricks-e2e` suite, so this one has no gate at all.

**Join blast radius, newly introduced by per-location selection.** Scheme-driven selection is per table location, but `CommonScanSpec.storage` is one whole-spec value: `resolve_one_join_side` resolves each side's own `effective_storage` and `join_fan_out_scan_spec` (`joins/sql_builders.rs:556`) keeps only `primary`'s. That collapse was variant-safe while both sides took their variant from one `storage_block` output; it is not once the variant follows each side's own location. An `s3://` fact joined to an `abfss://` dimension would run the S3 arm over the dim's Azure files, and two `abfss://` sides on different accounts would read the dim through the fact's account and SAS. Task 2.2 adds the plan-time guard. A **pre-existing** and separately-scoped defect is named but NOT fixed: the same collapse already discards a per-prefix vended *credential* difference today, and widening the guard to full backend equality could break every vended join against a catalog minting per-table STS keys — unverified either way, so it is recommended as its own tracked issue rather than folded in here.

Two secondary consequences, both loud rather than silent:

- A remote-signing warehouse (`s3.remote-signing-enabled`) vends no access key and now reports a clear plan-time error. Previously it fell through to whatever static credentials the CONNECTION held. Remote signing was never implemented; the error stops concealing that.
- A vended plain-`http://` endpoint, or an `abfs://` anchor, now requires `ALLOW_HTTP = 'true'` and is otherwise a clear error. This tightens rather than loosens: `ALLOW_HTTP` defaults to false, so a catalog cannot move vended credentials onto plaintext transport on its own authority. An earlier draft derived the permission from the vended endpoint's scheme and described that as "strictly narrower"; the claim was false in the default configuration and the derivation is withdrawn.

No wire-format change: `StorageProps`, `AdlsCred`, the variant set, and the externally-tagged `{"s3":…}` / `{"adls":…}` encoding are untouched, so every committed golden SQL and scan-spec JSON fixture passes unedited.

## Dependencies

None added; no dependency version changes. `UdfError` is already in scope in `lakehouse-catalog` (`exasol_udf_sdk::error::UdfError`, used by `session.rs`).

Issue #278 (slice F) owns vended-SAS E2E against a live Lakekeeper ADLS warehouse and is not a prerequisite.

## Migration

| Current | New |
|---------|-----|
| `resolve_vended_storage(result, base, anchor) -> StorageBackend` | `resolve_vended_storage(result, anchor, allow_http) -> Result<StorageBackend, UdfError>` |
| Variant follows `storage_block`'s CONNECTION-shape choice | Variant follows the anchor's URI scheme |
| Absent vended value preserves the static one, per field | Absent vended value is absent; a missing credential or address is an error |
| `allow_http` reaches the S3 payload via `storage_block` | `allow_http` threaded as its own resolved parameter, and gating plaintext under vending |
| Azure arm returns `base` unchanged (#276 tracked exception) | Azure arm extracts the host-matched vended SAS |
| A join collapses both sides onto `primary.effective_storage` unchecked | A join whose sides differ in variant or ADLS account is rejected at plan time |

Operator-facing migration: a vended CONNECTION that carries static storage fields keeps working only if the catalog vends what the scan needs. The fields are ignored, not rejected, so no CONNECTION has to be rewritten to be accepted — only to be *served*, and a shortfall is now a named error rather than a wrong-credential read.

## Test Disposition

Referenced by `vs-adapter/pushdown-planning-cloud-credentials`' Background. All 27 existing tests in `crates/lakehouse-catalog/src/vended.rs` that exercise `resolve_vended_storage`, and what happens to each. Deleting `base` and returning `Result` changes EVERY call expression, so no row is literally unedited; "KEEP" means every assertion is unchanged. Two invariants bound every row: the source-selection assertions (longest prefix wins; a matched entry is authoritative for the whole set and never falls back per-key to the flat `config` map) are the Iceberg REST compliance evidence and MUST NOT weaken; and no assertion about the vending-DISABLED path may change.

| Test | Disposition |
|------|-------------|
| `vended_storage_prefers_storage_credentials_over_flat_config` | KEEP. Fixture gains `client.region`; every assertion unchanged |
| `vended_storage_longest_matching_prefix_wins` | KEEP. Each entry gains `client.region`; every assertion unchanged |
| `vended_storage_falls_back_to_flat_config` | KEEP. Flat config gains `client.region`; every assertion unchanged |
| `vended_storage_uses_flat_config_when_no_storage_credentials` | KEEP. Fixture gains `client.region`; every assertion unchanged |
| `vended_storage_adopts_endpoint_and_path_style_from_flat_config` | KEEP — call updated to the new arity and `Result`; fixture already carries `s3.endpoint`; every assertion unchanged |
| `vended_storage_adopts_endpoint_from_storage_credentials` | KEEP — call updated to the new arity and `Result`; entry already carries `s3.endpoint`; every assertion unchanged |
| `oauth2_path_extracts_vended_credentials` | KEEP — call updated to the new arity and `Result`; fixture already carries `client.region`; every assertion unchanged |
| `vended_request_sends_access_delegation_header` | KEEP unedited. Does not call `resolve_vended_storage` |
| `vended_sts_values_not_in_error_messages` | KEEP unedited |
| `vended_storage_anchor_is_the_s3_table_location` | RESTATE, STRENGTHENED. Its HTTPS-anchor half asserted a silent fall-back to the flat config; it now asserts an unsupported-scheme error |
| `vended_creds_override_static_in_spec` | RESTATE. Three vended-key assertions kept; four "preserved from static" assertions DELETED; renamed to drop "override static" |
| `vended_storage_session_token_overrides_static` | RESTATE. Asserts the vended token is adopted; drops the "replaces the old static one" framing |
| `vended_overrides_static_across_all_auth_modes` | RESTATE. Per-mode vended-key assertions kept; "endpoint / path_style / allow_http preserved" assertions DELETED |
| `bearer_token_path_extracts_vended_from_config` | RESTATE. Vended-key assertions kept; "endpoint preserved" assertion DELETED |
| `vended_storage_adopts_region_from_flat_config` | RESTATE. Part A (vended region adopted) kept; Part B ("absent → static region preserved") becomes the undetermined-address error |
| `resolve_vended_storage_matched_entry_missing_key_does_not_fall_back_to_config` | RESTATE, core PRESERVED. The flat config's wrong secret must still never appear; the "preserve static" half becomes the missing-credential error |
| `resolve_vended_storage_selects_credential_source_once_for_all_six_values` | RESTATE, core PRESERVED. Single-selection and matched-entry-authoritative kept; two "preserve static, not read config" assertions become "absent → absent, and never the flat config's value" |
| `no_vending_no_sigv4_uses_static_storage_unchanged` | RESTATE. Disabled-path assertions unchanged; the incidental sub-call's fixture gains `client.region` |
| `vending_disabled_keeps_static_creds` | RESTATE. Disabled-path assertions unchanged; its empty-key sub-call now asserts the missing-credential error |
| `vending_disabled_uses_static_on_every_mode` | RESTATE. Disabled-path assertions unchanged; sub-call fixture already carries `client.region` |
| `resolve_vended_storage_empty_access_key_preserves_static` | REPLACE. Empty vended access key → missing-credential error |
| `resolve_vended_storage_empty_secret_key_preserves_static` | REPLACE. Empty vended secret key → missing-credential error |
| `resolve_vended_storage_absent_session_token_preserves_static` | REPLACE. Absent or empty session token → `None` |
| `resolve_vended_storage_unparseable_path_style_preserves_static` | REPLACE. Unparseable `s3.path-style-access` → `false` |
| `vended_storage_keeps_static_endpoint_and_path_style_when_absent` | DELETE. Its premise — absence preserves static — is the rule this plan removes |
| `resolve_vended_storage_allow_http_always_from_base` | REPLACE. `allow_http` comes from the threaded `ALLOW_HTTP` parameter; a vended plain-`http://` endpoint with `allow_http` false errors |
| `resolve_vended_storage_returns_an_adls_backend_unchanged` | REPLACE. Its premise is the discharged #276 exception; superseded by the Azure extraction tests |

## Implementation Tasks

1. **Catalog crate — the vended selector**
   - [ ] 1.1 Rewrite `resolve_vended_storage` in `crates/lakehouse-catalog/src/vended.rs`: drop the `base` parameter, return `Result<StorageBackend, UdfError>`, select the variant from the anchor's URI scheme with the REQUIRED catch-all arm returning `UdfError::User` naming the unsupported scheme, per § Design > Patterns' total-function row, and rewrite the doc comment to state the two-selector design intent instead of the S3-only anchor claim and the `#276` exception. Keep `select_credential_source` byte-identical. [expert]
   - [ ] 1.2 Build the S3 arm: required key pair, the region-or-endpoint address rule, `allow_http` taken from the threaded parameter, an error when the vended endpoint is plain-`http://` while `allow_http` is false, and absent-means-absent for session token, region, endpoint, and path-style. Replace `merge_vended_into_storage` with a construct-from-vended reader; keep `vended_config_value` as the single absence convention. [expert]
   - [ ] 1.3 Build the ADLS arm: recover `<host>` from each `adls.sas-token.<host>` key, match it against the anchor's host read after any `<container>@` userinfo, derive `account_name` from the host's first dot-separated label, and assemble `AdlsCred::Sas`. Error when no key matches, when the host yields no label, and when the anchor is `abfs://` while `allow_http` is false. Place the anchor host in the missing-SAS error where `redact_credentials` will not truncate it. [expert]
   - [ ] 1.4 Update `crates/lakehouse-catalog/src/test_support.rs`: refresh `static_storage`'s "Consumers" doc comment now that `vended` no longer needs a base fixture. Leave `static_backend` (still used by `namespace.rs`).

2. **Engine call site and the join guard**
   - [ ] 2.1 Propagate the new `Result` at `resolve_file_list`'s single call site in `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`, and correct the two S3-only anchor comments — the call-site block comment asserting the anchor "must be an S3 URI" and "can never match an S3 prefix", and `resolve_file_list`'s own doc paragraph describing the vended merge over static props.
   - [ ] 2.2 Reject at plan time a join whose sides do not all resolve to the same backend, with the comparison in a PURE function so it is testable without a catalog. Add a `pub(super) fn validate_sides_share_one_backend(sides: &[ResolvedJoinSide]) -> Result<(), UdfError>` to `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs`, beside `select_broadcast_sides` — documented there as "the pure, catalog-free core of side selection so it is unit-testable without a live Iceberg catalog" (`planning.rs:289`), which is this module's established convention for exactly this problem. It compares every side's `effective_storage` against the first side's — variant, and for `Adls` the `account_name` — and returns a `UdfError::User` naming the differing variants and storage accounts and no credential value. Call it from `plan_join` immediately after the per-side resolution loop (`joins/mod.rs:126-139`) and BEFORE the empty-side shortcut. Putting the comparison inline in `plan_join` was rejected: `plan_join` takes a `&CatalogSession` and reaches the divergent backends only by awaiting `resolve_one_join_side`, so the two backends are OUTPUTS of live catalog I/O and no unit test could supply them — the guard would ship with no falsifiable gate. Scope the comparison to variant and account only; do NOT compare full backend equality (see § Impact for the pre-existing per-prefix collapse this deliberately leaves alone). [expert]
   - [ ] 2.3 Thread the resolved `ALLOW_HTTP` value to the vended selector: widen `resolve_connection_config` to return it (2 call sites in `adapter/mod.rs`) so it is read once, then pass it as a `bool` alongside the existing virtual-schema scalars through `handle_pushdown`, `plan_join`, `resolve_one_join_side`, and `resolve_file_list` into `resolve_vended_storage`. Update the four test and harness call sites of `resolve_file_list`.
   - [ ] 2.4 Recommend a tracked GitHub issue for the pre-existing per-prefix vended-credential collapse in `join_fan_out_scan_spec`, citing that verifying whether any target catalog vends per-prefix credentials for two tables in one warehouse requires a live catalog. Do not fix it in this plan.

3. **Unit tests**
   - [ ] 3.1 Apply § Test Disposition to `vended.rs`'s existing tests: the KEEP, RESTATE, REPLACE, and DELETE rows exactly as tabled, changing no source-selection assertion and no disabled-path assertion. [expert]
   - [ ] 3.2 Add scheme-selection tests: `s3://`, `s3a://`, `abfss://`, `abfs://`, an HTTPS anchor, and a warehouse-style anchor substituted for an empty table location per `file_resolution.rs`'s fallback (a bare-account-id string with no scheme).
   - [ ] 3.3 Add error-path tests: absent and empty-string S3 key pair; a vended payload with a key pair but neither region nor endpoint; an ADLS payload with no `adls.sas-token.*` key; an ADLS payload whose only key's host does not match the anchor; a vended plain-`http://` endpoint with `allow_http` false; an `abfs://` anchor with `allow_http` false. Assert each message names no credential value, and assert the ADLS missing-SAS message still contains the anchor host AFTER `redact_error_text` runs.
   - [ ] 3.4 Add Azure extraction tests: happy path; multiple `adls.sas-token.<host>` keys with host-based selection; an anchor carrying a `<container>@` userinfo segment; `account_name` derived from the host; a host with no dot-separated label erroring; and the resulting backend holding the SAS state, never the account-key state.
   - [ ] 3.5 Extend `crates/lakehouse-catalog/tests/catalog_public_surface.rs`: pin the new arity and `Result<StorageBackend, UdfError>` return from the external vantage, keep the existing demoted-function guards, add the replaced mechanism reader's name to that guard list, and add a source-level probe (in the style of `demoted_and_deleted_functions_are_not_declared_public`) that EXTRACTS the variant names from `storage.rs`'s `enum StorageBackend` source — already in the `CATALOG_SOURCES` `include_str!` table at `catalog_public_surface.rs:38` — and asserts each extracted name appears in `vended.rs`. A hardcoded `["S3", "Adls"]` list does NOT satisfy this: it would keep passing after a third variant is added, which is the silent gap the probe exists to prevent. This is the compensating gate for the compile failure a scheme-string match cannot provide.
   - [ ] 3.6 Add a `crates/lakehouse-engine/src/adapter/connection.rs` test asserting a CONNECTION that supplies one storage credential set together with `use_vended_credentials = true` is ACCEPTED, that the Azure-and-S3 mixed-fields guard still rejects both sets, and that the SigV4 requirement still fires.
   - [ ] 3.7 Add unit tests for `validate_sides_share_one_backend` in `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs`'s `mod tests`, building the sides from the EXISTING `resolved_side` and `sample_storage` fixtures with NO catalog session — the same pattern `select_broadcast_sides`' tests already use. Assert that two sides differing in backend variant, and two `Adls` sides differing in `account_name`, each produce a `UdfError::User` naming no credential value, and that two sides sharing one backend return `Ok(())` so every existing single-backend join is unaffected.
   - [ ] 3.8 Add a `crates/lakehouse-engine/src/scan/object_store.rs` test proving `register_side_store` builds a store for an `abfs://` file list and for an `s3a://` file list, since `abfs://` appears nowhere in the repository today and the scheme mapping now admits it at plan time.

4. **E2E**
   - [ ] 4.1 Extend `lakekeeper_vended_creds_projection_filter` in `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`: keep the empty-static assertion and restate its comment as the REQUIRED shape rather than a stronger-than-necessary delegation proof.
   - [ ] 4.2 Add the Glue vended-payload assertions to `crates/lakehouse-engine/tests/cloud_e2e_test.rs`: build a `CatalogSession` from the vended CONNECTION, call `load_table_any_auth` with access delegation, select the credential source for the table location, then assert a non-empty `s3.access-key-id` AND a non-empty `s3.secret-access-key`, assert a non-empty `client.region` OR `s3.endpoint`, and REPORT whether `s3.session-token` is present. Fail naming the absent config key and no credential value. The key-pair assertions are the point: a passing scan alone cannot evidence them, because this CONNECTION carries static AWS keys the shipped rule would have read instead. [expert]

5. **Record-time spec edits** (`/speq:record`, not hand edits during implementation)
   - [ ] 5.1 Remove the superseded Background bullets and clauses from the two permanent specs. In `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md`: the `#276` tracked-exception bullet (`:60`), the "Why a passthrough and not a rejection" bullet (`:62`), the ADLS-arm clause (`:147`), AND the three bullets whose superseding text this plan's delta carries — the absence-preserves-static sentence at `:50-51` ("AWS S3 omits these config keys, so absence preserves the static values and the Glue vended path is unchanged"), the field-for-field-guarantee bullet at `:58` (it names `merge_vended_into_storage`, which § Dead Code Removal deletes), and the "not yet exercised for ADLS" bullet at `:61`. In `specs/vs-adapter/connection-credentials/spec.md`: the `#276` out-of-scope bullet (`:17`). Each of the three additional bullets asserts the removed rule in the PRESENT TENSE; leaving any of them merged would put both rules in the permanent library on a credentials path.
   - [ ] 5.2 Replace the feature-description line of `specs/vs-adapter/connection-credentials/spec.md` (line 3). It sits OUTSIDE every DELTA marker, so the recorder must apply it explicitly — `specs/_recorded/007-add-azure-static-storage-backend/vs-adapter/connection-credentials/spec.md:3` shows the recorder keeping the shipped description rather than adopting a delta's. Left unmerged, the permanent description keeps asserting, unqualified and now falsely, that the CONNECTION credential set selects the backend. The exact replacement text is the description line of this plan's `vs-adapter/connection-credentials/spec.md` delta.
   - [ ] 5.3 Apply the two scenario RENAMES in `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md`, AFTER the delta merge and as part of the same recording. Both delta blocks deliberately keep the SHIPPED heading verbatim so `DELTA:CHANGED` name-matches and replaces the scenario body — that is what deletes the superseded preservation clauses at `:90` and `:116-118`. The headings then no longer describe their bodies, so rename them here:
     - `### Scenario: Vended S3 credentials override static credentials regardless of catalog auth mode` → `### Scenario: Vended S3 credentials are the sole storage source regardless of catalog auth mode`
     - `### Scenario: Vended-credentials request advertises access delegation and adopts the vended region` → `### Scenario: Vended-credentials request advertises access delegation and takes every S3 transport value from the response`
     - Delete the `RENAME PENDING` note line from each scenario body once renamed.
     A `DELTA:REMOVED` + `DELTA:NEW` pair was rejected for this: `speq plan validate` requires GIVEN/WHEN/THEN in every `### Scenario:` block, so a heading-only REMOVED block fails validation, there is no `DELTA:REMOVED` precedent anywhere in this repository, and a recorder that applied REMOVED but skipped NEW would delete a credentials scenario outright. `DELTA:CHANGED` cannot lose the scenario.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 → 1.2 → 1.3 (all three rewrite one `match` in one function; strictly sequential) |
| Group B | 1.4, 2.1, 2.2, 2.3, 2.4 |
| Group C | 3.5, 3.6, 3.7, 3.8 (each touches a different file from 3.1-3.4) |
| Group D | 4.1, 4.2 |

Sequential dependencies:

- Group A → Group B (the call sites need the new signature)
- Group A → 3.1 → 3.2, 3.3, 3.4 — all four edit `vended.rs`'s `mod tests`, so they cannot run concurrently; 3.1 rewrites 27 existing tests before the new ones are added
- Group A → Group C
- Group B, Group C → Group D (E2E needs a buildable `.so`)
- Group D → Group 5 (record-time edits)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Parameter | `resolve_vended_storage`'s `base: &StorageBackend` | Deleting it is what makes the two selectors' input disjointness a signature property |
| Function | `merge_vended_into_storage` (`crates/lakehouse-catalog/src/vended.rs`) | The S3 arm constructs from the vended source instead of merging over a static one; replaced by a construct-from-vended reader |
| Test | `vended_storage_keeps_static_endpoint_and_path_style_when_absent` | Asserts the absence-preserves-static rule this plan removes |
| Test | `resolve_vended_storage_allow_http_always_from_base` | Its premise, a base to read `allow_http` from, is gone |
| Test | `resolve_vended_storage_returns_an_adls_backend_unchanged` | Asserts the discharged #276 passthrough |
| Test | `resolve_vended_storage_empty_access_key_preserves_static`, `resolve_vended_storage_empty_secret_key_preserves_static`, `resolve_vended_storage_absent_session_token_preserves_static`, `resolve_vended_storage_unparseable_path_style_preserves_static` | Each asserts one removed per-field preservation rule; each is replaced by the error or absence assertion that supersedes it |
| Comment | The S3-only anchor comments in `file_resolution.rs` and `resolve_vended_storage`'s doc | Both assert an S3-only anchor and cite the #276 exception; both are now false |
| Spec text | The `#276` Background bullets and ADLS-arm clause in the two permanent specs | Discharged; removed at `/speq:record` (task 5.1) |
| Spec text | `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md:50-51`, `:58`, `:61` | Three shipped Background bullets asserting the removed rule in the present tense: absence preserves the static values; the field-for-field guarantee naming the deleted `merge_vended_into_storage`; "not yet exercised for ADLS". Superseded by this plan's delta Background, removed at `/speq:record` (task 5.1) |
| Spec text | `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md:90` and `:116-118` — the static-preservation clauses | Deleted by the `DELTA:CHANGED` name-match on the shipped headings. The two headings are then stale and renamed as an explicit record-time edit (task 5.3), because a marker pair cannot express a rename |
| Spec text | `specs/vs-adapter/connection-credentials/spec.md:3` — the feature-description line | Sits outside every DELTA marker, so the recorder must be told to replace it or the permanent description keeps asserting that the CONNECTION selects the backend (task 5.2) |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Vended S3 credentials override static credentials regardless of catalog auth mode (delta heading kept for name-match; renamed to "…are the sole storage source…" at task 5.3) | Unit | `crates/lakehouse-catalog/src/vended.rs` | `vended_creds_are_the_sole_storage_source_across_all_auth_modes` |
| Vended-credentials request advertises access delegation and adopts the vended region (delta heading kept for name-match; renamed to "…and takes every S3 transport value from the response" at task 5.3) | Unit | `crates/lakehouse-catalog/src/vended.rs` | `vended_storage_takes_region_endpoint_and_path_style_from_the_response_only` |
| The storage backend under vending is selected from the table location's URI scheme | Unit | `crates/lakehouse-catalog/src/vended.rs` | `vended_backend_variant_comes_from_the_anchor_scheme` |
| A vended-credentials request the catalog does not satisfy is a clear error | Unit | `crates/lakehouse-catalog/src/vended.rs` | `unsatisfied_vended_request_errors_without_static_fallback` |
| A vended Azure SAS is selected by host and carries a consistent account name | Unit | `crates/lakehouse-catalog/src/vended.rs` | `vended_adls_sas_is_selected_by_anchor_host_with_derived_account_name` |
| One concept-level call resolves the effective scan storage from a loadTable response | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend` |
| A join whose sides resolve to different storage backends is rejected at plan time | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs` | `validate_sides_share_one_backend` tests, built from the existing `resolved_side` / `sample_storage` fixtures with no catalog session (task 3.7) |
| Optional credential fields default sensibly | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `absent_optional_fields_default_and_still_select_s3` (existing, line 745; its `for use_vended_credentials in [false, true]` loop is the clause under change) |
| Static storage credentials are ignored, not rejected, when vending is requested | Unit | `crates/lakehouse-engine/src/adapter/connection.rs` | `static_storage_fields_with_vending_are_accepted_and_unused` |
| Every consumer holds a backend and no consumer names one | Unit + Review | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `vended_selector_source_names_every_storage_backend_variant` — a source-level probe. The "exactly two selection sites" and "exactly one decision point" clauses are REVIEW-enforced, not test-enforced: no probe in the catalog crate can count selection sites in `lakehouse-engine`, and this delta says so rather than mapping them to a test that cannot see them |
| The vended selector reaches the SAS state without widening the enum | Unit | `crates/lakehouse-catalog/src/vended.rs` | `vended_adls_backend_holds_the_sas_state_never_the_account_key_state` |
| Vended credentials are exercised end to end against Glue | Integration (E2E) | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_scan_reads_with_vended_credentials` (existing, line 554; extended per task 4.2) |
| End-to-end scan over a vended-credential Lakekeeper warehouse returns correct rows | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_creds_projection_filter` (existing; extended per task 4.1) |

Unit tests are the right form for the first ten rows: `resolve_vended_storage` is pure computation over a deserialized `LoadTableResult` with no I/O and no ambient state. The two rows that do touch a live catalog are E2E.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning-cloud-credentials (S3 vended) | `make test-e2e-lakekeeper` | `lakekeeper_vended_creds_projection_filter` passes; the vended-warehouse row set equals the static-warehouse row set; no credential value in output |
| vs-adapter/pushdown-planning-cloud-credentials (unsupported scheme) | `cargo test -p lakehouse-catalog vended_backend_variant_comes_from_the_anchor_scheme -- --nocapture` | The HTTPS and scheme-less anchors produce a `UdfError::User` naming the scheme; no credential value in the message |
| vs-adapter/pushdown-planning-cloud-credentials (plaintext consent) | `cargo test -p lakehouse-catalog unsatisfied_vended_request_errors_without_static_fallback -- --nocapture` | A vended `http://` endpoint and an `abfs://` anchor each error when `allow_http` is false, naming the scheme and `ALLOW_HTTP`; no credential value |
| vs-adapter/pushdown-planning-cloud-credentials (join guard) | `cargo test -p lakehouse-engine validate_sides_share_one_backend` | An `s3://`-plus-`abfss://` join and a two-account ADLS join each error; a single-backend join returns `Ok(())` |
| vs-adapter/connection-credentials | `cargo test -p lakehouse-engine static_storage_fields_with_vending_are_accepted_and_unused` | Passes: the CONNECTION is accepted, the mixed-fields guard still rejects, the SigV4 guard still fires |
| vs-adapter/storage-backend-enum | `cargo test -p lakehouse-catalog --test catalog_public_surface` | Compiles and passes; a `base` parameter reintroduced on `resolve_vended_storage` is a compile failure here |
| e2e-harness/azure-e2e-harness (static Azure regression) | `make test-e2e-azure` | The `use_vended_credentials: false` Azure suite passes unchanged |
| e2e-harness/cloud-e2e-harness | `cargo test --features cloud-e2e --test cloud_e2e_test -- --test-threads=1` (with AWS env set) | The vended scan passes AND the vended payload assertion confirms a non-empty `client.region` or `s3.endpoint` |

### Verification Obligations

Two things this plan asserts but could not verify in the planning environment. Neither may be closed by inspection.

1. **Glue's vended payload carries a usable S3 credential set AND a store address.** The unverified premise is the WHOLE set — `s3.access-key-id`, `s3.secret-access-key`, and `client.region` or `s3.endpoint` — not the region alone. The currently-green `cloud_scan_reads_with_vended_credentials` cannot evidence any of it: its CONNECTION carries static AWS keys that the shipped preservation rule reads when the response vends nothing. Discharged only by task 4.2 running green against a live AWS Glue account. If any assertion fails, the premise is falsified and the decision returns to the interview — do not soften the rule to make the test pass.
2. **No scan-side change is needed for a vended SAS, and all four accepted schemes are registerable.** The first half is reasoned from `register_side_store`'s Azure arm configuring `MicrosoftAzureBuilder::with_url` from the file URI and never reading `account_name` (`crates/lakehouse-engine/src/scan/object_store.rs`); discharged by issue #278's live vended-SAS E2E, and until then a code-level reading rather than a live confirmation. The second half matters because `abfs://` appears NOWHERE in the repository today, so the scheme mapping admits at plan time a scheme no test proves the scan can register; task 3.8 discharges it for `abfs://` and `s3a://`.
3. **Whether any target catalog vends per-prefix credentials for two tables in one warehouse.** Unverifiable here; it decides whether the pre-existing `join_fan_out_scan_spec` credential collapse (task 2.4) is reachable in practice. This plan does not fix it and does not assert either way.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (host unit) | `cargo test` | 0 failures |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 errors/warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| E2E (S3 baseline) | `make test-e2e` | 0 failures |
| E2E (S3 vended) | `make test-e2e-lakekeeper` | 0 failures |
| E2E (static Azure) | `make test-e2e-azure` | 0 failures |

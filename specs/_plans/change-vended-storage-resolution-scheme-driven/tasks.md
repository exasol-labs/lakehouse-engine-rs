# Tasks: change-vended-storage-resolution-scheme-driven

## Phase 1: Catalog crate — the vended selector (Group A, sequential)
- [x] 1.1 Rewrite `resolve_vended_storage` in `crates/lakehouse-catalog/src/vended.rs`: drop `base`, return `Result<StorageBackend, UdfError>`, select variant from anchor scheme with REQUIRED catch-all error arm, rewrite doc comment. Keep `select_credential_source` byte-identical. [expert]
- [x] 1.2 Build the S3 arm: required key pair, region-or-endpoint address rule, `allow_http` from threaded parameter, plain-http error, absent-means-absent for session token/region/endpoint/path-style. Replace `merge_vended_into_storage` with a construct-from-vended reader. [expert]
- [x] 1.3 Build the ADLS arm: host-matched `adls.sas-token.<host>` selection, `account_name` derivation, `AdlsCred::Sas` assembly, `abfs://` + `allow_http=false` error, anchor host placed pre-redaction. [expert]

## Phase 1: Catalog crate — support (Group B, after Group A)
- [x] 1.4 Update `crates/lakehouse-catalog/src/test_support.rs`: refresh `static_storage`'s doc comment; leave `static_backend` untouched.

## Phase 2: Engine call site and join guard (Group B, after Group A)
- [x] 2.1 Propagate the new `Result` at `resolve_file_list`'s call site in `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`; correct the two S3-only anchor comments.
- [x] 2.2 Add `pub(super) fn validate_sides_share_one_backend` to `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs` beside `select_broadcast_sides`; call from `plan_join` after per-side resolution and before the empty-side shortcut; compare variant + `account_name` only. [expert]
- [x] 2.3 Thread resolved `ALLOW_HTTP` through `resolve_connection_config` (2 call sites in `adapter/mod.rs`), `handle_pushdown`, `plan_join`, `resolve_one_join_side`, `resolve_file_list` into `resolve_vended_storage`. Update the four test/harness call sites of `resolve_file_list`.
- [x] 2.4 Recommend a tracked GitHub issue for the pre-existing per-prefix vended-credential collapse in `join_fan_out_scan_spec`. Do not fix it here.

## Phase 3: Unit tests — vended.rs test disposition (sequential, after Group A)
- [x] 3.1 Apply § Test Disposition to `vended.rs`'s existing 27 tests: KEEP, RESTATE, REPLACE, DELETE rows exactly as tabled. No source-selection or disabled-path assertion changes. [expert]
- [x] 3.2 Add scheme-selection tests: `s3://`, `s3a://`, `abfss://`, `abfs://`, HTTPS anchor, warehouse-style bare-account-id anchor.
- [x] 3.3 Add error-path tests: absent/empty S3 key pair; key pair with neither region nor endpoint; no matching ADLS SAS key; plain-http vended endpoint with `allow_http=false`; `abfs://` anchor with `allow_http=false`. Assert no credential value leaks; ADLS host survives `redact_error_text`.
- [x] 3.4 Add Azure extraction tests: happy path, multi-key host selection, `<container>@` userinfo anchor, `account_name` derivation, no-dot-label error, backend holds SAS state never account-key state.

## Phase 3: Unit tests — disjoint files (Group C, after Group A)
- [x] 3.5 Extend `crates/lakehouse-catalog/tests/catalog_public_surface.rs`: pin new arity/`Result` return, keep demoted-function guards, add replaced reader to guard list, add source-level probe extracting `StorageBackend` variant names and asserting each appears in `vended.rs`.
- [x] 3.6 Add `crates/lakehouse-engine/src/adapter/connection.rs` test: CONNECTION with one storage credential set + `use_vended_credentials=true` is ACCEPTED; mixed-fields guard still rejects; SigV4 requirement still fires.
- [x] 3.7 Add unit tests for `validate_sides_share_one_backend` in `joins/planning.rs`'s `mod tests`, using existing `resolved_side`/`sample_storage` fixtures, no catalog session. Satisfied as a byproduct of task 2.2's TDD cycle: `sides_on_different_backend_variants_are_rejected`, `adls_sides_on_different_storage_accounts_are_rejected`, `sides_on_one_backend_are_accepted`, `fewer_than_two_sides_are_accepted`, `s3_sides_differing_in_credentials_are_accepted` — all assert no credential value leaks and cover every case this task lists. Verified by direct read; no separate agent dispatched.
- [x] 3.8 Add `crates/lakehouse-engine/src/scan/object_store.rs` test: `register_side_store` builds a store for `abfs://` and `s3a://` file lists.

## Phase 4: E2E (Group D, after Groups B and C)
- [x] 4.1 Extend `lakekeeper_vended_creds_projection_filter` in `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs`: keep empty-static assertion, restate comment as REQUIRED shape.
- [x] 4.2 Add Glue vended-payload assertions to `crates/lakehouse-engine/tests/cloud_e2e_test.rs`: non-empty key pair, region-or-endpoint, report session-token presence. Fail naming absent config key, no credential value. [expert]

## Phase 4: Review Fixes

Indices continue the Phase 4 space (4.1-4.2 are the E2E tasks above).

- [x] 4.3 Fix `s3_backend_from_vended` in `crates/lakehouse-catalog/src/vended.rs` so an absent-or-unparseable `s3.path-style-access` defaults to whether the response vended an `s3.endpoint` instead of a bare `false`, extend its doc comment with why the default is endpoint-coupled, add `vended_endpoint_without_path_style_stays_reachable_by_the_scan`, restate the unparseable-path-style test's name and rationale, and assert `path_style` on the `plaintext` fixture in the allow_http-threading test. [expert]
- [x] 4.4 Remove the clone-and-assert blocks from the three disabled-path tests in `crates/lakehouse-catalog/src/vended.rs`'s `mod tests`: rename `vending_disabled_keeps_static_creds` to `empty_vended_key_pair_is_a_missing_credential_not_a_licence_to_read_static` around its surviving empty-key refusal, and delete `no_vending_no_sigv4_uses_static_storage_unchanged` and `vending_disabled_uses_static_on_every_mode` whose remaining assertions duplicate `vended_creds_are_the_sole_storage_source_across_all_auth_modes`. [expert]
- [x] 4.5 In `crates/lakehouse-catalog/src/vended.rs`, rewrite the `"abfs"` refusal error message and the doc comment above `resolve_vended_storage` so the rationale states the true reason (no plaintext Azure path, silently reads over HTTPS otherwise, so `ALLOW_HTTP` consent is required rather than a downgrade) instead of the false "would carry credentials in the clear" claim; keep the literal substrings `ALLOW_HTTP`, `abfs://`, and `{anchor}` in the message, and correct the doc comment to say the two schemes (`http://` endpoint vs `abfs://` location) are gated for different reasons.
- [x] 4.6 In `crates/lakehouse-catalog/src/vended.rs`, normalize the anchor scheme to lowercase before matching in `resolve_vended_storage` (case-insensitive per RFC 3986 §3.1, matching the endpoint scheme's `eq_ignore_ascii_case` comparison already used nearby); extend the existing scheme-selection test with `S3://bucket/db/t` (resolves S3) and `ABFSS://container@myaccount.dfs.core.windows.net/db/t` (resolves ADLS) cases.
- [x] 4.7 In `crates/lakehouse-catalog/src/vended.rs`, amend `resolve_vended_storage`'s doc comment to state precisely that no CONNECTION *storage* field reaches this function but `anchor` is caller-supplied, and when table metadata carries no location the caller substitutes the CONNECTION's `warehouse` so that fallback selects the variant from a CONNECTION-derived string; extend the block comment at `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs` near the anchor/warehouse-fallback logic to name that consequence at the site that creates it.
- [x] 4.8 In `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs`, edit `validate_sides_share_one_backend`'s doc comment so the "separately tracked defect" sentence cites `#294` explicitly.
- [x] 4.9 In `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs`, add `pub(super) struct JoinSideResolution<'a> { session, storage, catalog, creds, allow_http }` beside `ResolvedJoinSide`, change `resolve_one_join_side` to take `(table_name, iceberg_ident, inputs: &JoinSideResolution<'_>, filter_json)` destructuring it when calling `resolve_file_list` positionally, delete the `#[allow(clippy::too_many_arguments)]` on `resolve_one_join_side`, and build the struct once in `plan_join` (`joins/mod.rs`) before the side loop; do not change `resolve_file_list`'s public signature or touch pre-existing suppressions on `plan_join`/`handle_pushdown`; re-run `cargo clippy --workspace --all-targets -- -D warnings`.
- [x] 4.10 In `crates/lakehouse-engine/src/adapter/connection.rs`, extend `static_storage_fields_with_vending_are_accepted_and_unused` with two missing cases: (1) Azure+S3 mixed fields plus `use_vended_credentials: true` still rejected with the "cannot both be supplied" message, (2) `use_sigv4: true` + `use_vended_credentials: true` with a missing `access_key` still rejected by the SigV4 field requirement; assert neither message contains the literal credential values used in the fixture; delete the "guards this does NOT re-test" doc-comment paragraph.
- [x] 4.11 In `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, in `vended_selector_source_names_every_storage_backend_variant`, restrict the searched text to the production region of `vended.rs` (cut at `"#[cfg(test)]"`, fall back to full length if not found) and change the per-variant assertion from `contains(variant)` to `contains(&format!("StorageBackend::{variant}"))`, keeping the existing "extracted no variant names" self-check.
- [x] 4.12 In `crates/lakehouse-catalog/src/test_support.rs`, fix the stale "Consumers" doc comments on `static_storage` and `static_backend` — `vended.rs`'s tests no longer consume either fixture (only `namespace` does now).

## Phase 5: Verification
- [x] 5.1 Run automated checklist (build, test, lint, format, E2E suites) — all green
- [x] 5.2 Scenario coverage audit — all 13 scenarios covered, tests confirmed present and passing
- [x] 5.3 Manual verification commands — all pass; cloud_e2e_test Glue assertions compile and skip cleanly (no AWS creds in this environment, expected per plan's Verification Obligations)

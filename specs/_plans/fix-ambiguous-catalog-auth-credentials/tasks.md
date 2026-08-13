# Tasks: fix-ambiguous-catalog-auth-credentials

## Phase 2: Implementation (Group A)
- [x] 1.1 Add failing test `token_with_complete_oauth_pair_is_rejected_under_both_kinds` to `crates/lakehouse-engine/src/adapter/connection_tests.rs`
- [x] 1.2 Add `validate_exclusive_catalog_auth_creds` (rule 6) to `crates/lakehouse-engine/src/adapter/connection.rs`, wire into `validate_creds`, renumber OAuth2 completeness rule to 7

## Phase 2: Implementation (Group B)
- [x] 2.1 Add failing test `supplied_catalog_auth_names_one_mode_per_field_shape` to `crates/lakehouse-catalog/src/creds_tests.rs`
- [x] 2.2 Declare `SuppliedCatalogAuth` + `ConnectionCreds::supplied_catalog_auth` in `crates/lakehouse-catalog/src/creds.rs`; relocate `non_empty` there and repoint all call sites [expert]

## Phase 2: Implementation (Group C)
- [x] 5.1 Split `serializes_catalog_auth_fields_when_present` in `crates/lakehouse-engine/tests/common/stack.rs` into token-mode and OAuth2-mode cases

## Phase 2: Implementation (Group D)
- [x] 3.1 Add three failing consumer-pin tests (`resolve_catalog_auth_is_unauthenticated_for_the_validation_rejected_shape`, `inject_catalog_auth_props_injects_nothing_for_the_validation_rejected_shape` in `crates/lakehouse-catalog/src/auth_tests.rs`; `resolve_unity_auth_is_unauthenticated_for_the_validation_rejected_shape` in `crates/lakehouse-catalog/src/unity/auth_tests.rs`)
- [x] 3.2 Rewrite `resolve_catalog_auth` and `inject_catalog_auth_props` (`crates/lakehouse-catalog/src/auth.rs`) to match on `creds.supplied_catalog_auth()` [expert]
- [x] 3.3 Rewrite `resolve_unity_auth` (`crates/lakehouse-catalog/src/unity/auth.rs`) to match on `creds.supplied_catalog_auth()`

## Phase 2: Implementation (Group E)
- [x] 4.1 Rename/correct `resolve_catalog_auth_precedence_non_network_branches` → `resolve_catalog_auth_selects_one_strategy_per_non_network_shape` in `crates/lakehouse-catalog/src/auth_tests.rs`
- [x] 6.1 Add E2E test `create_vs_ambiguous_catalog_auth_errors_no_secret` to `crates/lakehouse-engine/tests/e2e_scan_test.rs`

## Phase 4: Review Fixes
- [x] 4.1 Replace the two `non_empty(...).ok_or_else(...)` bindings at the top of `oauth2_client_credentials_grant` (`crates/lakehouse-catalog/src/auth.rs`) with a single `let SuppliedCatalogAuth::ClientCredentials { client_id, client_secret } = creds.supplied_catalog_auth() else { return Err(...) }`, so the grant reads the single mode owner instead of re-deriving pair completeness; keep the three-argument `&ConnectionCreds` signature and `resolve_catalog_auth`'s `ClientCredentials { .. }` arm binding nothing [expert]
- [x] 4.2 In `crates/lakehouse-engine/src/adapter/connection_tests.rs`, in the `pw_partial` case of `token_with_complete_oauth_pair_is_rejected_under_both_kinds`, add two assertions after the existing `client_secret` one: `assert!(msg.contains("missing field: client_secret"), "rule 7 must fire, not rule 6: {msg}")` and `assert!(!msg.contains("mutually exclusive"), "rule 6 must not fire on a token beside half a pair: {msg}")`
- [x] 4.3 In `crates/lakehouse-catalog/src/creds.rs`, extend the `supplied_catalog_auth` doc comment with a sentence stating that rule 6 tests field presence while this method tests non-emptiness, and that the two coincide because the engine's `parse_creds` normalizes every empty credential field to `None` before validation, so `Some("")` never reaches either
- [x] 4.4 In `crates/lakehouse-catalog/src/creds.rs`, reword the second paragraph of `non_empty`'s doc comment to scope the claim to the mode decision — state that the mode classifier treats an empty field as absent by calling this, and that `has_catalog_auth` deliberately does not, because it asks whether catalog auth was INTENDED rather than which mode was supplied
- [x] 4.5 In `crates/lakehouse-engine/tests/common/stack.rs`, delete the two-line `//` comment at lines 464-465 and give both `serializes_token_auth_field_when_present` and `serializes_oauth2_auth_fields_when_present` a `///` doc comment stating that token and OAuth2 are separate CONNECTION shapes because `validate_creds` rule 6 rejects a password supplying both, so each is modelled on its own

## Phase 3: Verification
- [x] 3.1v Run test suite (`cargo test`) — 0 failures
- [x] 3.2v Run linter (`cargo clippy --workspace --all-targets --all-features -- -D warnings`) — 0 warnings
- [x] 3.3v Run format check (`cargo fmt --check`) — no diffs
- [x] 3.4v Run build (`make cross-musl-udf-build`) — exit 0
- [x] 3.5v Run E2E (`make test-e2e`) against an isolated Docker stack — 254 passed, 0 failed
- [x] 3.6v `speq plan validate fix-ambiguous-catalog-auth-credentials` — pass (pre-existing style warnings only)

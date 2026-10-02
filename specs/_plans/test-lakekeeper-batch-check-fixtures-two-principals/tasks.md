# Tasks: test-lakekeeper-batch-check-fixtures-two-principals

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [x] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A)
- [x] 1.1 Add openfga-db, openfga-migrate, openfga to docker-compose.lakekeeper.yml; set LAKEKEEPER__AUTHZ_BACKEND=openfga; update header
- [x] 1.2 Add openfga dependency to docker-compose.lakekeeper.azure.yml
- [x] 1.3 Add OpenFGA services to ci.yml e2e-lakekeeper and e2e-azure jobs; update Makefile Requires comments
- [x] 1.4 Update deploy/README.md (bench box stays on allowall, no OpenFGA)
- [x] 1.5 Add three Keycloak clients to scripts/keycloak-realm-iceberg.json; fix lakekeeper_bootstrap doc comment
- [x] 1.6 Add token_for, server_info, ensure_authz_fixture, batch_check helpers to tests/common/lakekeeper.rs
- [x] 1.7 Run existing e2e_lakekeeper_test suite on OpenFGA stack; re-run probe facts against v1.14.2
- [x] 2.1 Add lakekeeper_stack_enforces_permissions
- [x] 2.2 Add lakekeeper_two_principals_hold_different_table_grants
- [x] 2.3 Add authz_principal_id_is_idp_prefix_and_token_subject (+ jwt_claims)
- [x] 2.4 Add the two privilege tests + set_checker_assignments
- [x] 2.5 Add authz_management_api_is_mounted_beside_catalog_path
- [x] 3.1 Add fixture codec + stack-free codec tests
- [x] 3.2 Add authz_batch_check_fixtures_match_live_contract
- [x] 3.3 Capture and commit four fixtures
- [x] 3.4 Write fixtures README.md

## Phase 3: Verification
- [x] 4.1 Run checklist (build, test, e2e, lint, format, plan validate)

## Phase 4: Review Fixes
- [x] 4.2 Delete `Substitutions::fill` and its tests `substitution_fills_every_placeholder_of_a_request`, `fill_then_normalize_returns_the_template`
- [x] 4.3 Delete `shape_matches`; rewrite its test usages to `shape_mismatch(..).is_some()` and drop the redundant assert
- [x] 4.4 Remove the `error_on_not_found` parameter from `batch_check_request` and `batch_check`; update the 7 call sites
- [x] 4.5 Fold `set_checker_assignments` free fn into `AuthzFixture::set_checker_assignments(&self, grants)`; update callers and import
- [x] 4.6 Derive `PartialEq, Eq` on `Scope`; delete `same_scope`
- [x] 4.7 Replace `call` with `get`/`post` over a private `exchange`; delete `use reqwest::Method`
- [x] 4.8 Add `AssignmentDiff` and pass it to `update_assignments`
- [x] 4.9 Panic on an unreadable response body in `exchange` instead of `unwrap_or_default`
- [x] 4.10 Include the response body in the 403 panic of `expect_status` and in `Exchange::allowed`
- [x] 4.11 Name the scope URL and user in `relations_of` and `update_assignments` failures
- [x] 4.12 Define `OPERATOR` from `lakekeeper::OAUTH_CLIENT_ID`/`OAUTH_CLIENT_SECRET` (made `pub(super)`)
- [x] 4.13 Add `lakekeeper::catalog_uri_host()`; use it in `catalog_url` and replace `lakekeeper_catalog_url_host`
- [x] 4.14 Document in `shape_mismatch` that it takes `Exchange::to_value` documents
- [x] 4.15 Delete the redundant doc comment on `AuthzFixture::read_check`
- [x] 4.16 Add tests for the non-UUID and multi-id branches of `replace_error_ids`
- [x] 4.17 Add stack-free tests for `jwt_claims` and the missing-result panic of `Exchange::allowed`
- [x] 4.18 Delete the redundant `use serde_json::json;` and the qualified `serde_json::Value` in the codec tests
- [x] 4.19 Replace `FIXTURE_CASES[1]`/`[2]` indexing with named `DENIED_CASE`/`MISSING_CASE` consts
- [x] 4.20 Name the URL in the failures of `authz_management_api_is_mounted_beside_catalog_path`
- [x] 4.21 Delete the doc comment on `operator_allows`
- [x] 4.22 Narrow decision-log Decision [5] claim about the `v1.14.2` re-run

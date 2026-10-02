# Code Review Findings: test-lakekeeper-batch-check-fixtures-two-principals

## Summary
- Files reviewed: 18
- Total findings: 21 (standard: 21, expert: 0)
- Evidence: `cargo clippy -p lakehouse-engine --features lakekeeper-e2e --test e2e_lakekeeper_test -- -D warnings` is clean, and `cargo test -p lakehouse-engine --features lakekeeper-e2e --test e2e_lakekeeper_test -- common::lakekeeper_authz::tests` passes 15 of 15. `common/mod.rs` keeps its existing `#![allow(dead_code)]`, so the compiler reports none of the dead code below. Each item was confirmed by a reference search instead.
- Routing: every fix is mechanical, and the compiler or the codec tests catch a wrong edit. No fix involves concurrency, and no fix can produce a test that passes over wrong behaviour.

## Standard fixes

### crates/lakehouse-engine/tests/common/lakekeeper_authz.rs

#### [UNUSED_FUNCTION] `Substitutions::fill` is called only by its own unit tests
- Location: lines 491-497
- Issue: no e2e test calls `fill`. `authz_batch_check_fixtures_match_live_contract` builds each request live and normalizes it, and the assertion `live["request"] == recorded["request"]` already pins the request. Its only callers are `substitution_fills_every_placeholder_of_a_request` and `fill_then_normalize_returns_the_template` in `lakekeeper_authz_tests.rs`. #415's client lives in `lakehouse-catalog` and cannot import the engine's `tests/common`.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, delete `Substitutions::fill`. In crates/lakehouse-engine/tests/common/lakekeeper_authz_tests.rs, delete the tests `substitution_fills_every_placeholder_of_a_request` and `fill_then_normalize_returns_the_template`.

#### [UNUSED_FUNCTION] `shape_matches` is called only by its own unit tests
- Location: lines 541-543
- Issue: the drift test calls `shape_mismatch`. `shape_matches` is a one-line `is_none()` wrapper whose only callers are the codec tests.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, delete `shape_matches`. In crates/lakehouse-engine/tests/common/lakekeeper_authz_tests.rs, delete the line `assert!(shape_matches(&live, &fixture));` in `shape_matches_when_only_a_message_differs`, since the `assert_eq!(shape_mismatch(..), None)` above it asserts the same thing. In each of the eight `shape_differs_when_*` tests, replace `assert!(!shape_matches(a, b))` with `assert!(shape_mismatch(a, b).is_some())`.

#### [DEAD_FLEXIBILITY] `error_on_not_found` is always `false`
- Location: `batch_check_request` line 461, `batch_check` line 470
- Issue: all 7 call sites pass `false` (e2e_lakekeeper_test.rs lines 721, 753, 782, 803, 831, 832, 908). The `true` case (404 `NoSuchTableException`) appears only in the planning probe and is not asserted by the suite. So the boolean flag parameter never changes.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, remove the `error_on_not_found: bool` parameter from `batch_check_request` and `batch_check`, and write the literal `"error-on-not-found": false` in `batch_check_request`'s `json!` body. In crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs, drop the trailing `false` argument from the calls at lines 721, 753, 782, 803, 831, 832, and 908. The committed fixtures already hold `"error-on-not-found": false`, so no recapture is needed.

#### [SHALLOW_MODULE] `set_checker_assignments` only forwards to `set_assignments`, which has no other caller
- Location: lines 301-323 (`AuthzFixture::set_assignments`), lines 333-336 (`set_checker_assignments`)
- Issue: the whole body of the free function is `fixture.set_assignments(&CHECKER, grants)`. `set_assignments` has exactly that one caller, so its `principal` parameter never changes. The design leaves two names for one operation.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, delete the free fn `set_checker_assignments`. Rename the method `AuthzFixture::set_assignments(&self, principal: &Principal, grants: &[Grant])` to `AuthzFixture::set_checker_assignments(&self, grants: &[Grant])`, drop its `principal` parameter, bind `let user_id = self.principal_id(&CHECKER);`, and move the deleted function's doc comment onto it. In crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs, replace the four calls `set_checker_assignments(fixture, X)` (lines 800, 818, 824, 833) with `fixture.set_checker_assignments(X)`, and remove `set_checker_assignments` from the `common::lakekeeper_authz` import list.

#### [STANDARD_LIBRARY_DUPLICATE] `same_scope` hand-writes the derived `PartialEq`
- Location: lines 164 (`Scope` derive), 326-331 (`same_scope`), 307 (call site)
- Issue: `same_scope` compares discriminants and, for `Table`, the `&'static str` payload. `#[derive(PartialEq)]` on `Scope` gives exactly the same result.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, change `#[derive(Clone, Copy)]` on `enum Scope` to `#[derive(Clone, Copy, PartialEq, Eq)]`, delete `fn same_scope`, and replace `same_scope(g.scope, scope)` in `set_assignments` with `g.scope == scope`.

#### [TOO_MANY_ARGUMENTS] `call` takes four arguments, one of which picks the branch
- Location: lines 91-105
- Issue: `call(method, url, token, body: Option<&Value>)` takes four arguments. Every caller pairs `Method::GET` with `None` and `Method::POST` with `Some(..)`, so the `Option` simply mirrors the method.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, replace `call` with `fn get(url: &str, token: &str) -> Exchange` and `fn post(url: &str, token: &str, body: &Value) -> Exchange`. Each one builds its request as `http_client().get(url).bearer_auth(token)` or `http_client().post(url).bearer_auth(token).json(body)` and passes it to a new private `fn exchange(request: reqwest::blocking::RequestBuilder, label: &str) -> Exchange`. That function holds the current send, status, body-read, and parse logic, with `label` set to `GET {url}` or `POST {url}` for the send-failure panic. Rewrite every `call(Method::GET, u, t, None)` as `get(u, t)` and every `call(Method::POST, u, t, Some(b))` as `post(u, t, b)`, then delete `use reqwest::Method;`.

#### [TOO_MANY_ARGUMENTS] `update_assignments` takes four arguments besides `self`
- Location: lines 275-291
- Issue: the signature is `update_assignments(&self, scope, user_id, writes, deletes)`. `writes` and `deletes` always travel together as one assignment diff.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, add a private `struct AssignmentDiff<'a> { writes: Vec<&'a str>, deletes: Vec<&'a str> }`. Change the method to `update_assignments(&self, scope: Scope, user_id: &str, diff: &AssignmentDiff)` and keep the early return when both vectors are empty. In `ensure_grant`, build `AssignmentDiff { writes: vec![grant.relation], deletes: vec![] }`. In `set_assignments` (renamed `set_checker_assignments` by the finding above), build `AssignmentDiff { writes, deletes }`.

#### [SWALLOWED_ERROR] A failed body read becomes an empty-string body
- Location: line 102
- Issue: `resp.text().unwrap_or_default()` discards the read error. The empty text then fails to parse and becomes `Value::String("")`. The status check can still pass, and the next panic blames the body instead (for example `whoami answered no id: ""`). In capture mode the empty body would be written into a fixture.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, in the function that reads the response body (now `call`, or `exchange` after the split above), replace `resp.text().unwrap_or_default()` with `resp.text().unwrap_or_else(|e| panic!("Lakekeeper {label} answered {status} with an unreadable body: {e}"))`. Use whatever variables name the method and URL in that function.

#### [CONTEXTLESS_ERROR] The 403 panic and `Exchange::allowed` drop the response body
- Location: line 112 (`expect_status` 403 branch), lines 79-82 (`Exchange::allowed`)
- Issue: the 403 branch prints only `RECREATE_HINT`, which assumes a stale `allowall` stack is the only cause, and drops Lakekeeper's `error.type` and message. The non-403 branch does print the body. `Exchange::allowed` panics with the status but no body, so a 403 `CannotInspectPermissions` reaching `operator_allows` reports only "has no result". Lakekeeper's management error bodies carry no secret, so printing them keeps the module's no-secret rule.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, change the 403 panic in `expect_status` to `panic!("{what} returned 403: {}; {RECREATE_HINT}", exchange.body)`. Change the panic in `Exchange::allowed` to `panic!("batch-check answer (status {}) has no result for check '{check_id}': {}", self.status, self.body)`.

#### [CONTEXTLESS_ERROR] Assignment failures do not say which scope or user failed
- Location: line 265 (`relations_of`), line 290 (`update_assignments`)
- Issue: `set_assignments` iterates six scopes, but the panic text `Lakekeeper GET assignments` or `Lakekeeper POST assignments` names neither the scope URL nor the user, so a failure cannot be traced to its scope.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, in both `relations_of` and `update_assignments`, bind `let url = self.assignments_url(scope);` once and use it for the request. Pass `&format!("Lakekeeper GET {url} for user '{user_id}'")` (respectively `POST`) as the `what` argument of `expect_status`.

#### [INFORMATION_LEAKAGE] The operator's credentials are defined twice
- Location: lines 42-45 (`OPERATOR`); crates/lakehouse-engine/tests/common/lakekeeper.rs lines 13-14 (`OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`)
- Issue: `OPERATOR` repeats the literals `"lakehouse"` and `"lakehouse-engine-secret"` that `lakekeeper.rs` already owns. `keycloak_client_credentials_token()` and `OPERATOR.token()` depend on separate copies, so changing the operator client means editing both modules.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper.rs, change `const OAUTH_CLIENT_ID` and `const OAUTH_CLIENT_SECRET` to `pub(super) const`. In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, define `OPERATOR` as `Principal { client_id: lakekeeper::OAUTH_CLIENT_ID, client_secret: lakekeeper::OAUTH_CLIENT_SECRET }`.

#### [INFORMATION_LEAKAGE] The host catalog base URL is built in two modules
- Location: lines 338-343 (`catalog_url`); crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs lines 60-62 (`lakekeeper_catalog_url_host`)
- Issue: `catalog_url` writes out `http://localhost:{lakekeeper_port()}/catalog` again, though the test binary already builds it in `lakekeeper_catalog_url_host`. A change to the host catalog path or port source needs edits in both places.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper.rs, add `pub fn catalog_uri_host() -> String { format!("http://localhost:{}/catalog", lakekeeper_port()) }`. In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, change the body of `catalog_url` to `format!("{}/v1/{warehouse_id}", lakekeeper::catalog_uri_host())`. In crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs, delete `fn lakekeeper_catalog_url_host` and replace its three calls (lines 90, 608, 845) with `lakekeeper::catalog_uri_host()`.

#### [MISSING_DESIGN_INTENT] `shape_mismatch` does not state which document it must receive
- Location: lines 535-539, with `is_exact_path` at lines 556-561
- Issue: the doc comment promises an exact comparison of `status`. That holds only when the root is an `Exchange::to_value` document, because `is_exact_path` anchors `status` at `$.status` and suffix-matches the other three fields. A caller that passes the whole fixture document would compare `$.response.status` by JSON type only.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, add this sentence to the doc comment of `shape_mismatch`: "`live` and `fixture` are `Exchange::to_value` documents (`{status, body}`); `status` is compared exactly only at the root."

#### [REDUNDANT_COMMENT] `read_check` doc comment restates the signature
- Location: line 220
- Issue: `/// A \`read_data\` check on \`table\` for the identity \`principal\`.` only repeats the function name and parameters. CLAUDE.md allows a comment only when it states a non-obvious "why".
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz.rs, delete the doc comment above `AuthzFixture::read_check`.

### crates/lakehouse-engine/tests/common/lakekeeper_authz_tests.rs

#### [MISSING_BOUNDARY_TEST] The non-UUID branch of `replace_error_ids` is untested
- Location: lakekeeper_authz.rs lines 523-529 (`else` branch); tests at lines 70-78
- Issue: the only test covers one well-formed `Error ID: <uuid>`. Nothing tests the prefix followed by fewer than 36 characters or by non-hex text, nor two error ids in one string, though both move the scan cursor along different paths.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz_tests.rs, add two tests. `normalization_keeps_an_error_id_prefix_not_followed_by_a_uuid` normalizes `json!({"m": "Error ID: short"})` and asserts `normalized["m"] == "Error ID: short"`. `normalization_replaces_every_error_id_in_one_string` normalizes `json!({"m": "Error ID: 01a0fbc9-4e6a-7252-8c73-c2511c37fa00 then Error ID: 02b0fbc9-4e6a-7252-8c73-c2511c37fa00"})` and asserts `normalized["m"] == "Error ID: <error-id> then Error ID: <error-id>"`.

#### [UNTESTED_ERROR_PATH] `jwt_claims` and `Exchange::allowed` have no stack-free tests
- Location: lakekeeper_authz.rs lines 132-141 (`jwt_claims`), lines 71-84 (`Exchange::allowed`)
- Issue: both are pure logic, yet only the live suite exercises them, and only on the happy path. None of `jwt_claims`'s three panic paths (no payload segment, invalid base64url, payload that is not JSON) is tested, and neither is the missing-result panic of `Exchange::allowed`.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz_tests.rs, add five tests. `jwt_claims_decodes_the_payload_segment` asserts that `jwt_claims(&format!("h.{}.s", URL_SAFE_NO_PAD.encode(r#"{"sub":"abc"}"#)))["sub"] == "abc"`. `jwt_claims_rejects_a_token_without_a_payload` uses `#[should_panic(expected = "three-part JWT")]` on `jwt_claims("abc")`. `jwt_claims_rejects_a_payload_that_is_not_base64url` uses `#[should_panic(expected = "not base64url")]` on `jwt_claims("h.!!!.s")`. `jwt_claims_rejects_a_payload_that_is_not_json` uses `#[should_panic(expected = "not JSON")]` on `jwt_claims(&format!("h.{}.s", URL_SAFE_NO_PAD.encode("not json")))`. `allowed_panics_when_the_check_has_no_result` uses `#[should_panic(expected = "has no result for check")]` on `Exchange { status: 403, body: json!({"error": {}}) }.allowed("read-data")`.

#### [UNUSED_IMPORT] `use serde_json::json;` duplicates the glob import
- Location: line 1; `serde_json::Value` at lines 22 and 36
- Issue: `use super::*;` already brings in the parent's `json` and `Value` imports (lakekeeper_authz.rs line 13), so the explicit import is redundant, and the fully qualified `serde_json::Value` repeats a name that is already in scope.
- Fix: In crates/lakehouse-engine/tests/common/lakekeeper_authz_tests.rs, delete `use serde_json::json;` and change the return types of `denied_response` and `allowed_response` from `serde_json::Value` to `Value`.

### crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs

#### [MAGIC_NUMBER] The denied-equals-missing assertion picks cases by array index
- Location: lines 966-967
- Issue: `&FIXTURE_CASES[1]` and `&FIXTURE_CASES[2]` stand for `denied` and `missing` only because of the current array order. If a case is inserted or reordered, the assertion silently compares the wrong pair.
- Fix: In crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs, declare `const ALLOWED_CASE`, `const DENIED_CASE`, `const MISSING_CASE`, and `const CANNOT_INSPECT_CASE` of type `FixtureCase`, holding the current four entries. Define `FIXTURE_CASES` as `[ALLOWED_CASE, DENIED_CASE, MISSING_CASE, CANNOT_INSPECT_CASE]`, and replace the two index lookups with `&DENIED_CASE` and `&MISSING_CASE`.

#### [CONTEXTLESS_ERROR] The management-path test's failures name no URL
- Location: lines 856, 863, 870 (`authz_management_api_is_mounted_beside_catalog_path`)
- Issue: `.expect("management info request")` and `.expect("catalog config request")` do not say which URL failed. The final `assert_eq!(catalog.status().as_u16(), 200)` has no message at all, so a failure shows only `left: 401, right: 200`.
- Fix: In crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs, in `authz_management_api_is_mounted_beside_catalog_path`, bind `let management_url = format!("{root}/management/v1/info");` and `let config_url = format!("{catalog_uri}/v1/config?warehouse={WAREHOUSE_STATIC}");` and use them in the requests. Replace the two `.expect(..)` calls with `.unwrap_or_else(|e| panic!("GET {management_url} failed to send: {e}"))` and the same for `config_url`, and give the last `assert_eq!` the message `"GET {config_url} must answer 200"`.

#### [REDUNDANT_COMMENT] `operator_allows` doc comment restates the body
- Location: line 718
- Issue: `operator_allows` is a private helper. Its doc comment repeats the name and the `error-on-not-found` argument, and states no "why" (CLAUDE.md § Code comment style).
- Fix: In crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs, delete the doc comment above `fn operator_allows`.

### specs/_plans/test-lakekeeper-batch-check-fixtures-two-principals/decision-log.md

#### [OUTDATED_COMMENT] Decision [5] overstates the `v1.14.2` re-run
- Location: line 53, last sentence of the Privilege bullet
- Issue: "Every `v1.8.16` probe fact held on `v1.14.2`." contradicts the fixtures README (§ Endpoint), which says that under `x-forwarded-prefix` only the catalog base was re-run and "the management base was not re-checked". #415 inherits both documents and would read the decision log's claim as full re-verification.
- Fix: In specs/_plans/test-lakekeeper-batch-check-fixtures-two-principals/decision-log.md, replace "Every `v1.8.16` probe fact held on `v1.14.2`." with "Every re-run `v1.8.16` probe fact held on `v1.14.2`; the management base under `x-forwarded-prefix` was not re-checked."

## Expert fixes
[none]

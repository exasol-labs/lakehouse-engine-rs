# Code Review Findings: add-aws-assume-role-credentials

## Summary
- Files reviewed: 44
- Total findings: 18 (standard: 17, expert: 1)

Checked and clean (no finding):
- `cargo fmt --all --check` exits 0.
- `cargo clippy -p lakehouse-catalog -p lakehouse-engine --all-targets --features exasol-e2e,unity-e2e,cloud-e2e` reports no warnings.
- `cargo test -p lakehouse-catalog --lib` passes (200 tests).
- `sts_stub.py` compiles with `py_compile`.
- Cross-group seams agree:
  - Group B calls `resolve_aws_identity(resolved.creds, &resolved.uri, allow_http)`, which matches Group A's `(ConnectionCreds, &str, bool)` export.
  - Group C's `"sealed":{"name":` probe matches the externally tagged, lowercase `ScanStorage` serde shape that `scan_storage_for` produces.
  - Both join sides route through `scan_storage_for_side` into `scan_storage_for`.
- `deploy/data-stack/*.tf` checked by hand, because `tofu` is not installed here:
  - Blocks, `jsonencode` object literals, and the quoted `"sts:ExternalId"` key parse as valid HCL.
  - `=` alignment follows `fmt`'s rule, where multi-line values leave the alignment group. This matches the existing `aws_iam_policy.engine_reader`.
  - Every argument name is valid for its resource. The graph has no reference cycle (user, role, user policy).
- The `minio-init` rewrite is idempotent. Running its script three times against a live `pgsty/silo` MinIO on the same data exited 0 each time, including `mc admin policy attach` on an already-attached policy.

## Standard fixes

### crates/lakehouse-catalog/src/sts.rs

#### [TOO_MANY_ARGUMENTS] `assume_role` takes five arguments
- Location: `assume_role`, lines 71-77
- Issue: `assume_role(creds, role_arn, catalog_uri, allow_http, timeout)` takes five parameters. It uses `catalog_uri` and `allow_http` only to call `StsEndpoint::resolve`, and it reads `timeout` only for the client and the timeout message.
- Fix: In crates/lakehouse-catalog/src/sts.rs:
  - Add a `timeout: Duration` field to `StsEndpoint`. Keep `StsEndpoint::resolve(creds, catalog_uri, allow_http)` setting it to `STS_TIMEOUT`.
  - In `resolve_aws_identity_within`, build `let endpoint = StsEndpoint { timeout, ..StsEndpoint::resolve(&creds, catalog_uri, allow_http).map_err(|msg| UdfError::User(redact_error_text(&msg, &base_secrets(&creds))))? };`.
  - Change `assume_role` to `async fn assume_role(creds: &ConnectionCreds, role_arn: &str, endpoint: &StsEndpoint) -> Result<SessionCredentials, String>`. Read `endpoint.timeout` wherever `timeout` was read.
  - Leave `resolve_aws_identity_within`'s four-parameter signature unchanged, since it is the plan-mandated test seam.
  - Run `cargo test -p lakehouse-catalog --lib sts` and confirm every test still passes.

### crates/lakehouse-catalog/src/sts_tests.rs

#### [UNTESTED_ERROR_PATH] An unroutable region has no test
- Location: `default_endpoint` in sts.rs, lines 165-177. No test in sts_tests.rs reaches it.
- Issue: `default_endpoint` returns "region '…' does not form a valid STS endpoint host; state aws_sts_endpoint instead" when `url.set_host` rejects the region. `region` is unvalidated CONNECTION input, and no test exercises this failure path.
- Fix: In crates/lakehouse-catalog/src/sts_tests.rs, add `#[tokio::test] async fn an_unroutable_region_without_an_sts_endpoint_is_an_error_naming_aws_sts_endpoint()`:
  - Doc line: `/// Scenario: a region that cannot form an STS host is refused, naming aws_sts_endpoint, before any request.`
  - Start `spawn_recording_sts(200, ASSUME_ROLE_RESPONSE)` only to own a recorder.
  - Build `ConnectionCreds { region: "eu west 1".into(), aws_sts_endpoint: None, ..role_creds(&sts) }`.
  - Call `resolve_aws_identity(creds, PLAIN_CATALOG_URI, true)` and assert `Err(UdfError::User(msg))`.
  - Assert that `msg` contains `does not form a valid STS endpoint host` and `aws_sts_endpoint`.
  - Assert that the recorder saw no request.

### crates/lakehouse-catalog/src/creds.rs

#### [MISSING_DOC_COMMENT] The three new public `ConnectionCreds` fields carry no doc comment
- Location: `ConnectionCreds`, lines 23-25
- Issue: `aws_assume_role_arn`, `aws_external_id`, and `aws_sts_endpoint` extend the catalog crate's public surface, and a reader cannot infer their semantics from the names. Unset or empty means no role. The endpoint default depends on the signing region. The external id is redacted.
- Fix: In crates/lakehouse-catalog/src/creds.rs, add one `///` line above each field:
  - `aws_assume_role_arn`: `/// IAM role [`crate::resolve_aws_identity`] assumes, signed by this set's key pair; unset or empty means the set acts as its own key pair.`
  - `aws_external_id`: `/// STS \`ExternalId\` sent with the \`AssumeRole\` call; \`Debug\` redacts it because a trust policy treats it as a shared value.`
  - `aws_sts_endpoint`: `/// STS endpoint override; unset resolves to the signing region's regional endpoint, else the global one.`

### crates/lakehouse-engine/src/adapter/adapter_tests.rs

#### [BOOLEAN_FLAG_PARAMETER] `sigv4_connection` branches on `role: bool`
- Location: `fn sigv4_connection(stub: &StsAndCatalog, role: bool) -> TestContext`
- Issue: The boolean selects between two CONNECTION shapes, so each call site reads `sigv4_connection(&stub, true)` with no named meaning. Test code follows the same guardrails as production code.
- Fix: In crates/lakehouse-engine/src/adapter/adapter_tests.rs:
  - Split `sigv4_connection` into `sigv4_connection(stub: &StsAndCatalog) -> TestContext`, which builds the no-role password, and `sigv4_role_connection(stub: &StsAndCatalog) -> TestContext`, which builds the same password extended with `aws_assume_role_arn`, `aws_external_id`, and `aws_sts_endpoint`.
  - Build both from one private `fn sigv4_password() -> Json`.
  - Replace every `sigv4_connection(&x, true)` with `sigv4_role_connection(&x)` and every `sigv4_connection(&x, false)` with `sigv4_connection(&x)`.

#### [SWALLOWED_ERROR] The no-role test discards the dispatch result
- Location: `a_connection_without_a_role_sends_no_sts_request`, the `let _ = dispatch(...)` line
- Issue: The create case is expected to succeed and the join case is expected to fail, but both results are discarded. If a no-role create broke after reaching the catalog, the test would still pass.
- Fix: In crates/lakehouse-engine/src/adapter/adapter_tests.rs, replace the loop body with two explicit cases:
  - `dispatch(&mut sigv4_connection(&create_stub), &role_create_request()).expect("a no-role create over an empty namespace succeeds")`.
  - `dispatch(&mut sigv4_connection(&join_stub), &role_join_pushdown_request()).expect_err("an unavailable catalog fails the join pushdown")`.
  - Move the shared assertions (zero STS requests, a non-empty catalog request list, and `Credential={BASE_AK}/` on each) into a private `fn assert_signed_by_the_stated_key_pair(stub: &StsAndCatalog)`, and call it for both stubs.

### crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs

#### [VAGUE_TEST_NAME] `a_role_join_seals_each_side_under_one_key` exercises no join
- Location: `a_role_join_seals_each_side_under_one_key`
- Issue: The test calls `scan_storage_for` twice on the same input and never reaches a join builder (`joins/sql_builders.rs::scan_storage_for_side`). What it actually proves is that two seals of one backend yield distinct envelopes that both open. That is a property of `seal_storage`, which `sealed_tests.rs` does not otherwise cover.
- Fix: Delete `a_role_join_seals_each_side_under_one_key` from crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs. In crates/lakehouse-engine/src/scan/sealed_tests.rs, add `#[test] fn sealing_one_backend_twice_yields_distinct_envelopes_that_both_open()`:
  - Doc line: `/// Scenario: every seal draws a fresh nonce, so two envelopes of one backend differ and both open under the key.`
  - Using `s3_backend()` and `derive_sealed_storage_key(SEALING_PASSWORD)`, seal twice.
  - `assert_ne!` the two payloads, and assert that each unseals to `s3_backend()`.

### crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs

#### [VAGUE_TEST_NAME] A non-vending role case sits inside `catalog_auth_secrets_never_in_scan_spec_with_vending`
- Location: `catalog_auth_secrets_never_in_scan_spec_with_vending`, the block from `// A role CONNECTION with a catalog token that does not vend` to the final `unseal_storage` assertion
- Issue: The appended role case explicitly does not vend, so the test name no longer states what the test covers. A failure in the role half reports under a vending name.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs:
  - Cut that block, including its inline comment, into a new `#[test] fn catalog_auth_secrets_never_in_a_role_scan_spec()` with the doc line `/// Scenario: Catalog auth props are never placed in a role CONNECTION's scan spec, whose session storage travels only sealed.`
  - Keep using the shared `CATALOG_AUTH_KEYS`.
  - In specs/_plans/add-aws-assume-role-credentials/plan.md § Scenario Coverage, row "Catalog auth props are never placed in any scan spec", add `catalog_auth_secrets_never_in_a_role_scan_spec` beside the existing test name.

### crates/lakehouse-engine/tests/build_convention.rs

#### [SHRINKABLE] The test-e2e recipe lookup now has three copies beside an unused helper
- Location: `make_test_e2e_runs_the_assume_role_binary`, `make_test_e2e_runs_the_direct_storage_binary`, `the_type_relaxation_suite_and_fixture_are_wired_into_run_fixtures_and_make_test_e2e`, and `makefile_recipe`
- Issue: The `lines.find(|line| line.starts_with("test-e2e:")).and_then(|_| lines.next())` lookup, together with the `workspace_root.join(...)` plus `read_to_string` preamble, is now repeated three times (Rule of Three reached). This change also added `makefile_recipe`, which already does the lookup, yet the new assume-role test does not use it.
- Fix: In crates/lakehouse-engine/tests/build_convention.rs:
  - Add `fn workspace_file(relative: &str) -> String`, which reads `Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").join(relative)` and panics with `"{relative} must be readable"`.
  - In the three tests above, replace the manual recipe lookup with `makefile_recipe(&workspace_file("Makefile"), "test-e2e:")`.
  - Replace every `std::fs::read_to_string(workspace_root.join(...))` in the file with `workspace_file(...)`.
  - Keep each test's assertion text unchanged.

#### [OUTDATED_COMMENT] The module doc says the file does no I/O
- Location: module doc, lines 1-10
- Issue: The doc says "Pure convention test (no I/O)… it reads no files at runtime". Five tests, three of them new in this change, read `Makefile`, `.github/workflows/ci.yml`, and `scripts/…` at runtime.
- Fix: In crates/lakehouse-engine/tests/build_convention.rs, replace lines 1-10 with a doc that says:
  - the file asserts build and suite-wiring conventions;
  - `CLAUDE.md` is embedded at compile time;
  - `Makefile`, `.github/workflows/ci.yml`, and the fixture scripts are read from the workspace at runtime;
  - no live service is needed.
  Keep it to four lines or fewer.

### crates/lakehouse-engine/tests/e2e_assume_role_test.rs

#### [UNTESTED_ERROR_PATH] The base-identity denial accepts any error
- Location: `the_base_identity_alone_is_denied_the_warehouse_bucket`, the `assert!(!msg.is_empty(), "denial must carry a message")` line
- Issue: The spec scenario "The base identity alone is denied the warehouse bucket" requires that the query "SHALL fail with an error that reports the store's access-denied response". The test accepts any non-empty error, so a planning failure unrelated to S3 authorization would pass it. That is a passing test over the wrong behavior.
- Fix: In crates/lakehouse-engine/tests/e2e_assume_role_test.rs:
  - Run `make test-e2e` once against the Docker stack and read the actual denial text (CLAUDE.md § Verification discipline).
  - Replace the `!msg.is_empty()` assertion with `assert!(msg.to_ascii_lowercase().contains("<the access-denied substring observed>"), "the denial must report the store's access-denied response: {msg}")`. The substring is expected to be `access denied` or `accessdenied`, as MinIO's S3 `AccessDenied` code reads.

### crates/lakehouse-engine/tests/e2e_unity_test.rs

#### [INFORMATION_LEAKAGE] The stub identity is declared in three places with nothing enforcing agreement
- Location: e2e_unity_test.rs `ROLE_BASE_ACCESS_KEY`, `ROLE_BASE_SECRET_KEY`, `ROLE_ARN`, and `ROLE_EXTERNAL_ID`; e2e_assume_role_test.rs `BASE_ACCESS_KEY`, `BASE_SECRET_KEY`, `ROLE_ARN`, and `EXTERNAL_ID`
- Issue: The base user, its secret, the role ARN, and the external id are hard-coded in docker-compose.yml (the `minio-init` and `sts-stub` environment) and again, separately, in both E2E binaries. Each copy's doc says it "matches docker-compose.yml". Changing the stub identity therefore means editing three files that nothing keeps in sync.
- Fix: In crates/lakehouse-engine/tests/common/stack.rs:
  - Add `pub const ASSUME_ROLE_BASE_ACCESS_KEY: &str = "lhassumebase";`, `pub const ASSUME_ROLE_BASE_SECRET_KEY: &str = "lhassumebasesecret123";`, `pub const ASSUME_ROLE_ARN: &str = "arn:aws:iam::123456789012:role/lakehouse-assume-role-demo";`, and `pub const ASSUME_ROLE_EXTERNAL_ID: &str = "lh+ext=id/2026:demo@example";`.
  - Give them one shared doc comment naming docker-compose.yml's `minio-init` and `sts-stub` services as the values they mirror.
  - Delete the four local constants from e2e_assume_role_test.rs and the four `ROLE_*` constants and their doc comment from e2e_unity_test.rs, then import the new constants in both files.
  - Keep `WRONG_EXTERNAL_ID` and `WRONG_BASE_SECRET` local to e2e_assume_role_test.rs.

#### [REDUNDANT_COMMENT] The Unity role test's doc comment runs to seven lines and restates the body
- Location: the doc comment above `unity_role_connection_reads_a_delta_table_through_the_session`
- Issue: CLAUDE.md § Code comment style says "A doc comment on a test names the scenario in one line" and asks for comments over four or five lines to be trimmed. This doc comment is seven lines, and its tail ("mirrors `unity_delta_delete_free_table_returns_its_rows`'s own assertions, against the role virtual schema…") describes what the body does.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, replace that doc comment with the single line `/// Scenario: A Unity Catalog CONNECTION with static keys naming the role reads a Delta table through the session.`

### crates/lakehouse-engine/tests/cloud_e2e_test.rs

#### [UNTESTED_ERROR_PATH] The cloud base-identity denial accepts any error
- Location: `cloud_assume_role_base_identity_alone_is_denied`, the `assert!(!msg.is_empty(), "denial must carry a message")` line
- Issue: The spec scenario "The assume-role base identity alone is denied by Glue" requires that the failure happen "because Glue denies the base identity". The test accepts any error, including a wrong namespace or an unreachable host.
- Fix: In crates/lakehouse-engine/tests/cloud_e2e_test.rs, replace the `!msg.is_empty()` assertion with `assert!(msg.to_ascii_lowercase().contains("accessdenied"), "Glue must deny the base identity: {msg}")`, since Glue answers `AccessDeniedException`. Confirm the substring on the § Checklist "Cloud assume-role" live run, and adjust it to the observed text if it differs.

#### [REDUNDANT_COMMENT] The env-var comment block repeats the module doc, and a trailing comment restates the test's contract
- Location: the four-line `// Cloud assume-role E2E env vars (issue #139)…` block above `ENV_ASSUME_ROLE_BASE_ACCESS_KEY_ID`; the last line of `cloud_assume_role_reaches_glue_and_s3_through_the_role`, `// No credential value or the external id is embedded in any variable printed above.`
- Issue: The `//` block repeats the four variable names and the SSM path, which the module doc (lines 22-30) already lists. The trailing comment repeats the test's own doc sentence "No credential value or the external id is printed to test output."
- Fix: In crates/lakehouse-engine/tests/cloud_e2e_test.rs, delete the four-line `//` block above the `ENV_ASSUME_ROLE_*` constants, and delete the trailing `// No credential value…` line.

### scripts/sts-stub/sts_stub.py

#### [UNUSED_VARIABLE] `BASE_ACCESS_KEY` is read but never checked, and neither is the signing service
- Location: `BASE_ACCESS_KEY` (line 36) and `_verify_signature` (lines 91-142)
- Issue: The spec requires the stub to recompute "the SigV4 signature for the service `sts` with the base user's secret". Plan task 3.1 names the same checks: "service `sts`, the base identity". `_verify_signature` never reads `BASE_ACCESS_KEY`, and it derives the signing key from whatever `service` and access key the `Authorization` header claims. A request signed with the base secret but for service `glue`, or under another key id, would therefore pass, and the E2E suite would stay green over that regression.
- Fix: In scripts/sts-stub/sts_stub.py `_verify_signature`, directly after the `_AUTH_RE` match:
  - Raise `AssumeRoleError("SignatureDoesNotMatch", "the request is not signed by the configured base access key")` when `match.group("access_key") != BASE_ACCESS_KEY`.
  - Raise `AssumeRoleError("SignatureDoesNotMatch", "the request is not signed for service sts")` when `match.group("service") != "sts"`.
  - Delete the docstring sentence "a wrong access key is indistinguishable from a wrong secret, since this stub knows only one identity's secret."
  - Keep both codes within the spec's `AccessDenied` / `SignatureDoesNotMatch` set.

#### [OUTDATED_COMMENT] The session-duration comment points at plan.md and misstates who expires the session
- Location: the comment above `SESSION_DURATION_SECONDS` (lines 44-45)
- Issue: The comment says "The engine treats a query-planning-to-scan gap beyond this as an expired session … see plan.md". The engine treats nothing; the store rejects the expired token. plan.md is also archived at `/speq:record` time, so the reference goes stale.
- Fix: In scripts/sts-stub/sts_stub.py, replace the two comment lines with `# AWS STS's default AssumeRole session lifetime, which the engine relies on (it sends no DurationSeconds).`

### deploy/data-stack/terraform.tfvars.example

#### [OUTDATED_COMMENT] The "Required" claim is defeated by the example's own placeholder
- Location: terraform.tfvars.example lines 7-9; `variable "assume_role_external_id"` in variables.tf
- Issue: The comment says "`tofu plan`/`apply` fails naming this variable until it is set to a real value". Because the example assigns `"CHANGE-ME-set-a-real-external-id"`, an operator who copies the example unchanged gets a successful plan. That plan provisions a live role whose trust policy requires an `ExternalId` committed to a public repository. Nothing validates the variable.
- Fix: In deploy/data-stack/variables.tf, add this block inside `variable "assume_role_external_id"`. RE2 caps a repeat count at 1000, so the length bound stays in `length()`:
  ```hcl
  validation {
    condition = (
      length(var.assume_role_external_id) >= 2
      && length(var.assume_role_external_id) <= 1224
      && can(regex("^[\\w+=,.@:/-]+$", var.assume_role_external_id))
      && !startswith(var.assume_role_external_id, "CHANGE-ME")
    )
    error_message = "assume_role_external_id must be a real STS ExternalId (2-1224 characters of [A-Za-z0-9_+=,.@:/-]), not the terraform.tfvars.example placeholder."
  }
  ```
  Keep the example's placeholder line and comment, which become accurate once the validation exists. Where `tofu` is available, run `tofu fmt -check` and `tofu validate` in deploy/data-stack. Otherwise record in the verification report that they could not run.

## Expert fixes

### crates/lakehouse-catalog/src/creds.rs, crates/lakehouse-catalog/src/sts.rs, crates/lakehouse-engine/src/adapter/connection.rs, crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [INFORMATION_LEAKAGE] "Does this set name a role" is decided in three modules across two crates, with two definitions
- Location:
  - sts.rs line 46: `non_empty(&creds.aws_assume_role_arn)`, so empty means no role.
  - connection.rs line 243 `validate_assume_role_creds`: `creds.aws_assume_role_arn.is_some()`, so empty means a role.
  - support.rs line 1589 `scan_storage_for`: `creds.aws_assume_role_arn.is_none()`, so empty means a role, and the set is sealed.
- Issue: The rule that decides whether a credential set names a role is re-derived at each consumer.
  - The sites disagree for `Some("")`. `resolve_aws_identity`, a public entry point that accepts any `ConnectionCreds`, returns the base identity unchanged. `scan_storage_for` still selects the sealed variant, and validation would demand a key pair.
  - The sites agree today only because the engine's `parse_creds` maps an empty string to `None`, and nothing enforces that coupling.
  - A change to what "names a role" means would need matching edits in both crates.
- Fix:
  - In crates/lakehouse-catalog/src/creds.rs, add `impl ConnectionCreds { /// The IAM role this set names for STS \`AssumeRole\`, or \`None\` when \`aws_assume_role_arn\` is unset or empty; the one definition every consumer reads. pub fn assume_role_arn(&self) -> Option<&str> { non_empty(&self.aws_assume_role_arn) } }`.
  - Replace the three sites with `creds.assume_role_arn()`:
    - sts.rs `resolve_aws_identity_within`: `let Some(role_arn) = creds.assume_role_arn() else { … }`.
    - connection.rs: `let names_role = creds.assume_role_arn().is_some();`.
    - support.rs: `if !creds.use_vended_credentials && creds.assume_role_arn().is_none()`.
  - Add a reachability assertion for `ConnectionCreds::assume_role_arn` to crates/lakehouse-catalog/tests/catalog_public_surface.rs, beside `connection_creds_sigv4_signing_region_is_reachable`.
  - Name the method in the plan's spec delta specs/_plans/add-aws-assume-role-credentials/vs-adapter/catalog-crate-public-surface-extensions/spec.md, where the public-surface extension is enumerated.
  - Add a support_tests.rs case: `scan_storage_for` over creds with `aws_assume_role_arn: Some(String::new())` and no vending returns `ScanStorage::Connection`.
  - Run `cargo test -p lakehouse-catalog` and `cargo test -p lakehouse-engine --lib adapter`.

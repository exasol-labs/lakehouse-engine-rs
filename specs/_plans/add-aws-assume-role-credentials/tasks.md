# Tasks: add-aws-assume-role-credentials

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: Catalog-crate AWS identity)
- [x] 1.1 Add `aws_assume_role_arn`, `aws_external_id`, `aws_sts_endpoint` to `ConnectionCreds`; redact `aws_external_id` in `Debug`; test in `creds_tests.rs`
- [x] 1.2 Add `quick-xml = { version = "0.39", features = ["serialize"] }` to workspace deps and `lakehouse-catalog/Cargo.toml`; confirm no new `Cargo.lock` package; `cargo deny check`
- [x] 1.3 Implement request side of `crates/lakehouse-catalog/src/sts.rs` [expert]
- [x] 1.4 Implement response side of `sts.rs` (XML parsing, error handling, redaction)
- [x] 1.5 Add `session_tests.rs` integration test (session signs loadTable + namespace enumeration)
- [x] 1.6 Re-export `resolve_aws_identity` from `lib.rs`; update `tests/catalog_public_surface.rs`

## Phase 2: Implementation (Group B: Adapter identity wiring and sealed transport)
- [x] 2.1 Parse the three fields in `parse_creds`; add `validate_assume_role_creds`; test in `connection_tests.rs`
- [x] 2.2 Resolve AWS identity once per request at both entry points [expert]
- [x] 2.3 Make `scan_storage_for` seal a role CONNECTION [expert]
- [x] 2.4 Pin that a role leaves the Delta reader's vended path unchanged (test-only)

## Phase 2: Implementation (Group C: Local assume-role E2E)
- [x] 3.1 Write the STS stub (`scripts/sts-stub/sts_stub.py`)
- [x] 3.2 Extend `docker-compose.yml` (minio-init role user, sts-stub service)
- [x] 3.3 Register the suite (Makefile, CI workflow, build_convention.rs tests)
- [x] 3.4 Extend `tests/common/stack.rs` (CatalogConnectionPassword fields, sts stub helpers)
- [x] 3.5 Write Iceberg REST scenarios in `e2e_assume_role_test.rs`
- [x] 3.6 Add direct-storage scenario to `e2e_assume_role_test.rs`
- [x] 3.7 Add Unity Catalog role scenario to `e2e_unity_test.rs`

## Phase 2: Implementation (Group D: Cloud E2E and AWS provisioning)
- [x] 4.1 Extend `deploy/data-stack` (IAM user, role, trust policy, SSM params)
- [x] 4.2 Add cloud assume-role tests to `cloud_e2e_test.rs`; document variables

## Phase 2: Implementation (Group E: Documentation)
- [x] 5.1 Update `docs/catalogs.md`
- [x] 5.2 Update `docs/security.md`
- [x] 5.3 Update `specs/mission.md`

## Phase 3: Verification
- [x] 6.1 Run automated checks (build, test, lint, format, deny, lockfile)
- [x] 6.2 Scenario coverage audit
- [x] 6.3 Manual verification (cloud/AWS-provisioning rows deferred — need live AWS credentials)

## Phase 4: Review Fixes
- [x] 4.3 In `sts.rs`, add a `timeout` field to `StsEndpoint` (set to `STS_TIMEOUT` by `resolve`), override it in `resolve_aws_identity_within`, and change `assume_role` to `(creds, role_arn, &StsEndpoint)`
- [x] 4.4 In `sts_tests.rs`, add `an_unroutable_region_without_an_sts_endpoint_is_an_error_naming_aws_sts_endpoint`, asserting the error text and that no STS request is sent
- [x] 4.5 In `creds.rs`, add a one-line `///` doc to `aws_assume_role_arn`, `aws_external_id`, and `aws_sts_endpoint`
- [x] 4.6 In `adapter_tests.rs`, split `sigv4_connection(stub, role: bool)` into `sigv4_connection` and `sigv4_role_connection`, both built from a private `sigv4_password()`
- [x] 4.7 In `adapter_tests.rs`, replace `a_connection_without_a_role_sends_no_sts_request`'s discarded dispatch with an explicit `expect` create case and `expect_err` join case, sharing `assert_signed_by_the_stated_key_pair`
- [x] 4.8 Delete `a_role_join_seals_each_side_under_one_key` from `support_tests.rs`; add `sealing_one_backend_twice_yields_distinct_envelopes_that_both_open` to `scan/sealed_tests.rs`
- [x] 4.9 In `pushdown_tests.rs`, move the non-vending role block into `catalog_auth_secrets_never_in_a_role_scan_spec`; name it in plan.md § Scenario Coverage
- [x] 4.10 In `build_convention.rs`, add `workspace_file(relative)`, route the three `test-e2e:` lookups through `makefile_recipe`, and replace every `read_to_string(workspace_root.join(..))` with `workspace_file`
- [x] 4.11 In `build_convention.rs`, replace the outdated "no I/O" module doc with a four-line doc naming the compile-time and runtime inputs
- [x] 4.12 In `e2e_assume_role_test.rs`, replace the base-identity denial's `!msg.is_empty()` with an access-denied substring assertion confirmed against a live `make test-e2e` run
- [x] 4.13 In `tests/common/stack.rs`, add the shared `ASSUME_ROLE_*` stub-identity constants; delete the local copies from `e2e_assume_role_test.rs` and `e2e_unity_test.rs` and import the shared ones
- [x] 4.14 In `e2e_unity_test.rs`, replace the Unity role test's seven-line doc with a one-line `/// Scenario:` doc
- [x] 4.15 In `cloud_e2e_test.rs`, replace the cloud base-identity denial's `!msg.is_empty()` with an `accessdenied` substring assertion
- [x] 4.16 In `cloud_e2e_test.rs`, delete the four-line env-var `//` block above the `ENV_ASSUME_ROLE_*` constants and the trailing "No credential value" comment
- [x] 4.17 In `sts_stub.py` `_verify_signature`, reject a request whose access key is not `BASE_ACCESS_KEY` or whose service is not `sts` with `SignatureDoesNotMatch`; drop the stale docstring sentence
- [x] 4.18 In `sts_stub.py`, replace the session-duration comment with the AWS-default-lifetime line
- [x] 4.19 In `deploy/data-stack/variables.tf`, add a `validation` block to `assume_role_external_id` rejecting the example placeholder and non-ExternalId values
- [x] 4.20 Add `ConnectionCreds::assume_role_arn()` in `creds.rs` as the one "names a role" rule; route `sts.rs`, `connection.rs`, and `support.rs` through it; add a public-surface probe, a `Some("")` `scan_storage_for` test, and name it in the public-surface spec delta [expert]

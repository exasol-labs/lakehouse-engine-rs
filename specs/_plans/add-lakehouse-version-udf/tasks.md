# Tasks: add-lakehouse-version-udf

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A) [x]
- [x] 2.1 Add an `ENGINE_VERSION` crate constant fed by `env!("CARGO_PKG_VERSION")` and the `LAKEHOUSE_VERSION` entry point returning it, in `crates/lakehouse-engine/src/lib.rs`. Update the module header doc from two entry points to three.
- [x] 2.2 Add `crates/lakehouse-engine/src/lib_tests.rs` asserting `ENGINE_VERSION` is a non-empty plain `X.Y.Z` string, and declare the sibling test module in `lib.rs`.
- [x] 2.3 Add a `VERSION_SCRIPT_NAME` constant and its `RETURNS VARCHAR(100)` DDL to `create_schema_and_scripts` in `crates/lakehouse-engine/tests/common/e2e_harness.rs`.
- [x] 2.4 Add `crates/lakehouse-engine/tests/e2e_version_udf_test.rs`: call the script against the built `.so` and assert the returned value equals the crate version, with no CONNECTION and no Virtual Schema created.
- [x] 2.5 Add the new suite to `make test-e2e` in the `Makefile`, and assert that wiring in `crates/lakehouse-engine/tests/build_convention.rs`.
- [x] 2.6 Add `ddl_version` and append its statement in `create_engine_scripts` (`deploy/scripts/install.sh`).
- [x] 2.7 Replace `smoke_test_sql` and `classify_fingerprint_response` with a version-based smoke test: call `LAKEHOUSE_VERSION()`, extract the value (dedicated extractor, not `extract_query_value`), compare against `RESOLVED_ENGINE_VERSION`. Verdicts: pass (exact match), version-mismatch, fingerprint-mismatch, other-error.
- [x] 2.8 Confirm `print_next_step_template` emits no `GRANT ACCESS ON CONNECTION` line for `LAKEHOUSE_VERSION`.
- [x] 2.9 Add `LAKEHOUSE_VERSION()` response modes to the exapump stub in `deploy/scripts/tests/install.test.sh`: matching version, differing version, fingerprint mismatch, and an unrelated error.
- [x] 2.10 Extend the engine-scripts DDL test to four scripts, asserting the `RUST SCALAR SCRIPT ... RETURNS VARCHAR(100)` shape for `LAKEHOUSE_VERSION` and the absence of a CONNECTION grant line for it.
- [x] 2.11 Replace `test_fingerprint_smoke_pass_and_fail` with version smoke-test cases: pass on exact match, fail on version mismatch, fail on fingerprint mismatch, fail on other error.
- [x] 2.12 Update the "What the command does" step list in `docs/install.md`: four scripts including `LAKEHOUSE_VERSION`, and the smoke test described as a positive version check.
- [x] 2.13 Add the `LAKEHOUSE_VERSION` DDL to the manual-install appendix, and correct the "All three scripts MUST be in the same schema" sentence: that co-location rule applies to the adapter, scan, and distributor scripts, which the adapter calls schema-qualified.
- [x] 2.14 Rewrite the smoke-test appendix around `SELECT LHVS.LAKEHOUSE_VERSION();` and its three outcomes, and document the standalone version query as an operator task.

## Phase 4: Review Fixes
- [x] 4.1 Add glob suffix to `extract_version_value` header-skip pattern (S1)

## Phase 5: Verification
- [x] 5.1 Automated checks (build, test, install tests, lint, shell lint, format)
- [x] 5.2 Scenario coverage audit
- [x] 5.3 Manual verification (E2E deferred to CI per user instruction)

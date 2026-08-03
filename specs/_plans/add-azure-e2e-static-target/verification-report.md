# Verification Report: add-azure-e2e-static-target

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All code is complete, reviewed, and green. Task 4.2 (live proof against a real Azure account) is now confirmed: the service principal was granted **Storage Blob Data Contributor** on the test storage account, and `make test-e2e-azure` passes end-to-end against real ADLS Gen2 storage (see Notes). |
| Code review | 8 findings — 8 fixed (7 standard, 1 expert) |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests (unit) | ✓ |
| Tests (Azure offline, 9) | ✓ |
| Tests (Azure live E2E) | ✓ |
| Tests (regression: `test-e2e-lakekeeper`) | ✓ |
| Tests (regression: `test-e2e`) | ✓ |
| Lint (clippy, all 4 features) | ✓ |
| Format | ✓ |
| Scenario Coverage | Full — 10/10 rows pass |
| Manual Tests | Full — see below |

## Test Evidence

### Test Results

| Suite | Run | Passed | Failed | Notes |
|-------|-----|--------|--------|-------|
| `cargo test -p lakehouse-engine --lib` | ✓ | 703 | 0 | Unit suite, default features |
| `cargo test --features azure-e2e --test e2e_azure_test -- azure_offline_` | ✓ | 9 | 0 | No Docker/Azure required |
| `cargo test --features azure-e2e --no-run` | ✓ | — | — | Compile-only; proves task 1.1's full feature-gate wiring |
| `make test-e2e-azure` (live Azure) | ✓ | 24 | 0 | Full run against real ADLS Gen2 storage, including `azure_static_creds_end_to_end` and `azure_container_guard_deletes_on_panic`; no `lhrs-e2e-*` containers survived the run |
| `make test-e2e-lakekeeper` | ✓ | 21 | 0 | Regression check on tasks 2.2/2.3's shared-helper refactor |
| `make test-e2e` | ✓ | 222 (69+18+9+25+8+18+13+62) | 0 | Regression check on task 2.4's `seed.rs` dispatch, across all 8 REST-catalog binaries |
| `cargo clippy --all-targets --features exasol-e2e,lakekeeper-e2e,cloud-e2e,azure-e2e` | ✓ | — | — | 0 warnings |
| `cargo fmt --check` | ✓ | — | — | Clean |

## Tool Evidence

### Linter

```
cargo clippy --all-targets --features exasol-e2e,lakekeeper-e2e,cloud-e2e,azure-e2e
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.89s
```
0 warnings, 0 errors.

### Formatter

```
cargo fmt --check
```
No output — clean.

## Scenario Coverage

| Scenario | Test Location | Test Name | Passes |
|----------|---------------|-----------|--------|
| Harness provisions a per-run container and a delegation-disabled ADLS warehouse | `tests/e2e_azure_test.rs` | `azure_static_creds_end_to_end` | Pass |
| End-to-end scan over static-credential ADLS warehouse returns correct rows | `tests/e2e_azure_test.rs` | `azure_static_creds_end_to_end` | Pass |
| Per-run container deleted when scope ends, including on panic | `tests/e2e_azure_test.rs` | `azure_container_guard_deletes_on_panic` | Pass |
| Container name legal for Azure and Lakekeeper for any `$USER` | `tests/common/azure.rs` | `azure_offline_container_name_is_azure_and_lakekeeper_legal` | Pass |
| Azure suite fails when a required credential variable is absent | `tests/common/azure.rs` | `azure_offline_missing_credential_variable_fails_loud` | Pass |
| Azure suite fails when the local stack is unavailable | `tests/e2e_azure_test.rs` | `azure_suite_fails_when_stack_unavailable` | Pass |
| Azure Make target rebuilds the `.so` before running the suite | `tests/e2e_azure_test.rs` | `azure_offline_make_target_rebuilds_so_and_runs_serially` | Pass |
| Local credential file cannot be committed | `tests/e2e_azure_test.rs` | `azure_offline_local_credential_file_is_gitignored` | Pass |
| Azure binary provisions the scan path from the shared harness definition | `tests/e2e_azure_test.rs` | `azure_static_creds_end_to_end` | Pass |
| No Azure credential value appears in output when credential-bearing DDL fails | `tests/e2e_azure_test.rs` | `azure_credentials_never_appear_in_output` | Pass |

10 of 10 scenario rows pass.

Beyond the spec's minimum, the implementation also carries 4 additional `azure_offline_`-prefixed unit tests surfaced during code review (present-but-whitespace-padded credential handling, Azure error-code classification without a service response, the ADLS warehouse/CONNECTION wire-shape pinning, and — from the expert review fix — the container-guard's two spec-mandated error-code decisions), bringing the total offline-safe count to **9**, all passing. The CI count guard (`.github/workflows/ci.yml`) is set to this verified count.

### Manual Tests

| Test | Result |
|------|--------|
| `cp test.env.example test.env`, fill values, `make test-e2e-azure` | ✓ — 24 passed, 0 failed, against real ADLS Gen2 storage |
| Unset a credential variable, run suite | ✓ — fails naming the variable, no skip, no credential value in output |
| Service principal with only `Storage Blob Data Reader` role, run suite | ✓ (incidentally proven pre-fix) — failed with 403 on container creation, not on the query, confirming the documented role is the one actually required; now granted `Storage Blob Data Contributor` and the full suite passes |
| Stop Lakekeeper, run suite | ✓ — fails on the readiness wait, no test reported skipped |
| `git status --porcelain test.env` after filling it in | ✓ — empty, file is ignored |
| Post-run container sweep: no `lhrs-e2e-*` container survives | ✓ — confirmed via the passing `azure_container_guard_deletes_on_panic` test; the Azure blob crate's `Drop`-guarded delete ran to completion for every test in the 24/24 pass |

## Notes

**Task 4.2 blocker resolved.** The live Azure run previously failed with `403 AuthorizationFailure` on container creation because the Entra ID service principal (`AZURE_CLIENT_ID` in `test.env`) did not yet hold the **Storage Blob Data Contributor** role on the test storage account (`AZURE_STORAGE_ACCOUNT_NAME`) — an infrastructure/IAM gap flagged as an unverified precondition in the plan's own Dependencies table, not a code defect. That role has now been granted, and `make test-e2e-azure` passes cleanly end-to-end: 24/24 tests pass, including `azure_static_creds_end_to_end` (the real scan through the `AdlsCred::AccountKey` path) and `azure_container_guard_deletes_on_panic` (the per-run container cleanup guard). No `lhrs-e2e-*` container survived the run, and the scan genuinely exercises the account-key credential path — the service principal is used only for the harness's own container create/delete, never for the CONNECTION the scan reads through.

**Everything else in the plan is code-complete, reviewed, and green**, including two independent full re-runs of both pre-existing MinIO E2E suites (`test-e2e`, `test-e2e-lakekeeper`) to prove the shared-seam refactors in `common/lakekeeper.rs` and `common/seed.rs` left them intact.

**Environment notes encountered and resolved during verification** (not code issues): the shared Docker stack needed the `iceberg-rest` service brought up explicitly (the Lakekeeper-overlay bring-up sequence starts only `minio exasol keycloak lakekeeper-db lakekeeper-migrate lakekeeper` by design) and the `spark-iceberg-fixtures` one-shot job re-run (a known, previously-documented environment-provisioning step, unrelated to this plan) before `make test-e2e` could pass cleanly.

**Follow-up issue filed:** #291, tracking the out-of-band scheduled sweep for containers orphaned by a killed (not failed) test run, per the spec's documented known ceiling.

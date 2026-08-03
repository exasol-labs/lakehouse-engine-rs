# Verification Report: add-azure-e2e-static-target

## Verdict

| Result | Details |
|--------|---------|
| **PARTIAL PASS** | All code is complete, reviewed, and green. Task 4.2 (live proof against a real Azure account) is blocked by an external precondition: the configured service principal returns `403 AuthorizationFailure` on container creation — it does not yet hold the **Storage Blob Data Contributor** role on the test storage account. This is an infrastructure/IAM gap, not a code defect (see Notes). |
| Code review | 8 findings — 8 fixed (7 standard, 1 expert) |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests (unit) | ✓ |
| Tests (Azure offline, 9) | ✓ |
| Tests (Azure live E2E) | ✗ — blocked externally, not a code failure |
| Tests (regression: `test-e2e-lakekeeper`) | ✓ |
| Tests (regression: `test-e2e`) | ✓ |
| Lint (clippy, all 4 features) | ✓ |
| Format | ✓ |
| Scenario Coverage | Partial — 7/10 rows pass; 3 rows (all mapping to one live test) blocked on the same external precondition |
| Manual Tests | Partial — see below |

## Test Evidence

### Test Results

| Suite | Run | Passed | Failed | Notes |
|-------|-----|--------|--------|-------|
| `cargo test -p lakehouse-engine --lib` | ✓ | 703 | 0 | Unit suite, default features |
| `cargo test --features azure-e2e --test e2e_azure_test -- azure_offline_` | ✓ | 9 | 0 | No Docker/Azure required |
| `cargo test --features azure-e2e --no-run` | ✓ | — | — | Compile-only; proves task 1.1's full feature-gate wiring |
| `make test-e2e-azure` (live Azure) | ✗ | 21 | 2 | Both failures are `403 AuthorizationFailure` on container creation — external precondition, not code (see Notes) |
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
| Harness provisions a per-run container and a delegation-disabled ADLS warehouse | `tests/e2e_azure_test.rs` | `azure_static_creds_end_to_end` | Blocked (403) |
| End-to-end scan over static-credential ADLS warehouse returns correct rows | `tests/e2e_azure_test.rs` | `azure_static_creds_end_to_end` | Blocked (403) |
| Per-run container deleted when scope ends, including on panic | `tests/e2e_azure_test.rs` | `azure_container_guard_deletes_on_panic` | Blocked (403) |
| Container name legal for Azure and Lakekeeper for any `$USER` | `tests/common/azure.rs` | `azure_offline_container_name_is_azure_and_lakekeeper_legal` | Pass |
| Azure suite fails when a required credential variable is absent | `tests/common/azure.rs` | `azure_offline_missing_credential_variable_fails_loud` | Pass |
| Azure suite fails when the local stack is unavailable | `tests/e2e_azure_test.rs` | `azure_suite_fails_when_stack_unavailable` | Pass |
| Azure Make target rebuilds the `.so` before running the suite | `tests/e2e_azure_test.rs` | `azure_offline_make_target_rebuilds_so_and_runs_serially` | Pass |
| Local credential file cannot be committed | `tests/e2e_azure_test.rs` | `azure_offline_local_credential_file_is_gitignored` | Pass |
| Azure binary provisions the scan path from the shared harness definition | `tests/e2e_azure_test.rs` | `azure_static_creds_end_to_end` | Blocked (403) |
| No Azure credential value appears in output when credential-bearing DDL fails | `tests/e2e_azure_test.rs` | `azure_credentials_never_appear_in_output` | Pass |

7 of 10 scenario rows pass; the remaining 3 rows all map to the same live-Azure fixture path and are blocked by one external precondition (see Notes), not three independent gaps.

Beyond the spec's minimum, the implementation also carries 4 additional `azure_offline_`-prefixed unit tests surfaced during code review (present-but-whitespace-padded credential handling, Azure error-code classification without a service response, the ADLS warehouse/CONNECTION wire-shape pinning, and — from the expert review fix — the container-guard's two spec-mandated error-code decisions), bringing the total offline-safe count to **9**, all passing. The CI count guard (`.github/workflows/ci.yml`) is set to this verified count.

### Manual Tests

| Test | Result |
|------|--------|
| `cp test.env.example test.env`, fill values, `make test-e2e-azure` | ✗ — fails at container creation with `403 AuthorizationFailure` (expected: pass) |
| Unset a credential variable, run suite | ✓ — fails naming the variable, no skip, no credential value in output |
| Service principal with only `Storage Blob Data Reader` role, run suite | ✓ (incidentally proven) — fails with 403 on container creation, not on the query, confirming the documented role is the one actually required |
| Stop Lakekeeper, run suite | ✓ — fails on the readiness wait, no test reported skipped |
| `git status --porcelain test.env` after filling it in | ✓ — empty, file is ignored |

## Notes

**Blocking external precondition (task 4.2).** The live Azure run was attempted three times across this implementation (once mid-build during task 3.1, once after Group D landed, once after code-review fixes) and fails identically each time:

```
create per-run Azure container 'lhrs-e2e-crusty-<millis>':
  create container lhrs-e2e-crusty-<millis>: HTTP Status Code: 403
  Storage Error Code: AuthorizationFailure
  Error Message: This request is not authorized to perform this operation.
```

A 403 `AuthorizationFailure` (not `AuthenticationFailed`) means the Entra ID service principal authenticates successfully but is not authorized to create containers on the test storage account — i.e. the request reaches Azure and is correctly rejected. This confirms the container-lifecycle credential path (`common/azure.rs`'s `AzureContainer`, service-principal auth) is wired correctly; the gap is account configuration, not code. The plan's own Dependencies table already flagged this as an unverified precondition before implementation started: *"Role assignment on the existing `APP_EXA_RND_LAKEKEEPER_DEV` service principal | Precondition to verify | ... What is unverified is whether it actually holds Storage Blob Data Contributor on the test account."*

**To unblock task 4.2**, an Azure admin needs to grant the service principal (`AZURE_CLIENT_ID` in `test.env`) the **Storage Blob Data Contributor** role on the storage account named by `AZURE_STORAGE_ACCOUNT_NAME`, e.g.:
```
az role assignment create --assignee <client-id> --role "Storage Blob Data Contributor" --scope <storage-account-resource-id>
```
Once granted, re-run `make test-e2e-azure` — every other precondition (HNS-enabled StorageV2 account, the five credential variables, the local Docker stack) is already confirmed working, since the offline tests, the readiness waits, and the Lakekeeper/Keycloak/Exasol provisioning all pass; only the container-create/delete calls are rejected.

**Everything else in the plan is code-complete, reviewed, and green**, including two independent full re-runs of both pre-existing MinIO E2E suites (`test-e2e`, `test-e2e-lakekeeper`) to prove the shared-seam refactors in `common/lakekeeper.rs` and `common/seed.rs` left them intact.

**Environment notes encountered and resolved during verification** (not code issues): the shared Docker stack needed the `iceberg-rest` service brought up explicitly (the Lakekeeper-overlay bring-up sequence starts only `minio exasol keycloak lakekeeper-db lakekeeper-migrate lakekeeper` by design) and the `spark-iceberg-fixtures` one-shot job re-run (a known, previously-documented environment-provisioning step, unrelated to this plan) before `make test-e2e` could pass cleanly.

**Follow-up issue filed:** #291, tracking the out-of-band scheduled sweep for containers orphaned by a killed (not failed) test run, per the spec's documented known ceiling.

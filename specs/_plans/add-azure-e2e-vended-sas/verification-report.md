# Verification Report: add-azure-e2e-vended-sas

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Test-only change adding a vended-SAS ADLS warehouse arm to the Azure E2E suite. All CI gates, host-side unit coverage, the MinIO regression suite, and a live run against the real Azure account are green, including the new `azure_static_and_vended_creds_end_to_end` test which proves the vended-SAS scan path end to end. |
| Code review | 4 findings — standard: 2, expert: 2 — all 4 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ (`cross-musl-udf-build` prerequisite was up to date — no production sources changed, test-only plan) |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | n/a (not tracked via coverage tool in this repo) |
| Integration | 13/13 scenarios in the plan's Scenario Coverage table have a passing test |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (`cargo test --workspace`) | 1016 | 1016 | 2 |
| Integration (`make test-e2e-azure`, live Azure) | 24 | 24 | 0 |
| Integration (`make test-e2e-lakekeeper`, MinIO regression) | 21 | 21 | 0 |
| Host-side subset, no Docker/Azure (`adls_` + `lakekeeper_connection_password_vended` filters) | 4 | 4 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `make test-e2e-azure` — both credential arms pass; per-run container and both warehouses named in output; no credential value leaked | ✓ |
| `cargo test --features azure-e2e --test e2e_azure_test adls_`, then `lakekeeper_connection_password_vended` filter — host-only, no Docker/Azure | ✓ |
| `unset AZURE_STORAGE_ACCOUNT_KEY; make test-e2e-azure` fails naming the missing variable, no credential value in message | ✓ (covered live by `missing_credential_variable_fails_loud`, which ran and passed as part of the same live suite run) |
| `make test-e2e-lakekeeper` — both MinIO arms still pass after the shared `common/lakekeeper.rs` / `common/seed.rs` edits | ✓ |
| Lakekeeper container logs corroborate `loadTable` traffic against the `-vended` warehouse | Not obtained — Lakekeeper's default log level did not emit request-level `loadTable` tracing in the captured window. Non-blocking: the plan states the readback assertion inside the test (`sas-enabled: true`) is the actual proof, and the log is corroboration only. |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings          → clean
cargo clippy --all-targets --features azure-e2e -- -D warnings → clean
cargo clippy --all-targets --features lakekeeper-e2e -- -D warnings → clean
```

### Formatter

```
cargo fmt --all -- --check → no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| e2e-harness | azure-e2e-harness | Harness provisions a per-run container and one ADLS warehouse per credential mode | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_static_and_vended_creds_end_to_end` | Pass |
| e2e-harness | azure-e2e-harness | Harness provisions a per-run container and one ADLS warehouse per credential mode (unit: name/key-prefix disjointness, per-mode sas-enabled, shared storage credential) | `crates/lakehouse-engine/tests/common/lakekeeper.rs` | `tests::adls_warehouse_matches_lakekeeper_profile_shape` | Pass |
| e2e-harness | azure-e2e-harness | End-to-end scan over the static-credential ADLS warehouse returns correct rows | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_static_and_vended_creds_end_to_end` | Pass |
| e2e-harness | azure-e2e-harness | End-to-end scan over the vended-credential ADLS warehouse returns correct rows | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_static_and_vended_creds_end_to_end` | Pass |
| e2e-harness | azure-e2e-harness | End-to-end scan over the vended-credential ADLS warehouse returns correct rows (CONNECTION carries no storage field) | `crates/lakehouse-engine/tests/common/lakekeeper.rs` | `tests::lakekeeper_connection_password_vended_omits_static_s3` | Pass |
| e2e-harness | azure-e2e-harness | Per-run container is deleted when its owning scope ends, including on panic | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_container_guard_deletes_on_panic` | Pass |
| e2e-harness | azure-e2e-harness | Per-run container is deleted when its owning scope ends, including on panic (per-clause delete/collision outcomes) | `crates/lakehouse-engine/tests/common/azure.rs` | `tests::container_guard_keys_each_spec_clause_on_exactly_one_code` | Pass |
| e2e-harness | azure-e2e-harness | Container name is legal for Azure and Lakekeeper whatever the user name contains | `crates/lakehouse-engine/tests/common/azure.rs` | `tests::container_name_is_azure_and_lakekeeper_legal` | Pass |
| e2e-harness | azure-e2e-harness | Azure suite fails when a required credential variable is absent | `crates/lakehouse-engine/tests/common/azure.rs` | `tests::missing_credential_variable_fails_loud` | Pass |
| e2e-harness | azure-e2e-harness | Azure suite fails when the local stack is unavailable | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_suite_fails_when_stack_unavailable` | Pass |
| e2e-harness | azure-e2e-harness | The Azure Make target rebuilds the .so before running the suite | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_make_target_rebuilds_so_and_runs_serially` | Pass |
| e2e-harness | azure-e2e-harness | Local credential file cannot be committed | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_local_credential_file_is_gitignored` | Pass |
| e2e-harness | azure-e2e-harness | Azure binary provisions the scan path from the shared harness definition | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_static_and_vended_creds_end_to_end` | Pass |
| e2e-harness | azure-e2e-harness | No Azure credential value appears in output when credential-bearing DDL fails | `crates/lakehouse-engine/tests/e2e_azure_test.rs` | `azure_credentials_never_appear_in_output` | Pass |

## Notes

- **Scope**: test-only change. No production crate, `.so`, CONNECTION field, environment variable, Make target, or Docker stack file changed. The `cross-musl-udf-build` prerequisite of `make test-e2e-azure` correctly ran as a no-op (nothing to rebuild) since no production source changed.
- **Code review**: 4 findings surfaced (2 standard, 2 expert), all applied:
  - Standard: corrected two outdated doc comments (`post_warehouse`'s stale "fails unless it exists afterwards" / unique-key-prefix-implies-identity claims; `create_warehouse_and_confirm`'s doc/assert message describing a failure mode it cannot actually surface).
  - Expert: fixed an implementation-coupled test where the vended CONNECTION-shape assertion re-derived the password via a second helper call instead of asserting the value actually installed (closed a masking hole where a wrong `provision()` wiring could still pass all vended assertions); fixed an assertion-order violation where the shared-harness provenance check ran after the static arm's block, breaking the plan's "every vended-specific assertion precedes the static arm's" invariant.
- **Live-run cost**: the plan's Impact section already recorded the CI job history (4:43–5:35 for the single-arm job across the four most recent successful `ci.yml` runs) against the 45-minute cap, estimating a doubled two-arm job lands near 11 minutes. This local run's `test-e2e-azure` step itself (compile + both arms, `.so` build skipped as up to date) completed in well under a minute (compile 32.83s + test execution 25.78s), consistent with the plan's estimate leaving substantial headroom under the CI cap; the actual CI job duration will be confirmed on first `ci.yml` run of this branch.
- **Non-goals unaffected**: SAS expiry/refresh, production-crate changes, a harness-side `loadTable` payload probe, and Entra ID service-principal/workload-identity credentials for the data path remain explicitly out of scope, per the plan.
- **Residual masking risk, accepted by the plan**: `AzureFixture::provision()` panics on any failure, so a *vended*-arm provisioning failure still aborts the shared fixture before the static arm runs — this is the plan's accepted one-fixture cost, not a defect introduced here.

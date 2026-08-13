# Verification Report: fix-vended-storage-shared-policy

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Shared vended-storage policy landed in `storage.rs`; both defects from issue #330 (missing `abfs://` consent gate on the Unity path, an over-strict "store address undetermined" error that would reject legal Databricks AWS responses) are fixed and unit-tested on both selectors. Full workspace build, unit/integration test suite, lint, format, and E2E suite are green. |
| Code review | 11 findings — 11 fixed (10 standard, 1 expert) |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test --workspace`) | ✓ |
| Tests (E2E, `make test-e2e`) | ✓ |
| Lint (`cargo clippy --workspace --all-targets -- -D warnings`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit + integration (`cargo test --workspace`) | full workspace | 1170 | 0 |
| E2E (`EXASOL_CONTAINER=lakehouse-engine-rs-2-exasol-1 make test-e2e`) | 9 test binaries | 237 | 0 |

The E2E run's first attempt failed (9 passed, 68 failed) because the `iceberg-rest`, `minio-init`,
and `spark-iceberg-fixtures` Docker Compose services were not running in this environment —
unrelated to the code change (`service at http://localhost:18181/v1/config did not become healthy
within 30s`, confirmed via `docker compose ps`). Brought the full stack up
(`docker compose up -d --wait minio-init iceberg-rest spark-iceberg-fixtures`) and re-ran; second
run is the result reported above, 0 failures.

### Manual Tests

| Test | Command | Result |
|------|---------|--------|
| Shared policy + both selectors | `cargo test -p lakehouse-catalog` | ✓ 170 passed, 0 failed |
| Public surface + repointed probes | `cargo test -p lakehouse-catalog --test catalog_public_surface` | ✓ 14 passed, 0 failed |
| Vended addressing end to end (Lakekeeper) | `EXASOL_CONTAINER=lakehouse-engine-rs-2-exasol-1 make test-e2e` | ✓ (see E2E result above; `e2e_lakekeeper_test` is env-gated separately and was not exercised as part of this suite — no live Lakekeeper stack in this environment) |
| Glue vended path | `cargo test -p lakehouse-engine --test cloud_e2e_test -- --nocapture` | Skipped without AWS credentials, as designed — key-pair assertions stay hard, region/endpoint presence is a report only |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
exit=0 (clean)
```

### Formatter

```
cargo fmt --check
exit=0 (no changes)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | storage-backend-enum | Vended policy and construction move into the enum's own module | `crates/lakehouse-catalog/src/storage_tests.rs` | `shared_home_builds_both_backends_from_neutral_vended_values` | Pass |
| vs-adapter | storage-backend-enum | The repointed source probe binds the shared home, both selectors, and both enums | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `shared_vended_home_constructs_every_storage_backend_variant`, `each_vended_selector_dispatches_every_vended_backend_kind`, `vended_kind_and_storage_backend_variant_sets_are_equal` | Pass |
| vs-adapter | storage-backend-enum | The vended selectors take a store address that cannot carry a credential | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `static_store_address_is_reachable_and_declares_no_credential_field`, `static_store_address_fields_are_not_public` | Pass |
| vs-adapter | unity-catalog-vended-credentials | An S3 vended response terminates in an S3 storage backend | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `s3_vended_response_terminates_in_s3_backend` | Pass |
| vs-adapter | unity-catalog-vended-credentials | A vended plaintext endpoint is honored only with operator consent | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `plaintext_endpoint_requires_allow_http` | Pass |
| vs-adapter | unity-catalog-vended-credentials | A plaintext abfs:// location is honored only with operator consent | `crates/lakehouse-catalog/src/unity/vended_tests.rs`, `crates/lakehouse-catalog/src/vended_tests.rs` | `abfs_location_requires_allow_http_on_the_unity_path`, `unsatisfied_vended_request_errors_without_static_fallback` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | Vended S3 credentials are the sole storage source regardless of catalog auth mode | `crates/lakehouse-catalog/src/vended_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/file_resolution_tests.rs` | `vended_creds_are_the_sole_storage_source_in_spec`, `vended_creds_are_the_sole_storage_source_across_all_auth_modes`, `vended_addressing_prefers_the_connection_endpoint_and_region` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set | `crates/lakehouse-catalog/src/storage_tests.rs`, `crates/lakehouse-catalog/src/vended_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/file_resolution_tests.rs` | `store_address_resolves_endpoint_and_region_independently_with_the_connection_winning`, `path_style_composes_the_vended_override_with_the_resolved_endpoint`, `vended_request_sends_access_delegation_header`, `vended_addressing_prefers_the_connection_endpoint_and_region` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | A vended-credentials request the catalog does not satisfy is a clear error | `crates/lakehouse-catalog/src/vended_tests.rs` | `unsatisfied_vended_request_errors_without_static_fallback`, `empty_vended_key_pair_is_a_missing_credential_not_a_licence_to_read_static` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | One concept-level call resolves the effective scan storage from a loadTable response | `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-catalog/src/vended_tests.rs` | `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend`, `resolve_vended_storage_selects_credential_source_once_for_all_six_values` | Pass |
| vs-adapter | catalog-crate-structure | The vended store-address type extends the crate's public surface through an explicit reviewed edit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `static_store_address_is_reachable_and_declares_no_credential_field`, `static_store_address_fields_are_not_public`, `shared_vended_policy_steps_are_not_public` | Pass |
| vs-adapter | connection-credentials | Static storage credentials are ignored, not rejected, when vending is requested | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution_tests.rs`, `crates/lakehouse-catalog/src/vended_tests.rs` | `vended_addressing_prefers_the_connection_endpoint_and_region`, `vended_creds_are_the_sole_storage_source_in_spec` | Pass |
| e2e-harness | cloud-e2e-harness | Vended credentials are exercised end to end against Glue | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_scan_reads_with_vended_credentials`, `cloud_glue_vends_the_s3_key_pair_for_the_table_location` | Compiles clean; env-gated, skips without AWS credentials (not exercised live) |
| e2e-harness | lakekeeper-e2e-harness | End-to-end scan over a vended-credential Lakekeeper warehouse returns correct rows | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_warehouse_scan_returns_rows` | Compiles clean; requires a live Lakekeeper/Keycloak overlay not present in this environment (not exercised) |
| azure-e2e | azure-e2e-harness | no scenario changes | n/a | n/a | Background-only delta; existing coverage unchanged |

## Notes

- **Both defects from issue #330 are fixed and reachable from both catalog kinds.** The `abfs://`
  plaintext consent gate and the plaintext-endpoint gate now live once in
  `storage.rs::adls_backend`/`storage.rs::s3_backend`; both selectors dispatch through them (proven
  by `each_vended_selector_dispatches_every_vended_backend_kind`). The "store address undetermined"
  error is deleted; a both-empty address now resolves successfully on both selectors.
- **Address rule verified as CONNECTION-wins, not inverted**, at both the unit layer
  (`store_address_resolves_endpoint_and_region_independently_with_the_connection_winning`) and the
  production call site (`vended_addressing_prefers_the_connection_endpoint_and_region`), the latter
  confirmed via mutation testing: substituting `&StaticStoreAddress::default()` at the
  `resolve_file_list` call site makes that one test fail and nothing else in the `lakehouse-engine`
  lib suite (802 passed / 1 failed under the mutation, reverted afterward).
- **Two deliberate error-precedence reorderings** on the Iceberg ADLS arm (extraction now precedes
  the `abfs` gate, which now precedes account-name derivation) were reviewed and found to break no
  asserted scenario; one test fixture (`vended_adls_account_name_requires_a_labelled_host`) needed a
  SAS added per unlabelled host to keep testing what it originally meant to test under the new order
  — verified honest via mutation testing (deleting the account-name-derivation filter still fails the
  test).
- **Environment gap, not a regression**: the first E2E run failed broadly (68/77 failures, all
  `service ... did not become healthy` panics) because this checkout's Docker Compose stack was
  missing `iceberg-rest`, `minio-init`, and `spark-iceberg-fixtures` — unrelated to this change.
  Brought the stack up and reran clean. Flagging so a future run in a fresh environment brings up the
  full base stack before invoking `make test-e2e`.
- **Not exercised live**: `cloud_e2e_test.rs` (needs AWS Glue credentials) and
  `e2e_lakekeeper_test.rs` (needs a live Lakekeeper/Keycloak Docker Compose overlay) both compile
  clean under `cargo clippy --workspace --all-targets -- -D warnings` but were not run against live
  infrastructure in this environment — consistent with their designed env-gated fail-not-skip
  contract, and consistent with the plan's Non-Goals (this plan does not require standing up those
  overlays).
- **Plan-vs-reality note carried forward from review**: plan task 6.3 and its Test Disposition
  attribute the softened `client.region`/`s3.endpoint` assertion to
  `cloud_scan_reads_with_vended_credentials`; the assertion actually lived in its sibling
  `cloud_glue_vends_s3_key_pair_and_store_address` (renamed during review fixes to
  `cloud_glue_vends_the_s3_key_pair_for_the_table_location`). The amendment landed correctly; only the
  plan's test-name attribution is imprecise — worth reconciling at record time.
- No wire-format change; no dependency added or changed.

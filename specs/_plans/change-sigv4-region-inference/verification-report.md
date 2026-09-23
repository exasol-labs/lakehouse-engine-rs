# Verification Report: change-sigv4-region-inference

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All automated checks pass with 0 failures. Every scenario in the plan's Scenario Coverage table has a passing test. 3 of 5 manual steps ran directly; 2 require `exapump`, unavailable in this session, and are covered by equivalent automated integration tests. |
| Code review | 9 findings — 9 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (3/5 run directly, 2/5 blocked by missing local tool, covered by automated tests) |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit + integration (`cargo test`) | full workspace | 1326 (plus per-binary totals summing to the same suites) | 0 |
| E2E (`make test-e2e`, live Docker Exasol) | 16 test binaries | 361 total across binaries, e.g. 81, 83, 36, 24, 21, 19, 14, 13, 12, 11, 10, 10, 9, 9, 9 | 0 |
| Cloud E2E (`cargo test -p lakehouse-engine --features cloud-e2e --test cloud_e2e_test -- --test-threads=1`) | opt-in, no AWS env vars set | 12 (all clean-skip internally where AWS creds are required) | 0 |

Raw logs: `target/speq-test.log`, `target/speq-e2e.log`, `target/speq-cloud-e2e.log`, `target/speq-build.log`, `target/speq-clippy.log`, `target/speq-fmt.log`.

### Manual Tests

| Test | Result |
|------|--------|
| vs-adapter/connection-credentials — non-standard host keeps the requirement (`exapump` against Docker Exasol) | Not run: `exapump` is not installed in this environment. Equivalent coverage: `sigv4_region_required_for_non_standard_glue_hosts` (connection_tests.rs), which drives the same `validate_sigv4_creds` code path with the same host matrix and asserts the identical refusal text and no key leakage. Passing (see cargo test log). |
| vs-adapter/connection-credentials — standard host passes the guard (`exapump` against Docker Exasol) | Not run: `exapump` unavailable. Equivalent coverage: `sigv4_region_derived_from_standard_glue_endpoint_is_accepted` (connection_tests.rs), asserting the guard accepts the standard Glue endpoint without a stated region. Passing. |
| e2e-harness/cloud-e2e-harness — `cloud_sigv4_region_derived_from_glue_endpoint_lists_table -- --nocapture` | ✓ Ran directly. `SKIPPED: cloud-e2e requires env var GLUE_CATALOG_URI` printed, test reports `ok` (clean skip, no AWS vars set). No credential value in output. |
| vs-adapter/pushdown-planning-cloud-credentials — `cloud_glue_vends_the_s3_key_pair_for_the_table_location -- --nocapture` | ✓ Ran directly. Same clean-skip pattern, `ok`, no credential value in output. |
| vs-adapter/catalog-crate-public-surface-extensions — `cargo test -p lakehouse-catalog --test catalog_public_surface` | ✓ Ran directly. 18/18 probes pass, including `connection_creds_sigv4_signing_region_is_reachable` and `signing_region_steps_are_not_public`. |

## Tool Evidence

### Linter

```
cargo clippy --all-targets
Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.53s
0 warnings, 0 errors
```

### Formatter

```
cargo fmt --check
(no output — no changes needed)
```

### Build

```
make cross-udf-build
exit 0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | connection-credentials | SigV4 requires access_key, secret_key, and a signing region | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `sigv4_requires_access_secret_region`, `sigv4_region_required_for_non_standard_glue_hosts` | Pass |
| vs-adapter | connection-credentials | A standard Glue endpoint supplies the signing region when CONNECTION omits region (acceptance) | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `sigv4_region_derived_from_standard_glue_endpoint_is_accepted` | Pass |
| lakehouse-catalog | sigv4 | Host-rule derivation matrix (standard/non-standard hosts) | `crates/lakehouse-catalog/src/sigv4_tests.rs` | `standard_glue_endpoint_supplies_the_signing_region_when_none_is_stated`, `non_standard_addresses_supply_no_signing_region` (plan.md named these `signing_region_derived_from_standard_glue_endpoint` / `signing_region_absent_for_non_standard_hosts`; implementation used clearer names for the same scenarios) | Pass |
| lakehouse-catalog | auth/iceberg_io | `loadTable` path signs for the derived region | `crates/lakehouse-catalog/src/auth_tests.rs`, `iceberg_io_tests.rs` | `sigv4_auth_carries_region_derived_from_glue_endpoint`, `sigv4_request_is_signed_for_the_carried_region` | Pass |
| lakehouse-catalog | namespace | Namespace enumeration signs for the derived region | `crates/lakehouse-catalog/src/namespace_tests.rs` | `signed_enumeration_is_signed_for_the_resolved_region` | Pass |
| e2e-harness | cloud-e2e-harness | Real Glue accepts both signatures (live, opt-in) | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_sigv4_region_derived_from_glue_endpoint_lists_table` | Pass (clean skip without AWS vars) |
| lakehouse-catalog | sigv4 | Glue endpoint signs even when CONNECTION states a different region | `sigv4_tests.rs`, `auth_tests.rs`, `connection_tests.rs` | `standard_glue_endpoint_region_signs_even_when_a_different_region_is_stated`, `sigv4_auth_derives_region_even_when_a_different_region_is_stated`, `sigv4_cross_region_glue_and_s3_is_supported`, `sigv4_stated_region_used_for_non_standard_endpoint` | Pass |
| vs-adapter | connection-credentials | Static storage credentials ignored, not rejected, under vending | `connection_tests.rs` | `static_storage_fields_with_vending_are_accepted_and_unused`, `sigv4_derived_region_places_no_store_under_vending` | Pass |
| lakehouse-catalog | catalog-crate-public-surface-extensions | Signing-region resolver is reachable, its steps are crate-private, no delivery-mechanism naming | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `connection_creds_sigv4_signing_region_is_reachable`, `signing_region_steps_are_not_public`, `demoted_and_deleted_functions_are_not_declared_public` | Pass |
| lakehouse-catalog | catalog-crate-public-surface-extensions | Refusal on both signing paths | `auth_tests.rs`, `namespace_tests.rs` | `sigv4_auth_refuses_without_signing_region`, `signed_enumeration_refuses_without_signing_region` | Pass |
| lakehouse-catalog | catalog-crate-public-surface-extensions | One-way dependency (catalog crate names no execution engine) | `crates/lakehouse-catalog/tests/catalog_crate_boundary.rs` | `catalog_manifest_declares_no_execution_engine_dependency` | Pass |
| e2e-harness | cloud-e2e-harness | Vended credentials exercised end to end against Glue | `cloud_e2e_test.rs` | `cloud_scan_reads_with_vended_credentials`, `cloud_glue_vends_the_s3_key_pair_for_the_table_location` | Pass (clean skip without AWS vars) |
| e2e-harness | cloud-e2e-harness | A region-less Glue CONNECTION lists the table through SigV4-signed catalog requests | `cloud_e2e_test.rs` | `cloud_sigv4_region_derived_from_glue_endpoint_lists_table` | Pass (clean skip without AWS vars) |

## Notes

- Two plan.md-listed unit test names (`signing_region_derived_from_standard_glue_endpoint`, `signing_region_absent_for_non_standard_hosts`) do not literally exist; the implementation named the equivalent tests `standard_glue_endpoint_supplies_the_signing_region_when_none_is_stated` and `non_standard_addresses_supply_no_signing_region` in `sigv4_tests.rs`. Same scenario, same assertions, both pass. Not a coverage gap.
- Manual verification steps 1 and 2 (non-standard/standard host checks via `exapump` against the Docker Exasol container brought up by `make test-e2e`) could not run in this environment because `exapump` is not installed here. The identical validation logic they exercise (`validate_sigv4_creds` in `crates/lakehouse-engine/src/adapter/connection.rs`) is covered end-to-end by `sigv4_region_required_for_non_standard_glue_hosts` and `sigv4_region_derived_from_standard_glue_endpoint_is_accepted`, both passing.
- Cloud E2E and its cloud-dependent manual steps ran with no `GLUE_CATALOG_URI`/AWS credentials set, so they exercised the clean-skip path rather than a live Glue round trip. This matches the plan's Checklist expectation ("0 failures, or a clean skip when the AWS variables are absent").
- All 9 code-review findings (guardrail/duplication/test-quality issues, no correctness bugs) were fixed in Phase 4 and verified present in the current source (`SignedEnumeration` struct, `pub(crate) MISSING_SIGNING_REGION`, `CloudVsTarget` struct, `vs_table_name` helper, removed `setup_cloud_vs_vended`, removed redundant inline comments).

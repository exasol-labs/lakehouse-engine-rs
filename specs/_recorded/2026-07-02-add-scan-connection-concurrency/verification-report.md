# Verification Report: add-scan-connection-concurrency

**Generated:** 2026-07-02

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | SDK bumped to 0.20.1 (closes #43), `S3_MAX_CONNECTIONS` knob added end-to-end and applied to the object store's HTTP client pool. Two real bugs were caught and fixed during E2E verification (see Notes) — both are now covered by green E2E runs. |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test -p lakehouse-engine`) | 13 suites | 347 | 2 |
| E2E (`make test-e2e`, local Exasol Docker stack) | 2 binaries | 39 (7 + 32) | 0 |

## Tool Evidence

### Linter

```
cargo clippy -p lakehouse-engine --all-targets
cargo clippy: No issues found
```

### Formatter

```
cargo fmt --check
(no output — clean)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | scan-execution-connection-concurrency | Scan configures its object store from the resolved connection budget | `crates/lakehouse-engine/tests/scan_two_arg.rs` | `scan_applies_s3_max_connections_to_object_store` | Pass |
| datafusion-scan | scan-execution-connection-concurrency | Scan falls back to a built-in default budget when the field is absent | `crates/lakehouse-engine/src/scan/spec.rs` | `s3_max_connections_round_trips_and_defaults` | Pass |
| datafusion-scan | scan-execution-connection-concurrency | FIXED value overrides the AUTO derivation at createVirtualSchema | `crates/lakehouse-engine/src/adapter/mod.rs` | `resolve_s3_max_connections_fixed_value_wins` | Pass |
| datafusion-scan | scan-execution-connection-concurrency | AUTO derivation sizes the per-instance budget from node capacity | `crates/lakehouse-engine/src/adapter/mod.rs` | `resolve_s3_max_connections_auto_scales_with_cores` | Pass |
| datafusion-scan | scan-execution-connection-concurrency | AUTO derivation falls back to the default budget when the core count is unknown | `crates/lakehouse-engine/src/adapter/mod.rs` | `resolve_s3_max_connections_auto_zero_cores_defaults` | Pass |
| datafusion-scan | scan-execution-connection-concurrency | Connection budget travels once in the shard-invariant common spec | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `common_spec_carries_s3_max_connections_exactly_once` | Pass |

## Notes

- **Dependency bump (Task 1, closes #43):** `exasol-udf-sdk`/`exasol-udf-macros` 0.20.0 → 0.20.1, verified published on crates.io before bumping. `resolve_cluster_nodes` unchanged, no new scenario needed per the plan.
- **Code review** (`speq:code-reviewer`) surfaced 3 findings, all addressed: (1) a `clippy::assertions_on_constants` warning from an assertion on a `const` — deleted; (2) a diverging built-in-default value (8 vs. 16) between `scan/spec.rs` and `adapter/mod.rs` — unified onto one shared constant (`adapter::DEFAULT_S3_MAX_CONNECTIONS`); (3) a redundant unit test fully subsumed by the new integration test — deleted (ponytail cleanup, `net: -12 lines`).
- **Two real bugs caught only by the E2E gate** (both are exactly why this project's CLAUDE.md requires E2E to run, not skip, without a live stack):
  1. **SLC/`.so` fingerprint mismatch.** The e2e harness (`e2e_scan_test.rs`, `e2e_capability_test.rs`) hardcodes `SLC_VERSION = "0.20.0"`, installing an SLC that no longer matched the `.so`'s SDK 0.20.1 after Task 1's bump. Per this repo's own CLAUDE.md rule ("keep the SLC and the consumer crate's `exasol-udf-sdk` version in lockstep"), confirmed a matching `language-container-rs` `v0.20.1` release exists and bumped the pin in both test files and the `Makefile`'s `install-slc` target.
  2. **`with_client_options` clobbers `with_allow_http`.** `AmazonS3Builder::with_client_options` *replaces* the builder's entire `ClientOptions` rather than merging into it (confirmed by reading `object_store` 0.13.2 source). Task 2.5's original ordering called `.with_allow_http(...)` *before* `.with_client_options(...)`, silently discarding the `allow_http` flag — broke every E2E test against plain-HTTP MinIO with `HTTP error: builder error`. Fixed by reordering: `.with_client_options(...)` now runs first, `.with_allow_http(...)` last, with a comment recording why the order matters.
- Both fixes are included in this branch's diff; full unit + E2E suites re-run green after each.

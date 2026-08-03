# Verification Report: change-vended-storage-resolution-scheme-driven

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Scheme-driven vended storage selection implemented and green: catalog rewrite, engine call sites, join backend guard, unit tests, and all three E2E suites pass. |
| Code review | 9 findings — 9 fixed (7 standard, 2 expert) |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test --workspace`) | ✓ |
| Lint (`cargo clippy --workspace --all-targets -- -D warnings`) | ✓ |
| Format (`cargo fmt --all -- --check`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (`cargo test --workspace`) | 1012 | 1012 | 2 |
| E2E — S3 baseline (`make test-e2e`) | 62 | 62 | 0 |
| E2E — S3 vended / Lakekeeper (`make test-e2e-lakekeeper`) | 21 | 21 | 0 |
| E2E — Static Azure (`make test-e2e-azure`) | 24 | 24 | 0 |

0 failures across every suite run.

### Manual Tests

| Test | Result |
|------|--------|
| `make test-e2e-lakekeeper` — `lakekeeper_vended_creds_projection_filter` (vended-warehouse row set equals static-warehouse row set; no credential value in output) | ✓ |
| `cargo test -p lakehouse-catalog vended_backend_variant_comes_from_the_anchor_scheme -- --nocapture` (HTTPS/scheme-less anchors error naming the scheme, no credential value) | ✓ |
| `cargo test -p lakehouse-catalog unsatisfied_vended_request_errors_without_static_fallback -- --nocapture` (plain-`http://` endpoint and `abfs://` anchor each error when `allow_http` false) | ✓ |
| Join guard tests (`sides_on_different_backend_variants_are_rejected`, `adls_sides_on_different_storage_accounts_are_rejected`, `sides_on_one_backend_are_accepted`, `fewer_than_two_sides_are_accepted`, `s3_sides_differing_in_credentials_are_accepted`) — cross-backend and cross-account joins rejected, single-backend joins pass | ✓ |
| `cargo test -p lakehouse-engine static_storage_fields_with_vending_are_accepted_and_unused` (CONNECTION accepted; mixed-fields guard still rejects; SigV4 guard still fires) | ✓ |
| `cargo test -p lakehouse-catalog --test catalog_public_surface` (compiles; pins new arity/`Result` return; variant-name probe) | ✓ (4 tests) |
| `make test-e2e-azure` (`use_vended_credentials: false` Azure suite unchanged) | ✓ |
| `cargo test --features cloud-e2e --test cloud_e2e_test -- --test-threads=1` (no AWS env in this environment) | ✓ — clean SKIP, not a failure. `cloud_glue_vends_s3_key_pair_and_store_address` compiles and short-circuits on absent `GLUE_CATALOG_URI` per its own gate. Live execution against AWS Glue is Verification Obligation #1 in plan.md, explicitly undischargeable in this environment — a repo maintainer with AWS credentials must run this before treating the Glue vended-payload claim as confirmed. |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
(clean, 0 warnings/errors)
```

### Formatter

```
cargo fmt --all -- --check
(no changes)
```

### Build

```
make cross-musl-udf-build
Compiling lakehouse-catalog v0.1.0
Compiling lakehouse-engine v0.31.2
Finished `release` profile [optimized] target(s) in 1m 14s
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning-cloud-credentials | Vended S3 credentials are the sole storage source across all auth modes | `crates/lakehouse-catalog/src/vended.rs` | `vended_creds_are_the_sole_storage_source_across_all_auth_modes` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | Vended request takes every S3 transport value from the response only | `crates/lakehouse-catalog/src/vended.rs` | `vended_storage_takes_region_endpoint_and_path_style_from_the_response_only` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | Backend selected from the table location's URI scheme | `crates/lakehouse-catalog/src/vended.rs` | `vended_backend_variant_comes_from_the_anchor_scheme` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | A vended request the catalog does not satisfy is a clear error | `crates/lakehouse-catalog/src/vended.rs` | `unsatisfied_vended_request_errors_without_static_fallback` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | A vended Azure SAS is selected by host with a consistent account name | `crates/lakehouse-catalog/src/vended.rs` | `vended_adls_sas_is_selected_by_anchor_host_with_derived_account_name` | Pass |
| vs-adapter | storage-backend-enum | One concept-level call resolves effective scan storage from a loadTable response | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `resolve_vended_storage_is_the_only_vended_entry_point_and_takes_no_backend` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | A join whose sides resolve to different storage backends is rejected at plan time | `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs` | `sides_on_different_backend_variants_are_rejected`, `adls_sides_on_different_storage_accounts_are_rejected`, `sides_on_one_backend_are_accepted` | Pass |
| vs-adapter | connection-credentials | Optional credential fields default sensibly | `crates/lakehouse-engine/src/adapter/connection.rs` | `absent_optional_fields_default_and_still_select_s3` | Pass |
| vs-adapter | connection-credentials | Static storage credentials ignored, not rejected, under vending | `crates/lakehouse-engine/src/adapter/connection.rs` | `static_storage_fields_with_vending_are_accepted_and_unused` | Pass |
| vs-adapter | storage-backend-enum | Every consumer holds a backend and no consumer names one (variant-name probe) | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `vended_selector_source_names_every_storage_backend_variant` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | Vended selector reaches SAS state without widening the enum | `crates/lakehouse-catalog/src/vended.rs` | `vended_adls_backend_holds_the_sas_state_never_the_account_key_state` | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | Vended credentials exercised end to end against Glue | `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `cloud_scan_reads_with_vended_credentials`, `cloud_glue_vends_s3_key_pair_and_store_address` | Pass (compiles; SKIP without live AWS — expected, see Verification Obligations) |
| vs-adapter | pushdown-planning-cloud-credentials | End-to-end scan over a vended-credential Lakekeeper warehouse returns correct rows | `crates/lakehouse-engine/tests/e2e_lakekeeper_test.rs` | `lakekeeper_vended_creds_projection_filter` | Pass |

## Notes

- **Scope.** 18 implementation tasks (1.1-4.2) plus 9 code-review findings (7 standard, 2 expert) across 4 parallel work groups (catalog vended selector, engine call sites + join guard, vended.rs test-disposition rewrite, disjoint unit tests) and a final E2E group.
- **Real regression caught in review, not shipped.** The expert-fix pass found that the vended S3 arm's `path_style` defaulted to `false` when a catalog vended an endpoint but no `s3.path-style-access` flag — `register_side_store` gates whether the endpoint reaches the object-store builder on `path_style`, so this silently dropped a vended endpoint at scan time (reading the wrong store, not failing loud). Fixed: absent-or-unparseable `path_style` now defaults to `endpoint.is_some()`. One residual, deliberately left: an *explicit* `s3.path-style-access: false` alongside a vended endpoint still resolves `path_style: false` (the catalog's stated preference is honored), which reproduces the same two-consumer divergence for that one shape — now pinned by a test (`vended_explicit_path_style_false_wins_over_the_endpoint_coupled_default`) rather than latent.
- **Case-sensitivity fix.** The anchor URI scheme is now matched case-insensitively (`to_ascii_lowercase()`), per RFC 3986 §3.1 — an uppercase `S3://` location no longer falls through to the unsupported-scheme error.
- **Tracked follow-up filed.** Issue [#294](https://github.com/exasol-labs/lakehouse-engine-rs/issues/294) — the pre-existing `join_fan_out_scan_spec` collapse that reads a broadcast join's dimension side through the fact side's storage credentials. Deliberately out of scope for this plan (task 2.4); the new join guard (`validate_sides_share_one_backend`) narrows the blast radius to variant/account mismatches only, and does not fix per-prefix credential divergence within one backend.
- **Verification Obligation left open, as the plan anticipated.** `cloud_glue_vends_s3_key_pair_and_store_address` (task 4.2) compiles and correctly skips without live AWS credentials, which this environment does not have. Discharging it — confirming Glue actually vends a usable S3 key pair and store address — requires a run against a live AWS Glue account, per plan.md § Verification Obligations #1. The Lakekeeper (MinIO) vended path IS fully verified end-to-end and is green.
- **Databricks Unity Catalog** has no in-repo E2E suite (unchanged by this plan) and remains unverified, as plan.md § Impact already states.

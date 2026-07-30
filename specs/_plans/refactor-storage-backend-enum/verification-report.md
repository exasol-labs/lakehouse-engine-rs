# Verification Report: refactor-storage-backend-enum

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `StorageBackend` enum lands across all 18 planned sites; S3 behavior unchanged; only wire change is the `storage` field's variant tag. Full workspace test suite, clippy, fmt, and the complete E2E suite are green. |
| Code review | 7 findings — standard: 4 fixed, expert: 3 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-musl-udf-build`, exit 0) |
| Tests | ✓ (`cargo test --workspace`, 917 passed / 0 failed across 40 binaries) |
| Lint | ✓ (`cargo clippy --workspace --all-targets -- -D warnings`, clean) |
| Format | ✓ (`cargo fmt --check`, no changes) |
| Scenario Coverage | ✓ (all plan scenarios below have a passing test) |
| Manual Tests | ✓ (all 7 manual checks below pass) |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | All new/changed logic covered — `StorageBackend` (`storage.rs`), `register_side_store` (`object_store.rs`), scan-spec wire tagging (`spec.rs`) each have dedicated unit tests; see Scenario Coverage below |
| Integration | All 18 planned production call sites exercised transitively via `dispatch_golden`, join goldens, vended-storage tests, and the E2E suite |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (`cargo test --workspace`) | 917 | 917 | 2 (unrelated benchmark tests in `micro_bench`) |
| E2E (`make test-e2e`, 8 binaries) | 190 | 190 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-catalog` | ✓ 0 failures; public-surface probe compiles with `StorageBackend`, without `build_s3_file_io` |
| `git diff` on `dispatch_golden/` fixtures differs only inside `storage` value | ✓ confirmed by the code-reviewer's byte-for-byte normalized diff |
| `rg -n 'StorageBackend::S3'` matches only permitted production sites + `#[cfg(test)]` | ✓ confirmed (`storage.rs`, `vended.rs` S3 arm, `object_store.rs` registration, `connection.rs` selection site, `scan/spec.rs` Default placeholder) |
| `rg -n 'extract_bucket\b|build_s3_store\b|build_s3_file_io'` — no production matches | ✓ confirmed; only the intended negative-assertion string in `catalog_public_surface.rs` |
| `cargo test -p lakehouse-catalog --test catalog_public_surface` | ✓ 0 failures |
| `cargo test -p lakehouse-engine common_blob_wire_is_byte_stable storage_props_wire_encoding_unchanged` | ✓ 0 failures; latter passes with no source edit |
| `cargo test -p lakehouse-catalog vended` | ✓ 0 failures, every pre-refactor assertion byte-identical |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
→ exit 0, clean (re-run after code-review fixes; also clean)
```

### Formatter

```
cargo fmt --check
→ exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | storage-backend-enum | One enum names the storage backend and answers every backend-specific question | `crates/lakehouse-catalog/src/storage.rs` | `catalog_storage_props_carries_the_six_s3_keys_only_when_present`, `secret_values_matches_the_wrapped_props_values_and_order`, `file_io_is_built_from_the_same_key_map_as_catalog_storage_props` | Pass |
| vs-adapter | storage-backend-enum | Every consumer holds a backend and no consumer names one | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `demoted_and_deleted_functions_are_not_declared_public` (renamed from `vended_mechanism_functions_are_not_declared_public` in review), `storage_backend_secret_values_and_file_io_are_reachable` | Pass |
| vs-adapter | storage-backend-enum | Every consumer holds a backend and no consumer names one | `crates/lakehouse-engine/tests/catalog_session_signatures.rs` | `file_resolution_entry_points_take_a_shared_session` | Pass |
| vs-adapter | storage-backend-enum | Every consumer holds a backend and no consumer names one | `crates/lakehouse-engine/src/adapter/connection.rs` | `storage_block_maps_creds_to_storage_props` | Pass |
| vs-adapter | storage-backend-enum | The scan registers its object store without naming the backend | `crates/lakehouse-engine/src/scan/object_store.rs` | `build_s3_store_applies_spec_connection_budget`, `register_side_store_registers_one_store_per_distinct_side`, `join_dimension_side_sharing_the_fact_bucket_is_not_registered_twice`, `join_with_empty_dimension_file_list_registers_only_the_fact_side`, `shared_bucket_join_store_answers_both_sides_sizes_from_the_spec` | Pass |
| vs-adapter | storage-backend-enum | The scan registers its object store without naming the backend | `crates/lakehouse-engine/tests/scan_no_head_test.rs`, `scan_join_test.rs` | existing sized-HEAD and join suites, passing with wrapped fixtures | Pass |
| datafusion-scan | scan-execution-spec-reconstitution | The scan-spec wire carries the backend as a tagged variant | `crates/lakehouse-engine/src/scan/spec.rs` | `common_blob_wire_is_byte_stable`, `from_json_error_never_contains_credentials` | Pass |
| datafusion-scan | scan-execution-spec-reconstitution | The scan-spec wire carries the backend as a tagged variant | `crates/lakehouse-engine/tests/shared_type_reexports.rs` | `storage_props_wire_encoding_unchanged` (unedited), `storage_backend_wire_encoding_tags_the_s3_payload` (new) | Pass |
| vs-adapter | storage-backend-enum / catalog-crate-structure | S3 behavior is unchanged across the refactor | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | all 10 golden fixtures | Pass |
| vs-adapter | pushdown-joins-module-structure | S3 behavior is unchanged across the refactor | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | join golden-SQL full-string assertions | Pass |
| vs-adapter | pushdown-planning-cloud-credentials | S3 behavior is unchanged across the refactor | `crates/lakehouse-catalog/src/vended.rs` | all 10 vended resolution tests | Pass |
| datafusion-scan | scan-execution-memory-and-credentials | S3 behavior is unchanged across the refactor | E2E harness (`make test-e2e`) | full S3 suite (8 binaries, 190 tests) against spark-iceberg-fixtures-provisioned environment | Pass |
| vs-adapter | pushdown-module-structure | Golden fixtures change ONLY in their `storage` value | `dispatch_golden.rs` | `group_by_fallback`, `multi_count_distinct_decline` decline-wrapper assertions | Pass |
| vs-adapter | pushdown-col-types-consolidation | Golden fixtures change ONLY in their `storage` value | `adapter/pushdown/support.rs` | leaf, non-`column`, nameless-column, unresolvable-column guard tests (unedited) | Pass |
| vs-adapter | pushdown-joins-module-structure | Golden strings change ONLY in their `storage` value | `joins/sql_builders.rs` | the three spec-bearing golden strings, `ineligible_join_decline` (unedited) | Pass |
| vs-adapter | catalog-crate-structure | Golden fixtures change ONLY in their `storage` value | `dispatch_golden.rs` | all 10 goldens, verified via normalized `git diff` | Pass |
| vs-adapter | pushdown-catalog-session | Per-shard scan-spec storage changes ONLY by its variant tag | `dispatch_golden.rs` | the five spec-bearing goldens; grant-count and `loadTable`-count assertions unedited | Pass |

## Notes

Implementation followed the plan's four-group parallelization (A → B → C → D) exactly: Group A (enum declaration) landed first since every other lane imports from it; Group B's three lanes (catalog, adapter, scan) landed concurrently as planned and only reached a green whole-workspace build after all three merged, matching the plan's explicit "no per-lane verification gate" note; Group C (goldens + remaining fixtures) and Group D (verification) followed.

One gap in the plan's file enumeration surfaced during Group D: `crates/lakehouse-engine/tests/e2e_int96_timestamp_test.rs` directly destructured `StorageProps` fields off `local_stack_storage()`'s return value, but this file is gated behind the `exasol-e2e` feature and so never appeared in the earlier `cargo test --workspace` runs — the gap only surfaced when `make test-e2e` tried to compile it. Fixed with a one-line unwrap (`let StorageBackend::S3(storage) = local_stack_storage();`); confirmed via `cargo check --workspace --all-targets --all-features` that no other feature-gated file has the same gap.

Code review raised 7 findings, all fixed: 4 standard (doc/test-name wording only, zero logic change) and 3 expert-tagged, all in `object_store.rs` — `register_side_store`'s 6-parameter signature was split into a per-side `ScanSide` struct and a whole-spec `StoreRegistration` struct (also renaming the leaked `s3_max_connections` parameter to backend-agnostic `connection_budget`, without touching the wire field of the same name), plus two stale module/entry-point doc comments. Every fix was re-verified against the full test suite, clippy, fmt, and a full `.so` rebuild + E2E re-run — all green, with zero change to the `shared_bucket_join_store_answers_both_sides_sizes_from_the_spec` and dedup-guard test outcomes that pin the refactor's core safety invariant.

No test was deleted or skipped. `build_s3_store_applies_spec_connection_budget` (naming a deleted function) was repointed at the new seam per the plan, not removed.

# Verification Report: add-delta-table-planning

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Delta table planning lands behind the `FormatReader` seam; zero bytes of the shipped Iceberg path changed; nothing wired into production pushdown. All automated and manual checks green against live infrastructure. |
| Code review | 18 findings — 18 fixed (14 standard, 4 expert) |

| Check | Status |
|-------|--------|
| Build (`.so`, `rust:1.94-bookworm`) | ✓ |
| Tests (host, workspace) | ✓ |
| Tests (Unity + Delta E2E, live stack) | ✓ |
| Tests (Iceberg E2E, live stack) | ✓ |
| Lint (`clippy --all-targets -D warnings`) | ✓ |
| Format (`fmt --check`) | ✓ |
| Spec validation (`speq plan validate`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit + integration (host, `cargo test --workspace`) | 1225 | 1225 | 0 |
| Live E2E — Unity + Delta (`--features unity-e2e --test e2e_unity_test`) | 11 | 11 | 0 |
| Live E2E — Iceberg (`make test-e2e`, 9 binaries) | 244 | 244 | 0 |

Coverage percentage was not measured this run (`cargo llvm-cov` not invoked); scenario-level coverage is enumerated below instead.

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine --lib adapter::pushdown::format` (36 tests — supersedes the plan's `--test delta_log_replay`, see Notes) | ✓ |
| `make unity-up && cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test -- --test-threads=1` | ✓ 11/11, no credential value in output |
| `cargo test -p lakehouse-catalog unity::client` | ✓ |
| `cargo test -p lakehouse-catalog --test catalog_public_surface` | ✓ 15/15 |
| `cargo test -p lakehouse-engine --test pushdown_public_surface` (16 items, up from the plan's 15 — see Notes) | ✓ compiles |
| `cargo test -p lakehouse-engine scan::spec` | ✓ |
| `cargo test -p lakehouse-engine --test scan_two_arg --test scan_plan_shape` (Iceberg regression gate) | ✓ 12/12, no assertion edited |
| `make cross-musl-udf-build` | ✓ exit 0, one `arrow` (58.3.0) and one `object_store` (0.13.2) resolved |
| Unity E2E fails (never skips) without the stack | ✓ — covered by the suite's own `unity_suite_fails_when_stack_unavailable` test, verified during task 2.10's investigation |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s) — 0 warnings, 0 errors
```

### Formatter

```
cargo fmt --all -- --check
exit 0, no diff
```

### Spec validation

```
speq plan validate add-delta-table-planning
Plan 'add-delta-table-planning' validation passed.
Validated 6 delta spec(s). Only pre-existing AND-step-count style warnings; 0 errors.
```

## Scenario Coverage

| Feature | Scenario | Test Location | Test Name | Passes |
|---------|----------|---------------|-----------|--------|
| vs-adapter/delta-table-planning | Delta table resolves current version's active files | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `replay_returns_only_the_files_active_at_the_current_version` | Pass |
| vs-adapter/delta-table-planning | Partition values incl. explicit NULL | `.../format/delta_replay_tests.rs` | `replay_carries_partition_values_and_an_explicit_null` | Pass |
| vs-adapter/delta-table-planning | Deletion vector carried exactly once | `.../format/delta_replay_tests.rs` | `replay_carries_a_readded_files_deletion_vector_exactly_once` | Pass |
| vs-adapter/delta-table-planning | Column-mapping mode + physical names carried once | `.../format/delta_schema_tests.rs` | `replay_carries_name_mode_column_mapping_and_physical_names` | Pass |
| vs-adapter/delta-table-planning | Storage credential resolved through the table's own catalog | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_planning_agrees_under_vended_and_static_credentials` | Pass |
| vs-adapter/delta-table-planning | Vending without a vending key never falls back to static | `.../format/delta_format_reader_tests.rs` | `vending_without_a_vending_key_errors_and_never_falls_back_to_static` | Pass |
| vs-adapter/delta-table-planning | Empty storage location rejected identically under both credential modes | `.../format/delta_format_reader_tests.rs` | `empty_storage_location_errors_identically_under_both_credential_modes` | Pass |
| vs-adapter/delta-table-planning | Unmapped Delta type refused at plan time | `.../format/delta_schema_tests.rs` | `unmapped_delta_type_is_refused_naming_the_column_and_issue_322` | Pass |
| vs-adapter/delta-table-planning | Format reader selected at one site, refuses a mismatched pairing | `.../format/format_tests.rs` | `format_reader_refuses_a_non_delta_table_under_the_unity_source` | Pass |
| vs-adapter/delta-table-planning | Iceberg planning byte-identical through the new seam | `.../format/iceberg_tests.rs` | `iceberg_reader_returns_resolve_file_lists_result_with_no_delta_block` | Pass |
| vs-adapter/delta-table-planning | Iceberg planning byte-identical (characterization) | `crates/lakehouse-engine/tests/scan_two_arg.rs` | existing suite, unedited | Pass |
| vs-adapter/delta-table-planning | Delta adds no production pushdown path | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `unity_kind_pushdown_is_refused_not_iceberg_routed` (unedited) | Pass |
| datafusion-scan/scan-execution-spec-reconstitution | Reconstitution carries Delta table + per-file blocks | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `delta_blocks_round_trip_losslessly_and_leave_iceberg_encodings_byte_identical` | Pass |
| vs-adapter/unity-catalog-client | Unity session reached only through the shared trait | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `load_table_returns_format_tag_vending_key_and_ordered_columns` (absorbed a deleted duplicate — see Notes) | Pass |
| vs-adapter/unity-catalog-client | Listing tags every admitted table Delta, keeps skip filter | `.../unity/client_tests.rs` | `list_tables_tags_every_admitted_table_delta_and_keeps_the_skip_filter` | Pass |
| vs-adapter/unity-catalog-client | Single-table load returns format tag + vending key + columns | `.../unity/client_tests.rs` | `load_table_returns_format_tag_vending_key_and_ordered_columns` | Pass |
| vs-adapter/unity-catalog-client | Single-table load refuses an unrecognized format | `.../unity/client_tests.rs` | `load_table_refuses_an_absent_or_unrecognized_data_source_format` | Pass |
| vs-adapter/catalog-crate-structure | Format tag + vending key extend the public surface | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | compile-time probe | Pass |
| vs-adapter/pushdown-module-structure | Format-reader seam extends the pushdown façade | `crates/lakehouse-engine/tests/pushdown_public_surface.rs` | compile-time probe (26 in-crate / 16 external — corrected, see Notes) | Pass |
| e2e-harness/unity-catalog-e2e-harness | Create virtual schema lists fixture tables + columns | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` (unedited) | Pass |
| e2e-harness/unity-catalog-e2e-harness | Suite resolves seeded Delta table's scan spec over MinIO | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_planning_agrees_under_vended_and_static_credentials` | Pass |

## Notes

**Plan-vs-implementation discrepancies found and reconciled during this audit:**

1. **`tests/delta_log_replay.rs` never exists.** `delta_replay`/`delta_schema` are deliberately `pub(super)` to keep both concrete readers off the crate's public surface — invisible to an external integration-test binary by design. Every scenario the plan assigned to that file lives instead as a crate-internal unit test in `format/delta_replay_tests.rs` or `format/delta_schema_tests.rs`, under the exact function names the plan specifies. No coverage gap; only the file path differs from plan.md's text (plan.md is archived as written, not corrected).
2. **Pushdown façade grew to 26/16, not 25/15.** The `TOO_MANY_ARGUMENTS` code-review fix introduced `ConnectionStorage` — collapsing three parameters (`storage`, `creds`, `allow_http`) threaded identically through `format_reader` and both reader constructors into one struct — as a fifth `pub` item. **Fixed the permanent spec delta** (`vs-adapter/pushdown-module-structure/spec.md`) to state "EXACTLY FIVE items" / 26/16 and name `ConnectionStorage`, so the merged spec matches the shipped probe counts.
3. **One duplicate-test deletion consolidated, not dropped, coverage.** The review's `DUPLICATE_TEST` fix removed `neutral_table_carries_the_format_tag_and_the_opaque_vending_key`, whose input and assertions were identical to `load_table_returns_format_tag_vending_key_and_ordered_columns`. The latter now carries that scenario's sole coverage.

**Real bug found and fixed outside the original task list, during live E2E verification (task 2.10):** the Unity Catalog E2E fixture harness vended a placeholder session token (`notused`) that MinIO genuinely rejects (`403 InvalidTokenId`) — confirmed the engine's token-forwarding code is contract-correct (a real Databricks/Unity vended credential always carries a token that must be forwarded) and fixed the harness instead: `scripts/unity/seed.sh` now mints a real MinIO STS session via SigV4 `AssumeRole`, the same mechanism the Lakekeeper overlay already uses. No production code changed for this fix.

**Environment note for future runs (not a code defect):** `make test-e2e` does not bring up its own compose stack (documented in the Makefile). The base Iceberg-REST stack (`iceberg-rest`, `minio-init`, `spark-iceberg-fixtures`) must be started per CI's sequence before running it; the Unity overlay alone is insufficient. An initial run in this session failed 68/77 tests purely on a missing `iceberg-rest` service (uniform health-check-timeout panic, not per-test logic) and was resolved by bringing up the correct stack — not a regression.

**Residual, out of this plan's scope (tracked for #320/#322):** cross-field uniqueness of `delta.columnMapping.id` under `Id`/`Name` mode is unchecked by this engine (the Delta protocol validates it only on the write path); a log that assigns two columns the same id would still be accepted. Stats-based pruning (#321), reader-feature gating and broad type mapping (#322), and removing the Unity Catalog pushdown refusal (#320) are unchanged and out of scope, as recorded in plan.md's Non-Goals.

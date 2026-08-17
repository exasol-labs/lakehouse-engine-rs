# Verification Report: add-delta-file-pruning

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Delta file pruning ships correctly: partition and statistics pruning wired end-to-end, verified against live Docker infrastructure (Unity Catalog, Exasol, MinIO), zero regressions in the Iceberg path. |
| Code review | 12 findings — 12 fixed (2 correctness-critical: unsound `NOT` negation, unchecked float narrowing) |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-musl-udf-build`, rebuilt against final code) |
| Tests | ✓ (1023 unit/integration passed, 0 failed) |
| Lint | ✓ (`cargo clippy --all-targets --all-features`, 0 warnings) |
| Format | ✓ (`cargo fmt --check`, clean) |
| Scenario Coverage | ✓ (36/36 named tests resolve to real, passing tests) |
| Manual Tests | ✓ (5/5, run live against Docker stack) |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit (lakehouse-engine lib) | `cargo test -p lakehouse-engine --lib` | 1023 | 0 |
| Full workspace (`cargo test`) | all crates, all targets | all green | 0 |
| E2E — Unity/Delta (`make test-e2e-unity`) | live Docker: Exasol + MinIO + Unity Catalog | 23 | 0 |
| E2E — Iceberg regression (`make test-e2e`) | live Docker: Exasol + MinIO + Iceberg REST | 254 | 0 |

Key new-module test counts: `delta_predicate.rs`/`delta_predicate_tests.rs` (translator) — 56 tests. `delta_replay.rs`/`delta_replay_tests.rs` (kernel replay wiring) — 34 tests. `delta_format_reader_tests.rs` — 8 tests. `pushdown_tests.rs` Delta-related — 615+ (crate total). `joins_tests.rs` — 116 tests (includes the per-leg broadcast-join pruning test added during final verification).

### Manual Tests

Run live against `UNITY_DELTA_E2E_VS` on the running Docker stack (Unity Catalog, Exasol, MinIO all healthy).

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL ... BASIC_PARTITIONED WHERE LETTER = 'a'` embeds exactly 2 `.parquet` paths under `letter=a/`, vs 6 unfiltered | ✓ — confirmed exact distinct file sets (2 vs 6) |
| `EXPLAIN VIRTUAL ... MULTI_PART_STATS WHERE ID <= 2` embeds exactly 2 `.parquet` paths, vs 5 unfiltered | ✓ — confirmed exact distinct file sets (2 vs 5), proving struct-stats pruning over the `multi-part-stats` checkpoint |
| `SELECT COUNT(*) ... BASIC_PARTITIONED WHERE LETTER = 'z'` | ✓ — returned `0`, no error, no UDF invocation |
| `SELECT LETTER, "NUMBER", A_FLOAT FROM BASIC_PARTITIONED` (unfiltered) | ✓ — all 6 rows returned, one `LETTER` NULL, identical to pre-change shape |
| `make test-e2e-unity` | ✓ — exit 0, 23/23 passed |

## Tool Evidence

### Linter

```
cargo clippy --all-targets --all-features
Finished `dev` profile [unoptimized + debuginfo] target(s)
0 warnings
```

### Formatter

```
cargo fmt --check
(no output — clean)
```

## Scenario Coverage

All 36 test names referenced by this plan's Scenario Coverage table were cross-checked against the codebase (`grep -rl "fn <name>"` across `crates/`) after code review and gap-fill; every one resolves to a real, passing test. Two documentation-drift corrections were made to the plan's table during final audit (test names implementers chose differ from the plan's proposed names but cover identical behavior — corrected in `plan.md`, not re-implemented), and one genuine coverage gap was found and closed: `each_delta_join_leg_prunes_by_its_own_side_local_predicate` in `joins_tests.rs` did not exist despite being required by the table — added during final verification, proving each leg of a broadcast-eligible Delta-Delta join prunes independently by its own local predicate.

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | delta-file-pruning | Equality on a partition column prunes every file in a non-matching partition | `delta_replay_tests.rs` | `a_partition_equality_prunes_every_file_in_a_non_matching_partition` | Pass |
| vs-adapter | delta-file-pruning | Equality on a partition column prunes every file in a non-matching partition | `delta_replay_tests.rs` | `an_is_null_partition_predicate_resolves_the_default_partition_file_alone` | Pass |
| vs-adapter | delta-file-pruning | Equality on a partition column prunes every file in a non-matching partition | `pushdown_tests.rs` | `a_delta_request_pruned_to_no_file_takes_the_empty_result_route` | Pass |
| vs-adapter | delta-file-pruning | A range predicate prunes files whose min/max bounds exclude the value | `delta_replay_tests.rs` | `a_range_predicate_prunes_files_whose_logged_bounds_exclude_the_value` | Pass |
| vs-adapter | delta-file-pruning | A range predicate prunes files whose min/max bounds exclude the value | `delta_replay_tests.rs` | `a_between_keeps_one_bound_when_the_other_fails_to_convert` | Pass |
| vs-adapter | delta-file-pruning | An untranslatable conjunct disables pruning for that conjunct only | `delta_predicate_tests.rs` | `and_with_untranslatable_child_keeps_translatable_conjunct` | Pass |
| vs-adapter | delta-file-pruning | An untranslatable conjunct disables pruning for that conjunct only | `delta_predicate_tests.rs` | `and_all_untranslatable_returns_none_not_a_true_predicate` | Pass |
| vs-adapter | delta-file-pruning | An untranslatable conjunct disables pruning for that conjunct only | `delta_replay_tests.rs` | `a_partly_untranslatable_conjunction_still_prunes_by_its_translatable_half` | Pass |
| vs-adapter | delta-file-pruning | An untranslatable branch of an OR disables pruning entirely | `delta_predicate_tests.rs` | `or_with_untranslatable_child_returns_none` | Pass |
| vs-adapter | delta-file-pruning | An untranslatable branch of an OR disables pruning entirely | `delta_replay_tests.rs` | `an_or_with_an_untranslatable_branch_keeps_every_file` | Pass |
| vs-adapter | delta-file-pruning | An IN list prunes as an OR-chain of equalities and never as an empty junction | `delta_predicate_tests.rs` | `in_list_translates_to_an_or_chain_of_equalities` | Pass |
| vs-adapter | delta-file-pruning | An IN list prunes as an OR-chain of equalities and never as an empty junction | `delta_predicate_tests.rs` | `empty_in_list_returns_none_not_a_false_predicate` | Pass |
| vs-adapter | delta-file-pruning | An IN list prunes as an OR-chain of equalities and never as an empty junction | `delta_replay_tests.rs` | `an_in_list_prunes_to_the_union_of_its_element_files` | Pass |
| vs-adapter | delta-file-pruning | A literal is typed from the column's Delta type or its node is dropped | `delta_predicate_tests.rs` | per-kind tests (`boolean_literal_becomes_boolean_scalar`, `exactnumeric_literal_becomes_integer_scalar`, `double_literal_becomes_double_scalar`, `string_literal_becomes_string_scalar`, `date_literal_becomes_days_since_the_epoch`, `exactnumeric_literal_rescales_to_the_decimal_column_scale`, `timestamp_literal_becomes_microseconds_on_a_zoneless_column`, `resolve_column_returns_none_for_unknown_column`, `literal_the_column_type_cannot_represent_yields_no_scalar`, `notequal_returns_none`, `empty_string_literal_yields_no_scalar`, `resolve_column_matches_case_insensitively`) | Pass |
| vs-adapter | delta-file-pruning | Enabling the kernel's skipping surfaces no statistic to the engine or the wire | `delta_replay_tests.rs` | `the_stats_disabling_option_is_what_suppresses_pruning` | Pass |
| vs-adapter | delta-file-pruning | Enabling the kernel's skipping surfaces no statistic to the engine or the wire | `delta_format_reader_tests.rs` | `pruning_changes_only_the_file_list_of_the_resolved_scan` | Pass |
| vs-adapter | delta-file-pruning | Enabling the kernel's skipping surfaces no statistic to the engine or the wire | `pushdown_tests.rs` | `a_non_pruning_delta_request_keeps_its_pre_change_field_set_and_carries_no_statistic` | Pass |
| vs-adapter | delta-file-pruning | A predicate the kernel cannot evaluate keeps every file | `delta_replay_tests.rs` | `a_predicate_over_a_statless_or_boolean_column_keeps_every_file` | Pass |
| vs-adapter | delta-file-pruning | A predicate the kernel cannot evaluate keeps every file | `delta_replay_tests.rs` | `pruning_under_column_mapping_records_its_observed_behavior` | Pass |
| vs-adapter | delta-file-pruning | Pruning reaches every request shape and changes no result end to end | `e2e_unity_test.rs` | `unity_delta_filters_prune_the_resolved_file_list` | Pass |
| vs-adapter | delta-file-pruning | Pruning reaches every request shape and changes no result end to end | `joins_tests.rs` | `each_delta_join_leg_prunes_by_its_own_side_local_predicate` | Pass |
| vs-adapter | delta-file-pruning | Pruning reaches every request shape and changes no result end to end | `e2e_unity_test.rs` | `unity_delta_pruned_pushdown_sql_carries_fewer_files_and_drives_the_scan_udf` | Pass |
| vs-adapter | delta-table-planning | A Delta table resolves its current version's active data files (CHANGED) | `delta_replay_tests.rs` | `replay_reads_the_active_files_out_of_a_multi_part_checkpoint` | Pass |
| vs-adapter | delta-table-planning | The Delta reader is reached from production pushdown under the Unity Catalog kind (CHANGED) | `pushdown_tests.rs` | `a_unity_catalog_pushdown_prunes_the_delta_file_list_by_its_filter` | Pass |
| e2e-harness | unity-catalog-e2e-harness-delta-queries | A query whose files were pruned returns the same rows as before pruning | `e2e_unity_test.rs` | `unity_delta_pruned_queries_return_unchanged_rows` | Pass |

## Notes

**Correctness bugs found and fixed by code review, not present in the shipped code:**
1. `predicate_not` negated a possibly-widened child (e.g. `NOT(id = 5 AND untranslatable)` would incorrectly prune files that still hold matching rows). Fixed by threading an exactness flag through the entire translation walk; `NOT` now returns `None` unless every node beneath it translated exactly.
2. Float literal conversion did an unchecked `f64 as f32` cast that could silently round a bound upward, narrowing a predicate below the request and dropping real rows. Fixed to fail open (`None`) when the literal doesn't round-trip exactly through `f32`.

Both were caught before merge, with regression tests added, and re-verified against the full crate test suite (0 regressions) and live E2E infrastructure.

**Known, deliberately recorded gaps (not defects):**
- `literal_timestamputc` (Exasol's real wire node name for a UTC timestamp literal) vs. `literal_timestamp_utc` (the pre-existing synthetic name this translator and the Iceberg translator both match) — a TSTZ comparison never prunes a Delta table today. Pre-existing gap shared with `iceberg_predicate.rs`, tracked as `(#242)`, now explicitly recorded in the `vs-adapter/delta-file-pruning` spec delta. Fails open — no wrong rows, only forgone pruning.
- `predicate_notequal` never prunes (always translates to `None`) — a deliberate, documented design choice, not a gap.

**Deployment note:** the Exasol container's UDF `.so` was found stale (built against SDK 0.21.0) partway through implementation, causing a fingerprint-mismatch failure on live query tests. Rebuilt (`make cross-musl-udf-build`, now SDK 0.22.1) and redeployed (`make bucketfs-upload-so`) during this verification pass — not a defect in this plan's code, but worth noting for anyone resuming this branch on a different Docker host.

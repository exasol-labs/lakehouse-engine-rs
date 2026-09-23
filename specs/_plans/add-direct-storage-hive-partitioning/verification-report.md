# Verification Report: add-direct-storage-hive-partitioning

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `HIVE_PARTITIONING` (default `TRUE`) discovers `key=value` directory segments as `VARCHAR` partition columns and prunes files at plan time before their footers are read. All checklist commands, all named unit and integration scenarios, and all manual verification queries pass. |
| Code review | 16 findings — 16 fixed |

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
| Unit (`cargo test`, lib) | 1343 | 1343 | 0 |
| Unit (workspace, all crates/targets) | 1844 | 1844 | 0 |
| Integration (`make test-e2e`, 16 suites) | 91+ (per-suite counts vary) | all | 0 |
| Integration (`e2e_direct_storage_test.rs`) | 33 | 33 | 0 |

## Tool Evidence

### Build

```
make cross-udf-build → exit 0
```

### Linter

```
cargo clippy --all-targets → clean, 0 warnings
```

### Formatter

```
cargo fmt --check → clean, no changes
```

## Scenario Coverage

| Scenario | Test Location | Test Name | Passes |
|----------|---------------|-----------|--------|
| A key=value directory segment declares a VARCHAR partition column | `tests/e2e_direct_storage_test.rs` | `hive_segments_declare_varchar_partition_columns_with_decoded_values` | Pass |
| A key=value directory segment declares a VARCHAR partition column | `src/adapter/parquet_directory_tests.rs` | `directory_segments_follow_the_key_value_rule_and_decode_values` | Pass |
| A table's partition columns are the union of its files' keys | `tests/e2e_direct_storage_test.rs` | `mixed_layout_unions_partition_keys_and_nulls_the_missing_key` | Pass |
| A table's partition columns are the union of its files' keys | `src/adapter/parquet_directory_tests.rs` | `declared_keys_are_the_ordered_union_and_fill_every_file`, `sample_mode_declares_only_the_sampled_files_keys` | Pass |
| Two partition keys that fold to the same name fail the refresh | `src/adapter/parquet_directory_tests.rs` | `two_declared_keys_folding_to_the_same_name_fail_naming_both_spellings` | Pass |
| A partition key that names a Parquet column overrides it | `tests/e2e_direct_storage_test.rs` | `partition_key_colliding_with_a_parquet_column_overrides_it` | Pass |
| A partition key that names a Parquet column overrides it | `src/adapter/parquet_directory_tests.rs` | `a_key_folding_onto_a_column_drops_the_column_and_keeps_the_key` | Pass |
| A file missing the colliding key's segment fails the refresh | `tests/e2e_direct_storage_test.rs` | `partition_key_collision_with_a_missing_segment_fails_the_refresh` | Pass |
| A file missing the colliding key's segment fails the refresh | `src/adapter/parquet_directory_tests.rs` | `a_file_missing_the_colliding_keys_segment_fails_the_fold_naming_it`, `a_sampled_files_missing_segment_fails_the_fold_under_sample_one_file` | Pass |
| HIVE_PARTITIONING = FALSE reads key=value segments as plain directories | `tests/e2e_direct_storage_test.rs` | `hive_partitioning_false_declares_no_partition_columns` | Pass |
| HIVE_PARTITIONING = FALSE reads key=value segments as plain directories | `src/adapter/parquet_directory_tests.rs` | `hive_partitioning_off_parses_no_segment_and_checks_no_collision` | Pass |
| A predicate on partition columns prunes files before their footers are read | `tests/e2e_direct_storage_test.rs` | `partition_filter_prunes_the_resolved_file_list`, `range_pruning_matches_exasols_native_varchar_ordering`, `zero_matching_files_prune_to_zero_rows_without_error` | Pass |
| A predicate on partition columns prunes files before their footers are read | `src/adapter/pushdown/format/partition_predicate_tests.rs`, `parquet_format_reader_tests.rs` | `partition_nodes_evaluate_under_three_valued_logic`, `range_and_between_nodes_evaluate_under_three_valued_logic`, `a_non_partition_node_never_prunes`, `a_partition_filter_prunes_files_before_their_footers_are_read`, `a_predicate_keeping_no_file_reads_no_footer_and_errors_never` | Pass |
| A declared column absent from every kept file reads NULL | `tests/e2e_direct_storage_test.rs` | `a_column_only_pruned_files_carry_reads_null` | Pass |
| A declared column absent from every kept file reads NULL | `src/adapter/pushdown/format/parquet_format_reader_tests.rs` | `a_declared_column_absent_from_kept_files_is_added_as_a_null_field` | Pass |
| One seam answers the file list and the schema for both callers | `src/adapter/parquet_directory_tests.rs` | `one_seam_returns_files_sizes_schema_and_footers` | Pass |
| Data files are listed recursively in a deterministic order | `src/adapter/parquet_directory_tests.rs` | `listing_is_recursive_filtered_and_deterministic` | Pass |
| The merge mode selects every footer or exactly one | `src/adapter/parquet_directory_tests.rs` | `merge_mode_selects_every_footer_or_the_first` | Pass |
| A file-keep predicate narrows the files before any footer is read | `src/adapter/parquet_directory_tests.rs` | `a_keep_predicate_narrows_files_before_any_footer_is_read` | Pass |
| The format reader is selected at the same one site for a third source | `src/adapter/pushdown/format/format_tests.rs` | `third_scan_source_selects_the_parquet_reader` | Pass |
| A direct-storage table resolves its files and schema through the shared seam | `parquet_format_reader_tests.rs`, `tests/e2e_direct_storage_test.rs` | `resolved_scan_carries_identity_bound_fields_and_no_deletes`, `file_entry_paths_round_trip_to_the_listed_object`, `hive_segments_declare_varchar_partition_columns_with_decoded_values` | Pass |
| The kept files' footers are read at plan time and the resulting cost is stated | `parquet_format_reader_tests.rs`, `tests/e2e_direct_storage_test.rs` | `plan_reads_selected_footers_and_lists_every_file`, `partition_filter_prunes_the_resolved_file_list` | Pass |
| A table's columns and data files come from the one shared directory seam | `src/adapter/direct_storage_tests.rs` | `columns_and_files_come_from_the_shared_seam` | Pass |
| HIVE_PARTITIONING reaches the shared seam on both paths | `direct_storage_properties_tests.rs`, `direct_storage_tests.rs`, `scan_resolution_tests.rs` | `directory_options_derive_both_switches`, `hive_partitioning_reaches_the_seam_on_enumeration`, `hive_partitioning_reaches_the_seam_on_pushdown` | Pass |
| Only first-level directories holding a data file become virtual tables | `tests/e2e_direct_storage_test.rs` | `discovery_scopes_to_first_level_directories_and_namespace_narrows_to_a_subtree` | Pass |

## Manual Testing

| Command | Expected | Result |
|---------|----------|--------|
| `SELECT COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA = 'DIRECT_LAKEHOUSE' AND COLUMN_TABLE = 'SALES'` | `YEAR`, `MONTH` as `VARCHAR(2000000) UTF8` after `ID`, `AMOUNT`, `DISCOUNT` | Pass — exact match |
| `SELECT ID, "YEAR", "MONTH" FROM DIRECT_LAKEHOUSE.SALES ORDER BY ID` | Rows: (1,2026,09), (2,2026,09), (3,2025,NULL), (4,2024,01) | Pass — exact match |
| `EXPLAIN VIRTUAL SELECT ID FROM DIRECT_LAKEHOUSE.SALES WHERE "YEAR" = '2026'` | Pushed file list names only `year=2026/month=09/p1.parquet` | Pass |
| `EXPLAIN VIRTUAL SELECT ID FROM DIRECT_LAKEHOUSE.SALES WHERE "YEAR" > '2025'` | Pushed file list names only `year=2026/month=09/p1.parquet`, excludes 2025/2024 | Pass |
| `SELECT K FROM DIRECT_HIVE_COLLISION.COLLISION_OVERRIDE` | `'1'` (directory value, not stored `99`) | Pass |
| `SELECT COUNT(*) FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA = 'DIRECT_HIVE_OFF' AND COLUMN_TABLE = 'SALES'` | `3` (no partition column) | Pass |
| `SELECT REGION FROM DIRECT_LAKEHOUSE.ENCODED` | One row, `a/b` | Pass |
| `CREATE VIRTUAL SCHEMA ... CATALOG_KIND = 'DIRECT_STORAGE'` over `direct_hive_collision_missing` | Fails, naming `K`, `k`, and `collision_missing_segment/p2.parquet`'s path | Pass (via `partition_key_collision_with_a_missing_segment_fails_the_refresh`, live E2E) |
| `cargo test -p lakehouse-engine parquet_directory` | All seam tests pass | Pass |
| `cargo test -p lakehouse-engine direct_storage` | Enumeration tests pass | Pass |
| `make test-e2e` | `discovery_scopes_to_first_level_directories_and_namespace_narrows_to_a_subtree` passes with `DEEP` declaring `ID`, `Y` | Pass |

## Notes

- Task 2.2.2's stop gate (Rust `str` ordering vs. Exasol's native `VARCHAR` ordering) held: the live
  `range_pruning_matches_exasols_native_varchar_ordering` test confirmed the two orderings agree.
  The test itself initially failed on a representation bug (comparing a percent-encoded path
  segment, `%C3%A9`, against the plain region value `é`, in two sibling test helpers,
  `regions_of` and `regions_named_by`) — not a real ordering mismatch, since both sides had in
  fact kept the same files. Fixed during Phase 5 verification; both are now decoded/encoded
  consistently before comparison. No return to planning was needed.
- One unrelated E2E test, `adapter_detects_container_cpuset` (`e2e_scan_test.rs`, untouched by
  this plan), initially failed because this host has exactly 4 cores, matching the Docker
  Compose default `LH_EXASOL_CPUSET` (`0-3`, also 4 CPUs) — its own documented precondition
  requires the container's cpuset to be strictly narrower than the host. Recreating the `exasol`
  container with `LH_EXASOL_CPUSET=0-2` resolved it; this is a local environment sizing detail,
  not a regression from this plan.
- Manual verification ran against the local Docker stack. The full `make test-e2e` run (16
  independent test binaries, each idempotently re-issuing the shared `LAKEHOUSE_CATALOG_CREDS`
  CONNECTION at its own `setup()`) leaves that CONNECTION in whichever suite's shape last ran.
  Re-running `e2e_direct_storage_test.rs` alone (still 33/33 green) restored the direct-storage
  shape before the manual `exapump` queries above; this is expected multi-suite shared-state
  behavior, not a defect.

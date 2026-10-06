# Verification Report: add-direct-storage-statistics-pruning

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Direct-storage planning drops a file when its Parquet footer bounds prove no row group can match the pushed filter (issue #412). All 76 live cases return the rows the scan returns without pruning. |
| Code review | 11 findings, 11 fixed (one soundness bug: a pushed `-0` literal could drop a file the scan returns rows from) |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-udf-build`, ABI fingerprint check for 3 UDFs) |
| Tests | ✓ (host 1902 passed, 0 failed; direct-storage E2E 38 passed, 0 failed) |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

The full `make test-e2e` set runs again after the version bump in the PR pipeline's test phase. Before the review fixes, it passed all 17 suites (419 tests).

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | Not measured |
| Integration | Not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit and integration (`cargo test --workspace`, host) | 1904 | 1902 | 2 (`tests/micro_bench.rs`, untouched) |
| Integration (E2E) `e2e_direct_storage_test` | 38 | 38 | 0 |
| Integration (E2E) full `make test-e2e` before review fixes | 419 | 419 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL ... WHERE ID <= 5` names `grp=a/f1.parquet` only | ✓ |
| `SELECT ID ... WHERE ID <= 5 ORDER BY ID` returns 1 to 5 | ✓ |
| `EXPLAIN VIRTUAL ... WHERE U32 > 3000000000` names `grp=b/f2.parquet` only | ✓ |
| `EXPLAIN VIRTUAL ... WHERE QUIET <= 5` names both files | ✓ |
| `SELECT ID ... WHERE DEC256 < '5'` returns 1 to 4 and 9 to 12 | ✓ |
| `EXPLAIN VIRTUAL ... WHERE ID <= 5 AND SECOND(DT, 3) = 0` names both files | ✓ |
| `EXPLAIN VIRTUAL ... WHERE ID > 100` names no `prune_types/` file, and `COUNT(*)` returns 0 | ✓ |
| `docs/catalogs.md` has the stored-NaN limitation paragraph linking #393 | ✓ |
| `cargo test -p lakehouse-engine parquet_directory` (28 passed) | ✓ |
| `cargo test -p lakehouse-engine --test scan_parquet_pruning` (4 passed) | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --all-targets: exit 0
cargo clippy --all-targets --features exasol-e2e: exit 0
Only warning line: cargo failed to auto-clean its registry cache (environment file permission).
```

### Formatter

```
cargo fmt --all -- --check: exit 0
speq plan validate add-direct-storage-statistics-pruning: pass (step-count warnings only)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | direct-storage-statistics-pruning | A footer whose row-group bounds exclude the filter drops the file | `tests/e2e_direct_storage_test.rs`; `format/parquet_format_reader_tests.rs` | `footer_statistics_prune_every_supported_type`; `a_footer_range_outside_the_filter_drops_the_file` | Pass |
| vs-adapter | direct-storage-statistics-pruning | Row-group statistics evaluate the filter under three-valued logic | `format/partition_predicate_tests.rs`; `format/footer_statistics_tests.rs` | `range_facts_evaluate_under_three_valued_logic`; `a_file_is_kept_iff_one_row_group_can_be_true` | Pass |
| vs-adapter | direct-storage-statistics-pruning | Integer, float, decimal, string, boolean, date, and timestamp columns prune on their footer bounds | `tests/e2e_direct_storage_test.rs` | `footer_statistics_prune_every_supported_type` (cases 15 to 53, 61, 62) | Pass |
| vs-adapter | direct-storage-statistics-pruning | A statistic whose ordering the Parquet specification leaves undefined keeps the file | `format/footer_statistics_tests.rs` | `an_undefined_ordering_or_statistic_keeps_the_file` | Pass |
| vs-adapter | direct-storage-statistics-pruning | A column the footer statistics cannot describe keeps the file | `format/footer_statistics_tests.rs`; `format/parquet_format_reader_tests.rs` | `an_undescribable_column_keeps_the_file`; `a_nested_column_keeps_a_file_its_leaf_bounds_would_exclude`; `a_column_the_scan_compares_as_text_prunes_no_file` | Pass |
| vs-adapter | direct-storage-statistics-pruning | Columns outside the prunable types keep both files end to end | `tests/e2e_direct_storage_test.rs` | `footer_statistics_prune_every_supported_type` (cases 54 to 59) | Pass |
| vs-adapter | direct-storage-statistics-pruning | A float column prunes ordering and equality comparisons on its bounds alone | `format/partition_predicate_tests.rs`; `tests/e2e_direct_storage_test.rs` | `float_comparisons_prune_on_bounds_and_negations_keep`; `footer_statistics_prune_every_supported_type` (cases 24 to 36, 69) | Pass |
| vs-adapter | direct-storage-statistics-pruning | A float bound or literal is widened or rejected so it never drops a matching row | `format/footer_statistics_tests.rs`; `format/partition_predicate_tests.rs`; `tests/scan_parquet_pruning.rs` | `float_bounds_widen_zero_and_reject_nan`; `float_literals_convert_to_the_scans_double_only_for_footer_statistics`; `scan_compares_pushed_literals_as_the_footer_scope_assumes` | Pass |
| vs-adapter | direct-storage-statistics-pruning | A literal the scan compares inexactly keeps every file, and the scan's rows for it are pinned live | `tests/e2e_direct_storage_test.rs`; `tests/scan_parquet_pruning.rs` | `footer_statistics_prune_every_supported_type` (cases 70, 71, 73); `scan_compares_pushed_literals_as_the_footer_scope_assumes` | Pass |
| vs-adapter | direct-storage-statistics-pruning | The scan's comparison against a stored NaN is characterized live and pinned | `tests/e2e_direct_storage_test.rs` | `stored_nan_comparison_outcome_is_pinned` | Pass |
| vs-adapter | direct-storage-statistics-pruning | Float pruning shares the stored-NaN exposure of the other float-bound pruning layers | `tests/e2e_direct_storage_test.rs` | `stored_nan_comparison_outcome_is_pinned` (out-of-range probes) | Pass |
| vs-adapter | direct-storage-statistics-pruning | Statistics pruning composes with every consumer of the file list | `format/parquet_format_reader_tests.rs`; `tests/e2e_direct_storage_test.rs`; `format/catalog_parquet_format_reader_tests.rs` | `statistics_pruning_keeps_unsampled_files_and_reaches_zero_files`; `statistics_pruning_narrows_a_join_leg`; existing tests unchanged | Pass |
| vs-adapter | direct-storage-statistics-pruning | Statistics pruning reads only the part of the filter the scan evaluates | `format/parquet_format_reader_tests.rs`; `joins/rendering_tests.rs`; `tests/e2e_direct_storage_test.rs` | `a_filter_the_scan_does_not_evaluate_prunes_no_file_on_statistics`; `a_leg_statistics_filter_holds_only_the_conjuncts_its_scan_carries`; case 74 | Pass |
| datafusion-scan | scan-execution-memory-and-credentials | Scan enables Parquet row-group and page pruning so the reader skips non-matching data (CHANGED) | `tests/scan_parquet_pruning.rs` | `scan_enables_rowgroup_and_page_pruning` (unchanged) | Pass |
| vs-adapter | direct-storage-hive-partitioning | A predicate on partition columns prunes files before their footers are read (CHANGED) | `tests/e2e_direct_storage_test.rs`; `format/parquet_format_reader_tests.rs` | `partition_filter_prunes_the_resolved_file_list`; `a_partition_filter_prunes_files_before_their_footers_are_read` | Pass |
| vs-adapter | direct-storage-table-planning | The kept files' footers are read at plan time and the resulting cost is stated (CHANGED) | `format/parquet_format_reader_tests.rs` | `plan_reads_selected_footers_and_lists_every_file` | Pass |
| vs-adapter | parquet-directory-seam | One seam answers the file list and the schema for both callers (CHANGED, Background only) | `adapter/parquet_directory_tests.rs` | `one_seam_returns_files_sizes_schema_and_footers` (unchanged) | Pass |

## Notes

- **Case 70 is not fixed (#TBD).** `AMOUNT = 1234567.89` on a `DECIMAL(10,2)` column returns no rows where an exact comparison returns ID 16. The scan parses a non-integer numeral as `f64` and compares the decimal column in `Decimal128(30,15)`. Setting `datafusion.sql_parser.parse_float_as_decimal = true` fixed it but broke FLOAT and DOUBLE comparisons (decimal overflow errors) and the NaN probe, so the change was reverted. `crates/lakehouse-engine/src/scan/mod.rs` is unchanged. The footer scope refuses the literal, so no file is dropped. Evidence: decision-log.md [M1].
- **Cases 71 and 73 are tracked by #461.** The scan compares a nanosecond column against a microsecond-truncated literal (case 71) and a millisecond column in milliseconds (case 73). The cases are pinned to the scan's current rows.
- **Case 72** was rewritten as `F <= 16777217`, which returns the exact rows.
- **Cases 26, 28, 36** were replaced because Exasol pushes the planned clauses in a different shape (decision-log.md [M3]).
- **Bug found by the E2E run (decision-log.md [M4]):** `parquet` 58.3.0 marks a chunk without min or max as deprecated, so the all-NULL chunk of a modern writer lost its valid null count. The deprecated gate now applies only when a min or max is present.
- **Soundness bug found by the review:** DataFusion 54.1 reads the numeral `-0` as `Int64(0)`, so the scan compares against `+0.0`, while the footer scope read `-0.0`. A negative-zero literal now stays unconvertible, so the file is kept.
- **Unreachable code paths with no test:** the `max_rep_level > 0` gate, because `parquet_to_arrow_schema` turns a repeated leaf into a List first.
- **Environment:** the Docker Exasol container runs with `LH_EXASOL_CPUSET=0-2` so `adapter_detects_container_cpuset` passes. Other E2E suites overwrite the shared `LAKEHOUSE_CATALOG_CREDS` connection, so a direct-storage query outside a test's `setup` can fail until a direct-storage test runs again.

# Tasks: add-direct-storage-statistics-pruning

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A)
- [x] 1.1 Add encode_parquet_with and nan_probe fixture
- [x] 1.2 Add E2E test stored_nan_comparison_outcome_is_pinned
- [x] 1.3 Add prune_types fixture and physical-type assertions
- [x] 1.4 Add E2E test footer_statistics_prune_every_supported_type (76 cases)
- [x] 1.5 Run live baselines against Docker Exasol, fix expected IDs, record decisions
- [x] 1.6 Update spec deltas with measured facts
- [x] 2.1 ColumnFacts trait and partition-value impl
- [x] 2.2 Orderings::within, three-valued Compare, nan_row_reachable [expert]
- [x] 2.3 FooterStatistics literal scope and for_footer_statistics
- [x] 2.4 Unit tests in partition_predicate_tests.rs
- [x] 2.5 scan_compares_pushed_literals_as_the_footer_scope_assumes
- [x] 3.1 Create footer_statistics.rs with footer_keeps and RowGroupFacts
- [x] 3.2 Implement footer statistics gates
- [x] 3.3 footer_statistics_tests.rs
- [x] 4.1 Pass the scan-evaluated filter as statistics filter [expert]
- [x] 4.2 Wire footer pruning into ParquetFormatReader::resolve_scan
- [x] 4.3 Reader and join-leg tests for the statistics filter
- [x] 4.4 Update plan_reads_selected_footers test and add footer pruning tests
- [x] 5.1 Assert Files column in E2E cases
- [x] 5.2 statistics_pruning_narrows_a_join_leg
- [x] 5.3 NaN probe out-of-range assertions
- [x] 5.4 Run full e2e_direct_storage_test and make test-e2e
- [x] 6.1 Rewrite Plan-time footer cost paragraph in docs/catalogs.md
- [x] 6.2 Add stored-NaN limitation paragraph to docs/catalogs.md

## Phase 3: Verification
- [x] 3.1 Run cargo test, clippy (both feature sets), fmt, make cross-udf-build, make test-e2e

## Phase 4: Review Fixes
- [x] 4.1 In `partition_predicate.rs`, delete `within_microseconds` and guard the `("literal_timestamp", DataType::Timestamp(..))` arm with `exact_timestamp(value, TimeUnit::Microsecond).is_some()`
- [x] 4.2 In `partition_predicate.rs`, add `enum Polarity { Plain, Negated }` with `flipped`, replace the `negated: bool` parameters of `Node::reachable`, `Orderings::reachable`, and `nan_row_reachable`, drop `nan_possible` from `Orderings::within`, add `Orderings::with_nan_row`, and update `RowGroupFacts::orderings` and the test `RangeFacts::orderings`
- [x] 4.3 Delete the `decision-log.md` citations from the comments in `partition_predicate.rs`, `footer_statistics.rs`, `parquet_format_reader.rs`, `tests/scan_parquet_pruning.rs`, and `tests/e2e_direct_storage_test.rs`, citing the spec feature or an issue where a source is still needed
- [x] 4.4 In `joins/rendering.rs`, add `screen_side_local` that partitions side-local conjuncts once, and make `type_screened_leg_filter` and `screened_leg_filter` both delegate to it
- [x] 4.5 In `partition_predicate_tests.rs`, delete `literal_in` and the direct `Translator { .. }` constructions, and express each literal assertion through `PartitionPredicate::for_footer_statistics(..).keeps(..)` or `PartitionPredicate::from_filter(..).keeps(..)`, keeping every assertion label
- [x] 4.6 In `parquet_format_reader_tests.rs`, add `batch_bytes(&RecordBatch) -> Vec<u8>` and use it in `int64_parquet` and `a_column_the_scan_compares_as_text_prunes_no_file`
- [x] 4.7 In `tests/scan_parquet_pruning.rs`, add `local_file_size(file_url) -> u64` that panics with the fixture name on a missing file, and use it in `pruning_spec`, `nested_spec`, and `literal_spec`
- [x] 4.8 In `tests/e2e_direct_storage_test.rs`, add `PRUNE_TYPES_FIRST_B_ID`, make `in_grp_a` use it, and rewrite `prune_types_day` without the literals `8` and `9`
- [x] 4.9 In `partition_predicate.rs`, make `double_literal` return `None` for a negative-zero literal, and add the `-0` DOUBLE and FLOAT assertions to `float_bounds_widen_zero_and_reject_nan` in `footer_statistics_tests.rs` [expert]
- [x] 4.10 In `footer_statistics.rs`, replace `footer_keeps` with `FooterStatisticsFilter { predicate, columns }` built by `new(statistics_filter, schema, logical)` with `comparable_columns` moved from `parquet_format_reader.rs`, use it in `ParquetFormatReader::resolve_scan`, and update the `keeps` test helper [expert]
- [x] 4.11 In `scan_resolution.rs`, add `ScanFilters { pruning, scan_evaluated }`, take it in `TableScanResolver::resolve` and `joins/planning.rs` `resolve_one_join_side`, and build it with named fields in `handle_pushdown`, `plan_join`, and `scan_resolution_tests.rs` [expert]

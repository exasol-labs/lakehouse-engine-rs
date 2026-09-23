# Tasks: add-direct-storage-hive-partitioning

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: Seam and property plumbing)
- [x] 2.1.1 Add `DirectoryOptions { merge_mode, hive_partitioning }` to `adapter/parquet_directory.rs` and `DirectStorageProperties::directory_options()`; replace every `merge_mode` carrier with `options: DirectoryOptions`
- [x] 2.1.2 Replace `partition_segments` with gated partition discovery; rename `ParquetFile.partition_segments` to `partition_values`
- [x] 2.1.3 Declare keys over the unfiltered listing (union or sampled), fill every file's map, add `partition_columns` and append nullable `Utf8` schema fields
- [x] 2.1.4 Uppercase-fold collision handling: key-vs-key fails, key-vs-column overrides or fails per the segment rule
- [x] 2.1.5 Add file-keep predicate parameter to `resolve_parquet_directory`, evaluated before any footer read
- [x] 2.1.6 Unit tests for the seam, properties, and enumeration scenarios; update every census call site

## Phase 2: Implementation (Group B: Planning, pruning, E2E)
- [x] 2.2.1 Add `declared_columns` parameter to `TableScanResolver::resolve` and `ScanSource::DirectParquet`; thread through join planning and update the nine `resolve` call sites
- [x] 2.2.2 [expert] Add `adapter/pushdown/format/partition_predicate.rs`: three-valued evaluation of partition filter nodes, string ordering verified live against Exasol VARCHAR comparison
- [x] 2.2.3 In `ParquetFormatReader::resolve_scan`, build the keep predicate, copy partition values into `FileEntry`, percent-encode path segments, append absent declared columns as nullable fields
- [x] 2.2.4 Unit tests in `partition_predicate_tests.rs` and `parquet_format_reader_tests.rs`
- [x] 2.2.5 E2E harness: `VsProps::with_hive_partitioning`, `write_parquet_fixture` verbatim key writing
- [x] 2.2.6 E2E fixtures in `tests/e2e_direct_storage_test.rs` (sales years, encoded region, mixed layout, regions ordering fixture, collision override/missing bases)
- [x] 2.2.7 E2E tests named in Scenario Coverage: union, override collision, missing-segment collision, pruning (equality/range/zero-match), ordering-vs-Exasol-native test
- [x] 2.2.8 Update `docs/catalogs.md`: `HIVE_PARTITIONING` row, partition-column rules paragraph, plan-time footer cost paragraph

## Phase 3: Verification
- [x] 3.1 `make cross-udf-build`
- [x] 3.2 `cargo test`
- [x] 3.3 `make test-e2e`
- [x] 3.4 `cargo clippy --all-targets`
- [x] 3.5 `cargo fmt --check`

## Phase 4: Review Fixes
- [x] 4.1 In `parquet_directory.rs`, replace `declare_partition_keys` with `declaration_scope(raw_files, mode)` and `union_of_partition_keys(raw_files)`; declare keys from `union_of_partition_keys(declaration_scope(..))` and build override candidates from `union_of_partition_keys(&raw_files)`; move the doc comment to `declaration_scope`
- [x] 4.2 In `resolve_parquet_directory`, rename `override_candidates` to `keys_any_file_carries`, delete its inline comment, and append the `candidates` sentence to `validate_key_column_overrides`'s doc
- [x] 4.3 In `parquet_directory.rs`, extract `kept_files`, `fold_sources_for`, and `attach_footers` from `resolve_parquet_directory` so it contains only calls to named steps
- [x] 4.4 Fix the `resolve_parquet_directory` doc: keys are declared from the UNFILTERED listing and never depend on `keep`; fold-every-file reads only kept files' footers
- [x] 4.5 Update the `MergeMode` doc and the `ParquetDirectory.partition_columns` doc to name the declaration scope
- [x] 4.6 Replace the `.so`-boundary reason in the `PartitionKeepPredicate` doc with the `.await`-across-`Send`-futures reason
- [x] 4.7 Add `const HIVE_DEFAULT_PARTITION` and use it in `decode_partition_value`
- [x] 4.8 Add `a_default_or_empty_key_segment_counts_as_carrying_the_colliding_key` to `parquet_directory_tests.rs`
- [x] 4.9 Add `sample_mode_reads_the_first_unfiltered_footer_even_when_keep_rejects_it` to `parquet_directory_tests.rs`
- [x] 4.10 Extend `directory_segments_follow_the_key_value_rule_and_decode_values` with the empty-key `=x` and invalid-UTF-8 `v=%FF` fixtures
- [x] 4.11 Rewrite `hive_partitioning_reaches_the_seam_on_pushdown` in `scan_resolution_tests.rs` against a `RecordingCatalog` listing one non-Parquet file, asserting `TRUE` prunes it and `FALSE` fails on the footer read; drop the comment and the `"maybe"` refusal half
- [x] 4.12 Delete `merge_schema_resolves_the_same_mode_on_both_paths` from `direct_storage_properties_tests.rs`
- [x] 4.13 Delete `ParquetFormatReader::new`, make its fields `pub(super)`, and build it with a struct literal in `format_reader` and `parquet_format_reader_tests.rs::try_resolve`
- [x] 4.14 Update the root `Cargo.toml` `percent-encoding` comment to name both the decode and encode uses
- [x] 4.15 Run `check_key_spelling_collisions` before the keep step over the declaration scope's raw files, test-first with `a_key_spelling_collision_fails_even_when_keep_prunes_one_spelling` [expert]
- [x] 4.16 Replace the fold-source tuple with `struct FoldSource { path, size, raw_keys, kept_index }` and attach footers by direct index instead of `iter_mut().find` [expert]

## Phase 5: Verification Fixes
- [x] 5.1 Fix `regions_of` in `tests/e2e_direct_storage_test.rs` to percent-decode the extracted region segment before comparing it to plain `REGION_VALUES` strings, using `percent_encoding::percent_decode_str(...).decode_utf8()`
- [x] 5.2 Fix the sibling bug in `regions_named_by` (same file): percent-encode each `REGION_VALUES` entry before substring-matching it against the pushed SQL's percent-encoded `region=.../p.parquet` path, using `percent_encoding::utf8_percent_encode`

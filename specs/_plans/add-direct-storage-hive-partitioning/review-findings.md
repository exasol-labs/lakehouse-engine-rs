# Code Review Findings: add-direct-storage-hive-partitioning

## Summary
- Files reviewed: 25
- Total findings: 16 (standard: 14, expert: 2)

## Standard fixes

### crates/lakehouse-engine/src/adapter/parquet_directory.rs

#### [SELECTOR_ARGUMENT] `declare_partition_keys` picks its scope from a `MergeMode` argument, and one caller forces the other mode to get a different operation
- Location: lines 102, 106, 277-292
- Issue: `declare_partition_keys(raw_files, mode)` branches on `mode` to choose all files or the first file. Line 106 then calls it with a hard-coded `MergeMode::FoldEveryFile`, whatever the real mode is, only to get "the union over every listed file". The argument chooses the operation, and the second call site needs an inline comment to explain why it lies about the mode.
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, replace `declare_partition_keys` with two private functions. `fn declaration_scope(raw_files: &[RawFile], mode: MergeMode) -> &[RawFile]` returns `raw_files` under `FoldEveryFile` and `&raw_files[..raw_files.len().min(1)]` under `SampleOneFile`. `fn union_of_partition_keys(raw_files: &[RawFile]) -> Vec<String>` holds the current first-appearance loop and takes no mode. In `resolve_parquet_directory`, set `declared_keys = union_of_partition_keys(declaration_scope(&raw_files, options.merge_mode))` and the override-candidate list to `union_of_partition_keys(&raw_files)`. Move the current doc comment of `declare_partition_keys` to `declaration_scope`.

#### [INLINE_COMMENT] Three-line inline comment justifies `override_candidates`
- Location: lines 103-105
- Issue: the inline comment "Always the fold-every-file union, regardless of the actual mode: ..." is there because the variable name and the `declare_partition_keys(..., MergeMode::FoldEveryFile)` call do not explain themselves. The guardrails forbid inline comments.
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory.rs::resolve_parquet_directory`, rename `override_candidates` to `keys_any_file_carries` and delete the three-line inline comment. Append one sentence to the doc comment of `validate_key_column_overrides`: "`candidates` holds every listed file's keys whatever the merge mode, because a key only an unsampled file carries still collides with a column the sampled footer stores."

#### [MIXED_ABSTRACTION_LEVEL] `resolve_parquet_directory` mixes step orchestration with loop-level bookkeeping
- Location: lines 108-133, 149-153
- Issue: the function orchestrates the steps (list, declare, check, read footers, validate, fold, build the schema). It also inlines three low-level blocks: a keep loop that fills two index-aligned parallel vectors (`files`, `kept_raw_keys`), a `match` that builds anonymous `(StorePath, u64, HashSet<String>)` tuples, and a footer-attach loop over `files.iter_mut()`. Three downstream functions destructure the tuple as `(path, _, _)`.
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, extract the keep loop into `fn kept_files<'a>(raw_files: &'a [RawFile], declared_keys: &[String], keep: &PartitionKeepPredicate) -> Vec<(&'a RawFile, ParquetFile)>`, which deletes the parallel `kept_raw_keys` vector. Extract the `match options.merge_mode` block into `fn fold_sources_for(mode: MergeMode, raw_files: &[RawFile], kept: &[(&RawFile, ParquetFile)]) -> Vec<FoldSource>`, using the `FoldSource` struct the Expert `[PERFORMANCE_ISSUE]` finding introduces. Extract the footer attach into `fn attach_footers(kept: Vec<(&RawFile, ParquetFile)>, fold_sources: &[FoldSource], read: &[ArrowReaderMetadata]) -> Vec<ParquetFile>`, which returns the files instead of mutating them. After this, `resolve_parquet_directory` should contain only calls to named steps.

#### [OUTDATED_COMMENT] The `resolve_parquet_directory` doc says the declared partition columns apply after filtering, which contradicts the code
- Location: lines 89-94
- Issue: the doc says "the declared partition columns and the fold-every-file mode's footer-read set both apply AFTER filtering". The code declares the keys from the UNFILTERED listing (line 102), and `declare_partition_keys`'s own doc says "always computed over the UNFILTERED listing, so the declared columns never depend on a keep predicate".
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, replace the sentence "the declared partition columns and the fold-every-file mode's footer-read set both apply AFTER filtering, so a rejected file costs no footer read" in the doc of `resolve_parquet_directory` with "the partition keys are declared from the UNFILTERED listing and never depend on `keep`, while the fold-every-file mode reads only kept files' footers, so a rejected file costs no footer read".

#### [OUTDATED_COMMENT] The `MergeMode` and `ParquetDirectory.partition_columns` docs predate partition declaration
- Location: lines 16, 71-72
- Issue: the `MergeMode` doc says "both modes return the same file list — only the footer set differs". The mode now also decides which files declare the partition keys. Under a keep predicate, the two modes can return different file lists: a key that only a later file carries is declared, and so prunable, under `FoldEveryFile` alone. The `partition_columns` doc says the columns are empty when "no file carries a partition segment". Under `SampleOneFile` they are also empty when only unsampled files carry one.
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, change the `MergeMode` doc to "Which files' footers the fold reads and whose paths declare the partition keys: every listed file, or only the first." Change the `ParquetDirectory.partition_columns` doc's "or no file carries a partition segment" to "or no file in the merge mode's declaration scope (every file, or the first file under `SampleOneFile`) carries a partition segment".

#### [OUTDATED_COMMENT] The `PartitionKeepPredicate` doc gives a false reason for `Send + Sync`
- Location: line 46
- Issue: the doc says "`Send + Sync` so it can cross the `.so` boundary's async call". Nothing crosses the `.so` boundary here. The bound is needed because the seam holds `&PartitionKeepPredicate` across `.await` points inside the `Send` futures its callers return (`FormatReader::resolve_scan` is `+ Send`).
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, replace "— `Send + Sync` so it can cross the `.so` boundary's async call" in the `PartitionKeepPredicate` doc with "; `Send + Sync` because the seam holds a reference to it across `.await` inside the `Send` futures its callers return".

#### [MAGIC_NUMBER] Hive's default-partition token is a bare string literal
- Location: line 266
- Issue: `decode_partition_value` compares against the inline literal `"__HIVE_DEFAULT_PARTITION__"`. This is a named Hive convention that stands in for NULL.
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, add a module-scope `const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";` and use it in `decode_partition_value`.

### crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs

#### [MISSING_BOUNDARY_TEST] No test that a default-partition or empty `k=` segment counts as carrying the colliding key
- Location: tests around lines 523-630 (collision tests)
- Issue: the spec (`direct-storage-hive-partitioning`, "A file missing the colliding key's segment fails the refresh") requires that a file under `k=__HIVE_DEFAULT_PARTITION__/` or `k=/` carries the segment, reads `K` as NULL, and does not fail. Plan tasks 1.2 and 1.4 built the raw carried-keys set for exactly this case. No unit or E2E test places a `K`-storing file under either segment, so reverting `validate_key_column_overrides` to the post-fill map would pass every test.
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs`, add `#[tokio::test] async fn a_default_or_empty_key_segment_counts_as_carrying_the_colliding_key()`. Store three files, each with a nullable `K` Int32 column: `{TABLE_ROOT}/k=1/p1.parquet`, `{TABLE_ROOT}/k=__HIVE_DEFAULT_PARTITION__/p2.parquet`, and `{TABLE_ROOT}/k=/p3.parquet`. Resolve under `options(MergeMode::FoldEveryFile)` with `&keep_all` and `.expect(...)` success. Assert `column_types(&directory.schema) == vec![("k".to_string(), DataType::Utf8)]`. Assert that `file_at(&directory, "p2.parquet").partition_values.get("k")` and the same for `p3.parquet` both equal `Some(&None)`.

#### [MISSING_BOUNDARY_TEST] No test for `SampleOneFile` when the keep predicate rejects the sampled file
- Location: `a_keep_predicate_narrows_files_before_any_footer_is_read`, line 675
- Issue: the plan (task 1.5) and the seam doc say `SampleOneFile` reads the first UNFILTERED file's footer, kept or not. Only the `FoldEveryFile` case of the keep predicate is tested. The case that matters, where the sampled file itself is rejected, needs these results: its footer is read, the schema comes from it, it is absent from `files`, and no kept file gets its footer. It has no test.
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs`, add `#[tokio::test] async fn sample_mode_reads_the_first_unfiltered_footer_even_when_keep_rejects_it()`. Store `{TABLE_ROOT}/year=2025/p1.parquet` (column `a` Int32) and `{TABLE_ROOT}/year=2026/p2.parquet` (column `b` Int32). Resolve with `options(MergeMode::SampleOneFile)` and a keep closure that accepts only `year == Some("2026")`. Assert `paths(&directory) == [p2]`, `probe.files_read() == [p1]`, `column_types(&directory.schema)` is `[("a", Int32), ("year", Utf8)]`, and `file_at(&directory, "p2.parquet").footer.is_none()`.

#### [MISSING_BOUNDARY_TEST] Segment parsing leaves the empty-key and invalid-UTF-8 branches untested
- Location: `directory_segments_follow_the_key_value_rule_and_decode_values`, line 335
- Issue: `parse_partition_segments` skips a segment with an empty key (`=x`, the `^[^/=]+` half of the rule). `decode_partition_value` keeps the raw text when percent-decoding does not yield valid UTF-8. The test exercises neither branch.
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory_tests.rs::directory_segments_follow_the_key_value_rule_and_decode_values`, add the fixtures `{TABLE_ROOT}/=x/p7.parquet` and `{TABLE_ROOT}/v=%FF/p8.parquet`. Assert `file_at(&directory, "p7.parquet").partition_values.get("")` is `None` and `directory.partition_columns` contains no `""`, with the message "a segment with an empty key declares nothing". Assert `file_at(&directory, "p8.parquet").partition_values.get("v") == Some(&Some("%FF".to_string()))`, with the message "a value that does not decode to UTF-8 keeps its raw text".

### crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs

#### [ASSERTION_FREE_TEST] `hive_partitioning_reaches_the_seam_on_pushdown` does not assert that the switch reaches the seam
- Location: lines 513-559
- Issue: the loop half runs `resolve("events", None, &[])` against an endpoint that answers every listing empty, and only checks that the call does not fail, for both `TRUE` and `FALSE`. It would still pass if `RequestSession::DirectStorage` ignored `hive_partitioning` or hard-coded it. The refusal half repeats `direct_storage_properties_tests.rs`. The test is mapped to the scenario "HIVE_PARTITIONING reaches the shared seam on both paths" but gives no evidence for it on the pushdown path.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs::hive_partitioning_reaches_the_seam_on_pushdown`, delete the three-line `//` comment above the test. Delete the `"maybe"` refusal half. Spawn a `RecordingCatalog` whose responder answers any target containing `list-type=2` with a non-truncated `ListBucketResult` holding one `<Contents><Key>events/year=2026/p.parquet</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><Size>18</Size></Contents>` (18 is the byte length of the body below), and answers every other target with `(200, "not a parquet file".to_string())`. Resolve `"events"` with the filter `{"type":"predicate_equal","left":{"type":"column","name":"YEAR"},"right":{"type":"literal_string","value":"2099"}}`. Under `HIVE_PARTITIONING = 'TRUE'`, assert `Ok` with `partition_columns == ["year"]` and no files. Under `'FALSE'`, assert an `Err` whose text contains `"failed to read the Parquet footer"`, which proves no key was declared and no file was pruned.

### crates/lakehouse-engine/src/adapter/direct_storage_properties_tests.rs

#### [DUPLICATE_TEST] `merge_schema_resolves_the_same_mode_on_both_paths` is a strict subset of `directory_options_derive_both_switches`
- Location: lines 174-192
- Issue: the older test asserts `MergeMode::for_merge_schema(resolved.merge_schema)` maps `TRUE` to `FoldEveryFile` and `FALSE` to `SampleOneFile`. The new `directory_options_derive_both_switches` asserts the same mapping through `directory_options()`, which is now the one derivation site both paths use, and also covers `HIVE_PARTITIONING`.
- Fix: In `crates/lakehouse-engine/src/adapter/direct_storage_properties_tests.rs`, delete `merge_schema_resolves_the_same_mode_on_both_paths` and its doc comment.

### crates/lakehouse-engine/src/adapter/pushdown/format/parquet_format_reader.rs

#### [TOO_MANY_ARGUMENTS] `ParquetFormatReader::new` grew to five arguments
- Location: lines 36-52
- Issue: `new(store, table_root, options, declared_columns, connection)` takes five arguments. The body only copies them into fields. The sibling `IcebergFormatReader` has no constructor and is built with a struct literal in `format_reader`.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/format/parquet_format_reader.rs`, mark the five `ParquetFormatReader` fields `pub(super)` and delete `impl ParquetFormatReader { fn new }`. In `crates/lakehouse-engine/src/adapter/pushdown/format/mod.rs::format_reader`, build `ParquetFormatReader { store, table_root, options, declared_columns, storage: connection.storage }` in the `ScanSource::DirectParquet` arm. In `parquet_format_reader_tests.rs::try_resolve`, build the same struct literal with `storage: &storage`.

### Cargo.toml

#### [OUTDATED_COMMENT] The dependency comment names only half of `percent-encoding`'s use
- Location: lines 85-86
- Issue: the comment says the crate "Percent-decodes Hive partition-directory values". `parquet_format_reader.rs` also uses it to percent-encode `FileEntry.path` segments (`URL_PARSE_UNSAFE`, `utf8_percent_encode`). Someone who removes the decode path based on this comment would break the encode path.
- Fix: In the root `Cargo.toml`, change the comment to "# Percent-decodes Hive partition-directory values and percent-encodes direct-storage FileEntry paths; already resolved to 2.3.2 transitively through url and object_store."

## Expert fixes

### crates/lakehouse-engine/src/adapter/parquet_directory.rs

#### [UNTESTED_ERROR_PATH] The key-spelling collision check covers only kept files, so a pruned query returns wrong rows instead of failing
- Location: lines 102, 123-135 (`check_key_spelling_collisions` over `fold_sources`)
- Issue: under `FoldEveryFile`, keys are declared over the UNFILTERED listing (line 102), but `check_key_spelling_collisions` runs only over `fold_sources`, which are the KEPT files. The failure path "two keys folding to the same name" therefore does not fire when pruning drops every file carrying one spelling. Example: a table created with only `year=2026/` directories later gains a `Year=2025/` directory. Then `declared_keys = ["Year", "year"]`, and every file's filled map holds both keys. `PartitionPredicate::keeps` resolves `YEAR` to the first uppercase match in `BTreeMap` order, which is `"Year"` (`'Y' < 'y'`). So `WHERE YEAR = '2026'` sees `None` for the `year=2026` file and prunes it. It also prunes the `Year=2025` file. The query returns zero rows, with no error, from a schema holding two same-named columns. Plan Impact and the docs promise that such a table fails. The only test (`two_declared_keys_folding_to_the_same_name_fail_naming_both_spellings`) uses `keep_all`, so the gap is untested. This check needs paths only and reads no footer, so its scope can equal the declaration scope at zero I/O cost.
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory.rs::resolve_parquet_directory`, run `check_key_spelling_collisions` BEFORE the keep loop, over the same raw files the keys are declared from: every `RawFile` under `FoldEveryFile`, and the first `RawFile` under `SampleOneFile` (use `declaration_scope` from the Standard `[SELECTOR_ARGUMENT]` fix), each paired with `raw_key_set(raw)`. Remove the current call that runs over `fold_sources`. Write the test first. In `parquet_directory_tests.rs`, add `#[tokio::test] async fn a_key_spelling_collision_fails_even_when_keep_prunes_one_spelling()`. Store `{TABLE_ROOT}/Year=2025/p1.parquet` and `{TABLE_ROOT}/year=2026/p2.parquet`. Resolve under `options(MergeMode::FoldEveryFile)` with a keep closure that accepts only `values.get("year").and_then(|v| v.as_deref()) == Some("2026")`. Assert `Err` whose text contains `Year`, `year`, and both file paths. Run it red, then apply the change and run it green.

#### [PERFORMANCE_ISSUE] Footer attach is quadratic in the kept-file count
- Location: lines 149-153
- Issue: each fold source is matched to its `ParquetFile` with `files.iter_mut().find(|file| &file.path == path)`, which is O(n²) under `FoldEveryFile`. The version before this change attached footers with an O(n) `zip`. Measured with a standalone `rustc -O` reproduction using realistic Hive paths (`warehouse/direct/sales/year=…/month=…/part-XXXXXXXX-c000.snappy.parquet`): n=10,000 takes 122.8 ms against 7.6 µs for `zip`; n=50,000 takes 4.58 s against 53 µs; n=100,000 takes 19.64 s against 165 µs. This is single-threaded adapter CPU on every `CREATE`/`REFRESH` and every pushdown request, and `docs/catalogs.md` states that nothing bounds a table's file count (#419).
- Fix: In `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, replace the `(StorePath, u64, HashSet<String>)` fold-source tuple with a private `struct FoldSource { path: StorePath, size: u64, raw_keys: HashSet<String>, kept_index: Option<usize> }`. `kept_index` is the source's position in the returned `files`: `Some(i)` for the i-th kept file under `FoldEveryFile`. Under `SampleOneFile` it is `Some(0)` exactly when the first raw file passed `keep` (it is then `files[0]`), and `None` otherwise. Change `validate_key_column_overrides` and `fold_schemas` to take `&[FoldSource]`. Replace the `files.iter_mut().find(...)` loop with a direct `files[index].footer = Some(Arc::clone(footer.metadata()))` for each source whose `kept_index` is `Some(index)`. Keep `merge_mode_selects_every_footer_or_the_first`, `a_keep_predicate_narrows_files_before_any_footer_is_read`, and the Standard `sample_mode_reads_the_first_unfiltered_footer_even_when_keep_rejects_it` test green. The last one proves a sampled but rejected file attaches no footer to any kept file.

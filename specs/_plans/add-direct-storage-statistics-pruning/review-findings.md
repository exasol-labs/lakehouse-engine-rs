# Code Review Findings: add-direct-storage-statistics-pruning

## Summary
- Files reviewed: 20
- Total findings: 11 (standard: 8, expert: 3)
- Soundness: one path can drop a file the scan returns rows from (the `-0` literal, first Expert finding). The other drop paths match the scan's comparison at DataFusion 54.1. These paths are the nanosecond timestamp unwrap-cast for `=`, `IN`, and `BETWEEN`, `total_cmp` float ordering with zero widening, decimal scale matching, the raw-versus-rewritten filter (the rewrite touches only function nodes), and the broadcast and N-scan join legs. `cargo clippy -p lakehouse-engine --all-targets` is clean, and the affected unit tests pass (81 passed).

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/format/partition_predicate.rs

#### [SHRINKABLE] `within_microseconds` re-implements `exact_timestamp` at microsecond precision
- Location: lines 539 to 544, used at line 520
- Issue: `within_microseconds(value)` returns the same answer as `exact_timestamp(value, TimeUnit::Microsecond).is_some()`. Both read the fraction after the last `.` and accept it when no non-zero digit follows the sixth. The copy also carries the bare literal `6`, which `exact_timestamp` names through its `TimeUnit::Microsecond` arm.
- Fix: In `partition_predicate.rs`, delete `within_microseconds` and change the guard of the `("literal_timestamp", DataType::Timestamp(..))` arm at line 520 to `if exact_timestamp(value, TimeUnit::Microsecond).is_some()`.

#### [BOOLEAN_FLAG_PARAMETER] Polarity and NaN possibility travel as bare `bool` flags
- Location: line 263 (`Node::reachable(facts, negated: bool)`), line 120 (`Orderings::reachable(self, comparison, negated: bool)`), line 142 (`nan_row_reachable(comparison, negated: bool)`), line 100 (`Orderings::within(min, max, literal, nan_possible: bool)`, 4 parameters)
- Issue: `negated` selects the NaN branch in `nan_row_reachable`. The call sites read `reachable(facts, false)` and `operand.reachable(facts, !negated)`, and neither says what the flag means. `Orderings::within` takes four parameters, and the last is a flag that is only copied into a field.
- Fix: In `partition_predicate.rs`, add `#[derive(Clone, Copy)] enum Polarity { Plain, Negated }` with `fn flipped(self) -> Self`. Replace the `negated: bool` parameter of `Node::reachable`, `Orderings::reachable`, and `nan_row_reachable` with `polarity: Polarity`. `PartitionPredicate::keeps` passes `Polarity::Plain`, the `Not` arm passes `polarity.flipped()`, and `nan_row_reachable` returns `Reachable::ANY` for `Polarity::Negated` or `Comparison::NotEqual`. Drop the `nan_possible` parameter from `Orderings::within`, so it always returns `nan_possible: false`. Add `pub(super) fn with_nan_row(self) -> Self`, which returns the value with `nan_possible: true`. In `footer_statistics.rs` `RowGroupFacts::orderings`, replace the last line with `Orderings::within(&min, &max, literal).map(|orderings| if statistics.nan_possible { orderings.with_nan_row() } else { orderings })`. Apply the same change to `RangeFacts::orderings` in `partition_predicate_tests.rs`.

#### [WORK_TRACKING_COMMENT] Comments cite entries of this plan's `decision-log.md`
- Location: `partition_predicate.rs` lines 17, 138, 508; `footer_statistics.rs` lines 105, 182; `parquet_format_reader.rs` line 46; `tests/scan_parquet_pruning.rs` line 590; `tests/e2e_direct_storage_test.rs` lines 728, 2550
- Issue: Each citation names an entry of `specs/_plans/add-direct-storage-statistics-pruning/decision-log.md`. Recording moves that file under `specs/_recorded/`, where every recorded plan has its own `decision-log.md`. After recording, a bare `decision-log.md [3]` names no particular file. AGENTS.md admits a comment that cites a spec or an issue. Each listed comment already states its reason in its own words.
- Fix: Delete the parenthesized `decision-log.md ...` citation from each listed comment and keep the rest of the sentence. Where a sentence then needs a source, cite the spec feature `vs-adapter/direct-storage-statistics-pruning` or an issue number instead. The NaN rule comment at `partition_predicate.rs` line 138 already cites `#393` and needs nothing more.

### crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs

#### [INFORMATION_LEAKAGE] `screened_leg_filter` recomputes the type-acceptance partition that `type_screened_leg_filter` already made
- Location: lines 168 to 188 (`screened_leg_filter`), lines 134 to 147 (`type_screened_leg_filter`)
- Issue: `type_screened_leg_filter` partitions the side-local conjuncts with `type_accepts` and rewrites the accepted set. `screened_leg_filter` then partitions the same conjuncts with `type_accepts` a second time to produce `carried`. The doc comment calls the function "the one owner", so that the rendered scan and statistics pruning cannot disagree. However, the agreement rests on two independent passes that happen to use the same predicate. If only `type_screened_leg_filter` changed its acceptance rule, `carried` could hold a conjunct that the leg's scan does not evaluate.
- Fix: In `joins/rendering.rs`, move the body of `type_screened_leg_filter` into a private `fn screen_side_local(side_local: &Json, col_types: &[(String, String)]) -> ScreenedLegFilter`. That function computes `accepted` once. It sets `carried: Some(accepted)` when `type_accepted_rewrite(&accepted, col_types)` succeeds, and `carried: None` in every other case. Make `type_screened_leg_filter` call it and return `(screened.scan, screened.type_declined)`, so its existing tests stay unchanged. Make `screened_leg_filter` return `screen_side_local(&side_local, col_types)` directly. Delete the second `partition_conjuncts` call.

### crates/lakehouse-engine/src/adapter/pushdown/format/partition_predicate_tests.rs

#### [IMPLEMENTATION_COUPLED_TEST] Literal tests build the private `Translator` and call `typed_literal`
- Location: line 860 (`literal_in`); lines 920 and 929 (direct `Translator { .. }` constructions); uses in `float_literals_convert_to_the_scans_double_only_for_footer_statistics` (line 870) and `footer_literals_convert_only_to_the_scans_value` (line 959)
- Issue: These assertions read the private `Translator` struct, its `LiteralScope` field, and the private `typed_literal` method. Refactoring how the predicate stores its literal rules would break the tests while pruning behavior stays the same, for example by replacing the scope field with two translator types. The behavior under test is observable through `PartitionPredicate::for_footer_statistics(..).keeps(..)` and `PartitionPredicate::from_filter(..).keeps(..)`.
- Fix: In `partition_predicate_tests.rs`, delete `literal_in` and the two direct `Translator { .. }` constructions, and express each assertion through `keeps`. A literal that converts to `v` keeps a row group bounded `[v, v]` under `=` and drops one bounded `[v.next_up(), v.next_up()]` (`f64::next_up` is stable since Rust 1.86). A literal that does not convert keeps a row group whose bounds exclude it under `=`. For the partition scope, assert with `PartitionPredicate::from_filter` over `file_with(..)` values. Keep every existing assertion label.

### crates/lakehouse-engine/src/adapter/pushdown/format/parquet_format_reader_tests.rs

#### [SHRINKABLE] Two new copies of the `ArrowWriter` byte-writing block
- Location: lines 84 to 96 (`int64_parquet`); lines 499 to 504 inside `a_column_the_scan_compares_as_text_prunes_no_file`
- Issue: Both blocks open an `ArrowWriter` over a `Vec<u8>`, write one batch, and close the writer. `parquet_bytes` in `parquet_fixture_tests.rs` holds the same block, so the two new copies pass the Rule of Three.
- Fix: In `parquet_format_reader_tests.rs`, add `fn batch_bytes(batch: &RecordBatch) -> Vec<u8>` that holds the writer block. Make `int64_parquet` build its batch and return `batch_bytes(&batch)`. Replace the inline writer in `a_column_the_scan_compares_as_text_prunes_no_file` with `let bytes = batch_bytes(&batch);`.

### crates/lakehouse-engine/tests/scan_parquet_pruning.rs

#### [SHRINKABLE] A third copy of the local file-size lookup, which reads a missing file as size 0
- Location: lines 501 to 504 (`literal_spec`), duplicating lines 61 to 64 (`pruning_spec`) and lines 210 to 213 (`nested_spec`)
- Issue: `literal_spec` is the third helper that strips `file://` and reads the file size with `.unwrap_or(0)`. If the fixture file is missing, the helper builds a `FileEntry` of size 0 instead of failing with the file's name.
- Fix: In `tests/scan_parquet_pruning.rs`, add `fn local_file_size(file_url: &str) -> u64`. It reads `std::fs::metadata` of the path with the `file://` prefix stripped and calls `.unwrap_or_else(|e| panic!("stat fixture {file_url}: {e}"))` on the result. Use it in `pruning_spec`, `nested_spec`, and `literal_spec`.

### crates/lakehouse-engine/tests/e2e_direct_storage_test.rs

#### [MAGIC_NUMBER] `prune_types_day` hard-codes the group boundary that `in_grp_a` names
- Location: lines 694 to 701 (`prune_types_day`), line 690 (`in_grp_a`)
- Issue: `prune_types_day` branches on the literal `8` and subtracts the literal `9`. `in_grp_a` and `prune_types_second` express the same `grp=a`/`grp=b` boundary through the helper. If the boundary moved, both literals here would also need editing.
- Fix: In `tests/e2e_direct_storage_test.rs`, add `const PRUNE_TYPES_FIRST_B_ID: i64 = 9;` and make `in_grp_a` return `id < PRUNE_TYPES_FIRST_B_ID`. Rewrite `prune_types_day` as follows. Compute `let (epoch_day, first_id) = if in_grp_a(id) { (EPOCH_DAYS_2024_01_01, 1) } else { (EPOCH_DAYS_2025_01_01, PRUNE_TYPES_FIRST_B_ID) };`, then return `epoch_day + i32::try_from(id - first_id).expect("prune_types day offsets fit i32")`.

## Expert fixes

### crates/lakehouse-engine/src/adapter/pushdown/format/partition_predicate.rs

#### [MISSING_BOUNDARY_TEST] A `-0` literal converts to `-0.0`, but the scan compares against `+0.0`
- Location: line 547 (`double_literal`), reached from line 525 and from `float_column_literal` at line 564
- Issue: The footer scope can drop a file that the scan returns rows from. DataFusion 54.1 parses the unary-minus numeral `-0` as `Int64(0)`: `datafusion-sql` `expr/unary_op.rs` line 70 calls `parse_sql_number(n, true)`, which returns `lit(0i64)`. DataFusion's coercion then casts that integer to `+0.0`. `double_literal` parses the same text with `str::parse::<f64>`, which returns `-0.0`. Both sides order floats with `total_cmp`, where `-0.0 < +0.0`. Take `D < -0` over a row group whose minimum is `-0.0` and that holds no smaller value. `Orderings::within` finds the minimum equal to the literal, so `<` cannot be TRUE, and `footer_keeps` drops the file. The scan returns the stored `-0.0` row, because `-0.0 < +0.0` in total order. `NOT (D >= -0)` takes the same path. A scratch program confirmed both parse results. The path is reachable only when Exasol pushes the integer text `-0`. For example, Exasol pushes a DOUBLE literal with an integral value as `literal_exactnumeric` text, as it pushes `2E0` as `"2"`. The fix is sound whatever Exasol pushes. No test covers a zero literal with a sign.
- Fix: In `partition_predicate.rs`, make `double_literal` also return `None` when the parsed value is a negative zero (`value == 0.0 && value.is_sign_negative()`). Add to its doc comment that DataFusion reads the integer numeral `-0` as `Int64(0)` and compares against `+0.0`. In `footer_statistics_tests.rs` `float_bounds_widen_zero_and_reject_nan`, add two assertions. First, `less("D", "-0")` keeps a file written from `doubles(&[-0.0, 1.0])` under `[("d", DataType::Float64)]`. Second, `less("F", "-0")` keeps a file written from a `Float32Array` of `[-0.0, 1.0]` under `[("f", DataType::Float32)]`. Run both assertions before the code change and confirm they fail.

### crates/lakehouse-engine/src/adapter/pushdown/format/footer_statistics.rs

#### [INFORMATION_LEAKAGE] The reader must pass one `columns` slice to two calls, and it owns half of the comparable-type rule
- Location: `footer_statistics.rs` line 26 (`footer_keeps`), line 183 (`compares_as_folded`); `parquet_format_reader.rs` lines 67 to 72 and line 91 (`comparable_columns`)
- Issue: Two modules split the rule that decides which columns footer statistics may compare, and in which type. `parquet_format_reader.rs` computes that set in `comparable_columns` (the tag round-trip) and passes it twice: once to `PartitionPredicate::for_footer_statistics` and once to `footer_keeps`. Nothing ties the two arguments together, so a caller could type literals against one set and bounds against another. `footer_statistics.rs` holds the other half of the same rule in `compares_as_folded` (FLOAT16 and time-zoned timestamps). The reader therefore knows three internals of the footer view: the tag rule, the predicate constructor, and the per-file call.
- Fix: In `footer_statistics.rs`, replace `footer_keeps` with `pub(super) struct FooterStatisticsFilter { predicate: PartitionPredicate, columns: Vec<(String, DataType)> }`. Build it with `pub(super) fn new(statistics_filter: &Json, schema: &Schema, logical: &[LogicalField]) -> Self`. The constructor computes the columns with `comparable_columns`, which moves from `parquet_format_reader.rs` together with its doc comment, and builds the predicate with `PartitionPredicate::for_footer_statistics`. Add `pub(super) fn keeps(&self, file: &ParquetFile) -> bool` with the current body of `footer_keeps`. Give the struct a doc comment stating its invariant: literals and bounds are typed from one column set. In `ParquetFormatReader::resolve_scan`, replace lines 68 to 71 with `let statistics = FooterStatisticsFilter::new(statistics_filter, &schema, &logical);` followed by `files.retain(|file| statistics.keeps(file));`, and remove the imports that become unused. In `footer_statistics_tests.rs`, make the `keeps` helper construct `FooterStatisticsFilter { predicate: PartitionPredicate::for_footer_statistics(Some(filter), &columns), columns }` and call `.keeps(file)`, so every existing assertion stays unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/scan_resolution.rs

#### [TOO_MANY_ARGUMENTS] `resolve` takes two adjacent `Option<&Json>` filters, and swapping them compiles silently
- Location: `scan_resolution.rs` line 118 (`TableScanResolver::resolve`, 4 parameters); `joins/planning.rs` line 223 (`resolve_one_join_side`, 6 parameters)
- Issue: `resolve` grew to four parameters, and `resolve_one_join_side` grew to six. The new `statistics_filter` sits next to `filter_json` and has the same type. A call with the two arguments swapped compiles. The tests that pass one filter for both arguments (`scan_resolution_tests.rs` line 532) would still pass. A swapped caller would let footer statistics read a filter that the adapter applies in its own outer `WHERE`, the case that `ParquetFormatReader::resolve_scan` documents as unsound.
- Fix: In `scan_resolution.rs`, add `pub(super) struct ScanFilters<'a> { pub(super) pruning: Option<&'a Json>, pub(super) scan_evaluated: Option<&'a Json> }`. Give it a doc comment stating that `scan_evaluated` is the part of `pruning` the scan itself evaluates, and `None` when the adapter applies the filter in its own outer `WHERE`. Change `resolve` to `resolve(&self, table_identifier: &str, filters: ScanFilters<'_>, declared_columns: &[(String, String)])`. Forward `filters.pruning` where `filter_json` was used, and pass `filters.scan_evaluated` as `ScanSource::DirectParquet { statistics_filter, .. }`. Change `resolve_one_join_side` in `joins/planning.rs` to take one `filters: ScanFilters<'_>` in place of `filter_json` and `statistics_filter`. Update the callers in `pushdown/mod.rs` (`handle_pushdown`), `joins/mod.rs` (`plan_join`), and `scan_resolution_tests.rs`, and build the struct with named fields at every call site.

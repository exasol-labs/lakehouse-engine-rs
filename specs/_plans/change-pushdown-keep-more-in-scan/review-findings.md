# Code Review Findings: change-pushdown-keep-more-in-scan

## Summary
- Files reviewed: 17
- Total findings: 8 (standard: 7, expert: 1)

## Standard fixes

### crates/vs-expression/src/scan_typing_tests.rs

#### [DUPLICATE_TEST] The unit test claims a scenario that the E2E test proves
- Location: line 1399, `a_date_part_function_takes_its_fixed_native_type`
- Issue: The test carries `/// Scenario: A date-part function takes its fixed native type`, and `e2e_split_aggregate_columns_match_native_oracle` (`crates/lakehouse-e2e/tests/e2e_local/capability.rs` line 3637) carries the same line. `specs/testing.md` § Coverage rule gives a unit test the line only when it is the scenario's one proof. The plan's Scenario Coverage table maps this scenario to the E2E test alone. The unit test itself stays: it pins the precision (`DECIMAL(4,0)` for `YEAR`, `DECIMAL(2,0)` for the rest), which the E2E row oracle cannot tell apart from a wider integer type.
- Fix: In crates/vs-expression/src/scan_typing_tests.rs, delete the line `/// Scenario: A date-part function takes its fixed native type` above `a_date_part_function_takes_its_fixed_native_type`. Keep the test body unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [OUTPUT_PARAMETER] `flatten_conjuncts` returns its result through a mutated argument
- Location: line 763
- Issue: `flatten_conjuncts(filter, &mut out)` fills a vector the caller supplies. This change moved it into `support.rs` as a shared `pub(super)` function, and each of its three callers repeats the same two-line setup: `ScanTranslator::screen_conjuncts` (support.rs line 714), `partition_conjuncts` (joins/rendering.rs line 86), and the leg-equality scan in joins/broadcast_policy.rs (line 77, wrapped in `if let Some(filter)`).
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, change the signature to `pub(super) fn flatten_conjuncts(filter: &Json) -> Vec<&Json>` and keep the recursion in a private helper that takes the accumulator. Make its doc comment state the return value: the top-level conjuncts of `filter`, nested `AND` nodes flattened, each `OR` or `NOT` kept as one conjunct. Update the three callers: `screen_conjuncts` iterates `flatten_conjuncts(filter)`, `partition_conjuncts` in crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs iterates `flatten_conjuncts(filter).into_iter()`, and crates/lakehouse-engine/src/adapter/pushdown/joins/broadcast_policy.rs replaces the `let mut conjuncts` block with `filter.map(flatten_conjuncts).unwrap_or_default()`.

### crates/lakehouse-engine/src/adapter/pushdown/mod.rs

#### [MIXED_ABSTRACTION_LEVEL] The dispatcher edits request JSON inline for the split WHERE
- Location: lines 200-236, `build_dispatch_sql`
- Issue: `build_dispatch_sql` routes request shapes. Its new `ScanWhere::Split` arm also clones the request JSON, overwrites the `filter` key, and repeats the seven-argument `qualified_single_table_fallback_pushdown` call that the `fallback` closure already wraps. The arm differs from `fallback()` only in the emission it passes.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, add `PushdownRequest::with_filter(&self, filter: Json) -> Self` next to `with_json`, returning the request with its `filter` replaced, with a one-line doc comment. In crates/lakehouse-engine/src/adapter/pushdown/mod.rs, change the `fallback` closure to take `emitted: &Emitted`, pass `emitted` at every existing `fallback()` call, and reduce the `ScanWhere::Split { declined, .. }` arm to `return fallback(&single_table_emission(request, &pushdown.with_filter(declined.clone()), scan.columns())?);`, keeping its two-line comment above it.

### crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs

#### [VAGUE_TEST_NAME] The test name and message describe pruning, which the test no longer observes
- Location: line 891, `pruning_input_unchanged_when_df_render_declines`
- Issue: The test asserts that `split_where` returns `ScanWhere::Split` whose declined half holds the original `SECOND` and `LENGTH` conjuncts. It observes no pruning input. `handle_pushdown` passes the raw request filter to the resolver before `split_where` runs, and the declined half is a new `predicate_and` of two conjuncts. The assertion message "the same trees the resolver prunes with" therefore no longer describes what the test checks.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs, rename `pruning_input_unchanged_when_df_render_declines` to `a_split_where_keeps_each_declined_conjunct_as_exasol_sent_it`, and change its assertion message to "the declined half must hold the declined conjuncts as Exasol sent them, not their scan rewrites".

### crates/lakehouse-e2e/tests/e2e_local/capability.rs

#### [SENTINEL_ERROR_VALUE] `fan_out_emits` returns an empty string when the plan has no EMITS clause
- Location: line 2862
- Issue: `fan_out_emits` maps a missing `EMITS (` clause to `""`. `where_split_mismatch` then reads that as "the fan-out does not emit ID". The four cases with `emits_id: false` therefore pass the check of the scenario "A declined filter does not widen the wrapper's projection" on a plan that has no fan-out EMITS clause at all.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/capability.rs, change `fan_out_emits` to return `Option<&str>`. In `where_split_mismatch`, return `Some(format!("`{}`: the plan has no fan-out EMITS clause: {plan}", case.query))` when it returns `None`, and compare `emits_id` only when it returns `Some`.

#### [MISSING_BOUNDARY_TEST] The fixture precondition does not establish the row that makes the split discriminate
- Location: lines 2933-2939, `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan`
- Issue: The precondition runs case 0 over the virtual table, so it runs the split that the test proves, and asserts ids `[5, 7]`. Its message claims that each conjunct removes rows the other keeps, but `[5, 7]` is only the result of the conjunction. The native-copy comparison catches a scan that drops `ID > 1` only while the native copy holds a row that the `LIKE` keeps and `ID > 1` removes (ID 1 today). If ID 1 stopped matching the `LIKE`, the precondition would still pass and a dropped scan filter would return native-equal rows. `specs/testing.md` § Correctness oracles requires a fixture guard to prove the shape the row test needs. Case 0 already compares the same query with the native copy, so the virtual-table assertion adds nothing.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/capability.rs, `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan`, delete the `row_scan` precondition. After `create_native_copy`, assert that `int_column(&conn.query_columns(&format!("SELECT ID FROM {NATIVE_COPY} WHERE C_DECIMAL_A LIKE '1%' ORDER BY ID"))[0])` equals `vec![1, 5, 7]`, with the message "fixture precondition: ID 1 must match the LIKE, so a scan that drops `ID > 1` returns a row the native copy does not".

### crates/lakehouse-engine/src/scan/raw_scan_tests.rs

#### [MAGIC_NUMBER] The row's epoch-millisecond literal needs a comment to be read
- Location: lines 1127-1128, `datafusion_truncates_an_overprecise_timestamp_literal`
- Issue: `let millis = 1_748_736_000_123_i64;` is an opaque epoch value, and the comment `// 2025-06-01 00:00:00.123` restates what the number encodes. `AGENTS.md` § Code style allows a comment only for a non-obvious why. `chrono` is already a dependency of `lakehouse-engine` (`Cargo.toml` line 55).
- Fix: In crates/lakehouse-engine/src/scan/raw_scan_tests.rs, replace the comment and the literal with `let millis = chrono::NaiveDate::from_ymd_opt(2025, 6, 1).and_then(|day| day.and_hms_milli_opt(0, 0, 0, 123)).expect("2025-06-01 00:00:00.123 is a valid instant").and_utc().timestamp_millis();`.

## Expert fixes

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [INFORMATION_COUPLING] The per-conjunct screen skips the rule the typed pass applies to each operand of an AND
- Location: lines 688-724, `ScanTranslator::split_where` and `ScanTranslator::screen_conjuncts`; line 739, the doc of `ScanWhere::Declined`
- Issue: Two places now decide whether a conjunct runs in the scan, and they disagree. The whole-filter render types the `predicate_and` node, and `rewrite_for_scan` declines that node when an operand is text (`refuse_implicit_text`, crates/vs-expression/src/scan_typing.rs line 592). `screen_conjuncts` renders each operand alone through `ScanTranslator::expression`, which never sees the `AND` node, so a text operand renders. The `_ => ScanWhere::Declined(raw)` arm of `split_where` hides the gap when no other conjunct declines. In that case the `Declined` doc ("No conjunct renders") is also false, because every conjunct renders alone. Observed on this clone's local stack with the deployed `.so`:
  - `SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_VARCHAR AND ID > 1` takes `Declined` (the scan spec has no `filter`). Exasol raises "operands in AND must be a boolean expression" (SQL state 42000), the error native Exasol raises.
  - `... WHERE C_VARCHAR AND C_DECIMAL_A LIKE '1%' AND ID > 1` takes `Split`. The scan spec carries `"filter":"(\"C_VARCHAR\" AND (1 < \"ID\"))"`, and the scan UDF fails with "Cannot infer common argument type for logical boolean operation Utf8 AND Boolean" (SQL state 22002).
  - Native Exasol rejects both queries with the 42000 error. The rows do not differ, only the error. `screen_leg_filter` had the same gap before this change and now shares the code, so the fix covers N-scan join legs too.
- Fix:
  1. Write the failing test first. In crates/lakehouse-e2e/tests/e2e_local/capability.rs, add an `Oracle::Error` variant whose branch in `declined_conjunct_mismatch` calls `native_copy_mismatch` with `PathCheck::Wrapper` and `Expect::Error`. Add the case `SELECT ID FROM {T} WHERE C_VARCHAR AND C_DECIMAL_A LIKE '1%' AND ID > 1` with `oracle: Oracle::Error` and `emits_id: true` to `DECLINED_CONJUNCT_CASES`, and raise the array length to 9. It fails today because the error classes differ.
  2. In crates/vs-expression/src/scan_typing.rs, add to `impl ScanTyping` a `pub fn render_conjunct(self, conjunct: &Json) -> Result<String, UdfError>` that calls `refuse_implicit_text(conjunct, &[self.infer_type(conjunct)])?` and then returns `self.render_expression(conjunct)?.sql`. Its doc comment states that an operand of a top-level `AND` renders alone only where it renders inside the `AND`.
  3. In crates/lakehouse-engine/src/adapter/pushdown/support.rs, add `ScanTranslator::conjunct(self, conjunct: &Json) -> Option<String>`, which calls `self.typing().render_conjunct(&compare_binary_bytes(conjunct.clone(), self.columns)).ok()`, and call it from `screen_conjuncts` in place of `self.expression(...)`, pushing the returned SQL.
  4. In the same file, change the doc of `ScanWhere::Declined` to "The filter declines and no conjunct renders alone: the raw tree, which Exasol applies itself."
  5. Run the new E2E case and confirm that the virtual query now raises the native 42000 error and that its plan holds `C_VARCHAR` in the `LHS_T0` wrapper's `WHERE` and `(1 < "ID")` in the scan filter.

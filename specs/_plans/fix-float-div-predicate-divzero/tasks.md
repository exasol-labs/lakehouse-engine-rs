# Tasks: fix-float-div-predicate-divzero

## PR Lifecycle
- [x] resolved
- [x] implemented
- [ ] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: Live pre-fix reproduction)
- [x] 1.1 Bring up the local Docker stack and reproduce issue #370's single-table predicate case: run the `FACT_LINEITEM` query with the `(L_LINENUMBER - L_LINENUMBER)` divisor, confirm the pushed filter from `EXPLAIN VIRTUAL` `PUSHDOWN_SQL`, and record the row count against the native oracle. Record the same for the `0/0` shape and for the broadcast-join fact-leg shape.
- [x] 1.2 Measure the GUARDED shape live against the `LHVS.GT_LINEITEM_SCAN` native oracle, in BOTH conjunct orders, using divisor `(L_LINENUMBER - 1)` and guard `(L_LINENUMBER - 1) <> 0`. Confirm from `EXPLAIN VIRTUAL` `PUSHDOWN_SQL` that both conjuncts reach the scan in one pushed filter, and record the pre-fix GUARDED and UNGUARDED row counts separately for each conjunct order. Write the measured outcome into the spec delta scenario.

## Phase 2: Implementation (Group B: Checked-division rendering)
- [x] 2.1 Add a failing unit test asserting the DataFusion dialect renders `vs_checked_float_div("A", 1)` for a `FLOAT_DIV` node, and that the Exasol dialect still renders `("A" / 1)`.
- [x] 2.2 Export the function name as one public constant with a doc comment stating the full contract the registered implementation must satisfy.
- [x] 2.3 Change the `FLOAT_DIV` arm so the DataFusion dialect renders the call from that constant and no longer wraps the left operand in `CAST(... AS DOUBLE)`. Leave the Exasol dialect byte-identical.
- [x] 2.4 Update the nine `float_div_*` rendering tests and retarget the divergence guard `float_div_casts_to_double_only_in_the_datafusion_dialect` to assert the new dialect pair.
- [x] 2.5 Add the checked-division function name to the sweep test's banned-token list, and confirm the sweep's Exasol expectation for `FLOAT_DIV` stays `("A" / "B")`.
- [x] 2.6 Remove `cast_to_double`, now unreachable. Keep `DOUBLE_TYPE`, which `render_cast_target` still uses.
- [x] 2.7 Update the pushdown-text assertion in `e2e_float_div_pushes_double_cast_projection` to the new rendering, and rename it to match.

## Phase 2: Implementation (Group C: Checked-division evaluation and end-to-end proof)
- [x] 3.1 Add failing unit tests for the scalar function over one batch: type pairings, NULL handling, zero divisor, `0/0`, `-0.0` divisor, overflow to `+Inf`, stored `NaN` operand.
- [x] 3.2 Implement the `ScalarUDFImpl` in a new `crates/lakehouse-engine/src/scan/checked_div.rs`. [expert]
- [x] 3.3 Register the function in `build_session_context` (`scan/object_store.rs`), reading the name from the `vs-expression` constant. Add a unit test asserting resolution.
- [x] 3.4 Add the classification arm to `classify_scan_error` (`scan/emit.rs`) so the error surfaces without the storage-failure prefix, recognised by type. Add a redaction unit test.
- [x] 3.5 Establish which route a two-literal division takes out of the scan. Add a test pinning the observed route.
- [x] 4.1 Rewrite `e2e_float_div_by_zero_projected_fails_with_inf_out_of_range` to assert a division-by-zero message, and record the SQL state observed live.
- [x] 4.2 Rewrite `e2e_zero_div_zero_projected_returns_silent_null` to assert the query now fails with the same division-by-zero message.
- [x] 4.3 Add an E2E test for the single-table predicate position: `> 0`/`< 0`, `x/0`/`0/0`, and assert the pushed filter via `EXPLAIN VIRTUAL`.
- [x] 4.4 Add an E2E test asserting a NULL divisor in a predicate still returns no rows and does not fail.
- [x] 4.5 Add an E2E test for the GUARDED shape task 1.2 measured, `e2e_float_div_guarded_by_a_non_zero_conjunct_matches_the_measured_outcome`, using divisor `(L_LINENUMBER - 1)`.
- [x] 4.6 Add an E2E test for the broadcast-join fact-leg filter: assert broadcast is retained and that the query fails.
- [x] 4.7 Add an E2E test for a division by zero inside a pushed aggregate argument.
- [x] 4.8 Confirm the two existing correct-division E2E tests still pass unchanged.
- [x] 4.9 Confirm both `dispatch_golden` fixtures, `datafusion-scan/scan-execution-plan-shape` tests, and `tests/scan_parquet_pruning.rs` pass unedited, with no golden diff.
- [x] 4.10 File the three tracked-exception issues from decision-log entries [7], [8], [12], then replace every `(#TODO-...)` token in `plan.md`, `decision-log.md`, and both spec deltas with the filed issue number. FAIL if `grep -rn 'TODO-suppression\|TODO-stored-nan\|TODO-scalar-fns' specs/_plans/fix-float-div-predicate-divzero/ --include='spec.md'` returns any line.
- [x] 4.11 Run the verification checklist.

## Phase 3: Verification
- [x] 5.1 Run `make cross-udf-build` (exit 0)
- [x] 5.2 Run `cargo test` (0 failures)
- [x] 5.3 Run `make test-e2e` (0 failures, fails not skips if Docker down)
- [x] 5.4 Run `cargo clippy --all-targets` (0 errors/warnings)
- [x] 5.5 Run `cargo fmt` (no changes)
- [x] 5.6 Scenario coverage audit against plan's Scenario Coverage table

## Phase 4: Review Fixes
- [x] 4.12 Rewrite the last sentence of the `CheckedFloatDivError` doc comment in `crates/lakehouse-engine/src/scan/checked_div.rs` to state that `ZeroDivisor` is raised whenever the divisor is zero, including a non-finite numerator read from the source table, and that `NonFiniteResult` covers a non-finite result under a non-zero divisor only.
- [x] 4.13 Add `checked_float_div_refuses_an_argument_count_other_than_two` to `crates/lakehouse-engine/src/scan/checked_div_tests.rs`, calling `invoke_with_args` directly with one and with three arguments and asserting each error names `takes exactly two arguments`.
- [x] 4.14 Add `checked_float_div_reports_a_stored_non_finite_numerator_over_a_zero_divisor_as_a_zero_divisor` to `crates/lakehouse-engine/src/scan/checked_div_tests.rs`, asserting `NaN / 0.0` names a division by zero and not a numeric value out of range.
- [x] 4.15 Add `a_second_session_records_no_failure_from_the_first` to `crates/lakehouse-engine/src/scan/checked_div_tests.rs`, asserting the recorded failure is scoped to the session that raised it.
- [x] 4.16 Change the byte-scan loop condition in `pushed_scan_filter` in `crates/lakehouse-engine/tests/e2e_scan_test.rs` to guard `end == 0`, so an empty filter value returns an empty string instead of panicking with a subtract overflow.
- [x] 4.17 Add a sentence to the `GUARD FIRST` bullet of the doc comment on `e2e_float_div_guarded_by_a_non_zero_conjunct_matches_the_measured_outcome` in `crates/lakehouse-engine/tests/e2e_scan_test.rs` naming `datafusion.execution.parquet.reorder_filters` as the config default the textual conjunct order depends on.
- [x] 4.18 Rename `renders_arithmetic_div` in `crates/vs-expression/src/lib_tests.rs` to `float_div_calls_checked_division_for_column_left_operand_and_literal_right_operand`.
- [x] 4.19 In `specs/_plans/fix-float-div-predicate-divzero/sql-comprehension/vs-expression-translator-float-div/spec.md`, replace the `LHVS.GT_LINEITEM_SCAN` oracle claim in the guarded scenario's GIVEN clause with the inline-literal-subquery oracle `native_lineitem_oracle` builds, and change the fixture range `(1..10)` to `(1 to 10)`.
- [x] 4.20 In `specs/_plans/fix-float-div-predicate-divzero/sql-comprehension/vs-expression-translator-float-div/spec.md`, qualify the row-filter mechanism clause with the `datafusion.execution.parquet.reorder_filters` default that produces the textual conjunct order.
- [x] 4.21 In `specs/_plans/fix-float-div-predicate-divzero/datafusion-scan/scan-execution-expression-pushdown/spec.md`, narrow the out-of-range clause to a non-zero divisor and add that a zero divisor names a division by zero whatever the numerator is.
- [x] 4.22 In `specs/_plans/fix-float-div-predicate-divzero/decision-log.md`, restate the tracked-exception passages of entries [7], [8], [12], and [5] as filed issues rather than placeholder tokens and a pending filing obligation.
- [x] 4.23 Add entry [13] to `specs/_plans/fix-float-div-predicate-divzero/decision-log.md` recording the session-recorded-failure mechanism, its rejected alternatives, and `Promotes to ADR: yes`.
- [x] 4.24 In `specs/_plans/fix-float-div-predicate-divzero/plan.md`, correct the § Requirements Concurrency row, add the `reframe_checked_division` leg to the § Architecture diagram, add the § Patterns row for the recorded first failure, and restate the tracked-exception preamble as fact.
- [x] 4.25 In `specs/_plans/fix-float-div-predicate-divzero/plan.md` § Verification § Scenario Coverage, replace the four test names that do not exist with the real ones and add rows for every shipped test the table omits.
- [x] 4.26 Change `reframe_checked_division` in `crates/lakehouse-engine/src/scan/emit.rs` so a recorded division composes with the incoming error instead of replacing it, appending the incoming message through the same redaction pipeline, and add the two collision tests to `crates/lakehouse-engine/src/scan/emit_tests.rs`. [expert]
- [x] 4.27 Fix a regression 4.26 introduced: `e2e_float_div_by_zero_in_filter_fails_like_native_exasol` and `e2e_float_div_guarded_by_a_non_zero_conjunct_matches_the_measured_outcome` now fail live because unconditional composition appends the same division failure a second time, under its flattened storage-read framing, for the predicate-position route (the plan's primary case). Narrow composition to the one case that is structurally distinguishable from the flattened-division route without message matching (`ResourcesExhausted`, detected before `classify_scan_error` discards the typed root), and replace (not compose) for every other incoming classification. Update `emit_tests.rs` accordingly and get `test-e2e` green again. [expert]

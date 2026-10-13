# Plan: fix-trunc-round-exact-decimal-date

## Summary

The VS computes pushed `TRUNC`, `ROUND`, and `DATE_TRUNC` with Exasol's semantics in three scan-session functions, so DECIMAL, integer, DOUBLE, DATE, and TIMESTAMP arguments return native Exasol's values or fail where native Exasol fails (#431, #201). One compact differential E2E suite compares the VS with a native Exasol copy of one edge-value fixture in every pushdown position.

## Context

- The DataFusion dialect renders `TRUNC`, `ROUND`, and `DATE_TRUNC` as DataFusion's `trunc`, `round`, and `date_trunc`, and those functions differ from Exasol (#431, #201).
- `trunc` converts a DECIMAL to DOUBLE. On `DBXA.SUPPLIER`, `TRUNC(S_ACCTBAL, 1)` returned a wrong value for 5,685 of 50,000 rows (11.4%).
- `round` on `Decimal128(18,2)` declares `Decimal128(19,2)` and returns `Decimal128(18,2)`, so a `ROUND` result announces a wrong type and fails comparisons (#431).
- `TRUNC` and `ROUND` with an Exasol format on a DATE or TIMESTAMP fail at plan time. DataFusion 54.1 `date_trunc` has no `decade`, `century`, or `millennium` unit, spells sub-second units in the singular, and converts a DATE to a nanosecond TIMESTAMP whose range excludes `0001-01-01` and `9999-12-31` (#201).
- The adapter advertises `FN_TRUNC`, `FN_ROUND`, and `FN_DATE_TRUNC`, and Exasol never re-checks an advertised capability, so each divergence is a wrong result for the user.
- The translator does not know Arrow column types, so no rendering in DataFusion SQL text alone can pick exact decimal arithmetic for a DECIMAL column and a format interpretation for a DATE column.
- The checked float division (#370) is the precedent: a function the scan session registers, named by a `crates/vs-expression` constant, with a typed error and a per-session failure record.
- The exact decimal kernel must not be slower than DataFusion's float `trunc`. Measured on 8M values: `SUM(TRUNC(x,1))` took 57.5 ms with float `trunc`, 16.2 ms with an `i64` kernel, and 43.4 ms with an `i128` kernel. `WHERE TRUNC(x,1) <> x` took 143.4 ms, 22.2 ms, and 50.4 ms.
- Native behavior was measured on Exasol 2026.2.0. The local Docker suite pins `exasol/docker-db:2025.1.16`, and CI also runs `exasol/docker-db:8.29.13`.
- The plan changes no architecture: the functions sit inside the `datafusion-scan` session builder, which already registers session functions, and `datafusion-scan` already depends on `crates/vs-expression`. The plan has no architecture delta.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| VS Expression Translator — TRUNC, ROUND, and DATE_TRUNC | NEW | `sql-functions/vs-expression-translator-trunc-round/spec.md` |
| VS Expression Translator — Scalar Functions | CHANGED | `sql-functions/vs-expression-translator-scalar-fns/spec.md` |
| VS Expression Translator — Date and Time Functions | CHANGED | `sql-functions/vs-expression-translator-date-fns/spec.md` |
| DataFusion Scan Execution — Expression Pushdown | CHANGED | `datafusion-scan/scan-execution-expression-pushdown/spec.md` |

## Impact

- Pushed `TRUNC` and `ROUND` on DECIMAL and integer columns return exact values. Queries that returned wrong values or failed comparisons now return native Exasol's values.
- `TRUNC`, `ROUND`, and `DATE_TRUNC` on DATE and TIMESTAMP columns push down for every format and unit in the vocabulary. They failed at plan time or returned wrong values before.
- `ROUND` past 36 digits, a non-finite DOUBLE step, and a date range overflow now fail as native Exasol fails. The error arrives with SQL state `22002` instead of native Exasol's `22003`, `22008`, `22769`, or `22770`. `docs/semantic-differences.md` records this.
- `DOUBLE` `ROUND` near a half now returns native Exasol's value, including `ROUND(0.49999999999999994E0) = 1`.
- The `D`, `DAY`, and `DY` formats, `DATE_TRUNC('week', ...)`, and any non-literal format or unit no longer reach the scan. Native Exasol evaluates them through the existing wrapper paths, which costs the pushdown of that expression.
- A `TRUNC` or `ROUND` over a string argument still fails at plan time, as before (#TBD).
- Downstream: the sibling project that consumes `crates/vs-expression` must register `exa_trunc`, `exa_round`, and `exa_date_trunc`, or stop advertising `FN_TRUNC`, `FN_ROUND`, and `FN_DATE_TRUNC`. A missing registration fails loudly at first use.
- No capability is added or withdrawn.

## Requirements

| Requirement | Details |
|-------------|---------|
| Kernel speed | `exa_trunc` over `Decimal128(15,2)` and `Decimal128(30,2)` is not slower than DataFusion's `trunc` over the same values as `Float64`, measured by the ignored host micro-benchmark that task 2.8 adds. |
| E2E runtime | The new E2E binary runs about 185 statements, about 72 of them scan queries through the VS, and finishes in under 2 minutes on the local Docker stack. Decision-log entry [10] derives the count from its case matrix. |

## Dependencies

- None new. `chrono` and `datafusion` are already dependencies of `crates/lakehouse-engine`.

## Implementation Tasks

### A. Translator (`crates/vs-expression`)

- [ ] 1.1 Capture, on the local Docker Exasol with `scripts/capture-pushdown-payload.sh`, the pushdown JSON for each form the E2E case table uses: a literal integer scale, a literal decimal scale, a scale column of type INT and of type `DECIMAL(5,2)`, a NULL scale, one spelling per format group, `D`, `DAY`, `DY`, a format column, every `DATE_TRUNC` unit, `week`, and a unit column. Use the observed node shapes as the inputs of the task 1.5 tests. Record in decision-log entry [10] every form Exasol folds before pushdown, so task 4.2 builds the case table without it.
- [ ] 1.2 Re-run with `exapump` the native probes whose values the spec deltas and decision-log entries [5] to [8] state, on the pinned image `exasol/docker-db:2025.1.16` and on `exasol/docker-db:8.29.13`, the image of the `E2E (8.29.x)` CI job (start it with the `EXASOL_IMAGE` override). Include every value of the `ROUND` example table in the trunc-round spec delta, the format-table thresholds on a TIMESTAMP one millisecond before and at each threshold (for example `TIMESTAMP '2024-05-15 23:59:59.999'` to `MM`, and 3 days 11:59:59.999 and 3 days 12:00:00 after a `WW` and a `W` start), and the readback of `-0.0`, one subnormal, and `1E250` through a DOUBLE column. Stop and report to the user before group B starts if any value differs between the images or from the spec deltas.
- [ ] 1.3 In `crates/vs-expression/src/lib.rs`, add the public constants `EXA_TRUNC_FN`, `EXA_ROUND_FN`, and `EXA_DATE_TRUNC_FN`, each with a doc comment that states the function's contract. Add the public types `ExaDatetimeFormat` (the 12 format groups) and `DateTruncUnit` (the 12 accepted units). Each type parses an Exasol spelling case-insensitively, returns its canonical token, and parses a canonical token. `D`, `DAY`, `DY`, and `week` parse to no value.
- [ ] 1.4 Replace the `"ROUND" | "TRUNC" | "LOG"` arm of `render_expression_inner`: `LOG` keeps its rendering, and `ROUND` and `TRUNC` classify the second argument by node shape (decision-log entry [2]), then render the `exa_` call or decline. Replace the `DATE_TRUNC` arm the same way. The Exasol dialect stays on the `VerbatimCall` branch and is not edited.
- [ ] 1.5 In `crates/vs-expression/src/lib_tests.rs`, change the DataFusion-dialect expectations to the `exa_` forms: lines 1188 and 1195 of `renders_math_scalar_functions`, the `("ROUND", "round")` and `("TRUNC", "trunc")` rows of the `two_arg` table in `renders_math_family_verbatim_in_exasol_dialect` (lines 2613 and 2614), line 1882 of `renders_date_trunc`, and line 2907 of `renders_date_trunc_verbatim_in_exasol_dialect`. Keep every Exasol-dialect assertion, including `DATE_TRUNC('month', "TS")`, and the sweep fixtures at lines 3044 and 3087. Add tests for every spelling of every group in two letter cases, every decline shape of the scenarios, the column-scale `CAST` wrapper from task 1.1, and byte-identical Exasol rendering of each accepted and declined node.

### B. Exasol scan functions (`crates/lakehouse-engine/src/scan/exasol_fns/`)

Numeric functions:

- [ ] 2.1 Create `scan/exasol_fns/mod.rs` with the `exa_trunc` and `exa_round` `ScalarUDFImpl` types: a one-or-two-argument signature, `return_type` that dispatches on the argument types (decision-log entry [5]), a plan-time error that names the function and an unsupported type, and `invoke_with_args` that dispatches to the kernels. Add the typed `ExasolFnError`, a failure record shared by all functions of the module (one `Arc<OnceLock<ExasolFnError>>` that `register_exasol_fns` creates), `find_exasol_fn_error`, and `session_exasol_fn_failure`. Call `register_exasol_fns` in `build_session_context` after `register_checked_float_div_udf`. [expert]
- [ ] 2.2 Add `scan/exasol_fns/decimal.rs`: exact `TRUNC` and `ROUND` for `Decimal128(p,s)` with `p` at most 36 and for every integer type as `DECIMAL(P,0)`. The scale may be a scalar or an array of integer, float, or decimal type, rounded half away from zero when not integral. Use `i64` arithmetic when `p` is at most 18 and the result fits, `i128` otherwise, and compute the power of ten once per call for a scalar scale. Apply the `ROUND` overflow rule at the result scale (decision-log entry [6]). [expert]
- [ ] 2.3 Add `scan/exasol_fns/float.rs`: the DOUBLE steps of decision-log entry [7], a table of correctly rounded powers of ten from `1E0` to `1E308`, an error for every non-finite power, intermediate, or result, `Float32` widened to `Float64`, and a `Decimal128` wider than 36 digits converted to `Float64` first. [expert]
- [ ] 2.4 In `scan/emit.rs`, make `classify_scan_error` recognise `ExasolFnError` by type after the checked division and before `ResourcesExhausted`. Rename `reframe_checked_division` to `reframe_session_function_failure` and make it read the checked-division record first and the `exasol_fns` record second, with the replace and compose rules unchanged. Update the call site in `scan/mod.rs` and the five call sites in `scan/emit_tests.rs`, and keep their assertions unchanged.
- [ ] 2.5 Add `scan/exasol_fns/exasol_fns_tests.rs`: the declared type equals the returned array's type for every accepted argument type, a string, Boolean, and `Decimal256` argument fails at planning with the function name, the three functions are present in a session from `build_session_context`, a recorded failure replaces a flattened storage failure and composes after a memory exhaustion, a recorded checked division takes precedence, and a literal-only call surfaces its message through `ctx.sql`. SQL-level tests write typed constants such as `CAST('12.345' AS DECIMAL(5,3))`, because a bare numeric literal parses as float in DataFusion (#475).
- [ ] 2.6 Add `scan/exasol_fns/decimal_tests.rs`: compare both kernels with a digit-string oracle over precisions 1, 4, 9, 18, 19, 28, and 36, every scale from 0 to the precision, scales from -40 to 40, and non-integer scales. Assert that the `i64` and `i128` paths agree, and pin the overflow boundary at the result scale for literal and per-row scales.
- [ ] 2.7 Add `scan/exasol_fns/float_tests.rs`: the measured DOUBLE table of decision-log entry [7], the tolerance boundary at `5.0E-14` and `5.1E-14` below a half, every non-finite case, and the widening of `Float32` and of a 38-digit decimal.
- [ ] 2.8 Add an ignored micro-benchmark to `crates/lakehouse-engine/tests/micro_bench.rs` that runs `SELECT SUM(exa_trunc(x, 1))` and `WHERE exa_trunc(x, 1) <> x` over 8M `Decimal128(15,2)` and `Decimal128(30,2)` values, and the same queries with `trunc` over the values cast to `Float64`. It prints the median of five runs per query. The values are built as typed Arrow arrays, not as SQL literals (#475).

Date and time functions, which extend the module the numeric tasks create:

- [ ] 3.1 Add `scan/exasol_fns/calendar.rs`: conversion between a year-month-day label and a day number in Exasol's calendar (Julian before 1582-10-15, Gregorian from that day), the weekday, the ISO week and ISO year, the week of year (`WW`), and the week of month (`W`). Convert a result label back to a proleptic Gregorian `Date32`, and fail for a label that calendar lacks. [expert]
- [ ] 3.2 Add `scan/exasol_fns/datetime.rs`: `TRUNC` and `ROUND` for `Date32` and for `Timestamp` in every unit, with or without a zone, for every format group, with the `TRUNC` period starts and `ROUND` conditions of the trunc-round spec's format table, the range-edge errors, and the error for a time format on a `Date32`. Compute on the label, with floor division for values before 1970, and never through a nanosecond intermediate for a `Date32`. [expert]
- [ ] 3.3 Add the `exa_date_trunc` `ScalarUDFImpl` to `scan/exasol_fns/mod.rs` and register it in `register_exasol_fns`. Implement its kernel in `datetime.rs`: every accepted unit, a DATE returned unchanged for a unit below a day, and `decade`, `century`, and `millennium` clamped to `0001-01-01`.
- [ ] 3.4 Add `scan/exasol_fns/calendar_tests.rs`: every measured label of decision-log entry [8], and the ISO week and weekday of every day from 1583-01-01 to 2100-12-31 against `chrono`'s ISO week as an independent oracle.
- [ ] 3.5 Add `scan/exasol_fns/datetime_tests.rs`: for every format, sweep each day of the windows 0001-01-01 to 0001-03-31, 1582-09-01 to 1583-01-31, 1899-12-01 to 1901-01-31, 1999-12-01 to 2001-01-31, 2023-12-01 to 2025-01-31, and 9998-12-01 to 9999-12-31. Check each result against an oracle that applies the format table's period starts and `ROUND` conditions to `chrono` date fields, the `chrono` ISO week from 1583 on, and the measured labels before 1583. Add a timestamp sweep at one millisecond before and at each `ROUND` threshold of the table. Cover every `TimeUnit` (`datetime_every_time_unit_and_zone`), a zoned timestamp, a timestamp before 1970, a `Date32` range edge that a nanosecond intermediate could not hold (`datetime_date_never_uses_nanoseconds`), and `exa_date_trunc` for every unit.

### C. Differential E2E and user documentation (`crates/lakehouse-engine/tests/`, `docs/`)

- [ ] 4.1 Add `tests/common/trunc_round_fixture.rs`. It holds the fixture values as Rust constants, the Iceberg schema and batches of the fact table `trunc_round` and the 3-row dimension table `trunc_round_dim` (columns `DIM_ID`, `DIM_MONTH`, names that `trunc_round` does not use), `seed_trunc_round` (drop and recreate both tables through `seed::create_and_append`), and `oracle_statements()`, which create both native tables and insert the same values. The columns, value classes, and row pairing are listed in decision-log entry [10]. Each native column has the Exasol type the VS exposes for its Iceberg column (for example `DECIMAL(10,0)` for `I32`, `DOUBLE` holding the exact f32 expansion for `FLT`, `VARCHAR` holding the VS text for `D38_2`). Timestamp values are exact to the millisecond, and the native timestamp column uses the precision `expected_timestamp_precision` returns.
- [ ] 4.2 Add `tests/common/trunc_round_cases.rs` with the case tables and one runner. The runner runs each statement on the VS and on the native table and requires the same outcome: equal declared column types and equal rows, or a failure on both sides. A case that expects a failure names the Exasol phrase the VS message must contain. For every VS failure, the runner also asserts that the message names `TRUNC` or `ROUND` and does not contain `assigned data could not be read`, as `tests/e2e_scan_test.rs` asserts for the checked division. Cases that assert pushdown read `EXPLAIN VIRTUAL` and count `exa_` calls, and the join cases also assert `has_broadcast_join_block` (accepted form) or its absence (declined form). The case tables follow the case matrix of decision-log entry [10], including its `WHERE`-position error cases and its two join cases. The runner collects every mismatch and asserts once.
- [ ] 4.3 Add `tests/e2e_trunc_round_test.rs` with a `OnceLock` setup (own namespace, VS, and native oracle schema) and six tests: `e2e_trunc_round_fixture_reads_back_like_native`, `e2e_trunc_round_select_list_matches_native`, `e2e_trunc_round_positions_match_native`, `e2e_trunc_round_broadcast_join_matches_native`, `e2e_trunc_round_errors_match_native`, and `e2e_trunc_round_declined_forms_fall_back_to_native`. Each test carries the `/// Scenario:` lines the Scenario Coverage table assigns to it.
- [ ] 4.4 Add `--test e2e_trunc_round_test` to the `test-e2e` recipe in `Makefile`.
- [ ] 4.5 Add a `TRUNC and ROUND fixture` bullet to `specs/testing.md` § Fixtures that names the fixture module, its native oracle table, and its probe columns.

User documentation:

- [ ] 5.1 Add a `TRUNC, ROUND, and DATE_TRUNC` section to `docs/semantic-differences.md`: the SQL state `22002` for every failure, the February 29 labels of Julian-only leap years, the string-argument failure at plan time, and the declined forms that native Exasol evaluates.
- [ ] 5.2 In `docs/capabilities.md` § Scalar functions, add one sentence under the table that names the `TRUNC`, `ROUND`, and `DATE_TRUNC` forms that native Exasol evaluates instead of the scan, and link the new semantic-differences section.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Translator | 1.1-1.5 | — | spec deltas `sql-functions/vs-expression-translator-trunc-round` (the rendering and decline scenarios), `sql-functions/vs-expression-translator-scalar-fns`, `sql-functions/vs-expression-translator-date-fns`; `crates/vs-expression/src/lib.rs`, `crates/vs-expression/src/lib_tests.rs`, `scripts/capture-pushdown-payload.sh` |
| B: Exasol scan functions | 2.1-2.8, 3.1-3.5 | A (reads the constants and the format types from `crates/vs-expression`) | spec deltas `datafusion-scan/scan-execution-expression-pushdown`, `sql-functions/vs-expression-translator-trunc-round` (the value, overflow, calendar, and range-edge scenarios); `crates/lakehouse-engine/src/scan/exasol_fns/` and its `_tests.rs` files, `scan/emit.rs`, `scan/emit_tests.rs`, `scan/mod.rs`, `scan/object_store.rs`, `scan/checked_div.rs` (pattern), `tests/micro_bench.rs` |
| C: Differential E2E and user documentation | 4.1-4.5, 5.1-5.2 | A, B (the suite runs the whole stack) | every spec delta of this plan as end-to-end proof; `specs/testing.md`; `crates/lakehouse-engine/tests/{e2e_trunc_round_test.rs,common/trunc_round_fixture.rs,common/trunc_round_cases.rs}`, `tests/common/{seed.rs,e2e_harness.rs,exasol_ws.rs,timestamp_precision.rs}`, `tests/e2e_pruning_test.rs` and `tests/common/pruning_cases.rs` (pattern), `Makefile`, `docs/semantic-differences.md`, `docs/capabilities.md` |

- Group B holds both kernel families because they share `scan/exasol_fns/mod.rs`, the failure record, and the type dispatch.
- Group C is the integration proof of groups A and B. Its knowledge is the E2E harness, the fixture, and the native oracle, which no kernel task needs.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Match arm branch | `crates/vs-expression/src/lib.rs`, `ROUND` and `TRUNC` in the `"ROUND" \| "TRUNC" \| "LOG"` arm | Replaced by the `exa_trunc` and `exa_round` rendering; the arm keeps `LOG` only |
| Match arm body | `crates/vs-expression/src/lib.rs`, the `DATE_TRUNC` arm that renders `date_trunc(...)` | Replaced by the `exa_date_trunc` rendering |
| Test assertions | `crates/vs-expression/src/lib_tests.rs` lines 1188, 1195, 1882, 2907, and the `ROUND` and `TRUNC` rows of the `two_arg` table (lines 2613, 2614) | They assert the removed DataFusion renderings and change to the `exa_` forms |
| Function name | `crates/lakehouse-engine/src/scan/emit.rs` `reframe_checked_division` | Renamed to `reframe_session_function_failure`, which reads both failure records |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| TRUNC and ROUND render as calls to the scan's Exasol functions in the DataFusion dialect | Unit, E2E | `crates/vs-expression/src/lib_tests.rs`; `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs` | `renders_trunc_round_as_exa_calls`, `renders_trunc_round_format_spellings_as_canonical_tokens`, `renders_trunc_round_verbatim_in_exasol_dialect`; `e2e_trunc_round_select_list_matches_native` |
| A TRUNC, ROUND, or DATE_TRUNC form the scan cannot reproduce falls back to native Exasol on every pushdown path | Unit, E2E | `crates/vs-expression/src/lib_tests.rs`; `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs` | `declines_trunc_round_forms_the_scan_cannot_reproduce`, `declines_date_trunc_week_and_unknown_units`; `e2e_trunc_round_declined_forms_fall_back_to_native`, `e2e_trunc_round_broadcast_join_matches_native` |
| TRUNC and ROUND on DECIMAL and integer arguments compute exactly | E2E, Unit | `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs`; `crates/lakehouse-engine/src/scan/exasol_fns/decimal_tests.rs` | `e2e_trunc_round_select_list_matches_native`, `e2e_trunc_round_positions_match_native`, `e2e_trunc_round_broadcast_join_matches_native`; `decimal_kernels_match_digit_string_oracle`, `decimal_i64_and_i128_paths_agree` |
| ROUND on DECIMAL fails where native Exasol overflows | E2E, Unit | `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs`; `crates/lakehouse-engine/src/scan/exasol_fns/decimal_tests.rs` | `e2e_trunc_round_errors_match_native`; `decimal_round_overflow_is_judged_at_the_result_scale` |
| TRUNC and ROUND on DOUBLE reproduce native Exasol's floating-point steps | E2E, Unit | `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs`; `crates/lakehouse-engine/src/scan/exasol_fns/float_tests.rs` | `e2e_trunc_round_select_list_matches_native`, `e2e_trunc_round_errors_match_native`; `float_steps_match_measured_native_values`, `float_round_tolerance_boundary`, `float_non_finite_steps_fail` |
| TRUNC and ROUND on DATE and TIMESTAMP follow native Exasol's period rules for every supported format | E2E, Unit | `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs`; `crates/lakehouse-engine/src/scan/exasol_fns/datetime_tests.rs` | `e2e_trunc_round_select_list_matches_native`, `e2e_trunc_round_positions_match_native`, `e2e_trunc_round_broadcast_join_matches_native`; `datetime_formats_match_oracle_over_day_windows` |
| TRUNC and ROUND on DATE and TIMESTAMP keep the argument's type and reject a time format on a DATE | E2E, Unit | `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs`; `crates/lakehouse-engine/src/scan/exasol_fns/datetime_tests.rs` | `e2e_trunc_round_select_list_matches_native`, `e2e_trunc_round_errors_match_native`; `datetime_every_time_unit_and_zone`, `datetime_date_never_uses_nanoseconds` |
| DATE_TRUNC on DATE and TIMESTAMP matches native Exasol for every supported unit | E2E, Unit | `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs`; `crates/lakehouse-engine/src/scan/exasol_fns/datetime_tests.rs` | `e2e_trunc_round_select_list_matches_native`, `e2e_trunc_round_positions_match_native`; `date_trunc_every_unit` |
| Week and ISO-year formats follow Exasol's calendar before 1582-10-15 | E2E, Unit | `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs`; `crates/lakehouse-engine/src/scan/exasol_fns/calendar_tests.rs` | `e2e_trunc_round_fixture_reads_back_like_native`, `e2e_trunc_round_select_list_matches_native`; `calendar_matches_measured_exasol_labels`, `calendar_iso_week_matches_chrono_from_1583`, `calendar_julian_only_label_fails` |
| Date and timestamp range edges fail where native Exasol fails | E2E, Unit | `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs`; `crates/lakehouse-engine/src/scan/exasol_fns/datetime_tests.rs` | `e2e_trunc_round_errors_match_native`; `datetime_range_edges` |
| Math scalar functions translate to DataFusion math calls | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_math_scalar_functions` (its `ROUND` and `TRUNC` assertions change to the `exa_` forms), `renders_trunc_round_as_exa_calls` |
| DATE_TRUNC renders as a call to the scan's Exasol DATE_TRUNC function in the DataFusion dialect | Unit, E2E | `crates/vs-expression/src/lib_tests.rs`; `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs` | `renders_date_trunc`, `renders_date_trunc_verbatim_in_exasol_dialect`, `declines_date_trunc_week_and_unknown_units`; `e2e_trunc_round_select_list_matches_native`, `e2e_trunc_round_declined_forms_fall_back_to_native` |
| The scan session registers the Exasol TRUNC, ROUND, and DATE_TRUNC functions every pushed expression needs | Unit, E2E | `crates/lakehouse-engine/src/scan/exasol_fns/exasol_fns_tests.rs`; `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs` | `declared_type_equals_returned_type`, `unsupported_argument_type_fails_at_planning`, `session_registers_the_exasol_functions`; `e2e_trunc_round_positions_match_native`, `e2e_trunc_round_broadcast_join_matches_native` |
| A TRUNC or ROUND failure reaches the user as the function's own error | Unit, E2E | `crates/lakehouse-engine/src/scan/exasol_fns/exasol_fns_tests.rs`, `crates/lakehouse-engine/src/scan/emit_tests.rs`; `crates/lakehouse-engine/tests/e2e_trunc_round_test.rs` | `recorded_failure_replaces_flattened_storage_failure`, `recorded_failure_composes_after_memory_exhaustion`, `checked_division_takes_precedence`, `literal_only_call_surfaces_its_message`; `e2e_trunc_round_errors_match_native` |

### Manual Testing

Run `make test-e2e` once to provision the virtual schema, then point `exapump` at the local Docker Exasol with `export EXAPUMP_DSN="exasol://sys:exasol@localhost:28563?validateservercertificate=0"` (the image's default `SYS` password, as `docker-compose.yml` notes).

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| VS Expression Translator — TRUNC, ROUND, and DATE_TRUNC | `exapump sql "SELECT ID, TRUNC(D18_2, 1), ROUND(DBL), TRUNC(DT, 'Q') FROM TRUNC_ROUND_VS.TRUNC_ROUND ORDER BY ID"` and the same statement against `TRUNC_ROUND_NATIVE.TRUNC_ROUND` | Identical rows and column types |
| VS Expression Translator — TRUNC, ROUND, and DATE_TRUNC | `exapump sql "EXPLAIN VIRTUAL SELECT TRUNC(D18_2, 1) FROM TRUNC_ROUND_VS.TRUNC_ROUND"` | The pushdown SQL contains `exa_trunc("D18_2", 1)` |
| VS Expression Translator — Date and Time Functions | `exapump sql "EXPLAIN VIRTUAL SELECT DATE_TRUNC('week', TS) FROM TRUNC_ROUND_VS.TRUNC_ROUND"` | The pushdown SQL contains no `exa_` call, and the wrapper SQL contains `DATE_TRUNC('week', ...)` |
| VS Expression Translator — Scalar Functions | `exapump sql "EXPLAIN VIRTUAL SELECT ROUND(D18_2, 1) FROM TRUNC_ROUND_VS.TRUNC_ROUND"` | The pushdown SQL contains `exa_round("D18_2", 1)` |
| DataFusion Scan Execution — Expression Pushdown | `exapump sql "SELECT ROUND(P36_0_MAX, -1) FROM TRUNC_ROUND_VS.TRUNC_ROUND"` | Fails with `F-UDF-CL-RUST-9001`; the message names `ROUND`, contains `numeric value out of range`, and contains no `assigned data could not be read` |
| VS Expression Translator — TRUNC, ROUND, and DATE_TRUNC (staging) | the `SUPPLIER` comparison of #431 on `DBXA` and `DBXA_COPY` (staging, manual only) | `SELECT COUNT(*)` of rows where the VS and native `TRUNC(S_ACCTBAL, 1)` differ returns 0 |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test (host) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures, `e2e_trunc_round_test` included |
| Micro-benchmark | `cargo test -p lakehouse-engine --test micro_bench -- --ignored --nocapture` | `exa_trunc` medians at or below the float `trunc` medians for both precisions |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |

## Open Questions

Non-blocking. A human opens these issues, and planning does not.

- `(#TBD)` A `TRUNC` or `ROUND` over a string argument fails at plan time, where native Exasol converts the string. The adapter could decline a bare VARCHAR column argument the way it declines a non-string `LIKE` subject.
- `(#TBD)` Add a `TRUNC` query to the benchmark suite. Its requirement belongs in `bench/requirements.md`.

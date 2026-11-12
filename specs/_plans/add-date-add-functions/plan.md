# Plan: add-date-add-functions

## Summary

The scan computes `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES` over a DATE or `TIMESTAMP(0)` to `TIMESTAMP(6)` argument, and the adapter advertises the four capabilities (#615, item 4). The typed scan pass tells a DATE from a TIMESTAMP, and every form the scan cannot match runs in Exasol. A date before 1582-10-15 is the one documented difference: the scan computes it in the proleptic Gregorian calendar.

## Context

- Issue #615 item 4 asks for the four functions. The adapter does not advertise them today, so Exasol computes them over the scanned rows and a WHERE filter that uses one keeps no scan filter.
- An earlier attempt withdrew `ADD_HOURS` and `ADD_MINUTES` because the scan declared its output type from DataFusion's result type, and Exasol rejected `TIMESTAMP(3)` where it expected `TIMESTAMP(0)`. The adapter now declares every computed select item with Exasol's `selectListDataTypes` type and casts the value at the emit boundary, so that mismatch no longer occurs.
- The typed scan pass (`crates/vs-expression/src/scan_typing.rs`) folds DATE, TIMESTAMP, and `TIMESTAMP WITH LOCAL TIME ZONE` into `TypeClass::Other`, so it cannot accept the functions over a DATE or TIMESTAMP and decline them over anything else.
- The local Exasol 2026.2.0 returns the result types that the new feature's Background lists, and keeps all nine digits of a `TIMESTAMP(9)` argument.
- Exasol rounds an exact count half away from zero and a DOUBLE count as `sign(x) * floor(|x| + 0.5)`: the DOUBLE 0.49999999999999994 adds 1, and the DECIMAL 0.499…9 with 30 digits adds 0. A BOOLEAN count adds 1 or 0.
- Exasol fails an `ADD_DAYS` or `ADD_WEEKS` count beyond ±999,999,999 with `numeric value out of range`, and a result outside 0001-01-01 to 9999-12-31 with `datetime field overflow` or `datetime field underflow`.
- DataFusion 55.2 rejects `INTERVAL * integer`, has no function that builds an interval from a number, drops the sub-day part of `Date32 + interval`, and returns a nanosecond timestamp from `date_trunc` over a `Date32`.
- No Iceberg table spec or Delta protocol section governs this change: it computes expressions over values the scan already reads and changes no read path, schema, or type mapping.
- Architecture: no change. The datafusion-scan component already owns the scan session and the functions it registers.
- Exasol uses the Julian calendar before 1582-10-15, and Arrow dates use the proleptic Gregorian calendar. On the local Exasol 2025.1.16, rendered with `TO_CHAR`, `ADD_DAYS(DATE '1500-03-01', -1)` returns 1500-02-29, and `ADD_DAYS(DATE '1582-10-15', -1)` returns 1582-10-04, so an argument after the switch whose result crosses it also differs. The scan computes proleptic Gregorian results, and `docs/semantic-differences.md` documents the difference. Date literals in pushed filters already meet this difference.
- Open question: whether the scan should reproduce Exasol's dates before 1582-10-15 needs a follow-up issue (#TBD).

## Features

| Feature | Status | Spec |
|---------|--------|------|
| VS Expression Translator, Date-Addition Functions | NEW | `sql-functions/vs-expression-translator-date-add-fns/spec.md` |
| VS Expression Translator, Date-Difference Functions | CHANGED | `sql-functions/vs-expression-translator-date-diff-fns/spec.md` |
| Pushdown Planning, Capability Extensions | CHANGED | `pushdown-capabilities/pushdown-planning-capability-extensions/spec.md` |

## Impact

- A query that uses one of the four functions over a DATE or `TIMESTAMP(0)` to `TIMESTAMP(6)` column, literal, or CAST computes it in the scan. A WHERE filter with such a call keeps its scan filter, and a select item no longer widens the projection to the full base row. Results equal native Exasol, except for an argument or a result before 1582-10-15, which `docs/semantic-differences.md` documents.
- Other argument forms run in Exasol SQL as before. `docs/semantic-differences.md` lists them.
- No breaking change. No property, CONNECTION field, or install step changes.

## Implementation Tasks

Every test that implements a scenario carries one `/// Scenario: <title>` line for each scenario the Scenario Coverage table maps to it.

### 1. Typed pass and rendering (`crates/vs-expression`)

- [ ] 1.1 Add `TypeClass::Date` and `TypeClass::Timestamp { precision: u8 }`. `TypeClass::of` maps `ExasolType::Date` and `ExasolType::Timestamp`, and keeps `TimestampWithLocalTimeZone` as `Other`. `node_class` gives `literal_date` the DATE class and `literal_timestamp` a TIMESTAMP class whose precision is the count of the value's fractional-second digits. `to_text` and `cast_to_text` decline both classes as they decline `Other`. `union` keeps its `_ => Other` arm. Update the `TypeClass` doc comments.
- [ ] 1.2 Add a `ResultRule` variant for date addition that records whether a DATE argument keeps the DATE class (`ADD_DAYS`, `ADD_WEEKS`) or becomes `Timestamp { precision: 0 }` (`ADD_HOURS`, `ADD_MINUTES`). Declare the four names as `VerbatimCall` entries in `SCALAR_FUNCTIONS`.
- [ ] 1.3 In `scalar_function_class`, route the new rule to one gate function. It requires two arguments. It accepts a DATE first argument and a TIMESTAMP first argument of precision 0 to 6, declines a TIMESTAMP of precision above 6 with a reason that names the nanosecond range, declines text through `refuse_implicit_text`, and declines every other class. It accepts an integer, number, or NULL count, wraps a BOOLEAN count in `BOOLEAN_NUMBER_NODE`, and declines any other count. It wraps the accepted first argument of the typed tree in a `function_scalar_cast` node to `ExasolType::Date` or `ExasolType::timestamp(6)`, which picks the rendering by class. It returns the result class that the scenario "The typed pass tells a DATE from a TIMESTAMP" states.
- [ ] 1.4 Export the four scan function names (`vs_add_days`, `vs_add_weeks`, `vs_add_hours`, `vs_add_minutes`) as public constants next to `CHECKED_FLOAT_DIV_FN`, with a doc comment that states the contract: the first argument is `Date32` or `Timestamp(Microsecond, None)`, the second is a number. Add the DataFusion-dialect per-name arm that renders `<function>(<arg_sql>, <count_sql>)` after an arity check. Delete the comment at `lib.rs` lines 888-889.
- [ ] 1.5 Unit tests in `scan_typing_tests.rs`: the class each source derives, including `MIN`, `MAX`, a CASE whose other branches are NULL, and a CASE over two date values, text conversion of both classes declines, a BOOLEAN count is wrapped, and every declined form of the scenario "A form the scan cannot match runs in Exasol" returns `Err`.
- [ ] 1.6 Unit tests in `lib_tests.rs`: the `render_scan_expression` rendering of each function over a DATE column, a TIMESTAMP column, a DATE literal, a TIMESTAMP literal, a CAST, and a nested call, each with its class-picked CAST; the Exasol-dialect verbatim rendering. Remove the four names from `unsupported_date_functions_decline_in_the_datafusion_dialect` and add its `/// Scenario:` line.

### 2. Scan functions and the scan rewrite (`crates/lakehouse-engine`)

- [ ] 2.1 Add `scan/datetime_add.rs`: one `ScalarUDFImpl` type parameterized by its unit, registered under the four names in the scan-session constructor next to `vs_checked_float_div`. It returns `Date32` for a `Date32` argument with days or weeks, and `Timestamp(Microsecond, None)` for a `Date32` argument with hours or minutes and for a `Timestamp(Microsecond, None)` argument. It fails planning for any other argument type, which the typed pass's CAST rules out. It accepts integer, unsigned integer, `Decimal128`, `Float32`, `Float64`, and `Null` counts, fails a NaN or infinite DOUBLE count before rounding with a message built with `numeric_overflow`, as `vs_checked_float_div` fails a non-finite operand, rounds the other counts as the rounding scenario states, checks the `ADD_DAYS` and `ADD_WEEKS` count limit, uses checked arithmetic, propagates NULL, and fails a result outside 0001-01-01 to 9999-12-31 23:59:59.999999. It records each failure in the session's `RaisedFailure`, and the message text before any `;` or `(` is exactly `data exception - datetime field overflow`, `data exception - datetime field underflow`, or `data exception - numeric value out of range: …` built with `numeric_overflow`. [expert]
- [ ] 2.2 Unit tests in `scan/datetime_add_tests.rs`, declared from `datetime_add.rs` with `#[cfg(test)] #[path = "datetime_add_tests.rs"] mod tests;`: DOUBLE ties ±0.5 to ±2.5 and 0.49999999999999994; exact DECIMAL rounding at scale 2 and scale 30; the count limit (DOUBLE 999999999.4 fails, DECIMAL 999999999.4999999999999 passes the limit and then fails the range); both range ends for `Date32` and for microsecond timestamps; NULL inputs; fractional seconds kept; NaN, +Infinity, and -Infinity counts fail (`non_finite_counts_fail`); proleptic Gregorian arithmetic before 1582-10-15, for example 1500-03-01 minus 1 day returns 1500-02-28 (`dates_before_the_gregorian_switch_use_the_proleptic_calendar`).
- [ ] 2.3 In `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, extend `timestamp_literal_floor` to accept a `function_scalar` node named `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES` as the non-literal operand, with a digit count of 6, and rename its `column` parameter to `operand`. `rewrite_timestamp_comparison` then rewrites the comparison, `BETWEEN`, and `IN` forms for these calls as it does for a column. Update the doc comments of both functions. Add `overprecise_timestamp_literal_against_a_date_addition_is_floored` to `support_tests.rs`: `=`, `>=`, `BETWEEN`, and `IN` over an `ADD_HOURS` call with a 7-digit literal on either side become the rewritten comparisons, and a literal of 6 or fewer digits leaves the tree unchanged.

### 3. Capabilities, documentation, and live verification

- [ ] 3.1 Add `FN_ADD_DAYS`, `FN_ADD_HOURS`, `FN_ADD_MINUTES`, and `FN_ADD_WEEKS` to `CAPABILITIES` next to `FN_DAYS_BETWEEN`, and delete the comment at `capabilities.rs` lines 106-107. In `capabilities_tests.rs` `reports_audited_capability_set`, move the four names to the advertised set, keep `FN_ADD_SECONDS`, `FN_ADD_MONTHS`, and `FN_ADD_YEARS` absent, and add the `/// Scenario:` lines of the two changed scenarios.
- [ ] 3.2 Update `docs/capabilities.md`: add the four capabilities to the Date / time row, and add one sentence after the table that names the argument forms the scan computes and states that the other forms run in Exasol SQL. Update the four rows of `docs/capability-matrix.md` to advertised, with a note that names the argument forms the scan computes.
- [ ] 3.3 Add a section to `docs/semantic-differences.md` for the four functions. It lists the argument forms that run in Exasol (`TIMESTAMP(7)` to `TIMESTAMP(9)`, `TIMESTAMP WITH LOCAL TIME ZONE`, text, a CASE over two date values or another date function as argument, a text count), and states that their results match native Exasol. Add a second section for dates before 1582-10-15: the scan adds the count in the proleptic Gregorian calendar, native Exasol uses the Julian calendar, so a call whose argument or result lies before 1582-10-15 returns a different value. List these examples as argument, count, native result (measured on Exasol 2026.2.0), and scan result: `ADD_DAYS` 1000-01-01, 1, 1000-01-03, 1000-01-02; `ADD_DAYS` 1500-03-01, -1, 1500-03-02, 1500-02-28; `ADD_DAYS` 1582-10-04, 0, 1582-10-05, 1582-10-04; `ADD_DAYS` 1582-10-16, -1, 1582-10-16, 1582-10-15; `ADD_HOURS` 1000-01-01, 24, 1000-01-03 00:00:00, 1000-01-02 00:00:00.
- [ ] 3.4 Add the E2E module `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` and register it in `main.rs`. Its test `date_add_functions_match_native` follows the case table of `text_conversion.rs` over `typed_table()` and a native copy of `TYPED_COLUMNS`. Rows cases: each function over `C_DATE`, `C_TS`, a DATE literal, a TIMESTAMP literal, a CAST, and a nested call, with counts `ID`, `C_DOUBLE`, `-C_DOUBLE`, `C_DECIMAL_A`, `C_BOOL`, and NULL, in a select list, a WHERE filter, a GROUP BY key, and a `MAX` argument, including a leap day, a month end, and a year end, each with `Has("vs_add_")`. Rows cases over a CASE and an aggregate: `ADD_DAYS(CASE WHEN ID > 2 THEN C_DATE END, 1)` and `ADD_DAYS(CASE WHEN ID > 2 THEN C_TS END, 1)` with `Has("vs_add_")`, and `ADD_DAYS(MAX(C_DATE), 1)`, `ADD_DAYS(MAX(C_TS), 1)`, `ADD_DAYS(MIN(C_DATE), 1)`, and `ADD_DAYS(MIN(C_TS), 1)` with `Unchecked`, because a scalar over a split aggregate renders in Exasol's merge wrapper. Rows cases of a WHERE filter against a literal 100 nanoseconds after the result for `C_DATE` 2024-01-01, each with `Has("vs_add_")`: `ADD_HOURS(C_DATE, 1) = TIMESTAMP '2024-01-01 01:00:00.0000001'`, the same with the literal on the left, `ADD_HOURS(C_DATE, 1) >= TIMESTAMP '2024-01-01 01:00:00.0000001'`, `ADD_HOURS(C_DATE, 1) BETWEEN TIMESTAMP '2024-01-01 01:00:00.0000001' AND TIMESTAMP '2024-01-02 01:00:00'`, and `ADD_MINUTES(C_DATE, 60) IN (TIMESTAMP '2024-01-01 01:00:00.0000001')`. Declined cases (a CASE over two dates, `DATE_TRUNC`) with `Lacks("vs_add_")`. Error cases with `Expect::Error`: `ADD_DAYS(C_DATE, 3000000)` and `ADD_WEEKS(C_DATE, 500000)` (overflow) and `ADD_DAYS(C_DATE, -800000)` (underflow) in a select list, a WHERE filter, and a `MAX` argument. Add the test `date_add_before_the_gregorian_switch_follows_the_proleptic_calendar`: over the virtual schema, `SELECT ADD_DAYS(C_DATE, -200000) ... WHERE C_DATE = DATE '2024-01-01'` returns 1476-06-02, the proleptic Gregorian date the test computes with `chrono`, and its pushdown names `vs_add_days`. Delete the comment at `capability.rs` lines 1354-1355.
- [ ] 3.5 Add `date_addition_over_a_nanosecond_timestamp_runs_in_exasol` to `timestamp_precision.rs`: on an engine that declares the nanosecond column `TIMESTAMP(9)` (2025.x and later), the four functions over it match a native copy and take `Lacks("vs_add_")`.
- [ ] 3.6 Build with `make cross-udf-build` and run `make e2e-up test-e2e`. For any form that differs from native Exasol, decline it in the gate of task 1.3 and list it in `docs/semantic-differences.md`. If a function's DATE or TIMESTAMP column form differs, remove its capability and its spec lines, as the interview decided. The scan results of task 3.3's examples before 1582-10-15 are computed, not measured: if `date_add_before_the_gregorian_switch_follows_the_proleptic_calendar` shows that Exasol displays an emitted date before 1582-10-15 differently, correct the scenario "A date before 1582-10-15 follows the proleptic Gregorian calendar in the scan" and the documented scan results to the measured values.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Typed pass and rendering | 1.1-1.6 | none | spec deltas `sql-functions/vs-expression-translator-date-add-fns`, `sql-functions/vs-expression-translator-date-diff-fns`; `crates/vs-expression/src/scan_typing.rs`, `scalar_functions.rs`, `lib.rs`, `scan_typing_tests.rs`, `lib_tests.rs` |
| B: Scan functions and the scan rewrite | 2.1-2.3 | A (function-name constants in `crates/vs-expression/src/lib.rs`) | spec delta `sql-functions/vs-expression-translator-date-add-fns` (rounding, range, finer-literal, and calendar scenarios); spec `pushdown-types/pushdown-planning-timestamp-literal-precision`; `crates/lakehouse-engine/src/scan/datetime_add.rs`, `scan/mod.rs`, `scan/checked_div.rs` (pattern), `scan/raised_failure.rs`, `scan/exasol_numeric.rs`, `adapter/pushdown/support.rs`, `adapter/pushdown/support_tests.rs` |
| C: Capabilities, docs, live verification | 3.1-3.6 | A, B | spec deltas `pushdown-capabilities/pushdown-planning-capability-extensions`, `sql-functions/vs-expression-translator-date-add-fns`; `crates/lakehouse-engine/src/adapter/capabilities.rs`, `capabilities_tests.rs`, `docs/capabilities.md`, `docs/capability-matrix.md`, `docs/semantic-differences.md`, `crates/lakehouse-e2e/tests/e2e_local/date_add.rs`, `timestamp_precision.rs`, `text_conversion.rs` (pattern), `crates/lakehouse-e2e/tests/common/e2e_harness.rs` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Comment | `crates/vs-expression/src/lib.rs` lines 888-889 | States that `ADD_HOURS` and `ADD_MINUTES` are not translated |
| Comment | `crates/lakehouse-engine/src/adapter/capabilities.rs` lines 106-107 | States that `ADD_HOURS` and `ADD_MINUTES` are not advertised |
| Comment | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` lines 1354-1355 | States that only the `*_BETWEEN` functions are advertised |
| Test data | `crates/vs-expression/src/lib_tests.rs` `unsupported_date_functions_decline_in_the_datafusion_dialect` | The four names now translate |
| Test data | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` `reports_audited_capability_set` absent list | The four names are now advertised |
| Doc rows | `docs/capability-matrix.md` `ADD_DAYS`, `ADD_HOURS`, `ADD_MINUTES`, `ADD_WEEKS` notes | The notes give the withdrawn reasons |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The typed pass tells a DATE from a TIMESTAMP | Unit | `crates/vs-expression/src/scan_typing_tests.rs` | `date_and_timestamp_classes_follow_their_source` |
| The typed pass tells a DATE from a TIMESTAMP | Integration | `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` | `date_add_functions_match_native` (CASE, `MIN`, and `MAX` rows) |
| ADD_DAYS and ADD_WEEKS over a DATE return a DATE from the scan | Unit | `crates/vs-expression/src/lib_tests.rs` | `date_addition_renders_scan_function_calls` |
| ADD_DAYS and ADD_WEEKS over a DATE return a DATE from the scan | Integration | `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` | `date_add_functions_match_native` |
| ADD_HOURS and ADD_MINUTES over a DATE return a timestamp from the scan | Unit | `crates/vs-expression/src/lib_tests.rs` | `date_addition_renders_scan_function_calls` |
| ADD_HOURS and ADD_MINUTES over a DATE return a timestamp from the scan | Integration | `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` | `date_add_functions_match_native` |
| The four functions over a TIMESTAMP keep its fractional seconds in the scan | Integration | `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` | `date_add_functions_match_native` |
| The scan rounds a fractional count as Exasol does | Unit | `crates/lakehouse-engine/src/scan/datetime_add_tests.rs` | `counts_round_as_exasol_rounds`, `non_finite_counts_fail` |
| The scan rounds a fractional count as Exasol does | Integration | `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` | `date_add_functions_match_native` |
| A result outside Exasol's datetime range fails the query | Unit | `crates/lakehouse-engine/src/scan/datetime_add_tests.rs` | `results_outside_the_range_fail`, `day_and_week_counts_beyond_the_limit_fail` |
| A result outside Exasol's datetime range fails the query | Integration | `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` | `date_add_functions_match_native` |
| A comparison with a TIMESTAMP literal finer than a microsecond returns native rows | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `overprecise_timestamp_literal_against_a_date_addition_is_floored` |
| A comparison with a TIMESTAMP literal finer than a microsecond returns native rows | Integration | `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` | `date_add_functions_match_native` (finer-literal rows) |
| A date before 1582-10-15 follows the proleptic Gregorian calendar in the scan | Unit | `crates/lakehouse-engine/src/scan/datetime_add_tests.rs` | `dates_before_the_gregorian_switch_use_the_proleptic_calendar` |
| A date before 1582-10-15 follows the proleptic Gregorian calendar in the scan | Integration | `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` | `date_add_before_the_gregorian_switch_follows_the_proleptic_calendar` |
| A form the scan cannot match runs in Exasol | Unit | `crates/vs-expression/src/scan_typing_tests.rs` | `unmatched_date_addition_forms_decline` |
| A form the scan cannot match runs in Exasol | Integration | `crates/lakehouse-e2e/tests/e2e_local/date_add.rs`, `timestamp_precision.rs` | `date_add_functions_match_native`, `date_addition_over_a_nanosecond_timestamp_runs_in_exasol` |
| The four functions render verbatim in the Exasol dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `date_addition_renders_verbatim_in_the_exasol_dialect` |
| The four functions render verbatim in the Exasol dialect | Integration | `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` | `date_add_functions_match_native` (declined cases render in the Exasol wrapper) |
| Unsupported date functions fall through as unsupported nodes | Unit | `crates/vs-expression/src/lib_tests.rs`, `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `unsupported_date_functions_decline_in_the_datafusion_dialect`, `reports_audited_capability_set` |
| ISO week capability is advertised so WEEK expressions push down | Unit | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `reports_audited_capability_set`, `capabilities_doc_lists_exactly_the_advertised_set` |

The rendering and capability-list tests are unit tests, because both are pure computation. Every behavior of the scan is covered by the E2E tests against the local Exasol Docker container (`specs/mission.md` § Commands, `specs/testing.md` § E2E suites).

### Manual Testing

Run after `make e2e-up test-e2e`, with `EXA` set to `exasol://sys:$EXASOL_SYS_PASSWORD@$EXASOL_HOST:$LH_EXASOL_PORT?validateservercertificate=0` from the Makefile's variables.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Date-Addition Functions | `exapump sql -d "$EXA" "EXPLAIN VIRTUAL SELECT ADD_HOURS(C_DATE, 1), ADD_DAYS(C_TS, C_DOUBLE) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE"` | The pushdown SQL contains `vs_add_hours(` and `vs_add_days(` |
| Date-Addition Functions | `exapump sql -d "$EXA" "SELECT ID, C_DATE, ADD_HOURS(C_DATE, 1.5) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE ORDER BY ID"` | Each non-NULL row shows the date at 02:00:00 as a `TIMESTAMP(0)` value |
| Date-Addition Functions | `exapump sql -d "$EXA" "SELECT ADD_DAYS(C_DATE, 3000000) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE"` | The query fails with `data exception - datetime field overflow` |
| Date-Addition Functions | `exapump sql -d "$EXA" "EXPLAIN VIRTUAL SELECT ADD_DAYS(DATE_TRUNC('month', C_DATE), 1) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE"` | The pushdown SQL does not contain `vs_add_` and contains `ADD_DAYS(` in the Exasol wrapper |
| Date-Difference Functions | `exapump sql -d "$EXA" "EXPLAIN VIRTUAL SELECT ADD_MONTHS(C_DATE, 1) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE"` | The pushdown SQL does not contain `ADD_MONTHS`, because Exasol computes it over the returned rows |
| Capability Extensions | `exapump sql -d "$EXA" "EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE ADD_WEEKS(C_DATE, 1) > DATE '2024-01-10'"` | The pushdown SQL contains a scan filter with `vs_add_weeks(` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make e2e-up test-e2e` | 0 failures |
| Lint | `make lint` | 0 errors or warnings |
| Format | `make fmt-check` | No changes |

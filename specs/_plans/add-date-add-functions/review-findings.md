# Code Review Findings: add-date-add-functions

## Summary
- Files reviewed: 22
- Total findings: 6 (standard: 4, expert: 2)

## Standard fixes

### crates/lakehouse-engine/src/scan/datetime_add.rs

#### [OUTDATED_COMMENT] The nanosecond-limit rationale documents the wrong constant
- Location: line 37-39 (`NANOS_PER_MICRO`), line 93-94 (`Unit::nanosecond_count_limit`)
- Issue: The doc comment on `NANOS_PER_MICRO` says that Exasol adds `ADD_HOURS` and `ADD_MINUTES` in 128-bit nanoseconds and that a larger count fails as a numeric overflow. That statement describes `Unit::nanosecond_count_limit`, not a unit-conversion constant of 1,000. A reader of `nanosecond_count_limit` does not see why the limit exists.
- Fix: In crates/lakehouse-engine/src/scan/datetime_add.rs, delete the two-line doc comment above `NANOS_PER_MICRO`. Replace the doc comment of `Unit::nanosecond_count_limit` with: "The largest whole hour or minute count whose nanoseconds fit 128 bits. Exasol adds these units in 128-bit nanoseconds, so a larger count fails as a numeric overflow, not as a datetime overflow."

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [INFORMATION_COUPLING] The adapter restates which functions the scan computes in whole microseconds
- Location: line 641-659 (`MICROSECOND_DIGITS`, `scan_fractional_digits`)
- Issue: `scan_fractional_digits` hard-codes the names `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES`. The same set is declared in `crates/vs-expression/src/scalar_functions.rs` and rendered in `crates/vs-expression/src/lib.rs`, which owns the `vs_add_*` contract. A date function added to the typed pass without an edit here would compare against a finer literal without the floor, and DataFusion would return wrong rows.
- Fix: In crates/vs-expression/src/lib.rs, next to `ADD_DAYS_FN`, add `pub const DATE_ADDITION_FUNCTIONS: [&str; 4] = ["ADD_DAYS", "ADD_WEEKS", "ADD_HOURS", "ADD_MINUTES"];` with the doc comment "The Exasol functions the DataFusion dialect renders as `vs_add_*`. Each scan result holds whole microseconds, so the adapter floors a finer timestamp literal compared with one." In crates/lakehouse-engine/src/adapter/pushdown/support.rs `scan_fractional_digits`, replace the `matches!` over the four names with `vs_expression::DATE_ADDITION_FUNCTIONS.contains(&name.as_str())`. Run `cargo test --workspace --lib overprecise_timestamp_literal_against_a_date_addition_is_floored`.

### crates/vs-expression/src/lib_tests.rs

#### [VAGUE_TEST_NAME] The Exasol-dialect test also asserts a DataFusion-dialect arity rule
- Location: line 2397-2409 (`date_addition_renders_verbatim_in_the_exasol_dialect`)
- Issue: The test name states the Exasol-dialect rendering. Its loop also asserts that `render_expression`, the DataFusion dialect, fails a one-argument call. Two concepts share one test, and the name hides the second.
- Fix: In crates/vs-expression/src/lib_tests.rs, move the `one_argument` assertion out of `date_addition_renders_verbatim_in_the_exasol_dialect` into a new test `date_addition_with_one_argument_fails_in_the_datafusion_dialect` that loops over `DATE_ADDITIONS` and asserts `render_expression(&scalar_fn(exasol_name, [col("d")])).is_err()`.

### crates/vs-expression/src/scan_typing_tests.rs

#### [VAGUE_TEST_NAME] The BOOLEAN-count test lacks its scenario line and names an effect it does not assert
- Location: line 808-815 (`a_boolean_date_addition_count_adds_one_or_zero`)
- Issue: The test asserts the rendered `CASE "B" WHEN TRUE THEN 1 WHEN FALSE THEN 0 END` wrapper, not an addition. It implements the BOOLEAN clause of the scenario "The scan rounds a fractional count as Exasol does" but carries no `/// Scenario:` line, which `AGENTS.md` § Code style requires.
- Fix: In crates/vs-expression/src/scan_typing_tests.rs, rename `a_boolean_date_addition_count_adds_one_or_zero` to `a_boolean_date_addition_count_renders_as_one_or_zero` and add the line `/// Scenario: The scan rounds a fractional count as Exasol does` above its `#[test]`.

## Expert fixes

### crates/vs-expression/src/scan_typing.rs

#### [UNTESTED_ERROR_PATH] A large constant ADD_HOURS or ADD_MINUTES count returns rows where native Exasol fails
- Location: line 451-507 (`date_addition_class`)
- Issue: Native Exasol checks a constant `ADD_HOURS` or `ADD_MINUTES` count before it reads a row. It fails the statement with `data exception - numeric value out of range` when the count times the nanoseconds per unit exceeds 36 digits (999,999,999,999,999,999,999,999,999,999,999,999), which is about 2.78e23 hours or 1.67e25 minutes. The scan checks only per non-NULL row, against 128 bits. Exasol folds every constant count (`1e15*1e15`, `CAST(1e30 AS DOUBLE)`, `POWER(10, 30)`, `-1e30`) to a `literal_exactnumeric` or `literal_double` node before pushdown. Measured on exasol/docker-db:2025.1.16, the running E2E stack:
  - `SELECT ID, ADD_HOURS(C_DATE, 1e30) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DATE IS NULL` returns 2 rows with NULL. The same query over `VALUES (CAST(NULL AS DATE))` fails with `numeric value out of range`.
  - `... WHERE ID < 0` returns 0 rows through the virtual schema. Native Exasol fails it.
  - `SELECT ID, ADD_HOURS(C_DATE, 1e24) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE` fails with `datetime field overflow` from the scan. Native Exasol fails with `numeric value out of range: value 3600000000000000000000000000000000000 (internal number representation) is not in [ -999999999999999999999999999999999999 .. 999999999999999999999999999999999999 ]`. The E2E harness compares this error class, so the two disagree for every constant count from 2.78e23 to 4.7e25 hours.
  - `ADD_DAYS` and `ADD_WEEKS` make no such check: `ADD_DAYS(d, 1e300)` over a NULL date returns NULL natively.
  - A non-constant count matches: `ADD_HOURS(C_DATE, ID * 1e15)` and `ADD_MINUTES(C_DATE, ID * -1e30)` raise the native error class through the scan.

  The deviation is neither fixed nor listed in `docs/semantic-differences.md`, and `specs/mission.md` Core Capability 12 accepts no other kind. Declining the constant form restores parity without moving the 36-digit rule into the scan.
- Fix:
  1. In crates/vs-expression/src/scan_typing.rs, add `const MAX_SCAN_CONSTANT_COUNT: f64 = 1e10;` with the doc comment "Exasol rejects a constant `ADD_HOURS` or `ADD_MINUTES` count whose nanoseconds exceed 36 digits before it reads a row, while the scan fails only per non-NULL row. No count above 1e10 yields a result inside 0001-01-01 to 9999-12-31 in any unit (the range spans 5,258,964,960 minutes), so Exasol computes such a call."
  2. In `date_addition_class`, after the `match count` block and before the `function_scalar_cast` wrapping, read `node["arguments"][1]`. When its `type` is `literal_exactnumeric` or `literal_double`, parse its `value` string with `str::parse::<f64>`. Return `Err(decline(format!("a constant date addition count beyond {MAX_SCAN_CONSTANT_COUNT} runs in Exasol, which checks it before it reads a row: {}", short(node))))` when the parse fails or the absolute value is above `MAX_SCAN_CONSTANT_COUNT`.
  3. In crates/vs-expression/src/scan_typing_tests.rs `unmatched_date_addition_forms_decline`, add per function name two count cases built with `lit`: `lit("literal_exactnumeric", "1000000000000000000000000000000")` and `lit("literal_double", "1.0000000000000000e+30")`, the value form Exasol sends, plus `lit("literal_exactnumeric", "10000000001")`. Add a test `a_constant_count_at_the_limit_renders` that asserts `render(&scalar_fn("ADD_MINUTES", [col("D"), lit("literal_exactnumeric", "10000000000")]))` is `Ok`.
  4. In crates/lakehouse-e2e/tests/e2e_local/date_add.rs `error_cases`, replace the counts `1e15`, `-1e15`, `1e30`, and `-1e30` with `ID * 1e15`, `ID * -1e15`, `ID * 1e30`, and `ID * -1e30`, keeping `Has(SCAN_FUNCTION)`. Add these cases with `Lacks(SCAN_FUNCTION)` and `Expect::Error`: `SELECT ID, ADD_HOURS(C_DATE, 1e24) FROM {T}`, `SELECT ID, ADD_MINUTES(C_DATE, -1e30) FROM {T}`, `SELECT ID, ADD_HOURS(C_DATE, 1e30) FROM {T} WHERE C_DATE IS NULL`, and `SELECT ID, ADD_HOURS(C_DATE, 1e30) FROM {T} WHERE ID < 0`.
  5. In specs/_plans/add-date-add-functions/sql-functions/vs-expression-translator-date-add-fns/spec.md, scenario "A form the scan cannot match runs in Exasol": append "or whose count is a numeric literal with a magnitude above 1e10" to the GIVEN step. Add the step "*AND* a numeric literal count with a magnitude above 1e10 SHALL decline, because Exasol rejects a constant `ADD_HOURS` or `ADD_MINUTES` count whose nanoseconds exceed 36 digits before it reads a row". In scenario "A result outside Exasol's datetime range fails the query", line 69, change "an `ADD_HOURS` or `ADD_MINUTES` count whose nanoseconds exceed 128 bits" to "an `ADD_HOURS` or `ADD_MINUTES` count that is not a literal and whose nanoseconds exceed 128 bits".
  6. In docs/semantic-differences.md, section "ADD_DAYS, ADD_WEEKS, ADD_HOURS, and ADD_MINUTES argument forms", add the list item "a numeric literal count with a magnitude above 1e10;". In docs/capabilities.md line 83, add "a numeric literal count above 1e10 in magnitude" to the parenthesized list.
  7. Run `cargo test --workspace --lib date_addition` and `make e2e-up test-e2e`.

### docs/semantic-differences.md

#### [OUTDATED_COMMENT] The pre-1582 table lists native values that the server does not return
- Location: docs/semantic-differences.md line 130-140; specs/_plans/add-date-add-functions/sql-functions/vs-expression-translator-date-add-fns/spec.md line 85 and 87
- Issue: Four of the five documented calls return the same date through the virtual schema as natively, and every "Native Exasol" value differs from the server's own rendering. The server renders a date through `TO_CHAR` and through the WebSocket rows the E2E harness reads. Measured on exasol/docker-db:2025.1.16 with `TO_CHAR(..., 'YYYY-MM-DD')`, native Exasol returns 1000-01-02 for `ADD_DAYS(DATE '1000-01-01', 1)`, 1500-02-29 for `ADD_DAYS(DATE '1500-03-01', -1)`, 1582-10-04 for `ADD_DAYS(DATE '1582-10-04', 0)`, 1582-10-15 for `ADD_DAYS(DATE '1582-10-16', -1)`, and 1000-01-02 00:00:00 for `ADD_HOURS(DATE '1000-01-01', 24)`. The documented values 1000-01-03, 1500-03-02, 1582-10-05, 1582-10-16, and 1000-01-03 00:00:00 match exapump's default output, which renders these dates one or two days later than the server. The same exapump offset shows for the virtual schema: exapump prints 1476-06-03 for `ADD_DAYS(C_DATE, -200000)`, while the passing E2E test `date_add_before_the_gregorian_switch_follows_the_proleptic_calendar` reads 1476-06-02. The spec scenario "A date before 1582-10-15 follows the proleptic Gregorian calendar in the scan" uses the same false example: native Exasol returns 1000-01-02, not 1000-01-03, for 1000-01-01 plus 1 day. A call over literal arguments also never reaches the scan, because Exasol folds it into a date literal before pushdown. The two calendars give different results only when the days between the argument and the result include February 29 of a year that is divisible by 100 but not by 400, or cross 1582-10-15.
- Fix:
  1. In docs/semantic-differences.md, section "ADD_DAYS, ADD_WEEKS, ADD_HOURS, and ADD_MINUTES before 1582-10-15", replace the last sentence of the first paragraph with: "A call returns a different value when the days between its argument and its result include February 29 of a year divisible by 100 but not by 400, which only the Julian calendar has, or cross 1582-10-15. A call whose arguments are all literals runs in Exasol, because Exasol computes it before pushdown."
  2. Replace the table with this one, where `d` is a column value, so that the scan computes the call:

     | Call | Native Exasol (2025.1.16) | Scan |
     |---|---|---|
     | `ADD_DAYS(d, 1)` with `d` = 1000-02-28 | `1000-02-29` | `1000-03-01` |
     | `ADD_DAYS(d, -1)` with `d` = 1500-03-01 | `1500-02-29` | `1500-02-28` |
     | `ADD_HOURS(d, 24)` with `d` = 1500-02-28 | `1500-02-29 00:00:00` | `1500-03-01 00:00:00` |
     | `ADD_DAYS(d, -1)` with `d` = 1582-10-15 | `1582-10-04` | `1582-10-14` |
     | `ADD_DAYS(d, -200000)` with `d` = 2024-01-01 | `1476-05-24` | `1476-06-02` |

  3. In specs/_plans/add-date-add-functions/sql-functions/vs-expression-translator-date-add-fns/spec.md, line 85, replace "so an argument of 1000-01-01 with a count of 1 day returns 1000-01-02" with "so an argument of 1500-03-01 with a count of -1 day returns 1500-02-28". On line 87, replace "returns 1000-01-03 for the same call" with "returns 1500-02-29 for the same call".
  4. In specs/_plans/add-date-add-functions/plan.md, Context bullet 9, replace the two measured examples with the `TO_CHAR` values above: `ADD_DAYS(DATE '1500-03-01', -1)` returns 1500-02-29, and `ADD_DAYS(DATE '1582-10-15', -1)` returns 1582-10-04.
  5. When you measure a date before 1601, render it with `TO_CHAR(<expr>, 'YYYY-MM-DD')` in the query or read it through the E2E harness. Do not read it from exapump's default output.

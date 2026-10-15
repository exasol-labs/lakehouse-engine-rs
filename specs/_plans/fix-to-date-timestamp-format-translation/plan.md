# Plan: fix-to-date-timestamp-format-translation

## Summary

The expression translator turns the Exasol format model of a pushed `TO_DATE` or `TO_TIMESTAMP` into the strftime pattern DataFusion parses, so these calls return native results in the scan instead of failing it (#433). A format with no exact strftime equivalent declines, and the adapter's existing decline route lets Exasol evaluate it.

## Context

- Exasol pushes `TO_DATE(s, fmt)` and `TO_TIMESTAMP(s, fmt)` with its own format model, and the DataFusion dialect forwards it unchanged (`crates/vs-expression/src/lib.rs`, arms `"TO_DATE"` and `"TO_TIMESTAMP"`). DataFusion's `to_date` and `to_timestamp` parse strftime patterns, so the scan fails. Reported on SaaS staging (TPC-H SF100, Iceberg). Reproduced on the Docker Exasol (2025.1.16, deployed build 0.52.2) over the `e2e_lakehouse.events` table: `TO_DATE(CAST(EVENT_DATE AS VARCHAR(10)), 'YYYY-MM-DD')` fails with `Error parsing timestamp from '2024-01-01' using format 'YYYY-MM-DD': input contains invalid characters`, in the select list and in a filter. The one-argument forms work. Related: #161, #216, #433.
- The unit test `renders_to_date_to_timestamp_verbatim_in_exasol_dialect` asserts the defective DataFusion rendering `to_date("S", 'YYYY-MM-DD')`.
- Each translated element was measured against native Exasol on about 130 input strings, with DataFusion 54.1 run on the same strings. The results set the translated set, the structural rules, and the listed parse differences (decision-log [1] to [3]).
- A DataFusion-dialect decline of an advertised call already reaches native Exasol on every pushdown path, with join pushdown on and off (decision-log [1]). The plan adds no adapter route. The E2E tests in group A prove it again on the implementing build.
- On Exasol 2025.1.16, `TO_DATE` returns `DATE` and `TO_TIMESTAMP` returns `TIMESTAMP(9)` for every format, and the scan declares `TIMESTAMP(9)` for the pushed expression. DataFusion's `to_timestamp` returns nanoseconds, so an `FF9` value keeps all nine digits.
- Out of scope, and independent of the format: `TO_TIMESTAMP` over a `DATE` source fails the scan (DataFusion's `to_timestamp` rejects `Date32`) where Exasol returns midnight. `TO_DATE` over a numeric source returns a day count since 1970 or a range error, where native Exasol refuses the query with `0A000`. Both reproduce with the one-argument form today.
- Iceberg and Delta compliance: not engaged. No section of the Iceberg table spec or the Delta protocol governs parsing an SQL string into a date. A `TO_DATE` or `TO_TIMESTAMP` filter never reaches file pruning, because `adapter/iceberg_predicate.rs` translates no `function_scalar` node. No type mapping, scan planning, or schema handling changes.
- Architecture: no change. The plan adds a private module inside the existing `vs-expression` translator component. `specs/architecture.md` does not mention format models.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| VS Expression Translator — Datetime Format Models | NEW | `sql-functions/vs-expression-translator-datetime-format/spec.md` |
| VS Expression Translator — Date and Time Functions | CHANGED | `sql-functions/vs-expression-translator-date-fns/spec.md` |

## Impact

- `TO_DATE` and `TO_TIMESTAMP` with a supported format (spec Background) run in the scan and return the native value. These queries failed before. An empty source string yields NULL, as in Exasol.
- Any other format runs in Exasol and returns native results. A filter that holds such a call pushes nothing into the scan, so the scan reads every row of its files.
- The listed parse differences (`docs/semantic-differences.md`) make a few input strings fail the query or return a value where native Exasol raises an error.
- Not breaking: no SQL syntax, virtual schema property, capability, or result type changes.

## Dependencies

- None. Renderer only. The unrecorded plan `fix-trunc-round-date-trunc-date-args` edits other arms of the same `function_scalar` match in `lib.rs`, so the plan that lands second rebases onto the first.

## Open Questions

- Should a follow-up issue (#TBD) cover the format-independent source-type differences named in Context?

## Implementation Tasks

### A: TO_DATE and TO_TIMESTAMP format translation

- [ ] 1.1 Create `crates/vs-expression/src/datetime_format_tests.rs` with two table-driven unit tests of `chrono_format(format, target) -> Option<String>`, where `target` is `Date` or `Timestamp`. No scenario tag, because the E2E tests of 2.1 and 2.3 prove the scenarios.
  - `translates_supported_formats`: `YYYY-MM-DD` → `%Y-%m-%d` for both targets, also as `yyyy-mm-dd` and `Yyyy-Mm-Dd`; `DD.MM.YYYY` → `%d.%m.%Y`; `YYYYMMDD` → `%Y%m%d`; `YYYY_MM_DD` → `%Y_%m_%d`; `YYYY/MM/DD`, `YYYY,MM;DD`, `YYYY:MM:DD`, and `YYYY MM DD` keep their separators. Timestamp target only: `YYYY-MM-DD HH24:MI:SS` → `%Y-%m-%d %H:%M:%S`; `YYYY-MM-DD HH24:MI` → `%Y-%m-%d %H:%M`; the tails `SS.FF3`, `ss.ff9`, and `SSFF6` after `YYYY-MM-DD HH24:MI:` → `%S%.3f`, `%S%.9f`, `%S%6f`; `HH24:MI:SS YYYY-MM-DD` → `%H:%M:%S %Y-%m-%d`.
  - `declines_unsupported_formats`, one case per decline reason: an unknown element (`DD-MON-YYYY`), a `TO_DATE` with a time (`YYYY-MM-DD HH24:MI`), a `TO_TIMESTAMP` with an hour but no minutes (`YYYY-MM-DD HH24`), and a missing `DD` (`YYYY-MM`).
  Both tests fail before 1.2.
- [ ] 1.2 Add `crates/vs-expression/src/datetime_format.rs`, declared in `lib.rs` as a private `mod datetime_format;`, with `#[cfg(test)] #[path = "datetime_format_tests.rs"] mod tests;`. Implement `chrono_format` as one anchored allowlist regex, `(?i)^(?:\.?FF[369]|HH24|YYYY|MM|MI|DD|SS|[-/.,;:_ ])*$`, with a comment of at most 2 lines (supported parts and separators only, anything else declines). Then tokenize, replace each token through a lookup table to its strftime form, and apply the structure rule: `YYYY`, `MM`, `DD` once each, and for `Timestamp` the time elements none, `HH24`+`MI`, `HH24`+`MI`+`SS`, or those three plus one `FFn` (decision-log [2]). `regex` is already compiled into the `.so` through DataFusion. Give the function a doc comment of at most 2 lines: `None` means no exact strftime equivalent exists, so the caller declines.
- [ ] 1.3 In `crates/vs-expression/src/lib_tests.rs`, change the format rows of `renders_to_date_to_timestamp` to Exasol format models (`YYYY-MM-DD` renders `to_date(nullif("S", ''), '%Y-%m-%d')`, `YYYY-MM-DD HH24:MI:SS` renders `to_timestamp(nullif("S", ''), '%Y-%m-%d %H:%M:%S')`), and keep its one-argument rows. In `renders_to_date_to_timestamp_verbatim_in_exasol_dialect`, change the DataFusion expectation to `'%Y-%m-%d'` and keep the Exasol expectation. Add `to_date_to_timestamp_decline_untranslatable_format` with the line `/// Scenario: A format the translation cannot express declines in the DataFusion dialect`. For a `column` format, a `literal_null` format, `'DD-MON-YYYY'`, a `TO_DATE` with `'YYYY-MM-DD HH24:MI'`, and three arguments, assert that `render_expression` errs, `render_expression_safe` returns `None`, and `render_expression_exasol` renders the call verbatim. Keep the `TO_DATE` and `TO_TIMESTAMP` rows of the declared-surface sweep unchanged. The changed and new tests fail before 1.4.
- [ ] 1.4 In `crates/vs-expression/src/lib.rs`, replace the `"TO_DATE"` and `"TO_TIMESTAMP"` arms of `render_expression_inner` with one arm. One argument renders as today. Two arguments render `to_date(nullif(<src>, ''), '<pattern>')` or `to_timestamp(nullif(<src>, ''), '<pattern>')` when the second argument is a `literal_string` and `chrono_format` returns a pattern. Every other shape returns `UdfError::User` naming the function and the reason. The Exasol-dialect `VerbatimCall` branch stays unchanged. Run `cargo test -p vs-expression`.
- [ ] 2.1 In `crates/lakehouse-engine/tests/e2e_capability_test.rs`, add `e2e_to_date_to_timestamp_translated_format_matches_native` with the lines `/// Scenario: TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls` and `/// Scenario: A string-literal format translates to a strftime pattern`. Over `vs_table()`:
  - `SELECT TO_DATE(CAST(event_date AS VARCHAR(10)), 'YYYY-MM-DD') ... WHERE id = 1` returns `2024-01-01`.
  - `SELECT TO_TIMESTAMP(CAST(event_date AS VARCHAR(10)) || ' 10:11:12', 'YYYY-MM-DD HH24:MI:SS') ... WHERE id = 1` returns the value of the native query `SELECT TO_TIMESTAMP('2024-01-01 10:11:12', 'YYYY-MM-DD HH24:MI:SS')`.
  - `SELECT id ... WHERE TO_TIMESTAMP(CAST(event_date AS VARCHAR(10)) || ' 10:11:12.250', 'yyyy-mm-dd hh24:mi:ss.ff3') > TIMESTAMP '2024-01-15 10:11:12.000' ORDER BY id` returns ids 15 to 20, 6 of the 20 rows.
  - `explain_virtual_sql` of the first and third queries contains `%Y-%m-%d` and `%H:%M:%S%.3f` inside the `LAKEHOUSE_SCAN` call, and neither contains `YYYY` or `yyyy`.
  Every query fails against the defect.
- [ ] 2.2 In the same file, add `e2e_to_date_translated_format_fails_on_rejected_input` with the line `/// Scenario: A string Exasol rejects for a translated format fails the query`. `query_error` must succeed for `SELECT TO_DATE(name, 'YYYY-MM-DD') FROM <vs_table()> WHERE id = 1` (`name` is `event-01`), for `SELECT COUNT(*) FROM <vs_table()> WHERE TO_DATE(name, 'YYYY-MM-DD') > DATE '2000-01-01'`, and for `SELECT TO_DATE(CAST(event_date AS VARCHAR(10)), 'YYYY-DD-MM') FROM <vs_table()> WHERE id = 13` (month 13). The native oracle `SELECT TO_DATE('event-01', 'YYYY-MM-DD')` must fail too. `explain_virtual_sql` of the first query must contain `%Y-%m-%d`, so the test fails against the defect, which also raises but forwards `YYYY-MM-DD`.
- [ ] 2.3 In the same file, add `e2e_to_date_declined_format_matches_native` with the line `/// Scenario: A declined format returns the native result on every pushdown path`. Over `vs_table()`, `SELECT id ... WHERE TO_DATE(CAST(id AS VARCHAR(2)) || '-JAN-2024', 'DD-MON-YYYY') > DATE '2024-01-17' ORDER BY id` returns 18, 19, 20, and `explain_virtual_sql` contains `TO_DATE(` and does not contain `to_date(`. It fails against the defect. The unit test of 1.3 proves that a format built from a column declines, and decision-log [1] measured the decline route on 16 query shapes.
- [ ] 2.5 In `docs/semantic-differences.md`, add a short section `## TO_DATE and TO_TIMESTAMP with a format`: the supported formats in the spec's words, the sentence "Other formats run in Exasol.", and a table of the likely input differences, one example each, virtual schema result next to native result: trailing space `'2024-01-01 '` under `YYYY-MM-DD` (error, `2024-01-01`); input shorter than the format `'2024-01-01'` under `YYYY-MM-DD HH24:MI:SS` (error, midnight); a `TO_TIMESTAMP` of `'1600-01-01 00:00:00'` (error, the value).
- [ ] 2.6 Run `cargo fmt --all`, `cargo clippy --all-targets` (zero warnings), and `cargo test --workspace`. Run `make test-e2e` against the Docker stack and report the real result. Measure changed-line coverage with `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info` and `python3 .claude/skills/test-strategy-review/scripts/changed_line_coverage.py <scratchpad>/lcov.info`. It must reach 90%.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: TO_DATE and TO_TIMESTAMP format translation | 1.1-2.6 | — | spec deltas `sql-functions/vs-expression-translator-datetime-format`, `sql-functions/vs-expression-translator-date-fns`; `crates/vs-expression/src/lib.rs`, `crates/vs-expression/src/datetime_format.rs`, `crates/vs-expression/src/datetime_format_tests.rs`, `crates/vs-expression/src/lib_tests.rs`, `crates/lakehouse-engine/tests/e2e_capability_test.rs`, `docs/semantic-differences.md` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Match arms | `crates/vs-expression/src/lib.rs`, `"TO_DATE"` and `"TO_TIMESTAMP"` arms of `render_expression_inner` | Replaced by one arm that translates or declines the format |
| Test assertions | `crates/vs-expression/src/lib_tests.rs`, format rows of `renders_to_date_to_timestamp` and the DataFusion assertion of `renders_to_date_to_timestamp_verbatim_in_exasol_dialect` | They assert the forwarded format, which this plan removes |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls | E2E + Unit | `crates/lakehouse-engine/tests/e2e_capability_test.rs`, `crates/vs-expression/src/lib_tests.rs` | `e2e_to_date_to_timestamp_translated_format_matches_native`, `renders_to_date_to_timestamp` |
| A string-literal format translates to a strftime pattern | E2E + Unit | `crates/lakehouse-engine/tests/e2e_capability_test.rs`, `crates/vs-expression/src/datetime_format_tests.rs` | `e2e_to_date_to_timestamp_translated_format_matches_native`, `translates_supported_formats` |
| A format the translation cannot express declines in the DataFusion dialect | Unit | `crates/vs-expression/src/lib_tests.rs`, `crates/vs-expression/src/datetime_format_tests.rs` | `to_date_to_timestamp_decline_untranslatable_format`, `declines_unsupported_formats` |
| A declined format returns the native result on every pushdown path | E2E | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_to_date_declined_format_matches_native` |
| A string Exasol rejects for a translated format fails the query | E2E | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_to_date_translated_format_fails_on_rejected_input` |

The decline scenario is a renderer property that Exasol SQL cannot observe, so unit tests prove it (`specs/testing.md` § Coverage rule). Its end-to-end effect is the next scenario.

### Manual Testing

`$DSN` is `exasol://sys:exasol@localhost:28563?validateservercertificate=0`, after `make test-e2e` has created `MY_LAKEHOUSE`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| VS Expression Translator — Datetime Format Models | `exapump sql "SELECT TO_TIMESTAMP(CAST(EVENT_DATE AS VARCHAR(10)) \|\| ' 10:11:12', 'YYYY-MM-DD HH24:MI:SS') FROM MY_LAKEHOUSE.EVENTS WHERE ID = 1" -d "$DSN"` | The native value `2024-01-01 10:11:12` |
| VS Expression Translator — Datetime Format Models | `exapump sql "SELECT PUSHDOWN_SQL FROM (EXPLAIN VIRTUAL SELECT TO_DATE(CAST(ID AS VARCHAR(2)) \|\| '-JAN-2024', 'DD-MON-YYYY') FROM MY_LAKEHOUSE.EVENTS)" -d "$DSN"` | Wrapper SQL with `TO_DATE(` and `'DD-MON-YYYY'` outside the `LAKEHOUSE_SCAN` call, and no `to_date(` |
| VS Expression Translator — Date and Time Functions | `exapump sql "SELECT TO_DATE(CAST(EVENT_DATE AS VARCHAR(10)), 'YYYY-MM-DD') FROM MY_LAKEHOUSE.EVENTS WHERE ID = 1" -d "$DSN"` | `2024-01-01` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| Coverage | `python3 .claude/skills/test-strategy-review/scripts/changed_line_coverage.py <scratchpad>/lcov.info` | Changed lines at or above 90% |

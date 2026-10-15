# Plan: fix-to-date-timestamp-format-translation

## Summary

The expression translator turns the Exasol format model of a pushed `TO_DATE` or `TO_TIMESTAMP` into the strftime pattern DataFusion parses, so these calls return native results in the scan instead of failing it (#TBD). A format with no exact strftime equivalent declines, and the adapter's existing decline route lets Exasol evaluate it.

## Context

- Exasol pushes `TO_DATE(s, fmt)` and `TO_TIMESTAMP(s, fmt)` with its own format model, and the DataFusion dialect forwards it unchanged (`crates/vs-expression/src/lib.rs`, arms `"TO_DATE"` and `"TO_TIMESTAMP"`). DataFusion's `to_date` and `to_timestamp` parse strftime patterns, so the scan fails. Reported on SaaS staging (TPC-H SF100, Iceberg). Reproduced on the Docker Exasol (2025.1.16, deployed build 0.52.2) over the `e2e_lakehouse.events` table: `TO_DATE(CAST(EVENT_DATE AS VARCHAR(10)), 'YYYY-MM-DD')` fails with `Error parsing timestamp from '2024-01-01' using format 'YYYY-MM-DD': input contains invalid characters`, in the select list and in a filter. The one-argument forms work. Related: #161, #216.
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

- `TO_DATE` and `TO_TIMESTAMP` with a format built from `YYYY`, `MM`, `DD`, `HH24`, `MI`, `SS`, `FF3`, `FF6`, `FF9`, and the separators `-` `/` `.` `,` `;` `:` and space run in the scan and return the native value. These queries failed before.
- Any other format, including `MON`, `MONTH`, `DY`, `DAY`, `IW`, `IYYY`, `FF1`, `FF2`, `FF4`, `FF5`, `FF7`, `FF8` (decision-log [3]), and a format built from a column, runs in Exasol through the decline route. Such a query returns native results instead of failing. A query whose filter holds such a call pushes none of that filter into the scan, so the scan returns every row of the files it reads.
- The listed parse differences (`docs/semantic-differences.md`) make a few input strings fail the query or return a value where native Exasol raises an error.
- Not breaking: no SQL syntax, virtual schema property, capability, or result type changes.

## Dependencies

- None. Renderer only. The unrecorded plan `fix-trunc-round-date-trunc-date-args` edits other arms of the same `function_scalar` match in `lib.rs`, so the plan that lands second rebases onto the first.

## Open Questions

- Which GitHub issue does the implementing commit close? None was given, so this plan cites (#TBD).
- Should a follow-up issue (#TBD) cover the format-independent source-type differences named in Context?

## Implementation Tasks

### A: TO_DATE and TO_TIMESTAMP format translation

- [ ] 1.1 Create `crates/vs-expression/src/datetime_format_tests.rs` with two table-driven unit tests of `chrono_format(format, target) -> Option<String>`, where `target` is `Date` or `Timestamp`. No scenario tag, because the E2E tests of 2.1 and 2.3 prove the scenarios.
  - `translates_supported_formats`: `YYYY-MM-DD` → `%Y-%m-%d` for both targets, also as `yyyy-mm-dd` and `Yyyy-Mm-Dd`; `DD.MM.YYYY` → `%d.%m.%Y`; `YYYYMMDD` → `%Y%m%d`; `YYYY/MM/DD`, `YYYY,MM;DD`, `YYYY:MM:DD`, and `YYYY MM DD` keep their separators. Timestamp target only: `YYYY-MM-DD HH24:MI:SS` → `%Y-%m-%d %H:%M:%S`; `YYYY-MM-DD HH24:MI` → `%Y-%m-%d %H:%M`; after `YYYY-MM-DD HH24:MI:`, the tails `SS.FF3`, `SS.FF6`, `ss.ff9`, `SSFF3`, and `SS,FF6` → `%S%.3f`, `%S%.6f`, `%S%.9f`, `%S%3f`, and `%S,%6f`; `HH24:MI:SS YYYY-MM-DD` → `%H:%M:%S %Y-%m-%d`.
  - `declines_unsupported_formats` (both targets unless noted): the elements `MON`, `MONTH`, `DY`, `DAY`, `D`, `DDD`, `IW`, `IYYY`, `RR`, `YY`, `YYY`, `Y`, `YYYYY`, `HH`, `HH12`, `AM`, `PM`, `J`, `Q`, `WW`, `CC`, `FF`, `FF1`, `FF2`, `FF4`, `FF5`, `FF7`, `FF8`, `FF30`, `SSSSS` (each inside an otherwise valid timestamp format); the characters `T`, `_`, `%`, `"`, `'`, and a tab; `YYYY-MM`, `MM-DD`, `DD`, and the empty format; `YYYY-MM-DD YYYY`; for the `Date` target `YYYY-MM-DD HH24:MI`, `YYYY-MM-DD HH24:MI:SS`, and `YYYY-MM-DD SS`; for the `Timestamp` target `YYYY-MM-DD HH24`, `YYYY-MM-DD MI`, `YYYY-MM-DD HH24:SS`, and `YYYY-MM-DD HH24:MI.FF3`.
  Both tests fail before 1.2.
- [ ] 1.2 Add `crates/vs-expression/src/datetime_format.rs`, declared in `lib.rs` as a private `mod datetime_format;`, with `#[cfg(test)] #[path = "datetime_format_tests.rs"] mod tests;`. Implement `chrono_format` per the new feature's Background: scan the format left to right, ignore case, and match the longest supported token first (`.FF3`/`.FF6`/`.FF9` as `%.3f`/`%.6f`/`%.9f`, then `FF3`/`FF6`/`FF9`, `HH24`, `YYYY`, `MM`, `MI`, `DD`, `SS`). Copy a separator from the set `- / . , ; :` and space. Return `None` for any other character or element, then apply the structural rules. Give the function a doc comment of at most 2 lines: `None` means no exact strftime equivalent exists, so the caller declines.
- [ ] 1.3 In `crates/vs-expression/src/lib_tests.rs`, change the format rows of `renders_to_date_to_timestamp` to Exasol format models (`YYYY-MM-DD` renders `to_date("S", '%Y-%m-%d')`, `YYYY-MM-DD HH24:MI:SS` renders `to_timestamp("S", '%Y-%m-%d %H:%M:%S')`), and keep its one-argument rows. In `renders_to_date_to_timestamp_verbatim_in_exasol_dialect`, change the DataFusion expectation to `'%Y-%m-%d'` and keep the Exasol expectation. Add `to_date_to_timestamp_decline_untranslatable_format` with the line `/// Scenario: A format the translation cannot express declines in the DataFusion dialect`. For a `column` format, a `literal_null` format, `'DD-MON-YYYY'`, a `TO_DATE` with `'YYYY-MM-DD HH24:MI'`, and three arguments, assert that `render_expression` errs, `render_expression_safe` returns `None`, and `render_expression_exasol` renders the call verbatim. Keep the `TO_DATE` and `TO_TIMESTAMP` rows of the declared-surface sweep unchanged. The changed and new tests fail before 1.4.
- [ ] 1.4 In `crates/vs-expression/src/lib.rs`, replace the `"TO_DATE"` and `"TO_TIMESTAMP"` arms of `render_expression_inner` with one arm. One argument renders as today. Two arguments render `to_date(<src>, '<pattern>')` or `to_timestamp(<src>, '<pattern>')` when the second argument is a `literal_string` and `chrono_format` returns a pattern. Every other shape returns `UdfError::User` naming the function and the reason. The Exasol-dialect `VerbatimCall` branch stays unchanged. Run `cargo test -p vs-expression`.
- [ ] 2.1 In `crates/lakehouse-engine/tests/e2e_capability_test.rs`, add `e2e_to_date_to_timestamp_translated_format_matches_native` with the lines `/// Scenario: TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls` and `/// Scenario: A string-literal format translates to a strftime pattern`. Over `vs_table()`:
  - `SELECT TO_DATE(CAST(event_date AS VARCHAR(10)), 'YYYY-MM-DD') ... WHERE id = 1` returns `2024-01-01`.
  - `SELECT TO_TIMESTAMP(CAST(event_date AS VARCHAR(10)) || ' 10:11:12', 'YYYY-MM-DD HH24:MI:SS') ... WHERE id = 1` returns the value of the native query `SELECT TO_TIMESTAMP('2024-01-01 10:11:12', 'YYYY-MM-DD HH24:MI:SS')`.
  - `SELECT id ... WHERE TO_TIMESTAMP(CAST(event_date AS VARCHAR(10)) || ' 10:11:12.250', 'yyyy-mm-dd hh24:mi:ss.ff3') > TIMESTAMP '2024-01-15 10:11:12.000' ORDER BY id` returns ids 15 to 20, 6 of the 20 rows.
  - `explain_virtual_sql` of the first and third queries contains `%Y-%m-%d` and `%H:%M:%S%.3f` inside the `LAKEHOUSE_SCAN` call, and neither contains `YYYY` or `yyyy`.
  Every query fails against the defect.
- [ ] 2.2 In the same file, add `e2e_to_date_translated_format_fails_on_rejected_input` with the line `/// Scenario: A string Exasol rejects for a translated format fails the query`. `query_error` must succeed for `SELECT TO_DATE(name, 'YYYY-MM-DD') FROM <vs_table()> WHERE id = 1` (`name` is `event-01`), for `SELECT COUNT(*) FROM <vs_table()> WHERE TO_DATE(name, 'YYYY-MM-DD') > DATE '2000-01-01'`, and for `SELECT TO_DATE(CAST(event_date AS VARCHAR(10)), 'YYYY-DD-MM') FROM <vs_table()> WHERE id = 13` (month 13). The native oracle `SELECT TO_DATE('event-01', 'YYYY-MM-DD')` must fail too. `explain_virtual_sql` of the first query must contain `%Y-%m-%d`, so the test fails against the defect, which also raises but forwards `YYYY-MM-DD`.
- [ ] 2.3 In the same file, add `e2e_to_date_declined_format_matches_native` with the line `/// Scenario: A declined format returns the native result on every pushdown path`. Over `vs_table()`, with `f(x)` = `TO_DATE(CAST(x AS VARCHAR(2)) || '-JAN-2024', 'DD-MON-YYYY')`:
  - select list: `SELECT id, f(id) ... WHERE id <= 3 ORDER BY id` returns 2024-01-01 to 2024-01-03;
  - filter: `SELECT id ... WHERE f(id) > DATE '2024-01-17' ORDER BY id` returns 18, 19, 20;
  - `GROUP BY` key: `SELECT f(MOD(id, 3) + 1), COUNT(*) ... GROUP BY f(MOD(id, 3) + 1) ORDER BY 1` returns (2024-01-01, 6), (2024-01-02, 7), (2024-01-03, 7);
  - aggregate argument: `SELECT MAX(f(id)) ...` returns 2024-01-20;
  - format built from a column: `SELECT id, TO_DATE(CAST(event_date AS VARCHAR(10)), 'YYYY-MM-' || SUBSTR(name, 1, 0) || 'DD') ... WHERE id <= 2 ORDER BY id` returns 2024-01-01 and 2024-01-02;
  - `explain_virtual_sql` of the select-list query contains `TO_DATE(` and does not contain `to_date(`.
  The expected values follow from the seed (`event_date` is 2024-01-01 plus `id - 1` days) and match the native results measured on copies of the table. Every query fails against the defect. If a query fails or returns other rows after 1.4, stop and report the path: its decline routing is a defect this plan does not schedule.
- [ ] 2.4 In `crates/lakehouse-engine/tests/e2e_join_test.rs`, add `e2e_join_declined_to_date_format_matches_native` with the same scenario line. For `VS_NAME` and `VS_NAME_LOW`, `SELECT o.O_ORDERKEY, c.C_NAME FROM <vs>.FACT_ORDERS o JOIN <vs>.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY WHERE TO_DATE(CAST(o.O_ORDERKEY AS VARCHAR(2)) || '-JAN-2024', 'DD-MON-YYYY') > DATE '2024-01-07' ORDER BY 1` returns (8, customer-03), (9, customer-04), (10, customer-05). It fails against the defect.
- [ ] 2.5 In `docs/semantic-differences.md`, add a section `## TO_DATE and TO_TIMESTAMP with a format`. State the translated elements and separators, that Exasol evaluates every other format with native results, and give a table of the input classes from the new feature's Background with one example each: the virtual schema result next to the native result. Examples: `'2024-01-01 '` and `'2024-01-01 10:11:12'` under `YYYY-MM-DD` (error, `2024-01-01`); `'2024-01-01'` under `YYYY-MM-DD HH24:MI:SS` (error, midnight); `'10:11:12.5'` under `HH24:MI:SS.FF3` (error, `.500`); an empty string (error, NULL); `TO_TIMESTAMP` of `'1600-01-01 00:00:00'` (error, the value); `'+2024-01-01'` (`2024-01-01`, error); `'-2024-01-01'` in a `TO_DATE(..., 'YYYY-MM-DD')` filter (row compared, error); `'0000-01-01'` in the same filter (row compared, error); `'2024-01-' || CHR(9) || '01'` (`2024-01-01`, error); `'20240101'` under `YYYY MM DD` (`2024-01-01`, error); `'2024-01-01 10:11:60'` (`2024-01-01 10:12:00`, error).
- [ ] 2.6 Run `cargo fmt --all`, `cargo clippy --all-targets` (zero warnings), and `cargo test --workspace`. Run `make test-e2e` against the Docker stack and report the real result. Measure changed-line coverage with `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info` and `python3 .claude/skills/test-strategy-review/scripts/changed_line_coverage.py <scratchpad>/lcov.info`. It must reach 90%.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: TO_DATE and TO_TIMESTAMP format translation | 1.1-2.6 | — | spec deltas `sql-functions/vs-expression-translator-datetime-format`, `sql-functions/vs-expression-translator-date-fns`; `crates/vs-expression/src/lib.rs`, `crates/vs-expression/src/datetime_format.rs`, `crates/vs-expression/src/datetime_format_tests.rs`, `crates/vs-expression/src/lib_tests.rs`, `crates/lakehouse-engine/tests/e2e_capability_test.rs`, `crates/lakehouse-engine/tests/e2e_join_test.rs`, `docs/semantic-differences.md` |

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
| A declined format returns the native result on every pushdown path | E2E | `crates/lakehouse-engine/tests/e2e_capability_test.rs`, `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_to_date_declined_format_matches_native`, `e2e_join_declined_to_date_format_matches_native` |
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

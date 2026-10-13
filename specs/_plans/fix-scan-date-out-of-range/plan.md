# Plan: fix-scan-date-out-of-range

## Summary

A stored DATE value outside Exasol's DATE range now fails the query with an error that names the output column and the stored day count, instead of coming back as 1970-01-01 (#470). The guard covers the raw-row path and the partial-aggregate path (`MIN`/`MAX` results and `GROUP BY` keys).

## Context

- On staging (Exasol 2025.2.0, lakehouse-engine 0.52.1), an Iceberg table with a Date32 value of 100000000 days (year 275,760) returns 1970-01-01 for `SELECT *`, for `MAX(D)`, and as a `GROUP BY D` group, with no error.
- Aggregate path: the Date32 arm of `arrow_value_at` (`crates/lakehouse-engine/src/scan/convert.rs`) substitutes 1970-01-01 through `.unwrap_or(epoch)` when chrono cannot represent the day count.
- Raw-row path: `ctx.emit_batch` only serializes the batch to Arrow IPC (exasol-udf-sdk 0.30.0 `src/context.rs` L320-339). The SLC converts each Date32 cell, so the raw-path 1970-01-01 originates in the SLC, outside this repo. Task 1.3 reproduces it.
- `coerce_batch_to_exa_types` (`scan/emit.rs`) returns a batch whose types already match the declaration untouched, so a raw-path guard must also run on that branch.
- Group-key columns on the grouped aggregate path are declared `VARCHAR(2000000)` and pass `coerce_partial_agg_columns` uncoerced, as Date32.
- Exasol's DATE range is 0001-01-01 to 9999-12-31. On the local Docker Exasol (2025.1.16), `ADD_DAYS(DATE '9999-12-31', 1)` fails with 22008 (datetime field overflow), and `ADD_DAYS(DATE '0001-01-01', -1)` fails with 22104 (datetime field underflow).
- Arrow counts Date32 days in the proleptic Gregorian calendar, so the range is -719162 to 2932896 days from 1970-01-01. Exasol's own `DAYS_BETWEEN(DATE '0001-01-01', DATE '1970-01-01')` returns -719164, because Exasol counts dates before 1582-10-15 in the Julian calendar (decision [3]).
- Neither format bounds a date. Iceberg spec § Primitive Types: "`date` | Calendar date without timezone or time". Iceberg spec § Appendix A, Parquet: "`date` | `int32` | `DATE` | Stores days from 1970-01-01." Delta PROTOCOL.md § Primitive Types: "date | A calendar date, represented as a year-month-day triple without a timezone." Delta PROTOCOL.md § Delta Data Type to Parquet Type Mappings: "date | `int32` | `DATE`". A date outside Exasol's range is therefore a valid source value that Exasol cannot hold. This is a trade-off forced by an Exasol type limit, recorded in the spec delta's scenario.
- `nan_domain_error` in `convert.rs` is the precedent: it raises `UdfError::User` for an out-of-domain value instead of coercing it.
- The scan emits through three sites only: `emit_one_batch` (`scan/emit.rs`, reached by the raw scan, the broadcast join, and the `COUNT(DISTINCT)` row scan) and the two `ctx.emit` calls in `scan/partial_agg.rs`. The guard sits at this format-neutral boundary, so one direct-storage Parquet fixture proves it for every source format.
- A query that reads such a value without emitting it (a filter, a pushed function such as `YEAR`, `COUNT(*)` under a filter) stays unchecked. `AGENTS.md` accepts that deviation only when `docs/semantic-differences.md` documents it.
- Architecture: no change. The guard sits inside the existing emit boundary of the scan UDF.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-value-conversion | CHANGED | `datafusion-scan/scan-execution-value-conversion/spec.md` |

## Impact

- A query that emits a DATE value outside 0001-01-01 to 9999-12-31 now fails where it used to return 1970-01-01. This is a behavior change for any table holding such a value, and it is intended: the earlier result was wrong data.
- Queries whose emitted dates lie inside the range return the same rows as before.
- A query that tests such a value only in a filter, or passes it to a pushed function, returns the same result as before. `docs/semantic-differences.md` documents it.

## Requirements

| Requirement | Details |
|-------------|---------|
| Cost | The guard makes one pass over each Date32 column of each emitted batch. Non-date columns are not touched |
| Error content | The message carries the output column name, the day count, and the range. It carries no credential or CONNECTION value |

## Implementation Tasks

### 1. Reproduce before any production change

- [ ] 1.1 Add a `date_range` raw Parquet fixture to `write_all_fixtures` in `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs`, at `{BASE_DIRECT}date_range/file1.parquet` through `write_parquet_fixture`, read through `VS_DIRECT` as table `DATE_RANGE`. Columns: `ID` (Int64) and `STORED_DATE` (Date32). Rows as Date32 day counts: 1 = 19723 (2024-01-01), 2 = -719162 (0001-01-01), 3 = 2932896 (9999-12-31), 4 = 100000000, 5 = 2932897 (10000-01-01), 6 = -719163 (one day before 0001-01-01). The column name is distinctive, so an assertion on it cannot match other words of the error text.
- [ ] 1.2 Write the E2E test `out_of_range_date_fails_the_query_on_every_emit_path` in the same file, with one `/// Scenario: A DATE value outside Exasol's DATE range fails the query` line. Use the file's `assert_fails_naming` helper for the failures. Queries over `DIRECT_LAKEHOUSE.DATE_RANGE`:
  - `SELECT ID, STORED_DATE ... WHERE ID IN (1, 2, 3) ORDER BY ID` returns 2024-01-01, 0001-01-01, and 9999-12-31.
  - `SELECT MIN(STORED_DATE), MAX(STORED_DATE) ... WHERE ID IN (1, 2, 3)` returns 0001-01-01 and 9999-12-31.
  - `SELECT ID, STORED_DATE ... WHERE ID = 4` fails naming `STORED_DATE` and `100000000`.
  - `SELECT MAX(STORED_DATE) ...` fails naming `100000000`, and `SELECT MIN(STORED_DATE) ...` fails naming `-719163`. Each also names the partial output column as the pushed `EMITS` clause declares it (read the name from `EXPLAIN VIRTUAL`).
  - `SELECT STORED_DATE, COUNT(*) ... WHERE ID IN (1, 4) GROUP BY STORED_DATE` fails naming `100000000` and the group-key output column (for example `GK_0`).
  - `SELECT ID ... WHERE STORED_DATE > DATE '9999-12-31' ORDER BY ID` returns 4 and 5.
- [ ] 1.3 Build with `make cross-udf-build` and run the test against the unchanged production code. Confirm that the raw, `MIN`/`MAX`, and `GROUP BY` failure assertions fail because 1970-01-01 comes back. On the same build, run `SELECT ID, STORED_DATE FROM DIRECT_LAKEHOUSE.DATE_RANGE WHERE ID IN (5, 6)` and record whether the SLC returns 1970-01-01, fails, or passes the value to Exasol: row 5 is inside chrono's range and row 4 is not, so the pair locates the SLC substitution. Record the result in the PR description. If row 2 does not read back as 0001-01-01, stop and report to the human, because the guard's lower bound depends on it (decision [3]). If a query fails while the adapter plans it (for example in footer-statistics pruning for the `STORED_DATE` filter) rather than at emit, also stop and report: that path is outside this plan's guard.

### 2. Guard

- [ ] 2.1 In `crates/lakehouse-engine/src/scan/convert.rs`, add one shared guard: Exasol's DATE bounds as inclusive Date32 day counts (-719162 and 2932896) and a function that checks one column against them, taking the column and its output column name. A non-Date32 column passes, and null cells are skipped. The first out-of-range value fails the call with `UdfError::User`, modeled on `nan_domain_error`. The message names the column and the day count and states the range, for example `DATE value out of range in output column STORED_DATE: 100000000 days from 1970-01-01 lies outside Exasol's DATE range 0001-01-01 to 9999-12-31`. Replace both `.unwrap_or(epoch)` fallbacks in the Date32 arm of `arrow_value_at` with an error from the same message builder, so no conversion substitutes 1970-01-01.
- [ ] 2.2 Add one table-driven unit test in `crates/lakehouse-engine/src/scan/convert_tests.rs`: -719162 and 2932896 pass, -719163 and 2932897 fail, the failure message holds the column name and the day count, a null cell and a non-Date32 column pass, and `arrow_value_at` on Date32 100000000 returns `Err`.
- [ ] 2.3 Raw-row path: in `coerce_batch_to_exa_types` (`crates/lakehouse-engine/src/scan/emit.rs`), run the guard on every output column that leaves as Date32, naming `declared[i].name`. It runs on both branches: before the early `return Ok(batch)` of a batch whose types already match, and on the coerced columns.
- [ ] 2.4 Aggregate path: in `coerce_partial_agg_columns` (`crates/lakehouse-engine/src/scan/partial_agg.rs`), run the guard on every column it returns, naming `declared[i].name`. This includes the group-key columns it returns uncoerced (index below `first_agg_column`). Both `partial_row_from_batch` (single group) and `run_grouped_partial_aggregate` (grouped) call it.
- [ ] 2.5 Rebuild with `make cross-udf-build` and run the E2E test from task 1.2 until it passes.

### 3. Documentation

- [ ] 3.1 Add a section "Dates outside Exasol's DATE range" to `docs/semantic-differences.md`. It states that a query emitting such a date fails and names the column, and that native Exasol cannot store such a date at all. It lists what a query that does not emit the value returns, with results observed live on the fixed build against `DATE_RANGE` row 4: `SELECT ID FROM ... WHERE STORED_DATE > DATE '9999-12-31'`, `SELECT ID, YEAR(STORED_DATE) FROM ... WHERE ID = 4`, and `SELECT COUNT(*) FROM ... WHERE STORED_DATE > DATE '9999-12-31'`. Reword the page's opening sentence ("A few pushed-down functions return a different result ...") so it also covers this filter case. Write down only results observed live.

### 4. Verification and delivery

- [ ] 4.1 Run `cargo fmt --all`, `cargo clippy --all-targets` (zero warnings), and `cargo test`.
- [ ] 4.2 Run `make test-e2e` against the local Docker Exasol. Every suite passes.
- [ ] 4.3 Measure changed-line coverage with `cargo llvm-cov --workspace --lcov`. If the changed lines fall below 90%, add one unit test for the uncovered guard call site in `emit_tests.rs` or `partial_agg_tests.rs`, and nothing more.
- [ ] 4.4 The implementing commit carries `Closes #470`.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: DATE range guard at the emit boundary | 1.1-4.4 | none | spec delta `datafusion-scan/scan-execution-value-conversion`; `crates/lakehouse-engine/src/scan/convert.rs`, `scan/emit.rs`, `scan/partial_agg.rs`, `scan/convert_tests.rs`, `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs`, `docs/semantic-differences.md` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Fallback | `crates/lakehouse-engine/src/scan/convert.rs` `arrow_value_at`, Date32 arm, both `.unwrap_or(epoch)` calls | Replaced by the shared out-of-range error |

## Open Questions

1. On the aggregate path, the error names the partial output column the adapter declares (for example `PARTIAL_max_0` or `GK_0`), not the source column the user typed (decision [2]). Confirm this is acceptable.
2. If task 1.3 shows that the SLC substitutes 1970-01-01 on the raw path, the SLC (`language-container-rs`) needs its own issue (#TBD), opened by a human.
3. A TIMESTAMP value outside Exasol's TIMESTAMP range may have the same defect. This plan does not check or change it (#TBD).

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A DATE value outside Exasol's DATE range fails the query | E2E (local Docker Exasol) | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `out_of_range_date_fails_the_query_on_every_emit_path` |
| (guard bounds and message, supporting unit test) | Unit | `crates/lakehouse-engine/src/scan/convert_tests.rs` | the table-driven guard test from task 2.2 |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| datafusion-scan/scan-execution-value-conversion | `exapump sql "SELECT ID, STORED_DATE FROM DIRECT_LAKEHOUSE.DATE_RANGE WHERE ID = 4" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | Error naming `STORED_DATE` and `100000000`, no row |
| datafusion-scan/scan-execution-value-conversion | `exapump sql "SELECT STORED_DATE, COUNT(*) FROM DIRECT_LAKEHOUSE.DATE_RANGE WHERE ID IN (1, 4) GROUP BY STORED_DATE" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | Error naming the group-key column and `100000000`, no 1970-01-01 group |
| datafusion-scan/scan-execution-value-conversion | `exapump sql "SELECT ID, STORED_DATE FROM DIRECT_LAKEHOUSE.DATE_RANGE WHERE ID IN (1, 2, 3) ORDER BY ID" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | Three rows: 2024-01-01, 0001-01-01, 9999-12-31 |

Run these after the direct-storage suite has created `DIRECT_LAKEHOUSE` and the `DATE_RANGE` fixture. The port is the Makefile default `LH_EXASOL_PORT` (28563). Use the overridden value if the local stack maps Exasol elsewhere.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |

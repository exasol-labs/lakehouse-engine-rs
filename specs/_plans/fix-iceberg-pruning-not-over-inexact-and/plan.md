# Plan: fix-iceberg-pruning-not-over-inexact-and

## Summary

This plan fixes #466: Iceberg plan-time pruning negates an `AND` it only partly translated, so it skips files that hold matching rows and returns too few rows without an error. One format-neutral rule set that tracks exactness now governs both the Iceberg and the Delta translator, and one E2E case table over one shared fixture proves pruning against a native Exasol oracle on Iceberg and on direct storage.

## Context

- `to_iceberg_predicate` (`crates/lakehouse-engine/src/adapter/iceberg_predicate.rs`) drops an untranslatable child of an `AND` and then negates the result under `NOT`. `NOT (k < 30 AND name LIKE 'x%')` becomes `k >= 30`, which skips every file with `k < 30`. On staging (Exasol 2025.2.0, engine 0.52.0), `SELECT COUNT(*) FROM TPCH_SF100.NATION WHERE NOT (N_NATIONKEY < 30 AND N_NAME LIKE 'x%')` returns 0 through the virtual schema and 25 natively, and `EXPLAIN VIRTUAL` shows the typed empty result `SELECT CAST(0 AS DECIMAL(18,0)) FROM DUAL`. The code is unchanged on main at `b703676`.
- The Iceberg `translate_between` keeps one bound when the other does not convert, and the result is negated under `NOT` as if it were exact.
- The Delta translator (`adapter/pushdown/format/delta_predicate.rs`) already tracks `Translated { predicate, exact }`, negates only an exact child, and marks a `BETWEEN` with a dropped bound inexact through its `fold_and`. The same query on the Delta copy returns 25.
- The direct-storage partition predicate (`adapter/pushdown/format/partition_predicate.rs`) evaluates reachable SQL truth values per file and is sound under `NOT` (decision [4]).
- The Iceberg leaf conversion rounds a `float` literal to `f32` and truncates `timestamp` and `timestamptz` literals to microseconds, including for nanosecond columns. A rounded leaf under `NOT` reintroduces the same defect (decision [3]).
- Apache Iceberg table spec, § "Scan Planning": "Scan predicates are converted to partition predicates using an _inclusive projection_: if a scan predicate matches a row, then the partition predicate must match that row’s partition." and "Scan predicates are also used to filter data and delete files using column bounds and counts that are stored by field id in manifests." The defect violates both. The `pushdown-file-pruning` delta quotes them and fixes the deviation, with no scoped exception.
- ADR `sound-partial-iceberg-predicate-translation-strict-or-not-handling` states the Iceberg rules for `AND`, `OR`, and `NOT` of an untranslatable child and leaves `NOT` over a partly translated child undefined. Decision [1] supersedes it.
- A filter applies at five levels: partition pruning, statistics pruning, Parquet row-group and page pruning in the scan UDF, the DataFusion row filter, and the adapter's outer `WHERE` for a predicate the DataFusion dialect declines. A translator unit test alone does not cover the defect class, so the E2E case table checks every level (decision [6]).
- CI measures unit-test coverage in the `unit-tests` job with `cargo llvm-cov --workspace --lcov --output-path lcov-unit.info`, the same command as `make coverage`. The `sonar` job uploads that report to SonarQube Cloud, whose quality gate judges coverage on new code. That gate is the only changed-line coverage check, and no tracked tool computes changed-line coverage locally.
- The plan changes no component, boundary, interface, or data flow, so it carries no architecture delta.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| pushdown-file-pruning | CHANGED | `specs/_plans/fix-iceberg-pruning-not-over-inexact-and/file-planning/pushdown-file-pruning/spec.md` |
| delta-file-pruning | CHANGED | `specs/_plans/fix-iceberg-pruning-not-over-inexact-and/delta/delta-file-pruning/spec.md` |

## Impact

- An Iceberg query whose filter puts `NOT` over a partly translated `AND`, `OR`, or `BETWEEN` returns every matching row. Before the fix it could return too few rows, or none, with no error.
- Such a query now scans the files it wrongly skipped before. This is the correct cost and no tuning applies.
- An Iceberg comparison against a literal that the column type cannot hold exactly (a `float` against `0.7`, a microsecond `timestamp` or `timestamptz` against a literal with seven fraction digits) no longer prunes files.
- Delta and direct-storage results do not change.
- `make test-e2e` gains the binary `e2e_pruning_test`.
- Breaking: none.

## Dependencies

- Fixes #466. The implementing commit references `Closes #466`.

## Implementation Tasks

### Group A: Exactness rules and the pruning case table

Run the tasks in order. Task 1.1 reproduces the defect before any code changes. Tasks 1.2 to 1.6 build the E2E case table and run it before the fix, so it fails first. Tasks 1.7 to 1.9 fix the translators.

- [ ] 1.1 Reproduce #466 on the local Docker stack before you change any code. Build with `make cross-udf-build`, then run, from the repository root:
  - `scripts/capture-pushdown-payload.sh 'SELECT COUNT(*) FROM {table} WHERE NOT (ID < 1000 AND C_VARCHAR LIKE '"'"'x%'"'"')'`
  - `scripts/capture-pushdown-payload.sh 'SELECT COUNT(*) FROM {table} WHERE C_VARCHAR NOT LIKE '"'"'x%'"'"''`

  Every seeded `ID` of `typed_distinct_probe` is below 1000, so both predicates select the same rows (decision [7]). Expected on main: the first returns 0 and its `EXPLAIN VIRTUAL` is the typed empty result with no `LAKEHOUSE_SCAN`. The second returns a non-zero count and scans. Record both outputs for the verification report. If the first count already equals the second, stop and report that #466 does not reproduce locally.
- [ ] 1.2 Write the shared pruning fixture. Create `crates/lakehouse-engine/tests/common/pruning_fixture.rs` and declare it in `tests/common/mod.rs`. It holds the rows, the file labels, the Arrow batch builder, the Parquet writer properties, and the oracle DDL and `INSERT` text, so the Iceberg seed, the raw-Parquet writer, and the oracle read one definition.
  - Columns: `ID` (long, unique), `P` (string, the partition column), `K` (long, statistics only), `S` (string, the `LIKE` target), `X` (double, the `ABS(X) > 0.1` target), `TS` (timestamp in microseconds, the `SECOND(TS, 3) > 1` target). Timestamps are exact to the millisecond. No string value is empty, because Exasol reads an empty string as NULL.
  - Writer properties, defined once and used by both writers: `set_max_row_group_row_count(Some(4))`, `set_data_page_row_count_limit(2)`, `set_write_batch_size(2)` (the page limit is checked only between write batches), `set_dictionary_enabled(false)`, and `set_statistics_enabled(EnabledStatistics::Page)`, so every row group of more than two rows holds several data pages and the file carries a column index and an offset index. Do not use the deprecated `set_max_row_group_size`.
  - Six files, two per partition value `'a'`, `'b'`, and NULL. Suggested labels and `K` values, with `|` between pages and `||` between row groups:
    - `a1`: `10, 12 | 18, 20`, range [10, 20].
    - `a2`: `15, 18 | 35, 40 || NULL, 22`, range [15, 40], overlapping `a1`.
    - `b1`: `5, 8 | 12, 14 || 30, 35 | 48, 50`, range [5, 50].
    - `b2`: `K` NULL in all three rows.
    - `n1`: `5, 7 | 33, 35 || 24, 25`, range [5, 35].
    - `n2`: `45, 60`, range [45, 60], disjoint from `a1`.
  - Each level below the file must be able to drop a matching row under a wrong evaluation, so the row assertion proves it:
    - Row group: `b1`'s first row group holds only `K` below 30, while the file's range reaches 50. It holds a row whose `S` does not match `'x%'`, so a wrong `K >= 30` for the #466 case would skip that row group and its matching row.
    - Page: the first row group of `a2` and of `n1` spans 30, so it survives a wrong `K >= 30`, while its first page holds only `K` below 30 and a row whose `S` does not match `'x%'`. A wrong evaluation on that page's min/max would skip the page and its matching row.
  - `S`, `X`, and `TS` each hold a NULL in some file and both a true and a false outcome of their untranslatable predicate across the fixture.
- [ ] 1.3 Write the two fixture writers.
  - In `crates/lakehouse-engine/tests/common/seed.rs`, add `seed_pruning_cases`, following `seed_partitioned`: namespace `e2e_pruning`, table `pruning_cases`, an identity partition on `P` (partition field id 1000), and one data file per label, written through `ParquetWriterBuilder` with the fixture's writer properties. The NULL partition's files use a partition key whose value is NULL (`Struct::from_iter([None])`). The `DefaultFileNameGenerator` prefix carries the label (for example `pc_a1`), so the label appears in each data-file path. Seeding is idempotent: an existing snapshot skips it.
  - In `crates/lakehouse-engine/tests/common/raw_parquet.rs`, let a caller pass `WriterProperties` to the Arrow writer, and keep `encode_parquet` and `write_parquet_fixture` working unchanged for their current callers. The direct-storage copy goes under `s3://warehouse/direct_pruning/pruning_cases/p=a/a1.parquet` and so on, with `p=__HIVE_DEFAULT_PARTITION__/` for the NULL partition, and the files omit `P`.
  - The oracle is a native table `PRUNING_ORACLE.CASES` with the same columns plus `FILE_LABEL VARCHAR(10)`, rebuilt with `CREATE OR REPLACE TABLE` on every run.
- [ ] 1.4 Create `crates/lakehouse-engine/tests/e2e_pruning_test.rs` (`#![cfg(feature = "exasol-e2e")]`) with its setup and a fixture-shape test, and add `--test e2e_pruning_test` to the `test-e2e` recipe line in the `Makefile`.
  - One `OnceLock` setup waits for the stack, seeds the Iceberg table, writes the raw-Parquet files, loads the oracle, installs the scripts, and creates two virtual schemas through `e2e_harness`: `PRUNING_ICEBERG` over namespace `e2e_pruning`, and `PRUNING_HIVE` with `CATALOG_KIND = 'DIRECT_STORAGE'` over `s3://warehouse/direct_pruning/` (the CONNECTION shape of `direct_storage_password` in `e2e_direct_storage_test.rs`). The connection stays uncapped, so `EXPLAIN VIRTUAL` shows the real plan.
  - `pruning_fixture_files_hold_the_row_groups_and_pages_the_cases_need` reads each written file's own footer, for the Iceberg data files (paths from `resolve_fixture_files("e2e_pruning", "pruning_cases")`) and for the raw-Parquet objects, with the page index loaded (`ReadOptionsBuilder::new().with_page_index()`). It asserts that `a2`, `b1`, and `n1` hold two or more row groups, that the column index and the offset index are present, that the `K` column chunk of the first row group of `a2` and of `n1` holds two or more data pages, and that its first page's `K` maximum is below 30 while the row group's `K` maximum is at least 30. Each failure message says that the fixture cannot prove page or row-group pruning (`specs/testing.md` § Fixtures and § Correctness oracles).
  - Run `cargo test --features exasol-e2e --test e2e_pruning_test -- --test-threads=1`. The fixture-shape test passes.
- [ ] 1.5 Write the plan-reading and oracle helpers in `e2e_pruning_test.rs`:
  - the fixture labels named in an `explain_virtual_sql` text (the Iceberg prefix `pc_<label>-` and the raw-Parquet key `/<label>.parquet`);
  - whether that text names `LAKEHOUSE_SCAN`;
  - the placement marker: `"filter":"` for the scan, or `LHS_T0` without `"filter":"` for the outer `WHERE`;
  - the sorted `ID` rows of a query, and the labels of `SELECT DISTINCT FILE_LABEL FROM PRUNING_ORACLE.CASES WHERE <p>`.
- [ ] 1.6 Write the case matrix and its two test functions, then run them before the fix.
  - One case table, one row per case: the predicate text, the placement (scan or outer `WHERE`), whether to also run `COUNT(*)`, and per format an optional expected label set. A case carries an expected set for a format only when its predicate translates fully for that format. A partly translated predicate carries none, because only soundness is part of the contract for it (`pushdown-file-pruning` Background).
  - Derive each expected set from the rules in the `pushdown-file-pruning` Background, Iceberg's inclusive evaluation, and the direct-storage three-valued rule. Iceberg evaluates the whole predicate twice: once on partition values, where a clause over a non-partition column counts as true, and once on per-file statistics. In iceberg-rust 0.10, the partition evaluator keeps a NULL partition for `!=` and `NOT IN`, the statistics evaluator answers "might match" for every `!=` and `NOT IN`, and a column that holds only NULLs fails `=`, `<`, `<=`, `>`, `>=`, and `IS NOT NULL`. For direct storage, a predicate translates fully only when it names partition columns alone, with literals that convert. Worked anchors for the suggested values:
    - `NOT (K < 30 AND S LIKE 'x%')`: partly translated on both formats, so no expected set. Only Sound applies.
    - `NOT (K >= 10 AND K <= 20)`: Iceberg keeps `a2`, `b1`, `n1`, and `n2`. Direct storage has no expected set.
    - `NOT (K < 30 AND P = 'a')`: Iceberg keeps all six, because its negation `K >= 30 OR P != 'a'` meets "might match" on `!=`. Direct storage has no expected set.
    - `NOT (K IS NOT NULL)`: Iceberg keeps `a2` and `b2`. Direct storage has no expected set.
    - `NOT (K <= 1000)`: Iceberg keeps none. Direct storage has no expected set.
    - `NOT (P IS NOT NULL)`: both keep `n1` and `n2`.
    - `NOT (P IN ('a', 'b') OR P IS NULL)`: both keep none.

    A live result that differs from a derived set is a finding to explain, never a value to copy into the table.
  - The cases cover every shape on `P`, on `K`, and on a mix of both. Shapes: `NOT (a AND u)` and `NOT (u AND a)` (the #466 rows, which also run `COUNT(*)`), `NOT (a AND b)` with both translatable, `NOT (a OR u)`, `NOT (u OR b)`, `NOT (NOT (a AND u))`, `(a AND u) OR b`, `NOT ((a AND u) OR b)`, and `NOT` over `IN`, `BETWEEN`, `IS NULL`, and `IS NOT NULL`. Here `a` and `b` translate and `u` does not. Across the table, `u` is a `LIKE` on `S`, `ABS(X) > 0.1`, and `SECOND(TS, 3) > 1`, which the DataFusion dialect declines. Add one `BETWEEN` with a bound that does not translate, for example `NOT (K BETWEEN 10 AND 20.5)`. If `EXPLAIN VIRTUAL` shows that Exasol never sends such a bound, drop the row and record that unit tests alone prove the case (`specs/testing.md` § Coverage rule).
  - For each case and each virtual schema, assert, with the predicate text in each failure message:
    1. The rows of `SELECT ID FROM <vs>.PRUNING_CASES WHERE <p> ORDER BY ID` equal the rows of the same query over the oracle. A `COUNT(*)` row also compares counts.
    2. Sound: the labels named in `EXPLAIN VIRTUAL` include the oracle's `FILE_LABEL` set for the predicate.
    3. Effective, only for a case with an expected set: the labels named equal the expected set.
    4. The plan names `LAKEHOUSE_SCAN` exactly when it names at least one label. For a case whose expected set is empty, the plan therefore takes the empty-result route.
    5. When the plan names a label, the placement marker holds. Read each marker from the live `EXPLAIN VIRTUAL` text before you pin it.
  - For each `NOT` case, read the scan spec's `filter` text, or the wrapper's `WHERE` for a declined case. Both render the filter tree Exasol sent. Confirm that the `NOT` reached the adapter. If Exasol rewrote a case's shape, for example by De Morgan, replace the case with one that keeps the shape.
  - Two test functions share the table. `iceberg_pruning_keeps_every_file_with_a_matching_row` carries `/// Scenario: Pruning keeps every file with a matching row for every filter shape`, `/// Scenario: A NOT over a partly translated predicate keeps every file with a matching row`, and `/// Scenario: A NOT over a fully translated predicate still prunes`. `hive_partition_pruning_keeps_every_file_with_a_matching_row` carries `/// Scenario: A predicate on partition columns prunes files before their footers are read`.
  - Run `cargo test --features exasol-e2e --test e2e_pruning_test -- --test-threads=1` before the fix. Expected: the Iceberg function fails on the #466 rows and on the other rows that put `NOT` over a partly translated node, and the direct-storage function and the fixture-shape test pass. Record the failing predicates. A failing direct-storage row is a new defect: stop and report it.
- [ ] 1.7 Write the shared rule set in `crates/lakehouse-engine/src/adapter/pruning_exactness.rs`, declared `mod pruning_exactness;` in `adapter/mod.rs`, with unit tests in `adapter/pruning_exactness_tests.rs` (decisions [1], [2], [5]). Write the tests first. [expert]
  - `Translated<P> { predicate, exact }` with an exact constructor.
  - A trait each format implements: an n-ary `AND` and an n-ary `OR` that each take a first element plus the rest, and a negation. An empty junction is unrepresentable.
  - A walk over `predicate_and`, `predicate_or`, and `predicate_not` that calls a format-supplied leaf function for every other node type, plus the conjunction and disjunction folds that a leaf reuses for `BETWEEN` and the Delta `IN` list.
  - Rules: `AND` drops a `None` child and is then inexact, and it is `None` when no child survives. `OR` is `None` when any branch is `None` or the list is empty, and exact only when every branch is exact. `NOT` of an exact child is its negation, exact. `NOT` of an inexact or `None` child is `None`. A node without its `expression` or `expressions` field is `None`.
  - The module doc comment states the why in at most two lines: negating a widened predicate narrows it.
  - Unit tests use a small test-local predicate type (for example a rendered `String`), one rule per test, each with an input that fails under the wrong rule: `NOT (a AND u)` and `NOT (u AND a)` give `None`; `NOT (a AND b)` gives the exact negation; `NOT (a OR u)` gives `None`; `NOT (NOT (a AND u))` gives `None`; `(a AND u) OR b` gives an inexact `a OR b`; `NOT` over that gives `None`; an empty `AND` and an empty `OR` give `None`.
- [ ] 1.8 Move the Iceberg translator onto the shared rule set (decisions [1], [3]). In `adapter/iceberg_predicate.rs`:
  - `to_iceberg_predicate` keeps its `pub` signature and returns only the predicate. The leaf function covers comparisons, `IN`, `BETWEEN`, `IS NULL`, and `IS NOT NULL`. `translate_between` builds its two bounds through the shared conjunction fold, so a dropped bound makes it inexact. Implement the trait for `iceberg::expr::Predicate` with `and`, `or`, and `negate`.
  - `literal_to_datum` drops a `float` literal unless `f64::from(value as f32) == value`. In both `parse_timestamp_no_tz` and `parse_timestamptz`, a literal whose fraction has more digits than the column's unit holds yields no datum: more than six for `Timestamp` and `Timestamptz`, more than nine for `TimestampNs` and `TimestamptzNs`. The fraction ends at the offset, so `Z`, `+HH:MM`, and `-HH:MM` never count as fraction digits. A nanosecond column converts the literal at nanosecond precision instead of through microseconds. A `timestamptz` literal with an offset converts to its UTC instant at the column's own precision.
  - Update the module doc comment to the current rule, in at most two lines.
  - Keep every existing test in `iceberg_predicate_tests.rs` unchanged. `between_with_one_failing_bound_keeps_other` and `not_of_translatable_negates` still hold. Add a `float` field and four timestamp fields (`timestamp`, `timestamp_ns`, `timestamptz`, `timestamptz_ns`) to `test_schema`, and add:
    - `not_over_an_and_that_dropped_a_conjunct_returns_none` (the #466 shape) and `not_over_a_between_that_dropped_a_bound_returns_none`;
    - `float_literal_that_f32_cannot_hold_returns_none` (`0.7`) and `float_literal_that_f32_holds_translates` (`0.5`);
    - `timestamp_literal_finer_than_the_column_unit_returns_none`: `2024-03-01 10:00:00.1234567` (`literal_timestamp`) against `timestamp` gives `None`;
    - `nanosecond_timestamp_literal_translates_at_nanosecond_precision`: `2024-03-01 10:00:00.123456789` against `timestamp_ns` gives the nanosecond value `1709287200123456789`;
    - `timestamptz_literal_with_an_offset_converts_to_the_utc_instant`: `2024-03-01 12:30:00.123456+02:30`, `2024-03-01 10:00:00.123456Z`, and `2024-03-01 10:00:00.123456` (`literal_timestamp_utc`) against `timestamptz` each give the microsecond value `1709287200123456`;
    - `timestamptz_literal_finer_than_the_column_unit_returns_none`: `2024-03-01 12:30:00.1234567+02:30`, `2024-03-01 10:00:00.1234567Z`, and `2024-03-01 10:00:00.1234567` against `timestamptz` each give `None`;
    - `nanosecond_timestamptz_literal_with_an_offset_converts_to_the_utc_instant`: `2024-03-01 12:30:00.123456789+02:30` and `2024-03-01 10:00:00.123456789Z` against `timestamptz_ns` each give the nanosecond value `1709287200123456789`;
    - `nanosecond_timestamptz_literal_finer_than_the_column_unit_returns_none`: `2024-03-01 12:30:00.1234567891+02:30` and `2024-03-01 10:00:00.1234567891Z` against `timestamptz_ns` each give `None`.

    `2024-03-01T10:00:00Z` is epoch second `1709287200`. Each `timestamptz` case runs both the `+02:30` and the `Z` form, so the offset branch of `has_tz_offset` and `parse_timestamptz` runs for both units. These eight literal tests carry `/// Scenario: A literal the column type cannot hold exactly imposes no constraint`.
- [ ] 1.9 Move the Delta translator onto the shared rule set with no behavior change (decisions [1], [4]). In `adapter/pushdown/format/delta_predicate.rs`, delete `Translated`, `impl Translated`, `fold_and`, `fold_or`, and the `predicate_not` arm. Implement the trait for `delta_kernel::Predicate` with `Predicate::and_from`, `Predicate::or_from`, and `Predicate::not`. `translate_in` and `translate_between` use the shared folds.
  - Every test in `delta_predicate_tests.rs` keeps its name and expected value. Only call sites of a moved or renamed fold change (`specs/testing.md` § Regression guards).
  - Add `/// Scenario: A NOT negates only a fully translated child` to `not_over_an_and_that_dropped_a_conjunct_returns_none`, `not_over_a_between_that_dropped_a_bound_returns_none`, and `not_over_a_fully_translatable_and_negates_the_whole_conjunction`.
- [ ] 1.10 Update `specs/testing.md` § Fixtures with one bullet for the pruning fixture: one Iceberg table and one direct-storage directory holding the same rows, a native oracle table with a `FILE_LABEL` column, file paths that carry the label, writer properties that give several row groups and several data pages per row group with a page index, the fixture-shape test that checks them, and the case table that runs on both. Extend the raw-Parquet bullet to say that a caller may pass writer properties.
- [ ] 1.11 Verify.
  - Start the Docker stack yourself and run `make test-e2e`. Every test passes and none is ignored, including the three functions of `e2e_pruning_test`.
  - Re-run the two task 1.1 commands. The first count now equals the second, and `EXPLAIN VIRTUAL` names `LAKEHOUSE_SCAN`.
  - Run `cargo test --workspace`, `cargo clippy --all-targets -- -D warnings`, `cargo clippy --all-targets --features exasol-e2e -- -D warnings`, and `cargo fmt --all -- --check`.
  - Changed-line coverage has one gate: SonarQube Cloud's quality gate on new code. CI's `unit-tests` job builds `lcov-unit.info` with `cargo llvm-cov --workspace --lcov --output-path lcov-unit.info`, and the `sonar` job judges the PR's new lines against it. The repository holds no tracked tool that checks changed lines locally, and this plan adds none. After the push, the PR's `Sonar Analysis` check passes its quality gate. If it fails on coverage, add unit tests for the lines Sonar reports as uncovered and push again.

Test budget: about 150 lines of unit tests (about 70 for the shared rule set, about 80 for the Iceberg translator) and about 420 lines of E2E code (about 170 for the fixture and its writers, about 50 for the fixture-shape test, about 200 for the helpers, the case table, and its assertions), against about 100 added and 60 removed production lines.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Exactness rules and the pruning case table | 1.1-1.11 | none | spec deltas `file-planning/pushdown-file-pruning` and `delta/delta-file-pruning`; decision-log [1]-[9] and Review Findings [1]-[7]; `crates/lakehouse-engine/src/adapter/{mod.rs,iceberg_predicate.rs,iceberg_predicate_tests.rs,pruning_exactness.rs,pruning_exactness_tests.rs}`, `crates/lakehouse-engine/src/adapter/pushdown/format/{delta_predicate.rs,delta_predicate_tests.rs}`; `crates/lakehouse-engine/tests/e2e_pruning_test.rs`, `crates/lakehouse-engine/tests/common/{pruning_fixture.rs,seed.rs,raw_parquet.rs,mod.rs}`; `Makefile`; `specs/testing.md`; read-only `crates/lakehouse-engine/src/adapter/pushdown/format/partition_predicate.rs`, `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs`, `crates/lakehouse-engine/tests/common/e2e_harness.rs`, `.github/workflows/ci.yml` (`unit-tests` and `sonar` jobs) |

One group, because the fixture's expected file sets and the rule set rest on the same exactness rules, and both spec deltas govern the same translators.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/lakehouse-engine/src/adapter/iceberg_predicate.rs`: `fold_and`, `fold_or` | Replaced by the shared folds in `adapter/pruning_exactness.rs` |
| Struct and impl | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate.rs`: `Translated`, `impl Translated` | Moved to `adapter/pruning_exactness.rs` |
| Function | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate.rs`: `fold_and`, `fold_or` | Moved to `adapter/pruning_exactness.rs` |
| Match arms | `predicate_and`, `predicate_or`, and `predicate_not` arms of `to_iceberg_predicate` and `translate_node` | The shared walk owns them |

## Open Questions

- A writable Delta fixture that runs the same case table is a follow-up `(#TBD)`. No issue exists yet, and planning opens none (decision [9]).

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| pushdown-file-pruning: A NOT over a partly translated predicate keeps every file with a matching row (NEW) | Integration (E2E) + Unit | `crates/lakehouse-engine/tests/e2e_pruning_test.rs`; `crates/lakehouse-engine/src/adapter/iceberg_predicate_tests.rs` | `iceberg_pruning_keeps_every_file_with_a_matching_row`; `not_over_an_and_that_dropped_a_conjunct_returns_none` |
| pushdown-file-pruning: A NOT over a fully translated predicate still prunes (NEW) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_pruning_test.rs` | `iceberg_pruning_keeps_every_file_with_a_matching_row` |
| pushdown-file-pruning: A literal the column type cannot hold exactly imposes no constraint (NEW) | Unit | `crates/lakehouse-engine/src/adapter/iceberg_predicate_tests.rs` | `float_literal_that_f32_cannot_hold_returns_none`, `float_literal_that_f32_holds_translates`, `timestamp_literal_finer_than_the_column_unit_returns_none`, `nanosecond_timestamp_literal_translates_at_nanosecond_precision`, `timestamptz_literal_with_an_offset_converts_to_the_utc_instant`, `timestamptz_literal_finer_than_the_column_unit_returns_none`, `nanosecond_timestamptz_literal_with_an_offset_converts_to_the_utc_instant`, `nanosecond_timestamptz_literal_finer_than_the_column_unit_returns_none` |
| pushdown-file-pruning: Pruning keeps every file with a matching row for every filter shape (NEW) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_pruning_test.rs` | `iceberg_pruning_keeps_every_file_with_a_matching_row`, `pruning_fixture_files_hold_the_row_groups_and_pages_the_cases_need` |
| delta-file-pruning: A NOT negates only a fully translated child (NEW) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate_tests.rs` | `not_over_an_and_that_dropped_a_conjunct_returns_none`, `not_over_a_between_that_dropped_a_bound_returns_none`, `not_over_a_fully_translatable_and_negates_the_whole_conjunction` |
| direct-storage-hive-partitioning: A predicate on partition columns prunes files before their footers are read (recorded, unchanged; new live cases) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_pruning_test.rs` | `hive_partition_pruning_keeps_every_file_with_a_matching_row` |
| Shared exactness rules (Background rules, decision [2]) | Unit | `crates/lakehouse-engine/src/adapter/pruning_exactness_tests.rs` | one test per rule listed in task 1.7 |

The literal-conversion and Delta `NOT` scenarios are pure computation over a synthesized filter, so unit tests prove them (`specs/testing.md` § Coverage rule). The `timestamptz` arm is reachable only under the node name `literal_timestamp_utc`, which no live Exasol request carries (#242), so its tests use a synthesized request.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| pushdown-file-pruning | `scripts/capture-pushdown-payload.sh 'SELECT COUNT(*) FROM {table} WHERE NOT (ID < 1000 AND C_VARCHAR LIKE '"'"'x%'"'"')'` | `EXPLAIN VIRTUAL` names `LAKEHOUSE_SCAN` and is not the typed empty result, and the count equals the count of the next command |
| pushdown-file-pruning | `scripts/capture-pushdown-payload.sh 'SELECT COUNT(*) FROM {table} WHERE C_VARCHAR NOT LIKE '"'"'x%'"'"''` | A non-zero count, the reference for the previous command |
| pushdown-file-pruning, delta-file-pruning | `make test-e2e` | Every test passes, including the three functions of `e2e_pruning_test`, and none is ignored |
| delta-file-pruning | `cargo test -p lakehouse-engine delta_predicate` | Every Delta translator test passes with unchanged names and expected values |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make test-e2e` | 0 failures, 0 ignored |
| Lint | `cargo clippy --all-targets -- -D warnings` | 0 warnings |
| Lint (E2E) | `cargo clippy --all-targets --features exasol-e2e -- -D warnings` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| Coverage (changed lines) | The PR's `Sonar Analysis` check, fed by CI's `cargo llvm-cov --workspace --lcov --output-path lcov-unit.info` | Quality gate passed on new code. This is the only changed-line coverage gate |
| Plan | `speq plan validate fix-iceberg-pruning-not-over-inexact-and` | Pass |

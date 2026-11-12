# Plan: add-export-iceberg-append

## Summary

`EXPORT ... INTO SCRIPT` appends to an Iceberg table that already exists. It binds every table column to a source column by name with widening, and refuses every mismatch in one error before it writes any data (#567). The plan files are new and touch no stacked branch. The implementation edits the stacked files that § Dependencies lists, so it rebases after each fix to the stacked PRs #607 to #610.

## Context

- On the stack top (#610, branch `docs/export-iceberg-lakekeeper-e2e-and-specs`) the export creates only new tables. `IcebergRestTarget::prepare` calls `TableWriteSession::refuse_existing_table` before any source row is read.
- Tables that other engines create have lower-case column names, `int` and `long` columns, required columns, and evolved schemas and partition specs. Exasol has no integer type and reports an unquoted identifier in upper case.
- Exasol passes the export's pre-read hook only the source column names. Each writer sees the column types.
- Each writer commits on its own, and its commit loop re-checks the writer's tag. The append reuses both unchanged and conforms to the ADRs `export-has-no-final-commit-per-writer-commits` and `export-commit-loop-rechecks-writer-tag`. Its only new catalog request is a load, which is safe to repeat (ADR `state-changing-catalog-requests-retried-by-their-operation`).
- iceberg-rust's fast append rejects a data file whose partition spec id differs from the table's default spec id. The sink writes spec id 0 today.
- A writer retries a conflicting commit as often as the table's `commit.retry.num-retries` allows: 4 when the table does not set it, 20 on a table that an export created.
- The stacked PRs are open and may still change. This plan restates none of their spec or docs text now. The architecture delta copies the stack top's sections, and task 5.1 copies them again from main.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| export-iceberg-append | NEW | `export/export-iceberg-append/spec.md` |
| export-iceberg-table | CHANGED, written at record time by task 5.1 | `export/export-iceberg-table/spec.md` |
| export-into-script | CHANGED, written at record time by task 5.1 | `export/export-into-script/spec.md` |

Architecture: `architecture.md` (CHANGED Components, Data Flow, Interfaces, Constraints).

## Impact

- Breaking: an export whose `TABLE_NAME` names an existing table no longer fails. It appends the rows. A job that relied on the refusal to prevent a second load must check for the table itself.
- An append needs a source column for every table column. A table with a column the export cannot write, such as `float`, a struct, or `timestamptz`, takes no append.
- A create-mode export that loses the race for a new table to another statement fails with an error that no longer says the export only creates new tables.
- Security: a holder of the export role can append to any table that the export accepts and that the CONNECTION's catalog identity may commit to, including tables that other engines own. `docs/security.md` states this.
- `docs/export.md` gains a section on appending to an existing table.

## Out of Scope

- Iceberg format versions 1 and 3. The export refuses such a table before it reads any source row. The requester agreed to format version 2 only (decision-log.md § Interview).
- Partitioned tables and changes to the table schema, as #567 § Out of scope states.

## Dependencies

- Stack #607 → #608 → #609 → #610. The plan and the implementation branch start from #610's branch. Group A rebases onto #610's branch after each stack fix and resolves conflicts in the stacked files below.

| Stacked file | Seam |
|--------------|------|
| `crates/lakehouse-catalog-write/src/iceberg_write.rs` | `refuse_existing_table` becomes `table_exists`; `existing_table_error` text |
| `crates/lakehouse-engine/src/export/target.rs` | `prepare` answer; `append_table` method |
| `crates/lakehouse-engine/src/export/iceberg_rest.rs` | existing-table branch of `prepare`; `append_table` delegates |
| `crates/lakehouse-engine/src/export/iceberg_sink.rs` | data files under the table's default partition spec |
| `crates/lakehouse-engine/src/export/batch.rs` | per-field source index; `Int32` and `Int64` builders with checked narrowing; required-NULL error; `arrow_type` visible to `binding.rs` |
| `crates/lakehouse-engine/src/export/mod.rs` | `WriterSpec::append`; mode dispatch in `write_group`; table name in batch errors |
| `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_write.rs` | delete `export_refuses_an_existing_table`; `Target`, `summary_values`, `sources()`, `STATIC`, and `VENDED` move to a support module |
| `crates/lakehouse-e2e/tests/e2e_local/export.rs` | `assert_refused_before_reading` and `objects_under` move to a support module |
| `scripts/spark-fixtures/run_fixtures.sh`, `e2e_local/main.rs`, `e2e_lakekeeper_test.rs` | one line each, plus one `mod` line per support module |
| `docs/export.md` | lines 9, 42, 83, 84, 87, 101, 103, 107-108, 117, 119; one new section |
| `docs/security.md` | § Who can bypass the export script |
| `docs/index.md` | line 33 |

## Sequencing With the Stacked PRs

- The new behavior is specified only in the NEW feature `export/export-iceberg-append`.
- `architecture.md` has BASE `d66175d2e63a6bd647976470a7a65e31dac52b9f`, the hash of `specs/architecture.md` at commit cc728feb of #610's branch. Task 5.1 copies each block again from main and refreshes the BASE.
- Task 5.1 writes the spec deltas below after #607 to #610 merge into main and this branch rebases onto main. It copies each section from main and changes only the named phrases.
- `export/export-iceberg-table`: the scenario "An export into an existing table is refused" is REMOVED.
- `export/export-iceberg-table`: the scenario "Two exports that create the same table leave one winner" is CHANGED. Its GIVEN states that both statements pass the pre-read check while the table does not exist.
- `export/export-iceberg-table` `## Background`: the bullet that starts "Before it reads any source row" says "finds that the table does not exist" in place of "refuses an existing table".
- The same bullet loses the sentence "It creates only new tables." and gains the sentence "An export whose pre-read check finds the table appends to it (export/export-iceberg-append)."
- The storage-credentials bullet says "vends for the table" in place of "vends for the new table".
- `export/export-into-script`: the description says "into the Iceberg table that `TABLE_NAME` names" in place of "into the new Iceberg table".
- `export/export-into-script` `## Background` says "lists the table" in place of "lists the new table".
- Task 3.2 deletes the test of the scenario that task 5.1 removes. The plan records only after task 5.1, so the permanent library never holds that scenario without its removal pending in the plan.

## Open Questions

1. Every table column must be bound, so a table with a column whose type the accepted-type table does not list takes no append. Examples are a `float`, struct, list, map, or `timestamptz` column. Spark's `TIMESTAMP` is `timestamptz` in Iceberg, so many Spark tables take no append. Should a later plan accept more source types, such as a `TIMESTAMP` written as UTC into `timestamptz`, the way the scan reads `timestamptz` into `TIMESTAMP`?
2. `specs/mission.md` Core Capability 14 reads "Export into a new Iceberg table". It needs an update through `/speq:mission` once this plan records.

## Implementation Tasks

### 1. Binding, append target, and writer dispatch (`crates/lakehouse-engine/src/export`, `crates/lakehouse-catalog-write`)

- [ ] 1.1 `iceberg_write.rs`: replace `TableWriteSession::refuse_existing_table` with `table_exists(&TableIdent) -> Result<bool, UdfError>` over the session's `table_exists`. Change the `existing_table_error` text, now reached only when a create loses to another statement, to "table <t> was created by another statement during this export". Update their tests in `iceberg_write_tests.rs`.
- [ ] 1.2 New `export/binding.rs` with `binding_tests.rs`: `bind(source columns with names and Exasol types, table Arrow schema)` returns, per table field, the index of its source column, or one error that names the table and lists every mismatch. It applies the spec delta's binding, mismatch, and accepted-type rules. A table column accepts the Arrow type that `batch::arrow_type` (made `pub(super)`) maps the source type to, plus `Decimal128(P,s)` with P ≥ p, `Int32` for `DECIMAL(p,0)` with p ≤ 9, and `Int64` for p ≤ 18. Every table field must be bound. The error renders the table column's type by its Iceberg name, such as `long`, `decimal(12,2)`, `float`, or `struct`. This module is the one owner of which source type a table column accepts. Unit tests: one case per mismatch kind, an unbound optional column, a `float` and a struct column, the precision bounds 9 and 10 and 18 and 19, a scale mismatch, and one source with every kind at once that yields one message.
- [ ] 1.3 `batch.rs`: `BatchBuilder` takes the per-field source index from 1.2 in place of its own name binding (`bind`, :52), so `batch.rs` does not depend on `binding.rs`. `Values` gains `Int32` and `Int64` builders fed through `rescaled` at scale 0, narrowed with `i32::try_from` and `i64::try_from`, which fail with the existing refusal "too large for the table column". A NULL for a non-nullable field fails with an error that names the column, in place of the `RecordBatch::try_new` error. Unit tests in `batch_tests.rs` for the integer builders, a value that fails the checked narrowing, and the required NULL.
- [ ] 1.4 `target.rs`: `prepare` answers whether the table exists (`TargetTable::New` or `TargetTable::Existing`). A new method `append_table(&WriterTag)` opens the existing table and returns its sink. Update the test targets that implement `ExportTarget`.
- [ ] 1.5 New `export/iceberg_append.rs` with `iceberg_append_tests.rs`: `appendable` refuses a table whose format version is not 2, or whose default partition spec has a field, with an error that names the table and the version or the partitioning. `open_existing` loads the table with `load_table`, re-runs `appendable`, and builds the `IcebergSink` on it. In `iceberg_rest.rs`, `prepare` calls `table_exists`: for an existing table it runs `load_table` and `appendable` and creates no namespace, and for a new table it runs `ensure_namespace` as today. `append_table` delegates to `open_existing`. In `iceberg_sink.rs` (:94), build the data file writer with the table's default partition spec, so a table whose unpartitioned spec has an id other than 0 accepts the commit. Unit tests of `appendable` on table metadata JSON under `src/export/testdata/`: format versions 1 and 3, a partitioned spec, and an unpartitioned spec with id 1. [expert]
- [ ] 1.6 `mod.rs`: `WriterSpec` gains `append`, which `export_hook` sets from the `prepare` answer. In `write_group`, append mode calls `append_table`, with or without `SINGLE_COMMIT`, and skips the create-path refusals of `arrow_schema`. Create mode keeps `arrow_schema`, `stage_table`, and `open_table`. Both modes bind through `binding::bind` against `sink.schema()`, and a binding error aborts the sink before any write. `write_group` wraps a batch error with the table name, so the required-NULL error names the table and the column. Unit tests in `export_tests.rs` for the mode dispatch only.
- [ ] 1.7 Measure changed-line coverage with base ref `origin/docs/export-iceberg-lakekeeper-e2e-and-specs` (Verification § Checklist), so the figure excludes the stacked PRs' lines. Below 90%, add unit tests of pure functions only. Where the uncovered lines sit in I/O code, first extract their decision as a pure function, such as the choice in `prepare` between a new table, an appendable table, and a refusal, computed from the answer of `table_exists` and the loaded table metadata. Add no fake server and no host test that spans modules (specs/testing.md § Testing strategy).

### 2. E2E: tables that another engine created (local Docker suite)

- [ ] 2.1 New `scripts/spark-fixtures/create_append_fixtures.sql`, listed in `run_fixtures.sh` before `create_int96_timestamp_fixture.sql`, whose SETs switch the session's Parquet writer (specs/testing.md § Fixtures), with its ground truth in a new `crates/lakehouse-e2e/tests/common/append_fixtures.rs`. Namespace `append_fixtures`. Each table is dropped with `PURGE` and created again, so the job stays idempotent:
  - `spark_target`, format version 2, with only columns the export can write: `name string`, `id bigint NOT NULL`, `views bigint`, `qty int`, `price decimal(12,2)`, `day date`, `ts timestamp_ntz`, `flag boolean`, `score double`, `legacy string`. It is created `PARTITIONED BY (day)` with 3 rows, then altered with `DROP PARTITION FIELD day`, `DROP COLUMN legacy`, and `ADD COLUMN note string`. Its current schema then has a field-ID gap, and its default spec is unpartitioned with id 1.
  - `spark_unwritable`, format version 2, unpartitioned, with 1 row: `id bigint NOT NULL`, `meta struct<k: string>`, `ratio float`.
  - `spark_partitioned`, format version 2, `PARTITIONED BY (id)`.
  - `spark_v1`, `'format-version'='1'`, unpartitioned.
- [ ] 2.2 New `crates/lakehouse-e2e/tests/e2e_local/export_append.rs`, with one `mod` line in `e2e_local/main.rs`. Move `assert_refused_before_reading` and `objects_under` from `e2e_local/export.rs` into a new support module `e2e_local/export_support.rs` that both files use (specs/testing.md § Test code). The fixtures persist across test runs on one stack, so every assertion is relative: snapshot ids and the current schema id read before the export, and rows keyed by a per-run `id` range. Add a helper that reads a table's metadata through the REST catalog's load-table endpoint if the local harness has none. Tests:
  - `append_binds_spark_table_columns_by_name`: unquoted upper-case source columns that name every column of `spark_target`, in another order. Every value reads back through a virtual schema in its column, and the current schema id is unchanged.
  - `append_widens_source_types`: `DECIMAL(18,0)` into `id`, `DECIMAL(9,0)` into `qty`, `DECIMAL(10,2)` into `price`, and `TIMESTAMP(3)` into `ts`, with every other column as in the binding test. Every value reads back equal.
  - `append_reports_every_mismatch_in_one_error`: a source that names every other column of `spark_target`, lacks the required `id` and the optional `note`, adds an `EXTRA` column, and has `DECIMAL(36,0)` for `views`. One error names the table, `id`, `note`, `EXTRA`, and `VIEWS`. Snapshot ids, the current schema id, and the objects under the table's data location are unchanged.
  - `append_refuses_table_with_unwritable_columns`: `ID DECIMAL(18,0)`, `META VARCHAR(100)`, and `RATIO DOUBLE` into `spark_unwritable`. One error names the table, `meta`, and `ratio`, each with its source type and its table column's type. Snapshot ids are unchanged.
  - `append_refuses_partitioned_and_v1_tables_before_reading`: a source whose rows raise an error when read, into `spark_partitioned` and `spark_v1`, through `assert_refused_before_reading`. Snapshot ids are unchanged.
  - `failed_single_commit_append_leaves_table_unchanged`: three cases with `SINGLE_COMMIT = 'true'`. In the first, the source raises an error at its 500th row. In the second, a source row holds NULL in `id`, and the error names the table and `id`. In the third, an export through the direct connection creates a per-run table, and an append into it runs through the `PROXIED` connection with a proxy filter line that answers the table's metadata path with HTTP 403, as `export_failed_single_commit_leaves_no_table` does; the proxy log shows that the writer wrote a data file. In every case, snapshot ids are unchanged and no object that the export wrote remains under the table's data location.

### 3. E2E: tables that an export created (Lakekeeper suite)

- [ ] 3.1 New `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_append.rs`, with one `mod` line in `e2e_lakekeeper_test.rs`. Move `Target`, `summary_values`, `sources()`, `STATIC`, and `VENDED` from `export_write.rs` into a new support module `e2e_lakekeeper/export_support.rs` that both files use (specs/testing.md § Test code). The tests write through `CONN_VENDED`, so a loaded table's vended credentials are exercised, and use `table_metadata`. Tests:
  - `append_into_export_created_table`: an export creates the table with 1,000 rows, and a second export appends 1,000. The row count is 1,000, 2,000 rows read back, and the earlier snapshot ids remain.
  - `single_commit_append_adds_one_snapshot`: one new snapshot, whose summary `added-records` is 1,000.
  - `parallel_append_commits_once_per_writer`: `WRITERS_PER_NODE = '4'` and 16,000 rows. Four new snapshots with one `lakehouse.export-id`, and every row exactly once.
  - `empty_source_appends_nothing`: a query that returns no rows, lacks a table column, and has a column the table lacks. Row count 0 and unchanged snapshot ids.
- [ ] 3.2 `export_write.rs`: delete `export_refuses_an_existing_table` (:382) and every helper that only it uses. In `export_racing_exports_leave_one_winner`, assert that the loser's error carries the `existing_table_error` text of task 1.1, so the test proves that the loser lost the create race.

### 4. Docs

- [ ] 4.1 `docs/export.md`: add a section "Appending to an existing table" after "The new table". It covers when the append applies, the binding rule (every table column needs a source column), the accepted-type table, and that a table with a column of an unlisted type takes no append. It lists the Exasol integer aliases (`INTEGER` as `DECIMAL(18,0)`, and `BIGINT` as `DECIMAL(36,0)`, which needs a cast to `DECIMAL(18,0)` for a `long` column; confirm both on the Docker Exasol with `exapump` first) and the refusals. It states that a source with no rows is not checked, and that without `SINGLE_COMMIT` a failed append keeps the rows of the writers that committed. It covers `SINGLE_COMMIT` and the commit retries that the table's `commit.retry.num-retries` allows (Iceberg's default is 4), and that `SINGLE_COMMIT` or fewer writers avoid commit contention.
- [ ] 4.2 `docs/export.md`, existing lines. Line 9 names a new or an existing table. Delete the refusal item "The table exists" (line 42). Line 83 says the export loads an existing table in place of refusing it. Lines 84, 101, 103, and 107-108 state that they apply to a new table, each with its append counterpart: each writer loads the existing table; the one `SINGLE_COMMIT` writer appends in one commit without staged creation; the files of a failed `SINGLE_COMMIT` append lie under the existing table's location. Line 87 says that the winner rule holds when both statements pass the pre-read check while the table does not exist, and that an export whose pre-read check finds the table appends to it. Line 117 adds load and commit rights on an existing table. Line 119 says "vends for the table". In `docs/security.md` § Who can bypass the export script, remove "the check for an existing table" from the skipped checks. Replace the third bullet with the fact that a direct call appends to no table that a plain `EXPORT` cannot append to, because the writer re-runs `appendable` and the binding. Add one sentence that the export role appends to any table that the CONNECTION's catalog identity may commit to. In `docs/index.md` line 33, write "into a new or an existing Iceberg table".

### 5. Record-time deltas

- [ ] 5.1 Precondition: #607, #608, #609, and #610 are merged into main, and this branch is rebased onto main. Write the spec deltas that § Sequencing With the Stacked PRs lists, copying each section from main. Copy each block of `architecture.md` again from main's `specs/architecture.md`, reapply this plan's edits, and set the BASE to `git hash-object specs/architecture.md`. If a stack fix renamed a scenario or reworded a phrase, apply the same change to the merged text. Then run `speq plan validate add-export-iceberg-append`.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Export append | 1.1-1.7, 2.1-2.2, 3.1-3.2, 4.1-4.2 | none | spec delta `export/export-iceberg-append`; `crates/lakehouse-engine/src/export/`, `crates/lakehouse-catalog-write/src/iceberg_write.rs`, `scripts/spark-fixtures/`, `crates/lakehouse-e2e/tests/e2e_local/export_append.rs`, `crates/lakehouse-e2e/tests/e2e_local/export_support.rs`, `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_append.rs`, `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_support.rs`, `crates/lakehouse-e2e/tests/common/append_fixtures.rs`, `docs/export.md`, `docs/security.md`, `docs/index.md` |
| R: Record-time deltas | 5.1 | A, and #607 to #610 merged into main | the plan's `architecture.md`; main's `specs/export/export-iceberg-table/spec.md`, `specs/export/export-into-script/spec.md`, `specs/architecture.md`; § Sequencing With the Stacked PRs |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `TableWriteSession::refuse_existing_table`, `crates/lakehouse-catalog-write/src/iceberg_write.rs` | Replaced by `table_exists` |
| Function | name binding in `BatchBuilder::bind`, `crates/lakehouse-engine/src/export/batch.rs` | Replaced by `binding::bind` |
| Test | `export_refuses_an_existing_table`, `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_write.rs` | Tests the refusal that this plan replaces |
| Doc | refusal item "The table exists", `docs/export.md` | The export appends to an existing table |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| An export appends to a table that an earlier export created | Integration | `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_append.rs` | `append_into_export_created_table` |
| A table that another engine created takes the source rows by column name | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `append_binds_spark_table_columns_by_name` |
| A source type that the table column widens appends unchanged values | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `append_widens_source_types` |
| Every column mismatch is reported in one error before any data is written | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `append_reports_every_mismatch_in_one_error` |
| A table with a column that no source type appends to takes no append | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `append_refuses_table_with_unwritable_columns` |
| A table the export cannot append to is refused before any data is read | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `append_refuses_partitioned_and_v1_tables_before_reading` |
| SINGLE_COMMIT appends all rows in exactly one snapshot | Integration | `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_append.rs` | `single_commit_append_adds_one_snapshot` |
| A failed SINGLE_COMMIT append leaves the table unchanged | Integration | `crates/lakehouse-e2e/tests/e2e_local/export_append.rs` | `failed_single_commit_append_leaves_table_unchanged` |
| Parallel writers each append their rows once | Integration | `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_append.rs` | `parallel_append_commits_once_per_writer` |
| An empty source appends nothing | Integration | `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_append.rs` | `empty_source_appends_nothing` |

Unit tests cover the pure rules: `binding_tests.rs` (mismatch kinds, unbound optional and unwritable columns, precision bounds), `iceberg_append_tests.rs` (`appendable`), and `batch_tests.rs` (integer builders with checked narrowing, required NULL).

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| export-iceberg-append | `make e2e-up`, `make cross-udf-build`, install per `docs/install.md`, then through `exapump`, twice: `EXPORT (SELECT 1 AS ID, 'x' AS NAME) INTO SCRIPT <schema>.LAKEHOUSE_EXPORT WITH CATALOG_CONNECTION = '<connection>' TABLE_NAME = 'manual.append_demo' ALLOW_HTTP = 'true'` | Row count 1 both times. A `SELECT` over a virtual schema on `manual` shows two rows with `ID` 1 and `NAME` `x` |
| export-iceberg-append | `EXPORT (SELECT CAST(1 AS DECIMAL(36,0)) AS VIEWS) INTO SCRIPT ... TABLE_NAME = 'append_fixtures.spark_target' ...` | One error that names `append_fixtures.spark_target`, each table column the source lacks, among them the required `id`, and `VIEWS` with `DECIMAL(36,0)` and `long` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace --lib` | 0 failures |
| E2E local | `make e2e-up test-e2e` | 0 failures |
| E2E Lakekeeper | `make lakekeeper-up test-e2e-lakekeeper` | 0 failures |
| Coverage | `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info`, then the changed-line check with base ref `origin/docs/export-iceberg-lakekeeper-e2e-and-specs` (#610's branch), so it counts only this plan's lines. The check uses `origin/main` as its base ref only after task 5.1 rebases this branch onto main | At least 90% of changed lines |
| Lint | `make lint` | 0 errors or warnings |
| Format | `make fmt-check` | No changes |

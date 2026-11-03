# Plan: add-export-into-script-entry-points

## Summary

The installer creates `LAKEHOUSE_EXPORT`, which answers `EXPORT ... INTO SCRIPT`, and `LAKEHOUSE_EXPORT_WRITER`, which counts the rows it receives, so Exasol reports the exported row count while nothing is written to the lakehouse (#565). The export validates its options and CONNECTION before any source row is read, spreads rows over node count × `WRITERS_PER_NODE` writers or one writer for `SINGLE_COMMIT`, and refuses columns Exasol cannot pass to a UDF with an error that names the column.

## Context

- Issue #565 is the first of eight export issues. It proves that Exasol runs the export query correctly and that every row reaches exactly one writer. Writing data and performance work are out of scope.
- Measured on the local Exasol 2025.1.16 container (probe SQL and a throwaway spike `.so`, recorded in `notes/planning.md`):
  - Exasol calls the export script once with the options, the source column names, the TRUNCATE, REPLACE, and CREATED BY flags, and either a CONNECTION name (`AT <name>`) or an inline address with user and password. It passes no column types.
  - The answer is one `SELECT` over `DUAL`, which Exasol binds to the export source. Its single column must be `DECIMAL(18,0)`, and Exasol reports the sum of its rows as the row count. An empty source reports 0.
  - A Rust SDK 0.30.0 entry point with the `export` hook answers the call, and `ctx.node_count()`, `ctx.script_schema()`, and `ctx.connection()` work inside it.
  - `EXPORT` needs the `EXPORT` system privilege. `AT <connection>` needs the user's `CONNECTION` privilege. A script that reads the CONNECTION needs `GRANT ACCESS ON CONNECTION ... FOR SCRIPT <that script>`. All of these work through one role.
  - Exasol refuses GEOMETRY, HASHTYPE, INTERVAL, and TIMESTAMP WITH LOCAL TIME ZONE as UDF input before writer code runs, with messages that do not name the column. Decision [2] places the check in the generated SQL.
  - The number of writer invocations is visible as `OUT_ROWS` of the `GROUP BY` part in `EXA_USER_PROFILE_LAST_DAY` after `ALTER SESSION SET PROFILE = 'ON'` and `FLUSH STATISTICS`.
  - `EXPORT <virtual table>` is refused by Exasol ("Use EXPORT (SELECT * FROM ...) instead"). A query over a virtual schema table joined with an Exasol table exports correctly.
  - In SDK 0.30.0 the first `ctx.next()` of a SET group loads the first row, so a writer counts with `while ctx.next()? { n += 1 }`. Counting the first row before the first `next()` over-counts by one per group.
- The local E2E stack is a single Exasol node. The opt-in `cloud-e2e` suite runs against a real cluster and can confirm writer counts across nodes.
- The architecture delta (`architecture.md` in this directory) adds the `export` component, the export data flow, the two script interfaces, and the writer-count constraint.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| EXPORT INTO SCRIPT Entry Points | NEW | `export/export-into-script/spec.md` |
| Single Shared Object for All Rust Entry Points | CHANGED | `packaging/single-so-two-entry-points/spec.md` |
| Version Query UDF | CHANGED | `packaging/version-udf/spec.md` |

## Impact

- New SQL surface: `EXPORT ... INTO SCRIPT <schema>.LAKEHOUSE_EXPORT AT <connection> WITH TABLE_NAME = '...'` reports the exported row count and writes nothing to the lakehouse.
- `install.sh` creates two more scripts in the target schema, and its next-step template prints an export role. An existing deployment gets the scripts by re-running `install.sh`. As before, `CREATE OR REPLACE` drops the ACCESS grants of the replaced scripts, so the operator re-runs the printed grant lines.
- `install.sh` issues the export DDL for every resolved release version. The export scripts of a release older than this change fail when called, because its `.so` lacks the two entry points.
- No breaking change for virtual schema queries.

## Dependencies

- `exasol-udf-sdk` gains the `export` cargo feature in the root `[workspace.dependencies]` pin. The version stays 0.30.0, and the spike `.so` built with the feature loaded against the installed 0.30.0 SLC.

## Implementation Tasks

### 1. Export entry points and their E2E proof

- [ ] 1.1 Enable the `export` feature on the root `[workspace.dependencies]` `exasol-udf-sdk` pin, next to `emit-arrow`.
- [ ] 1.2 Move the 300-group cap into `adapter/sharding.rs` as `group_count(node_count, per_node)`, which clamps to between 1 and 300. Make `shard_count` in `adapter/pushdown/support.rs` delegate to it and keep its file-count clamp, with the existing `shard_count_*` tests unchanged. Add a unit test in `sharding_tests.rs` for a product above 300 (`/// Scenario: The writer count never exceeds 300`).
- [ ] 1.3 Add `crates/lakehouse-engine/src/export/options.rs`: turn an SDK `ExportSpec` into validated export options or a `UdfError::User` that names the option and its accepted form. Cover: TRUNCATE, REPLACE, and CREATED BY refused with an append-only message naming the clause; no CONNECTION name refused with a message asking for `AT <connection>`, and an inline CONNECTION refused without echoing `connection_information`; `TABLE_NAME` missing or with an empty part; an unknown option, with the accepted list; `WRITERS_PER_NODE` not an integer of at least 1; `SINGLE_COMMIT` not `true` or `false` in any case; `WRITERS_PER_NODE` with `SINGLE_COMMIT = 'true'`, naming both. One table-driven unit test file, `options_tests.rs`, holds every invalid and valid case.
- [ ] 1.4 Add `export/sql.rs`, which renders the writer `SELECT` [expert]:
  - Shape: `SELECT "<schema>"."LAKEHOUSE_EXPORT_WRITER"('<writer spec>', <source columns>) FROM DUAL WHERE <guards> GROUP BY FLOOR(RANDOM() * <G>)`. The schema comes from `ctx.script_schema()`. G is 1 for `SINGLE_COMMIT = 'true'`, otherwise `group_count(ctx.node_count(), WRITERS_PER_NODE)`.
  - The writer spec is a JSON object that carries only the CONNECTION name.
  - One guard per source column: `(CASE WHEN TYPEOF(c) LIKE 'GEOMETRY%' OR TYPEOF(c) LIKE 'HASHTYPE%' OR TYPEOF(c) LIKE 'INTERVAL%' OR TYPEOF(c) LIKE '%WITH LOCAL TIME ZONE' THEN CAST('LAKEHOUSE_EXPORT: column "<name>" has type ' || TYPEOF(c) || ', which the export does not support; cast it to a supported type' AS DECIMAL(1,0)) ELSE 0 END) = 0`. The message must concatenate `TYPEOF(c)`: Exasol folds a constant `CAST` eagerly and fails every export.
  - `<name>` is the last quoted identifier of the `source_column_names` entry, with `""` unescaped. Every string literal doubles its `'`.
  - Unit tests in `sql_tests.rs`: the shape for one and several columns, G for each mode and the cap, a column name and a CONNECTION name that contain `'` and `"`, and no sentinel CONNECTION value in the output.
- [ ] 1.5 Add `export/mod.rs` with two public functions:
  - `generate_export_sql(ctx, json)` parses the spec with `ExportSpec::from_json`, validates it (1.3), and reads the CONNECTION with `ctx.connection(name)`, discarding the value at once. On failure it returns a named error that names the CONNECTION, `<schema>.LAKEHOUSE_EXPORT`, the `ctx.current_user()` grantee, and the missing `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` statement. Then it renders the SQL (1.4).
  - `run_writer(ctx)` advances to the first row, reads the writer spec from column 0, reads the CONNECTION with the same named-error pattern for `<schema>.LAKEHOUSE_EXPORT_WRITER`, and counts rows with `while ctx.next()?`, starting at 1 for the loaded first row. It returns the count.
  - The module emits no log event.
  - Unit tests in `export/export_tests.rs` use the SDK `TestContext`: a valid spec yields SQL without the sentinel password; each refusal and the missing-grant error carry no sentinel value; the writer returns the exact row count of a multi-row group.
- [ ] 1.6 Declare the entry points in `src/lib.rs`: `#[exasol_udf(name = "LAKEHOUSE_EXPORT", export_spec(export::generate_export_sql))]`, whose `run` returns an error saying the script runs only as the target of `EXPORT ... INTO SCRIPT`, and `#[exasol_udf(name = "LAKEHOUSE_EXPORT_WRITER")]` returning `Result<Option<i64>, UdfError>`. Update the module doc to five entry points. In `tests/two_entry_points_test.rs`, assert the five symbols and an exact count of 5, rename the test to `so_exports_five_entry_point_symbols`, and change its scenario line to `/// Scenario: One crate exports every Rust entry point`.
- [ ] 1.7 In `tests/common/e2e_harness.rs`, add the `LAKEHOUSE_EXPORT` and `LAKEHOUSE_EXPORT_WRITER` constants and DDL to `create_schema_and_scripts`, byte-identical to the installer DDL of task 2.1. Update `specs/testing.md` § Shared harness so the script list names both export scripts.
- [ ] 1.8 Add `crates/lakehouse-engine/tests/e2e_export_test.rs` behind `exasol-e2e` with a `OnceLock` setup through the harness, and list the binary on the `test-e2e` Makefile line. Exports name the CONNECTION the shared virtual schema uses. Each test carries the `/// Scenario:` line of the scenario it proves (Scenario Coverage table).
- [ ] 1.9 Row-count and writer-count tests in `e2e_export_test.rs`:
  - Sources: a 1,000-row Exasol table with a filter column, an empty query, and a seeded table of the local suite's virtual schema joined with an Exasol table.
  - A profile helper in the test file enables `PROFILE`, runs the export, runs `FLUSH STATISTICS`, and reads the export statement's `GROUP BY` `OUT_ROWS` from `EXA_USER_PROFILE_LAST_DAY`. It asserts 3 writers for `WRITERS_PER_NODE = '3'`, 1 for the default on one node, and 1 for `SINGLE_COMMIT = 'true'`.
- [ ] 1.10 Refusal tests in `e2e_export_test.rs`:
  - "Before any data is read" uses a source query whose rows raise an error when read, such as `SELECT 1/(a - a) AS x FROM <table>`, and asserts the option error, not the division error. Two or three invalid examples per scenario; the exhaustive table stays in `options_tests.rs`.
  - The type test uses a one-row table with one column of each unsupported type, and checks a table column, a column-list entry, and a query alias by name.
- [ ] 1.11 Privilege and credential tests in `e2e_export_test.rs`: create non-SYS users and a role holding the § Background privileges. One user exports successfully, one lacks the `LAKEHOUSE_EXPORT` grant (error before any row is read, through the failing-source query), and one lacks the writer grant. Every error message collected in the file is asserted free of the CONNECTION password.
- [ ] 1.12 In `crates/lakehouse-engine/tests/cloud_e2e_test.rs`, add `cloud_export_writer_counts`, one test that exports a 10,000-row table on the real cluster. It reads `NPROC()` and asserts, through the same profile reading, 1 writer for `SINGLE_COMMIT = 'true'`, `NPROC()` writers by default, and 2 × `NPROC()` writers for `WRITERS_PER_NODE = '2'` (when at most 300), each with the full row count. It skips like the other cloud tests when the variables are absent, and fails naming a missing `CLOUD_LHVS` export script. Add one line to the cloud suite notes in `deploy/README.md`: the cluster needs the export scripts from a release that contains this change.

### 2. Installer and documentation

- [ ] 2.1 In `deploy/scripts/install.sh`, add `ddl_export` (`CREATE OR REPLACE RUST SCALAR SCRIPT <schema>.LAKEHOUSE_EXPORT(...) EMITS (...) AS %udf_object <so>`) and `ddl_export_writer` (`CREATE OR REPLACE RUST SET SCRIPT <schema>.LAKEHOUSE_EXPORT_WRITER(...) RETURNS DECIMAL(18,0) AS %udf_object <so>`), and add both to `create_engine_scripts` after `ddl_version`.
- [ ] 2.2 In `print_next_step_template`, print an export block for every installing user, including `SYS`: `CREATE ROLE LAKEHOUSE_EXPORT_ROLE_<schema>;`, `GRANT EXPORT`, `GRANT CONNECTION LAKEHOUSE_CATALOG_CREDS`, `GRANT EXECUTE ON <schema>.LAKEHOUSE_EXPORT` and `..._WRITER`, two `GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT` lines, all to that role, and a comment line `-- Exporters: GRANT LAKEHOUSE_EXPORT_ROLE_<schema> TO <user>;`.
- [ ] 2.3 In `deploy/scripts/tests/install.test.sh`, rename `test_four_scripts_ddl_saas_path_types` to `test_six_scripts_ddl_saas_path_types` and assert both export DDL statements. Add `test_template_prints_export_role` for a non-SYS and a SYS installing user. Extend `test_docs_describe_version_verification` with the two export DDL statements of the by-hand appendix. Register the new test in `main()`.
- [ ] 2.4 Update `docs/install.md`: six scripts in "What the command does", the export DDL in "Appendix: create the scripts by hand", and "all six" wherever the script count appears. Add `docs/export.md` and link it from the `docs/index.md` table. It covers the statement and its options, the export role and the privileges it holds, writer counts, the absence of lakehouse writes in this release, the unsupported types with the cast advice and the quoted error prefix, the empty-source behavior, and wrapping a virtual table in `EXPORT (SELECT * FROM ...)`.
- [ ] 2.5 Update `docs/security.md` § Privilege model:
  - Exporting users hold the export role, not the engine role.
  - The `CONNECTION` privilege lets a user name the CONNECTION in other `IMPORT` and `EXPORT` statements but not read its password.
  - The export scripts receive only the CONNECTION name.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Export entry points | 1.1-1.12 | none | spec deltas `export/export-into-script` (all scenarios except the installer scenario), `packaging/single-so-two-entry-points`; `Cargo.toml`, `crates/lakehouse-engine/src/lib.rs`, `src/export/`, `src/adapter/sharding.rs`, `src/adapter/pushdown/support.rs`, `tests/two_entry_points_test.rs`, `tests/common/e2e_harness.rs`, `tests/e2e_export_test.rs`, `tests/cloud_e2e_test.rs`, `Makefile`, `specs/testing.md`, `deploy/README.md` |
| B: Installer and docs | 2.1-2.5 | none | spec delta `packaging/version-udf`, and the scenario "The installer creates the export scripts and prints their grants" plus § Background of `export/export-into-script`; `deploy/scripts/install.sh`, `deploy/scripts/tests/install.test.sh`, `docs/install.md`, `docs/export.md`, `docs/index.md`, `docs/security.md` |

- Both groups write the same two export DDL statements, one in the harness (1.7) and one in the installer (2.1). The architecture delta's Interfaces section and task 2.1 fix their text. Group A copies it from task 2.1.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test | `crates/lakehouse-engine/tests/two_entry_points_test.rs` `so_exports_three_entry_point_symbols` | Renamed to `so_exports_five_entry_point_symbols` with five symbols |
| Test | `deploy/scripts/tests/install.test.sh` `test_four_scripts_ddl_saas_path_types` | Renamed to `test_six_scripts_ddl_saas_path_types` |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A whole-table export reports every source row | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_whole_table_reports_every_row` |
| A filtered query exports only the rows it returns | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_filtered_query_reports_returned_rows` |
| A column-list export reports every row of the table | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_column_list_reports_every_row` |
| A join of a virtual schema table and an Exasol table exports the joined rows | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_vs_join_reports_joined_rows` |
| An empty source exports zero rows | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_empty_source_reports_zero` |
| WRITERS_PER_NODE sets the number of writers on each node | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs`; `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `export_writers_per_node_sets_writer_count`; `cloud_export_writer_counts` |
| The writer count never exceeds 300 | Unit | `crates/lakehouse-engine/src/adapter/sharding_tests.rs` | `group_count_caps_at_300` |
| SINGLE_COMMIT runs one writer for the whole cluster | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs`; `crates/lakehouse-engine/tests/cloud_e2e_test.rs` | `export_single_commit_runs_one_writer`; `cloud_export_writer_counts` |
| WRITERS_PER_NODE together with SINGLE_COMMIT is refused | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_refuses_writers_per_node_with_single_commit` |
| An invalid option fails the export before any data is read | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_refuses_invalid_options_before_reading` |
| An export without a named CONNECTION fails before any data is read | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_refuses_missing_or_inline_connection` |
| TRUNCATE, REPLACE, and CREATED BY are refused | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_refuses_truncate_replace_created_by` |
| A column of an unsupported type fails the export naming the column and its type | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_refuses_unsupported_column_types` |
| A user who holds the export privileges exports rows | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_role_user_exports_rows` |
| A missing script-scoped CONNECTION grant fails with a named error | Integration | `crates/lakehouse-engine/tests/e2e_export_test.rs` | `export_missing_script_grant_fails_named` |
| No CONNECTION value appears in the generated SQL, an error, or the script output | Unit + Integration | `crates/lakehouse-engine/src/export/export_tests.rs`; `crates/lakehouse-engine/tests/e2e_export_test.rs` | `generated_sql_and_errors_carry_no_connection_value`; `export_errors_carry_no_connection_password` |
| The installer creates the export scripts and prints their grants | Unit (installer) | `deploy/scripts/tests/install.test.sh` | `test_six_scripts_ddl_saas_path_types`; `test_template_prints_export_role` |
| One crate exports every Rust entry point | Integration | `crates/lakehouse-engine/tests/two_entry_points_test.rs` | `so_exports_five_entry_point_symbols` |
| All scripts resolve from the same uploaded artifact | Integration | `crates/lakehouse-engine/tests/e2e_version_udf_test.rs`; `crates/lakehouse-engine/tests/e2e_export_test.rs` | existing version and scan tests; `export_whole_table_reports_every_row` |
| The install script creates the version script with the other deployment scripts | Unit (installer) | `deploy/scripts/tests/install.test.sh` | `test_six_scripts_ddl_saas_path_types` |
| The install documentation describes the version-based verification | Unit (installer) | `deploy/scripts/tests/install.test.sh` | `test_docs_describe_version_verification` |

- The SQL half of the credentials scenario is proved by a unit test, because Exasol SQL cannot observe the generated statement (`specs/testing.md` § Coverage rule). The script-output half rests on the module emitting no log event and discarding the CONNECTION value right after the access check, which code review confirms.
- The multi-node half of the SINGLE_COMMIT and WRITERS_PER_NODE scenarios runs only in the opt-in `cloud-e2e` suite. If no cluster is available when this plan is implemented, the verification report names that as an open limitation. The single-node E2E test still proves one writer invocation for the whole statement.

### Manual Testing

Run against the local container after `make cross-udf-build`, `make bucketfs-upload-so`, and an `install.sh` run into schema `LHVS`. `$DSN` is `exasol://sys:exasol@localhost:28563?validateservercertificate=0`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| EXPORT INTO SCRIPT Entry Points | `exapump sql "CREATE OR REPLACE TABLE T AS SELECT LEVEL AS id FROM DUAL CONNECT BY LEVEL <= 1000" -d "$DSN"` then `exapump sql "EXPORT T INTO SCRIPT LHVS.LAKEHOUSE_EXPORT AT LAKEHOUSE_CATALOG_CREDS WITH TABLE_NAME = 'ns.t' WRITERS_PER_NODE = '3'" -d "$DSN"` | 1000 rows affected |
| EXPORT INTO SCRIPT Entry Points | `exapump sql "EXPORT T INTO SCRIPT LHVS.LAKEHOUSE_EXPORT AT LAKEHOUSE_CATALOG_CREDS WITH TABLE_NAME = 'ns.t' TRUNCATE" -d "$DSN"` | Error naming `TRUNCATE` and stating that the export only appends rows |
| EXPORT INTO SCRIPT Entry Points | `exapump sql "EXPORT (SELECT CAST('POINT(1 2)' AS GEOMETRY) AS g FROM DUAL) INTO SCRIPT LHVS.LAKEHOUSE_EXPORT AT LAKEHOUSE_CATALOG_CREDS WITH TABLE_NAME = 'ns.t'" -d "$DSN"` | Error containing `column "G" has type GEOMETRY` |
| Single Shared Object for All Rust Entry Points | `nm -D target/release/liblakehouse_engine.so \| grep __exa_udf_entry_` | Five symbols, including `__exa_udf_entry_LAKEHOUSE_EXPORT` and `__exa_udf_entry_LAKEHOUSE_EXPORT_WRITER` |
| Version Query UDF | `exapump sql "SELECT SCRIPT_NAME FROM EXA_ALL_SCRIPTS WHERE SCRIPT_SCHEMA = 'LHVS' ORDER BY 1" -d "$DSN"` | Six scripts, including `LAKEHOUSE_EXPORT` and `LAKEHOUSE_EXPORT_WRITER` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures, `e2e_export_test` included |
| Installer | `make test-install` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| Coverage | `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info` then `python3 .claude/skills/test-strategy-review/scripts/changed_line_coverage.py <scratchpad>/lcov.info` | Changed-line coverage at least 90% |

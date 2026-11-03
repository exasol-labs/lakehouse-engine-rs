# Plan: add-export-into-script-entry-points

## Summary

The installer creates two scripts: `LAKEHOUSE_EXPORT` answers `EXPORT ... INTO SCRIPT`, and `LAKEHOUSE_EXPORT_WRITER` counts the rows it receives. Exasol reports the exported row count, and nothing is written to the lakehouse (#565). The export checks its options and CONNECTION before any source row is read. It spreads rows over node count × `WRITERS_PER_NODE` writers, or over one writer with `SINGLE_COMMIT`. It refuses columns that Exasol cannot pass to a UDF, with an error that names the column. `install.sh --export-connection <name>` grants the export role.

## Context

- Issue #565 is the first of eight export issues. It proves that Exasol runs the export query correctly and that every row reaches exactly one writer. Writing data and performance work are out of scope.
- Measured on the local Exasol 2025.1.16 container with a spike `.so` built against the pinned SDK version (0.33.0) in the Makefile's `UDF_BUILDER_IMAGE` and loaded on the SLC of the same version (probe SQL and results in `notes/planning.md`):
  - Exasol calls the export script once with the options, the source column names, the TRUNCATE, REPLACE, and CREATED BY flags, and either a CONNECTION name (`AT <name>`) or an inline address with user and password. It passes no column types.
  - The answer is one `SELECT` over `DUAL`, which Exasol binds to the export source. Its single column must be `DECIMAL(18,0)`, and Exasol reports the sum of its rows as the row count. An empty source reports 0.
  - The SDK's `export` hook answers the call. `ctx.node_count()`, `ctx.script_schema()`, `ctx.current_user()` (the session user), and `ctx.connection()` work inside it, and `ctx.current_user()` and `ctx.connection()` also work in a SET writer. A missing script grant fails `ctx.connection()` with `insufficient privileges for using connection <name> in script <script>`.
  - In a SET group, the first row is readable when the writer starts, and the first `ctx.next()` returns true without advancing. `while ctx.next()? { n += 1 }` therefore counts the group's rows exactly, at one group and at four.
  - The writer accepts BOOLEAN, DATE, TIMESTAMP(3), TIMESTAMP(9), DOUBLE, CHAR, VARCHAR, and DECIMAL up to precision 36. The SLC truncates TIMESTAMP(9) values to microseconds, which does not change row counts.
  - `EXPORT` needs the `EXPORT` system privilege. `AT <connection>` needs the user's `CONNECTION` privilege. A script that reads the CONNECTION needs `GRANT ACCESS ON CONNECTION ... FOR SCRIPT <that script>`. All of these work through one role.
  - Without a check, Exasol's handling of a GEOMETRY, HASHTYPE, INTERVAL, or TIMESTAMP WITH LOCAL TIME ZONE source column varies between runs of the same statement: it refuses the column with a message that does not name it, or it exports the row. Decision [2] places the check in the generated SQL.
  - The number of writer invocations is visible as `OUT_ROWS` of the `GROUP BY` part in `EXA_USER_PROFILE_LAST_DAY` after `ALTER SESSION SET PROFILE = 'ON'` and `FLUSH STATISTICS`.
  - The group key `MOD(ROWNUM, G)` runs exactly G writers at G = 1, 4, and 300, also for a table and a join query of exactly G rows, and one writer per row for a source of fewer than G rows (decision [2]).
  - `EXPORT <virtual table>` is refused by Exasol ("Use EXPORT (SELECT * FROM ...) instead"). A query over a virtual schema table joined with an Exasol table exports correctly.
- Deviation from #565: #565 states that a column of an unsupported type fails the export with an error that names the column and its type. The check runs on source rows, so an empty source with a column of one of these types exports 0 rows without an error (measured on Exasol 2025.1.16 and 8.29.13). The requester accepted this exception (decision log § Review Findings), and the PR description names it.
- The local E2E stack is a single Exasol node, where SINGLE_COMMIT and the default both run one writer. Only the opt-in `cloud-e2e` suite on a multi-node cluster proves #565's criterion that "a multi-node cluster confirms that only one writer runs".
- The architecture delta (`architecture.md` in this directory) adds the `export` component, the export data flow, the two script interfaces, the installer flag, and the writer-count constraint.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| EXPORT INTO SCRIPT Entry Points | NEW | `export/export-into-script/spec.md` |
| Single Shared Object, Five Entry Points | CHANGED | `packaging/single-so-two-entry-points/spec.md` |
| Version Query UDF | CHANGED | `packaging/version-udf/spec.md` |

## Impact

- New SQL surface: `EXPORT ... INTO SCRIPT <schema>.LAKEHOUSE_EXPORT AT <connection> WITH TABLE_NAME = '...'` reports the exported row count and writes nothing to the lakehouse.
- `install.sh` creates two more scripts in the target schema. The new `--export-connection <name>` flag creates `LAKEHOUSE_EXPORT_ROLE_<schema>` and runs its grants. Without the flag, the next-step template prints them. `CREATE OR REPLACE` drops the script grants of replaced scripts, so a re-install with the flag restores them, and a re-install without it needs the printed lines again.
- `install.sh` issues the export DDL for every resolved release version. The export scripts of a release older than this change fail when called, because its `.so` lacks the two entry points.
- No breaking change for virtual schema queries.

## Dependencies

- `exasol-udf-sdk` gains the `export` cargo feature on its existing root `[workspace.dependencies]` pin. The version does not change. The engine's dev-dependency already enables `test-support`.
- The `cloud-e2e` proof needs a multi-node Exasol cluster that accepts a BucketFS upload (task 1.13).

## Open Questions

- Multi-node writer-count proof: if no multi-node cluster can be provisioned before the PR leaves draft, the requester decides whether #565's criterion "a multi-node cluster confirms that only one writer runs" may ship unverified (#TBD).
- Type-guard and group-key cost: a follow-up issue measures the per-row cost of the type guard and of the `ROWNUM` group key on a multi-node cluster, and revisits both in the export performance work (#TBD, decision [2]).

## Implementation Tasks

### 1. Export entry points and their proof

- [ ] 1.1 Add `export` to the features of the root `[workspace.dependencies]` `exasol-udf-sdk` pin, next to `emit-arrow`, keeping the pinned version.
- [ ] 1.2 Move the 300-group cap from `adapter/pushdown/support.rs` (`MAX_SHARD_COUNT`, `shard_count` at :28-35) into `adapter/sharding.rs` as `pub fn group_count(node_count: usize, per_node: usize) -> usize`, which multiplies with `saturating_mul` and clamps to between 1 and 300. Make `shard_count` delegate to it and keep its file-count clamp, with the existing `shard_count` tests in `support_tests.rs` unchanged. Add `group_count_caps_at_300` to `sharding_tests.rs`, covering `group_count(1, 1) == 1`, `group_count(2, 0) == 1`, `group_count(4, 100) == 300`, and `group_count(usize::MAX, usize::MAX) == 300`.
- [ ] 1.3 Add `crates/lakehouse-engine/src/export/options.rs`: turn an SDK `ExportSpec` into validated export options or a `UdfError::User` that names the option and its accepted form. Cover: TRUNCATE, REPLACE, and CREATED BY refused with an append-only message naming the clause; no CONNECTION name refused with a message asking for `AT <connection>`, and an inline CONNECTION refused without echoing `connection_information`; `TABLE_NAME` missing or with an empty part; an unknown option, with the accepted list; `WRITERS_PER_NODE` not an integer of at least 1 that fits a `usize`; `SINGLE_COMMIT` not `true` or `false` in any case; `WRITERS_PER_NODE` with `SINGLE_COMMIT = 'true'`, naming both. Option names arrive upper-cased from Exasol. One table-driven unit test file, `options_tests.rs`, holds every invalid and valid case, including `WRITERS_PER_NODE = '18446744073709551615'` (`usize::MAX`, accepted) and `'18446744073709551616'` (refused with the integer message).
- [ ] 1.4 Add `export/sql.rs`, which renders the writer `SELECT` [expert]:
  - Shape: `SELECT "<schema>"."LAKEHOUSE_EXPORT_WRITER"('<writer spec>', <source columns>) FROM DUAL WHERE <guards> GROUP BY MOD(ROWNUM, <G>)`. The key stays in the top-level `GROUP BY`, because an export SELECT over a subquery of `DUAL` fails (decision [2]). The schema comes from `ctx.script_schema()`. G is 1 for `SINGLE_COMMIT = 'true'`, otherwise `group_count(ctx.node_count(), WRITERS_PER_NODE)`.
  - The writer spec is a JSON object that carries only the CONNECTION name.
  - One guard per source column: `(CASE WHEN TYPEOF(c) LIKE 'GEOMETRY%' OR TYPEOF(c) LIKE 'HASHTYPE%' OR TYPEOF(c) LIKE 'INTERVAL%' OR TYPEOF(c) LIKE '%WITH LOCAL TIME ZONE' THEN CAST('LAKEHOUSE_EXPORT: column "<name>" has type ' || TYPEOF(c) || ', which the export does not support; cast it to a supported type' AS DECIMAL(1,0)) ELSE 0 END) = 0`. The message must concatenate `TYPEOF(c)`: Exasol folds a constant `CAST` eagerly and fails every export.
  - `<name>` is the last quoted identifier of the `source_column_names` entry, with `""` unescaped. Every string literal doubles its `'`.
  - Unit tests in `sql_tests.rs`: the shape for one and several columns, the group key and G for each mode and at the cap, and a column name and a CONNECTION name that contain `'` and `"`.
- [ ] 1.5 Add `export/mod.rs` with two public functions:
  - `generate_export_sql(ctx, json)` parses the spec with `ExportSpec::from_json`, validates it (1.3), and reads the CONNECTION with `ctx.connection(name)`, discarding the value at once. On failure it returns a named error that names the CONNECTION, `<schema>.LAKEHOUSE_EXPORT`, the `ctx.current_user()` grantee, and the missing `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` statement. Then it renders the SQL (1.4).
  - `run_writer(ctx)` reads the writer spec from column 0 of the first row, reads the CONNECTION with the same named-error pattern for `<schema>.LAKEHOUSE_EXPORT_WRITER`, and counts the group's rows with `while ctx.next()? { n += 1 }`. It returns the count.
  - The module emits no log event and no script output.
  - Unit tests in `export/export_tests.rs` use the SDK `TestContext`, built with `TestContext::set(rows)` and `with_input_columns`, `with_connection`, `with_node_count`, `with_script_schema`, and `with_current_user`: a valid spec yields SQL without the sentinel password; each refusal and the missing-grant error carry no sentinel value; the writer returns the exact row count of a multi-row group.
- [ ] 1.6 Declare the entry points in `crates/lakehouse-engine/src/lib.rs` next to the three at :22-34: `#[exasol_udf(name = "LAKEHOUSE_EXPORT", export_spec(export::generate_export_sql))]`, whose `run` returns an error saying the script runs only as the target of `EXPORT ... INTO SCRIPT`, and `#[exasol_udf(name = "LAKEHOUSE_EXPORT_WRITER")]` returning `Result<Option<i64>, UdfError>`. Change the module doc (line 1) to five entry points. In `crates/lakehouse-e2e/tests/e2e_local/two_entry_points.rs`, rename `so_exports_three_entry_point_symbols` to `so_exports_five_entry_point_symbols`, add the two export symbols to the contains-asserts (:29-40), change the count assert and its message (:45-50) to 5, and add `/// Scenario: One crate exports every Rust entry point`.
- [ ] 1.7 In `crates/lakehouse-e2e/tests/common/e2e_harness.rs`, add `EXPORT_SCRIPT_NAME` and `EXPORT_WRITER_SCRIPT_NAME` next to the constants at :26-35, and add the two DDL statements of task 2.1 to `create_schema_and_scripts_with_env` (:144-185). Update the script list in `specs/testing.md` § Shared harness (:67) to name the version script and both export scripts.
- [ ] 1.8 Add `crates/lakehouse-e2e/tests/e2e_local/export.rs` and declare `mod export;` in `e2e_local/main.rs` in alphabetical order. Each test calls `fixtures::stack()` and then a module-local lazy fixture (a `OnceLock`, as in `credential_exposure.rs:42`) that calls `fixtures::lakehouse_vs()` and creates the module's Exasol tables. Exports name `LAKEHOUSE_CATALOG_CREDS`, the CONNECTION of the shared virtual schema. Error assertions read the message through `common::exasol_ws::error_text`. Each test carries the `/// Scenario:` line of the scenario it proves (Scenario Coverage table).
- [ ] 1.9 Row-count and writer-count tests in `export.rs`:
  - Sources: a 1,000-row Exasol table with one column of each supported type (BOOLEAN, DATE, TIMESTAMP(3), TIMESTAMP(9), DOUBLE, CHAR(10), VARCHAR(100), DECIMAL(9,0), DECIMAL(18,0), DECIMAL(36,4)) and an integer filter column; an empty query over it; and the virtual schema's `events` table joined with an Exasol table.
  - A profile helper in the module enables `PROFILE`, runs the export, runs `FLUSH STATISTICS`, and reads the export statement's `GROUP BY` `OUT_ROWS` from `EXA_USER_PROFILE_LAST_DAY`. Every writer-count assertion is exact, and each export also asserts its row count:
    - `export_writers_per_node_sets_writer_count`: `WRITERS_PER_NODE = '3'` runs 3 writers over the 1,000-row table and over a query that returns exactly 3 rows, and 2 writers over a query that returns 2 rows. The default runs 1 writer on one node.
    - `export_writer_count_caps_at_300`: `WRITERS_PER_NODE = '500'` runs 300 writers over a 10,000-row table and over a query that returns exactly 300 rows.
    - `export_single_commit_runs_one_writer` (G = 1): 1 writer over the 1,000-row table.
- [ ] 1.10 Refusal tests in `export.rs`:
  - "Before any data is read" uses a source query whose rows raise an error when read, `SELECT 1/(a - a) AS x FROM <table>`, and asserts the option error, not the division error. One positive control exports the same source with valid options and asserts the division error. Each refusal scenario uses one invalid example; the exhaustive table stays in `options_tests.rs`.
  - The type test uses a one-row table with one column of each unsupported type, and checks a table column, a column-list entry, and a query alias by name. A second test exports a query over that table that returns no rows and asserts a row count of 0.
- [ ] 1.11 Privilege and credential tests in `export.rs`: module-local helpers create non-SYS users, as `credential_exposure.rs:49-62` does, and the export role with the statements of task 2.2. One user exports successfully. One lacks `ACCESS` for `LAKEHOUSE_EXPORT` and fails before any row is read, shown through the failing-source query. One lacks `ACCESS` for the writer. Every error message collected in the module is asserted free of the CONNECTION password.
- [ ] 1.12 In `crates/lakehouse-e2e/tests/cloud_e2e_test.rs`, add `cloud_export_writer_counts`, one test that exports a 10,000-row table through `CLOUD_LHVS.LAKEHOUSE_EXPORT` on the real cluster. It reads `NPROC()` and asserts, through the same profile reading, exactly 1 writer for `SINGLE_COMMIT = 'true'`, `NPROC()` writers by default, and 2 × `NPROC()` writers for `WRITERS_PER_NODE = '2'` (when at most 300), each with the full row count. A second `WRITERS_PER_NODE = '2'` export over a query that returns exactly 2 × `NPROC()` rows asserts 2 × `NPROC()` writers, which proves gap-free `ROWNUM` numbering across nodes. It exports `AT GLUE_CATALOG_CREDS` (`CLOUD_CATALOG_CONN`, :52) and creates that CONNECTION first with `build_create_connection_sql`, as `setup_cloud_vs` (:383-388) does. When `LH_EXASOL_USER` is not `SYS`, it grants that user `ACCESS ON CONNECTION GLUE_CATALOG_CREDS FOR SCRIPT` on both `CLOUD_LHVS` export scripts. It skips through `CloudEnv::from_env()` like the other cloud tests, and fails naming a missing `CLOUD_LHVS` export script.
- [ ] 1.13 Run `cloud_export_writer_counts` on a multi-node cluster before the PR leaves draft. Upload the branch-built `.so` and the SLC of the pinned version with `make install-slc bucketfs-upload-so EXASOL_HOST=<cluster> LH_EXASOL_PORT=<sql port> LH_BUCKETFS_PORT=<port> EXASOL_SYS_PASSWORD=<sys password> BUCKETFS_WRITE_PASS=<pass>`, the path `bench/run.sh:433-436` uses. Create `CLOUD_LHVS.LAKEHOUSE_EXPORT` and `CLOUD_LHVS.LAKEHOUSE_EXPORT_WRITER` with the DDL of task 2.1. Run the cloud suite command of `deploy/README.md:76` filtered to `cloud_export_writer_counts`, and record the node count and the writer counts in the verification report. Add these three steps to the cloud section of `deploy/README.md` (:62-77). If no multi-node cluster is available, raise the Open Question instead of marking the scenario verified.
- [ ] 1.14 Add `docs/export.md` and link it from the `docs/index.md` table (:27-38). It covers the statement and its options, the export role and `--export-connection`, writer counts, the absence of lakehouse writes in this release, the unsupported types with the cast advice and the quoted error prefix of tasks 1.3 to 1.5, the empty-source behavior, and wrapping a virtual table in `EXPORT (SELECT * FROM ...)`.

### 2. Installer and security documentation

- [ ] 2.1 In `deploy/scripts/install.sh`, add `ddl_export` and `ddl_export_writer` next to the `ddl_*` functions (:1182-1200), and add both to the `create_engine_scripts` statements array (:1267-1273) after `ddl_version`. Their text, which tasks 1.7 and 1.13 reuse:

  ```sql
  CREATE OR REPLACE RUST SCALAR SCRIPT <schema>.LAKEHOUSE_EXPORT(...) EMITS (...) AS
  %udf_object <so>
  /
  CREATE OR REPLACE RUST SET SCRIPT <schema>.LAKEHOUSE_EXPORT_WRITER(...) RETURNS DECIMAL(18,0) AS
  %udf_object <so>
  /
  ```

- [ ] 2.2 Export role in `install.sh`:
  - Add `--export-connection <name>` to `usage()` (:371-437) and to the value-flag allowlist of `parse_args` (:469-470).
  - Validate the name once in `parse_args`, before any download or SQL: it must match `^[A-Za-z][A-Za-z0-9_]{0,127}$` (Exasol refuses an identifier over 128 characters with "maximum length of identifier exceeded"), and is then upper-cased, as Exasol stores an unquoted name. Otherwise the install fails naming `--export-connection` and the accepted form. Every later use (the `EXA_ALL_CONNECTIONS` literal, the identifiers in the grants, and the template) takes the validated value.
  - Add `grant_export_role`, called after `create_engine_scripts` in `install_engine` (:1298) and `deploy_personal_launcher` (:1071) when the flag is set. It runs `SELECT CONNECTION_NAME AS SYSTEM_VALUE FROM EXA_ALL_CONNECTIONS WHERE CONNECTION_NAME = '<NAME>'` through `run_sql` and `extract_query_value`. When the CONNECTION exists, it checks `EXA_ALL_ROLES` the same way, runs `CREATE ROLE LAKEHOUSE_EXPORT_ROLE_<schema>` only when the role is absent, and then runs `GRANT EXPORT`, `GRANT CONNECTION <name>`, `GRANT EXECUTE ON SCRIPT` on both export scripts, and `GRANT ACCESS ON CONNECTION <name> FOR SCRIPT` on both, all to the role. A failed statement aborts the install naming it, as `create_engine_scripts` does (:1275-1276). When the CONNECTION does not exist, it prints a warning naming it and continues.
  - In `print_next_step_template` (:1346-1402), print the same export block for every installing user, including `SYS`, unless `grant_export_role` ran the grants. Use the `--export-connection` name when given, otherwise `LAKEHOUSE_CATALOG_CREDS` (:1354). Always end with `-- Exporters: GRANT LAKEHOUSE_EXPORT_ROLE_<schema> TO <user>;`.
- [ ] 2.3 In `deploy/scripts/tests/install.test.sh`:
  - Rename `test_four_scripts_ddl_saas_path_types` (:1069-1099) to `test_six_scripts_ddl_saas_path_types` and assert both export DDL statements.
  - Add `test_template_prints_export_role`: without `--export-connection`, the template prints the export block for a non-SYS and a `SYS` installing user.
  - Add `test_export_connection_grants_role`. Teach `write_exapump_stub` (:135-280) to answer the `EXA_ALL_CONNECTIONS` and `EXA_ALL_ROLES` queries under environment switches, as `EXAPUMP_DDL_FAIL` does. Assert:
    - The role is created when absent, and the six grants follow the script DDL.
    - An existing role gets the six grants without `CREATE ROLE`.
    - After the grants run, the template prints the `-- Exporters:` line and none of the role's statements.
    - A missing CONNECTION gives the warning, runs no `CREATE ROLE` or `GRANT`, prints the export block for that CONNECTION, and exits 0.
    - A failed grant aborts naming the statement.
    - `--export-connection` without a value fails with "requires a value".
  - Add `test_export_connection_name_validation`: `my_conn` reaches the SQL as `MY_CONN`; a 128-character name is accepted; a name with `'` or `"`, with a space, with `;` or `-`, starting with a digit, or of 129 characters fails naming the flag before the stub records any SQL.
  - Extend `test_docs_describe_version_verification` (:1189-1203) with the two export DDL statements of the by-hand appendix. Register the new tests in `main()` (:2468-2551).
- [ ] 2.4 Update `docs/install.md`: six scripts in "What the command does" (:121-122), the `--export-connection` row in the flags table (:140-161) with the accepted name form, the script and entry-point counts at :368, :468 ("five RUST entry points"), and :520, and the export DDL in "Appendix: create the scripts by hand" (:481-503).
- [ ] 2.5 Update `docs/security.md`:
  - Add an "Export grants" subsection to § Privilege model: exporting users hold the export role, not the engine role; the role's privileges; `--export-connection` runs them; the `CONNECTION` privilege lets the user name the CONNECTION in any `IMPORT` or `EXPORT` statement, so grant the role only to users who may use the CONNECTION's credentials.
  - Reconcile § Reader grants (:22) with the export role: the warning applies to `ACCESS` on the CONNECTION for `LAKEHOUSE_SCAN`, which the export role does not grant, and a reader that exports holds the export role.
  - Add "Who can bypass the export script" after § Who can bypass the adapter (:101-108): a holder of the export role can run `LAKEHOUSE_EXPORT_WRITER` directly with any writer spec and any rows, which skips every check `LAKEHOUSE_EXPORT` makes (options, type guards, writer count, SINGLE_COMMIT), so the writer treats its writer spec as untrusted input.
  - Extend § Re-installing (:78): a re-install with `--export-connection` runs the export role's grants again.
- [ ] 2.6 Extend the `install-script-e2e` job in `.github/workflows/ci.yml` (:286-326). It already starts the Exasol container, and its install step installs the latest release `.so` as `sys` into schema `LHVS` and installs `exapump` into `$HOME/.local/bin`. Add one step, `Export role grants (install.sh --export-connection, run twice)`, after the install step:
  - Create the CONNECTION `LH_EXPORT_CI` (dummy address, user, and password) with `exapump sql` and a DSN built from the install step's host, port, and `sys` password, with `validateservercertificate=0`.
  - Run `install.sh` twice with the install step's flags plus `--skip-slc --export-connection lh_export_ci`. The lowercase name exercises the upper-casing of task 2.2.
  - After the second run, query the views below with `exapump sql` and fail unless the role `LAKEHOUSE_EXPORT_ROLE_LHVS` holds exactly these six grants. Measured on Exasol 2025.1.16: the six grants appear in these views, and `CREATE OR REPLACE` of a script removes its `EXECUTE` and script-scoped `ACCESS` rows, so the second run proves that a re-install restores them.
    - `EXA_DBA_SYS_PRIVS`: `EXPORT`.
    - `EXA_DBA_CONNECTION_PRIVS`: `GRANTED_CONNECTION = 'LH_EXPORT_CI'`.
    - `EXA_DBA_OBJ_PRIVS`: `EXECUTE` on the scripts `LHVS.LAKEHOUSE_EXPORT` and `LHVS.LAKEHOUSE_EXPORT_WRITER`.
    - `EXA_DBA_RESTRICTED_OBJ_PRIVS`: `ACCESS` on the connection `LH_EXPORT_CI`, with `FOR_OBJECT_SCHEMA = 'LHVS'` and `FOR_OBJECT_NAME` each of the two scripts.
  - In the same step, assert from `EXA_ALL_SCRIPTS` that `LHVS.LAKEHOUSE_EXPORT` is a `RUST` script with `SCRIPT_INPUT_TYPE = 'SCALAR'` and `SCRIPT_RESULT_TYPE = 'EMITS'`, and `LHVS.LAKEHOUSE_EXPORT_WRITER` one with `SET` and `RETURNS`. Creating these scripts over a release `.so` that lacks the two entry points succeeds (measured), so the job needs no branch-built `.so`.
  - Update the job's leading comment: it now also proves the export role grants.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Export entry points | 1.1-1.14 | none | spec deltas `export/export-into-script` (all scenarios except the four installer scenarios), `packaging/single-so-two-entry-points`; `Cargo.toml`, `crates/lakehouse-engine/src/lib.rs`, `crates/lakehouse-engine/src/export/`, `crates/lakehouse-engine/src/adapter/sharding.rs`, `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, `crates/lakehouse-e2e/tests/e2e_local/{main.rs,export.rs,two_entry_points.rs}`, `crates/lakehouse-e2e/tests/common/e2e_harness.rs`, `crates/lakehouse-e2e/tests/cloud_e2e_test.rs`, `specs/testing.md`, `deploy/README.md`, `docs/export.md`, `docs/index.md` |
| B: Installer and security docs | 2.1-2.6 | none | spec delta `packaging/version-udf`, the four installer scenarios and § Background of `export/export-into-script`; `deploy/scripts/install.sh`, `deploy/scripts/tests/install.test.sh`, `.github/workflows/ci.yml` (`install-script-e2e` job), `docs/install.md`, `docs/security.md` |

- Task 2.1 states the export DDL text once. Tasks 1.7 and 1.13 copy it from this plan, not from Group B's files, so the groups share no file.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test | `crates/lakehouse-e2e/tests/e2e_local/two_entry_points.rs` `so_exports_three_entry_point_symbols` | Renamed to `so_exports_five_entry_point_symbols` with five symbols |
| Test | `deploy/scripts/tests/install.test.sh` `test_four_scripts_ddl_saas_path_types` | Renamed to `test_six_scripts_ddl_saas_path_types` |
| Constant | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` `MAX_SHARD_COUNT` | Moved into `sharding::group_count` |

## Verification

### Scenario Coverage

`E` is `crates/lakehouse-e2e/tests`.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A whole-table export reports every source row | Integration | `E/e2e_local/export.rs` | `export_whole_table_reports_every_row` |
| A filtered query exports only the rows it returns | Integration | `E/e2e_local/export.rs` | `export_filtered_query_reports_returned_rows` |
| A column-list export reports every row of the table | Integration | `E/e2e_local/export.rs` | `export_column_list_reports_every_row` |
| A join of a virtual schema table and an Exasol table exports the joined rows | Integration | `E/e2e_local/export.rs` | `export_vs_join_reports_joined_rows` |
| An empty source exports zero rows | Integration | `E/e2e_local/export.rs` | `export_empty_source_reports_zero` |
| An empty source with an unsupported column type exports zero rows | Integration | `E/e2e_local/export.rs` | `export_empty_unsupported_type_source_reports_zero` |
| WRITERS_PER_NODE sets the writer count | Integration | `E/e2e_local/export.rs`; `E/cloud_e2e_test.rs` | `export_writers_per_node_sets_writer_count`; `cloud_export_writer_counts` |
| The writer count never exceeds 300 | Integration + Unit | `E/e2e_local/export.rs`; `crates/lakehouse-engine/src/adapter/sharding_tests.rs` | `export_writer_count_caps_at_300`; `group_count_caps_at_300` |
| SINGLE_COMMIT runs one writer for the whole cluster | Integration | `E/e2e_local/export.rs`; `E/cloud_e2e_test.rs` | `export_single_commit_runs_one_writer`; `cloud_export_writer_counts` |
| WRITERS_PER_NODE together with SINGLE_COMMIT is refused | Integration | `E/e2e_local/export.rs` | `export_refuses_writers_per_node_with_single_commit` |
| An invalid option fails the export before any data is read | Integration | `E/e2e_local/export.rs` | `export_refuses_invalid_options_before_reading` |
| An export without a named CONNECTION fails before any data is read | Integration | `E/e2e_local/export.rs` | `export_refuses_missing_or_inline_connection` |
| TRUNCATE, REPLACE, and CREATED BY are refused | Integration | `E/e2e_local/export.rs` | `export_refuses_truncate_replace_created_by` |
| A column of an unsupported type fails the export naming the column and its type | Integration | `E/e2e_local/export.rs` | `export_refuses_unsupported_column_types` |
| A user who holds the export privileges exports rows | Integration | `E/e2e_local/export.rs` | `export_role_user_exports_rows` |
| A missing script-scoped CONNECTION grant fails with a named error | Integration | `E/e2e_local/export.rs` | `export_missing_script_grant_fails_named` |
| No CONNECTION value appears in the generated SQL or an error | Unit + Integration | `crates/lakehouse-engine/src/export/export_tests.rs`; `E/e2e_local/export.rs` | `generated_sql_and_errors_carry_no_connection_value`; `export_errors_carry_no_connection_password` |
| The installer creates the export scripts and prints the export role | E2E (CI) + Unit (installer) | `.github/workflows/ci.yml` job `install-script-e2e`; `deploy/scripts/tests/install.test.sh` | step `Export role grants (install.sh --export-connection, run twice)`; `test_six_scripts_ddl_saas_path_types`; `test_template_prints_export_role` |
| The installer grants the export role for a named export CONNECTION | E2E (CI) + Unit (installer) | `.github/workflows/ci.yml` job `install-script-e2e`; `deploy/scripts/tests/install.test.sh` | step `Export role grants (install.sh --export-connection, run twice)`; `test_export_connection_grants_role` |
| The installer prints the export role when the named export CONNECTION does not exist | Unit (installer) | `deploy/scripts/tests/install.test.sh` | `test_export_connection_grants_role` |
| The installer accepts only a simple identifier as the export CONNECTION name | E2E (CI) + Unit (installer) | `.github/workflows/ci.yml` job `install-script-e2e`; `deploy/scripts/tests/install.test.sh` | step `Export role grants (install.sh --export-connection, run twice)` (lowercase name); `test_export_connection_name_validation` |
| One crate exports every Rust entry point | Integration | `E/e2e_local/two_entry_points.rs` | `so_exports_five_entry_point_symbols` |
| All scripts resolve from the same uploaded artifact | Integration | `E/e2e_local/version_udf.rs`; `E/e2e_local/export.rs` | `version_udf_returns_the_compiled_crate_version` and the existing scan tests; `export_whole_table_reports_every_row` |
| The install script creates the version script with the other deployment scripts | Unit (installer) | `deploy/scripts/tests/install.test.sh` | `test_six_scripts_ddl_saas_path_types` |
| The install documentation describes the version-based verification | Unit (installer) | `deploy/scripts/tests/install.test.sh` | `test_docs_describe_version_verification` |

- A unit test proves the SQL half of the credentials scenario, because Exasol SQL cannot observe the generated statement (`specs/testing.md` § Coverage rule). The architecture constraint that the export scripts emit no script output rests on code review.
- The multi-node half of the SINGLE_COMMIT and WRITERS_PER_NODE scenarios runs only in the opt-in `cloud-e2e` suite, through task 1.13. The single-node E2E test proves one writer invocation for the whole statement.
- The `install-script-e2e` CI job (task 2.6) proves on a live Exasol that `install.sh` creates both export scripts and that a re-run leaves the role's six grants in place. The template text and the failure paths (missing CONNECTION, failed grant, invalid name) run against the `exapump` stub in `make test-install`.
- The CI job installs the latest release `.so`, so it proves the installer, not the branch's entry points. `export_role_user_exports_rows` proves on the local stack that the role's grants let a user export.

### Manual Testing

Run against the local container after `make e2e-up test-e2e`, which leaves schema `LHVS` with the branch-built scripts and the CONNECTION `LAKEHOUSE_CATALOG_CREDS`. `$DSN` is `exasol://sys:exasol@localhost:28563?validateservercertificate=0`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| EXPORT INTO SCRIPT Entry Points | `exapump sql "CREATE OR REPLACE TABLE LHVS.T AS SELECT LEVEL AS id FROM DUAL CONNECT BY LEVEL <= 1000" -d "$DSN"` then `exapump sql "EXPORT LHVS.T INTO SCRIPT LHVS.LAKEHOUSE_EXPORT AT LAKEHOUSE_CATALOG_CREDS WITH TABLE_NAME = 'ns.t' WRITERS_PER_NODE = '3'" -d "$DSN"` | 1000 rows affected |
| EXPORT INTO SCRIPT Entry Points | `exapump sql "EXPORT LHVS.T INTO SCRIPT LHVS.LAKEHOUSE_EXPORT AT LAKEHOUSE_CATALOG_CREDS WITH TABLE_NAME = 'ns.t' TRUNCATE" -d "$DSN"` | Error naming `TRUNCATE` and stating that the export only appends rows |
| EXPORT INTO SCRIPT Entry Points | `exapump sql "EXPORT (SELECT CAST('POINT(1 2)' AS GEOMETRY) AS g FROM DUAL) INTO SCRIPT LHVS.LAKEHOUSE_EXPORT AT LAKEHOUSE_CATALOG_CREDS WITH TABLE_NAME = 'ns.t'" -d "$DSN"` | Error containing `column "G" has type GEOMETRY` |
| Single Shared Object, Five Entry Points | `nm -D target/release/liblakehouse_engine.so \| grep __exa_udf_entry_` | Five symbols, including `__exa_udf_entry_LAKEHOUSE_EXPORT` and `__exa_udf_entry_LAKEHOUSE_EXPORT_WRITER` |
| Version Query UDF | `exapump sql "SELECT SCRIPT_NAME FROM EXA_ALL_SCRIPTS WHERE SCRIPT_SCHEMA = 'LHVS' ORDER BY 1" -d "$DSN"` | Six scripts, including `LAKEHOUSE_EXPORT` and `LAKEHOUSE_EXPORT_WRITER` |
| EXPORT INTO SCRIPT Entry Points | `deploy/scripts/install.sh --dsn "$DSN" --schema LHX --skip-slc --export-connection LAKEHOUSE_CATALOG_CREDS` then `exapump sql "SELECT PRIVILEGE FROM EXA_DBA_SYS_PRIVS WHERE GRANTEE = 'LAKEHOUSE_EXPORT_ROLE_LHX'" -d "$DSN"` | Install succeeds; the query returns `EXPORT`. Run it last: it uploads the release `.so`, so run `make bucketfs-upload-so` afterwards |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make e2e-up test-e2e` | 0 failures, `e2e_local::export` included |
| Cloud E2E | cloud suite command of `deploy/README.md:76`, filtered to `cloud_export_writer_counts` (task 1.13) | Passes on a multi-node cluster |
| Installer | `make test-install` | 0 failures |
| Installer E2E | CI job `install-script-e2e` on the PR (task 2.6) | Passes, including the export role grants step |
| Gate | `make fmt-check lint` | Exit 0, zero warnings |
| Coverage | `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info` then `python3 .claude/skills/test-strategy-review/scripts/changed_line_coverage.py <scratchpad>/lcov.info` | Changed-line coverage at least 90% |

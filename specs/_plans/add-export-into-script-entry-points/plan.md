# Plan: add-export-into-script-entry-points

## Summary

The installer creates one script, `LAKEHOUSE_EXPORT`, that answers `EXPORT ... INTO SCRIPT` and also runs as the writer: the SELECT it returns calls `LAKEHOUSE_EXPORT` again over the source rows, and each writer invocation counts the rows it receives. Exasol reports the exported row count, and nothing is written to the lakehouse (#565). The export checks its options and CONNECTION before any source row is read. It spreads rows over node count × `WRITERS_PER_NODE` writers, or over one writer with `SINGLE_COMMIT`. It refuses columns that Exasol cannot pass to a UDF, with an error that names the column or with Exasol's own type error. The installer prints the export grants as a template, as it does for the virtual schema grants.

## Context

- Issue #565 is the first of eight export issues. It proves that Exasol runs the export query correctly and that every row reaches exactly one writer. Writing data and performance work are out of scope.
- Measured on the local Exasol 2025.1.16 container with a spike `.so` built against the pinned SDK version (0.33.0) in the Makefile's `UDF_BUILDER_IMAGE` and loaded on the SLC of the same version (probe SQL and results in `notes/planning.md`):
  - Exasol calls the export script once with the options, the source column names, the TRUNCATE, REPLACE, and CREATED BY flags, and either a CONNECTION name (`AT <name>`) or an inline address with user and password. It passes no column types.
  - The answer is one `SELECT` over `DUAL`, which Exasol binds to the export source. Its single column must be `DECIMAL(18,0)`, and Exasol reports the sum of its rows as the row count. An empty source with supported column types reports 0.
  - The SDK's `export` hook answers the call. `ctx.node_count()`, `ctx.script_schema()`, `ctx.current_user()` (the session user), and `ctx.connection()` work inside it, and `ctx.current_user()` and `ctx.connection()` also work in a SET writer. A missing script grant fails `ctx.connection()` in the hook with `insufficient privileges for using connection <name> in script <script>`.
  - One `RUST SET SCRIPT ... RETURNS DECIMAL(18,0)` serves both roles, also on Exasol 8.29.13. Exasol calls its export hook for `EXPORT ... INTO SCRIPT`, and the SELECT the hook returns can call the same script, which then runs its `run` function once per group. The writer sees the same script name, schema, user, node count, and input column types whether the generated SELECT or a user's own SELECT calls it. Only its first argument, the writer spec, marks a generated call. Exasol refuses a call without arguments (`missing input parameters for SET UDF script`).
  - In a SET group, the first row is readable when the writer starts, and the first `ctx.next()` returns true without advancing. `while ctx.next()? { n += 1 }` therefore counts the group's rows exactly, at one group and at four.
  - The writer accepts BOOLEAN, DATE, TIMESTAMP(3), TIMESTAMP(9), DOUBLE, CHAR, VARCHAR, and DECIMAL up to precision 36. The SLC truncates TIMESTAMP(9) values to microseconds, which does not change row counts.
  - `EXPORT` needs the `EXPORT` system privilege. `AT <connection>` needs the user's `CONNECTION` privilege. A script that reads the CONNECTION needs `GRANT ACCESS ON CONNECTION ... FOR SCRIPT <that script>`. All of these work through one role, and one `ACCESS` grant on the export script covers the hook and the writers.
  - Without a check, Exasol's handling of a GEOMETRY, HASHTYPE, INTERVAL, or TIMESTAMP WITH LOCAL TIME ZONE source column varies between runs of the same statement: it refuses the column with a message that does not name it, or it exports the row. Decision [2] places the check in the generated SQL.
  - The number of writer invocations is visible as `OUT_ROWS` of the `GROUP BY` part in `EXA_USER_PROFILE_LAST_DAY` after `ALTER SESSION SET PROFILE = 'ON'` and `FLUSH STATISTICS`.
  - The group key `MOD(ROWNUM, G)` runs exactly G writers at G = 1, 4, and 300, also for a table and a join query of exactly G rows, and one writer per row for a source of fewer than G rows (decision [2]).
  - `EXPORT <virtual table>` is refused by Exasol ("Use EXPORT (SELECT * FROM ...) instead"). A query over a virtual schema table joined with an Exasol table exports correctly.
- Deviation from #565: its criterion "The installer creates both scripts and grants them access to the catalog CONNECTION" is mistaken, and the requester will reword #565. The installer creates one export script and prints the export grants as a template, because it runs before any CONNECTION exists (decision log § Review Findings).
- Deviation from #565: #565 states that a column of an unsupported type fails the export with an error that names the column and its type. Exasol's own UDF type check races the export's check, so the export is refused either with the named error (`LAKEHOUSE_EXPORT: column "<name>" has type <type>, ...`) or with Exasol's `[22002] Column type not supported: <type>`, which does not name the column. Which one a run returns is not deterministic: a one-row table with five unsupported columns gave the named error in 6 of 10 runs and Exasol's in 4 of 10 on 2025.1.16. An empty source with such a column errors in 3 to 9 of 10 runs and exports 0 rows otherwise, so the export makes no promise about it. The requester rewords #565, and the PR description names this exception.
- The local E2E stack is a single Exasol node, where SINGLE_COMMIT and the default both run one writer. Only the opt-in `cloud-e2e` suite on a multi-node cluster proves #565's criterion that "a multi-node cluster confirms that only one writer runs".
- The architecture delta (`architecture.md` in this directory) adds the `export` component, the export data flow, the export script interface, and the writer-count constraint.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| EXPORT INTO SCRIPT Entry Points | NEW | `export/export-into-script/spec.md` |
| Single Shared Object, Four Entry Points | CHANGED | `packaging/single-so-two-entry-points/spec.md` |
| Version Query UDF | CHANGED | `packaging/version-udf/spec.md` |

## Impact

- New SQL surface: `EXPORT ... INTO SCRIPT <schema>.LAKEHOUSE_EXPORT AT <connection> WITH TABLE_NAME = '...'` reports the exported row count and writes nothing to the lakehouse.
- `install.sh` creates one more script, `LAKEHOUSE_EXPORT`, in the target schema. Its next-step template prints the statements of `LAKEHOUSE_EXPORT_ROLE_<schema>` for the template's CONNECTION, and the installer runs none of them. `CREATE OR REPLACE` drops the script grants of a replaced script, so after a re-install the operator runs the role's `GRANT EXECUTE` and `GRANT ACCESS` lines again, as for the virtual schema grants.
- `install.sh` issues the export DDL for every resolved release version. The export script of a release older than this change fails when called, because its `.so` lacks the entry point.
- No breaking change for virtual schema queries.

## Dependencies

- `exasol-udf-sdk` gains the `export` cargo feature in `crates/lakehouse-engine/Cargo.toml` only, because only the engine crate declares the export entry point. The root `[workspace.dependencies]` pin keeps its version and `emit-arrow`. The engine's dev-dependency already enables `test-support`.
- The `cloud-e2e` proof needs a multi-node Exasol cluster that accepts a BucketFS upload (task 1.13).

## Open Questions

- Multi-node writer-count proof: if no multi-node cluster can be provisioned before the PR leaves draft, the requester decides whether #565's criterion "a multi-node cluster confirms that only one writer runs" may ship unverified (#TBD).
- Type-guard and group-key cost: a follow-up issue measures the per-row cost of the type guard and of the `ROWNUM` group key on a multi-node cluster, and revisits both in the export performance work (#TBD, decision [2]).

## Implementation Tasks

### 1. Export entry points and their proof

- [ ] 1.1 Enable the `export` feature of `exasol-udf-sdk` in `crates/lakehouse-engine/Cargo.toml` (`{ workspace = true, features = ["export"] }`), keeping the root `[workspace.dependencies]` pin and its version unchanged.
- [ ] 1.2 In `export.rs`, add a private `writer_count(node_count: usize, per_node: usize) -> usize` that multiplies with `saturating_mul` and clamps to between 1 and 300, and add `writer_count_caps_at_300` to `export_tests.rs` (`sql` section), covering `writer_count(1, 1) == 1`, `writer_count(2, 0) == 1`, `writer_count(4, 100) == 300`, and `writer_count(usize::MAX, usize::MAX) == 300`. The scan's `shard_count` and `adapter/sharding.rs` stay unchanged.
- [ ] 1.3 In `crates/lakehouse-engine/src/export.rs`, add option validation: turn an SDK `ExportSpec` into validated export options or a `UdfError::User` that names the option and its accepted form. Cover: TRUNCATE, REPLACE, and CREATED BY refused with an append-only message naming the clause; no CONNECTION name refused with a message asking for `AT <connection>`, and an inline CONNECTION refused without echoing `connection_information`; `TABLE_NAME` missing or with an empty part; an unknown option, with the accepted list; `WRITERS_PER_NODE` not an integer of at least 1 that fits a `usize`; `SINGLE_COMMIT` not `true` or `false` in any case; `WRITERS_PER_NODE` with `SINGLE_COMMIT = 'true'`, naming both. Option names arrive upper-cased from Exasol. The `options` section of `export_tests.rs` holds every invalid and valid case as tables, including `WRITERS_PER_NODE = '18446744073709551615'` (`usize::MAX`, accepted) and `'18446744073709551616'` (refused with the integer message).
- [ ] 1.4 In `export.rs`, add the writer `SELECT` renderer, which renders the writer `SELECT` [expert]:
  - Shape: `SELECT "<schema>"."LAKEHOUSE_EXPORT"('<writer spec>', <source columns>) FROM DUAL WHERE <guards> GROUP BY MOD(ROWNUM, <G>)`. The key stays in the top-level `GROUP BY`, because an export SELECT over a subquery of `DUAL` fails (decision [2]). The schema comes from `ctx.script_schema()`. G is 1 for `SINGLE_COMMIT = 'true'`, otherwise `writer_count(ctx.node_count(), WRITERS_PER_NODE)`.
  - The writer spec is a JSON object that carries only the CONNECTION name. The SELECT calls the export script itself, so `run` (task 1.6) is the writer.
  - One guard per source column: `(CASE WHEN TYPEOF(c) LIKE 'GEOMETRY%' OR TYPEOF(c) LIKE 'HASHTYPE%' OR TYPEOF(c) LIKE 'INTERVAL%' OR TYPEOF(c) LIKE '%WITH LOCAL TIME ZONE' THEN CAST('LAKEHOUSE_EXPORT: column "<name>" has type ' || TYPEOF(c) || ', which the export does not support; cast it to a supported type' AS DECIMAL(1,0)) ELSE 0 END) = 0`. The message must concatenate `TYPEOF(c)`: Exasol folds a constant `CAST` eagerly and fails every export.
  - `<name>` is the last quoted identifier of the `source_column_names` entry, with `""` unescaped. Every string literal doubles its `'`.
  - Unit tests in the `sql` section of `export_tests.rs`: the shape for one and several columns, the group key and G for each mode and at the cap, and a column name and a CONNECTION name that contain `'` and `"`.
- [ ] 1.5 Add `crates/lakehouse-engine/src/export.rs`, the single `export` module (production code and a sibling `export_tests.rs`), with two public functions:
  - `generate_export_sql(ctx, json)` parses the spec with `ExportSpec::from_json`, validates it (1.3), and reads the CONNECTION with `ctx.connection(name)`, discarding the value at once. On failure it returns a named error that names the CONNECTION, `<schema>.LAKEHOUSE_EXPORT`, the `ctx.current_user()` grantee, and the missing `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` statement. Then it renders the SQL (1.4).
  - `run_writer(ctx)` reads the writer spec from column 0 of the first row. A column 0 that is not a string, or a string that is not a writer spec, fails with an error saying that `LAKEHOUSE_EXPORT` runs as the target of `EXPORT ... INTO SCRIPT`; measured, the SDK's `get_string` on an integer column fails with `Type error: expected string, got Int64(1)`, so `run_writer` maps that error. It then reads the CONNECTION with the same named-error pattern, which is the only CONNECTION check on a direct call, and counts the group's rows with `while ctx.next()? { n += 1 }`. It returns the count.
  - The module emits no log event and no script output.
  - Unit tests in `export_tests.rs` use the SDK `TestContext`, built with `TestContext::set(rows)` and `with_input_columns`, `with_connection`, `with_node_count`, `with_script_schema`, and `with_current_user`: a valid spec yields SQL without the sentinel password; each refusal and the missing-grant error carry no sentinel value; the writer returns the exact row count of a multi-row group; the writer refuses an integer column 0 and a string that is not a writer spec.
- [ ] 1.6 Declare the entry point in `crates/lakehouse-engine/src/lib.rs` next to the three at :22-34: `#[exasol_udf(name = "LAKEHOUSE_EXPORT", export_spec(export::generate_export_sql))]` on a function that returns `Result<Option<i64>, UdfError>` and calls `export::run_writer`. The SDK macro picks RETURNS from the `Option` return type and wires the hook independently (spike measured). Change the module doc (line 1) to four entry points. In `crates/lakehouse-e2e/tests/e2e_local/two_entry_points.rs`, rename `so_exports_three_entry_point_symbols` to `so_exports_four_entry_point_symbols`, add the export symbol to the contains-asserts (:29-40), change the count assert and its message (:45-50) to 4, and add `/// Scenario: One crate exports every Rust entry point`.
- [ ] 1.7 In `crates/lakehouse-e2e/tests/common/e2e_harness.rs`, add `EXPORT_SCRIPT_NAME` next to the constants at :26-35, and add the DDL statement of task 2.1 to `create_schema_and_scripts_with_env` (:144-185). Update the script list in `specs/testing.md` § Shared harness (:67) to name the version script and the export script.
- [ ] 1.8 Add `crates/lakehouse-e2e/tests/e2e_local/export.rs` and declare `mod export;` in `e2e_local/main.rs` in alphabetical order. Each test calls `fixtures::stack()` and then a module-local lazy fixture (a `OnceLock`, as in `credential_exposure.rs:42`) that calls `fixtures::lakehouse_vs()` and creates the module's Exasol tables. Exports name `LAKEHOUSE_CATALOG_CREDS`, the CONNECTION of the shared virtual schema. Error assertions read the message through `common::exasol_ws::error_text`. Each test carries the `/// Scenario:` line of the scenario it proves (Scenario Coverage table).
- [ ] 1.9 Row-count and writer-count tests in `export.rs`:
  - Sources: a 1,000-row Exasol table with one column of each supported type (BOOLEAN, DATE, TIMESTAMP(3), TIMESTAMP(9), DOUBLE, CHAR(10), VARCHAR(100), DECIMAL(9,0), DECIMAL(18,0), DECIMAL(36,4)) and an integer filter column; an empty query over it; and the virtual schema's `events` table joined with an Exasol table.
  - A profile helper in the module enables `PROFILE`, runs the export, runs `FLUSH STATISTICS`, and reads the export statement's `GROUP BY` `OUT_ROWS` from `EXA_USER_PROFILE_LAST_DAY`. Every writer-count assertion is exact, and each export also asserts its row count:
    - `export_reports_every_source_row` is one table-driven test over the whole table, a filtered query, a column list, the virtual schema join, and the empty query.
    - `export_writer_layout_sets_writer_count` is one table-driven test, each case labelled: `WRITERS_PER_NODE = '3'` runs 3 writers over the 1,000-row table and over a query that returns exactly 3 rows, and 2 writers over a query that returns 2 rows; the default runs 1 writer on one node; `WRITERS_PER_NODE = '500'` runs 300 writers over a 10,000-row table and over a query that returns exactly 300 rows; `SINGLE_COMMIT = 'true'` (G = 1) runs 1 writer over the 1,000-row table.
- [ ] 1.10 Refusal tests in `export.rs`:
  - One table-driven test, `export_refuses_invalid_statements_before_reading`, labels each case and covers the invalid options, the missing and the inline CONNECTION, `TRUNCATE`, `REPLACE`, `CREATED BY`, and `WRITERS_PER_NODE` with `SINGLE_COMMIT`. "Before any data is read" uses a source query whose rows raise an error when read, `SELECT 1/(a - a) AS x FROM <table>`, and asserts the option error, not the division error. One positive control exports the same source with valid options and asserts the division error. Each case uses one invalid example; the exhaustive table stays in the `options` section of `export_tests.rs`.
  - The type test uses a one-row table with one column of each unsupported type. It exports a column-list entry, the whole table, and a query alias, and asserts that each export fails with the named error or with Exasol's `Column type not supported`. It makes one attempt per export.
- [ ] 1.11 Privilege and credential tests in `export.rs`: module-local helpers create non-SYS users, as `credential_exposure.rs:49-62` does, and the export role with the statements the template of task 2.2 prints. One user exports successfully. One lacks `ACCESS` for `LAKEHOUSE_EXPORT` and fails before any row is read, shown through the failing-source query. Every error message collected in the module is asserted free of the CONNECTION password.
- [ ] 1.12 In `crates/lakehouse-e2e/tests/cloud_e2e_test.rs`, add `cloud_export_writer_counts`, one test that exports a 10,000-row table through `CLOUD_LHVS.LAKEHOUSE_EXPORT` on the real cluster. It reads `NPROC()` and asserts, through the same profile reading, exactly 1 writer for `SINGLE_COMMIT = 'true'`, `NPROC()` writers by default, and 2 × `NPROC()` writers for `WRITERS_PER_NODE = '2'` (when at most 300), each with the full row count. A second `WRITERS_PER_NODE = '2'` export over a query that returns exactly 2 × `NPROC()` rows asserts 2 × `NPROC()` writers, which proves gap-free `ROWNUM` numbering across nodes. It exports `AT GLUE_CATALOG_CREDS` (`CLOUD_CATALOG_CONN`, :52) and creates that CONNECTION first with `build_create_connection_sql`, as `setup_cloud_vs` (:383-388) does. When `LH_EXASOL_USER` is not `SYS`, it grants that user `ACCESS ON CONNECTION GLUE_CATALOG_CREDS FOR SCRIPT CLOUD_LHVS.LAKEHOUSE_EXPORT`. It skips through `CloudEnv::from_env()` like the other cloud tests, and fails naming `CLOUD_LHVS.LAKEHOUSE_EXPORT` when that script is missing.
- [ ] 1.13 Run `cloud_export_writer_counts` on a multi-node cluster before the PR leaves draft. Upload the branch-built `.so` and the SLC of the pinned version with `make install-slc bucketfs-upload-so EXASOL_HOST=<cluster> LH_EXASOL_PORT=<sql port> LH_BUCKETFS_PORT=<port> EXASOL_SYS_PASSWORD=<sys password> BUCKETFS_WRITE_PASS=<pass>`, the path `bench/run.sh:433-436` uses. Create `CLOUD_LHVS.LAKEHOUSE_EXPORT` with the DDL of task 2.1. Run the cloud suite command of `deploy/README.md:76` filtered to `cloud_export_writer_counts`, and record the node count and the writer counts in the verification report. Add these three steps to the cloud section of `deploy/README.md` (:62-77). If no multi-node cluster is available, raise the Open Question instead of marking the scenario verified.
- [ ] 1.14 Add `docs/export.md` and link it from the `docs/index.md` table (:27-38). It covers the statement and its options, the export role and the template lines that create it, writer counts, the absence of lakehouse writes in this release, the unsupported types with the cast advice and both refusal errors, the empty-source behavior (0 rows for supported types, not promised for unsupported ones), and wrapping a virtual table in `EXPORT (SELECT * FROM ...)`.

### 2. Installer and security documentation

- [ ] 2.1 In `deploy/scripts/install.sh`, add `ddl_export` next to the `ddl_*` functions (:1182-1200), and add it to the `create_engine_scripts` statements array (:1267-1273) after `ddl_version`. Its text, which tasks 1.7 and 1.13 reuse:

  ```sql
  CREATE OR REPLACE RUST SET SCRIPT <schema>.LAKEHOUSE_EXPORT(...) RETURNS DECIMAL(18,0) AS
  %udf_object <so>
  /
  ```

- [ ] 2.2 In `print_next_step_template` (:1346-1402), after the virtual schema block, print an export block for every installing user, including `SYS`. It names the template's CONNECTION `LAKEHOUSE_CATALOG_CREDS` (:1354), so one CONNECTION serves the virtual schema and the export. The installer runs none of these lines.
  - A comment: run as a DBA after the CONNECTION exists; after a re-install, run the `GRANT EXECUTE` and `GRANT ACCESS` lines again; grant the role only to users who may use the CONNECTION's credentials.
  - `CREATE ROLE LAKEHOUSE_EXPORT_ROLE_<schema>;`, then `GRANT EXPORT`, `GRANT CONNECTION LAKEHOUSE_CATALOG_CREDS`, `GRANT EXECUTE ON SCRIPT <schema>.LAKEHOUSE_EXPORT`, and `GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT <schema>.LAKEHOUSE_EXPORT`, each `TO LAKEHOUSE_EXPORT_ROLE_<schema>;`.
  - `-- Exporters: GRANT LAKEHOUSE_EXPORT_ROLE_<schema> TO <user>;`.
- [ ] 2.3 In `deploy/scripts/tests/install.test.sh`:
  - Rename `test_four_scripts_ddl_saas_path_types` (:1069-1099) to `test_five_scripts_ddl_saas_path_types` and assert the export DDL statement.
  - Add `test_template_prints_export_role`: the template prints the export block for a non-SYS and a `SYS` installing user, and the `exapump` stub (:135-280) records no `CREATE ROLE` and no `GRANT` statement.
  - Extend `test_docs_describe_version_verification` (:1189-1203) with the export DDL statement of the by-hand appendix. Register the new test in `main()` (:2468-2551).
- [ ] 2.4 Update `docs/install.md`: five scripts in "What the command does" (:121-122), the script and entry-point counts at :368, :468 ("four RUST entry points"), and :520, the export DDL in "Appendix: create the scripts by hand" (:481-503), and one line in § Point the VS (:188-223) that points to the export grants in `docs/export.md`.
- [ ] 2.5 Update `docs/security.md`:
  - Add an "Export grants" subsection to § Privilege model: exporting users hold the export role, not the engine role; the role's four privileges; the installer prints them and runs none; the `CONNECTION` privilege lets the user name the CONNECTION in any `IMPORT` or `EXPORT` statement, so grant the role only to users who may use the CONNECTION's credentials.
  - Reconcile § Reader grants (:22) with the export role: the warning applies to `ACCESS` on the CONNECTION for `LAKEHOUSE_SCAN`, which the export role does not grant, and a reader that exports holds the export role.
  - Add "Who can bypass the export script" after § Who can bypass the adapter (:101-108): a holder of the export role can call `LAKEHOUSE_EXPORT` directly in a `SELECT` with a forged writer spec and any rows. That call runs the writer and skips every check the export call makes (options, type guards, writer count, SINGLE_COMMIT), so the writer treats its writer spec as untrusted input.
  - Extend § Re-installing (:78): the re-install also drops the export role's `EXECUTE` and `ACCESS` grants on `LAKEHOUSE_EXPORT`; run those two template lines again.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Export entry points | 1.1-1.14 | none | spec deltas `export/export-into-script` (all scenarios except the installer scenario), `packaging/single-so-two-entry-points`; `crates/lakehouse-engine/Cargo.toml`, `crates/lakehouse-engine/src/lib.rs`, `crates/lakehouse-engine/src/export.rs`, `crates/lakehouse-engine/src/export_tests.rs`, `crates/lakehouse-e2e/tests/e2e_local/{main.rs,export.rs,two_entry_points.rs}`, `crates/lakehouse-e2e/tests/common/e2e_harness.rs`, `crates/lakehouse-e2e/tests/cloud_e2e_test.rs`, `specs/testing.md`, `deploy/README.md`, `docs/export.md`, `docs/index.md` |
| B: Installer and security docs | 2.1-2.5 | none | spec delta `packaging/version-udf`, the installer scenario and § Background of `export/export-into-script`; `deploy/scripts/install.sh`, `deploy/scripts/tests/install.test.sh`, `docs/install.md`, `docs/security.md` |

- Task 2.1 states the export DDL text once. Tasks 1.7 and 1.13 copy it from this plan, not from Group B's files, so the groups share no file.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test | `crates/lakehouse-e2e/tests/e2e_local/two_entry_points.rs` `so_exports_three_entry_point_symbols` | Renamed to `so_exports_four_entry_point_symbols` with four symbols |
| Test | `deploy/scripts/tests/install.test.sh` `test_four_scripts_ddl_saas_path_types` | Renamed to `test_five_scripts_ddl_saas_path_types` |

## Verification

### Scenario Coverage

`E` is `crates/lakehouse-e2e/tests`.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A whole-table export reports every source row | Integration | `E/e2e_local/export.rs` | `export_reports_every_source_row` |
| A filtered query exports only the rows it returns | Integration | `E/e2e_local/export.rs` | `export_reports_every_source_row` |
| A column-list export reports every row of the table | Integration | `E/e2e_local/export.rs` | `export_reports_every_source_row` |
| A join of a virtual schema table and an Exasol table exports the joined rows | Integration | `E/e2e_local/export.rs` | `export_reports_every_source_row` |
| An empty source exports zero rows | Integration | `E/e2e_local/export.rs` | `export_reports_every_source_row` |
| WRITERS_PER_NODE sets the writer count | Integration | `E/e2e_local/export.rs`; `E/cloud_e2e_test.rs` | `export_writer_layout_sets_writer_count`; `cloud_export_writer_counts` |
| The writer count never exceeds 300 | Integration + Unit | `E/e2e_local/export.rs`; `crates/lakehouse-engine/src/export_tests.rs` | `export_writer_layout_sets_writer_count`; `writer_count_caps_at_300` |
| SINGLE_COMMIT runs one writer for the whole cluster | Integration | `E/e2e_local/export.rs`; `E/cloud_e2e_test.rs` | `export_writer_layout_sets_writer_count`; `cloud_export_writer_counts` |
| WRITERS_PER_NODE together with SINGLE_COMMIT is refused | Integration | `E/e2e_local/export.rs` | `export_refuses_invalid_statements_before_reading` |
| An invalid option fails the export before any data is read | Integration | `E/e2e_local/export.rs` | `export_refuses_invalid_statements_before_reading` |
| An export without a named CONNECTION fails before any data is read | Integration | `E/e2e_local/export.rs` | `export_refuses_invalid_statements_before_reading` |
| TRUNCATE, REPLACE, and CREATED BY are refused | Integration | `E/e2e_local/export.rs` | `export_refuses_invalid_statements_before_reading` |
| A column of an unsupported type is refused | Integration | `E/e2e_local/export.rs` | `export_refuses_unsupported_column_types` |
| A user who holds the export privileges exports rows | Integration | `E/e2e_local/export.rs` | `export_role_user_exports_rows` |
| A missing script-scoped CONNECTION grant fails with a named error | Integration | `E/e2e_local/export.rs` | `export_missing_script_grant_fails_named` |
| No CONNECTION value appears in the generated SQL or an error | Unit + Integration | `crates/lakehouse-engine/src/export_tests.rs`; `E/e2e_local/export.rs` | `generated_sql_and_errors_carry_no_connection_value`; `export_errors_carry_no_connection_password` |
| The installer creates the export script and prints the export grants | Unit (installer) | `deploy/scripts/tests/install.test.sh` | `test_five_scripts_ddl_saas_path_types`; `test_template_prints_export_role` |
| One crate exports every Rust entry point | Integration | `E/e2e_local/two_entry_points.rs` | `so_exports_four_entry_point_symbols` |
| All scripts resolve from the same uploaded artifact | Integration | `E/e2e_local/version_udf.rs`; `E/e2e_local/export.rs` | `version_udf_returns_the_compiled_crate_version` and the existing scan tests; `export_reports_every_source_row` |
| The install script creates the version script with the other deployment scripts | Unit (installer) | `deploy/scripts/tests/install.test.sh` | `test_five_scripts_ddl_saas_path_types` |
| The install documentation describes the version-based verification | Unit (installer) | `deploy/scripts/tests/install.test.sh` | `test_docs_describe_version_verification` |

- A unit test proves the SQL half of the credentials scenario, because Exasol SQL cannot observe the generated statement (`specs/testing.md` § Coverage rule). The architecture constraint that the export scripts emit no script output rests on code review.
- The multi-node half of the SINGLE_COMMIT and WRITERS_PER_NODE scenarios runs only in the opt-in `cloud-e2e` suite, through task 1.13. The single-node E2E test proves one writer invocation for the whole statement.
- The installer unit tests in `make test-install` prove the export DDL and the template text against the `exapump` stub. `export_role_user_exports_rows` proves on the local stack that the template's role statements let a user export.

### Manual Testing

Run against the local container after `make e2e-up test-e2e`, which leaves schema `LHVS` with the branch-built scripts and the CONNECTION `LAKEHOUSE_CATALOG_CREDS`. `$DSN` is `exasol://sys:exasol@localhost:28563?validateservercertificate=0`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| EXPORT INTO SCRIPT Entry Points | `exapump sql "CREATE OR REPLACE TABLE LHVS.T AS SELECT LEVEL AS id FROM DUAL CONNECT BY LEVEL <= 1000" -d "$DSN"` then `exapump sql "EXPORT LHVS.T INTO SCRIPT LHVS.LAKEHOUSE_EXPORT AT LAKEHOUSE_CATALOG_CREDS WITH TABLE_NAME = 'ns.t' WRITERS_PER_NODE = '3'" -d "$DSN"` | 1000 rows affected |
| EXPORT INTO SCRIPT Entry Points | `exapump sql "EXPORT LHVS.T INTO SCRIPT LHVS.LAKEHOUSE_EXPORT AT LAKEHOUSE_CATALOG_CREDS WITH TABLE_NAME = 'ns.t' TRUNCATE" -d "$DSN"` | Error naming `TRUNCATE` and stating that the export only appends rows |
| EXPORT INTO SCRIPT Entry Points | `exapump sql "EXPORT (SELECT CAST('POINT(1 2)' AS GEOMETRY) AS g FROM DUAL) INTO SCRIPT LHVS.LAKEHOUSE_EXPORT AT LAKEHOUSE_CATALOG_CREDS WITH TABLE_NAME = 'ns.t'" -d "$DSN"` | Error containing `column "G" has type GEOMETRY` |
| EXPORT INTO SCRIPT Entry Points | `exapump sql "SELECT LHVS.LAKEHOUSE_EXPORT(1) FROM DUAL" -d "$DSN"` | Error stating that `LAKEHOUSE_EXPORT` runs as the target of `EXPORT ... INTO SCRIPT` |
| Single Shared Object, Four Entry Points | `nm -D target/release/liblakehouse_engine.so \| grep __exa_udf_entry_` | Four symbols, including `__exa_udf_entry_LAKEHOUSE_EXPORT` |
| Version Query UDF | `exapump sql "SELECT SCRIPT_NAME FROM EXA_ALL_SCRIPTS WHERE SCRIPT_SCHEMA = 'LHVS' ORDER BY 1" -d "$DSN"` | Five scripts, including `LAKEHOUSE_EXPORT` |
| EXPORT INTO SCRIPT Entry Points | `deploy/scripts/install.sh --dsn "$DSN" --schema LHX --skip-slc` | Install succeeds; the template prints `CREATE ROLE LAKEHOUSE_EXPORT_ROLE_LHX;` and one `GRANT ACCESS ON CONNECTION LAKEHOUSE_CATALOG_CREDS FOR SCRIPT LHX.LAKEHOUSE_EXPORT` line, and `EXA_DBA_ROLES` holds no such role. Run it last: it uploads the release `.so`, so run `make bucketfs-upload-so` afterwards |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make e2e-up test-e2e` | 0 failures, `e2e_local::export` included |
| Cloud E2E | cloud suite command of `deploy/README.md:76`, filtered to `cloud_export_writer_counts` (task 1.13) | Passes on a multi-node cluster |
| Installer | `make test-install` | 0 failures |
| Gate | `make fmt-check lint` | Exit 0, zero warnings |
| Coverage | `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info` then `python3 .claude/skills/test-strategy-review/scripts/changed_line_coverage.py <scratchpad>/lcov.info` | Changed-line coverage at least 90% |

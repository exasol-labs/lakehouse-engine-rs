# Plan: add-direct-storage-refresh-e2e

## Summary

Adds one staged E2E test that proves `ALTER VIRTUAL SCHEMA ... REFRESH` for `CATALOG_KIND = 'DIRECT_STORAGE'` across the seven storage changes of issue #479 (A1 to A7), and closes the six documentation gaps of part B. The plan changes no engine behavior: it adds spec scenarios for the behavior the live Docker Exasol shows, a test that pins it, and user documentation written from that test.

## Context

- Issue #479 is the authoritative scope. Its part A lists seven storage changes (A1 to A7), each with a "before REFRESH" and an "after REFRESH" expectation. Its part B lists six documentation items (B1 to B6).
- No E2E test runs a REFRESH against a direct-storage virtual schema. `e2e_refresh_test.rs` covers only the Iceberg REST kind. `incompatible_pair_fails_create_and_refresh_naming_column_and_files` in `e2e_direct_storage_test.rs` asserts only the `CREATE VIRTUAL SCHEMA` rejection.
- The two refresh scenarios of `vs-adapter/refresh-and-set-properties` name only an Iceberg namespace in their GIVEN steps, so a direct-storage test cannot cite them as written. Their Iceberg-only clauses (a dropped and a renamed column) have no REFRESH E2E test today. This plan keeps those clauses unchanged and does not add that proof.
- No scenario states what a query returns between a storage change and the next REFRESH (A2, A3, A4, A5, A6, A7 "before"), or that a failed REFRESH keeps the previous declaration (A5 "after").
- The accepted ADR `vs-refresh-reuses-create-virtual-schema-enumeration` states that `refresh` runs the `createVirtualSchema` enumeration and rebuilds `TABLE_MAP` from scratch. The new scenarios conform to it.
- Behavior changes are out of scope (#479). A defect found by a test becomes a separate `fix(...)` issue, and the documentation states the current behavior.
- Spec check (`AGENTS.md`): the plan changes no scanning, pushdown, or type handling. Direct storage reads raw Parquet and implements neither the Iceberg table spec nor the Delta protocol (`direct-storage/direct-storage-table-planning` Background). Parquet has no table specification. The B4 type table names Parquet annotations as the Apache Parquet format defines them in `LogicalTypes.md`: "Allowed bit width values are `8`, `16`, `32`, `64`, and sign can be `true` or `false`" (Numeric Types), "Scale must be zero or a positive integer less than or equal to the precision" (DECIMAL), "`unit` must be one of `MILLIS`, `MICROS`, or `NANOS`" (TIMESTAMP). `parquet.thrift` marks the INT96 physical type "deprecated, new Parquet writers should not write data in INT96". The JSON `VARCHAR(2000000)` rendering of LIST, MAP, and struct columns is a named trade-off forced by Exasol's missing nested types, not a gap.
- Architecture: no change. The plan adds tests and documentation and moves no component, boundary, interface, or data flow.

## Live Observations

Recorded against the local Docker stack on 2026-10-09: Exasol `exasol/docker-db:2025.1.16`, SeaweedFS, and a `.so` built by `make cross-udf-build` from `main` at `f0c8dd4` (`LAKEHOUSE_VERSION()` = `0.52.1`). The probe used its own script schema, BucketFS path, CONNECTION, virtual schemas, and base paths (`plan479_probe*`), so it did not touch the suite's objects, and every probe object was removed afterwards. The virtual schema left `MERGE_SCHEMA` and `HIVE_PARTITIONING` absent.

| Case | Before REFRESH | After REFRESH |
|------|----------------|---------------|
| A1 new directory `t_new/` | `SELECT * FROM VS.T_NEW` fails: `object VS.T_NEW not found` (42000) | `T_NEW` listed, returns `(100, 'n1')` |
| A2 second file in `t_append/` | New row returned at once: ids 1, 2, 3 | Same rows. Columns stay `ID`, `LABEL` |
| A3 file with `NEW_COL` in `t_evolve/` | `SELECT NEW_COL` fails: `object NEW_COL not found`. `SELECT ID, LABEL` returns the rows of both files | `NEW_COL VARCHAR(2000000)` declared. NULL for ids 1, 2, and `x3`, `x4` for the new file |
| A4 `QTY` stored as INT64 in `t_evolve/`, declared `DECIMAL(10,0)` | A value of `5000000000` (outside INT32, inside `DECIMAL(10,0)`) reads back unchanged. With a value of `50000000000` in the table, `SELECT ID, QTY` fails: `numeric value out of range: value 50000000000 ... is not in [ -9999999999 .. 9999999999 ]` (22002). Top-N, `DISTINCT`, `GROUP BY`, `MIN`/`MAX`, and `QTY + 0` fail the same way. `SUM`, `AVG`, `COUNT(DISTINCT)`, and a filter on `QTY` return correct results. No query returned a truncated or NULL value | `QTY DECIMAL(20,0)`. Every row reads back, including `50000000000` |
| A5 `X` stored as Utf8 in a second file of `t_conflict/`, Int64 in the first | Every query on the table fails, including `SELECT ID` and `COUNT(*)`: `Parquet column 'X' is declared as Int64 in '<base>/t_conflict/file1.parquet' and as Utf8 in '<base>/t_conflict/file2.parquet', and no supported type relaxation widens either to the other` (22002) | `REFRESH` fails with the same message, which holds no credential value. Every other table keeps its declaration and stays queryable. `T_CONFLICT` still fails every query |
| A6 file under `t_hive/region=eu/` | `SELECT REGION` fails: `object REGION not found`. `SELECT ID, LABEL` returns the rows of both files | `REGION VARCHAR(2000000)` declared after the Parquet columns. `eu` for the new file, NULL for the old one |
| A7 last data file of `t_emptied/` deleted | `SELECT COUNT(*)` returns 0 and `SELECT *` returns no row, with no error | `T_EMPTIED` absent from `SYS.EXA_ALL_TABLES`. `SKIPPED_TABLES` holds `{"table":"t_emptied","reason":"holds no data file"}` |

- `HIVE_PARTITIONING` absent resolves to `TRUE` (A6 declares `REGION`), and `MERGE_SCHEMA` absent resolves to `TRUE` (A3 unions `NEW_COL`, A4 widens `QTY`).
- The issue's premise for A4 holds only for a value outside the declared Exasol type. An INT32 column declares `DECIMAL(10,0)`, which holds every value up to 9,999,999,999, so a value outside INT32 but below 10^10 reads back correctly. Decision-log entry [4] records how the test covers both values.
- SeaweedFS keeps an emptied directory as a listed prefix after its last object is deleted, and a delimiter listing still returns it. S3 drops such a prefix. A second full run of the probe, after deleting every object under its base path, returned identical results: the leftover empty directories were skipped at `CREATE`, not served.
- Timestamps (B4): a direct-storage Parquet `TIMESTAMP(MILLIS)`, `TIMESTAMP(MICROS)`, and `TIMESTAMP(NANOS)` column each declares `TIMESTAMP(3)`, and `2023-11-14 22:13:20.123456789` reads back as `.123000000`. This reproduces the open issue #461 end to end. The documentation states the current behavior and links #461.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| refresh-and-set-properties | CHANGED | `specs/_plans/add-direct-storage-refresh-e2e/vs-adapter/refresh-and-set-properties/spec.md` |
| direct-storage-table-planning | CHANGED | `specs/_plans/add-direct-storage-refresh-e2e/direct-storage/direct-storage-table-planning/spec.md` |
| direct-storage-table-discovery | CHANGED | `specs/_plans/add-direct-storage-refresh-e2e/direct-storage/direct-storage-table-discovery/spec.md` |

## Impact

- No engine behavior changes. Users see only documentation changes: the entry pages name all three table formats and all four catalog kinds, `docs/catalogs.md` gains a Parquet type table and a REFRESH table for direct storage, and `docs/tuning.md` names the size source of a Parquet table.
- One existing test is renamed. No breaking change.

## Dependencies

- Issue #461 owns the direct-storage timestamp precision. The B4 table states the current `TIMESTAMP(3)` declaration and links #461. When #461 lands, it updates that row.
- Issue #426 (per-column `adapterNotes`) is in progress on a spike branch that changes direct-storage planning. If #426 changes what a query returns before a REFRESH, it must update the two new `direct-storage-table-planning` scenarios and the matching assertions of the staged test in the same change.

## Implementation Tasks

All E2E work is in `crates/lakehouse-engine/tests/`. Every assertion message in the staged test starts with its case label (`A1:` to `A7:`), so a failure names its case.

1. Tests (part A)
   - [ ] 1.1 Add two helpers to `tests/common/raw_parquet.rs`, beside `put_fixture_object` and on the same `local_stack_s3_store`: `delete_fixture_prefix(uri: &str)` lists every object under the URI's key prefix and deletes each one, and `delete_fixture_object(uri: &str)` deletes the one object at the URI's key. Both panic naming the URI on a failure. Model the list-and-delete on `delete_prefix` in `tests/common/glue.rs`. Two helpers are needed because an `object_store` listing matches whole path segments, so a prefix listing of an object key does not return that object.
   - [ ] 1.2 In `e2e_direct_storage_test.rs`, add the constants `BASE_REFRESH = "s3://warehouse/direct_refresh/"`, `VS_REFRESH = "DIRECT_REFRESH_VS"`, and `CONN_REFRESH = "DIRECT_STORAGE_REFRESH_CREDS"`, and the fixture builders for the files in the table below. The fixtures are written by the test, never by `write_all_fixtures`, because the test mutates them.

      | Object under `BASE_REFRESH` | Written in | Columns and rows |
      |-----------------------------|-----------|------------------|
      | `t_append/file1.parquet` | initial | `ID` Int64 [1, 2], `LABEL` Utf8 [`a1`, `a2`] |
      | `t_evolve/file1.parquet` | initial | `ID` Int64 [1, 2], `QTY` Int32 [10, 20], `LABEL` Utf8 [`e1`, `e2`] |
      | `t_hive/p1.parquet` | initial | `ID` Int64 [1], `LABEL` Utf8 [`h1`] |
      | `t_emptied/file1.parquet` | initial, deleted in stage 1 | `ID` Int64 [1] |
      | `t_emptied/_SUCCESS` | initial | any non-empty bytes |
      | `t_conflict/file1.parquet` | initial | `ID` Int64 [1], `X` Int64 [42] |
      | `t_new/file1.parquet` | stage 1 | `ID` Int64 [100], `LABEL` Utf8 [`n1`] |
      | `t_append/file2.parquet` | stage 1 | `ID` Int64 [3], `LABEL` Utf8 [`a3`] |
      | `t_evolve/file2.parquet` | stage 1 | `ID` Int64 [3, 4], `QTY` Int64 [5_000_000_000, 50_000_000_000], `LABEL` Utf8 [`e3`, `e4`], `NEW_COL` Utf8 [`x3`, `x4`] |
      | `t_hive/region=eu/p2.parquet` | stage 1 | `ID` Int64 [2], `LABEL` Utf8 [`h2`] |
      | `t_conflict/file2.parquet` | stage 2 | `ID` Int64 [2], `X` Utf8 [`hello`] |

   - [ ] 1.3 Add the staged test `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`, carrying these `/// Scenario:` lines: "Refresh re-enumerates the namespace and returns a refresh response", "Refresh reflects table and column structure changes", "Until a refresh, a query reads the current files under the declared columns", "Until a refresh, a file the declaration cannot hold fails the query and never returns a wrong value", "A refresh that a column pair fails keeps the previous declaration queryable", and "A first-level directory holding no data file is skipped, not failed". The test runs these steps in order:
      - Setup: call `setup()`, which fails and never skips when Exasol or SeaweedFS is unavailable, then `delete_fixture_prefix(BASE_REFRESH)`, write the initial objects, and create `VS_REFRESH` with `direct_vs(VS_REFRESH, CONN_REFRESH)` and no `MERGE_SCHEMA` or `HIVE_PARTITIONING` property. Assert the served tables are exactly `T_APPEND`, `T_CONFLICT`, `T_EMPTIED`, `T_EVOLVE`, `T_HIVE`, and that `T_EVOLVE.QTY` declares `DECIMAL(10,0)`. Do not assert the `SKIPPED_TABLES` list at this point, because SeaweedFS keeps the empty directories of an earlier run.
      - Stage 1: write the stage-1 objects and delete `t_emptied/file1.parquet` with `delete_fixture_object`, leaving `t_emptied/_SUCCESS` in place. Then assert before REFRESH: A1 `SELECT * FROM T_NEW` fails. A2 `T_APPEND` returns ids 1, 2, 3 with their labels. A3 `SELECT NEW_COL FROM T_EVOLVE` fails, and `SELECT ID, LABEL FROM T_EVOLVE ORDER BY ID` returns ids 1 to 4. A4 `SELECT ID, QTY FROM T_EVOLVE` fails with a message containing `out of range`, and `SELECT QTY FROM T_EVOLVE WHERE ID = 3` returns `5000000000`. A6 `SELECT REGION FROM T_HIVE` fails, and `SELECT ID, LABEL FROM T_HIVE ORDER BY ID` returns ids 1, 2. A7 `SELECT ID FROM T_EMPTIED` returns zero rows with status `ok`.
      - Run `ALTER VIRTUAL SCHEMA DIRECT_REFRESH_VS REFRESH` and require success.
      - Assert after REFRESH: A1 `T_NEW` is served and returns `(100, 'n1')`. A2 `T_APPEND` still declares exactly `ID`, `LABEL` and returns the same three rows. A3 `T_EVOLVE` declares `ID`, `QTY`, `LABEL`, `NEW_COL` in that order, `NEW_COL` as `VARCHAR(2000000)`, and reads NULL for ids 1, 2 and `x3`, `x4` for ids 3, 4. A4 `QTY` declares `DECIMAL(20,0)` and reads 10, 20, 5000000000, 50000000000. A6 `T_HIVE` declares `ID`, `LABEL`, `REGION`, with `REGION` as `VARCHAR(2000000)`, reading NULL for id 1 and `eu` for id 2. A7 `T_EMPTIED` is not served, and the `SKIPPED_TABLES` entry of `ADAPTER_NOTES` (read from `SYS.EXA_ALL_VIRTUAL_SCHEMAS`) holds `t_emptied` with a reason containing `holds no data file`.
      - Stage 2: write `t_conflict/file2.parquet`. A5 before REFRESH: `SELECT ID, X FROM T_CONFLICT` and `SELECT ID FROM T_CONFLICT` both fail with a message naming `X`, `Int64`, `Utf8`, `t_conflict/file1.parquet`, and `t_conflict/file2.parquet`. A5 REFRESH: `try_execute` of the REFRESH returns status `error` with the same names, and the message does not contain `lhadminsecret123`. A5 after: the served tables equal the post-stage-1 set, and `T_NEW` and `T_EVOLVE` still return their post-stage-1 rows.
   - [ ] 1.4 Rename `incompatible_pair_fails_create_and_refresh_naming_column_and_files` to `incompatible_pair_fails_create_naming_column_and_files` with Serena `rename_symbol`. Its body and assertions stay unchanged.
   - [ ] 1.5 In `specs/testing.md` § Fixtures, extend the `raw_parquet.rs` bullet to name its prefix delete and object delete, and add one bullet: a fixture that a test mutates sits under its own base path and virtual schema, the test deletes every object under that base path before it writes the initial state, and it asserts no exact skipped-table list before its first mutation, because SeaweedFS keeps an emptied directory as a listed prefix.
   - [ ] 1.6 Run `cargo fmt --all`, `cargo clippy -p lakehouse-engine --all-targets --features exasol-e2e -- -D warnings`, and `make test-e2e`. The staged test and every existing test pass. Then run `cargo test --features exasol-e2e --test e2e_direct_storage_test refresh_tracks_storage_changes -- --test-threads=1` a second time, to prove that the reset makes the test pass again over the objects and empty directories the first run left.
2. Documentation (part B), written from the test of group A
   - [ ] 2.1 B1 `README.md`: rewrite the tagline and the Sharding bullet to name the three table formats (Iceberg, Delta, Parquet) and the four catalog kinds (Iceberg REST, Unity Catalog, AWS Glue, direct storage). Fix the Pushdown bullet's "Apache Iceberg and Databricks-managed Iceberg" the same way. Add Unity Catalog and direct storage to the Catalogs row of the docs table.
   - [ ] 2.2 B2 `docs/index.md`: fix the intro sentence and the Catalogs row as in 2.1.
   - [ ] 2.3 B3 `docs/architecture.md`, the flow at lines 21 to 26 and the paragraph below it: replace "resolve Iceberg snapshot + file list" and "Iceberg / Databricks Parquet" with format-neutral wording. The format reader resolves the file list from an Iceberg snapshot, a Delta log, a catalog's Parquet locations, or a directory listing, and the scan reads Parquet data files on object storage.
   - [ ] 2.4 B4 `docs/catalogs.md`, Direct storage section: add a "Parquet type → Exasol type" table in the form of the existing "Hive type in Glue → Exasol type" table. Rows: BOOLEAN to `BOOLEAN`; INT32 with `INT(8)` or `INT(16)` (signed or unsigned) to `DECIMAL(3,0)` or `DECIMAL(5,0)`; INT32 unannotated or `INT(32, true)` to `DECIMAL(10,0)`; INT32 `INT(32, false)` and INT64 (signed or unsigned) to `DECIMAL(20,0)`; FLOAT and DOUBLE to `DOUBLE PRECISION`; BYTE_ARRAY `STRING`, and a top-level `ENUM`, to `VARCHAR(2000000)`; INT32 `DATE` to `DATE`; `DECIMAL(p,s)` to `DECIMAL(p,s)` when p is at most 36, else `VARCHAR(2000000)` holding the decimal text; INT64 `TIMESTAMP` in `MILLIS`, `MICROS`, or `NANOS`, and INT96, to `TIMESTAMP`, which Exasol stores as `TIMESTAMP(3)`, so sub-millisecond digits are dropped (#461); `TIME` and `INTERVAL` to `VARCHAR(2000000)` text; LIST, MAP, and a group (struct) to `VARCHAR(2000000)` JSON; unannotated BYTE_ARRAY, unannotated FIXED_LEN_BYTE_ARRAY, `UUID`, `BSON`, `GEOMETRY`, and `GEOGRAPHY` refused, linking [Binary columns](#binary-columns). Take each row from `scan-types/type-mapping`, `scan-types/type-mapping-timestamp-precision`, `vs-adapter/binary-column-refusal`, and the E2E tests `all_types_directories_declare_and_return_their_mapped_values`, `events_directory_declares_and_returns_mixed_types_across_both_files`, and `complex_directory_declares_varchar_and_returns_parseable_json`.
   - [ ] 2.5 B5 `docs/catalogs.md`, Direct storage section: add a "What a REFRESH changes" subsection with one row per case of the staged test, stating the default `MERGE_SCHEMA = 'TRUE'`. New files appear on the next query. New tables, new columns, new partition keys, and wider column types appear only after a REFRESH. Until that REFRESH, a query that returns a value outside the declared type fails with a numeric out-of-range error and never returns a truncated value, and a value that still fits the declared type reads back unchanged. A file whose column type no widening rule folds fails every query on its table and fails the REFRESH, naming the column, both types, and both files, and the previous declaration stays queryable. A table directory that loses its last data file returns zero rows until the REFRESH, then disappears, and a directory that still holds an object such as `_SUCCESS` is listed in `SKIPPED_TABLES` with the reason `holds no data file`. Correct the "Plan-time footer cost" paragraph only where it contradicts the new subsection.
   - [ ] 2.6 B6 `docs/tuning.md`, the `JOIN_BROADCAST_MAX_BYTES` row: add the size source of a Parquet table (direct storage, Unity Catalog Parquet, and Glue Hive Parquet), the object size from the storage listing, next to the Iceberg manifest and the Delta `add` action.
   - [ ] 2.7 Check every changed documentation sentence against a scenario named in this plan's Verification or an existing test, and against `/speq:writing-guardrails`.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: direct-storage REFRESH E2E | 1.1-1.6 | none | spec deltas `vs-adapter/refresh-and-set-properties`, `direct-storage/direct-storage-table-planning`, `direct-storage/direct-storage-table-discovery`; `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs`, `crates/lakehouse-engine/tests/common/raw_parquet.rs`, `crates/lakehouse-engine/tests/common/e2e_harness.rs`, `specs/testing.md` |
| B: direct-storage and entry-page docs | 2.1-2.7 | A (B5 states what group A's test proves) | this plan's Live Observations; `specs/scan-types/type-mapping/spec.md`, `specs/scan-types/type-mapping-timestamp-precision/spec.md`, `specs/direct-storage/`; `README.md`, `docs/index.md`, `docs/architecture.md`, `docs/catalogs.md`, `docs/tuning.md` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | none | The plan removes no code. Task 1.4 renames one test and keeps its body |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Refresh re-enumerates the namespace and returns a refresh response (direct-storage arm) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` |
| Refresh re-enumerates the namespace and returns a refresh response (Iceberg arm, unchanged) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_refresh_test.rs` | `refresh_reenumerates_namespace` |
| Refresh reflects table and column structure changes (direct-storage arm) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` |
| Refresh reflects table and column structure changes (Iceberg added table and column, unchanged) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_refresh_test.rs` | `refresh_reenumerates_namespace`, `refresh_reflects_added_table_and_column_change` |
| Until a refresh, a query reads the current files under the declared columns | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` |
| Until a refresh, a file the declaration cannot hold fails the query and never returns a wrong value | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` |
| A refresh that a column pair fails keeps the previous declaration queryable | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` |
| A first-level directory holding no data file is skipped, not failed (unchanged, gains REFRESH proof) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`, `loose_file_and_empty_directory_serve_no_table` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| refresh-and-set-properties | After the staged test ran: `exapump sql -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0" "SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA='DIRECT_REFRESH_VS' ORDER BY 1"` | `T_APPEND`, `T_CONFLICT`, `T_EVOLVE`, `T_HIVE`, `T_NEW` |
| refresh-and-set-properties | `exapump sql -d "<same DSN>" "SELECT ID, QTY, NEW_COL FROM DIRECT_REFRESH_VS.T_EVOLVE ORDER BY ID"` | Four rows: `1,10,`, `2,20,`, `3,5000000000,x3`, `4,50000000000,x4` |
| direct-storage-table-planning | `exapump sql -d "<same DSN>" "SELECT ID FROM DIRECT_REFRESH_VS.T_CONFLICT"` | Fails with the fold error naming `X`, `Int64`, `Utf8`, and both `t_conflict` files, although the query does not read `X` |
| direct-storage-table-discovery | `exapump sql -d "<same DSN>" "ALTER VIRTUAL SCHEMA DIRECT_REFRESH_VS REFRESH"` | Fails with `Parquet column 'X' is declared as Int64 in '.../t_conflict/file1.parquet' and as Utf8 in '.../t_conflict/file2.parquet'` and no credential value |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0, `cargo exasol-udf validate` reports 3 UDFs OK |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures, against the local Exasol Docker container and SeaweedFS |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` (the CI step) and `cargo clippy -p lakehouse-engine --all-targets --features exasol-e2e -- -D warnings` (the feature-gated E2E file) | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |

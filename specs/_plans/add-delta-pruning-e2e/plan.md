# Plan: add-delta-pruning-e2e

## Summary

This plan runs the pruning E2E case table from #466 on Delta (#473). The case table moves into a shared test module, so Iceberg, Hive, and Delta check the same cases. The Unity suite (`make test-e2e-unity`, CI job `e2e-unity`) writes the pruning fixture rows as a Delta table, registers it in Unity Catalog, and checks every case against the native Exasol oracle through a third virtual schema. The default suite, `make test-e2e`, and the `e2e` CI job are unchanged.

## Context

- Issue #473: the case table in `crates/lakehouse-engine/tests/e2e_pruning_test.rs` runs on Iceberg and Hive direct storage only. Unit tests in `adapter/pushdown/format/delta_predicate_tests.rs` pin Delta's `NOT` handling, and delta-kernel does the Delta pruning itself (ADR `delta-kernel-prunes-adapter-only-translates`). No live test checks that Delta pruning keeps every file with a matching row.
- A Delta table is reachable only through `CATALOG_KIND = 'UNITY_CATALOG'`. Only the Unity suite starts Unity Catalog: `make test-e2e-unity` runs `make unity-up` (the `docker-compose.unity.yml` overlay plus `scripts/unity/seed.sh`) first, and the `e2e-unity` CI job runs the same bring-up. The default suite starts no Unity Catalog container.
- `e2e_pruning_test.rs` holds the case table (`Case`, `CASES`, `Placement`, `Format`), its assertions (`case_failures`, `assert_cases_hold`), and the fixture-shape helpers (`read_footer`, `assert_pruning_shape`). The file builds under `exasol-e2e` only, so the Unity suite (`unity-e2e`) cannot reach them. `tests/common/pruning_fixture.rs` (the rows, the Hive writer, and the oracle statements) already builds under both features.
- `e2e_unity_test.rs` already writes one Delta table by hand (`seed_delta_extra_types_table`: one data file and a one-commit `_delta_log`) and registers tables from Rust (`register_unity_table`: DELETE, then POST, always under `unity.delta_e2e`).
- `seed.sh` restarts the Unity Catalog container, whose test server keeps its catalog in memory, so a registration lasts until the next seed. The seed creates the `unity` catalog and the `unity.delta_e2e` schema only.
- The vendored Delta fixtures are never mutated (`scripts/unity/fixtures/PROVENANCE.md`), so the case table needs a new way to write a Delta table with a controlled row-group, page, and file layout.
- The Exasol column list of a Unity table comes from the Unity Catalog columns' `type_json` (`crates/lakehouse-engine/src/types/mapping.rs`), so a registration lists every column a test selects.
- The pruning fixture's `ts` column is timezone-naive: Arrow `Timestamp(Microsecond, None)`, written as Parquet `TIMESTAMP(isAdjustedToUTC = false)`.
- Delta protocol (`PROTOCOL.md`) sections that govern the fixture, quoted in decisions [1] to [3]: § Add File and Remove File, § Data Files, § Partition Value Serialization, § Per-file Statistics, § Primitive Types, § Timestamp without timezone (TimestampNtz), § Creation of New Log Entries, and § Consistency Between Table Metadata and Data Files. The fixture follows each one, and no deviation needs a scoped exception.
- `delta_kernel` 0.26 builds its skipping predicate with SQL WHERE semantics: each comparison carries a null check of its column, a partition value is its own exact minimum and maximum, and a data column's null check is `nullCount != numRecords`. Three of the 13 expected file sets therefore differ from Iceberg's (decision [6]).
- The plan changes no production code, component, boundary, or data flow, so it carries no architecture delta.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| delta-file-pruning | CHANGED | `specs/_plans/add-delta-pruning-e2e/delta/delta-file-pruning/spec.md` |

## Impact

- No behavior a user or operator sees changes. No production code changes.
- `make test-e2e`, its recipe, and the `e2e` CI job are unchanged. `e2e_pruning_test` keeps its three functions and their results.
- `e2e_unity_test` gains `delta_pruning_fixture_files_hold_the_row_groups_pages_and_statistics_the_cases_need` and `delta_pruning_keeps_every_file_with_a_matching_row`. Its setup also writes the Delta pruning table, registers it under `unity.e2e_pruning`, builds the oracle table `PRUNING_ORACLE.CASES`, and creates the virtual schema `PRUNING_DELTA`.
- The `Makefile`, `.github/workflows/ci.yml`, and `scripts/unity/seed.sh` are unchanged.
- Breaking: none.

## Dependencies

- Closes #473. The implementing commit references `Closes #473`.
- Follows #466, whose case table, fixture, and fixture-shape test are on `main`.
- Open question: the #473 acceptance criterion that names `make test-e2e` no longer matches this plan (decision [5]). A maintainer replaces it in the issue before the implementing commit closes #473. Agents do not edit the issue. Proposed text:
  > `make test-e2e-unity` runs `delta_pruning_keeps_every_file_with_a_matching_row` and the Delta fixture-shape test with 0 failures and 0 ignored. `make test-e2e` and the `e2e` CI job are unchanged and still pass.

## Implementation Tasks

### Group A: Shared case table and the Delta copy of the pruning fixture

Run the tasks in order. Task 1.1 moves the case table into a shared module. Tasks 1.2 and 1.3 write, register, and check the Delta table. Task 1.4 adds the Delta cases.

- [ ] 1.1 Move the case table into `crates/lakehouse-engine/tests/common/pruning_cases.rs` (decision [8]).
  - Declare the module in `tests/common/mod.rs` under `#[cfg(any(feature = "exasol-e2e", feature = "unity-e2e"))]`.
  - Move from `e2e_pruning_test.rs`, unchanged in behavior: `Placement`, `SCAN_FILTER_MARKER`, `OUTER_WHERE_ALIAS`, `Format` with `table()` and the virtual schema names `PRUNING_ICEBERG` and `PRUNING_HIVE`, `sorted_ids`, `oracle_labels`, `read_footer`, `assert_pruning_shape`, `Case`, `ALL_FILES`, `CASES`, and `case_failures`.
  - `assert_cases_hold` takes the connection and the format, and no longer calls a setup. Each case test calls its own binary's `setup()`, then `assert_cases_hold(&mut exa_conn(), format)`.
  - Extract the per-format loop body of `pruning_fixture_files_hold_the_row_groups_and_pages_the_cases_need` into `assert_one_shaped_file_per_label(format: &str, uris: &[String])`: each URI names one label, no label repeats, every label is present, and `assert_pruning_shape` holds on each footer. The Iceberg and Hive test calls it once per format.
  - Add `get_fixture_object(uri) -> Bytes` to `tests/common/raw_parquet.rs`, the GET counterpart of `put_fixture_object`. `read_footer` reads through it, so `pruning_cases.rs` builds no Tokio runtime of its own.
  - `e2e_pruning_test.rs` keeps `setup`, `hive_storage_password`, `runtime`, the fixture-shape test, and its two case tests, with every `/// Scenario:` line unchanged. Its module doc says, in at most two lines, that the case table lives in `pruning_cases.rs`.
  - Start the default stack and run `cargo test --features exasol-e2e --test e2e_pruning_test -- --test-threads=1`. The three functions pass unchanged.
- [ ] 1.2 Add the Delta table writer and the schema creation in `crates/lakehouse-engine/tests/e2e_unity_test.rs` (decisions [2] and [4]).
  - Add a private `write_delta_table` that writes a one-commit table. It takes the table location, the columns as name and Spark type JSON (the input of `spark_field`, so a struct type works), the partition columns, and per data file the relative path, the partition values, the Parquet bytes, and the stats JSON. It:
    - lists and deletes every object under the location through `local_stack_s3_store`, so each run drops the table and writes a new one, and never overwrites a live log entry or data file;
    - PUTs each data file through `put_fixture_object` and records its byte length as `size`;
    - writes `_delta_log/00000000000000000000.json` with one `protocol`, one `metaData` (the location's last path segment as `id`, `format` `{"provider": "parquet", "options": {}}`, `schemaString` from `spark_field`, `partitionColumns`, empty `configuration`, `createdTime` 0), and one `add` per file (`path`, `partitionValues`, `size`, `modificationTime` 0, `dataChange` true, `stats` as a JSON string);
    - sets `protocol` to `minReaderVersion` 1 and `minWriterVersion` 2, or, when a column is `timestamp_ntz`, to 3 and 7 with `readerFeatures` and `writerFeatures` both `["timestampNtz"]`.
  - Move `seed_delta_extra_types_table` onto it. Its log keeps the same protocol (1 and 2), the same six columns, and the same single `add`, whose `stats` is `{"numRecords":3}`. Only the table id changes, from `delta-extra-types` to `delta_extra_types`.
  - `register_unity_table` takes the namespace (`<catalog>.<schema>`) as a parameter. Before its DELETE and POST it creates the schema with `POST /schemas`, accepting HTTP 200 or an `error_code` that ends in `_ALREADY_EXISTS`, the rule of `ensure` in `seed.sh`. Its three existing call sites pass `UNITY_NAMESPACE`.
- [ ] 1.3 Write, register, and check the Delta copy of the pruning fixture (decisions [1], [3], and [4]).
  - In `crates/lakehouse-engine/tests/common/pruning_fixture.rs`:
    - extract `pruning_data_file(file) -> Bytes`: the batch without `p`, encoded with `pruning_writer_properties()`. `write_pruning_hive_copy` and the Delta copy both call it, so the two copies are byte-identical;
    - extract `pruning_relative_path(file)`: `p=a/<label>.parquet`, `p=b/<label>.parquet`, or `p=__HIVE_DEFAULT_PARTITION__/<label>.parquet`. `hive_object_uri` builds on it, so `labels_named` finds `/<label>.parquet` in both copies;
    - add `PruningFile::k_values()`, the `k` value of each row, so a caller computes the per-file statistics from the rows;
    - name the Delta copy in the module doc, in at most two lines.
  - In `e2e_unity_test.rs`, add `seed_pruning_delta_table`, the shape of `seed_delta_extra_types_table`:
    - one column definition, read by the Delta schema and by the registration: `id` `long` `LONG`, `p` `string` `STRING`, `k` `long` `LONG`, `s` `string` `STRING`, `x` `double` `DOUBLE`, `ts` `timestamp_ntz` `TIMESTAMP_NTZ`;
    - location `s3://warehouse/delta_pruning/pruning_cases`, partition column `p`, and `partitionValues` `{"p": "a"}`, `{"p": "b"}`, or `{"p": null}`;
    - stats per file, computed from `k_values()`: `numRecords`, and `minValues`, `maxValues`, and `nullCount` for `k`. `b2`, whose `k` is NULL in every row, carries `nullCount.k` 3 and no `k` bound;
    - it registers `pruning_cases` under `unity.e2e_pruning` with partition column `p`.
  - Extend `setup()`, which stays the binary's one `OnceLock`-guarded setup: execute `oracle_statements()`, call `seed_pruning_delta_table()`, and create the virtual schema `PRUNING_DELTA` with `create_virtual_schema_with_password`, `VsProps::new("PRUNING_DELTA", "unity.e2e_pruning").with_catalog_kind("UNITY_CATALOG").with_catalog_conn_name("PRUNING_DELTA_CREDS")`, the catalog URI `UNITY_CATALOG_URI_INTERNAL`, and `local_stack_connection_password()`. Every call panics when its service is unreachable, so the suite fails and never skips.
  - Add `delta_pruning_fixture_files_hold_the_row_groups_pages_and_statistics_the_cases_need`. It reads `_delta_log/00000000000000000000.json` through `get_fixture_object`, takes every `add` line, builds each URI from the location and `add.path`, and calls `assert_one_shaped_file_per_label("Delta", &uris)`. For each `add` it then asserts, with the footer from `read_footer` and the byte length from `get_fixture_object`:
    - `partitionValues.p` equals the fixture file's partition, JSON `null` for `n1` and `n2`;
    - `size` equals the object's byte length;
    - `stats.numRecords` equals the footer's row count, and `minValues.k`, `maxValues.k`, and `nullCount.k` equal the minimum, the maximum, and the summed null count over the footer's row-group statistics of `k`, with no `k` bound when those statistics carry none.
    Each failure message says that the Delta log disagrees with its data file, so the fixture cannot prove Delta pruning. The test carries `/// Scenario: Delta pruning keeps every file with a matching row for every filter shape`.
  - Run `make test-e2e-unity`. The fixture-shape test passes, and every existing test passes unchanged.
- [ ] 1.4 Add the Delta expected-set column and its test (decision [6]). [expert]
  - In `pruning_cases.rs`, add `Format::Delta` with its table `PRUNING_DELTA.PRUNING_CASES`, a `delta` field, a `.delta(...)` builder, and the `Format::Delta` arm of `Case::expected`, the shape of the `iceberg` and `hive` fields.
  - Give the 13 cases that carry an Iceberg set the Delta set from the decision [6] table. Give the partly translated cases no Delta set. `NOT (K BETWEEN 10 AND 20.5)` is one of them, because `20.5` does not convert to a `long` scalar, so the `BETWEEN` drops a bound.
  - Replace the comment above `CASES` with two lines that name the rule of each format, Delta's being: a NULL partition value or an all-NULL column fails every comparison, and `!=` and `NOT IN` prune.
  - In `e2e_unity_test.rs`, add `delta_pruning_keeps_every_file_with_a_matching_row`, which calls `setup()`, then `assert_cases_hold(&mut exa_conn(), Format::Delta)`, and carries `/// Scenario: Delta pruning keeps every file with a matching row for every filter shape`.
  - Run `make test-e2e-unity`. Both Delta functions pass.
  - A live Effective set that differs from the table is a finding. Explain it from `delta_kernel` 0.26 `src/scan/data_skipping.rs` and `src/kernel_predicates/mod.rs` before changing the table, and record the explanation in the decision log. Never copy a live set into the table unexplained. A Sound failure, a wrong row set, or a missing placement marker is an engine defect: stop and report it with the case and the plan text.
- [ ] 1.5 Update `specs/testing.md`.
  - § Fixtures, pruning bullet: an Iceberg table, a Hive direct-storage directory, and a Delta table registered in Unity Catalog under `unity.e2e_pruning` hold the same rows. The Delta fixture-shape test also checks that the Delta log's statistics match each data file's footer. One case table in `tests/common/pruning_cases.rs` runs on all three formats: `e2e_pruning_test` checks Iceberg and Hive in the local Docker suite, and `e2e_unity_test` checks Delta in the Unity suite.
  - § Fixtures, Delta bullet: keep the vendored-fixture rule. Add that a test needing a Delta table with a controlled layout writes it in `e2e_unity_test.rs`, which drops and rewrites the table on every run, and registers it under its own Unity Catalog schema. The Unity Catalog column registration, in `seed.sh` or in a test, lists every column a test selects.
  - § E2E suites and § Make targets and CI: no change. The `Unity Catalog` row already names `e2e_unity_test`, and no Make target or CI job changes.
- [ ] 1.6 Verify.
  - Start the default stack yourself as the `e2e` job does: `docker compose up -d --wait exasol seaweedfs iceberg-rest`, then the `spark-iceberg-fixtures` job. Run `make test-e2e`. Every test passes and none is ignored, including the three functions of `e2e_pruning_test`.
  - Run `make test-e2e-unity`. Every test passes and none is ignored, including the two Delta functions.
  - Run `cargo test --workspace`, `cargo clippy --all-targets -- -D warnings`, `cargo clippy --all-targets --features exasol-e2e -- -D warnings`, `cargo clippy --all-targets --features unity-e2e -- -D warnings`, and `cargo fmt --all -- --check`.
  - `git diff main -- Makefile .github/workflows/ci.yml scripts/unity/seed.sh` prints nothing.

Test budget: no production lines. About 200 added test lines (about 25 in `pruning_cases.rs`, about 15 in `raw_parquet.rs`, about 20 in `pruning_fixture.rs`, about 140 in `e2e_unity_test.rs`), about 280 lines moved from `e2e_pruning_test.rs` to `pruning_cases.rs`, and about 25 lines removed from `e2e_unity_test.rs` by the migration.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Shared case table and the Delta copy of the pruning fixture | 1.1-1.6 | none | spec delta `delta/delta-file-pruning`; decision-log [1]-[8]; `crates/lakehouse-engine/tests/{e2e_pruning_test.rs,e2e_unity_test.rs}`, `crates/lakehouse-engine/tests/common/{pruning_cases.rs,pruning_fixture.rs,raw_parquet.rs,mod.rs}`; `specs/testing.md`; read-only `crates/lakehouse-engine/tests/common/{e2e_harness.rs,stack.rs}`, `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate.rs`, `scripts/unity/seed.sh`, `delta_kernel-0.26.0/src/scan/data_skipping.rs`, `delta_kernel-0.26.0/src/kernel_predicates/mod.rs`, Delta `PROTOCOL.md` |

One group, because the shared case table, the fixture, its Unity registration, and the expected sets rest on one column definition and one layout.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Case table and fixture-shape helpers | `crates/lakehouse-engine/tests/e2e_pruning_test.rs`: `Placement`, `SCAN_FILTER_MARKER`, `OUTER_WHERE_ALIAS`, `Format`, `VS_ICEBERG`, `VS_HIVE`, `sorted_ids`, `oracle_labels`, `read_footer`, `assert_pruning_shape`, `Case`, `ALL_FILES`, `CASES`, `case_failures`, `assert_cases_hold` | Moved to `tests/common/pruning_cases.rs` |
| Inline Delta log | `crates/lakehouse-engine/tests/e2e_unity_test.rs`: the `protocol`, `metaData`, and `add` JSON in `seed_delta_extra_types_table` | Replaced by `write_delta_table` in the same file |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| delta-file-pruning: Delta pruning keeps every file with a matching row for every filter shape (NEW) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `delta_pruning_keeps_every_file_with_a_matching_row`, `delta_pruning_fixture_files_hold_the_row_groups_pages_and_statistics_the_cases_need` |
| pushdown-file-pruning and direct-storage scenarios proved by `e2e_pruning_test` (recorded, unchanged; case table moved) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_pruning_test.rs` | every test, run by `make test-e2e` |
| delta-type-mapping and delta-file-pruning scenarios proved by `e2e_unity_test` (recorded, unchanged; Delta writer and registration helper changed) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | every existing test, run by `make test-e2e-unity` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| delta-file-pruning | `make test-e2e-unity` | Every test passes, including the two Delta pruning functions of `e2e_unity_test`, and none is ignored |
| delta-file-pruning | `make test-e2e` | Every test passes unchanged, including the three functions of `e2e_pruning_test`, and none is ignored |
| delta-file-pruning | `exapump sql "SELECT COUNT(*) FROM PRUNING_DELTA.PRUNING_CASES WHERE NOT (K < 30 AND S LIKE 'x%')" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"`, after `make test-e2e-unity` | `17`, the count the same query returns over `PRUNING_ORACLE.CASES` |
| delta-file-pruning | `exapump sql "EXPLAIN VIRTUAL SELECT ID FROM PRUNING_DELTA.PRUNING_CASES WHERE NOT (K < 30 AND P = 'a')" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"`, after `make test-e2e-unity` | The pushdown SQL names `LAKEHOUSE_SCAN` and the files `/a2.parquet`, `/b1.parquet`, `/b2.parquet`, `/n1.parquet`, and `/n2.parquet`, and does not name `/a1.parquet` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make test-e2e` | 0 failures, 0 ignored, suite unchanged |
| E2E (Unity) | `make test-e2e-unity` | 0 failures, 0 ignored |
| Lint | `cargo clippy --all-targets -- -D warnings` | 0 warnings |
| Lint (E2E) | `cargo clippy --all-targets --features exasol-e2e -- -D warnings` | 0 warnings |
| Lint (Unity E2E) | `cargo clippy --all-targets --features unity-e2e -- -D warnings` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| Untouched files | `git diff main -- Makefile .github/workflows/ci.yml scripts/unity/seed.sh` | No output |
| Plan | `speq plan validate add-delta-pruning-e2e` | Pass |

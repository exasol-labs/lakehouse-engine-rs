# Plan: add-delta-pruning-e2e

## Summary

This plan runs the pruning E2E case table from #466 on Delta (#473). The test writes the pruning fixture rows as a Delta table, registers it in Unity Catalog, and checks every case against the native Exasol oracle through a third virtual schema, and `make test-e2e` brings up Unity Catalog so the new test runs in the default suite.

## Context

- Issue #473: the case table in `crates/lakehouse-engine/tests/e2e_pruning_test.rs` runs on Iceberg and Hive direct storage only. Unit tests in `adapter/pushdown/format/delta_predicate_tests.rs` pin Delta's `NOT` handling, and delta-kernel does the Delta pruning itself (ADR `delta-kernel-prunes-adapter-only-translates`). No live test checks that Delta pruning keeps every file with a matching row.
- The vendored Delta fixtures are never mutated (`scripts/unity/fixtures/PROVENANCE.md`), so the case table needs a new way to write a Delta table with a controlled row-group, page, and file layout.
- A Delta table is reachable only through `CATALOG_KIND = 'UNITY_CATALOG'`. `make test-e2e` starts no Unity Catalog container. `make test-e2e-unity` runs `make unity-up` (the `docker-compose.unity.yml` overlay plus `scripts/unity/seed.sh`) first. The `e2e` CI job starts `exasol`, `seaweedfs`, and `iceberg-rest` from `docker-compose.yml` alone.
- The overlay redefines the `exasol` service to add the `unitycatalog` host entry. A stack started without the overlay therefore has its Exasol container recreated by the first `make unity-up`.
- `seed.sh` restarts the Unity Catalog container, whose test server keeps its catalog in memory, so every registration made before the seed is lost.
- `e2e_unity_test.rs` already writes one Delta table by hand (`seed_delta_extra_types_table`: one data file and a one-commit `_delta_log`) and registers tables from Rust (`register_unity_table`: DELETE, then POST). These helpers are private to that binary, which builds under `unity-e2e` only.
- The Exasol column list of a Unity table comes from the Unity Catalog columns' `type_json` (`crates/lakehouse-engine/src/types/mapping.rs`), so a registration lists every column a test selects.
- The pruning fixture's `ts` column is timezone-naive: Arrow `Timestamp(Microsecond, None)`, written as Parquet `TIMESTAMP(isAdjustedToUTC = false)`.
- Delta protocol (`PROTOCOL.md`) sections that govern the fixture, quoted in decisions [1] to [3]: § Add File and Remove File, § Data Files, § Partition Value Serialization, § Per-file Statistics, § Primitive Types, § Timestamp without timezone (TimestampNtz), § Creation of New Log Entries, and § Consistency Between Table Metadata and Data Files. The fixture follows each one, and no deviation needs a scoped exception.
- `delta_kernel` 0.26 builds its skipping predicate with SQL WHERE semantics: each comparison carries a null check of its column, a partition value is its own exact minimum and maximum, and a data column's null check is `nullCount != numRecords`. Three of the 13 expected file sets therefore differ from Iceberg's (decision [6]).
- The `e2e-unity` job runs on Exasol 2025.1.16 only. No CI leg has read a Unity Delta table on Exasol 8.29.
- The plan changes no production code, component, boundary, or data flow, so it carries no architecture delta.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| delta-file-pruning | CHANGED | `specs/_plans/add-delta-pruning-e2e/delta/delta-file-pruning/spec.md` |

## Impact

- No behavior a user or operator sees changes. No production code changes.
- `make test-e2e` runs `make unity-up` before the suite, so it starts the Unity Catalog container and runs the seed. The first run after a stack started without the overlay recreates the Exasol container once, and its data volume survives.
- The `e2e` CI job starts its stack with the Unity overlay, pulls the Unity Catalog and AWS CLI images, and runs the seed. Both legs, `E2E` and `E2E (8.29.x)`, read a Unity Delta table.
- `e2e_pruning_test` gains `delta_pruning_keeps_every_file_with_a_matching_row`.
- Breaking: none.

## Dependencies

- Closes #473. The implementing commit references `Closes #473`.
- Follows #466, whose case table, fixture, and fixture-shape test are on `main`.

## Implementation Tasks

### Group A: Delta copy of the pruning fixture and its case column

Run the tasks in order. Tasks 1.1 to 1.3 build the shared helpers and the fixture. Task 1.4 brings Unity Catalog into the default suite. Task 1.5 proves the fixture's shape before task 1.6 adds the cases.

- [ ] 1.1 Move the Unity Catalog helpers into a shared module (decision [4]).
  - Create `crates/lakehouse-engine/tests/common/unity.rs` and declare it in `tests/common/mod.rs` under `#[cfg(any(feature = "exasol-e2e", feature = "unity-e2e"))]`.
  - Move from `e2e_unity_test.rs`, unchanged in behavior: `UNITY_CATALOG_URI_INTERNAL`, `unity_port`, `unity_catalog_url`, `wait_for_unity_catalog` with its 30-second readiness timeout, and `register_unity_table`.
  - `register_unity_table` gains a `namespace` (`<catalog>.<schema>`) parameter. Before its DELETE and POST it creates the catalog (`POST /catalogs`) and the schema (`POST /schemas`), accepting HTTP 200 or an `error_code` that ends in `_ALREADY_EXISTS`. Unity Catalog OSS 0.5.0 answers a re-create with HTTP 400 and `CATALOG_ALREADY_EXISTS` or `SCHEMA_ALREADY_EXISTS` (`seed.sh`, `ensure`).
  - `e2e_unity_test.rs` imports these and passes `UNITY_NAMESPACE` at its three `register_unity_table` call sites.
- [ ] 1.2 Add the shared Delta table writer (decision [2]).
  - Create `crates/lakehouse-engine/tests/common/delta_log.rs` and declare it in `tests/common/mod.rs` with the same `cfg` as `raw_parquet`, because `pruning_fixture.rs` uses it and `seed.rs` builds `pruning_fixture.rs` under every local-stack feature.
  - Move `spark_field` here from `e2e_unity_test.rs`: it builds the Spark `StructField` JSON that is both Delta's schema format and Unity Catalog's `type_json`. `unity.rs` and `e2e_unity_test.rs` import it.
  - One function writes a one-commit table. It takes the table location, the columns as name and Spark type JSON (the input of `spark_field`, so a struct type works), the partition columns, and per data file the relative path, the partition values, the Parquet bytes, and the stats JSON. It:
    - deletes every object under the location through `local_stack_s3_store`, so each run drops the table and writes a new one, and never overwrites a live log entry or data file;
    - PUTs each data file through `put_fixture_object` and records its byte length as `size`;
    - writes `_delta_log/00000000000000000000.json` with one `protocol`, one `metaData` (the location's last path segment as the table id, `format.provider` `parquet`, `schemaString` from `spark_field`, `partitionColumns`, empty `configuration`), and one `add` per file (`path`, `partitionValues`, `size`, `modificationTime` 0, `dataChange` true, `stats` as a JSON string);
    - sets `protocol` to `minReaderVersion` 1 and `minWriterVersion` 2, or, when a column is `timestamp_ntz`, to 3 and 7 with `readerFeatures` and `writerFeatures` both `["timestampNtz"]`.
  - Move `seed_delta_extra_types_table` onto it. Its log keeps the same protocol (1 and 2), the same six columns, and the same single `add`, whose `stats` is `{"numRecords":3}`. Only the table id changes, to `delta_extra_types`.
- [ ] 1.3 Write the Delta copy of the pruning fixture in `crates/lakehouse-engine/tests/common/pruning_fixture.rs` (decisions [1] and [3]).
  - One column definition, read by the Delta schema and by the Unity Catalog registration: `id` `long` `LONG`, `p` `string` `STRING`, `k` `long` `LONG`, `s` `string` `STRING`, `x` `double` `DOUBLE`, `ts` `timestamp_ntz` `TIMESTAMP_NTZ`.
  - Constants: location `s3://warehouse/delta_pruning/pruning_cases`, Unity namespace `unity.e2e_pruning`, table `pruning_cases`.
  - One function encodes a label's data file: the batch without `p`, with `pruning_writer_properties()`. `write_pruning_hive_copy` and the Delta writer both call it, so the two copies are byte-identical.
  - Data-file paths `p=a/<label>.parquet`, `p=b/<label>.parquet`, and `p=__HIVE_DEFAULT_PARTITION__/<label>.parquet`, the Hive copy's layout, so `labels_named` finds `/<label>.parquet` unchanged. `partitionValues` is `{"p": "a"}`, `{"p": "b"}`, or `{"p": null}`.
  - Stats per file, computed from the fixture rows: `numRecords`, and `minValues`, `maxValues`, and `nullCount` for `k`. `b2`, whose `k` is NULL in every row, carries `nullCount.k` 3 and no `k` bound.
  - `write_pruning_delta_table()` writes the table through the task 1.2 writer.
- [ ] 1.4 Bring Unity Catalog into `make test-e2e` and the `e2e` CI job (decision [5]).
  - `Makefile`: the `test-e2e` recipe runs `$(MAKE) unity-up` on its own line before the cargo line, as `test-e2e-unity` does. The cargo line keeps every flag and already lists `--test e2e_pruning_test`.
  - `.github/workflows/ci.yml`, `e2e` job: set `COMPOSE="docker compose -f docker-compose.yml -f docker-compose.unity.yml"` in the start step and use the same two files in the pull step, for `up -d --wait exasol seaweedfs iceberg-rest`, for the `spark-iceberg-fixtures` run, for the `dump-exasol-logs` step's `compose` input (with `services: unitycatalog`), and for the stop step's `down -v`. `make test-e2e` then starts only the Unity Catalog container and runs the seed.
  - Leave the `e2e-unity` job and the `test-e2e-unity` cargo line unchanged. They stay flag-identical.
- [ ] 1.5 Add the Delta virtual schema and extend the fixture-shape test in `crates/lakehouse-engine/tests/e2e_pruning_test.rs`.
  - In `setup`: call `wait_for_unity_catalog()` with the other readiness waits, call `write_pruning_delta_table()`, call `register_unity_table` for `pruning_cases` under `unity.e2e_pruning` with the task 1.3 columns and partition column `p`, and create the virtual schema `PRUNING_DELTA` with `create_virtual_schema_with_password`, `VsProps::new("PRUNING_DELTA", "unity.e2e_pruning").with_catalog_kind("UNITY_CATALOG").with_catalog_conn_name("PRUNING_DELTA_CREDS")`, the catalog URI `UNITY_CATALOG_URI_INTERNAL`, and `local_stack_connection_password()`. Call `register_unity_table` outside `runtime().block_on`, because its blocking HTTP client panics inside a Tokio runtime. Every call panics when its service is unreachable, so the suite fails and never skips.
  - Add `Format::Delta` with its table `PRUNING_DELTA.PRUNING_CASES`, and update the module doc to name the three formats in at most two lines.
  - Extend `pruning_fixture_files_hold_the_row_groups_and_pages_the_cases_need` to Delta. Read `_delta_log/00000000000000000000.json` from SeaweedFS, take every `add` line, and build each URI from the location and the `add.path`. For each file, assert:
    - `labels_named` names one label, and the log holds one file per label;
    - `assert_pruning_shape` holds on its footer;
    - `partitionValues.p` equals the fixture file's partition, JSON `null` for `n1` and `n2`;
    - `size` equals the object's byte length;
    - `stats.numRecords` equals the footer's row count, and `minValues.k`, `maxValues.k`, and `nullCount.k` equal the minimum, the maximum, and the summed null count over the footer's row-group statistics of `k`, with no `k` bound when those statistics carry none.
    Each failure message says that the Delta log disagrees with its data file, so the fixture cannot prove Delta pruning. Share the object GET between `read_footer` and the log read.
  - Add `/// Scenario: Delta pruning keeps every file with a matching row for every filter shape` to that test.
  - Run `make unity-up`, then `cargo test --features exasol-e2e --test e2e_pruning_test pruning_fixture_files -- --test-threads=1`. The fixture-shape test passes.
- [ ] 1.6 Add the Delta expected-set column and its test function (decision [6]). [expert]
  - Add a `delta` field, a `.delta(...)` builder, and the `Format::Delta` arm of `Case::expected`, the shape of the `iceberg` and `hive` fields.
  - Give the 13 cases that carry an Iceberg set the Delta set from the decision [6] table. Give the partly translated cases no Delta set. `NOT (K BETWEEN 10 AND 20.5)` is one of them, because `20.5` does not convert to a `long` scalar, so the `BETWEEN` drops a bound.
  - Replace the comment above `CASES` with two lines that name the rule of each format, Delta's being: a NULL partition value or an all-NULL column fails every comparison, and `!=` and `NOT IN` prune.
  - Add `delta_pruning_keeps_every_file_with_a_matching_row`, which calls `assert_cases_hold(Format::Delta)` and carries `/// Scenario: Delta pruning keeps every file with a matching row for every filter shape`.
  - Run `cargo test --features exasol-e2e --test e2e_pruning_test -- --test-threads=1`. All four functions pass.
  - A live Effective set that differs from the table is a finding. Explain it from `delta_kernel` 0.26 `src/scan/data_skipping.rs` and `src/kernel_predicates/mod.rs` before changing the table, and record the explanation in the decision log. Never copy a live set into the table unexplained. A Sound failure, a wrong row set, or a missing placement marker is an engine defect: stop and report it with the case and the plan text.
- [ ] 1.7 Update `specs/testing.md`.
  - § E2E suites, row `Local Docker`: the stack adds OSS Unity Catalog (`docker-compose.unity.yml`).
  - § Fixtures, pruning bullet: an Iceberg table, a Hive direct-storage directory, and a Delta table registered in Unity Catalog under `unity.e2e_pruning` hold the same rows. The fixture-shape test also checks that the Delta log's statistics match each data file's footer. The case table runs on all three formats.
  - § Fixtures, Delta bullet: keep the vendored-fixture rule. Add that a test needing a Delta table with a controlled layout writes it through `tests/common/delta_log.rs`, which drops and rewrites the table on every run, and registers it through `tests/common/unity.rs`. The Unity Catalog column registration, in `seed.sh` or in a test, lists every column a test selects.
  - § Make targets and CI: `make test-e2e` and `make test-e2e-unity` run `make unity-up` first. The `e2e` job starts its stack with the Unity overlay, so `make unity-up` does not recreate the Exasol container.
- [ ] 1.8 Verify.
  - Start the stack yourself with the Unity overlay: `docker compose -f docker-compose.yml -f docker-compose.unity.yml up -d --wait exasol seaweedfs iceberg-rest`, then the `spark-iceberg-fixtures` job as the `e2e` job runs it. Run `make test-e2e`. Every test passes and none is ignored, including the four functions of `e2e_pruning_test`.
  - Run `make test-e2e-unity`. `e2e_unity_test` passes unchanged after the helper move and the `delta_extra_types` migration.
  - Run `cargo test --workspace`, `cargo clippy --all-targets -- -D warnings`, `cargo clippy --all-targets --features exasol-e2e -- -D warnings`, `cargo clippy --all-targets --features unity-e2e -- -D warnings`, and `cargo fmt --all -- --check`.
  - After the push, both `e2e` legs pass. A failure only on `E2E (8.29.x)` that a Delta-specific behavior causes is a finding: report it with the failing case.

Test budget: no production lines. About 250 added test lines (about 50 for `delta_log.rs`, about 15 new in `unity.rs`, about 70 in `pruning_fixture.rs`, about 115 in `e2e_pruning_test.rs`), about 120 lines moved out of `e2e_unity_test.rs`, and about 25 lines removed from it by the migration.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Delta copy of the pruning fixture and its case column | 1.1-1.8 | none | spec delta `delta/delta-file-pruning`; decision-log [1]-[7]; `crates/lakehouse-engine/tests/e2e_pruning_test.rs`, `crates/lakehouse-engine/tests/common/{pruning_fixture.rs,delta_log.rs,unity.rs,raw_parquet.rs,mod.rs}`, `crates/lakehouse-engine/tests/e2e_unity_test.rs`; `Makefile`; `.github/workflows/ci.yml` (`e2e` job); `specs/testing.md`; read-only `crates/lakehouse-engine/tests/common/{e2e_harness.rs,stack.rs,seed.rs}`, `crates/lakehouse-engine/src/adapter/pushdown/format/delta_predicate.rs`, `scripts/unity/seed.sh`, `docker-compose.unity.yml`, `delta_kernel-0.26.0/src/scan/data_skipping.rs`, `delta_kernel-0.26.0/src/kernel_predicates/mod.rs`, Delta `PROTOCOL.md` |

One group, because the fixture, its Unity registration, and the expected sets rest on one column definition and one layout.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Functions and constants | `crates/lakehouse-engine/tests/e2e_unity_test.rs`: `UNITY_CATALOG_URI_INTERNAL`, `READINESS_TIMEOUT`, `unity_port`, `wait_for_unity_catalog`, `unity_catalog_url`, `register_unity_table`, `spark_field` | Moved to `tests/common/unity.rs` and `tests/common/delta_log.rs` |
| Inline Delta log | `crates/lakehouse-engine/tests/e2e_unity_test.rs`: the `protocol`, `metaData`, and `add` JSON in `seed_delta_extra_types_table` | Replaced by the shared writer in `tests/common/delta_log.rs` |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| delta-file-pruning: Delta pruning keeps every file with a matching row for every filter shape (NEW) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_pruning_test.rs` | `delta_pruning_keeps_every_file_with_a_matching_row`, `pruning_fixture_files_hold_the_row_groups_and_pages_the_cases_need` |
| delta-type-mapping and delta-file-pruning scenarios proved by `e2e_unity_test` (recorded, unchanged; fixture writer and helpers moved) | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | every test, run by `make test-e2e-unity` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| delta-file-pruning | `make test-e2e` | Every test passes, including the four functions of `e2e_pruning_test`, and none is ignored |
| delta-file-pruning | `exapump sql "SELECT COUNT(*) FROM PRUNING_DELTA.PRUNING_CASES WHERE NOT (K < 30 AND S LIKE 'x%')" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | `17`, the count the same query returns over `PRUNING_ORACLE.CASES` |
| delta-file-pruning | `exapump sql "EXPLAIN VIRTUAL SELECT ID FROM PRUNING_DELTA.PRUNING_CASES WHERE NOT (K < 30 AND P = 'a')" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | The pushdown SQL names `LAKEHOUSE_SCAN` and the files `/a2.parquet`, `/b1.parquet`, `/b2.parquet`, `/n1.parquet`, and `/n2.parquet`, and does not name `/a1.parquet` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make test-e2e` | 0 failures, 0 ignored |
| E2E (Unity) | `make test-e2e-unity` | 0 failures, 0 ignored |
| Lint | `cargo clippy --all-targets -- -D warnings` | 0 warnings |
| Lint (E2E) | `cargo clippy --all-targets --features exasol-e2e -- -D warnings` | 0 warnings |
| Lint (Unity E2E) | `cargo clippy --all-targets --features unity-e2e -- -D warnings` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| Plan | `speq plan validate add-delta-pruning-e2e` | Pass |

# Feature: Direct-Storage E2E Suite

Proves end to end that a plain directory of Parquet files on MinIO is queryable through the
lakehouse Virtual Schema under `CATALOG_KIND = 'DIRECT_STORAGE'`. The fixture set is written by a
raw Parquet writer rather than by a catalog. The catalog-free read path is therefore gated by the
same live Exasol suite every other read path is gated by.

## Background

* The suite needs NO new stack and NO new CI job. Direct storage has no catalog service, so the
  base `docker-compose.yml` already supplies everything the suite reads: MinIO with its single
  `warehouse` bucket, and Exasol. The `iceberg-rest` service is unused.
* CI's `e2e` job runs `make test-e2e`, so the Makefile's explicit `--test` list is the single
  registration point. A binary missing from that list never runs. The gate is then vacuous.
* Every fixture today is authored by `tests/common/seed.rs` through the iceberg-rust writer stack.
  That stack creates a catalog table and commits a snapshot. This suite needs the opposite: Parquet
  bytes put at a chosen object key with no catalog, no snapshot, and no Iceberg field-id metadata.
* `tests/common/e2e_harness.rs` already exposes `local_stack_storage()`, the S3 backend the E2E
  stack reads through. The fixture writer reuses it rather than re-declaring MinIO credentials.
* `packaging/iceberg-type-promotion-fixture` records the fixture-shape rule this suite inherits. A
  fixture test asserts the written file's PHYSICAL Parquet encoding from its own footer. A read
  test therefore cannot pass vacuously against a file whose types were silently normalized.
* Under `MERGE_SCHEMA = 'FALSE'` the declared type comes from ONE sampled footer. A file whose
  physical column is WIDER than the declaration fails every query that reads the column, because
  the scan refuses a physical type outside identity and the supported widening set
  (`datafusion-scan/type-relaxation`). That is the cost of the mode. The suite pins it rather than
  leaving it to be discovered.
* A fixture whose scenario requires a FAILED enumeration MUST sit under a base path no passing
  scenario shares. Enumeration walks EVERY first-level directory of the base path, so one
  unfoldable directory fails every virtual schema created over that root. The incompatible-pair
  fixture therefore sits under `s3://warehouse/direct_incompatible/`. Every other fixture of this
  feature sits under `s3://warehouse/direct/`.
* The discovery, property, CONNECTION-validation, and pushdown-parity scenarios live in
  `e2e-harness/direct-storage-e2e-properties`.

## Scenarios

### Scenario: The direct-storage binary is wired into the suite gate

* *GIVEN* the repository `Makefile`, whose `test-e2e` recipe enumerates its test binaries explicitly on one line, and CI's `e2e` job, which runs exactly that target
* *WHEN* the direct-storage E2E binary is added
* *THEN* the binary SHALL live at `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs`, SHALL be gated by the `exasol-e2e` cargo feature through a file-level attribute, and SHALL require NO `Cargo.toml` edit, because cargo auto-discovers every `tests/*.rs` target
* *AND* the `test-e2e` recipe SHALL gain `--test e2e_direct_storage_test` in its `--test` list, because a binary absent from that list never executes and CI runs that same target
* *AND* the EXISTING build-convention guard, which already reads the `Makefile`, locates the `test-e2e` recipe line, and asserts that it names the type-relaxation binary, SHALL be extended to assert that the line names this binary too, so a later edit that drops the binary fails the suite rather than silently retiring it; NO new guard file SHALL be added
* *AND* the CI workflow, the composite E2E setup action, and `docker-compose.yml` SHALL be UNCHANGED, so the suite adds no stack service and no status check
* *AND* the binary SHALL provision the scan path through the shared `common::e2e_harness` definition under one `OnceLock`-guarded setup, so its script DDL is byte-identical to every other E2E binary
* *AND* every test in the binary SHALL FAIL, never skip, when Exasol or MinIO is unreachable, reached through the panicking readiness helpers the shared harness already calls

### Scenario: A raw-Parquet fixture writer puts data files with no catalog

* *GIVEN* the fixture set this suite reads, which must be plain Parquet objects under `s3://warehouse/direct/` and `s3://warehouse/direct_incompatible/` with no Iceberg table, no Delta log, and no committed snapshot
* *WHEN* the suite authors a fixture
* *THEN* a helper module at `crates/lakehouse-engine/tests/common/raw_parquet.rs` SHALL take an object key and a `RecordBatch`, write the batch through an Arrow Parquet writer into an in-memory buffer, and PUT that buffer at that key through an object store, so one call authors one data file
* *AND* the helper SHALL be declared in `crates/lakehouse-engine/tests/common/mod.rs` beside the other shared helpers, exactly once, so no binary carries its own copy
* *AND* the helper SHALL derive its object-store credentials from the shared `local_stack_storage()` backend and MUST NOT re-declare the MinIO endpoint, key, or bucket, so the fixture writer and the engine read the same stack by construction
* *AND* the helper MUST NOT create a catalog table, commit a snapshot, write an Iceberg manifest, or attach Iceberg field-id metadata to the Arrow schema, because a fixture carrying any of those would stop testing the raw-Parquet path
* *AND* a fixture-shape test SHALL assert each written object's PHYSICAL column encoding by reading that object's own Parquet footer back, so a fixture whose types were silently normalized fails here rather than making a read test pass vacuously
* *AND* fixture authoring SHALL be idempotent across runs, because the MinIO volume outlives one test run

### Scenario: A directory of mixed-type Parquet files is declared and queried end to end

* *GIVEN* a fixture directory `s3://warehouse/direct/events/` holding TWO Parquet files that carry the mixed-type column set the Iceberg E2E fixture already uses (a 64-bit integer named `EVENT_ID`, a string, a date, a timestamp, a decimal, a boolean, and a double)
* *AND* a fixture directory `s3://warehouse/direct/nested/` holding `A/p1.parquet` and `B/p2.parquet` under two plain subdirectories
* *AND* a virtual schema created over `s3://warehouse/direct/` with `CATALOG_KIND = 'DIRECT_STORAGE'`
* *WHEN* an Exasol user reads `SYS.EXA_ALL_COLUMNS` for the served tables and then queries them
* *THEN* the virtual schema SHALL declare one virtual table named `EVENTS` and one named `NESTED`, each column carrying the Exasol type the Arrow-to-Exasol mapping resolves for its Parquet type
* *AND* a `SELECT` over `EVENTS` SHALL return every row of BOTH files with values equal to the values written, so a table is the union of its directory's files
* *AND* `NESTED` SHALL return the rows of both subdirectory files as ONE table with ZERO partition columns, so an unlimited-depth plain directory contributes files rather than columns
* *AND* a fixture directory `s3://warehouse/direct/complex/` holding one struct, one list, and one map column SHALL declare those three columns as `VARCHAR(2000000)` and SHALL return each value as parseable JSON, reusing the assertions the existing complex-type suite already applies
* *AND* every test in this scenario MUST fail, not skip, when the stack is unavailable

### Scenario: A column typed narrowly in one file and widely in another returns every row

* *GIVEN* a fixture directory `s3://warehouse/direct/widened/` whose first file declares one column as a 32-bit integer and a second column as a 32-bit float, and whose second file declares the same two columns as a 64-bit integer and a 64-bit float
* *AND* a virtual schema over `s3://warehouse/direct/` leaving `MERGE_SCHEMA` absent
* *WHEN* an Exasol user reads the declared column types and then queries the table
* *THEN* the virtual schema SHALL declare the WIDER Exasol type for each of the two columns, resolved once from the folded schema, so the declaration reflects the whole table rather than one file
* *AND* the query SHALL return the rows of BOTH files with the values written, the narrow file's values widened, and MUST NOT return NULL, a truncated value, or an error for either file
* *AND* the declared type SHALL be the type the scan registers, so no row crosses the emit boundary at a type the declaration does not name

### Scenario: A file whose column set is a subset returns NULL for the columns it lacks

* *GIVEN* a fixture directory `s3://warehouse/direct/missing_col/` whose first file carries columns `A` and `B` and whose second file carries only `A`
* *AND* a virtual schema over `s3://warehouse/direct/` leaving `MERGE_SCHEMA` absent
* *WHEN* an Exasol user queries that table
* *THEN* the virtual schema SHALL declare BOTH `A` and `B`, so the union of the directory's column sets is the table's column set
* *AND* the rows originating from the second file SHALL carry NULL for `B`, and MUST NOT be dropped, error, or repeat the first file's values
* *AND* the rows originating from the first file SHALL carry their written `B` values unchanged

### Scenario: A column no widening rule folds fails the refresh naming the column and the files

* *GIVEN* a fixture directory `s3://warehouse/direct_incompatible/incompatible/` whose first file declares column `X` as a string and whose second file declares `X` as a 64-bit integer, a pair no supported widening covers
* *AND* a virtual schema over `s3://warehouse/direct_incompatible/` leaving `MERGE_SCHEMA` absent, an ISOLATED base path that no other scenario of this feature creates a virtual schema over, because this scenario requires the enumeration of that base path to FAIL
* *WHEN* `CREATE VIRTUAL SCHEMA` or `REFRESH VIRTUAL SCHEMA` enumerates that table
* *THEN* the statement SHALL FAIL with an error naming the column `X`, both conflicting types, and BOTH file paths, so an operator can locate the offending files without listing the directory
* *AND* the adapter MUST NOT declare the column as `VARCHAR(2000000)`, drop it, pick one file's type, or return a partial table, because each of those answers a correctness question by guessing
* *AND* the error message MUST NOT contain any credential value

### Scenario: MERGE_SCHEMA FALSE declares the sampled footer's type on the refresh and the scan path

* *GIVEN* the `widened/` fixture directory of the widening scenario, whose first file in the listing order carries the NARROW types
* *AND* a virtual schema over `s3://warehouse/direct/` created with `MERGE_SCHEMA = 'FALSE'`
* *WHEN* an Exasol user reads the declared column types and then queries the table
* *THEN* the virtual schema SHALL declare the NARROW Exasol type resolved from that single sampled footer, so the mode is observable in the declaration
* *AND* the pushdown plan for a query over that table SHALL carry the SAME narrow logical type, which is what proves the mode reached the plan path and not only the refresh path

### Scenario: MERGE_SCHEMA FALSE narrows the declaration, not the file set

* *GIVEN* the same `widened/` fixture and the same `MERGE_SCHEMA = 'FALSE'` virtual schema
* *WHEN* an Exasol user queries a column every file shares versus a column a WIDE file stores wider than the narrow declaration
* *THEN* a query over the shared-type column SHALL return every row of BOTH files, so the mode narrows the declaration rather than the file set
* *AND* a query over the wider column SHALL fail with an error naming the column and both types, whether or not the wide file's values fit the narrow type, because the scan refuses that pair rather than narrowing it

### Scenario: A Delta table directory read as raw Parquet returns its tombstoned rows

* *GIVEN* a Delta table directory copied under `s3://warehouse/direct/`, whose transaction log removes at least one data file and whose removed data file still exists in object storage
* *AND* a virtual schema over `s3://warehouse/direct/` with `CATALOG_KIND = 'DIRECT_STORAGE'`
* *WHEN* an Exasol user queries the virtual table that directory produces
* *THEN* the query SHALL return the rows of EVERY Parquet file under that directory, INCLUDING the rows of the tombstoned file, and SHALL apply no deletion vector and no `remove` action
* *AND* the returned row count SHALL be asserted against the raw file contents rather than against the Delta table's current version, so the documented caveat is PINNED as observed behavior rather than stated only in prose
* *AND* the suite SHALL NOT treat this result as a defect, because reading a Delta directory as raw Parquet is a deliberate, documented consequence of the chosen `CATALOG_KIND` rather than a gap in this reader

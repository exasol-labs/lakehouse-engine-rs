# Feature: Direct-Storage E2E Suite

Proves end to end that a plain directory of Parquet files on MinIO is queryable through the
lakehouse Virtual Schema under `CATALOG_KIND = 'DIRECT_STORAGE'`. The fixture set is written by a
raw Parquet writer rather than by a catalog. The catalog-free read path is therefore gated by the
same live Exasol suite every other read path is gated by.

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: MERGE_SCHEMA FALSE declares the sampled footer's type on the refresh and the scan path

* *GIVEN* the `widened/` fixture directory of the widening scenario, whose first file in the listing order carries the NARROW types
* *AND* a virtual schema over `s3://warehouse/direct/` created with `MERGE_SCHEMA = 'FALSE'`
* *WHEN* an Exasol user reads the declared column types and then queries the table
* *THEN* the virtual schema SHALL declare the NARROW Exasol type resolved from that single sampled footer, so the mode is observable in the declaration
* *AND* the pushdown plan for a query over that table SHALL carry the SAME narrow logical type, which is what proves the mode reached the plan path and not only the refresh path
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: MERGE_SCHEMA FALSE narrows the declaration, not the file set

* *GIVEN* the same `widened/` fixture and the same `MERGE_SCHEMA = 'FALSE'` virtual schema
* *WHEN* an Exasol user queries a column every file shares versus a column a WIDE file stores wider than the narrow declaration
* *THEN* a query over the shared-type column SHALL return every row of BOTH files, so the mode narrows the declaration rather than the file set
* *AND* a query over the wider column SHALL fail with an error naming the column and both types, whether or not the wide file's values fit the narrow type, because the scan refuses that pair rather than narrowing it
<!-- /DELTA:NEW -->

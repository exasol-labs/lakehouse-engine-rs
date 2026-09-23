# Feature: Direct-Storage E2E Discovery and Properties

Proves live, against Exasol and MinIO, that the direct-storage catalog kind discovers the right
tables and reads its virtual-schema properties as specified. Proves that the kind rejects a
malformed CONNECTION at `CREATE VIRTUAL SCHEMA` rather than at query time. Proves that the kind
pushes down the same operations every other catalog kind pushes down.

## Background

* This delta amends ONE bullet of ONE scenario: `DEEP` now declares its `y=2026` segment as a
  partition column. Every other scenario and the recorded Background are unchanged.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Only first-level directories holding a data file become virtual tables

* *GIVEN* a base path `s3://warehouse/direct_discovery/` holding a directory `orders/` with one Parquet file, a directory `deep/` whose only Parquet file is at `deep/y=2026/p.parquet`, a directory `empty/` whose only object is `_SUCCESS`, a directory `hidden_only/` whose only Parquet file is at `hidden_only/_staging/p.parquet`, and a loose object `notes.parquet` directly under the base path
* *AND* a virtual schema created over that base path with the direct-storage `CATALOG_KIND`
* *WHEN* an Exasol user reads the served table list from `SYS.EXA_ALL_TABLES`
* *THEN* the virtual schema SHALL serve EXACTLY the tables `ORDERS` and `DEEP`, so a directory is a table when and only when the shared listing rules find a data file under it
* *AND* `EMPTY` and `HIDDEN_ONLY` SHALL be ABSENT from the served tables, and `CREATE VIRTUAL SCHEMA` SHALL still succeed, so a directory with no visible data file is skipped rather than fatal
* *AND* no table SHALL be served for `notes.parquet`, so a loose data file under the base path names no table
* *AND* a `SELECT` over `DEEP` SHALL return that file's rows with its `y=2026` segment as the partition column `Y`, because `HIVE_PARTITIONING` defaults to TRUE
* *AND* the test MUST fail, not skip, when Exasol or MinIO is unavailable
<!-- /DELTA:CHANGED -->

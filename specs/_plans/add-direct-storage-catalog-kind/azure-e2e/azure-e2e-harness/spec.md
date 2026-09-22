# Feature: Azure E2E Harness

Provisions the per-run Azure fixtures the Azure E2E suite reads and proves the engine scans them end to end. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta ADDS ONE scenario and is issue #407. It amends no recorded clause. The per-run
  container, both Iceberg warehouses, the seed configuration, and the container guard are unchanged.
* The added scenario is the ONLY live proof that the direct-storage catalog kind reaches Azure Data
  Lake Storage. Issue #407 names S3 and Azure Blob Storage together, and
  `vs-adapter/connection-credentials-direct-storage` accepts an `abfss` address. An S3-only
  suite would leave that acceptance rule asserted by unit tests alone. CLAUDE.md § Verification
  discipline requires a claimed capability be checked against a live system rather than assumed
  from a registry.
* The scenario reuses the per-run container rather than creating storage of its own. The Azure
  side therefore gains no new orphan surface. The existing container guard still deletes everything
  this suite creates.
* The fixture is a directory of raw Parquet files written directly into the per-run container under
  its own prefix, disjoint from both warehouses' `key-prefix` values. It is NOT an Iceberg table and
  is not seeded through the catalog, because the kind under test reaches no catalog.
* One flat table is enough here. `e2e-harness/direct-storage-e2e` covers schema merging, skip rules,
  naming, and failure messages against MinIO. Repeating them on Azure would multiply a slow,
  credential-gated suite to re-prove format-neutral behavior.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: End-to-end scan over a raw Parquet directory on ADLS returns correct rows

* *GIVEN* the per-run blob container the harness already creates, holding a prefix `direct/` under which one first-level directory `events/` carries two Parquet files written by the harness, at a prefix disjoint from both Iceberg warehouses' `key-prefix` values
* *AND* a CONNECTION whose address is the `abfss://<container>@<account>.dfs.core.windows.net/direct` storage base path and whose password carries `account_name` and `account_key` and NO catalog-authentication field
* *AND* a virtual schema created over that CONNECTION with the direct-storage `CATALOG_KIND` and no `NAMESPACE`
* *WHEN* a user runs `SELECT <subset of columns> FROM <vs>.EVENTS WHERE <predicate>`
* *THEN* `CREATE VIRTUAL SCHEMA` SHALL declare exactly ONE virtual table named for the `events` directory, with the columns the two files' folded footers carry
* *AND* the query SHALL return exactly the rows of BOTH files that satisfy the predicate, projected to the selected columns, with values matching what the harness wrote
* *AND* the scan SHALL read the files at their `abfss://` locations with the account key carried in the CONNECTION, so this scenario proves the `abfss` scheme reaches storage under a kind that authenticates no catalog
* *AND* no account-key value SHALL appear in any test output or in any returned SQL string
* *AND* the fixture SHALL be written under the per-run container's own guard, so it is deleted with the container when the owning scope ends, including on panic
* *AND* the test MUST fail (not skip) when the local stack or the Azure account is unavailable
<!-- /DELTA:NEW -->

# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol
SQL through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, aggregation, GROUP BY, and (new) Iceberg file-pruning
pushdown against a local Exasol Docker container.

## Background

* All E2E tests run against a local Exasol Docker container with MinIO and the Iceberg REST catalog.
* Tests MUST fail (not skip) when the Docker stack or MinIO is unavailable.
* All DSN/connection strings MUST include `validateservercertificate=0`.
* The file-pruning E2E seeds a partitioned Iceberg table whose data files are distributed
  across partition values, so a partition-column predicate can prune whole files.
* See `packaging/e2e-harness-grouped-order` for grouped-aggregate cases that deliberately
  place an aggregate before, between, or after the group keys in the `selectList` — the
  arrangement every case in this spec avoids.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: End-to-end nested aggregate over a grouped sub-select returns the correct outer count

* *GIVEN* an Exasol Docker container with the lakehouse VS adapter and scan UDF installed and a seeded Iceberg table backed by MinIO
* *AND* a nested-aggregate query matching `bench/run.sh` Q7's shape — an outer `COUNT(*)` over an inner high-cardinality grouped aggregate, e.g. `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM {vs_table} GROUP BY id) t`
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a `DataFusion SQL error: Schema error: No field named ...` (or any other planning-time pushdown-SQL-generation error) surfaced from the scan UDF
* *AND* the returned outer `COUNT(*)` MUST equal the number of distinct inner group-key values in the seeded data (equivalently, the single-node DataFusion result for the same nested query)
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable
<!-- /DELTA:NEW -->

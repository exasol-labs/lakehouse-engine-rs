# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol
SQL through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, aggregation, GROUP BY, and (new) Iceberg file-pruning
pushdown against a local Exasol Docker container.

## Background

* All E2E tests run against a local Exasol Docker container with MinIO and the Iceberg REST catalog.
* Tests MUST fail (not skip) when the Docker stack or MinIO is unavailable.
* The file-pruning E2E seeds a partitioned Iceberg table whose data files are distributed
  across partition values, so a partition-column predicate can prune whole files.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: End-to-end filtered query over a partitioned table returns correct rows with file pruning

* *GIVEN* the Docker stack is running with a seeded **partitioned** Iceberg table in the REST catalog over MinIO, whose data files are distributed across partition values
* *AND* the lakehouse VS adapter and scan UDF are installed
* *WHEN* a `SELECT` with a `WHERE` predicate on the partition column (and a second predicate on a value column) is issued against the virtual schema
* *THEN* the returned rows SHALL exactly match the seeded source rows satisfying the predicate, and SHALL be identical to the same query run with Iceberg pruning unable to apply (predicate forced untranslatable)
* *AND* where the harness can observe it (Iceberg `plan_files` output during file resolution), the resolved file list SHALL contain fewer files than the unpruned snapshot file count
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable
<!-- /DELTA:NEW -->

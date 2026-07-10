# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, and Iceberg file-pruning pushdown against a local
Exasol Docker container. The harness installs `LAKEHOUSE_SCAN` as a SCALAR EMIT script
and `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET distributor script. See
`packaging/e2e-harness-grouped-agg` for grouped-aggregate and nested-aggregate E2E
scenarios.

## Background

* Every E2E scenario runs against a local Exasol Docker container over MinIO and MUST fail (never skip) when the stack is unavailable.
* All E2E tests run against a local Exasol Docker container with MinIO and the Iceberg REST catalog.
* All DSN/connection strings MUST include `validateservercertificate=0`.
* The file-pruning E2E seeds a partitioned Iceberg table whose data files are distributed
  across partition values, so a partition-column predicate can prune whole files.
* See `packaging/e2e-harness-grouped-order` for grouped-aggregate cases that deliberately
  place an aggregate before, between, or after the group keys in the `selectList` — the
  arrangement every case in this spec avoids.

## Scenarios

### Scenario: End-to-end projection + filter + LIMIT query returns correct rows

* *GIVEN* the Docker stack is running with a seeded Iceberg table in the REST catalog over MinIO
* *AND* the Rust SLC and the `.so` are installed and the virtual schema is created
* *WHEN* a user runs `SELECT <subset of columns> FROM <vs>.<table> WHERE <predicate> LIMIT <n>`
* *THEN* the query SHALL return exactly the rows that satisfy the predicate, capped at `n`, projected to the selected columns
* *AND* the returned values SHALL match the seeded source data

### Scenario: E2E suite fails when the stack is unavailable

* *GIVEN* the Exasol container is not reachable
* *WHEN* the `exasol-e2e` test suite runs
* *THEN* the suite SHALL fail
* *AND* the suite MUST NOT report the affected tests as skipped or passed

### Scenario: Oversubscribed shard fan-out is observable via EXPLAIN VIRTUAL

* *GIVEN* an Exasol Docker container with the VS installed and a `parallelism_factor` VS property set
* *WHEN* an `EXPLAIN VIRTUAL` of a multi-shard scan query is executed
* *THEN* the EXPLAIN VIRTUAL output SHALL show a nested distributor subquery grouping on `shard_key` (not `IPROC()`) that drives `LAKEHOUSE_DISTRIBUTE_FILES`, wrapped by an outer ungrouped scalar `LAKEHOUSE_SCAN` invocation
* *AND* the outer scalar scan select SHALL NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: Harness provisions the scalar scan and the LUA distributor scripts

* *GIVEN* the E2E harness bootstrapping the lakehouse VS on the Exasol Docker container
* *WHEN* the harness creates the scan-path scripts
* *THEN* the harness SHALL create `LAKEHOUSE_SCAN` as a SCALAR SCRIPT (EMITS its dynamic output columns) referencing the uploaded `.so`
* *AND* the harness SHALL create `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET SCRIPT that passes each shard's `files` VARCHAR through unchanged, referencing no `.so`
* *AND* an end-to-end projection/filter query over the installed scripts SHALL return results identical to the single-node DataFusion equivalent (grouped/nested-aggregate coverage lives in `packaging/e2e-harness-grouped-agg`)
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: End-to-end filtered query over a partitioned table returns correct rows with file pruning

* *GIVEN* the Docker stack is running with a seeded **partitioned** Iceberg table in the REST catalog over MinIO, whose data files are distributed across partition values
* *AND* the lakehouse VS adapter and scan UDF are installed
* *WHEN* a `SELECT` with a `WHERE` predicate on the partition column (and a second predicate on a value column) is issued against the virtual schema
* *THEN* the returned rows SHALL exactly match the seeded source rows satisfying the predicate, and SHALL be identical to the same query run with Iceberg pruning unable to apply (predicate forced untranslatable)
* *AND* where the harness can observe it (Iceberg `plan_files` output during file resolution), the resolved file list SHALL contain fewer files than the unpruned snapshot file count
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

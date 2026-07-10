# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL
through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, aggregation, GROUP BY, and Iceberg file-pruning
pushdown against a local Exasol Docker container. The harness installs `LAKEHOUSE_SCAN`
as a SCALAR EMIT script and `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET distributor script.

## Background

* Every E2E scenario runs against a local Exasol Docker container over MinIO and MUST fail (never skip) when the stack is unavailable.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Oversubscribed shard fan-out is observable via EXPLAIN VIRTUAL

* *GIVEN* an Exasol Docker container with the VS installed and a `parallelism_factor` VS property set
* *WHEN* an `EXPLAIN VIRTUAL` of a multi-shard scan query is executed
* *THEN* the EXPLAIN VIRTUAL output SHALL show a nested distributor subquery grouping on `shard_key` (not `IPROC()`) that drives `LAKEHOUSE_DISTRIBUTE_FILES`, wrapped by an outer ungrouped scalar `LAKEHOUSE_SCAN` invocation
* *AND* the outer scalar scan select SHALL NOT be wrapped in a `SELECT * FROM (...)` materialization boundary
* *AND* the test MUST fail (not skip) if the stack is unavailable
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Harness provisions the scalar scan and the LUA distributor scripts

* *GIVEN* the E2E harness bootstrapping the lakehouse VS on the Exasol Docker container
* *WHEN* the harness creates the scan-path scripts
* *THEN* the harness SHALL create `LAKEHOUSE_SCAN` as a SCALAR SCRIPT (EMITS its dynamic output columns) referencing the uploaded `.so`
* *AND* the harness SHALL create `LAKEHOUSE_DISTRIBUTE_FILES` as a LUA SET SCRIPT that passes each shard's `files` VARCHAR through unchanged, referencing no `.so`
* *AND* an end-to-end projection/filter/aggregate/GROUP BY query over the installed scripts SHALL return results identical to the single-node DataFusion equivalent
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable
<!-- /DELTA:NEW -->

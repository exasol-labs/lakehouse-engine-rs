# Feature: E2E Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol SQL through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying correctness of projection, filter, aggregation, and GROUP BY pushdown against a local Exasol Docker container.

## Background

* All E2E tests run against a local Exasol Docker container with MinIO and the Iceberg REST catalog.
* Tests MUST fail (not skip) when the stack is unavailable.
* All DSN/connection strings MUST include `validateservercertificate=0`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: End-to-end grouped aggregate query returns correct per-group results

* *GIVEN* an Exasol Docker container with the lakehouse VS adapter and scan UDF installed
* *AND* an Iceberg table populated with rows across multiple distinct group-key values (e.g., region, product category)
* *WHEN* a grouped aggregate query is executed against the virtual schema (e.g., `SELECT region, SUM(amount) FROM vs.sales GROUP BY region`)
* *THEN* the result MUST match the same query executed on the raw Iceberg data via DataFusion directly
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: End-to-end multi-key GROUP BY with WHERE filter returns correct results

* *GIVEN* an Exasol Docker container with the VS installed and a multi-column Iceberg table
* *WHEN* a query with GROUP BY on two columns and a WHERE predicate is executed (e.g., `SELECT cat, region, COUNT(*) FROM vs.t WHERE status = 'ACTIVE' GROUP BY cat, region`)
* *THEN* the result MUST match the single-node DataFusion equivalent
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end GROUP BY with expression group key returns correct results

* *GIVEN* an Exasol Docker container with the VS installed and an Iceberg table with a timestamp column
* *WHEN* a query groups by a scalar expression over a column (e.g., `SELECT YEAR(order_date), COUNT(*) FROM vs.orders GROUP BY YEAR(order_date)`)
* *THEN* the result MUST match the equivalent single-node result
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end grouped AVG is correct across all groups

* *GIVEN* an Exasol Docker container with an Iceberg table populated with known values and multiple groups with unequal row counts
* *WHEN* `SELECT group_col, AVG(value_col) FROM vs.t GROUP BY group_col` is executed
* *THEN* the AVG per group MUST equal the arithmetic mean of that group's `value_col` values
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: High-cardinality grouped query completes via memory-pool spill

* *GIVEN* an Exasol Docker container whose scan UDF runs with a real-disk `/tmp` spill directory
* *AND* an Iceberg table with a high number of distinct group-key values
* *WHEN* `SELECT group_col, COUNT(*) FROM vs.t GROUP BY group_col` is executed against the virtual schema
* *THEN* the query SHALL complete and the per-group counts MUST match the single-node DataFusion equivalent
* *AND* the scan SHALL NOT crash the UDF process at high group cardinality
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: Oversubscribed shard fan-out is observable via EXPLAIN VIRTUAL

* *GIVEN* an Exasol Docker container with the VS installed and a `PARALLELISM_FACTOR` VS property set
* *WHEN* `EXPLAIN VIRTUAL SELECT region, COUNT(*) FROM vs.sales GROUP BY region` is executed
* *THEN* the EXPLAIN VIRTUAL output SHALL show the scan-driving SQL grouping on `shard_key` (not `IPROC()`)
* *AND* the test MUST fail (not skip) if the stack is unavailable
<!-- /DELTA:NEW -->

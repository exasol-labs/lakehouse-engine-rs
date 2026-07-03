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

### Scenario: End-to-end filtered query over a partitioned table returns correct rows with file pruning

* *GIVEN* the Docker stack is running with a seeded **partitioned** Iceberg table in the REST catalog over MinIO, whose data files are distributed across partition values
* *AND* the lakehouse VS adapter and scan UDF are installed
* *WHEN* a `SELECT` with a `WHERE` predicate on the partition column (and a second predicate on a value column) is issued against the virtual schema
* *THEN* the returned rows SHALL exactly match the seeded source rows satisfying the predicate, and SHALL be identical to the same query run with Iceberg pruning unable to apply (predicate forced untranslatable)
* *AND* where the harness can observe it (Iceberg `plan_files` output during file resolution), the resolved file list SHALL contain fewer files than the unpruned snapshot file count
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: End-to-end nested aggregate over a grouped sub-select returns the correct outer count

* *GIVEN* an Exasol Docker container with the lakehouse VS adapter and scan UDF installed and a seeded Iceberg table backed by MinIO
* *AND* a nested-aggregate query matching `bench/run.sh` Q7's shape — an outer `COUNT(*)` over an inner high-cardinality grouped aggregate, e.g. `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM {vs_table} GROUP BY id) t`
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a `DataFusion SQL error: Schema error: No field named ...` (or any other planning-time pushdown-SQL-generation error) surfaced from the scan UDF
* *AND* the returned outer `COUNT(*)` MUST equal the number of distinct inner group-key values in the seeded data (equivalently, the single-node DataFusion result for the same nested query)
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

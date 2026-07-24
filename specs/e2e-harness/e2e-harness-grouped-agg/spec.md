# Feature: End-to-End Harness — Grouped and Nested Aggregate Queries

End-to-end scenarios covering grouped-aggregate and nested-aggregate correctness against
a local Exasol Docker container: single-key and multi-key GROUP BY, expression group
keys, grouped AVG, high-cardinality spill, and a nested aggregate over a grouped
sub-select. Split out of `e2e-harness/e2e-harness` to keep that feature's core
projection/filter/LIMIT/file-pruning scenarios separate from aggregate-specific ones. See
`e2e-harness/e2e-harness-grouped-order` for grouped-aggregate cases that deliberately place
an aggregate before, between, or after the group keys in the `selectList`.

## Background

* Every E2E scenario runs against a local Exasol Docker container over MinIO and MUST fail (never skip) when the stack is unavailable.
* See `e2e-harness/e2e-harness` for the core projection/filter/LIMIT and file-pruning E2E scenarios and the harness's script-provisioning scenario.

## Scenarios

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

### Scenario: End-to-end nested aggregate over a grouped sub-select returns the correct outer count

* *GIVEN* an Exasol Docker container with the lakehouse VS adapter and scan UDF installed and a seeded Iceberg table backed by MinIO
* *AND* a nested-aggregate query matching `bench/run.sh` Q7's shape — an outer `COUNT(*)` over an inner high-cardinality grouped aggregate, e.g. `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM {vs_table} GROUP BY id) t`
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a `DataFusion SQL error: Schema error: No field named ...` (or any other planning-time pushdown-SQL-generation error) surfaced from the scan UDF
* *AND* the returned outer `COUNT(*)` MUST equal the number of distinct inner group-key values in the seeded data (equivalently, the single-node DataFusion result for the same nested query)
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

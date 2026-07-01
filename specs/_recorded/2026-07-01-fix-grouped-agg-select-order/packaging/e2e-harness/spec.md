# Feature: End-to-End Harness

End-to-end test suite that exercises the full lakehouse VS query path — from Exasol
SQL through the adapter and scan UDF to Iceberg Parquet files in MinIO — verifying
correctness of projection, filter, aggregation, GROUP BY, and Iceberg file-pruning
pushdown against a local Exasol Docker container.

## Background

* All E2E tests run against a local Exasol Docker container with MinIO and the Iceberg REST catalog.
* Tests MUST fail (not skip) when the Docker stack or MinIO is unavailable.
* All DSN/connection strings MUST include `validateservercertificate=0`.
* The new grouped-order cases deliberately place an aggregate before, between, or after
  the group keys — the arrangement every pre-existing GROUP BY E2E case avoided, which
  is how the select-list-order bug (#33) shipped undetected.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: End-to-end grouped aggregate with an aggregate before the group key returns correct results

* *GIVEN* an Exasol Docker container with the lakehouse VS adapter and scan UDF installed and an Iceberg table backed by MinIO
* *AND* a select list that places the aggregate BEFORE the group key (the issue #33 repro), e.g. `SELECT SUM(score), MOD(id,4) FROM {vs_table} GROUP BY MOD(id,4)`
* *WHEN* the grouped aggregate query is executed against the virtual schema
* *THEN* the query MUST succeed without an "Adapter generated invalid pushdown query ... Data type mismatch in column number N" error
* *AND* the per-group results MUST match the key-first ordering of the same query (already proven correct), accounting for the transposed column positions
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: End-to-end interleaved multi-key GROUP BY with an aggregate between the keys returns correct results

* *GIVEN* an Exasol Docker container with the VS installed and an Iceberg table backed by MinIO
* *AND* a select list that places an aggregate BETWEEN two group keys, e.g. `SELECT MOD(id,4), SUM(score), MOD(id,2) FROM {vs_table} GROUP BY MOD(id,4), MOD(id,2)`
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a pushdown column-type mismatch error
* *AND* the per-group results MUST match the single-node DataFusion equivalent (or the key-first ordering of the same query), accounting for column positions
* *AND* the test MUST fail (not skip) if the stack is unavailable
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: End-to-end expression group key placed after an aggregate returns correct results

* *GIVEN* an Exasol Docker container with the VS installed and an Iceberg table backed by MinIO
* *AND* a select list that places a scalar-expression group key AFTER the aggregate, e.g. `SELECT COUNT(*), MOD(id,4) FROM {vs_table} GROUP BY MOD(id,4)`
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a pushdown column-type mismatch error, and the expression group-key result column MUST carry its Exasol-declared type (e.g. a DECIMAL for `MOD(id,4)`), not a defaulted `VARCHAR(2000000)`
* *AND* the per-group results MUST match the key-first ordering of the same query
* *AND* the test MUST fail (not skip) if the stack is unavailable
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: End-to-end aggregate-first GROUP BY with HAVING returns correct results

* *GIVEN* an Exasol Docker container with the VS installed and an Iceberg table backed by MinIO
* *AND* a select list that places the aggregate before the group key and includes a HAVING clause, e.g. `SELECT SUM(score), MOD(id,4) FROM {vs_table} GROUP BY MOD(id,4) HAVING SUM(score) > n`
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a pushdown column-type mismatch error, applying the HAVING predicate in the outer wrapper (the adapter advertises `AGGREGATE_HAVING`) so only groups whose aggregate satisfies the predicate are returned
* *AND* the per-group results MUST match the key-first ordering of the same query with the same HAVING
* *AND* the test MUST fail (not skip) if the stack is unavailable
<!-- /DELTA:NEW -->

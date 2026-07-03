# Feature: End-to-End Harness — Grouped-Aggregate Select-List Ordering

Extends `packaging/e2e-harness` with grouped-aggregate E2E cases that deliberately place
an aggregate before, between, or after the group keys in the `selectList`, and — with the
advertisement of `AGGREGATE_GROUP_BY_TUPLE` (issue #53) — cases that prove a multi-column
GROUP BY is actually pushed down to the scan UDF (via `EXPLAIN VIRTUAL`) rather than
silently exercising the raw-scan fallback that Exasol aggregates itself.

## Background

* All E2E tests run against a local Exasol Docker container with MinIO and the Iceberg REST catalog.
* Tests MUST fail (not skip) when the Docker stack or MinIO is unavailable.
* All DSN/connection strings MUST include `validateservercertificate=0`.
* These cases assert against the already-correct key-first ordering of the same query
  (or the single-node DataFusion equivalent), accounting for the transposed column
  positions.
* A pushdown-occurred assertion inspects the `EXPLAIN VIRTUAL` output of the query and
  confirms the generated SQL fans out via `GROUP BY shard_key` (partial aggregation),
  not a raw `SELECT`-all fallback shape and not `IPROC()` sharding.

## Scenarios

### Scenario: End-to-end grouped aggregate with an aggregate before the group key returns correct results

* *GIVEN* an Exasol Docker container with the lakehouse VS adapter and scan UDF installed and an Iceberg table backed by MinIO
* *AND* a select list that places the aggregate BEFORE the group key (the issue #33 repro), e.g. `SELECT SUM(score), MOD(id,4) FROM {vs_table} GROUP BY MOD(id,4)`
* *WHEN* the grouped aggregate query is executed against the virtual schema
* *THEN* the query MUST succeed without an "Adapter generated invalid pushdown query ... Data type mismatch in column number N" error
* *AND* the per-group results MUST match the key-first ordering of the same query (already proven correct), accounting for the transposed column positions
* *AND* the test MUST fail (not skip) if the Exasol Docker container or MinIO is unavailable

### Scenario: End-to-end interleaved multi-key GROUP BY with an aggregate between the keys returns correct results

* *GIVEN* an Exasol Docker container with the VS installed and an Iceberg table backed by MinIO
* *AND* a select list that places an aggregate BETWEEN two group keys, e.g. `SELECT MOD(id,4), SUM(score), MOD(id,2) FROM {vs_table} GROUP BY MOD(id,4), MOD(id,2)`
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a pushdown column-type mismatch error
* *AND* the per-group results MUST match the single-node DataFusion equivalent (or the key-first ordering of the same query), accounting for column positions
* *AND* the `EXPLAIN VIRTUAL` output for this multi-key query MUST show the query was pushed down as partial aggregation (the pushed scan spec carries `group_keys` and the outer wrapper merges the per-shard partial-aggregate `PARTIAL_*` columns), does not contain `IPROC()`, and is not a raw row-scan fallback — proving the multi-key path actually exercises partial aggregation rather than the pre-#53 raw-scan fallback. (The `GROUP BY shard_key` fan-out appears only when the assigned file list spans more than one shard, so it is NOT a reliable pushdown indicator — a WHERE filter that prunes to a single file pushes down with no `shard_key` fan-out.)
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end expression group key placed after an aggregate returns correct results

* *GIVEN* an Exasol Docker container with the VS installed and an Iceberg table backed by MinIO
* *AND* a select list that places a scalar-expression group key AFTER the aggregate, e.g. `SELECT COUNT(*), MOD(id,4) FROM {vs_table} GROUP BY MOD(id,4)`
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a pushdown column-type mismatch error, and the expression group-key result column MUST carry its Exasol-declared type (e.g. a DECIMAL for `MOD(id,4)`), not a defaulted `VARCHAR(2000000)`
* *AND* the per-group results MUST match the key-first ordering of the same query
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end aggregate-first GROUP BY with HAVING returns correct results

* *GIVEN* an Exasol Docker container with the VS installed and an Iceberg table backed by MinIO
* *AND* a select list that places the aggregate before the group key and includes a HAVING clause, e.g. `SELECT SUM(score), MOD(id,4) FROM {vs_table} GROUP BY MOD(id,4) HAVING SUM(score) > n`
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a pushdown column-type mismatch error, applying the HAVING predicate in the outer wrapper (the adapter advertises `AGGREGATE_HAVING`) so only groups whose aggregate satisfies the predicate are returned
* *AND* the per-group results MUST match the key-first ordering of the same query with the same HAVING
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end multi-column GROUP BY over plain columns is pushed down (EXPLAIN-verified)

* *GIVEN* an Exasol Docker container with the VS installed and an Iceberg table backed by MinIO
* *AND* a query grouping by two or more plain columns with an aggregate and a WHERE filter, e.g. `SELECT MOD(id,4), MOD(id,2), COUNT(*) FROM {vs_table} WHERE score > 50.0 GROUP BY MOD(id,4), MOD(id,2)`
* *WHEN* the query is executed against the virtual schema
* *THEN* the `EXPLAIN VIRTUAL` output MUST show the query was pushed down as partial aggregation (the pushed scan spec carries `group_keys` and the outer wrapper merges the `PARTIAL_*` columns), does not contain `IPROC()`, and is not a raw row-scan fallback. (The WHERE filter here may prune the file list to a single shard, in which case there is legitimately no `GROUP BY shard_key` fan-out — so the pushdown evidence is `group_keys` in the scan spec, not `GROUP BY shard_key`.)
* *AND* the per-group results MUST be correct (each group's COUNT matches the expected per-bucket row counts and the totals sum to the filtered row count)
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end expression-valued multi-key tuple GROUP BY returns correct results

* *GIVEN* an Exasol Docker container with the VS installed and an Iceberg table backed by MinIO
* *AND* a query whose GROUP BY tuple contains two or more scalar-expression elements, each built from *advertised* scalar functions, e.g. `SELECT MOD(id,4), UPPER(name), COUNT(*) FROM {vs_table} GROUP BY MOD(id,4), UPPER(name)` (a mixed-type tuple: a DECIMAL key and a VARCHAR key). NOTE: keys built from *unadvertised* scalar operators — arithmetic (`/`, `*`) and `CAST` — are NOT pushed down; Exasol will not send them as pushed group keys (the adapter renders them, but capability advertisement for arithmetic/CAST is future scope), so it falls back to a raw scan. Expression tuple keys must therefore use advertised functions to exercise this path.
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a pushdown column-type mismatch error, and each expression group-key column MUST carry its OWN Exasol-declared type resolved by its select-list index (e.g. the DECIMAL key typed DECIMAL and the VARCHAR key typed VARCHAR), not a shared or defaulted `VARCHAR(2000000)`
* *AND* the `EXPLAIN VIRTUAL` output MUST show the query was pushed down as partial aggregation (the pushed scan spec carries `group_keys` and the outer wrapper merges the `PARTIAL_*` columns), not a raw row-scan fallback
* *AND* the per-group results MUST match the key-first ordering of the same query
* *AND* the test MUST fail (not skip) if the stack is unavailable

### Scenario: End-to-end multi-key GROUP BY with HAVING and LIMIT returns correct results

* *GIVEN* an Exasol Docker container with the VS installed and an Iceberg table backed by MinIO
* *AND* a query grouping by two keys with a HAVING predicate over the aggregate and a LIMIT, e.g. `SELECT MOD(id,4), MOD(id,2), SUM(score) FROM {vs_table} GROUP BY MOD(id,4), MOD(id,2) HAVING SUM(score) > n LIMIT k`
* *WHEN* the query is executed against the virtual schema
* *THEN* the query MUST succeed without a pushdown column-type mismatch error, applying the HAVING predicate in the outer wrapper so only groups whose aggregate satisfies the predicate are returned, and applying the LIMIT only in the outer wrapper (never in the per-shard partial scan)
* *AND* the returned groups MUST all satisfy the HAVING predicate and the row count MUST NOT exceed the LIMIT
* *AND* the per-group results MUST match the key-first ordering of the same query with the same HAVING and LIMIT
* *AND* the test MUST fail (not skip) if the stack is unavailable

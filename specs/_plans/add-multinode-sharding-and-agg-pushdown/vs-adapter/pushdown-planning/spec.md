# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it
resolves the Iceberg data-file list once, captures the requested projection, filter,
LIMIT, and any supported single-group aggregate, and emits the SQL that drives the
DataFusion scan SET UDF — sharded across cluster nodes — over exactly those files.

## Background

* The data-file list is resolved exactly once per pushdown, in the planning layer;
  the scan UDF never discovers files itself.
* The shard count comes from the `CLUSTER_NODES` virtual-schema property (default 1).
* A predicate or aggregate the adapter cannot translate is omitted/falls back rather
  than producing an incorrect result.
* Credentials MUST NOT appear in any returned SQL string or error message.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, and `AVG(col)`
* *AND* the capabilities list MUST NOT include `GROUP BY`, `HAVING`, `COUNT(DISTINCT ...)`, or join pushdown
* *AND* the capabilities list SHALL continue to include column projection, filter predicates, and LIMIT
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Aggregate query is translated into a partial-aggregate scan spec

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query whose select list is one or more supported aggregate functions over the whole table
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL recognise the request as an aggregate query and resolve the data-file list exactly once
* *AND* the adapter SHALL build a scan spec carrying, for each requested aggregate, its function kind and target column (the wildcard for `COUNT(*)`), plus any pushed-down filter so the partial aggregate covers filtered rows only
* *AND* the adapter MUST NOT push down an aggregate the scan UDF cannot compute, falling back to row scanning for that query instead
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Aggregate wrapper SQL merges per-shard partial results

* *GIVEN* an aggregate pushdown over a file list partitioned into one or more shards
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL drive the scan SET UDF to emit one partial-aggregate row per shard
* *AND* the SQL SHALL wrap those partial rows in an outer aggregation that merges them into the final result: `SUM` over per-shard partial counts for `COUNT`, `SUM` over partial sums for `SUM`, `MIN`/`MAX` over partial extrema for `MIN`/`MAX`
* *AND* the merged result SHALL equal the result of the same aggregate evaluated over all rows on a single node
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: AVG is pushed down as a sum/count pair and divided in the wrapper

* *GIVEN* a query selecting `AVG(col)` over the table
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the scan spec SHALL instruct the scan UDF to emit a partial `SUM(col)` and a partial `COUNT(col)` pair rather than a per-shard average
* *AND* the wrapper SQL SHALL compute the final average as `SUM(partial_sum) / SUM(partial_count)`
* *AND* the wrapper SQL SHALL yield NULL when the total partial count is zero, never dividing by zero
<!-- /DELTA:NEW -->

# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and any supported single-group or grouped aggregate, and emits the SQL that drives the DataFusion scan SET UDF — fanned out across G oversubscribed work-unit shards via `GROUP BY shard_key` — over exactly those files.

## Background

* The adapter receives a `pushdown` request carrying the projection, filter, and aggregate specification from Exasol.
* The adapter resolves the Iceberg snapshot and file list exactly once per query.
* The shard count G is `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300 and clamped to the file count, per the `parallelism/work-unit-sharding` feature; the scan-driving SQL groups on `shard_key`, never on `IPROC()`.
* Credentials MUST NOT appear in any returned SQL or error message.
* A predicate or group-key expression the adapter cannot translate is omitted from the scan spec; Exasol keeps it as a correctness backstop.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT`, `COUNT(*)`, `SUM`, `MIN`, `MAX`, and `AVG`
* *AND* the capabilities list SHALL include `AGGREGATE_GROUP_BY_COLUMN` and `AGGREGATE_GROUP_BY_EXPRESSION` for GROUP BY pushdown
* *AND* the capabilities list MUST NOT include `AGGREGATE_GROUP_BY_TUPLE`, `AGGREGATE_HAVING`, `FN_AGG_COUNT_DISTINCT`, or join pushdown
* *AND* the capabilities list SHALL continue to include column projection, filter predicates, and LIMIT
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Grouped aggregate query is detected and translated to a grouped scan spec

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query whose select list contains supported aggregate functions and a non-empty GROUP BY clause
* *WHEN* Exasol sends the corresponding `pushdown` request with `aggregationType: "group_by"`
* *THEN* the adapter SHALL recognise the request as a grouped aggregate query and render each GROUP BY expression node to a DataFusion SQL fragment using the VS expression translator
* *AND* the adapter SHALL build a scan spec carrying both the rendered group-key expressions and the aggregate plans
* *AND* the adapter MUST NOT push down a grouped aggregate if any group-key expression cannot be translated, falling back to row scanning instead

### Scenario: Grouped scan spec carries group-key rendered SQL fragments

* *GIVEN* a grouped aggregate pushdown request whose GROUP BY clause contains a mix of column references and scalar expressions (e.g., `YEAR(ts_col)`)
* *WHEN* the adapter builds the scan spec
* *THEN* the scan spec SHALL carry a `group_keys` field containing the rendered DataFusion SQL fragment for each group-key expression in order
* *AND* each group-key expression MUST be renderable by the VS expression translator in raising mode
* *AND* the scan UDF MUST use the same rendered expressions in its per-shard DataFusion GROUP BY clause

### Scenario: Grouped scan-driving SQL fans out via GROUP BY shard_key over G work units

* *GIVEN* a grouped aggregate pushdown over a file list partitioned into G work-unit shards
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL group the per-shard rows on `shard_key` (one group per shard), NOT on `IPROC()`
* *AND* G SHALL be `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300 and clamped to the file count, so the shard groups distribute round-robin across nodes and multiplex onto each node's core pool
* *AND* the scan SET UDF SHALL be invoked once per shard with that shard's explicit file subset

### Scenario: LIMIT is NOT pushed into per-shard scan for a grouped query

* *GIVEN* a grouped aggregate query with a LIMIT clause
* *WHEN* the adapter builds the grouped scan spec
* *THEN* the scan spec MUST NOT carry the LIMIT value in the per-shard partial scan
* *AND* the LIMIT SHALL appear only in the outer wrapper SQL that merges partial-aggregate results from all shards

### Scenario: NULL group keys are grouped together consistently

* *GIVEN* a table with rows where the GROUP BY column contains NULL values
* *WHEN* the grouped aggregate scan runs across one or more shards
* *THEN* all rows with a NULL value in the GROUP BY column SHALL be aggregated into a single group
* *AND* this behavior MUST match standard SQL GROUP BY NULL semantics (NULLs are equal for grouping purposes in both DataFusion and Exasol)

### Scenario: Grouped aggregate wrapper SQL re-groups partial results per user group key

* *GIVEN* a grouped aggregate pushdown fanned out over G shards via `GROUP BY shard_key`
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the inner `shard_key` grouping SHALL parallelize the scan while DataFusion performs the user GROUP BY inside each shard invocation, emitting one partial-aggregate row per distinct user group per shard
* *AND* the outer wrapper SQL SHALL GROUP BY the user group-key columns and merge the per-shard partials using the same SUM/MIN/MAX/AVG-pair decomposition as the single-group path
* *AND* the merged result per group SHALL equal the result of the same grouped aggregate evaluated over all rows on a single node

### Scenario: Adapter falls back to row scan for unsupported grouped aggregate shape

* *GIVEN* a pushdown request with `aggregationType: "group_by"` where any select-list item is not a supported aggregate function or a plain group-key column reference
* *OR* any group-key expression is not translatable by the VS expression translator
* *WHEN* the adapter processes the request
* *THEN* the adapter SHALL fall back to row scanning (emitting a row-scan ScanSpec with no aggregates field)
* *AND* Exasol SHALL apply the aggregate on the returned rows using its own engine
<!-- /DELTA:NEW -->

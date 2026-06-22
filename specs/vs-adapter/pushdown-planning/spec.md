# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it
resolves the Iceberg data-file list once, captures the requested projection, filter,
LIMIT, and any supported single-group or grouped aggregate, and emits the SQL that
drives the DataFusion scan SET UDF — fanned out across G oversubscribed work-unit
shards via `GROUP BY shard_key` — over exactly those files.

## Background

* The adapter receives a `pushdown` request carrying the projection, filter, and
  aggregate specification from Exasol.
* The adapter resolves the Iceberg snapshot and file list exactly once per query.
* The shard count G is `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300 and clamped
  to the file count, per the `parallelism/work-unit-sharding` feature; the scan-driving
  SQL groups on `shard_key`, never on `IPROC()`.
* Credentials MUST NOT appear in any returned SQL or error message.
* A predicate or group-key expression the adapter cannot translate is omitted from the
  scan spec; Exasol keeps it as a correctness backstop.

## Scenarios

### Scenario: Pushdown resolves the file list once and builds a scan-driving query

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query that projects a subset of columns
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL resolve the Iceberg snapshot and its data-file list exactly once
* *AND* the adapter SHALL return a JSON response of type `pushdown` containing SQL that invokes the scan SET UDF
* *AND* that SQL MUST pass the resolved data-file list as an explicit argument to the UDF
* *AND* the adapter MUST NOT require the scan UDF to discover files itself

### Scenario: Projection is pushed into the scan-driving query

* *GIVEN* a query that selects only some of the table's columns
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the generated scan-driving SQL SHALL carry only the projected columns to the UDF
* *AND* the UDF's declared EMITS column list SHALL match the projected columns in order and type

### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the scan spec passed to the UDF
* *AND* a predicate the adapter cannot translate SHALL be omitted from the scan spec rather than produce an incorrect result

### Scenario: LIMIT is pushed into the scan spec

* *GIVEN* a query with a LIMIT clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the scan spec passed to the UDF SHALL carry the row limit
* *AND* the generated SQL MAY also retain the LIMIT at the Exasol level as a correctness backstop

### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT`/`COUNT(*)`/`SUM`/`MIN`/`MAX`/`AVG`, `AGGREGATE_GROUP_BY_COLUMN`/`AGGREGATE_GROUP_BY_EXPRESSION`/`AGGREGATE_HAVING`, the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`, and (still) column projection, scalar select-list expressions, filter predicates, and LIMIT
* *AND* the capabilities list MUST NOT include `AGGREGATE_GROUP_BY_TUPLE`, `FN_AGG_COUNT_DISTINCT` (or any other `*_DISTINCT` aggregate), `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`, or join pushdown

### Scenario: Aggregate query is translated into a partial-aggregate scan spec

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query whose select list is one or more supported aggregate functions over the whole table
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL recognise the request as an aggregate query and resolve the data-file list exactly once
* *AND* the adapter SHALL build a scan spec carrying, for each requested aggregate, its function kind and target column (the wildcard for `COUNT(*)`), plus any pushed-down filter so the partial aggregate covers filtered rows only
* *AND* the adapter MUST NOT push down an aggregate the scan UDF cannot compute, falling back to row scanning for that query instead

### Scenario: Aggregate wrapper SQL merges per-shard partial results

* *GIVEN* an aggregate pushdown over a file list partitioned into one or more shards
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL drive the scan SET UDF to emit one partial-aggregate row per shard
* *AND* the SQL SHALL wrap those partial rows in an outer aggregation that merges them into the final result: `SUM` over per-shard partial counts for `COUNT`, `SUM` over partial sums for `SUM`, `MIN`/`MAX` over partial extrema for `MIN`/`MAX`
* *AND* the merged result SHALL equal the result of the same aggregate evaluated over all rows on a single node

### Scenario: AVG is pushed down as a sum/count pair and divided in the wrapper

* *GIVEN* a query selecting `AVG(col)` over the table
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the scan spec SHALL instruct the scan UDF to emit a partial `SUM(col)` and a partial `COUNT(col)` pair rather than a per-shard average
* *AND* the wrapper SQL SHALL compute the final average as `SUM(partial_sum) / SUM(partial_count)`
* *AND* the wrapper SQL SHALL yield NULL when the total partial count is zero, never dividing by zero

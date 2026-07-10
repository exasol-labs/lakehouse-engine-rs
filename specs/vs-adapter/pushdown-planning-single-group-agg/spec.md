# Feature: Pushdown Planning — Single-Group Aggregate

Single-group (ungrouped) aggregate pushdown: advertising which aggregate functions the
adapter supports, translating an ungrouped aggregate query into a partial-aggregate scan
spec, and merging each shard's partial-aggregate row into the final result. Split out of
`vs-adapter/pushdown-planning` to keep that feature's core file-resolution/projection/
filter/LIMIT scenarios separate from aggregate-specific ones. See
`vs-adapter/pushdown-planning-grouped-agg` for GROUP BY aggregate pushdown and
`vs-adapter/pushdown-planning-count-distinct` for `COUNT(DISTINCT ...)` decomposition.

## Background

* Single-group aggregate pushdown is decomposed into a per-shard partial aggregate (computed by the `LAKEHOUSE_SCAN` scalar EMIT UDF) and an outer ungrouped merge aggregation in the wrapper SQL.
* The `GROUP BY shard_key` used for cluster fan-out lives only inside the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery, never at the outer merge level.
* See `vs-adapter/pushdown-planning` for the shard-invariant common spec, file-list resolution, and non-aggregate pushdown scenarios.

## Scenarios

### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT`/`COUNT(*)`/`SUM`/`MIN`/`MAX`/`AVG`, `AGGREGATE_GROUP_BY_COLUMN`/`AGGREGATE_GROUP_BY_EXPRESSION`/`AGGREGATE_GROUP_BY_TUPLE`/`AGGREGATE_HAVING`, the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`, single-group `FN_AGG_COUNT_DISTINCT`, and (still) column projection, scalar select-list expressions, filter predicates, and LIMIT
* *AND* the adapter SHALL advertise `AGGREGATE_GROUP_BY_TUPLE` only because the grouped-aggregate detection and scan-driving SQL builder handle an arbitrary number of group keys (see `vs-adapter/pushdown-planning-grouped-agg`), so a GROUP BY over two or more keys is pushed down as node-local partial aggregation rather than falling back to a raw row scan that Exasol aggregates itself
* *AND* the adapter SHALL advertise `FN_AGG_COUNT_DISTINCT` because a single-group `COUNT(DISTINCT col)` is decomposed via per-shard local distinct sets merged by a scalar merge UDF (see `vs-adapter/pushdown-planning-count-distinct`); a `COUNT(DISTINCT ...)` inside a GROUP BY request still falls back to row scanning
* *AND* the adapter SHALL advertise the inner equi-join capabilities `JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI` (see `vs-adapter/pushdown-planning-join`), while the outer/all-condition join capabilities and any Cartesian-product capability remain absent
* *AND* the capabilities list MUST NOT include `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`, `JOIN_TYPE_LEFT_OUTER`/`JOIN_TYPE_RIGHT_OUTER`/`JOIN_TYPE_FULL_OUTER`, or `JOIN_CONDITION_ALL`

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
* *THEN* the generated SQL SHALL drive the `LAKEHOUSE_SCAN` SCALAR EMIT UDF — fired once per distributed shard row from the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor (or once directly for a single-shard plan) — to emit one partial-aggregate row per shard
* *AND* the SQL SHALL wrap those partial rows in an OUTER ungrouped aggregation over the scalar scan select that merges them into the final result: `SUM` over per-shard partial counts for `COUNT`, `SUM` over partial sums for `SUM`, `MIN`/`MAX` over partial extrema for `MIN`/`MAX`
* *AND* the `GROUP BY shard_key` used for cluster fan-out SHALL live only inside the nested distributor subquery, never at the outer merge level
* *AND* the merged result SHALL equal the result of the same aggregate evaluated over all rows on a single node

### Scenario: AVG is pushed down as a sum/count pair and divided in the wrapper

* *GIVEN* a query selecting `AVG(col)` over the table
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the scan spec SHALL instruct the scan UDF to emit a partial `SUM(col)` and a partial `COUNT(col)` pair rather than a per-shard average
* *AND* the wrapper SQL SHALL compute the final average as `SUM(partial_sum) / SUM(partial_count)`
* *AND* the wrapper SQL SHALL yield NULL when the total partial count is zero, never dividing by zero

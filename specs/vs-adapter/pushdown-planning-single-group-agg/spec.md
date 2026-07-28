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
* The shard-invariant common spec's `projection` field is consulted only on the row-scan dispatch path. On the aggregate dispatch path the scan UDF reads the `aggregates` field and derives the DataFusion physical projection from the partial-aggregate query text, so `projection` is left empty.
* A single-group `COUNT(DISTINCT ...)` is handled by a branch condition in detection, NOT by the ordinary partial/merge machinery. EXACTLY one `COUNT(DISTINCT col)`/`COUNT(DISTINCT expr)` and no other select-list item (Case 1) is decomposed into a dedicated DISTINCT row-scan fan-out (see `vs-adapter/pushdown-planning-count-distinct`). MORE THAN ONE distinct aggregate, or a distinct aggregate alongside any ordinary SUM/MIN/MAX/COUNT/AVG aggregate (Case 2/3), MUST decline the fan-out and route to a qualified single-table wrapper — the same shape the grouped-aggregate decline fallback uses (`vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback`) — whose OWN rendered SQL computes every aggregate (including every DISTINCT) over a materialized sharded raw scan, so Exasol passes the one-row result through. The adapter MUST NOT return a bare row scan for such a request: Exasol never re-aggregates a declined pushdown, so raw source columns where `selectListDataTypes` expects the aggregate columns are rejected (`sqlCode 04000`, column-count mismatch). A distinct fan-out MUST NOT be composed as a SELECT-list scalar subquery either: Exasol rejects an emitting UDF call nested in a scalar subquery at compile time (`sqlCode 04000`, "emitting function in expression").
* See `vs-adapter/pushdown-planning` for the shard-invariant common spec, file-list resolution, and non-aggregate pushdown scenarios.

## Scenarios

### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT`/`COUNT(*)`/`SUM`/`MIN`/`MAX`/`AVG`, `AGGREGATE_GROUP_BY_COLUMN`/`AGGREGATE_GROUP_BY_EXPRESSION`/`AGGREGATE_GROUP_BY_TUPLE`/`AGGREGATE_HAVING`, the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`, single-group `FN_AGG_COUNT_DISTINCT`, and (still) column projection, scalar select-list expressions, filter predicates, and LIMIT
* *AND* the adapter SHALL advertise `AGGREGATE_GROUP_BY_TUPLE` only because the grouped-aggregate detection and scan-driving SQL builder handle an arbitrary number of group keys (see `vs-adapter/pushdown-planning-grouped-agg`), so a GROUP BY over two or more keys is pushed down as node-local partial aggregation rather than falling back to a raw row scan that Exasol aggregates itself
* *AND* the adapter SHALL advertise `FN_AGG_COUNT_DISTINCT` because a single-group `COUNT(DISTINCT col)` is decomposed via a dedicated per-shard DISTINCT row-scan fan-out whose locally-distinct values are counted by an outer Exasol-native `COUNT(DISTINCT)` (see `vs-adapter/pushdown-planning-count-distinct`); a `COUNT(DISTINCT ...)` inside a GROUP BY request still falls back to row scanning
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

### Scenario: Single-group aggregate scan spec leaves the projection field empty

* *GIVEN* an ungrouped aggregate `pushdown` request over a table with more than one column (e.g. `SELECT COUNT(*)`, `SELECT SUM(col)`, or `SELECT MIN(col), MAX(col)`)
* *WHEN* the adapter builds the partial-aggregate scan spec
* *THEN* the shard-invariant common spec's `projection` field SHALL be empty, NOT the full base-table column list
* *AND* the referenced-column information SHALL be carried in the `aggregates` field, which is the field the aggregate scan-dispatch path consults; the `projection` field MUST NOT be read on that path
* *AND* an `EXPLAIN VIRTUAL` of the same query SHALL show `"projection":[]` in the emitted `LAKEHOUSE_SCAN` common spec, so the diagnostic output no longer misreports a full-column projection for an aggregate query
* *AND* the physical Parquet read SHALL remain pruned to the aggregate-referenced columns via DataFusion's own projection pushdown (see `datafusion-scan/scan-execution-partial-agg`), so the empty `projection` field does not widen the scan

### Scenario: Single-group COUNT(DISTINCT) detection fans out a lone distinct and declines multi-distinct or mixed shapes

* *GIVEN* a single-group (no GROUP BY) `pushdown` request whose select list carries one or more `function_aggregate` items
* *WHEN* the adapter classifies the select list to choose a dispatch path
* *THEN* a select list of EXACTLY one `COUNT(DISTINCT col)` or `COUNT(DISTINCT expr)` and no other item (Case 1) SHALL be planned as the dedicated DISTINCT row-scan fan-out counted by an outer native `COUNT(DISTINCT "V")` (see `vs-adapter/pushdown-planning-count-distinct`), using the existing fan-out builder unchanged
* *AND* a select list carrying MORE THAN ONE distinct aggregate, OR a `COUNT(DISTINCT ...)` alongside any ordinary SUM/MIN/MAX/COUNT/AVG aggregate (Case 2/3), SHALL decline the fan-out and route to a qualified single-table wrapper whose OWN rendered SQL computes every aggregate (including every DISTINCT) over a materialized sharded raw scan, so Exasol passes the one-row result through; the wrapper's output SHALL be N columns, one per select-list item
* *AND* the wrapper's inner materialized scan projection SHALL be narrowed to only the columns the request references — including columns nested inside aggregate arguments and CASE branches, plus filter, HAVING, and ORDER BY references — via the shared referenced-column helper the grouped-aggregate fallback also uses (`vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback`; issue #160), NEVER the full base-table schema
* *AND* the adapter MUST NOT return a bare row scan (`sqlCode 04000` column-count mismatch, since Exasol never re-aggregates a declined pushdown) and MUST NOT compose any distinct fan-out as a SELECT-list scalar subquery (`sqlCode 04000`, emitting UDF nested in a scalar subquery)

# Feature: Pushdown Planning, Single-Group Aggregate

The adapter advertises the supported aggregate capabilities, translates an ungrouped aggregate query into a partial-aggregate scan spec, and merges each shard's partial row into the final result.

## Background

* A single-group aggregate splits into a per-shard partial aggregate, computed by the `LAKEHOUSE_SCAN` scalar EMIT UDF, and an outer ungrouped merge aggregation in the wrapper SQL.
* The `GROUP BY shard_key` used for cluster fan-out lives only inside the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery, never at the outer merge level.
* The scan UDF reads the `aggregates` field of the common spec on the aggregate path and derives the DataFusion projection from the partial-aggregate query text, so `projection` is left empty. Only the row-scan path reads `projection`.
* Exasol marks every aggregate without GROUP BY `aggregationType: single_group`, including an aggregate referenced only in HAVING. Such a request never reaches the row-scan projection, because a row scan returns raw rows instead of the aggregated row.
* Exasol does not re-apply a HAVING, because the adapter advertises `AGGREGATE_HAVING`.
* A select item may also be a scalar function wrapping aggregates (`pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate`).
* Exactly one `COUNT(DISTINCT ...)` and no other select-list item (Case 1) becomes a DISTINCT row-scan fan-out (`pushdown-aggregates/pushdown-planning-count-distinct`). More than one distinct aggregate, or a distinct aggregate beside an ordinary aggregate (Case 2/3), routes to a qualified single-table wrapper (`pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback`). The wrapper computes every aggregate over a materialized sharded raw scan.
* Exasol never re-aggregates a declined pushdown, so a bare row scan where `selectListDataTypes` expects aggregate columns fails with `sqlCode 04000`. Exasol also rejects an emitting UDF call nested in a scalar subquery (`sqlCode 04000`).
* Exasol rejects an `OFFSET` in any ungrouped aggregated select (`sqlCode 42000`) and pushes no `orderBy` on an ungrouped aggregate request. The merge SELECT therefore takes no offset and renders no `ORDER BY`.

## Scenarios

### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT`/`COUNT(*)`/`SUM`/`MIN`/`MAX`/`AVG`, `AGGREGATE_GROUP_BY_COLUMN`/`AGGREGATE_GROUP_BY_EXPRESSION`/`AGGREGATE_GROUP_BY_TUPLE`/`AGGREGATE_HAVING`, the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`, single-group `FN_AGG_COUNT_DISTINCT`, and (still) column projection, scalar select-list expressions, filter predicates, and LIMIT
* *AND* the adapter SHALL advertise `AGGREGATE_GROUP_BY_TUPLE` only because the grouped-aggregate detection and scan-driving SQL builder handle an arbitrary number of group keys (see `pushdown-grouped-agg/pushdown-planning-grouped-agg`), so a GROUP BY over two or more keys is pushed down as node-local partial aggregation rather than falling back to a raw row scan that Exasol aggregates itself
* *AND* the adapter SHALL advertise `FN_AGG_COUNT_DISTINCT` because a single-group `COUNT(DISTINCT col)` is decomposed via a dedicated per-shard DISTINCT row-scan fan-out whose locally-distinct values are counted by an outer Exasol-native `COUNT(DISTINCT)` (see `pushdown-aggregates/pushdown-planning-count-distinct`); a `COUNT(DISTINCT ...)` inside a GROUP BY request still falls back to row scanning
* *AND* the adapter SHALL advertise the inner equi-join capabilities `JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI` (see `pushdown-joins/pushdown-planning-join`), while the outer/all-condition join capabilities and any Cartesian-product capability remain absent
* *AND* the capabilities list MUST NOT include `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`, `JOIN_TYPE_LEFT_OUTER`/`JOIN_TYPE_RIGHT_OUTER`/`JOIN_TYPE_FULL_OUTER`, or `JOIN_CONDITION_ALL`

### Scenario: Aggregate query is translated into a partial-aggregate scan spec

* *GIVEN* a virtual schema over an Iceberg table backed by SeaweedFS
* *AND* a query whose select list is one or more supported aggregate functions over the whole table, each either a bare aggregate or a scalar function wrapping aggregates (see `pushdown-aggregates/pushdown-planning-single-group-agg-scalar-over-aggregate`)
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL recognise the request as an aggregate query and resolve the data-file list exactly once
* *AND* the adapter SHALL build a scan spec carrying, for each requested aggregate — including every aggregate nested inside a scalar wrapper — its function kind and target column (the wildcard for `COUNT(*)`), plus any pushed-down filter so the partial aggregate covers filtered rows only
* *AND* the adapter MUST NOT push down an aggregate the scan UDF cannot compute, falling back to the qualified single-table wrapper for that query instead of a bare row scan whenever the select list carries an aggregate at any depth, because Exasol never re-aggregates a declined pushdown

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

* *GIVEN* an ungrouped aggregate `pushdown` request over a table with more than one column (e.g. `SELECT COUNT(*)`, `SELECT SUM(col)`, `SELECT MIN(col), MAX(col)`, or `SELECT ROUND(SUM(col), 2)`)
* *WHEN* the adapter builds the partial-aggregate scan spec
* *THEN* the shard-invariant common spec's `projection` field SHALL be empty, NOT the full base-table column list
* *AND* the referenced-column information SHALL be carried in the `aggregates` field, which is the field the aggregate scan-dispatch path consults; the `projection` field MUST NOT be read on that path
* *AND* an `EXPLAIN VIRTUAL` of the same query SHALL show `"projection":[]` in the emitted `LAKEHOUSE_SCAN` common spec, so the diagnostic output no longer misreports a full-column projection for an aggregate query
* *AND* for a scalar-over-aggregate select item the emitted `"projection"` SHALL likewise be empty and the emitted `"aggregates"` SHALL be non-empty, so the diagnostic output MUST NOT show the aggregate rendered as a projection expression (`"projection":[{"expr":"round(SUM(\"L_QUANTITY\"), 2)"}]`, the `EXPLAIN VIRTUAL` signature of issue #194)
* *AND* the physical Parquet read SHALL remain pruned to the aggregate-referenced columns via DataFusion's own projection pushdown (see `scan-aggregation/scan-execution-partial-agg`), so the empty `projection` field does not widen the scan

### Scenario: Single-group COUNT(DISTINCT) detection fans out a lone distinct and declines multi-distinct or mixed shapes

* *GIVEN* a single-group (no GROUP BY) `pushdown` request whose select list carries one or more `function_aggregate` items
* *WHEN* the adapter classifies the select list to choose a dispatch path
* *THEN* a select list of EXACTLY one `COUNT(DISTINCT col)` or `COUNT(DISTINCT expr)` and no other item (Case 1) SHALL be planned as the dedicated DISTINCT row-scan fan-out counted by an outer native `COUNT(DISTINCT "V")` (see `pushdown-aggregates/pushdown-planning-count-distinct`), using the existing fan-out builder unchanged
* *AND* a select list carrying MORE THAN ONE distinct aggregate, OR a `COUNT(DISTINCT ...)` alongside any ordinary SUM/MIN/MAX/COUNT/AVG aggregate (Case 2/3), SHALL decline the fan-out and route to a qualified single-table wrapper whose OWN rendered SQL computes every aggregate (including every DISTINCT) over a materialized sharded raw scan, so Exasol passes the one-row result through; the wrapper's output SHALL be N columns, one per select-list item
* *AND* the wrapper's inner materialized scan projection SHALL be narrowed to only the columns the request references — including columns nested inside aggregate arguments and CASE branches, plus filter, HAVING, and ORDER BY references — via the shared referenced-column helper the grouped-aggregate fallback also uses (`pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback`; issue #160), NEVER the full base-table schema
* *AND* the adapter MUST NOT return a bare row scan (`sqlCode 04000` column-count mismatch, since Exasol never re-aggregates a declined pushdown) and MUST NOT compose any distinct fan-out as a SELECT-list scalar subquery (`sqlCode 04000`, emitting UDF nested in a scalar subquery)

### Scenario: The single-row aggregate merge renders no OFFSET, because no offset can reach it

* *GIVEN* an ungrouped single-group aggregate query carrying a `LIMIT` and a non-zero `OFFSET` — for example `SELECT COUNT(*) FROM t ORDER BY 1 LIMIT 5 OFFSET 2`
* *WHEN* the statement is submitted to Exasol
* *THEN* Exasol SHALL reject it with `sqlCode 42000` ("OFFSET not allowed in aggregated selects") without issuing a `pushdown` request, so the adapter's single-group aggregate merge SELECT SHALL never receive a non-zero `limit.offset`
* *AND* the merge SELECT builder SHALL therefore take NO offset parameter and render NO `OFFSET`, keeping its existing `LIMIT n` rendering byte-identical to the pre-change output — including the `LIMIT 0` → zero-rows behaviour (issue #198)
* *AND* the adapter MUST NOT add collapse arithmetic, offset rendering, or a `User` decline at this site, because no production request can execute that code and an untested branch here would be a live failure mode on an unreachable path
* *AND* the per-shard scan spec SHALL carry NEITHER the row limit NOR any offset, so no shard's aggregate input is truncated

### Scenario: A single-group request that does not decompose routes to the qualified wrapper

* *GIVEN* a `pushdown` request marked `aggregationType: single_group` whose select list does not decompose, for example a constant beside an aggregate (`SELECT 'X', COUNT(*)`), `SUM` or `VARIANCE` over a text column, `STDDEV` or `VARIANCE` over an expression, or no aggregate outside HAVING (`SELECT 1 ... HAVING COUNT(*) > 0`)
* *WHEN* the adapter plans the request over a non-empty file list
* *THEN* the adapter SHALL route it to the qualified single-table wrapper, whose own rendered SQL computes the select list and HAVING over the materialized sharded raw scan
* *AND* the adapter MUST NOT return a bare row scan
* *AND* the result SHALL equal native Exasol: one row, or no row when HAVING is false

### Scenario: Single-group HAVING is applied on every path

* *GIVEN* an ungrouped aggregate query with a HAVING, for example `SELECT COUNT(*) FROM t HAVING COUNT(*) > 100`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render the HAVING after the outer merge when every aggregate the HAVING references is merged from a select-list aggregate
* *AND* the adapter SHALL otherwise route the request to the qualified single-table wrapper, which renders the HAVING itself
* *AND* a request carrying a `COUNT(DISTINCT ...)` and a HAVING SHALL route to the qualified single-table wrapper
* *AND* the result SHALL equal native Exasol: no row when the HAVING is false over the merged values

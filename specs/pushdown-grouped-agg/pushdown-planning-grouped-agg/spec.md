# Feature: Pushdown Planning, Grouped Aggregate Queries

Extends `pushdown/pushdown-planning` with GROUP BY aggregate detection and scan-driving SQL generation. The adapter renders group-key expressions through the VS expression translator and builds a grouped common scan spec for the scalar scan UDF. DataFusion runs the GROUP BY inside each scalar-scan invocation, and an outer wrapper merges the partials.

## Background

* A grouped aggregate pushdown arrives as `aggregationType: "group_by"` with a non-empty `groupBy` array and a select list of supported aggregate functions. The `groupBy` array can hold any number of elements, emitted as `GK_0..GK_{n-1}`.
* `vs_expression::render_expression` (raising mode) renders each group-key element independently, so keys can mix column references and scalar expressions. If any element fails to render, the adapter falls back to row scanning.
* Cluster fan-out (`GROUP BY shard_key`) lives in the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery. The outer wrapper re-groups the scan's partial rows on the user group keys.
* DataFusion emits one partial-aggregate row per distinct user group per shard. The outer wrapper merges them with the same SUM/MIN/MAX/AVG-pair decomposition as the single-group path.
* The grouped common spec's `projection` field is read only on the row-scan path. On the grouped path the scan UDF reads `group_keys` and `aggregates` and derives the DataFusion projection from the partial-aggregate query, so `projection` stays empty.
* LIMIT and OFFSET are never pushed into a per-shard grouped scan, because a per-shard bound would drop groups before the merge re-groups them. The merge SELECT renders the request's full final window through the shared limit-and-offset seam.
* Exasol accepts an OFFSET only alongside an ORDER BY. A grouped request with an offset always has a merge `ORDER BY` beside it, because an `ORDER BY` that cannot resolve over the merge routes the request to the qualified single-table wrapper (`pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback`). Exasol rejects an OFFSET in an ungrouped aggregated select (`sqlCode 42000`), so only the grouped merge and the wrapper carry one.
* Exasol validates the outer wrapper SELECT's column types positionally against `selectListDataTypes`, so the wrapper lists its items in the user's `selectList` order.
* The outer merge wrapper has only the stringified `GK_*` staging columns and the `PARTIAL_*` columns. A group-key sort key renders as a positional output ordinal that points at the type-cast expression, so it sorts on the native value and not on the `GK_*` VARCHAR. An aggregate sort key renders as that aggregate's merged expression over the `PARTIAL_*` columns, matched by `AggregatePlan` equality like the merged HAVING (`render_having_over_merge`). The merge wrapper is a GROUP BY query, so its `ORDER BY` can reference an aggregate expression directly without adding a hidden output column.
* Related features: `pushdown-grouped-agg/pushdown-planning-grouped-agg-scalar-over-aggregate` (scalar functions wrapping aggregates), `pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback` (requests that cannot use the partial/merge shape), and `storage-access/pushdown-planning-grouped-agg-credential-reference` (credential exposure in the generated SQL).


## Scenarios

### Scenario: Grouped aggregate query is detected and translated to a grouped scan spec

* *GIVEN* a virtual schema over an Iceberg table backed by SeaweedFS
* *AND* a query whose select list contains supported aggregate functions and a non-empty GROUP BY clause
* *WHEN* Exasol sends the corresponding `pushdown` request with `aggregationType: "group_by"`
* *THEN* the adapter SHALL recognise the request as a grouped aggregate query and render each GROUP BY expression node to a DataFusion SQL fragment using the VS expression translator
* *AND* the adapter SHALL build a scan spec carrying both the rendered group-key expressions and the aggregate plans, while retaining for each `selectList` item its original select-list index and its classification as either a group-key projection or an aggregate, so the outer wrapper SELECT can later be assembled in `selectList` order
* *AND* the adapter MUST NOT push down a grouped aggregate if any group-key expression cannot be translated, falling back to row scanning instead

### Scenario: Outer wrapper SELECT preserves user select-list order for interleaved keys and aggregates

* *GIVEN* a grouped aggregate pushdown whose `selectList` places one or more aggregates before, after, or between the group-key projections (e.g. `SELECT SUM(score), MOD(id,4)`, or `SELECT k1, SUM(score), k2`, or `SELECT COUNT(*), MOD(id,4)`)
* *WHEN* the adapter builds the outer wrapper SELECT, its cast list, and its GROUP BY list
* *THEN* the adapter SHALL emit the outer SELECT items in the exact order the corresponding items appear in `selectList`, interleaving group-key cast expressions and merged-aggregate expressions as required
* *AND* the Exasol-declared type applied to each group-key cast SHALL be resolved from the `selectListDataTypes` entry at that key's own select-list index, matched by index rather than by comparing rendered SQL strings
* *AND* the resulting pushdown query SHALL pass Exasol's positional pushdown-column-type check (no "Data type mismatch in column number N" error) for every arrangement of keys and aggregates
* *AND* the merged per-group result SHALL equal the result of the same query with the group keys listed first (which is already correct)

### Scenario: Grouped scan spec carries group-key rendered SQL fragments

* *GIVEN* a grouped aggregate pushdown request whose GROUP BY clause contains a mix of column references and scalar expressions (e.g., `YEAR(ts_col)`)
* *WHEN* the adapter builds the scan spec
* *THEN* the scan spec SHALL carry a `group_keys` field containing the rendered DataFusion SQL fragment for each group-key expression in order
* *AND* each group-key expression MUST be renderable by the VS expression translator in raising mode
* *AND* the scan UDF MUST use the same rendered expressions in its per-shard DataFusion GROUP BY clause
* *AND* the adapter SHALL resolve each group-key expression's Exasol-declared result type from the `selectListDataTypes` entry at the group-key item's own `selectList` index, so an expression key whose rendered SQL differs in whitespace or casing between `groupBy` and `selectList` still receives its correct declared type and CAST rather than silently defaulting to `VARCHAR(2000000)`

### Scenario: Grouped scan-driving SQL fans out via a nested shard_key distributor over G work units

* *GIVEN* a grouped aggregate pushdown over a file list partitioned into G work-unit shards
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL place the `GROUP BY shard_key` (one group per shard) INSIDE the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery, NOT at the outer merge level and NOT on `IPROC()`
* *AND* G SHALL be `node_count × parallelism_factor` capped at 300 and clamped to the file count, so the shard groups distribute round-robin across nodes and multiplex onto each node's core pool
* *AND* the `LAKEHOUSE_SCAN` SCALAR EMIT UDF SHALL be invoked over each distributed shard row with the shard-invariant grouped common spec spliced once as its first-argument literal and that shard's file subset as its second argument

### Scenario: LIMIT is NOT pushed into per-shard scan for a grouped query

* *GIVEN* a grouped aggregate query with a LIMIT clause
* *WHEN* the adapter builds the grouped scan spec
* *THEN* the shard-invariant common spec MUST NOT carry the LIMIT value, so no per-shard partial scan observes a LIMIT
* *AND* because the common spec is shared by every shard, the LIMIT-exclusion invariant SHALL hold for every shard by construction (the LIMIT is stripped from the single common spec, not per shard)
* *AND* the LIMIT SHALL appear only in the outer wrapper SQL that merges partial-aggregate results from all shards

### Scenario: NULL group keys are grouped together consistently

* *GIVEN* a table with rows where the GROUP BY column contains NULL values
* *WHEN* the grouped aggregate scan runs across one or more shards
* *THEN* all rows with a NULL value in the GROUP BY column SHALL be aggregated into a single group
* *AND* this behavior MUST match standard SQL GROUP BY NULL semantics (NULLs are equal for grouping purposes in both DataFusion and Exasol)

### Scenario: Grouped aggregate wrapper SQL re-groups partial results per user group key

* *GIVEN* a grouped aggregate pushdown fanned out over G shards via the nested `shard_key` distributor
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the inner distributor's `shard_key` grouping SHALL parallelize the scan across nodes while the scalar scan UDF performs the user GROUP BY inside each shard invocation, emitting one partial-aggregate row per distinct user group per shard
* *AND* the outer wrapper SQL SHALL GROUP BY the user group-key columns over the scalar scan select and merge the per-shard partials using the same SUM/MIN/MAX/AVG-pair decomposition as the single-group path, with no `SELECT * FROM (...)` wrapper between the merge and the scalar scan
* *AND* the outer wrapper SELECT list SHALL place each group-key cast expression and each merged-aggregate expression at the same ordinal position that item occupied in the user's `selectListDataTypes`, so the wrapper's result column order and per-column type match Exasol's positional pushdown validation for ANY interleaving of keys and aggregates, while the inner scalar scan's per-shard EMITS clause MAY remain keys-first (GK_* then PARTIAL_*) because it is matched only against the scan UDF's own output
* *AND* the merged result per group SHALL equal the result of the same grouped aggregate evaluated over all rows on a single node

### Scenario: Grouped aggregate scan spec leaves the projection field empty

* *GIVEN* a grouped aggregate `pushdown` request (`aggregationType: "group_by"`) over a table with more than one column (e.g. `SELECT a, COUNT(*) FROM t GROUP BY a`) that is decomposed into a partial/merge grouped aggregate, NOT the undecomposable single-table fallback that dispatches as a raw scan and legitimately carries a non-empty `projection`
* *WHEN* the adapter builds the grouped partial-aggregate scan spec
* *THEN* the shard-invariant grouped common spec's `projection` field SHALL be empty, NOT the full base-table column list
* *AND* the referenced-column information SHALL be carried in the `group_keys` and `aggregates` fields, which are the fields the grouped scan-dispatch path consults; the `projection` field MUST NOT be read on that path
* *AND* an `EXPLAIN VIRTUAL` of the same query SHALL show `"projection":[]` in the emitted `LAKEHOUSE_SCAN` grouped common spec
* *AND* the physical Parquet read SHALL remain pruned to the group-key and aggregate-referenced columns via DataFusion's own projection pushdown (see `scan-aggregation/scan-execution-grouped-agg`), so the empty `projection` field does not widen the scan

### Scenario: A grouped ORDER BY over a select-list aggregate resolves to that aggregate's merged partial expression

* *GIVEN* a grouped `pushdown` request that decomposes into the partial/merge grouped shape, whose `orderBy` sorts on an aggregate expression matching one of the detected select-list aggregate plans (`SELECT c_bool, SUM(c_price) FROM t GROUP BY c_bool ORDER BY SUM(c_price) DESC`)
* *WHEN* the adapter builds the outer merge SQL
* *THEN* the merge `ORDER BY` element SHALL be that aggregate's merged expression over the `PARTIAL_*` columns (for example `SUM("PARTIAL_sum_1")`), produced by the SAME merge rewriter and the SAME `AggregatePlan` equality match (kind plus source column) the merged HAVING uses
* *AND* the element SHALL render its direction and NULL placement through the one shared direction/NULL seam every `ORDER BY` the adapter emits routes through, so the grouped merge cannot drift from the other ordered paths
* *AND* the outer wrapper's visible SELECT list, its cast list, and its GROUP BY list SHALL be UNCHANGED, the merge `ORDER BY` references an aggregate expression, not an output column, so no hidden output column is added and Exasol's positional `selectListDataTypes` validation is unaffected
* *AND* a group-key sort key SHALL still render as its positional output ordinal, unchanged by this delta, including in an `orderBy` that mixes a group-key key with an aggregate key
* *AND* the per-shard partial scan SHALL still carry no `LIMIT` and no sort keys, so the anti-wrong-truncation invariant holds unchanged
* *AND* the merged result SHALL equal the same grouped query with the same `ORDER BY` evaluated over all rows on a single node

### Scenario: The grouped merge wrapper renders the request's OFFSET alongside its LIMIT

* *GIVEN* a `GROUP BY` aggregate `pushdown` request that decomposes into the partial/merge shape, carrying a merge-resolvable `orderBy`, a `limit` with `numElements` = `n`, and a non-zero `limit.offset` = `m`, for example `SELECT MOD(id,4) AS k, COUNT(*) FROM t GROUP BY MOD(id,4) ORDER BY k LIMIT 2 OFFSET 1`
* *WHEN* the adapter builds the grouped scan-driving SQL
* *THEN* the outer merge SELECT SHALL render `GROUP BY <keys> [HAVING …] ORDER BY <keys> LIMIT n OFFSET m`, in that clause order, with the offset rendered through the SAME shared limit-and-offset seam every other wrapper uses
* *AND* the per-shard grouped scan spec SHALL carry NEITHER the row limit NOR any offset, so no shard drops a group before the merge re-groups the partials
* *AND* a request whose `limit.offset` is zero or absent SHALL render no OFFSET clause
* *AND* the returned groups SHALL equal the same `GROUP BY … ORDER BY … LIMIT n OFFSET m` evaluated over all rows on a single node, for the seeded 20-row `events` fixture the example query SHALL return the groups ranked 2-3, NOT the groups ranked 1-2

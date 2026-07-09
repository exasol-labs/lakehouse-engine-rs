# Feature: Pushdown Planning — Grouped Aggregate Queries

Extends `vs-adapter/pushdown-planning` with the GROUP BY aggregate detection and
scan-driving SQL generation scenarios. When Exasol delegates a `GROUP BY` aggregate
query, the adapter detects the shape, renders group-key expressions via the VS
expression translator, builds a grouped common scan spec, and generates fan-out SQL
that runs DataFusion GROUP BY inside each shard invocation and merges the partials in
an outer wrapper. The grouped common spec is serialized once (shared by all shards)
and carries no LIMIT. See `vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate`
for scalar-function-wrapping-aggregates select items on this same path.

## Background

* A grouped aggregate pushdown arrives as `aggregationType: "group_by"` with a
  non-empty `groupBy` array and a select list of supported aggregate functions.
* The `groupBy` array MAY contain one, two, or more elements; the detection and
  scan-driving SQL builder treat the group-key list as arbitrary-length (emitting
  `GK_0..GK_{n-1}`) and impose no cap of one key.
* Group-key expressions are rendered by `vs_expression::render_expression` (raising
  mode); any failure on ANY element causes the adapter to fall back to row scanning.
* Each group-key element is rendered independently, so a multi-key GROUP BY MAY mix
  plain column references and scalar expressions in any combination.
* The inner `GROUP BY shard_key` parallelizes the scan; DataFusion performs the user
  GROUP BY inside each shard invocation, emitting per-user-group partials with
  group-key values as plain columns (GK_0..GK_{n-1}); the outer wrapper re-groups on
  those columns and merges the partials.
* LIMIT is never pushed into the per-shard grouped scan; the shared common spec is
  built with no LIMIT, so no shard observes one — it appears only in the outer
  wrapper.
* Exasol validates the outer wrapper SELECT's column types positionally against
  `selectListDataTypes`, so the wrapper SELECT must list its items in the user's
  `selectList` order.
* The grouped scan-driving SQL serializes the shard-invariant common spec once and
  carries only each shard's file subset per `VALUES` row, exactly as the row-scan
  fan-out.
* When a grouped select item cannot be decomposed into supported partials (an inner
  aggregate that is `DISTINCT`, a SUM/stat over a non-numeric type, an untranslatable
  argument, or a non-aggregate/non-group-key node), the adapter MUST NOT emit a bare
  raw full-row scan (whose column count does not match the aggregated query Exasol
  expects, causing SQL state `04000` "Expected number of columns is N but pushdown
  query has M"). It falls back to a qualified single-table wrapper that renders the
  exact grouped select list over a materialized sharded raw scan, analogous to the
  unified join fallback.

## Scenarios

### Scenario: Grouped aggregate query is detected and translated to a grouped scan spec

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
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

### Scenario: Grouped scan-driving SQL fans out via GROUP BY shard_key over G work units

* *GIVEN* a grouped aggregate pushdown over a file list partitioned into G work-unit shards
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL group the per-shard rows on `shard_key` (one group per shard), NOT on `IPROC()`
* *AND* G SHALL be `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300 and clamped to the file count, so the shard groups distribute round-robin across nodes and multiplex onto each node's core pool
* *AND* the scan SET UDF SHALL be invoked once per shard with the shard-invariant common spec serialized once as its first argument and that shard's file subset as its second argument

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

* *GIVEN* a grouped aggregate pushdown fanned out over G shards via `GROUP BY shard_key`
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the inner `shard_key` grouping SHALL parallelize the scan while DataFusion performs the user GROUP BY inside each shard invocation, emitting one partial-aggregate row per distinct user group per shard
* *AND* the outer wrapper SQL SHALL GROUP BY the user group-key columns and merge the per-shard partials using the same SUM/MIN/MAX/AVG-pair decomposition as the single-group path
* *AND* the outer wrapper SELECT list SHALL place each group-key cast expression and each merged-aggregate expression at the same ordinal position that item occupied in the user's `selectList`, so the wrapper's result column order and per-column type match Exasol's positional `selectListDataTypes` validation for ANY interleaving of keys and aggregates, while the inner fan-out EMITS clause and the scan UDF's per-shard SELECT MAY remain keys-first (GK_* then PARTIAL_*) because they are matched only against each other
* *AND* the merged result per group SHALL equal the result of the same grouped aggregate evaluated over all rows on a single node

### Scenario: Adapter falls back to a qualified single-table wrapper for an undecomposable grouped aggregate shape

* *GIVEN* a grouped `pushdown` request (`aggregationType: "group_by"`) whose select list contains an item the adapter cannot decompose into supported partials — an inner aggregate that is `DISTINCT`, a SUM/stat aggregate over a non-numeric type, an untranslatable aggregate argument, or a non-aggregate/non-group-key node
* *WHEN* the adapter processes the request
* *THEN* the adapter MUST NOT emit a bare raw full-row `ScanSpec` for a grouped request (that would return a column count differing from the request's `selectList`, causing a client-facing `04000` "Expected number of columns is N but pushdown query has M")
* *AND* the adapter SHALL instead render the exact grouped select list, GROUP BY, HAVING, ORDER BY, and LIMIT as ordinary Exasol SQL over a materialized single-table sharded raw scan — a qualified single-table wrapper analogous to the unified join fallback (`SELECT <grouped select list> FROM (<sharded raw fan-out>) GROUP BY ... HAVING ... ORDER BY ... LIMIT ...`) — so Exasol's core engine computes the aggregate over the returned rows
* *AND* the scalar-over-aggregate select items in that wrapper SHALL be rendered by the `crates/vs-expression` translator (aggregate names spliced verbatim, arguments recursed), since Exasol computes the aggregation over materialized rows rather than over merged partials
* *AND* the wrapper's result column count and per-column types SHALL match Exasol's positional `selectListDataTypes` validation
* *AND* the returned result SHALL equal the result of the same grouped query evaluated on a single node

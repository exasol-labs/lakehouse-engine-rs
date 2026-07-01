# Feature: Pushdown Planning — Grouped Aggregate Queries

Extends `vs-adapter/pushdown-planning` with the GROUP BY aggregate detection and
scan-driving SQL generation scenarios. When Exasol delegates a `GROUP BY` aggregate
query, the adapter detects the shape, renders group-key expressions via the VS
expression translator, builds a grouped scan spec, and generates fan-out SQL that
runs DataFusion GROUP BY inside each shard invocation and merges the partials in an
outer wrapper.

## Background

* A grouped aggregate pushdown arrives as `aggregationType: "group_by"` with a
  non-empty `groupBy` array and a select list of supported aggregate functions.
* Group-key expressions are rendered by `vs_expression::render_expression` (raising
  mode); any failure causes the adapter to fall back to row scanning.
* The inner `GROUP BY shard_key` parallelizes the scan; DataFusion performs the user
  GROUP BY inside each shard invocation, emitting per-user-group partials with
  group-key values as plain columns (GK_0..GK_{n-1}); the outer wrapper re-groups on
  those columns and merges the partials.
* LIMIT is never pushed into the per-shard grouped scan; it appears only in the outer
  wrapper.
* Exasol validates the outer wrapper SELECT's column types positionally against
  `selectListDataTypes`, so the wrapper SELECT must list its items in the user's
  `selectList` order.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Grouped aggregate query is detected and translated to a grouped scan spec

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a query whose select list contains supported aggregate functions and a non-empty GROUP BY clause
* *WHEN* Exasol sends the corresponding `pushdown` request with `aggregationType: "group_by"`
* *THEN* the adapter SHALL recognise the request as a grouped aggregate query and render each GROUP BY expression node to a DataFusion SQL fragment using the VS expression translator
* *AND* the adapter SHALL build a scan spec carrying both the rendered group-key expressions and the aggregate plans, while retaining for each `selectList` item its original select-list index and its classification as either a group-key projection or an aggregate, so the outer wrapper SELECT can later be assembled in `selectList` order
* *AND* the adapter MUST NOT push down a grouped aggregate if any group-key expression cannot be translated, falling back to row scanning instead
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Grouped aggregate wrapper SQL re-groups partial results per user group key

* *GIVEN* a grouped aggregate pushdown fanned out over G shards via `GROUP BY shard_key`
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the inner `shard_key` grouping SHALL parallelize the scan while DataFusion performs the user GROUP BY inside each shard invocation, emitting one partial-aggregate row per distinct user group per shard
* *AND* the outer wrapper SQL SHALL GROUP BY the user group-key columns and merge the per-shard partials using the same SUM/MIN/MAX/AVG-pair decomposition as the single-group path
* *AND* the outer wrapper SELECT list SHALL place each group-key cast expression and each merged-aggregate expression at the same ordinal position that item occupied in the user's `selectList`, so the wrapper's result column order and per-column type match Exasol's positional `selectListDataTypes` validation for ANY interleaving of keys and aggregates, while the inner fan-out EMITS clause and the scan UDF's per-shard SELECT MAY remain keys-first (GK_* then PARTIAL_*) because they are matched only against each other
* *AND* the merged result per group SHALL equal the result of the same grouped aggregate evaluated over all rows on a single node
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Outer wrapper SELECT preserves user select-list order for interleaved keys and aggregates

* *GIVEN* a grouped aggregate pushdown whose `selectList` places one or more aggregates before, after, or between the group-key projections (e.g. `SELECT SUM(score), MOD(id,4)`, or `SELECT k1, SUM(score), k2`, or `SELECT COUNT(*), MOD(id,4)`)
* *WHEN* the adapter builds the outer wrapper SELECT, its cast list, and its GROUP BY list
* *THEN* the adapter SHALL emit the outer SELECT items in the exact order the corresponding items appear in `selectList`, interleaving group-key cast expressions and merged-aggregate expressions as required
* *AND* the Exasol-declared type applied to each group-key cast SHALL be resolved from the `selectListDataTypes` entry at that key's own select-list index, matched by index rather than by comparing rendered SQL strings
* *AND* the resulting pushdown query SHALL pass Exasol's positional pushdown-column-type check (no "Data type mismatch in column number N" error) for every arrangement of keys and aggregates
* *AND* the merged per-group result SHALL equal the result of the same query with the group keys listed first (which is already correct)
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Grouped scan spec carries group-key rendered SQL fragments

* *GIVEN* a grouped aggregate pushdown request whose GROUP BY clause contains a mix of column references and scalar expressions (e.g., `YEAR(ts_col)`)
* *WHEN* the adapter builds the scan spec
* *THEN* the scan spec SHALL carry a `group_keys` field containing the rendered DataFusion SQL fragment for each group-key expression in order
* *AND* each group-key expression MUST be renderable by the VS expression translator in raising mode
* *AND* the scan UDF MUST use the same rendered expressions in its per-shard DataFusion GROUP BY clause
* *AND* the adapter SHALL resolve each group-key expression's Exasol-declared result type from the `selectListDataTypes` entry at the group-key item's own `selectList` index, so an expression key whose rendered SQL differs in whitespace or casing between `groupBy` and `selectList` still receives its correct declared type and CAST rather than silently defaulting to `VARCHAR(2000000)`
<!-- /DELTA:CHANGED -->

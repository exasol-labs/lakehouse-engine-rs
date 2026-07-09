# Feature: Pushdown Planning — Grouped Aggregate Queries

<!--
DELTA against specs/vs-adapter/pushdown-planning-grouped-agg/spec.md
Only changed/new scenarios and the changed Background bullet are shown with markers.
All other scenarios and Background bullets of the permanent spec are unchanged.
-->

Extends the single-table grouped aggregate pushdown path so a `selectList` item that is
a scalar function wrapping one or more aggregates (e.g. `ROUND(… SUM(…) / COUNT(*) …)`)
is decomposed into the existing shard-associative partial/merge plan — its inner
aggregates become partial columns and the scalar wrapper is rendered over the merged
partials in the outer wrapper — instead of declining to a bare row scan that fails with
a `04000` pushdown column-count mismatch. When a grouped select item is genuinely
undecomposable, the adapter falls back to a qualified single-table wrapper that returns
exactly the expected columns rather than a column-count-mismatched raw scan.

## Background

<!-- DELTA:CHANGED -->
* A grouped select item MAY be a `function_scalar` (or arithmetic) expression that
  wraps one or more `function_aggregate` nodes — e.g.
  `ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG = 'R' THEN 1 ELSE 0 END) / COUNT(*), 2)`.
  The adapter decomposes each such item's *inner* aggregates into the same
  shard-associative partial columns as a top-level aggregate, and renders the scalar
  wrapper over the *merged* partials in the outer wrapper SELECT (never per shard).
  The scalar wrapper itself is not decomposable — only its inner aggregates are.
* The outer wrapper renders a nested `function_aggregate` by rewriting it to its
  merged expression (e.g. `SUM(x)` → `SUM("PARTIAL_sum_0")`, `COUNT(*)` →
  `SUM("PARTIAL_count_0")`, `AVG(x)` → the merged SUM/COUNT pair), matched to the
  decomposed `AggregatePlan` list, exactly as a `function_aggregate` in a HAVING is
  rewritten. The scalar/arithmetic structure around the aggregates is preserved; only
  the aggregate leaves are rewritten — the outer wrapper has only `GK_*` and
  `PARTIAL_*` columns available, never a source column.
* When a grouped select item cannot be decomposed into supported partials (an inner
  aggregate that is `DISTINCT`, a SUM/stat over a non-numeric type, an untranslatable
  argument, or a non-aggregate/non-group-key node), the adapter MUST NOT emit a bare
  raw full-row scan (whose column count does not match the aggregated query Exasol
  expects, causing SQL state `04000` "Expected number of columns is N but pushdown
  query has M"). It falls back to a qualified single-table wrapper that renders the
  exact grouped select list over a materialized sharded raw scan, analogous to the
  unified join fallback.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Single-table grouped select item that is a scalar function wrapping aggregates is pushed down

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a single-table grouped `pushdown` request (`aggregationType: "group_by"`, non-empty `groupBy`) whose select list contains a select item that is a scalar function wrapping one or more aggregates — e.g. `ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG = 'R' THEN 1 ELSE 0 END) / COUNT(*), 2)` — alongside a group key `L_RETURNFLAG` and plain aggregates `SUM(L_QUANTITY)` and `AVG(L_EXTENDEDPRICE)`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT decline the request and SHALL NOT return an error for the scalar-over-aggregate select item
* *AND* the adapter SHALL decompose every `function_aggregate` nested inside that select item into the same partial `AggregatePlan` list it builds for a top-level aggregate, so each inner aggregate contributes a `PARTIAL_*` column the scan UDF emits per group per shard
* *AND* the adapter SHALL render the outer wrapper SELECT item as the scalar function applied to the *merged* form of each inner aggregate, at that item's original `selectList` ordinal
* *AND* the emitted pushdown query SHALL return exactly the number of columns in the request's `selectList` (no `04000` column-count mismatch)
* *AND* the merged per-group result SHALL equal the result of the same grouped query evaluated over all rows on a single node

### Scenario: Nested aggregates are rewritten to their merged partial expressions, never rendered over source columns

* *GIVEN* a grouped pushdown whose select list contains a scalar-over-aggregate item
* *WHEN* the adapter builds the outer wrapper SELECT for that item
* *THEN* every `function_aggregate` node reachable inside the scalar/arithmetic wrapper SHALL be rewritten to its merged expression over `PARTIAL_*` columns, matched to the decomposed `AggregatePlan` list by aggregate kind and argument
* *AND* the rendered wrapper item MUST NOT reference any source column of the base table (only `GK_*` group-key columns and `PARTIAL_*` partial columns exist in the outer wrapper)
* *AND* a top-level bare aggregate and a nested aggregate SHALL be rewritten by the same merge-rewriting path, so the two produce consistent merged SQL
* *AND* the adapter SHALL wrap the rendered item in `CAST(... AS <declared type>)` using the `selectListDataTypes` entry at that item's own ordinal, so the outer wrapper passes Exasol's positional pushdown-column-type validation

### Scenario: Inner aggregates shared across the grouped select list decompose into deduplicated partial columns

* *GIVEN* a grouped pushdown whose select list references the same inner aggregate more than once — e.g. `COUNT(*)` appears both as a bare select item and inside a `ROUND(... / COUNT(*) ...)` scalar-over-aggregate item
* *WHEN* the adapter decomposes the select list into partial `AggregatePlan`s
* *THEN* the adapter SHALL collapse aggregates that are equal by kind and argument into a single `PARTIAL_*` column rather than emitting a duplicate partial column per occurrence
* *AND* both the bare occurrence and the nested occurrence SHALL render to the same merged expression over that one shared partial column
* *AND* the merged result SHALL equal the same select list evaluated over all rows on a single node

### Scenario: Scalar-over-aggregate items interleaved with keys and plain aggregates preserve select-list order

* *GIVEN* a grouped pushdown whose select list places a scalar-over-aggregate item before, after, or between group keys and plain aggregates
* *WHEN* the adapter builds the outer wrapper SELECT, its cast list, and its GROUP BY list
* *THEN* the adapter SHALL emit the outer SELECT items in the exact order the corresponding items appear in `selectList`, interleaving group-key cast expressions, merged plain-aggregate expressions, and merged scalar-over-aggregate expressions as required
* *AND* the Exasol-declared type applied to each item's CAST SHALL be resolved from the `selectListDataTypes` entry at that item's own `selectList` ordinal, matched by index rather than by comparing rendered SQL strings
* *AND* the resulting pushdown query SHALL pass Exasol's positional pushdown-column-type check for every arrangement of keys, plain aggregates, and scalar-over-aggregate items
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter falls back to a qualified single-table wrapper for an undecomposable grouped aggregate shape

* *GIVEN* a grouped `pushdown` request (`aggregationType: "group_by"`) whose select list contains an item the adapter cannot decompose into supported partials — an inner aggregate that is `DISTINCT`, a SUM/stat aggregate over a non-numeric type, an untranslatable aggregate argument, or a non-aggregate/non-group-key node
* *WHEN* the adapter processes the request
* *THEN* the adapter MUST NOT emit a bare raw full-row `ScanSpec` for a grouped request (that would return a column count differing from the request's `selectList`, causing a client-facing `04000` "Expected number of columns is N but pushdown query has M")
* *AND* the adapter SHALL instead render the exact grouped select list, GROUP BY, HAVING, ORDER BY, and LIMIT as ordinary Exasol SQL over a materialized single-table sharded raw scan — a qualified single-table wrapper analogous to the unified join fallback (`SELECT <grouped select list> FROM (<sharded raw fan-out>) GROUP BY ... HAVING ... ORDER BY ... LIMIT ...`) — so Exasol's core engine computes the aggregate over the returned rows
* *AND* the scalar-over-aggregate select items in that wrapper SHALL be rendered by the `crates/vs-expression` translator (aggregate names spliced verbatim, arguments recursed), since Exasol computes the aggregation over materialized rows rather than over merged partials
* *AND* the wrapper's result column count and per-column types SHALL match Exasol's positional `selectListDataTypes` validation
* *AND* the returned result SHALL equal the result of the same grouped query evaluated on a single node
<!-- /DELTA:CHANGED -->

# Feature: Pushdown Planning — Grouped Aggregate Wrapper Fallback

Extends `vs-adapter/pushdown-planning-grouped-agg` with what happens when a grouped
request cannot be decomposed into the partial/merge shape at all — an undecomposable
select-list item, a HAVING that references an aggregate absent from the select list, or
an ORDER BY that resolves to neither a group key nor a select-list aggregate. The
adapter falls back to a qualified single-table wrapper that renders the grouped select
list, GROUP BY, HAVING, ORDER BY, and LIMIT as ordinary Exasol SQL over a materialized
sharded raw scan. The fallback is never an error: the wrapper preserves the HAVING
natively, so the adapter keeps the `AGGREGATE_HAVING` contract it advertised (issue
#195), and it renders an otherwise-unresolvable grouped `ORDER BY` natively too (issue
#198). The inner sharded raw scan MUST project only the columns the request references
(group keys, select-list aggregate arguments, filter, and any HAVING/ORDER BY columns),
not the full base-table schema (issue #160); the narrowing is computed by a single
shared referenced-column helper reused by the single-group `COUNT(DISTINCT)` Case 2/3
qualified-wrapper decline (`vs-adapter/pushdown-planning-count-distinct`), so both
decline paths narrow identically.

## Background

* When a grouped select item cannot be decomposed into supported partials (an inner
  aggregate that is `DISTINCT`, a SUM/stat over a non-numeric type, an untranslatable
  argument, or a non-aggregate/non-group-key node), the adapter MUST NOT emit a bare
  raw full-row scan (whose column count does not match the aggregated query Exasol
  expects, causing SQL state `04000` "Expected number of columns is N but pushdown
  query has M"). It falls back to a qualified single-table wrapper that renders the
  exact grouped select list over a materialized sharded raw scan, analogous to the
  unified join fallback.
* Before this feature's original fallback the inner scan projected the ENTIRE
  base-table schema (`full_row_projection`), streaming every column of every matching
  row regardless of what the query references (issue #160). It now projects only the
  referenced columns.
* The referenced-column set is group keys + SELECT-list aggregate arguments + filter
  columns + HAVING and ORDER BY references; it MUST expose every column the outer
  wrapper renders. When the request references no source column the projection falls
  back to at least one column, since an empty EMITS clause is invalid in Exasol.
* The narrowing is a single shared helper reused by the single-group `COUNT(DISTINCT)`
  Case 2/3 qualified-wrapper decline, so both decline paths narrow through one
  mechanism, not two.
* This wrapper's HAVING trigger is a HAVING referencing an aggregate absent from the
  select list. Before that trigger existed, that shape returned `UdfError::User` from
  the dispatcher, surfacing to the client as SQL state `22002` / `F-UDF-CL-RUST-9001` at
  `EXPLAIN VIRTUAL` time as well as at execution time (issue #195).
* `render_having_over_merge` rewrites each `function_aggregate` reference in the HAVING
  to its merged `PARTIAL_*` expression, matched to the detected select-list aggregate
  plans by `AggregatePlan` equality (kind plus source column). An aggregate with no
  matching plan cannot be rewritten, because the outer merge wrapper's only columns are
  `GK_*` and `PARTIAL_*` — the source column the aggregate names is not available there.
* An AND/OR junction node collapses when ANY child fails to render, so a HAVING mixing
  one matched aggregate with one unmatched aggregate fails as a whole, not partially.
* Whether the HAVING can be merged therefore decides the request SHAPE, not merely the
  SQL text: an unmergeable HAVING makes the partial/merge decomposition unavailable.
  The shape decision is owned by the one shared classifier
  (`vs-adapter/pushdown-module-structure`), so the non-empty dispatch path and the
  fully-pruned zero-row path agree by construction.
* The qualified single-table wrapper renders the HAVING through the `crates/vs-expression`
  translator with aggregate names spliced verbatim over the materialized `LHS_T0` rows, so
  Exasol's own engine evaluates it. The HAVING is preserved, never dropped — which is why
  routing to the wrapper does not breach the advertised `AGGREGATE_HAVING` capability.
* The referenced-column helper that narrows the wrapper's inner scan already walks the
  `having` node's full expression tree, so an aggregate argument reachable only from the
  HAVING is projected without further change.
* NO grouped decline is a hard error. Both — a select-list aggregate that fails the
  numeric-type gate, and a HAVING the adapter cannot render over the merge — fall through
  to the qualified single-table wrapper, whether or not a HAVING is present. The wrapper
  renders the HAVING itself, so nothing is dropped.
* Routing a non-numeric aggregate to the wrapper hands Exasol a native aggregate over a VARCHAR.
  That is Exasol's own expression to evaluate: where its implicit VARCHAR-to-numeric conversion
  succeeds the query returns a correct result (a class of query that previously hard-errored), and
  where it does not Exasol raises `22018` "invalid character value for cast" naming the offending
  value — the same outcome the user's HAVING produces against a native table.
* The grouped merge `ORDER BY` resolution moves INTO the one shared request-shape classifier,
  for the same reason the HAVING merge-rendering moved there (issue #195): whether the
  ordering can be expressed over the merge decides the request SHAPE, not merely the SQL text
  within an already-chosen shape. That is what keeps the non-empty dispatch path and the
  fully-pruned zero-row path in agreement by construction.
* An aggregate sort key ABSENT from the detected select-list plans has no `PARTIAL_*` column
  to merge over. The adapter does NOT invent one: Exasol declares a result type only for
  `selectList` items, so a partial column for an aggregate Exasol never declared would need a
  fabricated Exasol type, risking overflow on SUM and precision-driven misordering. Such a
  request routes to the qualified single-table wrapper instead, which renders the `ORDER BY`
  natively over materialized rows with no fabricated type.
* The shared referenced-column helper that narrows the wrapper's inner scan already walks the
  `orderBy` node's FULL expression tree, so every column an expression or aggregate sort key
  names is projected with no further change.
* Iceberg spec compliance: checked, not engaged. Verified against the Apache Iceberg table
  spec (https://iceberg.apache.org/spec/): the normative sections that could bear on a
  pushdown change are those governing schema/field-id resolution ("Schemas and Data Types",
  "Column Projection") and scan planning ("Scan Planning", manifest and partition filtering).
  This feature alters only which Exasol-side SQL shape an undecomposable grouped request
  routes to; it reads no manifest, resolves no snapshot or field id, applies no delete, and
  maps no type. No normative requirement applies, so there is no deviation to fix and none
  to track.
* Credentials MUST NOT appear in any returned SQL or error message.

## Scenarios

### Scenario: Adapter falls back to a qualified single-table wrapper for an undecomposable grouped aggregate shape

* *GIVEN* a grouped `pushdown` request (`aggregationType: "group_by"`) the adapter cannot decompose into supported partials — a select-list item that is a `DISTINCT` inner aggregate, a SUM/stat aggregate over a non-numeric type (whether or not a HAVING is present), an untranslatable aggregate argument, or a non-aggregate/non-group-key node — or a `having` the adapter cannot render over the merge (issue #195)
* *WHEN* the adapter processes the request
* *THEN* the adapter MUST NOT emit a bare raw full-row `ScanSpec` for a grouped request (that would return a column count differing from the request's `selectList`, causing a client-facing `04000` "Expected number of columns is N but pushdown query has M")
* *AND* the adapter SHALL instead render the exact grouped select list, GROUP BY, HAVING, ORDER BY, and LIMIT as ordinary Exasol SQL over a materialized single-table sharded raw scan — a qualified single-table wrapper analogous to the unified join fallback (`SELECT <grouped select list> FROM (<sharded raw fan-out>) GROUP BY ... HAVING ... ORDER BY ... LIMIT ...`) — so Exasol's core engine computes the aggregate over the returned rows
* *AND* the inner sharded raw scan's projection SHALL be narrowed to only the columns the request references — group keys, select-list aggregate arguments, filter columns, and HAVING and ORDER BY columns — NEVER the full base-table schema (issue #160), so the fallback scan prunes I/O and network transfer to the referenced-column set while still exposing every column the outer wrapper renders
* *AND* the referenced-column set SHALL be computed by a single shared helper reused by the single-group `COUNT(DISTINCT)` Case 2/3 qualified-wrapper decline (`vs-adapter/pushdown-planning-count-distinct`), so both decline paths narrow identically; when the request references no source column the projection SHALL fall back to at least one column, since an empty EMITS clause is invalid in Exasol
* *AND* the scalar-over-aggregate select items in that wrapper SHALL be rendered by the `crates/vs-expression` translator (aggregate names spliced verbatim, arguments recursed), since Exasol computes the aggregation over materialized rows rather than over merged partials
* *AND* the wrapper's result column count and per-column types SHALL match Exasol's positional `selectListDataTypes` validation
* *AND* the returned result SHALL equal the result of the same grouped query evaluated on a single node

### Scenario: A HAVING the adapter cannot render over the merge falls back instead of erroring

* *GIVEN* a grouped `pushdown` request carrying a `having` the adapter cannot rewrite over the `PARTIAL_*` merge columns, for any of its three reasons — an aggregate absent from the detected select-list plans (`SELECT MOD(id,4), COUNT(*) FROM t GROUP BY MOD(id,4) HAVING SUM(score) > 250.0`); such an aggregate as one operand of an AND/OR junction whose sibling operands DO match (`... HAVING COUNT(*) > 0 AND SUM(score) > 250.0`), which collapses the junction as a whole; or a `DISTINCT` aggregate, which has no partial/merge decomposition at all (`... HAVING COUNT(DISTINCT name) > 4`)
* *AND* a grouped request whose select-list aggregate column type fails the numeric gate WHILE a `having` is present, which is the same decline reached by a different route
* *WHEN* the adapter classifies the request shape
* *THEN* the adapter SHALL attempt the HAVING merge-rendering while classifying the shape and, when the HAVING does not render or the numeric gate fails, SHALL classify the request as the qualified single-table wrapper shape rather than the partial/merge grouped shape
* *AND* the adapter MUST NOT return an error for this shape at either `EXPLAIN VIRTUAL` plan time or execution time, because the wrapper renders the HAVING as ordinary Exasol SQL over materialized rows and therefore never drops a HAVING the adapter advertised `AGGREGATE_HAVING` for (issue #195)
* *AND* the inner sharded raw scan's projection SHALL carry every column the HAVING references, so `HAVING SUM(score) > 250.0` projects `SCORE` even when no select-list item, group key, filter, or ORDER BY element names it
* *AND* the returned result SHALL equal the result of the same grouped query evaluated on a single node, for the whole-predicate, mixed-junction, and `DISTINCT` shapes
* *AND* for a non-numeric aggregate the wrapper's native SQL SHALL yield Exasol's own outcome for that expression — a correct result where Exasol's implicit VARCHAR-to-numeric conversion succeeds, or Exasol's own `22018` "invalid character value for cast" where it does not — which is the same outcome the user's HAVING would produce against a native table, never an adapter-level decline
* *AND* a HAVING whose every referenced aggregate IS among the select-list plans SHALL still classify as the partial/merge grouped shape and SHALL still render its merged HAVING over the `PARTIAL_*` columns in the outer wrapper, unchanged by this delta
* *AND* because one shared classifier owns the decision, the fully-pruned zero-row path SHALL return the wrapper-shaped typed empty result for the same request, never an error

### Scenario: A grouped ORDER BY the merge cannot express routes to the qualified single-table wrapper instead of erroring

* *GIVEN* a grouped `pushdown` request whose `orderBy` resolves to NEITHER a group key NOR an aggregate among the detected select-list plans — a GROUP-KEY-ONLY select list with the sort aggregate absent from it and a request `LIMIT` (`SELECT c_nationkey FROM CUSTOMER GROUP BY c_nationkey ORDER BY SUM(c_acctbal) DESC LIMIT 5`, the "top N groups" shape issue #198 reports), an aggregate absent from a select list that carries a DIFFERENT aggregate (`SELECT c_bool, COUNT(*) FROM t GROUP BY c_bool ORDER BY SUM(c_price) DESC`), a `DISTINCT` aggregate, which has no partial/merge decomposition at all, or any other node the merge rewriter does not express
* *WHEN* the adapter classifies the request shape
* *THEN* the adapter SHALL resolve the grouped `ORDER BY` inside the ONE shared request-shape classifier and SHALL classify the request as the qualified single-table wrapper shape, exactly as an unmergeable HAVING does (issue #195)
* *AND* a GROUP-KEY-ONLY select list SHALL reach that classification through the SAME route as every other shape here: grouped detection SUCCEEDS for it with an EMPTY aggregate-plan list (its lone select item classifies as a group key, and the numeric type gate passes vacuously over zero plans), so the sort key resolves against zero plans, is therefore unresolvable, and routes to the wrapper — it is NOT filtered out ahead of detection
* *AND* the request's `LIMIT` SHALL render on that wrapper's outer SELECT, after its `GROUP BY` and its rendered `ORDER BY`, so a "top N groups" request over more than `n` groups returns EXACTLY the top `n` groups by the sort measure — not an arbitrary `n` of them — and returns every group when fewer than `n` exist; the per-shard fan-out legs SHALL carry no `LIMIT`, because the wrapper materializes raw rows and Exasol performs both the grouping and the cut
* *AND* the adapter MUST NOT return an error for this shape at either `EXPLAIN VIRTUAL` plan time or execution time, because the wrapper renders the `ORDER BY` as ordinary Exasol SQL over the materialized rows and the shared referenced-column helper already projects every column the sort expression names
* *AND* the adapter MUST NOT fabricate a `PARTIAL_*` column and an Exasol type for an aggregate Exasol declared no result type for, because a fabricated SUM type risks overflow and a fabricated numeric type risks precision-driven misordering
* *AND* because the shape decision is owned by that one classifier, the fully-pruned zero-row path SHALL return the wrapper-shaped typed empty result for the same request, never an error
* *AND* the returned result SHALL equal the same grouped query with the same `ORDER BY` evaluated over all rows on a single node
* *AND* this route SHALL be recorded as a deliberate, named trade rather than an unstated gap: the request loses partial/merge decomposition and materializes its referenced columns through Exasol, which is why an aggregate the select list ALREADY carries keeps the partial/merge path per the scenario above; a bounded partial/merge variant for the not-selected case is tracked as future work, `(#249)`

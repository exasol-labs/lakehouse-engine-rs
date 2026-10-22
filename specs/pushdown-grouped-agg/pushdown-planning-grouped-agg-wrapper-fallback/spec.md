# Feature: Pushdown Planning, Grouped Aggregate Wrapper Fallback

Extends `pushdown-grouped-agg/pushdown-planning-grouped-agg` for a grouped request that cannot use the partial/merge shape: an undecomposable select item, a HAVING that references an aggregate absent from the select list, or an ORDER BY that resolves to neither a group key nor a select-list aggregate. The adapter renders the grouped select list, GROUP BY, HAVING, ORDER BY, and LIMIT as ordinary Exasol SQL over a materialized sharded raw scan. The fallback is never an error.

## Background

* An undecomposable select item is a `DISTINCT` inner aggregate, a SUM or statistic over a non-numeric type, an untranslatable argument, or a node that is neither an aggregate nor a group key. A bare raw full-row scan would return a column count that does not match the query Exasol expects (SQL state `04000`), so the adapter falls back to a qualified single-table wrapper over a materialized sharded raw scan, like the unified join fallback.
* The inner raw scan projects only the referenced columns: group keys, select-list aggregate arguments, filter columns, and HAVING and ORDER BY references. It must expose every column the outer wrapper renders, and it projects at least one column, because an empty EMITS clause is invalid in Exasol. One shared helper computes the set. The single-group `COUNT(DISTINCT)` qualified-wrapper decline (`pushdown-aggregates/pushdown-planning-count-distinct`) uses the same helper. The helper walks the full `having` and `orderBy` expression trees.
* The HAVING renderer rewrites each `function_aggregate` in a HAVING to its merged `PARTIAL_*` expression, matched to the select-list aggregate plans by equality (kind plus source column). An aggregate with no matching plan cannot be rewritten, because the outer merge wrapper has only `GK_*` and `PARTIAL_*` columns. An AND/OR junction fails as a whole when any child fails to render.
* Whether the HAVING and the ORDER BY can be expressed over the merge decides the request shape, not only the SQL text. One shared classifier owns the decision, so the non-empty dispatch path and the fully-pruned zero-row path agree.
* The wrapper renders the HAVING through the vs-expression translator with aggregate names spliced verbatim over the materialized `LHS_T0` rows, so Exasol evaluates it. The HAVING is preserved, which keeps the advertised `AGGREGATE_HAVING` capability.
* A non-numeric aggregate routed to the wrapper becomes a native Exasol aggregate over a VARCHAR. Where Exasol's implicit VARCHAR-to-numeric conversion succeeds, the query returns a correct result. Where it does not, Exasol raises `22018` "invalid character value for cast", the same outcome as against a native table.
* An aggregate sort key absent from the select-list plans has no `PARTIAL_*` column to merge over. The adapter does not invent one, because Exasol declares a result type only for `selectList` items and a fabricated type risks SUM overflow and precision-driven misordering. The request routes to the wrapper, which renders the `ORDER BY` over materialized rows.


## Scenarios

### Scenario: Adapter falls back to a qualified single-table wrapper for an undecomposable grouped aggregate shape

* *GIVEN* a grouped `pushdown` request (`aggregationType: "group_by"`) the adapter cannot decompose into supported partials, a select-list item that is a `DISTINCT` inner aggregate, a SUM/stat aggregate over a non-numeric type (whether or not a HAVING is present), an untranslatable aggregate argument, or a non-aggregate/non-group-key node, or a `having` the adapter cannot render over the merge
* *WHEN* the adapter processes the request
* *THEN* the adapter MUST NOT emit a bare raw full-row `ScanSpec` for a grouped request (that would return a column count differing from the request's `selectList`, causing a client-facing `04000` "Expected number of columns is N but pushdown query has M")
* *AND* the adapter SHALL instead render the exact grouped select list, GROUP BY, HAVING, ORDER BY, and LIMIT as ordinary Exasol SQL over a materialized single-table sharded raw scan, a qualified single-table wrapper analogous to the unified join fallback (`SELECT <grouped select list> FROM (<sharded raw fan-out>) GROUP BY ... HAVING ... ORDER BY ... LIMIT ...`), so Exasol's core engine computes the aggregate over the returned rows
* *AND* the inner sharded raw scan's projection SHALL be narrowed to only the columns the request references, group keys, select-list aggregate arguments, filter columns, and HAVING and ORDER BY columns, NEVER the full base-table schema, so the fallback scan prunes I/O and network transfer to the referenced-column set while still exposing every column the outer wrapper renders
* *AND* the referenced-column set SHALL be computed by a single shared helper reused by the single-group `COUNT(DISTINCT)` qualified-wrapper decline (`pushdown-aggregates/pushdown-planning-count-distinct`), so both decline paths narrow identically; when the request references no source column the projection SHALL fall back to at least one column, since an empty EMITS clause is invalid in Exasol
* *AND* the scalar-over-aggregate select items in that wrapper SHALL be rendered by the vs-expression translator (aggregate names spliced verbatim, arguments recursed), since Exasol computes the aggregation over materialized rows rather than over merged partials
* *AND* the wrapper's result column count and per-column types SHALL match Exasol's positional `selectListDataTypes` validation
* *AND* the returned result SHALL equal the result of the same grouped query evaluated on a single node

### Scenario: A HAVING the adapter cannot render over the merge falls back instead of erroring

* *GIVEN* a grouped `pushdown` request carrying a `having` the adapter cannot rewrite over the `PARTIAL_*` merge columns, for any of its three reasons, an aggregate absent from the detected select-list plans (`SELECT MOD(id,4), COUNT(*) FROM t GROUP BY MOD(id,4) HAVING SUM(score) > 250.0`); such an aggregate as one operand of an AND/OR junction whose sibling operands DO match (`... HAVING COUNT(*) > 0 AND SUM(score) > 250.0`), which collapses the junction as a whole; or a `DISTINCT` aggregate, which has no partial/merge decomposition at all (`... HAVING COUNT(DISTINCT name) > 4`)
* *AND* a grouped request whose select-list aggregate column type fails the numeric gate WHILE a `having` is present, which is the same decline reached by a different route
* *WHEN* the adapter classifies the request shape
* *THEN* the adapter SHALL attempt the HAVING merge-rendering while classifying the shape and, when the HAVING does not render or the numeric gate fails, SHALL classify the request as the qualified single-table wrapper shape rather than the partial/merge grouped shape
* *AND* the adapter MUST NOT return an error for this shape at either `EXPLAIN VIRTUAL` plan time or execution time, because the wrapper renders the HAVING as ordinary Exasol SQL over materialized rows and therefore never drops a HAVING the adapter advertised `AGGREGATE_HAVING` for (issue #195)
* *AND* the inner sharded raw scan's projection SHALL carry every column the HAVING references, so `HAVING SUM(score) > 250.0` projects `SCORE` even when no select-list item, group key, filter, or ORDER BY element names it
* *AND* the returned result SHALL equal the result of the same grouped query evaluated on a single node, for the whole-predicate, mixed-junction, and `DISTINCT` shapes
* *AND* for a non-numeric aggregate the wrapper's native SQL SHALL yield Exasol's own outcome for that expression, a correct result where Exasol's implicit VARCHAR-to-numeric conversion succeeds, or Exasol's own `22018` "invalid character value for cast" where it does not, which is the same outcome the user's HAVING would produce against a native table, never an adapter-level decline
* *AND* a HAVING whose every referenced aggregate IS among the select-list plans SHALL still classify as the partial/merge grouped shape and SHALL still render its merged HAVING over the `PARTIAL_*` columns in the outer wrapper, unchanged by this delta
* *AND* because one shared classifier owns the decision, the fully-pruned zero-row path SHALL return the wrapper-shaped typed empty result for the same request, never an error

### Scenario: A grouped ORDER BY the merge cannot express routes to the qualified single-table wrapper instead of erroring

* *GIVEN* a grouped `pushdown` request whose `orderBy` resolves to NEITHER a group key NOR an aggregate among the detected select-list plans, a GROUP-KEY-ONLY select list with the sort aggregate absent from it and a request `LIMIT` (`SELECT c_nationkey FROM CUSTOMER GROUP BY c_nationkey ORDER BY SUM(c_acctbal) DESC LIMIT 5`, the "top N groups" shape), an aggregate absent from a select list that carries a DIFFERENT aggregate (`SELECT c_bool, COUNT(*) FROM t GROUP BY c_bool ORDER BY SUM(c_price) DESC`), a `DISTINCT` aggregate, which has no partial/merge decomposition at all, or any other node the merge rewriter does not express
* *WHEN* the adapter classifies the request shape
* *THEN* the adapter SHALL resolve the grouped `ORDER BY` inside the ONE shared request-shape classifier and SHALL classify the request as the qualified single-table wrapper shape, exactly as an unmergeable HAVING does
* *AND* a GROUP-KEY-ONLY select list SHALL reach that classification through the SAME route as every other shape here: grouped detection SUCCEEDS for it with an EMPTY aggregate-plan list (its lone select item classifies as a group key, and the numeric type gate passes vacuously over zero plans), so the sort key resolves against zero plans, is therefore unresolvable, and routes to the wrapper, it is NOT filtered out ahead of detection
* *AND* the request's `LIMIT` SHALL render on that wrapper's outer SELECT, after its `GROUP BY` and its rendered `ORDER BY`, so a "top N groups" request over more than `n` groups returns EXACTLY the top `n` groups by the sort measure, not an arbitrary `n` of them, and returns every group when fewer than `n` exist; the per-shard fan-out legs SHALL carry no `LIMIT`, because the wrapper materializes raw rows and Exasol performs both the grouping and the cut
* *AND* the adapter MUST NOT return an error for this shape at either `EXPLAIN VIRTUAL` plan time or execution time, because the wrapper renders the `ORDER BY` as ordinary Exasol SQL over the materialized rows and the shared referenced-column helper already projects every column the sort expression names
* *AND* the adapter MUST NOT fabricate a `PARTIAL_*` column and an Exasol type for an aggregate Exasol declared no result type for, because a fabricated SUM type risks overflow and a fabricated numeric type risks precision-driven misordering
* *AND* because the shape decision is owned by that one classifier, the fully-pruned zero-row path SHALL return the wrapper-shaped typed empty result for the same request, never an error
* *AND* the returned result SHALL equal the same grouped query with the same `ORDER BY` evaluated over all rows on a single node
* *AND* this route SHALL be a deliberate trade: the request loses partial/merge decomposition and materializes its referenced columns through Exasol, which is why an aggregate the select list ALREADY carries keeps the partial/merge path per the scenario above; a bounded partial/merge variant for the not-selected case is future work

### Scenario: Generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request through this feature's path
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the credential-reference guarantee of `storage-access/scan-spec-credential-reference` SHALL apply, so no credential value appears in generated SQL or error messages

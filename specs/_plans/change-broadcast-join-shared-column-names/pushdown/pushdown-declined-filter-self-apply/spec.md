# Feature: Pushdown Declined-Filter Self-Application

Guarantees that the adapter evaluates every WHERE predicate it accepts. The adapter applies, in its own returned SQL, any predicate it cannot push to DataFusion.

## Background

* Exasol does not re-apply a predicate whose capability the adapter advertised, and the response has no field to hand one back. An omitted predicate returns extra unfiltered rows.
* Three sites render the DataFusion-bound WHERE filter, and each returns `Option<String>`: the single-table path (`handle_pushdown`), the broadcast-join path (`render_broadcast_join`) and the N-scan per-leg path (`build_side_fan_out_sql`). `None` covers three outcomes: no filter, a filter that renders trivially true, and a declined filter. Only the declined filter must not be omitted.
* A filter declines when an adapter type-rewrite guard returns `None` or when the DataFusion-dialect renderer cannot express a node.
* `render_expression_safe` does not suppress a trivially-true result, so it returns `None` only for a declined filter. `crates/vs-expression` owns the trivially-true rule, and no call site re-tests it.
* Self-application renders the original request filter tree, not the type-rewritten tree. Exasol applies its own implicit coercions, including non-string-to-VARCHAR, which DataFusion refuses.
* A predicate unrenderable under both the DataFusion and the Exasol dialect is applied nowhere. The adapter returns a clean error, the same refusal as `pushdown/pushdown-planning-selectlist-expressions`. A self-applied predicate whose Exasol render is trivially true emits no clause and is not an error, so the adapter decides the error from a non-suppressing render.
* `handle_pushdown` computes the decline classification once per path and passes it down.
* The self-applied predicate must precede any aggregation, grouping or truncation the request carries. The single-table path therefore routes a declined filter through the qualified single-table wrapper, whose raw fan-out has no aggregate, sort or LIMIT.
* On the decline path the fan-out carries no filter, so projection width is the only remaining lever. A `SELECT *` cannot narrow, because Exasol validates its result positionally against the full base row.
* Iceberg file pruning keeps every conjunct, renderable or not, at both pruning inputs: the single-table scan-resolution tree and each join side's side-local predicate. Pruning only removes files that cannot match.
* The N-scan partition runs the type screen per side and per conjunct after attribution, because it needs the owning side's column metadata. `pushdown-joins/pushdown-planning-join-fallback` owns the leg-eligibility rule.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A broadcast-eligible join whose filter declines takes the N-scan fallback

* *GIVEN* an inner equi-join `pushdown` request that meets every other broadcast condition, two legs, an equi-condition, no Exasol postprocessing, a byte size at or below the broadcast threshold
* *AND* the request carries a non-null `filter` whose DataFusion-bound render declines, either because a type-rewrite guard returned `None` when run against the two legs' column metadata keyed by (relation alias, column) or because the DataFusion dialect cannot express a node in the tree
* *WHEN* the adapter renders the join pushdown
* *THEN* the adapter SHALL decline the broadcast plan and fall through to the unified unaccelerated N-scan fallback, which applies the predicate itself, honouring the recorded broadcast contract that a filter the translator cannot render is served by the fallback
* *AND* the adapter MUST NOT emit a broadcast plan whose scan spec carries no filter, because the broadcast SQL has no outer `WHERE` in which the predicate could be applied
* *AND* the decline SHALL be a clean `Ok(None)`-shaped fall-through, NOT an error, matching the unrenderable-condition and widened-projection declines already on that path
* *AND* both trigger classes SHALL reach this ONE outcome, so the adapter SHALL NOT grow a second broadcast-decline route for a type decline, REPLACING the recorded "whose DataFusion-bound render declines", which named only the dialect trigger because the broadcast site ran no type screen
* *AND* a filter the type-rewrite pipeline REWRITES rather than declines SHALL NOT trigger this decline, and SHALL keep the broadcast plan with the REWRITTEN tree carried in the common spec
* *AND* the returned rows SHALL equal native Exasol evaluation of the same join
<!-- /DELTA:CHANGED -->

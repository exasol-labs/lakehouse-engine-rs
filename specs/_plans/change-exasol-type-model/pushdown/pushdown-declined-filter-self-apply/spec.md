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
### Scenario: A predicate unrenderable under both dialects returns a clean error

* *GIVEN* a `pushdown` request whose filter carries a node that neither the DataFusion dialect nor the Exasol dialect renders, such as a node type the translator does not know
* *WHEN* the adapter attempts to self-apply the predicate in its outer `WHERE`
* *THEN* the adapter SHALL return a client-facing error naming the unrenderable predicate, at both the single-table wrapper and the N-scan wrapper
* *AND* the adapter MUST NOT return SQL that omits the predicate, so the query SHALL fail rather than return rows the predicate would have excluded
* *AND* the error SHALL redact exactly what the wrapper's other refusals redact
* *AND* Exasol SHALL NOT re-plan on that error, so the failure is final and visible to the client
<!-- /DELTA:CHANGED -->

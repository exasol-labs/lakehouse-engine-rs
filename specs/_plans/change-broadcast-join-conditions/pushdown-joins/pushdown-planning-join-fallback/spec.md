# Feature: Pushdown Planning, Unified Unaccelerated Join Fallback

One renderer serves every inner equi-join outside the two-leg broadcast contract. It scans each involved table through its own sharded fan-out subquery (a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor over an ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF) and rebuilds the join in Exasol's core engine as a left-to-right `INNER JOIN ... ON` chain, with leg-local `WHERE` conjuncts pushed into each leg.

## Background

* One implementation serves all N >= 2 legs. Broadcast (`pushdown-joins/pushdown-planning-join`) is an optimization selected within the join path, not a second renderer.
* A leg is one occurrence of a table in the FROM tree. Column-to-leg attribution is owned by `pushdown-joins/pushdown-planning-join-fallback-self-join`.
* Legs are numbered in the FROM tree's left-to-right traversal order, and subquery aliases follow that order.
* The adapter advertises `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI` with no per-query opt-out, and Exasol never re-plans on an adapter error. Every advertised join shape must therefore render by broadcast or by this fallback. Exasol pushes only those inner equi-joins, and the Exasol dialect renders every condition and clause Exasol sends, so the fallback has no join shape to refuse.
* The wrapper has four entry points: an inner equi-join of N >= 2 legs, a grouped request that did not decompose into the partial/merge shape (`GroupByWrapper`), a single-group request with more than one `COUNT(DISTINCT)` or a distinct mixed with an ordinary aggregate (`pushdown-aggregates/pushdown-planning-count-distinct`), and a row scan whose derived projection was widened. All four share one `ORDER BY` and window rendering site.
* The wrapper resolves its own `LIMIT` and `OFFSET` and renders them on its outer SELECT over limit-free, sort-free fan-out legs. A row limit that leaked into a leg would cap that leg's pre-join input and return wrong rows. A non-zero `limit.offset` never arrives without a non-empty `orderBy` (`pushdown-capabilities/pushdown-planning-order-by-capability`).
* The broadcast path declines, and this wrapper serves, for a request without Exasol postprocessing: a non-zero offset without `orderBy`, a non-column sort key other than a constant, an `orderBy` of constants only together with a non-zero offset, and a sort key absent from the broadcast projection (`pushdown-joins/pushdown-planning-join`).
* A top-level WHERE conjunct reaches a leg only if it is local to that leg and the scan translator renders it against that leg's own column metadata (`pushdown-types/pushdown-planning-scan-text-conversion`). Every other conjunct is residual and lands in the outer wrapper's `WHERE`. The leg and residual sets are exact complements (`pushdown/pushdown-declined-filter-self-apply`).
* The outer `WHERE` render has two outcomes. An absent residual set emits no clause. Any other residual set renders through the qualified Exasol-dialect render as it is.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A join outside the broadcast contract is declined safely

* *GIVEN* a `pushdown` request whose `from` clause is a join
* *WHEN* the join is not a single broadcast-eligible two-leg inner equi-join, it is above threshold, spans more than two legs, or carries a condition/filter/projection detail that keeps it off the broadcast path
* *THEN* the adapter SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL instead render the request through the SINGLE unified unaccelerated per-table-scan fallback (N ≥ 2 legs, the two-leg case being N = 2), so Exasol's core engine produces the correct result
* *AND* a conjunct of the join condition beside its join equalities SHALL keep the request off the broadcast path only where the same conjunct written in WHERE would, because the adapter plans it as a WHERE conjunct (`pushdown-joins/pushdown-planning-join-condition-filters`)
* *AND* spanning more than two legs SHALL by itself NEVER be a reason to return an error, every such inner join is served by the unified fallback
* *AND* the adapter SHALL return a HARD error, a client-facing `F-UDF-CL-RUST-9001`, NOT a request that Exasol retries natively (Exasol does not re-plan on an adapter error), ONLY when an involved table carries no readable identifier note
* *AND* the adapter MUST NOT emit any scan spec that would compute a different result than single-node evaluation
<!-- /DELTA:CHANGED -->

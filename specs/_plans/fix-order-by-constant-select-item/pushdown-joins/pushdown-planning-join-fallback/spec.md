# Feature: Pushdown Planning, Unified Unaccelerated Join Fallback

One renderer serves every inner equi-join outside the two-leg broadcast contract. It scans each involved table through its own sharded fan-out subquery (a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor over an ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF) and rebuilds the join in Exasol's core engine as a left-to-right `INNER JOIN ... ON` chain, with leg-local `WHERE` conjuncts pushed into each leg.

<!-- DELTA:CHANGED -->
## Background

* One implementation serves all N >= 2 legs. Broadcast (`pushdown-joins/pushdown-planning-join`) is an optimization selected within the join path, not a second renderer.
* A leg is one occurrence of a table in the FROM tree. Column-to-leg attribution is owned by `pushdown-joins/pushdown-planning-join-fallback-self-join`.
* Legs are numbered in the FROM tree's left-to-right traversal order, and subquery aliases follow that order.
* The adapter advertises `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI` with no per-query opt-out, and Exasol never re-plans on an adapter error. Every advertised join shape must therefore render by broadcast or by this fallback. Exasol pushes only those inner equi-joins, and the Exasol dialect renders every condition and clause Exasol sends, so the fallback has no join shape to refuse.
* The wrapper has four entry points: an inner equi-join of N >= 2 legs, a grouped request that did not decompose into the partial/merge shape (`GroupByWrapper`), a single-group request with more than one `COUNT(DISTINCT)` or a distinct mixed with an ordinary aggregate (`pushdown-aggregates/pushdown-planning-count-distinct`), and a row scan whose derived projection was widened. All four share one `ORDER BY` and window rendering site.
* The wrapper resolves its own `LIMIT` and `OFFSET` and renders them on its outer SELECT over limit-free, sort-free fan-out legs. A row limit that leaked into a leg would cap that leg's pre-join input and return wrong rows. A non-zero `limit.offset` never arrives without a non-empty `orderBy` (`pushdown-capabilities/pushdown-planning-order-by-capability`).
* The broadcast path declines, and this wrapper serves: a non-zero offset without `orderBy`, a non-column sort key other than a constant, an `orderBy` of constants only together with a non-zero offset, a sort key absent from the broadcast projection, and every request that requires Exasol postprocessing (`pushdown-joins/pushdown-planning-join`).
* A top-level WHERE conjunct reaches a leg only if it is local to that leg and the scan translator renders it against that leg's own column metadata (`pushdown-types/pushdown-planning-scan-text-conversion`). Every other conjunct is residual and lands in the outer wrapper's `WHERE`. The leg and residual sets are exact complements (`pushdown/pushdown-declined-filter-self-apply`).
* The outer `WHERE` render has two outcomes. An absent residual set emits no clause. Any other residual set renders through the qualified Exasol-dialect render as it is.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: The qualified wrapper renders the request's OFFSET on every entry point that reaches it

* *GIVEN* a `pushdown` request routed to the unified qualified wrapper, carrying a renderable non-empty `orderBy`, a `limit` with `numElements` = `n`, and a non-zero `limit.offset` = `m`
* *WHEN* the adapter builds the wrapper SQL, for each of the four entry points that reach this wrapper: an N-scan inner equi-join, a non-decomposing grouped request, a multi-`COUNT(DISTINCT)` decline, and a widened-projection row scan
* *THEN* the wrapper's outer SELECT SHALL render `ORDER BY <clause> LIMIT n OFFSET m`, in that clause order, through the SAME shared limit-and-offset seam every other wrapper uses
* *AND* the per-leg fan-out subqueries SHALL remain limit-free and sort-free, so the outer SELECT is the ONLY place either bound is applied and no leg drops a row that the join or the window still needs
* *AND* a request whose `limit.offset` is zero or absent SHALL render no `OFFSET` on all four entry points
* *AND* the wrapper's visible SELECT list SHALL be UNCHANGED by the offset, so the returned column count still equals the `selectList` item count Exasol validates positionally
* *AND* the adapter SHALL NOT render an `OFFSET` on a wrapper SELECT that renders no `ORDER BY`, because Exasol rejects that SQL with `sqlCode 42000` ("OFFSET not allowed in LIMIT without ORDER BY")
* *AND* that state SHALL NOT be given a failure branch of its own, because it is unreachable: this wrapper resolves no `ORDER BY` clause ONLY when the request's `orderBy` is absent or empty, and a request carrying a non-zero `limit.offset` always carries a non-empty `orderBy` (see the offset-implies-ordering invariant in `pushdown-capabilities/pushdown-planning-order-by-capability`)
* *AND* the adapter MUST NOT introduce a hard client-facing `User` decline for that unreachable state, because this seam serves all four entry points above: a decline here would turn a successful query into a hard failure on a branch no test exercises. The invariant SHALL instead be pinned by an assertion plus a unit test asserting that no emitted wrapper SQL contains an `OFFSET` token unpreceded by an `ORDER BY`, driven by the request shapes that actually reach this seam with an offset
* *AND* the returned result SHALL equal the same query with the same `ORDER BY … LIMIT n OFFSET m` evaluated over all rows on a single node

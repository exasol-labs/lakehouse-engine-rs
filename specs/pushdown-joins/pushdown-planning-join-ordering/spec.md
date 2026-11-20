# Feature: Pushdown Planning, Join Ordering and Limit

Serves a pushed `ORDER BY`, `LIMIT`, and `OFFSET` over a join. The broadcast path (`pushdown-joins/pushdown-planning-join`) bounds each fact shard to its own post-join top-N or cap, and the qualified wrapper of the unified unaccelerated fallback (`pushdown-joins/pushdown-planning-join-fallback`) resolves them on its outer SELECT.

## Background

* The broadcast contract, Exasol postprocessing, and the leg binding are defined in `pushdown-joins/pushdown-planning-join`. The wrapper and its four entry points are defined in `pushdown-joins/pushdown-planning-join-fallback`.
* The wrapper resolves its own `LIMIT` and `OFFSET` and renders them on its outer SELECT over limit-free, sort-free fan-out legs. A row limit that leaked into a leg would cap that leg's pre-join input and return wrong rows. A non-zero `limit.offset` never arrives without a non-empty `orderBy` (`pushdown-capabilities/pushdown-planning-order-by-capability`).
* The broadcast path declines, and this wrapper serves, for a request without Exasol postprocessing: a non-zero offset without `orderBy`, a non-column sort key other than a constant, an `orderBy` of constants only together with a non-zero offset, and a sort key absent from the broadcast projection (`pushdown-joins/pushdown-planning-join`).

## Scenarios

### Scenario: A bare LIMIT over a broadcast-eligible join is served by the broadcast path with a per-shard post-join cap

* *GIVEN* a `pushdown` request that is broadcast-eligible in every other respect, exactly two legs, a condition of join keys, the dimension side at or below the broadcast threshold, a renderable condition and filter, a non-widened projection
* *AND* the request carries a `limit` with `numElements` = `n`, an ABSENT or EMPTY `orderBy`, and a zero or absent `limit.offset`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out for that request and MUST NOT route it to the unified unaccelerated fallback
* *AND* the shard-invariant common scan spec's JOIN BLOCK SHALL carry a post-join cap of `n`, so every fact-shard invocation truncates its OWN joined output at `n` inside its DataFusion session rather than emitting its complete local join
* *AND* the common scan spec's own shard-invariant row-limit field SHALL remain absent, so the cap exists only where a join block does and no fallback leg spec can express one (`pushdown-joins/pushdown-planning-join-fallback`)
* *AND* that per-shard cap SHALL be applied AFTER the join and its WHERE filter, never to the fact side's pre-join scan, because an input-side cap discards fact rows that would have matched and keeps fact rows that produce no output (`scan-aggregation/scan-execution-join`)
* *AND* the generated scan-driving SQL SHALL render the SAME undivided `LIMIT n` once on the outer ungrouped scalar select over the distributor subquery, through the row-scan builder's limit seam, so the merge of up to `G × n` shard rows is truncated to `n`
* *AND* the adapter MUST NOT divide `n` across shards, because a shard producing fewer than `n` joined rows would then make the merged result short
* *AND* the returned result SHALL be `n` rows whenever the unbounded join produces at least `n`, and SHALL be a subset of the unbounded join's rows in every case, equal to some valid single-node `LIMIT n` answer

### Scenario: A pushed ordering over a broadcast-eligible join is served by an outer wrapper over the broadcast fan-out

* *GIVEN* a `pushdown` request that is broadcast-eligible in every other respect, carrying a non-empty `orderBy` whose every element is a bare `column` node that carries both `isAscending` and `nullsLast` and names a bare-column item of the broadcast projection, and an optional `limit` with `numElements` = `n` and an optional `limit.offset` = `m`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL serve the request with the broadcast fan-out, MUST NOT route it to the unified unaccelerated fallback, and SHALL wrap that fan-out in ONE outer `SELECT <visible projection items> FROM (<fan-out>) ORDER BY <keys>` plus the request's full retained window, rendering the window through the SAME shared limit-and-offset seam every other wrapper uses (` LIMIT n` byte-for-byte for a zero or absent offset, ` LIMIT n OFFSET m` for a non-zero one) and the select list and keys through the SAME shared emitted-identifier and direction/NULL-placement seams the declined row-scan wrapper uses (`pushdown-capabilities/pushdown-planning-topn`)
* *AND* the fan-out's own outer scalar select SHALL carry NO `LIMIT`, and the shard-invariant common scan spec SHALL carry NO `limit` and NO `order_by` key, so the wrapper is the ONLY place the global ordering and window are applied; what each shard's join block carries is set by this feature's two per-shard ordered-window scenarios
* *AND* an UNORDERED broadcast request, with or without a bare `LIMIT`, SHALL still be emitted as the bare fan-out with no wrapping select
* *AND* the outer wrapper SHALL always render an `ORDER BY`, because an ordered window carries at least one key, so no broadcast ordered plan returns rows that are silently unordered while `ORDER_BY_COLUMN` is advertised
* *AND* the returned result SHALL equal the same `ORDER BY … [LIMIT n [OFFSET m]]` evaluated over the same inner join on a single node

### Scenario: A zero-offset ORDER BY with LIMIT over a broadcast-eligible join bounds each shard to its own post-join top-N

* *GIVEN* a `pushdown` request that the ordered broadcast wrapper serves, carrying a `limit` with `numElements` = `n` and a zero or absent `limit.offset`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the shard-invariant common scan spec's JOIN BLOCK SHALL carry a post-join ordering equal to the request's sort keys, in pushed order, each with its column, direction, and NULL placement, together with a post-join cap of `n`, so every fact-shard invocation emits at most `n` joined rows, its own top-ranked ones
* *AND* the adapter MUST NOT divide `n` across shards, because every row of the global top-n can come from one shard
* *AND* a request carrying `limit.offset` EQUAL TO ZERO SHALL produce the same SQL as a request carrying no `offset` key, so the eligibility test is a non-zero test and not a key-presence test
* *AND* a request with no `limit` or a non-zero offset SHALL carry NEITHER post-join field, so every shard emits its complete local joined output for the wrapper to sort and window
* *AND* the returned result SHALL carry the same sort-key values, in the same order, as the same `ORDER BY … LIMIT n` evaluated over the same inner join on a single node, and every returned row SHALL be a row of that join

### Scenario: Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback

* *GIVEN* a `pushdown` request over a broadcast-eligible inner equi-join that requires no Exasol postprocessing, carrying one of: a `limit` with a NON-ZERO `limit.offset` and an absent or empty `orderBy`; an `orderBy` element that is an expression, an aggregate, or any non-`column` node other than a constant; an `orderBy` of constants only together with a NON-ZERO `limit.offset`; an `orderBy` element missing `isAscending` or `nullsLast`; or an `orderBy` element naming a column that is not a bare-column item of the broadcast projection
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL decline the broadcast plan and route the request to the unified unaccelerated fallback, whose qualified wrapper renders the ordering and the window table-qualified over the materialized join
* *AND* every arm EXCEPT projection membership SHALL be decided from the `pushdown` request ALONE, BEFORE either side is sized and before the broadcast render runs
* *AND* the projection-membership arm SHALL be decided LATER, at the broadcast construction site against the RENDERED broadcast projection, because that projection does not exist until the render has run, and a sort key naming a column that is not a bare-column item of it SHALL downgrade the request to the SAME unified unaccelerated fallback, by the same fall-through
* *AND* that decline SHALL be a clean fall-through and MUST NOT be a hard client-facing `User` error, because, unlike the declined single-table row-scan path (`pushdown-capabilities/pushdown-planning-topn`), a renderer that CAN express the shape still exists
* *AND* the adapter MUST NOT drop a non-constant sort key, a limit, or an offset it declined to serve on the broadcast path, so no reachable shape returns rows that are silently unordered, unwindowed, or wrongly truncated; a constant sort key is dropped, because it does not change the order (`pushdown-capabilities/pushdown-planning-order-by-capability`)
* *AND* the bare-`LIMIT`-with-non-zero-offset arm SHALL be a structural guard rather than a reachable branch, because a non-zero `limit.offset` never arrives without a non-empty `orderBy` (`pushdown-capabilities/pushdown-planning-order-by-capability`)

### Scenario: Constant sort keys leave a broadcast-eligible join on the broadcast path

* *GIVEN* a `pushdown` request that is broadcast-eligible in every other respect, whose `orderBy` holds constant sort keys, which the adapter drops when it parses the request (`pushdown-capabilities/pushdown-planning-order-by-capability`)
* *AND* every other sort key is a bare `column` node of the broadcast projection (`SELECT e.id, 1 FROM events e JOIN labels l ON e.id = l.id ORDER BY 2 DESC, 1 LIMIT 3`), or the request has no other sort key and carries a `limit` with a zero or absent `limit.offset` (`SELECT e.id, 1 FROM events e JOIN labels l ON e.id = l.id ORDER BY 2 LIMIT 3`)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out and MUST NOT route the request to the unified unaccelerated fallback
* *AND* the adapter SHALL serve the first query through the ordered broadcast window on its column keys, and the second query as a bare `LIMIT` over the broadcast fan-out
* *AND* the returned result SHALL equal the same query on a single node: ids 1, 2, 3 in that order for the first query, and 3 rows of the join for the second

### Scenario: A fallback leg's fan-out scan spec never carries a row limit

* *GIVEN* a join request carrying a `limit` with `numElements` = `n` and any combination of `orderBy`, `groupBy`, `having`, and `limit.offset`
* *AND* it is served by the unified unaccelerated fallback, for any N ≥ 2, or by a broadcast pair that leaves a join or a filter conjunct for Exasol
* *WHEN* the adapter builds each fan-out
* *THEN* every leg fan-out's scan spec SHALL carry `limit` = absent, an EMPTY `order_by`, and NO join block, so the leg emits every row the outer wrapper's join, ordering, and window still depend on
* *AND* the broadcast pair's scan spec SHALL carry `limit` = absent, an EMPTY `order_by`, and a join block with no post-join limit and no post-join ordering, so no shard cuts or sorts rows before Exasol's join or `WHERE` has run
* *AND* the request's `LIMIT` and `OFFSET` SHALL be rendered ONLY on the outer wrapper SELECT
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: The qualified wrapper renders a renderable expression sort key on every entry point that reaches it

* *GIVEN* a `pushdown` request routed to the unified qualified wrapper, whose `orderBy` carries a sort key that is an EXPRESSION node rather than a bare `column`, a scalar-function, arithmetic, aggregate, or CAST node, and which the qualified expression renderer CAN render
* *WHEN* the adapter builds the wrapper SQL, for each of the four entry points that reach this wrapper: an N-scan inner equi-join, a non-decomposing grouped request (`GroupByWrapper`), a multi-`COUNT(DISTINCT)` decline, and a widened-projection row scan
* *THEN* the wrapper's outer `ORDER BY` SHALL carry that expression rendered with table-qualified column references resolved from each `column` node's `tableName`, exactly as the wrapper renders its select list, WHERE, GROUP BY, and HAVING, so an expression sort key is rendered, NOT declined
* *AND* the element SHALL render its direction and NULL placement through the one shared direction/NULL seam, so the wrapper's ordering cannot drift from the other ordered paths
* *AND* an `orderBy` mixing a bare-column key with an expression key SHALL render EVERY element, in the pushed order, each with its own direction and NULL placement
* *AND* the wrapper's visible SELECT list SHALL be UNCHANGED by the ordering, the sort expression is rendered inline in the `ORDER BY`, so NO hidden output column is added and the returned column count still equals the `selectList` item count Exasol validates positionally
* *AND* the per-leg fan-out subqueries SHALL each project every column the sort expression names, via the SAME referenced-column narrowing helper that walks the `orderBy` node's full expression tree, so the wrapper never references a column a leg does not emit
* *AND* the returned result SHALL equal the same query with the same `ORDER BY` evaluated over all rows on a single node

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

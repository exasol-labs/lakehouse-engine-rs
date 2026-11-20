# Feature: Pushdown Planning, Join Ordering and Limit

Serves a pushed `ORDER BY`, `LIMIT`, and `OFFSET` over a join. The broadcast path (`pushdown-joins/pushdown-planning-join`) bounds each fact shard to its own post-join top-N or cap, and the qualified wrapper of the unified unaccelerated fallback (`pushdown-joins/pushdown-planning-join-fallback`) resolves them on its outer SELECT.

## Background

* The broadcast contract, Exasol postprocessing, and the leg binding are defined in `pushdown-joins/pushdown-planning-join`. The wrapper and its four entry points are defined in `pushdown-joins/pushdown-planning-join-fallback`.
* The wrapper resolves its own `LIMIT` and `OFFSET` and renders them on its outer SELECT over limit-free, sort-free fan-out legs. A row limit that leaked into a leg would cap that leg's pre-join input and return wrong rows. A non-zero `limit.offset` never arrives without a non-empty `orderBy` (`pushdown-capabilities/pushdown-planning-order-by-capability`).
* The broadcast path declines, and this wrapper serves, for a request without Exasol postprocessing: a non-zero offset without `orderBy`, a non-column sort key other than a constant, an `orderBy` of constants only together with a non-zero offset, and a sort key absent from the broadcast projection (`pushdown-joins/pushdown-planning-join`).

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A fallback leg's fan-out scan spec never carries a row limit

* *GIVEN* a join request carrying a `limit` with `numElements` = `n` and any combination of `orderBy`, `groupBy`, `having`, and `limit.offset`
* *AND* it is served by the unified unaccelerated fallback, for any N ≥ 2, or by a broadcast pair that leaves a join or a filter conjunct for Exasol
* *WHEN* the adapter builds each fan-out
* *THEN* every leg fan-out's scan spec SHALL carry `limit` = absent, an EMPTY `order_by`, and NO join block, so the leg emits every row the outer wrapper's join, ordering, and window still depend on
* *AND* the broadcast pair's scan spec SHALL carry `limit` = absent, an EMPTY `order_by`, and a join block with no post-join limit and no post-join ordering, so no shard cuts or sorts rows before Exasol's join or `WHERE` has run
* *AND* the request's `LIMIT` and `OFFSET` SHALL be rendered ONLY on the outer wrapper SELECT
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:CHANGED -->

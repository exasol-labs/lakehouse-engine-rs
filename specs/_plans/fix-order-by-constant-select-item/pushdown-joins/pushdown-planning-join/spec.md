# Feature: Pushdown Planning, Broadcast Inner Equi-Join

Extends pushdown planning (`pushdown/pushdown-planning`) with the broadcast inner equi-join. When the smaller side of a two-leg join is below the broadcast threshold, the adapter shards the larger (fact) side across the scan fan-out and replicates the smaller (dimension) side into the shard-invariant common spec, and a node-local DataFusion join runs inside the scan UDF (`scan-aggregation/scan-execution-join`). The two legs are two tables or one table joined to itself, and they may share column names. Every other join is served by the unified unaccelerated fallback (`pushdown-joins/pushdown-planning-join-fallback`).

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/pushdown-joins/pushdown-planning-join/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Constant sort keys leave a broadcast-eligible join on the broadcast path

* *GIVEN* a `pushdown` request that is broadcast-eligible in every other respect, whose `orderBy` holds constant sort keys, which the adapter drops when it parses the request (`pushdown-capabilities/pushdown-planning-order-by-capability`)
* *AND* every other sort key is a bare `column` node of the broadcast projection (`SELECT e.id, 1 FROM events e JOIN labels l ON e.id = l.id ORDER BY 2 DESC, 1 LIMIT 3`), or the request has no other sort key and carries a `limit` with a zero or absent `limit.offset` (`SELECT e.id, 1 FROM events e JOIN labels l ON e.id = l.id ORDER BY 2 LIMIT 3`)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out and MUST NOT route the request to the unified unaccelerated fallback
* *AND* the adapter SHALL serve the first query through the ordered broadcast window on its column keys, and the second query as a bare `LIMIT` over the broadcast fan-out
* *AND* the returned result SHALL equal the same query on a single node: ids 1, 2, 3 in that order for the first query, and 3 rows of the join for the second
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback

* *GIVEN* a `pushdown` request over a broadcast-eligible inner equi-join carrying one of: a `limit` with a NON-ZERO `limit.offset` and an absent or empty `orderBy`; an `orderBy` element that is an expression, an aggregate, or any non-`column` node other than a constant; an `orderBy` of constants only together with a NON-ZERO `limit.offset`; an `orderBy` element missing `isAscending` or `nullsLast`; or an `orderBy` element naming a column that is not a bare-column item of the broadcast projection
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL decline the broadcast plan and route the request to the unified unaccelerated fallback, whose qualified wrapper renders the ordering and the window table-qualified over the materialized join
* *AND* every arm EXCEPT projection membership SHALL be decided from the `pushdown` request ALONE, BEFORE either side is sized and before the broadcast render runs, so a request that requires Exasol postprocessing MUST NOT reach the broadcast renderer at all, and pays neither its condition, filter, and projection render nor its no-column-metadata error arm
* *AND* the projection-membership arm SHALL be decided LATER, at the broadcast construction site against the RENDERED broadcast projection, because that projection does not exist until the render has run, and a sort key naming a column that is not a bare-column item of it SHALL downgrade the request to the SAME unified unaccelerated fallback, by the same fall-through
* *AND* that decline SHALL be a clean fall-through and MUST NOT be a hard client-facing `User` error, because, unlike the declined single-table row-scan path (`pushdown-capabilities/pushdown-planning-topn`), a renderer that CAN express the shape still exists
* *AND* the adapter MUST NOT drop a non-constant sort key, a limit, or an offset it declined to serve on the broadcast path, so no reachable shape returns rows that are silently unordered, unwindowed, or wrongly truncated; a constant sort key is dropped, because it does not change the order (`pushdown-capabilities/pushdown-planning-order-by-capability`)
* *AND* the bare-`LIMIT`-with-non-zero-offset arm SHALL be a structural guard rather than a reachable branch, because a non-zero `limit.offset` never arrives without a non-empty `orderBy` (`pushdown-capabilities/pushdown-planning-order-by-capability`)
* *AND* a request that requires Exasol postprocessing SHALL force the unified unaccelerated fallback unconditionally
<!-- /DELTA:CHANGED -->

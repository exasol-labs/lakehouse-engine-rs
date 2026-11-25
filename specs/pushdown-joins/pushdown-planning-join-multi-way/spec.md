# Feature: Pushdown Planning, Multi-Way Join Broadcast

Extends the broadcast inner equi-join (`pushdown-joins/pushdown-planning-join`) to an inner-join tree of three or more legs. The adapter broadcasts the best eligible pair as one fan-out, every other leg keeps its own fan-out, and Exasol joins the fan-outs.

## Background

* A broadcast pair, its fact and dimension legs, eligibility, and width-bounded columns are defined in `pushdown-joins/pushdown-planning-join`. A leg's own fan-out is the one the unified unaccelerated fallback scans (`pushdown-joins/pushdown-planning-join-fallback`).
* The pair's fan-out repeats each dimension column on every joined row. When a later step drops rows, such as Exasol's join with the other legs, a filter Exasol runs, or an aggregate, a dimension text column is sent for rows that never reach the result, so in a join of three or more legs every dimension column the pair's fan-out emits must be width-bounded.

## Scenarios

### Scenario: A join of three or more legs broadcasts one pair and scans the other legs

* *GIVEN* a `pushdown` request over an inner-join tree of three or more legs, every join of which is inner
* *AND* at least one pair of its legs is eligible for broadcast
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL broadcast exactly one pair, as one fan-out whose join block joins each shard's files of the fact leg with every file of the dimension leg
* *AND* every other leg SHALL keep its own fan-out, carrying its leg-local filter, as the unified unaccelerated fallback scans a leg (`pushdown-joins/pushdown-planning-join-fallback`)
* *AND* Exasol SHALL join the pair's fan-out and the other fan-outs in an `INNER JOIN … ON` chain, attaching each join condition other than the pair's join keys where every leg it reads is in scope
* *AND* the pair's fan-out SHALL emit, under `<alias>_<column>` output names, each column of its two legs that the select list, the other join conditions, Exasol's `WHERE`, `GROUP BY`, `HAVING`, and `ORDER BY` read, and one join-key column of its first leg in FROM-tree order when they read none
* *AND* the pair's fan-out SHALL emit only the fact rows that match a dimension row under the pair's join keys and the pair's scan filter
* *AND* the pair's join block SHALL carry no post-join limit and no post-join ordering, and Exasol SHALL apply the remaining `WHERE`, aggregation, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, and `OFFSET` over the joined fan-outs
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query, for a star join, a chain, four legs, a three-leg self-join, legs that share column names, a dimension with duplicate join keys, a join condition that reads three legs (`c.k + o.k = l.k`), a `WHERE` conjunct across the pair and another leg, `GROUP BY` with `HAVING`, `ORDER BY … LIMIT … OFFSET`, and an empty table on any leg

### Scenario: The broadcast pair is the eligible pair whose dimension has a filter of its own

* *GIVEN* a `pushdown` request over an inner-join tree with two or more eligible pairs, such as `lineitem` joined to `part` filtered by `p.p_brand = 'Brand#23'` and to `supplier` with no filter
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL broadcast the pair that ranks first by, in order: its dimension has a WHERE or `ON` conjunct of its own that the scan renders; its fact leg has more bytes; its dimension has fewer bytes; its legs come first in FROM-tree order
* *AND* in the example the adapter SHALL broadcast `part` into the shards of `lineitem`, and scan `supplier` on its own
* *AND* a pair whose dimension has no filter of its own SHALL still be broadcast when no eligible pair ranks above it
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: A pair that is not eligible is not broadcast

* *GIVEN* a `pushdown` request over an inner-join tree of three or more legs
* *AND* a pair whose dimension exceeds `JOIN_BROADCAST_MAX_BYTES`, or whose fan-out would emit a dimension text column that is not width-bounded, such as `SELECT d.DESCRIPTION, …` with no literal condition on it
* *WHEN* the adapter plans the request
* *THEN* the adapter MUST NOT broadcast that pair, whether or not the request aggregates, because Exasol's join with the other legs can drop the rows that repeat that column
* *AND* when no pair is eligible, the adapter SHALL serve the request through the unified unaccelerated fallback, with one fan-out per leg and no join block (`pushdown-joins/pushdown-planning-join-fallback`)
* *AND* when the scan cannot render a pair's join keys or emitted columns, or an emitted output name is longer than Exasol's 128-character identifier limit, the adapter SHALL broadcast the next eligible pair in rank order, and every leg SHALL keep its own fan-out when no eligible pair renders
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

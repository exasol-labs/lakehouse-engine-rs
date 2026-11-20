<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning, Broadcast Inner Equi-Join

Extends pushdown planning (`pushdown/pushdown-planning`) with the broadcast inner equi-join. The adapter broadcasts one eligible pair of join legs: it shards the pair's larger (fact) leg across the scan fan-out and replicates its smaller (dimension) leg into the shard-invariant common spec, and a node-local DataFusion join runs inside the scan UDF (`scan-aggregation/scan-execution-join`). In a two-leg join the pair is the whole join. In a join of three or more legs every other leg keeps its own fan-out, and Exasol joins the fan-outs. The legs may be one table joined to itself, and they may share column names. A join with no eligible pair is served by the unified unaccelerated fallback (`pushdown-joins/pushdown-planning-join-fallback`).
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* A broadcast pair is two legs of an all-inner join tree that one or more join keys join, each key reading only those two legs (`pushdown-joins/pushdown-planning-join-condition-filters`). Its smaller leg by bytes after its own pruning is the dimension, and the other is the fact.
* A pair is eligible when its dimension is at or below `JOIN_BROADCAST_MAX_BYTES` and, when Exasol processes the pair's rows further (it aggregates them under Exasol postprocessing, joins them with other legs, or applies a filter conjunct to them), every dimension column that the select list, the other join conditions, Exasol's `WHERE`, `GROUP BY`, `HAVING`, and `ORDER BY` read is width-bounded. A request meets the broadcast contract when it is a two-leg join with an eligible pair and the scan renders every WHERE conjunct and every select-list item, so its broadcast fan-out is the whole plan.
* Exasol postprocessing means a non-null `aggregationType`, `group_by` or `single_group`. Exasol sets it on every request that aggregates, groups, or carries a HAVING, including one whose aggregates it stripped from the select list. A pushed `LIMIT` and a pushed `ORDER BY` do not force it.
* A width-bounded column has a fixed-width type (`DECIMAL`, `DOUBLE`, `DATE`, `TIMESTAMP`, or `BOOLEAN`), or is a text column that a top-level WHERE conjunct limits to string literals (`col = 'x'` or `col IN ('x', 'y')`). Every text column the adapter declares has the same declared length, so the type alone cannot tell a short code from a long description.
* A join leg is one occurrence of a table in the FROM tree, and `pushdown-joins/pushdown-planning-join-fallback-self-join` states which leg a `column` node reads. Exasol binds the pushdown result to the select list by position, so the column names the pushdown SQL emits stay inside it.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Broadcast join projection and filter are rendered per involved table

* *GIVEN* a `pushdown` request over an inner join with a broadcast pair
* *WHEN* the adapter resolves the projection and renders the WHERE filter
* *THEN* the adapter SHALL resolve each projected column's Exasol output type from the leg it belongs to, matching the column against that leg's involved-table column metadata
* *AND* the scan-driving SQL's declared EMITS column list SHALL match the projected join output columns in order and type
* *AND* each top-level WHERE conjunct whose legs all lie in the pair and that the scan renders against each column's own leg type (`pushdown-types/pushdown-planning-scan-text-conversion`) SHALL be carried in the common spec
* *AND* a conjunct local to a leg outside the pair SHALL be that leg's fan-out filter, as `pushdown-joins/pushdown-planning-join-fallback` states
* *AND* every conjunct that reads legs of more than one fan-out, or that the scan declines, SHALL be applied once in Exasol's `WHERE` over the fan-outs, as `pushdown/pushdown-declined-filter-self-apply` states for a declined conjunct, and the adapter MUST NOT emit a plan that omits a conjunct
* *AND* an ABSENT filter SHALL emit no scan-spec filter and no Exasol `WHERE`
* *AND* every column reference in the scan spec's filter and projection SHALL be qualified with its leg's relation alias `LHS_T{leg}`, the alias the join block carries for that relation, never with Exasol's `tableAlias`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A two-leg join with filters in its condition is planned as a broadcast fan-out

* *GIVEN* a two-leg inner join `pushdown` request whose condition holds its join keys and filters: a conjunct on either leg alone, a comparison across the legs such as `o.x > c.y`, or an `OR` across the legs, with or without a top-level WHERE filter
* *AND* its pair is eligible once those filters are WHERE conjuncts
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out and MUST NOT route the request to the unified unaccelerated fallback
* *AND* the common spec's filter SHALL carry each moved conjunct, rendered as a WHERE conjunct is
* *AND* a dimension text column that a moved conjunct limits to literals, such as `GROUP BY c.name` with `ON … AND c.name = 'v'`, SHALL count as width-bounded
* *AND* a moved conjunct on one occurrence of a self-join SHALL prune only that occurrence's files
* *AND* a moved conjunct that the scan translator declines SHALL be applied in Exasol's `WHERE` over the broadcast fan-out, as a declined WHERE conjunct is
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The broadcast pair is the eligible pair whose dimension has a filter of its own

* *GIVEN* a `pushdown` request over an inner-join tree with two or more eligible pairs, such as `lineitem` joined to `part` filtered by `p.p_brand = 'Brand#23'` and to `supplier` with no filter
* *WHEN* the adapter plans the request
* *THEN* the adapter SHALL broadcast the pair that ranks first by, in order: its dimension has a WHERE or `ON` conjunct of its own that the scan renders; its fact leg has more bytes; its dimension has fewer bytes; its legs come first in FROM-tree order
* *AND* in the example the adapter SHALL broadcast `part` into the shards of `lineitem`, and scan `supplier` on its own
* *AND* a pair whose dimension has no filter of its own SHALL still be broadcast when no eligible pair ranks above it
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A pair that is not eligible is not broadcast

* *GIVEN* a `pushdown` request over an inner-join tree of three or more legs
* *AND* a pair whose dimension exceeds `JOIN_BROADCAST_MAX_BYTES`, or whose fan-out would emit a dimension text column that is not width-bounded, such as `SELECT d.DESCRIPTION, …` with no literal condition on it
* *WHEN* the adapter plans the request
* *THEN* the adapter MUST NOT broadcast that pair, whether or not the request aggregates, because Exasol stores the pair's rows before it joins the other legs
* *AND* when no pair is eligible, the adapter SHALL serve the request through the unified unaccelerated fallback, with one fan-out per leg and no join block (`pushdown-joins/pushdown-planning-join-fallback`)
* *AND* when the scan cannot render a pair's join keys or emitted columns, or an emitted output name is longer than Exasol's 128-character identifier limit, the adapter SHALL broadcast the next eligible pair in rank order, and every leg SHALL keep its own fan-out when no eligible pair renders
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:NEW -->

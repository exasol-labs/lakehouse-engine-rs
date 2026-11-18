# Feature: Pushdown Planning, Broadcast Inner Equi-Join

Extends pushdown planning (`pushdown/pushdown-planning`) with the broadcast inner equi-join. When the smaller side of a two-leg join is below the broadcast threshold, the adapter shards the larger (fact) side across the scan fan-out and replicates the smaller (dimension) side into the shard-invariant common spec, and a node-local DataFusion join runs inside the scan UDF (`scan-aggregation/scan-execution-join`). The two legs are two tables or one table joined to itself, and they may share column names. Every other join is served by the unified unaccelerated fallback (`pushdown-joins/pushdown-planning-join-fallback`).

<!-- DELTA:CHANGED -->
## Background

* The broadcast contract: exactly two join legs, `join_type = "inner"`, a condition of one or more join keys once its filters have moved to WHERE (`pushdown-joins/pushdown-planning-join-condition-filters`), and a condition, filter, and projection that `crates/vs-expression` can render. A request that requires Exasol postprocessing also needs every dimension column its aggregation reads to be width-bounded. Any deviation takes the fallback.
* A width-bounded column has a fixed-width type (`DECIMAL`, `DOUBLE`, `DATE`, `TIMESTAMP`, or `BOOLEAN`), or is a text column that a top-level WHERE conjunct limits to string literals (`col = 'x'` or `col IN ('x', 'y')`). Every text column the adapter declares has the same declared length, so the type alone cannot tell a short code from a long description.
* Exasol postprocessing means a non-null `aggregationType`, `group_by` or `single_group`. Exasol sets it on every request that aggregates, groups, or carries a HAVING, including one whose aggregates it stripped from the select list. A pushed `LIMIT` and a pushed `ORDER BY` do not force it.
* A join leg is one occurrence of a table in the FROM tree. `pushdown-joins/pushdown-planning-join-fallback-self-join` owns the one binding that resolves a `column` node to its leg.
* Exasol binds the pushdown result to the select list by position, so the column names the pushdown SQL emits stay inside it.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A two-leg join on a composite key is planned as a broadcast fan-out

* *GIVEN* a two-leg inner join `pushdown` request whose condition is two or more join keys, such as `ON o.k1 = c.k1 AND o.k2 = c.k2`, whose smaller side is at or below the broadcast threshold, and that meets the rest of the broadcast contract
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out and MUST NOT route the request to the unified unaccelerated fallback
* *AND* the join block SHALL carry the condition rendered as the conjunction of every join key, each `column` node qualified with its leg's relation alias
* *AND* the order of the keys, and the side of `=` each leg is written on (`c.k1 = o.k1`), SHALL NOT change the plan's eligibility
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query, for a key of three columns, for a dimension side that holds duplicate composite keys, for a self-join on a key that includes a `DATE` column, and with an aggregate, a `GROUP BY`, or an `ORDER BY … LIMIT`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A join key that compares expressions is planned as a broadcast fan-out

* *GIVEN* a two-leg inner join `pushdown` request whose condition holds a join key with an expression side, such as `ON o.k = c.k + 1`, alone or beside a filter, and that meets the rest of the broadcast contract
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out and MUST NOT route the request to the unified unaccelerated fallback
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A two-leg join with filters in its condition is planned as a broadcast fan-out

* *GIVEN* a two-leg inner join `pushdown` request whose condition holds its join keys and filters: a conjunct on either leg alone, a comparison across the legs such as `o.x > c.y`, or an `OR` across the legs, with or without a top-level WHERE filter
* *AND* the request meets the rest of the broadcast contract once those filters are WHERE conjuncts
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL emit the broadcast fan-out and MUST NOT route the request to the unified unaccelerated fallback
* *AND* the common spec's filter SHALL carry each moved conjunct, rendered as a WHERE conjunct is
* *AND* a dimension text column that a moved conjunct limits to literals, such as `GROUP BY c.name` with `ON … AND c.name = 'v'`, SHALL count as width-bounded
* *AND* a moved conjunct on one occurrence of a self-join SHALL prune only that occurrence's files
* *AND* a moved conjunct that the scan translator declines SHALL take the unified unaccelerated fallback, as a declined WHERE filter does
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->

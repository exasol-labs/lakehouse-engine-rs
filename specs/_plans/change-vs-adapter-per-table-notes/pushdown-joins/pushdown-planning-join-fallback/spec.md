# Feature: Pushdown Planning, Unified Unaccelerated Join Fallback

One renderer serves every inner equi-join outside the two-table broadcast contract. It scans each involved table through its own sharded fan-out subquery (a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor over an ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF) and rebuilds the join in Exasol's core engine as a left-to-right `INNER JOIN ... ON` chain, with leg-local `WHERE` conjuncts pushed into each leg.

## Background

* One implementation serves all N >= 2 legs. Broadcast (`pushdown-joins/pushdown-planning-join`) is an optimization selected within the join path, not a second renderer.
* A leg is one occurrence of a table in the FROM tree. Column-to-leg attribution is owned by `pushdown-joins/pushdown-planning-join-fallback-self-join`.
* Legs are numbered in the FROM tree's left-to-right traversal order, and subquery aliases follow that order.
* The adapter advertises `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI` with no per-query opt-out, and Exasol never re-plans on an adapter error. Every advertised join shape must therefore render by broadcast or by this fallback. A hard client-facing error (`F-UDF-CL-RUST-9001`) is raised only for a shape the adapter cannot render at all.
* The wrapper has four entry points: an inner equi-join of N >= 2 legs, a grouped request that did not decompose into the partial/merge shape (`GroupByWrapper`), a single-group request with more than one `COUNT(DISTINCT)` or a distinct mixed with an ordinary aggregate (`pushdown-aggregates/pushdown-planning-count-distinct`), and a row scan whose derived projection was widened. All four share one `ORDER BY` and window rendering site.
* The wrapper resolves its own `LIMIT` and `OFFSET` and renders them on its outer SELECT over limit-free, sort-free fan-out legs. A row limit that leaked into a leg would cap that leg's pre-join input and return wrong rows. A non-zero `limit.offset` never arrives without a non-empty `orderBy` (`pushdown-capabilities/pushdown-planning-order-by-capability`).
* The broadcast path declines, and this wrapper serves: a non-zero offset without `orderBy`, a non-column sort key, a sort key missing its direction or NULL-placement flag, a sort key absent from the broadcast projection, and the four Exasol-postprocessing conditions.
* A top-level WHERE conjunct reaches a leg only if it is local to that leg, accepted by the type-rewrite pipeline against that leg's own column metadata, and renderable for DataFusion in its rewritten form. Every other conjunct is residual and lands in the outer wrapper's `WHERE`. The leg and residual sets are exact complements (`pushdown/pushdown-declined-filter-self-apply`).
* The outer `WHERE` render has three outcomes. An absent residual set emits no clause. A residual set that renders trivially true emits no clause and is not an error. A residual set the non-suppressing qualified render cannot express returns the wrapper's client-facing error.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A join outside the broadcast contract is declined safely

* *GIVEN* a `pushdown` request whose `from` clause is a join
* *WHEN* the join is not a single broadcast-eligible two-table inner equi-join, it is above threshold, non-equi, spans more than two involved tables, has overlapping column names across tables, needs Exasol postprocessing, or carries a condition/filter/projection detail that keeps it off the broadcast path
* *THEN* the adapter SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL instead render the request through the SINGLE unified unaccelerated per-table-scan fallback (N ≥ 2 involved tables, the two-table case being N = 2), so Exasol's core engine produces the correct result
* *AND* spanning more than two involved tables, or having overlapping column names, or needing Exasol postprocessing SHALL by itself NEVER be a reason to return an error, every such inner join is served by the unified fallback
* *AND* the adapter SHALL return a HARD error, a client-facing `F-UDF-CL-RUST-9001`, NOT a request that Exasol retries natively (Exasol does not re-plan on an adapter error), ONLY when it genuinely cannot render what it advertised: a non-inner join node in the tree, an involved table carrying no readable identifier note or no column metadata, or a condition/clause the translator cannot render at all
* *AND* the adapter MUST NOT emit any scan spec that would compute a different result than single-node evaluation
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Each join leg resolves its catalog identifier from its own identifier note

* *GIVEN* a join `pushdown` request whose `involvedTables` carries `CUSTOMER` and `ORDERS`, whose identifier notes name `lh.customer` and `lh.orders`
* *WHEN* the adapter plans the join
* *THEN* each leg SHALL take its catalog identifier from the identifier note of the `involvedTables` entry whose `name` equals the leg's table name
* *AND* every leg of a self-join, for which Exasol lists the table once per leg, SHALL resolve to the same catalog identifier
* *AND* a leg without a readable identifier note SHALL fail the request with the error of `file-planning/pushdown-planning-file-resolution`, and the adapter MUST NOT plan the join as a different shape
<!-- /DELTA:NEW -->

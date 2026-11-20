<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning, Unified Unaccelerated Join Fallback

A join with no eligible broadcast pair scans each involved table through its own sharded fan-out subquery (a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor over an ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF) and rebuilds the join in Exasol's core engine as a left-to-right `INNER JOIN ... ON` chain, with leg-local `WHERE` conjuncts pushed into each leg. A join with a broadcast pair (`pushdown-joins/pushdown-planning-join`) replaces the pair's two fan-outs with one and renders its other legs, its Exasol join chain, and its Exasol `WHERE` the same way.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* The N-scan is the join plan in which no leg is broadcast. It serves all N >= 2 legs, and a join with a broadcast pair differs from it only in the pair's fan-out.
* A leg is one occurrence of a table in the FROM tree, and column-to-leg attribution is owned by `pushdown-joins/pushdown-planning-join-fallback-self-join`. Legs are numbered in the FROM tree's left-to-right traversal order, and subquery aliases follow that order.
* The adapter advertises `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI` with no per-query opt-out, and Exasol never re-plans on an adapter error. Every advertised join shape must therefore render by broadcast or by this fallback. Exasol pushes only those inner equi-joins, and the Exasol dialect renders every condition and clause Exasol sends, so the fallback has no join shape to refuse.
* The wrapper has four entry points: an inner equi-join of N >= 2 legs, a grouped request that did not decompose into the partial/merge shape, a single-group request with more than one `COUNT(DISTINCT)` or a distinct mixed with an ordinary aggregate (`pushdown-aggregates/pushdown-planning-count-distinct`), and a row scan whose derived projection was widened. All four render `ORDER BY`, `LIMIT`, and `OFFSET` alike.
* A top-level WHERE conjunct reaches a fan-out only if every leg it reads lies in that fan-out and the scan renders it against those legs' own column types (`pushdown-types/pushdown-planning-scan-text-conversion`). Every other conjunct is residual and lands in the outer wrapper's `WHERE`, and the leg and residual sets are exact complements (`pushdown/pushdown-declined-filter-self-apply`). An absent residual set emits no `WHERE`, and any other renders through the qualified Exasol-dialect render as it is.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A join outside the broadcast contract is declined safely

* *GIVEN* a `pushdown` request whose `from` clause is a join
* *WHEN* no pair of its legs is broadcast, because every pair's dimension is above threshold or reads a dimension column that is not width-bounded where the width rule applies, or the scan cannot render any eligible pair's join keys or emitted columns
* *THEN* the adapter SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL instead emit one fan-out per leg, joined by Exasol's `INNER JOIN … ON` chain (N ≥ 2 legs, the two-leg case being N = 2), so Exasol's core engine produces the correct result
* *AND* a conjunct of the join condition beside its join equalities SHALL keep the request off the broadcast path only where the same conjunct written in WHERE would, because the adapter plans it as a WHERE conjunct (`pushdown-joins/pushdown-planning-join-condition-filters`)
* *AND* spanning more than two legs SHALL by itself NEVER be a reason to return an error or to withhold the broadcast
* *AND* the adapter SHALL return a HARD error, a client-facing `F-UDF-CL-RUST-9001`, NOT a request that Exasol retries natively (Exasol does not re-plan on an adapter error), ONLY when an involved table carries no readable identifier note
* *AND* the adapter MUST NOT emit any scan spec that would compute a different result than single-node evaluation
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper

* *GIVEN* a `pushdown` request whose `from` clause is a nested inner-join tree over three or more leaves, every join node of which is inner
* *AND* no pair of its legs is eligible for broadcast (`pushdown-joins/pushdown-planning-join`)
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT return an error and SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL emit the same SQL shape as for the two-leg (N = 2) case, differing only in the number of legs
* *AND* the adapter SHALL resolve each leg's Iceberg snapshot, data-file list, and logical schema exactly once, recovering each leg's original-cased Iceberg identifier from the schema-metadata mapping by its involved-table name, and SHALL treat an involved table absent from the mapping as the same stale-virtual-schema hard error the single-table path reports
* *AND* the adapter SHALL emit SQL that scans EACH leg independently through its own nested-distributor + scalar-scan fan-out subquery and reconstructs the original inner join over all N subquery results with a left-to-right `INNER JOIN … ON` chain in Exasol's core engine, each join condition attached to the `ON` of the join point at which every LEG it references is in scope
* *AND* every join condition, WHERE filter, select-list item, GROUP BY, HAVING, and ORDER BY the wrapper renders SHALL use table-qualified column references resolved to each `column` node's own LEG, so the wrapper is correct whether or not any two legs share a column name AND whether or not any two legs are the same table
* *AND* the returned result SHALL equal, as an order-independent multiset, the result of the same inner join evaluated on a single node
* *AND* the adapter MUST NOT read any leg's Parquet row data, and only file-level metadata crosses into each leg's scan spec
<!-- /DELTA:CHANGED -->

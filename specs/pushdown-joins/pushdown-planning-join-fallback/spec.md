# Feature: Pushdown Planning, Unified Unaccelerated Join Fallback

One renderer serves every inner equi-join outside the two-leg broadcast contract. It scans each involved table through its own sharded fan-out subquery (a nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor over an ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF) and rebuilds the join in Exasol's core engine as a left-to-right `INNER JOIN ... ON` chain, with leg-local `WHERE` conjuncts pushed into each leg.

## Background

* One implementation serves all N >= 2 legs. Broadcast (`pushdown-joins/pushdown-planning-join`) is an optimization selected within the join path, not a second renderer.
* A leg is one occurrence of a table in the FROM tree. Column-to-leg attribution is owned by `pushdown-joins/pushdown-planning-join-fallback-self-join`.
* Legs are numbered in the FROM tree's left-to-right traversal order, and subquery aliases follow that order.
* The adapter advertises `JOIN`, `JOIN_TYPE_INNER`, and `JOIN_CONDITION_EQUI` with no per-query opt-out, and Exasol never re-plans on an adapter error. Every advertised join shape must therefore render by broadcast or by this fallback. Exasol pushes only those inner equi-joins, and the Exasol dialect renders every condition and clause Exasol sends, so the fallback has no join shape to refuse.
* The wrapper has four entry points: an inner equi-join of N >= 2 legs, a grouped request that did not decompose into the partial/merge shape (`GroupByWrapper`), a single-group request with more than one `COUNT(DISTINCT)` or a distinct mixed with an ordinary aggregate (`pushdown-aggregates/pushdown-planning-count-distinct`), and a row scan whose derived projection was widened. All four share one `ORDER BY` and window rendering site.
* A top-level WHERE conjunct reaches a leg only if it is local to that leg and the scan translator renders it against that leg's own column metadata (`pushdown-types/pushdown-planning-scan-text-conversion`). Every other conjunct is residual and lands in the outer wrapper's `WHERE`. The leg and residual sets are exact complements (`pushdown/pushdown-declined-filter-self-apply`).
* The outer `WHERE` render has two outcomes. An absent residual set emits no clause. Any other residual set renders through the qualified Exasol-dialect render as it is.

## Scenarios

### Scenario: Join above the broadcast threshold falls back to the unified unaccelerated wrapper

* *GIVEN* an inner equi-join `pushdown` request over two involved tables whose smaller side exceeds the broadcast threshold
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit SQL through the SINGLE unified N-scan fallback renderer with N = 2, scanning each table independently through its own nested-distributor + scalar-scan fan-out subquery and reconstructing the inner join over both subquery results with an `INNER JOIN … ON` chain in Exasol's core engine
* *AND* the two-involved-table case SHALL use the identical fallback code path as the three-or-more-table case, differing only in the number of scanned sides
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node
* *AND* the adapter MUST NOT push either side's rows through a broadcast replication for this shape

### Scenario: A join outside the broadcast contract is declined safely

* *GIVEN* a `pushdown` request whose `from` clause is a join
* *WHEN* the join is not a single broadcast-eligible two-leg inner equi-join, it is above threshold, spans more than two legs, or carries a condition/filter/projection detail that keeps it off the broadcast path
* *THEN* the adapter SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL instead render the request through the SINGLE unified unaccelerated per-table-scan fallback (N ≥ 2 legs, the two-leg case being N = 2), so Exasol's core engine produces the correct result
* *AND* a conjunct of the join condition beside its join equalities SHALL keep the request off the broadcast path only where the same conjunct written in WHERE would, because the adapter plans it as a WHERE conjunct (`pushdown-joins/pushdown-planning-join-condition-filters`)
* *AND* spanning more than two legs SHALL by itself NEVER be a reason to return an error, every such inner join is served by the unified fallback
* *AND* the adapter SHALL return a HARD error, a client-facing `F-UDF-CL-RUST-9001`, NOT a request that Exasol retries natively (Exasol does not re-plan on an adapter error), ONLY when an involved table carries no readable identifier note
* *AND* the adapter MUST NOT emit any scan spec that would compute a different result than single-node evaluation

### Scenario: A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper

* *GIVEN* a `pushdown` request whose `from` clause is a nested inner-join tree over three or more leaves, every join node of which is inner
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT return an error and SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL serve the request through the SAME single unified fallback renderer used for the two-leg (N = 2) case, differing only in the number of legs
* *AND* the adapter SHALL resolve each leg's Iceberg snapshot, data-file list, and logical schema exactly once, recovering each leg's original-cased Iceberg identifier from the schema-metadata mapping by its involved-table name, and SHALL treat an involved table absent from the mapping as the same stale-virtual-schema hard error the single-table path reports
* *AND* the adapter SHALL emit SQL that scans EACH leg independently through its own nested-distributor + scalar-scan fan-out subquery and reconstructs the original inner join over all N subquery results with a left-to-right `INNER JOIN … ON` chain in Exasol's core engine, each join condition attached to the `ON` of the join point at which every LEG it references is in scope
* *AND* every join condition, WHERE filter, select-list item, GROUP BY, HAVING, and ORDER BY the wrapper renders SHALL use table-qualified column references resolved to each `column` node's own LEG, so the wrapper is correct whether or not any two legs share a column name AND whether or not any two legs are the same table
* *AND* the returned result SHALL equal, as an order-independent multiset, the result of the same inner join evaluated on a single node
* *AND* the adapter MUST NOT read any leg's Parquet row data in the planning layer, only file-level metadata crosses into each leg's scan spec

### Scenario: Join conditions attach greedily by LEG set and leg-local filters push into each leg

* *GIVEN* a unified unaccelerated fallback over N ≥ 2 legs with a set of join conditions and a WHERE filter
* *WHEN* the adapter renders the `INNER JOIN … ON` chain
* *THEN* the adapter SHALL attach each join condition to the earliest join point in the left-to-right chain at which every LEG the condition references is in scope, deciding scope by the SET of LEG INDEXES the condition touches, NEVER by column name, so shared column names across legs stay correctly qualified
* *AND* a join point at which no not-yet-attached condition becomes resolvable SHALL be rendered with `ON 1=1`
* *AND* a top-level WHERE conjunct SHALL be pushed INTO a leg's fan-out as a DataFusion filter if and only if it references only that ONE LEG AND the scan translator renders it against THAT LEG's own column metadata
* *AND* the screen SHALL run PER LEG and PER CONJUNCT, AFTER leg-local attribution, and MUST NOT be folded into the pre-attribution syntactic screen, because two N-scan legs MAY declare the same column name with different Exasol types and only the owning leg's metadata resolves it correctly
* *AND* every OTHER conjunct SHALL be RESIDUAL and remain in the outer wrapper's `WHERE`: cross-leg, OR-spanning, column-free, leg-local-but-unrenderable, or leg-local-but-declined
* *AND* each leg's fan-out SHALL emit only the columns that a join condition, a residual conjunct, or another clause of the request names, so a column that only a conjunct pushed into that leg names is not emitted
* *AND* the partition SHALL stay total and disjoint under the added condition, so no conjunct is dropped and none is applied twice
* *AND* a decline SHALL cost only the offending CONJUNCT's leg pushdown, so the same leg's other leg-local conjuncts SHALL still be pushed into its fan-out
* *AND* each leg-local conjunct SHALL be rendered exactly once, against its leg's column types, and the leg's filter SHALL be the conjunction of the accepted conjuncts' rendered SQL, so no second render can decline it
* *AND* each leg's Iceberg manifest-pruning predicate SHALL keep every conjunct attributed to THAT LEG in its RAW form, declined or not, so no decline SHALL change which files are opened, and a conjunct attributed to a DIFFERENT leg of the same table MUST NOT reach it
* *AND* the residual conjunct the outer wrapper renders SHALL be the RAW conjunct in the Exasol dialect, so Exasol applies its own conversions with the session's settings
* *AND* a residual set that is a lone `TRUE` conjunct SHALL render as the outer `WHERE (TRUE)` and SHALL NOT error, because Exasol keeps the `TRUE` that a folded `'' IS NULL` leaves in a conjunction
* *AND* an N-scan request in which no table occurs twice and whose filter has no declined conjunct SHALL emit the SQL that the golden-SQL fixtures pin for that shape
* *AND* the returned result SHALL equal the result of the same inner join evaluated on a single node, for any assignment of conditions to join points

### Scenario: Generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request through this feature's path
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the credential-reference guarantee of `storage-access/scan-spec-credential-reference` SHALL apply, so no credential value appears in generated SQL or error messages

### Scenario: Each join leg resolves its catalog identifier from its own identifier note

* *GIVEN* a join `pushdown` request whose `involvedTables` carries `CUSTOMER` and `ORDERS`, whose identifier notes name `lh.customer` and `lh.orders`
* *WHEN* the adapter plans the join
* *THEN* each leg SHALL take its catalog identifier from the identifier note of the `involvedTables` entry whose `name` equals the leg's table name
* *AND* every leg of a self-join, for which Exasol lists the table once per leg, SHALL resolve to the same catalog identifier
* *AND* a leg without a readable identifier note SHALL fail the request with the error of `file-planning/pushdown-planning-file-resolution`, and the adapter MUST NOT plan the join as a different shape

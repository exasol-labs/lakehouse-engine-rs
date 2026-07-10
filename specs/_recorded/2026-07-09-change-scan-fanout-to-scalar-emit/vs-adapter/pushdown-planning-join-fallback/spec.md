# Feature: Pushdown Planning — Unified Unaccelerated Join Fallback

Extends pushdown planning with the SINGLE unified renderer that serves every inner
equi-join outside the two-table broadcast contract. Each involved table is scanned
independently through its own sharded fan-out subquery — nested `LAKEHOUSE_DISTRIBUTE_FILES`
distributor over an ungrouped `LAKEHOUSE_SCAN` SCALAR EMIT UDF — and all N legs are
reconstructed into the original inner join by Exasol's core engine. The FROM clause is
rendered as a left-to-right `INNER JOIN … ON` chain (not a comma cross-join with one flat
`WHERE`): each join condition attaches to the `ON` of the join point at which every table
it references is in scope, and each side's side-local `WHERE` conjuncts are pushed into
that side's fan-out leg so DataFusion prunes and filters per leg. The unaccelerated
fallback has exactly one implementation for all N ≥ 2 involved tables.

## Background

* Every clause the wrapper renders (join conditions, WHERE, select list, GROUP BY, HAVING, ORDER BY) uses table-qualified column references resolved from each `column` node's `tableName`, so the wrapper is correct whether or not two involved tables share a column name.
* The adapter reads only file-level metadata in the planning layer — never a table's Parquet row data.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Join above the broadcast threshold falls back to the unified unaccelerated wrapper

* *GIVEN* an inner equi-join `pushdown` request over two involved tables whose smaller side exceeds the broadcast threshold
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit SQL through the SINGLE unified N-scan fallback renderer with N = 2 — scanning each table independently through its own nested-distributor + scalar-scan fan-out subquery and reconstructing the inner join over both subquery results with an `INNER JOIN … ON` chain in Exasol's core engine
* *AND* the two-involved-table case SHALL use the identical fallback code path as the three-or-more-table case, differing only in the number of scanned sides
* *AND* the returned result SHALL equal the result of the same inner equi-join evaluated on a single node
* *AND* the adapter MUST NOT push either side's rows through a broadcast replication for this shape
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper

* *GIVEN* a `pushdown` request whose `from` clause is a nested inner-join tree over three or more involved tables, every join node of which is inner
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL NOT return an error and SHALL NOT emit a broadcast plan for that request
* *AND* the adapter SHALL serve the request through the SAME single unified fallback renderer used for the two-table (N = 2) case, differing only in the number of involved tables
* *AND* the adapter SHALL resolve each involved table's Iceberg snapshot, data-file list, and logical schema exactly once — recovering each table's original-cased Iceberg identifier from the schema-metadata mapping by its involved-table name — and SHALL treat an involved table absent from the mapping as the same stale-virtual-schema hard error the single-table path reports
* *AND* the adapter SHALL emit SQL that scans EACH involved table independently through its own nested-distributor + scalar-scan fan-out subquery and reconstructs the original inner join over all N subquery results with a left-to-right `INNER JOIN … ON` chain in Exasol's core engine, each join condition attached to the `ON` of the join point at which every table it references is in scope
* *AND* every join condition, WHERE filter, select-list item, GROUP BY, HAVING, and ORDER BY the wrapper renders SHALL use table-qualified column references resolved from each `column` node's `tableName` against the involved table that owns it, so the wrapper is correct whether or not any two involved tables share a column name
* *AND* the returned result SHALL equal — as an order-independent multiset — the result of the same inner join evaluated on a single node
* *AND* the adapter MUST NOT read any involved table's Parquet row data in the planning layer — only file-level metadata crosses into each side's scan spec
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Join conditions attach greedily by table-name set and side-local filters push into each leg

* *GIVEN* a unified unaccelerated fallback over N ≥ 2 involved tables with a set of join conditions and a WHERE filter
* *WHEN* the adapter renders the `INNER JOIN … ON` chain
* *THEN* the adapter SHALL attach each join condition to the earliest join point in the left-to-right chain at which every table the condition references is in scope, deciding scope by the SET of `tableName`s the condition touches — NEVER by column name, so shared column names across sides stay correctly qualified
* *AND* a join point at which no not-yet-attached condition becomes resolvable SHALL be rendered with `ON 1=1`
* *AND* each side's SIDE-LOCAL WHERE conjuncts (referencing only that one table) SHALL be pushed INTO that side's fan-out leg as a DataFusion filter, so DataFusion performs row-group pruning and row filtering per leg
* *AND* only the RESIDUAL WHERE conjuncts — cross-table, OR-spanning, or untagged — SHALL remain in the outer wrapper's `WHERE`
* *AND* the returned result SHALL equal the result of the same inner join evaluated on a single node, for any assignment of conditions to join points
<!-- /DELTA:NEW -->

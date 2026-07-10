# Feature: Pushdown Planning — Broadcast Inner Equi-Join

Extends pushdown planning with the broadcast inner equi-join shape. When Exasol pushes a
two-table join whose smaller side is below the broadcast threshold, the adapter resolves
both sides' file lists once, shards only the larger (fact) side through the nested
distributor + scalar scan fan-out, replicates the smaller (dimension) side's full file
list into the shard-invariant common spec, and drives a node-local DataFusion join inside
the scalar scan UDF. Every join outside this broadcast contract is served by the unified
unaccelerated fallback renderer.

## Background

* The dimension side rides once in the shard-invariant common spec (full file list, table root, logical schema, join condition); only the fact side's per-shard file subset flows through the distributor, so every shard joins its fact subset against the same replicated dimension side node-locally.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Broadcast-eligible inner equi-join is planned as a broadcast fan-out

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a `pushdown` request whose `from` clause is a `join` node over exactly two involved tables joined by an equi-condition
* *AND* the smaller side's Iceberg-metadata byte size is at or below the broadcast threshold
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve BOTH tables' Iceberg snapshot, data-file list, per-file byte size, and logical schema exactly once, recovering each table's original-cased Iceberg identifier from the schema-metadata mapping by its involved-table name
* *AND* the adapter SHALL designate the larger side as the sharded fact side (its file list partitioned into G byte-balanced work-unit shards and driven through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor exactly as the single-table path does) and the smaller side as the replicated dimension side
* *AND* the adapter SHALL carry the dimension side's FULL file list, table root, and logical schema in the shard-invariant common spec (spliced once as the `LAKEHOUSE_SCAN` scalar UDF's first argument), and the fact side's per-shard file subset flowed through the distributor as the second argument
* *AND* the generated scan-driving SQL SHALL drive the `LAKEHOUSE_SCAN` SCALAR EMIT UDF so that each shard invocation joins its fact-file subset against the full replicated dimension side node-locally, with no cross-shard exchange and no `SELECT * FROM (...)` wrapper
* *AND* the adapter MUST NOT read either side's Parquet row data in the planning layer — only file-level metadata crosses into the scan spec
<!-- /DELTA:CHANGED -->

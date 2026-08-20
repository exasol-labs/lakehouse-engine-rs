# Feature: Pushdown Planning — Broadcast Inner Equi-Join

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the broadcast inner equi-join shape. When Exasol pushes a two-table join whose smaller side is below the broadcast threshold, the adapter resolves both sides' file lists once, shards only the larger (fact) side through the nested distributor + scalar scan fan-out, replicates the smaller (dimension) side's full file list into the shard-invariant common spec, and drives a node-local DataFusion join inside the scalar scan UDF (`datafusion-scan/scan-execution-join`). Every join outside this broadcast contract — above threshold, non-two-table, needing Exasol postprocessing, or otherwise ineligible — is served by the unified unaccelerated fallback renderer (`vs-adapter/pushdown-planning-join-fallback`), so a join is never wrong, only sometimes unaccelerated.

## Background

* **This delta corrects a behavioural mis-statement and is issue #324. Join planning is unchanged.** The recorded clauses stated that both sides' ICEBERG snapshot and ICEBERG-manifest byte size are what the broadcast decision reads. Join planning was never format-split: `resolve_one_join_side` calls the same `TableScanResolver::resolve` seam the single-table path calls (`vs-adapter/pushdown-format-neutral-resolution`), and a broadcast join over Delta tables reached through Unity Catalog is covered end to end by `e2e-harness/unity-catalog-e2e-harness-delta-queries`. The Iceberg-only phrasing described one caller of a path that has two.
* **There is no Delta-join sibling feature, and this delta does not create one.** Unlike `vs-adapter/pushdown-file-pruning` ↔ `vs-adapter/delta-file-pruning`, joins were never split per format, so the correction is a neutralization of this feature's own prose rather than a split.
* **What each side's byte size MEANS is the format reader's decision, not this feature's.** An Iceberg side's size is the manifest `file_size_in_bytes` sum for the resolved snapshot; a Delta side's is the `add` action `size` sum for the resolved version. Both arrive as the neutral per-file size on `FileEntry`, summed into the one quantity the threshold is compared against. What this feature owns is the comparison, not the source.
* **The no-Parquet-read guarantee is unchanged and is the load-bearing half.** Neither format reader opens a data file to size a side; both read the table's own metadata. That is what keeps broadcast eligibility a metadata-only decision.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Broadcast-eligible inner equi-join is planned as a broadcast fan-out

* *GIVEN* a virtual schema over a namespace whose tables are backed by MinIO
* *AND* a `pushdown` request whose `from` clause is a `join` node over exactly two involved tables joined by an equi-condition
* *AND* the smaller side's table-metadata byte size is at or below the broadcast threshold
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL resolve BOTH sides through the SAME format-reader seam the single-table scan uses (`vs-adapter/pushdown-format-neutral-resolution`) — obtaining each side's data-file list, per-file byte size, logical schema, table root, and effective storage exactly once — recovering each table's original-cased catalog identifier from the schema-metadata mapping by its involved-table name
* *AND* the adapter SHALL designate the larger side as the sharded fact side (its file list partitioned into G byte-balanced work-unit shards and driven through the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor exactly as the single-table path does) and the smaller side as the replicated dimension side
* *AND* the adapter SHALL carry the dimension side's FULL file list, table root, logical schema, and its OWN effective storage backend in the shard-invariant common spec's join block (spliced once as the `LAKEHOUSE_SCAN` scalar UDF's first argument), and the fact side's per-shard file subset flowed through the distributor as the second argument
* *AND* the whole-spec `storage` value SHALL be the FACT side's own effective storage, so each side of the emitted spec names the storage backend resolved for that side's own table location and neither side's backend is dropped
* *AND* the generated scan-driving SQL SHALL drive the `LAKEHOUSE_SCAN` SCALAR EMIT UDF so that each shard invocation joins its fact-file subset against the full replicated dimension side node-locally, with no cross-shard exchange, and with NO `SELECT * FROM (...)` wrapper for an UNORDERED request; an ORDERED request carries the outer wrapper this feature's ordering scenario specifies (issue #307)
* *AND* the adapter MUST NOT read either side's Parquet row data in the planning layer — only file-level metadata and per-side storage credentials cross into the scan spec
* *AND* the dimension side's backend SHALL be serialized ONCE inside the shard-invariant common blob and MUST NOT be repeated per shard, exactly as the fact side's already is
* *AND* a join whose two sides are DELTA tables reached through Unity Catalog SHALL take this same broadcast path with no Iceberg-specific step, because the resolution seam and the broadcast decision read only neutral resolved values
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: Small-side selection uses Iceberg metadata and the broadcast threshold

* *GIVEN* an inner equi-join `pushdown` request over two involved tables
* *WHEN* the adapter evaluates broadcast eligibility
* *THEN* this scenario SHALL be REMOVED, because its title and its sizing clause both name Iceberg manifest metadata as the source of the broadcast-threshold quantity, which stopped being the only source when the Delta reader shipped
* *AND* it SHALL be REPLACED by "Small-side selection uses table-format metadata and the broadcast threshold" below, which restates every invariant it carried — the no-Parquet-read guarantee, the smaller-side role assignment, both threshold arms, and the adapter-note default — against the neutral per-file size both format readers populate
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: Small-side selection uses table-format metadata and the broadcast threshold

* *GIVEN* an inner equi-join `pushdown` request over two involved tables
* *WHEN* the adapter evaluates broadcast eligibility
* *THEN* the adapter SHALL compute each side's byte size as the sum of that side's resolved per-file byte sizes, read from the table's own format metadata — an Iceberg manifest's `file_size_in_bytes` for the resolved snapshot, a Delta `add` action's `size` for the resolved version — without opening any Parquet file
* *AND* the adapter SHALL choose the side with the smaller metadata byte size as the broadcast (dimension) side and the other as the sharded (fact) side
* *AND* when the smaller side's byte size is at or below `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL plan the broadcast fan-out
* *AND* when the smaller side's byte size exceeds `JOIN_BROADCAST_MAX_BYTES` the adapter SHALL take the unified unaccelerated fallback instead
* *AND* the threshold SHALL be read from the persisted adapter note `JOIN_BROADCAST_MAX_BYTES`, defaulting to 134217728 when absent or unparseable
* *AND* the sum SHALL saturate, so a side whose byte total overflows `u64` is clamped to `u64::MAX` and is therefore never chosen as the broadcast side
<!-- /DELTA:NEW -->

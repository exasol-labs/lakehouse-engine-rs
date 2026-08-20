# Feature: Work-Unit File Sharding

Partitions the once-resolved data-file list into G oversubscribed work-units
("shards") and drives them across the cluster. Rather than sharding one-per-node, the
adapter sizes G to oversubscribe the cluster (G = node_count × parallelism_factor,
capped so the group set stays in Exasol's round-robin distribution regime) and emits a
fan-out. Cluster fan-out is separated from the scan itself: a tiny `LAKEHOUSE_DISTRIBUTE_FILES`
LUA SET script re-emits each shard's per-file list once per `shard_key` group so
`GROUP BY shard_key` distributes the assignments round-robin across nodes, and the
`LAKEHOUSE_SCAN` scalar EMIT UDF then scans each distributed file list node-locally
and STREAMS its rows. Because the scan is scalar (no top-level `GROUP BY`), Exasol does
not materialize the scan output. The shard-invariant common spec (including the table
root) is serialized ONCE as the scalar scan's first-argument literal; only each
shard's per-file subset flows through the distributor. Work assignment is computed
entirely in the planning layer; each scan invocation reads only its own shard of files
and no file is scanned twice.

## Background

* **This delta corrects format-scoped naming and is issue #324. Sharding behavior is unchanged.** Sharding never had, and never needed, a format branch: `shard_files_json` and `shard_count` are generic over any format's `FileEntry`, and a Delta table's shards are produced by the same code as an Iceberg table's. Naming the input "the Iceberg data-file list" and each file's size "from the Iceberg `FileScanTask`" described one caller of a function that has two.
* **Where a file's byte size comes from is the format reader's decision, not this feature's.** An Iceberg side reads it from the manifest's `file_size_in_bytes` (surfaced through `FileScanTask`); a Delta side reads it from the `add` action's `size`. Both arrive on the neutral `FileEntry` this feature partitions, which is the only thing the byte-balanced split needs to know.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: File list is partitioned into G byte-balanced disjoint shards covering every file

* *GIVEN* a resolved data-file list in which each file carries its byte size as resolved by its table's format reader and a computed shard count G
* *WHEN* the adapter partitions the file list
* *THEN* the adapter SHALL partition the file list into exactly G shards by cumulative file size, assigning each file in descending-size order to the shard whose running byte total is currently smallest
* *AND* the adapter SHALL treat any file whose reported byte size is 0 as weighing 1 byte, so the file is still assigned to a shard and never skipped
* *AND* every resolved file SHALL appear in exactly one shard and no file SHALL appear in more than one shard
* *AND* when G is at least the file count the adapter SHALL produce exactly one file per shard with no empty shards
* *AND* each shard SHALL carry, for every file it holds, both the file path and its byte size, so the resolved size is propagated into the per-shard payload rather than discarded
* *AND* the split SHALL be computed without reference to the table format that resolved the list, so an Iceberg file list and a Delta file list of the same shape produce the same shards
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Scan-driving query fans out via a nested distributor over a scalar scan UDF

* *GIVEN* a file list partitioned into more than one shard
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL nest a `GROUP BY shard_key` distributor subquery — a `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET UDF invoked over a `VALUES` relation of `(shard_key, files)` rows grouped on `shard_key` (NOT on `IPROC()`) — inside an outer ungrouped select that invokes the `LAKEHOUSE_SCAN` scalar EMIT UDF once per distributed row
* *AND* the shard-invariant common spec (including the table root) SHALL be serialized EXACTLY ONCE as a single SQL string literal spliced as the scalar scan UDF's first argument, shared by every shard invocation, rather than repeated per shard and rather than flowed through the distributor
* *AND* only each shard's per-file `files` subset SHALL flow through the distributor's `VALUES` rows as the scalar scan UDF's second argument, and the table root SHALL NOT appear in any per-shard argument
* *AND* the outer scalar select MUST NOT be wrapped in a `SELECT * FROM (...)` materialization boundary, so the scalar scan streams its rows rather than Exasol buffering them into a temp table
* *AND* the union of all shard outputs SHALL be identical as an order-independent multiset to the equivalent single-shard scan
<!-- /DELTA:CHANGED -->

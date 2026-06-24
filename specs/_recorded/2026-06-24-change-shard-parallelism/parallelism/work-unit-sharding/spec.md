# Feature: Work-Unit File Sharding

Partitions the once-resolved Iceberg data-file list into G oversubscribed work
units ("shards") and drives them as one scan query that Exasol distributes
across the cluster. Rather than sharding one-per-node, the adapter sizes G to
oversubscribe the cluster (`G = node_count × parallelism_factor`, capped so the
group set stays in Exasol's round-robin distribution regime) and emits a
`GROUP BY shard_key` fan-out. Exasol spreads the shard groups across nodes and
multiplexes them onto each node's fixed per-node VM pool (sized to
`NR_OF_CORES`), so a single node's cores are all exploited. Work assignment is
computed entirely in the planning layer; each scan UDF invocation reads only its
own shard of files and no file is scanned twice.

## Background

* The cluster node count is taken from the `CLUSTER_NODES` entry in the virtual
  schema's `adapterNotes` (captured once at `createVirtualSchema` via `NPROC()`),
  round-tripped to the adapter at pushdown time (default `1`).
* The shard count G is `node_count × parallelism_factor`, where
  `parallelism_factor` is a VS property. G is capped at `300` so it stays at or
  below Exasol's `max_dynamic_group_count` default — at or below that threshold
  Exasol distributes groups round-robin (balanced) across nodes; above it Exasol
  hash-partitions groups (no longer balanced). G is also clamped to `≥ 1` and
  `≤ file_count` so no shard is empty.
* Files are assigned to the G shards by a byte-balanced split
  (`partition_files_by_bytes`), called with G instead of node_count. Each file
  carries its `file_size_in_bytes` from the Iceberg `FileScanTask`; the split
  balances cumulative bytes per shard, not file count, so per-shard scan work is
  even. A file whose reported size is 0 is weighted as 1 byte so it is still
  assigned and never skipped.
* The fan-out groups on a per-shard key (`GROUP BY shard_key`), NOT on `IPROC()`.
  Each shard is its own group; Exasol assigns groups to nodes and runs each as a
  scan UDF invocation. Groups drive UDF invocations, not OS processes — actual
  concurrency on a node is bounded by that node's VM pool, and the engine
  multiplexes the shard groups onto it.
* File-to-shard assignment is computed once in the adapter; the scan UDF receives
  an explicit file list per invocation and never discovers files itself. No node
  scans another node's files.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: File list is partitioned into G byte-balanced disjoint shards covering every file

* *GIVEN* a resolved data-file list in which each file carries its `file_size_in_bytes` (from the Iceberg `FileScanTask`) and a computed shard count G
* *WHEN* the adapter partitions the file list
* *THEN* the adapter SHALL partition the file list into exactly G shards by cumulative file size, assigning each file in descending-size order to the shard whose running byte total is currently smallest
* *AND* the adapter SHALL treat any file whose reported `file_size_in_bytes` is 0 as weighing 1 byte, so the file is still assigned to a shard and never skipped
* *AND* every resolved file SHALL appear in exactly one shard and no file SHALL appear in more than one shard
* *AND* when G is at least the file count the adapter SHALL produce exactly one file per shard with no empty shards
<!-- /DELTA:CHANGED -->

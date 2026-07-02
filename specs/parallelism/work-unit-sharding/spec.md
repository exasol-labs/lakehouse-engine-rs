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
own shard of files and no file is scanned twice. The fan-out serializes the
shard-invariant common spec (including the Iceberg table root) once and carries
only each shard's per-file `(path, size)` subset per `VALUES` row.

## Background

* The cluster node count is taken from the `CLUSTER_NODES` entry in the virtual
  schema's `adapterNotes` (captured once at `createVirtualSchema` via
  `UdfContext::node_count()`), round-tripped to the adapter at pushdown time
  (default `1`).
* The shard count G is `node_count × parallelism_factor`, where
  `parallelism_factor` is a VS property. G is capped at `300` so it
  stays at or below Exasol's `max_dynamic_group_count` default — at or below that
  threshold Exasol distributes groups round-robin (balanced) across nodes; above
  it Exasol hash-partitions groups (no longer balanced). G is also clamped to
  `≥ 1` and `≤ file_count` so no shard is empty.
* Files are assigned to the G shards by a byte-balanced split
  (`partition_files_by_bytes`), called with G instead of node_count. Each file
  carries its `file_size_in_bytes` from the Iceberg `FileScanTask`; the split
  balances cumulative bytes per shard, not file count, so per-shard scan work is
  even. A file whose reported size is 0 is weighted as 1 byte so it is still
  assigned and never skipped.
* The byte-balanced split PROPAGATES each file's size through to the shard it
  lands in: a shard is a list of `(path, size)` entries, not bare paths, so the
  size the adapter already resolved travels into the per-shard payload rather
  than being dropped.
* The fan-out groups on a per-shard key (`GROUP BY shard_key`), NOT on `IPROC()`.
  Each shard is its own group; Exasol assigns groups to nodes and runs each as a
  scan UDF invocation. Groups drive UDF invocations, not OS processes — actual
  concurrency on a node is bounded by that node's VM pool, and the engine
  multiplexes the shard groups onto it.
* File-to-shard assignment is computed once in the adapter; the scan UDF receives
  an explicit file list per invocation and never discovers files itself. No node
  scans another node's files.
* The generated fan-out SQL serializes the shard-invariant common spec (including
  the Iceberg table root) exactly once as the scan SET UDF's first argument (a
  shared SELECT-list literal), and carries only each shard's `(path, size)`
  subset as the second (per-shard) argument in the `VALUES` rows. Credentials
  MUST NOT appear repeated per shard; they live once in the common spec literal.

## Scenarios

### Scenario: Shard count oversubscribes the cluster and is capped at the round-robin threshold

* *GIVEN* a resolved data-file list, a `CLUSTER_NODES` value, and a `PARALLELISM_FACTOR` VS property
* *WHEN* the adapter computes the shard count G for the scan-driving query
* *THEN* the adapter SHALL compute `G = CLUSTER_NODES × PARALLELISM_FACTOR`
* *AND* the adapter SHALL cap G at 300 so the resulting group set stays in Exasol's round-robin distribution regime
* *AND* the adapter SHALL clamp G to be at least 1 and at most the resolved file count

### Scenario: File list is partitioned into G byte-balanced disjoint shards covering every file

* *GIVEN* a resolved data-file list in which each file carries its `file_size_in_bytes` (from the Iceberg `FileScanTask`) and a computed shard count G
* *WHEN* the adapter partitions the file list
* *THEN* the adapter SHALL partition the file list into exactly G shards by cumulative file size, assigning each file in descending-size order to the shard whose running byte total is currently smallest
* *AND* the adapter SHALL treat any file whose reported `file_size_in_bytes` is 0 as weighing 1 byte, so the file is still assigned to a shard and never skipped
* *AND* every resolved file SHALL appear in exactly one shard and no file SHALL appear in more than one shard
* *AND* when G is at least the file count the adapter SHALL produce exactly one file per shard with no empty shards
* *AND* each shard SHALL carry, for every file it holds, both the file path and its byte size, so the resolved size is propagated into the per-shard payload rather than discarded

### Scenario: Fewer files than G produces one shard per file with no empty shards

* *GIVEN* a resolved file list whose length is smaller than the otherwise-computed G
* *WHEN* the adapter computes the shard count and partitions the files
* *THEN* the adapter SHALL clamp G down to the file count
* *AND* the adapter SHALL produce exactly one shard per file
* *AND* the adapter MUST NOT emit a scan invocation for an empty file shard

### Scenario: Scan-driving query fans the SET UDF across shards via GROUP BY shard_key

* *GIVEN* a file list partitioned into more than one shard
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL invoke the scan SET UDF once per shard, serializing the shard-invariant common spec (including the Iceberg table root) EXACTLY ONCE as a single SQL string literal in the SET UDF's first (SELECT-list) argument shared by every shard invocation, rather than repeating it per shard
* *AND* the SQL SHALL carry ONLY each shard's `(path, size)` subset as that invocation's second (per-shard) argument, placed in the `VALUES` rows, grouping the shard rows on a per-shard `shard_key` (NOT on `IPROC()`) so Exasol distributes shard groups across nodes and multiplexes them onto each node's core pool
* *AND* the table root SHALL NOT appear in any per-shard argument, appearing only once in the shared common spec literal
* *AND* the union of all shard outputs SHALL be identical in row content to the equivalent single-shard scan

### Scenario: Single node with G collapsing to one preserves the single-invocation query

* *GIVEN* a `CLUSTER_NODES` value of one and a configuration where G resolves to 1 (single file, or a parallelism factor of 1 on a single file)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit a single scan SET UDF invocation carrying the common spec argument and the whole file list as the per-shard argument
* *AND* the generated SQL MUST be behaviourally identical to the pre-sharding single-invocation execution path
* *AND* the common spec literal SHALL appear exactly once, and the file-list literal exactly once

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
  `parallelism_factor` is a VS property (default `8`). G is capped at `300` so it
  stays at or below Exasol's `max_dynamic_group_count` default — at or below that
  threshold Exasol distributes groups round-robin (balanced) across nodes; above
  it Exasol hash-partitions groups (no longer balanced). G is also clamped to
  `≥ 1` and `≤ file_count` so no shard is empty.
* Files are assigned to the G shards by the existing balanced split
  (`partition_files`), called with G instead of node_count.
* The fan-out groups on a per-shard key (`GROUP BY shard_key`), NOT on `IPROC()`.
  Each shard is its own group; Exasol assigns groups to nodes and runs each as a
  scan UDF invocation. Groups drive UDF invocations, not OS processes — actual
  concurrency on a node is bounded by that node's VM pool, and the engine
  multiplexes the shard groups onto it.
* File-to-shard assignment is computed once in the adapter; the scan UDF receives
  an explicit file list per invocation and never discovers files itself. No node
  scans another node's files.

## Scenarios

### Scenario: Shard count oversubscribes the cluster and is capped at the round-robin threshold

* *GIVEN* a resolved data-file list, a `CLUSTER_NODES` value, and a `PARALLELISM_FACTOR` VS property
* *WHEN* the adapter computes the shard count G for the scan-driving query
* *THEN* the adapter SHALL compute `G = CLUSTER_NODES × PARALLELISM_FACTOR`
* *AND* the adapter SHALL cap G at 300 so the resulting group set stays in Exasol's round-robin distribution regime
* *AND* the adapter SHALL clamp G to be at least 1 and at most the resolved file count

### Scenario: File list is partitioned into G balanced disjoint shards covering every file

* *GIVEN* a resolved data-file list and a computed shard count G
* *WHEN* the adapter partitions the file list
* *THEN* the adapter SHALL partition the file list into exactly G shards using the balanced split
* *AND* every resolved file SHALL appear in exactly one shard
* *AND* no file SHALL appear in more than one shard
* *AND* the file counts across shards SHALL differ by at most one

### Scenario: Fewer files than G produces one shard per file with no empty shards

* *GIVEN* a resolved file list whose length is smaller than the otherwise-computed G
* *WHEN* the adapter computes the shard count and partitions the files
* *THEN* the adapter SHALL clamp G down to the file count
* *AND* the adapter SHALL produce exactly one shard per file
* *AND* the adapter MUST NOT emit a scan invocation for an empty file shard

### Scenario: Scan-driving query fans the SET UDF across shards via GROUP BY shard_key

* *GIVEN* a file list partitioned into more than one shard
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL invoke the scan SET UDF once per shard
* *AND* the SQL SHALL carry each shard's file subset as that invocation's explicit scan-spec argument
* *AND* the SQL SHALL group the shard rows on a per-shard `shard_key` (NOT on `IPROC()`) so Exasol distributes shard groups across nodes and multiplexes them onto each node's core pool
* *AND* the union of all shard outputs SHALL be identical in row content to the equivalent single-shard scan

### Scenario: Single node with G collapsing to one preserves the single-invocation query

* *GIVEN* a `CLUSTER_NODES` value of one and a configuration where G resolves to 1 (single file, or a parallelism factor of 1 on a single file)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit a single scan SET UDF invocation over the whole file list
* *AND* the generated SQL MUST be behaviourally identical to the pre-sharding single-invocation execution path

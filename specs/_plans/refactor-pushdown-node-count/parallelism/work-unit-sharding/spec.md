# Feature: Work-Unit File Sharding

Partitions the once-resolved Iceberg data-file list into G oversubscribed work-units
("shards") and drives them across the cluster. Rather than sharding one-per-node, the
adapter sizes G to oversubscribe the cluster (G = node_count × parallelism_factor,
capped so the group set stays in Exasol's round-robin distribution regime) and emits a
fan-out. Cluster fan-out is separated from the scan itself: a tiny `LAKEHOUSE_DISTRIBUTE_FILES`
LUA SET script re-emits each shard's per-file list once per `shard_key` group so
`GROUP BY shard_key` distributes the assignments round-robin across nodes, and the
`LAKEHOUSE_SCAN` scalar EMIT UDF then scans each distributed file list node-locally
and STREAMS its rows. Because the scan is scalar (no top-level `GROUP BY`), Exasol does
not materialize the scan output. The shard-invariant common spec (including the Iceberg
table root) is serialized ONCE as the scalar scan's first-argument literal; only each
shard's per-file subset flows through the distributor. Work assignment is computed
entirely in the planning layer; each scan invocation reads only its own shard of files
and no file is scanned twice.

## Background

<!-- DELTA:CHANGED -->
* The cluster node count is read per pushdown from the running adapter script's own
  UDF handshake via `UdfContext::node_count()`, captured synchronously in `dispatch`
  before the tokio runtime is entered and threaded into the planning path. It is NOT
  taken from an `adapterNotes` entry, so no create-time node count is persisted or
  round-tripped. A `node_count()` of `0` (no live handshake) maps to `1`. See
  `vs-adapter/pushdown-planning`.
<!-- /DELTA:CHANGED -->
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
* Cluster fan-out is separated from the scan: a `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET
  distributor does the `GROUP BY shard_key` cross-node distribution (grouping on a
  per-shard key, NOT on `IPROC()`), and the `LAKEHOUSE_SCAN` SCALAR EMIT UDF scans
  each distributed file list node-locally and streams its rows, so scan output is
  not materialized into temp DB RAM. Groups drive distributor invocations, not OS
  processes — actual concurrency on a node is bounded by that node's VM pool, and
  the engine multiplexes the shard groups onto it.
* File-to-shard assignment is computed once in the adapter; the scan UDF receives
  an explicit file list per invocation and never discovers files itself. No node
  scans another node's files.
* The shard-invariant common spec is serialized once as the scalar scan's
  first-argument literal; only each shard's per-file subset flows through the
  distributor. Credentials MUST NOT appear repeated per shard; they live once in
  the common spec literal.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Shard count oversubscribes the cluster and is capped at the round-robin threshold

* *GIVEN* a resolved data-file list, a node count read from `UdfContext::node_count()` at pushdown, and a `PARALLELISM_FACTOR` VS property
* *WHEN* the adapter computes the shard count G for the scan-driving query
* *THEN* the adapter SHALL compute `G = node_count × PARALLELISM_FACTOR`
* *AND* the adapter SHALL cap G at 300 so the resulting group set stays in Exasol's round-robin distribution regime
* *AND* the adapter SHALL clamp G to be at least 1 and at most the resolved file count
<!-- /DELTA:CHANGED -->

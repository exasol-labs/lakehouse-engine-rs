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

* The cluster node count is read per pushdown from the running adapter script's own
  UDF handshake via `UdfContext::node_count()`, captured synchronously in `dispatch`
  before the tokio runtime is entered and threaded into the planning path. It is NOT
  taken from an `adapterNotes` entry, so no create-time node count is persisted or
  round-tripped. A `node_count()` of `0` (no live handshake) maps to `1`. See
  `vs-adapter/pushdown-planning`.
* The shard count G is `node_count × parallelism_factor`, where
  `parallelism_factor` is a VS property. G is capped at `300` so it
  stays at or below Exasol's `max_dynamic_group_count` default — at or below that
  threshold Exasol distributes groups round-robin (balanced) across nodes; above
  it Exasol hash-partitions groups (no longer balanced). G is also clamped to
  `≥ 1` and `≤ file_count` so no shard is empty.
* Files are assigned to the G shards by a byte-balanced split
  (`partition_files_by_bytes`), called with G instead of node_count. Each file
  carries the byte size the format reader resolved into its file entry; the split
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

### Scenario: Shard count oversubscribes the cluster and is capped at the round-robin threshold

* *GIVEN* a resolved data-file list, a node count read from `UdfContext::node_count()` at pushdown, and a `PARALLELISM_FACTOR` VS property
* *WHEN* the adapter computes the shard count G for the scan-driving query
* *THEN* the adapter SHALL compute `G = node_count × PARALLELISM_FACTOR`
* *AND* the adapter SHALL cap G at 300 so the resulting group set stays in Exasol's round-robin distribution regime
* *AND* the adapter SHALL clamp G to be at least 1 and at most the resolved file count

### Scenario: File list is partitioned into G byte-balanced disjoint shards covering every file

* *GIVEN* a resolved data-file list in which each file carries its byte size as resolved by its table's format reader and a computed shard count G
* *WHEN* the adapter partitions the file list
* *THEN* the adapter SHALL partition the file list into exactly G shards by cumulative file size, assigning each file in descending-size order to the shard whose running byte total is currently smallest
* *AND* the adapter SHALL treat any file whose reported byte size is 0 as weighing 1 byte, so the file is still assigned to a shard and never skipped
* *AND* every resolved file SHALL appear in exactly one shard and no file SHALL appear in more than one shard
* *AND* when G is at least the file count the adapter SHALL produce exactly one file per shard with no empty shards
* *AND* each shard SHALL carry, for every file it holds, both the file path and its byte size, so the resolved size is propagated into the per-shard payload rather than discarded
* *AND* the split SHALL be computed without reference to the table format that resolved the list, so an Iceberg file list and a Delta file list of the same shape produce the same shards

### Scenario: Fewer files than G produces one shard per file with no empty shards

* *GIVEN* a resolved file list whose length is smaller than the otherwise-computed G
* *WHEN* the adapter computes the shard count and partitions the files
* *THEN* the adapter SHALL clamp G down to the file count
* *AND* the adapter SHALL produce exactly one shard per file
* *AND* the adapter MUST NOT emit a scan invocation for an empty file shard

### Scenario: Scan-driving query fans out via a nested distributor over a scalar scan UDF

* *GIVEN* a file list partitioned into more than one shard
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL nest a `GROUP BY shard_key` distributor subquery — a `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET UDF invoked over a `VALUES` relation of `(shard_key, files)` rows grouped on `shard_key` (NOT on `IPROC()`) — inside an outer ungrouped select that invokes the `LAKEHOUSE_SCAN` scalar EMIT UDF once per distributed row
* *AND* the shard-invariant common spec (including the table root) SHALL be serialized EXACTLY ONCE as a single SQL string literal spliced as the scalar scan UDF's first argument, shared by every shard invocation, rather than repeated per shard and rather than flowed through the distributor
* *AND* only each shard's per-file `files` subset SHALL flow through the distributor's `VALUES` rows as the scalar scan UDF's second argument, and the table root SHALL NOT appear in any per-shard argument
* *AND* the outer scalar select MUST NOT be wrapped in a `SELECT * FROM (...)` materialization boundary, so the scalar scan streams its rows rather than Exasol buffering them into a temp table
* *AND* the union of all shard outputs SHALL be identical as an order-independent multiset to the equivalent single-shard scan

### Scenario: File distributor is a passthrough LUA SET script that re-emits each shard's file list

* *GIVEN* the fan-out distributor subquery driving cluster distribution
* *WHEN* the `LAKEHOUSE_DISTRIBUTE_FILES` SET UDF is invoked for one `shard_key` group
* *THEN* the distributor SHALL be a pure LUA SET script (created by its own DDL, carrying NO scan logic and NO data-file access) that re-emits the group's `files` VARCHAR value unchanged, one output row per `shard_key` group
* *AND* the distributor MUST NOT be a Rust entry point in the scan `.so` and MUST NOT open, resolve, or read any data file — it moves only the per-shard file-list string, so its buffered footprint is negligible and independent of the data volume scanned
* *AND* `GROUP BY shard_key` over the distributor SHALL cause Exasol to distribute the shard groups round-robin across nodes (for G ≤ 300) so each distributed file list is scanned on the node it lands on

### Scenario: Single shard short-circuits the distributor and calls the scalar scan directly

* *GIVEN* a `parallelism_factor` and file list where G resolves to 1 (single file, a parallelism factor of 1 on a single file, or any plan resolving to one shard)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL OMIT the `LAKEHOUSE_DISTRIBUTE_FILES` distributor and the inner `GROUP BY shard_key` entirely, emitting a from-less scalar `LAKEHOUSE_SCAN` invocation whose first argument is the common spec literal and whose second argument is the whole file-list literal
* *AND* a scalar EMIT UDF over constant-literal arguments SHALL fire exactly once, so no driving relation is required
* *AND* the common spec literal SHALL appear exactly once and the file-list literal exactly once
* *AND* the generated SQL SHALL be behaviourally identical (as an order-independent multiset) to the multi-shard fan-out collapsed to one shard

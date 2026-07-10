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

* Cluster fan-out is separated from the scan: a `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor does the `GROUP BY shard_key` cross-node distribution, and the `LAKEHOUSE_SCAN` SCALAR EMIT UDF scans each distributed file list node-locally and streams its rows, so scan output is not materialized into temp DB RAM.
* The shard-invariant common spec is serialized once as the scalar scan's first-argument literal; only each shard's per-file subset flows through the distributor.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: Scan-driving query fans the SET UDF across shards via GROUP BY shard_key

* *GIVEN* a file list partitioned into more than one shard
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL invoke the scan SET UDF once per shard, serializing the shard-invariant common spec EXACTLY ONCE as the SET UDF's first argument
* *AND* the SQL SHALL carry ONLY each shard's file subset as the per-shard argument, grouping the shard rows on a per-shard `shard_key` so Exasol distributes shard groups across nodes
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: Scan-driving query fans out via a nested distributor over a scalar scan UDF

* *GIVEN* a file list partitioned into more than one shard
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL nest a `GROUP BY shard_key` distributor subquery — a `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET UDF invoked over a `VALUES` relation of `(shard_key, files)` rows grouped on `shard_key` (NOT on `IPROC()`) — inside an outer ungrouped select that invokes the `LAKEHOUSE_SCAN` scalar EMIT UDF once per distributed row
* *AND* the shard-invariant common spec (including the Iceberg table root) SHALL be serialized EXACTLY ONCE as a single SQL string literal spliced as the scalar scan UDF's first argument, shared by every shard invocation, rather than repeated per shard and rather than flowed through the distributor
* *AND* only each shard's per-file `files` subset SHALL flow through the distributor's `VALUES` rows as the scalar scan UDF's second argument, and the table root SHALL NOT appear in any per-shard argument
* *AND* the outer scalar select MUST NOT be wrapped in a `SELECT * FROM (...)` materialization boundary, so the scalar scan streams its rows rather than Exasol buffering them into a temp table
* *AND* the union of all shard outputs SHALL be identical as an order-independent multiset to the equivalent single-shard scan
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: File distributor is a passthrough LUA SET script that re-emits each shard's file list

* *GIVEN* the fan-out distributor subquery driving cluster distribution
* *WHEN* the `LAKEHOUSE_DISTRIBUTE_FILES` SET UDF is invoked for one `shard_key` group
* *THEN* the distributor SHALL be a pure LUA SET script (created by its own DDL, carrying NO scan logic and NO data-file access) that re-emits the group's `files` VARCHAR value unchanged, one output row per `shard_key` group
* *AND* the distributor MUST NOT be a Rust entry point in the scan `.so` and MUST NOT open, resolve, or read any data file — it moves only the per-shard file-list string, so its buffered footprint is negligible and independent of the data volume scanned
* *AND* `GROUP BY shard_key` over the distributor SHALL cause Exasol to distribute the shard groups round-robin across nodes (for G ≤ 300) so each distributed file list is scanned on the node it lands on
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Single shard short-circuits the distributor and calls the scalar scan directly

* *GIVEN* a `parallelism_factor` and file list where G resolves to 1 (single file, a parallelism factor of 1 on a single file, or any plan resolving to one shard)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL OMIT the `LAKEHOUSE_DISTRIBUTE_FILES` distributor and the inner `GROUP BY shard_key` entirely, emitting a from-less scalar `LAKEHOUSE_SCAN` invocation whose first argument is the common spec literal and whose second argument is the whole file-list literal
* *AND* a scalar EMIT UDF over constant-literal arguments SHALL fire exactly once, so no driving relation is required
* *AND* the common spec literal SHALL appear exactly once and the file-list literal exactly once
* *AND* the generated SQL SHALL be behaviourally identical (as an order-independent multiset) to the multi-shard fan-out collapsed to one shard
<!-- /DELTA:CHANGED -->

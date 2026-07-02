# Feature: Work-Unit File Sharding

Partitions the once-resolved Iceberg data-file list into G oversubscribed work units ("shards") and drives them as one scan query that Exasol distributes across the cluster. Work assignment is computed entirely in the planning layer; each scan UDF invocation reads only its own shard of files and no file is scanned twice. The fan-out serializes the shard-invariant common spec once and carries only each shard's file subset per `VALUES` row.

## Background

* The generated fan-out SQL serializes the shard-invariant common spec exactly once as the scan SET UDF's first argument (a shared SELECT-list literal), and carries only each shard's file-URI subset as the second (per-shard) argument in the `VALUES` rows.
* Shard groups are distributed via `GROUP BY shard_key` (NOT `IPROC()`) so Exasol spreads them round-robin across nodes and multiplexes them onto each node's core pool.
* Credentials MUST NOT appear repeated per shard; they live once in the common spec literal.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan-driving query fans the SET UDF across shards via GROUP BY shard_key

* *GIVEN* a file list partitioned into more than one shard
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL invoke the scan SET UDF once per shard, serializing the shard-invariant common spec EXACTLY ONCE as a single SQL string literal in the SET UDF's first (SELECT-list) argument shared by every shard invocation, rather than repeating it per shard
* *AND* the SQL SHALL carry ONLY each shard's file-URI subset as that invocation's second (per-shard) argument, placed in the `VALUES` rows, grouping the shard rows on a per-shard `shard_key` (NOT on `IPROC()`) so Exasol distributes shard groups across nodes and multiplexes them onto each node's core pool
* *AND* the union of all shard outputs SHALL be identical in row content to the equivalent single-shard scan
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Single node with G collapsing to one preserves the single-invocation query

* *GIVEN* a `CLUSTER_NODES` value of one and a configuration where G resolves to 1 (single file, or a parallelism factor of 1 on a single file)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit a single scan SET UDF invocation carrying the common spec argument and the whole file list as the per-shard argument
* *AND* the generated SQL MUST be behaviourally identical to the pre-sharding single-invocation execution path
* *AND* the common spec literal SHALL appear exactly once, and the file-list literal exactly once
<!-- /DELTA:CHANGED -->

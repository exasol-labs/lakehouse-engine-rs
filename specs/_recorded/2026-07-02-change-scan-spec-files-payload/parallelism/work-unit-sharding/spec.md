# Feature: Work-Unit File Sharding

Partitions the once-resolved Iceberg data-file list into G oversubscribed work units ("shards") and drives them as one scan query that Exasol distributes across the cluster. Work assignment is computed entirely in the planning layer; each scan UDF invocation reads only its own shard of files and no file is scanned twice. The fan-out serializes the shard-invariant common spec once and carries only each shard's per-file `(path, size)` subset per `VALUES` row.

## Background

* Files are assigned to the G shards by a byte-balanced split (`partition_files_by_bytes`), called with G instead of node_count. Each file carries its `file_size_in_bytes` from the Iceberg `FileScanTask`; the split balances cumulative bytes per shard, not file count. A file whose reported size is 0 is weighted as 1 byte so it is still assigned and never skipped.
* The byte-balanced split PROPAGATES each file's size through to the shard it lands in: a shard is a list of `(path, size)` entries, not bare paths, so the size the adapter already resolved travels into the per-shard payload rather than being dropped.
* The generated fan-out SQL serializes the shard-invariant common spec (including the Iceberg table root) exactly once as the scan SET UDF's first argument, and carries only each shard's `(path, size)` subset as the second (per-shard) argument in the `VALUES` rows.
* Credentials MUST NOT appear repeated per shard; they live once in the common spec literal.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: File list is partitioned into G byte-balanced disjoint shards covering every file

* *GIVEN* a resolved data-file list in which each file carries its `file_size_in_bytes` (from the Iceberg `FileScanTask`) and a computed shard count G
* *WHEN* the adapter partitions the file list
* *THEN* the adapter SHALL partition the file list into exactly G shards by cumulative file size, assigning each file in descending-size order to the shard whose running byte total is currently smallest
* *AND* the adapter SHALL treat any file whose reported `file_size_in_bytes` is 0 as weighing 1 byte, so the file is still assigned to a shard and never skipped
* *AND* every resolved file SHALL appear in exactly one shard and no file SHALL appear in more than one shard
* *AND* when G is at least the file count the adapter SHALL produce exactly one file per shard with no empty shards
* *AND* each shard SHALL carry, for every file it holds, both the file path and its byte size, so the resolved size is propagated into the per-shard payload rather than discarded
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Scan-driving query fans the SET UDF across shards via GROUP BY shard_key

* *GIVEN* a file list partitioned into more than one shard
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL invoke the scan SET UDF once per shard, serializing the shard-invariant common spec (including the Iceberg table root) EXACTLY ONCE as a single SQL string literal in the SET UDF's first (SELECT-list) argument shared by every shard invocation, rather than repeating it per shard
* *AND* the SQL SHALL carry ONLY each shard's `(path, size)` subset as that invocation's second (per-shard) argument, placed in the `VALUES` rows, grouping the shard rows on a per-shard `shard_key` (NOT on `IPROC()`) so Exasol distributes shard groups across nodes and multiplexes them onto each node's core pool
* *AND* the table root SHALL NOT appear in any per-shard argument, appearing only once in the shared common spec literal
* *AND* the union of all shard outputs SHALL be identical in row content to the equivalent single-shard scan
<!-- /DELTA:CHANGED -->

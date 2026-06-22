# Feature: IPROC File Sharding

Partitions the once-resolved Iceberg data-file list across the active Exasol
cluster nodes so each node's scan UDF invocation reads only its own shard of
files, with no file scanned twice. The adapter expresses this fan-out as a
single scan-driving query that Exasol distributes across nodes via IPROC, with
work assignment computed entirely in the planning layer.

## Background

The cluster node count is taken from the `CLUSTER_NODES` entry in the virtual
schema's `adapterNotes`, round-tripped to the adapter at pushdown time (default
`1`). File-to-shard assignment is computed once in the adapter; the scan UDF
still receives an explicit file list per invocation and never discovers files
itself. No node scans another node's files.

## Scenarios

### Scenario: File list is partitioned into one shard per cluster node

* *GIVEN* a resolved data-file list and a `CLUSTER_NODES` value greater than one
* *WHEN* the adapter plans the scan-driving query
* *THEN* the adapter SHALL partition the file list into exactly `CLUSTER_NODES` shards
* *AND* every resolved file SHALL appear in exactly one shard
* *AND* no file SHALL appear in more than one shard
* *AND* the file counts across shards SHALL differ by at most one (balanced partition)

### Scenario: Fewer files than nodes produces no empty-shard scan invocations

* *GIVEN* a resolved file list whose length is smaller than `CLUSTER_NODES`
* *WHEN* the adapter plans the scan-driving query
* *THEN* the adapter SHALL produce exactly one shard per file
* *AND* the adapter MUST NOT emit a scan invocation for an empty file shard

### Scenario: Multi-node scan-driving query fans the SET UDF across nodes via IPROC

* *GIVEN* a file list partitioned into more than one shard
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL invoke the scan SET UDF once per shard
* *AND* the SQL SHALL carry each shard's file subset as that invocation's explicit scan-spec argument
* *AND* the SQL SHALL group the shard rows on an IPROC-derived node key so Exasol distributes each shard to a distinct node
* *AND* the union of all shard outputs SHALL be identical in row content to the equivalent single-shard scan

### Scenario: Single cluster node preserves the existing single-invocation query

* *GIVEN* a `CLUSTER_NODES` value of one (or unset)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL emit a single scan SET UDF invocation over the whole file list
* *AND* the generated SQL MUST be behaviourally identical to the pre-sharding single-node execution path

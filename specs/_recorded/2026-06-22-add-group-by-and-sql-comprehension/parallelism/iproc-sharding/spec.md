# Feature: IPROC File Sharding

This feature is removed and superseded by `parallelism/work-unit-sharding`. IPROC-per-node sharding is the wrong parallelization model: `GROUP BY IPROC()` yields exactly one group per node, capping parallel scan instances at the node count and leaving the rest of each node's per-node VM pool (sized to `NR_OF_CORES`) idle. The replacement feature oversubscribes work units (`GROUP BY shard_key`, G = node_count × parallelism_factor capped at 300) so Exasol distributes shard groups across nodes and multiplexes them onto each node's core pool. All scenarios below move to `parallelism/work-unit-sharding`.

## Background

* All scenarios in this feature are removed; the replacement scenarios live in `parallelism/work-unit-sharding`.
* The NPROC node-count capture itself is retained (in `vs-adapter/create-virtual-schema`); only the IPROC-driven *sharding* fan-out is removed.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: File list is partitioned into one shard per cluster node

* *GIVEN* a resolved data-file list and a `CLUSTER_NODES` value greater than one
* *WHEN* the adapter plans the scan-driving query
* *THEN* this behavior SHALL be removed; replaced by G-shard balanced partitioning in `parallelism/work-unit-sharding`

### Scenario: Fewer files than nodes produces no empty-shard scan invocations

* *GIVEN* a resolved file list whose length is smaller than `CLUSTER_NODES`
* *WHEN* the adapter plans the scan-driving query
* *THEN* this behavior SHALL be removed; replaced by the one-shard-per-file clamp in `parallelism/work-unit-sharding`

### Scenario: Multi-node scan-driving query fans the SET UDF across nodes via IPROC

* *GIVEN* a file list partitioned into more than one shard
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* this behavior SHALL be removed; replaced by the `GROUP BY shard_key` fan-out in `parallelism/work-unit-sharding`

### Scenario: Single cluster node preserves the existing single-invocation query

* *GIVEN* a `CLUSTER_NODES` value of one (or unset)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* this behavior SHALL be removed; replaced by the G-collapses-to-one scenario in `parallelism/work-unit-sharding`
<!-- /DELTA:REMOVED -->

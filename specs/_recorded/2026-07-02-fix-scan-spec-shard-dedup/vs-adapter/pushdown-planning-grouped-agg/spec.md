# Feature: Pushdown Planning — Grouped Aggregate Queries

Extends `vs-adapter/pushdown-planning` with GROUP BY aggregate detection and scan-driving SQL generation. When Exasol delegates a `GROUP BY` aggregate query, the adapter detects the shape, renders group-key expressions, builds a grouped common scan spec, and generates fan-out SQL that runs DataFusion GROUP BY inside each shard invocation and merges the partials in an outer wrapper. The grouped common spec is serialized once (shared by all shards) and carries no LIMIT.

## Background

* The grouped scan-driving SQL serializes the shard-invariant common spec once and carries only each shard's file subset per `VALUES` row, exactly as the row-scan fan-out.
* LIMIT is never pushed into the per-shard grouped scan; the shared common spec is built with no LIMIT, so no shard observes one — it appears only in the outer wrapper.
* The inner `GROUP BY shard_key` parallelizes the scan; DataFusion performs the user GROUP BY inside each shard invocation and the outer wrapper re-groups and merges the partials.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Grouped scan-driving SQL fans out via GROUP BY shard_key over G work units

* *GIVEN* a grouped aggregate pushdown over a file list partitioned into G work-unit shards
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL group the per-shard rows on `shard_key` (one group per shard), NOT on `IPROC()`
* *AND* G SHALL be `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300 and clamped to the file count, so the shard groups distribute round-robin across nodes and multiplex onto each node's core pool
* *AND* the scan SET UDF SHALL be invoked once per shard with the shard-invariant common spec serialized once as its first argument and that shard's file subset as its second argument
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: LIMIT is NOT pushed into per-shard scan for a grouped query

* *GIVEN* a grouped aggregate query with a LIMIT clause
* *WHEN* the adapter builds the grouped scan spec
* *THEN* the shard-invariant common spec MUST NOT carry the LIMIT value, so no per-shard partial scan observes a LIMIT
* *AND* because the common spec is shared by every shard, the LIMIT-exclusion invariant SHALL hold for every shard by construction (the LIMIT is stripped from the single common spec, not per shard)
* *AND* the LIMIT SHALL appear only in the outer wrapper SQL that merges partial-aggregate results from all shards
<!-- /DELTA:CHANGED -->

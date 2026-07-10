# Feature: Pushdown Planning — Grouped Aggregate Queries

Extends pushdown planning with GROUP BY aggregate detection and scan-driving SQL
generation. The adapter detects the grouped shape, renders group-key expressions,
builds a grouped common scan spec spliced once as the scalar scan UDF's first argument,
and generates fan-out SQL that runs DataFusion GROUP BY inside each scalar-scan
invocation and merges the partials in an outer wrapper. Cluster fan-out (`GROUP BY
shard_key`) lives inside the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery;
the outer wrapper re-groups the scalar scan's emitted partial rows on the user group
keys.

## Background

* DataFusion performs the user GROUP BY inside each scalar-scan invocation, emitting one partial-aggregate row per distinct user group per shard; the outer wrapper merges those partials on the user group keys with the same SUM/MIN/MAX/AVG-pair decomposition as the single-group path.
* The grouped common spec carries no LIMIT.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Grouped scan-driving SQL fans out via a nested shard_key distributor over G work units

* *GIVEN* a grouped aggregate pushdown over a file list partitioned into G work-unit shards
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the generated SQL SHALL place the `GROUP BY shard_key` (one group per shard) INSIDE the nested `LAKEHOUSE_DISTRIBUTE_FILES` distributor subquery, NOT at the outer merge level and NOT on `IPROC()`
* *AND* G SHALL be `node_count × parallelism_factor` capped at 300 and clamped to the file count, so the shard groups distribute round-robin across nodes and multiplex onto each node's core pool
* *AND* the `LAKEHOUSE_SCAN` SCALAR EMIT UDF SHALL be invoked over each distributed shard row with the shard-invariant grouped common spec spliced once as its first-argument literal and that shard's file subset as its second argument
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Grouped aggregate wrapper SQL re-groups partial results per user group key

* *GIVEN* a grouped aggregate pushdown fanned out over G shards via the nested `shard_key` distributor
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the inner distributor's `shard_key` grouping SHALL parallelize the scan across nodes while the scalar scan UDF performs the user GROUP BY inside each shard invocation, emitting one partial-aggregate row per distinct user group per shard
* *AND* the outer wrapper SQL SHALL GROUP BY the user group-key columns over the scalar scan select and merge the per-shard partials using the same SUM/MIN/MAX/AVG-pair decomposition as the single-group path, with no `SELECT * FROM (...)` wrapper between the merge and the scalar scan
* *AND* the outer wrapper SELECT list SHALL place each group-key cast expression and each merged-aggregate expression at the same ordinal position that item occupied in the user's `selectListDataTypes`, so the wrapper's result column order and per-column type match Exasol's positional pushdown validation for ANY interleaving of keys and aggregates, while the inner scalar scan's per-shard EMITS clause MAY remain keys-first (GK_* then PARTIAL_*) because it is matched only against the scan UDF's own output
* *AND* the merged result per group SHALL equal the result of the same grouped aggregate evaluated over all rows on a single node
<!-- /DELTA:CHANGED -->

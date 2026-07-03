# Feature: DataFusion Scan Execution — Grouped Aggregates and Memory Safety

Extends `datafusion-scan/scan-execution` with the bounded `RuntimeEnv` memory pool
(sized from the per-instance UDF memory limit with a spill backstop) and grouped
partial aggregate execution (one partial row per distinct user group per shard).

## Background

* A grouped scan spec carries `group_keys` (rendered DataFusion SQL fragments) and
  partial-aggregate instructions. The scan emits one partial row per distinct user
  group per shard; the adapter's outer wrapper SQL re-groups and merges the partials.
* The `group_keys` list MAY contain two or more rendered fragments; the DataFusion
  GROUP BY is built over all of them, so the per-shard partial key space is the
  product of the distinct values of every group key.
* The bounded memory pool and `/tmp` spill backstop apply uniformly regardless of the
  number of group keys.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: High-cardinality multi-key grouped scan completes under the bounded memory pool

* *GIVEN* a grouped scan spec whose `group_keys` list has two or more elements and whose shard's assigned files contain many distinct multi-key group combinations (a larger key space than any single key alone)
* *WHEN* the scan UDF runs the per-shard DataFusion GROUP BY over all group keys
* *THEN* the scan SHALL emit one partial row per distinct multi-key group observed in the shard, with each group key's value in its own `GK_{i}` column
* *AND* when `/tmp` is real disk with free space the grouped scan SHALL complete by spilling rather than failing, at any multi-key group cardinality
* *AND* when no spill disk is available the scan SHALL return a clean `ResourcesExhausted` error rather than OOM-crashing the VM
<!-- /DELTA:NEW -->

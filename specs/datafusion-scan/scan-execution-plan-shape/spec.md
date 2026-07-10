# Feature: DataFusion Scan Execution — Raw-Scan Physical Plan Shape

Guarantees the raw-row scan path's DataFusion physical plan stays the lean single-partition
pipeline it needs to be for throughput: no needless repartition, coalesce-partitions,
global sort, or global aggregate stage, with a bounded local top-N as the one intentional
exception when the scan spec carries an `order_by`. Split out of
`datafusion-scan/scan-execution` to keep that feature's core reconstitution/I/O/type-
mapping scenarios separate from physical-plan-shape scenarios.

## Background

* The raw-row scan pipeline is throughput-sensitive: needless physical-plan stages
  (a `RepartitionExec`, a `CoalescePartitionsExec`, a global `SortExec`, or a global
  aggregate) on the single-shard raw-scan path add CPU and latency without changing
  the result, and MUST be avoided so the per-instance pipeline stays
  `ParquetExec → FilterExec → ProjectionExec → CoalesceBatchesExec → emit`. A bounded
  top-N (an `ORDER BY … LIMIT n` TopK) is the one intentional exception, present only
  when the scan spec carries an `order_by`.
* See `datafusion-scan/scan-execution` for the core scan-invocation, reconstitution, and
  type-mapping scenarios this plan shape applies to.

## Scenarios

### Scenario: Raw-scan physical plan carries no needless repartition or coalesce-partitions stage

* *GIVEN* a scan spec on the raw-row path whose shard is one partition (one partition per shard, the single-instance scan unit), scanned through the custom `ParquetSource`-backed `TableProvider`
* *WHEN* the scan UDF builds the DataFusion physical plan for the assigned files, whether or not a base `ParquetAccessPlan` is attached for positional deletes
* *THEN* the physical plan SHALL NOT contain a repartition, a coalesce-partitions, a global sort, or a global aggregate stage on the raw-row path
* *AND* the plan SHALL remain the lean single-partition pipeline feeding the incremental emit, so no stage redistributes or re-buffers rows beyond what projection, filter, and batch coalescing require — the custom provider MUST NOT introduce a plan-shape regression versus the prior `ListingTable`
* *AND* Parquet row-group and predicate/page pruning SHALL still occur with a base `ParquetAccessPlan` attached, the opener intersecting pruning ON TOP of the injected row selection rather than disabling it
* *AND* the emitted rows SHALL be identical to those the unpruned, un-optimized plan would produce (with deletes applied)

### Scenario: Scan emits a bounded local top-N when the spec carries an order-by

* *GIVEN* a scan spec carrying an `order_by` sort-key list (each with a column, direction, and NULL placement), a row limit `n`, and no aggregates or group keys (the raw-row path)
* *WHEN* the scan UDF runs over its assigned files
* *THEN* the UDF SHALL apply `ORDER BY <keys> LIMIT n` in its DataFusion scan query so it emits at most `n` rows — its own local top-N over only its assigned files
* *AND* the rendered `ORDER BY` SHALL preserve each key's requested direction (`ASC`/`DESC`) and NULL placement (`NULLS FIRST`/`NULLS LAST`)
* *AND* the bounded sort SHALL be a top-N (retaining only the `n` extreme rows) rather than a full materialised global sort of the shard's rows

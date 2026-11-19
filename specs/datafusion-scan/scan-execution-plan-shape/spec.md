# Feature: DataFusion Scan Execution, Raw-Scan Physical Plan Shape

The raw-row scan path keeps a lean single-partition DataFusion physical plan. The one intentional extra stage is a bounded local top-N when the scan spec carries an `order_by`.

## Background

* The raw-row pipeline is `ParquetExec → FilterExec → ProjectionExec → CoalesceBatchesExec → emit`. A repartition, coalesce-partitions, global sort, or global aggregate stage on this path adds CPU and latency without changing the result.

* A file scan reads its files with at most `target_partitions` streams, whatever plan shape reads it. DataFusion's file-scan repartitioning is off. A stream that has nothing left to read takes the next unread file or byte range of the scan, so no file is bound to one stream.

## Scenarios

### Scenario: Raw-scan physical plan carries no needless repartition or coalesce-partitions stage

* *GIVEN* a scan spec on the raw-row path whose shard is one partition (one partition per shard, the single-instance scan unit), scanned through the custom `ParquetSource`-backed `TableProvider`
* *WHEN* the scan UDF builds the DataFusion physical plan for the assigned files, whether or not a base `ParquetAccessPlan` is attached for positional deletes
* *THEN* the physical plan SHALL NOT contain a repartition, a coalesce-partitions, a global sort, or a global aggregate stage on the raw-row path
* *AND* the plan SHALL remain the lean single-partition pipeline feeding the incremental emit, so no stage redistributes or re-buffers rows beyond what projection, filter, and batch coalescing require, the custom provider MUST NOT introduce a plan-shape regression versus the prior `ListingTable`
* *AND* Parquet row-group and predicate/page pruning SHALL still occur with a base `ParquetAccessPlan` attached, the opener intersecting pruning ON TOP of the injected row selection rather than disabling it
* *AND* the emitted rows SHALL be identical to those the unpruned, un-optimized plan would produce (with deletes applied)

### Scenario: Scan emits a bounded local top-N when the spec carries an order-by

* *GIVEN* a scan spec carrying an `order_by` sort-key list (each with a column, direction, and NULL placement), a row limit `n`, and no aggregates or group keys (the raw-row path)
* *WHEN* the scan UDF runs over its assigned files
* *THEN* the UDF SHALL apply `ORDER BY <keys> LIMIT n` in its DataFusion scan query so it emits at most `n` rows, its own local top-N over only its assigned files
* *AND* the rendered `ORDER BY` SHALL preserve each key's requested direction (`ASC`/`DESC`) and NULL placement (`NULLS FIRST`/`NULLS LAST`)
* *AND* the bounded sort SHALL be a top-N (retaining only the `n` extreme rows) rather than a full materialised global sort of the shard's rows


### Scenario: A file scan has one stream per file, up to the target

* *GIVEN* a file scan of several files, which may total less than 1 MiB
* *WHEN* the scan UDF builds the physical plan
* *THEN* the file scan SHALL have one stream per file, up to `target_partitions`

### Scenario: A data file larger than an even share of the scan is cut into byte ranges

* *GIVEN* a file scan with a data file larger than both 1 MiB and the scan's bytes divided by `target_partitions`
* *WHEN* the scan UDF builds the physical plan
* *THEN* the UDF SHALL cut the file into at most `target_partitions` contiguous byte ranges that cover it exactly once, each about that share
* *AND* a file no larger than that share SHALL NOT be cut

### Scenario: Every pushed plan shape keeps the scan's streams

* *GIVEN* scans with a filter, a LIMIT, a top-N, an aggregate, and a broadcast join
* *WHEN* DataFusion optimizes each physical plan
* *THEN* every file scan SHALL keep the stream count and the byte ranges the scan UDF built

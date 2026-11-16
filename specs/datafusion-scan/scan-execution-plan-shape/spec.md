# Feature: DataFusion Scan Execution, Raw-Scan Physical Plan Shape

The raw-row scan path keeps a lean single-partition DataFusion physical plan. The one intentional extra stage is a bounded local top-N when the scan spec carries an `order_by`.

## Background

* The raw-row pipeline is `ParquetExec → FilterExec → ProjectionExec → CoalesceBatchesExec → emit`. A repartition, coalesce-partitions, global sort, or global aggregate stage on this path adds CPU and latency without changing the result.

* The scan table builds its own file groups, at most `target_partitions` per file scan, and DataFusion's file-scan repartitioning is off. Each group is one stream.

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


### Scenario: A small data file is never cut into byte ranges

* *GIVEN* a file scan whose data files are each at most 8 MiB
* *WHEN* the scan table builds its file groups
* *THEN* every file SHALL sit whole in one group
* *AND* the files SHALL be spread over `target_partitions` groups, or over one group per file when there are fewer files

### Scenario: A shard of tiny files is read by several streams

* *GIVEN* a file scan whose files total less than 1 MiB
* *WHEN* the scan table builds its file groups
* *THEN* the scan table SHALL build one group per file, up to `target_partitions`

### Scenario: A large data file is cut into byte ranges

* *GIVEN* a data file larger than 8 MiB
* *WHEN* the scan table builds its file groups
* *THEN* the scan table SHALL cut the file into at most `target_partitions` contiguous byte ranges that cover it exactly once, each about an even share of the scan's bytes

### Scenario: Every pushed plan shape keeps the scan's file groups

* *GIVEN* scans with a filter, a LIMIT, a top-N, an aggregate, and a broadcast join
* *WHEN* DataFusion optimizes each physical plan
* *THEN* every file scan SHALL keep the groups the scan table built, and no small file SHALL carry a byte range

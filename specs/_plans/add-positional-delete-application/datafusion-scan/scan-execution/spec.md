# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers exactly the
Iceberg/Parquet data files assigned to its shard, sizes its DataFusion memory pool from the
per-instance memory limit reported in UDF metadata, applies the pushed-down projection, filter, and
LIMIT, and streams the matching rows back as Arrow IPC batches. It holds no state and discovers no
files of its own. The UDF receives its scan spec as TWO VARCHAR arguments — a shard-invariant common
spec serialized once for the whole fan-out (including the Iceberg table root), and a per-shard file
list — which it merges back into one spec at entry.

## Background

* Files are registered through a DataFusion `ParquetSource`-backed provider so projection/filter/
  LIMIT pushdown, row-group and page pruning, statistics, and streaming are preserved.
* The raw-row path is a lean single-partition pipeline (one partition per shard) with no
  repartition, coalesce-partitions, global sort, or global aggregate stage.
* Positional-delete application (see `datafusion-scan/scan-execution-positional-deletes`) attaches a
  per-data-file base `ParquetAccessPlan` to the same provider without changing this plan shape.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan registers only its assigned files and returns matching rows

* *GIVEN* a scan invocation receiving TWO VARCHAR arguments — a shard-invariant common spec argument (carrying the logical Iceberg schema, projection, filter, limit, storage credentials, the Iceberg table root, and tuning knobs) and a per-shard files argument listing specific Iceberg Parquet files in MinIO, each optionally carrying its associated positional-delete file references
* *AND* a projection naming a subset of columns
* *WHEN* the scan UDF runs for that invocation
* *THEN* the UDF SHALL read the common spec from the first input argument and the file list from the second, and reconstitute a single scan spec whose files (and their delete references) come from the second argument and whose every other field comes from the first (only serialized bytes crossing the `.so` boundary — both arguments are VARCHAR JSON)
* *AND* the UDF SHALL resolve each file entry to an absolute URI (absolute entries pass through; relative entries are joined onto the common spec's table root) and register ONLY those files through a custom `TableProvider` built over DataFusion's own `ParquetSource` (replacing the prior `ListingTable`) whose declared schema is the logical Iceberg schema (each field carrying its field-id metadata, bound via the existing field-id expression adapter), NOT a schema inferred from the first file, and MUST NOT resolve or discover any additional files from the catalog
* *AND* the `ParquetSource`-backed provider SHALL let a per-data-file base `ParquetAccessPlan` be attached for positional-delete application (see `datafusion-scan/scan-execution-positional-deletes`) while projection/filter/LIMIT pushdown and Parquet pruning are preserved
* *AND* the UDF SHALL emit one output row per surviving source row containing only the projected columns
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Raw-scan physical plan carries no needless repartition or coalesce-partitions stage

* *GIVEN* a scan spec on the raw-row path whose shard is one partition (one partition per shard, the single-instance scan unit), scanned through the custom `ParquetSource`-backed `TableProvider`
* *WHEN* the scan UDF builds the DataFusion physical plan for the assigned files, whether or not a base `ParquetAccessPlan` is attached for positional deletes
* *THEN* the physical plan SHALL NOT contain a repartition, a coalesce-partitions, a global sort, or a global aggregate stage on the raw-row path
* *AND* the plan SHALL remain the lean single-partition pipeline feeding the incremental emit, so no stage redistributes or re-buffers rows beyond what projection, filter, and batch coalescing require — the custom provider MUST NOT introduce a plan-shape regression versus the prior `ListingTable`
* *AND* Parquet row-group and predicate/page pruning SHALL still occur with a base `ParquetAccessPlan` attached, the opener intersecting pruning ON TOP of the injected row selection rather than disabling it
* *AND* the emitted rows SHALL be identical to those the unpruned, un-optimized plan would produce (with deletes applied)
<!-- /DELTA:CHANGED -->

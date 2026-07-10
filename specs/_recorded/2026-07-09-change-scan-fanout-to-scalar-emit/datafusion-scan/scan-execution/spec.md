# Feature: DataFusion Scan Execution

A disposable Rust SCALAR EMIT UDF that, for one query, builds a DataFusion session,
registers exactly the Iceberg/Parquet data files assigned to its shard, sizes its
DataFusion memory pool from the per-instance memory limit reported in UDF metadata,
applies the pushed-down projection, filter, and LIMIT, and streams the matching rows back
as Arrow IPC batches. It holds no state and discovers no files of its own. As a SCALAR
EMIT UDF, Exasol may batch MULTIPLE input rows into one `run()` call, so the UDF loops
over the batch, scanning each row's assigned file list. The UDF receives its scan spec as
TWO VARCHAR arguments — a shard-invariant common spec serialized once for the whole
fan-out (including the Iceberg table root), and a per-shard file list — which it merges
back into one `ScanSpec` per input row.

## Background

* Only serialized bytes cross the `.so` boundary — VARCHAR JSON arguments in, Arrow IPC bytes out; no typed Arrow value ever crosses it.
* The UDF is stateless and discovers no files of its own; it registers exactly the files assigned in its per-shard argument.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Scan loops over a batched scalar input and scans every assigned file list once

* *GIVEN* a SCALAR EMIT scan invocation whose `run()` call carries MULTIPLE input rows, each row holding the same shard-invariant common spec argument and its own per-shard files argument
* *WHEN* the scan UDF runs for that batch
* *THEN* the UDF SHALL loop `while ctx.next()` over every input row in the batch and scan each row's assigned file list, emitting that row's surviving output rows, so NO input row past the first is silently dropped
* *AND* the DataFusion runtime SHALL be built ONCE from the first row's (shard-invariant) thread configuration and reused across every row in the batch, then torn down deterministically exactly once after the batch is drained (preserving the `run_on_runtime` / `shutdown_timeout` teardown discipline that otherwise races detached object-store background tasks)
* *AND* a batch of exactly one input row SHALL produce byte-identical output to the pre-batching single-row scan, so the loop is a no-op for the would-be single-row call
* *AND* only serialized bytes (VARCHAR JSON arguments in, Arrow IPC bytes out) SHALL cross the `.so` boundary, unchanged by the batch loop
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Scan registers only its assigned files and returns matching rows

* *GIVEN* a scan input row carrying TWO VARCHAR arguments — a shard-invariant common spec argument (carrying the logical Iceberg schema, projection, filter, limit, storage credentials, the Iceberg table root, and tuning knobs) and a per-shard files argument listing specific Iceberg Parquet files in MinIO, each optionally carrying its associated positional-delete file references
* *AND* a projection naming a subset of columns
* *WHEN* the scan UDF processes that input row
* *THEN* the UDF SHALL read the common spec from the first input argument and the file list from the second, and reconstitute a single scan spec whose files (and their delete references) come from the second argument and whose every other field comes from the first (only serialized bytes crossing the `.so` boundary — both arguments are VARCHAR JSON)
* *AND* the UDF SHALL resolve each file entry to an absolute URI and register ONLY those files through the custom table provider whose declared schema is the logical Iceberg schema, and MUST NOT resolve or discover any additional files from the catalog
* *AND* the UDF SHALL emit one output row per surviving source row containing only the projected columns
<!-- /DELTA:CHANGED -->

# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers
exactly the Iceberg/Parquet data files assigned to its shard, sizes its DataFusion
`RuntimeEnv` memory pool from the per-instance memory limit reported in UDF metadata,
applies the pushed-down projection, filter, and LIMIT, and streams the matching rows
back as Arrow IPC batches. It holds no state and discovers no files of its own.

## Background

* The scan UDF reads its ScanSpec from a single JSON VARCHAR input column.
* The UDF MUST register only its assigned files and MUST NOT discover additional files.
* When the scan spec carries a logical Iceberg schema, column projection binds by Iceberg field-id (with a physical-name fallback) so results are correct across schema evolution; when it does not, the UDF falls back to first-file schema inference and physical-name binding.
* On the raw-row path the UDF emits each Arrow `RecordBatch` via the SDK's Arrow-IPC
  emit path (`EmitBatch`, behind the `emit-arrow` feature), which serializes the batch
  to Arrow IPC bytes internally — only IPC bytes cross the `.so` boundary, never typed
  Arrow objects, and no `Vec<Value>` intermediate is built per batch.
* DataFusion execution is bounded; a memory bound that cannot spill MUST surface as a
  clean error, never an OOM VM crash.
* Error messages MUST NOT contain storage access keys, secret keys, or session tokens.
* The raw-row scan pipeline is throughput-sensitive: needless physical-plan stages
  (a `RepartitionExec`, a `CoalescePartitionsExec`, a global `SortExec`, or a global
  aggregate) on the single-shard raw-scan path add CPU and latency without changing
  the result, and MUST be avoided so the per-instance pipeline stays
  `ParquetExec → FilterExec → ProjectionExec → CoalesceBatchesExec → emit`.
* A producer/consumer decode-emit overlap buffer (a bounded queue of fetched-but-not-
  yet-emitted batches) is NOT part of the committed scan path. It is a CONDITIONAL,
  measure-first capability: it SHALL only be added if the phase telemetry
  (`datafusion-scan/scan-execution-telemetry`) shows the emit phase and the
  object-storage import phase do not already overlap and that decoupling them yields a
  measured throughput gain. Until that evidence exists, the streaming discipline stays
  strictly fetch-one / emit / drop with no buffer.
* See `datafusion-scan/scan-execution-memory-and-credentials` for pool sizing and
  decode-bound scenarios.
* See `datafusion-scan/scan-execution-telemetry` for the phase-timing surface that
  gates the conditional buffer.
* See `datafusion-scan/scan-execution-partial-agg` for partial-aggregate output scenarios
  (ungrouped COUNT/SUM/MIN/MAX/AVG).
* See `datafusion-scan/scan-execution-grouped-agg` for grouped partial-aggregate
  memory, spill, and group-key scenarios.
* See `datafusion-scan/scan-execution-field-id-projection` for field-id-based column
  binding, physical-name fallback, null-fill for added nullable columns, and the
  backward-compatible first-file-inference fallback.

## Scenarios

### Scenario: Scan registers only its assigned files and returns matching rows

* *GIVEN* a scan spec listing specific Iceberg Parquet files in MinIO, carrying the logical Iceberg schema (each entry a `{field_id, name, arrow_type, nullable}` tuple derived once by the adapter)
* *AND* a projection naming a subset of columns
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL create a DataFusion session and register only the assigned files as one `ListingTable` whose declared schema is the logical Iceberg schema (each field carrying its `PARQUET:field_id` metadata), NOT a schema inferred from the first file
* *AND* the UDF MUST NOT resolve or discover any additional files from the catalog
* *AND* the UDF SHALL emit one output row per scanned source row containing only the projected columns

### Scenario: Filter predicate restricts the emitted rows

* *GIVEN* a scan spec carrying a translatable filter predicate
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL apply the predicate to the DataFusion scan
* *AND* the UDF SHALL emit only rows that satisfy the predicate

### Scenario: LIMIT caps the emitted rows

* *GIVEN* a scan spec carrying a row limit smaller than the matching row count
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL emit no more rows than the limit

### Scenario: Arrow batches are emitted incrementally as Arrow IPC and never double-materialized

* *GIVEN* a scan whose result spans multiple Arrow record batches
* *WHEN* the scan UDF processes the result stream
* *THEN* the UDF SHALL emit each batch via the SDK's Arrow-batch emit path (the `EmitBatch` API, gated by the `emit-arrow` feature), serializing the batch to Arrow IPC bytes so only IPC bytes cross the `.so` boundary
* *AND* the UDF SHALL fetch one batch, emit it, and drop it before fetching the next, never materializing the entire result set
* *AND* the UDF MUST NOT build an intermediate `Vec<Value>` row collection on the raw-row scan path, and no typed Arrow value SHALL cross the `.so` boundary — only the serialized IPC byte buffer

### Scenario: Arrow types map to the correct SDK Value variants

* *GIVEN* a table with integer, floating-point, string, boolean, date, and timestamp columns
* *WHEN* the scan UDF converts a batch of those columns
* *THEN* each Arrow column value SHALL map to the corresponding SDK `Value` variant per the `datafusion-scan/type-mapping` table
* *AND* an Arrow null SHALL map to `Value::Null`

### Scenario: Incompatible Arrow columns are emitted as JSON strings

* *GIVEN* a scan result containing columns of types Exasol cannot represent (list, struct, map, binary, or out-of-range decimal)
* *WHEN* the scan UDF converts a batch of those columns
* *THEN* the UDF SHALL serialize each such value to a JSON string and emit it as `Value::String` per the `datafusion-scan/type-mapping` rules
* *AND* the UDF MUST NOT emit any array, list, struct, or map `Value`

### Scenario: Scan reports a clear error when an assigned file is unreadable

* *GIVEN* a scan spec referencing a file that cannot be read from object storage
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL return an error identifying that the assigned data could not be read
* *AND* the error message MUST NOT contain storage access keys or secret keys

### Scenario: Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch

* *GIVEN* a scan spec carrying `emit_exa_types` (the declared Exasol EMITS type string per output column, positionally aligned)
* *AND* a result Arrow batch whose column types diverge from those declarations (e.g. an `Int32` column declared `DECIMAL(10,0)`, a `Utf8View` column declared `VARCHAR`, or a `Decimal128(10,0)` column declared `DECIMAL(10,0)`)
* *WHEN* the scan UDF processes the batch
* *THEN* the UDF SHALL coerce each output column to the Arrow type that `emit_batch`'s strict IPC feed requires for its declared ExaType, before passing the batch to `emit_batch`
* *AND* the coercion SHALL reproduce Exasol's DECIMAL precision binning: scale-0 precision ≤ 9 → `Int32`; scale-0 precision ≤ 18 → `Int64`; scale > 0 or precision 19..=36 → `Decimal128(p,s)`
* *AND* string-family declarations (`VARCHAR`, `CHAR`) SHALL coerce the column to `Utf8`, subsuming `Utf8View`/`BinaryView` view-type normalization
* *AND* a column already of the correct Arrow type SHALL be passed through unchanged (zero-copy fast path)
* *AND* when `emit_exa_types` is absent or shorter than the column count (specs that predate this field), unmatched columns SHALL fall back to view-type normalization only (`Utf8View` → `Utf8`, `BinaryView` → `Binary`)

### Scenario: Scan surfaces a clean memory-exhaustion error instead of crashing the VM

* *GIVEN* a scan whose execution exhausts the configured DataFusion memory pool (a `ResourcesExhausted` condition) on a node whose `/tmp` is not spill-capable disk
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL surface a clean error that identifies memory/resource exhaustion as the cause, and MUST NOT crash the UDF VM
* *AND* the error-redaction path MUST NOT reclassify a `ResourcesExhausted` condition as an "assigned data could not be read" storage error
* *AND* the surfaced error message MUST NOT contain any storage access key, secret key, or session token

### Scenario: Raw-scan physical plan carries no needless repartition or coalesce-partitions stage

* *GIVEN* a scan spec on the raw-row path whose `df_target_partitions` is `1` (one partition per shard, the single-instance scan unit)
* *WHEN* the scan UDF builds the DataFusion physical plan for the assigned files
* *THEN* the physical plan SHALL NOT contain a `RepartitionExec`, a `CoalescePartitionsExec`, a global `SortExec`, or a global aggregate stage on the raw-row path
* *AND* the plan SHALL be the lean pipeline `ParquetExec → FilterExec → ProjectionExec → CoalesceBatchesExec` feeding the incremental emit, so no stage redistributes or re-buffers rows beyond what projection, filter, and batch coalescing require
* *AND* the emitted rows SHALL be identical to those the unpruned, un-optimized plan would produce


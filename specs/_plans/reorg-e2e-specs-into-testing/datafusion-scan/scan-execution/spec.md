# Feature: DataFusion Scan Execution

A disposable Rust SCALAR EMIT UDF that, for one query, builds a DataFusion session,
registers exactly the Parquet data files assigned to its shard, sizes its
DataFusion `RuntimeEnv` memory pool from the per-instance memory limit reported in UDF
metadata, applies the pushed-down projection, filter, and LIMIT, and streams the matching
rows back as Arrow IPC batches. It holds no state and discovers no files of its own. As a
SCALAR EMIT UDF under SDK 0.21.0, the framework invokes `run()` ONCE per input row, so the
UDF scans exactly one row's assigned file list per call and never iterates the input with
`ctx.next()`. The UDF receives its scan spec as TWO VARCHAR arguments — a shard-invariant
common spec serialized once for the whole fan-out (including the table root), and a
per-shard `(path, size)` file list — which it merges back into one `ScanSpec` per call.
Arrow-column-to-SDK-`Value` conversion at the emit boundary — type mapping, incompatible-
column JSON rendering, and EMITS-type coercion — is owned by
`datafusion-scan/scan-execution-value-conversion`.

<!-- DELTA:CHANGED -->
## Background

* Only serialized bytes cross the `.so` boundary — VARCHAR JSON arguments in, Arrow IPC bytes out; no typed Arrow value ever crosses it.
* The scan UDF receives two VARCHAR JSON arguments per input row: `common` (shard-invariant: projection, filter, limit, aggregates, group keys, logical schema, EMITS types, a storage reference the UDF resolves itself, the table root, and tuning knobs) and `files` (this shard's assigned `(path, size)` entries). It merges them into one `ScanSpec` before running; see `datafusion-scan/scan-execution-spec-reconstitution` for the reconstitution and malformed-input scenarios.
* The UDF MUST register only its assigned files and MUST NOT discover additional files.
* Per-file metadata construction (no per-file `HEAD`) and relative/absolute path resolution
  against the table root are covered by `datafusion-scan/scan-execution-file-metadata`.
* `ScanSpec` carries no catalog identifier block — the scan UDF never contacts the catalog.
* Only `Value::String` types cross the `.so` boundary; both arguments are VARCHAR JSON.
* When the scan spec carries a logical schema, column projection binds each logical field by the binding key its format reader populated — by field-id, matched against the physical field's `PARQUET:field_id`, for Iceberg (always) and Delta `id` column mapping, with a physical-name fallback; by physical name, matched against the Parquet column's own name, for Delta `name` column mapping; by identity, matched against the logical name itself, for Delta `none` column mapping — so results are correct across schema evolution. When the scan spec carries no logical schema, the UDF falls back to first-file schema inference and physical-name binding.
* On the raw-row path the UDF emits each Arrow `RecordBatch` via the SDK's Arrow-IPC
  emit path (`EmitBatch`, behind the `emit-arrow` feature), which serializes the batch
  to Arrow IPC bytes internally — only IPC bytes cross the `.so` boundary, never typed
  Arrow objects, and no `Vec<Value>` intermediate is built per batch.
* DataFusion execution is bounded; a memory bound that cannot spill MUST surface as a
  clean error, never an OOM VM crash.
* Error messages MUST NOT contain storage access keys, secret keys, or session tokens.
* See `datafusion-scan/scan-execution-plan-shape` for the raw-scan physical-plan-shape
  guarantee (no needless repartition/coalesce/sort stage) and the bounded local top-N
  scenario.
* A producer/consumer decode-emit overlap buffer (a bounded queue of fetched-but-not-
  yet-emitted batches) is NOT part of the committed scan path. It is a CONDITIONAL,
  measure-first capability: it SHALL only be added if the phase telemetry
  (`scan-runtime/scan-execution-telemetry`) shows the emit phase and the
  object-storage import phase do not already overlap and that decoupling them yields a
  measured throughput gain. Until that evidence exists, the streaming discipline stays
  strictly fetch-one / emit / drop with no buffer.
* The scan registers files through a custom `ParquetSource`-backed table provider (`PositionalDeleteScanTable`); it does not use iceberg-rust's own Arrow reader, so it does not inherit that reader's INT96 coercion.
* INT96 is a legacy pre-Iceberg Parquet/Hive/Spark physical timestamp encoding absent from the Iceberg spec, whose Parquet mapping is INT64-only. Tolerating INT96 on read is a real-world-compatibility affordance for non-compliant writers, not a spec deviation.
* Logical Iceberg-to-Arrow and Arrow-to-Exasol type mapping is owned by `scan-types/type-mapping`; this feature owns only the physical Parquet decode configuration.
* INT96 physically carries nanosecond precision (Julian day + nanoseconds-within-day). Coercing to `"us"` deliberately truncates any sub-microsecond digits — a named trade-off, consistent with Iceberg's microsecond `timestamp` model, which promises no sub-microsecond precision. The engine never claimed to preserve INT96's extra precision.
* `coerce_int96_tz = "UTC"` makes the decoded batch's physical Arrow type `Timestamp(Microsecond, "UTC")` regardless of the Iceberg column type. For an Iceberg `timestamp` (WITHOUT time zone) column the field-id (production) path's logical schema is `Timestamp(Microsecond, None)`; the None-vs-UTC difference is reconciled at the EMITS-coercion step owned by `datafusion-scan/scan-execution-value-conversion` (see that feature's EMITS-coercion scenario), not only on the legacy inference path.
* Consequence of the root-cause-only, no-clamp decision: a coerced value above Exasol's own `TIMESTAMP` maximum (year > 9999) still fails at the Exasol emit boundary with a `TIMESTAMP` range error. This fix removes the arrow-decode overflow for values through `9999-12-31`; it does not make year > 9999 values scannable.
* See `scan-runtime/scan-execution-memory-and-credentials` for pool sizing and
  decode-bound scenarios.
* See `scan-runtime/scan-execution-telemetry` for the phase-timing surface that
  gates the conditional buffer.
* See `scan-aggregation/scan-execution-partial-agg` for partial-aggregate output scenarios
  (ungrouped COUNT/SUM/MIN/MAX/AVG).
* See `scan-aggregation/scan-execution-grouped-agg` for grouped partial-aggregate
  memory, spill, and group-key scenarios.
* See `scan-read-path/scan-execution-field-id-projection` for field-id-based column
  binding, physical-name fallback, null-fill for added nullable columns, and the
  backward-compatible first-file-inference fallback.
* See `datafusion-scan/scan-execution-spec-reconstitution` for the two-argument
  common/per-shard merge, malformed-input handling, and the no-catalog-block guarantee.
* See `datafusion-scan/scan-execution-file-metadata` for spec-backed per-file metadata
  (no per-file `HEAD`) and relative/absolute path resolution against the table root.
* Positional-delete application (see `scan-read-path/scan-execution-positional-deletes`)
  attaches a per-data-file base `ParquetAccessPlan` to the same `ParquetSource`-backed
  provider without changing this plan shape.
* See `scan-read-path/scan-execution-positional-deletes` for delete-application scenarios
  (base `ParquetAccessPlan` attachment, delete-set composition with pushdown, and the
  read-time backstop for unsupported delete mechanisms).
* Per-call Tokio runtime construction and the no-process-caching rule for the scan runtime
  are owned by `scan-runtime/scan-execution-threading`.
* See `datafusion-scan/scan-execution-value-conversion` for Arrow-to-`Value` type mapping,
  incompatible-column JSON emission, and EMITS-type coercion at the emit boundary.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan handles one input row per scalar run() call and never iterates with ctx.next()

* *GIVEN* the SDK-0.21.0 runtime invokes the SCALAR EMIT scan `run()` ONCE per input row, each call carrying that row's shard-invariant common spec argument (column 0) and its own per-shard files argument (column 1)
* *WHEN* the scan UDF runs for one such call
* *THEN* the UDF SHALL reconstitute exactly that one row's `ScanSpec`, register and scan only that row's assigned file list, and emit that row's surviving output rows
* *AND* the UDF MUST NOT call `ctx.next()`, which the SDK-0.21.0 runtime rejects with an error in scalar (`ExactlyOnce`) context
* *AND* across a multi-shard fan-out whose distributed rows each drive a SEPARATE `run()` call, every shard's rows SHALL be emitted, so NO shard is silently dropped (the regression the removed batch loop was hand-rolling around), and the DataFusion runtime SHALL be built and torn down once per call per `scan-runtime/scan-execution-threading`
* *AND* only serialized bytes (VARCHAR JSON arguments in, Arrow IPC bytes out) SHALL cross the `.so` boundary
<!-- /DELTA:CHANGED -->

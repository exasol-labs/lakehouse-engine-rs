# Feature: DataFusion Scan Execution

A disposable Rust SCALAR EMIT UDF that, for one query, builds a DataFusion session,
registers exactly the Iceberg/Parquet data files assigned to its shard, sizes its
DataFusion `RuntimeEnv` memory pool from the per-instance memory limit reported in UDF
metadata, applies the pushed-down projection, filter, and LIMIT, and streams the matching
rows back as Arrow IPC batches. It holds no state and discovers no files of its own.

## Background

* **This delta is issue #350.** It relocates WHERE the JSON serialization of a nested column happens
  and names the consequence for the value-conversion boundary. It changes ONE scenario. The emit-time
  ExaType coercion, the Arrow-IPC streaming discipline, the memory-exhaustion contract, the INT96
  decode configuration, and the `ctx.next()` prohibition are all untouched.
* **A nested column is rendered to JSON at the Arrow COLUMN level, upstream of the per-value
  conversion.** `datafusion-scan/nested-json-rendering` owns the rendering and applies it while the
  scan is still inside DataFusion, so the batch that reaches the emit boundary already carries `Utf8`.
  This is not a preference: `arrow::json::writer::make_encoder`'s encoder borrows the array and holds
  a reusable scratch buffer, so it is built once per column and reused across rows, which a per-cell
  `fn(&ArrayRef, usize)` signature cannot express.
* **The consequence is that `arrow_value_at` never receives a nested Arrow column, and its wildcard
  display-string arm is therefore left exactly as recorded.** That arm stays a wildcard match on
  `DataType`, unrouted through the Arrow classifier, per
  `datafusion-scan/type-mapping-module-structure`'s recorded exemptions — this delta adds no arm to it
  and removes none. Its only remaining reachable inputs are the NON-nested half of the incompatible
  set, which arrives already `Utf8` through `CAST(col AS VARCHAR)`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Incompatible Arrow columns are emitted as JSON strings

* *GIVEN* a scan result containing columns of types Exasol cannot represent — a NESTED type (list, struct, map) or a NON-NESTED one (binary or an out-of-range decimal)
* *WHEN* the scan UDF prepares a batch of those columns for emission
* *THEN* a NESTED column SHALL have been rendered to a `Utf8` column of valid JSON documents at the Arrow COLUMN level, before the batch reaches the per-value conversion boundary, per `datafusion-scan/nested-json-rendering`
* *AND* the per-value converter SHALL therefore receive that column already as `Utf8` and emit `Value::String`, and a null cell SHALL emit `Value::Null`
* *AND* a NON-NESTED incompatible column SHALL keep its recorded path unchanged — `CAST(col AS VARCHAR)` in the generated scan SQL, then `Value::String` — and this feature MUST NOT claim strict JSON conformance for it (issue #351)
* *AND* the UDF MUST NOT emit any array, list, struct, or map `Value`
* *AND* `arrow_value_at`'s wildcard display-string fallback arm SHALL be left byte-identical, gaining no nested arm, because a nested column can no longer reach it: the JSON rendering happens upstream, and the only columns the partial-aggregate path carries are group keys and aggregate results
<!-- /DELTA:CHANGED -->

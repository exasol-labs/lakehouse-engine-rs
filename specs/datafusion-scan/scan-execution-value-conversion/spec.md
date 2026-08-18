# Feature: DataFusion Scan Value Conversion

Converts each output Arrow column to the SDK `Value` variant (or coerced Arrow type) the emit
boundary requires. This covers the Arrow-to-`Value` variant mapping, the emission of Exasol-
incompatible columns as JSON strings, and the coercion of a batch's Arrow types to the declared
EMITS `ExaType` before `emit_batch`. It owns the value-conversion boundary only — file
registration, filtering, LIMIT, streaming, and error handling belong to
`datafusion-scan/scan-execution`; Iceberg-to-Arrow and Arrow-to-Exasol type-mapping *rules* belong
to `datafusion-scan/type-mapping`.

## Background

* **This delta is issue #350.** It relocates WHERE the JSON serialization of a nested column happens
  and names the consequence for the value-conversion boundary. It changes ONE scenario. The emit-time
  ExaType coercion is untouched.
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
* Only `Value::String` types cross the `.so` boundary on the value-conversion path; the raw-row path
  crosses as Arrow IPC bytes via `emit_batch` per `datafusion-scan/scan-execution`.
* Logical Iceberg-to-Arrow and Arrow-to-Exasol type mapping RULES are owned by
  `datafusion-scan/type-mapping`; this feature owns the runtime conversion step that applies those
  rules to an in-flight batch at the emit boundary.
* `coerce_int96_tz = "UTC"` makes the decoded batch's physical Arrow type `Timestamp(Microsecond,
  "UTC")` regardless of the Iceberg column type. For an Iceberg `timestamp` (WITHOUT time zone)
  column the field-id (production) path's logical schema is `Timestamp(Microsecond, None)`; the
  None-vs-UTC difference is reconciled at the EMITS-coercion scenario below — see
  `datafusion-scan/scan-execution`'s INT96 decode scenario for the physical-decode side of this
  reconciliation.

## Scenarios

### Scenario: Arrow types map to the correct SDK Value variants

* *GIVEN* a table with integer, floating-point, string, boolean, date, and timestamp columns
* *WHEN* the scan UDF converts a batch of those columns
* *THEN* each Arrow column value SHALL map to the corresponding SDK `Value` variant per the `datafusion-scan/type-mapping` table
* *AND* an Arrow null SHALL map to `Value::Null`

### Scenario: Incompatible Arrow columns are emitted as JSON strings

* *GIVEN* a scan result containing columns of types Exasol cannot represent — a NESTED type (list, struct, map) or a NON-NESTED one (binary or an out-of-range decimal)
* *WHEN* the scan UDF prepares a batch of those columns for emission
* *THEN* a NESTED column SHALL have been rendered to a `Utf8` column of valid JSON documents at the Arrow COLUMN level, before the batch reaches the per-value conversion boundary, per `datafusion-scan/nested-json-rendering`
* *AND* the per-value converter SHALL therefore receive that column already as `Utf8` and emit `Value::String`, and a null cell SHALL emit `Value::Null`
* *AND* a NON-NESTED incompatible column SHALL keep its recorded path unchanged — `CAST(col AS VARCHAR)` in the generated scan SQL, then `Value::String` — and this feature MUST NOT claim strict JSON conformance for it (issue #351)
* *AND* the UDF MUST NOT emit any array, list, struct, or map `Value`
* *AND* `arrow_value_at`'s wildcard display-string fallback arm SHALL be left byte-identical, gaining no nested arm, because a nested column can no longer reach it: the JSON rendering happens upstream, and the only columns the partial-aggregate path carries are group keys and aggregate results

### Scenario: Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch

* *GIVEN* a scan spec carrying `emit_exa_types` (the declared Exasol EMITS type string per output column, positionally aligned)
* *AND* a result Arrow batch whose column types diverge from those declarations (e.g. an `Int32` column declared `DECIMAL(10,0)`, a `Utf8View` column declared `VARCHAR`, or a `Decimal128(10,0)` column declared `DECIMAL(10,0)`)
* *WHEN* the scan UDF processes the batch
* *THEN* the UDF SHALL coerce each output column to the Arrow type that `emit_batch`'s strict IPC feed requires for its declared ExaType, before passing the batch to `emit_batch`
* *AND* the coercion SHALL reproduce Exasol's DECIMAL precision binning: scale-0 precision ≤ 9 → `Int32`; scale-0 precision ≤ 18 → `Int64`; scale > 0 or precision 19..=36 → `Decimal128(p,s)`
* *AND* string-family declarations (`VARCHAR`, `CHAR`) SHALL coerce the column to `Utf8`, subsuming `Utf8View`/`BinaryView` view-type normalization
* *AND* a column already of the correct Arrow type SHALL be passed through unchanged (zero-copy fast path)
* *AND* when `emit_exa_types` is absent or shorter than the column count (specs that predate this field), unmatched columns SHALL fall back to view-type normalization only (`Utf8View` → `Utf8`, `BinaryView` → `Binary`)

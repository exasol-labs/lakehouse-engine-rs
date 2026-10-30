# Feature: DataFusion Scan Value Conversion

The scan UDF converts each output Arrow column to the SDK `Value` variant, or the Arrow type, that the emit boundary requires. This covers the Arrow-to-`Value` mapping, JSON strings for Exasol-incompatible columns, and coercion to the declared EMITS `ExaType` before `emit_batch`.

## Background

* This feature owns the runtime conversion step. `scan-types/type-mapping` owns the type-mapping rules, and `datafusion-scan/scan-execution` owns file registration, filtering, LIMIT, streaming, and error handling.
* `scan-types/nested-json-rendering` renders a nested column to JSON at the Arrow column level, so the batch reaching the emit boundary already carries `Utf8` and `arrow_value_at` never receives a nested column.
* The scan reads each declared output type from `UdfContext::output_column(idx)`, which reports the `ExaType` of the call-site `EMITS (...)` clause. The `ExaType` variant is the bin Exasol chose (for example `Int32`, `Int64`, `Numeric`).
* Every scan call has a call-site `EMITS` clause. An absent or wrong-arity declaration is drift and is reported.
* `ExaType::Numeric` carries a required `precision` and `scale`. Exasol caps DECIMAL precision at 36, so a wider value is drift.
* `ExaType` is a closed set of ten variants.
* `ExaType::Timestamp { precision }` carries the declared fractional-second precision. `scan-types/type-mapping-timestamp-precision` owns the precision-to-`TimeUnit` table.

## Scenarios

### Scenario: Arrow types map to the correct SDK Value variants

* *GIVEN* a table with integer, floating-point, string, boolean, date, and timestamp columns
* *WHEN* the scan UDF converts a batch of those columns
* *THEN* each Arrow column value SHALL map to the corresponding SDK `Value` variant per the `scan-types/type-mapping` table
* *AND* an Arrow null SHALL map to `Value::Null`

### Scenario: Incompatible Arrow columns are emitted as JSON strings

* *GIVEN* a scan result containing columns of types Exasol cannot represent, a NESTED type (list, struct, map) or a NON-NESTED one (binary or an out-of-range decimal)
* *WHEN* the scan UDF prepares a batch of those columns for emission
* *THEN* a NESTED column SHALL have been rendered to a `Utf8` column of valid JSON documents at the Arrow COLUMN level, before the batch reaches the per-value conversion boundary, per `scan-types/nested-json-rendering`
* *AND* the per-value converter SHALL therefore receive that column already as `Utf8` and emit `Value::String`, and a null cell SHALL emit `Value::Null`
* *AND* a NON-NESTED incompatible column SHALL keep its recorded path unchanged, `CAST(col AS VARCHAR)` in the generated scan SQL, then `Value::String`, and this feature MUST NOT claim strict JSON conformance for it, except a binary column, which arrives as hex text per `vs-adapter/binary-column-rendering`
* *AND* the UDF MUST NOT emit any array, list, struct, or map `Value`
* *AND* `arrow_value_at`'s wildcard display-string fallback arm SHALL be left byte-identical, gaining no nested arm, because a nested column can no longer reach it: the JSON rendering happens upstream, and the only columns the partial-aggregate path carries are group keys and aggregate results

### Scenario: Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch

* *GIVEN* a scan call whose generated `EMITS (...)` clause declares one Exasol type per output column
* *AND* a result Arrow batch whose column types diverge from those declarations, for example an `Int32` column declared `DECIMAL(20,0)`, a `Float32` column declared `DOUBLE PRECISION`, or a `Utf8View` column declared `VARCHAR(2000000)`
* *WHEN* the scan UDF processes the batch
* *THEN* the UDF SHALL read the declared type of output column `i` from `UdfContext::output_column(i)`, and MUST NOT read any declared type carried in the scan spec
* *AND* the UDF SHALL coerce each output column to the Arrow type that `emit_batch`'s strict C Data Interface export requires for the reported `ExaType`, before passing the batch to `emit_batch`, taking the DECIMAL binning from the reported variant rather than re-deriving it from a type string: `Int32` to `Int32`, `Int64` to `Int64`, and `Numeric { precision, scale }` to `Decimal128(precision, scale)`
* *AND* the remaining variants SHALL map as `Double` to `Float64`, `Boolean` to `Boolean`, `Date` to `Date32`, `Timestamp { precision }` to `Timestamp(unit, None)` where `unit` FOLLOWS the reported `precision` under the one mapping `scan-types/type-mapping-timestamp-precision` owns, and the three that remain (`String`, `Char` and `Unsupported`) to `Utf8`, which subsumes `Utf8View`/`BinaryView` normalization and preserves the behavior the removed type-string path gave an unrecognized declaration
* *AND* the timestamp arm MUST NOT resolve a fixed `Microsecond` target at every precision, because `coerce_column` casts strictly (`safe: false`) and a microsecond target under a `TIMESTAMP(9)` declaration silently destroys the nanosecond digits of an Iceberg `timestamp_ns`/`timestamptz_ns` column, which `iceberg_primitive_to_arrow` already registers as Arrow `Timestamp(Nanosecond, _)`
* *AND* the timestamp arm MUST NOT carry its own precision-to-unit table, because the adapter's declaration producer reads the same table and two copies would let a declared `TIMESTAMP(9)` mean one width on the declaring side and another at the emit boundary
* *AND* the match over `ExaType` SHALL stay EXHAUSTIVE with no wildcard arm, so a variant added or removed upstream becomes a compile error here rather than a silent `Utf8` substitution
* *AND* the UDF SHALL fail the call on either emit path, naming the offending output column, when a `Numeric` column reports a `precision` or `scale` outside what `Decimal128` represents, and MUST NOT substitute `Utf8` or any other target type, because a valid Exasol NUMERIC declaration always carries a precision and a scale within that range
* *AND* a column already of the required Arrow type SHALL be passed through unchanged, on the zero-copy fast path
* *AND* the UDF SHALL fail the call, and MUST NOT emit the batch, when `output_column_count()` does not equal the batch's column count, naming both counts, or when `output_column(i)` returns an error for a column the batch carries, naming `i`
* *AND* the UDF MUST NOT fall back to a spec-carried or source-derived type in either failure case, because an absent declaration is drift rather than a legacy spec

### Scenario: A DATE value outside Exasol's DATE range fails the query

* *GIVEN* a table whose `date` column holds a value outside Exasol's DATE range of 0001-01-01 to 9999-12-31, which Iceberg, Delta, and Parquet can store because they keep a date as a 32-bit day count from 1970-01-01
* *WHEN* a query emits that value as a row, as a `MIN` or `MAX` result, or as a `GROUP BY` key
* *THEN* the query MUST fail with an error that names the output column and the stored day count
* *AND* the scan MUST NOT return a substitute value, such as 1970-01-01, in place of the stored value
* *AND* a query whose emitted dates all lie inside the range, including 0001-01-01 and 9999-12-31, SHALL return them unchanged
* *AND* a query that tests the value in a filter without emitting it SHALL NOT fail with this error

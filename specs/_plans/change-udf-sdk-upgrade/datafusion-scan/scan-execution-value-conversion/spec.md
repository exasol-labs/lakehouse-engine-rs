# Feature: DataFusion Scan Value Conversion

Converts each output Arrow column to the SDK `Value` variant (or coerced Arrow type) the emit
boundary requires. This covers the Arrow-to-`Value` variant mapping, the emission of Exasol-
incompatible columns as JSON strings, and the coercion of a batch's Arrow types to the declared
EMITS `ExaType` before `emit_batch`. It owns the value-conversion boundary only — file
registration, filtering, LIMIT, streaming, and error handling belong to
`datafusion-scan/scan-execution`; Iceberg-to-Arrow and Arrow-to-Exasol type-mapping *rules* belong
to `datafusion-scan/type-mapping`.

## Background

<!-- DELTA:REMOVED -->
* `ExaType::Numeric`'s optional `precision` and `scale` are a wire artifact. A valid
  Exasol NUMERIC declaration always carries both within `Decimal128`'s range. An absent or
  out-of-range payload is drift, and both emit paths fail the call on it.
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
* `ExaType::Numeric` carries a REQUIRED `precision` and `scale` since `exasol-udf-sdk` 0.28.1,
  resolved at the handshake with an SLC-side default when the database sends none. An absent
  payload is no longer constructible, so the drift guard both emit paths carried for it is deleted
  rather than left unreachable. The out-of-range guard stays, because Exasol caps DECIMAL precision
  at 36 and a wider value is still drift.
* The scan can no longer distinguish a database-supplied NUMERIC payload from the SLC's default.
  That detection was unreachable in practice, because a valid Exasol NUMERIC declaration always
  carries both values, and the plan accepts the loss rather than re-deriving the pair from
  `ColumnInfo::type_name`.
* `ExaType` is a closed set of ten variants since 0.28.1. Upstream removed `TimestampTz`,
  `Geometry`, `HashType`, `IntervalYearToMonth` and `IntervalDayToSecond` as unreachable, confirmed
  by its own live canaries on 8.29.x, 2025.1.x and 2026.1.x. No emitted value changes, because
  Exasol never declared a UDF column at any of those types.
* `ExaType::Timestamp { precision }` makes the declared fractional-second precision readable at the
  emit boundary for the first time, so the coercion target follows it instead of being fixed at
  microsecond. The precision-to-`TimeUnit` table is owned by
  `datafusion-scan/type-mapping-timestamp-precision`, not restated here; this feature owns only the
  rule that the coercion target is read from the declaration.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch

* *GIVEN* a scan call whose generated `EMITS (...)` clause declares one Exasol type per output column
* *AND* a result Arrow batch whose column types diverge from those declarations, for example an `Int32` column declared `DECIMAL(20,0)`, a `Float32` column declared `DOUBLE PRECISION`, or a `Utf8View` column declared `VARCHAR(2000000)`
* *WHEN* the scan UDF processes the batch
* *THEN* the UDF SHALL read the declared type of output column `i` from `UdfContext::output_column(i)`, and MUST NOT read any declared type carried in the scan spec
* *AND* the UDF SHALL coerce each output column to the Arrow type that `emit_batch`'s strict IPC feed requires for the reported `ExaType`, before passing the batch to `emit_batch`, taking the DECIMAL binning from the reported variant rather than re-deriving it from a type string: `Int32` to `Int32`, `Int64` to `Int64`, and `Numeric { precision, scale }` to `Decimal128(precision, scale)`
* *AND* the remaining variants SHALL map as `Double` to `Float64`, `Boolean` to `Boolean`, `Date` to `Date32`, `Timestamp { precision }` to `Timestamp(unit, None)` where `unit` FOLLOWS the reported `precision` under the one mapping `datafusion-scan/type-mapping-timestamp-precision` owns, and the three that remain (`String`, `Char` and `Unsupported`) to `Utf8`, which subsumes `Utf8View`/`BinaryView` normalization and preserves the behavior the removed type-string path gave an unrecognized declaration
* *AND* the timestamp arm MUST NOT resolve a fixed `Microsecond` target at every precision, because `coerce_column` casts strictly (`safe: false`) and a microsecond target under a `TIMESTAMP(9)` declaration silently destroys the nanosecond digits of an Iceberg `timestamp_ns`/`timestamptz_ns` column, which `iceberg_primitive_to_arrow` already registers as Arrow `Timestamp(Nanosecond, _)`
* *AND* the timestamp arm MUST NOT carry its own precision-to-unit table, because the adapter's declaration producer reads the same table and two copies would let a declared `TIMESTAMP(9)` mean one width on the declaring side and another at the emit boundary
* *AND* the match over `ExaType` SHALL stay EXHAUSTIVE with no wildcard arm, so a variant added or removed upstream becomes a compile error here rather than a silent `Utf8` substitution
* *AND* the UDF SHALL fail the call on either emit path, naming the offending output column, when a `Numeric` column reports a `precision` or `scale` outside what `Decimal128` represents, and MUST NOT substitute `Utf8` or any other target type, because a valid Exasol NUMERIC declaration always carries a precision and a scale within that range
* *AND* a column already of the required Arrow type SHALL be passed through unchanged, on the zero-copy fast path
* *AND* the UDF SHALL fail the call, and MUST NOT emit the batch, when `output_column_count()` does not equal the batch's column count, naming both counts, or when `output_column(i)` returns an error for a column the batch carries, naming `i`
* *AND* the UDF MUST NOT fall back to a spec-carried or source-derived type in either failure case, because an absent declaration is drift rather than a legacy spec
<!-- /DELTA:CHANGED -->

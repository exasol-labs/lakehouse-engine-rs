# Feature: DataFusion Scan Value Conversion

Converts each output Arrow column to the SDK `Value` variant (or coerced Arrow type) the emit
boundary requires. This covers the Arrow-to-`Value` variant mapping, the emission of Exasol-
incompatible columns as JSON strings, and the coercion of a batch's Arrow types to the declared
EMITS `ExaType` before `emit_batch`. It owns the value-conversion boundary only — file
registration, filtering, LIMIT, streaming, and error handling belong to
`datafusion-scan/scan-execution`; Iceberg-to-Arrow and Arrow-to-Exasol type-mapping *rules* belong
to `scan-types/type-mapping`.

## Background

* A nested column is rendered to JSON at the Arrow COLUMN level, upstream of the per-value
  conversion. `scan-types/nested-json-rendering` owns the rendering, so the batch that
  reaches the emit boundary already carries `Utf8`. `arrow_value_at` therefore never receives
  a nested Arrow column.
* Only `Value::String` types cross the `.so` boundary on the value-conversion path; the raw-row path
  crosses as Arrow IPC bytes via `emit_batch` per `datafusion-scan/scan-execution`.
* Logical Iceberg-to-Arrow and Arrow-to-Exasol type mapping RULES are owned by
  `scan-types/type-mapping`; this feature owns the runtime conversion step that applies those
  rules to an in-flight batch at the emit boundary.
* `coerce_int96_tz = "UTC"` makes the decoded batch's physical Arrow type `Timestamp(Microsecond,
  "UTC")` regardless of the Iceberg column type. For an Iceberg `timestamp` (WITHOUT time zone)
  column the field-id (production) path's logical schema is `Timestamp(Microsecond, None)`; the
  None-vs-UTC difference is reconciled at the EMITS-coercion scenario below — see
  `datafusion-scan/scan-execution`'s INT96 decode scenario for the physical-decode side of this
  reconciliation.
* The scan reads its declared output type from `UdfContext::output_column(idx)`, which
  reports the `ExaType` the engine chose for the call-site `EMITS (...)` clause. The
  `ExaType` variant IS the bin Exasol chose (e.g. `Int32`, `Int64`, `Numeric`), so the
  scan reads the result rather than re-deriving it from a type string.
* The declared-type list is not optional. Every scan call has a call-site `EMITS` clause.
  An absent or wrong-arity declaration is drift and is reported rather than worked around.
* `ExaType::Numeric` carries a REQUIRED `precision` and `scale` since `exasol-udf-sdk` 0.28.1,
  resolved at the handshake with an SLC-side default when the database sends none. An absent
  payload is not constructible, so neither emit path guards against one. The out-of-range guard
  stays, because Exasol caps DECIMAL precision at 36 and a wider value is still drift.
* The scan cannot distinguish a database-supplied NUMERIC payload from the SLC's default. That
  detection was unreachable in practice, because a valid Exasol NUMERIC declaration always carries
  both values, and the loss is accepted rather than re-deriving the pair from
  `ColumnInfo::type_name`.
* `ExaType` is a closed set of ten variants since 0.28.1. Upstream removed `TimestampTz`,
  `Geometry`, `HashType`, `IntervalYearToMonth` and `IntervalDayToSecond` as unreachable, confirmed
  by its own live canaries on 8.29.x, 2025.1.x and 2026.1.x. No emitted value changes, because
  Exasol never declared a UDF column at any of those types.
* `ExaType::Timestamp { precision }` makes the declared fractional-second precision readable at the
  emit boundary, so the coercion target follows it rather than being fixed at microsecond. The
  precision-to-`TimeUnit` table belongs to `scan-types/type-mapping-timestamp-precision`. This
  feature owns only the rule that the coercion target is read from the declaration.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A DATE value outside Exasol's DATE range fails the query

* *GIVEN* a table whose `date` column holds a value outside Exasol's DATE range of 0001-01-01 to 9999-12-31, which Iceberg, Delta, and Parquet can store because they keep a date as a 32-bit day count from 1970-01-01
* *WHEN* a query emits that value as a row, as a `MIN` or `MAX` result, or as a `GROUP BY` key
* *THEN* the query MUST fail with an error that names the output column and the stored day count
* *AND* the scan MUST NOT return a substitute value, such as 1970-01-01, in place of the stored value
* *AND* a query whose emitted dates all lie inside the range, including 0001-01-01 and 9999-12-31, SHALL return them unchanged
* *AND* a query that tests the value in a filter without emitting it SHALL NOT fail with this error
<!-- /DELTA:NEW -->

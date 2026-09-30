# Feature: Type Mapping Live Matrix

Proves the type mapping against a live Exasol on every Parquet-file source. One table-driven E2E matrix declares and reads a column of every mapped type, and checks the declared Exasol type and the returned values or the plan-time refusal.

## Background

* The matrix verifies `datafusion-scan/type-mapping`, `datafusion-scan/nested-json-rendering`, and `vs-adapter/binary-column-refusal` against a live Exasol.
* The matrix is one data table in `crates/lakehouse-engine/tests/common/type_matrix.rs`. Each row names a column, the type each source writes or declares for it, its expected Exasol declaration, and its expected outcome: the returned values or the refusal reason.
* The sources are DIRECT_STORAGE (files written by `arrow-rs` and `parquet`), Iceberg REST (files written by `iceberg-rust`), and Glue (files written by `arrow-rs`, typed by Hive type strings).
* A source omits a row that it cannot write or declare, and the row states why:
  * Iceberg has no 8-bit, 16-bit, or unsigned integer, `LargeUtf8`, `Decimal256`, `Time32`, `Duration`, `Interval`, or `ENUM` type. It has one `binary` type, and `iceberg-rust` cannot write a nested `LargeBinary` as `Binary`.
  * Hive has no type string for an unsigned integer, `LargeUtf8`, `Decimal256`, a time, `Duration`, `Interval`, `LargeBinary`, `FixedSizeBinary`, `ENUM`, or `UUID`.
  * Iceberg and Glue omit `INT96`, because `iceberg-rust` and `arrow-rs` write no `INT96`.
* Each source holds a `binary_values` table whose one data column holds bytes that are not valid UTF-8, apart from the `all_types` table. Direct storage adds an `annotated` table, written through `parquet`'s `SerializedFileWriter`, for `ENUM`, `UUID`, `INT96`, a nested `ENUM` member, and an unannotated `BYTE_ARRAY` with no embedded Arrow schema.

## Scenarios

### Scenario: Every mapped type declares and returns as its matrix row states on each Parquet-file source

* *GIVEN* the matrix tables on DIRECT_STORAGE, on the Iceberg REST catalog, and in a Glue database, with a column for every row the source can write
* *WHEN* the suite reads each column's declared type from `SYS.EXA_ALL_COLUMNS`, and runs `SELECT id, <column> ... ORDER BY id` and `SELECT id ...` for each column
* *THEN* each column SHALL declare the Exasol type of its row, per `datafusion-scan/type-mapping`
* *AND* `SELECT id, <column>` SHALL return the row's values, or fail at plan time with an error that contains the row's refusal reason, or, for a catalog `string` column over bytes that are not valid UTF-8, fail the query as `vs-adapter/binary-column-refusal` requires
* *AND* a row's values SHALL be the native value for a compatible type, the JSON document of `datafusion-scan/nested-json-rendering` for a nested type, and the Arrow display text for any other non-binary type
* *AND* `SELECT id` SHALL return every id of every table, and the suite SHALL check every row and report every mismatch in one failure

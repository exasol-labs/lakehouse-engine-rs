# Feature: Type Mapping Live Coverage

Proves the type mapping against a live Exasol on every source technology. Each technology's existing E2E suite declares and reads a column of every type that source can write, in a fixture table next to its other fixtures, and checks the declared Exasol type and the returned values or the plan-time refusal.

## Background

* The coverage verifies `datafusion-scan/type-mapping`, `datafusion-scan/nested-json-rendering`, `vs-adapter/delta-type-mapping`, `vs-adapter/glue-hive-type-mapping`, and `vs-adapter/binary-column-refusal` against a live Exasol.
* Each technology's suite reads its all-types tables through the virtual schema that suite already creates. The coverage adds no virtual schema and no CONNECTION.
* An all-types table holds the ids 1, 2, and 3. Rows 1 and 2 carry values, and row 3 is NULL in every column but `id`.
* Each suite compares a table's declared types, read from `SYS.EXA_ALL_COLUMNS`, with its exact expected list, compares the values of one `SELECT ID, <columns> ... ORDER BY ID` over the readable columns with the exact expected values, and reads each refused column alone. The suites share the column values their fixtures write.
* A type that another table of the same suite already reads is not repeated:
  * On direct storage, `events` covers `Int64`, `Utf8`, `Float64`, `Boolean`, `Date32`, `Timestamp`, and `Decimal128(10,2)`, and `complex` covers `List`, `Struct`, and `Map`.
  * On the Iceberg REST catalog, `events` covers `long`, `string`, `double`, `date`, and `timestamp`, and `complex_types_probe` covers `list`, `struct`, and `map`.
  * Through Unity Catalog, `stats_all_types` covers every Delta type but a decimal wider than Exasol's 36 digits and a `binary` struct member, and `sales_parquet` covers a Parquet table's `long` and `double`.
* A source omits a type that it cannot write or declare:
  * Iceberg has no 8-bit, 16-bit, or unsigned integer, `LargeUtf8`, `Decimal256`, `Time32`, `Duration`, `Interval`, `ENUM`, or `BSON` type, and `iceberg-rust` writes no `INT96`.
  * Delta and Spark have no unsigned integer, `LargeUtf8`, `Decimal256`, time, `Duration`, `Interval`, fixed-length binary, `ENUM`, or `UUID` type.
  * Hive has no type string for an unsigned integer, `LargeUtf8`, `Decimal256`, a time, `Duration`, a fixed-length binary, `ENUM`, or `UUID`, and `arrow-rs` writes no `INT96`.
* A catalog-declared timestamp declares the precision `datafusion-scan/type-mapping-timestamp-precision` gates on the engine version. Its fixture values are exact to the millisecond, so they read the same at every gated precision.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: Every type a Parquet file can carry declares and returns its mapped value on direct storage

* *GIVEN* the direct-storage directory `all_types`, written by `arrow-rs`, with an `Int8`, `Int16`, `Int32`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, `LargeUtf8`, `Decimal128(38,10)`, `Decimal256(50,2)`, `Time32`, `Time64`, `Duration`, `Interval(DayTime)`, `Binary`, `LargeBinary`, `FixedSizeBinary(16)`, and `Struct<x: Binary>` column
* *AND* the directory `annotated_types`, written by `parquet` with no embedded Arrow schema, with an `ENUM`, a `BSON`, a `UUID`, an `INT96`, an unannotated `BYTE_ARRAY`, and a struct column whose member is `ENUM`-annotated
* *WHEN* `e2e_direct_storage_test` checks both tables through its direct-storage virtual schema
* *THEN* each integer column SHALL declare the `DECIMAL(p,0)` of `datafusion-scan/type-mapping` and return its values, `Float32` SHALL declare `DOUBLE`, and `INT96` SHALL declare `TIMESTAMP(3)`
* *AND* `LargeUtf8` and `ENUM` SHALL return their text, and the decimals that exceed Exasol's `DECIMAL` domain and the time, duration, and interval columns SHALL declare `VARCHAR(2000000)` and return their text
* *AND* every binary, fixed-length, `UUID`, `BSON`, and unannotated `BYTE_ARRAY` column, and both struct columns, SHALL declare `VARCHAR(2000000)` and fail at plan time per `vs-adapter/binary-column-refusal`
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Every Iceberg type declares and returns its mapped value through the Iceberg REST catalog

* *GIVEN* the Iceberg tables the suite seeds in its REST catalog namespace: `all_types`, with an `int`, `float`, `decimal(10,2)`, `decimal(38,10)`, `boolean`, `time`, `timestamptz`, `timestamp_ns`, `binary`, `fixed(16)`, `uuid`, and `struct<x: binary>` column, and `binary_values`, whose `string` column holds bytes that are not valid UTF-8
* *WHEN* `e2e_scan_test` checks both tables through its Iceberg REST virtual schema
* *THEN* `int`, `float`, `decimal(10,2)`, and `boolean` SHALL declare `DECIMAL(10,0)`, `DOUBLE`, `DECIMAL(10,2)`, and `BOOLEAN`, `decimal(38,10)` and `time` SHALL declare `VARCHAR(2000000)`, and `timestamptz` and `timestamp_ns` SHALL declare their gated precision, each returning its values
* *AND* `binary`, `fixed(16)`, `uuid`, and the struct column SHALL declare `VARCHAR(2000000)` and fail at plan time per `vs-adapter/binary-column-refusal`
* *AND* reading the `binary_values` column SHALL fail the query, naming the invalid UTF-8 data
* *AND* the `events` table SHALL declare `ID DECIMAL(20,0)`, `NAME VARCHAR(2000000)`, `SCORE DOUBLE`, `EVENT_DATE DATE`, and `EVENT_TS` at its gated precision, exactly, and return its values
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Every Delta type declares and returns its mapped value through Unity Catalog

* *GIVEN* the Delta table `delta_extra_types`, registered in Unity Catalog, whose one-commit log declares a `decimal(38,10)` and a `struct<x: binary>` column
* *WHEN* `e2e_unity_test` checks it through its Unity Catalog virtual schema
* *THEN* `decimal(38,10)` SHALL declare `VARCHAR(2000000)` and return its text
* *AND* the struct column SHALL declare `VARCHAR(2000000)` and fail at plan time per `vs-adapter/binary-column-refusal`
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Every Spark type a Unity Parquet table declares returns its mapped value

* *GIVEN* the Unity Catalog Parquet table `all_types_parquet`, whose columns declare through their `type_json` a `byte`, `short`, `integer`, `float`, `boolean`, `string`, `decimal(10,2)`, `decimal(38,10)`, `date`, `timestamp`, `timestamp_ntz`, `binary`, `array<integer>`, `map<string,integer>`, `struct<a: integer, b: string>`, `struct<x: binary>`, and `variant` column
* *WHEN* `e2e_unity_test` checks it through its Unity Catalog virtual schema
* *THEN* each native column SHALL declare the Exasol type of `vs-adapter/delta-type-mapping` and return its values, both timestamps at their gated precision
* *AND* `decimal(38,10)` SHALL declare `VARCHAR(2000000)` and return its text, and the `array`, `map`, and `struct` columns SHALL return the JSON documents of `datafusion-scan/nested-json-rendering`
* *AND* the `binary`, `struct<x: binary>`, and `variant` columns SHALL declare `VARCHAR(2000000)` and fail at plan time naming the column and its type
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Every Hive type declares and returns its mapped value on a Glue Parquet table

* *GIVEN* the Glue Parquet table `all_types`, which declares every Hive type of `vs-adapter/glue-hive-type-mapping`, and the table `binary_values`, which declares `c_bytes string` over a `BYTE_ARRAY` with no annotation whose bytes are not valid UTF-8
* *WHEN* `e2e_glue_test` checks both tables through its Glue virtual schema
* *THEN* each scalar Hive type SHALL declare its Exasol type and return its values, `timestamp` at its gated precision, and a `string` column over an unannotated `BYTE_ARRAY` SHALL return its text per `vs-adapter/binary-column-refusal`
* *AND* each `array`, `map`, and `struct` column SHALL return the JSON document of `datafusion-scan/nested-json-rendering`
* *AND* the `binary`, `struct<b:binary>`, `uniontype`, `interval_day_time`, malformed, and empty-type columns SHALL fail at plan time per `vs-adapter/glue-hive-type-mapping`
* *AND* reading the `binary_values` column SHALL fail the query per `vs-adapter/binary-column-refusal`, naming the invalid UTF-8 sequence
<!-- /DELTA:REMOVED -->

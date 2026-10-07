# Feature: Type Mapping Live Coverage

Proves the type mapping against a live Exasol on every source technology. Each technology's existing E2E suite declares and reads a column of every type that source can write, in a fixture table next to its other fixtures, and checks the declared Exasol type and the returned values or the plan-time refusal.

## Background

* The coverage verifies `scan-types/type-mapping`, `scan-types/nested-json-rendering`, `delta/delta-type-mapping`, `glue/glue-hive-type-mapping`, and `vs-adapter/binary-column-refusal` against a live Exasol.
* A type that another table of the same suite already reads is not repeated:
  * On direct storage, `events` covers `Int64`, `Utf8`, `Float64`, `Boolean`, `Date32`, `Timestamp`, and `Decimal128(10,2)`, and `complex` covers `List`, `Struct`, and `Map`.
  * On the Iceberg REST catalog, `events` covers `long`, `string`, `double`, `date`, and `timestamp`, and `complex_types_probe` covers `list`, `struct`, and `map`.
  * Through Unity Catalog, `stats_all_types` covers every Delta type but the decimal matrix and a `binary` struct member, and `sales_parquet` covers a Parquet table's `long` and `double`.
* The decimal matrix is `decimal(10,2)`, `decimal(18,0)`, `decimal(36,6)`, and `decimal(38,10)`. Text order inverts the numeric order of the `decimal(18,0)` values, so a column declared `VARCHAR` fails its ordering check.
* Unity fixtures register their columns in the shapes real Unity clients write: Databricks reports `type_precision` and `type_scale` as 0, and Unity's Spark connector omits them (#463).
* A source omits a type that it cannot write or declare:
  * Iceberg has no 8-bit, 16-bit, or unsigned integer, `LargeUtf8`, `Decimal256`, `Time32`, `Duration`, `Interval`, `ENUM`, or `BSON` type, and `iceberg-rust` writes no `INT96`.
  * Delta and Spark have no unsigned integer, `LargeUtf8`, `Decimal256`, time, `Duration`, `Interval`, fixed-length binary, `ENUM`, or `UUID` type.
  * Hive has no type string for an unsigned integer, `LargeUtf8`, `Decimal256`, a time, `Duration`, a fixed-length binary, `ENUM`, or `UUID`, and `arrow-rs` writes no `INT96`.
* A catalog-declared timestamp declares the precision `scan-types/type-mapping-timestamp-precision` gates on the engine version.

## Scenarios

### Scenario: Every type a Parquet file can carry declares and returns its mapped value on direct storage

* *GIVEN* the direct-storage directory `all_types`, written by `arrow-rs`, with an `Int8`, `Int16`, `Int32`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, `LargeUtf8`, `Decimal128(18,0)`, `Decimal128(36,6)`, `Decimal128(38,10)`, `Decimal256(50,2)`, `Time32`, `Time64`, `Duration`, `Interval(DayTime)`, `Binary`, `LargeBinary`, `FixedSizeBinary(16)`, and `Struct<x: Binary>` column
* *AND* the directory `annotated_types`, written by `parquet` with no embedded Arrow schema, with an `ENUM`, a `BSON`, a `UUID`, an `INT96`, an unannotated `BYTE_ARRAY`, and a struct column whose member is `ENUM`-annotated
* *WHEN* `e2e_direct_storage_test` checks both tables through its direct-storage virtual schema
* *THEN* each integer column SHALL declare the `DECIMAL(p,0)` of `scan-types/type-mapping` and return its values, `Float32` SHALL declare `DOUBLE`, `INT96` SHALL declare `TIMESTAMP(3)`, and `Decimal128(18,0)` and `Decimal128(36,6)` SHALL declare `DECIMAL(p,s)`, return their values, and order and aggregate numerically
* *AND* `LargeUtf8` and `ENUM` SHALL return their text, and the decimals that exceed Exasol's `DECIMAL` domain and the time, duration, and interval columns SHALL declare `VARCHAR(2000000)` and return their text
* *AND* every binary, fixed-length, `UUID`, `BSON`, and unannotated `BYTE_ARRAY` column, and both struct columns, SHALL declare `VARCHAR(2000000)` and fail at plan time per `vs-adapter/binary-column-refusal`
* *AND* the `events` table SHALL declare the Exasol type `scan-types/type-mapping` maps for each of its `Int64`, `Utf8`, `Float64`, `Boolean`, `Date32`, `Timestamp`, and `Decimal128(10,2)` columns and return the written values, and the `complex` table SHALL declare its `List`, `Struct`, and `Map` columns `VARCHAR(2000000)` and return the JSON documents of `scan-types/nested-json-rendering`

### Scenario: Every Iceberg type declares and returns its mapped value through the Iceberg REST catalog

* *GIVEN* the Iceberg tables the suite seeds in its REST catalog namespace: `all_types`, with an `int`, `float`, the decimal matrix, `boolean`, `time`, `timestamptz`, `timestamp_ns`, `binary`, `fixed(16)`, `uuid`, and `struct<x: binary>` column, and `binary_values`, whose `string` column holds bytes that are not valid UTF-8
* *WHEN* `e2e_scan_test` checks both tables through its Iceberg REST virtual schema
* *THEN* `int`, `float`, and `boolean` SHALL declare `DECIMAL(10,0)`, `DOUBLE`, and `BOOLEAN`, `decimal(10,2)`, `decimal(18,0)`, and `decimal(36,6)` SHALL declare `DECIMAL(p,s)` and order and aggregate numerically, `decimal(38,10)` and `time` SHALL declare `VARCHAR(2000000)`, and `timestamptz` and `timestamp_ns` SHALL declare their gated precision, each returning its values
* *AND* `binary`, `fixed(16)`, `uuid`, and the struct column SHALL declare `VARCHAR(2000000)` and fail at plan time per `vs-adapter/binary-column-refusal`
* *AND* reading the `binary_values` column SHALL fail the query, naming the invalid UTF-8 data
* *AND* the `events` table SHALL declare `ID DECIMAL(20,0)`, `NAME VARCHAR(2000000)`, `SCORE DOUBLE`, `EVENT_DATE DATE`, and `EVENT_TS` at its gated precision, exactly, and return its values

### Scenario: Every Delta type declares and returns its mapped value through Unity Catalog

* *GIVEN* the Delta table `delta_extra_types`, registered in Unity Catalog in the Databricks shape, whose one-commit log declares the decimal matrix and a `struct<x: binary>` column
* *WHEN* `e2e_unity_test` checks it through its Unity Catalog virtual schema
* *THEN* `decimal(10,2)`, `decimal(18,0)`, and `decimal(36,6)` SHALL declare `DECIMAL(p,s)`, return their values, and order and aggregate numerically, and `decimal(38,10)` SHALL declare `VARCHAR(2000000)` and return its text
* *AND* the struct column SHALL declare `VARCHAR(2000000)` and fail at plan time per `vs-adapter/binary-column-refusal`
* *AND* the `stats_all_types` table SHALL declare each mappable column at the Exasol type of `delta/delta-type-mapping` and return its values, SHALL return its `array`, `map`, and `struct` columns as the JSON documents of `scan-types/nested-json-rendering`, and SHALL refuse `binary_col` per `vs-adapter/binary-column-refusal`

### Scenario: Every Spark type a Unity Parquet table declares returns its mapped value

* *GIVEN* the Unity Catalog Parquet table `all_types_parquet`, registered in the Spark connector's shape, whose columns declare through their `type_json` a `byte`, `short`, `integer`, `float`, `boolean`, `string`, the decimal matrix, `date`, `timestamp`, `timestamp_ntz`, `binary`, `array<integer>`, `map<string,integer>`, `struct<a: integer, b: string>`, `struct<x: binary>`, and `variant` column
* *WHEN* `e2e_unity_test` checks it through its Unity Catalog virtual schema
* *THEN* each native column SHALL declare the Exasol type of `delta/delta-type-mapping` and return its values, both timestamps at their gated precision
* *AND* `decimal(10,2)`, `decimal(18,0)`, and `decimal(36,6)` SHALL declare `DECIMAL(p,s)` and order and aggregate numerically, `decimal(38,10)` SHALL declare `VARCHAR(2000000)` and return its text, and the `array`, `map`, and `struct` columns SHALL return the JSON documents of `scan-types/nested-json-rendering`
* *AND* the `binary`, `struct<x: binary>`, and `variant` columns SHALL declare `VARCHAR(2000000)` and fail at plan time naming the column and its type
* *AND* the `sales_parquet` table SHALL declare `ID` as `DECIMAL(20,0)`, `AMOUNT` as `DOUBLE`, `YEAR` as `DECIMAL(10,0)`, and `REGION` as `VARCHAR(2000000)`, in that order, and return its values

### Scenario: Every Hive type declares and returns its mapped value on a Glue Parquet table

* *GIVEN* the Glue Parquet table `all_types`, which declares every Hive type of `glue/glue-hive-type-mapping` and the decimal matrix, and the table `binary_values`, which declares `c_bytes string` over a `BYTE_ARRAY` with no annotation whose bytes are not valid UTF-8
* *WHEN* `e2e_glue_test` checks both tables through its Glue virtual schema
* *THEN* each scalar Hive type SHALL declare its Exasol type and return its values, `timestamp` at its gated precision, and a `string` column over an unannotated `BYTE_ARRAY` SHALL return its text per `vs-adapter/binary-column-refusal`, and `decimal(18,0)` SHALL order and aggregate numerically
* *AND* each `array`, `map`, and `struct` column SHALL return the JSON document of `scan-types/nested-json-rendering`
* *AND* the `binary`, `struct<b:binary>`, `uniontype`, `interval_day_time`, malformed, and empty-type columns SHALL fail at plan time per `glue/glue-hive-type-mapping`
* *AND* reading the `binary_values` column SHALL fail the query per `vs-adapter/binary-column-refusal`, naming the invalid UTF-8 sequence

### Scenario: Decimals declare DECIMAL and order numerically through Lakekeeper and on ADLS Gen2

* *GIVEN* the Iceberg `all_types` table seeded in Lakekeeper's static-credentials warehouse, and the direct-storage `events` directory on ADLS Gen2 with a `Decimal128(18,0)` column
* *WHEN* `e2e_lakekeeper_test` and `e2e_azure_test` check them
* *THEN* the Lakekeeper `decimal(10,2)`, `decimal(18,0)`, and `decimal(36,6)` columns SHALL declare `DECIMAL(p,s)` and return their values
* *AND* the ADLS column SHALL declare `DECIMAL(18,0)`, and both `decimal(18,0)` columns SHALL order and aggregate numerically

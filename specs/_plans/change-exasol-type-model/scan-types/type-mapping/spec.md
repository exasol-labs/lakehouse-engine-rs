# Feature: DataFusion-to-Exasol Type Mapping

Defines the single mapping from DataFusion/Arrow column types to Exasol SQL types, and the companion Iceberg-to-Arrow mapping that builds the logical schema the scan registers. Types Exasol supports map directly. Types it cannot represent (lists, structs, maps, and out-of-range decimals) become `VARCHAR` JSON strings. The same mapping governs the `createVirtualSchema` declaration, the Arrow-to-Value conversion in the scan, and the logical schema in the scan spec.

<!-- DELTA:CHANGED -->
## Background

* Exasol represents BOOLEAN, DECIMAL(1≤p≤36, 0≤s≤p), DOUBLE PRECISION, VARCHAR(n≤2,000,000), CHAR(n≤2,000), DATE, TIMESTAMP(p≤9), TIMESTAMP WITH LOCAL TIME ZONE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, GEOMETRY, and HASHTYPE. It has no array, list, struct, or map type. `TIMESTAMP WITH LOCAL TIME ZONE` is not a valid UDF `EMITS` output type (`sqlCode 22002: Column type not supported`), so this mapping never targets it.
* One mapping governs three places: the `createVirtualSchema` declaration (Arrow type to declared Exasol type), the scan UDF's conversion of Arrow values to SDK values, and the logical schema in the scan spec (Iceberg type to Arrow type).
* Every decimal maps to `DECIMAL(p, s)` only inside Exasol's domain `1 ≤ p ≤ 36` and `0 ≤ s ≤ p`, and falls back to `VARCHAR(2000000)` otherwise. This holds for a catalog-declared decimal (an Iceberg or Unity Catalog `DECIMAL`) and for an Arrow `Decimal128`. A Parquet decimal always has `0 ≤ s ≤ p`.
* Compatible Arrow types map directly:

  | Arrow type | Exasol type | Value variant |
  |---|---|---|
  | Boolean | BOOLEAN | `Value::Bool` |
  | Int8 / Int16 / Int32 | DECIMAL(precision, 0) | numeric |
  | Int64 / UInt32 / UInt64 | DECIMAL(20, 0) | numeric |
  | UInt8 / UInt16 | DECIMAL(precision, 0) | numeric |
  | Float32 / Float64 | DOUBLE PRECISION | `Value::Double` |
  | Utf8 / LargeUtf8 | VARCHAR(2000000) | `Value::String` |
  | Date32 | DATE | date |
  | Timestamp(_, _) | TIMESTAMP | timestamp |
  | Decimal128(p,s) where 1≤p≤36 and 0≤s≤p | DECIMAL(p, s) | numeric |
  | Decimal128(p,s) where p>36, s<0, or s>p | VARCHAR(2000000) via JSON | `Value::String` |

* Incompatible Arrow types (List, LargeList, FixedSizeList, Struct, Map, Union, Binary, LargeBinary, BinaryView, FixedSizeBinary, Duration, Time32, Time64, Interval, Decimal256) are declared `VARCHAR(2000000)`, except `FixedSizeBinary(L)`, which is declared `VARCHAR(2L)`. List, LargeList, FixedSizeList, Struct, and Map keep the logical Arrow type `Utf8` and render as JSON (`scan-types/nested-json-rendering`). The binary types render as lowercase hex (`vs-adapter/binary-column-rendering`). The scan UDF casts every other member to VARCHAR. An Arrow null maps to `Value::Null` regardless of column type.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: In-range Decimal128 maps to a precise Exasol DECIMAL

* *GIVEN* an Arrow `Decimal128(p, s)` column with `1 ≤ p ≤ 36` and `0 ≤ s ≤ p`
* *WHEN* the type is resolved for the Exasol schema
* *THEN* the resolver SHALL return `DECIMAL(p, s)`, preserving the source precision and scale
* *AND* the scan SHALL emit each value as a number, without JSON serialization
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Out-of-range Decimal128 falls back to VARCHAR via JSON

* *GIVEN* an Arrow `Decimal128(p, s)` column with `p > 36`, a negative `s`, or `s > p`
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL declare the column as `VARCHAR(2000000)`
* *AND* the scan SHALL emit each value as a JSON string
* *AND* the resolver MUST NOT declare a `DECIMAL` that Exasol rejects, such as `DECIMAL(10,-2)` or `DECIMAL(5,10)`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Incompatible Arrow types are serialized to JSON VARCHAR

* *GIVEN* a column of an incompatible Arrow type, either a NESTED type (`List`, `LargeList`, `FixedSizeList`, `Struct`, `Map`) or a NON-NESTED one (`Binary`, `LargeBinary`, `FixedSizeBinary`, `Union`, `Duration`, `Time32`, `Time64`, `Interval`, `Decimal256`, or an out-of-range `Decimal128`)
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL declare the column as `VARCHAR(2000000)` for EVERY member of both halves, except a `FixedSizeBinary(L)` column, which SHALL be declared `VARCHAR(2L)`
* *AND* a NESTED column's value SHALL be rendered as a valid JSON document per `scan-types/nested-json-rendering`, which owns that contract
* *AND* a NON-NESTED column's value, other than a `Binary`, `LargeBinary`, or `FixedSizeBinary` one, SHALL keep its recorded `CAST(col AS VARCHAR)` Arrow-display rendering byte-identical, and this feature MUST NOT claim strict JSON conformance for it
* *AND* a `Binary`, `LargeBinary`, `BinaryView`, or `FixedSizeBinary` column's value SHALL render as lowercase hex per `vs-adapter/binary-column-rendering`
* *AND* the converter MUST NOT emit any array, list, struct, or map `Value` for either half
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A Parquet-sourced column maps through the Arrow-input direction

* *GIVEN* a column whose source type is an Arrow type folded from one or more Parquet footers, rather than an Iceberg primitive or a Unity Catalog type name
* *WHEN* the adapter declares that column's Exasol type at `createVirtualSchema`
* *THEN* the declared type SHALL be the Arrow-input answer this feature specifies for that Arrow type, except a timestamp, which `scan-types/type-mapping-timestamp-precision` declares
* *AND* a column whose source type tag the adapter cannot read SHALL be declared `VARCHAR(2000000)` rather than fail the statement
* *AND* the column's declared Exasol type and its logical Arrow type in the scan SHALL agree: a column declared `VARCHAR(2000000)` SHALL be read as a string-family Arrow type, not as its footer type
* *AND* a nested Parquet struct, list, or map column SHALL render as JSON per `scan-types/nested-json-rendering`
* *AND* a Parquet decimal, whose scale lies between zero and the precision, SHALL map by the in-range and out-of-range `Decimal128` scenarios
<!-- /DELTA:CHANGED -->

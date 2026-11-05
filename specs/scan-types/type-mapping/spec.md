# Feature: DataFusion-to-Exasol Type Mapping

Defines the single mapping from DataFusion/Arrow column types to Exasol SQL types, and the companion Iceberg-to-Arrow mapping that builds the logical schema the scan registers. Types Exasol supports map directly. Types it cannot represent (lists, structs, maps, and out-of-range decimals) become `VARCHAR` JSON strings. The same mapping governs the `createVirtualSchema` declaration, the Arrow-to-Value conversion in the scan, and the logical schema in the scan spec.

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

## Scenarios

### Scenario: Compatible Arrow types map to their Exasol type

* *GIVEN* a column of a compatible Arrow type from the mapping table
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL return the Exasol type given by the mapping table for that Arrow type
* *AND* the converter SHALL produce the `Value` variant given by the mapping table
* *AND* an Arrow null SHALL convert to `Value::Null`

### Scenario: In-range Decimal128 maps to a precise Exasol DECIMAL

* *GIVEN* an Arrow `Decimal128(p, s)` column with `1 ≤ p ≤ 36` and `0 ≤ s ≤ p`
* *WHEN* the type is resolved for the Exasol schema
* *THEN* the resolver SHALL return `DECIMAL(p, s)`, preserving the source precision and scale
* *AND* the scan SHALL emit each value as a number, without JSON serialization

### Scenario: Out-of-range Decimal128 falls back to VARCHAR via JSON

* *GIVEN* an Arrow `Decimal128(p, s)` column with `p > 36`, a negative `s`, or `s > p`
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL declare the column as `VARCHAR(2000000)`
* *AND* the scan SHALL emit each value as a JSON string
* *AND* the resolver MUST NOT declare a `DECIMAL` that Exasol rejects, such as `DECIMAL(10,-2)` or `DECIMAL(5,10)`

### Scenario: Incompatible Arrow types are serialized to JSON VARCHAR

* *GIVEN* a column of an incompatible Arrow type, either a NESTED type (`List`, `LargeList`, `FixedSizeList`, `Struct`, `Map`) or a NON-NESTED one (`Binary`, `LargeBinary`, `FixedSizeBinary`, `Union`, `Duration`, `Time32`, `Time64`, `Interval`, `Decimal256`, or an out-of-range `Decimal128`)
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL declare the column as `VARCHAR(2000000)` for EVERY member of both halves, except a `FixedSizeBinary(L)` column, which SHALL be declared `VARCHAR(2L)`
* *AND* a NESTED column's value SHALL be rendered as a valid JSON document per `scan-types/nested-json-rendering`, which owns that contract
* *AND* a NON-NESTED column's value, other than a `Binary`, `LargeBinary`, or `FixedSizeBinary` one, SHALL keep its recorded `CAST(col AS VARCHAR)` Arrow-display rendering byte-identical, and this feature MUST NOT claim strict JSON conformance for it
* *AND* a `Binary`, `LargeBinary`, `BinaryView`, or `FixedSizeBinary` column's value SHALL render as lowercase hex per `vs-adapter/binary-column-rendering`
* *AND* the converter MUST NOT emit any array, list, struct, or map `Value` for either half

### Scenario: A mixed-column Parquet file round-trips through schema mapping and scan

* *GIVEN* an Iceberg Parquet file with both compatible columns (int, string, timestamp) and incompatible columns (a POPULATED list and a POPULATED struct, never a zero-field struct, which sidesteps the field-wise path this scenario exists to cover)
* *WHEN* `createVirtualSchema` declares the table and the scan UDF reads the file
* *THEN* the declared schema SHALL type the compatible columns by the mapping table and the incompatible columns as `VARCHAR(2000000)`
* *AND* the scan SHALL emit the compatible columns as their mapped `Value` variants and the list and struct columns as valid JSON documents that parse, per `scan-types/nested-json-rendering`
* *AND* every emitted column value SHALL be of an Exasol-compatible type

### Scenario: A Parquet-sourced column maps through the Arrow-input direction

* *GIVEN* a column whose source type is an Arrow type folded from one or more Parquet footers, rather than an Iceberg primitive or a Unity Catalog type name
* *WHEN* the adapter declares that column's Exasol type at `createVirtualSchema`
* *THEN* the declared type SHALL be the Arrow-input answer this feature specifies for that Arrow type, except a timestamp, which `scan-types/type-mapping-timestamp-precision` declares
* *AND* a column whose source type tag the adapter cannot read SHALL be declared `VARCHAR(2000000)` rather than fail the statement
* *AND* the column's declared Exasol type and its logical Arrow type in the scan SHALL agree: a column declared `VARCHAR(2000000)` SHALL be read as a string-family Arrow type, not as its footer type
* *AND* a nested Parquet struct, list, or map column SHALL render as JSON per `scan-types/nested-json-rendering`

### Scenario: The scan-spec tag vocabulary covers every Arrow type the compatible-type classifier admits

* *GIVEN* the Arrow-input classifier that decides which Arrow types Exasol represents directly, and the compact scan-spec Arrow-type tag vocabulary that spells an Arrow type as a string for the logical schema and for the neutral Parquet column source
* *WHEN* a producer renders a tag for an Arrow type that classifier admits
* *THEN* the tag vocabulary SHALL carry ONE distinct entry for EVERY Arrow type the classifier admits, so the vocabulary's domain is the classifier's domain rather than one producer's
* *AND* that extension SHALL cover at least `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `LargeUtf8`, and `Timestamp` at EVERY `TimeUnit` in both the timezone-naive and the timezone-aware form, because the classifier returns a non-VARCHAR Exasol type for each of them while the recorded vocabulary spells none of them
* *AND* the renderer and the parser SHALL ROUND-TRIP every type in that domain, so an Arrow type rendered to a tag and parsed back SHALL equal the type it started as
* *AND* the renderer MUST NOT fall back to the string tag for a type the classifier ADMITS, because that fallback declares the column `VARCHAR(2000000)` and registers it as a string with no error anywhere, which is what a Spark-written `INT64 TIMESTAMP(MILLIS)` column would otherwise receive
* *AND* the string tag SHALL remain the answer for every type the classifier REFUSES, so a nested or otherwise unrepresentable type keeps its recorded JSON-VARCHAR path unchanged, except the binary types: `Binary`, `LargeBinary`, and `BinaryView` SHALL render the tag `binary`, and `FixedSizeBinary(L)` the tag `fixed_size_binary(L)`, so the scan reads the bytes and never decodes them as text
* *AND* every recorded tag spelling SHALL be UNCHANGED and every recorded parse answer SHALL stay byte-identical, so a scan spec written before this extension deserializes to the same Arrow types

### Scenario: A geospatial column is refused

* *GIVEN* an Iceberg `geometry` or `geography` column, or a direct-storage `GEOMETRY`- or `GEOGRAPHY`-annotated column, at any depth
* *WHEN* the pushdown plans a request that reads or emits the column
* *THEN* the request SHALL fail at plan time with a reason naming the column, the member when the column nests one, and the type
* *AND* the listing SHALL still declare the column `VARCHAR(2000000)`

# Feature: DataFusion-to-Exasol Type Mapping

Defines the single authoritative mapping from DataFusion/Arrow column types to Exasol
SQL types, so that every column an Iceberg table exposes is queryable through Exasol.
Types Exasol supports natively map directly; types Exasol cannot represent (vectors,
lists, structs, maps, binary, and other complex Arrow types) are serialized to JSON
strings and surfaced as `VARCHAR`, so result sets always come back in Exasol-compatible
types. The same mapping governs both the `createVirtualSchema` schema declaration and
the Arrow→`Value` conversion in the scan, keeping the declared column type and the
emitted value type in agreement.

## Background

* Exasol's representable types are: BOOLEAN, DECIMAL(p≤36, s≤36), DOUBLE PRECISION,
  VARCHAR(n≤2,000,000), CHAR(n≤2,000), DATE, TIMESTAMP(p≤9), TIMESTAMP WITH LOCAL TIME
  ZONE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, GEOMETRY, HASHTYPE. Exasol has
  no array, list, struct, or map type.
* The mapping is applied in two places that MUST stay consistent: the adapter's
  `createVirtualSchema` schema declaration (Arrow type → declared Exasol column type)
  and the scan UDF's Arrow `RecordBatch` → SDK `Value` conversion (Arrow value →
  `Value` variant).
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
  | Timestamp(_, None) | TIMESTAMP | timestamp |
  | Timestamp(_, Some(_)) | TIMESTAMP WITH LOCAL TIME ZONE | timestamp |
  | Decimal128(p,s) where p≤36 and s≤36 | DECIMAL(p, s) | numeric |
  | Decimal128(p,s) where p>36 or s>36 | VARCHAR(2000000) via JSON | `Value::String` |

* Incompatible Arrow types — List, LargeList, FixedSizeList, Struct, Map, Union, Binary,
  LargeBinary, FixedSizeBinary, Duration, Time32, Time64, Interval, Decimal256 — have no
  Exasol equivalent. They are serialized to a JSON string in the scan UDF (via DataFusion
  `CAST(col AS VARCHAR)` / `arrow_cast`) before conversion to `Value::String`, and
  declared as VARCHAR(2000000) in the schema response.
* An Arrow null maps to `Value::Null` regardless of column type.

## Scenarios

### Scenario: Compatible Arrow types map to their Exasol type

* *GIVEN* a column of a compatible Arrow type from the mapping table
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL return the Exasol type given by the mapping table for that Arrow type
* *AND* the converter SHALL produce the `Value` variant given by the mapping table
* *AND* an Arrow null SHALL convert to `Value::Null`

### Scenario: In-range Decimal128 maps to a precise Exasol DECIMAL

* *GIVEN* an Arrow `Decimal128(p, s)` column with `p ≤ 36` and `s ≤ 36`
* *WHEN* the type is resolved for the Exasol schema
* *THEN* the resolver SHALL return `DECIMAL(p, s)` preserving the source precision and scale
* *AND* the converter SHALL produce a numeric `Value` without JSON serialization

### Scenario: Out-of-range Decimal128 falls back to VARCHAR via JSON

* *GIVEN* an Arrow `Decimal128(p, s)` column with `p > 36` or `s > 36`
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL declare the column as `VARCHAR(2000000)`
* *AND* the converter SHALL serialize the value to a JSON string and produce `Value::String`

### Scenario: Incompatible Arrow types are serialized to JSON VARCHAR

* *GIVEN* a column of an incompatible Arrow type (List, Struct, Map, or Binary)
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL declare the column as `VARCHAR(2000000)`
* *AND* the converter SHALL serialize the Arrow value to a JSON string and produce `Value::String`
* *AND* the converter MUST NOT emit any array, list, struct, or map `Value`

### Scenario: A mixed-column Parquet file round-trips through schema mapping and scan

* *GIVEN* an Iceberg Parquet file with both compatible columns (int, string, timestamp) and incompatible columns (a list and a struct)
* *WHEN* `createVirtualSchema` declares the table and the scan UDF reads the file
* *THEN* the declared schema SHALL type the compatible columns by the mapping table and the incompatible columns as `VARCHAR(2000000)`
* *AND* the scan SHALL emit the compatible columns as their mapped `Value` variants and the incompatible columns as JSON strings
* *AND* every emitted column value SHALL be of an Exasol-compatible type

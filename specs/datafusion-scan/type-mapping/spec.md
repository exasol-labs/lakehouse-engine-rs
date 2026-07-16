# Feature: DataFusion-to-Exasol Type Mapping

Defines the single authoritative mapping from DataFusion/Arrow column types to Exasol
SQL types, and the companion Iceberg-to-Arrow mapping used to build the logical schema
the scan registers, so that every column an Iceberg table exposes is queryable through
Exasol. Types Exasol supports natively map directly; types Exasol cannot represent
(vectors, lists, structs, maps, binary, and out-of-range decimals) are serialized to
JSON strings and surfaced as `VARCHAR`. The same mapping governs the `createVirtualSchema`
schema declaration, the Arrow-to-Value conversion in the scan, and the logical schema
carried into the scan spec, keeping declared and emitted types in agreement.

## Background

* Exasol's representable types are: BOOLEAN, DECIMAL(p≤36, s≤36), DOUBLE PRECISION,
  VARCHAR(n≤2,000,000), CHAR(n≤2,000), DATE, TIMESTAMP(p≤9), TIMESTAMP WITH LOCAL TIME
  ZONE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, GEOMETRY, HASHTYPE. Exasol has
  no array, list, struct, or map type. `TIMESTAMP WITH LOCAL TIME ZONE` is a valid Exasol
  column type but NOT a valid UDF `EMITS` output type — Exasol rejects it at scan-script
  compile time (`sqlCode 22002: Column type not supported`) — so this mapping never targets it.
* The mapping is applied in three places that MUST stay consistent: the adapter's
  `createVirtualSchema` schema declaration (Arrow type → declared Exasol column type),
  the scan UDF's Arrow `RecordBatch` → SDK `Value` conversion (Arrow value →
  `Value` variant), and the logical schema carried into the scan spec (Iceberg type →
  Arrow `DataType`).
* Complex Arrow/Iceberg types (list, struct, map, binary) and out-of-range decimals map
  to a string-family type surfaced as JSON `VARCHAR`.
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
  | Decimal128(p,s) where p≤36 and s≤36 | DECIMAL(p, s) | numeric |
  | Decimal128(p,s) where p>36 or s>36 | VARCHAR(2000000) via JSON | `Value::String` |

* Both timezone-naive (`Timestamp(_, None)`) and timezone-aware (`Timestamp(_, Some(_))`)
  Arrow timestamps map to plain Exasol `TIMESTAMP`. An Iceberg `timestamptz` /
  `timestamptz_ns` column is registered internally as the timezone-aware Arrow
  `Timestamp(_, Some("UTC"))` — so DataFusion's timestamp comparisons, date-function
  evaluation, and predicate binding stay timezone-correct — but is declared to Exasol and
  emitted as plain `TIMESTAMP` carrying the UTC-instant value. The Iceberg table spec
  defines a `timestamptz` value as an instant whose values "are stored as UTC and do not
  retain a source time zone" (`2017-11-16 17:10:34 PST` is stored/retrieved as
  `2017-11-17 01:10:34 UTC` and these values are considered identical), so no per-value
  timezone information exists to lose. Mapping to plain `TIMESTAMP` is a deliberate, named
  Exasol target-type trade-off: because Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as
  a UDF `EMITS` output type, the declared Exasol column type cannot distinguish
  `timestamptz` from `timestamp` at the Exasol SQL surface. This is analogous to the
  struct/list/map JSON-`VARCHAR` trade-off — a target-type limitation, not a change to any
  emitted value.
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

### Scenario: Iceberg logical schema maps to Arrow types for scan registration

* *GIVEN* an Iceberg table's current schema whose fields include primitive types (int, long, double, string, boolean, date, timestamp) and complex/out-of-range types (list, struct, map, out-of-range decimal)
* *WHEN* the adapter derives the logical schema it carries into the scan spec
* *THEN* each Iceberg field SHALL map to the Arrow `DataType` the scan UDF registers for that column, consistent with the existing Iceberg-to-Exasol mapping (primitive types to their direct Arrow equivalents; complex and out-of-range types to a string-family Arrow type that surfaces as JSON `VARCHAR`)
* *AND* each mapped field SHALL preserve the source Iceberg field-id and its required/optional nullability
* *AND* the mapping used for the logical schema SHALL agree with the `createVirtualSchema` schema declaration so the declared Exasol column type and the registered Arrow type stay in agreement

### Scenario: Iceberg timestamptz maps to plain Exasol TIMESTAMP

* *GIVEN* an Iceberg `timestamptz` or `timestamptz_ns` column, whose values the Iceberg spec stores as UTC with no retained source time zone
* *WHEN* the adapter resolves the column's Exasol type for the `createVirtualSchema` declaration and the scan `EMITS` clause, and the scan coerces the column at the emit boundary
* *THEN* the resolver SHALL return Exasol `TIMESTAMP` and MUST NOT return `TIMESTAMP WITH LOCAL TIME ZONE`, because Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` output type (`sqlCode 22002: Column type not supported`)
* *AND* the scan UDF SHALL register the column as the timezone-aware Arrow `Timestamp(_, Some("UTC"))`, so DataFusion timestamp comparisons, date-function evaluation, and predicate binding stay timezone-correct
* *AND* the emit-boundary coercion SHALL cast that column to `Timestamp(_, None)` preserving the underlying UTC-instant value, so the emitted `TIMESTAMP` is the UTC wall-clock instant and no value is shifted
* *AND* the declared Exasol column type MUST NOT distinguish `timestamptz` from `timestamp` at the Exasol SQL surface — a deliberate, named target-type trade-off, not a change to any emitted value

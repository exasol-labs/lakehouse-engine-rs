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

* Exasol's representable types are: BOOLEAN, DECIMAL(1≤p≤36, 0≤s≤p), DOUBLE PRECISION,
  VARCHAR(n≤2,000,000), CHAR(n≤2,000), DATE, TIMESTAMP(p≤9), TIMESTAMP WITH LOCAL TIME
  ZONE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, GEOMETRY, HASHTYPE. Exasol has
  no array, list, struct, or map type. `TIMESTAMP WITH LOCAL TIME ZONE` is a valid Exasol
  column type but NOT a valid UDF `EMITS` output type — Exasol rejects it at scan-script
  compile time (`sqlCode 22002: Column type not supported`) — so this mapping never targets it.
* A CATALOG-DECLARED decimal (an Iceberg `PrimitiveType::Decimal` or a Unity Catalog
  `DECIMAL`) is checked against Exasol's full `DECIMAL` domain — `1 ≤ p ≤ 36` and `s ≤ p` —
  and falls back to `VARCHAR(2000000)` otherwise. The compatible-Arrow-types table's
  `Decimal128(p,s) where p≤36 and s≤36` row governs only the ARROW-INPUT direction
  (`arrow_to_exasol_type` / `compatible_exasol_type`), whose scale is signed and has no
  `s ≤ p` analogue, and stays unchanged.
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

### Scenario: A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp

* *GIVEN* an EMITS type string of the form `TIMESTAMP(p)` for an integer precision `p` in 0-9 — the shape the adapter now declares for a projected TIMESTAMP CAST expression once `exasol_type_from_json` (`vs-adapter/pushdown-planning`) reads `fractionalSecondsPrecision`
* *WHEN* the scan resolves that column's Arrow coercion target via `exasol_type_to_arrow` at the emit boundary (`target_arrow_type`)
* *THEN* `exasol_type_to_arrow` SHALL return `Some(DataType::Timestamp(TimeUnit::Microsecond, None))` for every `TIMESTAMP(p)`, `p` in 0-9, identical to the target it already returns for bare `TIMESTAMP` — because Arrow's Microsecond unit is this project's fixed internal representation for every Exasol TIMESTAMP precision, and the declared `p` only governs Exasol's own type check, never the Arrow unit
* *AND* the function MUST NOT return `None` for a `TIMESTAMP(p)` string, so the column stays a timestamp and is NOT routed through the `Utf8`/string path — which would stringify the value and violate the `TIMESTAMP(p)` EMITS declaration
* *AND* a bare `TIMESTAMP` string SHALL continue to map to `Some(DataType::Timestamp(TimeUnit::Microsecond, None))`, unchanged by this scenario
* *AND* `exasol_type_to_arrow` SHALL leave its `TIMESTAMP WITH LOCAL TIME ZONE` exact-match arm unchanged, because `exasol_type_from_json`'s WLTZ branch short-circuits before any precision logic (`vs-adapter/pushdown-planning`, decision [3]) and emits the bare literal `TIMESTAMP WITH LOCAL TIME ZONE` with no `(p)` suffix, so no precision-aware WLTZ arm is ever needed

### Scenario: A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR

* *GIVEN* a column whose catalog-declared type is a decimal carrying an unsigned precision `p` and an unsigned scale `s` — an Iceberg `PrimitiveType::Decimal { precision, scale }` or a Unity Catalog `DECIMAL` whose `type_precision`/`type_scale` the neutral column carries
* *WHEN* the adapter resolves that column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL return `DECIMAL(p,s)` if and only if `1 ≤ p ≤ 36` AND `s ≤ p`, and SHALL return `VARCHAR(2000000)` otherwise, so `p = 0` yields `VARCHAR(2000000)` rather than the invalid `DECIMAL(0,0)` and `s > p` yields `VARCHAR(2000000)` rather than an invalid shape such as `DECIMAL(5,10)`
* *AND* exactly ONE function in `crates/lakehouse-engine/src/types/mapping.rs` SHALL own that PREDICATE, exactly one SHALL own the two returned STRINGS that branch on it, and BOTH catalog kinds SHALL read their answer from those rather than each carrying its own copy — the guard is the significant design decision here, and a second copy is what let the two kinds agree by coincidence rather than by construction
* *AND* the string-returning owner SHALL be declared PRIVATE to `types/mapping.rs`, because its only consumers are the Iceberg and Unity arms in that same file; the predicate owner SHALL be declared `pub(crate)`, because one consumer of the same decision lives OUTSIDE that file — the VS `initial-default` encoding gate in `adapter/pushdown/file_resolution.rs`, whose scenario `datafusion-scan/scan-execution-field-id-projection` owns — and a predicate hidden from a consumer is a predicate that consumer copies
* *AND* the guard MUST NOT carry a separate `s ≤ 36` test, because `s ≤ p` and `p ≤ 36` already imply it and a redundant third condition invites the halves to drift, and MUST NOT carry a lower-bound test on `s`, because both catalog-sourced fields are unsigned and a negative scale is unrepresentable — unlike the Arrow `Decimal128(u8, i8)` path, which this scenario does NOT govern
* *AND* the resolver MUST NOT fail, return a `Result`, or abort the enumeration on either bad pair — the `VARCHAR(2000000)` fallback absorbs them exactly as it absorbs `p > 36`, keeping `column_source_type_to_exasol` and `build_listing_virtual_tables` infallible
* *AND* every pair already mapped SHALL keep its recorded answer byte-identical: `(18,4)` and `(10,2)` stay `DECIMAL(18,4)` and `DECIMAL(10,2)`, the boundary pair `(36,36)` stays `DECIMAL(36,36)` because `s ≤ p` holds there, `(1,0)` stays `DECIMAL(1,0)`, and `(38,10)` and `(18,37)` stay `VARCHAR(2000000)`

### Scenario: The Iceberg-to-Arrow logical mapping reads the same catalog-decimal guard

* *GIVEN* an Iceberg `PrimitiveType::Decimal { precision, scale }` carrying an unsigned precision `p` and an unsigned scale `s`
* *WHEN* the VS resolves that column's LOGICAL ARROW type for the scan spec's logical schema, rather than its Exasol declaration string
* *THEN* the resolver SHALL return `Decimal128(p, s)` if and only if `1 ≤ p ≤ 36` AND `s ≤ p`, and SHALL return `Utf8` otherwise, reading that predicate from the SAME single owner the Exasol-string resolver reads and MUST NOT carry its own copy of it
* *AND* the two directions SHALL therefore be in lockstep BY CONSTRUCTION rather than by convention: for every catalog-declared decimal, `Decimal128(p,s)` accompanies the `DECIMAL(p,s)` declaration and `Utf8` accompanies the `VARCHAR(2000000)` declaration, with no pair producing one of each
* *AND* the resolver MUST NOT return a `Decimal128` tag for a column `createVirtualSchema` declares `VARCHAR(2000000)` — the lockstep is load-bearing in both directions, which is why it is recorded rather than left implicit: such a tag breaks the single-source-of-truth contract this feature records for `exasol_type_to_arrow`, and arrow-rs rejects `precision == 0` and `scale > precision` when a `Decimal128Array` is built, so the tag would name an Arrow type the scan cannot instantiate at all
* *AND* every Arrow answer already recorded SHALL stay byte-identical: `(18,4)`, `(36,36)`, and `(36,0)` stay `Decimal128`, and `(38,10)` and `(18,37)` stay `Utf8` — the two pairs that move are `p = 0` and `s > p`, which move from `Decimal128` to `Utf8`
* *AND* the mapping SHALL remain the LOGICAL Iceberg-to-Arrow mapping, unaffected by physical Parquet decode coercion, and this scenario MUST NOT be read as governing the ARROW-INPUT direction (`arrow_to_exasol_type` / `compatible_exasol_type`), whose signed `Decimal128(u8, i8)` scale has no `s ≤ p` analogue

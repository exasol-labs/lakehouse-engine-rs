# Feature: DataFusion-to-Exasol Type Mapping

Defines the single mapping from DataFusion/Arrow column types to Exasol SQL types, and the companion Iceberg-to-Arrow mapping that builds the logical schema the scan registers. Types Exasol supports map directly. Types it cannot represent (lists, structs, maps, and out-of-range decimals) become `VARCHAR` JSON strings. The same mapping governs the `createVirtualSchema` declaration, the Arrow-to-Value conversion in the scan, and the logical schema in the scan spec.

## Background

* Exasol represents BOOLEAN, DECIMAL(1≤p≤36, 0≤s≤p), DOUBLE PRECISION, VARCHAR(n≤2,000,000), CHAR(n≤2,000), DATE, TIMESTAMP(p≤9), TIMESTAMP WITH LOCAL TIME ZONE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, GEOMETRY, and HASHTYPE. It has no array, list, struct, or map type.
* `TIMESTAMP WITH LOCAL TIME ZONE` is not a valid UDF `EMITS` output type. Exasol rejects it at scan-script compile time (`sqlCode 22002: Column type not supported`), so this mapping never targets it.
* The mapping is applied in three places that stay consistent: the `createVirtualSchema` declaration (Arrow type to declared Exasol type), the scan UDF's `RecordBatch` to SDK `Value` conversion, and the logical schema in the scan spec (Iceberg type to Arrow `DataType`).
* A catalog-declared decimal (an Iceberg `PrimitiveType::Decimal` or a Unity Catalog `DECIMAL`) is checked against Exasol's `DECIMAL` domain, `1 ≤ p ≤ 36` and `s ≤ p`, and falls back to `VARCHAR(2000000)` otherwise. The `Decimal128(p,s) where p≤36 and s≤36` row of the table below governs only the Arrow-input direction (`arrow_to_exasol_type` / `compatible_exasol_type`), whose scale is signed and has no `s ≤ p` analogue.
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

* Incompatible Arrow types (List, LargeList, FixedSizeList, Struct, Map, Union, Binary, LargeBinary, BinaryView, FixedSizeBinary, Duration, Time32, Time64, Interval, Decimal256) have no Exasol equivalent and are declared `VARCHAR(2000000)`, except `FixedSizeBinary(L)`, which is declared `VARCHAR(2L)`. List, LargeList, FixedSizeList, Struct, and Map are rendered as JSON by `scan-types/nested-json-rendering`. The binary types render as lowercase hex per `vs-adapter/binary-column-rendering`. Every other member is serialized in the scan UDF with `CAST(col AS VARCHAR)` / `arrow_cast` before conversion to `Value::String`.
* An Arrow null maps to `Value::Null` regardless of column type.
* `iceberg_type_to_arrow` is not recursive. A column's logical Arrow type stays `Utf8` for every list, struct, and map, so the JSON string is the column's type everywhere it is read: the registered DataFusion schema, the `ScanSpec::logical_schema` tag vocabulary, the pushdown planner's `needs_json_fallback` decisions, and the `VARCHAR(2000000)` declaration. A nested Arrow type would make the column a real nested type in DataFusion, which has no comparison, ordering, hashing, or aggregation operator for `Struct` or `Map`. The adapter would then have to decline every WHERE predicate, GROUP BY key, aggregate argument, and join condition on such a column at five decision sites, and resolve the logical schema before the filter decision.
* The nested field tree reaches the scan on a separate field. A rendering keyed by a file's physical nested names would emit a column-mapped Delta table's `col-…` identifiers as JSON keys, so `LogicalField` carries an optional, format-neutral nested descriptor. It names each nested field's logical name and the one binding key the format's column mapping selects (`field_id`, `physical_name`, or identity), recursively. It is not a type, and the column's type remains `Utf8`.
* `needs_json_fallback` is also true for the binary types and an out-of-range `Decimal128`, so a separate predicate owns the five nested Arrow variants. The two answer different questions: whether a type needs serializing, and whether the JSON encoder renders it.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR

* *GIVEN* a column whose catalog-declared type is a decimal carrying an unsigned precision `p` and an unsigned scale `s`, an Iceberg `PrimitiveType::Decimal { precision, scale }` or a Unity Catalog `decimal(p,s)` parsed from the column's `type_json`
* *WHEN* the adapter resolves that column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL return `DECIMAL(p,s)` if and only if `1 ≤ p ≤ 36` AND `s ≤ p`, and SHALL return `VARCHAR(2000000)` otherwise, so `p = 0` yields `VARCHAR(2000000)` rather than the invalid `DECIMAL(0,0)` and `s > p` yields `VARCHAR(2000000)` rather than an invalid shape such as `DECIMAL(5,10)`
* *AND* exactly ONE function in `crates/lakehouse-engine/src/types/mapping.rs` SHALL own that PREDICATE, exactly one SHALL own the two returned Exasol types that branch on it, and BOTH catalog kinds SHALL read their answer from those rather than each carrying its own copy, the guard is the significant design decision here, and a second copy is what let the two kinds agree by coincidence rather than by construction
* *AND* the type-returning owner SHALL be declared PRIVATE to `types/mapping.rs`, because its only consumers are the Iceberg and Unity arms in that same file; the predicate owner SHALL be declared `pub(crate)`, because one consumer of the same decision lives OUTSIDE that file, the VS `initial-default` encoding gate in `adapter/pushdown/file_resolution.rs`, whose scenario `scan-read-path/scan-execution-field-id-projection-absent-fields` owns, and a predicate hidden from a consumer is a predicate that consumer copies
* *AND* the guard MUST NOT carry a separate `s ≤ 36` test, because `s ≤ p` and `p ≤ 36` already imply it and a redundant third condition invites the halves to drift, and MUST NOT carry a lower-bound test on `s`, because both catalog-sourced fields are unsigned and a negative scale is unrepresentable, unlike the Arrow `Decimal128(u8, i8)` path, which this scenario does NOT govern
* *AND* the resolver MUST NOT fail, return a `Result`, or abort the enumeration on either bad pair, the `VARCHAR(2000000)` fallback absorbs them exactly as it absorbs `p > 36`, keeping `column_source_type_to_exasol` and `build_listing_virtual_tables` infallible
* *AND* every pair already mapped SHALL keep its recorded answer byte-identical: `(18,4)` and `(10,2)` stay `DECIMAL(18,4)` and `DECIMAL(10,2)`, the boundary pair `(36,36)` stays `DECIMAL(36,36)` because `s ≤ p` holds there, `(1,0)` stays `DECIMAL(1,0)`, and `(38,10)` and `(18,37)` stay `VARCHAR(2000000)`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The Iceberg-to-Arrow logical mapping reads the same catalog-decimal guard

* *GIVEN* an Iceberg `PrimitiveType::Decimal { precision, scale }` carrying an unsigned precision `p` and an unsigned scale `s`
* *WHEN* the VS resolves that column's LOGICAL ARROW type for the scan spec's logical schema, rather than its Exasol declaration
* *THEN* the resolver SHALL return `Decimal128(p, s)` if and only if `1 ≤ p ≤ 36` AND `s ≤ p`, and SHALL return `Utf8` otherwise, reading that predicate from the SAME single owner the Exasol-type resolver reads and MUST NOT carry its own copy of it
* *AND* the two directions SHALL therefore be in lockstep BY CONSTRUCTION rather than by convention: for every catalog-declared decimal, `Decimal128(p,s)` accompanies the `DECIMAL(p,s)` declaration and `Utf8` accompanies the `VARCHAR(2000000)` declaration, with no pair producing one of each
* *AND* the resolver MUST NOT return a `Decimal128` tag for a column `createVirtualSchema` declares `VARCHAR(2000000)`, the lockstep is load-bearing in both directions, which is why it is recorded rather than left implicit: such a tag pairs an Arrow decimal with a `VARCHAR` declaration, and arrow-rs rejects `precision == 0` and `scale > precision` when a `Decimal128Array` is built, so the tag would name an Arrow type the scan cannot instantiate at all
* *AND* every Arrow answer already recorded SHALL stay byte-identical: `(18,4)`, `(36,36)`, and `(36,0)` stay `Decimal128`, and `(38,10)` and `(18,37)` stay `Utf8`, the two pairs that move are `p = 0` and `s > p`, which move from `Decimal128` to `Utf8`
* *AND* the mapping SHALL remain the LOGICAL Iceberg-to-Arrow mapping, unaffected by physical Parquet decode coercion, and this scenario MUST NOT be read as governing the ARROW-INPUT direction (`arrow_to_exasol_type` / `compatible_exasol_type`), whose signed `Decimal128(u8, i8)` scale has no `s ≤ p` analogue

> The version-gated timestamp declaration precision, its default on an unreadable version, the
> Arrow-input resolver's exclusion from the gate, and the `timestamptz`/`TIMESTAMP(p)` scenarios live
> in `scan-types/type-mapping-timestamp-precision`.
<!-- /DELTA:CHANGED -->

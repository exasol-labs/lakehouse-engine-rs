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

* Incompatible Arrow types (List, LargeList, FixedSizeList, Struct, Map, Union, Binary, LargeBinary, FixedSizeBinary, Duration, Time32, Time64, Interval, Decimal256) have no Exasol equivalent and are declared `VARCHAR(2000000)`. List, LargeList, FixedSizeList, Struct, and Map are rendered as JSON by `scan-types/nested-json-rendering`. Every other member except the binary types is serialized in the scan UDF with `CAST(col AS VARCHAR)` / `arrow_cast` before conversion to `Value::String`. A binary column is refused at plan time per `vs-adapter/binary-column-refusal`.
* An Arrow null maps to `Value::Null` regardless of column type.
* `iceberg_type_to_arrow` is not recursive. A column's logical Arrow type stays `Utf8` for every list, struct, and map, so the JSON string is the column's type everywhere it is read: the registered DataFusion schema, the `ScanSpec::logical_schema` tag vocabulary, the pushdown planner's `needs_json_fallback` decisions, and the `VARCHAR(2000000)` declaration. A nested Arrow type would make the column a real nested type in DataFusion, which has no comparison, ordering, hashing, or aggregation operator for `Struct` or `Map`. The adapter would then have to decline every WHERE predicate, GROUP BY key, aggregate argument, and join condition on such a column at five decision sites, and resolve the logical schema before the filter decision.
* The nested field tree reaches the scan on a separate field. A rendering keyed by a file's physical nested names would emit a column-mapped Delta table's `col-…` identifiers as JSON keys, so `LogicalField` carries an optional, format-neutral nested descriptor. It names each nested field's logical name and the one binding key the format's column mapping selects (`field_id`, `physical_name`, or identity), recursively. It is not a type, and the column's type remains `Utf8`.
* `needs_json_fallback` is also true for `Binary` and an out-of-range `Decimal128`, so a separate predicate owns the five nested Arrow variants. The two answer different questions: whether a type needs serializing, and whether the JSON encoder renders it.

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

* *GIVEN* a column of an incompatible Arrow type, either a NESTED type (`List`, `LargeList`, `FixedSizeList`, `Struct`, `Map`) or a NON-NESTED one (`Binary`, `LargeBinary`, `FixedSizeBinary`, `Union`, `Duration`, `Time32`, `Time64`, `Interval`, `Decimal256`, or an out-of-range `Decimal128`)
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL declare the column as `VARCHAR(2000000)` for EVERY member of both halves, unchanged by this delta
* *AND* a NESTED column's value SHALL be rendered as a valid JSON document per `scan-types/nested-json-rendering`, which owns that contract
* *AND* a NON-NESTED column's value, other than a `Binary`, `LargeBinary`, or `FixedSizeBinary` one, SHALL keep its recorded `CAST(col AS VARCHAR)` Arrow-display rendering byte-identical, and this feature MUST NOT claim strict JSON conformance for it
* *AND* every request that reads or emits a `Binary`, `LargeBinary`, or `FixedSizeBinary` column SHALL be refused at plan time per `vs-adapter/binary-column-refusal` until a rendering for binary is defined
* *AND* the converter MUST NOT emit any array, list, struct, or map `Value` for either half
* *AND* exactly ONE predicate in `crates/lakehouse-engine/src/types/mapping.rs` SHALL own the NESTED half's arm list, and every consumer SHALL read its answer from that predicate rather than re-matching on `DataType`, so no second copy can classify a type into the wrong half
* *AND* that predicate MUST NOT be `needs_json_fallback`, and `needs_json_fallback` SHALL keep its recorded `fn(&DataType) -> bool` signature and its recorded answer for every input, so its four existing call sites are unchanged: an out-of-range `Decimal128` column SHALL stay in the CAST path that the nested predicate diverts columns away from

### Scenario: A mixed-column Parquet file round-trips through schema mapping and scan

* *GIVEN* an Iceberg Parquet file with both compatible columns (int, string, timestamp) and incompatible columns (a POPULATED list and a POPULATED struct, never a zero-field struct, which sidesteps the field-wise path this scenario exists to cover)
* *WHEN* `createVirtualSchema` declares the table and the scan UDF reads the file
* *THEN* the declared schema SHALL type the compatible columns by the mapping table and the incompatible columns as `VARCHAR(2000000)`
* *AND* the scan SHALL emit the compatible columns as their mapped `Value` variants and the list and struct columns as valid JSON documents that parse, per `scan-types/nested-json-rendering`
* *AND* every emitted column value SHALL be of an Exasol-compatible type

### Scenario: Iceberg logical schema maps to Arrow types for scan registration

* *GIVEN* an Iceberg table's current schema whose fields include primitive types (int, long, double, string, boolean, date, timestamp) and complex/out-of-range types (list, struct, map, out-of-range decimal)
* *WHEN* the adapter derives the logical schema it carries into the scan spec
* *THEN* each Iceberg field SHALL map to the Arrow `DataType` the scan UDF registers for that column, consistent with the existing Iceberg-to-Exasol mapping (primitive types to their direct Arrow equivalents; complex and out-of-range types to a string-family Arrow type that surfaces as JSON `VARCHAR`)
* *AND* `iceberg_type_to_arrow` SHALL keep returning `DataType::Utf8` for `list`, `struct`, and `map`, and MUST NOT recurse into element, field, key, or value types to build a nested Arrow type, because the column's logical type IS the rendered JSON string, see this delta's Background bullet for the five pushdown decision sites a nested logical type would oblige this plan to change
* *AND* each mapped field SHALL preserve the source Iceberg field-id and its required/optional nullability
* *AND* a `list`, `struct`, or `map` field SHALL ADDITIONALLY carry the format-neutral nested descriptor `scan-types/nested-json-rendering` consumes, every nested field's LOGICAL name plus the ONE binding key the format's column-mapping selects, recursively, and a primitive field SHALL carry NONE, so a spec authored before the descriptor existed deserializes unchanged
* *AND* the mapping used for the logical schema SHALL agree with the `createVirtualSchema` schema declaration so the declared Exasol column type and the registered Arrow type stay in agreement

### Scenario: A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR

* *GIVEN* a column whose catalog-declared type is a decimal carrying an unsigned precision `p` and an unsigned scale `s`, an Iceberg `PrimitiveType::Decimal { precision, scale }` or a Unity Catalog `decimal(p,s)` parsed from the column's `type_json`
* *WHEN* the adapter resolves that column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL return `DECIMAL(p,s)` if and only if `1 ≤ p ≤ 36` AND `s ≤ p`, and SHALL return `VARCHAR(2000000)` otherwise, so `p = 0` yields `VARCHAR(2000000)` rather than the invalid `DECIMAL(0,0)` and `s > p` yields `VARCHAR(2000000)` rather than an invalid shape such as `DECIMAL(5,10)`
* *AND* exactly ONE function in `crates/lakehouse-engine/src/types/mapping.rs` SHALL own that PREDICATE, exactly one SHALL own the two returned STRINGS that branch on it, and BOTH catalog kinds SHALL read their answer from those rather than each carrying its own copy, the guard is the significant design decision here, and a second copy is what let the two kinds agree by coincidence rather than by construction
* *AND* the string-returning owner SHALL be declared PRIVATE to `types/mapping.rs`, because its only consumers are the Iceberg and Unity arms in that same file; the predicate owner SHALL be declared `pub(crate)`, because one consumer of the same decision lives OUTSIDE that file, the VS `initial-default` encoding gate in `adapter/pushdown/file_resolution.rs`, whose scenario `scan-read-path/scan-execution-field-id-projection-absent-fields` owns, and a predicate hidden from a consumer is a predicate that consumer copies
* *AND* the guard MUST NOT carry a separate `s ≤ 36` test, because `s ≤ p` and `p ≤ 36` already imply it and a redundant third condition invites the halves to drift, and MUST NOT carry a lower-bound test on `s`, because both catalog-sourced fields are unsigned and a negative scale is unrepresentable, unlike the Arrow `Decimal128(u8, i8)` path, which this scenario does NOT govern
* *AND* the resolver MUST NOT fail, return a `Result`, or abort the enumeration on either bad pair, the `VARCHAR(2000000)` fallback absorbs them exactly as it absorbs `p > 36`, keeping `column_source_type_to_exasol` and `build_listing_virtual_tables` infallible
* *AND* every pair already mapped SHALL keep its recorded answer byte-identical: `(18,4)` and `(10,2)` stay `DECIMAL(18,4)` and `DECIMAL(10,2)`, the boundary pair `(36,36)` stays `DECIMAL(36,36)` because `s ≤ p` holds there, `(1,0)` stays `DECIMAL(1,0)`, and `(38,10)` and `(18,37)` stay `VARCHAR(2000000)`

### Scenario: The Iceberg-to-Arrow logical mapping reads the same catalog-decimal guard

* *GIVEN* an Iceberg `PrimitiveType::Decimal { precision, scale }` carrying an unsigned precision `p` and an unsigned scale `s`
* *WHEN* the VS resolves that column's LOGICAL ARROW type for the scan spec's logical schema, rather than its Exasol declaration string
* *THEN* the resolver SHALL return `Decimal128(p, s)` if and only if `1 ≤ p ≤ 36` AND `s ≤ p`, and SHALL return `Utf8` otherwise, reading that predicate from the SAME single owner the Exasol-string resolver reads and MUST NOT carry its own copy of it
* *AND* the two directions SHALL therefore be in lockstep BY CONSTRUCTION rather than by convention: for every catalog-declared decimal, `Decimal128(p,s)` accompanies the `DECIMAL(p,s)` declaration and `Utf8` accompanies the `VARCHAR(2000000)` declaration, with no pair producing one of each
* *AND* the resolver MUST NOT return a `Decimal128` tag for a column `createVirtualSchema` declares `VARCHAR(2000000)`, the lockstep is load-bearing in both directions, which is why it is recorded rather than left implicit: such a tag breaks the single-source-of-truth contract this feature records for `exasol_type_to_arrow`, and arrow-rs rejects `precision == 0` and `scale > precision` when a `Decimal128Array` is built, so the tag would name an Arrow type the scan cannot instantiate at all
* *AND* every Arrow answer already recorded SHALL stay byte-identical: `(18,4)`, `(36,36)`, and `(36,0)` stay `Decimal128`, and `(38,10)` and `(18,37)` stay `Utf8`, the two pairs that move are `p = 0` and `s > p`, which move from `Decimal128` to `Utf8`
* *AND* the mapping SHALL remain the LOGICAL Iceberg-to-Arrow mapping, unaffected by physical Parquet decode coercion, and this scenario MUST NOT be read as governing the ARROW-INPUT direction (`arrow_to_exasol_type` / `compatible_exasol_type`), whose signed `Decimal128(u8, i8)` scale has no `s ≤ p` analogue

> The version-gated timestamp declaration precision, its default on an unreadable version, the
> Arrow-input resolver's exclusion from the gate, and the `timestamptz`/`TIMESTAMP(p)` scenarios live
> in `scan-types/type-mapping-timestamp-precision`.

### Scenario: A Parquet-sourced column maps through the Arrow-input direction

* *GIVEN* a column whose neutral source-tagged type is the TAG STRING of an Arrow type folded from one or more Parquet footers, rather than an Iceberg primitive or a Unity Catalog type name
* *WHEN* the adapter resolves that column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL gain a THIRD arm on the neutral source type, and that arm SHALL read the tag back to an Arrow type through the EXISTING tag parser and return the ARROW-INPUT answer this feature already specifies for that `DataType`, so the mapping table has one owner and the new source adds no second table
* *AND* the arm SHALL carry the tag as a STRING rather than as an Arrow type, because `catalog/catalog-crate-structure` forbids the crate declaring the neutral type from depending on `arrow`, and the tag vocabulary is the engine's existing arrow-free spelling of an Arrow type
* *AND* an unparseable tag SHALL resolve to `VARCHAR(2000000)` rather than failing, keeping the resolver infallible, because the only producer renders its tags from that same vocabulary and a tag it cannot render is a defect the declaration absorbs rather than a user error
* *AND* the resolver SHALL stay INFALLIBLE for the new arm, returning `VARCHAR(2000000)` for every type Exasol cannot represent rather than a `Result`, so the shared listing pipeline keeps its recorded infallible signature
* *AND* the DECLARED Exasol type and the column's LOGICAL Arrow tag SHALL be in lockstep: a column the resolver declares `VARCHAR(2000000)` SHALL carry a string-family Arrow tag rather than its footer type, exactly as the Iceberg logical mapping already requires, so no column is registered at a type its declaration contradicts
* *AND* a nested column SHALL ADDITIONALLY carry the format-neutral nested descriptor `scan-types/nested-json-rendering` consumes, so a Parquet struct, list, or map is rendered as JSON by the same renderer rather than reaching the cast path with no string kernel
* *AND* the Arrow-input decimal guard SHALL be UNCHANGED, because the Apache Parquet format admits only a precision above zero and a scale between zero and the precision inclusive, so the guard's `p <= 36 && s <= 36` test never admits a pair Exasol rejects from this producer
* *AND* the Iceberg and Unity arms of the resolver MUST be UNCHANGED, and every recorded answer of the catalog-decimal guard, the Iceberg-to-Arrow mapping, and the timestamp-precision rules MUST stay byte-identical

### Scenario: The scan-spec tag vocabulary covers every Arrow type the compatible-type classifier admits

* *GIVEN* the Arrow-input classifier that decides which Arrow types Exasol represents directly, and the compact scan-spec Arrow-type tag vocabulary that spells an Arrow type as a string for the logical schema and for the neutral Parquet column source
* *WHEN* a producer renders a tag for an Arrow type that classifier admits
* *THEN* the tag vocabulary SHALL carry ONE distinct entry for EVERY Arrow type the classifier admits, so the vocabulary's domain is the classifier's domain rather than one producer's
* *AND* that extension SHALL cover at least `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `LargeUtf8`, and `Timestamp` at EVERY `TimeUnit` in both the timezone-naive and the timezone-aware form, because the classifier returns a non-VARCHAR Exasol type for each of them while the recorded vocabulary spells none of them
* *AND* the renderer and the parser SHALL ROUND-TRIP every type in that domain, so an Arrow type rendered to a tag and parsed back SHALL equal the type it started as
* *AND* the renderer MUST NOT fall back to the string tag for a type the classifier ADMITS, because that fallback declares the column `VARCHAR(2000000)` and registers it as a string with no error anywhere, which is what a Spark-written `INT64 TIMESTAMP(MILLIS)` column would otherwise receive
* *AND* the string tag SHALL remain the answer for every type the classifier REFUSES, so a nested, binary, or otherwise unrepresentable type keeps its recorded JSON-VARCHAR path unchanged
* *AND* every recorded tag spelling SHALL be UNCHANGED and every recorded parse answer SHALL stay byte-identical, so a scan spec written before this extension deserializes to the same Arrow types

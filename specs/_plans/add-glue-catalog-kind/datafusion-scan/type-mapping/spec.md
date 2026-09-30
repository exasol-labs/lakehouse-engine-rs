<!-- DELTA:CHANGED -->
# Feature: DataFusion-to-Exasol Type Mapping

Defines the single authoritative mapping from DataFusion/Arrow column types to Exasol
SQL types, and the companion Iceberg-to-Arrow mapping used to build the logical schema
the scan registers, so that every column an Iceberg table exposes is queryable through
Exasol. Types Exasol supports natively map directly; types Exasol cannot represent
(vectors, lists, structs, maps, and out-of-range decimals) are serialized to
JSON strings and surfaced as `VARCHAR`. The same mapping governs the `createVirtualSchema`
schema declaration, the Arrow-to-Value conversion in the scan, and the logical schema
carried into the scan spec, keeping declared and emitted types in agreement.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* **This delta is issue #350.** It splits the recorded "incompatible Arrow types" set in two,
  because the two halves now reach Exasol by different mechanisms: List, LargeList, FixedSizeList,
  Struct, and Map are rendered as real JSON by `datafusion-scan/nested-json-rendering`, while every
  other member of the set — Union, Duration, Time32, Time64, Interval, Decimal256, and an
  out-of-range `Decimal128` — keeps its recorded `CAST(col AS VARCHAR)` Arrow-display path,
  byte-identical. A Binary, LargeBinary, or FixedSizeBinary column is refused at plan time on
  every source per `vs-adapter/binary-column-refusal` (#351), which reads a top-level Parquet
  `ENUM` column as text.
  Every declared EXASOL type is unchanged: all of them were and remain `VARCHAR(2000000)`.
* **`iceberg_type_to_arrow` is deliberately NOT made recursive, and that is the load-bearing design
  decision of issue #350.** A column's LOGICAL Arrow type stays `Utf8` for every list, struct, and
  map, so the JSON string is the column's type everywhere the type is read: in the registered
  DataFusion table schema, in the compact `ScanSpec::logical_schema` tag vocabulary, in the pushdown
  planner's `needs_json_fallback` decisions, and in Exasol's own `VARCHAR(2000000)` declaration. A
  recursive nested Arrow tag would instead make the column a genuine nested type during DataFusion
  execution, where DataFusion has no comparison, ordering, hashing, or aggregation operator for
  `Struct` or `Map` — which would oblige the adapter to newly DECLINE every WHERE predicate, GROUP BY
  key, aggregate argument, and join condition referencing such a column at five separate decision
  sites, and to re-sequence `handle_pushdown` so the logical schema is resolved before the filter
  decision. Keeping the logical type `Utf8` leaves all five sites, the whole capability surface, and
  the `ScanSpec` wire tag untouched.
* **The nested field TREE, unlike the nested TYPE, does reach the scan, on a separate field.** A
  rendering keyed by the file's physical nested names would emit a column-mapped Delta table's
  `col-…` identifiers as JSON keys, so `LogicalField` carries an optional, format-neutral nested
  descriptor naming each nested field's LOGICAL name and the ONE binding key its format's
  column-mapping selects — the SAME `field_id` XOR `physical_name` XOR identity choice `LogicalField`
  already makes for a top-level column, recursed. It is NOT a type: it is the information the JSON
  renderer needs to resolve names, and the column's type remains `Utf8`. `datafusion-scan/nested-json-rendering`
  owns what the renderer does with it; the tag vocabulary this feature owns gains no entry.
* **The JSON-rendered nested set needs its OWN predicate, because `needs_json_fallback` is too
  broad.** `needs_json_fallback` is also true for `Binary` and an out-of-range `Decimal128`. An
  out-of-range `Decimal128` keeps the `CAST(col AS VARCHAR)` path this delta leaves untouched, and
  a `Binary` column is refused before it, per `vs-adapter/binary-column-refusal`. A single predicate
  owning the five nested Arrow variants is therefore added beside it rather than folded into it, and
  the two answer different questions: "does this type need serializing at all" versus "is this type
  rendered by the JSON encoder".
* **Apache Iceberg spec check.** The Iceberg-to-Arrow direction this feature owns is UNCHANGED by
  this delta, so its recorded compliance surface is unchanged. The Iceberg spec's § Nested Types,
  § Column Projection, and § JSON single-value serialization obligations that this plan does engage
  are quoted and answered in `datafusion-scan/nested-json-rendering`, which owns the rendering.
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
* Complex Arrow/Iceberg types (list, struct, map) and out-of-range decimals map
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

* Incompatible Arrow types — List, LargeList, FixedSizeList, Struct, Map, Union, Binary,
  LargeBinary, FixedSizeBinary, Duration, Time32, Time64, Interval, Decimal256 — have no
  Exasol equivalent and are declared as VARCHAR(2000000) in the schema response. Each one
  except Binary, LargeBinary, and FixedSizeBinary is serialized to a JSON string in the scan
  UDF (via DataFusion `CAST(col AS VARCHAR)` / `arrow_cast`) before conversion to
  `Value::String`. A binary column is refused at plan time per `vs-adapter/binary-column-refusal`.
* An Arrow null maps to `Value::Null` regardless of column type.
* **Split, issue #359: the timestamp-precision version gate moved to
  `datafusion-scan/type-mapping-timestamp-precision`.** This feature's scenario count crossed this
  library's per-spec organization threshold once issue #359 landed; the version-gated
  `TIMESTAMP(6)`/`TIMESTAMP` declaration, its default on an unreadable version, the Arrow-input
  resolver's exclusion from the gate, the amended `timestamptz` scenario, and the `TIMESTAMP(p)` EMITS
  round-trip now live in that sibling feature. This feature keeps the general Arrow/Exasol
  type-compatibility surface: compatible types, the Decimal128 domain, incompatible-type JSON
  serialization, and the Iceberg-to-Arrow logical schema mapping.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Incompatible Arrow types are serialized to JSON VARCHAR

* *GIVEN* a column of an incompatible Arrow type — either a NESTED type (`List`, `LargeList`, `FixedSizeList`, `Struct`, `Map`) or a NON-NESTED one (`Binary`, `LargeBinary`, `FixedSizeBinary`, `Union`, `Duration`, `Time32`, `Time64`, `Interval`, `Decimal256`, or an out-of-range `Decimal128`)
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL declare the column as `VARCHAR(2000000)` for EVERY member of both halves, unchanged by this delta
* *AND* a NESTED column's value SHALL be rendered as a valid JSON document per `datafusion-scan/nested-json-rendering`, which owns that contract
* *AND* a NON-NESTED column's value, other than a `Binary`, `LargeBinary`, or `FixedSizeBinary` one, SHALL keep its recorded `CAST(col AS VARCHAR)` Arrow-display rendering byte-identical, and this feature MUST NOT claim strict JSON conformance for it
* *AND* every request that reads or emits a `Binary`, `LargeBinary`, or `FixedSizeBinary` column SHALL be refused at plan time per `vs-adapter/binary-column-refusal` until issue #351 defines a rendering for binary, and that feature reads a top-level Parquet `ENUM` column as text
* *AND* the converter MUST NOT emit any array, list, struct, or map `Value` for either half
* *AND* exactly ONE predicate in `crates/lakehouse-engine/src/types/mapping.rs` SHALL own the NESTED half's arm list, and every consumer SHALL read its answer from that predicate rather than re-matching on `DataType`, so no second copy can classify a type into the wrong half
* *AND* that predicate MUST NOT be `needs_json_fallback`, and `needs_json_fallback` SHALL keep its recorded `fn(&DataType) -> bool` signature and its recorded answer for every input, so its four existing call sites are unchanged: an out-of-range `Decimal128` column SHALL stay in the CAST path that the nested predicate diverts columns away from
<!-- /DELTA:CHANGED -->

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

* **This delta is issue #350.** It splits the recorded "incompatible Arrow types" set in two,
  because the two halves now reach Exasol by different mechanisms: List, LargeList, FixedSizeList,
  Struct, and Map are rendered as real JSON by `datafusion-scan/nested-json-rendering`, while every
  other member of the set — Binary, LargeBinary, FixedSizeBinary, Union, Duration, Time32, Time64,
  Interval, Decimal256, and an out-of-range `Decimal128` — keeps its recorded `CAST(col AS VARCHAR)`
  Arrow-display path, byte-identical, with Binary's JSON validity owned by issue #351. Every declared
  EXASOL type is unchanged: all of them were and remain `VARCHAR(2000000)`.
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
  broad.** `needs_json_fallback` is also true for `Binary` and an out-of-range `Decimal128`, both of
  which must keep the `CAST(col AS VARCHAR)` path this delta leaves untouched. A single predicate
  owning the five nested Arrow variants is therefore added beside it rather than folded into it, and
  the two answer different questions: "does this type need serializing at all" versus "is this type
  rendered by the JSON encoder".
* **Apache Iceberg spec check.** The Iceberg-to-Arrow direction this feature owns is UNCHANGED by
  this delta, so its recorded compliance surface is unchanged. The Iceberg spec's § Nested Types,
  § Column Projection, and § JSON single-value serialization obligations that this plan does engage
  are quoted and answered in `datafusion-scan/nested-json-rendering`, which owns the rendering.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Incompatible Arrow types are serialized to JSON VARCHAR

* *GIVEN* a column of an incompatible Arrow type — either a NESTED type (`List`, `LargeList`, `FixedSizeList`, `Struct`, `Map`) or a NON-NESTED one (`Binary`, `LargeBinary`, `FixedSizeBinary`, `Union`, `Duration`, `Time32`, `Time64`, `Interval`, `Decimal256`, or an out-of-range `Decimal128`)
* *WHEN* the type is resolved for the Exasol schema and a value of it is converted
* *THEN* the resolver SHALL declare the column as `VARCHAR(2000000)` for EVERY member of both halves, unchanged by this delta
* *AND* a NESTED column's value SHALL be rendered as a valid JSON document per `datafusion-scan/nested-json-rendering`, which owns that contract
* *AND* a NON-NESTED column's value SHALL keep its recorded `CAST(col AS VARCHAR)` Arrow-display rendering byte-identical, and this feature MUST NOT claim strict JSON conformance for it — Binary's JSON validity is issue #351
* *AND* the converter MUST NOT emit any array, list, struct, or map `Value` for either half
* *AND* exactly ONE predicate in `crates/lakehouse-engine/src/types/mapping.rs` SHALL own the NESTED half's arm list, and every consumer SHALL read its answer from that predicate rather than re-matching on `DataType`, so no second copy can classify a type into the wrong half
* *AND* that predicate MUST NOT be `needs_json_fallback`, and `needs_json_fallback` SHALL keep its recorded `fn(&DataType) -> bool` signature and its recorded answer for every input, so its four existing call sites are unchanged: a `Binary` and an out-of-range `Decimal128` column SHALL stay in the CAST path that the nested predicate diverts columns away from
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A mixed-column Parquet file round-trips through schema mapping and scan

* *GIVEN* an Iceberg Parquet file with both compatible columns (int, string, timestamp) and incompatible columns (a POPULATED list and a POPULATED struct — never a zero-field struct, which sidesteps the field-wise path this scenario exists to cover)
* *WHEN* `createVirtualSchema` declares the table and the scan UDF reads the file
* *THEN* the declared schema SHALL type the compatible columns by the mapping table and the incompatible columns as `VARCHAR(2000000)`
* *AND* the scan SHALL emit the compatible columns as their mapped `Value` variants and the list and struct columns as valid JSON documents that parse, per `datafusion-scan/nested-json-rendering`
* *AND* every emitted column value SHALL be of an Exasol-compatible type
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Iceberg logical schema maps to Arrow types for scan registration

* *GIVEN* an Iceberg table's current schema whose fields include primitive types (int, long, double, string, boolean, date, timestamp) and complex/out-of-range types (list, struct, map, out-of-range decimal)
* *WHEN* the adapter derives the logical schema it carries into the scan spec
* *THEN* each Iceberg field SHALL map to the Arrow `DataType` the scan UDF registers for that column, consistent with the existing Iceberg-to-Exasol mapping (primitive types to their direct Arrow equivalents; complex and out-of-range types to a string-family Arrow type that surfaces as JSON `VARCHAR`)
* *AND* `iceberg_type_to_arrow` SHALL keep returning `DataType::Utf8` for `list`, `struct`, and `map`, and MUST NOT recurse into element, field, key, or value types to build a nested Arrow type, because the column's logical type IS the rendered JSON string — see this delta's Background bullet for the five pushdown decision sites a nested logical type would oblige this plan to change
* *AND* each mapped field SHALL preserve the source Iceberg field-id and its required/optional nullability
* *AND* a `list`, `struct`, or `map` field SHALL ADDITIONALLY carry the format-neutral nested descriptor `datafusion-scan/nested-json-rendering` consumes — every nested field's LOGICAL name plus the ONE binding key the format's column-mapping selects, recursively — and a primitive field SHALL carry NONE, so a spec authored before the descriptor existed deserializes unchanged
* *AND* the mapping used for the logical schema SHALL agree with the `createVirtualSchema` schema declaration so the declared Exasol column type and the registered Arrow type stay in agreement
<!-- /DELTA:CHANGED -->

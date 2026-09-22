# Feature: Type Mapping

Maps every Arrow and catalog-declared type to the Exasol type the adapter declares and the scan emits. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta ADDS TWO scenarios and is issue #407. The first states what a column maps to when its
  source type is a raw Arrow type read from a Parquet footer. The neutral source-tagged column type
  therefore gains a third case with a recorded answer rather than an implied one.
* **The second scenario widens the scan-spec tag vocabulary, because it is silently lossy for the
  new producer.** `arrow_type_to_tag` and `arrow_type_from_tag`
  (`crates/lakehouse-engine/src/types/mapping.rs`) spell twelve types and fall back to the string
  tag for everything else. Their own doc comment scopes them to the types reachable from
  `iceberg_type_to_arrow`. The Arrow-input classifier `compatible_exasol_type` admits a strictly
  larger set. It returns a non-VARCHAR Exasol type for `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`,
  `UInt64`, `LargeUtf8`, and `Timestamp` at every `TimeUnit`. None of those is nested or
  unrepresentable, so the JSON-fallback substitution never fires for them. The renderer falls
  through to the string tag with no error. A Parquet `INT64 TIMESTAMP(MILLIS)` column, which is
  ordinary Spark output, would therefore be declared `VARCHAR(2000000)` and registered as a string.
  Widening the vocabulary to the classifier's domain is what makes the round trip this plan rests on
  lossless in fact rather than by assumption.
* The answer is the ARROW-INPUT direction this feature already specifies. Until now that direction
  had no producer. Three scenarios govern it: the compatible-type table, the in-range
  `Decimal128` rule, and the out-of-range `Decimal128` fallback. Each described behavior nothing
  exercised. The direct-storage catalog kind exercises all three. Each keeps its recorded
  answer byte-identical.
* The two decimal guards stay SEPARATE and neither changes. The shared catalog guard takes an
  unsigned scale and tests `s <= p`. The Arrow-input guard takes a SIGNED `i8` scale and has no
  `s <= p` analogue. This feature's Iceberg-to-Arrow scenario records that difference deliberately.
  Folding one into the other would accept a negative scale Exasol rejects.
* The Arrow-input guard is sufficient over the new producer's input domain. The Apache Parquet
  format constrains its `DECIMAL` logical type to a precision above zero and a scale between zero
  and the precision inclusive. Every decimal reaching the resolver therefore satisfies `1 <= p` and
  `0 <= s <= p`. `datafusion-scan/type-mapping-module-structure` carries that argument in its
  per-producer reachability clause.
* Apache Iceberg and Delta specification check: NOT implicated. The added scenario maps a raw
  Parquet type, not an Iceberg or a Delta declared type. The Iceberg and Unity arms of the neutral
  source type are untouched, so this feature's Iceberg-to-Exasol and Iceberg-to-Arrow rules keep
  their recorded conformance argument unchanged.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A Parquet-sourced column maps through the Arrow-input direction

* *GIVEN* a column whose neutral source-tagged type is the TAG STRING of an Arrow type folded from one or more Parquet footers, rather than an Iceberg primitive or a Unity Catalog type name
* *WHEN* the adapter resolves that column's Exasol type for the `createVirtualSchema` declaration
* *THEN* the resolver SHALL gain a THIRD arm on the neutral source type, and that arm SHALL read the tag back to an Arrow type through the EXISTING tag parser and return the ARROW-INPUT answer this feature already specifies for that `DataType`, so the mapping table has one owner and the new source adds no second table
* *AND* the arm SHALL carry the tag as a STRING rather than as an Arrow type, because `vs-adapter/catalog-crate-structure` forbids the crate declaring the neutral type from depending on `arrow`, and the tag vocabulary is the engine's existing arrow-free spelling of an Arrow type
* *AND* an unparseable tag SHALL resolve to `VARCHAR(2000000)` rather than failing, keeping the resolver infallible, because the only producer renders its tags from that same vocabulary and a tag it cannot render is a defect the declaration absorbs rather than a user error
* *AND* the resolver SHALL stay INFALLIBLE for the new arm, returning `VARCHAR(2000000)` for every type Exasol cannot represent rather than a `Result`, so the shared listing pipeline keeps its recorded infallible signature
* *AND* the DECLARED Exasol type and the column's LOGICAL Arrow tag SHALL be in lockstep: a column the resolver declares `VARCHAR(2000000)` SHALL carry a string-family Arrow tag rather than its footer type, exactly as the Iceberg logical mapping already requires, so no column is registered at a type its declaration contradicts
* *AND* a nested column SHALL ADDITIONALLY carry the format-neutral nested descriptor `datafusion-scan/nested-json-rendering` consumes, so a Parquet struct, list, or map is rendered as JSON by the same renderer rather than reaching the cast path with no string kernel
* *AND* the Arrow-input decimal guard SHALL be UNCHANGED, because the Apache Parquet format admits only a precision above zero and a scale between zero and the precision inclusive, so the guard's `p <= 36 && s <= 36` test never admits a pair Exasol rejects from this producer
* *AND* the Iceberg and Unity arms of the resolver MUST be UNCHANGED, and every recorded answer of the catalog-decimal guard, the Iceberg-to-Arrow mapping, and the timestamp-precision rules MUST stay byte-identical
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The scan-spec tag vocabulary covers every Arrow type the compatible-type classifier admits

* *GIVEN* the Arrow-input classifier that decides which Arrow types Exasol represents directly, and the compact scan-spec Arrow-type tag vocabulary that spells an Arrow type as a string for the logical schema and for the neutral Parquet column source
* *WHEN* a producer renders a tag for an Arrow type that classifier admits
* *THEN* the tag vocabulary SHALL carry ONE distinct entry for EVERY Arrow type the classifier admits, SUPERSEDING the recorded scoping of that vocabulary to the types reachable from the Iceberg-to-Arrow mapping, so the vocabulary's domain is the classifier's domain rather than one producer's
* *AND* that extension SHALL cover at least `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `LargeUtf8`, and `Timestamp` at EVERY `TimeUnit` in both the timezone-naive and the timezone-aware form, because the classifier returns a non-VARCHAR Exasol type for each of them while the recorded vocabulary spells none of them
* *AND* the renderer and the parser SHALL ROUND-TRIP every type in that domain, so an Arrow type rendered to a tag and parsed back SHALL equal the type it started as
* *AND* the renderer MUST NOT fall back to the string tag for a type the classifier ADMITS, because that fallback declares the column `VARCHAR(2000000)` and registers it as a string with no error anywhere, which is what a Spark-written `INT64 TIMESTAMP(MILLIS)` column would otherwise receive
* *AND* the string tag SHALL remain the answer for every type the classifier REFUSES, so a nested, binary, or otherwise unrepresentable type keeps its recorded JSON-VARCHAR path unchanged
* *AND* every recorded tag spelling SHALL be UNCHANGED and every recorded parse answer SHALL stay byte-identical, so a scan spec written before this extension deserializes to the same Arrow types
<!-- /DELTA:NEW -->

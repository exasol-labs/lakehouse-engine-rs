# Feature: Delta Schema Type Mapping

Maps every type a Delta table schema can declare either onto the Arrow tag the scan binds it by or
onto a named per-column refusal, so a Delta column is queryable when this engine can render its value
faithfully and refused when it cannot — never described by a tag that returns the wrong value.

## Background

The Delta Lake protocol specification (`delta-io/delta`, `PROTOCOL.md`, `master`) defines the type
surface this feature maps, quoted from its § Schema Serialization Format:

* `string` — *"UTF-8 encoded string of characters"*; `long` — *"8-byte signed integer"*; `integer` —
  *"4-byte signed integer"*; `short` — *"2-byte signed integer numbers. Range: -32768 to 32767"*;
  `byte` — *"1-byte signed integer number. Range: -128 to 127"*; `float` — *"4-byte single-precision
  floating-point numbers"*; `double` — *"8-byte double-precision floating-point numbers"*; `boolean` —
  *"`true` or `false`"*; `binary` — *"A sequence of binary data."*; `date` — *"A calendar date,
  represented as a year-month-day triple without a timezone."*
* `decimal` — *"signed decimal number with fixed precision (maximum number of digits) and scale
  (number of digits on right side of dot). The precision and scale can be up to 38."*
* `timestamp` — *"Microsecond precision timestamp elapsed since the Unix epoch ... its
  `isAdjustedToUTC` must be set to `true`"*; `timestamp without time zone` — *"Microsecond precision
  timestamp in a local timezone ... It doesn't have the timezone information ... its `isAdjustedToUTC`
  must be set to `false`. To use this type, a table must support a feature `timestampNtz`."*
* `void` — *"A column that contains only `null` values and is never materialized in data files."*, and
  normatively: *"On write, writers MUST omit `void` columns from data files; they do not appear in the
  data file's schema. On read, readers MUST reconstruct them as all-`null` columns, consistent with
  the rule that columns present in the table schema but missing from a data file are read as
  `null`."*, plus *"`void` is not gated by any table feature and applies to all tables."*
* Complex types — a struct is *"encoded as a JSON object"* with `type` *"Always the string
  \"struct\""*; an array *"stores a variable length collection of items of some type"* with
  `elementType` *"The type of element stored in this array"*; a map *"stores an arbitrary length
  collection of key-value pairs with a single `keyType` and a single `valueType`"*; and *"Variant data
  uses the Delta type name `variant` for Delta schema serialization."*
* `interval year to month` and `interval day to second` appear in NO section of `PROTOCOL.md`'s
  primitive-type table. They exist as `delta_kernel` 0.26 `PrimitiveType` variants because the Spark
  connector produces such `schemaString` type names — the same post-facto situation `void` documents.
  They are therefore mapped defensively rather than from a normative definition.

* **This is issue #322's type-mapping half.** It supersedes `vs-adapter/delta-table-planning` §
  "A Delta type this plan does not map is refused at plan time", which mapped ten primitives, refused
  everything else at TABLE scope, and cited #322 as the tracked gap. That scenario is REMOVED in the
  same plan.
* **The project's "incompatible Arrow types → JSON `VARCHAR`" convention is partly unreachable, and
  that is what shapes this feature's refusal list.** `raw_scan` registers the logical schema — an
  incompatible column tagged `utf8` — as the DataFusion table schema, and DataFusion's physical-expression
  adapter validates physical-against-logical castability at file open, BEFORE any per-value JSON
  conversion runs. Verified against `arrow-cast` 58.3's `can_cast_types`:
  `(Struct(_), _) => false` makes `Struct → Utf8` unreachable, and `Map` reaches the
  `(_, Utf8) => from_type.is_primitive()` arm as `false`, so `Map → Utf8` is unreachable too. Neither
  ever reaches the JSON path, on EITHER table format. Every existing test asserting that fallback uses
  a zero-field struct, which sidesteps the cast. Issue #350 owns designing real JSON rendering for
  struct and map on both formats and removing Delta's refusal once it lands.
* **`binary` is castable but LOSSY, which is worse than uncastable.**
  `can_cast_types(Binary, Utf8)` is `true`, and the cast replaces every byte sequence that is not
  valid UTF-8 with NULL. That is silent data corruption — precisely the failure mode issue #322
  exists to prevent — so `binary` is refused rather than tagged `utf8`. Issue #350 covers it.
* **`array` is castable element-wise, so it is mapped rather than refused — but not blindly.**
  `can_cast_types` recurses: `(List(inner), Utf8) => can_cast_types(inner, Utf8)`. An `array<integer>`
  is castable; an `array<struct<…>>`, an `array<variant>`, and an `array<binary>` are not (the first
  two by castability, the third by the same lossiness `binary` is refused for). The array rule is
  therefore recursive on its element type rather than a blanket `utf8` tag.
* **The `array` rendering is Arrow's own display text, not strict JSON.** `arrow-cast`'s to-string
  path runs `value_to_string`, which formats through `ArrayFormatter`, so an `array<integer>` renders
  as `[1, 2]` and an `array<string>` renders unquoted. That inaccuracy is pre-existing on the Iceberg
  path, does not error, and does not corrupt a value; it is #350's scope, not this feature's. This
  feature commits to the column being surfaced as `VARCHAR(2000000)` carrying a bracketed rendering of
  the elements, and commits to nothing about JSON conformance.
* **`byte` and `short` reuse the existing `int32` tag rather than adding `int8`/`int16` tags.** The
  compact tag vocabulary shared by `arrow_type_to_tag`/`arrow_type_from_tag` in
  `crates/lakehouse-engine/src/types/mapping.rs` has no `int8` or `int16` entry, and Exasol's own
  mapping gives Int8, Int16, and Int32 the same `DECIMAL(precision, 0)` shape with no
  Exasol-visible distinction. The Parquet reader produces Arrow `Int8`/`Int16` physically; the scan's
  existing physical-expression adapter widens each to the logical `Int32` losslessly. Reusing `int32`
  therefore adds no cross-format wire vocabulary and changes no emitted value, while a new tag would
  touch the shared classifier every format reads.
* **Refusal is scoped to the COLUMN, not the table.** A Delta table carrying one struct column is
  otherwise fully readable, and refusing the whole table would make a real-world lakehouse table
  unreachable over a column nobody selected. The `stats_all_types` fixture is exactly this shape: 13
  of its 16 columns are mappable, 3 are not.
* **A refused column is ABSENT from the logical schema, which is the defense-in-depth half of the
  scoping decision.** The adapter gate below produces the clear message; the absence guarantees that
  if the gate ever misses a path, the scan fails with a DataFusion "no field named" error rather than
  emitting a silently-NULLed `binary` column. A tag-and-hope design has no such backstop.
* **`ScanSpec` is unchanged.** The refused-column list travels on the adapter-internal `ResolvedScan`,
  not on the wire, so no `ScanSpec`, `FileEntry`, or `LogicalField` field is added and the
  format-neutrality rule needs no widening. The Iceberg format reader returns an EMPTY refused-column
  list, because it maps every Iceberg type and refuses none.
* **Type classification runs BEFORE the column-mapping binding key.** A column this feature refuses is
  never checked for its `delta.columnMapping.*` annotation, so a table is refused for a column's TYPE
  rather than for an annotation on a column the engine will not read.
* Every error this feature surfaces is a `UdfError`, never a panic, and carries no vended or static
  credential value.
* **Apache Iceberg spec check — this feature changes no Iceberg behavior and closes no Iceberg gap.**
  It touches the Delta schema path alone. The Iceberg table spec's Column Projection rule that
  "projection must be done using field ids" is unaffected, and the recorded deviation from its ordered
  resolution rule (1) in `datafusion-scan/scan-execution-field-id-projection` stays exactly as
  recorded. The struct/map/`binary` unreachability this feature documents ALSO affects the Iceberg
  path, where `iceberg_type_to_arrow` maps them to `Utf8` today; that asymmetry is deliberate — this
  plan does not change Iceberg behavior — and issue #350 owns unifying both formats.

## Scenarios

### Scenario: Every Delta type Exasol represents natively maps to its own Arrow tag

* *GIVEN* a Delta table schema declaring one column of each type in the native set — exactly
  `boolean`, `byte`, `short`, `integer`, `long`, `float`, `double`, `string`, `date`, `timestamp`,
  `timestamp without time zone`, and `decimal(p,s)` whose `p` and `s` satisfy Exasol's catalog-decimal
  domain
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* each column SHALL carry exactly the Arrow tag this table gives it, and SHALL carry its
  nullability from the Delta schema:

  | Delta type | Arrow tag | Declared Exasol type |
  |---|---|---|
  | `boolean` | `bool` | BOOLEAN |
  | `byte` | `int32` | DECIMAL(3,0) |
  | `short` | `int32` | DECIMAL(5,0) |
  | `integer` | `int32` | DECIMAL(10,0) |
  | `long` | `int64` | DECIMAL(20,0) |
  | `float` | `float32` | DOUBLE PRECISION |
  | `double` | `float64` | DOUBLE PRECISION |
  | `string` | `utf8` | VARCHAR(2000000) |
  | `date` | `date32` | DATE |
  | `timestamp` | `timestamptz_us` | TIMESTAMP |
  | `timestamp without time zone` | `timestamp_us` | TIMESTAMP |
  | `decimal(p,s)`, `1 ≤ p ≤ 36` and `s ≤ p` | `decimal128(p,s)` | DECIMAL(p,s) |

* *AND* `byte` and `short` SHALL both map to the EXISTING `int32` tag, and this feature MUST NOT add an
  `int8` or an `int16` tag to the shared tag vocabulary, because Exasol gives Int8, Int16, and Int32
  the same `DECIMAL(precision, 0)` shape and the Parquet reader's physical `Int8`/`Int16` widens to
  logical `Int32` losslessly through the scan's existing physical-expression adapter
* *AND* the decimal domain check SHALL read the SINGLE shared
  `exasol_representable_catalog_decimal` predicate in `crates/lakehouse-engine/src/types/mapping.rs`
  and MUST NOT carry its own copy, so the Delta, Iceberg, and Unity Catalog answers stay in lockstep
  by construction, as `datafusion-scan/type-mapping` requires
* *AND* the ten tags this table shares with the superseded scenario SHALL stay byte-identical, so no
  already-queryable Delta column changes its declared type

### Scenario: A Delta type Exasol cannot represent natively is surfaced as a VARCHAR rendering

* *GIVEN* a Delta table schema declaring one column of each type in the text-rendered set — exactly
  `decimal(p,s)` outside Exasol's catalog-decimal domain (`p = 0`, `p > 36`, or `s > p`), `void`,
  `interval year to month`, `interval day to second`, and `array<E>` where `E` is itself a type from the
  native set of the scenario above or from this set, applied recursively
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* each such column SHALL carry the `utf8` Arrow tag and SHALL be declared to Exasol as
  `VARCHAR(2000000)`, consistent with how `iceberg_primitive_to_arrow` and `iceberg_type_to_arrow`
  already handle their equivalents
* *AND* the `array<E>` rule SHALL be RECURSIVE on `E` rather than a blanket tag for every array,
  because `arrow-cast`'s `can_cast_types` recurses through `(List(inner), Utf8) =>
  can_cast_types(inner, Utf8)`, so `array<array<integer>>` is mappable while `array<struct<…>>` is not
* *AND* a `void` column SHALL read as all-NULL rather than as an error or an empty string, because the
  protocol requires writers to omit it from every data file and readers to *"reconstruct them as
  all-`null` columns"* — the same missing-physical-column rule the scan's field-id and physical-name
  binding already applies
* *AND* the reader MUST NOT claim strict JSON conformance for the rendered text: an `array` surfaces as
  a bracketed rendering of its elements produced by Arrow's own value formatter, and exact JSON
  rendering for nested types is issue #350
* *AND* an out-of-Exasol-domain `decimal` SHALL reach this set through the SAME shared
  `exasol_representable_catalog_decimal` predicate the native set reads, so no decimal pair can be
  classified native by one call site and text-rendered by another

### Scenario: A Delta type whose Arrow form cannot be rendered faithfully is refused by name

* *GIVEN* a Delta table schema declaring a column of a type in the refused set — exactly `binary`,
  `struct`, `map`, `variant`, and `array<E>` where `E` is itself in this set
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL emit NO logical field for that column, SHALL record the column's name and a
  refusal reason naming its Delta type, and MUST NOT emit a logical field whose Arrow tag widens,
  narrows, or otherwise misdescribes the column
* *AND* the refusal reason SHALL name the ACTUAL cause per type rather than the generic
  "this engine does not map at plan time" placeholder the superseded scenario used: `binary` because
  casting it to text replaces every non-UTF-8 byte sequence with NULL; `struct` and `map` because
  `arrow-cast`'s `can_cast_types` reports NO cast from either to `Utf8`, so the project's JSON-`VARCHAR`
  convention is unreachable for them; `variant` because its on-disk form is an opaque
  `(metadata BINARY, value BINARY)` pair in a Delta-specific binary encoding whose Arrow form is a
  struct, so a rendering would be a meaningless blob rather than the value; and `array<E>` by
  inheriting `E`'s reason
* *AND* the `binary`, `struct`, and `map` reasons SHALL cite issue #350, and MUST NOT cite issue #322,
  because #322 is this plan and a closed issue cited in a shipped error text reads as an unfixed gap
  with no owner
* *AND* `variant`'s refusal SHALL stand INDEPENDENTLY of the reader-feature gate that also refuses
  `variantType` and `variantType-preview` (`vs-adapter/delta-reader-feature-gating`), so a table
  declaring a `variant` column without declaring the feature is still refused
* *AND* the type classification SHALL run BEFORE the column's `delta.columnMapping.*` binding key is
  read, so a refused column is refused for its TYPE and never for a missing annotation on a column the
  engine will not read

### Scenario: A refused column refuses only the requests that read or emit it

* *GIVEN* a Delta table whose schema mixes mappable columns with at least one refused column — the
  `stats_all_types` shape, whose 13 mappable columns are `byte_col`, `short_col`, `int_col`,
  `long_col`, `float_col`, `double_col`, `date_col`, `timestamp_col`, `timestamp_ntz_col`,
  `string_col`, `decimal_col`, `boolean_col`, and `array_col`, and whose 3 refused columns are
  `binary_col`, `map_col`, and `nested_struct`
* *WHEN* the adapter handles a pushdown request against that table
* *THEN* the adapter SHALL plan the request normally when it reads and emits only mappable columns,
  and SHALL return a `UdfError` naming the refused column and its refusal reason when the request
  would read or emit a refused one
* *AND* the refusal decision SHALL be taken from the pushdown request's OWN column references —
  collected by ONE recursive walk over the whole request JSON for every `column` node's name — UNIONED
  with the final projection the adapter renders, and MUST NOT be assembled from a per-clause
  enumeration, because a per-clause list silently omits each pushdown capability added after it and
  would route a refused column past the gate
* *AND* the request-JSON half SHALL cover a refused column reached through a WHERE filter, a GROUP BY
  key, an ORDER BY key, an aggregate argument, or a join condition, because a `binary` column pushed
  into the scan's filter would otherwise be compared as text with every non-UTF-8 value silently NULL
* *AND* the projection half SHALL cover the full-base-row projection the adapter falls back to for a
  `SELECT *`, an aggregate select list, and an untranslatable select-list item, so `SELECT *` over a
  table carrying a refused column is refused rather than emitting a column the scan cannot bind
* *AND* the gate SHALL run BEFORE the zero-active-files early return, so a query naming a refused
  column against an EMPTY Delta table is refused rather than answered with an empty result
* *AND* the gate SHALL run on the JOIN path for each resolved side as well as on the single-table
  path, so a refused column reached through a join leg is refused by the same rule
* *AND* a refused column SHALL be ABSENT from the logical schema the scan registers, so a gate miss
  fails with a DataFusion unresolved-column error rather than emitting a silently-NULLed `binary`
  column — the refusal's correctness MUST NOT rest on the gate alone

### Scenario: A Delta table with no mappable column is refused as a whole

* *GIVEN* a Delta table whose EVERY schema column is in the refused set
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL refuse the table with a `UdfError` naming every refused column and its
  reason, rather than returning an EMPTY logical schema
* *AND* the refusal SHALL be justified in the reader's own contract by the consequence of the
  alternative: `raw_scan` treats an empty `logical_schema` as "infer the schema from the first data
  file", which would bind columns by physical file order and by physical file name — the exact
  unauthorized binding `vs-adapter/delta-table-planning` refuses an ordinal field-id for
* *AND* the whole-table refusal SHALL be the ONLY case in which a refused column refuses a request
  that does not name it, so the per-column scoping stays the rule and this stays its single, stated
  exception

### Scenario: The castability claims behind the mapping are asserted, not assumed

* *GIVEN* the three sets above, whose membership is decided by whether the Arrow type the scan
  physically reads for a Delta type can be cast to `Utf8` and whether that cast preserves the value
* *WHEN* the type-mapping test suite runs
* *THEN* it SHALL assert `arrow::compute::can_cast_types(physical, DataType::Utf8)` directly for each
  representative physical Arrow type, and SHALL assert `true` for `Binary`, `List(Int32)`,
  `Interval(YearMonth)`, `Interval(DayTime)`, and an out-of-domain `Decimal128`, and `false` for a
  populated `Struct`, a `Map`, and a `List(Struct)`
* *AND* those assertions SHALL use a POPULATED struct rather than a zero-field struct, because a
  zero-field struct sidesteps the field-wise cast check and is why the existing `convert_tests` and
  `mapping_tests` assertions passed against a convention that does not hold
* *AND* the suite SHALL therefore FAIL rather than silently re-partition the sets when an
  `arrow-cast` upgrade changes any of those answers, which is the only mechanism that keeps the three
  sets honest over time

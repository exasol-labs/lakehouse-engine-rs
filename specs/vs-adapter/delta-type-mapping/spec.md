# Feature: Delta Schema Type Mapping

Maps every type a Delta table schema can declare either onto the Arrow tag the scan binds it by or
onto a named per-column refusal, so a Delta column is queryable when this engine can render its value
faithfully and refused when it cannot — never described by a tag that returns the wrong value.

## Background

* **This delta is issue #349.** It adds ONE scenario: the per-column validation of a table's recorded
  `delta.typeChanges` history, which the Delta protocol makes a READER obligation the moment
  `typeWidening` is allow-listed (`vs-adapter/delta-reader-feature-gating`). No existing scenario
  changes — the native, text-rendered, and refused type sets, the shared decimal predicate, the
  per-column refusal scoping, the whole-table refusal, and the castability assertions are all
  untouched.
* **The Delta Lake protocol specification (`delta-io/delta`, `PROTOCOL.md`, `master`) states two
  reader obligations, and this feature owns the second.** § Reader Requirements for Type Widening:
  *"Readers must allow reading data files written before the table underwent any supported type
  change, and must convert such values to the current, wider type."* — met by
  `datafusion-scan/type-relaxation`. *"Readers must validate that they support all type changes in
  the `delta.typeChanges` field in the table schema for the table version they are reading and fail
  when finding any unsupported type change."* — met here.
* **The supported type changes, quoted verbatim from § Type Widening**, are the set this validation
  accepts:

  > - Integer widening:
  >   - `Byte` -> `Short` -> `Int` -> `Long`
  > - Floating-point widening:
  >   - `Float` -> `Double`
  >   - `Byte`, `Short` or `Int` -> `Double`
  > - Date widening:
  >   - `Date` -> `Timestamp without timezone`
  > - Decimal widening - `p` and `s` denote the decimal precision and scale respectively.
  >   - `Decimal(p, s)` -> `Decimal(p + k1, s + k2)` where `k1 >= k2 >= 0`.
  >   - `Byte`, `Short` or `Int` -> `Decimal(10 + k1, k2)` where `k1 >= k2 >= 0`.
  >   - `Long` -> `Decimal(20 + k1, k2)` where `k1 >= k2 >= 0`.

* **The decimal constraint is `k1 >= k2 >= 0`, which is STRICTLY STRONGER than "precision and scale
  may both grow".** `k2 >= 0` forbids the scale shrinking and `k1 >= k2` forbids the INTEGRAL digit
  count shrinking, so `decimal(10,1)` → `decimal(11,3)` is not a legal widening even though both
  precision and scale grow. Encoding the rule as the pair of inequalities rather than as
  `P' >= P && S' >= S` is what makes the validation match the protocol instead of a paraphrase of it.
* **`long` → `double` is deliberately ABSENT from the protocol's list** — the floating-point bullet
  names `Byte`, `Short` or `Int` and omits `Long`, which is lossy above 2^53. A validation that
  admitted it would accept a table no conforming Delta writer produces.
* **The metadata shape, quoted from § Type Change Metadata**, is a JSON list whose objects carry
  `fromType` (required), `toType` (required), and `fieldPath` (optional — *"When updating the type of
  a map key/value or array element only"*, with values `"key"`, `"value"`, `"element"`, dotted for
  nesting). `tableVersion` was REQUIRED in the accepted-and-superseded RFC and is absent from the
  current specification, yet Delta 3.2-era clients still write it — the vendored `type-widening`
  fixture carries `tableVersion: 2` on all thirteen of its entries. The parser therefore MUST ignore
  keys it does not know rather than reject the entry.
* **`delta.typeChanges` is a VALIDATION input, never a cast input.** The protocol's conversion rule
  names only *"the current, wider type"*, writers *"may remove the `delta.typeChanges` metadata …
  if all data files use the same field types as the table schema"*, and removing the feature
  REQUIRES removing it. A reader that consulted it to decide a cast would therefore break on a table
  that legally carries none. The scan reads the physical Parquet type from each file's own footer and
  casts to the current logical type, which is what `datafusion-scan/type-relaxation` records.
* **A `fieldPath` entry names a map key/value or an array element, and this engine already answers
  those columns without a scalar cast.** A `map` column is refused outright and an `array<E>` column
  is text-rendered, so a widening recorded inside either changes the rendered text at worst and can
  never surface a wrong scalar value. Validating such an entry by its pair alone — ignoring the path
  — is therefore sufficient and avoids parsing a nested path grammar for a column whose value never
  reaches Exasol as that type.
* **The refusal reuses the EXISTING per-column mechanism rather than adding a table-scoped gate.** An
  unsupported recorded change concerns exactly one column, and this feature already carries a
  refused-column list on `ResolvedScan` that refuses only the requests reading or emitting that
  column. A table-scoped refusal would make an otherwise readable table unreachable over a column
  nobody selected — the same argument that scoped the type refusals per column.
* **Apache Iceberg spec check — this delta changes no Iceberg behavior.** It reads a Delta-specific
  schema annotation and adds no code on the Iceberg resolution path. Iceberg's own promotions,
  including the two `date` rows this engine refuses, are `vs-adapter/iceberg-type-promotion`'s scope.

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
* *AND* the projection half SHALL union in the full-base-row projection ONLY when the request's own
  select list is absent or empty — a genuine `SELECT *` — so `SELECT *` over a table carrying a
  refused column is refused rather than emitting a column the scan cannot bind
* *AND* the projection half MUST NOT union in the full-base-row projection the adapter separately
  renders when a select-list item is an aggregate or otherwise untranslatable, because that fallback
  is a synthetic placeholder the scan never reads — each such item's own referenced columns already
  reach the touched-column set through the request-JSON walk's aggregate-argument coverage above — and
  unioning it would refuse a bare `COUNT(*)`, which reads no column value, on a table carrying an
  unrelated refused column
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

### Scenario: Every recorded Delta type change is validated, and an unsupported one refuses its column

* *GIVEN* a Delta table whose schema carries `delta.typeChanges` entries on one or more fields — the
  shape a `typeWidening` or `typeWidening-preview` table records — including at least one entry whose
  `fromType`/`toType` pair the Delta protocol's supported list does NOT contain
* *WHEN* the Delta format reader builds that table's logical schema
* *THEN* the reader SHALL validate EVERY recorded entry's `fromType`/`toType` pair against the
  protocol's supported list, because the protocol obliges a reader to *"validate that they support
  all type changes in the `delta.typeChanges` field … and fail when finding any unsupported type
  change"*
* *AND* the reader SHALL accept exactly the protocol's list — the `Byte` → `Short` → `Int` → `Long`
  integer chain, `Float` → `Double`, `Byte`/`Short`/`Int` → `Double`, `Date` → `Timestamp without
  timezone`, `Decimal(p,s)` → `Decimal(p+k1,s+k2)`, `Byte`/`Short`/`Int` → `Decimal(10+k1,k2)`, and
  `Long` → `Decimal(20+k1,k2)` — and SHALL refuse every other pair, including `Long` → `Double`,
  which the protocol omits because it is lossy above 2^53
* *AND* each decimal target SHALL be checked as `k1 >= k2 >= 0` rather than as `P' >= P` and
  `S' >= S`, so `decimal(10,1)` → `decimal(11,3)` is REFUSED — its integral digit count shrinks from
  9 to 8 — while `decimal(10,1)` → `decimal(12,3)` is accepted
* *AND* the reader SHALL record the offending column's name and a refusal reason naming both types,
  through the SAME refused-column list this feature already carries, so an unsupported change refuses
  only the requests that read or emit that column and leaves the rest of the table queryable
* *AND* the reader SHALL ignore an entry key it does not recognize — notably `tableVersion`, which
  the superseded RFC required and Delta 3.2-era clients still write, including on all thirteen
  entries of the vendored `type-widening` fixture — and MUST NOT refuse an otherwise valid entry for
  carrying one
* *AND* an entry carrying a `fieldPath` SHALL be validated by its `fromType`/`toType` pair alone,
  without parsing the path, because a `fieldPath` names a map key/value or array element and this
  engine already refuses `map` outright and text-renders `array<E>`, so no scalar value is at risk
* *AND* the reader MUST NOT consult `delta.typeChanges` to decide any CAST, because the protocol lets
  a writer remove the annotation once every data file matches the schema and REQUIRES its removal
  when the feature is dropped — the cast reads each file's own physical Parquet type against the
  current logical type (`datafusion-scan/type-relaxation`)
* *AND* a table carrying NO `delta.typeChanges` annotation SHALL pass this validation unchanged,
  whether or not it declares the `typeWidening` reader feature, so every already-queryable Delta
  fixture keeps its recorded behavior byte-identical

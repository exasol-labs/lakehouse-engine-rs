# Feature: Delta Schema Type Mapping

Maps every type a Delta table schema can declare either onto the Arrow tag the scan binds it by or
onto a named per-column refusal, so a Delta column is queryable when this engine can render its value
faithfully and refused when it cannot — never described by a tag that returns the wrong value.

## Background

* **This delta is issue #350.** It moves `struct` and `map` OUT of the refused set and into the
  JSON-rendered set, recurses the `delta.typeChanges` validation into nested fields (the #357 review
  finding, which only becomes reachable once nested columns are scannable), and adds the nested field
  descriptor the JSON renderer needs to key a column-mapped table's struct by its LOGICAL inner names.
  `binary` and `variant` stay refused. The native type set, the shared decimal predicate, the
  per-column refusal scoping, and the whole-table refusal are untouched.
* **The recorded diagnosis is discharged, not amended.** This feature already records that *"`raw_scan`
  registers the logical schema — an incompatible column tagged `utf8` — as the DataFusion table
  schema, and DataFusion's physical-expression adapter validates physical-against-logical castability
  at file open, BEFORE any per-value JSON conversion runs … Neither ever reaches the JSON path, on
  EITHER table format. … Issue #350 owns designing real JSON rendering for struct and map on both
  formats and removing Delta's refusal once it lands."* That is this delta.
  `datafusion-scan/nested-json-rendering` owns the rendering; this feature owns only which Delta type
  reaches it.
* **The three `can_cast_types` answers this feature pinned are still TRUE and still asserted — but
  they no longer DECIDE the sets.** `Struct → Utf8` and `Map → Utf8` remain uncastable and
  `List(Int32) → Utf8` remains castable-but-display-text. Under issue #350 all three are bypassed
  rather than relied on: the scan diverts a nested physical column away from the cast entirely. The
  assertions are retained because they are what makes the bypass's NECESSITY falsifiable — if an
  `arrow-cast` upgrade ever added a `Struct → Utf8` cast, the suite must fail so the bypass is
  re-justified rather than silently redundant.
* **`array<E>` no longer classifies by `can_cast_types` recursion, and the rule gets SIMPLER, not
  more complex.** The old recursion existed because `(List(inner), Utf8) => can_cast_types(inner,
  Utf8)` decided mappability. The JSON renderer recurses natively through every nesting depth, so a
  container now classifies by whether every MEMBER type is itself renderable — which is the same
  recursion, re-based on renderability instead of castability, and it extends unchanged to `struct`
  and `map`.
* **`binary` stays refused at EVERY depth, and that is a deliberate scope boundary rather than a
  correctness claim.** The JSON encoder renders a `Binary` member as a quoted lowercase hexadecimal
  string — faithful, and the same convention the Iceberg spec's Appendix D gives `binary` and `fixed` —
  so a nested `binary` is NOT the lossy `Utf8` cast this feature refuses it for. Admitting it would
  nonetheless change Binary's reach, which issue #351 owns and issue #350 is scoped out of. So
  `array<binary>` stays refused exactly as recorded, and `struct` and `map` containing a `binary`
  member JOIN it. The asymmetry is named rather than hidden: an ICEBERG table's nested `binary` IS
  rendered as hexadecimal, because the Iceberg format reader refuses no type at all — a pre-existing
  structural difference this delta does not introduce and does not close.
* **`variant` stays refused at every depth for its own recorded reason** — an opaque
  `(metadata BINARY, value BINARY)` pair whose rendering would be a meaningless blob, not the value —
  which the JSON encoder does not change.
* **`binary`'s refusal reason must stop citing issue #350.** This feature already records the rule:
  *"a closed issue cited in a shipped error text reads as an unfixed gap with no owner"*. #350 closes
  with this plan, so the citation moves to #351, which owns Binary's JSON validity. This is a message
  edit under this feature's own recorded rule, NOT a change to Binary's behavior.
* **The `fieldPath` justification is discharged; the pair-only VALIDATION survives it.** This feature
  records that *"an entry carrying a `fieldPath` SHALL be validated by its `fromType`/`toType` pair
  alone, without parsing the path, because a `fieldPath` names a map key/value or array element and
  this engine already refuses `map` outright and text-renders `array<E>`, so no scalar value is at
  risk"*. The premise is gone: a map's and an array's members now reach Exasol inside the rendered
  JSON. The pair-only validation is still CORRECT — the protocol's supported-pair rule does not depend
  on the path — but the path must now be RETAINED and reported, so an operator can locate the
  offending field in a nested tree instead of being told only the top-level column name.
* **The #357 gap this delta closes, stated precisely:** `build_delta_table_schema` reads
  `delta.typeChanges` from `schema.fields()` — TOP-LEVEL `StructField`s only. Nothing recurses into a
  struct's inner `StructField`s. While `struct` and `map` were refused before the check ran, no nested
  annotation was reachable; making them scannable makes an unvalidated nested type change reachable,
  which is exactly the reader obligation `PROTOCOL.md` § Reader Requirements for Type Widening states.
* **Delta assigns column-mapping annotations to NESTED fields, and the vendored fixture proves it.**
  `scripts/unity/fixtures/stats-all-types` declares `delta.columnMapping.mode = name` and its
  `nested_struct`'s three inner fields carry `delta.columnMapping.physicalName` values
  `col-7f2f94cf-7082-430c-bba7-852bc6c5215e`, `col-26fcfd6b-04c7-4772-8bdf-04ac9425f06e`, and
  `col-92dcf16d-d249-48a9-afb8-93deeaf7ce23`. A renderer reading physical names would emit those as
  JSON object names, so the nested descriptor is what makes a column-mapped Delta struct usable at all
  rather than a cosmetic improvement.
* **Only a STRUCT field carries a name to reconcile.** A Delta `array`'s `elementType` and a `map`'s
  `keyType`/`valueType` are unnamed types, not `StructField`s, so they carry no column-mapping
  annotation and no logical name — they bind by identity, and the descriptor records them as unnamed
  members. The recursion therefore has exactly one naming case.
* **`ScanSpec` stays format-neutral.** The nested descriptor is defined by
  `datafusion-scan/type-mapping` as a recursion of `LogicalField`'s OWN binding-key choice — a
  `field_id`, a `physical_name`, or neither. Delta populates it from `delta.columnMapping.id` under
  `id` mode, from `delta.columnMapping.physicalName` under `name` mode, and from neither under `none`
  mode — the same three-way choice this feature already makes per top-level column. Iceberg populates
  `field_id`; a future format populates whichever it has. No Delta-specific struct reaches the wire.
* **Apache Iceberg spec check — this delta changes no Iceberg behavior and closes no Iceberg gap.**
  It reads Delta-specific schema annotations and adds no code on the Iceberg resolution path. The
  Iceberg-side obligations this plan engages are quoted and answered by
  `datafusion-scan/nested-json-rendering`. The struct/map unreachability this feature previously
  documented for BOTH formats is resolved for both by that feature, so the recorded asymmetry
  (*"that asymmetry is deliberate — this plan does not change Iceberg behavior — and issue #350 owns
  unifying both formats"*) is discharged for struct and map, and survives only for `binary`.
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
* **The refusal reuses the EXISTING per-column mechanism rather than adding a table-scoped gate.** An
  unsupported recorded change concerns exactly one column, and this feature already carries a
  refused-column list on `ResolvedScan` that refuses only the requests reading or emitting that
  column. A table-scoped refusal would make an otherwise readable table unreachable over a column
  nobody selected — the same argument that scoped the type refusals per column.

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
* The Iceberg format reader returns an EMPTY refused-column list, because it maps every Iceberg type
  and refuses none.
* **Type classification runs BEFORE the column-mapping binding key.** A column this feature refuses is
  never checked for its `delta.columnMapping.*` annotation, so a table is refused for a column's TYPE
  rather than for an annotation on a column the engine will not read.
* Every error this feature surfaces is a `UdfError`, never a panic, and carries no vended or static
  credential value.

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

* *GIVEN* a Delta table schema declaring one column of each type in the text-rendered set — exactly `decimal(p,s)` outside Exasol's catalog-decimal domain (`p = 0`, `p > 36`, or `s > p`), `void`, `interval year to month`, `interval day to second`, and a CONTAINER type — `array<E>`, `struct<…>`, or `map<K,V>` — every one of whose member types is itself from the native set, from this set, or is itself such a container, applied recursively
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* each such column SHALL carry the `utf8` Arrow tag and SHALL be declared to Exasol as `VARCHAR(2000000)`, consistent with how `iceberg_primitive_to_arrow` and `iceberg_type_to_arrow` already handle their equivalents
* *AND* `struct` and `map` SHALL be members of THIS set and MUST NOT be refused, so the `stats_all_types` fixture's `map_col` and `nested_struct` become queryable
* *AND* the container rule SHALL be RECURSIVE on every member type — an `array`'s element, a `struct`'s every field, and a `map`'s key AND value — and the recursion SHALL be based on whether each member is JSON-RENDERABLE, replacing the recorded `can_cast_types` basis, so `array<struct<…>>` and `map<string, array<integer>>` are mappable while `array<binary>` and `struct<v: variant>` are not
* *AND* an `array`, `struct`, or `map` column SHALL be surfaced as STRICT JSON per `datafusion-scan/nested-json-rendering`, replacing the recorded commitment to *"a bracketed rendering of the elements"* produced by Arrow's value formatter, so `array_col` returns a JSON array of bare numbers rather than `[1, 2]` display text
* *AND* a `void` column SHALL read as all-NULL rather than as an error or an empty string, and a `void` field NESTED inside a container SHALL render as an explicit JSON `null`, because the protocol requires writers to omit it from every data file and readers to *"reconstruct them as all-`null` columns"*
* *AND* an out-of-Exasol-domain `decimal` SHALL reach this set through the SAME shared `exasol_representable_catalog_decimal` predicate the native set reads, so no decimal pair can be classified native by one call site and text-rendered by another
* *AND* an out-of-domain `decimal` NESTED inside a container SHALL keep the container mappable, because the JSON renderer renders a decimal as a bare JSON number at any depth with no Exasol DECIMAL domain to satisfy

### Scenario: A Delta type whose Arrow form cannot be rendered faithfully is refused by name

* *GIVEN* a Delta table schema declaring a column of a type in the refused set — exactly `binary`, `variant`, and any container (`array`, `struct`, or `map`) at least one of whose member types is itself in this set, at any nesting depth
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL emit NO logical field for that column, SHALL record the column's name and a refusal reason naming its Delta type, and MUST NOT emit a logical field whose Arrow tag widens, narrows, or otherwise misdescribes the column
* *AND* `struct` and `map` MUST NOT appear in this set on their own account, and their recorded refusal reasons — *"which arrow-cast reports no cast to text for"* — SHALL be DELETED rather than retained, because the reason no longer describes anything the engine does
* *AND* the refusal reason SHALL name the ACTUAL cause per type: `binary` because casting it to text replaces every non-UTF-8 byte sequence with NULL; `variant` because its on-disk form is an opaque `(metadata BINARY, value BINARY)` pair in a Delta-specific binary encoding whose Arrow form is a struct, so a rendering would be a meaningless blob rather than the value; and a container by naming its own declared type, the PATH of the offending member, and that member's reason
* *AND* ONE composer SHALL build every container refusal — for an `array`'s element, a `struct`'s field, and a `map`'s key or value alike — replacing the recorded array-only composer, so nesting adds no message layer per kind and no operator is told the column has a member's type
* *AND* the `binary` reason SHALL cite issue #351 and MUST NOT cite issue #350, because #350 closes with this plan and a closed issue cited in a shipped error text reads as an unfixed gap with no owner
* *AND* `binary` SHALL stay refused at EVERY nesting depth even though the JSON encoder renders a `Binary` member as faithful lowercase hexadecimal, because widening Binary's reach is issue #351's scope and not this plan's
* *AND* `variant`'s refusal SHALL stand INDEPENDENTLY of the reader-feature gate that also refuses `variantType` and `variantType-preview` (`vs-adapter/delta-reader-feature-gating`), so a table declaring a `variant` column without declaring the feature is still refused
* *AND* the type classification SHALL run BEFORE the column's `delta.columnMapping.*` binding key is read, so a refused column is refused for its TYPE and never for a missing annotation on a column the engine will not read

### Scenario: A refused column refuses only the requests that read or emit it

* *GIVEN* a Delta table whose schema mixes mappable columns with at least one refused column — the `stats_all_types` shape, whose 15 mappable columns are `byte_col`, `short_col`, `int_col`, `long_col`, `float_col`, `double_col`, `date_col`, `timestamp_col`, `timestamp_ntz_col`, `string_col`, `decimal_col`, `boolean_col`, `array_col`, `map_col`, and `nested_struct`, and whose ONE refused column is `binary_col`
* *WHEN* the adapter handles a pushdown request against that table
* *THEN* the adapter SHALL plan the request normally when it reads and emits only mappable columns, and SHALL return a `UdfError` naming the refused column and its refusal reason when the request would read or emit `binary_col`
* *AND* `map_col` and `nested_struct` SHALL move from the refused list to the mappable list, so a request naming either is planned normally, and the fixture's refused count falls from 3 to 1
* *AND* the refusal decision SHALL be taken from the pushdown request's OWN column references — collected by ONE recursive walk over the whole request JSON for every `column` node's name — UNIONED with the final projection the adapter renders, and MUST NOT be assembled from a per-clause enumeration
* *AND* the request-JSON half SHALL cover a refused column reached through a WHERE filter, a GROUP BY key, an ORDER BY key, an aggregate argument, or a join condition
* *AND* the projection half SHALL union in the full-base-row projection ONLY when the request's own select list is absent or empty — a genuine `SELECT *` — so `SELECT *` over a table carrying a refused column is refused rather than emitting a column the scan cannot bind
* *AND* the projection half MUST NOT union in the full-base-row projection the adapter separately renders when a select-list item is an aggregate or otherwise untranslatable
* *AND* the gate SHALL run BEFORE the zero-active-files early return, so a query naming a refused column against an EMPTY Delta table is refused rather than answered with an empty result
* *AND* the gate SHALL run on the JOIN path for each resolved side as well as on the single-table path
* *AND* a refused column SHALL be ABSENT from the logical schema the scan registers, so a gate miss fails with a DataFusion unresolved-column error rather than emitting a silently-NULLed `binary` column

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

* *GIVEN* the three sets above, whose membership is now decided by whether the JSON renderer can render the Arrow type the scan physically reads — no longer by whether that type can be cast to `Utf8`
* *WHEN* the type-mapping test suite runs
* *THEN* it SHALL STILL assert `arrow::compute::can_cast_types(physical, DataType::Utf8)` directly for each representative physical Arrow type, asserting `true` for `Binary`, `List(Int32)`, `Interval(YearMonth)`, `Interval(DayTime)`, and an out-of-domain `Decimal128`, and `false` for a populated `Struct`, a `Map`, and a `List(Struct)` — every recorded answer unchanged
* *AND* those assertions SHALL now pin the NECESSITY OF THE BYPASS rather than the membership of the refused set: a future `arrow-cast` that added a `Struct → Utf8` cast MUST fail this suite, so the deliberate diversion of a nested physical column away from the cast is re-justified rather than left silently redundant
* *AND* the suite SHALL ADDITIONALLY assert that the `List(Int32) → Utf8` cast, which IS available, produces Arrow display text and NOT valid JSON, because that available-but-wrong cast is precisely why availability cannot be the membership test
* *AND* those assertions SHALL use a POPULATED struct rather than a zero-field struct, because a zero-field struct sidesteps the field-wise cast check and is why the existing `convert_tests` and `mapping_tests` assertions passed against a convention that did not hold
* *AND* the suite SHALL therefore FAIL rather than silently re-partition the sets when an `arrow-cast` upgrade changes any of those answers

### Scenario: Every recorded Delta type change is validated, and an unsupported one refuses its column

* *GIVEN* a Delta table whose schema carries `delta.typeChanges` entries on one or more fields — including at least one entry on a field NESTED inside a `struct`, a `map`, or an `array`, and at least one entry whose `fromType`/`toType` pair the Delta protocol's supported list does NOT contain
* *WHEN* the Delta format reader builds that table's logical schema
* *THEN* the reader SHALL validate EVERY recorded entry's `fromType`/`toType` pair against the protocol's supported list, at EVERY nesting depth, because the protocol obliges a reader to *"validate that they support all type changes in the `delta.typeChanges` field … and fail when finding any unsupported type change"*
* *AND* the validation SHALL RECURSE into a `struct`'s inner `StructField`s, a `map`'s key and value types, and an `array`'s element type, replacing the recorded top-level-only walk over `schema.fields()`, because a nested annotation became reachable the moment `struct` and `map` became scannable (the issue #357 finding)
* *AND* the reader SHALL accept exactly the protocol's list — the `Byte` → `Short` → `Int` → `Long` integer chain, `Float` → `Double`, `Byte`/`Short`/`Int` → `Double`, `Date` → `Timestamp without timezone`, `Decimal(p,s)` → `Decimal(p+k1,s+k2)`, `Byte`/`Short`/`Int` → `Decimal(10+k1,k2)`, and `Long` → `Decimal(20+k1,k2)` — and SHALL refuse every other pair, including `Long` → `Double`, at every depth
* *AND* each decimal target SHALL be checked as `k1 >= k2 >= 0` rather than as `P' >= P` and `S' >= S`, so `decimal(10,1)` → `decimal(11,3)` is REFUSED while `decimal(10,1)` → `decimal(12,3)` is accepted
* *AND* the reader SHALL record the offending TOP-LEVEL column's name and a refusal reason naming both types AND the offending field's PATH within that column, through the SAME refused-column list this feature already carries, so an unsupported nested change refuses only the requests that read or emit that column and an operator can locate the field
* *AND* the reported path SHALL compose the STRUCTURAL path from the top-level column down to the annotated field with that entry's OWN `fieldPath` when it carries one, so an entry with `fieldPath: "value"` on a nested field `attrs` of column `payload` reports `payload.attrs.value`
* *AND* the entry's `fromType`/`toType` pair SHALL remain the SOLE validation input and the `fieldPath` MUST NOT be interpreted, coerced into a type decision, or required to resolve against the schema — the pair-only rule is retained; only the recorded JUSTIFICATION for discarding the path is discharged, because a map's and an array's members now reach Exasol inside the rendered JSON
* *AND* the reader SHALL ignore an entry key it does not recognize — notably `tableVersion`, which the superseded RFC required and Delta 3.2-era clients still write, including on all thirteen entries of the vendored `type-widening` fixture — and MUST NOT refuse an otherwise valid entry for carrying one
* *AND* a MALFORMED annotation at ANY depth SHALL surface a `UdfError` exactly as a malformed top-level annotation already does, and MUST NOT be silently skipped because it sits inside a container
* *AND* the reader MUST NOT consult `delta.typeChanges` to decide any CAST at any depth, because the protocol lets a writer remove the annotation once every data file matches the schema and REQUIRES its removal when the feature is dropped
* *AND* a SUPPORTED nested type change SHALL require no compensating action: the renderer serializes the value the Parquet reader returns at the FILE's physical type, so a nested `int` → `long` widening renders an identical JSON number and a nested `decimal(10,1)` → `decimal(12,3)` widening renders `1.5` in the old file and `1.500` in the new one — a rendering difference, never a wrong value, named as a limitation in `datafusion-scan/nested-json-rendering`
* *AND* a table carrying NO `delta.typeChanges` annotation SHALL pass this validation unchanged, whether or not it declares the `typeWidening` reader feature, so every already-queryable Delta fixture keeps its recorded behavior byte-identical

### Scenario: Every nested field's logical name and binding key reach the scan

* *GIVEN* a Delta table declaring a `struct`, `map`, or `array` column, under each of the three column-mapping modes in force — `id`, `name`, and `none`
* *WHEN* the Delta format reader builds that column's `LogicalField`
* *THEN* the field SHALL carry the format-neutral nested descriptor `datafusion-scan/type-mapping` defines, describing the column's whole nested tree: each STRUCT field's LOGICAL name plus the ONE binding key the mode in force selects, and each `array` element and `map` key/value as an UNNAMED member
* *AND* the binding key SHALL be chosen by the SAME three-way rule this feature already applies per top-level column: `delta.columnMapping.id` under `id` mode, `delta.columnMapping.physicalName` under `name` mode, and NEITHER under `none` mode, so at most one member of the pair is ever populated at any depth
* *AND* a MAPPABLE column missing the annotation its mode requires SHALL be refused at the depth it is missing, by the same rule and the same refused-column list a missing top-level annotation already uses, because a nested field whose physical identity the writer never wrote cannot be bound
* *AND* the descriptor SHALL be populated by the SAME recursive walk that validates nested `delta.typeChanges`, so a nested field is visited exactly once and the two answers cannot disagree about which fields exist
* *AND* a PRIMITIVE column's `LogicalField` SHALL carry NO descriptor, so every already-recorded Delta logical field stays byte-identical on the wire
* *AND* the `stats_all_types` fixture's `nested_struct` SHALL carry the logical names `inner_int`, `inner_string`, and `inner_double` with their `name`-mode physical names `col-7f2f94cf-7082-430c-bba7-852bc6c5215e`, `col-26fcfd6b-04c7-4772-8bdf-04ac9425f06e`, and `col-92dcf16d-d249-48a9-afb8-93deeaf7ce23`, which is the pairing that lets the renderer key the JSON by logical name

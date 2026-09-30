# Feature: Delta Schema Type Mapping

Maps every type a Delta table schema can declare either onto the Arrow tag the scan binds it by or
onto a named per-column refusal, so a Delta column is queryable when this engine can render its value
faithfully and refused when it cannot — never described by a tag that returns the wrong value.

<!-- DELTA:CHANGED -->
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
  so a nested `binary` does NOT lack the faithful rendering that a top-level one lacks. Admitting it would
  nonetheless change Binary's reach, which issue #351 owns and issue #350 is scoped out of. So
  `array<binary>` stays refused exactly as recorded, and `struct` and `map` containing a `binary`
  member JOIN it. An Iceberg table's `binary`, `fixed(L)`, and `uuid` are refused at every depth by
  the same rule, per `vs-adapter/binary-column-refusal`.
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
  unifying both formats"*) is discharged for struct and map, and `vs-adapter/binary-column-refusal`
  discharges it for `binary`.
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
* **`binary` has no faithful text rendering, so it is refused rather than tagged `utf8`.**
  `can_cast_types(Binary, Utf8)` is `true`, but a byte sequence that is not valid UTF-8 has no text
  value. A `utf8`-tagged column over such bytes fails the whole query (sqlCode 22002,
  `emit_batch: IPC read: Invalid UTF8 sequence`, measured live). Issue #351 owns a rendering.
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
  emitting a `binary` column under a text tag. A tag-and-hope design has no such backstop.
* The Iceberg format reader refuses a declared `binary`, `fixed(L)`, or `uuid` column per
  `vs-adapter/binary-column-refusal`, and maps every other Iceberg type.
* **Type classification runs BEFORE the column-mapping binding key.** A column this feature refuses is
  never checked for its `delta.columnMapping.*` annotation, so a table is refused for a column's TYPE
  rather than for an annotation on a column the engine will not read.
* Every error this feature surfaces is a `UdfError`, never a panic, and carries no vended or static
  credential value.
* **This delta is issue #359.** It AMENDS the "Declared Exasol type" column of TWO rows in ONE
  scenario's mapping table and adds no scenario. Every Arrow tag in that table, every nullability
  clause, the `byte`/`short` `int32` clause, the shared decimal-guard clause, and the
  ten-tags-byte-identical clause stay unchanged, as do the text-rendered set, the per-name refusal set,
  and every other scenario of this feature.
* **The Arrow side does NOT move; only the Exasol declaration does.** A Delta `timestamp` keeps the
  `timestamptz_us` tag and a `timestamp without time zone` keeps `timestamp_us`, so the scan binds,
  filters, and coerces exactly as recorded. What changes is the string Exasol is told the column is —
  and only on an engine that can express a fractional-second precision.
* **The declared type is not this feature's own decision to make.** The version rule and both
  declaration strings are owned by `datafusion-scan/type-mapping`; the single `ctx.database_version()`
  read is owned by `vs-adapter/create-virtual-schema`; and the production function that renders a Delta
  column's declaration is `unity_type_name_to_exasol`, whose clause `vs-adapter/
  unity-catalog-create-virtual-schema` owns. This table's "Declared Exasol type" column mirrors those
  answers so a reader of the Delta mapping sees the same declaration the adapter emits — it MUST NOT
  become a second statement of the rule.
* **Delta's own timestamp resolution is microsecond, so the amended declaration is faithful rather
  than generous.** The Delta protocol defines `timestamp` and `timestamp without time zone` as
  microsecond-precision types, matching the Iceberg spec's microsecond `timestamp`/`timestamptz` this
  plan quotes, so the same `TIMESTAMP(6)` target is correct for both formats and neither needs a
  format-specific precision.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A Delta type whose Arrow form cannot be rendered faithfully is refused by name

* *GIVEN* a Delta table schema declaring a column of a type in the refused set — exactly `binary`, `variant`, and any container (`array`, `struct`, or `map`) at least one of whose member types is itself in this set, at any nesting depth
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL emit NO logical field for that column, SHALL record the column's name and a refusal reason naming its Delta type, and MUST NOT emit a logical field whose Arrow tag widens, narrows, or otherwise misdescribes the column
* *AND* `struct` and `map` MUST NOT appear in this set on their own account, and their recorded refusal reasons — *"which arrow-cast reports no cast to text for"* — SHALL be DELETED rather than retained, because the reason no longer describes anything the engine does
* *AND* the refusal reason SHALL name the ACTUAL cause per type: `binary` by its declared type, because binary has no faithful text rendering until issue #351 (`vs-adapter/binary-column-refusal`); `variant` because its on-disk form is an opaque `(metadata BINARY, value BINARY)` pair in a Delta-specific binary encoding whose Arrow form is a struct, so a rendering would be a meaningless blob rather than the value; and a container by naming its own declared type, the PATH of the offending member, and that member's reason
* *AND* ONE composer SHALL build every container refusal — for an `array`'s element, a `struct`'s field, and a `map`'s key or value alike — replacing the recorded array-only composer, so nesting adds no message layer per kind and no operator is told the column has a member's type
* *AND* the `binary` reason SHALL cite issue #351 and MUST NOT cite issue #350, because #350 closes with this plan and a closed issue cited in a shipped error text reads as an unfixed gap with no owner
* *AND* `binary` SHALL stay refused at EVERY nesting depth even though the JSON encoder renders a `Binary` member as faithful lowercase hexadecimal, because widening Binary's reach is issue #351's scope and not this plan's
* *AND* `variant`'s refusal SHALL stand INDEPENDENTLY of the reader-feature gate that also refuses `variantType` and `variantType-preview` (`vs-adapter/delta-reader-feature-gating`), so a table declaring a `variant` column without declaring the feature is still refused
* *AND* the type classification SHALL run BEFORE the column's `delta.columnMapping.*` binding key is read, so a refused column is refused for its TYPE and never for a missing annotation on a column the engine will not read
<!-- /DELTA:CHANGED -->

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

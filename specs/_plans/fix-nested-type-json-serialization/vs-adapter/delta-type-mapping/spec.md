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

## Scenarios

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The castability claims behind the mapping are asserted, not assumed

* *GIVEN* the three sets above, whose membership is now decided by whether the JSON renderer can render the Arrow type the scan physically reads — no longer by whether that type can be cast to `Utf8`
* *WHEN* the type-mapping test suite runs
* *THEN* it SHALL STILL assert `arrow::compute::can_cast_types(physical, DataType::Utf8)` directly for each representative physical Arrow type, asserting `true` for `Binary`, `List(Int32)`, `Interval(YearMonth)`, `Interval(DayTime)`, and an out-of-domain `Decimal128`, and `false` for a populated `Struct`, a `Map`, and a `List(Struct)` — every recorded answer unchanged
* *AND* those assertions SHALL now pin the NECESSITY OF THE BYPASS rather than the membership of the refused set: a future `arrow-cast` that added a `Struct → Utf8` cast MUST fail this suite, so the deliberate diversion of a nested physical column away from the cast is re-justified rather than left silently redundant
* *AND* the suite SHALL ADDITIONALLY assert that the `List(Int32) → Utf8` cast, which IS available, produces Arrow display text and NOT valid JSON, because that available-but-wrong cast is precisely why availability cannot be the membership test
* *AND* those assertions SHALL use a POPULATED struct rather than a zero-field struct, because a zero-field struct sidesteps the field-wise cast check and is why the existing `convert_tests` and `mapping_tests` assertions passed against a convention that did not hold
* *AND* the suite SHALL therefore FAIL rather than silently re-partition the sets when an `arrow-cast` upgrade changes any of those answers
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Every nested field's logical name and binding key reach the scan

* *GIVEN* a Delta table declaring a `struct`, `map`, or `array` column, under each of the three column-mapping modes in force — `id`, `name`, and `none`
* *WHEN* the Delta format reader builds that column's `LogicalField`
* *THEN* the field SHALL carry the format-neutral nested descriptor `datafusion-scan/type-mapping` defines, describing the column's whole nested tree: each STRUCT field's LOGICAL name plus the ONE binding key the mode in force selects, and each `array` element and `map` key/value as an UNNAMED member
* *AND* the binding key SHALL be chosen by the SAME three-way rule this feature already applies per top-level column: `delta.columnMapping.id` under `id` mode, `delta.columnMapping.physicalName` under `name` mode, and NEITHER under `none` mode, so at most one member of the pair is ever populated at any depth
* *AND* a MAPPABLE column missing the annotation its mode requires SHALL be refused at the depth it is missing, by the same rule and the same refused-column list a missing top-level annotation already uses, because a nested field whose physical identity the writer never wrote cannot be bound
* *AND* the descriptor SHALL be populated by the SAME recursive walk that validates nested `delta.typeChanges`, so a nested field is visited exactly once and the two answers cannot disagree about which fields exist
* *AND* a PRIMITIVE column's `LogicalField` SHALL carry NO descriptor, so every already-recorded Delta logical field stays byte-identical on the wire
* *AND* the `stats_all_types` fixture's `nested_struct` SHALL carry the logical names `inner_int`, `inner_string`, and `inner_double` with their `name`-mode physical names `col-7f2f94cf-7082-430c-bba7-852bc6c5215e`, `col-26fcfd6b-04c7-4772-8bdf-04ac9425f06e`, and `col-92dcf16d-d249-48a9-afb8-93deeaf7ce23`, which is the pairing that lets the renderer key the JSON by logical name
<!-- /DELTA:NEW -->

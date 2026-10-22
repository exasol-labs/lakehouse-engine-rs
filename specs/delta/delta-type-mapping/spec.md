# Feature: Delta Schema Type Mapping

The adapter maps each type a Delta table schema can declare onto an Arrow tag or onto a named per-column refusal. A Delta column is queryable when the engine can render its value faithfully and refused when it cannot.

## Background

* The classification runs once at `createVirtualSchema`, over Unity Catalog's `type_json` (the Delta `StructField` JSON with its metadata) and the `delta.columnMapping.mode` table property. The outcome reaches each pushdown through the column notes (`vs-adapter/column-source-notes`). Where a scenario says the Delta format reader builds a logical field or records a refusal, the declaration does so and the reader takes the result from the notes.
* A malformed `delta.typeChanges` or `delta.columnMapping.*` annotation refuses its column on the refused-column list. It does not fail the virtual schema.
* Nested rendering is owned by `scan-types/nested-json-rendering`. This feature decides which Delta type reaches it. A container (`array`, `struct`, `map`) is mappable when every member type is JSON-renderable.
* `binary` and `variant` are refused at every depth. `binary` has no faithful text rendering. A `utf8`-tagged column over non-UTF-8 bytes fails the query, because Exasol rejects the emitted bytes with `Invalid bytestream`. `vs-adapter/binary-column-refusal` owns the `binary` refusal. The JSON encoder renders a nested `Binary` as lowercase hexadecimal, but the engine keeps nested `binary` refused. `variant` is an opaque `(metadata BINARY, value BINARY)` pair whose rendering would be a meaningless blob. Iceberg `binary`, `fixed(L)`, and `uuid` are refused the same way.
* `can_cast_types` answers that `Struct → Utf8` and `Map → Utf8` are not castable, and that `List(Int32) → Utf8` is castable but yields display text, not JSON. The scan diverts a nested physical column away from the cast, and the tests pin these answers so a change in the Arrow cast kernel forces a review. A zero-field struct sidesteps the cast check, so tests use a populated struct.
* `byte` and `short` use the `int32` tag. The scan's physical-expression adapter widens the Parquet `Int8` and `Int16` to `Int32` losslessly.
* The Delta protocol `void` type is never materialized in data files. Writers omit it, and readers reconstruct it as an all-NULL column. `interval year to month` and `interval day to second` are not in `PROTOCOL.md`. They exist as `delta_kernel` `PrimitiveType` variants, so they map defensively.
* A Delta `timestamp` keeps the `timestamptz_us` tag and `timestamp without time zone` keeps `timestamp_us`. Only the declared Exasol precision depends on the engine version. `scan-types/type-mapping` owns the version rule and the declaration strings, `vs-adapter/create-virtual-schema` owns the `ctx.database_version()` read, and `spark_primitive_to_exasol` renders the declaration (`vs-adapter/unity-catalog-create-virtual-schema`). The table below mirrors those answers and does not restate the rule. Delta timestamps have microsecond precision.
* Refusal is scoped to the column. A table with one struct column stays readable on its other columns. A refused column is absent from the logical schema, so a gate miss fails with a DataFusion "no field named" error and never emits a `binary` column under a text tag.
* Type classification runs before the column-mapping binding key is read, so a table is refused for a column's type and not for an annotation on a column the engine will not read.
* Delta protocol "Reader Requirements for Type Widening": a reader converts values from files written before a type change to the current, wider type (`scan-types/type-relaxation`), and validates every type change in `delta.typeChanges` and fails on an unsupported one (this feature). The supported pairs are the integer chain `Byte` → `Short` → `Int` → `Long`, `Float` → `Double`, `Byte`/`Short`/`Int` → `Double`, `Date` → `Timestamp without timezone`, `Decimal(p, s)` → `Decimal(p + k1, s + k2)`, `Byte`/`Short`/`Int` → `Decimal(10 + k1, k2)`, and `Long` → `Decimal(20 + k1, k2)`, each with `k1 >= k2 >= 0`. The decimal constraint is stronger than "precision and scale both grow": `decimal(10,1)` → `decimal(11,3)` is not legal. `Long` → `Double` is absent from the protocol because it is lossy above 2^53.
* A `delta.typeChanges` entry has `fromType`, `toType`, and an optional `fieldPath` (`"key"`, `"value"`, `"element"`, dotted for nesting). Some Delta clients also write `tableVersion`, which the current specification omits, so the parser ignores unknown keys.
* `delta.typeChanges` is a validation input and never a cast input. A writer may remove it once all data files match the schema. The scan reads the physical Parquet type from each file's footer and casts to the current logical type.
* The refusal uses the per-column refused-column list on `ResolvedScan`. A table-scoped refusal would make a readable table unreachable over a column nobody selected.
* Delta assigns column-mapping annotations to nested struct fields. Under `name` mode a nested struct's inner fields carry generated physical names such as `col-<uuid>`, so the renderer must key the JSON by logical inner names.
* Only a struct field carries a name. A Delta `array`'s `elementType` and a `map`'s `keyType` and `valueType` are unnamed members and bind by identity.
* The nested descriptor is a recursion of `LogicalField`'s own binding-key choice (`field_id`, `physical_name`, or neither), defined by `scan-types/type-mapping`. Delta populates it from `delta.columnMapping.id` under `id` mode, from `delta.columnMapping.physicalName` under `name` mode, and from neither under `none` mode. No Delta-specific struct reaches the wire.
* `raw_scan` registers the logical schema as the DataFusion table schema, and a column tagged `utf8` over an incompatible physical type fails at file open before any per-value JSON conversion runs. `raw_scan` also treats an empty `logical_schema` as "infer from the first data file", which binds columns by physical file order and name.
* Every error is a `UdfError`, never a panic, and carries no credential value.

## Scenarios

### Scenario: Every Delta type Exasol represents natively maps to its own Arrow tag

* *GIVEN* a Delta table schema declaring one column of each type in the native set, exactly
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
  | `timestamp` | `timestamptz_us` | TIMESTAMP(6) on Exasol 2025.x and later, TIMESTAMP on 8.x |
  | `timestamp without time zone` | `timestamp_us` | TIMESTAMP(6) on Exasol 2025.x and later, TIMESTAMP on 8.x |
  | `decimal(p,s)`, `1 ≤ p ≤ 36` and `s ≤ p` | `decimal128(p,s)` | DECIMAL(p,s) |

* *AND* `byte` and `short` SHALL both carry the `int32` tag, and the Delta reader MUST NOT
  tag either column `int8` or `int16`, because the Parquet reader produces physical `Int8` and
  `Int16` and the scan's physical-expression adapter widens each to logical `Int32`
  losslessly
* *AND* `compatible_exasol_type` SHALL return `DECIMAL(3,0)`, `DECIMAL(5,0)`, and `DECIMAL(10,0)` for `byte`, `short`, and `integer`, exactly as the table above declares
* *AND* the decimal domain check SHALL read the SINGLE shared
  `exasol_representable_catalog_decimal` predicate in `crates/lakehouse-engine/src/types/mapping.rs`
  and MUST NOT carry its own copy, so the Delta, Iceberg, and Unity Catalog answers stay in lockstep
  by construction, as `scan-types/type-mapping` requires
* *AND* the two TIMESTAMP declarations SHALL read the version rule and both declaration strings from
  the SINGLE owner `scan-types/type-mapping` specifies, so a Delta timestamp and an Iceberg
  timestamp are declared at the same precision by construction; this table mirrors that answer and
  MUST NOT restate the rule or either literal
* *AND* only the fractional-second precision of the two timestamp rows SHALL depend on the Exasol version, and it SHALL differ on the 2025.x arm only

### Scenario: A Delta type Exasol cannot represent natively is surfaced as a VARCHAR rendering

* *GIVEN* a Delta table schema declaring one column of each type in the text-rendered set, exactly `decimal(p,s)` outside Exasol's catalog-decimal domain (`p = 0`, `p > 36`, or `s > p`), `void`, `interval year to month`, `interval day to second`, and a CONTAINER type, `array<E>`, `struct<…>`, or `map<K,V>`, every one of whose member types is itself from the native set, from this set, or is itself such a container, applied recursively
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* each such column SHALL carry the `utf8` Arrow tag and SHALL be declared to Exasol as `VARCHAR(2000000)`, as the Iceberg mapping does for its equivalents
* *AND* `struct` and `map` SHALL be members of THIS set and MUST NOT be refused, so a `map` column and a nested `struct` column are queryable
* *AND* the container rule SHALL be RECURSIVE on every member type, an `array`'s element, a `struct`'s every field, and a `map`'s key AND value, and the recursion SHALL be based on whether each member is JSON-RENDERABLE, not on `can_cast_types`, so `array<struct<…>>` and `map<string, array<integer>>` are mappable while `array<binary>` and `struct<v: variant>` are not
* *AND* an `array`, `struct`, or `map` column SHALL be surfaced as STRICT JSON per `scan-types/nested-json-rendering`, so an `array` of integers returns a JSON array of bare numbers rather than `[1, 2]` display text
* *AND* a `void` column SHALL read as all-NULL rather than as an error or an empty string, and a `void` field NESTED inside a container SHALL render as an explicit JSON `null`, because the protocol requires writers to omit it from every data file and readers to *"reconstruct them as all-`null` columns"*
* *AND* an out-of-Exasol-domain `decimal` SHALL reach this set through the SAME shared `exasol_representable_catalog_decimal` predicate the native set reads, so no decimal pair can be classified native by one call site and text-rendered by another
* *AND* an out-of-domain `decimal` NESTED inside a container SHALL keep the container mappable, because the JSON renderer renders a decimal as a bare JSON number at any depth with no Exasol DECIMAL domain to satisfy

### Scenario: A Delta type whose Arrow form cannot be rendered faithfully is refused by name

* *GIVEN* a Delta table schema declaring a column of a type in the refused set, exactly `binary`, `variant`, and any container (`array`, `struct`, or `map`) at least one of whose member types is itself in this set, at any nesting depth
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* the reader SHALL emit NO logical field for that column, SHALL record the column's name and a refusal reason naming its Delta type, and MUST NOT emit a logical field whose Arrow tag widens, narrows, or otherwise misdescribes the column
* *AND* `struct` and `map` MUST NOT appear in this set on their own account
* *AND* the refusal reason SHALL name the ACTUAL cause per type: `binary` by its declared type, as `vs-adapter/binary-column-refusal` specifies; `variant` because its on-disk form is an opaque `(metadata BINARY, value BINARY)` pair in a Delta-specific binary encoding whose Arrow form is a struct, so a rendering would be a meaningless blob rather than the value; and a container by naming its own declared type, the PATH of the offending member, and that member's reason
* *AND* ONE composer SHALL build every container refusal, for an `array`'s element, a `struct`'s field, and a `map`'s key or value alike, so nesting adds no message layer per kind and no operator is told the column has a member's type
* *AND* `binary` SHALL stay refused at EVERY nesting depth even though the JSON encoder renders a `Binary` member as faithful lowercase hexadecimal
* *AND* `variant`'s refusal SHALL stand INDEPENDENTLY of the reader-feature gate that also refuses `variantType` and `variantType-preview` (`delta/delta-reader-feature-gating`), so a table declaring a `variant` column without declaring the feature is still refused
* *AND* the type classification SHALL run BEFORE the column's `delta.columnMapping.*` binding key is read, so a refused column is refused for its TYPE and never for a missing annotation on a column the engine will not read

### Scenario: A refused column refuses only the requests that read or emit it

* *GIVEN* a Delta table whose schema mixes mappable columns, including `array`, `map`, and nested `struct` columns, with one refused column `binary_col`
* *WHEN* the adapter handles a pushdown request against that table
* *THEN* the adapter SHALL plan the request normally when it reads and emits only mappable columns, and SHALL return a `UdfError` naming the refused column and its refusal reason when the request would read or emit `binary_col`
* *AND* a request naming a `map` or nested `struct` column SHALL be planned normally, because neither type is refused
* *AND* the refusal decision SHALL be taken from the pushdown request's OWN column references, collected by ONE recursive walk over the whole request JSON for every `column` node's name, UNIONED with the final projection the adapter renders, and MUST NOT be assembled from a per-clause enumeration
* *AND* the request-JSON half SHALL cover a refused column reached through a WHERE filter, a GROUP BY key, an ORDER BY key, an aggregate argument, or a join condition
* *AND* the projection half SHALL union in the full-base-row projection ONLY when the request's own select list is absent or empty, a genuine `SELECT *`, so `SELECT *` over a table carrying a refused column is refused rather than emitting a column the scan cannot bind
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
  file", which would bind columns by physical file order and by physical file name, the exact
  unauthorized binding `delta/delta-table-planning` refuses an ordinal field-id for
* *AND* the whole-table refusal SHALL be the ONLY case in which a refused column refuses a request
  that does not name it, so the per-column scoping stays the rule and this stays its single, stated
  exception

### Scenario: The castability claims behind the mapping are asserted, not assumed

* *GIVEN* the three sets above, whose membership is decided by whether the JSON renderer can render the Arrow type the scan physically reads, not by whether that type can be cast to `Utf8`
* *WHEN* the type-mapping test suite runs
* *THEN* it SHALL assert `arrow::compute::can_cast_types(physical, DataType::Utf8)` directly for each representative physical Arrow type, asserting `true` for `Binary`, `List(Int32)`, `Interval(YearMonth)`, `Interval(DayTime)`, and an out-of-domain `Decimal128`, and `false` for a populated `Struct`, a `Map`, and a `List(Struct)`
* *AND* those assertions SHALL pin the NECESSITY OF THE BYPASS rather than the membership of the refused set: an Arrow change that added a `Struct → Utf8` cast MUST fail this suite, so the deliberate diversion of a nested physical column away from the cast is re-justified rather than left silently redundant
* *AND* the suite SHALL assert that the `List(Int32) → Utf8` cast, which IS available, produces Arrow display text and NOT valid JSON, because that available-but-wrong cast is why availability cannot be the membership test
* *AND* those assertions SHALL use a POPULATED struct rather than a zero-field struct, because a zero-field struct sidesteps the field-wise cast check

### Scenario: Every recorded Delta type change is validated, and an unsupported one refuses its column

* *GIVEN* a Delta table whose schema carries `delta.typeChanges` entries on one or more fields, including at least one entry on a field NESTED inside a `struct`, a `map`, or an `array`, and at least one entry whose `fromType`/`toType` pair the Delta protocol's supported list does NOT contain
* *WHEN* `createVirtualSchema` declares that table's columns from Unity Catalog's `type_json` metadata, which carries the log's `StructField` annotations
* *THEN* the reader SHALL validate EVERY recorded entry's `fromType`/`toType` pair against the protocol's supported list, at EVERY nesting depth, because the protocol obliges a reader to *"validate that they support all type changes in the `delta.typeChanges` field … and fail when finding any unsupported type change"*
* *AND* the validation SHALL RECURSE into a `struct`'s inner `StructField`s, a `map`'s key and value types, and an `array`'s element type, because a nested annotation is reachable once `struct` and `map` are scannable
* *AND* the reader SHALL accept exactly the protocol's list, the `Byte` → `Short` → `Int` → `Long` integer chain, `Float` → `Double`, `Byte`/`Short`/`Int` → `Double`, `Date` → `Timestamp without timezone`, `Decimal(p,s)` → `Decimal(p+k1,s+k2)`, `Byte`/`Short`/`Int` → `Decimal(10+k1,k2)`, and `Long` → `Decimal(20+k1,k2)`, and SHALL refuse every other pair, including `Long` → `Double`, at every depth
* *AND* each decimal target SHALL be checked as `k1 >= k2 >= 0` rather than as `P' >= P` and `S' >= S`, so `decimal(10,1)` → `decimal(11,3)` is REFUSED while `decimal(10,1)` → `decimal(12,3)` is accepted
* *AND* the reader SHALL record the offending TOP-LEVEL column's name and a refusal reason naming both types AND the offending field's PATH within that column, through the refused-column list, so an unsupported nested change refuses only the requests that read or emit that column and an operator can locate the field
* *AND* the reported path SHALL compose the STRUCTURAL path from the top-level column down to the annotated field with that entry's OWN `fieldPath` when it carries one, so an entry with `fieldPath: "value"` on a nested field `attrs` of column `payload` reports `payload.attrs.value`
* *AND* the entry's `fromType`/`toType` pair SHALL remain the SOLE validation input and the `fieldPath` MUST NOT be interpreted, coerced into a type decision, or required to resolve against the schema
* *AND* the reader SHALL ignore an entry key it does not recognize, notably `tableVersion`, which some Delta clients still write, and MUST NOT refuse an otherwise valid entry for carrying one
* *AND* a MALFORMED annotation at ANY depth SHALL refuse its top-level column, with a reason naming the field's path and the problem, through the same refused-column list an unsupported change uses, and MUST NOT be silently skipped because it sits inside a container, nor fail `createVirtualSchema`
* *AND* the reader MUST NOT consult `delta.typeChanges` to decide any CAST at any depth, because the protocol lets a writer remove the annotation once every data file matches the schema and REQUIRES its removal when the feature is dropped
* *AND* a SUPPORTED nested type change SHALL require no compensating action: the renderer serializes the value the Parquet reader returns at the FILE's physical type, so a nested `int` → `long` widening renders an identical JSON number and a nested `decimal(10,1)` → `decimal(12,3)` widening renders `1.5` in the old file and `1.500` in the new one, a rendering difference, never a wrong value, named as a limitation in `scan-types/nested-json-rendering`
* *AND* a table carrying NO `delta.typeChanges` annotation SHALL pass this validation unchanged, whether or not it declares the `typeWidening` reader feature, so such a table's columns are classified by type alone

### Scenario: Every nested field's logical name and binding key reach the scan

* *GIVEN* a Delta table declaring a `struct`, `map`, or `array` column, under each of the three column-mapping modes in force, `id`, `name`, and `none`
* *WHEN* the Delta format reader builds that column's `LogicalField`
* *THEN* the field SHALL carry the format-neutral nested descriptor `scan-types/type-mapping` defines, describing the column's whole nested tree: each STRUCT field's LOGICAL name plus the ONE binding key the mode in force selects, and each `array` element and `map` key/value as an UNNAMED member
* *AND* the binding key SHALL be chosen by the SAME three-way rule this feature applies per top-level column: `delta.columnMapping.id` under `id` mode, `delta.columnMapping.physicalName` under `name` mode, and NEITHER under `none` mode, so at most one member of the pair is ever populated at any depth
* *AND* a MAPPABLE column missing the annotation its mode requires SHALL be refused at the depth it is missing, by the same rule and the same refused-column list a missing top-level annotation uses, because a nested field whose physical identity the writer never wrote cannot be bound
* *AND* the descriptor SHALL be populated by the SAME recursive walk that validates nested `delta.typeChanges`, so a nested field is visited exactly once and the two answers cannot disagree about which fields exist
* *AND* a PRIMITIVE column's `LogicalField` SHALL carry NO descriptor, so a primitive field adds nothing to the wire
* *AND* under `name` mode a nested struct's inner fields SHALL carry their logical names plus their generated physical names, which is the pairing that lets the renderer key the JSON by logical name

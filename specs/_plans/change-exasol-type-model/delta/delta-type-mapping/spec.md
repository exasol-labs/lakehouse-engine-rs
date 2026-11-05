# Feature: Delta Schema Type Mapping

The adapter maps each type a Delta table schema can declare onto an Arrow tag or onto a named per-column refusal. A Delta column is queryable when the engine can render its value faithfully and refused when it cannot.

## Background

* The classification runs once at `createVirtualSchema`, over Unity Catalog's `type_json` (the Delta `StructField` JSON with its metadata) and the `delta.columnMapping.mode` table property. The outcome reaches each pushdown through the column notes (`vs-adapter/column-source-notes`). Where a scenario says the Delta format reader builds a logical field or records a refusal, the declaration does so and the reader takes the result from the notes.
* A malformed `delta.typeChanges` or `delta.columnMapping.*` annotation refuses its column on the refused-column list. It does not fail the virtual schema.
* Nested rendering is owned by `scan-types/nested-json-rendering`. This feature decides which Delta type reaches it. A container (`array`, `struct`, `map`) is mappable when every member type is JSON-renderable.
* `binary` maps to the `binary` tag at the top level and renders as lowercase hex at every depth (`vs-adapter/binary-column-rendering`). A `utf8`-tagged column over non-UTF-8 bytes fails the query, because Exasol rejects the emitted bytes with `Invalid bytestream`.
* `variant` is refused at every depth. It is an opaque `(metadata BINARY, value BINARY)` pair whose rendering would be a meaningless blob.
* `can_cast_types` answers that `Struct → Utf8` and `Map → Utf8` are not castable, and that `List(Int32) → Utf8` is castable but yields display text, not JSON. The scan diverts a nested physical column away from the cast, and the tests pin these answers so a change in the Arrow cast kernel forces a review. A zero-field struct sidesteps the cast check, so tests use a populated struct.
* `byte` and `short` use the `int32` tag. The scan's physical-expression adapter widens the Parquet `Int8` and `Int16` to `Int32` losslessly.
* The Delta protocol `void` type is never materialized in data files. Writers omit it, and readers reconstruct it as an all-NULL column. `interval year to month` and `interval day to second` are not in `PROTOCOL.md`. They exist as `delta_kernel` `PrimitiveType` variants, so they map defensively.
* A Delta `timestamp` keeps the `timestamptz_us` tag and `timestamp without time zone` keeps `timestamp_us`. Only the declared Exasol precision depends on the engine version. `scan-types/type-mapping` owns the version rule and the declaration strings, `vs-adapter/create-virtual-schema` owns the `ctx.database_version()` read, and `spark_primitive_to_exasol` renders the declaration (`vs-adapter/unity-catalog-create-virtual-schema`). The table below mirrors those answers and does not restate the rule. Delta timestamps have microsecond precision.
* Refusal is scoped to the column. A table with one struct column stays readable on its other columns. A refused column is absent from the logical schema, so a gate miss fails with a DataFusion "no field named" error and never emits a refused column.
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

<!-- DELTA:CHANGED -->
### Scenario: Every Delta type Exasol represents natively maps to its own Arrow tag

* *GIVEN* a Delta table schema declaring one column of each type in the native set, exactly
  `boolean`, `byte`, `short`, `integer`, `long`, `float`, `double`, `string`, `date`, `timestamp`,
  `timestamp without time zone`, and `decimal(p,s)` with `1 ≤ p ≤ 36` and `0 ≤ s ≤ p`
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
  | `decimal(p,s)`, `1 ≤ p ≤ 36` and `0 ≤ s ≤ p` | `decimal128(p,s)` | DECIMAL(p,s) |

* *AND* `byte` and `short` SHALL both carry the `int32` tag, and the Delta reader MUST NOT
  tag either column `int8` or `int16`, because the Parquet reader produces physical `Int8` and
  `Int16` and the scan widens each to logical `Int32` losslessly
* *AND* `byte`, `short`, and `integer` SHALL be declared `DECIMAL(3,0)`, `DECIMAL(5,0)`, and `DECIMAL(10,0)`, exactly as the table above declares
* *AND* a Delta `decimal(p,s)` SHALL map to `DECIMAL(p,s)` exactly when `1 ≤ p ≤ 36` and `0 ≤ s ≤ p`, the domain `scan-types/type-mapping` states for every decimal, and to the `VARCHAR` rendering otherwise
* *AND* a Delta timestamp SHALL be declared at the precision `scan-types/type-mapping` gives an Iceberg timestamp on the same Exasol version
* *AND* only the fractional-second precision of the two timestamp rows SHALL depend on the Exasol version, and it SHALL differ on the 2025.x arm only
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A Delta type Exasol cannot represent natively is surfaced as a VARCHAR rendering

* *GIVEN* a Delta table schema declaring one column of each type in the text-rendered set, exactly `decimal(p,s)` outside `1 ≤ p ≤ 36` and `0 ≤ s ≤ p` (`p = 0`, `p > 36`, or `s > p`), `void`, `interval year to month`, `interval day to second`, and a CONTAINER type, `array<E>`, `struct<…>`, or `map<K,V>`, every one of whose member types is itself from the native set, from this set, or is itself such a container, applied recursively
* *WHEN* the Delta format reader resolves that table's scan
* *THEN* each such column SHALL carry the `utf8` Arrow tag and SHALL be declared to Exasol as `VARCHAR(2000000)`, as the Iceberg mapping does for its equivalents
* *AND* `struct` and `map` SHALL be members of THIS set and MUST NOT be refused, so a `map` column and a nested `struct` column are queryable
* *AND* the container rule SHALL be RECURSIVE on every member type, an `array`'s element, a `struct`'s every field, and a `map`'s key AND value, and the recursion SHALL be based on whether each member is JSON-RENDERABLE, not on whether Arrow can cast it to text, so `array<struct<…>>`, `map<string, array<integer>>`, and `array<binary>` are mappable while `struct<v: variant>` is not
* *AND* an `array`, `struct`, or `map` column SHALL be surfaced as STRICT JSON per `scan-types/nested-json-rendering`, so an `array` of integers returns a JSON array of bare numbers rather than `[1, 2]` display text
* *AND* a `void` column SHALL read as all-NULL rather than as an error or an empty string, and a `void` field NESTED inside a container SHALL render as an explicit JSON `null`, because the protocol requires writers to omit it from every data file and readers to *"reconstruct them as all-`null` columns"*
* *AND* a Delta `decimal(p,s)` SHALL map to `DECIMAL(p,s)` exactly when `1 ≤ p ≤ 36` and `0 ≤ s ≤ p`, the domain `scan-types/type-mapping` states for every decimal, and to the `VARCHAR` rendering otherwise
* *AND* an out-of-domain `decimal` NESTED inside a container SHALL keep the container mappable, because the JSON renderer renders a decimal as a bare JSON number at any depth with no Exasol DECIMAL domain to satisfy
<!-- /DELTA:CHANGED -->

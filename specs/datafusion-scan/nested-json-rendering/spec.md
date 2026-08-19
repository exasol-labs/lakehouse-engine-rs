# Feature: Nested-Type JSON Rendering

Renders every list, struct, and map column as a single valid JSON document per value, so a nested
lakehouse column is queryable through Exasol as the `VARCHAR(2000000)` the schema already declares
for it instead of failing the scan or returning Arrow display text.

## Background

* **This feature is issue #350.** `datafusion-scan/type-mapping` has recorded a JSON-`VARCHAR`
  contract for List, Struct, and Map since the project began; no encoder was ever built for it.
  `vs-adapter/delta-type-mapping` already records the diagnosis verbatim: *"`raw_scan` registers the
  logical schema — an incompatible column tagged `utf8` — as the DataFusion table schema, and
  DataFusion's physical-expression adapter validates physical-against-logical castability at file
  open, BEFORE any per-value JSON conversion runs … Neither ever reaches the JSON path, on EITHER
  table format."* This feature builds the encoder and routes both formats through it.
* **Binary is OUT OF SCOPE and its behavior MUST NOT change.** Issue #351 owns Binary's JSON
  validity. A top-level Delta `binary` column stays refused and a top-level Iceberg binary column
  keeps its `CAST(col AS VARCHAR)` display-text path, unchanged by this feature.
* **Only a list of PRIMITIVES survives today; a list of structs fails like a struct.** Measured live
  against Exasol over a seeded Iceberg table: `list<string>` and `list<int>` return display text, while
  `list<struct<a: int>>` fails with the same physical-to-logical cast error as a bare struct, because
  `arrow-cast` recurses `(List(inner), Utf8) => can_cast_types(inner, Utf8)` and the inner struct
  answers false. So "list works" describes exactly one case and this feature must not be scoped to the
  other three as if list were already correct.
* **Today's list rendering loses a null ELEMENT to the empty string**, which the issue did not state.
  Measured character-exact through Exasol: `["hello","world"]` returns `[hello, world]` (14 chars),
  `["a", null]` returns `[a, ]` (5 chars), `[null, 5]` returns `[, 5]` (5 chars), an empty list returns
  `[]`, and a NULL list returns SQL NULL. So the current text is invalid JSON in the unquoted-string
  case AND ambiguous between a null element and an empty string.
* **The same defect surfaces at two different layers per format, and only the Delta one is a clean
  refusal.** On the Delta path the adapter refuses the column at PLAN time through its refused-column
  list. On the Iceberg path `adapter/pushdown/format/iceberg.rs` hardcodes an EMPTY refused-column
  list, so nothing is refused and the failure lands at SCAN time as
  `scan failed: assigned data could not be read: Execution error: Cannot cast column …`. This feature
  removes the cause, so both surfaces go away; it does not add an Iceberg refusal mechanism.
* **The JSON shape is chosen for Exasol SQL ergonomics, not from the Iceberg spec's Appendix D.**
  The Apache Iceberg table spec's § JSON single-value serialization
  (https://iceberg.apache.org/spec/#json-single-value-serialization) does prescribe a JSON shape per
  type, and this feature deliberately does NOT adopt it:

  | Iceberg type | Appendix D JSON representation | Appendix D example |
  |---|---|---|
  | `struct` | *"JSON object by field ID"* | `{"1": 1, "2": "bar"}` |
  | `list` | *"JSON array of values"* | `[1, 2, 3]` |
  | `map` | *"JSON object of key and value arrays"* | `{ "keys": ["a", "b"], "values": [1, 2] }` |

  Appendix D is scoped to metadata single values, NOT to query results: § Schemas states *"default
  values are serialized using the JSON single-value serialization in Appendix D"*, § Bound
  serialization scopes the binary form to *"the lower and upper bounds maps of manifest files"*, and
  the spec defines no JSON encoding for scan output rows at all. Adopting Appendix D's shapes would
  make the emitted VARCHAR unusable from Exasol SQL: a struct keyed by numeric field ID cannot be
  read by a `JSON_VALUE(col, '$.city')`-style path, and a map split into parallel `keys`/`values`
  arrays cannot be read by key at all. This feature therefore keys a struct by FIELD NAME and a map
  by its STRINGIFIED KEY, and records the divergence here rather than leaving it silent.
* **The Iceberg spec permits any type as a map key, and requires keys to be non-null.** § Nested
  Types (https://iceberg.apache.org/spec/#nested-types): *"A `map` is a collection of key-value pairs
  with a key type and a value type. Both the key field and value field each have an integer id that
  is unique in the table schema. Map keys are required and map values can be either optional or
  required. Both map keys and map values may be any type, including nested types."* A JSON object
  name is a string (RFC 8259), so every non-string key MUST be stringified.
* **The Iceberg spec states NO key-uniqueness and NO key-ordering rule for a `map` value.** Neither
  appears anywhere in the spec; the only "unique … map keys" sentence in the document is about the
  table-metadata `refs` field, not the `map` data type. RFC 8259 says object names *SHOULD* be unique
  and that an object is an unordered collection. This feature therefore preserves the source entry
  order and does NOT deduplicate: a spec-legal duplicate-keyed map renders as a JSON object with
  repeated names, which a consumer may resolve as last-wins. Naming that is the honest alternative to
  a deduplication rule the spec does not authorize.
* **Struct field order is the physical file's field order, and the Iceberg spec makes that
  non-semantic.** § Column Projection: *"Columns in Iceberg data files are selected by field id. The
  table schema's column names and order may change after a data file is written, and projection must
  be done using field ids."* § Schema Evolution permits *"reordering existing fields"*. So two data
  files of one table may legally carry a struct's fields in different orders, and the rendered JSON
  key order follows each file. Because both the DataFusion-side and the Exasol-side view of the
  column read the SAME rendered string, the two engines can never disagree about a value; the
  consequence is confined to a logically-equal value rendering as two distinct strings across such
  files, which `GROUP BY` and `DISTINCT` would then separate. The rendering is NOT re-sorted into a
  canonical key order, because a canonical order would diverge from the declared schema order that
  every single-layout table (the overwhelming majority) renders today.
* **Field NAMES in the rendered JSON are the LOGICAL names, resolved by binding key.** The Iceberg
  spec's § Nested Types gives every nested field its own id — *"Each field in the tuple is named and
  has an integer id that is unique in the table schema"* (struct), *"The element field has an integer
  id that is unique in the table schema"* (list), *"Both the key field and value field each have an
  integer id"* (map) — and § Column Projection makes id-based projection normative. The Delta
  protocol likewise assigns `delta.columnMapping.id` and `delta.columnMapping.physicalName` to nested
  fields, so a column-mapped Delta struct stores names like `col-7f2f94cf-7082-430c-bba7-852bc6c5215e`
  on disk. Rendering the physical name would emit those opaque identifiers as JSON keys — the vendored
  `stats-all-types` fixture is exactly this shape (`delta.columnMapping.mode = name`, three UUID-named
  inner fields). The nested field tree is therefore resolved to logical names before rendering, by the
  same binding-key rule the top-level columns already use.
* **The JSON rendering is longer than the display text it replaces, so Exasol's
  `VARCHAR(2000000)` cap is more reachable than before.** Quoting and escaping add bytes to every
  string, and `explicit_nulls` adds a name/`null` pair for every null field. A value whose rendering
  exceeds the declared length fails at the Exasol emit boundary with a length error rather than being
  truncated, because a truncated JSON document is both invalid and silently wrong.
* **The encoder is `arrow::json::writer::make_encoder`,** reachable today with no new external
  dependency: `arrow-json` 58.3.0 is already in `Cargo.lock` and the `arrow` umbrella re-exports it
  as `arrow::json` under its `json` feature, which `datafusion`'s `arrow/default` edge already
  enables. The engine crate declares `features = ["json"]` explicitly so the availability stops
  resting on a transitive feature another crate happens to turn on.
* **`make_encoder` returns `Result` and three of its failure modes are reachable**, so the encoder is
  fallible rather than infallible: a non-`Utf8` map key (`"Only UTF8 keys supported by JSON MapArray
  Writer"`), a null map key or entry, and a `Union` type. The first is what the map-key
  stringification exists to remove; the remaining two surface as clean errors.
* **A top-level null cell MUST be guarded before encoding.** `make_encoder`'s `Encoder::encode`
  documents *"The behaviour is unspecified if `idx` corresponds to a null index"*, and unguarded it
  renders a null struct as `{}` and a null list as `[]` — both valid JSON and both wrong — while a
  `DataType::Null` child panics through an `unreachable!()`. A null nested value is an Exasol NULL,
  not the four characters `null`.
* **Apache Iceberg spec check.** This feature touches scanning and schema/type handling, so its
  Iceberg-compliance surface is stated in full above: the map-key type rule, the absence of key
  uniqueness/ordering rules, the non-semantic struct field order, the id-based nested projection
  requirement, and the deliberate, reasoned divergence from Appendix D's single-value JSON shapes.
  Nested-level TYPE PROMOTION renders the FILE's physical value with no cast, and for ICEBERG that is
  not a deviation at all: the spec's § Schema Evolution promotion table admits `int` → `long`,
  `float` → `double`, and `decimal(P,S)` → `decimal(P',S)` whose Requirements cell reads *"Widen
  precision only"* with the scale symbol `S` unchanged on both sides, so every Iceberg promotion
  renders identical JSON digits. Only DELTA permits the scale to grow, so only a Delta
  `decimal(10,1)` → `decimal(12,3)` widening renders `1.5` in the old file and `1.500` in the new one.
  That is a rendering difference, never a wrong value, and it is the direct consequence of
  `delta.typeChanges` being a validation input and never a cast input
  (`vs-adapter/delta-type-mapping`); it is recorded as a named limitation, not fixed here.
* **The legacy no-logical-schema path fails for a DIFFERENT reason and therefore needs its own fix.**
  There the registered schema is inferred from the first data file, so it declares the column at its
  real nested type and `build_scan_sql`'s `needs_json_fallback` branch emits `CAST(col AS VARCHAR)`.
  A spike measured the outcome: `This feature is not implemented: Unsupported CAST from
  Struct("street": Utf8, "city": Utf8) to Utf8View`, and the same for `Map` — note `Utf8View`, not
  `Utf8`, because that is what DataFusion resolves `VARCHAR` to. A `list` on that path succeeds and
  returns the same display text. So the two paths fail at two different sites, which is why one
  encoder must be reachable from both.
* **Parquet FILTER PUSHDOWN silently DROPS a predicate over such a column, and that is a
  pre-existing wrong-rows bug this feature must fix rather than inherit.** DataFusion 54.1 decides
  filter pushdown in two places against two different schemas.
  `ParquetSource::try_pushdown_filters` asks `can_expr_be_pushed_down_with_schemas(&filter,
  table_schema)` against the TABLE schema, where the column is `Utf8` — a primitive — so it answers
  `Supported` and the optimizer REMOVES the `FilterExec`. At file-open time `build_row_filter`
  re-checks each conjunct against the PHYSICAL file schema, where the column is `List`/`Struct`/`Map`,
  sets `non_primitive_columns = true`, returns `None`, and the conjunct is dropped from the candidate
  set. Nothing errors: the predicate is simply never applied. Measured against a real Parquet file
  with `pushdown_filters = true`: `SELECT id WHERE tags = '["hello","world"]'` returned BOTH rows,
  `WHERE addr = '{"street":"Main St","city":"Berlin"}'` returned BOTH rows, and
  `WHERE id = 2 AND tags = '["hello","world"]'` returned row 2 instead of nothing — with
  `pushdown_rows_matched=0, pushdown_rows_pruned=0, predicate_evaluation_errors=0` confirming the row
  filter was never built. This ALREADY happens today for a `list` column, which is a silent
  wrong-rows bug of the same root cause as this issue. For `struct` and `map` it would be NEWLY
  exposed, because today those queries fail loudly instead — turning a hard error into a silent wrong
  answer is a regression in kind, so the fix is part of this feature, not a follow-up.
* **That bug was then confirmed END TO END through Exasol, on BOTH table formats, and it is worse than
  a single operator.** Over a 4-row seeded Iceberg table every comparison predicate on a `list` column
  matched every row: `TAGS = '[hello, world]'`, `TAGS = 'zzz-no-such-value'`, `TAGS LIKE '%hello%'`,
  `TAGS IN (…)`, `TAGS > 'ZZZZZZZZ'`, `TAGS <> '[]'`, `UPPER(TAGS) = 'ZZZ'`, and
  `LENGTH(TAGS) = 999` each returned all 4 rows, and `COUNT(*)` under the same predicate returned 4.
  It is PER-CONJUNCT, not per-WHERE: `WHERE ID > 2 AND TAGS = 'zzz'` returned rows 3 and 4, so the
  primitive conjunct applied while the nested one vanished. It reproduces on DELTA too:
  `WHERE ARRAY_COL = 'zzz-no-such-value'` returned all 4 rows of `stats_all_types`. Controls rule out
  the filter path itself — a plain Iceberg `VARCHAR` and a plain Delta `STRING` column both correctly
  returned 0 rows for a non-matching literal. `EXPLAIN VIRTUAL` shows the predicate genuinely inside
  the scan spec (`"filter":"(\"TAGS\" = '[hello, world]')"`) with NO compensating outer WHERE, which
  is precisely the delegation hazard CLAUDE.md warns about: Exasol never re-checks an advertised
  capability.
* **Two shapes are NOT affected and must stay unaffected.** `IS NULL` and `IS NOT NULL` are honoured
  today (they returned the correct 1 and 3 rows), and SELECT-LIST expressions over the column are
  correct per row (`LENGTH(TAGS)` and `'<' || TAGS || '>'` both returned right answers). Only
  comparison conjuncts are lost, which is consistent with the row-filter builder skipping what it
  cannot express rather than with a projection-level defect.
* **`pushdown_filters = false` makes every one of those queries correct**, measured on the same
  fixture: the `FilterExec` survives and evaluates the JSON-rendering expression inside an ordinary
  `BinaryExpr` perfectly. The predicate therefore stays inside DataFusion, which transfers fewer rows
  across the `.so` boundary than declining it to an Exasol-dialect wrapper would.
* **DataFusion also builds a statistics-pruning predicate over the rendered column, and that is the
  remaining place this design could lose rows silently.** Observed in the same `EXPLAIN ANALYZE`:
  `pruning_predicate=tags_null_count@2 != row_count@3 AND tags_min@0 <= ["hello","world"] AND
  ["hello","world"] <= tags_max@1, required_guarantees=[tags in (["hello","world"])]`, plus a
  bloom-filter stage. Nothing errored and nothing was pruned
  (`row_groups_pruned_statistics=1 total → 1 matched`, `num_predicate_creation_errors=0`), which is
  consistent with Parquet holding no statistics for a group node — but a SINGLE-row-group fixture
  cannot discriminate "statistics unavailable" from "statistics available and happened to match". The
  hazard is concrete if statistics ever DO resolve: Parquet keeps statistics for a nested column's
  LEAF values, so a `tags_min`/`tags_max` of `"hello"`/`"world"` compared against the rendered
  document `["hello","world"]` evaluates `"hello" <= '["hello","world"]'` as FALSE — `[` sorts below
  `h` — and prunes a row group that does contain the match. Row loss from pruning is silent, so this
  feature requires positive proof rather than the absence of an observed failure.
* **The binding RULE stays owned by `datafusion-scan/scan-execution-field-id-projection`; this
  feature owns only applying it at DEPTH.** That feature's recorded ordered resolution — an embedded
  `PARQUET:field_id`, then a logical field's declared physical name, then `schema.name-mapping.default`,
  then the physical name unchanged — its NULL-fill for an absent nullable field, its `initial-default`
  substitution, and its required-absent error are all unchanged and MUST NOT be restated or amended
  here. Nothing but the JSON rendering consumes a nested field, which is why the recursion is owned by
  this feature rather than by that one.
* **Iceberg `schema.name-mapping.default` nested entries stay unparsed.**
  `datafusion-scan/scan-execution-field-id-projection` already records that only TOP-LEVEL entries are
  flattened and that nested `fields` entries are out of scope as issue #28. A nested field of a file
  written with no embedded field-id therefore binds by its own physical name, exactly as the
  identity fallback already does at the top level.

## Scenarios

### Scenario: A list, struct, or map value renders as one valid JSON document

* *GIVEN* an Arrow column of type `List`, `LargeList`, `FixedSizeList`, `Struct`, or `Map` carrying populated values
* *WHEN* the scan renders that column for emission
* *THEN* every non-null cell SHALL render as a single valid JSON document that `serde_json` parses, with NO surrounding array, newline, or record framing
* *AND* a `List` SHALL render as a JSON array, a `Struct` as a JSON object keyed by field name, and a `Map` as a JSON object keyed by its stringified key
* *AND* a string value SHALL be rendered as a JSON string with `"`, `\`, control characters, and non-ASCII characters escaped per RFC 8259, so `[hello, world]` — the Arrow display text this feature replaces — SHALL NOT be produced for a `List` of strings
* *AND* nesting SHALL recurse to arbitrary depth, so a `List<Struct<a>>` renders as `[{"a":10},{"a":20}]` and a `Struct<inner: List<Utf8>>` renders as `{"inner":["p","q"]}`
* *AND* a `Date32` SHALL render as a quoted ISO-8601 date, a `Timestamp` as a quoted RFC-3339 instant, a `Decimal128` as a bare JSON number, and a `Binary` member as a quoted lowercase hexadecimal string — the same hexadecimal convention the Iceberg spec's Appendix D gives `binary` and `fixed`
* *AND* the rendered bytes SHALL be valid UTF-8 by construction, so the conversion to a Rust `String` never needs a lossy fallback

### Scenario: A null nested value emits SQL NULL, not the text "null"

* *GIVEN* a nested column carrying a null cell, a cell whose struct field is null, a cell whose list element is null, and a cell whose map value is null
* *WHEN* the scan renders that column
* *THEN* the NULL CELL SHALL emit an Exasol NULL — a null in the rendered `Utf8` column, converted to `Value::Null` — and MUST NOT emit the four characters `null`, an empty JSON object `{}`, or an empty JSON array `[]`
* *AND* the renderer SHALL test the cell's nullity BEFORE invoking the encoder, because the encoder's own contract leaves a null index unspecified and renders `{}` for a null struct and `[]` for a null list
* *AND* a null struct FIELD and a null map VALUE SHALL each render as an explicit `null` inside the document — `{"street":"Second St","city":null}` — rather than being omitted, so every row of one column renders the same object shape and an Exasol `JSON_VALUE` path never disappears between rows
* *AND* a null list ELEMENT SHALL render as `null` at its position, preserving element count and order
* *AND* an EMPTY list SHALL render as `[]` and an empty map as `{}`, each distinct from the null cell's SQL NULL

### Scenario: A non-string map key is stringified into the JSON object name

* *GIVEN* a `Map` column whose key type is not `Utf8`, `LargeUtf8`, or `Utf8View` — an `Int32`, `Date32`, `Boolean`, `Decimal128`, `FixedSizeBinary`, or a nested key type, all of which the Iceberg spec permits
* *WHEN* the scan renders that column
* *THEN* the renderer SHALL replace the key child array with a `Utf8` array of stringified keys BEFORE encoding, and the rendered object name SHALL be that string — `42` for the integer key `42`, `true` for the boolean key `true`, `2026-08-18` for that date key
* *AND* a NESTED key type SHALL be stringified as its own JSON rendering, so a `Struct` key becomes the object name `{"a":1}` — one rule covering every type the spec permits, with no key type left unhandled
* *AND* every other key type SHALL be stringified through the Arrow-to-`Utf8` cast, and a key type that cast rejects SHALL surface a clean error naming the key type, never a wrong value
* *AND* the key strings SHALL be escaped as JSON object names by the same encoder that escapes string values, so a key containing `"` or `\` yields a valid document
* *AND* an ARRAY-OF-PAIRS shape (`[{"key":42,"value":"v"}]`) MUST NOT be produced, because it would make the emitted VARCHAR unreadable by an Exasol JSON path expression, which is the whole reason the column is surfaced as JSON at all
* *AND* entry order SHALL be the source array's own entry order, and duplicate stringified keys SHALL NOT be deduplicated, because the Iceberg spec states no key-uniqueness rule for a `map` value

### Scenario: Rendered field names are the table's logical names, not the file's physical ones

* *GIVEN* a table whose nested field names on disk differ from the schema's — a Delta table under `id` or `name` column mapping whose inner `StructField`s carry `delta.columnMapping.physicalName`, or an Iceberg table whose nested field was renamed, reordered, added, or dropped after a data file was written
* *WHEN* the scan renders a nested column from such a file
* *THEN* the rendered JSON object names SHALL be the table's CURRENT LOGICAL field names, so the `stats-all-types` fixture's `nested_struct` renders `{"inner_int":…,"inner_string":…,"inner_double":…}` and MUST NOT render its `col-7f2f94cf-…` physical names
* *AND* the nested field tree SHALL be resolved by the SAME first-match-wins binding order the top-level columns already use — an embedded `PARQUET:field_id` matching the logical field's id, then a logical field declaring this physical field's name, then the physical name unchanged — so one rule covers Iceberg field-ids, Delta `id` mapping, Delta `name` mapping, and identity binding, and no format-specific nested branch exists
* *AND* a logical nested field ABSENT from the file SHALL render as an explicit `null`, and a physical nested field NO logical field claims SHALL be omitted from the rendering, matching the Iceberg spec's § Column Projection rule that an unresolved field id *"Return[s] `null`"*
* *AND* the rendered field ORDER SHALL be the physical file's order, per the Background bullet on non-semantic struct field order

### Scenario: A nested physical column is diverted around the physical-to-logical cast

* *GIVEN* a scan whose logical schema declares a column `Utf8` — the tag every list, struct, and map carries — and whose physical Parquet file carries that column as `List`, `LargeList`, `FixedSizeList`, `Struct`, or `Map`
* *WHEN* the column-binding expression adapter rewrites a projection or filter expression referencing that column
* *THEN* the adapter SHALL substitute the JSON-rendering expression for that column and MUST NOT let the delegated `DefaultPhysicalExprAdapter` attempt a physical-to-logical cast on it, because `arrow-cast` offers NO `Struct → Utf8` or `Map → Utf8` cast — the recorded `sqlCode 22002` failure — and the `List → Utf8` cast it DOES offer produces Arrow display text rather than JSON
* *AND* the substituted expression's `data_type()` SHALL be `DataType::Utf8`, so the expression the Parquet opener receives agrees with the registered table schema and no downstream stage sees a nested type
* *AND* the diversion SHALL be keyed on the logical field's DECLARED nested member descriptor — the one signal also available before any file is opened, and therefore the only one the row-filter-pushdown withdrawal below can read too — with the resolved column's type additionally required to be one of those five nested variants, read through the single owning predicate `datafusion-scan/type-mapping` defines; so no format identity, column name, or tag string is consulted and the Iceberg and Delta paths are served by the identical code
* *AND* a physically nested column whose logical field declares NO member descriptor SHALL be left to the delegated adapter and fail loudly there, and MUST NOT be diverted and rendered: keying the diversion on the physical type alone would render such a column while the pushdown withdrawal below — which cannot see a physical type — left row-filter pushdown ON for its table, which is exactly the silent wrong-rows path this feature exists to close
* *AND* the schema handed to the delegated adapter SHALL substitute the bound physical FIELD WHOLE — name, type, nullability, and metadata together — and MUST NOT substitute the data type alone, because `DefaultPhysicalExprAdapter` emits a bare `Column` only on FULL field equality and otherwise emits a cast: a file whose nested column carries NO `PARQUET:field_id` (an Iceberg name-mapping file, a Delta `none`-mode table, or plain Hive Parquet) then differs in metadata alone and fails with `Cast error: Casting from Utf8 to Struct(...) not supported`, which is the same class of failure this scenario exists to remove
* *AND* the substitution SHALL stop the tree walk at the node it replaces rather than descending into it, because a rewrite that re-enters its own output wraps the same column endlessly
* *AND* every PRIMITIVE physical-to-logical cast SHALL keep flowing through the delegated `DefaultPhysicalExprAdapter` unchanged, so `datafusion-scan/type-relaxation`'s recorded requirement that *"the delegated `DefaultPhysicalExprAdapter` SHALL insert the physical-to-logical cast into the physical expression tree"* still holds for all 13 supported relaxation pairs
* *AND* the adapter's recorded absent-with-default interception, its nullable-absent NULL fill, its required-absent error, and its rename-of-resolved-columns-back-to-physical-names pass SHALL all keep their recorded behavior, and a nested column absent from a file SHALL NULL-fill through that same existing path rather than through a nested-specific one
* *AND* the Parquet opener's name-based consumers — the projection read plan, the column reassignment, and the projector — SHALL still resolve the wrapped column against the real physical file schema, so the nested column is actually read from the file rather than silently projected away

### Scenario: One encoder serves both the logical-schema path and the legacy inference path

* *GIVEN* a scan spec carrying NO logical schema, where the registered DataFusion table schema is inferred from the first data file and therefore declares a nested column at its real nested Arrow type
* *WHEN* the scan builds its SQL for the single-table path or the broadcast-join path
* *THEN* the generated SELECT item for a JSON-RENDERED NESTED column SHALL invoke the JSON encoder and MUST NOT emit `CAST(col AS VARCHAR)`, which fails outright for `Struct` and `Map` and produces display text for `List`
* *AND* the generated SELECT item for every NON-NESTED incompatible column SHALL keep emitting `CAST(col AS VARCHAR)` byte-identically, so an out-of-range `Decimal128` and a `Binary` column are unaffected (issue #351)
* *AND* the single-table path and the broadcast-join path SHALL make that choice through the SAME predicate and the SAME rendering helper, so the two select-list builders cannot disagree about a column
* *AND* the encoder itself SHALL have exactly ONE implementation, reached both as the expression the binding adapter substitutes and as the function the generated SQL invokes, so the rendered document for a given value is identical whichever path produced it

### Scenario: A predicate over a rendered nested column is evaluated, never silently dropped

* *GIVEN* a scan whose registered table schema carries at least one JSON-rendered nested column, and a pushed-down filter comparing that column to a literal — the shape that today returns EVERY row for a `list` column and fails loudly for a `struct` or `map` column
* *WHEN* the scan builds the Parquet data source for that table
* *THEN* the scan SHALL DISABLE Parquet filter pushdown for that table, so the optimizer keeps a `FilterExec` that evaluates the predicate over the RENDERED `Utf8` column and MUST NOT let DataFusion remove the `FilterExec` on the strength of the logical schema and then drop the conjunct against the physical one
* *AND* the returned rows SHALL be exactly those the predicate selects: `WHERE tags = '["hello","world"]'` SHALL return only the matching row and MUST NOT return every row, and `WHERE id = 2 AND tags = '["hello","world"]'` SHALL return no row when row 2's tags differ — the measured wrong answers this scenario removes
* *AND* the fix SHALL apply to `list` as well as to `struct` and `map`, because the dropped-conjunct bug is PRE-EXISTING for `list` and has the same root cause as this issue — a nested physical column under a `Utf8` logical one — so leaving it would ship a known silent wrong-rows bug beside its own fix
* *AND* the fix SHALL hold for EVERY comparison shape measured wrong end to end — `=`, `<>`, `>`, `IN`, `LIKE`, and a scalar function wrapping the column such as `UPPER(col)` or `LENGTH(col)` — and for a conjunction mixing a primitive predicate with a nested one, whose primitive half already applied while the nested half vanished
* *AND* `IS NULL` and `IS NOT NULL` SHALL keep returning the rows they already return correctly, and a SELECT-LIST expression over the column SHALL keep returning the per-row values it already returns correctly, so the fix adds no regression to the two shapes that were never broken
* *AND* it SHALL hold on BOTH table formats, because the bug was reproduced on an Iceberg `list<string>` and on a Delta `array<integer>` alike, and the scan side MUST NOT branch on table format
* *AND* the decision SHALL be taken from the presence of a JSON-rendered nested column in the table schema, read through the SAME single owning predicate the cast diversion reads, and MUST NOT be taken from substring-matching the rendered filter text for a column name
* *AND* the accepted cost SHALL be named: a query over a table that carries a nested column loses Parquet ROW-LEVEL filter pushdown for all its columns, so late materialization no longer skips rows within a row group. Row-group and page PRUNING from statistics is a separate stage and is unaffected

### Scenario: Every pushdown shape treats a nested column as the VARCHAR Exasol declared

* *GIVEN* a pushdown request whose WHERE predicate, GROUP BY key, ORDER BY key, aggregate argument, `COUNT(DISTINCT)` argument, join condition, or select-list expression references a list, struct, or map column
* *WHEN* the adapter plans that request
* *THEN* the adapter SHALL treat that column exactly as it treats any other `VARCHAR(2000000)` column, and this feature MUST NOT add a type-based decline gate, a capability withdrawal, or an error path for any of those shapes — the ONE correctness hole, a dropped Parquet row-filter conjunct, is closed inside the scan by the preceding scenario rather than by declining the predicate to an Exasol-dialect wrapper, which would transfer every row across the `.so` boundary to achieve the same result
* *AND* a GROUP BY key, an ORDER BY key, `COUNT(*)`, `COUNT(DISTINCT)`, and an `IS NOT NULL` predicate over such a column SHALL each return correct results with no gate, which was measured directly rather than inferred
* *AND* the column's logical Arrow type SHALL remain `Utf8` through the whole plan, so DataFusion evaluates every such expression as an ordinary string operation over the RENDERED JSON — the same text Exasol itself sees in the emitted `VARCHAR(2000000)` — which is what makes the DataFusion-side and Exasol-side evaluations of one predicate agree by construction rather than by luck
* *AND* the recorded TopN decline that keys on `needs_json_fallback` over the sort key's logical tag SHALL keep its recorded behavior byte-identical: a nested column's tag is `utf8`, so it does NOT trigger that decline, and an out-of-range `decimal128(p,s)` tag still does
* *AND* this MUST be verified against a live Exasol instance rather than inferred from the capability registry or from code inspection — `EXPLAIN VIRTUAL` plus the executed query for each shape above — because a capability this adapter advertises has no Exasol-side fallback
* *AND* a predicate over a nested column MUST NOT prune any row group, page, or file on Parquet statistics, and that MUST be proven POSITIVELY against a MULTI-row-group file whose per-group leaf statistics would falsely exclude the rendered document — never inferred from a single-row-group scan that pruned nothing, which cannot distinguish absent statistics from statistics that happened to match
* *AND* if any pruning stage is found to evaluate such a predicate against statistics describing the PHYSICAL nested values, that stage SHALL be disabled for the column rather than left to prune, because a pruned row group returns FEWER rows with no error — the one silent-wrong-rows failure mode this design admits
* *AND* the accepted cost SHALL be named rather than left implicit: with statistics pruning off for the column, the scan reads rows a statistics-prunable predicate would have skipped — a performance property, never a correctness one

### Scenario: The rendered column crosses the emit boundary as the declared VARCHAR

* *GIVEN* a nested column the schema declares to Exasol as `VARCHAR(2000000)` and an `EMITS` declaration derived from that same declaration
* *WHEN* the scan reaches the emit boundary
* *THEN* the column SHALL already carry Arrow `Utf8`, so `coerce_batch_to_exa_types` passes it through with no cast, and this feature MUST NOT add any nested-aware branch to the emit path
* *AND* the declared Exasol type MUST stay `VARCHAR(2000000)` for every list, struct, and map column, unchanged by this feature, because Exasol has no array, list, struct, or map type
* *AND* a rendered document longer than 2,000,000 characters SHALL surface Exasol's own length error and MUST NOT be truncated, because a truncated JSON document is both invalid and silently wrong

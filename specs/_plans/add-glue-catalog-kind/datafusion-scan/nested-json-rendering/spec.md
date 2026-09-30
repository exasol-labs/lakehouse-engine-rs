# Feature: Nested-Type JSON Rendering

Renders every list, struct, and map column as a single valid JSON document per value, so a nested
lakehouse column is queryable through Exasol as the `VARCHAR(2000000)` the schema already declares
for it instead of failing the scan or returning Arrow display text.

<!-- DELTA:CHANGED -->
## Background

* **This feature is issue #350.** `datafusion-scan/type-mapping` has recorded a JSON-`VARCHAR`
  contract for List, Struct, and Map since the project began; no encoder was ever built for it.
  `vs-adapter/delta-type-mapping` already records the diagnosis verbatim: *"`raw_scan` registers the
  logical schema — an incompatible column tagged `utf8` — as the DataFusion table schema, and
  DataFusion's physical-expression adapter validates physical-against-logical castability at file
  open, BEFORE any per-value JSON conversion runs … Neither ever reaches the JSON path, on EITHER
  table format."* This feature builds the encoder and routes both formats through it.
* **Binary rendering is out of scope (#351).** Issue #351 owns Binary's JSON validity. Every binary
  column, and every column with a binary member, is refused at plan time on every source per
  `vs-adapter/binary-column-refusal`.
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
* **The defect this feature removes surfaced at a different layer per format.** On the Delta path
  the adapter refused a nested column at PLAN time through its refused-column list. On the Iceberg
  path the failure landed at SCAN time as
  `scan failed: assigned data could not be read: Execution error: Cannot cast column …`. This feature
  removes the cause, so both surfaces go away. It adds no Iceberg refusal for a nested column.
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
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: A null nested value emits SQL NULL, not the text "null"

* *GIVEN* a nested column carrying a null cell, a cell whose struct field is null, a cell whose list element is null, and a cell whose map value is null
* *WHEN* the scan renders that column
* *THEN* the NULL CELL SHALL emit an Exasol NULL — a null in the rendered `Utf8` column, converted to `Value::Null` — and MUST NOT emit the four characters `null`, an empty JSON object `{}`, or an empty JSON array `[]`
* *AND* the renderer SHALL test the cell's nullity BEFORE invoking the encoder, because the encoder's own contract leaves a null index unspecified and renders `{}` for a null struct and `[]` for a null list
* *AND* a null struct FIELD and a null map VALUE SHALL each render as an explicit `null` inside the document — `{"street":"Second St","city":null}` — rather than being omitted, so every row of one column renders the same object shape and an Exasol `JSON_VALUE` path never disappears between rows
* *AND* a null list ELEMENT SHALL render as `null` at its position, preserving element count and order
* *AND* an EMPTY list SHALL render as `[]` and an empty map as `{}`, each distinct from the null cell's SQL NULL

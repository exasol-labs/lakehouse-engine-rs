# Feature: Nested-Type JSON Rendering

Renders every list, struct, and map column as one valid JSON document per value, so a nested lakehouse column is queryable through Exasol as the `VARCHAR(2000000)` the schema declares for it.

## Background

* Binary values are not rendered. Every binary column, and every column with a binary member, is refused at plan time on every source (`vs-adapter/binary-column-refusal`).
* The JSON shape is chosen for Exasol SQL ergonomics and deliberately differs from the Iceberg spec's Appendix D, § JSON single-value serialization:

  | Iceberg type | Appendix D JSON representation | Appendix D example |
  |---|---|---|
  | `struct` | *"JSON object by field ID"* | `{"1": 1, "2": "bar"}` |
  | `list` | *"JSON array of values"* | `[1, 2, 3]` |
  | `map` | *"JSON object of key and value arrays"* | `{ "keys": ["a", "b"], "values": [1, 2] }` |

* Appendix D is scoped to metadata single values, not query results, and the spec defines no JSON encoding for scan output rows. A struct keyed by numeric field id cannot be read by a `JSON_VALUE(col, '$.city')` path, and a map split into parallel `keys` and `values` arrays cannot be read by key. This feature keys a struct by field name and a map by its stringified key.
* Iceberg spec, § Nested Types: "Map keys are required and map values can be either optional or required. Both map keys and map values may be any type, including nested types." A JSON object name is a string (RFC 8259), so every non-string key is stringified.
* The Iceberg spec states no key-uniqueness and no key-ordering rule for a `map` value. RFC 8259 says object names SHOULD be unique and an object is an unordered collection. The rendering preserves the source entry order and does not deduplicate, so a duplicate-keyed map renders as a JSON object with repeated names, which a consumer can resolve as last-wins.
* Struct field order is the physical file's field order. § Column Projection says "projection must be done using field ids", and § Schema Evolution permits "reordering existing fields", so two files of one table can carry a struct's fields in different orders and the rendered key order follows each file. DataFusion and Exasol read the same rendered string, so they never disagree about a value. A logically equal value can render as two distinct strings across such files, which `GROUP BY` and `DISTINCT` then separate. The rendering is not re-sorted, because a canonical order would diverge from the declared schema order that every single-layout table renders.
* Field names in the rendered JSON are the logical names, resolved by binding key. The Iceberg spec gives every nested field its own id (§ Nested Types), and § Column Projection makes id-based projection normative. The Delta protocol assigns `delta.columnMapping.id` and `delta.columnMapping.physicalName` to nested fields, so a column-mapped Delta struct stores names like `col-7f2f94cf-7082-430c-bba7-852bc6c5215e` on disk. The nested field tree is resolved to logical names before rendering, by the binding-key rule the top-level columns use. The vendored `stats-all-types` fixture has this shape.
* `scan-read-path/scan-execution-field-id-projection` owns the binding rule: an embedded `PARQUET:field_id`, then a logical field's declared physical name, then `schema.name-mapping.default`, then the physical name unchanged. It also owns the NULL-fill for an absent nullable field, the `initial-default` substitution, and the required-absent error. This feature applies the rule at depth. Nested `schema.name-mapping.default` entries are not parsed, so a nested field of a file with no embedded field id binds by its own physical name.
* Nested-level type promotion renders the file's physical value with no cast. Every Iceberg promotion renders identical JSON digits, because Iceberg's decimal promotion is "Widen precision only". A Delta `decimal(10,1)` to `decimal(12,3)` widening renders `1.5` in the old file and `1.500` in the new one. That is a rendering difference, not a wrong value, because `delta.typeChanges` is a validation input and never a cast input (`delta/delta-type-mapping`).
* The rendering is longer than Arrow display text, so Exasol's `VARCHAR(2000000)` cap is easier to reach. A value whose rendering exceeds the declared length fails at the emit boundary with a length error, because a truncated JSON document is invalid and silently wrong.
* The encoder is `arrow::json::writer::make_encoder`, from the `arrow` umbrella crate under its `json` feature, which the engine crate declares explicitly. It returns `Result`, and three failure modes are reachable: a non-`Utf8` map key (`"Only UTF8 keys supported by JSON MapArray Writer"`), a null map key or entry, and a `Union` type. Map-key stringification removes the first, and the other two surface as clean errors.
* A top-level null cell is checked before encoding. `Encoder::encode` documents "The behaviour is unspecified if `idx` corresponds to a null index". Unguarded, it renders a null struct as `{}` and a null list as `[]`, both valid JSON and both wrong, and a `DataType::Null` child panics through `unreachable!()`. A null nested value is an Exasol NULL, not the text `null`.
* A scan spec with no logical schema infers the registered schema from the first data file, so a nested column keeps its real nested type and `build_scan_sql` emits `CAST(col AS VARCHAR)`. DataFusion fails that cast for `Struct` and `Map` (`Unsupported CAST from Struct(...) to Utf8View`) and produces display text for `List`. One encoder therefore serves both paths.
* Parquet filter pushdown drops a predicate over such a column. DataFusion decides pushdown twice against two schemas. `ParquetSource::try_pushdown_filters` checks the table schema, where the column is `Utf8`, answers `Supported`, and the optimizer removes the `FilterExec`. At file open, `build_row_filter` rechecks each conjunct against the physical schema, where the column is `List`, `Struct`, or `Map`, returns `None`, and drops the conjunct. No error is raised and the predicate is never applied. Measured with `pushdown_filters = true`: `SELECT id WHERE tags = '["hello","world"]'` returned both rows, and `WHERE id = 2 AND tags = '["hello","world"]'` returned row 2 instead of no row.
* The bug holds end to end through Exasol on both table formats and is per conjunct. Over a 4-row Iceberg table, `TAGS = 'zzz-no-such-value'`, `TAGS LIKE '%hello%'`, `TAGS IN (…)`, `TAGS > 'ZZZZZZZZ'`, `TAGS <> '[]'`, `UPPER(TAGS) = 'ZZZ'`, and `LENGTH(TAGS) = 999` each returned all 4 rows, and `WHERE ID > 2 AND TAGS = 'zzz'` returned rows 3 and 4. A Delta `array<integer>` column behaves the same. A plain `VARCHAR` or `STRING` column returns 0 rows for a non-matching literal. `EXPLAIN VIRTUAL` shows the predicate inside the scan spec with no outer WHERE, and Exasol never rechecks an advertised capability.
* `IS NULL`, `IS NOT NULL`, and select-list expressions over the column are correct today. Only comparison conjuncts are lost.
* With `pushdown_filters = false`, every one of those queries is correct, because the `FilterExec` survives and evaluates the JSON-rendering expression. The predicate stays inside DataFusion, which transfers fewer rows across the `.so` boundary than declining it to an Exasol-dialect wrapper.
* DataFusion also builds a statistics-pruning predicate over the rendered column. Parquet keeps statistics for a nested column's leaf values, so a `tags_min` of `"hello"` and `tags_max` of `"world"` compared against the rendered document `["hello","world"]` evaluates `"hello" <= '["hello","world"]'` as false, because `[` sorts below `h`. That prunes a row group that contains the match, and the row loss is silent. A single-row-group fixture cannot show whether statistics resolve for a group node.

## Scenarios

### Scenario: A list, struct, or map value renders as one valid JSON document

* *GIVEN* an Arrow column of type `List`, `LargeList`, `FixedSizeList`, `Struct`, or `Map` carrying populated values
* *WHEN* the scan renders that column for emission
* *THEN* every non-null cell SHALL render as a single valid JSON document that `serde_json` parses, with NO surrounding array, newline, or record framing
* *AND* a `List` SHALL render as a JSON array, a `Struct` as a JSON object keyed by field name, and a `Map` as a JSON object keyed by its stringified key
* *AND* a string value SHALL be rendered as a JSON string with `"`, `\`, control characters, and non-ASCII characters escaped per RFC 8259, so `[hello, world]`, the Arrow display text this feature replaces, SHALL NOT be produced for a `List` of strings
* *AND* nesting SHALL recurse to arbitrary depth, so a `List<Struct<a>>` renders as `[{"a":10},{"a":20}]` and a `Struct<inner: List<Utf8>>` renders as `{"inner":["p","q"]}`
* *AND* a `Date32` SHALL render as a quoted ISO-8601 date, a `Timestamp` as a quoted RFC-3339 instant, a `Decimal128` as a bare JSON number, and a `Binary` member as a quoted lowercase hexadecimal string, the same hexadecimal convention the Iceberg spec's Appendix D gives `binary` and `fixed`
* *AND* the rendered bytes SHALL be valid UTF-8 by construction, so the conversion to a Rust `String` never needs a lossy fallback

### Scenario: A null nested value emits SQL NULL, not the text "null"

* *GIVEN* a nested column carrying a null cell, a cell whose struct field is null, a cell whose list element is null, and a cell whose map value is null
* *WHEN* the scan renders that column
* *THEN* the NULL CELL SHALL emit an Exasol NULL, a null in the rendered `Utf8` column, converted to `Value::Null`, and MUST NOT emit the four characters `null`, an empty JSON object `{}`, or an empty JSON array `[]`
* *AND* the renderer SHALL test the cell's nullity BEFORE invoking the encoder, because the encoder's own contract leaves a null index unspecified and renders `{}` for a null struct and `[]` for a null list
* *AND* a null struct FIELD and a null map VALUE SHALL each render as an explicit `null` inside the document, `{"street":"Second St","city":null}`, rather than being omitted, so every row of one column renders the same object shape and an Exasol `JSON_VALUE` path never disappears between rows
* *AND* a null list ELEMENT SHALL render as `null` at its position, preserving element count and order
* *AND* an EMPTY list SHALL render as `[]` and an empty map as `{}`, each distinct from the null cell's SQL NULL

### Scenario: A non-string map key is stringified into the JSON object name

* *GIVEN* a `Map` column whose key type is not `Utf8`, `LargeUtf8`, or `Utf8View`, an `Int32`, `Date32`, `Boolean`, `Decimal128`, `FixedSizeBinary`, or a nested key type, all of which the Iceberg spec permits
* *WHEN* the scan renders that column
* *THEN* the renderer SHALL replace the key child array with a `Utf8` array of stringified keys BEFORE encoding, and the rendered object name SHALL be that string, `42` for the integer key `42`, `true` for the boolean key `true`, `2026-08-18` for that date key
* *AND* a NESTED key type SHALL be stringified as its own JSON rendering, so a `Struct` key becomes the object name `{"a":1}`, one rule covering every type the spec permits, with no key type left unhandled
* *AND* every other key type SHALL be stringified through the Arrow-to-`Utf8` cast, and a key type that cast rejects SHALL surface a clean error naming the key type, never a wrong value
* *AND* the key strings SHALL be escaped as JSON object names by the same encoder that escapes string values, so a key containing `"` or `\` yields a valid document
* *AND* an ARRAY-OF-PAIRS shape (`[{"key":42,"value":"v"}]`) MUST NOT be produced, because it would make the emitted VARCHAR unreadable by an Exasol JSON path expression, which is the whole reason the column is surfaced as JSON at all
* *AND* entry order SHALL be the source array's own entry order, and duplicate stringified keys SHALL NOT be deduplicated, because the Iceberg spec states no key-uniqueness rule for a `map` value

### Scenario: Rendered field names are the table's logical names, not the file's physical ones

* *GIVEN* a table whose nested field names on disk differ from the schema's, a Delta table under `id` or `name` column mapping whose inner `StructField`s carry `delta.columnMapping.physicalName`, or an Iceberg table whose nested field was renamed, reordered, added, or dropped after a data file was written
* *WHEN* the scan renders a nested column from such a file
* *THEN* the rendered JSON object names SHALL be the table's CURRENT LOGICAL field names, so the `stats-all-types` fixture's `nested_struct` renders `{"inner_int":…,"inner_string":…,"inner_double":…}` and MUST NOT render its `col-7f2f94cf-…` physical names
* *AND* the nested field tree SHALL be resolved by the SAME first-match-wins binding order the top-level columns already use, an embedded `PARQUET:field_id` matching the logical field's id, then a logical field declaring this physical field's name, then the physical name unchanged, so one rule covers Iceberg field-ids, Delta `id` mapping, Delta `name` mapping, and identity binding, and no format-specific nested branch exists
* *AND* a logical nested field ABSENT from the file SHALL render as an explicit `null`, and a physical nested field NO logical field claims SHALL be omitted from the rendering, matching the Iceberg spec's § Column Projection rule that an unresolved field id *"Return[s] `null`"*
* *AND* the rendered field ORDER SHALL be the physical file's order, per the Background bullet on non-semantic struct field order

### Scenario: A nested physical column is diverted around the physical-to-logical cast

* *GIVEN* a scan whose logical schema declares a column `Utf8`, the tag every list, struct, and map carries, and whose physical Parquet file carries that column as `List`, `LargeList`, `FixedSizeList`, `Struct`, or `Map`
* *WHEN* the column-binding expression adapter rewrites a projection or filter expression referencing that column
* *THEN* the adapter SHALL substitute the JSON-rendering expression for that column and MUST NOT let the delegated `DefaultPhysicalExprAdapter` attempt a physical-to-logical cast on it, because `arrow-cast` offers NO `Struct → Utf8` or `Map → Utf8` cast, the recorded `sqlCode 22002` failure, and the `List → Utf8` cast it DOES offer produces Arrow display text rather than JSON
* *AND* the substituted expression's `data_type()` SHALL be `DataType::Utf8`, so the expression the Parquet opener receives agrees with the registered table schema and no downstream stage sees a nested type
* *AND* the diversion SHALL be keyed on the logical field's DECLARED nested member descriptor, the one signal also available before any file is opened, and therefore the only one the row-filter-pushdown withdrawal below can read too, with the resolved column's type additionally required to be one of those five nested variants, read through the single owning predicate `scan-types/type-mapping` defines; so no format identity, column name, or tag string is consulted and the Iceberg and Delta paths are served by the identical code
* *AND* a physically nested column whose logical field declares NO member descriptor SHALL be left to the delegated adapter and fail loudly there, and MUST NOT be diverted and rendered: keying the diversion on the physical type alone would render such a column while the pushdown withdrawal below, which cannot see a physical type, left row-filter pushdown ON for its table, which is exactly the silent wrong-rows path this feature exists to close
* *AND* the schema handed to the delegated adapter SHALL substitute the bound physical FIELD WHOLE, name, type, nullability, and metadata together, and MUST NOT substitute the data type alone, because `DefaultPhysicalExprAdapter` emits a bare `Column` only on FULL field equality and otherwise emits a cast: a file whose nested column carries NO `PARQUET:field_id` (an Iceberg name-mapping file, a Delta `none`-mode table, or plain Hive Parquet) then differs in metadata alone and fails with `Cast error: Casting from Utf8 to Struct(...) not supported`, which is the same class of failure this scenario exists to remove
* *AND* the substitution SHALL stop the tree walk at the node it replaces rather than descending into it, because a rewrite that re-enters its own output wraps the same column endlessly
* *AND* every PRIMITIVE physical-to-logical cast SHALL keep flowing through the delegated `DefaultPhysicalExprAdapter` unchanged, so `scan-types/type-relaxation`'s recorded requirement that *"the delegated `DefaultPhysicalExprAdapter` SHALL insert the physical-to-logical cast into the physical expression tree"* still holds for all 13 supported relaxation pairs
* *AND* the adapter's recorded absent-with-default interception, its nullable-absent NULL fill, its required-absent error, and its rename-of-resolved-columns-back-to-physical-names pass SHALL all keep their recorded behavior, and a nested column absent from a file SHALL NULL-fill through that same existing path rather than through a nested-specific one
* *AND* the Parquet opener's name-based consumers, the projection read plan, the column reassignment, and the projector, SHALL still resolve the wrapped column against the real physical file schema, so the nested column is actually read from the file rather than silently projected away

### Scenario: One encoder serves both the logical-schema path and the legacy inference path

* *GIVEN* a scan spec carrying NO logical schema, where the registered DataFusion table schema is inferred from the first data file and therefore declares a nested column at its real nested Arrow type
* *WHEN* the scan builds its SQL for the single-table path or the broadcast-join path
* *THEN* the generated SELECT item for a JSON-RENDERED NESTED column SHALL invoke the JSON encoder and MUST NOT emit `CAST(col AS VARCHAR)`, which fails outright for `Struct` and `Map` and produces display text for `List`
* *AND* the generated SELECT item for every NON-NESTED incompatible column SHALL keep emitting `CAST(col AS VARCHAR)` byte-identically, so an out-of-range `Decimal128` and a `Binary` column are unaffected
* *AND* the single-table path and the broadcast-join path SHALL make that choice through the SAME predicate and the SAME rendering helper, so the two select-list builders cannot disagree about a column
* *AND* the encoder itself SHALL have exactly ONE implementation, reached both as the expression the binding adapter substitutes and as the function the generated SQL invokes, so the rendered document for a given value is identical whichever path produced it

### Scenario: A predicate over a rendered nested column is evaluated, never silently dropped

* *GIVEN* a scan whose registered table schema carries at least one JSON-rendered nested column, and a pushed-down filter comparing that column to a literal, the shape that today returns EVERY row for a `list` column and fails loudly for a `struct` or `map` column
* *WHEN* the scan builds the Parquet data source for that table
* *THEN* the scan SHALL DISABLE Parquet filter pushdown for that table, so the optimizer keeps a `FilterExec` that evaluates the predicate over the RENDERED `Utf8` column and MUST NOT let DataFusion remove the `FilterExec` on the strength of the logical schema and then drop the conjunct against the physical one
* *AND* the returned rows SHALL be exactly those the predicate selects: `WHERE tags = '["hello","world"]'` SHALL return only the matching row and MUST NOT return every row, and `WHERE id = 2 AND tags = '["hello","world"]'` SHALL return no row when row 2's tags differ, the measured wrong answers this scenario removes
* *AND* the fix SHALL apply to `list` as well as to `struct` and `map`, because the dropped-conjunct bug is PRE-EXISTING for `list` and has the same root cause as this issue, a nested physical column under a `Utf8` logical one, so leaving it would ship a known silent wrong-rows bug beside its own fix
* *AND* the fix SHALL hold for EVERY comparison shape measured wrong end to end, `=`, `<>`, `>`, `IN`, `LIKE`, and a scalar function wrapping the column such as `UPPER(col)` or `LENGTH(col)`, and for a conjunction mixing a primitive predicate with a nested one, whose primitive half already applied while the nested half vanished
* *AND* `IS NULL` and `IS NOT NULL` SHALL keep returning the rows they already return correctly, and a SELECT-LIST expression over the column SHALL keep returning the per-row values it already returns correctly, so the fix adds no regression to the two shapes that were never broken
* *AND* it SHALL hold on BOTH table formats, because the bug was reproduced on an Iceberg `list<string>` and on a Delta `array<integer>` alike, and the scan side MUST NOT branch on table format
* *AND* the decision SHALL be taken from the presence of a JSON-rendered nested column in the table schema, read through the SAME single owning predicate the cast diversion reads, and MUST NOT be taken from substring-matching the rendered filter text for a column name
* *AND* the accepted cost SHALL be named: a query over a table that carries a nested column loses Parquet ROW-LEVEL filter pushdown for all its columns, so late materialization no longer skips rows within a row group. Row-group and page PRUNING from statistics is a separate stage and is unaffected

### Scenario: Every pushdown shape treats a nested column as the VARCHAR Exasol declared

* *GIVEN* a pushdown request whose WHERE predicate, GROUP BY key, ORDER BY key, aggregate argument, `COUNT(DISTINCT)` argument, join condition, or select-list expression references a list, struct, or map column
* *WHEN* the adapter plans that request
* *THEN* the adapter SHALL treat that column exactly as it treats any other `VARCHAR(2000000)` column, and this feature MUST NOT add a type-based decline gate, a capability withdrawal, or an error path for any of those shapes, the ONE correctness hole, a dropped Parquet row-filter conjunct, is closed inside the scan by the preceding scenario rather than by declining the predicate to an Exasol-dialect wrapper, which would transfer every row across the `.so` boundary to achieve the same result
* *AND* a GROUP BY key, an ORDER BY key, `COUNT(*)`, `COUNT(DISTINCT)`, and an `IS NOT NULL` predicate over such a column SHALL each return correct results with no gate, which was measured directly rather than inferred
* *AND* the column's logical Arrow type SHALL remain `Utf8` through the whole plan, so DataFusion evaluates every such expression as an ordinary string operation over the RENDERED JSON, the same text Exasol itself sees in the emitted `VARCHAR(2000000)`, which is what makes the DataFusion-side and Exasol-side evaluations of one predicate agree by construction rather than by luck
* *AND* the recorded TopN decline that keys on `needs_json_fallback` over the sort key's logical tag SHALL keep its recorded behavior byte-identical: a nested column's tag is `utf8`, so it does NOT trigger that decline, and an out-of-range `decimal128(p,s)` tag still does
* *AND* a predicate over a nested column MUST NOT prune any row group, page, or file on Parquet statistics
* *AND* if any pruning stage is found to evaluate such a predicate against statistics describing the PHYSICAL nested values, that stage SHALL be disabled for the column rather than left to prune, because a pruned row group returns FEWER rows with no error, the one silent-wrong-rows failure mode this design admits
* *AND* the accepted cost SHALL be named rather than left implicit: with statistics pruning off for the column, the scan reads rows a statistics-prunable predicate would have skipped, a performance property, never a correctness one

### Scenario: The rendered column crosses the emit boundary as the declared VARCHAR

* *GIVEN* a nested column the schema declares to Exasol as `VARCHAR(2000000)` and an `EMITS` declaration derived from that same declaration
* *WHEN* the scan reaches the emit boundary
* *THEN* the column SHALL already carry Arrow `Utf8`, so `coerce_batch_to_exa_types` passes it through with no cast, and this feature MUST NOT add any nested-aware branch to the emit path
* *AND* the declared Exasol type MUST stay `VARCHAR(2000000)` for every list, struct, and map column, unchanged by this feature, because Exasol has no array, list, struct, or map type
* *AND* a rendered document longer than 2,000,000 characters SHALL surface Exasol's own length error and MUST NOT be truncated, because a truncated JSON document is both invalid and silently wrong

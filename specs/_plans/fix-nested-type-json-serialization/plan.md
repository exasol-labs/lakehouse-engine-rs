# Plan: fix-nested-type-json-serialization

## Summary

Build the JSON encoder the type-mapping spec has promised since the project began, so Iceberg and
Delta `list`, `struct`, and `map` columns scan successfully and return one valid JSON document per
value instead of failing with `sqlCode 22002` or returning Arrow display text. Closes issue #350 and
the nested `delta.typeChanges` validation gap issue #357 found.

## Design

### Context

Three defects share one root cause. `iceberg_type_to_arrow` maps every nested Iceberg type to
`DataType::Utf8`, and the compact `ScanSpec::logical_schema` tag vocabulary transports that as
`"utf8"`. The physical Parquet column keeps its real nested type, so DataFusion's
`DefaultPhysicalExprAdapter` — reached through this repo's `FieldIdExprAdapter`, which wraps it — must
reconcile a physical `Struct`/`Map`/`List` against a logical `Utf8` at file open. `arrow-cast` has no
`Struct → Utf8` or `Map → Utf8` kernel, so struct and map fail the scan outright; it does have a
`List → Utf8` kernel for a list of PRIMITIVES, which renders Arrow display text (`[hello, world]` —
unquoted, and `[a, ]` for a null element, both invalid JSON), so only that one case silently returns
wrong text; `list<struct>` fails like a struct, because the kernel recurses into the element type. No JSON encoder exists anywhere in the repo. Delta side-stepped the
whole problem by refusing `struct` and `map` at plan time, citing this issue.

- **Goals** — every list, struct, and map column scans and returns valid JSON, identical on the
  Iceberg and Delta paths; the JSON is keyed by the table's LOGICAL field names, including for a
  column-mapped Delta table; nested `delta.typeChanges` entries are validated; no pushdown capability
  changes and no `ScanSpec` type-tag change.
- **Non-Goals** — Binary's JSON validity (issue #351, and its current display/refusal behavior must
  not change); a canonical (re-sorted) JSON key order; casting a nested-level type promotion to the
  current logical type; parsing Iceberg `schema.name-mapping.default` nested entries (issue #28);
  retaining Parquet row-level filter pushdown on a table that carries a nested column, which is traded
  away to stop DataFusion silently dropping such a predicate.

### Decision

Render nested columns to JSON **at the Arrow column level inside the scan**, and keep the column's
**logical type `Utf8` everywhere**. The logical type is what DataFusion plans on, what the pushdown
planner reads, and what Exasol declared — so leaving it `Utf8` means the JSON string *is* the column's
type end to end, and no pushdown decision changes. What crosses the wire in addition is not a type but
a **nested field descriptor**: each nested field's logical name plus the one binding key its format's
column-mapping selects, which is the information the renderer needs to key the JSON by logical name.

#### Architecture

```
PLAN TIME (VS adapter)                        SCAN TIME (UDF)
┌──────────────────────────┐                  ┌───────────────────────────────────────────┐
│ Iceberg / Delta reader   │                  │ registered table schema: nested = Utf8    │
│  arrow_type  = "utf8" ───┼── ScanSpec ─────▶│                                           │
│  nested      = descriptor│  LogicalField    │ FieldIdExprAdapter                        │
└──────────────────────────┘                  │  ├─ nested physical col → json_render()   │──▶ Utf8
   (unchanged tag vocabulary)                 │  └─ primitive col → DefaultPhysicalExpr…  │──▶ cast
                                              │        (type-relaxation, untouched)       │
                                              └───────────────────────────────────────────┘
                                                            │
                                              legacy path (no logical schema):
                                              build_scan_sql / render_join_select_item
                                                nested → json_render(col)
                                                other incompatible → CAST(col AS VARCHAR)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Divert, don't cast | `FieldIdExprAdapter::rewrite` | `arrow-cast` has no correct nested→text cast; the one that exists is wrong. Intercepting before delegation leaves every primitive relaxation cast untouched. |
| One owning predicate | `types/mapping.rs` | `needs_json_fallback` is too broad — true for `Binary` and out-of-range `Decimal128`, both of which must keep the CAST path. A narrower nested predicate is added beside it, not folded into it. |
| One encoder, two applications | `scan/json_render.rs` | Reached as a substituted physical expression (logical-schema path) and as a function in generated SQL (legacy inference path), so both paths render a value identically by construction. |
| Binding key, recursed | nested descriptor | `LogicalField` already chooses `field_id` XOR `physical_name` XOR identity per column. The nested descriptor is that same choice at depth — a widened existing concept, not a new format-specific field. |
| Refusal by member path | `delta_schema.rs` | One composer for `array` element, `struct` field, and `map` key/value replaces the array-only composer, so nesting adds no message layer per kind. |
| Keep the filter, drop the pushdown | `PositionalDeleteScanTable::scan` | DataFusion decides Parquet filter pushdown against the logical schema (`Utf8`, primitive, so "supported") and then re-checks against the physical one (nested, so the conjunct is dropped) — silently returning every row. Disabling the row-filter pushdown for such a table keeps a `FilterExec` that evaluates the predicate correctly. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Logical type stays `Utf8`; nested type never enters the tag vocabulary | Make `iceberg_type_to_arrow` recursive and give `arrow_type_to_tag`/`arrow_type_from_tag` a recursive nested grammar (the direction the issue proposed) | A nested logical type makes the column a genuine `Struct`/`Map` during DataFusion execution, where DataFusion has no comparison, ordering, hashing, or aggregation operator for it. The adapter would then have to newly DECLINE five pushdown shapes at five separate decision sites — WHERE filters (`type_accepted_rewrite`), N-scan per-leg conjuncts (`type_screened_leg_filter`), GROUP BY keys (`classify_request_shape`), aggregate arguments incl. `COUNT(DISTINCT)` (`validate_agg_col_types`), and the broadcast join condition (`render_broadcast_join`) — and `handle_pushdown` would have to be re-sequenced so the logical schema resolves BEFORE the filter decision it currently precedes by 37 lines. Keeping `Utf8` leaves all five sites, the capability constant, and the wire tag untouched. |
| Nested field descriptor rides on `LogicalField` as a separate optional field | Carry the nested type inside `arrow_type`; or render the file's PHYSICAL nested names and refuse column-mapped Delta tables | The vendored `stats-all-types` fixture — the only Delta fixture with a struct — is `delta.columnMapping.mode = name` with `col-7f2f94cf-…` physical inner names, so rendering physical names would emit opaque identifiers as JSON keys for the common Unity/Databricks table shape, and refusing them would leave the plan with no working Delta struct coverage at all. A descriptor is not a type: the column's type is the JSON string. |
| Struct key order = physical file order, not re-sorted | Sort object names lexicographically for a file-independent canonical form | The Iceberg spec makes struct field order non-semantic and permits reordering, so two files may legally differ and a logically-equal value can render as two strings. Both the DataFusion-side and Exasol-side views read the SAME rendered string, so the engines can never DISAGREE; the effect is confined to `GROUP BY`/`DISTINCT` separating such values. Canonicalizing would change the rendering for every single-layout table (the overwhelming majority) to satisfy a rare one. |
| Map keys stringified into JSON object names | Array of `{"key":…,"value":…}` pairs, preserving key types | Interview A2: an array-of-pairs shape makes the emitted VARCHAR unreadable by an Exasol JSON path expression, which is the only reason the column is surfaced as JSON. Key type is recoverable from the declared catalog type. |
| Struct keyed by field NAME; map as one object | Apache Iceberg § JSON single-value serialization: struct *"JSON object by field ID"* (`{"1":1}`), map *"JSON object of key and value arrays"* (`{"keys":[…],"values":[…]}`) | Appendix D is scoped to metadata single values — default values and manifest bounds — and the spec defines no JSON encoding for scan output rows. Both Appendix D shapes are unusable from Exasol SQL: a field-ID-keyed object has no readable path, and parallel key/value arrays cannot be read by key. Recorded as a named, reasoned divergence rather than a silent gap. |
| `binary` stays refused at every nesting depth on the Delta path | Admit nested `binary` — the encoder renders faithful lowercase hex, matching Iceberg's own Appendix D convention for `binary`/`fixed` | Interview A1 scopes Binary out entirely (issue #351). Admitting it nested would widen Binary's reach, so `array<binary>` keeps its recorded refusal and `struct`/`map` containing `binary` join it. The resulting Iceberg-vs-Delta asymmetry (Iceberg refuses no type, so its nested binary IS rendered) is pre-existing and named, not introduced. |
| Disable Parquet row-filter pushdown for a table carrying a nested column | Decline the predicate in the VS adapter and self-apply it in the Exasol wrapper (`type_accepted_rewrite`, one site — not five); or accept the current behavior | Measured: with `pushdown_filters = true` a predicate over such a column is DROPPED, returning every row — already true today for `list`, and it would turn today's hard `struct`/`map` error into a silent wrong answer. With `pushdown_filters = false` every measured query returns correct rows, because the rendering expression evaluates fine inside a `FilterExec`. Keeping the predicate in DataFusion also transfers fewer rows across the `.so` boundary than declining it to Exasol would, and needs no re-sequencing of `handle_pushdown`. |
| `arrow_value_at` gains no nested arm | Add a nested arm as a defensive backstop | Under this design a nested column is rendered to `Utf8` before the value-conversion boundary, and the partial-aggregate path that calls `arrow_value_at` carries only group keys and aggregate results. An arm there would be unreachable code, and `datafusion-scan/type-mapping-module-structure` records that this wildcard arm stays a wildcard. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/nested-json-rendering | NEW | `specs/_plans/fix-nested-type-json-serialization/datafusion-scan/nested-json-rendering/spec.md` |
| datafusion-scan/type-mapping | CHANGED | `specs/_plans/fix-nested-type-json-serialization/datafusion-scan/type-mapping/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `specs/_plans/fix-nested-type-json-serialization/datafusion-scan/scan-execution/spec.md` |
| vs-adapter/delta-type-mapping | CHANGED | `specs/_plans/fix-nested-type-json-serialization/vs-adapter/delta-type-mapping/spec.md` |
| e2e-harness/e2e-harness | CHANGED | `specs/_plans/fix-nested-type-json-serialization/e2e-harness/e2e-harness/spec.md` |
| e2e-harness/unity-catalog-e2e-harness-delta-queries | CHANGED | `specs/_plans/fix-nested-type-json-serialization/e2e-harness/unity-catalog-e2e-harness-delta-queries/spec.md` |

## Impact

Iceberg and Delta `list`, `struct`, and `map` columns become queryable and return valid JSON.

Three consequences are visible to anyone already querying these tables. First, an `array`/`list`
column's returned TEXT changes: it was Arrow display text (`[1, 2]`, and unquoted for strings) and
becomes strict JSON (`[1,2]`, `["hello","world"]`) — a query that string-matched the old rendering
must be updated. Second, Delta `struct` and `map` columns stop being refused, so `SELECT *` over a
table carrying one now succeeds where it previously failed; the `stats_all_types` fixture's refused
count falls from three columns to one. Third, a NULL nested value now returns SQL NULL where a list
column previously returned the text of Arrow's null rendering.

This plan also fixes a pre-existing silent wrong-rows bug it uncovered while verifying the design.
A WHERE predicate over a `list` column today returns EVERY row: DataFusion approves the filter
pushdown against the logical schema (where the column is `Utf8`) and then drops the conjunct against
the physical one (where it is `List`), applying it nowhere. Confirmed end to end through Exasol on
both formats — `=`, `<>`, `>`, `IN`, `LIKE`, `UPPER(col) =`, and `LENGTH(col) =` each matched all 4
rows of a seeded Iceberg table, `WHERE ID > 2 AND TAGS = 'zzz'` returned rows 3 and 4, and a Delta
`array<integer>` behaved identically — while plain `VARCHAR` and `STRING` control columns filtered
correctly. `IS NULL`, `IS NOT NULL`, and select-list expressions were already correct and stay so.
Same root cause as issue #350, so it is fixed here rather than tracked separately.

Two limitations are deliberate and named. A nested-level type widening renders the file's physical
value, so a Delta `decimal(10,1)` → `decimal(12,3)` change renders `1.5` in the old file and `1.500`
in the new one. And a JSON rendering is longer than the display text it replaces, so a very large
nested value can exceed the declared `VARCHAR(2000000)` and fail with Exasol's length error rather
than being truncated.

No breaking change to `createVirtualSchema`: every affected column was already declared
`VARCHAR(2000000)`. No capability is added or withdrawn. `Binary` behavior is unchanged (issue #351);
its Delta refusal message changes only its cited issue number, from the closing #350 to #351.

## Dependencies

- `arrow-json = "58"` declared directly on the engine crate. Version 58.3.0 is already resolved in
  `Cargo.lock`, and `arrow_json::writer::{make_encoder, NullableEncoder, Encoder, EncoderOptions}` are
  fully public there — verified by compiling against them. A direct dependency is preferred over
  reaching it as `arrow::json`, which works today only because DataFusion enables `arrow/default`.
  No new external crate enters the build.
- Issue #322 (closed, merged) introduced the Delta `struct`/`map` refusal this plan removes.
- Issue #351 owns Binary; issue #28 owns Iceberg nested name-mapping. Both stay open and out of scope.

## Migration

| Current | New |
|---------|-----|
| `LogicalField` with no `nested` key | `LogicalField` with an optional `nested` descriptor, serde-skipped when absent, so a spec serialized before this change deserializes unchanged |
| Delta `struct`/`map` column absent from the logical schema and listed as refused | Present in the logical schema, tagged `utf8`, carrying a nested descriptor |

## Implementation Tasks

1. Re-confirm the baseline against the Docker Exasol stack before changing anything, so the fix is
   measured against a captured starting point on the implementer's own build. Planning already
   established it end to end on a pristine `9b39cbf` build: `struct`, `map<string,string>`,
   `map<int,string>`, and `list<struct>` all fail with the physical-to-logical `Utf8` cast error
   (sqlCode 22002); `list<string>` returns `[hello, world]` and `[a, ]`; and every comparison predicate
   over a nested column returns EVERY row on both formats.
2. Add the single owning predicate for the JSON-rendered nested Arrow set (`List`, `LargeList`,
   `FixedSizeList`, `Struct`, `Map`) to `crates/lakehouse-engine/src/types/mapping.rs`, with sibling
   tests pinning that `needs_json_fallback`'s answer is unchanged for every input — notably `Binary`
   and an out-of-range `Decimal128`, which must stay in the CAST path.
3. Declare `arrow`'s `json` feature on the engine crate so `arrow::json::writer::make_encoder` stops
   depending on a transitive feature another crate enables.
4. Add the format-neutral nested field descriptor to `LogicalField` in
   `crates/lakehouse-engine/src/scan/spec.rs` — recursively, each nested field carrying its logical
   name and the one binding key its format selects — with serde round-trip tests proving a spec
   authored before the field existed still deserializes and that a primitive column serializes no new
   key. [expert]
5. Build `crates/lakehouse-engine/src/scan/json_render.rs`: the column-level encoder over
   `arrow::json::writer::make_encoder` with `explicit_nulls(true)`, the top-level null guard that
   emits an Arrow null rather than `{}`/`[]`/`"null"`, and the map-key stringification that replaces a
   non-`Utf8` key child with a `Utf8` array (nested keys via their own JSON rendering, other types via
   the Arrow-to-`Utf8` cast, an unrenderable key type via a clean error). Declare it a private
   submodule re-exported flat from `scan/mod.rs` per `datafusion-scan/scan-module-structure`. Sibling
   tests must use POPULATED values and assert every rendered document parses as JSON. [expert]
6. Derive the nested descriptor recursively on the Iceberg path in
   `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg.rs`, preserving each nested field's
   Iceberg field-id, and assert in tests that `iceberg_type_to_arrow` still returns `Utf8` for
   `list`/`struct`/`map`.
7. Rebuild the Delta nested walk in
   `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs` as ONE recursion producing
   three answers per column: renderability classification (`struct`/`map` join the `utf8`-tagged set;
   `binary` and `variant` stay refused at any depth), nested `delta.typeChanges` validation with a
   composed `column.field[.fieldPath]` path in the refusal, and the nested descriptor's logical
   names and binding keys read from `delta.columnMapping.*` under the mode in force. Delete
   `struct_refusal` and `map_refusal`, replace `array_element_refusal` with one container-member
   composer, and re-point `binary_refusal`'s citation from #350 to #351. [expert]
8. Thread the nested descriptors into `FieldIdResolution` in
   `crates/lakehouse-engine/src/scan/raw_scan.rs` beside `declared_physical_names`, and resolve each
   file's nested field tree onto the logical one inside
   `crates/lakehouse-engine/src/scan/field_id_projection.rs` — renaming, reordering, dropping
   unclaimed physical fields, and null-filling absent logical fields — by recursing `bind_columns`'
   existing first-match-wins binding order rather than adding a second rule. [expert]
9. Divert nested columns around the cast in `FieldIdExprAdapter`: give the delegated
   `DefaultPhysicalExprAdapter` a logical schema in which each nested column carries its resolved
   physical nested type so no cast is attempted, then substitute the JSON-rendering expression in the
   existing post-delegation pass. Prove in tests that all 13 `datafusion-scan/type-relaxation` pairs
   still cast through the delegate and that the Parquet opener still reads the nested column. [expert]
10. Stop DataFusion silently dropping a predicate over a rendered nested column: disable Parquet
    row-filter pushdown for a table whose schema carries one, reading the decision from the same
    owning predicate the cast diversion reads. Assert that `WHERE tags = '["hello","world"]'` returns
    only the matching row and that `WHERE id = 2 AND tags = '…'` returns none — both measurably wrong
    today. [expert]
11. Prove statistics pruning cannot drop a row for a predicate over a rendered nested column. Build a
    MULTI-row-group Parquet file whose per-group leaf statistics would falsely exclude the rendered
    document — a `list<string>` whose leaf min/max are `"hello"`/`"world"` against the document
    `["hello","world"]`, where `[` sorts below `h` — and assert the matching row still returns.
    Disable the offending pruning stage for the column if any stage evaluates it. A spike
    `EXPLAIN ANALYZE` showed DataFusion DOES build `pruning_predicate=tags_null_count@2 != row_count@3
    AND tags_min@0 <= ["hello","world"] AND ["hello","world"] <= tags_max@1` plus a bloom-filter stage
    for exactly this shape. [expert]
12. Route the legacy no-logical-schema path through the same encoder: in
    `crates/lakehouse-engine/src/scan/raw_scan.rs`'s `build_scan_sql` and
    `crates/lakehouse-engine/src/scan/join_scan.rs`'s `render_join_select_item`, emit the JSON
    encoder for a nested type and keep `CAST(col AS VARCHAR)` byte-identical for every non-nested
    incompatible type. Create `crates/lakehouse-engine/src/scan/join_scan_tests.rs`, which does not
    exist yet.
13. Update the assertions that pin today's wrong behavior. `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`'s
    `a_list_column_tagged_utf8_is_cast_by_the_field_id_expression_adapter` currently asserts the display
    text `"[1, 2, 3]"` and must assert the JSON `"[1,2,3]"` — a spike run confirmed it is the single
    failing test after the fix. Replace the degenerate zero-field-struct assertions in
    `crates/lakehouse-engine/src/scan/convert_tests.rs` and
    `crates/lakehouse-engine/src/types/mapping_tests.rs` with populated nested values, and add the
    assertion that the available `List(Int32) → Utf8` cast produces display text and NOT valid JSON.
14. Add the Iceberg E2E fixture and suite. Every shape IS writable with iceberg-rust 0.10 — a live
    probe wrote all six, `map<int, string>` included — so correct the stale
    `crates/lakehouse-engine/tests/common/seed.rs` comment claiming iceberg-rust exposes no struct/list
    writer. The obstacle is nested field-id reassignment: `iceberg-rest-fixture` assigns fresh ids on
    `create_table` and `overlay_iceberg_field_ids` repairs only TOP-LEVEL ids by name, so a batch built
    from the authored schema fails with `DataInvalid => Field id 9 not found in struct array`. Build
    the Arrow batch from `iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())`
    AFTER `create_table`; `create_and_append_files` takes its batches up front and cannot do this, so
    this seed needs its own create-then-write path. Then add a `seed_complex_types_probe` helper in
    `crates/lakehouse-engine/tests/common/seed.rs` writing populated `list<string>`, `list<int>`,
    `struct<street,city>`, `map<string,string>`, `map<int,string>`, and `list<struct<a>>` columns with
    null, empty-collection, and null-member rows; a new `e2e_complex_type_test.rs`; and the new binary
    added to the `test-e2e` make target.
15. Update the Delta E2E expectations in `crates/lakehouse-engine/tests/e2e_unity_test.rs`: the
    refused set narrows to `binary_col` alone, and `nested_struct` must return the LOGICAL inner names
    `inner_int`/`inner_string`/`inner_double` — never a `col-`-prefixed physical name.
16. Verify live against the Docker Exasol stack that a WHERE predicate, a GROUP BY key, an ORDER BY
    key, an aggregate argument, `COUNT(DISTINCT)`, a join condition, and a select-list expression over
    a nested column each return correct rows, capturing `EXPLAIN VIRTUAL` for each, and record the
    evidence. Extend the decline mechanism only if a shape is found to misbehave.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1 |
| Group B | 2, 3, 4, 5 |
| Group C | 6, 7 |
| Group D | 8, 12, 13 |
| Group E | 9 |
| Group F | 10, 11, 14, 15 |
| Group G | 16 |

Sequential dependencies:
- Group A → Group B (the live repro fixes the target behavior before any code changes)
- Group B → Group C (both format readers populate the descriptor type task 4 defines)
- Group B, Group C → Group D (the scan needs the predicate, the encoder, and the descriptor)
- Group D → Group E (the cast diversion consumes the resolved nested field tree from task 8)
- Group E → Group F (the pushdown fix and the pruning proof both need the diverted expression in place) → Group G

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `struct_refusal` in `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs` | `struct` is no longer refused; its "arrow-cast reports no cast to text" reason no longer describes anything the engine does |
| Function | `map_refusal` in `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs` | Same, for `map` |
| Function | `array_element_refusal` in `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs` | Replaced by one container-member composer serving `array` elements, `struct` fields, and `map` keys/values alike |
| Test | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` struct/map refusal assertions | Assert a refusal that no longer happens |
| Test | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_format_reader_tests.rs` `map_col` refusal fixture | Same |
| Test | `crates/lakehouse-engine/src/adapter/pushdown/refused_columns_tests.rs` `MAP_REASON` constant and its uses | Same |
| Assertion | `a_list_column_tagged_utf8_is_cast_by_the_field_id_expression_adapter` in `crates/lakehouse-engine/src/scan/raw_scan_tests.rs` | Pins the Arrow display text `"[1, 2, 3]"` this plan replaces with `"[1,2,3]"`; kept but re-asserted, not deleted |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| nested-json-rendering / A list, struct, or map value renders as one valid JSON document | Unit | `crates/lakehouse-engine/src/scan/json_render_tests.rs` | `populated_nested_values_render_as_valid_json_documents` |
| nested-json-rendering / A null nested value emits SQL NULL, not the text "null" | Unit | `crates/lakehouse-engine/src/scan/json_render_tests.rs` | `null_cells_emit_sql_null_and_null_members_render_explicitly` |
| nested-json-rendering / A non-string map key is stringified into the JSON object name | Unit | `crates/lakehouse-engine/src/scan/json_render_tests.rs` | `non_utf8_map_keys_stringify_into_object_names` |
| nested-json-rendering / Rendered field names are the table's logical names, not the file's physical ones | Integration | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `nested_fields_resolve_to_logical_names_across_binding_keys` |
| nested-json-rendering / A nested physical column is diverted around the physical-to-logical cast | Integration | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `nested_physical_column_bypasses_the_cast_and_yields_utf8` |
| nested-json-rendering / One encoder serves both the logical-schema path and the legacy inference path | Integration | `crates/lakehouse-engine/tests/scan_column_binding.rs` | `inferred_schema_path_renders_nested_columns_through_the_same_encoder` |
| nested-json-rendering / A predicate over a rendered nested column is evaluated, never silently dropped | Integration | `crates/lakehouse-engine/tests/scan_parquet_pruning.rs` | `predicate_over_a_rendered_nested_column_is_applied_not_dropped` |
| nested-json-rendering / Every pushdown shape treats a nested column as the VARCHAR Exasol declared | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_complex_type_test.rs` | `nested_columns_push_down_as_the_declared_varchar_in_every_shape` |
| nested-json-rendering / The rendered column crosses the emit boundary as the declared VARCHAR | Integration | `crates/lakehouse-engine/tests/scan_batch_loop.rs` | `rendered_nested_column_passes_the_emit_coercion_unchanged` |
| type-mapping / Incompatible Arrow types are serialized to JSON VARCHAR | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `nested_and_non_nested_incompatible_halves_are_owned_by_one_predicate_each` |
| type-mapping / A mixed-column Parquet file round-trips through schema mapping and scan | Integration | `crates/lakehouse-engine/tests/scan_column_binding.rs` | `mixed_column_parquet_file_emits_json_for_populated_list_and_struct` |
| type-mapping / Iceberg logical schema maps to Arrow types for scan registration | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `nested_iceberg_fields_stay_utf8_tagged_and_carry_a_nested_descriptor` |
| scan-execution / Incompatible Arrow columns are emitted as JSON strings | Unit | `crates/lakehouse-engine/src/scan/convert_tests.rs` | `incompatible_columns_emit_json_strings` |
| delta-type-mapping / A Delta type Exasol cannot represent natively is surfaced as a VARCHAR rendering | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `containers_classify_recursively_by_renderability` |
| delta-type-mapping / A Delta type whose Arrow form cannot be rendered faithfully is refused by name | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `refused_set_is_binary_variant_and_containers_of_them` |
| delta-type-mapping / A refused column refuses only the requests that read or emit it | Unit | `crates/lakehouse-engine/src/adapter/pushdown/refused_columns_tests.rs` | `only_binary_col_refuses_requests_in_the_stats_all_types_shape` |
| delta-type-mapping / The castability claims behind the mapping are asserted, not assumed | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `arrow_castability_to_utf8_pins_the_three_delta_type_sets` |
| delta-type-mapping / Every recorded Delta type change is validated, and an unsupported one refuses its column | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `nested_type_changes_are_validated_and_refuse_with_a_composed_path` |
| delta-type-mapping / Every nested field's logical name and binding key reach the scan | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `nested_descriptor_carries_logical_names_and_mode_selected_binding_keys` |
| e2e-harness / An Iceberg table's list, struct, and map columns return valid JSON end to end | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_complex_type_test.rs` | `iceberg_nested_columns_return_valid_json_end_to_end` |
| unity-catalog-e2e-harness-delta-queries / A refused column refuses only the queries naming it | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_refused_column_refuses_only_the_queries_naming_it` |
| unity-catalog-e2e-harness-delta-queries / A Delta table's varied types return their expected Exasol types and values | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_varied_types_return_their_expected_exasol_types_and_values` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| datafusion-scan/nested-json-rendering | `SELECT ID, TAGS, ADDR, ATTRS FROM LAKEHOUSE_VS.COMPLEX_PROBE ORDER BY ID` via `exapump` against the Docker Exasol | `TAGS` = `["hello","world"]`, `ADDR` = `{"street":"Main St","city":"Berlin"}`, `ATTRS` = `{"a":"1","b":"2"}`; the NULL row returns SQL NULL for each |
| datafusion-scan/type-mapping | `SELECT COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA='LAKEHOUSE_VS' AND COLUMN_TABLE='COMPLEX_PROBE'` | every nested column declared `VARCHAR(2000000)` |
| vs-adapter/delta-type-mapping | `SELECT NESTED_STRUCT, MAP_COL, ARRAY_COL FROM UNITY_VS.STATS_ALL_TYPES` | `NESTED_STRUCT` keyed `inner_int`/`inner_string`/`inner_double` with no `col-` name; `ARRAY_COL` a strict JSON array; the query succeeds where it previously refused |
| vs-adapter/delta-type-mapping (refusal) | `SELECT BINARY_COL FROM UNITY_VS.STATS_ALL_TYPES` | error naming `binary_col`, citing issue #351 and not #350 |
| datafusion-scan/nested-json-rendering (pushdown) | `EXPLAIN VIRTUAL SELECT COUNT(*) FROM LAKEHOUSE_VS.COMPLEX_PROBE WHERE TAGS = '["hello","world"]'` then the same query without `EXPLAIN VIRTUAL` | the pushdown SQL shows the predicate reaching the scan as a string comparison; the query returns the matching row count |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E (Iceberg) | `make test-e2e` | 0 failures |
| E2E (Unity/Delta) | `make test-e2e-unity` | 0 failures |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 warnings |
| Format | `cargo fmt` | No changes |

# Tasks: fix-nested-type-json-serialization

## Phase 2: Implementation (Group A)
- [x] 2.1 Re-confirm the baseline against the Docker Exasol stack before changing anything: reproduce
      that `struct`, `map<string,string>`, `map<int,string>`, and `list<struct>` fail with the
      physical-to-logical `Utf8` cast error (sqlCode 22002); `list<string>` returns `[hello, world]`
      and `[a, ]`; and every comparison predicate over a nested column returns EVERY row on both
      formats. Capture the evidence.

## Phase 2: Implementation (Group B)
- [x] 2.2 Add the single owning predicate for the JSON-rendered nested Arrow set (`List`, `LargeList`,
      `FixedSizeList`, `Struct`, `Map`) to `crates/lakehouse-engine/src/types/mapping.rs`, with sibling
      tests pinning that `needs_json_fallback`'s answer is unchanged for every input — notably
      `Binary` and an out-of-range `Decimal128`, which must stay in the CAST path.
- [x] 2.3 Declare `arrow`'s `json` feature on the engine crate so `arrow::json::writer::make_encoder`
      stops depending on a transitive feature another crate enables.
- [x] 2.4 Add the format-neutral nested field descriptor to `LogicalField` in
      `crates/lakehouse-engine/src/scan/spec.rs` — recursively, each nested field carrying its
      logical name and the one binding key its format selects — with serde round-trip tests proving a
      spec authored before the field existed still deserializes and that a primitive column
      serializes no new key. [expert]
- [x] 2.5 Build `crates/lakehouse-engine/src/scan/json_render.rs`: the column-level encoder over
      `arrow::json::writer::make_encoder` with `explicit_nulls(true)`, the top-level null guard that
      emits an Arrow null rather than `{}`/`[]`/`"null"`, and the map-key stringification that
      replaces a non-`Utf8` key child with a `Utf8` array (nested keys via their own JSON rendering,
      other types via the Arrow-to-`Utf8` cast, an unrenderable key type via a clean error). Declare
      it a private submodule re-exported flat from `scan/mod.rs`. Sibling tests must use POPULATED
      values and assert every rendered document parses as JSON. [expert]

## Phase 2: Implementation (Group C)
- [x] 2.6 Derive the nested descriptor recursively on the Iceberg path in
      `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg.rs`, preserving each nested
      field's Iceberg field-id, and assert in tests that `iceberg_type_to_arrow` still returns `Utf8`
      for `list`/`struct`/`map`.
- [x] 2.7 Rebuild the Delta nested walk in
      `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema.rs` as ONE recursion
      producing three answers per column: renderability classification (`struct`/`map` join the
      `utf8`-tagged set; `binary` and `variant` stay refused at any depth), nested
      `delta.typeChanges` validation with a composed `column.field[.fieldPath]` path in the refusal,
      and the nested descriptor's logical names and binding keys read from `delta.columnMapping.*`
      under the mode in force. Delete `struct_refusal` and `map_refusal`, replace
      `array_element_refusal` with one container-member composer, and re-point `binary_refusal`'s
      citation from #350 to #351. [expert]

## Phase 2: Implementation (Group D)
- [x] 2.8 Thread the nested descriptors into `FieldIdResolution` in
      `crates/lakehouse-engine/src/scan/raw_scan.rs` beside `declared_physical_names`, and resolve
      each file's nested field tree onto the logical one inside
      `crates/lakehouse-engine/src/scan/field_id_projection.rs` — renaming, reordering, dropping
      unclaimed physical fields, and null-filling absent logical fields — by recursing
      `bind_columns`' existing first-match-wins binding order rather than adding a second rule. [expert]
- [x] 2.9 Route the legacy no-logical-schema path through the same encoder: in
      `crates/lakehouse-engine/src/scan/raw_scan.rs`'s `build_scan_sql` and
      `crates/lakehouse-engine/src/scan/join_scan.rs`'s `render_join_select_item`, emit the JSON
      encoder for a nested type and keep `CAST(col AS VARCHAR)` byte-identical for every non-nested
      incompatible type. Create `crates/lakehouse-engine/src/scan/join_scan_tests.rs`, which does not
      exist yet.
- [x] 2.10 Update the assertions that pin today's wrong behavior.
      `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`'s
      `a_list_column_tagged_utf8_is_cast_by_the_field_id_expression_adapter` currently asserts the
      display text `"[1, 2, 3]"` and must assert the JSON `"[1,2,3]"`. Replace the degenerate
      zero-field-struct assertions in `crates/lakehouse-engine/src/scan/convert_tests.rs` and
      `crates/lakehouse-engine/src/types/mapping_tests.rs` with populated nested values, and add the
      assertion that the available `List(Int32) → Utf8` cast produces display text and NOT valid
      JSON.

## Phase 2: Implementation (Group E)
- [x] 2.11 Divert nested columns around the cast in `FieldIdExprAdapter`: give the delegated
      `DefaultPhysicalExprAdapter` a logical schema in which each nested column carries its resolved
      physical nested type so no cast is attempted, then substitute the JSON-rendering expression in
      the existing post-delegation pass. Prove in tests that all 13 `datafusion-scan/type-relaxation`
      pairs still cast through the delegate and that the Parquet opener still reads the nested
      column. [expert]

## Phase 2: Implementation (Group F)
- [x] 2.12 Stop DataFusion silently dropping a predicate over a rendered nested column: disable
      Parquet row-filter pushdown for a table whose schema carries one, reading the decision from the
      same owning predicate the cast diversion reads. Assert that `WHERE tags = '["hello","world"]'`
      returns only the matching row and that `WHERE id = 2 AND tags = '…'` returns none. [expert]
- [x] 2.13 Prove statistics pruning cannot drop a row for a predicate over a rendered nested column.
      Build a MULTI-row-group Parquet file whose per-group leaf statistics would falsely exclude the
      rendered document — a `list<string>` whose leaf min/max are `"hello"`/`"world"` against the
      document `["hello","world"]`, where `[` sorts below `h` — and assert the matching row still
      returns. Disable the offending pruning stage for the column if any stage evaluates it. [expert]
- [x] 2.14 Add the Iceberg E2E fixture and suite. Correct the stale
      `crates/lakehouse-engine/tests/common/seed.rs` comment claiming iceberg-rust exposes no
      struct/list writer. Build the Arrow batch from
      `iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())` AFTER
      `create_table` via its own create-then-write path. Add a `seed_complex_types_probe` helper in
      `crates/lakehouse-engine/tests/common/seed.rs` writing populated `list<string>`, `list<int>`,
      `struct<street,city>`, `map<string,string>`, `map<int,string>`, and `list<struct<a>>` columns
      with null, empty-collection, and null-member rows; a new `e2e_complex_type_test.rs`; and the
      new binary added to the `test-e2e` make target.
- [x] 2.15 Update the Delta E2E expectations in `crates/lakehouse-engine/tests/e2e_unity_test.rs`: the
      refused set narrows to `binary_col` alone, and `nested_struct` must return the LOGICAL inner
      names `inner_int`/`inner_string`/`inner_double` — never a `col-`-prefixed physical name.

## Phase 2: Implementation (Group G)
- [x] 2.16 Verify live against the Docker Exasol stack that a WHERE predicate, a GROUP BY key, an
      ORDER BY key, an aggregate argument, `COUNT(DISTINCT)`, a join condition, and a select-list
      expression over a nested column each return correct rows, capturing `EXPLAIN VIRTUAL` for each,
      and record the evidence. Extend the decline mechanism only if a shape is found to misbehave.

## Phase 4: Review Fixes
- [x] 4.1 Give the cast diversion and the row-filter-pushdown withdrawal ONE owning predicate:
      rewrite `ColumnBinding::nested_columns` in
      `crates/lakehouse-engine/src/scan/field_id_projection.rs` to iterate `self.nested` — the
      declared descriptors — mapped to physical indices, delete the `None => verbatim(field)`
      fallback, and restate the doc paragraph. Add a test proving a physically nested column with no
      descriptor fails the cast rather than rendering, and a test in
      `crates/lakehouse-engine/src/scan/raw_scan_tests.rs` pinning that a schema whose
      `nested_columns` diverts at least one column always yields `pushdown_filters == false` from
      `scan_table_parquet_format`. [expert]
- [x] 4.2 Make `crates/lakehouse-engine/tests/scan_parquet_pruning.rs` prove all four pruning stages
      rather than three disabled ones: `sum_pruned` returns `Option<usize>` and the assertion loop
      fails naming the stage when the metric is ABSENT; the nested fixture writes
      `EnabledStatistics::Page` and a bloom filter so those stages actually run; and an added footer
      assertion pins row group 0's `tags.list.item` chunk statistics to `min = "hello"` /
      `max = "world"` so the falsely-excluding premise is verified rather than asserted in prose.
      If a newly enabled stage prunes the matching row, disable it for such a table in
      `raw_scan::scan_table_parquet_format` and correct its doc comment. [expert]
- [x] 4.3 Close the join-condition verification gap in
      `crates/lakehouse-engine/tests/e2e_complex_type_test.rs`: add a join over the rendered nested
      column against a SECOND distinct table to
      `nested_columns_push_down_as_the_declared_varchar_in_every_shape`, and add
      `explain_virtual_sql` assertions for the GROUP BY, ORDER BY, aggregate-argument, and
      `COUNT(DISTINCT)` shapes. If the join shape fails for a reason outside this plan, record it as
      a cited GitHub issue inline in the spec scenario and in `decision-log.md` rather than working
      around it. [expert]

- [x] 4.4 Remove the direct `arrow-json = "58"` dependency from
      `crates/lakehouse-engine/Cargo.toml` and its comment block; change `arrow = { workspace = true }`
      to `arrow = { workspace = true, features = ["json"] }`; change
      `crates/lakehouse-engine/src/scan/json_render.rs`'s import from
      `arrow_json::writer::{EncoderOptions, make_encoder}` to
      `arrow::json::writer::{EncoderOptions, make_encoder}`.
- [x] 4.5 Reduce `claim_logical`'s argument count in
      `crates/lakehouse-engine/src/scan/field_id_projection.rs` from four to two by introducing a
      `PhysicalKeys<'a>` struct holding `name`, `embedded_id`, and `mapped_field_id`, changing the
      signature to `claim_logical(physical: PhysicalKeys<'_>, logical: &[BindingKeys<'_>])`, and
      updating both call sites.
- [x] 4.6 Remove the duplicated `entries`/`sorted` state from `ResolvedMembers::Entries` in
      `crates/lakehouse-engine/src/scan/field_id_projection.rs`, reducing it to
      `Entries { key: Box<NestedResolution>, value: Box<NestedResolution> }`, and have
      `NestedResolution::apply`'s `Entries` arm read entries/sortedness from `self.field.data_type()`
      matched as `DataType::Map(entries, sorted)`.
- [x] 4.7 Narrow the public API surface in `crates/lakehouse-engine/src/scan/mod.rs` and
      `field_id_projection.rs`/`json_render.rs`: delete the `pub use
      field_id_projection::{NestedResolution, resolve_nested_field};` re-export, narrow
      `NestedResolution`, `resolve_nested_field`, `resolved_field`, and `apply` to `pub(super)`, and
      change `render_nested_column_as_json`'s re-export and definition to `pub(crate)`.
- [x] 4.8 Restore `build_join_sql`'s deleted doc comment verbatim from
      `git show main:crates/lakehouse-engine/src/scan/join_scan.rs` in
      `crates/lakehouse-engine/src/scan/join_scan.rs`, appending one sentence noting the JSON-render
      scalar function is registered here so `render_join_select_item` can name it.
- [x] 4.9 Move `register_nested_json_render_udf(ctx)` out of `build_scan_sql` (raw_scan.rs) and
      `build_join_sql` (join_scan.rs) into `build_session_context` in
      `crates/lakehouse-engine/src/scan/object_store.rs` so registration happens once per session at
      construction; update `register_nested_json_render_udf`'s doc comment accordingly.
- [x] 4.10 Reword the `make_encoder` error message in
      `crates/lakehouse-engine/src/scan/json_render.rs` (lines ~51-56) to state the attempt, the
      type, and the underlying cause without asserting "no encoder exists"; add
      `a_map_column_with_a_null_key_is_refused_with_a_clean_error` to
      `crates/lakehouse-engine/src/scan/json_render_tests.rs` proving a `MapArray` with a null key
      fails cleanly.
- [x] 4.11 Make `populated_nested_values_render_as_valid_json_documents` in
      `crates/lakehouse-engine/src/scan/json_render_tests.rs` actually parse every non-null rendered
      cell through the existing `parsed()` helper before comparing to the expected literal.
- [x] 4.12 Extend `crates/lakehouse-engine/src/scan/json_render_tests.rs` with `LargeList` and
      `FixedSizeList` rendering cases in `populated_nested_values_render_as_valid_json_documents`,
      and extend `non_utf8_map_keys_stringify_into_object_names` with a `List<Map<Int32, Utf8>>` case
      and a `Map` with a `LargeUtf8` key child.
- [x] 4.13 Rename `a_list_column_tagged_utf8_is_cast_by_the_field_id_expression_adapter` in
      `crates/lakehouse-engine/src/scan/raw_scan_tests.rs` (re-locate via Serena search if the cited
      line number is stale) to
      `a_list_column_tagged_utf8_is_json_rendered_by_the_field_id_expression_adapter`, deleting the
      doc-comment sentence that reconciles the old name with the behavior.
- [x] 4.14 Rewrite the outdated present-tense bug description in
      `crates/lakehouse-engine/tests/e2e_complex_type_test.rs` (~lines 244-247) above
      `nested_columns_push_down_as_the_declared_varchar_in_every_shape` so the wrong-rows behavior is
      stated as the regression this test guards against, not current behavior.
- [x] 4.15 Close a scenario-coverage gap the orchestrator's Phase 5b audit found: 3 of the
      `## Verification > Scenario Coverage` table's ~22 named scenarios had no test anywhere under
      any name. Added, in `crates/lakehouse-engine/tests/scan_column_binding.rs`:
      `mixed_column_parquet_file_emits_json_for_populated_list_and_struct` (type-mapping — one
      Parquet file with a populated `list`, a populated `struct`, and an ordinary primitive column
      together; asserts the nested columns render as valid JSON and the primitive passes through
      unaffected). Added, in `crates/lakehouse-engine/tests/scan_batch_loop.rs`:
      `rendered_nested_column_passes_the_emit_coercion_unchanged` (nested-json-rendering — a
      declared-VARCHAR emit coercion and an undeclared-type coercion both leave an already-rendered
      nested JSON column byte-identical, proving `coerce_batch_to_exa_types` needs no nested-aware
      branch). The third,
      `inferred_schema_path_renders_nested_columns_through_the_same_encoder` (nested-json-rendering —
      comparing the legacy no-logical-schema path against the field-id path for byte-identical
      output), could NOT be added in `scan_column_binding.rs` as planned: the legacy path's JSON
      rendering only exists once `lakehouse_render_nested_json` is registered on the DataFusion
      session, and that registration (`register_nested_json_render_udf`, `NestedJsonRenderUdf`,
      `NESTED_JSON_RENDER_UDF_NAME` — all `pub(super)` in `raw_scan.rs`) is reachable only through
      `build_session_context` (`pub(super)` in `object_store.rs`), which itself hard-requires an
      S3-shaped `StorageBackend` with no local-filesystem variant, so it cannot be pointed at a local
      `file://` fixture the way every other integration test in this file is. Task 4.7 of this same
      plan deliberately narrowed these items' visibility, so widening one back for this test is a
      production-code decision left to the orchestrator rather than made here. Options: (a) add a
      narrow `pub` (not `#[cfg(test)]`, which integration-test binaries cannot see) seam for
      registering the render UDF on an externally-built session, or (b) relocate this one scenario's
      test into `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`, which already has `pub(super)`
      access and already hosts both single-path predecessors this scenario compares.
      Resolved via option (b): the test lives in
      `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`, not the plan's originally-cited
      `tests/scan_column_binding.rs`, because the legacy-path UDF registration is intentionally
      crate-private (per the task 4.7 review fix) and unreachable from an external
      integration-test binary.

## Phase 3: Verification
- [x] 3.1 Run test suite (cargo test) — 1497 passed, 0 failed, workspace-wide
- [x] 3.2 Run linter (cargo clippy --workspace --all-targets -- -D warnings) — 0 warnings
- [x] 3.3 Run formatter check (cargo fmt) — clean, no diff
- [x] 3.4 Build UDF .so (make cross-musl-udf-build) — exit 0

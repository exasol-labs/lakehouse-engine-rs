# Verification Report: fix-nested-type-json-serialization

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Iceberg and Delta `list`/`struct`/`map` columns scan and return valid JSON keyed by logical names; the pre-existing silent wrong-rows predicate bug is fixed; every pushdown shape verified correct live against Docker Exasol. |
| Code review | 13 findings — 13 fixed (10 standard, 3 expert) |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-musl-udf-build`, exit 0) |
| Tests | ✓ (1497 passed, 0 failed, workspace-wide) |
| Lint | ✓ (`cargo clippy --workspace --all-targets -- -D warnings`, 0 warnings) |
| Format | ✓ (`cargo fmt --all -- --check`, clean) |
| Scenario Coverage | ✓ (all 22 scenarios in the plan's table have a passing test; 3 gaps found during audit were closed) |
| Manual Tests | ✓ (all 5 manual-testing rows verified live against Docker Exasol) |

## Test Evidence

### Coverage

Not measured via `cargo llvm-cov` this run (not requested by the plan's checklist). Scenario-level
coverage is instead pinned directly against the plan's Scenario Coverage table below.

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`lakehouse-engine` lib) | 1091 | 1091 | 0 |
| Unit + Integration (full workspace, `cargo test --workspace`) | 1497 | 1497 | 2 (pre-existing, unrelated) |

### Manual Tests

| Test | Result |
|------|--------|
| `SELECT ID, TAGS, ADDR, ATTRS FROM LAKEHOUSE_VS.COMPLEX_PROBE ORDER BY ID` | ✓ — `TAGS`/`ADDR`/`ATTRS` render as valid JSON keyed by logical names; NULL row returns SQL NULL for each |
| `SELECT COLUMN_NAME, COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS WHERE ... COLUMN_TABLE='COMPLEX_PROBE'` | ✓ — every nested column declared `VARCHAR(2000000)` |
| `SELECT NESTED_STRUCT, MAP_COL, ARRAY_COL FROM UNITY_VS.STATS_ALL_TYPES` | ✓ — `NESTED_STRUCT` keyed by `inner_int`/`inner_string`/`inner_double`, no `col-` physical name; `ARRAY_COL` strict JSON; query succeeds where it previously refused |
| `SELECT BINARY_COL FROM UNITY_VS.STATS_ALL_TYPES` (refusal) | ✓ — errors citing issue #351, not #350 |
| `EXPLAIN VIRTUAL SELECT COUNT(*) ... WHERE TAGS = '["hello","world"]'` then unguarded | ✓ — pushdown SQL shows the predicate reaching the scan as a string comparison; query returns the matching row count only |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
exit code 0, 0 warnings
```

### Formatter

```
cargo fmt --all -- --check
exit code 0, no diff
```

### Live Docker Exasol Verification (task 2.16)

All 7 pushdown shapes over a nested column verified correct against `COMPLEX_TYPE_VS.COMPLEX_PROBE`,
each with `EXPLAIN VIRTUAL` evidence:

1. **WHERE**: predicate pushed as a string comparison into the scan; returns only the matching row (not every row, as it did pre-fix).
2. **GROUP BY**: pushed with `group_keys`/`aggregates`; 4 correct groups.
3. **ORDER BY**: correct VARCHAR lexical sort of the rendered JSON text.
4. **Aggregate argument (MAX)**: pushed as a node-local partial max + Exasol-side final max; correct lexical-max result.
5. **COUNT(DISTINCT)**: pushed as a per-shard DISTINCT projection, counted by Exasol's own `COUNT(DISTINCT)`; correct distinct count.
6. **JOIN condition**: nested column as an equi-join key against a native Exasol side table; correct matches, no false positives.
7. **Select-list expression (UPPER)**: pushed as a projected scalar expression; correct upper-cased JSON text.

No pushdown shape misbehaved, so the pushdown-decline mechanism was left untouched — confirming the
plan's core design bet (keeping the logical type `Utf8` everywhere so no pushdown site needs a new
decline) held up live.

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| nested-json-rendering | A list, struct, or map value renders as one valid JSON document | Unit | `crates/lakehouse-engine/src/scan/json_render_tests.rs` | `populated_nested_values_render_as_valid_json_documents` | Pass |
| nested-json-rendering | A null nested value emits SQL NULL, not the text "null" | Unit | `crates/lakehouse-engine/src/scan/json_render_tests.rs` | `null_cells_emit_sql_null_and_null_members_render_explicitly` | Pass |
| nested-json-rendering | A non-string map key is stringified into the JSON object name | Unit | `crates/lakehouse-engine/src/scan/json_render_tests.rs` | `non_utf8_map_keys_stringify_into_object_names` | Pass |
| nested-json-rendering | Rendered field names are the table's logical names, not the file's physical ones | Integration | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `nested_fields_resolve_to_logical_names_across_binding_keys` | Pass |
| nested-json-rendering | A nested physical column is diverted around the physical-to-logical cast | Integration | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `nested_physical_column_bypasses_the_cast_and_yields_utf8` | Pass |
| nested-json-rendering | One encoder serves both the logical-schema path and the legacy inference path | Integration | `crates/lakehouse-engine/src/scan/raw_scan_tests.rs` (relocated — see Notes) | `inferred_schema_path_renders_nested_columns_through_the_same_encoder` | Pass |
| nested-json-rendering | A predicate over a rendered nested column is evaluated, never silently dropped | Integration | `crates/lakehouse-engine/tests/scan_parquet_pruning.rs` | `predicate_over_a_rendered_nested_column_is_applied_not_dropped` | Pass |
| nested-json-rendering | Every pushdown shape treats a nested column as the VARCHAR Exasol declared | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_complex_type_test.rs` | `nested_columns_push_down_as_the_declared_varchar_in_every_shape` | Pass |
| nested-json-rendering | The rendered column crosses the emit boundary as the declared VARCHAR | Integration | `crates/lakehouse-engine/tests/scan_batch_loop.rs` | `rendered_nested_column_passes_the_emit_coercion_unchanged` | Pass |
| type-mapping | Incompatible Arrow types are serialized to JSON VARCHAR | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `nested_and_non_nested_incompatible_halves_are_owned_by_one_predicate_each` | Pass |
| type-mapping | A mixed-column Parquet file round-trips through schema mapping and scan | Integration | `crates/lakehouse-engine/tests/scan_column_binding.rs` | `mixed_column_parquet_file_emits_json_for_populated_list_and_struct` | Pass |
| type-mapping | Iceberg logical schema maps to Arrow types for scan registration | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `nested_iceberg_fields_stay_utf8_tagged_and_carry_a_nested_descriptor` | Pass |
| scan-execution | Incompatible Arrow columns are emitted as JSON strings | Unit | `crates/lakehouse-engine/src/scan/convert_tests.rs` | `incompatible_columns_emit_json_strings` | Pass |
| delta-type-mapping | A Delta type Exasol cannot represent natively is surfaced as a VARCHAR rendering | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `containers_classify_recursively_by_renderability` | Pass |
| delta-type-mapping | A Delta type whose Arrow form cannot be rendered faithfully is refused by name | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `refused_set_is_binary_variant_and_containers_of_them` | Pass |
| delta-type-mapping | A refused column refuses only the requests that read or emit it | Unit | `crates/lakehouse-engine/src/adapter/pushdown/refused_columns_tests.rs` | `only_binary_col_refuses_requests_in_the_stats_all_types_shape` | Pass |
| delta-type-mapping | The castability claims behind the mapping are asserted, not assumed | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `arrow_castability_to_utf8_pins_the_three_delta_type_sets` | Pass |
| delta-type-mapping | Every recorded Delta type change is validated, and an unsupported one refuses its column | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `nested_type_changes_are_validated_and_refuse_with_a_composed_path` | Pass |
| delta-type-mapping | Every nested field's logical name and binding key reach the scan | Unit | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `nested_descriptor_carries_logical_names_and_mode_selected_binding_keys` | Pass |
| e2e-harness | An Iceberg table's list, struct, and map columns return valid JSON end to end | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_complex_type_test.rs` | `iceberg_nested_columns_return_valid_json_end_to_end` | Pass |
| unity-catalog-e2e-harness-delta-queries | A refused column refuses only the queries naming it | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_refused_column_refuses_only_the_queries_naming_it` | Pass |
| unity-catalog-e2e-harness-delta-queries | A Delta table's varied types return their expected Exasol types and values | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_varied_types_return_their_expected_exasol_types_and_values` | Pass |

## Notes

- **Test-location deviation (documented above)**: `inferred_schema_path_renders_nested_columns_through_the_same_encoder`
  was planned to live in `tests/scan_column_binding.rs` (an external integration-test binary) but was
  built instead as a unit test in `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`, because the
  legacy-path UDF registration (`register_nested_json_render_udf`, `NestedJsonRenderUdf`) is
  intentionally crate-private per an earlier code-review fix (`[UNUSED_FUNCTION]`) and unreachable
  from an external test binary. The test still drives both real code paths (physical-plan expression
  substitution and SQL-embedded UDF call) and asserts byte-identical output.
- **Deviation from the plan's pruning-stage expectation**: the plan's Consequences table anticipated
  disabling statistics/page-index/bloom-filter pruning for a nested-carrying column. Live measurement
  (task 2.13) found these stages cannot resolve leaf statistics for a physically-nested Parquet column
  at all — `parquet-rs`'s `parquet_column` returns `None` for nested fields — so no false-exclusion
  hazard is reachable, and disabling the stages would have cost every *primitive* column of a
  nested-carrying table its row-group pruning for no benefit. All three stages were left ON; the test
  (`predicate_over_a_rendered_nested_column_is_applied_not_dropped`) proves the matching row survives,
  and a discriminating control query proves the stage is still live and prunes when it can. The plan's
  `datafusion-scan/nested-json-rendering` spec delta was updated to record this as the actual, verified
  behavior rather than the originally-anticipated disable.
- **A residual coverage gap was found and closed during Phase 5b** (orchestrator-run scenario-coverage
  audit against the plan's table): 3 of 22 named scenarios had no test under any name. All 3 were
  added; see the Scenario Coverage table above (all now Pass).
- **A separate, pre-existing, unrelated bug was found and NOT fixed** (out of scope for this plan):
  a self-join aliases both join legs identically in the generated unaccelerated-join wrapper SQL,
  turning `ON a.col = b.col` into an unconditional cross product. Reproduced identically with a plain
  primitive column (`ON a.ID = b.ID`), confirming it predates and is unrelated to nested-JSON
  rendering. Recommend filing a tracked GitHub issue separately; not addressed here.
- No `make test-e2e` / `make test-e2e-unity` full-suite runs were performed during implementation or
  this verification pass, per the orchestrator's instruction to avoid redundant expensive E2E runs —
  those are deferred to the dedicated test+record gate, which runs the real suites before recording.
  Task 2.16's live verification and the expert-fix agent's targeted `--test e2e_complex_type_test`
  run are the only live-stack executions during implementation, both justified (task 2.16 is the
  plan's own manual-verification task; the targeted run was needed to resolve a review finding that
  branched on live join behavior).

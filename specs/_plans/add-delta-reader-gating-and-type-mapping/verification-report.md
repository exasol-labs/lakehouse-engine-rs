# Verification Report: add-delta-reader-gating-and-type-mapping

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Delta reader-protocol gating and full type mapping shipped; per-column (not per-table) refusal for binary/struct/map/variant; all suites green after two real bugs found and fixed during implementation/review. |
| Code review | 15 findings — standard: 9 (7 fixed, 2 already resolved by the concurrent expert-tier pass), expert: 6 (6 fixed) |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-musl-udf-build`, exit 0) |
| Tests | ✓ (`cargo test --workspace`, 1347 passed, 0 failed, 2 pre-existing ignored micro-benchmarks unrelated to this plan) |
| Lint | ✓ (`cargo clippy --all-targets --workspace`, 0 warnings) |
| Format | ✓ (`cargo fmt --check`, no diff) |
| Scenario Coverage | ✓ (all 19 named scenario tests present and passing) |
| Manual Tests | ✓ (covered by the three live-cluster E2E scenarios below; see Notes) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (workspace) | `cargo test --workspace` | 1347 | 2 (pre-existing `micro_bench.rs`, unrelated) |
| E2E — Unity/Delta | `make test-e2e-unity` (real Exasol + MinIO + Unity Catalog Docker stack) | 20 | 0 |
| E2E — Iceberg regression | `make test-e2e` (real Exasol + MinIO + Iceberg REST Docker stack, spark-fixtures seeded) | 64 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| Unsupported reader feature (`TYPE_WIDENING`/`UNSHREDDED_VARIANT`) refuses loud, names the feature, cites #349, session survives | ✓ — `unity_delta_unsupported_reader_feature_fails_the_query_loud` |
| 13-column `STATS_ALL_TYPES` projection + `SELECT COUNT(*) = 4`, correct Exasol types, non-NULL byte/short, bracketed array rendering | ✓ — `unity_delta_varied_types_return_their_expected_exasol_types_and_values` |
| `binary_col`/`map_col`/`nested_struct`/`SELECT *`/WHERE-on-refused-column all refuse citing #350; mappable projection still succeeds same run | ✓ — `unity_delta_refused_column_refuses_only_the_queries_naming_it` |

## Tool Evidence

### Linter

```
Checking lakehouse-engine v0.36.0 (.../crates/lakehouse-engine)
Finished `dev` profile [unoptimized + debuginfo] target(s) in 12.81s
(0 warnings, 0 errors — cargo clippy --all-targets --workspace)
```

### Formatter

```
cargo fmt --check — exit 0, no diff
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | delta-reader-feature-gating | Reader feature outside allow-list refuses before log replay | `format/delta_protocol_tests.rs` | `a_reader_feature_outside_the_allow_list_is_refused_by_its_protocol_name` | Pass |
| vs-adapter | delta-reader-feature-gating | Reader feature outside allow-list refuses before log replay (integration) | `format/delta_replay_tests.rs` | `an_unsupported_reader_feature_is_refused_before_any_schema_or_file_read` | Pass |
| vs-adapter | delta-reader-feature-gating | Every allow-listed feature keeps its table queryable | `format/delta_replay_tests.rs` | `every_shipped_fixture_whose_reader_features_are_allow_listed_still_resolves` | Pass |
| vs-adapter | delta-reader-feature-gating | Reader version outside readable range refused | `format/delta_protocol_tests.rs` | `a_reader_version_outside_the_kernels_range_is_refused_before_any_feature_check` | Pass |
| vs-adapter | delta-reader-feature-gating | Legacy-protocol table carries no explicit reader-feature list | `format/delta_replay_tests.rs` | `a_legacy_reader_version_table_passes_the_gate_and_keeps_its_column_mapping_mode` | Pass |
| vs-adapter | delta-reader-feature-gating | Gate runs inside snapshot construction; no bypass path | `format/delta_replay_tests.rs` | `the_protocol_gate_runs_inside_snapshot_construction` | Pass |
| vs-adapter | delta-type-mapping | Every natively representable type maps to its own tag | `format/delta_schema_tests.rs` | `every_natively_representable_delta_type_maps_to_its_own_arrow_tag` | Pass |
| vs-adapter | delta-type-mapping | Unrepresentable type surfaced as VARCHAR rendering (incl. recursive array) | `format/delta_schema_tests.rs` | `a_type_exasol_cannot_represent_is_tagged_utf8_including_a_recursive_array` | Pass |
| vs-adapter | delta-type-mapping | `void` reads all-NULL under `name` column mapping | `format/delta_replay_tests.rs` | `a_void_column_reads_as_all_null_under_name_column_mapping` | Pass |
| vs-adapter | delta-type-mapping | List column tagged utf8 cast by the field-id expression adapter | `scan/raw_scan_tests.rs` | `a_list_column_tagged_utf8_is_cast_by_the_field_id_expression_adapter` | Pass |
| vs-adapter | delta-type-mapping | binary/struct/map/variant refused by name, citing #350 | `format/delta_schema_tests.rs` | `binary_struct_map_and_variant_are_refused_with_their_own_reason_citing_350` | Pass |
| vs-adapter | delta-type-mapping | Refused column refuses only requests referencing it | `pushdown/pushdown_tests.rs` | `a_refused_delta_column_refuses_only_the_requests_that_reference_it` | Pass |
| vs-adapter | delta-type-mapping | Refused column reached through a join leg is refused | `pushdown/joins/joins_tests.rs` | `a_refused_delta_column_reached_through_a_join_leg_is_refused` | Pass |
| vs-adapter | delta-type-mapping | Table with no mappable column refused as a whole | `format/delta_schema_tests.rs` | `a_table_whose_every_column_is_refused_is_refused_as_a_whole` | Pass |
| vs-adapter | delta-type-mapping | Castability claims pinned against arrow's own answer | `types/mapping_tests.rs` | `arrow_castability_to_utf8_pins_the_three_delta_type_sets` | Pass |
| vs-adapter | delta-table-planning | Delta reader reached from production pushdown, Unity Catalog kind | `pushdown/pushdown_tests.rs` | `a_unity_catalog_pushdown_gates_the_delta_protocol_and_refuses_per_column` | Pass |
| vs-adapter | pushdown-module-structure | Façade admits exactly one item for the Delta refused-column list | `pushdown_surface_probe_tests.rs` / `tests/pushdown_public_surface.rs` | compile-time `use` probes (26 and 16 items) | Pass |
| e2e | unity-catalog-e2e-harness-delta-queries | Unsupported reader feature fails the query loud | `tests/e2e_unity_test.rs` | `unity_delta_unsupported_reader_feature_fails_the_query_loud` | Pass |
| e2e | unity-catalog-e2e-harness-delta-queries | Varied Delta types return expected Exasol types/values | `tests/e2e_unity_test.rs` | `unity_delta_varied_types_return_their_expected_exasol_types_and_values` | Pass |
| e2e | unity-catalog-e2e-harness-delta-queries | Column this engine cannot render refuses only queries naming it | `tests/e2e_unity_test.rs` | `unity_delta_refused_column_refuses_only_the_queries_naming_it` | Pass |

## Notes

- **Two real production bugs were found and fixed during implementation, not just theorized in review:**
  1. **`COUNT(*)` wrongly refused** — the pushdown gate originally unioned a synthetic "widened to full row" projection (produced for any aggregate/untranslatable select-list item, including bare `COUNT(*)`, which has no column argument) into its touched-columns set, so a table with any refused column made `COUNT(*)` fail even though it reads no column value. Fixed by making the widened-vs-not distinction a first-class `Option<&[ProjectionItem]>` argument rather than a separate boolean, so the invalid combination is unrepresentable. Reproduced RED against the live Unity/Delta stack before fixing, GREEN after (`make test-e2e-unity` 20/20).
  2. **Cross-join-side information leakage** (found during code review, `INFORMATION_LEAKAGE`) — the join-side refusal gate charged a request-global touched-column set to every join side, so a refused column named only in a query against one table's alias could refuse a `SELECT` naming only the *other* side's identically-named mappable column. Fixed by attributing column references to their declaring side (tagged references to their own side; untagged/ambiguous references charged to every side, fail-safe) before intersecting with that side's own refused list.
- **Manual Testing table** (plan.md) specifies raw `exapump sql` commands against a manually created virtual schema. These are not run separately: the three E2E tests listed under Manual Tests above exercise the identical queries against the identical live Unity Catalog/Delta fixtures, with stronger machine-checked assertions (exact error text, session survival, column values, refusal-then-success-on-the-same-connection) than a human reading `exapump` output would apply. Re-running them by hand would duplicate, not add to, this evidence.
- **`make test-e2e-unity` and `make test-e2e` were run against the same Docker host sequentially**, since the base compose stack (`exasol`, `minio`, `iceberg-rest`) and the Unity overlay (`unitycatalog`) both reuse the `exasol`/`minio` services; bringing up the Iceberg-regression stack recreated the `exasol` container, so the Unity stack (`make unity-up`, including its Delta fixture seed) was brought back up and `test-e2e-unity` re-run afterward to produce final, current evidence — both suites' logged results above are from that final sequential run, not stale runs from mid-implementation.
- **2 ignored tests** in the full `cargo test --workspace` run are pre-existing `#[ignore]`-tagged entries in `micro_bench.rs`, untouched by this plan.
- **Non-goals confirmed unaffected:** No Iceberg behavior changed (its `refused_columns` list stays permanently empty, short-circuiting the gate). No `ScanSpec` wire field was added — confirmed by the unchanged golden pushdown-dispatch test suite (`dispatch_golden_tests.rs`, updated only for the new `RefusedColumn`/plumbing struct-literal fields, not a wire-format change).

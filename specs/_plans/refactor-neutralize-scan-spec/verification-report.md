# Verification Report: refactor-neutralize-scan-spec

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `ScanSpec` fully neutralized (one `DeleteMechanism` list, one binding key per logical field, neutral partition fields); every Iceberg wire encoding and regression-gate assertion byte-identical/unedited; Delta `id`/`name`/`none` column mapping expressible end to end; 1247 host tests + 266 E2E tests (Iceberg + Unity) all pass, 0 failures |
| Code review | 8 findings — standard: 7, expert: 1 — all 8 fixed |

| Check | Status |
|-------|--------|
| Build (`.so`, `make cross-musl-udf-build`) | ✓ |
| Tests (host, `cargo test --workspace`) | ✓ |
| Tests (Iceberg E2E, `make test-e2e`) | ✓ |
| Tests (Unity E2E, `make test-e2e-unity`) | ✓ |
| Lint (`cargo clippy --workspace --all-targets`) | ✓ |
| Format (`cargo fmt --all -- --check`) | ✓ |
| Spec validation (`speq plan validate`) | ✓ (style warnings only, no errors) |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (host, `cargo test --workspace`) | 1249 | 1247 | 2 |
| Integration (Iceberg E2E, `make test-e2e`, 9 binaries) | 254 | 254 | 0 |
| Integration (Unity E2E, `make test-e2e-unity`) | 12 | 12 | 0 |

The 2 ignored tests are pre-existing (unrelated to this plan; not gated by the plan's checklist).

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine scan::spec` | ✓ Round-trip, mixed-mechanism refusal, byte-stability tests pass; `common_blob_wire_is_byte_stable` unedited |
| `cargo test -p lakehouse-engine --test scan_column_binding --test scan_name_mapping` | ✓ Both binding-strategy tests pass; the two name-mapping tests pass unedited |
| `cargo test -p lakehouse-engine --test scan_positional_deletes` | ✓ 21 passed, 0 failed; equality-delete/Puffin refusal messages unchanged |
| `cargo test -p lakehouse-engine adapter::pushdown::format` | ✓ Replay and schema tests pass; each Delta mode reports exactly one binding key per column |
| `make unity-up && cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test -- --test-threads=1` | ✓ 12/12 pass, including `unity_delta_planning_agrees_under_vended_and_static_credentials`; no credential value in output |
| `cargo test -p lakehouse-catalog --test catalog_public_surface` | ✓ Compiles and passes; `grep -r DeleteMechanism crates/lakehouse-catalog` returns nothing |
| Iceberg regression gate (`scan_two_arg`, `scan_plan_shape`, `scan_no_head_test`) | ✓ 0 failures; no assertion, expected value, or golden edited |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets
Finished `dev` profile [unoptimized + debuginfo] target(s)
(0 warnings, 0 errors)
```

### Formatter

```
cargo fmt --all -- --check
(no output — clean)
```
Two stray formatting violations surfaced mid-implementation (one in `file_resolution.rs` from task 2.5, one across several task-2.11 test files' import lists) were fixed mechanically by the orchestrator via `cargo fmt --all` — neither reflected a design decision.

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | scan-execution-spec-reconstitution | Reconstitution carries per-file positional-delete references | `crates/lakehouse-engine/tests/scan_two_arg.rs` | existing delete-carrying test | Pass |
| datafusion-scan | scan-execution-spec-reconstitution | Reconstitution carries neutral partition values and a neutral delete mechanism list | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `neutral_file_entries_round_trip_losslessly_and_leave_iceberg_encodings_byte_identical` | Pass |
| datafusion-scan | scan-execution-spec-reconstitution | (same) | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `partition_values_distinguish_an_explicit_null_from_an_absent_column` | Pass |
| datafusion-scan | scan-execution-spec-reconstitution | (same) | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `a_file_entry_mixing_a_deletion_vector_with_an_iceberg_delete_is_refused` | Pass |
| datafusion-scan | scan-execution-spec-reconstitution | (same) | `crates/lakehouse-engine/src/scan/spec_tests.rs` | `common_blob_wire_is_byte_stable` (unedited) | Pass |
| datafusion-scan | scan-execution-field-id-projection | Column projection binds by a logical field's declared physical name | `crates/lakehouse-engine/tests/scan_column_binding.rs` | `declared_physical_name_binds_the_renamed_physical_column` | Pass |
| datafusion-scan | scan-execution-field-id-projection | (same) | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `declared_physical_name_wins_over_a_covering_name_mapping_entry` | Pass |
| datafusion-scan | scan-execution-field-id-projection | A logical field carrying no binding key binds by its own name | `crates/lakehouse-engine/tests/scan_column_binding.rs` | `identity_bound_fields_bind_by_name_and_keep_the_default_fill_semantics` | Pass |
| datafusion-scan | scan-execution-field-id-projection | (same) | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `identity_bound_logical_field_carries_no_parquet_field_id_metadata` | Pass |
| datafusion-scan | scan-execution-field-id-projection | Scan without a logical schema falls back to first-file inference | `crates/lakehouse-engine/src/scan/raw_scan_tests.rs` | `a_logical_schema_of_identity_fields_still_installs_the_binding_adapter` | Pass |
| datafusion-scan | scan-execution-positional-deletes | An unapplicable delete file is rejected with a clean error (read-time backstop) | `crates/lakehouse-engine/src/scan/positional_deletes_tests.rs` | `backstop_rejects_every_unappliable_delete_mechanism` | Pass |
| datafusion-scan | scan-execution-positional-deletes | (same) | `crates/lakehouse-engine/tests/scan_positional_deletes.rs` | existing equality-delete/Puffin rejection tests | Pass |
| vs-adapter | delta-table-planning | Partition values are carried per data file, including a NULL partition value | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `replay_carries_partition_values_and_an_explicit_null` | Pass |
| vs-adapter | delta-table-planning | (same) | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `partition_columns_are_threaded_through_verbatim_and_in_order` | Pass |
| vs-adapter | delta-table-planning | A data file's deletion vector reference is carried verbatim exactly once | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `replay_carries_a_readded_files_deletion_vector_exactly_once` | Pass |
| vs-adapter | delta-table-planning | (same) | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `replay_carries_no_iceberg_delete_reference_on_any_entry` (renamed from `replay_leaves_the_iceberg_delete_list_empty_on_every_entry`) | Pass |
| vs-adapter | delta-table-planning | (same) | `crates/lakehouse-engine/src/adapter/pushdown/file_resolution_tests.rs` | `relativization_leaves_a_deletion_vectors_path_or_inline_dv_untouched` | Pass |
| vs-adapter | delta-table-planning | Each logical field carries the binding key its column-mapping mode selects | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `each_column_mapping_mode_selects_its_own_binding_key` | Pass |
| vs-adapter | delta-table-planning | (same) | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `id_mode_column_without_a_column_mapping_id_is_refused_naming_the_column` (unedited assertions) | Pass |
| vs-adapter | delta-table-planning | (same) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_planning_agrees_under_vended_and_static_credentials` | Pass |
| vs-adapter | delta-table-planning | Iceberg planning is byte-identical through the new seam | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `iceberg_reader_returns_empty_partition_columns_and_field_id_bound_logical_fields` | Pass |
| vs-adapter | delta-table-planning | (same) | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | existing suite, construction shapes only | Pass |
| vs-adapter | delta-table-planning | (same) | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | existing suite, every golden unedited | Pass |
| vs-adapter | catalog-crate-structure | The catalog access layer lives in a standalone crate the engine depends on one way | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | existing probe; no catalog source names `DeleteMechanism` | Pass |

## Notes

**Scope note (orchestrator-discovered gaps, absorbed into the plan's own tasks rather than left as post-hoc patches):**
- Task 2.5's scope grew to include a `DeleteMechanism::object_store_path` accessor in `scan/spec.rs` — no original task named it, but `store_router.rs` and `object_store.rs` needed the same "a deletion vector's reference is not an object-store path" knowledge that `relativize_shards_to_root` required. One accessor, three call sites, one owner.
- A second gap (`build_logical_schema` in `adapter/pushdown/mod.rs` constructing `LogicalField` for Iceberg fields) surfaced only once task 2.5 forced full-crate compilation; fixed as task 2.5b.
- Task 2.5's implementer-expert-agent crashed on an API error immediately after completing its work (before its final report). The orchestrator independently verified completeness via `cargo check` error-attribution (zero errors across all 6 of its files) and confirmed the required scenario test existed, before marking the task done.

**Behavior correction, not a regression:** task 2.3's single-pass binding rewrite fixed two latent defects in the old `rename_physical_to_logical`/`resolved_logical_field_ids` pair — a column bound via the physical-name fallback, or one whose physical counterpart carried an embedded field-id unknown to the logical schema, used to be wrongly treated as "absent" and default/NULL-filled instead of reading real data. No existing test or recorded scenario pinned the old (wrong) behavior, and Iceberg output is provably unchanged (every Iceberg logical field always carries `field_id`, so neither corrected path is ever reached on the Iceberg read path). The code-review's one expert finding required — and now has — a test driving both corrected paths through the actual rewritten expression (`present_field_binds_real_value_not_default`), not just the intermediate binding set, verified via an explicit RED→GREEN probe against the pre-fix binding logic.

**Spec validation warnings** (non-blocking, pre-existing style guidance): several recorded scenarios have 4-9 AND-steps versus the CLI's recommended ≤3; these are unchanged from planning and not part of this plan's scope to restructure.

**Known scope exclusions per the plan's Non-Goals** (unaffected by this refactor, confirmed unchanged): applying a deletion vector or materializing a partition column at scan time (#320); Delta pushdown parity and stats pruning (#321); Delta reader-feature gating and broad type mapping (#322); no Delta path is wired into `handle_pushdown` in production.

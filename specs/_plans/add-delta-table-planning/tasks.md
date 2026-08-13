# Tasks: add-delta-table-planning

## Phase 2: Implementation (Group A)
- [x] 2.1 Add `delta_kernel` 0.26 + `delta_kernel_default_engine` 0.26 deps to `crates/lakehouse-engine/Cargo.toml`; confirm one `arrow`/`object_store` version and `.so` build
- [x] 2.2 Add `TableFormat` enum + `format`/`vended_credential_key` fields to `CatalogTable`; map Unity `data_source_format`; update `catalog_public_surface.rs` [expert]
- [x] 2.3 Add Delta wire types to `scan/spec.rs` (table block, per-file block, `CommonScanSpec.delta`, `FileEntry.delta`, JSON-OBJECT wire variant); prove lossless round-trip + Iceberg byte-identity [expert]
- [x] 2.4 Extract undecorated `StorageBackend` → `Arc<dyn ObjectStore>` builder out of `build_side_store` in `scan/object_store.rs`; preserve S3 `with_client_options`-before-`with_allow_http` ordering

## Phase 2: Implementation (Group B)
- [x] 2.5 Create `adapter::pushdown::format` submodule: `FormatReader` trait, `ResolvedScan`, `ScanSource`, `format_reader` dispatch, `IcebergFormatReader`; update pushdown surface probes (21→25 in-crate, 11→15 external) [expert]
- [x] 2.6 Implement Delta log replay submodule: current version resolution, JSON commit + checkpoint replay, one `FileEntry` per active file, re-add dedup for deletion vectors, NULL default-partition mapping [expert]
- [x] 2.7 Implement Delta schema submodule: ordered `LogicalField` list, field-id from column mapping or ordinal, Delta table block, type mapping with `UdfError` for unmapped types (cite #322)

## Phase 2: Implementation (Group C)
- [x] 2.8 Implement `DeltaFormatReader` composing tasks 2.4/2.6/2.7: storage-location validation before vended/static split, vending-key requirement, `resolve_uc_vended_storage`, redaction of secrets in errors [expert]

## Phase 2: Implementation (Group D)
- [x] 2.9 Add offline integration test `tests/delta_log_replay.rs` against vendored fixtures (`basic_partitioned`, `table-with-dv-small`, `cdf-column-mapping-name-mode`, `stats-all-types`) — resolved as crate-internal unit tests, not the literal external file; see report for 2.9
- [x] 2.10 Add live-stack test to `tests/e2e_unity_test.rs` under `unity-e2e` feature: vended vs static credential agreement, deletion-vector assertion; fail (never skip) when stack unreachable

## Phase 2: Implementation (Group E)
- [x] 2.11 Verify scope boundary: `unity_kind_pushdown_is_refused_not_iceberg_routed` passes unedited, `handle_pushdown` names nothing new, `CatalogKind` probe unweakened
- [x] 2.12 Run Iceberg characterization gate: `scan_two_arg.rs`, `scan_plan_shape.rs`, `dispatch_golden_tests.rs` pass with no edits

## Phase 4: Review Fixes (Expert)
- [x] 4.1 Make `delta_schema.rs`'s `field_id` and `physical_name` fallible and mode-aware: refuse with a `UdfError::User` naming the column, the mode and the annotation under `Id`/`Name` mode when the column-mapping id is absent or does not fit `i32` or the physical name is absent/not a string; keep the 1-based ordinal id and logical physical name under `None` mode; rewrite `explicit_column_mapping_id_wins_over_ordinal_position_when_present` and add `id_mode_column_without_a_column_mapping_id_is_refused_naming_the_column` + `id_mode_column_without_a_physical_name_is_refused_naming_the_column` [expert]
- [x] 4.2 Add `redacted_masks_every_effective_storage_secret_in_a_raised_error` and `a_failed_log_read_reports_no_static_credential_value` to `delta_format_reader_tests.rs`, exercising `redacted` directly and `read_delta_log` end to end against a closed port [expert]
- [x] 4.3 Make `ScanSpec::files_from_json` reject an entry carrying both a `delta` block and a non-empty `deletes` list, naming the entry index and echoing neither path nor raw input; drop that combination from `every_file_entry_combination()` and add `a_file_entry_carrying_both_a_delta_block_and_iceberg_deletes_is_refused` [expert]
- [x] 4.4 Strengthen `unity_delta_planning_agrees_under_vended_and_static_credentials` to assert every file carries a `delta` block with exactly one `"letter"` partition entry, that the path-sorted values equal `[None, a, a, b, c, e]`, and that no value is the `__HIVE_DEFAULT_PARTITION__` literal; narrow the agreement assertion's message to agreement alone [expert]

## Phase 4: Review Fixes (Standard)
- [x] 4.5 Delete `dotted_identifier` in `format/mod.rs`; use `crate::adapter::tables::catalog_identifier_string` in `format_reader`'s non-Delta refusal and in `DeltaFormatReader::table_name`
- [x] 4.6 Rewrite `build_delta_table_schema`'s `column_mapping_mode` doc sentence in `delta_schema.rs` to state the argument is the protocol-gated mode already in force (from `DeltaSnapshot::column_mapping_mode`), not the raw `delta.columnMapping.mode` property
- [x] 4.7 Change `build_delta_table_schema`'s second parameter and `wire_column_mapping_mode`'s parameter to bare `ColumnMappingMode` (drop the `Option`/`None` arm) in `delta_schema.rs`; update the `read_delta_log` call site and every `delta_schema_tests.rs` call site; delete `explicit_column_mapping_mode_property_none_also_yields_wire_mode_none`
- [x] 4.8 Delete `unmapped_delta_type_does_not_emit_a_logical_field_for_any_column` from `delta_schema_tests.rs`
- [x] 4.9 Wrap the `build_table_root_store(...)` call in `read_delta_log` (`delta_format_reader.rs`) with `.map_err(|error| redacted(error, secrets))`
- [x] 4.10 Move the `name_mapping`-empty rationale out of the inline comment inside the `ResolvedScan { .. }` literal in `delta_format_reader.rs` into the doc comment on `impl FormatReader for DeltaFormatReader::resolve_scan`
- [x] 4.11 Move the content of all eight inline `delta_kernel` contract comments in `delta_replay.rs` into the module-level `//!` doc block as a "delta_kernel scan-row contract" paragraph list; delete the inline comments
- [x] 4.12 Add `ConnectionStorage<'a>` struct (storage/creds/allow_http) in `format/mod.rs`; change `format_reader` and `DeltaFormatReader::new` to take `&ConnectionStorage<'a>`; give `IcebergFormatReader` a single `connection` field; update all call sites (`format_tests.rs`, `delta_format_reader_tests.rs`, `iceberg_tests.rs`, `e2e_unity_test.rs`) and the two surface-probe `use` lists + doc counts
- [x] 4.13 Rewrite `TableInfo`'s doc comment in `unity/client.rs` to drop the false `full_name`-exclusivity claim and give `table_id` its own defensive-tolerance justification; extend `DELTA_DATA_SOURCE_FORMAT`'s doc to name both the listing-admission and single-table-load uses
- [x] 4.14 Normalise empty/whitespace-only `data_source_format` in `neutral_table_format` (`unity/client.rs`) to render through `ABSENT_DATA_SOURCE_FORMAT`; add an `""` row to the parameterised table in `unity/client_tests.rs`
- [x] 4.15 Change the `vended_credential_key` projection in `neutral_table` (`unity/client.rs`) to a trim-aware emptiness check; update both docs; add `a_whitespace_only_table_id_projects_to_an_absent_vending_key` test
- [x] 4.16 Delete `neutral_table_carries_the_format_tag_and_the_opaque_vending_key` and `neutral_table_format_maps_the_uppercase_unity_vocabulary` from `unity/client_tests.rs`
- [x] 4.17 Rewrite the stale "static-key on the OSS server" wording in `e2e_unity_test.rs`, `docker-compose.unity.yml`, and `scripts/unity/README.md` to describe the minted MinIO STS session; verify `Makefile` and `lakehouse-catalog/src/unity/vended.rs` (already touched by the expert-fix pass) before editing them
- [x] 4.18 Replace `delta_static_storage`'s hand-built `StorageBackend::S3` body in `e2e_unity_test.rs` with `lakehouse_engine::adapter::connection::storage_block(&delta_creds(false), true)`

## Phase 3: Verification
- [x] 3.1 Run automated checks (build, test, test-e2e-unity, test-e2e, clippy, fmt, speq plan validate) — all green; see verification-report.md
- [x] 3.2 Scenario coverage audit against plan's Verification > Scenario Coverage table — all 21 scenarios covered; corrected the pushdown-module-structure spec delta's 25/15 item counts to 26/16 (ConnectionStorage, a review fix) — see verification-report.md
- [x] 3.3 Manual verification per plan's Verification > Manual Testing table — all commands run against real infra; see verification-report.md

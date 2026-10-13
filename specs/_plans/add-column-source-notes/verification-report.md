# Verification Report: add-column-source-notes

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Every group (A to E, tasks 1.1 to 3.9) is implemented. Host tests, clippy, and format pass, and the local, Unity, Glue, and Lakekeeper E2E suites passed on the implementation before the review fixes. |
| Code review | 36 findings (36 standard, 0 expert) — 36 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (through the E2E suites) |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | not measured |
| Integration | not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Host (`cargo test --workspace`, after the review fixes) | 1464 engine lib tests, 226 catalog tests, other binaries | all, 0 failed | 2 micro-benchmarks |
| E2E local Docker (`make test-e2e`, after the review fixes; includes `make cross-udf-build`) | 18 suites | all, 0 failed | 0 |
| E2E Unity (`make test-e2e-unity`, after the review fixes) | 38 | 38 | 0 |
| E2E Glue (`make test-e2e-glue`, before the review fixes) | 20 | 20 | 0 |
| E2E Lakekeeper (`make test-e2e-lakekeeper`, before the review fixes) | 49 | 49 | 0 |

The Glue and Lakekeeper suites were not re-run after the review fixes. The user chose to leave the full suites to the CI pipeline.

### Manual Tests

The plan's manual steps are covered by the E2E tests named in Scenario Coverage. They were not run as separate exapump commands.

| Test | Result |
|------|--------|
| Column Source Notes, Direct-Storage Table Planning, Direct-Storage Hive Partitioning | ✓ (`e2e_direct_storage_test.rs`) |
| Pushdown File Resolution, Pushdown File Pruning (Iceberg rename, rename swap) | ✓ (`e2e_refresh_test.rs`) |
| Unity Catalog Create Virtual Schema, Parquet and Delta planning | ✓ (`e2e_unity_test.rs`) |
| Glue Table Planning | ✓ (`e2e_glue_test.rs`, run before the review fixes) |

## Tool Evidence

### Linter

```
cargo clippy --all-targets: exit 0, no warnings from this workspace
(only cargo's "failed to auto-clean cache data" registry permission notice)
The review-fix pass also ran it with unity-e2e, glue-e2e, exasol-e2e, and lakekeeper-e2e: clean
```

### Formatter

```
cargo fmt --all -- --check: exit 0, no changes
```

### Line delta

Production lines (`crates/*/src/**`, excluding `*_tests.rs`, new files counted through `git add -N`), against base `af91305`: Phase 1 +85, Phase 2 -3, Phase 3 +111 (before the review fixes), cumulative +221 after the review fixes (+1090 / -869). The positive delta is an accepted exception (decision [13]).

## Scenario Coverage

All 68 test names of the plan's Scenario Coverage table exist in the tree. The Glue suite runs `glue_columns_persist_source_notes` as the named check `check_glue_columns_persist_source_notes` inside its one end-to-end test.

| Scenario | Test Type | Test Location | Test Name | Passes |
|----------|-----------|---------------|-----------|--------|
| Every declared column carries its source shape in its adapterNotes | E2E | `tests/e2e_direct_storage_test.rs`, `tests/e2e_refresh_test.rs`, `tests/e2e_unity_test.rs`, `tests/e2e_glue_test.rs` | `direct_storage_columns_persist_source_notes`, `iceberg_columns_persist_source_notes`, `unity_delta_and_parquet_columns_persist_source_notes`, `glue_columns_persist_source_notes` | Pass |
| A partition column's note carries its position among the partition columns | E2E | `tests/e2e_direct_storage_test.rs`, `tests/e2e_unity_test.rs` | `partition_column_notes_carry_their_position`, `unity_parquet_table_is_listed_and_returns_its_rows_and_partition_values` | Pass |
| A refused column's note records its refusal and the column stays declared | E2E | `tests/e2e_scan_test.rs`, `tests/e2e_direct_storage_test.rs` | `iceberg_binary_column_note_records_the_refusal`, `direct_storage_binary_column_note_records_the_refusal` | Pass |
| Pushdown takes the declared schema from the column notes for every kind | E2E + Integration | `tests/e2e_direct_storage_test.rs`, `src/adapter/pushdown/scan_resolution_tests.rs` | `pushdown_plans_from_column_notes_without_reading_a_footer`, `resolver_assembles_the_declared_schema_from_each_tables_notes` | Pass |
| A column without a readable note fails the pushdown naming the fix | Unit | `src/adapter/column_notes_tests.rs`, `src/adapter/pushdown/pushdown_tests.rs` | `missing_or_unreadable_note_names_table_column_and_refresh`, `pushdown_with_a_note_less_column_fails_without_fallback` | Pass |
| Refresh and set properties rebuild every column note from the source | E2E | `tests/e2e_refresh_test.rs` | `refresh_and_set_properties_rebuild_column_notes_from_the_source` | Pass |
| A source change after the last REFRESH plans against the declaration | E2E | `tests/e2e_direct_storage_test.rs` | `source_change_after_refresh_plans_against_the_declaration` | Pass |
| A column note longer than the per-column limit fails the statement | E2E + Unit | `tests/e2e_direct_storage_test.rs`, `src/adapter/column_notes_tests.rs` | `over_limit_column_note_fails_create_and_refresh`, `encoder_refuses_a_note_over_the_limit_naming_table_and_column` | Pass |
| Refresh re-enumerates the namespace and returns a refresh response | E2E | `tests/e2e_refresh_test.rs` | `refresh_reenumerates_namespace` | Pass |
| The format reader is selected at the same one site for a third source | Unit | `src/adapter/pushdown/format/format_tests.rs` | `direct_parquet_source_carries_only_store_and_table_root` | Pass |
| A direct-storage table resolves its files and schema through the shared seam | Integration | `src/adapter/pushdown/format/parquet_format_reader_tests.rs` | `direct_storage_scan_takes_its_schema_from_notes_and_files_from_the_listing` | Pass |
| A direct-storage pushdown plans without reading a footer | E2E | `tests/e2e_direct_storage_test.rs` | `pushdown_plans_from_column_notes_without_reading_a_footer` | Pass |
| A key=value directory segment declares a VARCHAR partition column | E2E | `tests/e2e_direct_storage_test.rs` | `hive_segments_declare_varchar_partition_columns_with_decoded_values` | Pass |
| A table's partition columns are the union of its files' keys | E2E | `tests/e2e_direct_storage_test.rs` | `mixed_layout_unions_partition_keys_and_nulls_the_missing_key` | Pass |
| Two spellings of one partition key fail the refresh and the query | E2E | `tests/e2e_direct_storage_test.rs` | `two_spellings_of_one_partition_key_fail_refresh_and_query` | Pass |
| A partition key matches its declared column across letter case | E2E | `tests/e2e_direct_storage_test.rs` | `partition_key_matches_its_declared_column_across_letter_case` | Pass |
| A predicate on partition columns prunes files before their footers are read | E2E | `tests/e2e_direct_storage_test.rs`, `tests/e2e_pruning_test.rs` | `partition_filter_prunes_the_resolved_file_list`, `hive_partition_pruning_keeps_every_file_with_a_matching_row` | Pass |
| A declared column absent from every kept file reads NULL | E2E | `tests/e2e_direct_storage_test.rs` | `a_column_only_pruned_files_carry_reads_null` | Pass |
| One seam answers the file list and the folded schema for enumeration | Integration | `src/adapter/parquet_directory_tests.rs` | `schema_answer_lists_every_file_and_folds_without_a_keep_predicate` | Pass |
| The merge mode selects every footer or exactly one | Integration | `src/adapter/parquet_directory_tests.rs` | `merge_mode_reads_every_footer_or_exactly_the_first` | Pass |
| A file-keep predicate narrows the files before any footer is read | Integration | `src/adapter/parquet_directory_tests.rs` | `listing_answer_keep_predicate_narrows_files_without_a_footer_read` | Pass |
| The listing answer serves a caller that declares its own partition columns | Integration | `src/adapter/parquet_directory_tests.rs` | `listing_answer_fills_caller_declared_partition_columns_and_reads_no_footer` | Pass |
| Two spellings of one declared partition key fail the listing answer | Integration | `src/adapter/parquet_directory_tests.rs` | `listing_answer_fails_on_two_spellings_of_a_declared_key` | Pass |
| MERGE_SCHEMA selects one footer or every footer at enumeration | E2E + Integration | `tests/e2e_direct_storage_test.rs`, `src/adapter/direct_storage_tests.rs` | `merge_schema_false_declares_the_narrow_sampled_type_and_refuses_a_wider_file_column`, `merge_schema_selects_footers_at_enumeration_only` | Pass |
| HIVE_PARTITIONING decides the declared partition columns at enumeration | E2E | `tests/e2e_direct_storage_test.rs` | `hive_partitioning_false_declares_no_partition_columns` | Pass |
| A table's columns and data files come from the one shared directory seam | Integration | `src/adapter/direct_storage_tests.rs` | `columns_carry_tag_and_declaration_from_one_fold` | Pass |
| The neutral column's declaration slot extends the crate's public surface through an explicit reviewed edit | Unit (compile-time probe) | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `catalog_column_declaration_slot_is_reachable` | Pass |
| The Iceberg column source carries the whole Iceberg field through an explicit reviewed edit | Unit (compile-time probe) | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `iceberg_column_source_carries_the_field` | Pass |
| The neutral table's catalog properties extend the crate's public surface through an explicit reviewed edit | Unit (compile-time probe) | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `catalog_table_properties_are_reachable` | Pass |
| The column declaration entry point replaces the Iceberg logical-schema builder on the pushdown façade | Unit (compile-time probe) | `src/adapter/pushdown_surface_probe_tests.rs`, `tests/pushdown_public_surface.rs` | probe item lists and counts | Pass |
| Every pushdown request shape resolves through the one format-reader seam | Integration + E2E | `src/adapter/pushdown/pushdown_tests.rs`, `src/adapter/pushdown/scan_resolution_tests.rs`, `tests/e2e_unity_test.rs` | `every_request_shape_resolves_through_the_format_reader_seam`, `a_refusing_declaration_fails_before_any_reader_loads_its_table_under_every_kind`, `unity_delta_join_and_aggregate_pushdown_return_correct_rows` | Pass |
| Pushdown resolves the file list once and builds a scan-driving query | Unit | `src/adapter/pushdown/support_tests.rs` | `pushdown_resolves_files_once_builds_scan_sql` | Pass |
| A column renamed at the source after the last REFRESH reads under its declared name | E2E | `tests/e2e_refresh_test.rs` | `renamed_iceberg_column_reads_under_its_declared_name` | Pass |
| A column dropped at the source after the last REFRESH reads what older data files store | E2E | `tests/e2e_refresh_test.rs` | `dropped_iceberg_column_reads_what_older_files_store` | Pass |
| A declared required column that the current schema marks optional fails the query | E2E | `tests/e2e_refresh_test.rs` | `iceberg_column_made_optional_after_refresh_fails_until_refresh` | Pass |
| A filter column resolves through its declared field id | E2E + Unit | `tests/e2e_refresh_test.rs`, `src/adapter/iceberg_predicate_tests.rs` | `filter_on_rename_swapped_iceberg_columns_prunes_by_declared_field_id`, `filter_column_resolves_through_its_declared_field_id` | Pass |
| A Glue Iceberg table is planned from its metadata location by the one Iceberg planner | Integration + E2E | `src/adapter/pushdown/format/iceberg_tests.rs`, `tests/e2e_glue_test.rs` | `a_glue_iceberg_table_is_planned_from_its_metadata_file`, `glue_columns_persist_source_notes` | Pass |
| A Glue Parquet table is planned by the shared catalog-declared Parquet reader | Integration | `src/adapter/pushdown/format/catalog_parquet_format_reader_tests.rs` | `glue_parquet_table_is_planned_by_the_shared_reader_with_nullable_catalog_columns` | Pass |
| The logical schema is the catalog's declared column list | E2E | `tests/e2e_unity_test.rs` | `unity_parquet_all_types_declare_and_return_their_mapped_values` | Pass |
| A Unity Parquet column with no usable type descriptor is refused, and nullability always follows the file | Unit | `src/adapter/pushdown/format/catalog_parquet_format_reader_tests.rs` | `declaration_refuses_a_column_without_a_usable_type_json_and_forces_nullable` | Pass |
| Partition columns come from the catalog and partition values from the file paths | Integration | `src/adapter/pushdown/format/catalog_parquet_format_reader_tests.rs` | `partition_columns_come_from_partition_index_and_values_from_paths` | Pass |
| The reader never infers partition columns from paths or partition metadata logging, and fails a plan whose partition column is refused | Integration | `src/adapter/pushdown/format/catalog_parquet_format_reader_tests.rs` | `refused_partition_column_note_fails_the_plan` | Pass |
| Enumeration names tables through the shared fold and stops at catalog metadata | E2E | `tests/e2e_unity_test.rs` | `unity_create_declares_a_delta_table_whose_location_holds_no_log` | Pass |
| Create virtual schema declares each Delta column from its type_json and the table's column-mapping mode property | E2E | `tests/e2e_unity_test.rs` | `unity_create_declares_delta_columns_from_catalog_metadata` | Pass |
| A Delta column whose column-mapping metadata is unusable is refused in its note and the table stays listed | E2E + Unit | `tests/e2e_unity_test.rs`, `src/adapter/pushdown/format/delta_schema_tests.rs` | `unity_unusable_column_mapping_metadata_refuses_only_its_column`, `unusable_annotation_or_mode_refuses_the_column` | Pass |
| The client carries each table's properties verbatim from the list sweep and the single-table load | E2E | `tests/e2e_unity_test.rs` | `unity_session_carries_table_properties_from_list_and_load` | Pass |
| Each logical field carries the binding key its column-mapping mode selects | E2E + Unit | `tests/e2e_unity_test.rs`, `src/adapter/pushdown/format/delta_schema_tests.rs` | `unity_delta_column_mapped_tables_return_logical_column_values`, `declaration_carries_the_mode_selected_key_from_type_json` | Pass |
| A declaration that disagrees with the Delta log fails the query | E2E + Integration | `tests/e2e_unity_test.rs`, `src/adapter/pushdown/format/delta_format_reader_tests.rs` | `unity_delta_query_fails_when_the_catalog_copy_disagrees_with_the_log`, `declaration_check_refuses_a_mode_or_partition_mismatch` | Pass |
| Every recorded Delta type change is validated, and an unsupported one refuses its column | E2E + Unit | `tests/e2e_unity_test.rs`, `src/adapter/pushdown/format/delta_schema_tests.rs` | `unity_unsupported_type_change_in_type_json_refuses_its_column`, `a_field_carrying_an_unsupported_recorded_type_change_is_refused_naming_both_types`, `malformed_type_change_annotation_refuses_its_column` | Pass |
| A declared non-nullable column that the Delta log marks nullable fails the query | E2E + Integration | `tests/e2e_unity_test.rs`, `src/adapter/pushdown/format/delta_format_reader_tests.rs` | `unity_delta_non_nullable_declaration_of_a_nullable_column_fails`, `declaration_check_refuses_a_non_nullable_declaration_of_a_nullable_field` | Pass |
| A filter column resolves through its declared binding key | Integration | `src/adapter/pushdown/format/delta_format_reader_tests.rs` | `filter_resolves_through_the_declared_physical_name_after_a_rename_swap` | Pass |

## Notes

- Iceberg filters bind through the scanned snapshot's schema, not the current schema (the `iceberg` 0.10.0 library binds the predicate against it). The spec deltas are worded accordingly.
- Two refusals (no mappable column, refused partition column) now name the virtual table instead of a format label, because the notes parse runs before the format is known.
- `adapter_detects_container_cpuset` passes only with `LH_EXASOL_CPUSET=0-2` on a 4-core host.
- The Phase 1 and cumulative line-delta exceptions are recorded in decision [13]. The PR does not meet issue #426 acceptance criterion 4.

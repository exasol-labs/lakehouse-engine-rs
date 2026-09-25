# Tasks: add-unity-parquet-table-routing

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: Catalog admission and neutral metadata)
- [x] 1.1 ColumnInfo type_json/partition_index fields, neutral_column fill
- [x] 1.2 CatalogTable.partition_columns, neutral_table fill, census update
- [x] 1.3 Single admission function (Delta/Parquet), format tagging
- [x] 1.4 Catalog test renames/additions per plan
- [x] 1.5 unity_schema_tests.rs additions

## Phase 2: Implementation (Group D: Scan-side case fold and type admission)
- [x] 4.1 claim_logical identity uppercase fold, ambiguous-fold failure
- [x] 4.2 FieldIdExprAdapterFactory::create admission/refusal, rewrite-time failure [expert]
- [x] 4.3 field_id_projection_tests.rs / type_relaxation_tests.rs new tests
- [x] 4.4 e2e_direct_storage_test.rs rename + assertions, docs/catalogs.md update
- [x] 4.5 cargo test, docker compose + make test-e2e (no widening to pass)

## Phase 2: Implementation (Group B: Unity Parquet reader and dispatch)
- [x] 2.1 parquet_directory.rs list_parquet_files
- [x] 2.2 Extract UnityTableStorage [expert]
- [x] 2.3 UnityParquetFormatReader [expert]
- [x] 2.4 Rename ScanSource::UnityDelta -> Unity, exhaustive format_reader dispatch
- [x] 2.5 test_support_tests.rs generic prefix listing, unity_parquet_format_reader_tests.rs
- [x] 2.6 docs/catalogs.md Unity section update

## Phase 2: Implementation (Group C: Unity Parquet E2E coverage)
- [x] 3.1 unity-e2e feature gates, sales_parquet fixtures + Unity Catalog registration
- [x] 3.2 resolve_unity_scan + new E2E tests
- [x] 3.3 make unity-up && make test-e2e-unity, all pass

## Phase 3: Verification
- [ ] 5.1 cargo test
- [ ] 5.2 docker compose up -d && make test-e2e
- [ ] 5.3 make unity-up && make test-e2e-unity
- [ ] 5.4 cargo clippy --all-targets (+ --features unity-e2e)
- [ ] 5.5 cargo fmt --check
- [ ] 5.6 Scenario coverage audit
- [ ] 5.7 Manual testing

## Phase 4: Review Fixes
- [x] 4.6 Delete the three-line admission comment above the `Ok(format)` arm in `list_tables` (unity/client.rs)
- [x] 4.7 Replace the `list_tables_tags_each_admitted_table_by_its_own_format` doc with one Scenario line and reword its "admission filter is unchanged" assertion message (unity/client_tests.rs)
- [x] 4.8 Reorder the `columns` JSON in `list_tables_admits_a_parquet_base_table_with_its_partition_columns` so `partition_index` order differs from column order (unity/client_tests.rs)
- [x] 4.9 Replace three multi-line new test docs with one-line `/// Scenario:` docs (unity/client_tests.rs)
- [x] 4.10 Add a doc comment to `pub(super) fn ensure_table_has_a_mappable_column`, naming the whole-table refusal and `table_kind` (delta_format_reader.rs)
- [x] 4.11 Remove the `secrets` parameter from `UnityParquetFormatReader::plan`, derive it from `storage` (unity_parquet_format_reader.rs)
- [x] 4.12 Replace the `resolve_with` doc so it names `storage`, not `keys` (unity_parquet_format_reader_tests.rs)
- [x] 4.13 Add `a_failed_listing_reports_no_static_credential_value` (unity_parquet_format_reader_tests.rs)
- [x] 4.14 Add `catalog_columns_equal_ignoring_letter_case_fail_the_plan` (unity_parquet_format_reader_tests.rs)
- [x] 4.15 Rename `parquet_table_entry_typed` to `parquet_table_entry` and drop its `table_type` parameter (unity_schema_tests.rs)
- [x] 4.16 Reword the partition-column assertion message in `hive_partitioning_reaches_the_seam_on_enumeration` (direct_storage_tests.rs)
- [x] 4.17 Add `FieldIdResolution::for_logical_schema` and use it in `raw_scan.rs` and `column_binding_for` (field_id_projection.rs, raw_scan.rs, test_support_tests.rs)
- [x] 4.18 Replace the `SALES_PARQUET_LOCATION` doc (e2e_unity_test.rs)
- [x] 4.19 Replace the `register_sales_parquet_table` and `sales_parquet_batch` docs (e2e_unity_test.rs)
- [x] 4.20 List the full admitted-type set in the Unity Parquet tables bullet (docs/catalogs.md)
- [x] 4.21 Judge a primitive file column under a nested declaration by admission in `ColumnBinding::refused_columns`, plus test (field_id_projection.rs) [expert]
- [x] 4.22 Admit an all-NULL (`DataType::Null`) file column under every declared type in `admits`, plus test and spec clause (field_id_projection.rs) [expert]

# Tasks: add-glue-catalog-kind

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [x] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group 0: Run-first type baseline)
- [x] 0.1 Add type-matrix data table and fixtures (no production change)
- [x] 0.2 Add e2e_type_matrix_test.rs and Makefile test-e2e entry
- [x] 0.3 Run the matrix on unchanged main, record notes/type-matrix-baseline.md
- [x] 0.4 Check the baseline against the recorded specs, stop on contradiction

## Phase 2: Implementation (Group F2: Glue orphan sweep)
- [x] 7.6 Add .github/workflows/glue-orphan-sweep.yml

## Phase 2: Implementation (Group A: Dependencies)
- [x] 1.1 Workspace dependency pins
- [x] 1.2 Crate Cargo.toml changes and glue-e2e feature
- [x] 1.3 Confirm compilers are at least 1.94.1
- [x] 1.4 cargo check, tree, and deny checks

## Phase 2: Implementation (Group B: Glue metadata model)
- [x] 2.1 CatalogTable/ColumnSourceType/SkipReason/CatalogPartition additions and census
- [x] 2.2 glue/ module and GlueCatalogSession [expert]
- [x] 2.3 glue/routing.rs
- [x] 2.4 list_tables and load_table_for_planning
- [x] 2.5 Glue Iceberg listing from metadata.json [expert]
- [x] 2.6 partitions()
- [x] 2.7 hive_type.rs and Glue mapping arm
- [x] 2.8 lib.rs exports and public-surface test
- [x] 2.9 Mock Glue, client, routing, hive-type, ambient-credential tests

## Phase 2: Implementation (Group D1: Binary refusal)
- [x] 4.1 binary_cause and Iceberg binary refusal
- [x] 4.2 Iceberg refusal tests
- [x] 4.3 Parquet footer binary classification [expert]
- [x] 4.4 Direct-storage refused columns
- [x] 4.5 Direct-storage refusal tests, rerun matrix

## Phase 2: Implementation (Group C: Listing pattern and typed predicate)
- [x] 3.1 FilePattern and listing split
- [x] 3.2 raw_location_prefix and list_location_files
- [x] 3.3 parquet_directory tests
- [x] 3.4 Typed PartitionPredicate [expert]
- [x] 3.5 Predicate tests

## Phase 2: Implementation (Group D2: Glue table planning)
- [x] 5.1 CatalogParquetFormatReader rename and generalization [expert]
- [x] 5.2 Glue file source and plan_glue_partitions [expert]
- [x] 5.3 Split IcebergFormatReader by metadata source [expert]
- [x] 5.4 ScanSource::Glue
- [x] 5.5 Planning tests

## Phase 2: Implementation (Group E: Adapter)
- [x] 6.1 CatalogKind::Glue
- [x] 6.2 CONNECTION handling
- [x] 6.3 construct_catalog_client arm
- [x] 6.4 RequestSession::Glue
- [x] 6.5 SKIPPED_TABLES in adapter notes
- [x] 6.6 Measure adapterNotes size limit
- [x] 6.7 cargo test and clippy per feature

## Phase 2: Implementation (Group F: Glue E2E gate)
- [x] 7.1 common/glue.rs harness
- [x] 7.2 Glue fixtures
- [x] 7.3 e2e_glue_test.rs
- [x] 7.4 Makefile and test.env.example
- [x] 7.5 CI job e2e-glue

## Phase 2: Implementation (Group G: Docs and verification)
- [x] 8.1 docs/catalogs.md and docs/security.md
- [x] 8.2 Measure .so size
- [x] 8.3 Final verification matrix

## Phase 4: Review Fixes
- [x] 4.6 catalog_schema: name the column in every refused-column reason and extend the test fragments for `u`, `i`, `m`, `e`
- [x] 4.7 build_adapter_notes: compute the size base with `Json::Object(notes.clone()).to_string().len()`
- [x] 4.8 Extract `insert_skipped_tables` from `build_adapter_notes`, above `fit_skipped_tables`
- [x] 4.9 connection.rs: add `supplied_catalog_auth_fields` and use it in both validators
- [x] 4.10 glue client_tests: retry test uses `fast_retry_session`, drop wall-clock assertion, rename to `throttling_and_503_are_retried_until_success`
- [x] 4.11 glue client_tests: add 503-status, signing-time-rejection, and operation-timeout tests (with `MockResponse::hang()`)
- [x] 4.12 glue client_tests: delete duplicate missing-table test and strengthen the planning-load test
- [x] 4.13 glue client_tests: rename the concurrency-named test to `iceberg_metadata_files_are_each_read_once_in_listing_order`
- [x] 4.14 mock_glue_tests: `operation()` returns `Option<&str>`, add `storage_requests()`, update callers in client_tests and engine test_support_tests
- [x] 4.15 mock_glue_tests and test_support_tests: `.expect("a Glue request body is JSON")` instead of `unwrap_or(Null)`
- [x] 4.16 routing_tests: cover `virtual_view` lower case
- [x] 4.17 adapter_tests: oversized-list test builds `with_one_more` by serializing instead of recomputing the formula
- [x] 4.18 connection_tests: split the no-region case out of `glue_connection_requires_the_sigv4_fields`
- [x] 4.19 format_tests: Glue reader-selection test resolves the scan and asserts which reader was selected
- [x] 4.20 iceberg_tests: split `a_glue_iceberg_table_is_planned_from_its_metadata_file` into three tests
- [x] 4.21 test_support_tests: share `user_message`; remove copies in iceberg_tests and delta_schema_tests
- [x] 4.22 test_support_tests: share `load_table_body_with_columns`; use it in `binary_iceberg_table_body`
- [x] 4.23 scan_resolution_tests: assert the first-dot split with `sales.orders.v2`
- [x] 4.24 Delete the six redundant helper doc comments
- [x] 4.25 e2e_glue_test: delete the duplicate absent-variable block in `glue_suite_fails_when_stack_unavailable`
- [x] 4.26 e2e_glue_test: panic with context on GetDatabase and S3-list errors
- [x] 4.27 e2e_glue_test/common glue.rs: use `glue_failure` for GetTables and GetPartitions errors
- [x] 4.28 e2e_glue_test: derive the unpushed oracle from the `ORDERS` fixture
- [x] 4.29 e2e_glue_test/type_matrix: stop discarding cleanup and setup failures
- [x] 4.30 e2e_glue_test: remove the duplicated `env` field from `GlueFixture`
- [x] 4.31 common/glue.rs: move Hive column data into `HiveTypeColumn.data`
- [x] 4.32 common/glue.rs and type_matrix.rs: replace positional fixture constructors with struct literals
- [x] 4.33 type_matrix.rs: name the Iceberg field id constants
- [x] 4.34 glue-orphan-sweep.yml: correct the capture comment

## Phase 9: Verification
- [ ] 9.1 Run verification checklist
- [ ] 9.2 Scenario coverage audit
- [ ] 9.3 Manual verification

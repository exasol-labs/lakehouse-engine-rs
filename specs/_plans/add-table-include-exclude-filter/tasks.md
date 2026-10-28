# Tasks: add-table-include-exclude-filter

## Phase 2: Implementation (Group A: Listing predicate)
- [x] 1.1 Change `CatalogClient::list_tables` to take the `exposes` predicate, make it a provided method, delete the Iceberg REST and direct storage overrides, filter in the Unity Catalog and Glue overrides, and adapt every existing call site and `FixedCatalogClient`

## Phase 2: Implementation (Group B: Table filter end to end)
- [x] 2.1 Create `adapter/table_filter.rs` (`TableFilter`, parse function, `exposes`, iterative matcher) and declare it in `adapter/mod.rs`
- [x] 2.2 Unit tests in `adapter/table_filter_tests.rs`
- [x] 3.1 Add `TABLE_INCLUDE` and `TABLE_EXCLUDE` to `PROPERTY_NAMES`, return `CheckedProperties` from `check_properties`, adapt callers
- [x] 3.2 Wire the `TableFilter` into `list_source` for the full listing and the partial refresh
- [x] 4.1 E2E `table_filter.rs`: `include_and_exclude_choose_the_exposed_tables`, `set_properties_relists_the_exposed_tables`
- [x] 4.2 E2E `direct_storage.rs`: `hidden_directories_are_never_read_or_recorded_as_skipped`
- [x] 4.3 E2E `property_validation.rs`: extend `create_accepts_every_known_property_name`, add `create_rejects_an_empty_table_pattern_entry`
- [x] 5.1 Docs: `docs/catalogs.md` section and `docs/tuning.md` rows

## Phase 4: Review Fixes
- [x] 4.4 `lakehouse-catalog/src/client.rs`: move the body of `IcebergRestCatalogClient::resolve_listing` into `load_listed_tables` and delete `resolve_listing`
- [x] 4.5 `lakehouse-catalog/src/client.rs`: rewrite the first sentence of the `CatalogClient::list_table_idents` doc comment to state it returns every listed identifier before any `exposes` predicate applies
- [x] 4.6 `lakehouse-catalog/src/client_tests.rs`: delete the `list_tables` override from `impl CatalogClient for FixedCatalogClient`
- [x] 4.7 `lakehouse-e2e/tests/e2e_unity_test.rs`: add `unity_table_include_narrows_the_list_sweep`, asserting `TABLE_INCLUDE = 'CM_NAME_MODE'` exposes only `CM_NAME_MODE`
- [x] 4.8 `lakehouse-engine/src/adapter/direct_storage.rs`: replace the three doc-comment lines above `list_table_idents` with the two-line comment from the finding
- [x] 4.9 `lakehouse-engine/src/adapter/table_filter.rs`: derive only `Debug, PartialEq` on `Pattern` and `Debug, Default, PartialEq` on `TableFilter`
- [x] 4.10 `e2e_local/direct_storage.rs`: in `hidden_directories_are_never_read_or_recorded_as_skipped`, run a full `REFRESH` and a `SET TABLE_EXCLUDE = 'CONFLICT, EMPTY'` and assert after each that only `GOOD` is served and nothing is skipped
- [x] 4.11 `lakehouse-e2e/tests/e2e_glue_test.rs`: add `check_a_hidden_broken_table_does_not_fail_the_listing` as the 13th fixture check after `("broken table", ...)`, excluding `BROKEN_ICEBERG` and asserting a full `REFRESH` succeeds and serves exactly `ROUTED_TABLES` [expert]

## Phase 5: Verification
- [x] 5.2 Build: `make cross-udf-build`
- [x] 5.3 Unit tests: `cargo test --workspace`
- [x] 5.4 E2E (local Docker): `make test-e2e`
- [x] 5.5 Lint and format: `make fmt-check lint`
- [x] 5.6 Scenario coverage audit
- [x] 5.7 Manual testing against local Docker Exasol

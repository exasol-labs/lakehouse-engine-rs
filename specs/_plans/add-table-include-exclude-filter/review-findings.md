# Code Review Findings: add-table-include-exclude-filter

## Summary
- Files reviewed: 24
- Total findings: 8 (standard: 7, expert: 1)
- Evidence: `cargo clippy --workspace --all-targets --all-features` finishes with no warning, and `cargo test --workspace --lib table_filter` passes 6 of 6 tests.

## Standard fixes

### crates/lakehouse-catalog/src/client.rs

#### [SHALLOW_MODULE] `resolve_listing` is now a single-caller pass-through
- Location: lines 244-261 (`IcebergRestCatalogClient::resolve_listing`) and lines 309-315 (`load_listed_tables`)
- Issue: The change deleted the `IcebergRestCatalogClient::list_tables` override, which was the second caller of `resolve_listing`. `load_listed_tables` is now the only caller, and its whole body is `self.resolve_listing(&idents).await`. A reader follows two functions to learn one behavior.
- Fix: In `crates/lakehouse-catalog/src/client.rs`, move the body of `IcebergRestCatalogClient::resolve_listing` (the empty-input early return, the session lookup, the `buffered(CATALOG_REQUEST_CONCURRENCY)` admission stream, and the collect) into the `async move` block of `impl CatalogClient for IcebergRestCatalogClient` → `load_listed_tables`, reading the cloned `idents` vector. Then delete `resolve_listing`. Run `cargo test --workspace --lib client` to confirm the Iceberg REST listing tests still pass.

#### [OUTDATED_COMMENT] `list_table_idents` is defined through a `list_tables` call that now filters
- Location: lines 199-200
- Issue: The doc comment of `CatalogClient::list_table_idents` says it returns "Every identifier `list_tables` would return or skip". `list_tables` now takes the `exposes` predicate and returns or skips only the identifiers that the predicate accepts, while `list_table_idents` still returns every listed identifier. The definition is now true only for a predicate that accepts everything.
- Fix: In `crates/lakehouse-catalog/src/client.rs`, change the first sentence of the doc comment on `CatalogClient::list_table_idents` to "Every identifier the namespace listing names, in listing order, before any `exposes` predicate applies, without loading any table's metadata, so a caller can pick tables by name before paying for their loads." Keep the comment at two lines.

### crates/lakehouse-catalog/src/client_tests.rs

#### [SHRINKABLE] `FixedCatalogClient` overrides `list_tables` and ignores the predicate
- Location: lines 81-88 (`impl CatalogClient for FixedCatalogClient` → `list_tables`)
- Issue: The test double overrides `list_tables` and discards `_exposes`. The trait doc says every override applies `exposes` before it admits, skips, or loads a table, so the double breaks the contract it stands in for. The provided `list_tables` body already produces the same listing from the double's `list_table_idents` and `load_listed_tables`: tables `orders` and `payments`, then skipped `not_a_table` and `orders_summary`, for any namespace.
- Fix: In `crates/lakehouse-catalog/src/client_tests.rs`, delete the `list_tables` method from `impl CatalogClient for FixedCatalogClient`, so the double uses the provided trait body. Run `cargo test --workspace --lib client_tests` and confirm `boxed_client_lists_neutral_tables_and_skipped_entries_with_reasons`, `namespace_segments_reach_the_client_unjoined`, and `an_iceberg_column_carries_its_iceberg_source_type` still pass.

### crates/lakehouse-catalog/src/unity/client.rs

#### [MISSING_BOUNDARY_TEST] No test calls the Unity Catalog listing with a predicate that rejects a table
- Location: lines 186-187 (`UnityCatalogSession::list_tables`, the `.filter(|(ident, _)| exposes(ident))` step)
- Issue: Every call of the Unity Catalog `list_tables` override passes `&|_| true`. The local E2E tests of this plan run against the Iceberg REST catalog and direct storage, which use the provided trait body. Deleting the `.filter` line from the Unity Catalog override therefore fails no test in any suite, although `vs-properties/table-filter` requires the filter for every catalog kind. The Unity Catalog E2E suite runs on the local Docker stack (`docker-compose.unity.yml`), so an E2E test can prove the override.
- Fix: In `crates/lakehouse-e2e/tests/e2e_unity_test.rs`, add a test `unity_table_include_narrows_the_list_sweep` tagged `/// Scenario: Include and exclude patterns choose the exposed tables, and exclude wins`. Follow `unity_column_mapping_property_reaches_create_and_scoped_refresh`: call `setup()`, then `create_virtual_schema_with_password` with `VsProps::new("UNITY_TABLE_FILTER_VS", UNITY_NAMESPACE).with_catalog_kind("UNITY_CATALOG").with_catalog_conn_name("UNITY_TABLE_FILTER_CREDS").with_property("TABLE_INCLUDE", "CM_NAME_MODE")`, `UNITY_CATALOG_URI_INTERNAL`, and `&local_stack_connection_password()`. Assert that `enumerated_table_names(&mut conn, "UNITY_TABLE_FILTER_VS")` equals `["CM_NAME_MODE"]`. Run it with `make test-e2e-unity`.

### crates/lakehouse-engine/src/adapter/direct_storage.rs

#### [OUTDATED_COMMENT] The doc comment of the deleted `list_tables` override now heads `list_table_idents`
- Location: lines 84-86
- Issue: The change deleted `DirectStorageCatalogClient::list_tables` but kept its two-line doc comment ("Each first-level directory under the base path is a table with an empty namespace; `namespace` is unread ..."). That comment now runs into the doc comment of `list_table_idents`, which gives one method a three-line comment assembled from two methods. `AGENTS.md` § Code style limits a comment to 1 or 2 lines.
- Fix: In `crates/lakehouse-engine/src/adapter/direct_storage.rs`, replace the three doc-comment lines above `fn list_table_idents` with these two lines: `/// Each first-level directory under the base path, sorted by name, is a table with an empty` and `/// namespace; one delimiter listing reads below no directory, and `namespace` is unread.`

### crates/lakehouse-engine/src/adapter/table_filter.rs

#### [UNUSED_FUNCTION] `Clone` and `Eq` are derived but never used
- Location: line 10 (`Pattern`) and line 58 (`TableFilter`)
- Issue: No code clones a `Pattern` or a `TableFilter`, and no code needs `Eq`. `CheckedProperties` derives only `Debug`, `Default`, and `PartialEq`, matching `TuningProperties`. The extra derives add trait impls that nothing calls.
- Fix: In `crates/lakehouse-engine/src/adapter/table_filter.rs`, change the derive on `Pattern` to `#[derive(Debug, PartialEq)]` and the derive on `TableFilter` to `#[derive(Debug, Default, PartialEq)]`. Run `cargo check --workspace --all-targets` to confirm nothing needs them.

### crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs

#### [MISSING_BOUNDARY_TEST] The full REFRESH and `setProperties` requests never meet a hidden table whose load fails
- Location: `hidden_directories_are_never_read_or_recorded_as_skipped` (end of file)
- Issue: The scenario "A hidden table is never loaded and never recorded as skipped" names `createVirtualSchema`, `refresh`, and `setProperties` requests. The test sends only `createVirtualSchema` and a partial `REFRESH TABLES`. A full `REFRESH` and a `SET` read the filter from the persisted properties, not from the request. `set_properties_relists_the_exposed_tables` proves that these requests omit a hidden table from the response, but it cannot tell a filter that runs before the load from one that runs after it, because none of its tables fails to load.
- Fix: In `crates/lakehouse-e2e/tests/e2e_local/direct_storage.rs`, in `hidden_directories_are_never_read_or_recorded_as_skipped`, after the `skipped_entries` assertion that follows the filtered `CREATE`, run `conn.execute(&format!("ALTER VIRTUAL SCHEMA {VS_TABLE_FILTER} REFRESH"))` and then `conn.execute(&format!("ALTER VIRTUAL SCHEMA {VS_TABLE_FILTER} SET TABLE_EXCLUDE = 'CONFLICT, EMPTY'"))`. After each statement, assert that `served_tables(&mut conn, VS_TABLE_FILTER)` equals `["GOOD"]` and that `skipped_entries(&mut conn, VS_TABLE_FILTER)` is empty, with a message that names the statement. Keep the existing `REFRESH TABLES GOOD CONFLICT` step last.

## Expert fixes

### crates/lakehouse-catalog/src/glue/client.rs

#### [UNTESTED_ERROR_PATH] No test proves that a hidden Glue table cannot fail the listing
- Location: lines 224-225 (`GlueCatalogSession::list_tables`, the `.filter(...)` step before `self.admit`)
- Issue: The Glue override must drop a hidden table before `admit`, because `admit` reads an Iceberg table's metadata file and fails the statement when that read fails. `vs-properties/table-filter` requires "no Glue `GetTable`, no Iceberg metadata-file read" for a hidden table. Every call of this override passes `&|_| true`, and the local E2E suite never reaches Glue, so deleting the `.filter` line fails no test. The Glue suite already registers `BROKEN_ICEBERG`, a table whose load fails every full `REFRESH` (`check_a_broken_unrequested_table_fails_only_a_full_refresh`). That table can prove the filter. The fix is routed to the expert agent because `glue_checks_pass_against_one_provisioned_fixture` has an ordering contract (the broken-table check must stay after every check that runs a full `REFRESH`) and because the Glue suite needs AWS credentials, so a host run cannot verify it.
- Fix: In `crates/lakehouse-e2e/tests/e2e_glue_test.rs`, add a check `check_a_hidden_broken_table_does_not_fail_the_listing(run: &GlueRun, conn: &mut ExaConn)` tagged `/// Scenario: A hidden table is never loaded and never recorded as skipped`. It runs `ALTER VIRTUAL SCHEMA {VS} SET TABLE_EXCLUDE = '<BROKEN_ICEBERG uppercased>'` and then `ALTER VIRTUAL SCHEMA {VS} REFRESH`, asserts that both statements succeed, and asserts that the `SYS.EXA_ALL_VIRTUAL_TABLES` names of `{VS}`, ordered by name, equal the uppercased `ROUTED_TABLES`. Append it as the 13th entry of the `checks` array in `glue_checks_pass_against_one_provisioned_fixture`, after `("broken table", ...)`, because it needs the broken table that check registers. Change the array type to `[(&str, FixtureCheck); 13]`. Update that function's doc comment to say that the last two checks run after the broken table is registered. Run `cargo clippy --workspace --all-targets --all-features`, and run `make test-e2e-glue` when AWS credentials are available. Otherwise state in the verification report that CI runs it.

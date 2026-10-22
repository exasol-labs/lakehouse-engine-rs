# Code Review Findings: change-vs-adapter-per-table-notes

## Summary
- Files reviewed: 38
- Total findings: 16 (standard: 16, expert: 0)
- `cargo clippy --all-targets` and `cargo clippy -p lakehouse-engine --all-targets --features exasol-e2e,unity-e2e,glue-e2e` report no warnings.
- No code, doc, or `specs/mission.md` line outside `specs/_plans/` names `TABLE_MAP`, `NOTE_TABLE_MAP`, `read_table_map`, or `build_table_map`. The leftovers in `specs/architecture.md`, `specs/file-planning/pushdown-planning-file-resolution/spec.md`, and `specs/_decision/046-*` sit outside the changed-files list, and the plan's deltas and `architecture.md` replace the first two at record time.
- No new test asserts on a fake's request log. `ScanSpec`, `FileEntry`, and `LogicalField` are unchanged. Every new module keeps its unit tests in a sibling `_tests.rs` file.

## Standard fixes

### crates/lakehouse-catalog/src/client_tests.rs

#### [ASSERTION_FREE_TEST] The listed-identifier round-trip test exercises only the test double
- Location: line 221, `boxed_client_loads_the_listed_identifiers_back_into_the_full_listing`
- Issue: `boxed_client()` returns `Box::new(FixedCatalogClient)`, the fake declared in this file. The test compares `FixedCatalogClient::list_table_idents` plus `FixedCatalogClient::load_listed_tables` with `FixedCatalogClient::list_tables`, so no production code can make it fail. `catalog_client_trait_and_neutral_types_are_reachable` in `tests/catalog_public_surface.rs` already proves that both new trait methods are reachable through a boxed client.
- Fix: In crates/lakehouse-catalog/src/client_tests.rs, delete the test `boxed_client_loads_the_listed_identifiers_back_into_the_full_listing`. Keep the `FixedCatalogClient` implementations of `list_table_idents` and `load_listed_tables`, because the trait requires them.

#### [MISSING_DOC_COMMENT] The Iceberg scoped-refresh unit tests carry no scenario tag
- Location: line 433 `empty_namespace_builds_no_session_and_no_grant`, line 469 `enumeration_builds_exactly_one_session`
- Issue: plan.md § Scenario Coverage maps "A scoped refresh lists Iceberg identifiers and loads the requested tables on one session" to these two tests (one session, empty batch). AGENTS.md requires one `/// Scenario: <title>` line per implemented scenario. Neither test carries it.
- Fix: In crates/lakehouse-catalog/src/client_tests.rs, add the line `/// Scenario: A scoped refresh lists Iceberg identifiers and loads the requested tables on one session` directly above the `#[tokio::test]` attribute of `empty_namespace_builds_no_session_and_no_grant` and of `enumeration_builds_exactly_one_session`.

### crates/lakehouse-catalog/src/glue/client.rs

#### [REDUNDANT_COMMENT] The `tables` doc comment restates the SDK parameter
- Location: line 106, `GlueCatalogSession::tables`
- Issue: `/// `attributes` narrows each returned table to the named attributes; `None` returns them whole.` describes what `set_attributes_to_get` does. It states no invariant, quirk, or constraint, and `tables` is private.
- Fix: In crates/lakehouse-catalog/src/glue/client.rs, delete the doc comment line above `async fn tables(`.

### crates/lakehouse-catalog/src/unity/client_tests.rs

#### [MISSING_DOC_COMMENT] The identifier-listing query test carries no scenario tag
- Location: line 133, `identifier_listing_query_omits_columns_properties_and_username`
- Issue: plan.md maps "A scoped refresh lists Unity table names without columns and loads only the requested tables" to this new test, and plan.md § Verification names it as the only proof that the listing omits columns. It has no `/// Scenario:` line.
- Fix: In crates/lakehouse-catalog/src/unity/client_tests.rs, add `/// Scenario: A scoped refresh lists Unity table names without columns and loads only the requested tables` directly above the `#[test]` attribute of `identifier_listing_query_omits_columns_properties_and_username`.

### crates/lakehouse-catalog/tests/catalog_public_surface.rs

#### [MISSING_DOC_COMMENT] The extended trait-reachability test carries no scenario tag
- Location: line 125, `catalog_client_trait_and_neutral_types_are_reachable`
- Issue: task 2.1 extends this test "for the scenario 'The Unity Catalog session is reached only through the shared catalog-client trait'", and plan.md § Scenario Coverage maps the scenario to it. The test has no `/// Scenario:` line.
- Fix: In crates/lakehouse-catalog/tests/catalog_public_surface.rs, add `/// Scenario: The Unity Catalog session is reached only through the shared catalog-client trait` directly above the `#[test]` attribute of `catalog_client_trait_and_neutral_types_are_reachable`.

### crates/lakehouse-engine/src/adapter/mod.rs

#### [SHRINKABLE] `build_listing_virtual_tables` returns a clone of its own input
- Location: line 448 `type VirtualTables`, line 527 `Ok((tables_json, listing.skipped.clone()))`, line 245 caller in `handle_create_virtual_schema`
- Issue: with the table map gone, the second element of `VirtualTables` is `listing.skipped.clone()`, a copy of a field the caller already holds. `handle_create_virtual_schema` uses it only to log one warning per skip, and the skipped-table notes now come from `list_source`. The tuple costs a clone and a destructuring at every call site and carries no information.
- Fix: In crates/lakehouse-engine/src/adapter/mod.rs, change `build_listing_virtual_tables` to return `Result<Vec<Json>, UdfError>` and return `Ok(tables_json)`. Delete the `VirtualTables` alias. In `handle_create_virtual_schema`, bind `let tables_json = build_listing_virtual_tables(...)...?;` and iterate `&listing.skipped` in the `skip_warning` loop. Update the callers the compiler reports: in crates/lakehouse-engine/src/adapter/adapter_tests.rs, replace each `let (tables, _)` or `let (tables_json, _)` with a single binding, and in `refresh_writes_an_identifier_note_on_every_returned_table`, `iceberg_listing_is_behavior_identical_behind_the_trait`, and `an_all_skipped_namespace_creates_an_empty_schema_recording_every_skip`, replace each use of the returned `skipped` with `listing.skipped`. In crates/lakehouse-engine/src/adapter/catalog_client_tests.rs, change the fn-pointer return type in `construction_site_is_exhaustive_and_fallible_for_every_kind` to `Result<Vec<Json>, UdfError>`, drop the `.map(|(tables, _)| tables)` in `listed_tables`, and replace each `let (x, _)` destructuring with a single binding.

### crates/lakehouse-engine/src/adapter/direct_storage.rs

#### [OUTDATED_COMMENT] `list_tables` claims `namespace` is unread but copies and forwards it
- Location: lines 81-91, `DirectStorageCatalogClient::list_tables`
- Issue: the doc comment says "`namespace` is unread since the base path already encodes `CATALOG_CONNECTION` + `NAMESPACE`". The body now allocates `namespace.to_vec()` and passes it to `list_table_idents`, which ignores it as `_namespace`.
- Fix: In crates/lakehouse-engine/src/adapter/direct_storage.rs, rename the `list_tables` parameter to `_namespace`, delete the line `let namespace = namespace.to_vec();`, and call `self.list_table_idents(&[]).await?` inside the `Box::pin(async move { ... })`. Keep the doc comment text.

### crates/lakehouse-engine/src/adapter/scoped_refresh.rs

#### [INFORMATION_LEAKAGE] The skip-merge reimplements the skip entry's key encoding
- Location: lines 91-109, `ScopedRefresh::merge_skipped_tables`
- Issue: `skipped_tables.rs` decides that `SkipEntry::table` holds `catalog_identifier_string(ident)` (`impl From<&SkippedTable> for SkipEntry`, line 37). `merge_skipped_tables` assumes the same encoding independently: it builds a `HashMap` over `persisted.entries` by `entry.table` and looks it up with `catalog_identifier_string(ident)`. A change to the recorded `table` value in `skipped_tables.rs` would make the merge drop every persisted entry silently, and the `skipped_tables.rs` module doc claims that no other module depends on the entry format.
- Fix: In crates/lakehouse-engine/src/adapter/skipped_tables.rs, add to `impl SkippedTables` a documented method `pub fn entry_lookup<'a>(&'a self) -> impl Fn(&CatalogTableIdent) -> Option<&'a SkipEntry> + 'a` that builds a `HashMap<&str, &SkipEntry>` from `self.entries` keyed by `entry.table` once and returns a closure that looks up `catalog_identifier_string(ident)`; import `lakehouse_catalog::CatalogTableIdent` and `std::collections::HashMap` there. In crates/lakehouse-engine/src/adapter/scoped_refresh.rs, replace `persisted_by_table` with `let persisted_entry = persisted.entry_lookup();`, replace the unrequested branch with `persisted_entry(ident).cloned()`, and remove the now-unused `catalog_identifier_string` and `HashMap` imports. Run `cargo test -p lakehouse-engine scoped_refresh` and confirm that the merge tests still pass.

### crates/lakehouse-engine/src/adapter/skipped_tables.rs

#### [REDUNDANT_COMMENT] The `fit_skipped_tables` doc comment restates the return type and exceeds two lines
- Location: lines 108-110, `fit_skipped_tables`
- Issue: the added sentence "Returns the prefix and the omitted count it records, `carried` included." restates the `(Vec<Json>, usize)` signature, and it grows the comment to three lines. AGENTS.md § Code style limits a comment to one or two lines.
- Fix: In crates/lakehouse-engine/src/adapter/skipped_tables.rs, replace the three-line doc comment of `fit_skipped_tables` with these two lines: `/// The longest listing-order prefix of `entries` within [`ADAPTER_NOTES_MAX_BYTES`], and the` and `/// omitted count it records, `carried` included; `base_len` is the notes length with an empty list.`

### crates/lakehouse-engine/src/adapter/scoped_refresh_tests.rs

#### [MISSING_DOC_COMMENT] The new partial-refresh unit tests carry no scenario tags
- Location: lines 37, 55, 132, 169, 200, 228
- Issue: plan.md § Scenario Coverage maps three partial-refresh scenarios to tests in this file, and AGENTS.md requires one `/// Scenario: <title>` line per implemented scenario. None of the tests in this file carries one.
- Fix: In crates/lakehouse-engine/src/adapter/scoped_refresh_tests.rs, add these lines directly above each test's `#[test]` attribute:
  - `only_requested_identifiers_are_selected_for_loading`: `/// Scenario: A partial refresh loads the metadata of the requested tables only`
  - `every_identifier_that_flattens_to_a_requested_name_is_selected`: `/// Scenario: A partial refresh resolves each requested name as a full refresh would`
  - `merge_keeps_unrequested_listed_entries_in_listing_order`, `merge_drops_entries_the_source_no_longer_lists`, `merge_carries_the_persisted_omitted_count`, and `merge_writes_no_omitted_count_when_the_sum_is_zero`: `/// Scenario: A partial refresh merges SKIPPED_TABLES with the persisted list`

#### [MISSING_BOUNDARY_TEST] An empty `requestedTables` array is untested
- Location: `RequestedTables::from_request` in crates/lakehouse-engine/src/adapter/scoped_refresh.rs, lines 22-43
- Issue: the tests cover an absent `requestedTables`, a malformed value, and lists of one or more names. No test pins `{"type": "refresh", "requestedTables": []}`. The code treats it as a partial refresh that loads and returns no table, unlike a refresh without the key, which reloads every table.
- Fix: In crates/lakehouse-engine/src/adapter/scoped_refresh_tests.rs, add the test `an_empty_requested_tables_array_is_a_partial_refresh_that_selects_nothing`. It calls `requested(&[])`, builds `ScopedRefresh::new(&requested, &[], &[ident(&[], "orders")])`, and asserts that `selection()` is empty.

### crates/lakehouse-engine/src/adapter/adapter_tests.rs

#### [SHRINKABLE] Four tests repeat the same function-local imports
- Location: lines 391-392, 1097-1098, 1128-1129, 1168-1169; `cat_ident` at line 1119
- Issue: `refresh_writes_an_identifier_note_on_every_returned_table`, `the_identifier_note_records_the_bare_directory_name_and_round_trips`, `create_vs_records_the_multi_level_identifier_in_the_table_note`, and `a_flatten_collision_among_listed_tables_fails_the_listing` each open with `use crate::adapter::pushdown::test_support::named_table;` and `use lakehouse_catalog::TableFormat;`. `cat_ident` spells out `lakehouse_catalog::CatalogTableIdent` twice, because the import moved out of `super::*`.
- Fix: In crates/lakehouse-engine/src/adapter/adapter_tests.rs, extend the file-level imports to `use crate::adapter::pushdown::test_support::{named_table, with_identifier_notes};` and `use lakehouse_catalog::{CatalogTableIdent, SkipReason, TableFormat};`. Delete the two function-local `use` lines from the four tests. Write `CatalogTableIdent` without the `lakehouse_catalog::` prefix in `cat_ident`. Leave the function-local imports of `iceberg_listing_is_behavior_identical_behind_the_trait` unchanged.

### crates/lakehouse-engine/tests/e2e_local/refresh.rs

#### [SWALLOWED_ERROR] `drop_table_ignoring_missing` discards every drop error
- Location: lines 160-169 (definition), line 481 (fixture reset), line 576 (the step that drops `table_two`)
- Issue: `let _ = catalog.drop_table(...).await;` discards a network error or an HTTP 500 as well as the missing-table case its name and plan task 3.4 allow. At line 576 the drop is the step under test, so a failed drop surfaces later as a misleading "the dropped TABLE_TWO must be gone" failure. The pinned iceberg-rust REST client maps a 404 on `drop_table` to `ErrorKind::TableNotFound`.
- Fix: In crates/lakehouse-engine/tests/e2e_local/refresh.rs, change the body of `drop_table_ignoring_missing` to bind `let ident = TableIdent::new(ns.clone(), table_name.to_string());` and then `match catalog.drop_table(&ident).await { Ok(()) => {}, Err(e) if e.kind() == iceberg::ErrorKind::TableNotFound => {}, Err(e) => panic!("drop table {}.{table_name}: {e}", ns.join(".")) }`. At line 576, replace the `drop_table_ignoring_missing` call with a direct `catalog.drop_table(&TableIdent::new(namespace.clone(), "table_two".to_string()))` awaited through `rt.block_on` and `.unwrap_or_else(|e| panic!("drop table_two: {e}"))`, because `table_two` must exist at that step.

#### [MISSING_DOC_COMMENT] Extended and rewritten refresh tests lack the scenario tags the plan maps to them
- Location: line 174 `refresh_reenumerates_namespace`, line 475 `refresh_tables_heals_and_refreshes_only_the_requested_tables`, line 606 `refresh_flatten_collision_returns_same_error_as_create`
- Issue: plan.md § Scenario Coverage maps these scenarios to these tests, and task 1.9 requires every new or renamed test to carry the tag of each scenario it implements. The tags are missing:
  - `refresh_reenumerates_namespace`: "Refresh rebuilds every identifier note and preserves other adapter notes"
  - `refresh_tables_heals_and_refreshes_only_the_requested_tables`: "Pushdown derives the scanned Iceberg table from the involved virtual table" and "Each join leg resolves its catalog identifier from its own identifier note"
  - `refresh_flatten_collision_returns_same_error_as_create`: "Multi-level Iceberg namespaces flatten deterministically into Exasol table names"
- Fix: In crates/lakehouse-engine/tests/e2e_local/refresh.rs, add these lines to the existing `/// Scenario:` block above each test's `#[test]` attribute: `/// Scenario: Refresh rebuilds every identifier note and preserves other adapter notes` for `refresh_reenumerates_namespace`; `/// Scenario: Pushdown derives the scanned Iceberg table from the involved virtual table` and `/// Scenario: Each join leg resolves its catalog identifier from its own identifier note` for `refresh_tables_heals_and_refreshes_only_the_requested_tables`; and `/// Scenario: Multi-level Iceberg namespaces flatten deterministically into Exasol table names` for `refresh_flatten_collision_returns_same_error_as_create`.

### crates/lakehouse-engine/tests/e2e_unity_test.rs

#### [MISSING_DOC_COMMENT] The extended Unity listing test lacks its identifier-note scenario tags
- Location: line 731, `unity_create_virtual_schema_lists_fixture_tables_and_columns`
- Issue: task 1.8 extends this test with the identifier-note assertions, and plan.md maps "Create virtual schema records each table's Unity Catalog identifier in its identifier note" and "A Unity Catalog table's identity survives the round trip from the involved table" to it. The test carries only the scoped-refresh tag.
- Fix: In crates/lakehouse-engine/tests/e2e_unity_test.rs, add `/// Scenario: Create virtual schema records each table's Unity Catalog identifier in its identifier note` and `/// Scenario: A Unity Catalog table's identity survives the round trip from the involved table` to the `/// Scenario:` block above the `#[test]` attribute of `unity_create_virtual_schema_lists_fixture_tables_and_columns`.

### crates/lakehouse-engine/tests/e2e_glue_test.rs

#### [VAGUE_TEST_NAME] The listing check also proves the partial refresh
- Location: lines 266-377, `check_listing_includes_routed_tables_and_records_every_skip`
- Issue: the check's name states the listing and the skip record. From line 323 on, it also runs `ALTER VIRTUAL SCHEMA ... REFRESH TABLES ICEBERG_ORDERS ORC_TABLE` and asserts the partial-refresh behavior, a second concept. A failure in that step reports as a failed "listing" check.
- Fix: In crates/lakehouse-engine/tests/e2e_glue_test.rs, move the code from `let named = ICEBERG_ORDERS.to_uppercase();` through the final `assert_records_every_skip(conn, &database);` into a new function `check_refresh_tables_redeclares_only_the_named_tables(run: &GlueRun, conn: &mut ExaConn)`, which recomputes `routed` and `database` itself. Move the `/// Scenario: A scoped refresh lists Glue table names and loads only the requested tables` line from the listing check onto the new function. Register it in `glue_checks_pass_against_one_provisioned_fixture` as `("refresh tables", check_refresh_tables_redeclares_only_the_named_tables)` directly before the `"broken table"` entry, and change the array length from 9 to 10.

## Expert fixes
[none]

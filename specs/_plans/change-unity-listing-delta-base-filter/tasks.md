# Tasks: change-unity-listing-delta-base-filter

## Phase 2: Implementation (Group 1 — foundation)
- [x] 2.1 Neutral skip-reason model in `crates/lakehouse-catalog/src/client.rs`: add `SkipReason` (`NotLoadableIcebergTable`; `NotDeltaBaseTable { detail: String }`) and `SkippedTable { ident: CatalogTableIdent, reason: SkipReason }`; change `CatalogListing.skipped` to `Vec<SkippedTable>`; re-export both from `lib.rs`. [expert]

## Phase 2: Implementation (Group 2 — after Task 1)
- [x] 2.2 Update the Iceberg REST client's single 404-skip site (`resolve_listing`) to push `SkippedTable { ident, reason: SkipReason::NotLoadableIcebergTable }`; skip semantics unchanged.
- [x] 2.3 Unity Delta-base filter in `crates/lakehouse-catalog/src/unity/client.rs`: add `data_source_format: Option<String>` to `TableInfo` (serde default) + correct doc; in `list_tables`, admit iff `neutral_table_type` is `Table` AND `data_source_format == "DELTA"`, else push `SkippedTable` with `SkipReason::NotDeltaBaseTable { detail }`; extract admission decision as a pure function. [expert]
- [x] 2.4 Adapter warn render in `crates/lakehouse-engine/src/adapter/mod.rs`: thread `Vec<SkippedTable>` through `build_listing_virtual_tables` + `handle_create_virtual_schema` warn loop; render one `warn` line per entry by matching `reason` — no `CatalogKind` match. [expert]

## Phase 2: Implementation (Group 3 — after prod code)
- [x] 2.5 Unity client unit tests (`crates/lakehouse-catalog/src/unity/client_tests.rs`): update `lists_tables_in_catalog_schema`; add `includes_managed_and_external_delta_base_tables` + `skips_view_non_delta_and_other_type_with_reason`; fix `follows_pagination_across_pages` fixtures + audited fixture edits per plan Task 5.
- [x] 2.6 Engine integration tests (`crates/lakehouse-engine/src/adapter/unity_schema_tests.rs`): set `DELTA` on `table_entry`; replace view test with exclusion test; add non-`DELTA`+other-type exclusion test; keep enumerate/no-per-table-get/collision tests green.
- [x] 2.7 Catalog-crate mechanical `skipped` updates: `crates/lakehouse-catalog/tests/catalog_public_surface.rs` + `crates/lakehouse-catalog/src/client_tests.rs`.
- [x] 2.8 Engine-side mechanical `skipped` updates: `crates/lakehouse-engine/src/adapter/adapter_tests.rs` + `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs`.

## Phase 4: Review Fixes
(appended by fix-task agents if code review finds issues)
- [x] 4.1 `crates/lakehouse-catalog/src/unity/client.rs`: change `delta_base_skip_reason` to take the raw wire `table_type` (`fn delta_base_skip_reason(raw_table_type: &str, data_source_format: Option<&str>) -> Option<SkipReason>`), call `neutral_table_type` inside it, collapse the `View`/`Other` arms into one `table_type={raw_table_type}` arm (deleting the hardcoded `"table_type=VIEW"` literal), drop the now-redundant `neutral_table_type` call in `list_tables`, and update/extend the `delta_base_skip_reason_*` tests to pass raw spellings with unchanged `detail` strings. [expert]
- [x] 4.2 `crates/lakehouse-catalog/src/client_tests.rs`: stop `FixedCatalogClient` fabricating a `View` inside `tables` — replace it with a reachable Unity-sourced Delta base table, add a `NotDeltaBaseTable` skipped entry alongside the `NotLoadableIcebergTable` one, delete `a_listed_view_carries_columns_and_no_storage_location`, correct the struct doc, and update every assertion that counts or indexes `listing.tables`. [expert]
- [x] 4.3 `crates/lakehouse-engine/src/adapter/mod.rs`: extract the skipped-entry warn rendering out of `handle_create_virtual_schema` into a private `fn skip_warning(entry: &SkippedTable) -> String` with byte-identical templates, reduce the loop body to `udf_log!(ctx, warn, "{}", skip_warning(entry));`, and pin both rendered strings with `skip_warning_renders_the_legacy_iceberg_line_and_the_unity_detail_line` in `adapter_tests.rs`. [expert]
- [x] 4.4 `crates/lakehouse-engine/src/adapter/mod.rs`: delete the two-line narration comment above the skipped-entry warn loop (lines 286-287) and move its content into the doc comment of `skip_warning`.
- [x] 4.5 `crates/lakehouse-catalog/src/client.rs`: rewrite the second paragraph of the `SkipReason` doc comment (line 71 onward) to state the enum carries no `CatalogKind` value (not "no catalog-kind identity"), and that consumers render wording by matching the reason so no second `CatalogKind` match site is reintroduced downstream.
- [x] 4.6 `crates/lakehouse-catalog/src/client.rs`: add a doc comment to the `SkipReason::NotLoadableIcebergTable` variant (line 76) stating that the Iceberg REST catalog listed the identifier but `loadTable` reported it is not a loadable Iceberg table, so the entry is skipped rather than failing the whole enumeration.
- [x] 4.7 `crates/lakehouse-catalog/src/client_tests.rs`: rename `boxed_client_lists_neutral_tables_and_skipped_identifiers` to `boxed_client_lists_neutral_tables_and_skipped_entries_with_reasons`.
- [x] 4.8 `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs`: add `#[test] fn excluding_every_entry_yields_an_empty_but_successful_schema` covering the boundary where every listed entry is excluded (a view and a non-Delta table, no survivors), asserting `Ok`, empty response tables, empty table map, and zero per-table `get_table` calls.

## Phase 5: Verification
- [x] 5.1 Build — `make cross-musl-udf-build` (exit 0)
- [x] 5.2 Test — `cargo test` (0 failures)
- [x] 5.3 Lint — `cargo clippy --all-targets` (0 warnings)
- [x] 5.4 Format — `cargo fmt --check` (no changes)

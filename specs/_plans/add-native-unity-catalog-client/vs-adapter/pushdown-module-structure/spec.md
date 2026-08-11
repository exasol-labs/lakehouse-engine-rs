# Feature: Pushdown Module Structure

Decomposes the virtual-schema pushdown-planning code into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

## Background

* This delta redraws the frozen `crate::adapter::pushdown::<name>` façade a second deliberate time. `resolve_table_schema` leaves the façade because the shared `CatalogClient` listing pipeline (plan `add-native-unity-catalog-client`, issue #318) replaces its ONLY production caller: its load-and-extract half moves into `IcebergRestCatalogClient::load_table` in `lakehouse-catalog`, and its Exasol-mapping-and-uppercasing half moves into the shared listing pipeline. No `vs-adapter/pushdown-planning*` scenario changes, because the redraw removes one item and alters no decision and no generated SQL.
* The façade stays FROZEN after this redraw: the two probe files still fail the build on any unplanned narrowing, and a further change to the item set still needs its own spec delta.
* This delta SUPERSEDES the count clause of the "Public pushdown façade resolves at every pre-refactor path" scenario — "the 22-item in-crate probe `use` list and the 12-item external probe `use` list" — to the 21-item in-crate probe `use` list and the 11-item external probe `use` list, because `resolve_table_schema` leaves both lists.
* This delta SUPERSEDES two clauses of the "The pushdown façade releases exactly the three items the catalog extraction relocates" scenario: the retention clause "`resolve_file_list` and `resolve_table_schema` SHALL KEEP their names and their `pub` visibility on the façade", and the count clause "the in-crate probe SHALL name 22 items and the external probe SHALL name 12". After this delta `resolve_table_schema` is DELETED from the façade, `resolve_file_list` ALONE keeps its name and `pub` visibility, and the two probes name 21 items (in-crate) and 11 items (external).
* Both probe files are edited by this plan: `src/adapter/pushdown_surface_probe_tests.rs` drops the `resolve_table_schema` import and changes its doc-comment count from "22-item" to "21-item"; `tests/pushdown_public_surface.rs` drops the `resolve_table_schema` import and changes its doc comment from "12 items … subset of that probe's 22" to "11 items … subset of that probe's 21".

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The pushdown façade drops resolve_table_schema when the shared catalog-client pipeline replaces its only caller

* *GIVEN* the frozen `crate::adapter::pushdown::<name>` baseline asserted by two compile-time probes — `src/adapter/pushdown_surface_probe_tests.rs` naming 22 items from an in-crate vantage and `tests/pushdown_public_surface.rs` naming 12 externally-`pub` items — with `resolve_table_schema` among both lists
* *WHEN* the shared `CatalogClient` listing pipeline replaces `resolve_table_schema`'s only production caller and the function is deleted
* *THEN* `resolve_table_schema` SHALL leave the façade and no other item SHALL be added, removed, narrowed, or widened, so the in-crate probe SHALL name 21 items and the external probe SHALL name 11 items, and both MUST compile
* *AND* `resolve_file_list` SHALL KEEP its name and its `pub` visibility on the façade, so the scan path's file-resolution entry point is unaffected by the deletion
* *AND* both probe doc comments SHALL state the reduced count — 21 in-crate, 11 external — because the compiler catches only narrowing, not deletion, so the count is what makes the removal visible in review
* *AND* the façade SHALL stay FROZEN after this redraw: any further change to the item set requires its own spec delta against `vs-adapter/pushdown-module-structure`
<!-- /DELTA:NEW -->

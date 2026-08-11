# Feature: Pushdown Catalog HTTP Session

Builds the catalog HTTP state — one `reqwest` client, the resolved catalog-auth strategy, and the `/v1/config` prefix — once per pushdown request and reuses it across every table's `loadTable` GET. This delta records that `resolve_table_schema` leaves the file-resolution entry-point set and that the createVirtualSchema schema-loop guarantees relocate behind the shared `CatalogClient` trait; the pushdown session-reuse mechanism itself is untouched.

## Background

* This delta records that deleting `resolve_table_schema` (plan `add-native-unity-catalog-client`, issue #318) removes it from this feature's file-resolution entry-point set and relocates the createVirtualSchema schema-loop guarantees into `IcebergRestCatalogClient::list_tables`. The mechanism moves; the enumerated tables, declared column names and types, `TABLE_MAP`, warnings, and errors stay byte-identical.
* The shared `CatalogClient` listing pipeline replaces `resolve_table_schema`'s only production caller. Its load-and-extract half moves into `IcebergRestCatalogClient::load_table` in `lakehouse-catalog`; its Exasol-mapping-and-uppercasing half moves into the shared listing pipeline. The frozen `pushdown` façade reduction is recorded by `vs-adapter/pushdown-module-structure`; the case-fold-home relocation is recorded by `vs-adapter/create-virtual-schema`.
* This delta SUPERSEDES the `*AND* resolve_table_schema SHALL likewise take &CatalogSession as its first parameter and SHALL NOT construct a session of its own` clause (line 76) of the scenario "CatalogSession is public and every file-resolution entry point takes one". After deletion, `resolve_file_list` ALONE remains as a `&CatalogSession`-taking file-resolution entry point; `resolve_table_schema` leaves that set entirely.
* This delta SUPERSEDES the scenario "createVirtualSchema resolves every table's schema on one shared session": its one-session-per-enumeration, empty-batch-no-resolution-grant, skip-non-loadable, two-grants-on-OAuth-mode (empty namespace one grant), and grant-failure-before-loop guarantees move from the adapter schema loop into `IcebergRestCatalogClient::list_tables`, now tested by `crates/lakehouse-catalog/src/client_tests.rs::enumeration_builds_exactly_one_session` and `::empty_namespace_builds_no_session_and_no_grant`.
* The pushdown catalog-HTTP session mechanism is otherwise untouched. Single-table and N-table-join session reuse, the per-table `loadTable` GET, the parse-before-config guarantee, and error-path redaction all stay as recorded. Only the schema-loop path leaves this feature.
* The compile-time proof `crates/lakehouse-engine/tests/catalog_session_signatures.rs` drops its `schema_resolution_entry_point_takes_a_shared_session` proof (and its `accepts_shared_session_for_schema_resolution` helper) and the covered-scenario doc line for the relocated scenario, keeping `file_resolution_entry_points_take_a_shared_session` pinning `resolve_file_list`. This edit is recorded by this delta, not by `vs-adapter/pushdown-module-structure`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: CatalogSession is public and every file-resolution entry point takes one

* *GIVEN* `CatalogSession` declared `pub` in the `lakehouse-catalog` crate, and the `refactor-catalog-http-session` wrapper pair `resolve_file_list(catalog_uri: &str, …)` plus `resolve_file_list_with_session(&CatalogSession, …)`
* *WHEN* any caller — the single-table pushdown path, a join leg, or an external integration test — resolves a file list
* *THEN* exactly ONE file-resolution function SHALL remain, named `resolve_file_list`, `pub`, taking `&CatalogSession` as its first parameter, and `resolve_file_list_with_session` SHALL be DELETED rather than kept as an alias
* *AND* `resolve_table_schema` SHALL be DELETED from the file-resolution entry-point set — its only production caller replaced by the shared `CatalogClient` listing pipeline — so after deletion `resolve_file_list` is the SOLE `&CatalogSession`-taking file-resolution entry point and no `resolve_table_schema` entry point remains to take a session
* *AND* `resolve_file_list` SHALL NOT retain a `catalog_uri: &str` parameter, because the session already carries the catalog URI and a second copy could disagree with it
* *AND* the caller SHALL validate every involved-table identifier BEFORE it builds the session, so a malformed identifier still issues ZERO catalog HTTP requests and returns the same parse error as before — the guarantee moves from inside `resolve_file_list` to its callers, and is not dropped
* *AND* `CatalogSession`'s fields MUST stay private and `CatalogAuth` MUST stay crate-private to `lakehouse-catalog`, so making the type public exposes no auth internals
* *AND* the external E2E callers (`tests/common/e2e_harness.rs`, `tests/e2e_scan_test.rs`) SHALL construct a `CatalogSession` themselves and pass it, which is the capability the crate extraction exists to grant; the file lists they resolve MUST be unchanged
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: createVirtualSchema resolves every table's schema on one shared session

* *GIVEN* a createVirtualSchema request over a namespace holding N Iceberg tables, whose schema resolution moves from an adapter-side loop calling `resolve_table_schema` per table into `IcebergRestCatalogClient::list_tables` in `lakehouse-catalog`
* *WHEN* the Iceberg REST catalog client enumerates the namespace and resolves every enumerated table's columns
* *THEN* the client SHALL build exactly ONE `CatalogSession` for the whole enumeration and reuse it across every table via a private session-taking load helper distinct from the trait `load_table`, so an N-table namespace costs one schema-loop grant instead of N
* *AND* an empty identifier batch (a namespace the catalog reports as holding no table) SHALL build NO resolution `CatalogSession` and perform NO resolution-phase OAuth2 grant — the resolution half of the guarantee, migrating from the removed engine test to `crates/lakehouse-catalog/src/client_tests.rs::empty_namespace_builds_no_session_and_no_grant` (which drives the private `resolve_listing(&[])` directly against an unreachable URI under OAuth2 creds) and exercised end-to-end through the public `list_tables` by `::list_tables_over_empty_namespace_lists_nothing`; the enumeration-phase grant of the next clause is UNAFFECTED, so an empty namespace still costs ONE grant under OAuth2 client-credentials and ZERO under the no-auth and static-token modes
* *AND* on the OAuth2 client-credentials mode — both `client_id` and `client_secret` set — the namespace-enumeration `RestCatalog` SHALL retain its own independent grant and `/v1/config` handshake, so such a request still performs TWO grants in total rather than one; on the static-token and no-auth modes it performs NO grant and one extra `/v1/config` lookup; on the SigV4 mode `list_namespace_tables` builds no `RestCatalog` at all
* *AND* a table the catalog reports as not a loadable Iceberg table SHALL still be skipped with the same warning and SHALL NOT abort the enumeration, routed into `CatalogListing.skipped`, so the non-Iceberg-table skip behavior is unchanged
* *AND* an OAuth2 grant failure SHALL surface once, before the per-table loop, instead of once at the first table, because the session is built ahead of the enumeration
* *AND* the enumerated tables, each table's resolved column list, and its Exasol type mapping MUST stay byte-identical to the pre-refactor per-session output, verified by `crates/lakehouse-catalog/src/client_tests.rs::enumeration_builds_exactly_one_session`
<!-- /DELTA:CHANGED -->

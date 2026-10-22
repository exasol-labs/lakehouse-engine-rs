# Plan: change-vs-adapter-per-table-notes

## Summary

Each virtual table records its own catalog identifier in its table-level `adapterNotes` as `{"name": "<catalog identifier>"}`, both pushdown paths read it from `involvedTables[i].adapterNotes`, and the schema-level `TABLE_MAP` is dropped (#425). `ALTER VIRTUAL SCHEMA ... REFRESH TABLES ...` then lists only the source's table identifiers, loads only the requested tables, returns only those, and merges `SKIPPED_TABLES` with the persisted list.

## Context

- `build_adapter_notes` (`crates/lakehouse-engine/src/adapter/mod.rs`) writes one schema-level map from each Exasol table name to its catalog identifier. The single-table pushdown (`resolve_pushdown_identifier`) and the join pushdown (`detect_join` in `adapter/pushdown/joins/planning.rs`) look the involved table up in that map through `read_table_map`.
- The map grows with the namespace, lives apart from the virtual tables it describes, and causes the "not in TABLE_MAP" error class.
- A planning-time probe (`probe/run.sh`, a Python3 adapter driven by `exapump`) ran against the Docker Exasol 2025.1.16 and 8.29.13, the two versions CI runs the local E2E suite on. Both versions gave these results:
  - Exasol persists table-level `adapterNotes` in `SYS.EXA_ALL_VIRTUAL_TABLES.ADAPTER_NOTES`.
  - Exasol returns the notes in `involvedTables[i].adapterNotes` for a single-table pushdown, a two-table join, and a self-join. A self-join lists the table once per leg.
  - For a `refresh` that carries `requestedTables`, Exasol keeps every unrequested virtual table with its notes and replaces each returned table.
  - Exasol creates a returned table that has no virtual table yet.
  - Exasol drops a requested table that the response omits.
  - Exasol accepts a requested name that matches nothing.
  - Exasol replaces the schema-level `adapterNotes` with the response's value.
  - An unquoted `REFRESH TABLES t2` arrives as `["T2"]`.
- A `refresh` request carries `type`, `requestedTables`, and `schemaMetadataInfo` with `name`, `adapterNotes`, and `properties`. It carries no virtual table and no table-level note (probe check (f), Exasol 2025.1.16). Only a listing of the source can therefore map a requested Exasol name to a catalog identifier.
- The adapter answers a partial refresh with every table today, so `REFRESH TABLES` acts as a full `REFRESH` (recorded scenario "Refresh echoes requestedTables when present"). The full listing loads the metadata of every table, and customer namespaces hold thousands of tables.
- Each catalog kind can list table identifiers without loading table metadata. Each call below was verified live during planning (#425):
  - Iceberg REST: `listTables` and `listNamespaces`, whose responses carry identifiers only. The full listing already calls them first and then loads each table (`list_namespace_tables`, `resolve_listing` in `crates/lakehouse-catalog/src/client.rs`).
  - Unity Catalog: `GET /tables` with `omit_columns`, `omit_properties`, and `omit_username` set to `true`. Databricks (`demo_sales_catalog.sales`) and OSS Unity Catalog v0.5.0 honor all three, page as before, and keep `name`, `table_type`, and `data_source_format`. Databricks' `table-summaries` endpoint is unusable: OSS answers 404, it omits `data_source_format`, and its schema filter is a `LIKE` pattern.
  - AWS Glue: `GetTables` with `AttributesToGet` `NAME` returns names only, pages with `NextToken`, and needs only `glue:GetTables`. `NAME, TABLE_TYPE` returns `EXTERNAL_TABLE`, which cannot route a table.
  - Direct storage: the one delimiter listing of the base path. The full listing already reads each directory's files after it.
- The Unity Catalog and Glue full listings read every table's metadata from one paged sweep. Glue then reads each Iceberg table's `metadata.json`. Each kind decides admission and skip reason inside its full listing today.
- `SKIPPED_TABLES` entries store catalog identifiers, and `requestedTables` carries Exasol names. The one `flatten_table_name` fold (`adapter/tables.rs`) maps an identifier to its Exasol name.
- The name-collision check (`build_table_map`) covers only loaded tables. The recorded collision scenarios name "two distinct Iceberg identifiers" and "two directories".
- #425 decides: no migration, no fallback to the old map, and no test that asserts the old key is absent. A refresh writes the per-table notes into an existing virtual schema.
- `decision-log.md` [1] is the plan's one ADR candidate. The architecture changes are in `architecture.md`.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| create-virtual-schema-adapter-notes | CHANGED | `vs-adapter/create-virtual-schema-adapter-notes/spec.md` |
| create-virtual-schema-declaration-details | CHANGED | `vs-adapter/create-virtual-schema-declaration-details/spec.md` |
| refresh-and-set-properties | CHANGED | `vs-adapter/refresh-and-set-properties/spec.md` |
| column-source-notes | CHANGED | `vs-adapter/column-source-notes/spec.md` |
| pushdown-planning-file-resolution | CHANGED | `file-planning/pushdown-planning-file-resolution/spec.md` |
| pushdown-format-neutral-resolution | CHANGED | `file-planning/pushdown-format-neutral-resolution/spec.md` |
| pushdown-planning-join-fallback | CHANGED | `pushdown-joins/pushdown-planning-join-fallback/spec.md` |
| pushdown-catalog-session | CHANGED | `catalog/pushdown-catalog-session/spec.md` |
| unity-catalog-client | CHANGED | `unity-catalog/unity-catalog-client/spec.md` |
| unity-catalog-client-parquet-admission | CHANGED | `unity-catalog/unity-catalog-client-parquet-admission/spec.md` |
| unity-catalog-create-virtual-schema | CHANGED | `unity-catalog/unity-catalog-create-virtual-schema/spec.md` |
| glue-catalog-client | CHANGED | `glue/glue-catalog-client/spec.md` |
| direct-storage-table-discovery | CHANGED | `direct-storage/direct-storage-table-discovery/spec.md` |
| glue-table-planning | CHANGED | `glue/glue-table-planning/spec.md` |

## Impact

- **Breaking:** a virtual schema created before this change fails every pushdown until it is refreshed once. The error names the virtual table and `ALTER VIRTUAL SCHEMA ... REFRESH`. One `ALTER VIRTUAL SCHEMA <name> REFRESH` per virtual schema fixes it, and dependent views and grants survive. A `REFRESH TABLES` fixes only the tables it names. A downgrade to an older adapter needs one `REFRESH` under that adapter, because the older adapter reads the old map.
- **Behavior change:** `ALTER VIRTUAL SCHEMA ... REFRESH TABLES <t>` refreshes only the named tables. It used to refresh every table. A named table the source no longer serves, or now skips, loses its virtual table. A `REFRESH TABLES` creates a named table that has no virtual table yet. Run a plain `REFRESH` to pick up every new table or to refresh everything.
- `REFRESH TABLES` lists the namespace's table names and loads the metadata of the named tables only. Its cost no longer grows with the metadata of every table. A table that fails to load, or a name collision among tables, no longer fails it unless the statement names that table or that name.
- After a partial refresh, `SKIPPED_TABLES` keeps the entries of unrequested tables the source still lists, so a skip reason can be stale until the next full `REFRESH`. `SKIPPED_TABLES_OMITTED` is an upper bound until then.
- Each virtual table's catalog identifier is readable from `SYS.EXA_ALL_VIRTUAL_TABLES.ADAPTER_NOTES`.

## Migration

| Current | New |
|---------|-----|
| Schema-level `adapterNotes` key `TABLE_MAP` maps Exasol names to catalog identifiers | Each virtual table's `adapterNotes` is `{"name": "<catalog identifier>"}`. Nothing reads or writes the old key. An inherited one stays inert in the schema-level notes |
| Pushdown of a table missing from the map fails with "not in TABLE_MAP; drop and recreate the virtual schema" | Pushdown of a table without a readable note fails naming the table and `ALTER VIRTUAL SCHEMA ... REFRESH` |
| `REFRESH TABLES` loads and re-declares every table | `REFRESH TABLES` lists table names, then loads and re-declares only the requested tables. Every other virtual table keeps its declaration |

## Implementation Tasks

### 1. Per-table identifier note

- [ ] 1.1 Run `specs/_plans/change-vs-adapter-per-table-notes/probe/run.sh` against the Docker Exasol (`docker compose up -d --wait exasol`) before any code change. Confirm that checks (a) to (f) print what the Context section states, on the default image and with `EXASOL_IMAGE=exasol/docker-db:8.29.13` in a separate container. If check (b) shows Exasol dropping T1 or T3, or check (f) shows the request carrying virtual tables, stop before group B and return the plan to planner-agent. Planner-agent then reverts the partial-refresh artifacts before implementation continues: the partial-refresh scenarios and Background lines of `vs-adapter/refresh-and-set-properties`, the `requestedTables` scoping in `vs-adapter/create-virtual-schema-adapter-notes` and `vs-adapter/column-source-notes`, the removal of "Refresh echoes requestedTables when present", the deltas of `unity-catalog/unity-catalog-client`, `unity-catalog/unity-catalog-client-parquet-admission`, and `glue/glue-catalog-client`, the scoped-refresh scenarios of `catalog/pushdown-catalog-session` and `direct-storage/direct-storage-table-discovery`, decisions [2], [3], and [5], and the `REFRESH TABLES` and identifier-listing lines of the architecture delta. Record the outcome as a comment on the plan PR.
- [ ] 1.2 Add `crates/lakehouse-engine/src/adapter/table_identity.rs` (decision [4]) with unit tests in `table_identity_tests.rs`. It encodes the note for a `CatalogTableIdent` as the JSON string `{"name": catalog_identifier_string(ident)}`. It reads the identifier from one `involvedTables` entry and ignores every other member. A missing note, a note that is JSON `null`, a note that is not a JSON object, or a missing or non-string `name` fails with a `UdfError::User` that names the virtual table and `ALTER VIRTUAL SCHEMA ... REFRESH`. Unit tests: `a_missing_or_unreadable_note_fails_naming_the_table_and_refresh`, `members_other_than_name_are_ignored`.
- [ ] 1.3 In `build_listing_virtual_tables`, attach each table's identifier note as `adapterNotes` on its `tables[]` entry. Move the name-collision check of `build_table_map` into one function in `adapter/tables.rs`, next to `flatten_table_name`, with the same error text. It takes the identifiers of the loaded tables: `build_listing_virtual_tables` passes the tables the listing admits, as `build_table_map` does today, and group B's partial refresh reaches it through `build_listing_virtual_tables` over the tables its load admits. Stop writing the map in `build_adapter_notes`, and delete `NOTE_TABLE_MAP`, `build_table_map`, and `read_table_map`.
- [ ] 1.4 Make `resolve_pushdown_identifier` read the identifier from `involvedTables[0]` through `table_identity`, for every catalog kind, including the Unity Catalog path of `file-planning/pushdown-format-neutral-resolution`.
- [ ] 1.5 Make `detect_join` take each leaf's identifier through `table_identity`, from the `involvedTables` entry whose `name` equals the leaf's table name. A self-join's legs read the same entry. A leaf without a readable note returns the `table_identity` error, never `JoinShape::Ineligible`.
- [ ] 1.6 Rewrite the unit tests that build or assert the old map, so the identifier travels in `involvedTables[i].adapterNotes` and in each response table's `adapterNotes`. Files: `adapter/adapter_tests.rs`, `adapter/unity_schema_tests.rs`, `adapter/pushdown/pushdown_tests.rs`, `adapter/pushdown/dispatch_golden_tests.rs`, and `adapter/pushdown/joins/{joins,planning,rendering,sql_builders}_tests.rs`. Declare one shared fixture helper in the pushdown test support module. Keep every golden SQL string unchanged. Delete `read_table_map_absent_returns_empty` and `table_map_round_trips_through_adapter_notes`. Rename `pushdown_unknown_involved_table_errors` to `pushdown_without_an_identifier_note_fails_naming_refresh`, `records_table_map_and_rejects_collision` to `records_identifier_notes_and_rejects_collision`, and `table_map_records_the_bare_directory_name_and_round_trips` to `the_identifier_note_records_the_bare_directory_name_and_round_trips`. Add `each_leg_reads_its_identifier_from_its_own_involved_table` and `a_leg_without_an_identifier_note_is_err` to `joins/planning_tests.rs`. Add no assertion that the old key is absent.
- [ ] 1.7 Add the unit test `refresh_writes_an_identifier_note_on_every_returned_table` in `adapter_tests.rs`: the response to a `refresh` request carries an identifier note on every returned table, and an unrelated schema-level entry survives. Extend the existing refresh tests in `adapter_tests.rs` that assert the old map.
- [ ] 1.8 Add one shared reader `identifier_note(conn, vs_name, table) -> Option<String>` next to `skip_reason` in `tests/common/e2e_harness.rs`. It returns the `name` member of the table's `SYS.EXA_ALL_VIRTUAL_TABLES.ADAPTER_NOTES`. Every identifier-note assertion below calls it, and no test parses the note itself. Add no new E2E test for the note. Extend these existing tests instead:
  - `refresh_reenumerates_namespace` (`tests/e2e_local/refresh.rs`): after `CREATE`, `T_ORIG`'s note names `e2e_refresh_reenum.t_orig`. After `REFRESH`, both `T_ORIG` and the new `T_NEW` carry their notes.
  - `refresh_flatten_collision_returns_same_error_as_create`: before the collision is introduced, `EU__ORDERS`'s note names the multi-level identifier `<namespace>.eu.orders`.
  - `non_ascii_table_and_column_stay_queryable` (`tests/e2e_local/non_ascii_identifier.rs`): `STRASSE`'s note names the identifier ending in `straße`, so the note round-trips non-ASCII JSON.
  - `unity_create_virtual_schema_lists_fixture_tables_and_columns` (`tests/e2e_unity_test.rs`): each fixture table's note names its `catalog.schema.table` identifier.
  - The `listing` check of `glue_read_only_checks_pass_against_one_provisioned_fixture` (`tests/e2e_glue_test.rs`): each routed table's note names its Glue identifier.

  The direct-storage note is asserted by the test of 3.5.
- [ ] 1.9 Update the comments and doc comments that name the old map: `adapter/pushdown/scan_resolution.rs` (`resolve`), `adapter/pushdown/joins/planning.rs` (`JoinLeaf::table_identifier`, `detect_join`), `crates/lakehouse-catalog/src/client.rs` (`catalog_identifier_string`), and `tests/e2e_local/non_ascii_identifier.rs`. No existing test carries the `/// Scenario:` tag of a scenario this plan renames or removes. Each new or renamed test carries the tag of the scenario it implements, quoting the title from its delta, for example `/// Scenario: Create virtual schema records each table's catalog identifier in the table's adapterNotes`.
- [ ] 1.10 In `tests/e2e_local/refresh.rs`, rewrite `refresh_partial_requested_tables_still_refreshes_whole_namespace` into `refresh_tables_heals_and_refreshes_only_the_requested_tables`. It keeps the `NS_PARTIAL` fixture and its VS, and section 3 extends it, so the upgrade and the partial refresh share one fixture and one virtual schema. In group A it runs these steps:
  1. It resets the fixture: `table_one`, `table_two`, and `table_three`, each recreated empty of later changes.
  2. It creates a test-local `PYTHON3` adapter script. The script declares the three tables with their columns and writes no table-level note.
  3. It creates `REFRESH_PARTIAL_VS` with that script and the engine's properties for `NS_PARTIAL`.
  4. It replaces the script with the engine adapter through the harness helper that creates the adapter script, pointed at the same script name, so the statement is `CREATE OR REPLACE ... ADAPTER SCRIPT`.
  5. It asserts that a single-table query on `TABLE_ONE` and a join of `TABLE_ONE` and `TABLE_TWO` fail with the error that names the table and `ALTER VIRTUAL SCHEMA ... REFRESH`.
  6. It runs `ALTER VIRTUAL SCHEMA ... REFRESH`, then asserts that both queries return rows and that each table's `identifier_note` names its catalog identifier.

  The step-5 assertion holds only if both pushdown paths read the identifier note before any column note, as 1.4 and 1.5 implement. The Python adapter writes no column notes either. Drop the old test's assertion that `TABLE_TWO` gains `NEW_COL`.
- [ ] 1.11 Update the documentation of the identifier note. In `docs/catalogs.md`, state that each virtual table's catalog identifier is in `SYS.EXA_ALL_VIRTUAL_TABLES.ADAPTER_NOTES`, and remove the old map from the size-cap paragraph of "Reading SKIPPED_TABLES". In the upgrade paragraph of `docs/install.md`, state that each existing virtual schema needs one `ALTER VIRTUAL SCHEMA <name> REFRESH` after the upgrade, and that a downgrade needs one `REFRESH` under the older adapter.
- [ ] 1.12 In the "AdapterNotes" row of the `specs/mission.md` Domain Glossary, replace "table map, skipped tables, and tuning values" with "skipped tables and tuning values at schema level, one identifier note per virtual table, and one note per column".

### 2. Identifier-only catalog listing

- [ ] 2.1 Add `list_table_idents(namespace)` and `load_listed_tables(idents)` to `CatalogClient` in `crates/lakehouse-catalog/src/client.rs` (decision [5]). The doc comments state that the first returns every identifier the full listing would return or skip, in listing order, and loads no table's metadata, and that the second admits or skips exactly the given identifiers as `list_tables` would and loads no other table. Implement both on `FixedCatalogClient` (`client_tests.rs`). Extend `catalog_client_trait_and_neutral_types_are_reachable` in `crates/lakehouse-catalog/tests/catalog_public_surface.rs` to call both through a boxed client, for the scenario "The Unity Catalog session is reached only through the shared catalog-client trait".
- [ ] 2.2 Iceberg REST (`IcebergRestCatalogClient`): `list_table_idents` returns `list_namespace_tables` as neutral identifiers, and `load_listed_tables` is `resolve_listing`, which takes neutral identifiers and keeps its one session, its empty-batch shortcut, and its 404 skip (`vs-adapter/create-virtual-schema`, scenarios "One non-Iceberg table in the namespace is skipped rather than aborting the schema" and "A non-404 per-table load failure aborts createVirtualSchema"). `list_tables` becomes the two calls in sequence.
- [ ] 2.3 Unity Catalog (`crates/lakehouse-catalog/src/unity/client.rs`): extract the admission of one `TableInfo` (`admission`, then `neutral_table` or a `SkippedTable`) into one function that `list_tables` and `load_listed_tables` call. `list_table_idents` pages `GET /tables` with `omit_columns`, `omit_properties`, and `omit_username` set to `true`, from one pure function that builds the query pairs; unit test `identifier_listing_query_omits_columns_properties_and_username`. `load_listed_tables` issues `get_table_info` per given identifier. Scope the `omit_columns` comment in `list_table_infos` to the full listing. [expert]
- [ ] 2.4 Glue (`crates/lakehouse-catalog/src/glue/client.rs`): extract the routing of one `Table` (`route_of`, then a `SkippedTable` or `listed_table`, which reads an Iceberg table's metadata file) into one function that `list_tables` and `load_listed_tables` call, with `METADATA_READ_CONCURRENCY` in both. `list_table_idents` pages `GetTables` with `attributes_to_get(TableAttributes::Name)` under the existing continuation rule. `load_listed_tables` calls `GetTable` per given identifier. `load_table` for pushdown stays unchanged. [expert]
- [ ] 2.5 Direct storage (`crates/lakehouse-engine/src/adapter/direct_storage.rs`): `list_table_idents` returns the sorted first-level directory names of the one delimiter listing. Extract the resolution of one directory (`resolve_parquet_directory`, then the `NoDataFile` skip or the `CatalogTable`) into one function that `load_listed_tables` calls per given directory. `list_tables` becomes the two calls in sequence.

### 3. Partial refresh

- [ ] 3.1 Add `crates/lakehouse-engine/src/adapter/skipped_tables.rs` (decision [4]) with unit tests in `skipped_tables_tests.rs`. Move `insert_skipped_tables`, `fit_skipped_tables`, and `omitted_entry_len` from `adapter/mod.rs` into it, keeping the names of the tests that cover them. Add the reader of the persisted `SKIPPED_TABLES` entries and `SKIPPED_TABLES_OMITTED` count from `schemaMetadataInfo.adapterNotes`.
- [ ] 3.2 Add `crates/lakehouse-engine/src/adapter/scoped_refresh.rs` (decision [4]) with unit tests in `scoped_refresh_tests.rs`. `RequestedTables` is built once from `requestedTables`, and its `contains(exasol_name)` is the only test of whether a name is requested. A pure function selects every listed identifier whose `flatten_table_name` is requested, so a requested name that several identifiers flatten to selects all of them (decision [2]). A pure merge walks the listed identifiers in listing order: a requested identifier contributes the skip entry the load gave it, and an unrequested one contributes its persisted entry, matched by catalog identifier string. The merge caps the result through `skipped_tables`, adds the persisted `SKIPPED_TABLES_OMITTED` count to the cap's count, and writes no count when the sum is 0. Unit tests: `only_requested_identifiers_are_selected_for_loading`, `every_identifier_that_flattens_to_a_requested_name_is_selected`, `a_requested_name_matching_no_identifier_selects_nothing`, `merge_keeps_unrequested_listed_entries_in_listing_order`, `merge_drops_entries_the_source_no_longer_lists`, `merge_carries_the_persisted_omitted_count`, `merge_writes_no_omitted_count_when_the_sum_is_zero`. [expert]
- [ ] 3.3 In `handle_create_virtual_schema`, answer a `refresh` that carries `requestedTables` with `list_table_idents`, the selection of 3.2, `load_listed_tables` over the selection, `build_listing_virtual_tables` over that listing, which runs the collision function of 1.3 over the tables the load admits, and the merged skip list of 3.2. The partial refresh reads no identifier note. `setProperties` and a `refresh` without `requestedTables` keep `list_tables`. Keep the echo in `build_schema_response`, and rewrite its doc comment to state the scoping Exasol applies.
- [ ] 3.4 In `tests/e2e_local/refresh.rs`, extend `refresh_tables_heals_and_refreshes_only_the_requested_tables` (1.10). Add no new test.
  - The fixture reset of step 1 drops `table_three` with `drop_table`, ignoring a missing table, and not with `drop_table_if_exists`. The local REST fixture answers both `loadTable` and the table-exists check of a corrupt table with HTTP 500 (verified during planning).
  - Between steps 5 and 6, `table_one` and `table_two` gain `new_col`, and the test runs `REFRESH TABLES TABLE_ONE`. Then:
    - `TABLE_ONE` declares `NEW_COL`, returns its row, and carries its note.
    - `TABLE_TWO` declares no `NEW_COL`.
    - `TABLE_TWO`'s query still fails naming `ALTER VIRTUAL SCHEMA ... REFRESH`.
    - The join fails naming `TABLE_TWO`, the leg without a note.
  - After step 6, `TABLE_TWO` declares `NEW_COL`. Then the test overwrites `table_three`'s current metadata file with invalid JSON through `put_fixture_object` (`tests/common/raw_parquet.rs`). A full `REFRESH` fails, `REFRESH TABLES TABLE_ONE` succeeds, and `TABLE_THREE` keeps its declaration.
  - The test drops `table_two` from the catalog and runs `REFRESH TABLES TABLE_TWO, NO_SUCH_TABLE`. The statement succeeds. `TABLE_TWO` is gone, `TABLE_ONE` stays queryable, and `TABLE_THREE` stays declared.

  Extend `refresh_flatten_collision_returns_same_error_as_create` with a `REFRESH TABLES BASELINE` step, which succeeds while `EU__ORDERS` collides, and a `REFRESH TABLES EU__ORDERS` step, which fails with the collision error. Extend `non_ascii_table_and_column_stay_queryable` with `REFRESH TABLES STRASSE`, which selects the identifier `straße` through the `ß`-to-`SS` fold and keeps the table queryable. Extend `refresh_and_set_properties_rebuild_column_notes_from_the_source` with a `REFRESH TABLES` step that names the evolved table. Rewrite the comments that name the old map.
- [ ] 3.5 In `tests/e2e_local/direct_storage.rs`, prove the partial refresh mostly by extending `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`. Its final state already has `t_conflict` failing a full `REFRESH`, so the test gains a stage B there:
  - `write_refresh_initial_objects` adds four directories that hold only a `_SUCCESS` object, so the listing skips them: `t_skip_req`, `t_skip_unreq`, `t_gone_req`, and `t_gone_unreq`. The existing stage-A assertions do not change, because a skipped directory serves no table.
  - Stage B makes these changes:
    - `t_append` gains a file with the column `EXTRA`.
    - `t_emptied` gains a Parquet file, so it loads.
    - `t_new` loses its data file, so the listing now skips it, and the request leaves it out.
    - `t_gone_req` and `t_gone_unreq` lose their objects and their directory key (`delete_fixture_object` on the bare directory key). SeaweedFS keeps listing an emptied directory until that key is deleted (verified during planning).
  - Stage B then runs `REFRESH TABLES T_APPEND, T_EMPTIED, T_SKIP_REQ, T_GONE_REQ` and asserts:
    - The statement succeeds, although a full `REFRESH` fails on `t_conflict`.
    - `T_APPEND` declares `EXTRA`.
    - `T_EMPTIED` is created and reads its rows.
    - `T_CONFLICT` keeps its declaration and `SELECT ID` still reads.
    - `T_NEW` stays declared.
    - Among the fixture's own directories, `SKIPPED_TABLES` holds the entries for `t_skip_req` and `t_skip_unreq`, in that order, and none for `t_emptied`, `t_gone_req`, `t_gone_unreq`, or `t_new`. The assertion is limited to these directories, because SeaweedFS keeps the emptied directories of earlier runs (`specs/testing.md` § Fixtures).
  - Stage B finally deletes the objects of `t_hive` and runs `REFRESH TABLES T_HIVE`. `T_HIVE` is gone, and `SKIPPED_TABLES` holds `t_hive` with the reason `holds no data file`.
  - Each `SKIPPED_TABLES` read goes through one shared reader `skipped_entries(conn, vs_name) -> Vec<(String, String)>` in `tests/common/e2e_harness.rs`. `skip_reason` and the `listing` check of `tests/e2e_glue_test.rs` switch to it and drop their own JSON parsing.

  Add one new test, `refresh_tables_resolves_a_requested_name_as_a_full_refresh_would`, under its own base path `direct_refresh_names/`, because its collision fails every full `REFRESH` of its base path (`specs/testing.md` § Fixtures). It reuses `put_refresh_file`-style helpers parameterized by base path, not a copy of them. It follows the steps of the scenario "A partial refresh resolves each requested name as a full refresh would". It also asserts these direct-storage identifier notes:
  - after `CREATE`, `SALES` names `Sales` and `OTHER` names `other`;
  - after the last `REFRESH TABLES SALES`, `SALES` names `sales`;
  - a final full `REFRESH` succeeds and keeps `sales` and `other`.
- [ ] 3.6 Extend `unity_create_virtual_schema_lists_fixture_tables_and_columns` (`tests/e2e_unity_test.rs`) and the `listing` check of the Glue fixture test (`tests/e2e_glue_test.rs`) with one `REFRESH TABLES` step each. Before the step, the test reads the named table's column notes through `column_adapter_notes`. After the step, it asserts that they are equal, that the named table keeps its identifier note, and that every other virtual table keeps its declaration and stays queryable.
  - The Unity Catalog step names one fixture table and `NO_SUCH_TABLE`.
  - The Glue step names `ICEBERG_ORDERS` and the skipped `ORC_TABLE`. `ORC_TABLE` stays undeclared, and `skipped_entries` still holds its reason.
- [ ] 3.7 Add a final mutating step to the Glue fixture test, and rename the test `glue_checks_pass_against_one_provisioned_fixture`, because it no longer only reads. Add no `provision()` run. The step runs after the read-only checks, because it makes every later full `REFRESH` of the fixture fail:
  - It registers a Glue Iceberg table whose `metadata_location` names a missing object, through the table writer of `tests/common/glue.rs`.
  - A full `REFRESH` fails naming that table.
  - `REFRESH TABLES` of one fixture table succeeds.

  The run's `Drop` deletes the broken table with the run's database. The step joins the `failed` list like the read-only checks, so a failure still reports every check.
- [ ] 3.8 Update `docs/catalogs.md`. In "Reading SKIPPED_TABLES", state the partial-refresh merge and that `SKIPPED_TABLES_OMITTED` is an upper bound after a partial refresh, until the next full `REFRESH`. Add a paragraph on `REFRESH TABLES`: it lists table names, loads only the named tables, re-declares only them, and is not failed by a broken unnamed table or by a name collision among unnamed tables. The paragraph also states that a `REFRESH TABLES` after the upgrade writes identifier notes on the named tables only, so the upgrade needs one plain `REFRESH`.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Identifier note | 1.1-1.12 | none | spec deltas `vs-adapter/create-virtual-schema`, `vs-adapter/create-virtual-schema-declaration-details`, `file-planning/pushdown-planning-file-resolution`, `file-planning/pushdown-format-neutral-resolution`, `pushdown-joins/pushdown-planning-join-fallback`, `unity-catalog/unity-catalog-create-virtual-schema`, `direct-storage/direct-storage-table-discovery` (identifier-note scenarios), `glue/glue-table-planning`, and the "Refresh rebuilds every identifier note" scenario of `vs-adapter/refresh-and-set-properties`; `crates/lakehouse-engine/src/adapter/{mod.rs,table_identity.rs,tables.rs}`, `adapter/pushdown/joins/planning.rs`, `adapter/pushdown/scan_resolution.rs`, `crates/lakehouse-catalog/src/client.rs` (doc comment), the unit test files of 1.6, `tests/common/e2e_harness.rs`, `tests/e2e_local/{refresh.rs,non_ascii_identifier.rs}`, `tests/e2e_unity_test.rs`, `tests/e2e_glue_test.rs`, `docs/catalogs.md`, `docs/install.md`, `specs/mission.md`, `specs/_plans/change-vs-adapter-per-table-notes/probe/` |
| B: Partial refresh | 2.1-3.8 | A (shares `adapter/mod.rs`, `adapter/tables.rs`, `adapter_tests.rs`, `crates/lakehouse-catalog/src/client.rs`, `tests/common/e2e_harness.rs`, `tests/e2e_local/{refresh.rs,non_ascii_identifier.rs}`, `tests/e2e_unity_test.rs`, `tests/e2e_glue_test.rs`, `docs/catalogs.md`) | spec deltas `vs-adapter/refresh-and-set-properties` (partial-refresh scenarios), `vs-adapter/create-virtual-schema-adapter-notes`, `vs-adapter/column-source-notes`, `catalog/pushdown-catalog-session`, `unity-catalog/unity-catalog-client`, `unity-catalog/unity-catalog-client-parquet-admission`, `glue/glue-catalog-client`, `direct-storage/direct-storage-table-discovery` (the scoped-refresh scenario), and the identifier-listing constraint of `architecture.md` § Constraints; `crates/lakehouse-catalog/src/{client.rs,unity/client.rs,glue/client.rs}` and their `_tests.rs` files, `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, `crates/lakehouse-engine/src/adapter/{mod.rs,direct_storage.rs,skipped_tables.rs,scoped_refresh.rs}`, `tests/common/{e2e_harness.rs,glue.rs,raw_parquet.rs}`, `tests/e2e_local/{refresh.rs,non_ascii_identifier.rs,direct_storage.rs}`, `tests/e2e_unity_test.rs`, `tests/e2e_glue_test.rs`, `specs/testing.md` § Fixtures, `docs/catalogs.md` |

The groups run in sequence. Group B starts after every group A task is checked off, because both edit `handle_create_virtual_schema` and the same test and docs files, and group B reaches the collision function of 1.3 through `build_listing_virtual_tables`. Group B is one vertical slice: the identifier-only listing of section 2 exists only for the partial refresh of section 3, and section 3's E2E tests prove section 2's scenarios.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Constant | `crates/lakehouse-engine/src/adapter/mod.rs` `NOTE_TABLE_MAP` | The schema-level map is no longer written or read |
| Function | `crates/lakehouse-engine/src/adapter/mod.rs` `read_table_map` | Replaced by `table_identity` |
| Function | `crates/lakehouse-engine/src/adapter/mod.rs` `build_table_map` | The collision check moves into `adapter/tables.rs`; the map is gone |
| Test | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` `read_table_map_absent_returns_empty`, `table_map_round_trips_through_adapter_notes` | Test a removed function |
| Test | `crates/lakehouse-engine/tests/e2e_local/refresh.rs` `refresh_partial_requested_tables_still_refreshes_whole_namespace` | Rewritten as `refresh_tables_heals_and_refreshes_only_the_requested_tables` on the same fixture. Its assertion that `TABLE_TWO` gains `NEW_COL` becomes false |
| Test name | `crates/lakehouse-engine/tests/e2e_glue_test.rs` `glue_read_only_checks_pass_against_one_provisioned_fixture` | Renamed `glue_checks_pass_against_one_provisioned_fixture`, because its last step mutates the fixture |
| Duplicate parsing | `crates/lakehouse-engine/tests/common/e2e_harness.rs` `skip_reason`, and the `SKIPPED_TABLES` parsing in the `listing` check of `tests/e2e_glue_test.rs` | Both read through the shared `skipped_entries` |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Create virtual schema enumerates every table in the configured namespace | E2E | `crates/lakehouse-engine/tests/e2e_local/scan.rs` ; `crates/lakehouse-engine/tests/e2e_local/refresh.rs` | `e2e_create_vs_enumerates_namespace_tables`; `refresh_reenumerates_namespace` (the identifier notes after `CREATE`) |
| One non-Iceberg table in the namespace is skipped rather than aborting the schema | Unit | `crates/lakehouse-catalog/src/client_tests.rs` ; `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `unloadable_table_is_reported_skipped_not_failed`; `skipped_tables_are_recorded_with_reasons_for_every_kind` |
| A namespace whose every table is non-Iceberg yields an empty virtual schema | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `an_all_skipped_namespace_creates_an_empty_schema_recording_every_skip` |
| Create virtual schema records each table's catalog identifier in the table's adapterNotes | E2E | `crates/lakehouse-engine/tests/e2e_local/refresh.rs` ; `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` | `refresh_reenumerates_namespace`, `refresh_flatten_collision_returns_same_error_as_create` (multi-level identifier); `refresh_tables_resolves_a_requested_name_as_a_full_refresh_would` (directory names) |
| createVirtualSchema adapterNotes omit the cluster node count | E2E | `crates/lakehouse-engine/tests/e2e_local/scan.rs` | `create_vs_omits_cluster_nodes_from_adapter_notes` |
| Every skipped table is recorded with its reason under every catalog kind | E2E + Unit | `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` ; `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`; `skipped_tables_are_recorded_with_reasons_for_every_kind` |
| A listing with no skip records an empty list that replaces the previous one | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `a_listing_without_skips_records_an_empty_list_replacing_the_previous` |
| A namespace whose every table is skipped still creates an empty virtual schema | Unit | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `an_all_skipped_namespace_creates_an_empty_schema_recording_every_skip` |
| A skipped-table list too long for adapterNotes is capped to the longest prefix that fits | Unit | `crates/lakehouse-engine/src/adapter/skipped_tables_tests.rs` | `an_oversized_skipped_list_is_capped_to_the_longest_prefix_that_fits_the_exasol_limit`; `a_skipped_list_that_fits_keeps_every_entry_and_drops_a_stale_omitted_count` |
| Multi-level Iceberg namespaces flatten deterministically into Exasol table names | E2E | `crates/lakehouse-engine/tests/e2e_local/refresh.rs` ; `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` | `refresh_flatten_collision_returns_same_error_as_create`; `refresh_tables_resolves_a_requested_name_as_a_full_refresh_would` (the `CREATE` step: a skipped identifier collides with no loaded one in the shared check) |
| A non-ASCII Iceberg table and column name stay queryable end to end | E2E | `crates/lakehouse-engine/tests/e2e_local/non_ascii_identifier.rs` | `non_ascii_table_and_column_stay_queryable` (extended with the non-ASCII identifier note and `REFRESH TABLES STRASSE`) |
| createVirtualSchema reads the database version once and threads the resolved precision | E2E | `crates/lakehouse-engine/tests/e2e_local/timestamp_precision.rs` | `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` |
| Refresh re-enumerates the namespace and returns a refresh response | E2E | `crates/lakehouse-engine/tests/e2e_local/refresh.rs` | `refresh_reenumerates_namespace` |
| Refresh reflects table and column structure changes | E2E | `crates/lakehouse-engine/tests/e2e_local/refresh.rs` ; `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` | `refresh_reflects_added_table_and_column_change`; `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` |
| Refresh rebuilds every identifier note and preserves other adapter notes | E2E + Unit | `crates/lakehouse-engine/tests/e2e_local/refresh.rs` ; `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `refresh_tables_heals_and_refreshes_only_the_requested_tables` (the upgrade heal), `refresh_reenumerates_namespace`; `refresh_writes_an_identifier_note_on_every_returned_table` (the preserved unknown entry) |
| A partial refresh returns only the requested tables and keeps the others | E2E | `crates/lakehouse-engine/tests/e2e_local/refresh.rs` | `refresh_tables_heals_and_refreshes_only_the_requested_tables` (`REFRESH TABLES TABLE_ONE` before the full `REFRESH`) |
| A partial refresh drops a requested virtual table the source no longer serves | E2E | `crates/lakehouse-engine/tests/e2e_local/refresh.rs` ; `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` | `refresh_tables_heals_and_refreshes_only_the_requested_tables` (`REFRESH TABLES TABLE_TWO, NO_SUCH_TABLE`); `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` (`REFRESH TABLES T_HIVE`) |
| A partial refresh merges SKIPPED_TABLES with the persisted list | E2E + Unit | `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` ; `crates/lakehouse-engine/src/adapter/scoped_refresh_tests.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` (stage B); `merge_carries_the_persisted_omitted_count`, `merge_writes_no_omitted_count_when_the_sum_is_zero` (cap arithmetic) |
| A partial refresh resolves each requested name as a full refresh would | E2E + Unit | `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` ; `crates/lakehouse-engine/tests/e2e_local/refresh.rs` ; `crates/lakehouse-engine/src/adapter/scoped_refresh_tests.rs` | `refresh_tables_resolves_a_requested_name_as_a_full_refresh_would`; `refresh_flatten_collision_returns_same_error_as_create` (the `REFRESH TABLES` steps), `non_ascii_table_and_column_stay_queryable` (`STRASSE` selects `straße`); `every_identifier_that_flattens_to_a_requested_name_is_selected` |
| A partial refresh loads the metadata of the requested tables only | E2E + Unit | `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` ; `crates/lakehouse-engine/tests/e2e_local/refresh.rs` ; `crates/lakehouse-engine/tests/e2e_glue_test.rs` ; `crates/lakehouse-engine/src/adapter/scoped_refresh_tests.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` (stage B, `t_conflict`); `refresh_tables_heals_and_refreshes_only_the_requested_tables` (the corrupt `table_three`); `glue_checks_pass_against_one_provisioned_fixture` (the broken-table step); `only_requested_identifiers_are_selected_for_loading` (the adapter's selection) |
| Refresh and set properties rebuild every column note from the source | E2E | `crates/lakehouse-engine/tests/e2e_local/refresh.rs` | `refresh_and_set_properties_rebuild_column_notes_from_the_source` (extended with a `REFRESH TABLES` step) |
| Pushdown derives the scanned Iceberg table from the involved virtual table | E2E + Unit | `crates/lakehouse-engine/tests/e2e_local/scan.rs` ; `crates/lakehouse-engine/tests/e2e_local/refresh.rs` ; `crates/lakehouse-engine/src/adapter/table_identity_tests.rs` | `e2e_pushdown_scans_table_from_involved_tables`; `refresh_tables_heals_and_refreshes_only_the_requested_tables`; `a_missing_or_unreadable_note_fails_naming_the_table_and_refresh`, `members_other_than_name_are_ignored` |
| Pushdown resolves the file list once and builds a scan-driving query | E2E | `crates/lakehouse-engine/tests/e2e_local/scan.rs` | `e2e_projection_filter_limit_returns_correct_rows` |
| Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent | Unit | `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` ; `crates/lakehouse-catalog/src/namespace_tests.rs` | `a_recorded_identifier_recovers_its_namespace_segments_and_table_name`; `parse_table_ident_handles_multilevel_namespace` |
| A Unity Catalog table's identity survives the round trip from the involved table | E2E + Unit | `crates/lakehouse-engine/tests/e2e_unity_test.rs` ; `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` ; `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` ; `crates/lakehouse-engine/src/adapter/table_identity_tests.rs` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` (extended, its queries resolve through the `catalog.schema.table` notes); `unity_table_identity_round_trips_through_the_recorded_identifier`; `a_malformed_unity_identifier_is_refused_in_unity_terms_before_any_catalog_contact`; `a_missing_or_unreadable_note_fails_naming_the_table_and_refresh` |
| A join outside the broadcast contract is declined safely | E2E | `crates/lakehouse-engine/tests/e2e_local/join.rs` | `e2e_above_threshold_unaccelerated_fallback_shape` |
| Each join leg resolves its catalog identifier from its own identifier note | E2E + Unit | `crates/lakehouse-engine/tests/e2e_local/join.rs` ; `crates/lakehouse-engine/tests/e2e_local/refresh.rs` ; `crates/lakehouse-engine/src/adapter/pushdown/joins/planning_tests.rs` | `e2e_broadcast_join_result_correct`, `e2e_self_join_on_primitive_column_matches_single_node`; `refresh_tables_heals_and_refreshes_only_the_requested_tables` (the join fails naming `TABLE_TWO`); `a_leg_without_an_identifier_note_is_err` |
| A scoped refresh lists Iceberg identifiers and loads the requested tables on one session | E2E + Unit | `crates/lakehouse-engine/tests/e2e_local/refresh.rs` ; `crates/lakehouse-catalog/src/client_tests.rs` | `refresh_tables_heals_and_refreshes_only_the_requested_tables` (including the corrupt `table_three`); `enumeration_builds_exactly_one_session` (one session), `empty_namespace_builds_no_session_and_no_grant` (empty batch) |
| The Unity Catalog session is reached only through the shared catalog-client trait | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `catalog_client_trait_and_neutral_types_are_reachable` |
| A scoped refresh lists Unity table names without columns and loads only the requested tables | E2E + Unit | `crates/lakehouse-engine/tests/e2e_unity_test.rs` ; `crates/lakehouse-catalog/src/unity/client_tests.rs` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` (extended); `identifier_listing_query_omits_columns_properties_and_username` |
| The list request stays within the Unity Catalog wire contract | Unit | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `lists_tables_in_catalog_schema` |
| Create virtual schema enumerates every table in the configured Unity Catalog namespace | E2E | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` |
| Create virtual schema includes managed and external Delta base tables, including a shallow clone | Unit | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `lists_managed_external_shallow_clone_and_parquet_base_tables` |
| Both catalog kinds enumerate through one shared listing pipeline | E2E | `crates/lakehouse-engine/tests/e2e_local/scan.rs` ; `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `e2e_create_vs_enumerates_namespace_tables`; `unity_create_virtual_schema_lists_fixture_tables_and_columns` |
| Create virtual schema excludes every entry that is not a plannable base table and warns per exclusion | Unit | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `excludes_view_non_delta_and_other_type_entries`; `excluding_every_entry_yields_an_empty_but_successful_schema` |
| Create virtual schema records each table's Unity Catalog identifier in its identifier note | E2E + Unit | `crates/lakehouse-engine/tests/e2e_unity_test.rs` ; `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` (extended); `records_identifier_notes_and_rejects_collision` |
| A scoped refresh lists Glue table names and loads only the requested tables | E2E | `crates/lakehouse-engine/tests/e2e_glue_test.rs` | `glue_checks_pass_against_one_provisioned_fixture` (the `listing` check's `REFRESH TABLES` step and the broken-table step) |
| A first-level directory holding no data file is skipped, not failed | E2E | `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` |
| Table naming and the identifier note round trip reuse the shared helpers | E2E + Unit | `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` ; `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `refresh_tables_resolves_a_requested_name_as_a_full_refresh_would` (the notes, the skipped `sales`, and the collision); `the_identifier_note_records_the_bare_directory_name_and_round_trips` |
| A scoped refresh lists directory names and reads only the requested directories | E2E | `crates/lakehouse-engine/tests/e2e_local/direct_storage.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` (stage B), `refresh_tables_resolves_a_requested_name_as_a_full_refresh_would` |
| A Glue table resolves from its recorded identifier and fails loud when it is no longer plannable | E2E | `crates/lakehouse-engine/tests/e2e_glue_test.rs` | `glue_checks_pass_against_one_provisioned_fixture` |

- A row marked Unit covers a scenario whose behavior this plan leaves unchanged and whose wording alone changes. Its existing test stays, and this plan adds no E2E test for it. The `catalog/pushdown-catalog-session` delta changes only its Background and adds one scenario.
- The pure selection test `only_requested_identifiers_are_selected_for_loading` proves the adapter's selection, because `handle_create_virtual_schema` passes only that selection to `load_listed_tables`. Each kind's E2E test proves that kind's load: an unrequested table whose load fails fails a full `REFRESH` and does not fail `REFRESH TABLES`. Direct storage has `t_conflict` in stage B of `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`. Iceberg REST has the corrupt `table_three` of `refresh_tables_heals_and_refreshes_only_the_requested_tables`. Glue has the broken-table step of `glue_checks_pass_against_one_provisioned_fixture`.
- The plan adds one E2E test, `refresh_tables_resolves_a_requested_name_as_a_full_refresh_would`, and one virtual schema for it. Every other E2E proof extends an existing test, its fixture, and its virtual schema. The Glue suite gains no `provision()` run.
- Edge cases that the E2E tests cover:
  - a requested name that matches nothing (`NO_SUCH_TABLE`);
  - several names in one statement;
  - a requested name that selects a differently spelled identifier (`STRASSE` selects `straße`);
  - a multi-level identifier (`EU__ORDERS`);
  - a requested table that has no virtual table yet (`T_EMPTIED`);
  - a requested table the listing now skips (`T_HIVE`, `ORC_TABLE`);
  - a requested table the source dropped (`TABLE_TWO`, `t_gone_req`);
  - an unrequested table that became unloadable (`table_three`, `t_conflict`, the broken Glue table) or skipped (`t_new`);
  - a join whose one leg still lacks its note.
- The local `apache/iceberg-rest-fixture:1.10.1` answers `loadTable` on a table whose current metadata file holds invalid JSON with HTTP 500 (`RuntimeIOException`), and still lists the table's identifier (verified during planning). The Iceberg REST load skips only on 404, so the full listing fails on that table.
- The rule of "A partial refresh loads the metadata of the requested tables only" has no Unity Catalog test. Exasol SQL cannot observe it, because no Unity Catalog table can fail the one-sweep full listing. The rule holds because `load_listed_tables` issues `get_table_info` once per given identifier.
- No test counts calls on a fake client, because `specs/testing.md` § Testing strategy forbids asserting on a fake's request log.
- The Unity Catalog and Glue E2E tests prove that the identifier-only listing and the load of listed tables work against the real services. That the listing omits columns is not observable through Exasol SQL, so the query-builder unit test proves it for Unity Catalog, and the Glue builder call is a single SDK argument.

### Manual Testing

The commands run against the local Docker Exasol after `make test-e2e`, which builds the `.so`, installs it, and leaves the E2E virtual schemas in place. They use `REFRESH_REENUM_VS`, because `REFRESH_PARTIAL_VS` ends with a dropped and a corrupt table. `$DSN` is `exasol://sys:exasol@localhost:28563?validateservercertificate=0`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| create-virtual-schema | `exapump sql -d "$DSN" "SELECT TABLE_NAME, ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA = 'REFRESH_REENUM_VS'"` | One row per table. Each `ADAPTER_NOTES` is `{"name":"e2e_refresh_reenum.t_orig"}` or `{"name":"e2e_refresh_reenum.t_new"}` |
| create-virtual-schema-declaration-details | `exapump sql -d "$DSN" "SELECT COUNT(*) FROM <non-ASCII E2E virtual schema>.STRASSE"` | The seeded row count, resolved through the table's identifier note |
| pushdown-format-neutral-resolution | `make test-e2e-unity` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` passes, and its queries resolve each Unity table through its `catalog.schema.table` identifier note |
| pushdown-planning-file-resolution | `exapump sql -d "$DSN" "SELECT ID FROM REFRESH_REENUM_VS.T_ORIG"` | One row. The pushdown reads `e2e_refresh_reenum.t_orig` from the table's note |
| pushdown-planning-join-fallback | `make test-e2e` (module `join`) | Every join and self-join test passes |
| unity-catalog-create-virtual-schema | `make test-e2e-unity` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` passes, reading `catalog.schema.table` notes |
| unity-catalog-client | `make test-e2e-unity` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` passes, including its `REFRESH TABLES` step |
| unity-catalog-client-parquet-admission | `cargo test -p lakehouse-catalog unity` | 0 failures |
| glue-table-planning | `make test-e2e-glue` | `glue_checks_pass_against_one_provisioned_fixture` passes |
| glue-catalog-client | `make test-e2e-glue` | `glue_checks_pass_against_one_provisioned_fixture` passes, including its `REFRESH TABLES` step and its broken-table step |
| direct-storage-table-discovery | `exapump sql -d "$DSN" "SELECT TABLE_NAME, ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_TABLES WHERE TABLE_SCHEMA = '<direct-storage E2E virtual schema>'"` | Each `ADAPTER_NOTES` is `{"name":"<original-cased directory name>"}` |
| pushdown-catalog-session | `make test-e2e` | Every local E2E test passes |
| refresh-and-set-properties | `exapump sql -d "$DSN" "ALTER VIRTUAL SCHEMA REFRESH_REENUM_VS REFRESH TABLES T_ORIG"`, then the `SYS.EXA_ALL_VIRTUAL_TABLES` query above with `LAST_REFRESH` | Only `T_ORIG`'s `LAST_REFRESH` changes. `T_NEW` keeps its row and stays queryable |
| create-virtual-schema-adapter-notes | `exapump sql -d "$DSN" "SELECT ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_SCHEMAS WHERE SCHEMA_NAME = 'REFRESH_REENUM_VS'"` | A JSON object with `PARALLELISM_FACTOR`, the tuning entries, and `SKIPPED_TABLES` |
| column-source-notes | `exapump sql -d "$DSN" "SELECT COLUMN_TABLE, COLUMN_NAME, ADAPTER_NOTES FROM SYS.EXA_ALL_VIRTUAL_COLUMNS WHERE COLUMN_SCHEMA = 'REFRESH_REENUM_VS'"`, before and after the `REFRESH TABLES T_ORIG` above | `T_NEW`'s column notes are unchanged |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Protocol gate | `specs/_plans/change-vs-adapter-per-table-notes/probe/run.sh` | Checks (a) to (f) match the Context section |
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures |
| E2E Unity | `make test-e2e-unity` | 0 failures |
| E2E Glue | `make test-e2e-glue` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |

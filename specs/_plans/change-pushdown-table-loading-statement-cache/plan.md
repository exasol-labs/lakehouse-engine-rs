# Plan: change-pushdown-table-loading-statement-cache

## Summary

The adapter loads each table once per statement and virtual schema, reuses one catalog client across the statement's pushdown requests, and prunes each table reference with its own filter. `construct_catalog_client` becomes the only catalog-kind match that builds a client or loads a table. Each kind's client loads a pushdown table through one engine-side scan load, `CatalogClient` loses `load_table`, and `ScanSource` is deleted. A self-join loads its table once, and Delta join legs replay their logs concurrently. Closes #583.

## Context

- Exasol sends one pushdown request per pushed-down query block, so a UNION ALL, a CTE referenced twice, a join that is not pushed down, or a subquery calls the adapter several times (#583).
- Each pushdown request rebuilds the Tokio runtime, reads the CONNECTION, assumes the STS role, resolves the catalog session, runs the Lakekeeper batch-check, and loads every table (`adapter/mod.rs` `dispatch`, `TableScanResolver::for_request` and `resolve`).
- The #583 probe shows one adapter VM per statement and virtual schema: process-static state survives every call of a statement and is gone in the next statement.
- Pushdown matches the catalog kind two more times beside `construct_catalog_client`: in `RequestSession` (`pushdown/scan_resolution.rs`) and in `ScanSource` (`pushdown/format/mod.rs`). `CatalogClient::load_table` has production callers only in the Unity and Glue arms of `TableScanResolver::resolve`. Its Iceberg REST implementation has none, and its direct-storage implementation always fails.
- The user's design criteria are the last three lines of `decision-log.md` § Interview.
- Every format reader fuses the table load with filter pruning in `FormatReader::resolve_scan`, so no load can be reused across references.
- Perf issue 7: a self-join loads its table twice, because the resolver loads per leg with no reuse per identifier.
- Perf issue 6: Delta join legs replay serially, because the synchronous log replay blocks the adapter's current-thread runtime inside the async `resolve_scan`.
- Reusing a load within a statement conforms to the Iceberg table spec ("Readers use the snapshot that was current when they load the table metadata", § Optimistic Concurrency) and to the Delta protocol ("readers can read a consistent snapshot of a Delta table, even in the face of concurrent writes", § Overview). This plan has no deviation from either.
- `AGENTS.md` ("They hold no cache"), `specs/mission.md` (Out of Scope: caching), `specs/architecture.md` ("No state survives query completion"), and `docs/index.md` ("Every query is stateless") need the statement-scoped exception that #583 lists. The architecture change is in `architecture.md` in this plan directory.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| file-planning/pushdown-format-neutral-resolution | CHANGED | `file-planning/pushdown-format-neutral-resolution/spec.md` |
| file-planning/pushdown-planning-file-resolution | CHANGED | `file-planning/pushdown-planning-file-resolution/spec.md` |
| file-planning/iceberg-type-promotion | CHANGED | `file-planning/iceberg-type-promotion/spec.md` |
| catalog/pushdown-catalog-session | CHANGED | `catalog/pushdown-catalog-session/spec.md` |
| vs-adapter/lakekeeper-permission-check | CHANGED | `vs-adapter/lakekeeper-permission-check/spec.md` |
| vs-adapter/catalog-kind-selection | CHANGED | `vs-adapter/catalog-kind-selection/spec.md` |
| connection/connection-credentials-assume-role-session-use | CHANGED | `connection/connection-credentials-assume-role-session-use/spec.md` |
| delta/delta-table-planning | CHANGED | `delta/delta-table-planning/spec.md` |
| unity-catalog/unity-catalog-client | CHANGED | `unity-catalog/unity-catalog-client/spec.md` |
| direct-storage/direct-storage-table-discovery | CHANGED | `direct-storage/direct-storage-table-discovery/spec.md` |
| direct-storage/direct-storage-table-planning | CHANGED | `direct-storage/direct-storage-table-planning/spec.md` |

## Impact

- Query results and pushdown SQL do not change.
- A statement that references a table several times sends fewer catalog and storage requests: one authentication, one `AssumeRole`, and one table load per virtual schema and table.
- All references to one Iceberg or Delta table in a statement read the same snapshot.
- Under `PERMISSION_CHECK = 'LAKEKEEPER'`, a later pushdown of a statement checks only the tables that no earlier pushdown of the statement checked.
- Adapter memory now holds each loaded table until the statement ends: up to 32 MB of Iceberg manifests per loaded table, one Delta engine thread, and the full Parquet listing or Glue partition list.
- `docs/index.md` states the statement-scoped reuse. No CONNECTION field, property, catalog kind, install step, or capability changes. No breaking change.

## Implementation Tasks

1. Format load and prune (behavior-preserving, existing tests green)
   - [ ] 1.1 Iceberg and the shared interface, in `adapter/pushdown/format/`. [expert]
     - Replace `FormatReader` with the `LoadedTable: Send + Sync` trait, whose one method is `prune(declared, filter) -> PlannedFiles`. Each loaded table is owned (`'static`), because task 3.1 keeps it across requests. `ResolvedScan::resolve` takes `&dyn LoadedTable`. Until task 2.2 deletes it, `format_reader` returns the opened `Arc<dyn LoadedTable>`.
     - The Iceberg loaded table is owned and has two constructors: from the REST `loadTable` on a `&CatalogSession`, and from a Glue metadata file. It keeps the vended storage, `checked_table_root`, the name mapping, and the built `iceberg::table::Table`.
     - Each prune passes `plan_file_tasks` a clone of the kept `Table`, and the clones share one `ObjectCache`. The cache keeps iceberg-rust's default capacity of 32 MB per kept table (`DEFAULT_CACHE_SIZE_BYTES` in iceberg 0.10 `io/object_cache.rs`), because the table lives for one statement.
     - The prune runs `refuse_relaxed_nullability` and all filter work.
     - Move `iceberg_tests.rs` to load then prune, with assertions unchanged.
   - [ ] 1.2 Delta, in `format/delta_format_reader.rs`. [expert]
     - The Delta loaded table keeps the vended storage, the table-root store, and an `Arc<DeltaSnapshot>`.
     - The prune runs the `declared_files` checks and `active_files` with the reference's predicate.
     - Move `delta_format_reader_tests.rs` to load then prune, with assertions unchanged.
   - [ ] 1.3 Catalog Parquet and direct Parquet, in `format/catalog_parquet_format_reader.rs`, `format/parquet_format_reader.rs`, and `adapter/parquet_directory.rs`.
     - The Unity Parquet and direct-storage loaded tables keep the full `list_parquet_files` listing before `keep`. Derive `Clone` on `ListedFile` and `ParquetFile` where the prune needs it.
     - The Glue Hive loaded table keeps the partition list and the table store. Its prune lists the files of each partition that the reference's filter keeps.
     - The prune builds the `PartitionPredicate`. A filter error that a reader raises before its load today keeps its text.
     - Move `catalog_parquet_format_reader_tests.rs` and the direct Parquet tests to load then prune, with assertions unchanged.
   - [ ] 1.4 Delta replay off the runtime thread (perf issue 6). In the Delta loaded table, run `DeltaSnapshot::open` in the load and `active_files` in the prune inside `tokio::task::spawn_blocking`, with owned inputs (the store `Arc`, the table root as a `String`, the `Arc<DeltaSnapshot>`, the cloned `DeclaredSchema` and filter). Redact the joined error afterwards. Keep `TokioBackgroundExecutor`, because `TokioMultiThreadExecutor` panics on a current-thread runtime. [expert]
2. Catalog clients load for pushdown (`decision-log.md` [1])
   - [ ] 2.1 Remove `load_table` from `CatalogClient` in `crates/lakehouse-catalog/src/client.rs`.
     - Make `UnityCatalogSession::load_table` and `GlueCatalogSession::load_table` inherent `pub` methods with their current bodies.
     - Delete the `IcebergRestCatalogClient` implementation, the `DirectStorageCatalogClient` implementation in `adapter/direct_storage.rs`, and the implementation of the fake `FixedCatalogClient` in `lakehouse-catalog/src/client_tests.rs`.
     - Delete `boxed_client_loads_one_table_by_segmented_identifier` (`client_tests.rs`) and `load_table_returns_a_clear_error_naming_the_direct_storage_kind` (`direct_storage_tests.rs`).
     - Move `iceberg_client_tags_every_table_iceberg_with_no_vending_key` and `rest_listing_projects_the_loaded_metadata_into_the_neutral_table` (`client_tests.rs`) to `load_listed_tables`. The Unity, Glue, and `tests/it/catalog_public_surface.rs` callers call the inherent methods.
     - Make `IcebergRestCatalogClient::session` public.
   - [ ] 2.2 Add `adapter/pushdown/scan_catalog.rs` with the engine trait `ScanCatalog: CatalogClient` and one implementation per catalog kind. [expert]
     - `check_identifier` applies the kind's identifier rule without I/O: `parse_table_ident`, `unity_table_ident`, `parse_glue_table_ident`, or `direct_storage_directory`. `unity_table_ident` and `direct_storage_directory` move here from `scan_resolution.rs`. Their tests move with them from `scan_resolution_tests.rs` to `scan_catalog_tests.rs`, with names and expected values unchanged: `a_recorded_identifier_recovers_its_namespace_segments_and_table_name`, `a_recorded_identifier_without_a_table_name_is_refused_before_any_catalog_request`, and `a_direct_storage_identifier_naming_no_first_level_directory_is_refused`.
     - `authorize` batch-checks identifiers under an enforced `PermissionGate` and returns the identifiers it admits. Iceberg REST runs it on its session. Every other kind admits none, as today.
     - `load_for_scan(table_identifier, connection) -> Arc<dyn LoadedTable>` is the one scan load. Iceberg REST opens the Iceberg loaded table on its session. Unity Catalog calls its `load_table`, then matches the format tag exhaustively: Delta, Parquet, or the refusal that names the table and the format. Glue calls its `load_table`, then matches Iceberg (metadata file), Parquet (Hive partitions), or the refusal. Direct storage composes the root from its own `root` and the directory name in a pure function (`direct_storage_directory`, then `ObjectLocation::child`), then opens the directory listing. Check that `DirectStorageCatalogClient::new` builds the same admission-limited store and root that `for_request` builds today.
     - `construct_catalog_client` returns `Box<dyn ScanCatalog>` and becomes the only `CatalogKind` match that builds a client or loads a table. `TableScanResolver::for_request` takes that client and no longer matches the kind. The pushdown arm still builds one client per request in this task.
     - Delete `ScanSource`, `format_reader`, and `RequestSession`.
     - Move the refusal tests `format_reader_refuses_an_iceberg_table_under_the_unity_source` and `format_reader_refuses_a_delta_table_under_the_glue_source` from `format_tests.rs` to `scan_catalog_tests.rs`, against each kind's format-tag match, and tag both with "A Unity Catalog or Glue table is planned by the format its catalog reports".
     - Replace `third_scan_source_selects_the_parquet_reader` with `a_direct_storage_load_composes_its_root_from_the_identifier` in `scan_catalog_tests.rs`, tagged "A direct-storage pushdown loads its table from the directory its identifier note names". It calls the root composition with plain inputs and lists no store.
     - The new unit tests of tasks 2.2, 3.3, and 3.4 build request JSON, column nodes, and file entries through the builders in `adapter/pushdown/test_support_tests.rs`.
     - Untag the test at `catalog_public_surface.rs:123`, because its scenario is removed with no replacement.
3. Statement state (#583, perf issue 7, `decision-log.md` [3])
   - [ ] 3.1 Add `adapter/statement_state.rs`. [expert]
     - `StatementStates<T>` maps `schemaMetadataInfo.name` to an `Arc<T>`. Its lookup takes the name and a build closure, and runs the closure on a miss. `statement_state.rs` declares the one process static, a `StatementStates<VirtualSchemaState>`. `adapter_call` passes it into `dispatch`, whose pushdown arm uses it.
     - `VirtualSchemaState` holds the Tokio runtime, the `ResolvedConnectionConfig`, the `Box<dyn ScanCatalog>`, the admitted identifiers, and one `tokio::sync::OnceCell<Arc<dyn LoadedTable>>` per identifier.
     - The state keeps the runtime, because each kept Iceberg `Table` holds a handle to the runtime it was built on.
     - A failed build or load stores nothing. A request without a name gets a state that the map does not keep.
     - Every test that calls `dispatch` passes a new `StatementStates` of its own, built by one helper in `adapter/pushdown/test_support_tests.rs`, so no test shares state and none takes a lock.
     - On a miss, the pushdown arm's build closure runs today's sequence: runtime, `resolve_connection_config`, then `construct_catalog_client`. On every request it parses the permission and maps `ctx.current_user()`.
   - [ ] 3.2 `TableScanResolver::for_request` takes the `VirtualSchemaState`. [expert]
     - It checks each identifier through `check_identifier`, then calls `authorize` only for the identifiers that the state has not admitted. A pure helper returns those identifiers.
     - `resolve` gets the loaded table through the identifier's cell, so concurrent self-join legs and later requests share one load, then prunes with the reference's filter.
     - The state's admitted set replaces `TableAdmission`.
   - [ ] 3.3 Unit tests.
     - `adapter/statement_state_tests.rs`: `a_second_virtual_schema_name_misses_the_state` and `a_request_without_a_schema_name_is_not_kept` call the lookup of a local `StatementStates<u32>` with plain build closures, so they need no `TestContext` or CONNECTION. For the names A, A, and B, both A lookups return one `Arc` and B returns another. Two lookups without a name each run their closure.
     - `pushdown/scan_resolution_tests.rs`: `only_unchecked_identifiers_are_batch_checked` for the helper of task 3.2, over identifiers of which the state admitted some and not others, so a helper that returns all or none fails.
   - [ ] 3.4 Add `delta_join_legs_replay_concurrently_on_a_current_thread_runtime` to `joins/joins_tests.rs`, modeled on `join_legs_resolve_concurrently_in_leg_index_order`: two Delta legs on a `current_thread` runtime over a store whose first `_delta_log` read waits for the other leg's first read. A serial replay times out.
4. E2E proof
   - [ ] 4.1 Add `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs` and declare it in `tests/e2e_local/main.rs`. Move `proxy.rs`'s `setup`, `props`, `password_via_proxy`, and `count`, with the constants they read, into `e2e_local/fixtures.rs`, the area's shared module. Add one helper beside `proxy_log_since` in `tests/common/stack.rs` that waits more than one second, takes the mark, runs the statement, and counts the matching request lines in the proxy log after the mark. Every request-count test of tasks 4.1 to 4.4 uses it.
     - Write and run `a_script_reloads_the_table_for_each_of_its_statements` before every other task of groups C and D. The test creates a Lua script that runs the same `SELECT` over `PROXY_ICEBERG_VS` twice through `query()`, runs it once with `EXECUTE SCRIPT`, and asserts two `loadTable` GETs in the proxy log after the mark.
     - If the test sees one `loadTable` GET, stop and return the cache-key decision to the user.
   - [ ] 4.2 Add the remaining tests to `statement_reuse.rs`.
     - `a_statement_authenticates_once_per_virtual_schema`: a UNION ALL over `PROXY_ICEBERG_VS` sends one `/v1/config` GET.
     - `every_reference_to_a_table_loads_it_once`: one case table whose rows are a UNION ALL, a CTE referenced twice, and a self-join with a different filter per occurrence, each sending one `loadTable` GET per table, and a `PROXY_DIRECT_VS` UNION ALL, sending one directory LIST per table.
     - `a_later_statement_reads_a_file_added_after_the_first`: its fixture has its own namespace, virtual schema, and base path, which the test empties before it writes the initial state. It writes the table and the added file through `tests/common/seed.rs`.
     - `two_virtual_schemas_in_one_statement_load_the_table_once_each`: two virtual schemas with their own CONNECTION over one catalog table send two `loadTable` GETs for a UNION ALL of both, and each branch's rows equal the seeded source data.
     - `a_role_session_serves_every_pushdown_of_a_statement`: a UNION ALL over `PROXY_ROLE_VS` sends one `AssumeRole` POST.
     - Drop the assume-role scenario tag from `adapter_tests.rs`'s `assume_role_sends_one_sts_request_per_create_and_per_join_pushdown`, because this E2E test now proves the scenario.
   - [ ] 4.3 Add `unity_delta_self_join_loads_the_table_once` to `e2e_unity_test.rs` on the proxy setup of `unity_catalog_and_delta_log_reads_go_through_the_proxy`. A self-join with a different filter per occurrence sends one Unity table GET and one `_delta_log` LIST, and its rows equal the single-node oracle.
   - [ ] 4.4 Add two tests to `e2e_lakekeeper_test.rs`.
     - `a_union_statement_checks_every_table_its_pushdowns_read`: `E2E_TABLE UNION ALL E2E_TABLE` returns rows for `ALLOWED_USER`. The orders `E2E_TABLE UNION ALL TABLE_ALPHA` and `TABLE_ALPHA UNION ALL E2E_TABLE` are rows of one case table, and each is refused for a user granted only `E2E_TABLE`. The test reads each refusal through `error_text`, as `assert_refused` does.
     - `a_statement_sends_one_token_request_and_one_batch_check`: on the proxy setup of `lakekeeper_oauth_batch_check_and_vended_reads_go_through_the_proxy`, with OAuth2 client credentials and the check on, `E2E_TABLE UNION ALL E2E_TABLE` sends one token POST to `keycloak-via-proxy`, one `/v1/config` GET, one `/v1/action/batch-check` POST, and one `loadTable` GET after the mark.
     - Retag `permission_check_decides_every_single_table_shape` with the new scenario title, and add that title's tag to `permission_check_refuses_the_same_query_once_a_grant_is_revoked`. Drop the removed title's tag from the unit tests in `scan_resolution_tests.rs` and `lakehouse-catalog/src/lakekeeper_tests.rs`.
5. Rule amendments and user docs
   - [ ] 5.1 `AGENTS.md` § Architecture rules: after "UDFs are stateless and disposable. They hold no cache, persist no metadata, and keep no state across calls.", add the one exception: the adapter keeps each virtual schema's catalog client and loaded tables for the pushdown requests of one statement, keyed by the virtual schema name, because an adapter VM serves one statement. The scan UDF keeps none.
   - [ ] 5.2 `specs/mission.md`: Core Capability 1 says the Virtual Schema is stateless across statements. The Out of Scope line "Caching, result reuse, materialization, query acceleration" names cross-statement caching and states that metadata reuse within one statement is in scope.
   - [ ] 5.3 `docs/index.md:13-14`: replace "Every query is stateless: no caching, no materialization, no data copied out." with a sentence that the adapter reuses table metadata within one statement and keeps nothing across statements, with no materialization and no data copied out.
   - [ ] 5.4 `specs/testing.md` § Correctness oracles: add the rule that a scenario about how many catalog, storage, or STS requests the adapter sends is proved by counting request lines in the local forward proxy's log after a mark. Replace "A fixture guard is the one exception." with a sentence that names the two exceptions to "An E2E test asserts only on what Exasol shows": the fixture guard and this proxy-log count.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: format load and prune | 1.1-1.4 | none | spec deltas `file-planning/pushdown-format-neutral-resolution`, `file-planning/iceberg-type-promotion`, `delta/delta-table-planning` (Background); `crates/lakehouse-engine/src/adapter/pushdown/format/`, `crates/lakehouse-engine/src/adapter/parquet_directory.rs`, and their `_tests.rs` files |
| B: catalog clients and statement state | 2.1-2.2, 3.1-3.4 | A (uses the `LoadedTable` API) | spec deltas `catalog/pushdown-catalog-session`, `vs-adapter/lakekeeper-permission-check`, `vs-adapter/catalog-kind-selection`, `connection/connection-credentials-assume-role-session-use`, `unity-catalog/unity-catalog-client`, `direct-storage/direct-storage-table-discovery`, `direct-storage/direct-storage-table-planning`, `file-planning/pushdown-planning-file-resolution`, `delta/delta-table-planning` (format-tag scenario); `crates/lakehouse-catalog/src/client.rs`, `unity/client.rs`, `glue/client.rs`, `tests/it/catalog_public_surface.rs`; `crates/lakehouse-engine/src/adapter/mod.rs`, `adapter/statement_state.rs`, `adapter/direct_storage.rs`, `adapter/permission.rs`, `adapter/pushdown/scan_catalog.rs`, `adapter/pushdown/scan_resolution.rs`, `adapter/pushdown/mod.rs`, `adapter/pushdown/joins/`, and their `_tests.rs` files |
| C: E2E proof | 4.1-4.4 | B | all spec deltas; `crates/lakehouse-e2e/tests/e2e_local/`, `crates/lakehouse-e2e/tests/common/stack.rs`, `crates/lakehouse-e2e/tests/e2e_unity_test.rs`, `crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs` |
| D: rule amendments and user docs | 5.1-5.4 | C (task 4.1's script test) | `architecture.md` delta in this plan; `AGENTS.md`, `specs/mission.md`, `specs/testing.md`, `docs/index.md` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Trait method and implementations | `CatalogClient::load_table` in `crates/lakehouse-catalog/src/client.rs`, with its `IcebergRestCatalogClient`, `DirectStorageCatalogClient` (`adapter/direct_storage.rs`), and `FixedCatalogClient` (`client_tests.rs`) implementations | The scan load of each kind's client replaces it, and Unity Catalog and Glue keep inherent loads |
| Enum | `RequestSession` in `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution.rs` | Replaced by the clients `construct_catalog_client` builds |
| Enum | `TableAdmission` in `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution.rs` | Replaced by the statement state's admitted set |
| Enum, trait, function | `ScanSource`, `FormatReader`, and `format_reader` in `crates/lakehouse-engine/src/adapter/pushdown/format/mod.rs` | Replaced by `ScanCatalog::load_for_scan` and `LoadedTable` |
| Test | `boxed_client_loads_one_table_by_segmented_identifier` in `lakehouse-catalog/src/client_tests.rs`; `load_table_returns_a_clear_error_naming_the_direct_storage_kind` in `adapter/direct_storage_tests.rs` | Test the removed trait method |
| Test | `format_reader_selects_the_delta_reader_for_a_delta_table_without_contacting_the_catalog`, `format_reader_selects_an_iceberg_source_without_contacting_the_catalog`, `third_scan_source_selects_the_parquet_reader`, and `format_reader_selects_readers_for_a_glue_table_by_format_without_contacting_the_catalog` in `adapter/pushdown/format/format_tests.rs` | Test the removed `format_reader`; the Unity, Glue, and local E2E suites prove the selection, and task 2.2 replaces the direct-storage test |
| Test | `one_catalog_session_serves_every_table_the_resolver_resolves`, `one_unity_catalog_session_serves_every_table_the_resolver_resolves`, `one_session_or_store_per_request_serves_every_leg`, `request_session_has_one_variant_per_kind` in `adapter/pushdown/scan_resolution_tests.rs` | Fake-server request counts for the removed scenario and the removed enum; `statement_reuse.rs` proves the replacing scenario |
| Test | `a_two_leg_join_resolves_both_legs_on_one_catalog_session` in `adapter/pushdown/pushdown_tests.rs` | Same as above |

Each deleted test goes with its section banner and its test-only imports.

## Out of scope

- Perf issue 1: a file list that exceeds Exasol's 2,000,000-character literal still fails the query.
- Perf issue 2: a string column compared with a number is a semantic difference, not an issue.
- Perf issue 3: a Glue Hive table still pages through every partition once per statement.
- Perf issue 4: without a CRC file, each Delta reference still reads its commit JSONs twice, and a later reference reads them again in its own prune.
- Perf issue 5: Delta checkpoint footers are still read twice, in two GETs each.
- Perf issue 8: concurrent Unity Catalog legs over different tables can still each request an OAuth token, so the spec bounds token requests only for Iceberg REST.
- Perf issue 9: Iceberg manifest fetches stay capped at the adapter's core count.
- Perf issue 10: unreachable object storage still fails slowly.
- Perf issue 11: a pushdown still has no overall deadline.
- Perf issue 12: large IN lists still cost adapter CPU.
- Perf issue 13: each pushdown request still parses the schema adapterNotes several times.
- Perf issue 14: direct storage still lists a directory one page at a time.
- Perf issue 15: adapter memory still grows with the file list, now for every table a statement loads (see Impact).
- Perf issue 16: a numeric literal still never prunes a direct-storage partition.
- Perf issue 17: STS AssumeRole runs once per statement and virtual schema, but it still runs ahead of the catalog calls and is still wasted under vended credentials.
- Caching across statements or sessions, caching pruned file lists or results, and any state in the scan UDF (#583).
- The per-kind input rules stay as they are: CONNECTION validation (`validate_kind_preconditions` in `adapter/connection.rs`), the Lakekeeper kind check (`require_supported_kind` in `adapter/permission.rs`), and the CREATE namespace rule (`adapter/mod.rs`). Each checks input before any request and loads no table.

## Verification

### Scenario Coverage

The E2E tests that count requests rely on the proxy-log rule of task 5.4.

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The pushdowns of one statement share one catalog session per virtual schema | E2E | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs`, `crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs` | `a_statement_authenticates_once_per_virtual_schema` (`/v1/config`), `a_statement_sends_one_token_request_and_one_batch_check` (OAuth2 token) |
| Every reference to one table in a statement loads the table once and prunes it with its own filter | E2E | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs` | `every_reference_to_a_table_loads_it_once` (Iceberg REST, direct storage), `a_later_statement_reads_a_file_added_after_the_first`, `a_script_reloads_the_table_for_each_of_its_statements` |
| Every reference to one table in a statement loads the table once and prunes it with its own filter (Unity Delta) | E2E | `crates/lakehouse-e2e/tests/e2e_unity_test.rs` | `unity_delta_self_join_loads_the_table_once`. Unity Parquet, Glue Iceberg, and Glue Hive have no load-count test: the Glue suite runs on real AWS without a request log. `unity_parquet_table_is_listed_and_returns_its_rows_and_partition_values` and `glue_checks_pass_against_one_provisioned_fixture` cover their rows |
| Two virtual schemas in one statement use their own CONNECTION | E2E | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs` | `two_virtual_schemas_in_one_statement_load_the_table_once_each` (the untagged unit test `a_second_virtual_schema_name_misses_the_state` covers the key) |
| A request's table resolutions run concurrently rather than one after another | E2E, Unit | `crates/lakehouse-e2e/tests/e2e_unity_test.rs`, `crates/lakehouse-engine/src/adapter/pushdown/joins/joins_tests.rs` | `unity_delta_join_and_aggregate_pushdown_return_correct_rows` (existing, runs the join); `delta_join_legs_replay_concurrently_on_a_current_thread_runtime` and `join_legs_resolve_concurrently_in_leg_index_order` prove the overlap. The new test is a scoped exception to `specs/testing.md` § Testing strategy and § Coverage rule (a host test over several modules, a fake store's call order, and a timeout), because neither Exasol SQL nor the second-granular proxy log shows the overlap, and `join_legs_resolve_concurrently_in_leg_index_order` is its precedent |
| Single-table pushdown builds one catalog session and reuses it | E2E | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs`, `crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs` | `a_statement_authenticates_once_per_virtual_schema`, `a_statement_sends_one_token_request_and_one_batch_check` |
| N-table join reuses one session across all legs | E2E | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs` | `every_reference_to_a_table_loads_it_once` (its self-join statement) |
| The Lakekeeper permission check reuses the request's one catalog session | E2E | `crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs` | `a_statement_sends_one_token_request_and_one_batch_check` |
| One batch-check per pushdown decides every table that no earlier pushdown of the statement checked | E2E, Unit | `crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs`, `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` | `a_union_statement_checks_every_table_its_pushdowns_read`, `a_statement_sends_one_token_request_and_one_batch_check`, `permission_check_decides_every_single_table_shape` (retagged), `permission_check_refuses_the_same_query_once_a_grant_is_revoked` (existing: admitted tables do not carry across statements); `only_unchecked_identifiers_are_batch_checked` |
| The adapter assumes the role once per statement and virtual schema and substitutes the session | E2E | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs` | `a_role_session_serves_every_pushdown_of_a_statement` |
| Absent CATALOG_KIND resolves the Iceberg REST catalog kind | E2E, Unit | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs`, `crates/lakehouse-engine/src/adapter/catalog_kind_tests.rs` | `a_statement_authenticates_once_per_virtual_schema` (no `CATALOG_KIND`); the existing tagged test |
| A pushdown request under the Unity Catalog kind is planned as a Delta scan | E2E | `crates/lakehouse-e2e/tests/e2e_unity_test.rs` | `unity_delta_join_and_aggregate_pushdown_return_correct_rows` |
| A Unity Catalog or Glue table is planned by the format its catalog reports | E2E, Unit | `crates/lakehouse-e2e/tests/e2e_unity_test.rs`, `crates/lakehouse-e2e/tests/e2e_glue_test.rs`, `crates/lakehouse-engine/src/adapter/pushdown/scan_catalog_tests.rs` | `unity_delta_join_and_aggregate_pushdown_return_correct_rows`, `unity_parquet_table_is_listed_and_returns_its_rows_and_partition_values`, `glue_checks_pass_against_one_provisioned_fixture`; the two moved refusal tests, because no live catalog reports a format its kind does not plan |
| The Delta reader is reached from production pushdown under the Unity Catalog kind | E2E | `crates/lakehouse-e2e/tests/e2e_unity_test.rs` | `unity_delta_join_and_aggregate_pushdown_return_correct_rows`; the tagged `pushdown_tests.rs` tests keep their tag |
| A direct-storage pushdown loads its table from the directory its identifier note names | E2E, Unit | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs`, `crates/lakehouse-engine/src/adapter/pushdown/scan_catalog_tests.rs` | `every_reference_to_a_table_loads_it_once` (its direct-storage statement); `a_direct_storage_load_composes_its_root_from_the_identifier` |

### Manual Testing

Run after `make cross-udf-build` and `make e2e-up`, with `DSN='exasol://sys:exasol@localhost:8563?validateservercertificate=0'`, `PROXY=$(docker ps -q --filter label=com.docker.compose.service=proxy)`, and the `PROXY_ICEBERG_VS`, `PROXY_DIRECT_VS`, and `PROXY_ROLE_VS` virtual schemas that `e2e_local/proxy.rs` creates. Take `MARK=$(date +%s)` before each statement.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| file-planning/pushdown-format-neutral-resolution, file-planning/pushdown-planning-file-resolution, file-planning/iceberg-type-promotion, vs-adapter/catalog-kind-selection | `exapump sql "SELECT COUNT(*) FROM (SELECT * FROM PROXY_ICEBERG_VS.MOR_POS_FILE UNION ALL SELECT * FROM PROXY_ICEBERG_VS.MOR_POS_FILE UNION ALL SELECT * FROM PROXY_ICEBERG_VS.MOR_DV)" -d "$DSN"; docker logs --since "$MARK" "$PROXY" 2>&1 \| grep -c 'GET http://iceberg-rest-via-proxy:8181/v1/config'` | Twice the `MOR_POS_FILE` count plus the `MOR_DV` count, then `1` |
| catalog/pushdown-catalog-session | `exapump sql "SELECT COUNT(*) FROM PROXY_ICEBERG_VS.MOR_POS_FILE a JOIN PROXY_ICEBERG_VS.MOR_POS_FILE b ON a.ID = b.ID" -d "$DSN"; docker logs --since "$MARK" "$PROXY" 2>&1 \| grep -c 'GET http://iceberg-rest-via-proxy:8181/v1/.*/tables/mor_pos_file'` | The self-join count, then `1` |
| direct-storage/direct-storage-table-discovery, direct-storage/direct-storage-table-planning | `exapump sql "SELECT COUNT(*) FROM (SELECT * FROM PROXY_DIRECT_VS.ROWS UNION ALL SELECT * FROM PROXY_DIRECT_VS.ROWS)" -d "$DSN"; docker logs --since "$MARK" "$PROXY" 2>&1 \| grep -c 'list-type=2'` | `6`, then `1` |
| vs-adapter/lakekeeper-permission-check | `make lakekeeper-up`, then as a user granted only the first table: `exapump sql "SELECT COUNT(*) FROM (SELECT * FROM <vs>.<granted> UNION ALL SELECT * FROM <vs>.<not granted>)" -d "<user DSN>"`, with the names of `e2e_lakekeeper_test.rs`'s `permission_setup` | An error that names the table that is not granted, and no rows |
| connection/connection-credentials-assume-role-session-use | `exapump sql "SELECT COUNT(*) FROM (SELECT * FROM PROXY_ROLE_VS.<table> UNION ALL SELECT * FROM PROXY_ROLE_VS.<table>)" -d "$DSN"; docker logs --since "$MARK" "$PROXY" 2>&1 \| grep -c 'POST http://seaweedfs-via-proxy:8333/ '` | Twice the table count, then `1` |
| delta/delta-table-planning, unity-catalog/unity-catalog-client | `make unity-up`, then `exapump sql "SELECT COUNT(*) FROM <unity vs>.<delta table> a JOIN <unity vs>.<delta table> b ON a.<key> = b.<key>" -d "$DSN"`, with the names of `e2e_unity_test.rs`'s proxy setup | The single-node oracle count |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace` | 0 failures |
| E2E local | `make e2e-up test-e2e` | 0 failures |
| E2E Lakekeeper | `make lakekeeper-up test-e2e-lakekeeper` | 0 failures |
| E2E Unity | `make unity-up test-e2e-unity` | 0 failures |
| E2E Glue | `make test-e2e-glue`, on real AWS per `specs/testing.md` § E2E suites, because tasks 1.1, 1.3, and 2.2 rewrite the Glue Iceberg and Glue Hive loads | 0 failures |
| Lint and format | `make fmt-check lint` | 0 errors or warnings, no changes |

# Tasks: change-pushdown-table-loading-statement-cache

## Phase 2: Implementation (Group A: format load and prune)
- [x] 1.1 Iceberg and the shared interface, in `adapter/pushdown/format/` [expert]
- [x] 1.2 Delta, in `format/delta_format_reader.rs` [expert]
- [x] 1.3 Catalog Parquet and direct Parquet
- [x] 1.4 Delta replay off the runtime thread (perf issue 6) [expert]

## Phase 2: Implementation (Group B: catalog clients and statement state)
- [x] 2.1 Remove `load_table` from `CatalogClient`
- [x] 2.2 Add `adapter/pushdown/scan_catalog.rs` with `ScanCatalog` [expert]
- [x] 3.1 Add `adapter/statement_state.rs` [expert]
- [x] 3.2 `TableScanResolver::for_request` takes the `VirtualSchemaState` [expert]
- [x] 3.3 Unit tests (`statement_state_tests.rs`, `scan_resolution_tests.rs`)
- [x] 3.4 Add `delta_join_legs_replay_concurrently_on_a_current_thread_runtime`

## Phase 2: Implementation (Group C: E2E proof)
- [x] 4.1 `statement_reuse.rs`, fixtures move, proxy-log helper, `a_script_reloads_the_table_for_each_of_its_statements`
- [x] 4.2 Remaining `statement_reuse.rs` tests
- [x] 4.3 `unity_delta_self_join_loads_the_table_once`
- [x] 4.4 Two Lakekeeper tests, retags

## Phase 2: Implementation (Group D: rule amendments and user docs)
- [x] 5.1 `AGENTS.md` exception
- [x] 5.2 `specs/mission.md`
- [x] 5.3 `docs/index.md`
- [x] 5.4 `specs/testing.md`

## Phase 4: Review Fixes
- [x] 7.1 In `AGENTS.md` line 49, replace "Format knowledge lives in the plan-time `FormatReader`" with "Format knowledge lives in each table format's plan-time `LoadedTable`"
- [x] 7.2 In `joins/mod.rs` `plan_join`, replace the comment above `let identifiers` with "Built once for every leg, so every leg's identifier passes its rule before any catalog request."
- [x] 7.3 In `statement_state.rs`, replace the `StatementStates` doc comment with "States keyed by virtual schema name, so two virtual schemas never read each other's state, even when one VM serves both."
- [x] 7.4 In `pushdown/mod.rs`, delete the `#[cfg(test)] use crate::adapter::ResolvedConnectionConfig;` import and add it to `pushdown_tests.rs` and `test_support_tests.rs`
- [x] 7.5 In `parquet_directory.rs`, add a doc comment above `ParquetListing::list`
- [x] 7.6 In `adapter_tests.rs`, add the case-table test `only_a_nonempty_virtual_schema_name_keys_the_statement_state` over `virtual_schema_name`
- [x] 7.7 In `joins_tests.rs`, tag both concurrency tests with `/// Scenario: A request's table resolutions run concurrently rather than one after another`
- [x] 7.8 In `test_support_tests.rs`, add `spawn_async_fake_http_server` and rebuild `RendezvousCatalog::spawn` and `delta_log_rendezvous` on it
- [x] 7.9 In `statement_reuse.rs`, delete the constant `requests` field of `Reference` and assert one load per case
- [x] 7.10 In `statement_reuse.rs`, add a `configs` field to `Reference` and assert the `/v1/config` lookup count per case
- [x] 7.11 Move `seed_catalog`, `id_val_batch`, `ensure_id_val_table`, `drop_table_if_exists`, `append_file` (and their private helpers) from `refresh.rs` to `fixtures.rs`, and import them from `crate::fixtures`
- [x] 7.12 Make the proxy fixtures one lazy setup each: `setup_proxy` takes an adapter schema, the unreachable-proxy tests use `LHVS_PROXY_ENV`, and `proxy_virtual_schemas` and both suites' `create_proxy_virtual_schema` run once behind a `OnceLock` [expert]

## Phase 3: Verification
- [x] 6.1 Build (`make cross-udf-build`)
- [x] 6.2 Unit tests (`cargo test --workspace`)
- [x] 6.3 E2E local (`make e2e-up test-e2e`)
- [x] 6.4 E2E Lakekeeper (`make lakekeeper-up test-e2e-lakekeeper`)
- [x] 6.5 E2E Unity (`make unity-up test-e2e-unity`)
- [x] 6.6 Lint and format (`make fmt-check lint`)

## Phase 5: Verification Fixes
- [x] 8.1 `two_virtual_schemas_in_one_statement_load_the_table_once_each` failed in the full local run: the `ORDER BY 1, 2` UNION ALL returned branch B unsorted. Reproduce against live Exasol, find the cause, fix it [expert]
- [x] 6.7 E2E Glue (`make glue-up test-e2e-glue`)
- [x] 6.8 Manual tests (proxy-log request counts)

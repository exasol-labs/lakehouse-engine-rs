# Code Review Findings: change-pushdown-table-loading-statement-cache

## Summary
- Files reviewed: 53
- Total findings: 12 (standard: 11, expert: 1)

The production change holds. Every item of the plan's Dead Code Removal table is gone, and no test carries the tag of a removed scenario. `cargo clippy --workspace --all-targets` reports no warning, with and without the E2E features. The remaining findings are stale comments, gaps in test proof, and test-fixture design.

The reported deviations were judged as follows:
- Accepted: 1 (runtime-free `StatementCatalog`), 2 (`handle_pushdown` is `pub(crate)`), 3 (the permission check is passed per request), 4 (the Delta snapshot engine owns the store), 5 (a Glue client-build error before an identifier error; neither error sends a request), 6 (follows from task 1.1's load and prune split), and 7 (`E2E_DIM_TABLE` lives in the virtual schema's namespace).
- Not accepted: 8. See the expert finding.

## Standard fixes

### AGENTS.md

#### [OUTDATED_COMMENT] Architecture rule names the deleted `FormatReader`
- Location: line 49
- Issue: The rule says "Format knowledge lives in the plan-time `FormatReader`". This change deleted `FormatReader`, and `LoadedTable` in `crates/lakehouse-engine/src/adapter/pushdown/format/mod.rs` replaced it.
- Fix: In AGENTS.md line 49, replace "Format knowledge lives in the plan-time `FormatReader`" with "Format knowledge lives in each table format's plan-time `LoadedTable`".

### crates/lakehouse-engine/src/adapter/pushdown/joins/mod.rs

#### [OUTDATED_COMMENT] Comment credits the shared load to building the resolver once
- Location: lines 131-132, in `plan_join`
- Issue: The comment says the resolver is built once "so ... a self-join's legs share one load". `StatementCatalog::loaded_table` shares the load through its per-identifier cell, and that sharing holds even when a leg builds its own resolver. Only the first reason, that identifiers are validated before any catalog request, depends on building the resolver once.
- Fix: In `plan_join`, replace the two-line comment above `let identifiers` with `// Built once for every leg, so every leg's identifier passes its rule before any catalog request.`

### crates/lakehouse-engine/src/adapter/statement_state.rs

#### [OUTDATED_COMMENT] Two doc comments disagree on whether virtual schemas share a VM
- Location: lines 19-23
- Issue: The `STATEMENT_STATES` doc says "Exasol runs one adapter VM per statement and virtual schema", so no two virtual schemas share a VM. The `StatementStates` doc says the name key "separates the virtual schemas that share a VM". A reader cannot tell whether the name key is required or only defensive.
- Fix: In statement_state.rs, replace the `StatementStates` doc comment with `/// States keyed by virtual schema name, so two virtual schemas never read each other's state, even when one VM serves both.`

### crates/lakehouse-engine/src/adapter/pushdown/mod.rs

#### [UNUSED_IMPORT] Test-only import added to the production module
- Location: lines 9-10
- Issue: `#[cfg(test)] use crate::adapter::ResolvedConnectionConfig;` exists only for `use super::*` in `pushdown_tests.rs` and `test_support_tests.rs`. AGENTS.md § Code style keeps test code out of the production file, except a test-visibility `pub use` re-export.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/mod.rs, delete the `#[cfg(test)]` attribute and the `use crate::adapter::ResolvedConnectionConfig;` line below it. Add `use crate::adapter::ResolvedConnectionConfig;` to the imports of `pushdown/pushdown_tests.rs` and `pushdown/test_support_tests.rs`, then run `cargo test --workspace --lib pushdown` to confirm both compile.

### crates/lakehouse-engine/src/adapter/parquet_directory.rs

#### [MISSING_DOC_COMMENT] Public `ParquetListing::list` has no doc comment
- Location: line 162
- Issue: `parquet_directory` is a `pub mod`, and `ParquetListing::list` is a new `pub async fn` with no doc comment. Its sibling `kept_files` has one.
- Fix: In parquet_directory.rs, add `/// Lists every data file below \`prefix\` once, with no partition column declared and no footer read.` above `pub async fn list`.

### crates/lakehouse-engine/src/adapter/mod.rs

#### [MISSING_BOUNDARY_TEST] The state key mapping has no test for a missing or empty name
- Location: line 132, `virtual_schema_name`
- Issue: `virtual_schema_name` decides whether a pushdown's state is kept. It maps an absent `schemaMetadataInfo`, an absent `name`, and an empty `name` to `None`. No test covers any of these inputs. `a_request_without_a_schema_name_is_not_kept` starts from `None` and never reads a request.
- Fix: In crates/lakehouse-engine/src/adapter/adapter_tests.rs, add a test `only_a_nonempty_virtual_schema_name_keys_the_statement_state`. Make it one case table over `virtual_schema_name`, with requests built by `serde_json::json!` like the neighboring `dispatch_*` tests: `{"schemaMetadataInfo": {"name": "VS"}}` gives `Some("VS")`, `{"schemaMetadataInfo": {"name": ""}}` gives `None`, `{"schemaMetadataInfo": {}}` gives `None`, and `{}` gives `None`.

### crates/lakehouse-engine/src/adapter/pushdown/joins/joins_tests.rs

#### [MISSING_DOC_COMMENT] The concurrency tests carry no scenario tag
- Location: line 682 (`join_legs_resolve_concurrently_in_leg_index_order`) and line 753 (`delta_join_legs_replay_concurrently_on_a_current_thread_runtime`)
- Issue: The plan's Scenario Coverage names these two tests as the proof that join legs overlap, because neither Exasol SQL nor the proxy log shows the overlap. No test in the repository carries `/// Scenario: A request's table resolutions run concurrently rather than one after another`. `specs/testing.md` § Coverage rule requires the tag on a unit test that is a scenario's one proof.
- Fix: In joins_tests.rs, add `/// Scenario: A request's table resolutions run concurrently rather than one after another` directly above the `#[tokio::test(...)]` attribute of `join_legs_resolve_concurrently_in_leg_index_order` and of `delta_join_legs_replay_concurrently_on_a_current_thread_runtime`.

#### [SHRINKABLE] Second copy of the rendezvous fake server
- Location: lines 706-745 (`delta_log_rendezvous`) and lines 612-650 (`RendezvousCatalog::spawn`)
- Issue: `delta_log_rendezvous` repeats the accept loop of `RendezvousCatalog::spawn`: it binds `127.0.0.1:0`, accepts, reads one request, waits on a `tokio::sync::Barrier`, and writes one response. `specs/testing.md` § Test code requires the change that writes the second copy of a setup block to extract it into a shared helper.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/test_support_tests.rs, add one `pub(super)` helper `spawn_async_fake_http_server`. It takes a responder `Fn(String) -> futures::future::BoxFuture<'static, (u16, &'static str, String)> + Send + Sync + 'static` that maps a request target to a response. It binds `127.0.0.1:0`, reads each request with `read_http_request`, awaits the responder, writes `http_response`, and returns the endpoint URL. Rebuild `RendezvousCatalog::spawn` and `delta_log_rendezvous` on that helper, keeping their barrier logic in their responders. Run both concurrency tests.

### crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs

#### [DEAD_FLEXIBILITY] Every case of the reference table expects one request
- Location: lines 89-95 (`Reference`) and lines 106-146
- Issue: The `requests` field of `Reference` is `1` in all four cases of `every_reference_to_a_table_loads_it_once`, so the field never varies.
- Fix: In statement_reuse.rs, delete the `requests: usize` field from `Reference` and from its four initializers. In the loop, assert the counted value with `assert_eq!(requests, 1, "{}: loads", case.shape)`.

#### [MISSING_BOUNDARY_TEST] No test counts `/v1/config` for a multi-leg join
- Location: lines 97-154, `every_reference_to_a_table_loads_it_once`
- Issue: The test carries `/// Scenario: N-table join reuses one session across all legs`. That scenario's THEN clause requires at most one OAuth2 grant and one `/v1/config` lookup for the whole join request. The self-join row counts only `loadTable` GETs. `a_statement_authenticates_once_per_virtual_schema` counts `/v1/config` for a UNION ALL only. The deleted `a_two_leg_join_resolves_both_legs_on_one_catalog_session` was the last test that counted `/v1/config` for a join.
- Fix: In statement_reuse.rs, add a `configs: usize` field to `Reference`. Set it to 1 for the three `PROXY_ICEBERG_VS` rows and to 0 for the direct-storage row. Count both patterns with `count_proxy_requests([case.request.as_str(), config_get().as_str()], ...)`, and assert `configs == case.configs` with the message `"{}: /v1/config lookups"`.

#### [INFORMATION_COUPLING] The new test module imports helpers from the refresh test module
- Location: lines 15-17
- Issue: `statement_reuse.rs` imports `append_file`, `drop_table_if_exists`, `ensure_id_val_table`, `id_val_batch`, and `seed_catalog` from `crate::refresh`. This change made three of them `pub(crate)` only for this import. `specs/testing.md` § Test code requires a helper that a second test file needs to move into the area's support module, and the plan names `e2e_local/fixtures.rs` as that module. A change to `refresh.rs` now breaks an unrelated module.
- Fix: Move `seed_catalog`, `id_val_batch`, `ensure_id_val_table`, `drop_table_if_exists`, and `append_file` from crates/lakehouse-e2e/tests/e2e_local/refresh.rs to crates/lakehouse-e2e/tests/e2e_local/fixtures.rs. Move with them every private helper they call, such as `id_val_schema` and `load`. Import them from `crate::fixtures` in refresh.rs, table_filter.rs, and statement_reuse.rs. Run `cargo check --workspace --features lakehouse-e2e/exasol-e2e --tests`.

## Expert fixes

### crates/lakehouse-e2e/tests/e2e_local/fixtures.rs

#### [TACTICAL_SHORTCUT] The proxy fixture is rebuilt by every test, because tests with another proxy setting overwrite its scripts
- Location: line 202 (`proxy_virtual_schemas`). Also proxy.rs lines 62 and 82, e2e_lakekeeper_test.rs line 1586 (`create_proxy_virtual_schema`), and e2e_unity_test.rs line 2626 (`create_proxy_virtual_schema`).
- Issue: `proxy_virtual_schemas` rebuilds everything on each of its six calls: it reinstalls the `LHVS_PROXY` scripts, rewrites the Parquet fixture, and recreates and refreshes three virtual schemas. A one-time setup would break, because `no_proxy_hosts_bypass_an_unreachable_proxy` and `an_unreachable_proxy_fails_create_naming_the_proxy_without_its_password` reinstall the same `LHVS_PROXY` scripts with an unreachable proxy. libtest runs `proxy::*` before `statement_reuse::*`, so the tests are coupled through shared Exasol script state. `specs/testing.md` § Shared harness requires one lazy setup per fixture. The Lakekeeper and Unity `create_proxy_virtual_schema` helpers also rebuild on every call, and each now has two callers.
- Fix:
  1. In e2e_local/fixtures.rs, add an `adapter_schema: &str` parameter to `setup_proxy`.
  2. In proxy.rs, make `no_proxy_hosts_bypass_an_unreachable_proxy` and `an_unreachable_proxy_fails_create_naming_the_proxy_without_its_password` pass a new constant `LHVS_PROXY_ENV`. Build their `VsProps` with `.with_adapter_schema(LHVS_PROXY_ENV)`.
  3. Wrap the body of `proxy_virtual_schemas` in a `static DONE: OnceLock<()>` with `get_or_init`, as `lakehouse_vs` does, so `LHVS_PROXY` keeps the working proxy setting for every later test.
  4. In e2e_lakekeeper_test.rs and e2e_unity_test.rs, guard `create_proxy_virtual_schema` the same way. In the Lakekeeper suite, have callers take the catalog URI from `proxy_catalog_uri()` instead of the return value.
  5. Run `make e2e-up test-e2e`, then `make lakekeeper-up test-e2e-lakekeeper` and `make unity-up test-e2e-unity`.

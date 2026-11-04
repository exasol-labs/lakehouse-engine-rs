# Verification Report: change-pushdown-table-loading-statement-cache

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The adapter loads each table once per statement and virtual schema. Build, 2,094 host tests, lint, and the local, Lakekeeper, Unity, and Glue E2E suites pass. |
| Code review | 12 findings, 12 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | not measured |
| Integration | not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit and host integration (`cargo test --workspace`) | 2,096 | 2,094 | 2 |
| E2E local (`make e2e-up test-e2e`) | 349 | 349 | 0 |
| E2E Lakekeeper (`make lakekeeper-up test-e2e-lakekeeper`) | 52 | 52 | 0 |
| E2E Unity (`make unity-up test-e2e-unity`) | 44 | 44 | 0 |
| E2E Glue (`make glue-up test-e2e-glue`, real AWS) | 20 | 20 | 0 |

The 2 ignored host tests are the existing `micro_bench.rs` benchmarks.

The first full local run failed 1 test, `two_virtual_schemas_in_one_statement_load_the_table_once_each`. The test ordered a `UNION ALL` with a trailing `ORDER BY`. Exasol 2025.1.16 pushes that ORDER BY into the first virtual-schema leg only and does not sort the union, so the second leg's rows came back in file order. `EXPLAIN VIRTUAL` shows the first leg's request with `orderBy` and the second leg's request without it. The behavior is the same on `main`. The test now orders outside a derived table. It passed 6 of 6 isolated runs and the full local rerun.

### Manual Tests

Run against the local stack after `make test-e2e`, with each proxy-log count taken after a mark.

| Test | Result |
|------|--------|
| `PROXY_ICEBERG_VS`: `MOR_POS_FILE` twice plus `MOR_DV` in one UNION ALL | ✓ 126,057 rows (2 × 16 + 126,025), 1 `/v1/config` GET |
| `PROXY_ICEBERG_VS`: `MOR_POS_FILE` self-join on `ID` | ✓ 16 rows, 1 `loadTable` GET for `mor_pos_file` |
| `PROXY_DIRECT_VS`: `ROWS` UNION ALL `ROWS` | ✓ 6 rows, 1 directory LIST |

The Lakekeeper refusal, the assume-role count, and the Unity self-join are proved by the E2E tests in the Scenario Coverage table and were not repeated by hand.

## Tool Evidence

### Linter

```
make lint: cargo clippy --workspace --all-targets --all-features -- -D warnings: exit 0, no warnings
```

### Formatter

```
make fmt-check: exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| file-planning | pushdown-format-neutral-resolution | The pushdowns of one statement share one catalog session per virtual schema | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs`, `crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs` | `a_statement_authenticates_once_per_virtual_schema`, `a_statement_sends_one_token_request_and_one_batch_check` | Pass |
| file-planning | pushdown-format-neutral-resolution | Every reference to one table in a statement loads the table once and prunes it with its own filter | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs`, `crates/lakehouse-e2e/tests/e2e_unity_test.rs` | `every_reference_to_a_table_loads_it_once`, `a_later_statement_reads_a_file_added_after_the_first`, `a_script_reloads_the_table_for_each_of_its_statements`, `unity_delta_self_join_loads_the_table_once` | Pass |
| file-planning | pushdown-format-neutral-resolution | Two virtual schemas in one statement use their own CONNECTION | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs` | `two_virtual_schemas_in_one_statement_load_the_table_once_each` | Pass |
| file-planning | pushdown-format-neutral-resolution | A request's table resolutions run concurrently rather than one after another | `crates/lakehouse-engine/src/adapter/pushdown/joins/joins_tests.rs`, `crates/lakehouse-e2e/tests/e2e_unity_test.rs` | `delta_join_legs_replay_concurrently_on_a_current_thread_runtime`, `join_legs_resolve_concurrently_in_leg_index_order`, `unity_delta_join_and_aggregate_pushdown_return_correct_rows` | Pass |
| file-planning | pushdown-planning-file-resolution | Pushdown derives the scanned Iceberg table from the involved virtual table | existing tagged tests | 3 tagged tests | Pass |
| file-planning | iceberg-type-promotion | A promotion this engine reads resolves through the shared relaxation cast | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | the moved promotion tests (no scenario tag on `main` or this branch) | Pass |
| catalog | pushdown-catalog-session | Single-table pushdown builds one catalog session and reuses it | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs`, `crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs` | `a_statement_authenticates_once_per_virtual_schema`, `a_statement_sends_one_token_request_and_one_batch_check` | Pass |
| catalog | pushdown-catalog-session | N-table join reuses one session across all legs | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs` | `every_reference_to_a_table_loads_it_once` | Pass |
| catalog | pushdown-catalog-session | The Lakekeeper permission check reuses the request's one catalog session | `crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs` | `a_statement_sends_one_token_request_and_one_batch_check` | Pass |
| vs-adapter | lakekeeper-permission-check | One batch-check per pushdown decides every table that no earlier pushdown of the statement checked | `crates/lakehouse-e2e/tests/e2e_lakekeeper_test.rs`, `crates/lakehouse-engine/src/adapter/pushdown/scan_resolution_tests.rs` | `a_union_statement_checks_every_table_its_pushdowns_read`, `a_statement_sends_one_token_request_and_one_batch_check`, `permission_check_decides_every_single_table_shape`, `permission_check_refuses_the_same_query_once_a_grant_is_revoked`, `only_unchecked_identifiers_are_batch_checked` | Pass |
| vs-adapter | catalog-kind-selection | Absent CATALOG_KIND resolves the Iceberg REST catalog kind | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs`, `crates/lakehouse-engine/src/adapter/catalog_kind_tests.rs` | `a_statement_authenticates_once_per_virtual_schema`, the existing tagged test | Pass |
| vs-adapter | catalog-kind-selection | A pushdown request under the Unity Catalog kind is planned as a Delta scan | `crates/lakehouse-e2e/tests/e2e_unity_test.rs` | `unity_delta_join_and_aggregate_pushdown_return_correct_rows` (no scenario tag on `main` or this branch) | Pass |
| connection | connection-credentials-assume-role-session-use | The adapter assumes the role once per statement and virtual schema and substitutes the session | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs` | `a_role_session_serves_every_pushdown_of_a_statement` | Pass |
| delta | delta-table-planning | A Unity Catalog or Glue table is planned by the format its catalog reports | `crates/lakehouse-engine/src/adapter/pushdown/scan_catalog_tests.rs`, `crates/lakehouse-e2e/tests/e2e_unity_test.rs`, `crates/lakehouse-e2e/tests/e2e_glue_test.rs` | `a_unity_catalog_load_refuses_an_iceberg_table`, `a_glue_load_refuses_a_delta_table`, `unity_parquet_table_is_listed_and_returns_its_rows_and_partition_values`, `glue_checks_pass_against_one_provisioned_fixture` | Pass |
| delta | delta-table-planning | The Delta reader is reached from production pushdown under the Unity Catalog kind | `crates/lakehouse-e2e/tests/e2e_unity_test.rs` | `unity_delta_join_and_aggregate_pushdown_return_correct_rows` | Pass |
| direct-storage | direct-storage-table-planning | A direct-storage pushdown loads its table from the directory its identifier note names | `crates/lakehouse-e2e/tests/e2e_local/statement_reuse.rs`, `crates/lakehouse-engine/src/adapter/pushdown/scan_catalog_tests.rs` | `every_reference_to_a_table_loads_it_once`, `a_direct_storage_load_composes_its_root_from_the_identifier` | Pass |

No test or doc under `crates/` or `docs/` names any of the 7 removed scenario titles.

## Notes

- Pre-existing wrong-results defect, outside this plan: with a virtual-schema table as the first leg, `UNION ALL ... ORDER BY [LIMIT n]` binds the ORDER BY and LIMIT to that leg only. With `ORDER BY 1 LIMIT 5`, two 20-row legs return 25 rows, while native tables return 5. `EXPLAIN VIRTUAL` shows that Exasol builds the second leg's request without `orderBy` or `limit`. The adapter honors every request it receives, but the adapter advertises ORDER BY and LIMIT, so users see the deviation. No issue covers it yet (#TBD).
- Deviations from the plan text:
  - The pushdown pipeline takes a runtime-free `StatementCatalog`. `VirtualSchemaState` holds that catalog and the Tokio runtime. The reason is that `#[tokio::test]` tests panic when a runtime drops inside a runtime.
  - The Delta loaded table keeps no separate table-root store, because the snapshot's engine owns it.
  - `a_union_statement_checks_every_table_its_pushdowns_read` uses `E2E_DIM_TABLE`, because `TABLE_ALPHA` is in a namespace that `VS_PERMISSION` does not map.
  - Error order differs in two cases where no request is sent: a Glue client-build error now precedes an identifier error, and an Iceberg name-mapping or table-build error now precedes a relaxed-nullability refusal.
- Two scenarios carry no `/// Scenario:` tag, on `main` or this branch: "A pushdown request under the Unity Catalog kind is planned as a Delta scan" and "A promotion this engine reads resolves through the shared relaxation cast". The tests listed above prove both.
- Each statement's adapter VM now holds every table it loaded until the statement ends: up to 32 MB of Iceberg manifests per table, one Delta engine thread, and the full Parquet listing or Glue partition list (perf issue 15).

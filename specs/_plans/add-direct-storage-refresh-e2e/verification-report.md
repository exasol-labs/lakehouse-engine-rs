# Verification Report: add-direct-storage-refresh-e2e

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The staged direct-storage REFRESH E2E test passes against the local Docker Exasol, twice in a row, and the six documentation gaps of issue #479 are closed. |
| Code review | 10 findings, 10 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ (see Notes on one unrelated untracked spike test) |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | Not measured. The plan changes no production code. |
| Integration | Not measured. The plan adds one E2E test and test helpers. |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace --no-fail-fast`) | 1923 | 1922 | 0 |
| Integration (`make test-e2e`, `LH_EXASOL_CPUSET=0-1`) | all test binaries, exit 0 | all | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `SYS.EXA_ALL_TABLES` of `DIRECT_REFRESH_VS` lists `T_APPEND`, `T_CONFLICT`, `T_EVOLVE`, `T_HIVE`, `T_NEW` | ✓ |
| `SELECT ID, QTY, NEW_COL FROM DIRECT_REFRESH_VS.T_EVOLVE ORDER BY ID` returns `1,10,`, `2,20,`, `3,5000000000,x3`, `4,50000000000,x4` | ✓ |
| `SELECT ID FROM DIRECT_REFRESH_VS.T_CONFLICT` fails with the fold error naming `X`, `Int64`, `Utf8`, and both `t_conflict` files | ✓ |
| `ALTER VIRTUAL SCHEMA DIRECT_REFRESH_VS REFRESH` fails with the same message and no credential value | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings: exit 0
cargo clippy -p lakehouse-engine --all-targets --features exasol-e2e -- -D warnings: exit 0
```

### Formatter

```
cargo fmt --all -- --check: exit 0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | refresh-and-set-properties | Refresh re-enumerates the namespace and returns a refresh response (direct-storage arm) | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` | Pass |
| vs-adapter | refresh-and-set-properties | Refresh reflects table and column structure changes (direct-storage arm) | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` | Pass |
| direct-storage | direct-storage-table-planning | Until a refresh, a query reads the current files under the declared columns | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` | Pass |
| direct-storage | direct-storage-table-planning | Until a refresh, a file the declaration cannot hold fails the query and never returns a wrong value | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` | Pass |
| direct-storage | direct-storage-table-discovery | A refresh that a column pair fails keeps the previous declaration queryable | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema` | Pass |
| direct-storage | direct-storage-table-discovery | A first-level directory holding no data file is skipped, not failed | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `refresh_tracks_storage_changes_and_a_failed_refresh_keeps_the_schema`, `loose_file_and_empty_directory_serve_no_table` | Pass |
| vs-adapter | refresh-and-set-properties | Iceberg arms of the two refresh scenarios (unchanged) | `crates/lakehouse-engine/tests/e2e_refresh_test.rs` | `refresh_reenumerates_namespace`, `refresh_reflects_added_table_and_column_change` | Pass |

## Notes

- `cargo test --workspace` exits 101 because of `spike_float_decimal_rule::replay`, which panics with `set PROBES_JSONL`. It lives in an untracked spike file that predates this plan and is not part of the change. All other 1922 tests pass with `--no-fail-fast`.
- The plan has no `workspace/version` delta and changes no shipped code, so the workspace version stays unchanged. Earlier test-only and docs-only PRs did the same.
- The first `make test-e2e` run failed in `adapter_detects_container_cpuset`, because the container CPU set equaled the host core count. The run was repeated with `LH_EXASOL_CPUSET=0-1` and passed. This is an environment precondition.
- Serena `rename_symbol` could not rename the test in task 1.4, because the file sits behind the `exasol-e2e` feature. The rename was applied as a text replacement, and no other reference exists.
- Not independently verified in documentation: the Glue Hive Parquet size source (the claim rests on the Glue planning spec), and the explicit MILLIS, MICROS, and NANOS timestamp annotations (the claim rests on the plan's live probe).
- Out of scope, not fixed: the `CATALOG_KIND` row of `docs/tuning.md` lists only `UNITY_CATALOG` and contradicts `docs/catalogs.md`.

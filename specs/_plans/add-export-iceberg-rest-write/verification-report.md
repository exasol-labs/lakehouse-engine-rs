# Verification Report: add-export-iceberg-rest-write

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Build, lint, format, unit tests, both E2E suites, and the changed-line coverage gate all pass. The manual steps were not run by hand. Equivalent E2E tests cover them. |
| Code review | 21 findings (standard 17, expert 4), all 21 fixed as tasks 4.4 to 4.24. The review ran once, before this verification, and was not repeated. |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (covered by E2E, not run by hand) |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Changed production lines (host `cargo llvm-cov`, threshold 90%) | 95.4% (2058 of 2157 lines) |

The repository script `changed_line_coverage.py` diffs `merge-base..HEAD`, so it reported 0/0 on the uncommitted work. The figure above comes from a copy of the script that diffs the working tree. The E2E suites run the `.so` inside Exasol, so `llvm-cov` does not count them.

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace --lib`) | 2090 | 2090 | 0 |
| E2E local (`make e2e-up test-e2e`) | 368 | 368 | 0 |
| E2E Lakekeeper (`make lakekeeper-up test-e2e-lakekeeper`) | 63 | 63 | 0 |

Build: `make cross-udf-build` exited 0.

### Manual Tests

| Test | Result |
|------|--------|
| Export into a new table, then read it back | ✓ `export_creates_missing_namespace_and_table`, `export_round_trips_every_supported_type` |
| The same export again is refused | ✓ `export_refuses_an_existing_table` |
| An empty source creates no table | ✓ `export_empty_source_creates_no_table` |
| An export without `ALLOW_HTTP` is refused | ✓ `export_plain_http_needs_allow_http` |
| An export without the script-scoped grant is refused | ✓ `export_missing_script_grant_fails_named` |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)
```

### Formatter

```
cargo fmt --all -- --check
(no changes)
```

## Scenario Coverage

`local` is `crates/lakehouse-e2e/tests/e2e_local/export.rs`. `lakekeeper` is `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_write.rs`.

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| export | export-iceberg-table | The export creates a missing namespace and table | `lakekeeper` | `export_creates_missing_namespace_and_table` | Pass |
| export | export-iceberg-table | Every supported type reads back unchanged | `lakekeeper` | `export_round_trips_every_supported_type` | Pass |
| export | export-iceberg-table | A column of an unsupported type fails the export | `lakekeeper` | `export_refuses_unsupported_column_types` | Pass |
| export | export-iceberg-table | An export into an existing table is refused | `lakekeeper` | `export_refuses_an_existing_table` | Pass |
| export | export-iceberg-table | Two exports that create the same table leave one winner | `lakekeeper` | `export_racing_exports_leave_one_winner` | Pass |
| export | export-iceberg-table | Concurrent writers create one table, every commit carries the export ID | `lakekeeper` | `export_concurrent_writers_commit_once_with_the_export_id` | Pass |
| export | export-iceberg-table | A commit retried after an unknown outcome adds its rows once | `crates/lakehouse-catalog-write/src/iceberg_write_tests.rs` | `unknown_outcome_with_tagged_reload_commits_once`, `staged_unknown_outcome_with_created_table_commits_once`, `conflict_with_untagged_reload_commits_again` | Pass |
| export | export-iceberg-table | A bad CONNECTION fails before any data is read | `lakekeeper` | `export_bad_catalog_connection_fails_before_reading` | Pass |
| export | export-iceberg-table | A wrong storage key fails the writer | `lakekeeper` | `export_wrong_storage_secret_fails_at_the_writer` | Pass |
| export | export-iceberg-table | SINGLE_COMMIT on a catalog without staged creation fails | `local` | `export_single_commit_on_catalog_refusing_stage_create_fails` | Pass |
| export | export-iceberg-table | SINGLE_COMMIT creates the table only with its rows | `lakekeeper` | `export_single_commit_creates_the_table_with_its_rows` | Pass |
| export | export-iceberg-table | A failed SINGLE_COMMIT export leaves no table | `local` | `export_failed_single_commit_leaves_no_table` | Pass |
| export | export-iceberg-table | An empty source exports zero rows | `lakekeeper` | `export_empty_source_creates_no_table` | Pass |
| export | export-into-script | A whole-table export writes every source row | `local` | `export_writes_every_source_row` | Pass |
| export | export-options | WRITERS_PER_NODE sets the writer count | `local` | `export_writer_layout_sets_writer_count` | Pass |
| export | export-options | Invalid options and statements fail before reading | `local` | `export_refuses_invalid_statements_before_reading` | Pass |
| export | export-options | Plain HTTP needs ALLOW_HTTP | `local` | `export_plain_http_needs_allow_http` | Pass |
| export | export-options | A CONNECTION or column name the export cannot write is refused | `local` | `export_refuses_unwritable_connections_and_column_names` | Pass |
| export | export-privileges | A user who holds the export privileges exports rows | `local` | `export_role_user_exports_rows` | Pass |
| export | export-privileges | A missing script-scoped CONNECTION grant fails with a named error | `local` | `export_missing_script_grant_fails_named` | Pass |
| export | export-privileges | No CONNECTION value appears in the SQL or an error | `local` | `export_errors_carry_no_connection_password` | Pass |

## Notes

- Not re-run in this verification: the `unsupported table location` unit test, the vended-credential refresh tests, and the OAuth renewal tests. They are part of the 2090 passing unit tests. I did not name each one in the audit.
- The `deploy/scripts/tests/install.test.sh` installer test is unchanged and was not run here.
- Coverage was measured on the working tree before the implementation commit. Re-measure with the repository script after the commit if the gate must come from `HEAD`.
- Open plan items marked `(#TBD)`: STS role-session renewal, and an upstream iceberg-rust change that exposes a commit's requirements and updates. Both need a human-opened issue.

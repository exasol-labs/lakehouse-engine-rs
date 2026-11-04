# Tasks: add-export-into-script-entry-points

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [x] tested-green
- [ ] recorded
- [ ] pr-ready

Task text lives in plan.md § Implementation Tasks. Numbers match.

## Phase 2: Implementation (Group A)
- [x] 1.1 Add `export` feature to the `exasol-udf-sdk` workspace pin
- [x] 1.2 Add a private `writer_count` to `export/sql.rs` with `writer_count_caps_at_300`
- [x] 1.3 Add `export/options.rs` with option validation and `options_tests.rs`
- [x] 1.4 Add `export/sql.rs` writer SELECT renderer with type guards [expert]
- [x] 1.5 Add `export/mod.rs` with `generate_export_sql` and `run_writer`
- [x] 1.6 Declare the `LAKEHOUSE_EXPORT` entry point in `lib.rs`; update `two_entry_points.rs`
- [x] 1.7 Add `EXPORT_SCRIPT_NAME` and export DDL to the E2E harness; update `specs/testing.md`
- [x] 1.8 Add `e2e_local/export.rs` scaffold and fixtures
- [x] 1.9 Row-count and writer-count E2E tests
- [x] 1.10 Refusal E2E tests
- [x] 1.11 Privilege and credential E2E tests
- [x] 1.12 Add `cloud_export_writer_counts` to `cloud_e2e_test.rs`
- [~] 1.13 Run the cloud test on a multi-node cluster, or raise the Open Question (the three `deploy/README.md` steps are written; the run itself is pending)
- [x] 1.14 Add `docs/export.md` and link it from `docs/index.md`

## Phase 2: Implementation (Group B)
- [x] 2.1 Add `ddl_export` to `install.sh`
- [x] 2.2 Print the export role template in `print_next_step_template`
- [x] 2.3 Update `install.test.sh`
- [x] 2.4 Update `docs/install.md`
- [x] 2.5 Update `docs/security.md`

## Phase 3: Verification
- [~] 3.1 Run the plan's Checklist (build, test, E2E, fmt-check lint done; `make test-install` blocked by missing `jq`; coverage not run)
- [~] 3.2 Cloud E2E (task 1.13): no multi-node cluster
- [~] 3.3 Manual testing per plan: `exapump` not installed on host; equivalent E2E tests pass

## Phase 4: Review Fixes
- [x] 4.1 In `export/sql.rs` `WriterSelect::render`, reduce the group-key comment to `// The key stays top-level: an export SELECT over a subquery of DUAL fails.`
- [x] 4.2 In `export/mod.rs` `require_connection_access`, change the role clause to `or to a role that {user} holds`, drop the `ctx.script_schema()` argument, and remove the `LAKEHOUSE_EXPORT_ROLE_LHVS` fragment from `export_tests.rs`
- [x] 4.3 In `export/options.rs`, move `check_table_name` from `WithOptions::writer_layout` into `ExportOptions::from_spec`
- [x] 4.4 In `deploy/scripts/install.sh` `print_next_step_template`, change the export line to end `one role serves every exporting user. SYS needs no grant.`
- [x] 4.5 In `e2e_local/export.rs` `exporters`, bind the ACCESS grant separately and grant the three-entry `privileges` array to both the role and `NO_ACCESS_EXPORTER`, without the `[..3]` slice
- [x] 4.6 Move `parse_int` into `common/exasol_ws.rs`, add `profiled_export_counts` there, and reduce `profiled_export` and `cloud_profiled_export` to calls to it; delete `cloud_int`

## Phase 5: Independent Review Fixes
- [x] 5.1 In `cloud_export_writer_counts`, filter the second export's source to `per_node_two` rows and expect `per_node_two` rows, so it holds one row per writer when the writer count is capped
- [x] 5.2 Remove `sharding::group_count` and its test, restore `support.rs` `MAX_SHARD_COUNT` and `shard_count`, and keep the writer-count calculation as a private `writer_count` in `export/sql.rs` with its own unit tests
- [x] 5.3 Delete the `Export script shape` step from the `install-script-e2e` job and restore the job's leading comment
- [x] 5.4 Re-check `verification-report.md` against the edits and remove its CI step mention

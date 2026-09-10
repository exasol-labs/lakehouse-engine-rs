# Plan: fix-substr-left-unicode-expressions-pushdown

## Summary

Enables the `unicode_expressions` DataFusion feature on `crates/lakehouse-engine` so the advertised `FN_SUBSTR` and `FN_LEFT` capabilities execute in the scan UDF instead of failing the query. Closes issue #187.

## Design

### Context

The adapter advertises `FN_SUBSTR` (`crates/lakehouse-engine/src/adapter/capabilities.rs:111`) and `FN_LEFT` (`:98`). The translator renders `SUBSTR` as `substr(...)` (`crates/vs-expression/src/lib.rs:1222`). Exasol delegates both fully and never re-applies them, so a scan that cannot plan the fragment fails the whole query.

The scan UDF resolves a rendered fragment through two separate DataFusion surfaces. A plain call reaches the scalar-function registry. `SUBSTR(...)` does not parse as a plain call: `sqlparser` 0.62 turns `SUBSTR` and `SUBSTRING` into the `Substring` SQL AST node (`parser/mod.rs:1570`), which reaches only the registered `ExprPlanner` list. `SessionStateDefaults::default_expr_planners()` registers the one default planner for that node, `UnicodeFunctionPlanner`, behind `#[cfg(feature = "unicode_expressions")]` (`datafusion-54.1.0/src/execution/session_state_defaults.rs:96`). The workspace pins `datafusion = { version = "54.1", default-features = false, features = ["parquet", "sql"] }` (root `Cargo.toml:41`), so that gate is off and the planner is absent.

A probe against the current workspace confirms the split. `SELECT substr('abcdef', 2, 3)` and `SELECT substring('abcdef', 2, 3)` both fail with the exact error issue #187 reports. `left`, `right`, `character_length`, `strpos`, `lpad`, `upper`, `replace` and `date_part` all plan today, because `datafusion` depends on `datafusion-functions` without `default-features = false`, so that crate keeps its own `default` feature set and registers the unicode UDFs.

That last fact narrows the bug. `left(...)` plans at the DataFusion level today, so the `LEFT` failure issue #187 reports does not come from the translator's `LEFT` to `left` mapping. It comes from the pushdown request Exasol sends for `LEFT(...)`. Task 3.1 captures that request and records which node arrives.

- **Goals**: make `SUBSTR` and `LEFT` queries return rows through the pushdown path. Add a host regression test that fails if the feature is dropped again. Add an end-to-end test that proves the scan, not Exasol, evaluated the expression.
- **Non-Goals**: no audit of every advertised capability against every DataFusion feature. No withdrawal of `FN_SUBSTR` or `FN_LEFT`. No change to the translator, to `capabilities.rs`, or to any rendering rule.

### Decision

Declare the feature on the member manifest, not on `[workspace.dependencies]`:

```toml
# crates/lakehouse-engine/Cargo.toml
datafusion = { workspace = true, features = ["unicode_expressions"] }
```

`workspace = true` inherits the version, `default-features = false`, and the `["parquet", "sql"]` set from the root manifest. The member entry unions `unicode_expressions` onto that set.

The member manifest is the placement this repository already uses for every added feature: `arrow` gains `json`, `object_store` gains `azure`, `tokio` gains `rt-multi-thread` and `sync`, and `delta_kernel` is declared there outright. The root manifest states the reason at `Cargo.toml:27`: CI's `Swatinem/rust-cache` key hashes member manifests, and the root manifest is not a `cargo metadata` member because it declares no `[package]`.

#### Blast radius

Enabling `datafusion/unicode_expressions` adds two `ExprPlanner` hooks and nothing else. `UnicodeFunctionPlanner` implements exactly `plan_position` and `plan_substring` (`datafusion-functions-54.1.0/src/unicode/planner.rs`). The feature forwards to `datafusion-sql?/unicode_expressions` and `datafusion-functions/unicode_expressions`, both already enabled transitively, and both declared as empty feature lists that pull in no crate. A local check confirmed `Cargo.lock` does not change, so the `cargo deny` license gate sees the same dependency set.

The translator never emits `POSITION(... IN ...)`, so `plan_position` stays unreached. `INSTR` and `LOCATE` render as `strpos(...)`, a plain call.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Enable `unicode_expressions` | Withdraw `FN_SUBSTR` and `FN_LEFT` from `capabilities.rs` | The user rejected withdrawal. Withdrawal moves two common functions back to Exasol-side evaluation over full scan output. |
| Declare on `crates/lakehouse-engine/Cargo.toml` | Declare on root `[workspace.dependencies]` | The member manifest enters CI's rust-cache key, the root manifest does not. The repository already documents this preference and follows it for four other dependencies. |
| No `shared-key` bump in CI | Bump `workspace-rlib`, `llvm-cov` and `workspace-arm64` together | The member-manifest placement rotates all three keys on its own. A hand bump would be redundant and risks the partial-bump hazard issue #133 records. |
| Two spec deltas, no capability audit | One delta on the scan spec only | The gap has two halves: the scan could not plan the fragment, and the advertisement carried no executability requirement. Each half belongs to the feature that owns it. |
| Test the filter position as well as the select list | Test the issue's select-list repro only | The advertised capability covers both positions and both render the same fragment. An untested predicate position is how issue #370 reached production. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-expression-pushdown | CHANGED | `specs/_plans/fix-substr-left-unicode-expressions-pushdown/datafusion-scan/scan-execution-expression-pushdown/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `specs/_plans/fix-substr-left-unicode-expressions-pushdown/vs-adapter/pushdown-planning-capability-extensions/spec.md` |

## Impact

`SUBSTR` and `LEFT` over a virtual-schema column stop failing and start returning rows. No breaking change: no query that works today changes its result, because the only added behavior is a planner for an AST node that previously had none. `Cargo.lock` does not change, so no new third-party code enters the `.so`.

## Dependencies

None beyond the pinned `datafusion` 54.1.0 already in the lockfile. The `unicode_expressions` feature exists on that version and pulls in no crate.

## Implementation Tasks

### 1. Enable the DataFusion feature

- [ ] 1.1 In `crates/lakehouse-engine/Cargo.toml`, change `datafusion = { workspace = true }` to `datafusion = { workspace = true, features = ["unicode_expressions"] }`. Add a one-line comment stating that the feature registers `UnicodeFunctionPlanner`, which is what plans the `Substring` AST node that `substr(...)` parses to. Do NOT edit the root `Cargo.toml` `[workspace.dependencies]` entry.
- [ ] 1.2 Run `git diff Cargo.lock` and confirm it reports no change. A changed lockfile means the feature pulled in a crate and the license gate needs a second look.
- [ ] 1.3 Correct the stale namespace list in the root `Cargo.toml` comment at line 33. It names `workspace-rlib` and `llvm-cov` but omits the third namespace, `workspace-arm64`, written by `ci.yml`'s `arm64` job at line 370. Add it, so a future hand bump covers all three and does not repeat the partial-bump failure issue #133 records.

### 2. Host regression test for the scan path

- [ ] 2.1 Add `crates/lakehouse-engine/tests/scan_substr_expression.rs`. Copy the Docker-free harness shape from `tests/scan_column_binding.rs`: write a local Parquet file with `ArrowWriter`, build a `ScanSpec` over a `file://` URL, and drive `run_raw_scan_with_session` with `session_config_for_spec`. Seed one string column with values that make a substring assertion unambiguous.
- [ ] 2.2 Assert the select-list position: a `ProjectionItem::Expr { expr: "substr(\"NAME\", 1, 5)" }` emits the expected substring for every row. This test MUST fail on the unpatched manifest with `Substring could not be planned by registered expr planner`, so run it once with task 1.1 reverted before keeping it.
- [ ] 2.3 Assert the filter position: a spec whose `filter` carries a `substr(...) = '<literal>'` comparison emits exactly the matching rows. Pushed predicates bypass the emit-boundary checks the select-list path has, so this position needs its own case.
- [ ] 2.4 Assert that a `ProjectionItem::Expr` carrying `left("NAME", 5)` still plans and evaluates in the same spec, pinning the scenario clause that the plain-call path is unchanged.

### 3. End-to-end test for the advertised capability

- [ ] 3.1 Start the stack first: `make test-e2e` never starts docker compose, and without it every DB-backed test fails rather than skips. Run `docker compose -f docker-compose.yml up -d --wait minio minio-init iceberg-rest exasol`. Then reproduce issue #187 with `scripts/capture-pushdown-payload.sh`, using the issue's own query shape. Record which `function_scalar` node Exasol sends for `LEFT(...)`, because `left(...)` plans at the DataFusion level today and the reported `LEFT` failure must therefore originate in the pushdown request. Note the finding in `decision-log.md`.
- [ ] 3.2 Add `e2e_substr_left_pushdown` to `crates/lakehouse-engine/tests/e2e_capability_test.rs`, in a new section after 8.16. Query the shared `E2E_TABLE` seed through `vs_table()`, whose `name` column holds `event-NN`, and assert `SUBSTR(name, 1, 5)` and `LEFT(name, 5)` both return `event` while `SUBSTR(name, 7, 2)` returns the two-digit id. Use `setup_e2e()`, `exa_conn()` and `query_columns` exactly as `e2e_upper_varchar_pushdown` does.
- [ ] 3.3 In the same test, call `explain_virtual_sql` for the query and assert the generated pushdown SQL contains `substr(`. Without this assertion the test passes even if Exasol silently evaluates the expression itself, which would leave the bug unfixed and undetected.
- [ ] 3.4 Confirm the new test runs under `make test-e2e`. `e2e_capability_test` is already in that target's `--test` list, so no `Makefile` change is expected. Check that no change is needed rather than assuming it.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: unicode_expressions fix and its two tests | 1.1-1.3, 2.1-2.4, 3.1-3.4 | — | spec deltas `datafusion-scan/scan-execution-expression-pushdown` and `vs-adapter/pushdown-planning-capability-extensions`; `crates/lakehouse-engine/Cargo.toml`, root `Cargo.toml`, `crates/lakehouse-engine/tests/scan_substr_expression.rs`, `crates/lakehouse-engine/tests/e2e_capability_test.rs` |

One group, no parallelism. Both spec deltas describe one defect and both tests prove the same one-line manifest change. Splitting them would give two agents the same root-cause explanation and the same `Cargo.toml` to contend on. Task 1.1 gates every test task, so the group runs in task order.

Traceability: task 1.1 implements both spec deltas. Tasks 2.1 to 2.4 cover the `datafusion-scan/scan-execution-expression-pushdown` scenario. Tasks 3.1 to 3.4 cover the `vs-adapter/pushdown-planning-capability-extensions` scenario. Tasks 1.2 and 1.3 carry no spec delta on purpose: 1.2 checks the lockfile claim this plan makes, and 1.3 corrects a build-comment defect the planning brief asked the planner to resolve.

No task carries `[expert]`. The manifest edit is one line, the host test copies an existing harness, and the end-to-end test copies an existing test shape. The plan resolved the CI cache-key question at plan time, so no implementation task has to reason about it.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | The change adds a cargo feature and two tests. It removes no code path and retires no capability. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Scan plans a rendered SUBSTR fragment in select-list and filter positions | Integration | `crates/lakehouse-engine/tests/scan_substr_expression.rs` | `substr_expression_in_select_list_evaluates`, `substr_expression_in_filter_selects_matching_rows`, `left_expression_still_plans_alongside_substr` |
| Advertised FN_SUBSTR and FN_LEFT return rows instead of failing the query | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_substr_left_pushdown` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| datafusion-scan/scan-execution-expression-pushdown | `cargo test -p lakehouse-engine --test scan_substr_expression` | 3 passed, 0 failed |
| datafusion-scan/scan-execution-expression-pushdown (pre-fix repro) | Revert task 1.1, then rerun the command above | Fails with `Substring could not be planned by registered expr planner` |
| vs-adapter/pushdown-planning-capability-extensions | `docker compose -f docker-compose.yml up -d --wait minio minio-init iceberg-rest exasol`, then `make test-e2e` | `e2e_substr_left_pushdown` passes. Read the exit code directly, because piping the target through `tail` hides it. |
| vs-adapter/pushdown-planning-capability-extensions (pushdown proof) | `scripts/capture-pushdown-payload.sh 'SELECT SUBSTR(c_varchar, 1, 1), LEFT(c_varchar, 1) FROM {table} WHERE id <= 5'` | The `EXPLAIN VIRTUAL` output carries `substr(` in the scan SQL, and the real execution returns rows rather than `F-UDF-CL-RUST-9001` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` (stack up first, see Manual Testing) | 0 failures |
| Lint | `cargo clippy --all-targets -- -D warnings` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| Lockfile | `git diff --stat Cargo.lock` | No change |

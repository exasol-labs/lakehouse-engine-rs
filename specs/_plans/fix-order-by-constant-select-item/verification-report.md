# Verification Report: fix-order-by-constant-select-item

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The adapter drops constant sort keys at parse time. The row-scan, grouped, and join paths return the native rows, and a constant above the select-list count no longer fails the query. |
| Code review | 6 findings — 6 fixed |

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
| Unit (changed lines, `support.rs`) | 100.0% (28/28) |
| Unit (workspace lines, `cargo llvm-cov --workspace --lib`) | 90.89% |
| Integration | not measured (E2E runs the deployed `.so`) |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace --lib`) | 1984 | 1984 | 0 |
| Integration (`make e2e-up test-e2e`, local Docker Exasol) | 378 | 378 | 0 |

The E2E run's first minute overlapped with another session's `export_` E2E tests on the same Exasol container. The run still passed all 378 tests.

### Manual Tests

Run against the Docker Exasol after `make e2e-up test-e2e` deployed the fixed build.

| Test | Result |
|------|--------|
| `SELECT ID, 5 … ORDER BY 2 DESC, 1 LIMIT 3`: rows `1,5`, `2,5`, `3,5`, no `Too large value` error | ✓ |
| `SELECT ID, 1, COUNT(*) … GROUP BY ID ORDER BY 2 DESC, 1 LIMIT 3`: ids 1, 2, 3, each count 1 | ✓ |
| Join `SELECT e.ID, 1 … ORDER BY 2 DESC, 1 LIMIT 3`: ids 1, 2, 3 | ✓ |
| `SELECT ID, 1 … ORDER BY 2 LIMIT 3 OFFSET 18`: 2 rows (19, 20) | ✓ |
| `EXPLAIN VIRTUAL … ORDER BY 1+0 DESC, ID LIMIT 3`: scan spec carries `"limit":3` and `"order_by"` on `ID`; pushed SQL is `ORDER BY "ID" ASC NULLS LAST LIMIT 3` | ✓ |
| `EXPLAIN VIRTUAL SELECT ID, 1 … ORDER BY 2 LIMIT 3`: scan spec carries `"limit":3` and no `"order_by"`; pushed SQL ends `LIMIT 3` with no `ORDER BY` | ✓ |
| `EXPLAIN VIRTUAL` join `… ORDER BY 2 DESC, 1 LIMIT 3`: one `LAKEHOUSE_SCAN` fan-out with a `join` block (`post_join_order_by` on `ID`, `post_join_limit` 3); no separate scan legs | ✓ |
| `EXPLAIN VIRTUAL` join `… ORDER BY 2 LIMIT 3 OFFSET 18`: two scan legs `LHS_T0` and `LHS_T1` joined in Exasol SQL; wrapper ends `ORDER BY FALSE ASC NULLS LAST LIMIT 3 OFFSET 18` | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.88s
```

### Formatter

```
cargo fmt --all -- --check
(no output, exit 0)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| pushdown-capabilities | pushdown-planning-order-by-capability | A constant sort key does not become a select-list position | `crates/lakehouse-e2e/tests/e2e_local/order_by_constant.rs`, `join.rs` | `constant_sort_key_returns_the_native_order`, `constant_sort_key_planning_follows_the_other_keys`, `constant_sort_key_join_routes_by_the_other_keys`, `e2e_join_fallback_constant_sort_key_returns_the_native_order` | Pass |
| pushdown-capabilities | pushdown-planning-order-by-capability | An ORDER BY of constants only keeps its OFFSET window | `order_by_constant.rs` | `constant_only_order_by_keeps_its_offset_window` | Pass |
| pushdown-capabilities | pushdown-planning-order-by-capability | An ORDER BY the adapter cannot bound as a top-N remains correctness-safe | `order_by_constant.rs` | `constant_sort_key_returns_the_native_order`, `constant_sort_key_planning_follows_the_other_keys` | Pass |
| pushdown-joins | pushdown-planning-join | Constant sort keys leave a broadcast-eligible join on the broadcast path | `order_by_constant.rs` | `constant_sort_key_join_routes_by_the_other_keys` | Pass |
| pushdown-joins | pushdown-planning-join | Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback | `order_by_constant.rs` | `constant_sort_key_join_routes_by_the_other_keys`, `constant_only_order_by_keeps_its_offset_window` | Pass |
| pushdown-capabilities | pushdown-planning-topn | A constant sort key does not decide the bounded top-N path | `order_by_constant.rs` | `constant_sort_key_planning_follows_the_other_keys` | Pass |
| pushdown-capabilities | pushdown-planning-topn | An ORDER BY of constants only runs as an unordered LIMIT | `order_by_constant.rs` | `constant_only_order_by_runs_as_an_unordered_limit` | Pass |
| pushdown-capabilities | pushdown-planning-topn | Per-shard row limit is emitted only alongside the matching per-shard sort | `order_by_constant.rs` | `constant_sort_key_planning_follows_the_other_keys` | Pass |
| pushdown-capabilities | pushdown-planning-topn | Unsupported ordered-query shapes decline the ordered-top-N path | `order_by_constant.rs`, `scan.rs` | `constant_sort_key_planning_follows_the_other_keys`, `order_by_without_limit_falls_back_correctly` | Pass |

## Notes

- Repro on main (task 1.2, recorded in `tasks.md`): the folded-constant, grouped, and join shapes returned ids 20, 19, 18 or failed with SQL state 04000. Five of the six new E2E tests failed on main. The offset-window test passed on main as predicted.
- The work sits uncommitted on branch `fix/620-order-by-constant-select-item`. Nothing is committed or pushed.
- The plan cites issue #620. The PR body needs `Closes #620`.

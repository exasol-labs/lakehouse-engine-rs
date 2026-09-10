# Verification Report: fix-substr-left-unicode-expressions-pushdown

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `SUBSTR` and `LEFT` queries return rows through the pushdown path. All new tests pass. |
| Code review | 1 finding — 1 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-udf-build` exit 0) |
| Tests | ✓ (`cargo test` exit 0) |
| E2E | ✓ (`e2e_substr_left_pushdown` passes; 2 pre-existing `e2e_int96_timestamp_test` failures on `main`) |
| Lint | ✓ (`cargo clippy --all-targets -- -D warnings` exit 0) |
| Format | ✓ (`cargo fmt --all -- --check` exit 0) |
| Lockfile | ✓ (`git diff --stat Cargo.lock` empty) |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Host (`cargo test`) | All | All | 0 |
| E2E (`make test-e2e`) | All | All except 2 pre-existing | 2 pre-existing (unrelated `e2e_int96_timestamp_test`, missing fixture) |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine --test scan_substr_expression` — 3 host regression tests | ✓ 3 passed |
| Pre-fix repro (revert task 1.1, rerun scan_substr_expression) | ✓ Fails with `Substring could not be planned by registered expr planner` |
| `e2e_substr_left_pushdown` via `make test-e2e` | ✓ Passed |
| Pushdown proof (`explain_virtual_sql` contains `substr(`) | ✓ Asserted in E2E test |

## Tool Evidence

### Linter

```
cargo clippy --all-targets -- -D warnings: exit 0, no warnings
```

### Formatter

```
cargo fmt --all -- --check: exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | scan-execution-expression-pushdown | SUBSTR in select-list | `tests/scan_substr_expression.rs` | `substr_expression_in_select_list_evaluates` | Pass |
| datafusion-scan | scan-execution-expression-pushdown | SUBSTR in filter | `tests/scan_substr_expression.rs` | `substr_expression_in_filter_selects_matching_rows` | Pass |
| datafusion-scan | scan-execution-expression-pushdown | LEFT alongside SUBSTR | `tests/scan_substr_expression.rs` | `left_expression_still_plans_alongside_substr` | Pass |
| vs-adapter | pushdown-planning-capability-extensions | FN_SUBSTR and FN_LEFT return rows | `tests/e2e_capability_test.rs` | `e2e_substr_left_pushdown` | Pass |

## Notes

- 2 `e2e_int96_timestamp_test` failures (`e2e_int96_far_future_timestamp_scans_without_overflow`, `e2e_int96_fixture_present_and_int96_encoded`) are pre-existing on `main`. The `int96_ts_far_future` table does not exist in the test catalog.
- Exasol rewrites `LEFT(col, n)` into a `SUBSTR` `function_scalar` node before the pushdown request reaches the adapter. No `LEFT` node exists in the wire format. This finding is recorded in `decision-log.md` entry [5].

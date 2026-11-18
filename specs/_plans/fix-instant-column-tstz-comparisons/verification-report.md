# Verification Report: fix-instant-column-tstz-comparisons

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | A `TIMESTAMP WITH LOCAL TIME ZONE` value compared directly with an instant column folds to its UTC date and time, and returns the native rows in every session; build, unit tests, local E2E suite, lint, format, and manual tests pass |
| Code review | 7 findings — 7 fixed |

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
| Unit (`cargo test --workspace --lib`) | 2074 | 2074 | 0 |
| Integration (`make e2e-up test-e2e`, local Docker suite) | 415 | 415 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| pushdown-planning-clock-functions: `EXPLAIN VIRTUAL` of `TZ > CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` in a `PACIFIC/KIRITIMATI` session | ✓ the scan filter holds `2024-02-29 20:00:00.000` and no `SESSIONTIMEZONE`; `SESSIONTIMEZONE` appears only in Exasol's `filter_expr_string_for_debug` of the incoming request |
| session-time-zone: `SELECT COUNT(*) ... WHERE TZ > CURRENT_TIMESTAMP - INTERVAL '1' HOUR` in a `PACIFIC/KIRITIMATI` session, with `SESSION_TIME_ZONE` unset and `ASIA/TOKYO` | ✓ 1 row in both, as the native TSTZ copy; a TSTZ-literal bound returns 3 rows in both, as the native copy |
| `docs/semantic-differences.md` workaround: join with a native TSTZ copy in an `EUROPE/BERLIN` session | ✓ `v.TZ = n.TZ` matches 0 of 4 rows, `CONVERT_TZ(v.TZ, 'UTC', SESSIONTIMEZONE) = n.TZ` matches 4 of 4 |

## Tool Evidence

### Linter

```
make lint: cargo clippy --workspace --all-targets --all-features -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s), exit 0
```

### Formatter

```
make fmt-check: cargo fmt --all -- --check, exit 0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| pushdown | pushdown-planning-clock-functions | A TIMESTAMP WITH LOCAL TIME ZONE value compared with an instant column compares the instants | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_tstz_value_compared_with_an_instant_column_compares_the_instants` | Pass |
| pushdown | pushdown-planning-clock-functions | (same, pruning step) | `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` | `clock_and_tstz_filters_prune_files` | Pass |
| pushdown | pushdown-planning-clock-functions | (same, every instant source) | `crates/lakehouse-engine/src/adapter/pushdown/clock_tests.rs` | `tstz_values_compared_with_an_instant_column_fold_to_utc` | Pass |
| pushdown | pushdown-planning-clock-functions | A plain value compared with an instant column compares with its UTC date and time | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_plain_value_compared_with_an_instant_column_compares_its_utc_date_and_time` | Pass |
| pushdown | pushdown-planning-clock-functions | A clock function reaches the pushed SQL as a literal | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `each_clock_function_reaches_the_pushed_sql_as_a_literal` | Pass |
| pushdown | pushdown-planning-clock-functions | A filter on a clock function or a time zone literal prunes files at plan time | `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` | `clock_and_tstz_filters_prune_files` | Pass |
| pushdown | pushdown-planning-clock-functions | A TIMESTAMP WITH LOCAL TIME ZONE literal in a comparison runs in the scan | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_tstz_literal_comparison_runs_in_the_scan` | Pass |
| pushdown | pushdown-planning-clock-functions | The clock functions fold in the zone that Exasol uses when the session zone matches | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `clock_functions_match_native_when_the_session_zone_matches` | Pass |
| vs-properties | session-time-zone | A session zone that differs from the folding zone gets the folding zone's values | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_differing_session_zone_gets_the_folding_zone_values` | Pass |
| vs-properties | session-time-zone | An unreadable TZ fails only a query that needs it | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `an_unreadable_tz_fails_only_a_query_that_needs_it` | Pass |

## Notes

- The pruning virtual schema now sets `SESSION_TIME_ZONE = 'EUROPE/BERLIN'`, because the default local image runs with `DBTIMEZONE` UTC, where the old and the new fold agree.
- The architecture delta's BASE was rebased to the current `specs/architecture.md`; the commits after planning changed only lines outside `## Constraints`.
- Remote suites (Lakekeeper, Unity, Glue, Azure) were not run; the change targets none of their specific features. CI runs them on the PR.

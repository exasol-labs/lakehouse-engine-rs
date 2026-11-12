# Verification Report: add-date-add-functions

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The scan computes `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES`, and the adapter advertises them. Results match native Exasol on 367 local E2E tests, except for the differences that `docs/semantic-differences.md` lists. |
| Code review | 6 findings — 6 fixed (standard: 4, expert: 2); 2 orchestrator follow-ups also fixed |

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
| Unit | Not measured (`cargo llvm-cov` not run; CI measures it) |
| Integration | Not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`) | 2122 | 2120 | 2 (both existed before this plan) |
| Integration (`make e2e-up test-e2e`, local Exasol Docker) | 367 | 367 | 0 |

The project hook limits the suites to the local E2E suite. The plan touches no catalog, storage, or remote-only feature, so no remote suite ran.

### Manual Tests

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL SELECT ADD_HOURS(C_DATE, 1), ADD_DAYS(C_TS, C_DOUBLE) ...`: the pushdown contains `vs_add_hours(` and `vs_add_days(` | ✓ |
| `SELECT ID, C_DATE, ADD_HOURS(C_DATE, 1.5) ...`: each non-NULL row is the date at 02:00:00, NULL dates return NULL | ✓ |
| `SELECT ADD_DAYS(C_DATE, 3000000) ...`: fails with `data exception - datetime field overflow` | ✓ |
| `EXPLAIN VIRTUAL SELECT ADD_DAYS(DATE_TRUNC('month', C_DATE), 1) ...`: no `vs_add_`, `ADD_DAYS(` in the Exasol wrapper | ✓ |
| `EXPLAIN VIRTUAL SELECT ADD_MONTHS(C_DATE, 1) ...`: the pushdown projects only `C_DATE`, Exasol computes `ADD_MONTHS` | ✓ |
| `EXPLAIN VIRTUAL SELECT ID ... WHERE ADD_WEEKS(C_DATE, 1) > DATE '2024-01-10'`: the scan filter contains `vs_add_weeks(` | ✓ |

## Tool Evidence

### Linter

```
make lint (cargo clippy --workspace --all-targets --all-features -- -D warnings): exit 0, no warnings
```

### Formatter

```
make fmt-check (cargo fmt --all -- --check): exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| sql-functions | date-add-fns | The typed pass tells a DATE from a TIMESTAMP | `crates/vs-expression/src/scan_typing_tests.rs`, `crates/lakehouse-e2e/tests/e2e_local/date_add.rs` | `date_and_timestamp_classes_follow_their_source`, `date_add_functions_match_native` | Pass |
| sql-functions | date-add-fns | ADD_DAYS and ADD_WEEKS over a DATE return a DATE from the scan | `crates/vs-expression/src/lib_tests.rs`, `date_add.rs` | `date_addition_renders_scan_function_calls`, `date_add_functions_match_native` | Pass |
| sql-functions | date-add-fns | ADD_HOURS and ADD_MINUTES over a DATE return a timestamp from the scan | `crates/vs-expression/src/lib_tests.rs`, `date_add.rs` | `date_addition_renders_scan_function_calls`, `date_add_functions_match_native` | Pass |
| sql-functions | date-add-fns | The four functions over a TIMESTAMP keep its fractional seconds in the scan | `date_add.rs` | `date_add_functions_match_native` | Pass |
| sql-functions | date-add-fns | The scan rounds a fractional count as Exasol does | `crates/lakehouse-engine/src/scan/datetime_add_tests.rs`, `date_add.rs` | `counts_round_as_exasol_rounds`, `non_finite_counts_fail`, `date_add_functions_match_native` | Pass |
| sql-functions | date-add-fns | A result outside Exasol's datetime range fails the query | `datetime_add_tests.rs`, `date_add.rs` | `results_outside_the_range_fail`, `day_and_week_counts_beyond_the_limit_fail`, `hour_and_minute_counts_beyond_the_nanosecond_range_fail_as_numeric_out_of_range`, `date_add_functions_match_native` | Pass |
| sql-functions | date-add-fns | A comparison with a TIMESTAMP literal finer than a microsecond returns native rows | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs`, `date_add.rs` | `overprecise_timestamp_literal_against_a_date_addition_is_floored`, `date_add_functions_match_native` | Pass |
| sql-functions | date-add-fns | A date before 1582-10-15 follows the proleptic Gregorian calendar in the scan | `datetime_add_tests.rs`, `date_add.rs` | `dates_before_the_gregorian_switch_use_the_proleptic_calendar`, `date_add_before_the_gregorian_switch_follows_the_proleptic_calendar` | Pass |
| sql-functions | date-add-fns | A form the scan cannot match runs in Exasol | `scan_typing_tests.rs`, `date_add.rs`, `timestamp_precision.rs` | `unmatched_date_addition_forms_decline`, `date_add_functions_match_native`, `date_addition_over_a_nanosecond_timestamp_runs_in_exasol` | Pass |
| sql-functions | date-add-fns | The four functions render verbatim in the Exasol dialect | `crates/vs-expression/src/lib_tests.rs`, `date_add.rs` | `date_addition_renders_verbatim_in_the_exasol_dialect`, `date_add_functions_match_native` | Pass |
| sql-functions | date-diff-fns | Unsupported date functions fall through as unsupported nodes | `lib_tests.rs`, `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `unsupported_date_functions_decline_in_the_datafusion_dialect`, `reports_audited_capability_set` | Pass |
| pushdown-capabilities | capability-extensions | ISO week capability is advertised so WEEK expressions push down | `capabilities_tests.rs` | `reports_audited_capability_set`, `capabilities_doc_lists_exactly_the_advertised_set` | Pass |

## Notes

- The live run measured three behaviors the plan had not covered, and the implementation now matches them:
  - A NULL date with an over-limit count returns NULL natively and in the scan.
  - An `ADD_HOURS` or `ADD_MINUTES` count whose nanoseconds exceed 128 bits fails with `numeric value out of range` natively. The scan now fails the same way.
  - Native Exasol rejects a constant count beyond 36 digits when it compiles the query. The typed pass declines a literal count above 1e10, so Exasol computes that call.
- One difference is documented and not fixed. A declined call with an over-limit literal count returns 0 rows through the virtual schema when plan-time file pruning leaves no file, where native Exasol fails. The adapter's empty-result answer omits the select item. A fix belongs in the general empty-result planner, outside this plan.
- The pre-1582 examples in `docs/semantic-differences.md` were re-measured on the local Exasol with `TO_CHAR`, because exapump's default output displays dates before 1601 one or two days late.
- `speq plan validate` passes. It warns that four scenarios have more than three AND steps.
- The follow-up question about reproducing Exasol's dates before 1582-10-15 is still open (#TBD).

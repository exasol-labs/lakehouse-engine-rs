# Verification Report: add-join-pushdown-broadcast

**Generated:** 2026-07-07

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Broadcast inner equi-join pushdown (backlog BL-001 Phase 1) is implemented, code-reviewed, and fully green: 499 host tests (441 lakehouse-engine + 58 vs-expression) and 63 E2E tests against live Exasol Docker, including the previously-working shared-column join test and two new aggregate-over-join cases surfaced by a correctness fix mid-implementation. |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-musl-udf-build` / `cargo build --release` in `rust:1.94-bookworm`, exit 0) |
| Tests | ✓ (441 lakehouse-engine + 58 vs-expression host tests, 0 failed) |
| Lint | ✓ (`cargo clippy --all-targets`, no issues) |
| Format | ✓ (`cargo fmt --check`, clean) |
| Scenario Coverage | ✓ (all 15 planned scenarios + 6 added during the correctness fix) |
| Manual Tests | ✓ (both plan.md Manual Testing queries verified via e2e_join_test.rs) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit/Host (lakehouse-engine) | `cargo test -p lakehouse-engine` | 441 | 2 |
| Unit/Host (vs-expression) | `cargo test -p vs-expression` | 58 | 0 |
| E2E (live Exasol Docker) | `make test-e2e` | 63 | 0 |

E2E breakdown: `e2e_capability_test` 8, `e2e_count_distinct_test` 6, `e2e_join_test` 6, `e2e_scan_test` 43.

### Manual Tests

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL SELECT c.C_NAME, o.O_ORDERDATE FROM LH.CUSTOMER c JOIN LH.ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY WHERE o.O_ORDERDATE >= DATE '1995-01-01'` drives a single scan-UDF broadcast fan-out, not two independent per-table scans | ✓ (`e2e_broadcast_join_pushdown_shape`) |
| `SELECT COUNT(*), MIN(o.O_ORDERDATE) FROM LH.CUSTOMER c JOIN LH.ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY` returns a row count/min date equal to independently-computed ground truth | ✓ (`e2e_aggregate_over_join_result_correct`) — required the mid-implementation fix; the join planner initially ignored the aggregate and emitted an invalid raw-join projection |

## Tool Evidence

### Linter

```
cargo clippy -p lakehouse-engine --all-targets: No issues found
cargo clippy --all-targets (workspace, post code-review fixes): No issues found
```

### Formatter

```
cargo fmt -p lakehouse-engine -- --check: clean (exit 0)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning-join | Adapter advertises inner equi-join capabilities | `adapter/capabilities.rs` | `advertises_inner_equi_join_capabilities` | Pass |
| vs-adapter | pushdown-planning-join | Broadcast-eligible inner equi-join is planned as a broadcast fan-out | `tests/scan_plan_shape.rs` | `join_broadcast_fan_out_sql_shape` | Pass |
| vs-adapter | pushdown-planning-join | Small-side selection uses Iceberg metadata and the broadcast threshold | `adapter/pushdown.rs` (in-module) | `select_broadcast_sides` tests (7) | Pass |
| vs-adapter | pushdown-planning-join | Join above the broadcast threshold falls back to an unaccelerated two-scan join | `adapter/pushdown.rs` (in-module) | `join_above_threshold_unaccelerated_sql` | Pass |
| vs-adapter | pushdown-planning-join | Join projection and EMITS span both involved tables | `adapter/pushdown.rs` (in-module) | `join_projection_emits_attribute_each_side_owning_type` | Pass |
| vs-adapter | pushdown-planning-join | Join condition is rendered via the vs-expression translator | `adapter/pushdown.rs` (in-module) | `join_condition_renders_via_translator` | Pass |
| vs-adapter | pushdown-planning-join | A join outside the broadcast contract is declined safely | `adapter/pushdown.rs` (in-module) | `join_outside_contract_declined_safely` | Pass |
| datafusion-scan | scan-execution-join | Scan reconstitutes a join scan spec carrying two file lists | `tests/scan_join_test.rs` | `join_spec_reconstitutes_two_file_lists` | Pass |
| datafusion-scan | scan-execution-join | Scan registers both tables and executes the inner equi-join | `tests/scan_join_test.rs` | `join_executes_inner_equi` | Pass |
| datafusion-scan | scan-execution-join | Join projection, filter, and LIMIT are applied and rows streamed as Arrow IPC | `tests/scan_join_test.rs` | `join_projection_filter_limit_streamed` | Pass |
| datafusion-scan | scan-execution-join | The bounded dimension side is the hash-join build side | `tests/scan_join_test.rs` | `join_build_side_is_dimension` | Pass |
| datafusion-scan | scan-execution-join | Scan reports a clear error when an assigned join file is unreadable | `tests/scan_join_test.rs` | `join_unreadable_file_errors_without_secrets` | Pass |
| vs-adapter | pushdown-planning (CHANGED) | Adapter advertises join pushdown for supported shapes | `adapter/capabilities.rs` | `reports_capabilities_includes_inner_join` | Pass |
| vs-adapter | pushdown-planning-capability-extensions (CHANGED) | Inner equi-join capabilities advertised | `adapter/capabilities.rs` | `advertises_inner_equi_join_capabilities` | Pass |
| vs-adapter | pushdown-planning-capability-extensions (CHANGED) | `reports_audited_capability_set` includes join capabilities without disallowed shapes | `adapter/capabilities.rs` | `reports_audited_capability_set` | Pass |
| vs-adapter | pushdown-planning-join (fix, ADR [6]/[7]) | Shared-column-name join uses qualified two-scan, not bare-name broadcast rendering | `adapter/pushdown.rs` (in-module) + `e2e_scan_test.rs` | `join_above_threshold_unaccelerated_sql` variant + `e2e_pushdown_resolves_files_once_multi_table` | Pass |
| vs-adapter | pushdown-planning-join (fix, ADR [7]) | Aggregate over a join routes through the qualified two-scan wrapper | `adapter/pushdown.rs` (in-module) + `tests/e2e_join_test.rs` | in-module aggregate-routing test + `e2e_aggregate_over_join_uses_two_scan_wrapper` / `e2e_aggregate_over_join_result_correct` | Pass |
| vs-expression | (extended) | Column node renders table-qualified when `tableAlias` is present, bare when absent | `vs-expression/src/lib.rs` (in-module) | qualified-column tests (2) | Pass |
| vs-adapter | pushdown-planning-join | Live capability round-trip advertises JOIN/JOIN_TYPE_INNER/JOIN_CONDITION_EQUI | `tests/e2e_capability_test.rs` | `e2e_advertises_inner_equi_join_capability` | Pass |
| vs-adapter | pushdown-planning-join | Broadcast join result matches independently-computed join | `tests/e2e_join_test.rs` | `e2e_broadcast_join_result_correct` | Pass |
| vs-adapter | pushdown-planning-join | Unaccelerated fallback result is identical to the broadcast result | `tests/e2e_join_test.rs` | `e2e_above_threshold_result_matches_broadcast` | Pass |

## Notes

**Mid-implementation correctness fix (ADR [6]/[7] in decision-log.md).** Live E2E testing against a real Exasol Docker container surfaced two regressions in the initial Groups B-D implementation, both stemming from the same root cause: the unaccelerated two-scan fallback reused the broadcast path's bare-name rendering (gated by a disjoint-column-name guard), and returned a hard error instead of falling back when that guard failed. Exasol does **not** retry natively on that error — it's a hard SQL failure to the caller. This broke (1) a pre-existing, previously-working test joining two tables that both have an `id` column, and (2) any aggregate wrapped around a join (the plan's own second Manual Testing example query). Fixed by giving the two-scan fallback its own table-qualified rendering (`vs-expression` gained an optional column `tableAlias`), independent of the broadcast guard, and routing any join carrying an aggregate/GROUP BY/HAVING/ORDER BY/LIMIT through that qualified two-scan path unconditionally (matching pre-JOIN-capability behavior exactly). A hard `Err` is now truly last-resort. Both regressions are covered by new tests (host + E2E) and verified fixed.

**Code review**: one pass, 3 minor findings (no blocking issues), 2 applied (drove the two-scan alias SQL from the existing named constants instead of re-hardcoding the literals; downgraded an over-exposed `pub` helper to `pub(crate)`), 1 waived (a documented one-line pass-through wrapper, explicitly called lowest-priority by the reviewer).

**Scope discipline**: outer joins, non-equi conditions, >2-table joins in one pushdown, and any large/large shuffle strategy (Phase 2, BL-001) remain out of scope and are declined safely by `detect_join` before reaching the broadcast/two-scan decision — verified by `join_outside_contract_declined_safely` and dedicated unit tests for each declined shape.

**Origin**: motivated by live `EXA_USER_PROFILE_LAST_DAY` telemetry captured against Exasol cluster `test1` on 2026-07-06 (see `docs/performance.md` "Bottleneck analysis"), which found unpushed `JOIN` operators costing 1.5-3.5s CPU apiece over up to 180M rows on the four worst Trino losses in the TPC-H suite (Q2, Q3, Q5, NQ3). Query-level before/after timing against a live cluster was not re-measured in this pass (the `test1` cluster was stopped after the telemetry capture to avoid AWS costs, per the session's cost constraint) — that comparison is a natural follow-up once this PR is merged and a cluster is brought back up for benchmarking.

# Verification Report: add-multi-way-join-broadcast

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | A join of three or more tables broadcasts one eligible pair and scans the other legs, and a declined conjunct runs in Exasol's `WHERE` while the broadcast stays; every suite is green and every `dispatch_golden` fixture is byte-identical. |
| Code review | 9 findings — 9 fixed |

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
| Unit | 91.9 (workspace lines, `make coverage`); 97.4 for `adapter/pushdown/joins/` |
| Integration | not measured (E2E runs the deployed `.so`) |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`) | 2259 | 2257 | 2 (pre-existing micro-benchmarks `bench_emit_only`, `bench_scan_only`) |
| Integration (`make test-e2e`, local Exasol 2025.1.16) | 449 | 449 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| pushdown-planning-join: `EXPLAIN VIRTUAL` of LINEITEM ⋈ ORDERS ⋈ DIM_SUPPLIER with `s.S_NAME = 'supplier-02'`, `GROUP BY` on `MY_LAKEHOUSE_JOIN` | ✓ Two fan-outs: one carries `"join":{` with `dim_supplier` broadcast into LINEITEM's shards, ORDERS is plain; outer `GROUP BY` |
| pushdown-planning-join-fallback: the same query on `MY_LAKEHOUSE_JOIN_LOW` | ✓ Three plain fan-outs `LHS_T0`, `LHS_T1`, `LHS_T2`, no `"join":{` |
| pushdown-planning-join-ordering: the first query with `ORDER BY 2 DESC LIMIT 1` | ✓ No `post_join_*`, `"limit"`, or `"order_by"` in any spec; outer `ORDER BY … LIMIT 1`; rows (`supplier-02,33`) equal the native copy |
| pushdown-declined-filter-self-apply: two-leg join with `o.O_ORDERDATE LIKE '2024-01-0%'` | ✓ Selecting `c.C_CUSTKEY`: one fan-out with `"join":{` and `WHERE (("LHS_T0_O_ORDERDATE" LIKE '2024-01-0%'))`. Selecting `c.C_NAME`, as the plan's command does: two plain fan-outs, because the width rule refuses CUSTOMER's unbounded `C_NAME` once a conjunct runs in Exasol |
| pushdown-planning-scan-text-conversion: the same join without `EXPLAIN`, default and `NLS_DATE_FORMAT = 'DD.MM.YYYY'` | ✓ `9,45` and `0,` on the virtual schema and the native copy alike |
| Benchmark (#673 criterion) | ✓ Q2 and NQ3 broadcast a pair (ORDERS into LINEITEM, PART into PARTSUPP); Q1 keeps the N-scan. See Notes for times |

## Tool Evidence

### Linter

```
$ make fmt-check lint
cargo clippy --workspace --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)
exit 0
```

### Formatter

```
$ make fmt-check
exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| pushdown-joins | pushdown-planning-join | Broadcast join projection and filter are rendered per involved table | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_declined_conjunct_runs_in_exasol_where` | Pass |
| pushdown-joins | pushdown-planning-join | A two-leg join with filters in its condition is planned as a broadcast fan-out | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_join_with_on_filters_takes_the_broadcast_path_and_matches_native` | Pass |
| pushdown-joins | pushdown-planning-join | A join of three or more legs broadcasts one pair and scans the other legs | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_multi_way_join_broadcasts_one_pair_and_matches_native` | Pass |
| pushdown-joins | pushdown-planning-join | The broadcast pair is the eligible pair whose dimension has a filter of its own | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_multi_way_join_broadcasts_the_filtered_dimension_pair` | Pass |
| pushdown-joins | pushdown-planning-join | The broadcast pair is the eligible pair whose dimension has a filter of its own (byte and FROM-order tie-breaks) | `crates/lakehouse-engine/src/adapter/pushdown/joins/broadcast_policy_tests.rs` | `ranks_candidate_pairs_by_filter_then_fact_bytes_then_dimension_bytes_then_order` | Pass |
| pushdown-joins | pushdown-planning-join | A pair that is not eligible is not broadcast | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_multi_way_join_without_eligible_pair_keeps_the_n_scan` | Pass |
| pushdown-joins | pushdown-planning-join-fallback | A join outside the broadcast contract is declined safely | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_above_threshold_unaccelerated_fallback_shape` | Pass |
| pushdown-joins | pushdown-planning-join-fallback | A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_three_table_join_result_correct` | Pass |
| pushdown-joins | pushdown-planning-join-ordering | A fallback leg's fan-out scan spec never carries a row limit | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_multi_way_join_with_limit_cuts_no_shard` | Pass |
| pushdown | pushdown-declined-filter-self-apply | A broadcast join applies a declined filter conjunct in Exasol's WHERE | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_declined_conjunct_runs_in_exasol_where` | Pass |
| pushdown | pushdown-declined-filter-self-apply | A single-table WHERE filter splits at AND between the scan and the wrapper | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_declined_conjunct_runs_in_exasol_where` | Pass |
| pushdown | pushdown-planning-selectlist-expressions | A wrapped derived projection routes to a native wrapper on every path | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_with_a_declined_select_item_keeps_the_broadcast` | Pass |
| pushdown-types | pushdown-planning-scan-text-conversion | A join filter that converts a date to text runs in Exasol, and such a join key keeps the N-scan | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_join_date_to_text_filter_runs_in_exasol_and_key_keeps_the_n_scan` | Pass |

Unit tests beside them: `broadcast_policy_tests.rs` (budget, width rule with three legs and with a declined conjunct, a declined literal bounds nothing, pair roles, ranking), `plan_tests.rs` (chain positions, two-leg window gate), `rendering_tests.rs` (`placement_puts_each_conjunct_in_exactly_one_fan_out_or_the_residual`), `sql_builders_tests.rs` (three-leg hybrid shape, declined conjunct in Exasol's `WHERE`, wrapped select list keeps the broadcast, next pair on a render decline). All 24 `dispatch_golden` fixtures are unchanged.

## Notes

- Benchmark. `make bench` (docker, SF 0.3) fails two single-table checks (`not pushed: shard_key`) on `main` and on this branch alike, so they predate this change. Its single-run join times are noise-dominated at this scale. A warm A/B on the branch build, seven runs each, against a virtual schema with `JOIN_BROADCAST_MAX_BYTES = '1'` (the N-scan `main` takes for these joins), returned identical rows and these medians: Q2 0.44s broadcast vs 0.65s N-scan, NQ3 0.29s vs 0.38s, Q1 0.25s vs 0.26s (Q1 selects unbounded text of its small tables, so no pair is eligible).
- The manual `pushdown-declined-filter-self-apply` command in `plan.md` selects `c.C_NAME`. Its expected output (a broadcast fan-out) predates the width rule: once a conjunct runs in Exasol, an unbounded dimension text column keeps the N-scan, as the `pushdown-planning-join` Background states. The same query selecting `c.C_CUSTKEY` shows the broadcast with the outer `WHERE`.
- The grouping returns only the eligible pairs in rank order; the per-pair `NScanReason` named in the first draft of the Design was removed in review because nothing observable read it, and `plan.md` was updated.
- `render_broadcast_join` narrowed from `pub(crate)` to the pushdown module, since it now takes the pushdown-internal `LegGroup`; the visibility probe lists 26 items.
- Existing E2E tests whose assertions this change made false were updated: the two declined-filter N-scan tests were replaced, three- and four-table N-scan proofs moved to `VS_NAME_LOW`, and two capability tests now assert the broadcast under an Exasol `SELECT` for a select item only Exasol computes.
- Most of the refactor was written beside its tests rather than strictly test-first; the review's width-rule fix was test-first (RED shown, then GREEN).
- Remote suites were not run: the change targets no catalog, storage, or cloud behavior.

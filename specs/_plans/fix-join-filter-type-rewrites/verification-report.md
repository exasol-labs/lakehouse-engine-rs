# Verification Report: fix-join-filter-type-rewrites

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Both join WHERE-filter sites now run the full type-rewrite pipeline; #215 closed, #223 slice 2 closed, #228 narrowed. All unit and live-Exasol E2E scenarios pass; no golden-SQL fixture changed for untriggered filters. |
| Code review | 8 findings — 8 fixed (6 standard, 2 expert) |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (host unit, `cargo test --workspace`) | ✓ |
| Tests (E2E, live Docker Exasol, `make test-e2e`) | ✓ |
| Lint (`cargo clippy --all-targets`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Spec validation (`speq feature validate`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`) | 40 binaries | 687 (joins module) / all workspace binaries green | 2 (pre-existing, unrelated) |
| Integration (`make test-e2e`, live Docker Exasol) | 8 binaries | 206 | 0 |

Joins-module unit test count grew from a pre-plan baseline to 687 passed in `cargo test -p lakehouse-engine --lib` (the specific module `adapter::pushdown::joins` carries the join-filter-type-coercion tests; the 687 figure is the full library test count, all green). `e2e_join_test` binary: 23 passed under the project's required `--test-threads=1` (Makefile pins this — the star-schema fixture tables are shared across tests in that binary, so parallel runs of that one binary reproduce a pre-existing, unrelated isolation race unless serialized).

### Manual Tests

| Test | Result |
|------|--------|
| `e2e_broadcast_like_on_decimal_column_falls_back_and_filters` — broadcast declines, N-scan wrapper, no `"join":{` | ✓ |
| `e2e_broadcast_like_on_date_column_stays_broadcast_and_filters` — broadcast retained, `CAST(` over `O_ORDERDATE` | ✓ |
| `e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where` — LIKE in outer WHERE, table-qualified, absent from leg | ✓ |
| `e2e_join_instr_with_start_position_returns_native_result` — native Exasol INSTR result, EXPLAIN VIRTUAL confirms verbatim 3-arg form and no `strpos(` | ✓ |
| `join_decimal_stringification_renders_trimmed_at_both_join_sites` — trimmed `decimal_to_varchar_exasol` form at both join sites | ✓ |
| `e2e_join_decimal_stringification_matches_native_at_both_surfaces` — independent Rust-computed oracle, `LENGTH(O_TOTALPRICE) > 3` matches at `VS_NAME` and `VS_NAME_LOW` | ✓ |
| Regression: `cargo test -p lakehouse-engine golden_` — all golden-SQL fixtures unchanged | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --all-targets  →  exit 0, 0 warnings
```

### Formatter

```
cargo fmt --check  →  exit 0, no diff
```

### Spec validation

```
speq feature validate  →  exit 0 (pre-existing style warnings on unrelated features, 0 errors)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | Broadcast-join filter over a non-string LIKE subject declines the broadcast plan | `joins/sql_builders.rs` | `broadcast_declines_like_over_decimal_side_column` | Pass |
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | Broadcast-join filter over a non-string LIKE subject declines the broadcast plan | `tests/e2e_join_test.rs` | `e2e_broadcast_like_on_decimal_column_falls_back_and_filters` | Pass |
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | Broadcast-join filter over a DATE LIKE subject keeps the broadcast plan | `joins/sql_builders.rs` | `broadcast_keeps_plan_and_casts_like_over_date_side_column` | Pass |
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | Broadcast-join filter over a DATE LIKE subject keeps the broadcast plan | `tests/e2e_join_test.rs` | `e2e_broadcast_like_on_date_column_stays_broadcast_and_filters` | Pass |
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | N-scan side-local conjunct the type pipeline declines becomes a residual conjunct | `joins/sql_builders.rs` | `n_scan_type_declined_side_local_conjunct_moves_to_outer_where` | Pass |
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | N-scan side-local conjunct the type pipeline declines becomes a residual conjunct | `tests/e2e_join_test.rs` | `e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where` | Pass |
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | N-scan side-local conjunct the type pipeline declines becomes a residual conjunct | `joins/rendering.rs` | `type_screened_leg_filter_declines_type_accepted_but_unrenderable_rewrite` | Pass |
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | N-scan side-local conjunct the type pipeline rewrites reaches its leg rewritten | `joins/sql_builders.rs` | `n_scan_date_like_side_local_conjunct_reaches_leg_as_cast` | Pass |
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | Two N-scan sides sharing a column name are each screened against their own side's types | `joins/rendering.rs` | `type_screened_leg_filter_uses_owning_side_types_for_shared_column_name` | Pass |
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | Join filter with no type-rewrite trigger emits byte-identical SQL | `joins/sql_builders.rs` | `golden_broadcast_join_sql_unchanged`, `golden_n_scan_join_sql_unchanged` | Pass |
| join-filter-type-coercion | pushdown-planning-join-filter-type-coercion | Join filter with no type-rewrite trigger emits byte-identical SQL | `joins/sql_builders.rs` | `broadcast_absent_and_trivially_true_filter_stay_eligible` | Pass |
| pushdown-planning-join | pushdown-planning-join | Broadcast join projection and filter are rendered per involved table | `joins/sql_builders.rs` | `broadcast_declines_like_over_decimal_side_column`, `broadcast_keeps_plan_and_casts_like_over_date_side_column` | Pass |
| pushdown-planning-join-fallback | pushdown-planning-join-fallback | Join conditions attach greedily by table-name set and side-local filters push into each leg | `joins/sql_builders.rs` | `n_scan_leg_residual_partition_is_total_and_disjoint_with_type_screen` | Pass |
| pushdown-declined-filter-self-apply | pushdown-declined-filter-self-apply | Broadcast-eligible join whose filter declines takes the N-scan fallback | `joins/sql_builders.rs` | `broadcast_declines_like_over_decimal_side_column`, `broadcast_keeps_plan_and_casts_like_over_date_side_column` | Pass |
| pushdown-declined-filter-self-apply | pushdown-declined-filter-self-apply | Broadcast-eligible join whose filter declines takes the N-scan fallback | `tests/e2e_join_test.rs` | `e2e_broadcast_like_on_decimal_column_falls_back_and_filters` | Pass |
| pushdown-declined-filter-self-apply | pushdown-declined-filter-self-apply | N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct | `joins/rendering.rs` | `type_screened_leg_filter_partition_is_total_and_fails_closed` | Pass |
| pushdown-declined-filter-self-apply | pushdown-declined-filter-self-apply | N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct | `tests/e2e_join_test.rs` | `e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where` | Pass |
| pushdown-planning-string-fn-type-coercion | pushdown-planning-string-fn-type-coercion | INSTR and LOCATE coerce their first two arguments and decline beyond two | `joins/sql_builders.rs` | `join_instr_beyond_two_args_declines_at_both_join_sites` | Pass |
| pushdown-planning-string-fn-type-coercion | pushdown-planning-string-fn-type-coercion | INSTR and LOCATE coerce their first two arguments and decline beyond two | `tests/e2e_join_test.rs` | `e2e_join_instr_with_start_position_returns_native_result` | Pass |
| pushdown-planning-decimal-string-format | pushdown-planning-decimal-string-format | WHERE-clause stringification of a DECIMAL column renders the trimmed form | `joins/sql_builders.rs` | `join_decimal_stringification_renders_trimmed_at_both_join_sites` | Pass |
| pushdown-planning-decimal-string-format | pushdown-planning-decimal-string-format | WHERE-clause stringification of a DECIMAL column renders the trimmed form | `tests/e2e_join_test.rs` | `e2e_join_decimal_stringification_matches_native_at_both_surfaces` | Pass |
| pushdown-planning-like-type-coercion | pushdown-planning-like-type-coercion | LIKE on a VARCHAR or CHAR column pushes down unchanged | `joins/sql_builders.rs` | `join_like_over_varchar_side_column_pushes_down_unchanged` | Pass |

All test names were confirmed to exist (`grep -rl "fn <name>"`) and pass in the final full run (`cargo test --workspace` + `make test-e2e`), after the code-review fix pass reconciled the plan's Scenario Coverage table with the tests actually shipped (two names in the original plan draft — `broadcast_filter_runs_type_rewrites_over_union_of_side_columns` and `broadcast_type_decline_and_dialect_decline_share_one_route` — were replaced with the shipped `broadcast_declines_like_over_decimal_side_column` / `broadcast_keeps_plan_and_casts_like_over_date_side_column` pair, which cover the same claims).

## Notes

- **Code review**: 8 findings, all fixed. 2 expert-tier: (1) extracted `type_accepted_rewrite` in `support.rs` as the sole owner of the type-acceptance predicate, previously duplicated between `classify_where_filter` and `type_screened_leg_filter`; (2) replaced a VS-to-VS test oracle with an independent Rust-computed one for the DECIMAL-stringification E2E test, closing a gap where a wrong-but-nonzero trim would have passed. 6 standard-tier: 3 missing unit tests added (VARCHAR-LIKE no-op regression, DECIMAL-stringification at both sites, INSTR >2-arg at both sites), an outdated 2-consumer doc comment updated to the current 3-consumer contract, a test whose only assertion was observationally identical to "predicate silently dropped" hardened with `EXPLAIN VIRTUAL`-based assertions, and 3 doc-comment fixes removing ephemeral plan-task-number references (`task 3.N`) that would dangle once `/speq:record` archives this plan, restoring one accidentally-deleted doc comment.
- **Scope discipline held**: no change to the join SELECT-list projection path, no new guard/decline outcome, no work on #223 slices 1/3, no fix to #228's root cause (`vs-expression`'s INSTR/LOCATE arity rendering) — confirmed by the reviewer's independent trace of `plan_join`/`resolve_file_list` still consuming the RAW filter for Iceberg manifest pruning, unaffected by this plan.
- **Pre-existing, unrelated issue observed and NOT fixed** (out of scope): running the `e2e_join_test` binary without `--test-threads=1` reproduces a cross-test isolation race on a shared `GROUND_TRUTH_LINEITEM` table (two unrelated pre-existing tests both `CREATE OR REPLACE` it). The project's `Makefile` already pins `--test-threads=1` for this reason; noted here only so an ad-hoc parallel run isn't mistaken for a regression from this plan.
- Issue bookkeeping for the PR body (per plan.md's table): `Closes #215`; comment on #223 narrowing it to slices 1 and 3 (slice 2 fixed here); comment on #228 noting exposure narrowed at the two join WHERE surfaces (root cause untouched, do not close); #279/#285 are prior merged context, not re-closed.

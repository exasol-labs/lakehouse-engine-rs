# Verification Report: fix-single-group-scalar-over-aggregate

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The plan is fully implemented, reviewed, and verified live. #194's silent wrong answer and #188's hard error are both fixed and confirmed against a live Exasol Docker container. |
| Code review | 16 findings (12 standard + 4 expert; the reviewer's own one-line verdict undercounted standard findings as 9) — 16 fixed, 0 outstanding |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-musl-udf-build`, exit 0) |
| Tests | ✓ (`cargo test --workspace`, 1593 passed, 0 failed) |
| Lint | ✓ (`cargo clippy --all-targets`, 0 warnings) |
| Format | ✓ (`cargo fmt --all -- --check`, clean) |
| Scenario Coverage | ✓ (all 12 unit-level scenarios + all integration scenarios covered) |
| Manual Tests | ✓ (all 5 rows confirmed live against Docker Exasol) |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | Not measured (`cargo llvm-cov` is not part of this plan's checklist) |
| Integration | Not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (host, `cargo test --workspace`) | 1593 | 1593 | 0 |
| E2E (`make test-e2e`, live Docker Exasol) | 296 | 296 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `SELECT ROUND(SUM(L_QUANTITY), 2) FROM <VS>.FACT_LINEITEM` (#194 shape) — one merged row matching the native oracle | ✓ (VS: 1 row, `110`; oracle: `110`) |
| `SELECT ROUND(VARIANCE(L_EXTENDEDPRICE), 4) FROM <VS>.FACT_LINEITEM` (#188 shape) — no `Invalid function 'variance'`, value matches oracle | ✓ (VS: `3500.0`; oracle: `3500.0`) |
| `EXPLAIN VIRTUAL` on the #194 query shows non-empty `"aggregates"`, `"projection":[]`, no `"expr"` containing an aggregate | ✓ |
| Floor: `ROUND(COUNT(DISTINCT REGION), 2)` routes to the qualified wrapper and matches the oracle | ✓ (VS: `4`; oracle: `4`; pushed SQL shows the wrapper, no `PARTIAL_*` column) |
| No regression: bare `VARIANCE` and `ROUND(VAR_SAMP(...), 4)` still match the oracle | ✓ (both `3500.0`, unchanged) |

Fixture note: live verification ran against this repo's Iceberg `FACT_LINEITEM`/`FACT_ORDERS` Docker fixture rather than plan.md's original TPC-H `LINEITEM`/`CUSTOMER` seed values — the query shapes, oracle-comparison method, and pass/fail verdicts are identical; only the numeric fixture values differ.

## Tool Evidence

### Linter

```
cargo clippy --all-targets --workspace
Exit 0, 0 warnings.
```

### Formatter

```
cargo fmt --all -- --check
Exit 0, no diff.
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | Scalar-over-aggregate decomposes into partial columns + one merged row | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `single_group_scalar_over_aggregate_matches_golden` | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | Scalar-over-aggregate decomposes into partial columns + one merged row | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_aggregate_round_sum_matches_native_oracle` | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | Scalar-wrapped statistical aggregate resolves via shared AggKind tables, no aggregate name reaches DataFusion | `dispatch_golden_tests.rs` | `single_group_scalar_over_variance_matches_golden` | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | Scalar-wrapped statistical aggregate resolves via shared AggKind tables | `tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_variance_matches_native_oracle` | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | Shared inner aggregates across the select list collapse into one deduplicated partial column | `src/adapter/pushdown/single_group_agg_tests.rs` | `single_group_scalar_over_aggregate_dedups_shared_inner_aggregates` | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | Shared inner aggregates dedup, live | `tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_aggregate_shared_count_matches_native_oracle` | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | Scalar-over-aggregate and bare-aggregate items interleave in select-list order, each with its own declared type | `single_group_agg_tests.rs` | `single_group_scalar_over_aggregate_preserves_selectlist_order_and_item_types` | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | Interleaved order, live | `tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_aggregate_interleaved_matches_native_oracle` | Pass |
| vs-adapter | pushdown-planning-selectlist-expressions | A nested aggregate the merge cannot decompose widens the projection instead of being evaluated per shard | `src/adapter/pushdown/support_tests.rs` | `project_columns_widens_on_aggregate_nested_in_{scalar_item,function_scalar_cast_item,function_scalar_case_item,arithmetic_node,predicate_node}` | Pass |
| vs-adapter | pushdown-planning-selectlist-expressions | Nested aggregate widening, live (COUNT DISTINCT floor) | `tests/e2e_count_distinct_test.rs` | `e2e_scalar_wrapped_count_distinct_routes_to_wrapper_and_matches_native_oracle` | Pass |
| vs-adapter | pushdown-planning-selectlist-expressions | Nested aggregate widening on the broadcast-join path, live | `tests/e2e_join_test.rs` | `e2e_scalar_over_aggregate_ungrouped_join_matches_native_oracle` | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | A fully-pruned file list yields one shape-correct, correctly-typed empty row | `src/adapter/pushdown/empty_result_tests.rs` | `empty_single_group_scalar_over_aggregate_emits_one_typed_row` | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | Empty-result shape, live | `tests/e2e_scan_test.rs` | all-files-pruned E2E test | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | The scalar-over-aggregate decomposition mechanism has ONE owner shared by both aggregate planners | `src/adapter/pushdown/scalar_over_agg_tests.rs` | `scalar_over_agg_primitives_serve_both_planners_with_no_planner_dependency` | Pass |
| vs-adapter | pushdown-planning-single-group-agg | Grouped scalar-over-aggregate stays on its existing golden path, unedited | `dispatch_golden_tests.rs` | `grouped_aggregate_matches_golden` | Pass |
| vs-adapter | pushdown-planning-single-group-agg-scalar-over-aggregate | `detect_aggregates` accepts a decomposable scalar-over-aggregate and still declines an undecomposable one | `single_group_agg_tests.rs` | `detect_aggregates_accepts_scalar_over_aggregate_and_still_declines_undecomposable` | Pass |
| vs-adapter | pushdown-planning-single-group-agg | Single-group aggregate scan spec leaves the projection field empty | `tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_aggregate_explain_virtual_shows_empty_projection` | Pass |
| vs-adapter | pushdown-planning-selectlist-expressions | Top-level-aggregate widening is unchanged by the new subtree probe | `support_tests.rs` | `project_columns_top_level_aggregate_widening_is_unchanged_by_the_subtree_probe` | Pass |
| vs-adapter | pushdown-planning-selectlist-expressions | A widened derived projection routes to the qualified native wrapper on every consuming path | `dispatch_golden_tests.rs` | `nested_aggregate_decline_matches_qualified_wrapper_golden` | Pass |

## Notes

- **Live E2E caught two real defects that unit tests alone missed**, confirming the value of the mandatory Docker verification pass:
  1. `empty_agg_sql`'s scalar-over-aggregate arm substituted a bare, untyped `NULL` into the scalar expression; Exasol rejects an untyped `NULL` as a scalar-function argument (`sqlCode 0A000: Feature not supported: Round with wrong type`), confirmed live in isolation (`SELECT ROUND(NULL, 2) FROM DUAL` fails; a typed `CAST(NULL AS ...)` succeeds). Fixed by threading `col_types` into `empty_agg_sql` so the substituted null carries its own argument column's Exasol type.
  2. Code review's expert pass found a test (`aggregate_query_builds_partial_agg_spec`) silently asserting against a malformed `SELECT  FROM (...)` merge SELECT; fixed by making an empty merge list unrepresentable via the new `AggregateMergeInputs` type.
- **Design deviation, tracked, not a defect:** the review-fix pass added `pub struct AggregateMergeInputs` to the pushdown façade (an external test crate needed to name it), contradicting plan.md's original Call-Site Census claim that the façade would gain no item. `plan.md` was corrected in place, and a new spec delta (`vs-adapter/pushdown-module-structure/spec.md`) documents the addition; both frozen surface-probe files and their doc-comment counts were updated to match.
- **No version bump** in this run, per the caller's explicit instruction — the workspace stays at `0.40.1`.
- E2E ran once, after all implementation and review fixes landed, against a Docker Exasol stack brought up once and reused for the entire implementation (per the caller's instruction to avoid repeated/spurious e2e cycles); this is the single, fully-justified run gating this report.
- One pre-existing, out-of-plan-scope design note surfaced during Group F's coverage audit and was left untouched: `scalar_over_agg.rs` briefly imported `parse_agg_item` from `single_group_agg.rs` before the review's expert pass (9.2) relocated it — by the time of this report, that relocation is done and the shared module names neither planner.

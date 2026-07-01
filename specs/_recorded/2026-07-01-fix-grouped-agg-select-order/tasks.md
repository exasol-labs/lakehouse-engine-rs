# Tasks: fix-grouped-agg-select-order

## Phase 2: Implementation (Group A — core refactor, single cohesive worker)
- [x] 1.1 Change `detect_group_by_aggregates` to return ordered classification (original index + group-key/aggregate slot) [expert]
- [x] 1.2 Rewrite `build_grouped_aggregate_scan_sql` outer-SELECT assembly to place fragments at original select-list ordinal (leave inner fan-out EMITS untouched) [expert]
- [x] 1.3 Replace `group_key_exasol_types` string-`position` lookup and detection expression-key `group_keys.contains(&sql)` with index-based matching [expert]
- [x] 1.4 Update `handle_pushdown`'s call site to pass ordering/classification data through [expert]

## Phase 2: Implementation (Group B — unit tests)
- [x] 2.1 Extend `make_group_by_request` helper to accept `selectListDataTypes`
- [x] 2.2 Add `detect_group_by_aggregates` tests for all four orderings (index preservation)
- [x] 2.3 Add `build_grouped_aggregate_scan_sql` tests asserting outer SELECT order + per-item CAST type
- [x] 2.4 Add regression test: expression group key whitespace/casing drift resolves type by index (no VARCHAR fallback)

## Phase 2: Implementation (Group C — E2E tests)
- [x] 3.1 E2E: aggregate before single group key (#33 repro) `test_group_by_agg_before_key`
- [x] 3.2 E2E: interleaved multi-key GROUP BY `test_group_by_interleaved_multi_key`
- [x] 3.3 E2E: expression group key after aggregate `test_group_by_expr_key_after_agg`
- [x] 3.4 E2E: aggregate-first + HAVING `test_group_by_agg_first_with_having`

## Phase 4: Code Review
- [x] 4.1 Review all changed files (0 actionable findings)

## Phase 4b: Fix (discovered during E2E — HAVING over aggregates)
- [x] 4.2 Render HAVING against the merge decomposition: map each `function_aggregate` in the HAVING predicate to its `SUM("PARTIAL_*")` merge expression (renderer has no aggregate case, so raw HAVING was silently dropped → wrong results). Fix misleading "Exasol post-processes" comment. Add unit test asserting merge-expr HAVING. [expert]

## Phase 5: Verification
- [x] 5.1 Build (`make cross-musl-udf-build`) — ran as part of make test-e2e, .so built (0.17.1)
- [x] 5.2 Test (`cargo test`) — 322 lib + others pass
- [x] 5.3 E2E (`make test-e2e`) — GREEN after 4.2: 32/32 scan + 7/7 capability, MAKE_EXIT=0
- [x] 5.4 Lint (`cargo clippy --all-targets`) — clean
- [x] 5.5 Format (`cargo fmt`) — clean

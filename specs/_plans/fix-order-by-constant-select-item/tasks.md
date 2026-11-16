# Tasks: fix-order-by-constant-select-item

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [x] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: Constant sort keys)
- [x] 1.1 Add E2E tests (order_by_constant.rs, join.rs) and untag unit test scenario line
- [x] 1.2 Reproduce on main against Docker Exasol; record results here
  - Repro on main's `.so` (exapump, EVENTS ids 1-20). Pushed ORDER BY / result:
    - `SELECT id, 1 ... ORDER BY 2 DESC, 1 LIMIT 3` and the `5` variant, and the `WHERE id <= 5` no-LIMIT one: `ORDER BY "_LH_PROJ_1" DESC NULLS FIRST, "ID" ASC NULLS LAST`, ids 1,2,3 (1..5): pass on main.
    - `SELECT id FROM t ORDER BY 1+0 DESC, id LIMIT 3`: pushes `ORDER BY 1 DESC NULLS FIRST, "ID" ASC NULLS LAST LIMIT 3`, returns 20,19,18 (wrong).
    - Grouped `SELECT id, 1, COUNT(*) ... GROUP BY id ORDER BY 2 DESC, 1 LIMIT 3`: pushes `ORDER BY 1 DESC NULLS FIRST, "GK_0" ...`, returns 20,19,18 (wrong). `5` variant: fails `Too large value 5 as select list column reference in ORDER BY (largest possible value is 3)`, SQL state 04000.
    - Join `SELECT e.id, 1 ... ORDER BY 2 DESC, 1 LIMIT 3`: N-scan wrapper (no broadcast), pushes `ORDER BY 1 DESC NULLS FIRST, "LHS_T0"."ID" ...`, returns 20,19,18 (wrong). `5` variant fails `Too large value 5 ... (largest possible value is 2)`, 04000.
    - `SELECT id, 1 ... ORDER BY 2 LIMIT 3 OFFSET 18`: pushes `ORDER BY "_LH_PROJ_1" ASC NULLS LAST LIMIT 3 OFFSET 18`, returns 2 rows (19, 20): passes on main.
  - New tests on main: 1 passed (`constant_only_order_by_keeps_its_offset_window`), 5 failed: `constant_sort_key_returns_the_native_order` (folded `1+0` row: [20,19,18]), `constant_sort_key_planning_follows_the_other_keys` (no `"order_by":` in the scan spec), `constant_sort_key_join_routes_by_the_other_keys` (join takes the N-scan route, not broadcast), `constant_only_order_by_runs_as_an_unordered_limit` (pushed SQL keeps `ORDER BY "_LH_PROJ_1"`), `e2e_join_fallback_constant_sort_key_returns_the_native_order` ([20,19,18]).
- [x] 2.1 Failing unit test in support_tests.rs for constant-key removal
- [x] 2.2 Implement constant-key removal in PushdownRequest::parse
- [x] 2.3 Run `make e2e-up test-e2e`; record result
  - `make test-e2e` (stack up, `.so` rebuilt): 378 passed, 0 failed, including the six new tests.
- [x] 3.1 Update docs/capabilities.md

## Phase 3: Verification
- [x] 4.1 `make fmt-check lint`, `cargo test --workspace --lib`, changed-line coverage >= 90%
  - `make fmt-check lint` clean; `cargo test --workspace --lib`: 302 + 52 + 1445 + 185 = 1984 passed, 0 failed. Changed-line coverage 27/28 = 96.4% (the one miss, an empty `orderBy` array, is now covered by a unit case); workspace line coverage 91.81%.

## Phase 4: Review Fixes
- [x] 4.1 Reword the `PushdownRequest` doc comment in support.rs to "no constant key that Exasol pushed" and name the `FALSE` key for an offset-only constant `orderBy`
- [x] 4.2 Extend `constant_only_order_by_keeps_its_offset_window` with the `5` variants and assert `ORDER BY FALSE` in each pushed SQL
- [x] 4.3 Replace the join-route check in `constant_only_order_by_keeps_its_offset_window` with `assert_two_scan_join_wrapper`
- [x] 4.4 Delete `first_column_ids` in order_by_constant.rs and inline it into `assert_rows`
- [x] 4.5 Add `events_labels_join` to fixtures.rs and use it in order_by_constant.rs and join.rs, deleting `labels_table` and `join_from`
- [x] 4.6 Add `limit_with_offset` to the request builder in test_support_tests.rs and use it in `parsed_order_by`

## Phase 5: Verification (post-review)
- [x] 5.1 Build, unit, lint, format: `make cross-udf-build` exit 0; `cargo test --workspace --lib` 1984 passed; `make fmt-check lint` clean
- [x] 5.2 `make e2e-up test-e2e`: 378 passed, 0 failed
- [x] 5.3 Changed-line coverage 28/28 = 100%; workspace lines 90.89%
- [x] 5.4 Manual tests from plan: 8/8 pass

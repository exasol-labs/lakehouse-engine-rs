# Tasks: fix-join-decline-hard-fail (revision addressing PR #78 review)

## Phase 2: Implementation (Group A — vs-expression aggregate seam)
- [x] 2.A1 Add `function_aggregate` arm to `render_expression_inner` (vs-expression/src/lib.rs) [expert]
- [x] 2.A2 vs-expression unit tests for aggregate rendering (plan task 4.1)

## Phase 2: Implementation (Group B — seam unification) [after A]
- [x] 2.B1 Unify `render_selectlist_item_qualified` / `render_aggregate_qualified` onto the vs-expression aggregate path (plan task 1.2) [expert]
- [x] 2.B2 pushdown seam unit tests: scalar-over-aggregate item renders; top-level bare aggregate byte-compatible (plan task 4.2)

## Phase 2: Implementation (Group C — single unified N>=2 join renderer)
- [x] 2.C1 Collapse `JoinShape` (fold Eligible + MultiTable into one shape; detect_join asserts inner+equi over N>=2) (plan task 2.1) [expert]
- [x] 2.C2 Route handle_pushdown through single `plan_join`; broadcast eligibility computed inside it (plan task 2.2)
- [x] 2.C3 Make `build_n_scan_join_sql` the sole N>=2 fallback renderer; remove build_unaccelerated_join_sql / build_two_scan_join_sql / resolve_join_sides / LHS_FACT/LHS_DIM (plan task 2.3) [expert]

## Phase 2: Implementation (Group D — purge retry fiction) [after B, C]
- [x] 2.D1 Delete "retry natively" framing from the 15 sites; reword genuine last-resort errors as hard errors; delete now-unreachable decline sites (plan task 3.1)
- [x] 2.D2 Update `msg.contains("retry")` test (pushdown.rs:7936); retire TooManyTables decline facet / dead reason paths (plan task 3.2)

## Phase 2: Implementation (Group E — detection/builder tests + seed) [after C]
- [x] 2.E1 pushdown host tests: detect_join unifies N=2/3/4; non-inner -> hard Err; missing TABLE_MAP -> hard Err; build_n_scan_join_sql N=2/3/4 qualified; shared-column triple (plan task 4.3)
- [x] 2.E2 Extend join E2E seed for scalar-over-aggregate shape at N=2 and N>=3 (plan task 5.1)

## Phase 2: Implementation (Group F — E2E behavior) [after E]
- [x] 2.F1 e2e_scalar_over_aggregate_grouped_join_result_correct (N=2) + _n_table (N>=3): success, unified N-scan wrapper, result == single-node (plan task 5.2)

## Phase 2b: Code review
- [x] 2.R1 code-reviewer pass: 4 findings verified fixed, 0 blocking; fixed 1 trivial outdated comment (pushdown.rs:7419)

## Phase 3: Verification
- [x] 3.1 cargo fmt --check + cargo clippy --all-targets (CI-exact) — clean
- [x] 3.2 cargo test (host unit) — 0 failures (453 lakehouse lib + 61 vs-expression + integration bins)
- [x] 3.3 make test-e2e (Docker) — 78 passed, 0 failed across 5 suites (join binary 10/10; fixed ground-truth via native materialization to sidestep the out-of-scope single-table scalar-over-aggregate limitation)
- [x] 3.4 Scenario coverage audit + verification-report.md
- [x] 3.5 Version bump lakehouse-engine 0.24.2 -> 0.24.3 (fix); Cargo.lock synced

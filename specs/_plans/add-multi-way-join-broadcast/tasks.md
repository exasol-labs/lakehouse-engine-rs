# Tasks: add-multi-way-join-broadcast

- [x] 1. Grouping (`broadcast_policy.rs`): candidate pairs from join keys, eligibility (budget, width rule for aggregation, more than two legs, or a declined conjunct), ranking, and a reason per pair not broadcast. Replaces `BroadcastFacts` and `n_scan_reason`. Unit tests in `broadcast_policy_tests.rs`, including `ranks_candidate_pairs_by_filter_then_fact_bytes_then_dimension_bytes_then_order`.
- [x] 2. Plan model (`plan.rs`): `FanOut`, `JoinPlan` (N-scan, one broadcast pair at its lowest leg's position), and tail placement with the two-leg window gate.
- [x] 3. Condition placement (`rendering.rs`): fan-out-local filter and residual over a plan, per-leg naming from the plan replacing `LegColumns`. Unit test: placement is total and disjoint across fan-outs and the residual.
- [x] 4. Rendering (`sql_builders.rs`): `render_join_plan` for the N-scan, the two-leg broadcast, and the hybrid; drop `build_n_scan_join_sql`, `build_broadcast_plan_sql`, `build_broadcast_join_sql` as entry points and `JoinSides`/`select_broadcast_sides`; every `dispatch_golden` fixture byte-identical.
- [x] 5. Orchestration (`joins/mod.rs`): `plan_join` runs grouping, placement, tail, rendering, and tries eligible pairs in rank order; remove `plan_broadcast_join`.
- [x] 6. Unit tests of the rendered shapes: three-leg hybrid, declined conjunct in Exasol's `WHERE` on a two-leg broadcast, wrapped select list keeps the broadcast, fallback to the next pair on a render decline.
- [x] 7. E2E (`e2e_local/join.rs`): the tests the plan's Scenario Coverage names; remove the two declined-filter N-scan tests and the "broadcast stays strictly two-table" assertions; move N-scan shape assertions to `VS_NAME_LOW`.
- [x] 8. Docs and mission: `query-guide.md`, `capabilities.md`, `architecture.md`, `tuning.md`, `capability-matrix.md` check, `specs/mission.md`.
- [x] 9. Verification: build, unit tests, local E2E, lint, format, manual checks.

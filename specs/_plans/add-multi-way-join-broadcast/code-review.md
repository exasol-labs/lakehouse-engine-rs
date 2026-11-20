# Code Review Findings: add-multi-way-join-broadcast

## Summary
- Files reviewed: 30
- Total findings: 9

Evidence: `cargo test --workspace --lib pushdown` passes (706 tests, exit 0), and `make lint` passes (exit 0). No E2E suite was run for this review. Every `dispatch_golden` testdata file is unchanged.

## Findings

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs

#### [DECISION_SPLIT_ACROSS_MODULES] Tail placement is decided in the renderer, not in `plan.rs`
- Location: lines 134-140 (`render_join_plan`), and `plan.rs` lines 90-123 (`broadcast_serves_window`)
- Issue: The architecture delta assigns "where the aggregation, ordering, and limit run" to one module, and the plan puts it in `plan.rs`. `plan.rs` owns only the gate (`leg_count > 2 || aggregation_type(..).is_some()`, then window classification). The decision that actually sends the tail to the shards (`plan.fan_outs().len() == 1 && residual.is_none() && aggregation_type(..).is_none()`) is written inline in `render_join_plan`. The two predicates restate the same rule ("more legs or aggregation means Exasol renders the tail") in two modules, so a change to one can silently disagree with the other.
- Fix: Add `pub(super) fn tail_runs_in_shards(plan: &JoinPlan, residual: Option<&Json>, pushdown: &PushdownRequest) -> bool` to `plan.rs` that returns `plan.fan_outs().len() == 1 && residual.is_none() && aggregation_type(pushdown.json()).is_none()`. Replace the inline condition at `sql_builders.rs:134-136` with a call to it, and write `broadcast_serves_window`'s early return as `if leg_count > 2 || aggregation_type(pushdown.json()).is_some()` next to it, so `plan.rs` holds both halves of the rule.

#### [HIDDEN_ASSUMPTION] `render_broadcast_join` builds its placement from a hard-coded `FanOut::pair(1, 0)`
- Location: line 59
- Issue: `render_broadcast_join` runs `place_conditions` and `pair_emission` against `JoinPlan::broadcasting(2, &FanOut::pair(1, 0))`, whatever the real fact and dimension are. `render_join_plan` then passes the real pair (`plan.fan_outs()[0]`) only to `build_pair_fan_out_sql`. The result is correct today only because `place_conditions` and `pair_emission` read just `pair.legs()`. If either one starts reading `sharded` or `broadcast`, the two-leg path silently uses the wrong roles when leg 0 is the fact.
- Fix: Add a `pair: &FanOut` parameter to `render_broadcast_join`, build the plan with `JoinPlan::broadcasting(2, pair)`, and pass `&plan.fan_outs()[0]` from `render_join_plan` line 137. In `rendering_tests.rs` and `sql_builders_tests.rs`, pass `&FanOut::pair(1, 0)`. Those fixtures make CUSTOMER, leg 0, the dimension.

### crates/lakehouse-engine/src/adapter/pushdown/joins/plan.rs

#### [DUPLICATE_CODE] `qualify_window_clauses` duplicates `sql_builders::qualified_request`
- Location: lines 125-133
- Issue: `qualify_window_clauses` loops over `["selectList", "orderBy"]` and calls `legs.qualify`. `sql_builders.rs:217` `qualified_request` does the same over `["selectList", "filter", "orderBy"]`. That is two copies of one leg-qualification rewrite.
- Fix: Move `qualified_request` into `attribution.rs` as `JoinLegs::qualify_request(&self, pushdown: &PushdownRequest) -> PushdownRequest`, keeping its three clauses. Call it from `sql_builders.rs` (both call sites) and from `broadcast_serves_window` as `legs.qualify_request(pushdown)`, reading `.json()` where a `&Json` is needed. Delete `qualify_window_clauses`.

#### [AMBIGUOUS_NAME] Two unrelated types are named `FanOut` in the pushdown module tree
- Location: `plan.rs` line 11; `support.rs` line 229
- Issue: `support::FanOut` is a rendered scan (spec plus `EMITS`), re-exported from `adapter::pushdown`. `joins::plan::FanOut` is a group of legs, re-exported as `joins::FanOut`. `dispatch_golden_tests.rs` imports `super::*` and then `super::joins::FanOut`, so which `FanOut` is in scope depends on import shadowing. One name for two concepts misleads readers of both modules.
- Fix: Rename `joins::plan::FanOut` to `LegGroup` with Serena `rename_symbol`, and update its doc to "One fan-out of a join plan: ...". Keep `support::FanOut` unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs

#### [DUPLICATED_DECISION] "Which fan-out holds a conjunct" is written three times, with different answers for a column-free conjunct
- Location: lines 158-161 (`lies_in`), lines 225-231 (`holds` closure in `place_conditions`); `broadcast_policy.rs` lines 163-166 (`lies_in_one_fan_out`)
- Issue: The architecture delta gives condition placement one owner. `place_conditions` puts a conjunct that reads no column into the broadcast fan-out. `lies_in` and `lies_in_one_fan_out` treat the same conjunct as residual. The grouping in `broadcast_policy.rs` re-derives placement for the width rule (`dimension_reads`) instead of asking the placement module. The disagreement is harmless today because such a conjunct names no column, but the three tests can drift.
- Fix: Add `pub(super) fn fan_out_holds(legs: &JoinLegs, fan_out: &FanOut, conjunct: &Json) -> bool` to `rendering.rs`, with the body of the `holds` closure. Use it for `local` and `structural` in `place_conditions`. Change `fan_out_filter` to take `leg: usize` and test `fan_out_holds(legs, &FanOut::plain(leg), c)`, then delete `lies_in`. In `broadcast_policy.rs`, replace `lies_in_one_fan_out(graph.legs, conjunct.tree, pair)` with `JoinPlan::broadcasting(graph.bytes.len(), pair).fan_outs().iter().any(|f| fan_out_holds(graph.legs, f, conjunct.tree))`, and delete `lies_in_one_fan_out`.

#### [STALE_COMMENT] `fan_out_filter` documents consumers it no longer has
- Location: lines 141-153
- Issue: The doc says the fan-out's `ScanSpec.filter` (b) and the outer wrapper's WHERE (c) receive trees from this function. Its one caller is leg pruning in `joins/mod.rs:180`, which passes `&[leg]`. The fan-out filters and the residual now come from `place_conditions`. The widened `fan_out_legs: &[usize]` parameter has no multi-leg caller.
- Fix: Replace the doc of `fan_out_filter` with one line: "The conjuncts that read only leg `leg`, raw, for that leg's file pruning: pruning by them is sound for an inner join, even when the scan declines one." The parameter change is part of the previous finding.

### crates/lakehouse-engine/src/adapter/pushdown/joins/broadcast_policy.rs

#### [DEAD_CODE] `Grouping::refused` and `NScanReason` are computed in production and read only by tests
- Location: lines 13-21 (`NScanReason`), line 53 (`refused`), lines 62-65
- Issue: `broadcast_candidates` in `joins/mod.rs` reads only `.ranked`. No production code reads `refused` or any `NScanReason`, and no output (SQL, error, or log) exposes it, so `every_pair_not_broadcast_has_a_reason` and `Graph::reason` assert state that nothing can observe. This keeps production code alive for its tests.
- Fix: Delete `NScanReason` and `Grouping::refused`, make `group_legs` return `Vec<FanOut>` (the ranked eligible pairs), and turn `refusal` into `fn eligible(graph, pair) -> bool`. In `broadcast_policy_tests.rs`, replace `Graph::reason` with `fn broadcast(&self) -> bool { !self.grouping().is_empty() }`, change each `Some(..)`/`None` expectation to `false`/`true`, and in `every_pair_not_broadcast_has_a_reason` assert `grouping == []` only. Update the plan's Design item 1, which names the per-pair reason.

#### [WIDTH_RULE_GAP] A literal conjunct the scan declines still counts as width-bounding
- Location: lines 186-221 (`literal_bounded_columns`)
- Issue: `literal_bounded_columns` reads every WHERE conjunct, including those with `renders == false`. Since this change, a declined conjunct no longer takes the N-scan: it runs in Exasol's WHERE after the fan-out has emitted every row. A declined `col = 'x'` on a dimension text column therefore does not bound what the fan-out repeats per joined row, but it still clears the width rule for that column. This is the case the rule exists to refuse. `has_own_filter` in the same file already filters on `conjunct.renders`.
- Fix: In `literal_bounded_columns`, change `.filter_map(|conjunct| { let conjunct = conjunct.tree; ...` to first skip conjuncts with `!conjunct.renders`, for example `.filter(|conjunct| conjunct.renders).filter_map(...)`. Add a case to `a_two_leg_row_scan_reads_unbounded_text_only_once_a_conjunct_is_left_for_exasol` in `broadcast_policy_tests.rs`: a declined `string_equal("CUSTOMER", "C_NAME", "x")` listed in `declined`, expecting the pair to be refused.

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs

#### [STALE_TEST_NAME] Four tests are named after the removed `build_n_scan_join_sql`
- Location: lines 439, 484, 520, 561
- Issue: `build_n_scan_join_sql` was removed as an entry point. The tests `build_n_scan_join_sql_produces_qualified_n_scan_wrapper`, `build_n_scan_join_sql_for_q1_shape_supplier_nation_region`, `build_n_scan_join_sql_for_nq3_shape_part_partsupp_supplier_nation`, and `build_n_scan_join_sql_renders_qualified_when_three_tables_share_column_name` name a function that no longer exists.
- Fix: Rename each by replacing the `build_n_scan_join_sql_` prefix with `n_scan_`, for example `n_scan_produces_qualified_n_scan_wrapper`.

# Plan: add-multi-way-join-broadcast

## Summary

A join of three or more tables broadcasts one eligible pair of legs into the shards of its larger leg and keeps the N-scan for the other legs, and a filter conjunct the scan cannot run moves to Exasol's `WHERE` while the broadcast stays, for two-leg joins too (#673). The two-leg broadcast, the hybrid, and the N-scan come out of one join plan model and one renderer.

## Context

- Every join of three or more tables takes the N-scan today: each table is scanned with only its own filters, and Exasol stores every fact row before it joins (#673).
- The planner makes one yes/no broadcast choice, and three things can say no (the shape, the policy, a render decline). Each sends the whole query to the N-scan.
- The broadcast policy assumes legs 0 and 1 as its fact and dimension.
- #639 (aggregate in the shards) and #663 (left outer join) are later shapes of the same plan model, so each decision has one owner.
- The plan model is an architecture constraint, see `architecture.md` in this plan directory.
- Sorting by an expression inside each shard is out of scope (#675).

## Features

| Feature | Status | Spec |
|---------|--------|------|
| pushdown-joins/pushdown-planning-join | CHANGED | `pushdown-joins/pushdown-planning-join/spec.md` |
| pushdown-joins/pushdown-planning-join-fallback | CHANGED | `pushdown-joins/pushdown-planning-join-fallback/spec.md` |
| pushdown-joins/pushdown-planning-join-ordering | CHANGED | `pushdown-joins/pushdown-planning-join-ordering/spec.md` |
| pushdown/pushdown-declined-filter-self-apply | CHANGED | `pushdown/pushdown-declined-filter-self-apply/spec.md` |
| pushdown-types/pushdown-planning-scan-text-conversion | CHANGED | `pushdown-types/pushdown-planning-scan-text-conversion/spec.md` |
| pushdown/pushdown-planning-selectlist-expressions | CHANGED | `pushdown/pushdown-planning-selectlist-expressions/spec.md` |

## Impact

- A join of three or more tables with an eligible pair sends Exasol only the pair's joined rows instead of every row of both tables. Its pushdown SQL changes: one fan-out carries a join block, and Exasol joins it with the other legs' fan-outs.
- A two-leg broadcast-eligible join whose filter holds a conjunct the scan cannot run (a date-to-text conversion, for example) keeps the broadcast. Its pushdown SQL gains an Exasol `WHERE` over the fan-out, and its `ORDER BY` and `LIMIT` move from the shards to Exasol.
- A join with no eligible pair, and every other two-leg join, produce the same SQL as before.
- No CONNECTION field, property, or capability changes. `JOIN_BROADCAST_MAX_BYTES` keeps its name and default and now applies to the dimension of any pair.
- No breaking change.

## Design

The four decisions of #673, each in one module of `crates/lakehouse-engine/src/adapter/pushdown/joins/`:

1. **Grouping** (`broadcast_policy.rs`). Input: per leg, its bytes after pruning and whether it has a leg-local conjunct the scan renders; per candidate pair, its join keys (`condition_filters::is_join_key` over every join's condition, keys reading exactly two legs). Output: the eligible pairs in rank order, each a fan-out (`plan.rs`: `LegGroup { sharded, broadcast: Vec<usize> }`, at most one broadcast leg in this policy). A pair over budget or with an unbounded dimension column is left out. The width rule reads the dimension columns that the select list, the other join conditions, the residual `WHERE`, `GROUP BY`, `HAVING`, and `ORDER BY` read, never the placeholder join-key column, and applies when the request aggregates, the join has more than two legs, or a conjunct is left for Exasol's `WHERE`. Ranking per the pair-choice scenario.
2. **Condition placement** (`rendering.rs`). Widen `leg_local_filter` to the conjuncts whose legs all lie in one fan-out; the rest is residual. The scan translator screen stays at the renderer (ADR `screen-renderability-at-the-render-consumer-not-in-the-partition-classifier`): a declined conjunct joins the residual. Each leg's pruning predicate keeps its raw leg-local conjuncts, as today. The pair's join keys go to its join block, and every other join condition stays in Exasol's `ON` chain.
3. **Tail placement** (`plan.rs`). The tail runs in the shards only when the plan has one fan-out and the residual is empty, through today's `classify_join_window` and `place_broadcast_window`. Otherwise Exasol renders it with `outer_wrapper_clauses`. A two-leg request whose window is `JoinWindowPlan::BroadcastUnservable`, or whose sort key is not in the broadcast projection, keeps the N-scan with or without a residual (#675).
4. **Rendering** (`sql_builders.rs`). One `render_join_plan` replaces the composition of `build_n_scan_join_sql`, `build_broadcast_plan_sql`, and `build_broadcast_join_sql`. A plain fan-out uses `build_side_fan_out_sql`, a broadcast fan-out builds its `JoinSpec` from its fan-out roles. Several fan-outs join through `build_n_scan_join_from`, generalized from legs to fan-outs; the pair's relation takes its fact leg's alias, and its columns bind as `"LHS_T{fact}"."LHS_T{leg}_{COLUMN}"`. `LegColumns` becomes a per-leg naming that the renderer derives from the plan. A single fan-out with an Exasol tail keeps today's unaliased `FROM (<fan-out>)`, so every existing golden stays byte-identical.

`plan_join` in `joins/mod.rs` runs the four in order. The policy returns the eligible pairs in rank order, and the renderer takes the first one whose broadcast fan-out renders; when none renders, every leg is plain. A wrapped select list keeps the broadcast: the fan-out emits the plain columns the select list reads, and the outer Exasol SELECT renders the original items (`render_join_plan` takes over the decline in `render_broadcast_join_emitting` on `RowProjection::Wrapped`). The scan UDF and `JoinSpec` do not change: the pair's spec is today's broadcast spec without `post_join_limit` and `post_join_order_by`.

Docs and mission, in the same PR:

- `docs/query-guide.md`: replace "Only joins of two tables are broadcast joins" and the table row at line 35 with the pair rule, and update the verified-against version.
- `docs/capabilities.md` (join section) and `docs/architecture.md` line 82: the pair, the hybrid, and the declined conjunct in Exasol's `WHERE`.
- `docs/tuning.md`: the `JOIN_BROADCAST_MAX_BYTES` row names the dimension of a pair, not of a two-table join.
- `docs/capability-matrix.md`: check the join notes; no capability changes.
- `specs/mission.md`: Core Capability 6 and Out of Scope name the broadcast of one pair inside a multi-way inner join, and keep general multi-way join optimization out of scope.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Struct + fns | `joins/planning.rs` `JoinSides`, `JoinSides::fact_leg`, `select_broadcast_sides` | Fact and dimension come from the plan's fan-outs, not from legs 0 and 1 |
| Function | `joins/mod.rs` `plan_broadcast_join` | Replaced by the grouping decision and `render_join_plan` |
| Functions | `joins/sql_builders.rs` `build_broadcast_plan_sql`, `build_broadcast_join_sql`, `build_n_scan_join_sql` as separate entry points | Folded into `render_join_plan`; the golden sibling module drives `render_join_plan` |
| Struct | `joins/broadcast_policy.rs` `BroadcastFacts` | Replaced by the join graph input |
| Enum | `joins/rendering.rs` `LegColumns` | Replaced by per-leg naming from the plan |
| Test | `joins/planning_tests.rs` tests of `select_broadcast_sides` | Moved to the ranking tests of `broadcast_policy_tests.rs` |
| Test | `e2e_local/join.rs` `e2e_broadcast_declined_filter_falls_back_to_n_scan_and_filters`, `e2e_broadcast_declined_filter_excludes_rows` | Their scenario is removed; replaced by the declined-conjunct test below |
| Assertion | `e2e_local/join.rs` `e2e_three_table_join_result_correct`, `e2e_four_table_join_result_correct`: "broadcast stays strictly two-table" | Made false by this change; both tests run on `VS_NAME_LOW` to keep proving the N-scan |
| Assertion | `e2e_local/join.rs` `e2e_three_leg_self_join_matches_single_node`, `e2e_three_table_join_on_filters_reach_their_leg_scans`, `e2e_three_table_join_equality_over_two_legs_stays_in_on`, `e2e_scalar_over_aggregate_grouped_join_n_table_result_correct` | Any N-scan shape assertion moves to `VS_NAME_LOW`, because the fixtures are under the threshold |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Broadcast join projection and filter are rendered per involved table | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_declined_conjunct_runs_in_exasol_where` |
| A two-leg join with filters in its condition is planned as a broadcast fan-out | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_join_with_on_filters_takes_the_broadcast_path_and_matches_native` (adds a declined `ON` conjunct case) |
| A join of three or more legs broadcasts one pair and scans the other legs | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_multi_way_join_broadcasts_one_pair_and_matches_native` (case table over the listed shapes; runs the pair fan-out's SQL to count its rows) |
| The broadcast pair is the eligible pair whose dimension has a filter of its own | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_multi_way_join_broadcasts_the_filtered_dimension_pair` |
| The broadcast pair is the eligible pair whose dimension has a filter of its own (byte and FROM-order tie-breaks) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/broadcast_policy_tests.rs` | `ranks_candidate_pairs_by_filter_then_fact_bytes_then_dimension_bytes_then_order` |
| A pair that is not eligible is not broadcast | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_multi_way_join_without_eligible_pair_keeps_the_n_scan` (over threshold on `VS_NAME_LOW`, and a wide dimension text column without aggregation) |
| A join outside the broadcast contract is declined safely | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_above_threshold_unaccelerated_fallback_shape` |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_three_table_join_result_correct` (on `VS_NAME_LOW`) |
| A fallback leg's fan-out scan spec never carries a row limit | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_multi_way_join_with_limit_cuts_no_shard` |
| A broadcast join applies a declined filter conjunct in Exasol's WHERE | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_declined_conjunct_runs_in_exasol_where` |
| A single-table WHERE filter splits at AND between the scan and the wrapper | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_declined_conjunct_runs_in_exasol_where` (the changed line; the single-table proof in `capability.rs` is unchanged) |
| A wrapped derived projection routes to a native wrapper on every path | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_with_a_declined_select_item_keeps_the_broadcast` (two legs and three legs, `TO_CHAR(o.O_ORDERDATE)` in the select list) |
| A join filter that converts a date to text runs in Exasol, and such a join key keeps the N-scan | E2E | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_join_date_to_text_filter_runs_in_exasol_and_key_keeps_the_n_scan` (renames `e2e_broadcast_like_on_date_column_falls_back_and_filters`) |

Unit tests beside the E2E proofs, for pure logic: `broadcast_policy_tests.rs` (no pair over budget or unbounded is broadcast; width rule applies with three legs without aggregation), `rendering_tests.rs` (conjunct placement is total and disjoint across fan-outs and the residual),. Every existing `dispatch_golden` fixture stays byte-identical, and none is added, because two specs pin their count.

### Manual Testing

Against this clone's local stack (`.claude/CLAUDE.md` overrides exported, `EXAPUMP_DSN='exasol://sys:exasol@127.0.0.1:38563?validateservercertificate=0'`), after `make e2e-up test-e2e` has seeded the join fixtures.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| pushdown-planning-join | `exapump sql "EXPLAIN VIRTUAL SELECT s.S_NAME, SUM(l.L_QUANTITY) FROM MY_LAKEHOUSE_JOIN.FACT_LINEITEM l JOIN MY_LAKEHOUSE_JOIN.FACT_ORDERS o ON o.O_ORDERKEY = l.L_ORDERKEY JOIN MY_LAKEHOUSE_JOIN.DIM_SUPPLIER s ON l.L_SUPPKEY = s.S_SUPPKEY WHERE s.S_NAME = 'Supplier#1' GROUP BY s.S_NAME" -d "$EXAPUMP_DSN"` | One `LAKEHOUSE_SCAN` whose spec holds `"join":{` with `DIM_SUPPLIER`'s files, one plain `LAKEHOUSE_SCAN` for `FACT_ORDERS`, joined by `INNER JOIN … ON`, and an outer `GROUP BY` |
| pushdown-planning-join-fallback | The same query against `MY_LAKEHOUSE_JOIN_LOW` | Three plain fan-outs `LHS_T0`, `LHS_T1`, `LHS_T2`, no `"join":{` |
| pushdown-planning-join-ordering | The first query with `ORDER BY 2 DESC LIMIT 1` | No `post_join_limit`, no `post_join_order_by`, no `"limit"` in any spec; `LIMIT 1` on the outer SELECT |
| pushdown-declined-filter-self-apply | `exapump sql "EXPLAIN VIRTUAL SELECT o.O_ORDERKEY, c.C_NAME FROM MY_LAKEHOUSE_JOIN.FACT_ORDERS o JOIN MY_LAKEHOUSE_JOIN.DIM_CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY WHERE o.O_ORDERDATE LIKE '2024-01-0%'" -d "$EXAPUMP_DSN"` | One fan-out with `"join":{` and an outer `WHERE` holding the `LIKE` |
| pushdown-planning-scan-text-conversion | The previous query, then the same `SELECT` without `EXPLAIN VIRTUAL` under `ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY'` | Rows equal the same query over native copies of the tables |
| Benchmark (#673 criterion) | `mv bench/.env bench/.env.off 2>/dev/null; make bench` on `main`, then on the branch | The report's multi-way join queries (NQ3 and the 3- and 4-way joins) show a join block in their pushdown SQL on the branch, and their run times side by side |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace` | 0 failures |
| E2E | `make e2e-up test-e2e` (with this clone's stack overrides) | 0 failures |
| E2E (Lakekeeper) | not required: the change targets no catalog behavior | Not run |
| Lint | `make lint` | 0 errors/warnings |
| Format | `make fmt-check` | No changes |

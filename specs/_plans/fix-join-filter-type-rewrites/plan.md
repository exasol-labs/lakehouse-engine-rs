# Plan: fix-join-filter-type-rewrites

## Summary

Wire `apply_type_rewrites` into both join WHERE-filter render sites: `render_broadcast_join`'s
combined filter and the N-scan fallback's per-leg filter. Both screen purely syntactically today and
render the tree bare.

That hard-fails the DataFusion scan on a `LIKE` over a non-string column (#215) and silently
mis-answers a DECIMAL stringification (#223 slice 2). Every decline routes through the join path's
existing self-application outcome, which PR #285 (#279) made safe.

## Design

### Context

Three type-rewrite guards ship today, wired into two render surfaces: the single-table WHERE filter
(via `classify_where_filter`) and the select-list projection (via `project_columns`, shared by the
broadcast join).

The two JOIN WHERE-filter sites were left unwired and are the only JOIN WHERE-filter surfaces with no
column-type awareness:

| Site | File | Current screen |
|---|---|---|
| Broadcast combined filter | `joins/sql_builders.rs` `render_broadcast_join` | `datafusion_renderable(f)` — `render_expression_safe(expr).is_some()`, purely syntactic |
| N-scan per-leg filter | `joins/sql_builders.rs` `build_side_fan_out_sql`, fed by `build_n_scan_join_sql` | `renderable_only` / `declined_only` — `partition_conjuncts(filter, datafusion_renderable)`, purely syntactic |

This plan does NOT make type-rewrite coverage complete. Three pushed expression surfaces stay unwired
and keep their exposure: the grouped-aggregate render path, the aggregate-argument render path, and
#223 slice 3's GROUP-BY-only DECIMAL keys (#223 slices 1 and 3).

A syntactic screen answers "does this tree render at all", never "will DataFusion accept these
column types". A `LIKE` on a `DECIMAL`/`DATE`/integer column is a perfectly renderable LIKE node, so
it passes and is rendered bare — precisely the tree that kills DataFusion's `type_coercion` planner
at scan execution time (SQL state 22002, no result at all).

The fix was blocked until now for a correctness reason, not a technical one: a decline was believed
safe because "Exasol re-applies a delegated predicate", which #279 disproved live. #285 replaced that
with real self-application, so a decline at either join site now has a route that actually evaluates
the predicate.

- **Goals** — make both join WHERE-filter surfaces run the SAME pipeline the single-table WHERE
  surface runs, with each surface's decline routed through the outcome that surface already has;
  close #215 and #223 slice 2; narrow #228's exposure at those surfaces.
- **Non-Goals** — no new guard, no new type dispatch, no new decline outcome, no new error path; no
  change to the join SELECT-list projection path (already correct via `project_columns`); no fix for
  #228's root cause (the `crates/vs-expression` `INSTR`/`LOCATE` arity rendering defect); no
  change to Iceberg manifest pruning; no work on #223 slices 1 and 3.

### Decision

Reuse, do not rebuild. Both sites get the existing pipeline; the only new code is one small
partition helper the N-scan site needs because its type universe is per-side.

#### Architecture

```
BROADCAST SITE  (render_broadcast_join)

  disjoint_schema_guard(left_cols, right_cols)   ── must pass FIRST
        │  (guarantees a bare column name resolves to one Exasol type)
        ▼
  col_types = left_cols ∪ right_cols
        │
        ▼
  classify_where_filter(filter_json, &col_types)     ← EXISTING sole owner
        │        (apply_type_rewrites → render_df_filter_safe)
        ├── (Some(rendered), None) ──▶ carry in common spec (rewritten tree)
        ├── (None, None) ───────────▶ absent / trivially-true: no filter
        └── (None, Some(raw)) ──────▶ Ok(None): decline broadcast → N-scan fallback


N-SCAN SITE  (build_n_scan_join_sql)     [restructured: legs FIRST, residual LAST]

  where_filter ──▶ renderable_only ──▶ leg_eligible   (syntactic, unchanged)
                └▶ declined_only  ─────────────────┐   (syntactic, unchanged)
                                                   │
  for each side i:                                 │
     side_local_filter(leg_eligible, table_i)      │
        │                                          │
        ▼                                          │
     type_screened_leg_filter(side_local, cols_per_side[i])   ← NEW helper
        ├── leg filter (REWRITTEN)  ──▶ build_side_fan_out_sql │
        └── type-declined (RAW) ────────────────────┤
                                                   │
  cross_side_residual_filter(leg_eligible) ────────┤
                                                   ▼
                              residual ──▶ render_self_applied_where (Exasol dialect, qualified)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Reuse the existing owner | `classify_where_filter` at the broadcast site | That function is already the sole owner of "rewrite, then decide scan-spec filter vs. self-apply" (`_decision/045`). Re-deriving the sequence at a second site would create the second owner that decision exists to prevent. |
| Per-side, post-attribution screen | `type_screened_leg_filter` in `joins/rendering.rs` | The N-scan path has no disjoint-column-name precondition, so the type universe MUST be the owning side's own columns. That is only knowable after `side_local_filter` attributes the conjunct. |
| One helper returns both halves | `type_screened_leg_filter -> (Option<Json>, Option<Json>)` | Both halves derive from the same `col_types` and the caller needs both. Returning them together makes the total-and-disjoint invariant a property of one function instead of an agreement between two. |
| Fail closed | `type_screened_leg_filter`'s `None` arm | If the re-formed accepted tree does not itself survive the pipeline OR is not `datafusion_renderable`, the WHOLE side-local set goes residual. A conjunct applied nowhere returns wrong rows; a conjunct applied in the wrapper is merely slower. |
| Screen the REWRITTEN tree, not just the raw one | `type_screened_leg_filter`'s partition predicate | The leg renders the REWRITTEN tree, so that is the tree whose renderability must be established. `classify_where_filter` carries the same arm at the broadcast site (`support.rs:1092`); omitting it here would drop a type-accepted-but-unrenderable conjunct from BOTH the leg and the residual — #279's exact defect at a new site. |
| Per-conjunct, not per-tree, decline | N-scan screen | One type-declining conjunct must not forfeit its side's other pushable conjuncts. The single-table surface declines whole-filter only because it has no partition to absorb one conjunct. |

Design-philosophy check on the one new abstraction (`type_screened_leg_filter`): it lives beside the
`renderable_only`/`declined_only` and `side_local_filter`/`cross_side_residual_filter` pairs in the
module that already owns conjunct partitioning — no new boundary, no new dependency direction. It is
deeper than its interface: one call yields a partition that is total, disjoint, fail-closed, and
type-correct per side, none of which a caller could get right by calling `partition_conjuncts` twice
itself. Its one-sentence responsibility: "split one side's side-local conjuncts into the rewritten
set its leg may render and the raw set the outer wrapper must apply."

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Wire the FULL `apply_type_rewrites` pipeline, not only the LIKE guard | Wire `like_subject_type_guard` alone, the literal scope of #215 | Reuses the exact single-table mechanism with zero new guard code; every decline it adds is already made safe by #285; closes #223 slice 2 (a silent wrong answer, worse than #215's loud failure) and narrows #228 at the same two sites for free. A LIKE-only wiring would leave two known silent-wrong-answer paths open at surfaces we are already editing. |
| Broadcast: reuse `classify_where_filter` | Call `apply_type_rewrites` + `render_df_filter_safe` inline at the join site | Avoids a second owner of the classification; inherits the absent-vs-trivially-true-vs-declined three-way distinction the join site would otherwise have to re-derive (and previously got wrong, per #279). |
| N-scan: restructure so per-side legs are computed BEFORE the residual | Keep the current order and post-hoc subtract type-declined conjuncts from the legs | The residual must be able to receive conjuncts the per-side pass rejects, so the residual cannot be finalized first. Reordering is the smaller and more obviously-total change. |
| N-scan: screen per conjunct | Screen the whole side-local tree, declining all of a side's conjuncts on one bad one | Per-conjunct keeps pushdown for everything the leg can apply; the partition already exists to express it, so this costs nothing structurally. |
| New feature file for the join surfaces | Append the six scenarios to `pushdown-planning-like-type-coercion` | That feature already carries 10 scenarios (the library's organization threshold) and the join surfaces raise a concern it does not have: which column-type universe a surface may screen against. Same precedent as `pushdown-planning-string-fn-type-coercion-composition`, split for the same reason. |
| Guard runs twice per conjunct in `type_screened_leg_filter` | A map-and-partition variant of `partition_conjuncts` that keeps the rewritten conjunct | Planning-time trees are tiny and this runs once per pushdown request; reusing the existing `partition_conjuncts` verbatim beats a new traversal primitive for an unmeasured cost. If it ever matters, the upgrade is local to one function. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| pushdown-planning-join-filter-type-coercion | NEW | `vs-adapter/pushdown-planning-join-filter-type-coercion/spec.md` |
| pushdown-planning-join | CHANGED | `vs-adapter/pushdown-planning-join/spec.md` |
| pushdown-planning-join-fallback | CHANGED | `vs-adapter/pushdown-planning-join-fallback/spec.md` |
| pushdown-declined-filter-self-apply | CHANGED | `vs-adapter/pushdown-declined-filter-self-apply/spec.md` |
| pushdown-planning-string-fn-type-coercion | CHANGED | `vs-adapter/pushdown-planning-string-fn-type-coercion/spec.md` |
| pushdown-planning-decimal-string-format | CHANGED | `vs-adapter/pushdown-planning-decimal-string-format/spec.md` |
| pushdown-planning-like-type-coercion | CHANGED | `vs-adapter/pushdown-planning-like-type-coercion/spec.md` |

## Impact

Query results change — in every case from wrong or failing to correct:

| Query shape at a join WHERE filter | Before | After |
|---|---|---|
| `LIKE` over a non-string, non-DATE column | Hard scan failure, `F-UDF-CL-RUST-9001` / SQL state 22002, no result | Correct rows (broadcast falls back to N-scan; N-scan side-local becomes a residual outer-`WHERE` conjunct) |
| `LIKE` over a `DATE` column | Hard scan failure, no result | Correct rows under the default `NLS_DATE_FORMAT`, still pushed down as `CAST(<col> AS VARCHAR) LIKE …`; broadcast plan retained. The #216 tracked exception (an altered session date format) carries over unchanged from the single-table WHERE surface — it is not a new exception |
| `INSTR`/`LOCATE` with a 3rd or 4th argument | Silently wrong position (arguments dropped by the renderer) | Correct rows, evaluated natively by Exasol via the decline |
| `CAST`/`CONCAT`/`LENGTH` over a `DECIMAL` column | Silently wrong rows (full-scale decimal text, e.g. `2912.00`) | Correct rows (Exasol-trimmed form, `2912`) |
| A bare `LIKE`/governed-string-function column absent from `involvedTables` metadata | Rendered bare into the broadcast plan | Broadcast declined, N-scan fallback self-applies the conjunct |
| Anything with no type-rewrite trigger | Correct | Byte-identical SQL, unchanged |

The unresolvable-column row is a NEW decline trigger unrelated to type coercion: both `Option`-returning
guards decline on a `col_types` lookup miss (`support.rs:1074-1078`), so a name absent from the involved
tables' metadata forfeits the broadcast plan where it previously rendered. Blast radius is bounded to
requests whose `involvedTables` metadata omits a column the filter references — the adapter builds
`col_types` from that same metadata, so a well-formed Exasol request cannot hit it; a malformed or
truncated one now takes the safe route instead of the bare one.

Performance: each decline costs exactly one thing — the broadcast variant loses the broadcast
optimization and takes the N-scan fallback, the N-scan variant loses one conjunct's per-leg row
filtering. Both are slower than a working pushdown, and neither shape ever had a working pushdown to
lose. Iceberg manifest pruning is unaffected in every case, so no query opens more files.

No breaking changes to the wire format, the scan-spec schema, the UDF ABI, or any public surface.

### Issue bookkeeping (for the PR body and the recorder — get the scoping exactly right)

| Issue | Action | Must NOT say |
|---|---|---|
| #215 | `Closes #215` | — |
| #223 | Comment narrowing it to slices 1 (computed-expression arguments) and 3 (GROUP-BY-only keys); slice 2 (join per-leg filter) is fixed here. Edit the body's slice-2 bullet to note it. | `Closes #223` — two of its three slices remain open |
| #228 | Comment noting the exposure narrowed: the #210 decline-mitigation now also covers the two join WHERE surfaces. Root cause (the `crates/vs-expression` `INSTR`/`LOCATE` arity rendering defect) is untouched. | `Closes #228` / `Fixes #228` |
| #279 / #285 | Prior context only — already merged into this branch. | Do not re-close |

### Recorder note

`pushdown-planning-like-type-coercion`, `pushdown-planning-string-fn-type-coercion`, and
`pushdown-planning-decimal-string-format` each carry a now-false statement in their INTRO prose (not
in a scenario): the first enumerates "both render surfaces", the other two list the join per-leg
filter path as out of scope under #223. Each delta states the correction as a Background bullet with
the `REPLACING` convention AND inside a `DELTA:CHANGED` scenario clause, but the stale intro sentence
itself must be struck by hand at merge time — a delta marker cannot reach intro prose.

## Dependencies

Builds on branch `feat/fix-declined-filter-self-apply` (PR #285, #279), already merged into this
worktree. #285's self-application mechanism is a hard prerequisite: without it, every decline this
plan adds would drop a predicate instead of applying it.

#215's body declares a second dependency on #228. It is SOFT and need not land first: the
`INSTR`/`LOCATE` >2-argument arity decline this wiring makes newly reachable is already SAFE
post-#285 (it self-applies and returns Exasol's native result), and #228's own fix would later
REPLACE that decline with a faithful multi-argument rendering — a strictly better outcome at the same
sites, not an invalidation of this wiring.

## Implementation Tasks

- [ ] 1.1 Add failing unit tests in `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` `tests` for `render_broadcast_join`: a `LIKE` over a `DECIMAL(20,0)` side column returns `Ok(None)`; a `LIKE` over a `DATE` side column returns `Ok(Some(..))` whose `filter` carries the CAST-to-VARCHAR form; an `INSTR(a, b, 3)` filter returns `Ok(None)`; an absent filter and a trivially-true filter each stay eligible with no scan-spec filter.
- [ ] 1.2 Add `join_col_types(request, join) -> Vec<(String, String)>` as a `pub(super)` helper in `joins/rendering.rs`, the SOLE producer of the broadcast surface's column-type union (`involved_table_columns` of `join.tables[0]` extended with `join.tables[1]`'s). In `render_broadcast_join`, after `disjoint_schema_guard` passes, call it for `col_types` and replace the `datafusion_renderable` pre-check plus the raw `render_df_filter_safe` call with one `classify_where_filter(filter_json, &col_types)`; a non-`None` declined half returns `Ok(None)`. Change `extract_join_projection` (`joins/rendering.rs:28-29`) to call `join_col_types` too instead of rebuilding its own `combined` union, so one decision has one owner. Update `render_broadcast_join`'s doc comment to name the type-rewrite pass, the union type universe, and why the disjoint guard must precede it.
- [ ] 1.3 Remove the now-unused `datafusion_renderable` import from `joins/sql_builders.rs` and confirm `cargo clippy --all-targets` reports no unused import or dead code.
- [ ] 2.1 Add failing unit tests in `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` `tests` for a new `type_screened_leg_filter`: all conjuncts accepted → whole rewritten tree in the leg half, `None` in the declined half; all declined → `None` leg, whole set declined; mixed → each conjunct in exactly one half; a `DATE`-column `LIKE` arrives in the leg half in CAST-to-VARCHAR form; a `DECIMAL`-column `LIKE` arrives in the declined half in RAW form; a conjunct whose REWRITE the type pipeline ACCEPTS but the DataFusion dialect CANNOT render lands in the DECLINED half in RAW form (never dropped from both halves); two sides declaring the same column name with different Exasol types are each screened against their own `col_types`.
- [ ] 2.2 Add `type_screened_leg_filter(side_local, col_types) -> (Option<Json>, Option<Json>)` to `joins/rendering.rs`, beside `renderable_only`/`declined_only`. Partition `side_local`'s conjuncts on BOTH conditions applied to the REWRITTEN conjunct — `apply_type_rewrites(c, col_types)` is `Some(rw)` AND `datafusion_renderable(&rw)` — because the leg renders the rewritten tree, so that is the tree whose renderability must hold. Return the accepted set REWRITTEN as the leg half and the rejected set RAW as the declined half. Fail closed in BOTH directions: if the re-formed accepted tree does not itself survive the pipeline, OR survives but is not `datafusion_renderable`, return `(None, Some(side_local.clone()))`. Document why the screen is per-side and post-attribution, why the renderability check must target the REWRITTEN tree (mirroring `classify_where_filter`'s `(Some(raw), Some(tree)) if !datafusion_renderable(tree)` arm at `support.rs:1092`), and why the fail-closed direction is residual. [expert]
- [ ] 2.3 Add failing unit tests in `joins/sql_builders.rs` `tests` for `build_n_scan_join_sql`: a side-local `LIKE` over that side's `DECIMAL` column appears table-qualified in the outer `WHERE` and NOT in that side's fan-out leg; a side-local `LIKE` over that side's `DATE` column appears in its leg as CAST-to-VARCHAR and NOT in the outer `WHERE`; a side with one type-declined and one type-accepted conjunct pushes the accepted one into its leg; every top-level conjunct of a mixed filter appears exactly once across legs plus outer `WHERE`; `golden_n_scan_join_sql_unchanged` and `golden_broadcast_join_sql_unchanged` still pass byte-identically.
- [ ] 2.4 Restructure `build_n_scan_join_sql` so the per-side fan-out loop runs BEFORE the residual is assembled: keep `renderable_only`/`declined_only` as the syntactic phase, then per side call `side_local_filter` followed by `type_screened_leg_filter(.., &cols_per_side[i])`, pass the leg half to `build_side_fan_out_sql`, accumulate the declined halves, and conjoin the residual from three disjoint parts — `cross_side_residual_filter(leg_eligible)`, `declined_only(where_filter)`, and the accumulated type-declined set. Update the doc comments on `build_n_scan_join_sql` and `build_side_fan_out_sql` (whose `side_filter` is now pre-screened AND pre-rewritten). [expert]
- [ ] 3.1 Add a live-Exasol E2E test in `crates/lakehouse-engine/tests/e2e_join_test.rs`: a below-threshold two-table inner equi-join whose WHERE carries `LIKE` over the `DECIMAL` `O_CUSTKEY` column. Assert the pushed SQL carries the N-scan wrapper and NOT a broadcast join block, and that the returned rows equal the ground-truth filtered set (row content, not just "no crash").
- [ ] 3.2 Add a live-Exasol E2E test: the same below-threshold join with `LIKE` over the `DATE` `O_ORDERDATE` column. Assert the pushed SQL DOES carry a broadcast join block (the CAST arm keeps the optimization) and that the rows equal the ground-truth filtered set. The test MUST NOT depend on the ambient session date format: either assert under the default `NLS_DATE_FORMAT` or set it explicitly on the session first, so the ground-truth comparison is format-independent and the #216 tracked exception stays out of scope.
- [ ] 3.3 Add a live-Exasol E2E test against `VS_NAME_LOW`, whose lowered `join_broadcast_max_bytes` forces the N-scan fallback, so the N-scan per-leg path is exercised: a side-local `LIKE` over the `DECIMAL` `O_CUSTKEY` column. Assert the conjunct appears in the N-scan wrapper's outer `WHERE` and not in a leg's scan spec, and that the rows equal the ground-truth filtered set.
- [ ] 3.4 Add a live-Exasol E2E test demonstrating the #228 side effect: a join WHERE filter carrying `INSTR(C_NAME, <substr>, 3)` — a three-argument call over a VARCHAR column — returns the rows Exasol computes natively, not the rows a start-position-ignoring `strpos` would return. Pick seed data where the two answers differ.
- [ ] 3.5 Extend `crates/lakehouse-engine/tests/common/seed.rs`'s star-schema seeding with ONE scale > 0 DECIMAL column on `fact_orders` — Iceberg `decimal(P, S)` with S ≥ 2, values whose trailing zeros are significant to the stringified length (e.g. `2912.00`, so `LENGTH` differs by 3 between DataFusion's full-scale text and Exasol's trimmed text). Add the field to the Iceberg schema builder and the array to `make_orders_batch`. No existing test asserts `fact_orders`'s column count or does `SELECT *` over it, so the addition is additive — but re-run `cargo test` and `make test-e2e` to confirm no fixture regressed.
- [ ] 3.6 Add a live-Exasol E2E test in `crates/lakehouse-engine/tests/e2e_join_test.rs` for the #223-slice-2 decimal case: a join WHERE filter stringifying task 3.5's scale > 0 DECIMAL column (`LENGTH(<col>) > n`, #211's headline repro shape) returns the SAME rows as native Exasol evaluation of the same predicate. Run it at BOTH join surfaces — `VS_NAME` (broadcast) and `VS_NAME_LOW` (N-scan per-leg). Without this the plan's headline justification for wiring the full pipeline over the LIKE guard alone has no live evidence, and the divergence is silent (correct row count, wrong rows).
- [ ] 4.1 Run the full checklist below and record the results, including `make test-e2e` against a manually started Docker stack.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3 |
| Group B | 2.1, 2.2, 2.3, 2.4 |
| Group C | 3.1, 3.2, 3.3, 3.4, 3.5, 3.6 |
| Group D | 4.1 |

Sequential dependencies:
- **Group A → Group B is UNCONDITIONAL, not parallel.** Both groups edit both files: tasks 1.1/1.3 and
  2.3/2.4 all touch `sql_builders.rs` (2.4 restructures `build_n_scan_join_sql`, a production function
  in it), and tasks 1.2 and 2.2 both touch `joins/rendering.rs`. Two agents editing either file
  concurrently would conflict, so A lands first.
- Group B → Group C (the E2E tests need both sites wired and a rebuilt `.so`).
- Group C → Group D.

Within Group A the order is fixed: 1.1 → 1.2 → 1.3.
Within Group B the order is fixed: 2.1 → 2.2 → 2.3 → 2.4.
Within Group C, 3.5 → 3.6 (the test needs the seeded column); 3.1–3.4 are independent of both and of
each other.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Import | `datafusion_renderable` in `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | Its only use in that file was the broadcast site's syntactic pre-check, replaced by `classify_where_filter`. Still used in `joins/rendering.rs`, so the function itself stays. |

No other code is orphaned: `renderable_only`, `declined_only`, `cross_side_residual_filter`, and
`side_local_filter` all keep their roles, and `render_df_filter_safe` keeps its use in
`build_side_fan_out_sql`.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| join-filter-type-coercion / A broadcast-join filter over a non-string LIKE subject declines the broadcast plan | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `broadcast_declines_like_over_decimal_side_column` |
| join-filter-type-coercion / A broadcast-join filter over a non-string LIKE subject declines the broadcast plan | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_like_on_decimal_column_falls_back_and_filters` |
| join-filter-type-coercion / A broadcast-join filter over a DATE LIKE subject keeps the broadcast plan | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `broadcast_keeps_plan_and_casts_like_over_date_side_column` |
| join-filter-type-coercion / A broadcast-join filter over a DATE LIKE subject keeps the broadcast plan | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_like_on_date_column_stays_broadcast_and_filters` |
| join-filter-type-coercion / An N-scan side-local conjunct the type pipeline declines becomes a residual conjunct | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `n_scan_type_declined_side_local_conjunct_moves_to_outer_where` |
| join-filter-type-coercion / An N-scan side-local conjunct the type pipeline declines becomes a residual conjunct | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where` |
| join-filter-type-coercion / An N-scan side-local conjunct the type pipeline declines becomes a residual conjunct | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `type_screened_leg_filter_declines_type_accepted_but_unrenderable_rewrite` |
| join-filter-type-coercion / An N-scan side-local conjunct the type pipeline rewrites reaches its leg rewritten | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `n_scan_date_like_side_local_conjunct_reaches_leg_as_cast` |
| join-filter-type-coercion / Two N-scan sides sharing a column name are each screened against their own side's types | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `type_screened_leg_filter_uses_owning_side_types_for_shared_column_name` |
| join-filter-type-coercion / A join filter with no type-rewrite trigger emits byte-identical SQL | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `golden_broadcast_join_sql_unchanged`, `golden_n_scan_join_sql_unchanged` (existing, must not change) |
| join-filter-type-coercion / A join filter with no type-rewrite trigger emits byte-identical SQL | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `broadcast_absent_and_trivially_true_filter_stay_eligible` |
| pushdown-planning-join / Broadcast join projection and filter are rendered per involved table | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `broadcast_declines_like_over_decimal_side_column`, `broadcast_keeps_plan_and_casts_like_over_date_side_column` |
| pushdown-planning-join-fallback / Join conditions attach greedily by table-name set and side-local filters push into each leg | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `n_scan_leg_residual_partition_is_total_and_disjoint_with_type_screen` |
| pushdown-declined-filter-self-apply / A broadcast-eligible join whose filter declines takes the N-scan fallback | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `broadcast_declines_like_over_decimal_side_column`, `broadcast_keeps_plan_and_casts_like_over_date_side_column` |
| pushdown-declined-filter-self-apply / A broadcast-eligible join whose filter declines takes the N-scan fallback | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_like_on_decimal_column_falls_back_and_filters` |
| pushdown-declined-filter-self-apply / An N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `type_screened_leg_filter_partition_is_total_and_fails_closed` |
| pushdown-declined-filter-self-apply / An N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where` |
| pushdown-planning-string-fn-type-coercion / INSTR and LOCATE coerce their first two arguments and decline beyond two | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `join_instr_beyond_two_args_declines_at_both_join_sites` |
| pushdown-planning-string-fn-type-coercion / INSTR and LOCATE coerce their first two arguments and decline beyond two | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_join_instr_with_start_position_returns_native_result` |
| pushdown-planning-decimal-string-format / WHERE-clause stringification of a DECIMAL column renders the trimmed form | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `join_decimal_stringification_renders_trimmed_at_both_join_sites` |
| pushdown-planning-decimal-string-format / WHERE-clause stringification of a DECIMAL column renders the trimmed form | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_join_decimal_stringification_matches_native_at_both_surfaces` |
| pushdown-planning-like-type-coercion / LIKE on a VARCHAR or CHAR column pushes down unchanged | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `join_like_over_varchar_side_column_pushes_down_unchanged` |

Unit tests are used where the scenario is a pure JSON-tree-to-SQL-string computation with no I/O —
the established convention for this module's golden-SQL and dispatch fixtures. Every scenario whose
claim is about the RESULT a live query returns additionally carries a Docker-Exasol E2E test, per
CLAUDE.md § "Verification discipline": a claimed SQL capability fix must be verified against a live
Exasol instance, not inferred from a rendered string.

One scenario is deliberately pinned at the partition level only — "Two N-scan sides sharing a column
name are each screened against their own side's types" — and its row-equality claim was removed rather
than E2E-backed. No seed table declares a column name shared with another at a different type (all
four use disjoint prefixes `C_`/`O_`/`L_`/`S_`), and the claim is about WHICH `col_types` slice the
screen consults, which is pure planning-time computation a unit test fully determines. The live
row-equality guarantee at the same surface is covered by
`e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where`, which exercises the identical
residual route with a non-colliding name. See `decision-log.md` § [11].

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| pushdown-planning-join-filter-type-coercion (broadcast decline) | `docker compose up -d --wait minio exasol iceberg-rest` then `make test-e2e` | `e2e_broadcast_like_on_decimal_column_falls_back_and_filters` passes; the captured pushed SQL contains `"LHS_T0"`/`"LHS_T1"` and does NOT contain `"join":{` |
| pushdown-planning-join-filter-type-coercion (broadcast DATE CAST) | `make test-e2e` | `e2e_broadcast_like_on_date_column_stays_broadcast_and_filters` passes; the captured pushed SQL DOES contain `"join":{` and a `CAST(` over `O_ORDERDATE` |
| pushdown-planning-join-fallback (N-scan residual) | `make test-e2e` | `e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where` passes; the LIKE appears in the wrapper's outer `WHERE`, table-qualified, and in no leg's scan spec |
| pushdown-planning-string-fn-type-coercion (#228 narrowing) | `make test-e2e` | `e2e_join_instr_with_start_position_returns_native_result` passes and returns Exasol's native row set, not the start-position-ignoring one |
| pushdown-planning-decimal-string-format (join trimmed form) | `cargo test -p lakehouse-engine join_decimal_stringification_renders_trimmed_at_both_join_sites` | 1 passed; the rendered fragment carries the trimmed `decimal_to_varchar_exasol` form at both join sites |
| pushdown-planning-decimal-string-format (live row equality) | `make test-e2e` | `e2e_join_decimal_stringification_matches_native_at_both_surfaces` passes; `LENGTH(<scale-2 DECIMAL col>) > n` returns the same rows through `VS_NAME` and `VS_NAME_LOW` as native Exasol evaluation — the pre-fix run returns a DIFFERENT row set, not an error |
| Regression: untriggered join filters unchanged | `cargo test -p lakehouse-engine golden_` | All golden-SQL fixtures pass with no fixture edits |

`make test-e2e` does NOT start the Docker stack — bring it up first, or every DB-backed test FAILS
(it never skips) and mimics a real regression. Check for a stray `bench/.env` before debugging a
seemingly hung run.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (host unit) | `cargo test` | 0 failures |
| Test (E2E, live Docker Exasol) | `docker compose up -d --wait minio exasol iceberg-rest` then `make test-e2e` | 0 failures; DB-backed tests run, not skipped |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| Spec validation | `speq feature validate` | Pass |

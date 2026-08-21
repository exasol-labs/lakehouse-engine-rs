# Plan: fix-single-group-scalar-over-aggregate

## Summary

An ungrouped aggregate wrapped in a scalar function — `ROUND(SUM(l_quantity), 2)` — is
currently pushed into the scan's per-shard projection instead of the partial/merge plan, so
every shard computes the whole aggregate over its own files and the query silently returns
one unmerged partial row per shard. This plan decomposes that shape into the same
partial/merge plan a bare aggregate already gets, and adds a depth-insensitive projection
guard so any nested aggregate the merge cannot decompose routes to the qualified wrapper
rather than being evaluated per shard. `Closes #194` and `Closes #188`.

## Design

### Context

Two reported bugs share one code path. Issue #194 is a silent wrong answer: `SELECT
ROUND(SUM(l_quantity), 2) FROM LINEITEM` returns `25304.00` in one row against a native
Exasol schema and four rows (`7477.00`, `7033.00`, `8018.00`, `2776.00`) through the
virtual schema, with no error raised. Issue #188 is a hard failure: `SELECT
ROUND(VARIANCE(c_acctbal), 4) FROM CUSTOMER` fails with `Error during planning: Invalid
function 'variance'` because DataFusion defines `var_samp` but not Exasol's `VARIANCE`
alias.

Both reach the same node. The nested aggregate is rendered into a per-shard DataFusion
query: #194's aggregate computes there (wrongly), #188's fails to plan there. Fix the
per-shard rendering and both close.

Issue #194's own "Decided approach" — decline in `parse_agg_item` and fall through to
`RequestShape::RowScan` — is superseded. That fallthrough is not a fix; it is the bug's own
mechanism. `detect_aggregates` requires every select-list item to be literally
`function_aggregate`, so a `function_scalar` item already declines today and
`classify_request_shape` already yields `RowScan`. `parse_agg_item` is never reached. The
`RowScan` arm routes to the qualified wrapper only when the derived projection is *widened*,
and it is not widened here: `project_columns`'s `function_scalar` arm calls
`render_expression_safe`, whose `function_aggregate` arm renders a nested aggregate verbatim
as SQL text — deliberately, because the grouped merge substitution depends on that arm. The
item therefore renders successfully as a `ProjectionItem::Expr` and reaches the per-shard
`EMITS` clause.

- **Goals** — one merged row for every ungrouped scalar-over-aggregate query; no aggregate
  function name crossing into DataFusion; a correctness floor that holds for every shape
  the merge cannot decompose, on every path that consumes the derived projection; one owner
  for the scalar-over-aggregate decomposition mechanism, shared with the grouped planner.
- **Non-Goals** — grouped scalar-over-aggregate (already shipped as
  `vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate`); metadata-only aggregate
  answering from Iceberg manifest statistics; new capability advertisement; broadcast-join
  aggregate pushdown (a nested aggregate over a join routes to the existing unaccelerated
  N-scan fallback, exactly as a top-level one does).

### Decision

Fix in two layers: a correctness floor that cannot be bypassed, and a performance path that
keeps the common shape off the floor.

**Layer 1 — the floor.** Probe every select-list item's whole subtree for a
`function_aggregate` before `project_columns` dispatches on node type, and widen the derived
projection when one is found. This is the single site all three consumers of that projection
read: the single-table row-scan routing, the broadcast-join eligibility check, and the
empty-result path. It subsumes the existing unknown-node arm's handling of a *top-level*
aggregate — that item widened before and widens now, byte-identically — and extends the rule
to every depth. Without this layer, every shape decomposition declines is silently wrong.

**Layer 2 — the performance path.** Classify a decomposable scalar-over-aggregate item as a
single-group aggregate item, mirroring what the grouped planner already does, so the
partial/merge plan is preserved.

The two layers cannot conflict: `build_dispatch_sql` reads the widening signal only inside
its `RequestShape::RowScan` arm, which `classify_request_shape` reaches only after both
aggregate tiers decline. A decomposable item is classified `SingleGroupAgg` and never
consults the signal.

#### Architecture

```
                      pushdown request selectList item
                                    │
              ┌─────────────────────┴──────────────────────┐
              │                                            │
   classify_request_shape                         project_columns
   (Tier 2: detect_aggregates)                    (Layer 1 floor)
              │                                            │
   ┌──────────┴───────────┐                    subtree contains
   │                      │                    function_aggregate?
 bare aggregate    scalar-over-aggregate                   │
   │                      │                          yes → widen
   │              scalar_over_agg  ◄── NEW module           │
   │              (classify / sentinelize /                 │
   │               fold / render-over-merge)                │
   │                      │            ▲                    │
   └──────────┬───────────┘            │                    │
              │                   grouped_agg               │
   SingleGroupAgg: partial/merge   (same owner)   RequestShape::RowScan
              │                                        + widened
              ▼                                             ▼
   one merged row                            qualified_single_table_
   (4 partial rows in, 1 out)                fallback_pushdown
                                             (Exasol aggregates natively)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Single owner for a repeated decision | `scalar_over_agg` submodule | The sentinelize / classify / fold / render-over-merge quartet now serves two planners; a second copy is exactly the back-door leakage that lets the two answers drift |
| Dependency inversion | `render_scalar_over_merge(node, plans, merged)` | The merged `PARTIAL_*` expressions come in as a parameter instead of the new module calling `grouped_agg::merge_select_items`, so the shared module never names either planner and no module cycle forms |
| Guard before dispatch, not per arm | `project_columns` subtree probe | One decision site instead of one guard per pushable node type; the rule then holds for arms added later without remembering to guard them |
| Derive, do not store | `ordinary_plans` folds nested aggregates; `single_group_plan_types` reads slots off that folded list | Keeps `detect_aggregates`'s public signature and `SingleGroupItem`'s existing variants intact, so no façade item is added and no external test caller has to name a new type |
| Caller owns select-list assembly | `build_scan_driving_sql` takes the ready-to-emit merge SELECT items | The SQL builder keeps owning fan-out and `EMITS` assembly and stops owning what the merge SELECT says, which now depends on select-list classification — the same split `build_grouped_aggregate_scan_sql` already has |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Full partial/merge decomposition **plus** the projection guard | Guard only, routing every scalar-over-aggregate to the qualified wrapper | The wrapper is correct but materializes the whole referenced-column scan output in Exasol temp-DB RAM. Adding `ROUND(...)` around a working `SUM` would take the query from four partial rows to every scanned row, and adding a `GROUP BY` would make it fast again — an incoherent performance model, since the grouped path already decomposes. The reusable primitives carry no GROUP BY state, so decomposition is mostly wiring |
| The guard ships regardless of the decomposition | Rely on decomposition alone | Decomposition declines on a `DISTINCT` inner aggregate, a statistical aggregate over an expression, a demoted non-numeric column, a residual bare column, and an unrenderable residual. Each of those falls to `RowScan`, where the bug lives. The guard is what makes "declines" mean "correct but slower" instead of "silently wrong" |
| Extract the quartet into a new `scalar_over_agg` submodule | Widen the four to `pub(super)` in place; move them into `support.rs` | In-place widening leaves the mechanism owned by one of its two consumers. `support.rs` is the established home for a cross-sibling primitive, but these four are one cohesive, nameable responsibility that reads as a module rather than as loose helpers. The submodule list is explicitly "a design decision recorded in the plan, not a normative contract" |
| Fold nested plans inside `ordinary_plans`, keeping `detect_aggregates`'s signature | Return a new `SingleGroupDetection` struct mirroring `GroupedAggregateDetection` | The struct is the closer mirror but has to be `pub` to appear in a `pub fn`'s return type, which changes the frozen façade and both surface probes. Folding inside `ordinary_plans` is behaviour-identical for every shape without a nested aggregate, so no existing caller changes |
| Deduplicate folded plans | Emit one plan per occurrence | Not an optimization. `render_scalar_over_merge` resolves each nested aggregate to the first structurally-equal slot, so `[Count, Sum, Count]` would bind the nested `COUNT(*)` to slot 0 while its `EMITS` column was declared at slot 2 |
| Fix #188 by routing through the existing `AggKind` tables | Add a name-alias map on the `vs-expression` `function_aggregate` arm | Aliasing at the translator would keep the aggregate executing per shard — it would turn #188's hard error into #194's silent wrong answer. Decomposition means no aggregate name reaches DataFusion at all |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate` | NEW | `vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate/spec.md` |
| `vs-adapter/pushdown-planning-selectlist-expressions` | CHANGED | `vs-adapter/pushdown-planning-selectlist-expressions/spec.md` |
| `vs-adapter/pushdown-planning-single-group-agg` | CHANGED | `vs-adapter/pushdown-planning-single-group-agg/spec.md` |

## Impact

An ungrouped aggregate wrapped in a scalar function stops returning wrong results. Queries
of the form `SELECT ROUND(SUM(col), 2) FROM t` currently return one row per scan shard,
each holding that shard's partial aggregate; after this change they return the one merged
row Exasol returns for the same query over a native schema. Any dashboard, report, or
saved query built on the current output changes value — that is the fix, not a regression,
and the old output was never correct.

`SELECT ROUND(VARIANCE(col), 4) FROM t` and the other scalar-wrapped statistical aliases
stop failing with `Invalid function 'variance'` and start returning a value.

No capability advertisement changes, so Exasol pushes exactly the shapes it pushes today.
No breaking change to any interface an operator configures. Shapes the merge cannot
decompose become slower rather than wrong: they route to the qualified wrapper, where
Exasol aggregates the returned rows, so the referenced columns cross the UDF boundary
instead of a handful of partial values.

## Dependencies

None external. Every primitive this plan reuses already exists in
`crates/lakehouse-engine/src/adapter/pushdown/`.

## Implementation Tasks

### 1. Correctness floor

- [ ] 1.1 Add a depth-insensitive `function_aggregate` subtree probe to `project_columns` (`adapter/pushdown/support.rs`), applied to each select-list item before the node-type match, setting `needs_full_fallback = true` on a hit. Verify the top-level-aggregate outcome is unchanged and that all eighteen `testdata/dispatch_golden/` fixtures stay byte-identical. [expert]
- [ ] 1.2 Add unit tests in `support_tests.rs` proving the widening fires for a nested aggregate under each pushable node family — `function_scalar`, `function_scalar_cast`, `function_scalar_case`, an arithmetic node, and a predicate node — and does NOT fire for a scalar item with no nested aggregate.
- [ ] 1.3 Add a `dispatch_golden` fixture for a single-table request whose select list is `ROUND(SUM(col), 2)` and which the decomposition declines (inner `DISTINCT`), asserting the qualified-wrapper SQL rather than a per-shard projection.

### 2. Shared decomposition owner

- [ ] 2.1 Create `adapter/pushdown/scalar_over_agg.rs` plus `scalar_over_agg_tests.rs`, moving `sentinelize_aggregates`, `sentinel_column_node`, `agg_sentinel_token`, `classify_scalar_over_aggregate`, `render_scalar_over_merge`, and `fold_aggregate_plan` out of `grouped_agg.rs` at `pub(super)` visibility. Change `render_scalar_over_merge` to take the merged `PARTIAL_*` expressions as a parameter instead of calling `merge_select_items`, so the new module names neither planner. Repoint `grouped_agg.rs` and register the submodule in `mod.rs`. [expert]
- [ ] 2.2 Prove the move changed no output: `grouped_aggregate.sql`, `grouped_all_agg_kinds.sql`, and `group_by_fallback.sql` must match byte-for-byte, and every existing grouped scalar-over-aggregate unit test must pass unedited.

### 3. Single-group decomposition

- [ ] 3.1 Add `SingleGroupItem::ScalarOverAggregate { select_index, node, declared_type }` and extend `detect_aggregates` to classify such an item via `scalar_over_agg::classify_scalar_over_aggregate`, declining the whole detection when it declines. Keep the `groupBy` gate, add no `aggregationType` check, and keep the function's signature. [expert]
- [ ] 3.2 Widen `ordinary_plans` to fold every nested aggregate into the returned list, deduplicated by `AggregatePlan` equality through `scalar_over_agg::fold_aggregate_plan`. Signature unchanged; output unchanged for every select list with no nested aggregate. [expert]
- [ ] 3.3 Add `single_group_plan_types(pushdown_req, items)`, aligned 1:1 with `ordinary_plans(items)`: each slot takes the `selectListDataTypes` entry of a top-level occurrence at its own ordinal, and the default otherwise. Derive the alignment from the folded plan list rather than re-running the fold.
- [ ] 3.4 Change `build_scan_driving_sql` / `build_aggregate_scan_sql` (`support.rs`) so the outer merge SELECT items arrive ready-to-emit from the caller and `aggregate_types` means per-plan `EMITS` types. Assemble the merge SELECT in `mod.rs`'s `SingleGroupAgg` arm: a bare aggregate through the existing cast-merge helper, a scalar-over-aggregate item through `render_scalar_over_merge`, each cast to its own item's declared type, in `selectList` order. [expert]
- [ ] 3.5 Add the `ScalarOverAggregate` arm to `empty_agg_sql` (`empty_result.rs`): the item's zero-row value cast to its own declared type through the shared declared-type CAST helper, keeping the one-row single-group empty shape.
- [ ] 3.6 Update the exhaustive-match and helper sites the new variant reaches — `single_group_agg_tests.rs`'s `agg_of`/`distinct_of` helpers, `empty_result.rs`, `support.rs`'s distinct-fan-out builders — and the external callers of the changed `build_scan_driving_sql` signature: `tests/scan_plan_shape.rs`. Full census in *Call-Site Census* below.

### 4. Verification

- [ ] 4.1 `dispatch_golden` fixtures for the new decomposed shapes: a lone `ROUND(SUM(col), 2)`, a deduplicating `COUNT(*)` + `ROUND(SUM(col) / COUNT(*), 2)`, an interleaved list, a scalar-wrapped `VARIANCE`, and the empty-result one-row shape.
- [ ] 4.2 Unit tests per new-feature scenario in `single_group_agg_tests.rs`, `scalar_over_agg_tests.rs`, `empty_result_tests.rs`, and `request_shape_tests.rs`.
- [ ] 4.3 E2E: the issue #194 repro (`ROUND(SUM(l_quantity), 2)` over TPC-H `LINEITEM`) and the issue #188 repro (`ROUND(VARIANCE(c_acctbal), 4)` over `CUSTOMER`), each asserted against the native-schema oracle, plus regression assertions that bare `VARIANCE` and `ROUND(VAR_SAMP(...), 4)` still return their current values.
- [ ] 4.4 E2E for the floor: a scalar-wrapped aggregate over a join (the shape `carries_aggregation_clause` does not recognise), and a scalar-wrapped `COUNT(DISTINCT)` — both must return the single-node result, not a per-shard row set.
- [ ] 4.5 `EXPLAIN VIRTUAL` assertion that the #194 query now emits a non-empty `"aggregates"` and `"projection":[]`, and no `"expr"` containing an aggregate.

## Call-Site Census

Handed over as a checklist because a signature or variant change here fans out across the
crate and the host `cargo test` does not compile the e2e test crates, so an omission
surfaces only at the e2e gate.

**`SingleGroupItem` — new variant reaches every exhaustive match:**
`single_group_agg.rs` (`ordinary_plans`; `has_distinct` and `is_lone_count_distinct` use
`matches!` and need no arm), `empty_result.rs:139` (`empty_agg_sql`), `support.rs:278` and
`support.rs:327` (irrefutable `let … else` on a lone `Distinct`, no arm needed — confirm),
`request_shape.rs:79` (`RequestShape::SingleGroupAgg` field type, unchanged),
`single_group_agg_tests.rs:8` (`agg_of`) and `:16` (`distinct_of`) — two `panic!` arms each,
now three.

**`build_scan_driving_sql` — signature change, `pub` façade item:**
`mod.rs` (the one production caller), `tests/scan_plan_shape.rs:23` and `:423-427`
(external crate — will NOT compile under host `cargo test` alone),
`tests/pushdown_public_surface.rs:22` and `src/adapter/pushdown_surface_probe_tests.rs:24`
(`use` lists only — no edit needed as long as the NAME survives). Do not put
`SingleGroupItem` in this function's signature: it is `pub` in its own module but not
re-exported on the façade, so an external caller could not name it.

**`ordinary_plans` — behaviour widens, signature unchanged:**
`request_shape.rs:161` (numeric gate — now gates nested plans too, which is intended),
`mod.rs:595`, `topn_tests.rs:44-45`, `single_group_agg_tests.rs:296` and `:384`,
`tests/scan_plan_shape.rs:427`.

**`detect_aggregates` — signature unchanged, accepted shapes widen:**
`request_shape.rs:160`, `topn_tests.rs:43`, `empty_result_tests.rs:160`,
`support_tests.rs:1623`, `grouped_agg_tests.rs:188` (asserts `is_none()` for a grouped
request — must stay `None`), `single_group_agg_tests.rs` (many; several assert `is_none()`
for shapes that must STILL decline), `tests/scan_plan_shape.rs:423`.

**Moved out of `grouped_agg.rs`:** `sentinelize_aggregates`, `sentinel_column_node`,
`agg_sentinel_token`, `classify_scalar_over_aggregate`, `render_scalar_over_merge`,
`fold_aggregate_plan` — callers are `detect_group_by_aggregates`,
`build_grouped_aggregate_scan_sql`, `render_having_over_merge`, and the grouped tests.

**Façade:** during the review-fix pass, `AggregateMergeInputs` was added to the façade —
`build_scan_driving_sql`'s three aggregate-only parameters (`aggregate_types`, `merge_select`,
`request_limit`) collapsed into this one type to make an empty merge SELECT unrepresentable.
This IS a façade change, so `vs-adapter/pushdown-module-structure` needed a delta; the review
fix authored one at
`specs/_plans/fix-single-group-scalar-over-aggregate/vs-adapter/pushdown-module-structure/spec.md`,
and both frozen surface-probe doc comments/counts were updated to match. The new submodule
still needs no delta — that feature records the submodule list as "a design decision recorded
in the plan, not a normative contract".

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3 |
| Group B | 2.1, 2.2 |
| Group C | 3.1, 3.2, 3.3 |
| Group D | 3.4, 3.5 |
| Group E | 3.6 |
| Group F | 4.1, 4.2 |
| Group G | 4.3, 4.4, 4.5 |

Sequential dependencies:
- Group A and Group B are independent of each other and may run concurrently.
- Group B → Group C (the single-group classifier calls the relocated primitives).
- Group C → Group D (the merge assembly consumes the folded plan and type lists).
- Group D → Group E → Group F → Group G.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `aggregate_exasol_types` (`adapter/pushdown/support.rs`) | Its `function_aggregate` filter shifts every index after a skipped scalar item, so it can serve neither the per-plan `EMITS` types nor the per-item outer CAST once a scalar-over-aggregate item exists. Delete it once tasks 3.3 and 3.4 repoint its callers (`mod.rs`, `empty_result.rs`); if a caller genuinely still needs the filtered shape, keep it and record why |
| Function | `sentinelize_aggregates`, `sentinel_column_node`, `agg_sentinel_token`, `classify_scalar_over_aggregate`, `render_scalar_over_merge`, `fold_aggregate_plan` in `grouped_agg.rs` | Relocated to `scalar_over_agg.rs` by task 2.1 — the originals must be removed, not left as pass-throughs |
| Test | Any `single_group_agg_tests.rs` assertion that a scalar-wrapped aggregate select list yields `detect_aggregates(...) == None` | Such a test would encode the pre-fix behaviour. Check for one; if it exists, it changes rather than is deleted |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Single-group select item that is a scalar function wrapping aggregates is decomposed into partial columns and one merged row | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_aggregate_round_sum_matches_native_oracle` |
| Single-group select item that is a scalar function wrapping aggregates is decomposed into partial columns and one merged row | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `single_group_scalar_over_aggregate_matches_golden` |
| A scalar-wrapped statistical aggregate resolves through the shared AggKind tables, so no aggregate function name reaches DataFusion | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_variance_matches_native_oracle` |
| A scalar-wrapped statistical aggregate resolves through the shared AggKind tables, so no aggregate function name reaches DataFusion | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `single_group_scalar_over_variance_matches_golden` |
| Inner aggregates shared across the single-group select list collapse into one deduplicated partial column | Unit | `crates/lakehouse-engine/src/adapter/pushdown/single_group_agg_tests.rs` | `single_group_scalar_over_aggregate_dedups_shared_inner_aggregates` |
| Inner aggregates shared across the single-group select list collapse into one deduplicated partial column | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_aggregate_shared_count_matches_native_oracle` |
| Scalar-over-aggregate and plain aggregate items interleave in select-list order with per-item declared types | Unit | `crates/lakehouse-engine/src/adapter/pushdown/single_group_agg_tests.rs` | `single_group_scalar_over_aggregate_preserves_selectlist_order_and_item_types` |
| Scalar-over-aggregate and plain aggregate items interleave in select-list order with per-item declared types | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_aggregate_interleaved_matches_native_oracle` |
| A nested aggregate the merge cannot decompose widens the projection instead of being evaluated per shard | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `project_columns_widens_on_nested_aggregate_under_every_pushable_arm` |
| A nested aggregate the merge cannot decompose widens the projection instead of being evaluated per shard | Integration | `crates/lakehouse-engine/tests/e2e_count_distinct_test.rs` | `e2e_scalar_wrapped_count_distinct_routes_to_wrapper_and_matches_native_oracle` |
| A nested aggregate the merge cannot decompose widens the projection instead of being evaluated per shard (broadcast-join path) | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_scalar_over_aggregate_ungrouped_join_matches_native_oracle` |
| A fully-pruned file list yields one shape-correct empty row for a scalar-over-aggregate select list | Unit | `crates/lakehouse-engine/src/adapter/pushdown/empty_result_tests.rs` | `empty_single_group_scalar_over_aggregate_emits_one_typed_row` |
| A fully-pruned file list yields one shape-correct empty row for a scalar-over-aggregate select list | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_aggregate_all_files_pruned_returns_one_row` |
| The scalar-over-aggregate decomposition mechanism has ONE owner shared by both aggregate planners | Unit | `crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg_tests.rs` | `scalar_over_agg_primitives_serve_both_planners_with_no_planner_dependency` |
| The scalar-over-aggregate decomposition mechanism has ONE owner shared by both aggregate planners | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `grouped_aggregate_matches_golden` (existing, must stay green and unedited) |
| Aggregate query is translated into a partial-aggregate scan spec (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/single_group_agg_tests.rs` | `detect_aggregates_accepts_scalar_over_aggregate_and_still_declines_undecomposable` |
| Single-group aggregate scan spec leaves the projection field empty (CHANGED) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_single_group_scalar_over_aggregate_explain_virtual_shows_empty_projection` |
| Scalar select-list expression is pushed into the scan-driving query (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `project_columns_top_level_aggregate_widening_is_unchanged_by_the_subtree_probe` |
| A widened derived projection routes to a native wrapper on every path (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `nested_aggregate_decline_matches_qualified_wrapper_golden` |

Unit tests are used only where the scenario is pure SQL-string or classification computation
with no I/O: golden-fixture SQL assembly, select-list classification, and the projection
guard. Every behavioural claim about returned ROWS is an integration test against a live
Exasol container with a native-schema oracle.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| `pushdown-planning-single-group-agg-scalar-over-aggregate` (#194) | `exapump -c "$DSN" sql "SELECT ROUND(SUM(l_quantity), 2) FROM DBX.LINEITEM"` | Exactly ONE row, value equal to `SELECT ROUND(SUM(l_quantity), 2) FROM TEST.LINEITEM` (`25304.00` on the seeded fixture) — not four rows |
| `pushdown-planning-single-group-agg-scalar-over-aggregate` (#188) | `exapump -c "$DSN" sql "SELECT ROUND(VARIANCE(c_acctbal), 4) FROM DBX.CUSTOMER"` | One row with a numeric value equal to the same query against `TEST.CUSTOMER`; no `Invalid function 'variance'` error |
| `pushdown-planning-single-group-agg-scalar-over-aggregate` (plan shape) | `exapump -c "$DSN" sql "EXPLAIN VIRTUAL SELECT ROUND(SUM(l_quantity), 2) FROM DBX.LINEITEM"` | The `LAKEHOUSE_SCAN` common spec shows a non-empty `"aggregates"` and `"projection":[]`; no `"expr"` containing `SUM(` |
| `pushdown-planning-selectlist-expressions` (the floor) | `exapump -c "$DSN" sql "SELECT ROUND(COUNT(DISTINCT l_orderkey), 2) FROM DBX.LINEITEM"` | One row equal to the same query against `TEST.LINEITEM`; `EXPLAIN VIRTUAL` shows the qualified wrapper, not an aggregate inside a per-shard projection |
| `pushdown-planning-single-group-agg` (no regression) | `exapump -c "$DSN" sql "SELECT SUM(l_quantity), VARIANCE(l_quantity) FROM DBX.LINEITEM"` | Unchanged from before this plan: one row, values equal to the native-schema oracle |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `docker compose up -d` (the stack the target does NOT start), then `make test-e2e` | 0 failures; a DB-backed test must FAIL, never skip, without the stack |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |

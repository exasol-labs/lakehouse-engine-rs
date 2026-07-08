# Plan: fix-scalar-over-aggregate-grouped-pushdown

## Summary

Fix issue #82: a single-table grouped query whose select list contains a scalar
function wrapping aggregates (e.g. `ROUND(100.0 * SUM(CASE …) / COUNT(*), 2)`)
hard-fails through the Virtual Schema with a `04000` pushdown column-count mismatch —
by teaching the single-table grouped partial/merge path to decompose the item's inner
aggregates into partial columns and render the scalar wrapper over the merged partials
(`Closes #82`), with a qualified single-table wrapper fallback replacing the broken
bare row-scan for any residual undecomposable grouped shape.

## Design

### Context

Issue #82 is the single-table analogue of the join-path class PR #78 / ADR-096 already
fixed for joins. The join fix (`fix-join-decline-hard-fail`) explicitly listed as a
**Non-Goal**: "No change to the single-table partial/merge aggregate decomposition
paths … those detect a top-level `function_aggregate` *before* recursing and remain
behavior-compatible." That is exactly the gap #82 lives in.

The single-table grouped path is a *separate* code path from the join fallback. Its
detector, `detect_group_by_aggregates` (`pushdown.rs:837`), classifies each `selectList`
item as one of: a top-level `function_aggregate` (→ `AggregatePlan`), a bare literal
(→ `Constant`), or a group-key projection (a plain `column` or a scalar rendering to a
group key). Anything else — including a `function_scalar`/arithmetic node wrapping
aggregates — makes the whole detection return `None`. The request then falls through
single-group detection (also `None`) to `build_scan_driving_sql` with `aggregates:
None` — a **bare raw full-row scan**. For a `group_by` request Exasol expects the
pushdown query to return exactly the `selectList` columns; a raw scan returns the
projected source columns instead → SQL state `04000` "Expected number of columns is 5
but pushdown query has 6", a hard client-facing error with no native re-plan.

Two facts make the correct fix a natural, low-risk extension rather than new
machinery:

1. **The rendering seam already exists.** PR #78 taught `crates/vs-expression`'s
   `render_expression_inner` a `function_aggregate` arm (`lib.rs:736`) that splices the
   aggregate name verbatim and recurses into arguments — so a scalar-over-aggregate
   *renders* today. What is missing is (a) single-table grouped *detection* accepting
   it, and (b) the partial/merge *decomposition* of its inner aggregates.
2. **The merge-rewrite machinery already exists.** `render_having_over_merge`
   (`pushdown.rs:1799`) already renders a node tree over the merge decomposition,
   rewriting each `function_aggregate` to its merged expression (`SUM(x)` →
   `SUM("PARTIAL_sum_0")`) matched to the `AggregatePlan` list — used today for HAVING.
   Its one gap: `render_having_operand`'s catch-all (`pushdown.rs:1900`) delegates a
   *scalar function wrapping an aggregate* whole-subtree to `render_expression`, which
   renders the nested aggregate **verbatim over source columns** (absent from the
   outer wrapper). Descending scalars/arithmetic to rewrite *every* nested aggregate to
   its merged partial form closes that gap and serves both HAVING and the select list.

A scalar-over-aggregate in a grouped select list is structurally the same problem as
an aggregate in a HAVING clause: render the surrounding scalar/arithmetic structure
while rewriting each aggregate leaf to its merged `PARTIAL_*` expression.

- **Goals**
  - The single-table grouped path pushes down a scalar-over-aggregate select item:
    inner aggregates decompose into the existing partial `AggregatePlan` machinery
    (node-local partial aggregate → Exasol merge), the scalar wrapper renders over the
    merged partials in the outer wrapper at the item's ordinal, cast to its declared
    type — so #82's query returns results instead of `04000`.
  - Inner aggregates equal by kind + argument collapse to one shared `PARTIAL_*`
    column across all select items (a `COUNT(*)` used bare and inside `ROUND` is one
    partial column).
  - Select-list order and positional type validation hold for any interleaving of
    keys, plain aggregates, and scalar-over-aggregate items.
  - The grouped **fallback** never emits a column-count-mismatched bare row scan:
    an undecomposable grouped shape falls back to a qualified single-table wrapper that
    renders the exact grouped select list over a materialized sharded raw scan.
- **Non-Goals**
  - No change to the UDF scan side: the inner aggregates are ordinary `AggregatePlan`s;
    the scan UDF already emits one partial row per group carrying N group keys + M
    partial aggregate values for arbitrary M (`datafusion-scan/scan-execution-grouped-agg`).
  - No change to `crates/vs-expression`: the verbatim `function_aggregate` rendering it
    gained in PR #78 is reused as-is (for the fallback path); the merge-rewrite lives in
    the adapter.
  - No new advertised capabilities.
  - The **no-GROUP-BY single-group** scalar-over-aggregate (e.g. `SELECT ROUND(SUM(x)/
    COUNT(*), 2) FROM t`) is the sibling of #82 on the single-group path; it is out of
    scope for this issue and tracked separately (see decision log [5]).

### Decision

Extend the single-table grouped partial/merge path in three coordinated adapter-only
changes; the UDF and vs-expression are untouched.

#### Architecture

```
handle_pushdown (group_by request)
  └─ detect_group_by_aggregates
        selectList item →
          ├─ function_aggregate                → AggregatePlan (top-level)   [today]
          ├─ literal                           → Constant                    [today]
          ├─ group-key projection              → GroupKey                    [today]
          ├─ scalar/arithmetic wrapping        → ScalarOverAggregate     ★ NEW
          │     function_aggregate(s)             + inner aggregates folded
          │                                        into the AggregatePlan list
          │                                        (deduplicated by kind+arg)
          └─ else / undecomposable inner       → None → QUALIFIED SINGLE-   ★ CHANGED
                                                   TABLE WRAPPER fallback
                                                   (was: bare row scan → 04000)

  build_grouped_aggregate_scan_sql (outer wrapper)
    per select item, at its selectList ordinal:
      GroupKey            → GK_* cast                                        [today]
      Aggregate           → merged PARTIAL_* expr, cast                      [today]
      ScalarOverAggregate → scalar wrapper with each nested aggregate    ★ NEW
                            rewritten to its merged PARTIAL_* expr, cast
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Fold inner aggregates into the existing `AggregatePlan` list | `detect_group_by_aggregates` | Reuses the whole partial/merge decomposition + scan-UDF layout unchanged; the UDF just sees more aggregate plans |
| Descend scalars/arithmetic, rewrite each aggregate leaf to merged form | generalized `render_having_over_merge` | The select-list scalar-over-aggregate is the same problem as a HAVING aggregate; one renderer serves both |
| Deduplicate aggregates by `AggregatePlan` equality (kind + argument) | detection fold | `render_having_over_merge` already matches aggregates by `AggregatePlan` equality; a shared `COUNT(*)` becomes one partial column |
| CAST the wrapper item to `selectListDataTypes[ordinal]` | outer wrapper builder | Passes Exasol positional pushdown-column-type validation |
| Qualified single-table wrapper, never a bare row scan, for a grouped decline | grouped fallback | Bare row scan returns the wrong column count (`04000`); the wrapper returns exactly the `selectList` columns, mirroring the join fallback (N=1) |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| (a) Full push-down: decompose inner aggregates into partials + render scalar wrapper over merged partials | (b) Route the whole grouped scalar-over-aggregate through the qualified single-table wrapper (Exasol aggregates over raw rows) | (a) is consistent with the *existing grouped partial/merge architecture* (reuses `AggregatePlan` decomposition, the scan-UDF layout, and `render_having_over_merge`) AND keeps node-local aggregation — the mission's "node-local aggregation keeps network transfer small". (b) would ship every matching row per group to Exasol, defeating decomposition even for the plain aggregates in the same query. (b) is retained only as the residual-shape safety net. |
| Reuse & generalize `render_having_over_merge` to descend scalars | A new independent select-list merge renderer | The HAVING renderer already does aggregate→merged rewrite matched by `AggregatePlan` equality; the only gap is scalar/arithmetic descent, which also fixes scalar-over-aggregate in HAVING for free |
| No UDF-side change | Emit a distinct partial layout for scalar-over-aggregate | The inner aggregates are ordinary `AggregatePlan`s; the grouped scan already emits N keys + M partials for arbitrary M — keeps the VS thin and the `.so` untouched |
| Grouped fallback → qualified single-table wrapper | Keep the bare row-scan fallback; or hard-error the decline | Bare row scan is the `04000` bug; a hard error on a common shape is a client-facing failure with no native retry. The wrapper always returns the correct column count/types |
| No `crates/vs-expression` delta | Add a merge-aware arm to vs-expression | Merge-rewrite knows about `PARTIAL_*` columns and the merge UDF — adapter-local concern; vs-expression's verbatim aggregate arm is reused unchanged for the fallback |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-grouped-agg | CHANGED | `vs-adapter/pushdown-planning-grouped-agg/spec.md` |

- **pushdown-planning-grouped-agg** delta: NEW "Single-table grouped select item that
  is a scalar function wrapping aggregates is pushed down"; NEW "Nested aggregates are
  rewritten to their merged partial expressions, never rendered over source columns";
  NEW "Inner aggregates shared across the grouped select list decompose into
  deduplicated partial columns"; NEW "Scalar-over-aggregate items interleaved with keys
  and plain aggregates preserve select-list order"; CHANGED "Adapter falls back to row
  scan for unsupported grouped aggregate shape" → "…to a qualified single-table wrapper
  …" (never a column-count-mismatched bare row scan); CHANGED Background bullet for
  scalar-over-aggregate decomposition and the fallback.

## Dependencies

None new. Reuses `detect_group_by_aggregates`, `GroupedSelectItem`,
`build_grouped_aggregate_scan_sql`, `render_having_over_merge` /
`render_having_operand`, `merge_select_items` / `cast_merge_items`, `parse_agg_item`,
`AggregatePlan` equality, `validate_agg_col_types`, `build_scan_driving_sql` (for the
fallback's inner raw fan-out), and `crates/vs-expression`'s `function_aggregate` arm
(for the fallback's verbatim rendering).

## Implementation Tasks

1. Detection — accept and decompose a scalar-over-aggregate grouped select item
   - [ ] 1.1 Add a `ScalarOverAggregate { select_index, node }` variant to
     `GroupedSelectItem`; in `detect_group_by_aggregates` (`pushdown.rs:837`), when a
     select item is neither aggregate/literal/group-key, walk it for nested
     `function_aggregate` nodes and classify it as `ScalarOverAggregate` when it
     contains at least one (and every other leaf is a group key, group-key-derived
     expression, or literal renderable by `vs-expression`). [expert]
   - [ ] 1.2 Fold each nested aggregate into the shared `AggregatePlan` list via
     `parse_agg_item`, deduplicating by `AggregatePlan` equality (kind + argument) so a
     `COUNT(*)` used bare and inside a scalar becomes one `PARTIAL_*` column; record,
     per `ScalarOverAggregate` item, the mapping needed to rewrite its nested
     aggregates to the shared plans. Decline (→ fallback, task 3) if any nested
     aggregate is `DISTINCT`, targets a non-numeric type, or has an untranslatable
     argument. [expert]
2. Outer wrapper — render the scalar wrapper over merged partials
   - [ ] 2.1 Generalize `render_having_operand` (`pushdown.rs:1876`) so a
     `function_scalar` / arithmetic node recurses into a merge-aware renderer that
     rewrites *every* nested `function_aggregate` to its merged `PARTIAL_*` expression
     (matched to `plans` by `AggregatePlan` equality), preserving the scalar/arithmetic
     structure — instead of delegating the whole subtree to `render_expression`
     (which renders aggregates verbatim over absent source columns). This also fixes a
     scalar-over-aggregate inside a HAVING. [expert]
   - [ ] 2.2 In `build_grouped_aggregate_scan_sql` (`pushdown.rs:1423`), render each
     `ScalarOverAggregate` select item at its `select_index` ordinal using the task-2.1
     renderer, wrapped in `CAST(... AS <selectListDataTypes[select_index]>)`; extend the
     outer SELECT/cast/GROUP BY assembly to interleave it with `GroupKey` and
     `Aggregate` items in `selectList` order. [expert]
3. Fallback — qualified single-table wrapper, never a bare row scan
   - [ ] 3.1 When the grouped path declines (undecomposable item, per task 1.2), route
     the request to a qualified single-table wrapper: `SELECT <grouped select list
     rendered via vs-expression, aggregates verbatim> FROM (<`build_scan_driving_sql`
     raw sharded fan-out>) GROUP BY <keys> HAVING <…> ORDER BY <…> LIMIT <n>`, so the
     pushdown query returns exactly the `selectList` columns and Exasol aggregates over
     the returned rows. Replace the grouped path's fall-through to the bare row-scan
     `build_scan_driving_sql`. [expert]
   - [ ] 3.2 Ensure the fallback carries the group keys, HAVING, ORDER BY, and LIMIT
     into the outer wrapper (not the per-shard common blob — the per-shard scan stays
     LIMIT-free), and emits the shape-correct empty result when the file list is empty.
4. Host unit tests (`crates/lakehouse-engine/src/adapter/pushdown.rs` `#[cfg(test)]`)
   - [ ] 4.1 `detect_group_by_aggregates` over #82's exact select list classifies the
     `ROUND(… SUM(CASE …)/COUNT(*) …)` item as `ScalarOverAggregate` and folds its inner
     `SUM(CASE …)` and `COUNT(*)` into the plan list (deduplicated against a bare
     `COUNT(*)` select item).
   - [ ] 4.2 `build_grouped_aggregate_scan_sql` for #82's request emits an outer wrapper
     whose scalar-over-aggregate column is the `ROUND(… SUM("PARTIAL_*") / SUM("PARTIAL_*") …)`
     merged form (no source column reference), cast to the declared type, at the correct
     ordinal; the column count equals the `selectList` length.
   - [ ] 4.3 Interleaving unit test: a scalar-over-aggregate item placed before / between
     / after keys and plain aggregates yields outer SELECT items in `selectList` order,
     each cast from `selectListDataTypes` at its own ordinal.
   - [ ] 4.4 Fallback unit test: a grouped request whose scalar-over-aggregate wraps a
     `COUNT(DISTINCT …)` (undecomposable) emits the qualified single-table wrapper
     (`SELECT … FROM (…) GROUP BY …`) with `selectList`-matching column count, NOT a bare
     `SELECT * FROM (…)` row scan.
5. E2E (`crates/lakehouse-engine/tests/e2e_scan_test.rs`, local Exasol Docker)
   - [ ] 5.1 Add a Phase-5 GROUP BY E2E: #82's query
     (`SELECT L_RETURNFLAG, SUM(L_QUANTITY), AVG(L_EXTENDEDPRICE), ROUND(100.0 *
     SUM(CASE WHEN L_RETURNFLAG='R' THEN 1 ELSE 0 END)/COUNT(*), 2) FROM <vs>.FACT_LINEITEM
     GROUP BY L_RETURNFLAG`) runs green through the VS and matches the native-table
     ground truth (this query `04000`s before the fix); assert via `assert_group_by_pushed_down`
     that it pushes down (merged wrapper, no `SELECT * FROM (…)` row-scan wrapper).
   - [ ] 5.2 Add a shared-inner-aggregate E2E (a query with a bare `COUNT(*)` and a
     `ROUND(…/COUNT(*)…)`) asserting one merged partial column and correct results.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Branch | grouped path fall-through to bare row-scan `build_scan_driving_sql` (`pushdown.rs` ~2291) | Replaced by the qualified single-table wrapper fallback (task 3.1); the bare row scan is the `04000` bug for grouped requests |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Single-table grouped select item that is a scalar function wrapping aggregates is pushed down | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_scalar_over_aggregate_round` |
| Nested aggregates are rewritten to their merged partial expressions, never rendered over source columns | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_scalar_over_aggregate_renders_merged_partials` |
| Inner aggregates shared across the grouped select list decompose into deduplicated partial columns | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_group_by_shared_inner_aggregate_dedup` |
| Scalar-over-aggregate items interleaved with keys and plain aggregates preserve select-list order | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_scalar_over_aggregate_preserves_selectlist_order` |
| Adapter falls back to a qualified single-table wrapper for an undecomposable grouped aggregate shape | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `grouped_undecomposable_falls_back_to_qualified_wrapper` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| pushdown-planning-grouped-agg | `EXPLAIN VIRTUAL SELECT L_RETURNFLAG, SUM(L_QUANTITY), AVG(L_EXTENDEDPRICE), ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG='R' THEN 1 ELSE 0 END)/COUNT(*), 2) FROM <vs>.FACT_LINEITEM GROUP BY L_RETURNFLAG;` (via `exapump` against local Exasol Docker) | Pushed SQL is the merged outer wrapper (`ROUND(… SUM("PARTIAL_*") / SUM("PARTIAL_*") …)`, `GROUP BY "GK_0"`), no `SELECT * FROM (…)` row-scan wrapper, no `04000` |
| pushdown-planning-grouped-agg | Run the same query (not EXPLAIN) through the VS and compare to the same query over a native Exasol copy of `FACT_LINEITEM` | Identical per-group rows |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test (host) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures (fails, not skips, without DB) |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |

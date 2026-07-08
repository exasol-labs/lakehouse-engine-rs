# Decision Log: fix-scalar-over-aggregate-grouped-pushdown

Date: 2026-07-08

## Interview

This plan was initiated headless from GitHub issue #82, not a live interview. The
issue text is the authoritative intent. No human was available mid-planning; per the
headless escalation bar, conventional decisions were made and documented below, and
nothing rose to the irreducible-decision threshold requiring escalation.

**Q (from the orchestrator's key design question):** Should the fix (a) fully push
down the scalar-over-aggregate by decomposing inner aggregates into partials and
rendering the scalar wrapper in the outer merge (join path's approach, best perf, most
work), or (b) route the single-table grouped scalar-over-aggregate through a qualified
single-table wrapper over the scan analogous to the join fallback?

**A (assumed & documented — decision [1]):** (a) as the primary path, with (b) as the
residual-shape safety net. (a) is the most consistent with the *existing grouped
partial/merge architecture* and preserves node-local aggregation (the mission's
minimal-network-transfer requirement); the merge-rewrite machinery it needs already
exists (`render_having_over_merge`) and needs only generalization. (b) is retained
strictly for undecomposable shapes so a grouped decline never emits a
column-count-mismatched bare row scan.

## Design Decisions

### [1] Full push-down (decompose inner aggregates) as primary; qualified wrapper as fallback

- **Decision:** Push down a scalar-over-aggregate grouped select item by folding its
  inner aggregates into the existing partial `AggregatePlan` decomposition and rendering
  the scalar wrapper over the merged partials in the outer wrapper. Fall back to a
  qualified single-table wrapper (Exasol aggregates over a materialized sharded raw
  scan) only when an inner aggregate is genuinely undecomposable.
- **Alternatives:** Route the whole grouped scalar-over-aggregate through the qualified
  wrapper (approach b) unconditionally — simpler, but ships every matching row per group
  to Exasol, defeating node-local aggregation even for the plain aggregates in the same
  query; contradicts the mission's minimal-transfer requirement.
- **Rationale:** (a) reuses the `AggregatePlan` decomposition, the scan-UDF grouped
  partial layout, and the merge-rewrite renderer — maximal consistency with the shipped
  grouped architecture and best performance. (b) alone regresses performance; (b) as a
  narrow safety net satisfies the "never a `04000` bare row scan" correctness rule.
- **Promotes to ADR:** yes

### [2] Reuse and generalize `render_having_over_merge` for select-list merge rendering

- **Decision:** Generalize the existing HAVING merge-rewrite renderer so scalar/
  arithmetic operand nodes recurse and rewrite *every* nested `function_aggregate` to
  its merged `PARTIAL_*` expression, instead of delegating a whole scalar subtree to
  `render_expression` (which renders aggregates verbatim over absent source columns).
  The same renderer serves both the grouped select list and HAVING.
- **Alternatives:** A new, independent select-list merge renderer parallel to the HAVING
  one — duplicates the aggregate→merged rewrite and risks drift (the exact failure mode
  PR #78 diagnosed for the two-copy join renderers).
- **Rationale:** A scalar-over-aggregate select item is structurally identical to an
  aggregate-bearing HAVING operand; one renderer avoids divergence and also fixes a
  scalar-over-aggregate inside HAVING as a side effect.
- **Promotes to ADR:** yes

### [3] No UDF-side or vs-expression change

- **Decision:** Keep the fix adapter-only. Inner aggregates are ordinary
  `AggregatePlan`s (the grouped scan UDF already emits N keys + M partials for arbitrary
  M); the merge-rewrite lives in the adapter; vs-expression's verbatim `function_aggregate`
  arm (added in PR #78) is reused unchanged for the fallback path.
- **Alternatives:** Emit a distinct partial layout for scalar-over-aggregate (UDF
  change) or add a merge-aware arm to vs-expression.
- **Rationale:** Keeps the VS thin and the `.so` untouched; merge-rewrite is inherently
  adapter-local (it knows about `PARTIAL_*` columns and the merge UDF).
- **Promotes to ADR:** no

### [4] Deduplicate inner aggregates by `AggregatePlan` equality

- **Decision:** Aggregates equal by kind + argument collapse to one shared `PARTIAL_*`
  column across the whole select list (a `COUNT(*)` used bare and inside a scalar is one
  partial column); every occurrence renders to the same merged expression.
- **Alternatives:** One partial column per textual occurrence — wastes wire bytes and a
  partial column per duplicate, and `render_having_over_merge` already matches by
  `AggregatePlan` equality, so a non-deduplicated list would need a different match key.
- **Rationale:** Matches the existing equality-based merge match; minimizes partial
  columns and network transfer.
- **Promotes to ADR:** no

### [5] Scope limited to the GROUP BY (grouped) path; single-group sibling deferred

- **Decision:** This plan fixes only the grouped (`aggregationType: "group_by"`) path,
  matching issue #82's exact scope. The no-GROUP-BY single-group scalar-over-aggregate
  (e.g. `SELECT ROUND(SUM(x)/COUNT(*), 2) FROM t`) declines on the `detect_aggregates`
  single-group path via the same class of gap and is left for a separate follow-up.
- **Alternatives:** Fix both paths in one plan.
- **Rationale:** Respects the issue's scope in headless mode; the single-group fix is a
  distinct code path (`detect_aggregates`, single-group merge) and would broaden scope
  without a tracking issue. Flagged here so a follow-up issue can be filed.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->

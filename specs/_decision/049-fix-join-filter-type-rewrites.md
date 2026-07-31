# Decisions: fix-join-filter-type-rewrites

## ADR: Wire the full type-rewrite pipeline into both join WHERE-filter sites, not only the LIKE guard

**ID:** wire-full-type-rewrite-pipeline-into-join-where-filter-sites
**Plan:** fix-join-filter-type-rewrites
**Status:** Accepted

### Context

`render_broadcast_join`'s combined WHERE filter and the N-scan fallback's per-leg WHERE filter
called `render_df_filter_safe` directly, with no column-type awareness at all. The single-table
path instead runs `apply_type_rewrites`, an ordered three-guard pipeline: `like_subject_type_guard`
(#207) → `string_function_arg_type_guard` (#210, INSTR/LOCATE arity) →
`rewrite_decimal_stringifications` (#211). Issue #215 asked only for the LIKE guard to close a
hard scan failure; the other two guards were already written, tested, and composed into the one
pipeline function.

### Decision

Both join sites run `apply_type_rewrites` in full, rather than `like_subject_type_guard` alone.
Every decline the wider pipeline produces is already safe: PR #285 made broadcast fall back to
N-scan and an N-scan side-local decline become a residual outer-`WHERE` conjunct.

### Options Considered

| Option | Verdict |
|--------|---------|
| Wire the full `apply_type_rewrites` pipeline | ✓ Chosen — reuses the exact single-table mechanism with zero new guard code; every decline is already made safe by #285; closes #223 slice 2 (a silent wrong answer) and narrows #228 at the same two sites for free |
| Wire `like_subject_type_guard` alone, matching #215's literal scope | ✗ Rejected — leaves two known wrong-answer paths open at surfaces already being edited, one of them (#223 slice 2) silently wrong rather than loudly failing |

### Consequences

The pipeline function stays the sole sequencer of its three passes; no call site — including these
two new ones — sequences the guards itself. Issue #223 narrows to slices 1 and 3; issue #228's
exposure narrows without being closed.

## ADR: The N-scan type screen runs per side and per conjunct, after attribution

**ID:** n-scan-type-screen-runs-per-side-per-conjunct-after-attribution
**Plan:** fix-join-filter-type-rewrites
**Status:** Accepted

### Context

Unlike the broadcast site, the N-scan path has no disjoint-column-name precondition — two sides may
declare the same column name with different Exasol types. The leg-eligibility partition
(`renderable_only`/`declined_only`) runs over the whole top-level conjunct set before
`side_local_filter` attributes any conjunct to a table, so the type screen cannot reuse that
pre-attribution pass without resolving a shared name against an arbitrary side.

### Decision

Screen each conjunct individually, against `cols_per_side[i]`, after `side_local_filter` has
attributed it to a table — never over a combined pre-attribution conjunct set. A decline costs only
the offending conjunct's leg pushdown, not its side's other conjuncts.

### Options Considered

| Option | Verdict |
|--------|---------|
| Per-side, per-conjunct, post-attribution screen | ✓ Chosen — the only screen that resolves a shared column name against the owning side's own type, and the existing partition already supports per-conjunct granularity for free |
| Fold the type condition into the pre-attribution syntactic screen using a combined cross-side type map | ✗ Rejected — a combined map resolves a shared name against an arbitrary side, either pushing a non-string LIKE into a leg (hard scan failure) or forfeiting a valid string LIKE's pushdown |
| Screen each side's whole side-local tree at once, declining all of a side's conjuncts on one bad one | ✗ Rejected — the partition already expresses a per-conjunct decision, so per-tree granularity gives up pushdown for no structural gain |

### Consequences

The N-scan and broadcast surfaces necessarily use different column-type universes — the owning
side's own columns versus the disjoint-guarded bare-name union — a distinction future planners must
preserve rather than unify.

## ADR: Screen the tree you render, not the tree you received

**ID:** screen-the-tree-you-render-not-the-tree-you-received
**Plan:** fix-join-filter-type-rewrites
**Status:** Accepted

### Context

Plan-review round 1 found that the original `type_screened_leg_filter` design partitioned conjuncts
on `apply_type_rewrites(c, col_types).is_some()` alone and then handed the leg the REWRITTEN tree,
with no check that the rewritten tree was itself `datafusion_renderable`. A conjunct that is
type-accepted but whose rewritten form is unrenderable would fall out of both the leg half and the
residual half — applied nowhere, extra rows, no error. That is #279's exact defect recurring at a
new site; the broadcast surface already guards against it via `classify_where_filter`'s
`(Some(raw), Some(tree)) if !datafusion_renderable(tree)` arm.

### Decision

The leg-eligibility predicate requires BOTH conditions on the REWRITTEN conjunct — type-accepted AND
`datafusion_renderable` — because the leg renders the rewritten tree, not the raw one. The
fail-closed arm fires in both directions: if a side's re-formed accepted-conjunct tree does not
survive the pipeline, or survives but is not renderable, the whole side-local set goes residual
rather than being silently dropped.

### Options Considered

| Option | Verdict |
|--------|---------|
| Screen the REWRITTEN tree for both type-acceptance and renderability | ✓ Chosen — matches what the leg actually renders; mirrors the guard `classify_where_filter` already carries at the broadcast site |
| Screen the raw tree for renderability and the rewritten tree only for type-acceptance | ✗ Rejected — lets a type-accepted-but-rewritten-unrenderable conjunct escape both the leg and the residual, reproducing #279 at a new site |

### Consequences

"Screen the tree you render, not the tree you received" is the general form of the #279 defect and
applies to any future render surface wired to the type-rewrite pipeline — a later planner adding a
fourth surface must apply the same rule rather than re-deriving it.

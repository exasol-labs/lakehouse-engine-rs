# Decisions: fix-join-filter-type-rewrites

## ADR: Wire the full type-rewrite pipeline into both join WHERE-filter sites, not only the LIKE guard

**ID:** wire-full-type-rewrite-pipeline-into-join-where-filter-sites
**Plan:** fix-join-filter-type-rewrites
**Status:** Accepted

### Context

The broadcast join's combined WHERE filter and the N-scan fallback's per-leg WHERE filter rendered with no column-type awareness. The single-table path runs `apply_type_rewrites`, an ordered pipeline of the LIKE guard (#207), the string-function guard (#210), and the DECIMAL stringification rewrite (#211). Issue #215 asked only for the LIKE guard.

### Decision

Both join sites run the full `apply_type_rewrites` pipeline. Every decline it produces is already safe, since PR #285 made broadcast fall back to N-scan and made an N-scan side-local decline a residual outer `WHERE` conjunct.

### Options Considered

| Option | Verdict |
|--------|---------|
| Wire only the LIKE guard, matching #215's literal scope | Rejected: leaves two known wrong-answer paths open at surfaces already being edited, one of them (#223 slice 2) silently wrong |

### Consequences

The pipeline function stays the only sequencer of its passes, and no call site sequences the guards itself. Issue #223 narrows to slices 1 and 3, and #228's exposure narrows without closing.

## ADR: The N-scan type screen runs per side and per conjunct, after attribution

**ID:** n-scan-type-screen-runs-per-side-per-conjunct-after-attribution
**Plan:** fix-join-filter-type-rewrites
**Status:** Accepted

### Context

N-scan sides may declare the same column name with different Exasol types. The leg-eligibility partition runs over the whole conjunct set before `side_local_filter` attributes a conjunct to a table, so a type screen there would resolve a shared name against an arbitrary side.

### Decision

The adapter screens each conjunct individually against its owning side's columns, after `side_local_filter` attributes it. A decline costs only that conjunct's leg pushdown.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fold the type condition into the pre-attribution screen with a combined type map | Rejected: resolves a shared name against an arbitrary side, which either pushes a non-string LIKE into a leg or forfeits a valid string LIKE |
| Screen each side's whole tree at once | Rejected: the partition already decides per conjunct, so this gives up pushdown for no gain |

### Consequences

N-scan uses the owning side's columns and broadcast uses the disjoint-guarded bare-name union. Future planners must keep this distinction.

## ADR: Screen the tree you render, not the tree you received

**ID:** screen-the-tree-you-render-not-the-tree-you-received
**Plan:** fix-join-filter-type-rewrites
**Status:** Accepted

### Context

Partitioning conjuncts on type acceptance alone and then rendering the rewritten tree lets a conjunct whose rewritten form is unrenderable fall out of both the leg and the residual. It is then applied nowhere and returns extra rows with no error, which is the #279 defect. The broadcast site already guards this in `classify_where_filter`.

### Decision

The leg-eligibility predicate requires the rewritten conjunct to be type-accepted and `datafusion_renderable`. If a side's re-formed accepted tree fails the pipeline or is unrenderable, the whole side-local set becomes residual.

### Options Considered

| Option | Verdict |
|--------|---------|
| Screen the raw tree for renderability and the rewritten tree only for type acceptance | Rejected: lets a type-accepted but unrenderable rewritten conjunct escape both halves, reproducing #279 |

### Consequences

This rule is the general form of #279 and applies to any future render surface wired to the pipeline.

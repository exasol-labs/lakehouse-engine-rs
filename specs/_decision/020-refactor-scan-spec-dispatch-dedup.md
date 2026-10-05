# Decisions: refactor-scan-spec-dispatch-dedup

## ADR: Shared `RequestShape` Classifier Consumed By Both the Dispatch and Empty-Result Paths

**ID:** shared-request-shape-classifier-dispatch-and-empty-result
**Plan:** `refactor-scan-spec-dispatch-dedup`
**Status:** Accepted

### Context

The routing decision (grouped aggregate, then single-group aggregate, then row scan, with the same type validation and HAVING decline) was coded twice, in the dispatcher and in the empty-result path, kept in sync only by convention.

### Decision

One classifier decides the request shape, and both the dispatcher and the empty-result path render only their own shape from it. It owns the three-tier priority, the aggregate type gates, the HAVING decline, and the HAVING merge-render fragment. The dispatcher keeps the single-group sub-split (lone `COUNT(DISTINCT)`, `DISTINCT`, ordinary) as a rendering concern.

### Options Considered

| Option | Verdict |
|--------|---------|
| Put the single-group sub-split into the shape enum | Rejected: the empty-result path treats those cases as one shape, so the split is rendering, not routing |
| Keep two hand-synced trees | Rejected: this is the drift issue #175 reports |
| Name the enum `PlanShape` | Rejected: collides with the DataFusion physical-plan-shape concept |

### Consequences

A routing rule change lands once and both paths pick it up.

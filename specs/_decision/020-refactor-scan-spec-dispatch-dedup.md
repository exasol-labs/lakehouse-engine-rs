# Decisions: refactor-scan-spec-dispatch-dedup

## ADR: Flatten-Embed `CommonScanSpec` Into `ScanSpec`

**ID:** flatten-embed-common-scan-spec-into-scan-spec
**Plan:** `refactor-scan-spec-dispatch-dedup`
**Status:** Accepted

### Context

`ScanSpec` duplicated the shard-invariant fields of `CommonScanSpec`, and the copy code drifted silently whenever a field was added. The UDF wire format must stay byte-identical.

### Decision

`ScanSpec` embeds `CommonScanSpec` with a flattened serde field next to the file list. Every shard-invariant read and construction site moves to the nested form.

### Options Considered

| Option | Verdict |
|--------|---------|
| `Deref` to `CommonScanSpec` | Rejected: discouraged pattern that does not remove the construction-site edits |
| Declarative macro generating both structs | Rejected: more clever than the codebase's "prefer simple" bar |
| Nested, non-flattened field | Rejected: changes the wire to nested JSON |

### Consequences

The shard-invariant fields have one declaration, and the compiler forces every site to migrate. The migration touches about 100 read sites and 85 construction sites in one compile unit, so it goes to the expert executor, with the golden dispatch-SQL baseline as the drift detector.

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

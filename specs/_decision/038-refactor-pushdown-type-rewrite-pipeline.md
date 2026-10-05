# Decisions: refactor-pushdown-type-rewrite-pipeline

## ADR: Two fixed-body pipeline functions, not one function with a pass-selection parameter

**ID:** two-fixed-pass-list-functions-not-a-pass-selection-parameter
**Plan:** refactor-pushdown-type-rewrite-pipeline
**Status:** Superseded by one-type-rewrite-pipeline-function-for-both-render-surfaces

### Context

The filter pipeline runs three passes and the select-list pipeline runs two, omitting the LIKE-subject pass because that wiring was not yet done (issue #219).

### Decision

The filter and select-list pipelines are two separate functions with fixed pass sequences.

### Options Considered

| Option | Verdict |
|--------|---------|
| One function with an `include_like_pass` flag | Rejected: a toggle reads as a supported configuration, not as a tracked gap |
| A registry of boxed rewrite passes | Rejected: issue #259 rejects it, and the pass list is never assembled at runtime |

### Consequences

The missing LIKE-subject pass stays visible as a tracked gap, and closing #219 is a one-line change in one function.

# Decisions: refactor-joins-module-split

## ADR: Nested directory module for an oversized `pushdown/` submodule

**ID:** pushdown-joins-nested-directory-module
**Plan:** `refactor-joins-module-split`
**Status:** Accepted

### Context

Issue #129 flags `pushdown/joins.rs` (3,749 lines) as an oversized file with no size
guardrail. It holds four distinct concerns — join-shape detection, broadcast-side
selection, alias-qualified expression rendering, and broadcast + N-scan SQL assembly —
plus ~2,070 lines of tests in one central `mod tests`, behind the existing
`pushdown/mod.rs` façade that all consumers rely on.

### Decision

Convert `joins` from a flat sibling file to a directory module
`joins/{mod.rs, planning.rs, rendering.rs, sql_builders.rs}`. `mod joins;` in
`pushdown/mod.rs` resolves a directory module identically to a file, so the façade and
every consumer `use` path stay unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Directory module `joins/{mod.rs, planning.rs, rendering.rs, sql_builders.rs}` | ✓ Chosen — keeps `crate::adapter::pushdown::joins::*` and `mod joins;` unchanged while splitting the file; establishes a reusable pattern for the other oversized files issue #129 tracks |
| Flat sibling files (`pushdown/joins_planning.rs`, …), matching every other `pushdown/` submodule | ✗ Rejected — loses the `joins` namespace scoping the interview asked for |

### Consequences

`pushdown/` gains its first nested module-within-a-module, setting precedent for
`scan/mod.rs` and `grouped_agg.rs`, the other oversized files issue #129 tracks, to
follow the same oversized-submodule-becomes-a-directory-module pattern behind an
unchanged façade.

# Decisions: fix-211-decimal-string-trailing-zeros

## ADR: Type-aware DECIMAL-to-string trim decision lives in the adapter; vs-expression stays type-blind

**ID:** decimal-string-trim-in-adapter-not-vs-expression
**Plan:** fix-211-decimal-string-trailing-zeros
**Status:** Accepted

### Context

Exasol trims trailing scale zeros when converting a DECIMAL to text, but the DataFusion path keeps the full scale, which gives wrong results (issue #211). Column types are not on the wire, and `vs-expression` is stateless and shared.

### Decision

The adapter decides where to apply the DECIMAL-to-string trim, using its column types. `vs-expression` gains only a pure formatting primitive and a synthetic node it renders without inspecting types.

### Options Considered

| Option | Verdict |
|--------|---------|
| Column-type awareness inside `vs-expression` | Rejected: no type context, and the crate is shared |

### Consequences

This extends the `like-guard-in-adapter-not-vs-expression` precedent to projections and WHERE filters. Future type-dependent pushdown gaps are fixed in the adapter.

## ADR: Inject an adapter-synthesized decimal_to_varchar_exasol node

**ID:** decimal-to-varchar-exasol-synthetic-node
**Plan:** fix-211-decimal-string-trailing-zeros
**Status:** Accepted

### Context

A bare DECIMAL column at a stringification point (`CAST` to VARCHAR or CHAR, `CONCAT`, `LENGTH`, including nesting inside chained `||`) must render Exasol's trimmed format.

### Decision

The adapter rewrites each such point into a one-argument `decimal_to_varchar_exasol` node, and `vs-expression` renders the argument and applies the trim.

### Options Considered

| Option | Verdict |
|--------|---------|
| Post-process the rendered SQL string | Rejected: fragile and cannot reach a nested `CONCAT` argument |
| Generic raw-SQL passthrough node | Rejected: broader, less self-documenting surface |

### Consequences

`vs-expression` gains one narrow node type, and issue #210 reuses it with no further `vs-expression` change.

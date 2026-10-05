# Decisions: fix-211-decimal-string-trailing-zeros

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

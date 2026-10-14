# Decisions: fix-projected-literal-pushdown

## ADR: Expr EMITS columns are named positionally-unique; Column EMITS names stay real

**ID:** positional-unique-emits-naming-expr-real-name-column
**Plan:** fix-projected-literal-pushdown
**Status:** Accepted

### Context

Projected literals were named by their rendered SQL text and deduplicated by it, so `SELECT 1, name, 1` collapsed from three columns to two or produced duplicate EMITS names. Exasol rejects both.

### Decision

A projected column keeps its real quoted name in the EMITS clause, and a projected expression gets a positionally-unique synthetic name. The rule applies when the SQL is built.

### Options Considered

| Option | Verdict |
|--------|---------|
| Name every item positionally | Rejected: breaks the outer top-N `ORDER BY`, which references projected columns by real name |
| Deduplicate items that render to the same SQL | Rejected: collapses legitimately repeated literals and reproduces the bug |

### Consequences

The row-scan, broadcast-join, and empty-result builders share the rule and inherit the fix.

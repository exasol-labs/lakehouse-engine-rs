# Decisions: fix-projected-literal-pushdown

## ADR: Expr EMITS columns are named positionally-unique; Column EMITS names stay real

**ID:** positional-unique-emits-naming-expr-real-name-column
**Plan:** fix-projected-literal-pushdown
**Status:** Accepted

### Context

`project_columns` rendered a projected literal or scalar expression as an `Expr` item named
by its rendered SQL text (`emit_name()`), then deduped items sharing that text. Two identical
`1` literals in `SELECT 1, name, 1` share the rendered text `1`, so the dedup collapsed the
query from arity 3 to 2, and the EMITS builders produced duplicate column names `"1"`, `"1"`
for any case that survived — both rejected by Exasol. The fix needed a naming rule that keeps
repeated `Expr` items distinct while leaving a real column's outer-`ORDER BY` reference intact.

### Decision

Move the EMITS-uniqueness guarantee out of the value-based dedup and into a positional naming
rule applied at SQL-build time in `build_row_scan_sql` and `empty_pushdown_sql`: a `Column`
item keeps its real quoted source-column name; an `Expr` item gets a positional-unique
synthetic EMITS identifier, never its rendered SQL text.

### Options Considered

| Option | Verdict |
|--------|---------|
| Column keeps real name; Expr gets positional-unique synthetic name | ✓ Chosen — repeated literals occupy distinct positions and an outer `ORDER BY` over a projected column still resolves by its real name |
| Name every item positionally (including columns) | ✗ Rejected — breaks the top-N outer `ORDER BY`, which references projected columns by real name |
| Dedup items that render to identical SQL | ✗ Rejected — collapses legitimately-repeated literals, reproducing the arity bug |

### Consequences

Repeated literal or expression select-list items now always occupy distinct EMITS positions
with distinct synthetic names, while queries with a projected-column `ORDER BY` keep resolving
against real column names. The naming rule is shared by the row-scan, broadcast-join, and
empty-result builders, so all three inherit the fix from one seam.

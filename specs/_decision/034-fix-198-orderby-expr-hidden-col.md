# Decisions: fix-198-orderby-expr-hidden-col

## ADR: Render a declined-path expression ORDER BY over hidden base columns in the Exasol dialect

**ID:** declined-order-by-expression-hidden-base-columns-exasol-dialect
**Plan:** fix-198-orderby-expr-hidden-col
**Status:** Accepted

### Context

The declined row-scan wrapper must render an expression sort key. Exasol declares result types only for select-list items, so a computed sort expression has no EMITS type.

### Decision

The adapter appends the sort expression's base columns as hidden scan columns. The wrapper's outer `ORDER BY` renders the expression in the Exasol dialect over those columns, so Exasol evaluates it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hidden DataFusion-computed expression column | Rejected: needs an EMITS type Exasol never supplies, and a wrong guess misorders (a VARCHAR guess sorts lexicographically) |

### Consequences

- One renderer serves every Exasol-evaluated `ORDER BY` clause.

## ADR: A grouped ORDER BY over an aggregate absent from the select list routes to the qualified single-table wrapper

**ID:** unresolvable-grouped-order-by-routes-to-qualified-wrapper
**Plan:** fix-198-orderby-expr-hidden-col
**Status:** Accepted

### Context

A grouped `ORDER BY` can sort on an aggregate outside the select list. The merge wrapper has only group-key and partial columns, and Exasol declares no type for that aggregate.

### Decision

The adapter matches the aggregate sort key against the select-list plans. A match keeps the partial/merge path, and no match routes to the qualified single-table wrapper, not to an error.

### Options Considered

| Option | Verdict |
|--------|---------|
| Append the aggregate as an extra partial plan | Rejected: needs a fabricated Exasol type and risks SUM overflow and misordering |

### Consequences

- Such requests return a correct answer but lose partial/merge decomposition.

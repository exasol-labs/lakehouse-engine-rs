# Decisions: fix-198-orderby-expr-hidden-col

## ADR: Advertise ORDER_BY_EXPRESSION rather than detect the appended select-list item

**ID:** advertise-order-by-expression-not-selectlist-detection
**Plan:** fix-198-orderby-expr-hidden-col
**Status:** Accepted

### Context

While `ORDER_BY_EXPRESSION` is unadvertised, Exasol appends the sort key of an expression `ORDER BY` to the pushed `selectList`, which leaks a `HIDDEN_COL_n` column (issue #198). The payload is byte-identical to a query that genuinely selects that expression, and no field distinguishes the two.

### Decision

The adapter advertises `ORDER_BY_EXPRESSION`, so Exasol pushes a structured `orderBy` element.

### Options Considered

| Option | Verdict |
|--------|---------|
| Detect and strip the appended select-list item | Rejected: impossible, since no test on the payload is correct for both shapes |

### Consequences

The adapter must render every ordered shape it can now receive. The fix covers every consumer of a pushed `orderBy`.

## ADR: Render a declined-path expression ORDER BY over hidden base columns in the Exasol dialect

**ID:** declined-order-by-expression-hidden-base-columns-exasol-dialect
**Plan:** fix-198-orderby-expr-hidden-col
**Status:** Accepted

### Context

The declined row-scan wrapper must render an expression sort key. Exasol declares result types only for select-list items, so no EMITS type exists for a computed sort expression.

### Decision

The adapter appends the sort expression's base columns as hidden scan columns, typed from `involvedTables[0].columns`. The wrapper's outer `ORDER BY` renders the expression in the Exasol dialect over those columns, so Exasol evaluates it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hidden DataFusion-computed expression column | Rejected: needs an EMITS type Exasol never supplies, and a wrong guess breaks coercion or ordering (a VARCHAR guess sorts lexicographically) |

### Consequences

The Exasol-dialect renderer is the single route for every Exasol-evaluated `ORDER BY` clause, with no second renderer.

## ADR: A grouped ORDER BY over an aggregate absent from the select list routes to the qualified single-table wrapper

**ID:** unresolvable-grouped-order-by-routes-to-qualified-wrapper
**Plan:** fix-198-orderby-expr-hidden-col
**Status:** Accepted

### Context

A grouped `ORDER BY` can sort on an aggregate outside the select list. The merge wrapper has only group-key and partial columns, and Exasol declares no type for that aggregate.

### Decision

The adapter resolves the aggregate sort key against the detected select-list plans with the HAVING merge rewriter. A match keeps the partial/merge path, and no match routes to the qualified single-table wrapper, not to an error.

### Options Considered

| Option | Verdict |
|--------|---------|
| Append the aggregate as an extra partial plan | Rejected: needs a fabricated Exasol type and risks SUM overflow and misordering |

### Consequences

Such requests return a correct bounded answer but lose partial/merge decomposition. A bounded variant is tracked as issue #249.

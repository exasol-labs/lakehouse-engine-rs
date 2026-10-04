# Decisions: fix-207-like-non-string-column

## ADR: Type-aware LIKE decision lives in the adapter, not vs-expression

**ID:** like-guard-in-adapter-not-vs-expression
**Plan:** fix-207-like-non-string-column
**Status:** Accepted

### Context

A pushed-down `LIKE` over a non-string column fails in DataFusion, which does not coerce to VARCHAR as Exasol does (issue #207). The `LIKE` subject carries no type on the wire, and `vs-expression` is a stateless translator shared with a sibling project, so it cannot see column types.

### Decision

The adapter rewrites or declines a `LIKE` filter using its column-type map before rendering. `vs-expression` stays type-blind.

### Options Considered

| Option | Verdict |
|--------|---------|
| Handle types inside `vs-expression` | Rejected: it has no column-type context and is shared |

### Consequences

Any future non-string-subject pushdown gap, such as the join per-leg path or the select-list path, is fixed in the adapter, not in `vs-expression`.

## ADR: CAST DATE to VARCHAR, decline every other non-string LIKE subject

**ID:** like-cast-date-decline-other-nonstring
**Plan:** fix-207-like-non-string-column
**Status:** Accepted

### Context

Exasol implicitly casts any `LIKE` subject to VARCHAR. The guard must decide how to handle each non-string subject type.

### Decision

The adapter casts DATE subjects to VARCHAR. It declines pushdown of the whole top-level filter for every other non-string subject, including unresolvable column types, so Exasol evaluates it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Cast every non-string type | Rejected: DataFusion's decimal, double, and timestamp string formatting differs from Exasol's and silently changes matches |

### Consequences

DATE casting matches Exasol only under the default `NLS_DATE_FORMAT`, an accepted exception tracked as issue #216. Decimal-to-string formatting is deferred to issue #211. Declining the whole filter follows the existing all-or-nothing backstop, so partial rewriting never changes results.

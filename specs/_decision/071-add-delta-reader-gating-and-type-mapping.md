# Decisions: add-delta-reader-gating-and-type-mapping

## ADR: Scope the Delta type refusal to the column, not the table

**ID:** scope-delta-type-refusal-to-column-not-table
**Plan:** `add-delta-reader-gating-and-type-mapping`
**Status:** Accepted

### Context

The shipped behavior refused a whole table on any unmapped column. That made the vendored `stats-all-types` fixture, which mixes mappable columns with `binary`, `map`, and `struct` columns, wholly unqueryable and blocked the E2E acceptance criterion of issue #322. It also makes a real table with one struct column unreachable over a column nobody selected.

### Decision

A refused column is omitted from the logical schema and recorded on `ResolvedScan`. One adapter gate refuses a pushdown request that reads or emits a refused column, and every other request on the table plans normally. This supersedes the table-scoped refusal.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the table-scoped refusal | Rejected: leaves the fixture and such real tables unqueryable |
| Omit refused columns from the `createVirtualSchema` declaration | Rejected: duplicates the classification across two type vocabularies and silently shrinks `SELECT *` |
| Author an all-mappable fixture and keep table scope | Rejected: adds a network-dependent seed step for less useful behavior |

### Consequences

Omitting the column from the logical schema is defense in depth: a gate miss fails with an unresolved-column error instead of emitting a silent NULL column.

## ADR: The refused-column gate reads a total recursive JSON walk, never a per-clause enumeration

**ID:** delta-refused-column-gate-total-recursive-json-walk
**Plan:** `add-delta-reader-gating-and-type-mapping`
**Status:** Accepted

### Context

The gate needs every column a pushdown request touches, through a filter, GROUP BY, ORDER BY, aggregate argument, join condition, or the emitted projection.

### Decision

The gate collects the name of every `column` node in one recursive walk over the whole request JSON. It unions the rendered projection only when the request's select list is absent or empty, a genuine `SELECT *`. The synthetic full-row projection rendered for an aggregate or untranslatable select item is never unioned, because the scan never reads it and each item's own columns are already covered by the walk. The widened-versus-not distinction is an `Option` argument, so the invalid combination cannot be expressed.

### Options Considered

| Option | Verdict |
|--------|---------|
| Enumerate the clauses that can carry a column | Rejected: omits every later pushdown capability, and a miss routes a refused column into the scan |
| Union the full-row projection unconditionally | Rejected: refuses `COUNT(*)`, which reads no column, against any table with an unrelated refused column |

### Consequences

The walk catches filters that compare a `binary` column as text, where every non-UTF-8 value would silently become NULL. The join path charges each tagged reference to its own side, and untagged or ambiguous references to every side. This stops a refused column on one side from refusing a select that names only an identically named mappable column on the other.

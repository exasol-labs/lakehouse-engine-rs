# Decisions: fix-198-orderby-expr-hidden-col

## ADR: Advertise ORDER_BY_EXPRESSION rather than detect the appended select-list item

**ID:** advertise-order-by-expression-not-selectlist-detection
**Plan:** fix-198-orderby-expr-hidden-col
**Status:** Accepted

### Context

An `ORDER BY` on an expression or aggregate absent from the client's select list leaks an
extra `HIDDEN_COL_n` result column (issue #198), because Exasol silently appends the sort
key to the pushed `selectList` while `ORDER_BY_EXPRESSION` is unadvertised. Measured on the
wire: `SELECT id, c_price FROM t ORDER BY ABS(c_price)` (the bug) and `SELECT id, c_price,
ABS(c_price) AS a FROM t ORDER BY ABS(c_price)` (correct, genuinely selected) push a
byte-identical `selectList` and yield identical adapter-generated SQL — Exasol picks the
client-facing column name (`HIDDEN_COL_2` vs `A`) server-side with no signal to the adapter.
An exhaustive key scan of the raw payload found no disambiguating field.

### Decision

Fix #198 by advertising `ORDER_BY_EXPRESSION`, so Exasol pushes a structured `orderBy`
element instead of appending the sort key to the `selectList`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise `ORDER_BY_EXPRESSION` | ✓ Chosen — the wire payload is byte-identical between the leaking and correct shapes, so advertising is the only mechanism that removes the ambiguity |
| Detect the trailing appended `selectList` item and strip it | ✗ Rejected — proven impossible, not merely difficult: no test on the payload can be correct for both shapes |

### Consequences

The adapter gains an obligation to render every ordered shape it can now reach faithfully
(see the atomicity ADR below), but the leak is closed at the only point that can close it,
and the fix generalizes to every consumer of a pushed `orderBy`, not just issue #198's own
repro shapes.

## ADR: The capability advertisement and its backing paths land as one atomic change

**ID:** order-by-expression-advertisement-and-backing-paths-atomic
**Plan:** fix-198-orderby-expr-hidden-col
**Status:** Accepted

### Context

Advertising `ORDER_BY_EXPRESSION` makes Exasol delegate the ordering and stop re-sorting
returned rows. Verified live by advertising the capability with no backing path, then
reverting: the row-scan repro returned rows in raw file order with no error — a silent
wrong-order regression, strictly worse than the leak — and the grouped repro hard-errored
on the pre-existing unresolvable-`ORDER BY` decline.

### Decision

No commit may advertise `ORDER_BY_EXPRESSION` before every reachable ordered path — the
declined row-scan wrapper, the grouped merge, the qualified single-table wrapper, and the
N-scan join wrapper — renders an expression sort key faithfully or declines with a `User`
error naming the key.

### Options Considered

| Option | Verdict |
|--------|---------|
| One atomic change: advertise only once every backing path exists | ✓ Chosen — measured, not hypothetical; the alternative returns successful-but-wrong results |
| Advertise first, add rendering paths incrementally across commits | ✗ Rejected — every intermediate commit would ship a silent wrong-order regression strictly worse than the bug it fixes |

### Consequences

The implementation work cannot land incrementally; task group D (advertisement) is
sequenced last, after every backing path (groups A-C) is in the tree.

## ADR: Render a declined-path expression ORDER BY over hidden base columns in the Exasol dialect

**ID:** declined-order-by-expression-hidden-base-columns-exasol-dialect
**Plan:** fix-198-orderby-expr-hidden-col
**Status:** Accepted

### Context

The declined row-scan wrapper needs to render an expression or aggregate sort key Exasol
delegates but the adapter cannot bound as a per-shard top-N. Exasol declares a result type
only for `selectList` items, never for a sort-key expression, so no Exasol EMITS type exists
for a value the sort expression alone would compute.

### Decision

Append the sort expression's referenced BASE columns as hidden scan columns, each carrying
its declared Exasol type read from `involvedTables[0].columns`, and render the wrapper's
outer `ORDER BY` as the expression translated to the Exasol dialect over those emitted
identifiers, so Exasol evaluates the sort expression itself.

### Options Considered

| Option | Verdict |
|--------|---------|
| Hidden base columns + Exasol-dialect expression rendering | ✓ Chosen — base columns already carry a declared type from `involvedTables[0].columns`; no type has to be invented |
| Hidden DataFusion-computed expression column, sorted on its emitted alias | ✗ Rejected — needs a declared EMITS type Exasol never supplies; a wrong guess breaks per-column coercion or corrupts the ranking (e.g. a VARCHAR guess sorts lexicographically) |

### Consequences

The Exasol-dialect renderer (`crates/vs-expression`), already used by the qualified wrapper
and the N-scan join wrapper for their own clauses, becomes the single seam every
Exasol-evaluated `ORDER BY` clause routes through — no second renderer is introduced.

## ADR: A grouped ORDER BY over an aggregate absent from the select list routes to the qualified single-table wrapper

**ID:** unresolvable-grouped-order-by-routes-to-qualified-wrapper
**Plan:** fix-198-orderby-expr-hidden-col
**Status:** Accepted

### Context

A grouped `ORDER BY` may sort on an aggregate the select list does not carry (issue #198's
own "top N groups" repro). The outer merge wrapper's only columns are `GK_*` and
`PARTIAL_*`; an aggregate absent from the detected select-list plans has no `PARTIAL_*`
column to merge over, and Exasol declares a result type only for `selectList` items — never
for an aggregate outside it.

### Decision

Resolve the aggregate sort key against the detected select-list plans via the existing
HAVING merge rewriter. A match keeps the partial/merge path; no match routes the request to
`RequestShape::GroupByWrapper` instead of the prior hard error.

### Options Considered

| Option | Verdict |
|--------|---------|
| Route an unresolvable aggregate sort key to the qualified single-table wrapper | ✓ Chosen — the wrapper needs no fabricated type at all, and is the recorded issue #195 precedent for the structurally identical unmergeable-HAVING case |
| Append the missing aggregate as an extra `PARTIAL_*` plan | ✗ Rejected — needs a fabricated Exasol type for a column Exasol never declared one for; risks SUM overflow and precision-driven misordering |

### Consequences

The "top N groups" shape and the different-aggregate-in-select-list shape both get a
correct bounded answer instead of a hard error, at the cost of losing partial/merge
decomposition for that one request; a bounded partial/merge variant for the not-selected
case is tracked as future work (issue #249), named explicitly rather than left an unstated
gap.

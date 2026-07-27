# Feature: Pushdown Planning — Capability Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the newly advertised
capabilities: scalar select-list expression pushdown, HAVING clause pushdown, and
decomposable statistical aggregate pushdown via sufficient statistics. Each extends the
translator or aggregate planner with a shard-associative partial/merge path.

## Background

* This delta replaces the full-base-row widening rule for a declined `ORDER BY` with a
  hidden-sort-column rule, and corrects the declined-`ORDER BY` correctness-safety
  scenario to describe the self-contained wrapper the adapter actually renders. Every
  other capability-extensions scenario is unchanged.
* Exasol validates a returned pushdown query's column count POSITIONALLY against the
  original `selectList`. A returned row wider or narrower than that select list is
  rejected with `sqlCode 04000` ("Expected number of columns is N but pushdown query has
  M"), and Exasol never re-projects a declined pushdown — it runs the returned SQL as the
  final answer as-is.
* The adapter's DERIVED PROJECTION is the projection-item list the adapter builds from the
  select list. It normally has one item per select-list item, but a SEPARATE, pre-existing
  fallback widens it to the full base row when any select-list item is untranslatable, is
  an unknown or aggregate node, or carries a declared EMITS type Exasol rejects (see the
  "Projected constant whose declared EMITS type Exasol rejects declines to the full base
  row" scenario). The declined-`ORDER BY` rules below are stated against that derived
  projection, NOT against the raw select-list arity: they preserve whatever the derivation
  produced and never widen it further. A request whose derived projection that pre-existing
  fallback has ALREADY widened never reaches these scenarios: the dispatcher routes it to the
  qualified single-table wrapper first — on the widening signal itself, for every base-table
  column count, including one coincidentally equal to the select-list arity — before the
  declined-`ORDER BY` path runs (see "A widened derived projection routes to a native wrapper
  on every path"). No exception is tracked here.
* A select-list item's quoted EMITS identifier is produced by ONE seam: the real
  source-column name for a bare column, the positional synthetic `_LH_PROJ_{index}` for a
  rendered expression. The per-shard EMITS clause and any outer wrapper's explicit column
  list render through that same seam, so they agree positionally by construction.
* The declined-`ORDER BY` path is the unoptimized correctness restoration for an ordered
  shape the adapter cannot bound as a top-N. It is distinct from the bounded per-shard
  top-N of `vs-adapter/pushdown-planning-topn`, whose eligibility check and matched
  rendering this delta leaves unchanged.
* Column Exasol types are read from `involvedTables[0].columns`; a sort key is always a
  bare `column` node, because the adapter advertises `ORDER_BY_COLUMN` but not
  `ORDER_BY_EXPRESSION`. An `orderBy` element that is not a bare column, or that omits its
  direction or NULL-placement flag, is filtered out by the sort-key parser, so a non-empty
  `orderBy` can still yield zero parsed sort keys.

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: Projected literal with an ORDER BY on an unprojected column declines to the full base row

* *GIVEN* a row-scan `pushdown` request whose select list projects only literal/constant items and whose `orderBy` sorts on a source column absent from that projection (e.g. `SELECT 1 FROM t ORDER BY name LIMIT 5`), which the adapter cannot serve as a bounded top-N
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL project the full base row for this shape so the declined-ORDER-BY wrapper's outer `ORDER BY` resolves against emitted columns, and MUST NOT emit a narrowed literal-only projection whose declined-ORDER-BY wrapper references a column the scan no longer emits
* *AND* this SHALL preserve the pre-fix behavior for this unsupported shape (a well-formed declined-ORDER-BY wrapper) rather than introduce a distinct scan-time failure mode
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: ORDER BY on a column outside the derived projection emits the sort key as a hidden scan column

* *GIVEN* a row-scan `pushdown` request that the adapter cannot serve as a bounded top-N, carrying an `orderBy` whose parsed bare-column sort key is NOT a bare-column item of the adapter's derived projection — a different column entirely (`SELECT score FROM t WHERE id = 1 ORDER BY id`), a column referenced only inside a projected expression (`SELECT id || '-' || name FROM t WHERE id <= 3 ORDER BY id`), or a literal-only select list (`SELECT 1 FROM t ORDER BY name LIMIT 5`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL append each such sort-key column, resolved by name from `involvedTables[0].columns`, to the per-shard scan's projection and its declared EMITS list AFTER every item the derivation already produced, so every pre-existing item keeps its position and its unchanged EMITS identifier
* *AND* the adapter MUST NOT widen the derived projection to the full base row, because the returned query would then carry one column per base-table column where Exasol positionally expects one per select-list item, which Exasol rejects with `sqlCode 04000`
* *AND* the declined-`ORDER BY` wrapper SHALL name the derived projection's pre-extension items EXPLICITLY by their EMITS identifiers rather than using `SELECT *`, so each appended sort-key column is visible to the outer `ORDER BY` but absent from the returned result, and the returned column count and order EQUAL the derived projection's pre-extension column count and order
* *AND* the returned result SHALL equal the same query evaluated over all matching rows on a single node, in the requested sort-key order, direction, and NULL placement, EXCEPT for a sort key whose column requires the JSON-fallback VARCHAR cast — which orders on the emitted JSON string rather than the native value, pre-existing behaviour on this declined path that this scenario does not change, tracked as an accurately-scoped exception, `(#233)`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Hidden sort-key columns are appended at most once and never invented

* *GIVEN* a declined-`ORDER BY` row-scan `pushdown` request whose `orderBy` names a column already present as a bare-column item of the derived projection, or names the same column in more than one sort key, or names a column absent from `involvedTables[0].columns`, or consists only of elements the sort-key parser rejects
* *WHEN* the adapter appends hidden sort-key columns and builds the wrapper
* *THEN* a sort-key column already present as a bare-column item of the derived projection SHALL NOT be appended, and a column named by two or more sort keys SHALL be appended at most once, because a repeated EMITS identifier is a duplicate-column error
* *AND* a sort-key column that cannot be resolved from `involvedTables[0].columns` SHALL be left unresolved — neither appended nor otherwise special-cased — preserving the existing shape for this defensive case, which is unreachable in practice because every pushed sort key names a real table column
* *AND* when no `orderBy` element parses into a sort key the adapter SHALL return the unwrapped scan-driving SQL unchanged, emitting NEITHER a wrapper nor an `ORDER BY` clause, because rendering an empty sort-key list would produce a bare `ORDER BY` with no elements — invalid SQL
* *AND* when the derived projection has no item to name explicitly the adapter SHALL leave the wrapper's `SELECT *` in place, because an empty explicit select list is not valid SQL
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: An ORDER BY the adapter cannot bound as a top-N remains correctness-safe

* *GIVEN* the adapter advertises `ORDER_BY_COLUMN` and Exasol pushes an `order_by` in a `pushdown` request that the adapter cannot serve as an ordered top-N (no accompanying `LIMIT`, a sort key that is not a bare projected column, or a request that also carries aggregates / group keys / a `having`)
* *WHEN* the adapter builds the scan-driving SQL
* *THEN* the adapter SHALL fall back to the unoptimized declined path for that shape, carrying neither a per-shard row limit nor per-shard sort keys ahead of the ordering, and MUST NOT emit a scan spec that would compute a different result than single-node evaluation
* *AND* the adapter SHALL render the ordering ITSELF, as a self-contained global `ORDER BY` (plus the request's `LIMIT`, if any) wrapping the unbounded fan-out, and SHALL NOT rely on Exasol re-applying an `ORDER BY` it retains — once `ORDER_BY_COLUMN` is advertised Exasol delegates a pushed `orderBy` and does not re-sort the returned rows
* *AND* that wrapper SHALL preserve the derived projection's pre-extension column count and order, emitting any sort-key column that projection lacks as a hidden scan column per the scenarios above, so a declined `ORDER BY` never becomes an Exasol column-count rejection nor a reference to a column the scan does not emit
<!-- /DELTA:CHANGED -->

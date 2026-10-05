# Decisions: fix-declined-filter-self-apply

## ADR: There is no Exasol-side fallback for a predicate whose capability the adapter advertised

**ID:** no-exasol-side-fallback-for-an-advertised-capability
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

Three sites treated a declined WHERE-filter render as safe to omit, assuming Exasol would re-evaluate the predicate. Verified live, three separate decline sources each returned all 12 rows of a probe table where 0, 3, and 7 were correct, and `EXPLAIN VIRTUAL` showed no `WHERE` in the emitted SQL. The pushdown response carries only `type` and `sql`. Exasol splits the query from the capabilities response alone, before the pushdown request exists.

### Decision

Once the capabilities response advertises a predicate or function shape, Exasol delegates it fully and never re-checks or re-applies it. The adapter must generate equivalent SQL for anything it cannot push to DataFusion. This is recorded in `CLAUDE.md` as a general fact, without discovery narrative or issue number.

### Options Considered

| Option | Verdict |
|--------|---------|
| Leave the omission as an unverified assumption | Rejected: it was disproven live, and leaving it would reseed the defect. The only documented escape (`EXCLUDED_CAPABILITIES`) is whole-capability and whole-schema at DDL time, not per query |

### Consequences

Every declined predicate must be self-applied in the adapter's returned SQL, because omission is never correct once a capability is advertised. This fact justifies the other decisions in this plan.

## ADR: The recorded LIKE-guard consequence is corrected, not merely superseded

**ID:** correct-recorded-like-guard-consequence-not-merely-superseded
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted
**Supersedes:** like-guard-in-adapter-not-vs-expression

### Context

Decision 026 states that a non-string LIKE declines the whole top-level filter so Exasol evaluates it natively, mirroring an all-or-nothing backstop that does not exist. Decisions 031 (HAVING) and 035 (ORDER BY/OFFSET) each corrected one clause of the same false family and left the WHERE-filter clause, which issues #207, #219, and #215 inherited, unrevisited.

### Decision

The decline scope in decision 026 (all-or-nothing, never partial rewriting) stays. The stated consequence changes: the adapter's own outer WHERE applies the declined filter, not Exasol.

### Options Considered

| Option | Verdict |
|--------|---------|
| Supersede without naming the error | Rejected: leaves the contradiction unaddressed and reintroduces the defect class in the permanent library |

### Consequences

The decision log is correct for the HAVING, ORDER BY/OFFSET, and WHERE-filter clauses of the same false backstop family.

## ADR: The single-table decline routes to the existing qualified single-table wrapper

**ID:** single-table-decline-routes-to-the-qualified-single-table-wrapper
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

The single-table path serves five shapes: row scan, top-N, single-group aggregate, grouped aggregate, and `COUNT(DISTINCT)`. A declined filter must be evaluated before every other clause those shapes render.

### Decision

On a filter decline, the dispatcher routes to the qualified single-table wrapper. The wrapper renders the original, un-type-rewritten predicate as its own `WHERE` between the raw fan-out and every other clause, and the fan-out spec has no filter.

### Options Considered

| Option | Verdict |
|--------|---------|
| Wrap the emitted SQL in `SELECT * FROM (...) WHERE <predicate>` | Rejected: four of five shapes would filter after aggregation or truncation |
| A new row-scan-only wrapper that errors on the other four shapes | Rejected: worse than reusing a path that handles all five |

### Consequences

The fast path is untouched when the filter renders. A materialization boundary appears only on the slower decline path, and the wrapper gains a fourth route, not a new shape.

## ADR: Screen renderability at the render consumer, not inside the shared partition classifier

**ID:** screen-renderability-at-the-render-consumer-not-in-the-partition-classifier
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

`side_local_filter` has a second consumer: `plan_join` passes its result to join-side resolution as the Iceberg manifest-pruning predicate. A renderability condition inside it would also strip declined conjuncts from pruning, opening more files with no failing test.

### Decision

The partition functions stay structural. One renderability screen, with exact-complement `renderable_only` and `declined_only` halves, applies at the two render call sites in the N-scan join builder. Each side's pruning predicate keeps every side-local conjunct.

### Options Considered

| Option | Verdict |
|--------|---------|
| Add the condition inside the partition functions | Rejected: silently degrades Iceberg pruning |
| Render the full filter into the outer `WHERE` unconditionally | Rejected: churns every join query's golden SQL and fails on any Exasol-unrenderable conjunct |
| Have the side fan-out builder report which conjuncts it did not push | Rejected: plumbing for a decision the partition can make directly |

### Consequences

A condition added to a function with both a pruning and a rendering consumer degrades pruning invisibly. Screen at the consumer, not in the shared classifier.

## ADR: The full-base-row projection is keyed off the absent select list, not off the decline

**ID:** decline-route-projects-the-full-base-row
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

A genuine `SELECT *` routed to the wrapper was narrowed to the columns the rendered clauses name. Exasol validates the pushdown result positionally, so that gives a `04000` error or a truncated result. Captured live with `EXPLAIN VIRTUAL`, `SELECT *` omits the `selectList` key but sends a full-row `selectListDataTypes`, while eight other shapes carry a non-empty `selectList`. The shape that must not narrow is exactly "no select list".

### Decision

The referenced-column projection returns every base column in order with its Exasol type when the request has no select list, and narrows otherwise. The wrapper has no projection branch of its own. The test accepts an absent key, JSON `null`, `[]`, and a non-array value as "no select list", so a future Exasol wire-form change is a no-op. The same test already decides the wrapper's outer SELECT list, so the two agree on arity by construction.

### Options Considered

| Option | Verdict |
|--------|---------|
| Project the full base row whenever a filter was declined | Rejected: forfeits column narrowing (#160) for declined requests that name their columns, on the route whose fan-out carries no filter and ships every row |
| Pass a precomputed projection override as a parameter | Rejected: a parameter for what the request already states |
| Test only for the absent key Exasol sends today | Rejected: a wire-form change would reintroduce the `04000` |

### Consequences

A route added ahead of a classifier inherits every shape the classifier used to divert, so the wrapper's column-shape contract keys off the request's own shape. Only the omitted key was observed live, and the other forms are tolerated by choice. Narrowing (#160) stays on the decline route for every request with a select list, which matters most there because all rows cross the UDF boundary.

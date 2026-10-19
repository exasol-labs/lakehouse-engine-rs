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


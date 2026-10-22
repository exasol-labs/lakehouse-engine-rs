# Decisions: fix-declined-filter-self-apply

## ADR: There is no Exasol-side fallback for a predicate whose capability the adapter advertised

**ID:** no-exasol-side-fallback-for-an-advertised-capability
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

The adapter omitted a WHERE predicate it declined to render, assuming Exasol would re-evaluate it. Exasol does not: it splits the query from the capabilities response alone, and the pushdown response carries only the SQL, so omitted predicates returned wrong rows.

### Decision

Once the capabilities response advertises a predicate or function shape, Exasol delegates it fully and never re-checks it. The adapter generates equivalent SQL for anything it cannot push to DataFusion.

### Options Considered

| Option | Verdict |
|--------|---------|
| Leave the omission as an unverified assumption | Rejected: it is false, and the only escape (excluding a capability) is whole-schema at DDL time, not per query |

### Consequences

- Every declined predicate is applied in the adapter's returned SQL, because omission is never correct once a capability is advertised.

## ADR: Screen renderability at the render consumer, not inside the shared partition classifier

**ID:** screen-renderability-at-the-render-consumer-not-in-the-partition-classifier
**Plan:** fix-declined-filter-self-apply
**Status:** Accepted

### Context

The partition classifier also feeds Iceberg manifest pruning for join sides. A renderability condition inside it would strip declined conjuncts from pruning and open more files with no failing test.

### Decision

The partition classifier stays structural. One renderability screen, split into exact-complement renderable and declined halves, applies where the join builder renders SQL. Each side's pruning predicate keeps every side-local conjunct.

### Options Considered

| Option | Verdict |
|--------|---------|
| Add the condition inside the partition functions | Rejected: silently degrades Iceberg pruning |
| Render the full filter into the outer `WHERE` unconditionally | Rejected: changes every join query's SQL and fails on any conjunct Exasol cannot render |
| Have the side fan-out builder report which conjuncts it did not push | Rejected: adds plumbing for a decision the partition can make directly |

### Consequences

- A condition added to a function shared by pruning and rendering degrades pruning invisibly, so screen at the consumer.


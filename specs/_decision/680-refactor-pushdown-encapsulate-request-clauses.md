# Decisions: refactor-pushdown-encapsulate-request-clauses

## ADR: A fan-out's columns come from where each WHERE conjunct is placed, never from the request

**ID:** fan-out-columns-come-from-the-where-placement
**Plan:** refactor-pushdown-encapsulate-request-clauses
**Status:** Accepted

### Context

A wrapper's fan-out that took its columns from the whole request emitted every column its scan filter read, because nothing tied the columns to the WHERE the wrapper renders (#680). Planning code read the request's raw JSON in many places, so any new path could repeat that.

### Decision

Planning reads and rewrites the pushdown request only through `PushdownRequest`, which owns where each clause lives and treats a JSON-null clause as absent. A `WherePlacement` decides, once per scan shape, which conjuncts each scan applies and which the wrapper applies. A fan-out's columns and the wrapper's WHERE are both derived from that placement, and the only placement that hands the whole WHERE to the wrapper is the named `whole`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Request accessors plus one placement type that both the WHERE and the columns read | ✓ Chosen: a mismatched column list cannot be written |
| A newtype over the wrapper's raw filter passed beside the request | ✗ Rejected: planning still reads the request's raw JSON, and the full filter still fits the newtype |
| One placement algorithm for one scan and for N scans | ✗ Rejected: one scan renders a filter that renders whole as one SQL, so merging changes the single-table pushdown SQL |

### Consequences

- A new wrapper path gets its fan-out columns from a `WherePlacement`. Passing the request's filter where a placement is expected does not compile.
- A new request clause is added to `PushdownRequest` before any planning code reads it.

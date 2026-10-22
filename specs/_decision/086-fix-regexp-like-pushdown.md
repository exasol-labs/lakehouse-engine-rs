# Decisions: fix-regexp-like-pushdown

## ADR: Exasol evaluates every regular-expression function and predicate

**ID:** exasol-evaluates-every-regular-expression-function-and-predicate
**Plan:** fix-regexp-like-pushdown
**Status:** Accepted

### Context

DataFusion's regex engine cannot reproduce Exasol's PCRE match. Exasol accepts a value only when the first match from the value's start ends at the value's end, while DataFusion matches anywhere. DataFusion also rejects look-around and backreferences, and its `\d`, `\w`, `\s`, and `\b` match Unicode where Exasol's match ASCII only.

### Decision

The adapter advertises no regular-expression capability and renders none. Exasol evaluates `REGEXP_LIKE`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, and `REGEXP_COUNT`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `FN_PRED_REGEXP_LIKE` and anchor the pushed pattern as `^(?:<pat>)$` | Rejected: alternation shapes such as `(A\|AR)(G\|GENTINA)?` still differ, and the other differences remain |
| Keep it and decline a pattern that fails a compile check | Rejected: a compile check proves syntax, not match parity |

### Consequences

- A filter with a `REGEXP_LIKE` conjunct still pushes its other `AND` conjuncts and the projection.
- An `OR` that contains one pushes no filter, and an aggregate over a query with one runs in Exasol.

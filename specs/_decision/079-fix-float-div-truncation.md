# Decisions: fix-float-div-truncation

## ADR: Render the DOUBLE cast in the DataFusion dialect only, not in both dialects

**ID:** float-div-cast-datafusion-dialect-only
**Plan:** fix-float-div-truncation
**Status:** Accepted

### Context

`FLOAT_DIV` inherited DataFusion's operand-typed `/`, which truncates integer and decimal operands and returns silently wrong values (issue #186). Exasol's own `/` is already `FN_FLOAT_DIV`, verified live on integer, decimal, and column operands.

### Decision

`FLOAT_DIV` renders `(CAST(<left> AS DOUBLE) / <right>)` in the DataFusion dialect and the bare `(<left> / <right>)` in the Exasol dialect. It is the first arithmetic operator whose rendering differs by dialect, so the both-dialects identity guard is retargeted to assert the divergence, as was done for the CHAR CAST divergence.

### Options Considered

| Option | Verdict |
|--------|---------|
| Cast in both dialects | Rejected: rewrites five Exasol-facing consumer sites and two golden fixtures for no correctness gain, and leaks a DataFusion workaround into Exasol's parser |

### Consequences

The issue's decided approach narrows, because its staging repros were all DataFusion-dialect. One guard test changes from an identity to a divergence assertion.

## ADR: Record the divide-by-zero behaviour from measurement; do not emulate it

**ID:** float-div-zero-behaviour-from-measurement-not-emulation
**Plan:** fix-float-div-truncation
**Status:** Accepted

### Context

Live measurement showed that native Exasol raises `22012` for `x/0` on every operand pairing and admits no non-finite `DOUBLE`. `0/0` yields `NaN`, which the raw-scan `emit_batch` path delivers as a silent NULL. Predicate-position divide-by-zero silently admits or rejects rows where Exasol raises `22012`.

### Decision

The three divide-by-zero cases are recorded with separate owners and none is emulated. `x/0` needs no fix, because the query fails before and after the change. `0/0` widens the tracked NaN-at-emit gap #246. Predicate-position divide-by-zero is tracked in the new issue #370.

### Options Considered

| Option | Verdict |
|--------|---------|
| Render `NULLIF(<right>, 0)` | Rejected: NULL is the wrong answer seen in `0/0`, it conflates zero and NULL divisors, and it silences the loud `x/0` failure |
| Widen the `is_nan()` check at emit to `!is_finite()` | Rejected: the emit boundary cannot tell a computed non-finite from a stored one, and it misses the predicate case |
| Decline `FLOAT_DIV` entirely | Rejected: withholds the truncation fix and regresses scalar-over-aggregate decomposition, which needs `SUM(x) / COUNT(*)` |
| One blanket tracked-exception issue | Rejected: #246 is a projected-value divergence and #370 a predicate row-count divergence on different code paths |

### Consequences

#246's reach widens from `DOUBLE` numerators to integer and decimal numerators. #370 covers the single-table predicate position and the broadcast-join leg.

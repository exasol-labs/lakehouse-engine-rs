# Decisions: fix-210-string-functions-type-blind

## ADR: Decline pushdown, never cast, for BOOLEAN/DOUBLE/TIMESTAMP string-position arguments

**ID:** string-fn-decline-noncoercible-types-not-cast
**Plan:** fix-210-string-functions-type-blind
**Status:** Accepted

### Context

Exasol string functions hard-fail DataFusion planning when a string argument is a column of a type other than VARCHAR, CHAR, DATE, or DECIMAL (issue #210). The Iceberg spec assigns no text form to boolean, double, or timestamp, so Exasol and DataFusion differ: `TRUE` vs `true`, and a space vs `T` separator in timestamps.

### Decision

For any resolvable string-argument type other than VARCHAR, CHAR, DATE, or DECIMAL, the adapter declines the whole tree and Exasol evaluates it. It never casts BOOLEAN, DOUBLE, or TIMESTAMP to VARCHAR.

### Options Considered

| Option | Verdict |
|--------|---------|
| Cast to VARCHAR | Rejected: the text forms diverge, so a loud failure becomes a quiet wrong answer |

### Consequences

This follows the #207 reasoning that an unformattable type falls back to native evaluation. A new string-argument type needs a proven Exasol-faithful text form before it moves from decline to coerce.

## ADR: INSTR/LOCATE beyond two arguments declines pushdown unconditionally on argument type

**ID:** instr-locate-arity-decline-over-type-coerce
**Plan:** fix-210-string-functions-type-blind
**Status:** Accepted

### Context

The translator renders `INSTR` and `LOCATE` from the first two arguments only and drops a third (start position) or fourth (occurrence) argument (issue #228). Coercing the first two arguments would let such a call plan and return a wrong position.

### Decision

`INSTR` and `LOCATE` with more than two arguments decline the whole tree regardless of argument types, including all-VARCHAR calls. The argument classifier has three outcomes: not governed, coerce, decline.

### Options Considered

| Option | Verdict |
|--------|---------|
| Coerce the first two arguments regardless of arity | Rejected: a truncated rendering plans successfully and returns a wrong position |
| Render the dropped arguments | Rejected: separate defect (#228), out of this plan's typing-only scope |

### Consequences

`INSTR(c_varchar, 'b', 3)`, previously a silently wrong pushdown, now falls back to Exasol. Faithful rendering of the dropped arguments stays tracked in #228.

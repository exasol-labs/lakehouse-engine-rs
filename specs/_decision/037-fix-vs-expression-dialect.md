# Decisions: fix-vs-expression-dialect

## ADR: One declaration gates the function_scalar dispatch and drives the sweep table

**ID:** declaration-gates-dispatch-and-sweep
**Plan:** fix-vs-expression-dialect
**Status:** Accepted

### Context

An inline name list in the guarded arm is a second copy of the translated-name set next to the DataFusion arms. A name missing from the list would fall through to DataFusion rendering on the Exasol path without an error.

### Decision

One declaration lists each translated `function_scalar` name with an `ExasolForm`, either `VerbatimCall` or `Shaped`. It gates the dispatch, so an undeclared name is declined in both dialects. It renders the Exasol dialect for `VerbatimCall` names, so arm order no longer carries dialect precedence. It also drives the sweep test. A unit test iterates the declared names to check the verbatim surface.

### Options Considered

| Option | Verdict |
|--------|---------|
| Inline name list in the guarded arm | Rejected: a second copy of the name set with nothing to keep it in sync |
| A flat verbatim-name set with a hand-written sweep table | Rejected: leaves two ways to forget a name, in the set or in the sweep row |
| A per-name enum with exhaustive `match` | Rejected on cost: about 160 lines of boilerplate and a rewrite of about 80 arm patterns in a 3,351-line file, for a compile failure instead of a test failure |

### Consequences

A name added to a DataFusion arm without a declaration row cannot be translated, so the author's test fails at once. A `VerbatimCall` name cannot diverge from what Exasol sent. Only the five non-`function_scalar` node types, matched on the `type` string, remain reviewed by hand.

## ADR: Withdraw the four now-family capabilities rather than re-render them

**ID:** withdraw-now-family-capabilities
**Plan:** fix-vs-expression-dialect
**Status:** Accepted

### Context

`CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP` need Exasol's session and database zones, and neither reaches the scan. The pushdown request carries no zone, the scan spec has no temporal field, the scan opens no connect-back session, and the SDK context exposes no clock or zone. The scan reads its own UTC container clock once per shard, while Exasol's value is statement-constant. Measured live, a pushed `SYSTIMESTAMP` differed from Exasol's by two hours, and a `GROUP BY SYSTIMESTAMP` over two files returned two distinct timestamps.

### Decision

The adapter withdraws `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP` from the advertised capabilities, so Exasol evaluates them. `docs/capabilities.md` lists them under "Handled by Exasol". The names leave the crate's declaration and their DataFusion rendering arms are deleted, so the gate declines all four in both dialects with the standard `unsupported scalar function` error.

### Options Considered

| Option | Verdict |
|--------|---------|
| Accept the divergence and file an issue | Rejected by the human: an advertised capability would return a wrong value |
| Plumb zones and a statement-level anchor into the scan spec over a connect-back call | Not rejected on merit: it is the only route to correct pushdown but is far outside a rendering fix, tracked as issue #263 |
| Withdraw only `SYSDATE` and `SYSTIMESTAMP` | Rejected: all four are wrong on the scan path |

### Consequences

Predicates containing a now-family name are applied by Exasol over returned rows, and three of the four lose select-list pushdown. Other date/time capabilities stay advertised because they take their value from their arguments. The lost pushdown returned wrong values. Restoring it with full time-zone fidelity is issue #263.

## ADR: The declaration is rewritten into a structural, per-name form after a second review round

**ID:** translated-scalar-fns-declaration-gates-dispatch-and-sweep
**Plan:** fix-vs-expression-dialect
**Status:** Accepted

### Context

With a flat name set and a hand-written sweep table, a new arm such as `SUBSTRING`, `NVL`, or `DATE_BIN` could escape both and render DataFusion SQL on the Exasol path, because nothing derived the table or set from the DataFusion arms.

### Decision

A human chose a structural fix: the flat set becomes one per-name declaration that gates the dispatch, renders the Exasol dialect for declared names ahead of every per-name arm, and is iterated by the sweep test, so a declared name with no fixture fails by name.

### Options Considered

| Option | Verdict |
|--------|---------|
| Soften the plan's claims to match the flat design | Rejected: leaves the gap where a new arm escapes both the set and the table |
| A compile-enforced per-name enum | Rejected on cost (see `declaration-gates-dispatch-and-sweep`) |

### Consequences

An undeclared name is unreachable, a declared name cannot escape the sweep, a `VerbatimCall` name cannot diverge from Exasol, and a `Shaped` name has the sweep row the test forces. The declaration was split out as a no-op refactor step, proved by the unchanged suite.

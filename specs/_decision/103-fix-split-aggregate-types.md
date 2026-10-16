# Decisions: fix-split-aggregate-types

## ADR: A split-aggregate column takes an exact Exasol type, or the split declines

**ID:** split-aggregate-columns-take-exact-types-or-decline
**Plan:** fix-split-aggregate-types
**Status:** Accepted
**Supersedes:** char-group-key-blank-pad-before-grouping

### Context

Exasol sends a type only for each select-list item and each `CAST` node. A nested aggregate and an unprojected group key arrive untyped, and a guessed type returns errors, lost digits, or split groups (#476).

### Decision

Every partial and group-key column of a split aggregate takes its type from Exasol's own declaration or from the source column type. When neither exists, the adapter routes the request to the qualified wrapper, where Exasol evaluates the expression. Group keys travel typed, and the merge groups on the typed values.

### Options Considered

| Option | Verdict |
|--------|---------|
| Plan the expression in DataFusion and map its Arrow type | Rejected: DataFusion types expressions differently from Exasol, for example a decimal division or `DATE_TRUNC` over a date |
| Copy Exasol's typing rules into the adapter | Rejected: the rules depend on literal values, the session, and the Exasol version, so the copy would drift |
| Keep a fixed default type | Rejected: any fixed type is wrong for text, dates, or exact decimals |

### Consequences

- A nested `MIN`, `MAX`, or `SUM` over an expression, and an unprojected expression key, run through the wrapper, which ships the scanned rows to Exasol. Selecting the same aggregate on its own keeps the split path.
- A `CHAR(n)` key needs no DataFusion-side pad: Exasol pads the emitted key, and the scan cuts blank excess beyond `n` before the emit.
- If Exasol sends a type for every expression node, the same rule keeps those requests on the split path without new typing code.

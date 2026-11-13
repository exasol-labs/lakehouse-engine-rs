# Decisions: fix-scan-second-fraction

## ADR: The adapter copies a typing rule only when it depends on exact argument types alone and holds on every supported version

**ID:** copy-typing-rule-only-when-exact-and-version-stable
**Plan:** fix-scan-second-fraction
**Status:** Accepted
**Supersedes:** split-aggregate-columns-take-exact-types-or-decline

### Context

A split-aggregate column needs the exact type Exasol gives it, and Exasol's expression typing depends on literal values, the session, and the Exasol version. The typing of `SECOND` over a `TIMESTAMP(p)` argument depends on the argument's precision alone.

### Decision

A split-aggregate column takes the type Exasol declares, the source column type, or a copied typing rule, or the split declines. The adapter copies a rule only when it depends on nothing but the exact types of its arguments and holds on every supported Exasol version.

### Options Considered

| Option | Verdict |
|--------|---------|
| Plan the expression in DataFusion and map its Arrow type | Rejected: DataFusion types `date_part` and decimal quotients differently from Exasol |
| Copy Exasol's general expression typing | Rejected: the rules depend on literal values, the session, and the version, so the copy drifts |
| Keep a fixed default type | Rejected: any fixed type is wrong for text, dates, or exact decimals |

### Consequences

- The feature specs list the qualifying rules, so a new rule that meets the policy needs no new ADR. Two qualify: an aggregate's result type and `DECIMAL(2+p, p)` for `SECOND` over `TIMESTAMP(p)`.
- An expression that no rule types still runs in the wrapper when it is a nested aggregate argument or an unprojected group key.

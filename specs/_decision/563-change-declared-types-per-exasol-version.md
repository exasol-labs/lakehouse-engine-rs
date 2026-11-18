# Decisions: pushdown-statement-context

## ADR: Exact types follow the Exasol version of the pushdown

**ID:** exact-types-follow-the-exasol-version
**Plan:** pushdown-statement-context
**Status:** Accepted
**Supersedes:** declared-types-from-typed-pass

### Context

Exasol 8 and 2025 type `TO_TIMESTAMP`, `ADD_HOURS` and `ADD_MINUTES` over a DATE, a TIMESTAMP literal, `SECOND` over a DATE, and a CASE over a DATE and a TIMESTAMP differently. Without an exact type, a split aggregate over these forms runs in the qualified wrapper (#563).

### Decision

One type inference serves the scan and every declared type, and it derives the exact type the Exasol version of the pushdown gives an expression. The adapter reads the version from its UDF context. On a version string with no numeric major, it derives an exact type only where Exasol 8 and 2025 agree.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive only the types Exasol 8 and 2025 share | Rejected: common date forms lose the split path on every version |
| A virtual schema property that names the version | Rejected: the UDF context reports the version on every request |

### Consequences

- An E2E test checks each version-dependent rule against native Exasol on Exasol 8 and 2025.
- A new Exasol release that changes a rule needs a new version arm.

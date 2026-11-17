# Decisions: refactor-scan-typing-declared-types

## ADR: Declared types come from the typed pass

**ID:** declared-types-from-typed-pass
**Plan:** refactor-scan-typing-declared-types
**Status:** Accepted
**Supersedes:** copy-typing-rule-only-when-exact-and-version-stable

### Context

A split-aggregate column, an unprojected group key, and a qualified-wrapper join item need the exact type Exasol gives them, and Exasol states none for a nested node. The typed pass already derives exact types for the scan, and a second rule set for declared types drifts from it (#635).

### Decision

One type inference serves the scan and every declared type. It derives an exact type only where Exasol 8 and 2025 give it alike. A split aggregate without an exact type for a partial column or group key runs in the qualified wrapper.

### Options Considered

| Option | Verdict |
|--------|---------|
| A separate rule set for declared types | Rejected: a rule added to one set does not reach the other |
| Copy Exasol's general expression typing | Rejected: a type that depends on the session or the version breaks the emit or the merge |

### Consequences

- A wrong exact rule breaks the emit or the merge, not only the pushdown. The feature specs list each exact rule, and an E2E test checks it against native Exasol on both versions.
- A grouped `ORDER BY` on an aggregate absent from the select list keeps the partial/merge path when it matches a select-list plan, and otherwise routes to the qualified single-table wrapper, not to an error.

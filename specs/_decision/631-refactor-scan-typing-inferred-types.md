# Decisions: refactor-scan-typing-inferred-types

## ADR: The typed pass infers Exasol types, exact only where verified live

**ID:** typed-pass-infers-exasol-types
**Plan:** refactor-scan-typing-inferred-types
**Status:** Accepted

### Context

The typed pass kept its own coarse copy of the Exasol type, so each new rule meant adding precision or other parameters back into it (#631). A node's type decides text conversion, timestamp units, literal floors, and integer text width.

### Decision

The pass derives each node's type as an `ExasolType` when that type is known, as a kind with unknown parameters (text, integer, number, timestamp), as NULL, or as unknown. A rule returns an exact type only after a live Exasol check confirms it. Otherwise it returns a kind. Each scalar function has exactly one result rule.

Timestamp precision has one home in vs-expression. `TimestampUnit` names the Arrow unit that holds a given number of fractional digits. `TimestampLiteral` gives a literal's type precision, its significant digits, and its floor.

### Consequences

- Inferring a new type adds a rule, not a variant.
- A kind without known parameters takes the conservative path: the widest integer width, or declining.
- A rule whose result differs between Exasol 8 and 2025 derives no exact type: `TO_TIMESTAMP` stays a timestamp kind, and a DATE brought together with a TIMESTAMP stays unknown.

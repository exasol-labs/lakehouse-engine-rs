# Decisions: refactor-typed-scan-translator

## ADR: The scan never converts session-formatted types to or from text

**ID:** scan-never-converts-session-formatted-text
**Plan:** refactor-typed-scan-translator
**Status:** Accepted

### Context

Exasol writes and reads DATE, TIMESTAMP, DOUBLE, and non-integer DECIMAL text by the session's NLS settings, which the pushdown request does not carry. The adapter decided per call site whether the scan may compute an expression, and paths that skipped the type checks returned wrong text or wrong rows (#587).

### Decision

One `ScanTranslator`, holding the request's column types, renders every expression the scan computes. It declines any conversion between text and these types, explicit or implicit, so Exasol performs it. Integer and BOOLEAN text, whose form no session setting changes, stay in the scan.

### Options Considered

| Option | Verdict |
|--------|---------|
| A scan function that reproduces Exasol's text form (`exa_to_varchar`) | Rejected: the form depends on the session, so it matches only the default settings |
| Read the session's NLS settings into the scan | Rejected for now: the scan could no longer decline and would have to reproduce every format a session can set |
| Let Exasol convert constant text before the scan | Deferred to a follow-up |

### Consequences

- A filter such as `date_col = '2024-01-01'` runs in Exasol, prunes no files, and in a join loses the broadcast plan. A typed literal (`DATE '2024-01-01'`) keeps the scan filter.
- A new type rule is written once, in the translator, instead of per path.

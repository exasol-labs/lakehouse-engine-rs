# Decisions: refactor-neutralize-scan-spec

## ADR: The Delta column-mapping MODE is not carried in the scan spec at all

**ID:** delta-column-mapping-mode-not-carried-on-wire
**Plan:** `refactor-neutralize-scan-spec`
**Status:** Accepted

### Context

Issue #342 gives per-column binding data a neutral home but none to the Delta column-mapping mode. The mode decides which binding key each field gets, so a home had to be chosen.

### Decision

The column-mapping mode gets no neutral home. Plan time consumes it to decide each field's binding key, and the scan side reads the key and never a mode.

### Options Considered

| Option | Verdict |
|--------|---------|
| Carry the mode on the common scan spec | Rejected: a second home for one decision that can disagree with the keys |
| Keep a minimal Delta-named block holding only the mode | Rejected: issue #342 exists to remove that block |

### Consequences

Each field carries one binding key, or none for identity binding, with nothing on the wire that can drift from it. A future mode resolves to an existing key shape.

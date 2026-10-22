# Decisions: refactor-neutralize-scan-spec

## ADR: The Delta column-mapping MODE is not carried in the scan spec at all

**ID:** delta-column-mapping-mode-not-carried-on-wire
**Plan:** `refactor-neutralize-scan-spec`
**Status:** Accepted

### Context

The Delta column-mapping mode decides which binding key each field gets. The format-neutral scan spec holds per-column binding keys but has no place for the mode.

### Decision

The column-mapping mode gets no neutral home. Plan time consumes it to decide each field's binding key, and the scan side reads the key and never a mode.

### Options Considered

| Option | Verdict |
|--------|---------|
| Carry the mode on the common scan spec | Rejected: a second home for one decision that can disagree with the keys |
| Keep a minimal Delta-named block holding only the mode | Rejected: reintroduces a format-specific block in the neutral spec |

### Consequences

- Each field carries one binding key, or none for identity binding, so nothing on the wire can drift from it.
- A future mode resolves to an existing key shape.

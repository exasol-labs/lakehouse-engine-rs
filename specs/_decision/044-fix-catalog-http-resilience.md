# Decisions: fix-catalog-http-resilience

## ADR: Catalog HTTP requests time out on silence, not on total duration

**ID:** catalog-http-times-out-on-silence
**Plan:** fix-catalog-http-resilience
**Status:** Accepted

### Context

The adapter sends its catalog requests to catalogs it does not control, over links of unknown speed. A `loadTable` answer grows with the table's metadata, so its transfer time has no fixed upper limit.

### Decision

An attempt of a catalog HTTP request fails when 10 seconds pass without the answer's headers or without the next part of its body. A retry starts only when its answer is due within 30 seconds of the first attempt. A body that keeps arriving is read to its end.

### Options Considered

| Option | Verdict |
|--------|---------|
| A read timeout per attempt and a 30-second retry window | ✓ Chosen: it tells a hung catalog from a slow transfer and leaves room to retry a stalled attempt |
| A bound on the whole request, body included | ✗ Rejected: it fails every attempt of a large answer on a slow link, although the catalog answers |
| A whole-request bound that a virtual schema property overrides | ✗ Rejected: any default still fails such an answer, and each affected customer must find and set the property |

### Consequences

- A hung catalog fails the request within 30 seconds, retries included.
- A catalog that sends body parts less than 10 seconds apart is never cut off.

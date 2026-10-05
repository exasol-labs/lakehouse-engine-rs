# Decisions: fix-191-order-by-offset

## ADR: Advertise LIMIT_WITH_OFFSET and render the offset in the same commit

**ID:** advertise-limit-with-offset-and-render-atomically
**Plan:** fix-191-order-by-offset
**Status:** Accepted

### Context

`ORDER BY ... LIMIT n OFFSET m` returns ranks 1..n instead of m+1..m+n (issue #191). While `LIMIT_WITH_OFFSET` is unadvertised, no request field carries the offset. Advertising it without rendering the offset leaves Exasol applying neither bound, which makes the result unbounded.

### Decision

The adapter advertises `LIMIT_WITH_OFFSET` and renders the offset in the declined row-scan wrapper, the grouped merge, and the join wrapper in the same commit. The capability flag flips last.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reconstruct the offset from the request without advertising | Rejected: no request field carries it, verified on the wire |
| Flip the flag first and fix rendering afterwards | Rejected: the intermediate state returns a still-wrong result |

### Consequences

Capability and rendering cannot land separately. Every commit before the flag flip matches pre-change output.

## ADR: Decline the bounded top-N on any non-zero offset; own the window in the wrappers

**ID:** decline-bounded-topn-on-offset-own-window-in-wrappers
**Plan:** fix-191-order-by-offset
**Status:** Accepted

### Context

Per-shard `LIMIT n OFFSET m` does not compose, because each shard skips its own first m rows. A correct bounded variant needs an n+m per-shard over-fetch and a merge-side window, which is a performance feature.

### Decision

Any non-zero offset declines the bounded per-shard top-N path. Only wrapper SELECTs, which run a global `ORDER BY` over an unbounded fan-out, render the offset, and the offset never reaches a per-shard scan spec or the scan UDF.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fetch n+m rows per shard and window at the merge | Rejected: a performance feature that adds a second window-arithmetic seam |

### Consequences

Offset queries take the unbounded path and lose per-shard row savings. Zero-offset requests are unchanged.

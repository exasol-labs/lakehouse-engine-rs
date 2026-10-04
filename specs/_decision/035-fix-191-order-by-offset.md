# Decisions: fix-191-order-by-offset

## ADR: Advertise LIMIT_WITH_OFFSET and render the offset in the same commit

**ID:** advertise-limit-with-offset-and-render-atomically
**Plan:** fix-191-order-by-offset
**Status:** Accepted

### Context

Issue #191: `ORDER BY … LIMIT n OFFSET m` silently returns ranks 1..n instead of
(m+1)..(m+n). Live verification against the local Docker stack established that while
`LIMIT_WITH_OFFSET` stays unadvertised, no `pushdownRequest` field carries the offset, so no
adapter-side detection can recover it. Flipping only the capability flag, with no rendering
change, left the result unchanged and still wrong: Exasol had stopped applying either bound,
turning a wrongly-unshifted result into a wrongly-unbounded one.

### Decision

Add `"LIMIT_WITH_OFFSET"` to `CAPABILITIES` and land the offset rendering on every reachable
wrapper (the declined row-scan wrapper, the grouped merge, and the qualified/N-scan join
wrapper) at the same commit, sequenced so the capability flag flips last.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise and render atomically, flag last | ✓ Chosen — the advertisement is the only mechanism that surfaces the offset; flag-last ordering keeps the tree green at every task boundary because the offset extractor returns 0 until the flag flips |
| Leave the capability unadvertised and reconstruct the offset from the request | ✗ Rejected — no `pushdownRequest` field carries it while unadvertised, verified on the wire |
| Flip the flag first, fix rendering afterwards | ✗ Rejected — verified live that the intermediate state returns an unchanged, still-wrong result because Exasol then applies neither bound |

### Consequences

The capability and its rendering cannot land as separate commits; every commit up to the
flag flip is byte-identical to pre-change output, and the flag flip is the single activating
change.

## ADR: Decline the bounded top-N on any non-zero offset; own the window in the wrappers

**ID:** decline-bounded-topn-on-offset-own-window-in-wrappers
**Plan:** fix-191-order-by-offset
**Status:** Accepted

### Context

The bounded per-shard top-N fast path computes each shard's own local `ORDER BY … LIMIT n`.
A per-shard `LIMIT n OFFSET m` is not composable: each shard would skip its OWN first `m`
rows, so the union of per-shard windows is not the global window. Making it correct would
require an `n + m` per-shard over-fetch and a windowing step at the merge — a performance
feature, not a correctness fix.

### Decision

Any non-zero offset declines the bounded per-shard top-N path unconditionally. The offset is
rendered only by wrapper SELECTs that already run a self-contained global `ORDER BY` over an
unbounded fan-out, where the window is exact by construction.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline the bounded path on any non-zero offset; fix correctness only in the wrappers | ✓ Chosen — correctness bug fix, smallest and lowest-risk diff; a bounded offset variant is possible future work |
| Extend the per-shard TopK to fetch `n + m` rows and window at the merge | ✗ Rejected — a performance feature, not required to fix the correctness bug, and adds a second window-arithmetic seam |

### Consequences

Every offset-carrying ordered query takes the unbounded declined path rather than the
bounded top-N, trading per-shard row-count savings for correctness on this one shape; the
matched bounded path stays byte-identical for every offset-free or zero-offset request.

The offset never crosses into a per-shard scan spec or the scan UDF.


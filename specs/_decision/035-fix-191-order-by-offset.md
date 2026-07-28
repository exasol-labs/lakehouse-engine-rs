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

## ADR: ScanSpec gains no offset field

**ID:** scanspec-carries-no-offset-field
**Plan:** fix-191-order-by-offset
**Status:** Accepted

### Context

Every offset the adapter honours must be applied over the complete, globally-ordered fan-out
result — a per-shard OFFSET would skip a different row set on every shard and cannot compose
into a global window (the same reasoning behind declining the bounded top-N on any non-zero
offset).

### Decision

The offset never crosses into a per-shard scan spec or the scan UDF. `ScanSpec` keeps only
its existing `limit: Option<u64>` field; the offset is applied exclusively by an Exasol-side
wrapper SELECT over the complete fan-out.

### Options Considered

| Option | Verdict |
|--------|---------|
| No offset field on `ScanSpec`; apply the offset only in the wrapper SQL | ✓ Chosen — correct by construction, and keeps the scan-spec wire shape and the scan UDF untouched by this plan |
| Carry the offset per shard alongside the limit | ✗ Rejected — incorrect by construction: a per-shard OFFSET skips a different row set on every shard |

### Consequences

The scan UDF and the scan-spec wire contract require no change for this plan; every offset
this plan adds support for is visible only in the Exasol-side SQL the adapter generates.

## ADR: The bounded path's two unreachability guards are a conditional chain, not independent guards

**ID:** bounded-topn-offset-guards-are-a-conditional-chain
**Plan:** fix-191-order-by-offset
**Status:** Accepted

### Context

Plan review (round 1, UNSTATED_ASSUMPTION) showed that the matched bounded top-N's
unreachability with an offset had been argued from Exasol's SQL grammar alone — which
establishes only that the user query has an `ORDER BY` somewhere, not that Exasol forwards
it as `pushdownRequest.orderBy`. The declined-path guard (`detect_topn`) and the
per-shard-limit-withholding guard (`effective_limit`) both read the same
`has_order_by = order_by_present(pushdown_req)` binding, so a request with `limit.offset >
0` and `orderBy` absent or empty would leave `effective_limit` non-null and render a bare
`LIMIT n` with no ORDER BY and no OFFSET — issue #191 silently unfixed.

### Decision

Ran a second live capture — 24 query shapes through `EXPLAIN VIRTUAL` against the Docker
stack — to establish the true reachability. Every one of 11 offset-carrying shapes pushed a
non-empty `orderBy`; where Exasol cannot delegate the ordering it withholds `limit`
entirely. The two guards hold as a CHAIN on that live-verified `offset > 0 ⇒
order_by_present` invariant, not as independent guards, and the plan and its spec deltas now
state the true reason instead of the grammar-only argument.

### Options Considered

| Option | Verdict |
|--------|---------|
| Re-derive reachability from a live capture and correct the stated reason | ✓ Chosen — the code-only argument was incomplete; the live capture is the only source that settles what Exasol actually forwards |
| Trust the grammar-only argument as sufficient | ✗ Rejected — plan review showed it does not establish what the adapter receives, only what the user query contains |

### Consequences

The plan's Background section now documents the offset-implies-ordering invariant explicitly,
with the capture table as evidence, and every "unreachable with an offset" claim in the
affected specs cites this invariant rather than the grammar alone.

## ADR: Every pinned unreachability claim needs a live end-to-end backstop, not only a debug_assert!

**ID:** pinned-unreachability-needs-live-e2e-backstop
**Plan:** fix-191-order-by-offset
**Status:** Accepted

### Context

Plan review (round 2, NFR_IGNORED) found that pinning the matched bounded top-N's
unreachability-with-an-offset using only a `debug_assert!` leaves no backstop in production:
`debug_assert!` compiles out of the release-profile `.so` the adapter ships as. If a future
Exasol build stopped withholding `limit` on an ordering it cannot delegate, the guard chain
would break silently, reopening issue #191 with every existing test still green, because none
of them exercises that shape.

### Decision

Every site pinned as unreachable with an offset — the matched bounded top-N's row-scan SQL,
the single-group aggregate merge, and the lone-`COUNT(DISTINCT)` wrapper — gets a live
end-to-end canary in addition to its `debug_assert!`: an `sqlCode 42000` rejection assertion
for the two one-row merge sites, and an unrenderable-ordering canary
(`ORDER BY HASH_MD5(id) LIMIT 5 OFFSET 2`) for the bounded-top-N site, each of which fails
loudly if the underlying Exasol behavior the invariant depends on ever changes.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pair every `debug_assert!` with a live e2e canary | ✓ Chosen — the release-mode `.so` runs with `debug_assert!` compiled out, so only a live test guards production behavior |
| Rely on `debug_assert!` alone | ✗ Rejected — proven to guard nothing in the release-profile build the adapter ships as |

### Consequences

The standard is now normative across the plan rather than task-local: every future claim
that a render site is unreachable with a given input must carry a live end-to-end assertion,
not only a debug-mode guard.

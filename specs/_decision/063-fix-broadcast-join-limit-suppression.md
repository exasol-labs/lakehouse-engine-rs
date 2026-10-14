# Decisions: fix-broadcast-join-limit-suppression

## ADR: The post-join cap is a field of `JoinSpec`, so a join-less spec cannot express one

**ID:** join-post-limit-lives-in-join-block
**Plan:** fix-broadcast-join-limit-suppression
**Status:** Accepted

### Context

A bare `LIMIT` over a broadcast-eligible inner join needs a per-shard cap applied after the join, never to the pre-join fact scan, which returns wrong rows without error. The fallback leg spec has no join and must never carry that cap, but the shared scan limit field is also written on join-less specs by other in-crate paths.

### Decision

The post-join cap is a field of the join block, not of the shared scan spec. A spec without a join block has no field on which a post-join cap can be set, so the guarantee holds by type.

### Options Considered

| Option | Verdict |
|--------|---------|
| Bare write of the shared scan limit at the broadcast construction site | Rejected: other in-crate paths legally write the same field on join-less specs |
| Limit parameter on the shared fan-out helper | Rejected: a convention a future caller can get wrong |
| Paired constructor or setter guarded by a debug assertion | Rejected: binds only callers that use it, and the assertion holds only in debug builds |
| Narrow field visibility to the crate | Rejected: the fallback builder is in-crate, and no visibility level closes the gap |
| DataFusion `.limit()` on the joined DataFrame | Rejected: the rendered SQL `LIMIT` is already correct and DataFusion does not push a fetch into inner-join inputs |

### Consequences

The compiler enforces the pre-join/post-join distinction. The new wire field is additive and backward compatible, and existing join struct literals needed updating. This reverses an earlier plan decision that relied on the shared limit field plus a paired constructor.

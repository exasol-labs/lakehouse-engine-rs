# Decisions: fix-broadcast-join-limit-suppression

## ADR: The post-join cap is a field of `JoinSpec`, so a join-less spec cannot express one

**ID:** join-post-limit-lives-in-join-block
**Plan:** fix-broadcast-join-limit-suppression
**Status:** Accepted

### Context

A bare `LIMIT` over a broadcast-eligible inner join needs a per-shard cap applied after the join. Applied to the pre-join fact scan, it returns wrong rows without error. The fallback leg has no join and must never carry the cap.

### Decision

The post-join cap is a field of the join block, not of the shared scan spec. A spec without a join block has no field on which a post-join cap can be set, so the guarantee holds by type.

### Options Considered

| Option | Verdict |
|--------|---------|
| Write the shared scan limit when building the broadcast spec | Rejected: other paths legally write the same field on join-less specs |
| Limit parameter on the shared fan-out helper | Rejected: a convention a future caller can get wrong |
| Paired constructor or setter with a debug assertion | Rejected: binds only callers that use it, and only in debug builds |
| Narrow field visibility to the crate | Rejected: the fallback builder is in the crate |
| DataFusion `.limit()` on the joined DataFrame | Rejected: the rendered SQL `LIMIT` is already correct, and DataFusion does not push a fetch into inner-join inputs |

### Consequences

- The compiler enforces the pre-join and post-join distinction.
- The wire field is additive and backward compatible.

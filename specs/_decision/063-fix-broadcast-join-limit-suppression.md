# Decisions: fix-broadcast-join-limit-suppression

## ADR: The post-join cap is a field of `JoinSpec`, so a join-less spec cannot express one

**ID:** join-post-limit-lives-in-join-block
**Plan:** fix-broadcast-join-limit-suppression
**Status:** Accepted

### Context

A bare `LIMIT` over a broadcast-eligible inner equi-join needed a per-shard post-join cap: each
fact-shard's own joined output must truncate at `n`, applied strictly after the node-local join
and its `WHERE`, never to the fact side's pre-join scan — an input-side cap discards fact rows
that would have matched while keeping fact rows that produce zero output, returning wrong rows
with no error. The design therefore required a guarantee that a fallback leg spec — which
constructs no join block and feeds a bare single-table scan — can never carry that cap. The
project's shared `CommonScanSpec.limit` field is written on join-less specs from four other
production paths in the same crate and module tree (the single-table row-scan builder and its
three call sites, plus the grouped-aggregate path's explicit clear), and the same shared fan-out
helper builds both the broadcast spec and the fallback leg spec. Any mechanism built on that one
field — a bare write at the broadcast construction site, a `limit` parameter threaded through the
shared fan-out helper, a paired `ScanSpec::with_broadcast_join` constructor, a
`with_post_join_limit` setter guarded by `debug_assert!(join.is_some())`, or narrowing
`CommonScanSpec.limit`/`.join` to `pub(crate)` — left the fallback leg builder a legal, uncompiled-
against route to set the same field, because it sits in the same crate and the same module tree
as every legitimate join-less writer. Verification with Serena's `find_referencing_symbols` and a
compiled visibility probe confirmed no field-visibility level closes that gap; it only relocates
it, at a measured cost of roughly 29 out-of-crate test-file migrations for zero protection.

### Decision

Give `JoinSpec` its own `post_join_limit: Option<u64>` field, additive and
`#[serde(default, skip_serializing_if = "Option::is_none")]`. The broadcast builder sets it in the
`JoinSpec` literal it already constructs; the join scan reads the cap from the join block instead
of from `spec.common.limit`. `CommonScanSpec.limit` is never written and never read on the join
path. Because the cap now lives inside the join block, a scan spec that carries no join block has
no field on which a post-join cap could be set — true by type, in-crate and out-of-crate, in debug
builds and in release, with no constructor to route through and no convention to enforce it.

### Options Considered

| Option | Verdict |
|--------|---------|
| `JoinSpec.post_join_limit`, an additive field of the join block | ✓ Chosen — the guarantee holds by type: a join-less spec has no field to carry the cap on |
| Bare write of `CommonScanSpec.limit` at the broadcast construction site | ✗ Rejected — the same field is legally written on join-less leg specs by four other in-crate paths |
| `limit` parameter added to the shared fan-out helper (`join_fan_out_scan_spec`) | ✗ Rejected — a convention a future caller can still get wrong; not enforced by the compiler |
| Paired `ScanSpec::with_broadcast_join(self, join, post_join_limit)` constructor | ✗ Rejected — binds only callers that go through it; `build_side_fan_out_sql` stays free to write the raw field |
| `with_post_join_limit(self, n)` guarded by `debug_assert!(join.is_some())` | ✗ Rejected — holds only in debug builds, and still leaves the pairing implicit in release |
| Narrow `CommonScanSpec.limit`/`.join` to `pub(crate)` | ✗ Rejected — `build_side_fan_out_sql` is in-crate, so a `pub(crate)` field stays freely writable from exactly the site the guarantee is about; disproved by a compiled probe |
| DataFusion `.limit()` call on the joined `DataFrame` in the UDF, instead of the rendered SQL clause | ✗ Rejected — the SQL-level `LIMIT` after the `INNER JOIN`/`WHERE` already renders correctly, and DataFusion's optimizer independently refuses to push a fetch into a non-cross inner join's inputs; no reason to move the mechanism |

### Consequences

The pre-join/post-join distinction is enforced by the compiler rather than by a comment, a
call-signature convention, or a runtime assertion that only fires in debug builds — a fallback leg
builder cannot express a post-join cap because it constructs no `JoinSpec` at all. The cost is one
additive, backward-compatible wire field (a spec written before this change deserializes
unchanged, and an unordered broadcast request's emitted SQL stays byte-identical) plus nine
existing `JoinSpec { … }` struct-literal updates, since Rust cannot default-fill an added field in
a literal. This decision reverses an earlier plan decision that built the same guarantee on
`CommonScanSpec.limit` plus a paired constructor, after verification showed that approach could
not deliver the claimed property.

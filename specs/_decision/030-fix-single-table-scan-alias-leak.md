# Decisions: fix-single-table-scan-alias-leak

## ADR: Single chokepoint in handle_pushdown after the join gate

**ID:** single-table-alias-strip-single-chokepoint
**Plan:** fix-single-table-scan-alias-leak
**Status:** Accepted

### Context

Issue #193: when a query aliases its table (`FROM CUSTOMER c`), Exasol stamps `tableAlias:"C"`
on every `column` node in the pushdown request — even for an unqualified `WHERE C_CUSTKEY <= 3`.
The `crates/vs-expression` renderer honors a present `tableAlias` and emits `"C"."C_CUSTKEY"`,
which does not resolve against the node-local DataFusion scan relation (bare column names only).
A spike proved the failure spans every single-table shape: row-scan/projection, a scalar
expression over a column, single-group aggregate, grouped aggregate, and ordered top-N.

### Decision

Strip `tableAlias` from the whole single-table `pushdown_req` once, immediately after
`detect_join` returns `NotAJoin` and before `filter_json_raw`/`extract_projection`. Every
downstream single-table render site — the filter, select-list expressions, GROUP BY keys,
HAVING, ORDER BY, aggregate arguments, and Iceberg pruning — consumes the stripped tree.

### Options Considered

| Option | Verdict |
|--------|---------|
| One chokepoint in `handle_pushdown`, after the join gate | ✓ Chosen — the single-table scan relation never wants qualified names, so stripping is unconditionally correct at the entry; the filter is rendered upstream of `build_dispatch_sql` and also feeds Iceberg pruning, so this is the one site covering every consumer |
| Strip at each render site individually (filter, select-list, group keys, agg args, topn) | ✗ Rejected — fragile, duplicates logic, and risks missing a shape (the spike found five failing shapes) |

### Consequences

One deep JSON clone of the `pushdownRequest` per query-planning call, not per node or shard,
buys a single provably-total site instead of five per-render-site strips that could each miss a
shape. The join OUTER wrapper's qualified rendering is untouched, since the join path returns
from `plan_join` before this chokepoint.

## ADR: Renderer keeps honoring tableAlias; stripping is the caller's responsibility

**ID:** vs-expression-renderer-keeps-honoring-table-alias
**Plan:** fix-single-table-scan-alias-leak
**Status:** Accepted

### Context

The `vs-expression` renderer emits `"ALIAS"."NAME"` whenever a `column` node carries
`tableAlias`. The join OUTER wrapper (`vs-adapter/pushdown-planning-join-fallback`) depends on
that qualified rendering. The false assumption that the renderer's default single-table path was
already alias-free is what caused #193.

### Decision

The `vs-expression` renderer continues to emit `"ALIAS"."NAME"` whenever a `column` node carries
`tableAlias`. The single-table caller strips the alias before rendering. The
"Bare column reference translates to quoted identifier" scenario and the renderer's doc comment
now document the caller/renderer contract explicitly.

### Options Considered

| Option | Verdict |
|--------|---------|
| Renderer keeps honoring `tableAlias`; caller strips | ✓ Chosen — the join OUTER wrapper needs qualified rendering; documents the contract explicitly so a future planner does not re-litigate the fix in the wrong layer |
| Make the renderer drop `tableAlias` | ✗ Rejected — would break the join OUTER wrapper, which is load-bearing on qualified rendering |

### Consequences

A future single-relation caller must strip `tableAlias` itself before rendering; the renderer
gives no free alias-free guarantee. The contract is now named in both the spec and the source
doc comment, closing the gap that caused #193.

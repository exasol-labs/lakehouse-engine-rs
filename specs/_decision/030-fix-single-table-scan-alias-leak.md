# Decisions: fix-single-table-scan-alias-leak

## ADR: Single chokepoint in handle_pushdown after the join gate

**ID:** single-table-alias-strip-single-chokepoint
**Plan:** fix-single-table-scan-alias-leak
**Status:** Accepted

### Context

When a query aliases its table, Exasol stamps `tableAlias` on every column node, and the renderer emits `"C"."C_CUSTKEY"`. That name does not resolve against the single-table DataFusion scan, which has bare column names (issue #193). Every single-table shape fails.

### Decision

The adapter strips `tableAlias` from the whole single-table request once, right after join detection returns not-a-join. Every downstream render site and Iceberg pruning then consume the stripped request.

### Options Considered

| Option | Verdict |
|--------|---------|
| Strip at each render site | Rejected: fragile, duplicates logic, and can miss a shape (the spike found five failing shapes) |

### Consequences

The strip costs one deep JSON clone per query-planning call. The join OUTER wrapper keeps qualified rendering because the join path returns before the strip.

## ADR: Renderer keeps honoring tableAlias; stripping is the caller's responsibility

**ID:** vs-expression-renderer-keeps-honoring-table-alias
**Plan:** fix-single-table-scan-alias-leak
**Status:** Accepted

### Context

The join OUTER wrapper depends on the renderer emitting qualified `"ALIAS"."NAME"` when a column carries `tableAlias`. The wrong assumption that the single-table path was already alias-free caused #193.

### Decision

The renderer keeps emitting the qualified name, and the single-table caller strips the alias first. The spec scenario and the renderer's doc comment state this contract.

### Options Considered

| Option | Verdict |
|--------|---------|
| Renderer drops `tableAlias` | Rejected: breaks the join OUTER wrapper |

### Consequences

A future single-relation caller must strip `tableAlias` itself, because the renderer gives no alias-free guarantee.

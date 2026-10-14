# Decisions: fix-single-group-scalar-over-aggregate

## ADR: Both layers ship: full partial/merge decomposition AND the projection guard

**ID:** both-layers-ship-decomposition-and-projection-guard
**Plan:** fix-single-group-scalar-over-aggregate
**Status:** Accepted

### Context

An ungrouped aggregate wrapped in a scalar function, such as `ROUND(SUM(l_quantity), 2)`, was rendered into a per-shard projection instead of the partial/merge plan. Each shard returned its own unmerged partial row, a silent wrong answer (issue #194). Routing these shapes to the wrapper is correct but moves the whole scan output into Exasol temp-DB RAM.

### Decision

A decomposable scalar-over-aggregate select item gets full partial/merge decomposition, mirroring the grouped planner. Independently, `project_columns` probes each select item's whole subtree for a `function_aggregate` once, before dispatching on node type, and widens the derived projection on a hit. The guard is the correctness floor for every shape decomposition declines, and decomposition keeps that floor from being the normal outcome.

### Options Considered

| Option | Verdict |
|--------|---------|
| Wrapper route alone | Rejected: forces every scalar-wrapped aggregate onto the slow, RAM-heavy path and creates two planners that answer the same question differently |
| Decomposition alone, no guard | Rejected: every shape decomposition declines falls back into the original bug |

### Consequences

The layers cannot conflict: `build_dispatch_sql` reads the widening signal only in its row-scan arm, which is reached after both aggregate tiers decline. The cost is a larger diff, with a behaviour widening of `ordinary_plans` and a signature change to `build_scan_driving_sql`, covered by a `dispatch_golden` byte-identity gate and a call-site census.

## ADR: Issue #188 is fixed by routing through the existing AggKind tables, never by aliasing in the translator

**ID:** fix-188-via-aggkind-tables-not-translator-alias
**Plan:** fix-single-group-scalar-over-aggregate
**Status:** Accepted

### Context

`VARIANCE` is Exasol's alias for `VAR_SAMP`, and DataFusion has no `variance`, so `ROUND(VARIANCE(c_acctbal), 4)` fails with an invalid-function planning error. A name-alias map in the translator would keep the aggregate running per shard, turning #188's loud error into #194's silent wrong answer.

### Decision

Every nested aggregate's function name resolves through the two existing `AggKind` tables, so `VARIANCE` reaches `VarSamp`. A scalar-wrapped `VARIANCE` scenario and golden fixture assert it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Exasol-to-DataFusion aggregate alias map in `vs-expression` | Rejected: keeps the aggregate executing per shard, giving a silent wrong answer |

### Consequences

Decomposition emits only `(cnt, sum, sum_sq)` partial columns, so no aggregate name is spliced into the DataFusion query text. A statistical aggregate over a rendered expression declines `parse_agg_item`, widens, and Exasol computes it in the wrapper.

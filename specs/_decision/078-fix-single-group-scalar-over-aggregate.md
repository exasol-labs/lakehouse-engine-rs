# Decisions: fix-single-group-scalar-over-aggregate

## ADR: Both layers ship: full partial/merge decomposition AND the projection guard

**ID:** both-layers-ship-decomposition-and-projection-guard
**Plan:** fix-single-group-scalar-over-aggregate
**Status:** Accepted

### Context

An ungrouped aggregate wrapped in a scalar function (`ROUND(SUM(l_quantity), 2)`) was
rendered into a per-shard projection instead of the partial/merge plan, so every shard
computed the whole aggregate over its own files and returned one unmerged partial row per
shard (issue #194) — a silent wrong answer, verified live. The wrapper-only route (routing
every such shape to the qualified single-table wrapper) is correct but materializes the
entire referenced-column scan output in Exasol temp-DB RAM: wrapping a working `SUM` in
`ROUND` would collapse the query from four partial rows to every scanned row, while adding
a `GROUP BY` would restore speed via the grouped planner's existing decomposition — an
incoherent performance model. Decomposition alone was also considered and rejected: it
declines on a `DISTINCT` inner aggregate, a statistical aggregate over a rendered
expression, a demoted non-numeric column, a residual bare column, and an unrenderable
residual, and each of those falls through to `RequestShape::RowScan`, which is exactly
where the bug lives.

### Decision

Fix #194 with full partial/merge decomposition of a decomposable scalar-over-aggregate
select item (mirroring the grouped planner), and independently add a depth-insensitive
nested-`function_aggregate` guard to `project_columns` that widens the derived projection.
The guard is the correctness floor for every shape decomposition declines; the
decomposition is what keeps the floor from being the normal outcome.

### Options Considered

- The wrapper route alone. Correct but forces every scalar-wrapped aggregate onto the slow,
  RAM-heavy path, and creates two planners (grouped vs. single-group) that answer the same
  conceptual question differently.
- Decomposition alone, with no guard. Rejected: it leaves every shape decomposition declines
  routed straight back into the pre-existing bug (`RequestShape::RowScan`).

### Consequences

The two layers cannot conflict: `build_dispatch_sql` reads the widening signal only inside
its `RequestShape::RowScan` arm, reached only after both aggregate tiers decline, so a
decomposable item is classified `SingleGroupAgg` and never consults the signal. The
tradeoff accepted is a larger diff than the wrapper route alone — a behaviour widening of
`ordinary_plans` and a signature change to the `pub` `build_scan_driving_sql` — bought with
a `dispatch_golden` byte-identity gate and an explicit call-site census.

Probe each select-list item's whole subtree for a `function_aggregate` once, before `project_columns` dispatches on node type, and widen the derived projection on a hit.

---

## ADR: Issue #188 is fixed by routing through the existing AggKind tables, never by aliasing in the translator

**ID:** fix-188-via-aggkind-tables-not-translator-alias
**Plan:** fix-single-group-scalar-over-aggregate
**Status:** Accepted

### Context

`VARIANCE` is Exasol's alias for `VAR_SAMP`; DataFusion defines `var`, `var_samp`, and
`var_pop` but no `variance`. A scalar-wrapped `ROUND(VARIANCE(c_acctbal), 4)` reaches
DataFusion planning with the uppercased name spliced verbatim and fails with
`Error during planning: Invalid function 'variance'`. Adding an Exasol→DataFusion aggregate
name-alias map to `vs-expression`'s `function_aggregate` arm was considered, but it would
keep the aggregate executing per shard — converting #188's loud planning error into #194's
silent wrong answer.

### Decision

Resolve every nested aggregate's function name through the two `[(&str, AggKind)]` tables
`vs-adapter/pushdown-agg-sql-consolidation` gives one owner each, so `VARIANCE` → `VarSamp`
is reached rather than re-implemented, asserted with a dedicated scalar-wrapped-`VARIANCE`
scenario and golden fixture.

### Options Considered

- Add an Exasol→DataFusion aggregate name-alias map directly to `vs-expression`'s
  `function_aggregate` arm.

### Consequences

Decomposition emits only `(cnt, sum, sum_sq)` sufficient-statistic partial columns, so no
aggregate function name is spliced into the DataFusion query text at all — the alias bug
closes by construction. The floor covers the residue: a statistical aggregate over a
rendered expression declines `parse_agg_item`, widens, and is computed natively by Exasol in
the wrapper.

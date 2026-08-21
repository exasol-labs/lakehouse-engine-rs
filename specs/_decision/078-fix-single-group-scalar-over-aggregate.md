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

---

## ADR: The projection guard is a subtree probe before the node-type dispatch, not a per-arm check

**ID:** projection-guard-subtree-probe-not-per-arm
**Plan:** fix-single-group-scalar-over-aggregate
**Status:** Accepted

### Context

`function_aggregate` was excluded from the pushable projection set only at the TOP level.
`project_columns`'s `function_scalar`-family arm called `render_expression_safe`, whose
`function_aggregate` arm renders a nested aggregate verbatim as SQL text — deliberately,
since the grouped merge substitution depends on that arm — so a nested aggregate reached
the per-shard `EMITS` clause undetected. Adding a nested-aggregate check inside only the
`function_scalar`-family arm would leave every other pushable arm — the cast/extract/case
node types and the eleven predicate node types, several of which already recurse into an
aggregate — to be remembered individually, and would leave the next arm added unguarded.

### Decision

Probe each select-list item's whole subtree for a `function_aggregate` once, before
`project_columns` dispatches on node type, and widen the derived projection on a hit.

### Options Considered

- A nested-aggregate check inside the `function_scalar`-family arm only. Rejected: leaves
  every other pushable arm unguarded today and leaves future arms unguarded by default.

### Consequences

One decision site replaces fourteen. It also subsumes the existing unknown-node arm's
handling of a top-level `function_aggregate` — that item widened before and widens now,
byte-identically — so the rule `vs-adapter/pushdown-planning-selectlist-expressions`
already states ("the pushable node-type set SHALL be exactly the set the translator
renders MINUS `function_aggregate`") becomes true at every depth rather than only at the
root.

---

## ADR: The shared quartet moves to a new scalar_over_agg submodule, not to support.rs and not widened in place

**ID:** scalar-over-agg-new-submodule-not-support-rs
**Plan:** fix-single-group-scalar-over-aggregate
**Status:** Accepted

### Context

`sentinelize_aggregates`, `sentinel_column_node`, `agg_sentinel_token`,
`classify_scalar_over_aggregate`, `render_scalar_over_merge`, and `fold_aggregate_plan`
were private to `adapter/pushdown/grouped_agg.rs`, and the single-group planner needed the
same mechanism. Widening them to `pub(super)` in place would leave the mechanism owned by
one of its two consumers, making the single-group planner depend on the grouped planner for
a decision neither owns. Moving them to `support.rs` — the established home for a
cross-sibling primitive per `vs-adapter/pushdown-agg-sql-consolidation` — was considered but
these six form one cohesive, nameable responsibility that reads as a module rather than as
loose helpers, and `support.rs` is already the crate's largest catch-all.

### Decision

Extract the six primitives from `grouped_agg.rs` into `adapter/pushdown/scalar_over_agg.rs`
at `pub(super)` visibility, with a sibling `scalar_over_agg_tests.rs`.

### Options Considered

- Widen the six to `pub(super)` in place inside `grouped_agg.rs`.
- Move them to `support.rs`.

### Consequences

`vs-adapter/pushdown-module-structure` records the submodule list as "a design decision
recorded in the plan, not a normative contract", so the new submodule needs no spec delta
and the façade is untouched. Copying the quartet into `single_group_agg.rs` instead would
have been the back-door leakage the design philosophy warns about: two modules
independently assuming the same sentinel token format and the same decline rules, with
nothing enforcing agreement.

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

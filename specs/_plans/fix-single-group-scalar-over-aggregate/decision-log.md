# Decision Log: fix-single-group-scalar-over-aggregate

## Interview

**Q:** Which fix direction should the plan target — (A) route scalar-wrapped single-group
aggregates to the existing qualified single-table wrapper (mirrors the already-implemented
`RequestShape::GroupByWrapper` / COUNT(DISTINCT)-Case-2/3 decline pattern; simple,
consistent, but forfeits the partial/merge optimization for this query shape), (B) full
decomposition mirroring the grouped-aggregate scalar-over-aggregate mechanism (preserves
partial/merge parallelism, more novel for the ungrouped case), or (C) let planner-agent
decide after investigation?

**A:** (C) — planner-agent owns the call, based on the feasibility findings (the reusable
decomposition primitives are already generic over "list of partial columns", not GROUP
BY-specific). Document the chosen direction and its tradeoff explicitly. Lean toward
whichever a senior engineer on this codebase would ship, given the mission's bias toward
preserving the partial/merge optimization where it is cheap, but do not force decomposition
if the investigation shows it is substantially riskier or larger than the wrapper route for
no strong benefit.

**Q:** Should this plan also close out #188 (scalar-wrapped VARIANCE alias-mapping planning
error), or stay scoped strictly to #194?

**A:** Include #188. Same root mechanism — scalar-wrapping bypasses the aggregate-aware
path. Whichever fix direction is chosen for #194 must be verified to also cover
scalar-wrapped VARIANCE/STDDEV/statistical-aggregate aliases as part of this plan's test
matrix. Close #188 alongside #194 if the chosen fix resolves it — verify, don't assume.

## Design Decisions

### [1] Both layers ship: full partial/merge decomposition AND the projection guard

- **Decision:** Fix #194 with full partial/merge decomposition of a decomposable
  scalar-over-aggregate select item (mirroring the grouped planner), and independently add a
  depth-insensitive nested-`function_aggregate` guard to `project_columns` that widens the
  derived projection. The guard is the correctness floor for every shape decomposition
  declines; the decomposition is what keeps the floor from being the normal outcome.
- **Alternatives:** The wrapper route alone (direction A). Rejected as the *whole* answer,
  though it is retained as the floor. Correct but it materializes the entire
  referenced-column scan output in Exasol temp-DB RAM — this project has already measured
  that the transparent-VS path always buffers emit-UDF scan output. `SELECT
  ROUND(SUM(l_quantity), 2) FROM LINEITEM` would go from four partial rows to every scanned
  row, so wrapping a working `SUM` in `ROUND` would collapse the query's performance, and
  adding a `GROUP BY` would restore it — since the grouped path already decomposes. Two
  planners would then give two different answers to one question.
  Decomposition alone was also rejected: it declines on a `DISTINCT` inner aggregate, a
  statistical aggregate over a rendered expression, a demoted non-numeric column, a residual
  bare column, and an unrenderable residual — each of which falls through to
  `RequestShape::RowScan`, which is where the bug lives.
- **Rationale:** The two layers are the correctness and the performance halves of one fix,
  and they cannot conflict: `build_dispatch_sql` reads the widening signal only inside its
  `RequestShape::RowScan` arm, which is reached only after both aggregate tiers decline.
  Decomposition is mostly wiring because the four reusable primitives carry no GROUP BY
  state — they operate on a `Vec<AggregatePlan>` keyed by structural equality — and the
  single-group merge already consumes the same `merge_select_items` formulas the grouped
  merge does.
- **Tradeoff accepted:** A larger diff than the wrapper route, including a behaviour
  widening of `ordinary_plans` and a signature change to the `pub` `build_scan_driving_sql`.
  Bought with a `dispatch_golden` byte-identity gate and an explicit call-site census.
- **Promotes to ADR:** yes

### [2] Issue #194's own "Decided approach" is superseded, not implemented

- **Decision:** Do NOT add a decline in `parse_agg_item`. The issue's stated fix —
  "decline classification in `parse_agg_item` → fall through to `RequestShape::RowScan`" —
  describes the current behaviour, which is the bug.
- **Alternatives:** Implement the issue as written. Rejected: `detect_aggregates` already
  requires every select-list item to be literally `function_aggregate`, so a
  `function_scalar` item already declines and `classify_request_shape` already yields
  `RowScan`. `parse_agg_item` is never reached for `ROUND(SUM(x), 2)`.
- **Rationale:** The defect is one layer down. `project_columns`'s `function_scalar` arm
  renders the nested aggregate successfully via `render_expression_safe` — whose
  `function_aggregate` arm exists deliberately, to serve the grouped merge substitution — so
  the widening signal stays `false` and the `RowScan` arm never routes to the wrapper. The
  expression reaches the per-shard `EMITS` clause.
- **Promotes to ADR:** no

### [3] The projection guard is a subtree probe before the node-type dispatch, not a per-arm check

- **Decision:** Probe each select-list item's whole subtree for a `function_aggregate` once,
  before `project_columns` dispatches on node type, and widen on a hit.
- **Alternatives:** Add a nested-aggregate check inside the `function_scalar`-family arm
  only. Rejected: it leaves each of the other pushable arms — the cast/extract/case node
  types and the eleven predicate node types, several of which already recurse into an
  aggregate — to be remembered individually, and leaves the next arm added unguarded.
- **Rationale:** One decision site instead of fourteen. It also subsumes the existing
  unknown-node arm's handling of a top-level `function_aggregate` — that item widened before
  and widens now, byte-identically — so the rule
  `vs-adapter/pushdown-planning-selectlist-expressions` already states ("the pushable
  node-type set SHALL be exactly the set the translator renders MINUS `function_aggregate`")
  becomes true at every depth rather than only at the root.
- **Promotes to ADR:** yes

### [4] The shared quartet moves to a new `scalar_over_agg` submodule, not to `support.rs` and not widened in place

- **Decision:** Extract `sentinelize_aggregates`, `sentinel_column_node`,
  `agg_sentinel_token`, `classify_scalar_over_aggregate`, `render_scalar_over_merge`, and
  `fold_aggregate_plan` from `grouped_agg.rs` into `adapter/pushdown/scalar_over_agg.rs` at
  `pub(super)` visibility, with a sibling `scalar_over_agg_tests.rs`.
- **Alternatives:** Widen the six to `pub(super)` in place — rejected, it leaves the
  mechanism owned by one of its two consumers, and the single-group planner would then
  depend on the grouped planner for a decision neither owns. Move them to `support.rs` —
  the established home for a cross-sibling primitive per
  `vs-adapter/pushdown-agg-sql-consolidation`, but rejected because these six are one
  cohesive, nameable responsibility that reads as a module rather than as loose helpers, and
  `support.rs` is already the crate's largest catch-all.
- **Rationale:** `vs-adapter/pushdown-module-structure` records the submodule list as "a
  design decision recorded in the plan, not a normative contract", so a new submodule needs
  no spec delta and the façade is untouched. Copying the quartet into
  `single_group_agg.rs` would be the back-door leakage the design philosophy warns about:
  two modules independently assuming the same sentinel token format and the same decline
  rules, with nothing enforcing agreement.
- **Promotes to ADR:** yes

### [5] `render_scalar_over_merge` takes the merged expressions as a parameter

- **Decision:** Change the signature so the caller supplies the merged `PARTIAL_*`
  expressions, instead of the function calling `grouped_agg::merge_select_items` itself.
- **Alternatives:** Keep the internal call and have `scalar_over_agg` depend on
  `grouped_agg`. Rejected: `grouped_agg` is a consumer of `scalar_over_agg`, so that edge
  closes a module cycle.
- **Rationale:** The consumer defines the abstraction it needs. Both planners already hold
  the merged expressions — the single-group merge reaches the same formulas through
  `cast_merge_items` — so passing them in costs nothing and leaves the shared module naming
  neither planner.
- **Promotes to ADR:** no

### [6] Deduplicating the folded plan list is a correctness requirement, not an optimization

- **Decision:** Fold nested and top-level aggregates into one plan list deduplicated by
  `AggregatePlan` equality, and record in the spec that this is required for correctness.
- **Alternatives:** Emit one plan per occurrence, treating dedup as a later optimization.
  Rejected on inspection: `render_scalar_over_merge` resolves each nested aggregate to the
  FIRST structurally-equal slot via `plans.iter().position(|p| *p == plan)`, so for `SELECT
  COUNT(*), ROUND(SUM(q) / COUNT(*), 2)` an un-deduplicated `[Count, Sum, Count]` list would
  bind the nested `COUNT(*)` to slot 0 while its `EMITS` column was declared at slot 2.
- **Rationale:** The position-based lookup is only total over a deduplicated list. Writing
  this into the spec stops a future reader from "optimizing away" the fold.
- **Promotes to ADR:** no

### [7] `detect_aggregates` keeps its signature; the fold lives in `ordinary_plans`

- **Decision:** Add `SingleGroupItem::ScalarOverAggregate { select_index, node,
  declared_type }` and widen `ordinary_plans` to fold nested aggregates with dedup, keeping
  `detect_aggregates -> Option<Vec<SingleGroupItem>>`. Add `single_group_plan_types` for the
  per-plan declared types, deriving its alignment from the folded plan list rather than
  re-running the fold.
- **Alternatives:** Return a new `SingleGroupDetection { items, plans, plan_types }`
  mirroring `GroupedAggregateDetection`. That is the closer mirror, and it was rejected only
  on cost: the struct must be `pub` to appear in a `pub fn`'s return type, which changes the
  frozen `crate::adapter::pushdown` façade and both surface probes' stated counts, and
  `vs-adapter/pushdown-module-structure` requires an explicit reviewed spec delta for that.
- **Rationale:** Widening `ordinary_plans` is output-identical for every select list without
  a nested aggregate, so no existing caller changes behaviour and no façade item is added.
  The asymmetry with the grouped planner is deliberate and recorded here so a future
  reader does not read it as an oversight.
- **Tradeoff accepted:** The single-group and grouped detections now have different return
  shapes for the same conceptual job. If a third consumer appears, unify on the struct and
  pay the façade delta then.
- **Promotes to ADR:** no

### [8] Issue #188 is fixed by routing through the existing AggKind tables, never by aliasing in the translator

- **Decision:** Resolve every nested aggregate's function name through the two
  `[(&str, AggKind)]` tables `vs-adapter/pushdown-agg-sql-consolidation` gives one owner
  each, so `VARIANCE` → `VarSamp` is reached rather than re-implemented. Assert it with a
  dedicated scalar-wrapped-`VARIANCE` scenario and golden fixture, not as an assumed side
  effect.
- **Alternatives:** Add an Exasol→DataFusion aggregate name-alias map to
  `vs-expression`'s `function_aggregate` arm. Rejected as actively harmful: it would let the
  aggregate keep executing per shard, converting #188's loud planning error into #194's
  silent wrong answer.
- **Rationale:** Decomposition emits only `(cnt, sum, sum_sq)` sufficient-statistic partial
  columns, so no aggregate function name is spliced into the DataFusion query text at all.
  The alias bug closes by construction. The floor covers the residue: a statistical
  aggregate over a rendered expression declines `parse_agg_item`, widens, and is computed
  natively by Exasol in the wrapper.
- **Promotes to ADR:** yes

### [9] The nested-aggregate defect on the broadcast-join path is in scope

- **Decision:** Cover the ungrouped-join variant in this plan's scenarios and e2e matrix,
  fixed by the same guard.
- **Alternatives:** Scope strictly to the single-table path, as both issues report.
  Rejected: `carries_aggregation_clause` (`joins/planning.rs`) inspects only TOP-LEVEL
  `function_aggregate` select items, so `SELECT ROUND(SUM(a.x), 2) FROM a JOIN b …` with
  `aggregationType: "single_group"` is not recognised as carrying an aggregation clause and
  reaches the broadcast projection through the same `project_columns` call. It is the same
  defect at a second entry point.
- **Rationale:** The guard lives at the single site all three consumers of the derived
  projection read — single-table routing, broadcast-join eligibility, and the empty-result
  path — so the join variant is fixed for free. Leaving it unverified would ship a known
  silent-wrong-answer path.
- **Promotes to ADR:** no

### [10] No metadata-only aggregate answering; the Iceberg check is recorded as not engaged

- **Decision:** Every aggregate this plan handles is computed by scanning data files. Record
  the Iceberg check in the spec with retrieved quotes, and name the one clause that could
  have been engaged.
- **Alternatives:** Answer `COUNT(*)` from Iceberg manifest `record_count`. Out of scope and
  recorded as unsound to reach for here: `record_count` counts rows before delete
  application, while "Delete files and deletion vector metadata that match the filters must
  be applied to data files at read time" (Apache Iceberg table spec, Scan Planning), and all
  five per-column stat maps are `optional` in every format version.
- **Rationale:** "Data files that match the query filter must be read by the scan" (same
  section) makes scanning unconditionally conformant, so there is no deviation to fix and
  none to track. Quotes were retrieved from the published spec rather than recalled, per the
  project's Iceberg-compliance rule.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated in revision mode after plan-reviewer blockers, and by speq-implement after code review. -->

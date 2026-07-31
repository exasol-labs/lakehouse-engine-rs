# Decision Log: fix-join-filter-type-rewrites

## Interview

**Q:** The join filter render sites (`render_broadcast_join`'s combined filter, and
`build_side_fan_out_sql`'s per-leg filter used by the N-scan fallback) currently call
`render_df_filter_safe` directly with NO type-awareness at all — not just for LIKE. The single-table
path instead runs `apply_type_rewrites` (`support.rs:1074`), an ordered 3-guard pipeline:
`like_subject_type_guard` (#207) → `string_function_arg_type_guard` (#210, INSTR/LOCATE arity) →
`rewrite_decimal_stringifications` (#211). Should the plan wire the FULL pipeline into both join
sites, or ONLY the LIKE guard?

**A:** The full `apply_type_rewrites` pipeline. Rationale accepted: it reuses the exact single-table
mechanism (no new guard code), every decline it produces is already made safe by #285 (broadcast
falls back to N-scan; an N-scan side-local decline becomes a residual outer-`WHERE` conjunct), and it
closes a real silent-wrong-answer exposure at the join surfaces for `INSTR`/`LOCATE` calls with more
than two arguments as a documented side effect — while explicitly NOT claiming to close #228.

## Design Decisions

### [1] Wire the full type-rewrite pipeline into both join WHERE-filter sites, not only the LIKE guard

- **Decision:** Both join sites run `apply_type_rewrites` — all three guards — rather than
  `like_subject_type_guard` alone, which is the literal scope of #215.
- **Alternatives:** Wire only the LIKE guard, matching #215's title exactly and leaving #223 slice 2
  and #228's join-surface exposure for separate plans. Rejected: the two other guards are already
  written, already tested, and already composed into the one pipeline function; declining to call
  them at a site we are editing anyway leaves two known wrong-answer paths open, one of which (#223
  slice 2, DECIMAL stringification) is a SILENT wrong answer and therefore worse than #215's loud
  failure.
- **Rationale:** The pipeline function exists precisely so no call site sequences the passes itself
  (`vs-adapter/pushdown-planning-string-fn-type-coercion-composition`, issue #259). Calling one pass
  at a new site would be the first violation of that ownership. Wiring the pipeline is both the
  smaller diff and the correct one.
- **Promotes to ADR:** yes

### [2] Reuse `classify_where_filter` at the broadcast site instead of inlining rewrite-then-render

- **Decision:** `render_broadcast_join` calls `classify_where_filter(filter_json, &col_types)` and
  declines the broadcast plan when the declined half is `Some`.
- **Alternatives:** Call `apply_type_rewrites` then `render_df_filter_safe` inline at the join site.
  Rejected: `classify_where_filter` is already the sole owner of that classification per
  `_decision/045`, and it also owns the absent / trivially-true / declined three-way distinction —
  the exact distinction the broadcast site previously got wrong (it conflated absent with declined,
  which is what #279 found).
- **Rationale:** One owner, one classification. Inlining would recreate the second owner that
  decision exists to prevent, and would re-expose the three-way distinction to being re-derived
  incorrectly.
- **Promotes to ADR:** no

### [3] The N-scan type screen runs per side and per conjunct, AFTER attribution

- **Decision:** The type screen is applied to each conjunct individually, against
  `cols_per_side[i]`, after `side_local_filter` has attributed the conjunct to a table — never over
  the combined pre-attribution conjunct set.
- **Alternatives:** (a) Fold the type condition into `renderable_only`/`declined_only` using a
  combined cross-side type map. Rejected: the N-scan path has NO disjoint-column-name precondition
  (unlike broadcast, which is gated by `disjoint_schema_guard`), so two sides MAY declare the same
  column name with different Exasol types; a combined map resolves such a name against an arbitrary
  side and would either push a non-string LIKE into a leg (hard scan failure) or forfeit a valid
  string LIKE's pushdown. (b) Screen each side's whole side-local tree at once, declining all of that
  side's conjuncts on one bad conjunct. Rejected: the partition already exists to express a
  per-conjunct decision, so per-tree granularity would give up pushdown for no structural gain.
- **Rationale:** Correctness forces per-side; the existing partition makes per-conjunct free.
- **Promotes to ADR:** yes

### [4] Restructure `build_n_scan_join_sql` so per-side legs are computed before the residual

- **Decision:** The per-side fan-out loop moves ahead of the residual assembly, and the residual is
  conjoined from three disjoint parts: `cross_side_residual_filter(leg_eligible)`,
  `declined_only(where_filter)`, and the type-declined conjuncts accumulated during the loop.
- **Alternatives:** Keep the current order and subtract the type-declined conjuncts from the legs
  after the residual has been rendered. Rejected: the residual is rendered to a String before the
  loop today, so it cannot receive a conjunct the loop discovers; reordering is both smaller and
  makes the total-and-disjoint property visible at the assembly site.
- **Rationale:** The data dependency runs legs → residual, so the code must too.
- **Promotes to ADR:** no

### [5] `type_screened_leg_filter` returns both halves and fails closed

- **Decision:** One new `pub(super)` helper in `joins/rendering.rs` returns
  `(leg_filter_rewritten, type_declined_raw)`. If the re-formed accepted-conjunct tree does not
  itself survive the pipeline, the WHOLE side-local set is returned as declined.
- **Alternatives:** Two separate functions mirroring the `renderable_only`/`declined_only` pair.
  Rejected: both halves derive from the same `col_types` and the caller needs both, so splitting them
  turns a function-local invariant (total and disjoint) into an agreement between two call sites.
  Also rejected: propagating the whole-tree decline as an error — a conjunct applied nowhere returns
  wrong rows, whereas a conjunct applied in the outer wrapper is merely slower, so the safe direction
  is unconditionally "residual".
- **Rationale:** The helper is deeper than its interface: one call yields a partition that is total,
  disjoint, fail-closed, and type-correct per side, none of which a caller gets right by calling
  `partition_conjuncts` twice. It sits beside the module's existing partition pairs, adding no new
  boundary.
- **Promotes to ADR:** no

### [6] Accept running the guard twice per conjunct rather than adding a map-and-partition primitive

- **Decision:** `type_screened_leg_filter` reuses `partition_conjuncts` verbatim with an
  `apply_type_rewrites(c, cols).is_some()` predicate, then rewrites the accepted tree once — so the
  guard runs twice per conjunct plus once per side.
- **Alternatives:** Add a mapping variant of `partition_conjuncts` that keeps the rewritten conjunct,
  avoiding the second pass. Rejected on YAGNI grounds: these trees are top-level WHERE conjuncts
  evaluated once per pushdown request at planning time, so the cost is unmeasurable, and a new
  traversal primitive would need its own ownership story against
  `vs-adapter/pushdown-module-structure`'s "one traversal primitive" rule.
- **Rationale:** Reusing the existing primitive keeps the diff to one function; if the cost ever
  matters, the upgrade stays local.
- **Promotes to ADR:** no

### [7] The join-surface scenarios go in a new feature, not into `pushdown-planning-like-type-coercion`

- **Decision:** A new feature `vs-adapter/pushdown-planning-join-filter-type-coercion` carries the
  six join-surface scenarios; `pushdown-planning-like-type-coercion` gets only a Background
  correction and one changed scenario clause pointing at it.
- **Alternatives:** Append the six scenarios to `pushdown-planning-like-type-coercion`. Rejected:
  that feature already holds 10 scenarios — the library's per-spec organization threshold — so the
  append would block recording pending a user reorganization decision. Also rejected on content
  grounds: the join surfaces raise a concern the single-table surfaces do not have (which
  column-type universe a surface may screen against), so they are not merely more instances of the
  same scenario shape.
- **Rationale:** Same precedent and same reason as
  `pushdown-planning-string-fn-type-coercion-composition`, which was split off its parent feature
  once that feature crossed the threshold.
- **Promotes to ADR:** no

### [8] Scope discipline on #223, #228, and the projection path

- **Decision:** This plan closes #215 and #223 slice 2 only. #223 slices 1 (computed-expression
  arguments) and 3 (GROUP-BY-only keys) stay open, so #223 MUST be narrowed by comment, never closed.
  #228 is referenced as "exposure narrowed at the join WHERE surfaces", never closed — its root cause
  is the `INSTR`/`LOCATE` arity rendering defect in `crates/vs-expression`, untouched here. The join
  SELECT-list projection path is out of scope: it reaches `project_columns` through
  `extract_join_projection` and has run the pipeline since #207/#210/#211.
- **Alternatives:** Claim `Closes #223` because the plan touches the surface #223's fix direction
  names. Rejected: two of #223's three slices are untouched, and an over-broad close hides real
  remaining work.
- **Rationale:** An inaccurately-scoped close is worse than no close — it converts tracked work into
  a silent gap, which CLAUDE.md forbids.
- **#228 as a declared dependency:** #215's body declares a dependency on #228. It is discharged here
  as SOFT, not missed: the `INSTR`/`LOCATE` >2-argument arity decline this wiring makes newly
  reachable is already safe post-#285, and #228's own fix would later REPLACE that decline with a
  faithful multi-argument rendering — strictly better at the same sites, not an invalidation of this
  wiring. #228 therefore need not land first.
- **Promotes to ADR:** no

### [9] The Iceberg-specification compliance planning gate is a deliberate N/A here

- **Decision:** No Apache Iceberg table-spec section is quoted or checked for this plan, and that is
  a considered N/A rather than a skipped gate.
- **Alternatives:** Quote a spec section anyway to satisfy the letter of the CLAUDE.md rule.
  Rejected as noise.
- **Rationale:** CLAUDE.md's gate applies to features touching scanning, pushdown, or schema/type
  handling as they relate to the Iceberg table format. This change is entirely Exasol-dialect vs.
  DataFusion-dialect SQL type-coercion translation inside the adapter's SQL builders: no manifest
  reading, no snapshot resolution, no field-id or Iceberg type mapping, and no change to which files
  are opened (Iceberg manifest pruning keeps every side-local conjunct in raw form, explicitly
  asserted in the deltas). The one Iceberg-adjacent fact the change relies on — that a decimal's
  trailing scale digits are an artifact of the fixed scale S in the `decimal(P, S)` primitive, not
  data — is already quoted and recorded in `vs-adapter/pushdown-planning-decimal-string-format`'s
  Background and is not re-litigated here. Recorded explicitly so a future auditor does not read the
  gate's absence as an omission.
- **Promotes to ADR:** no

### [10] Live-Exasol E2E evidence is required for the decline-routing behavior, not unit tests alone

- **Decision:** FIVE Docker-Exasol E2E tests are mandatory deliverables — broadcast decline, broadcast
  DATE CAST, N-scan residual, `INSTR` three-argument, and DECIMAL stringification row-equality at both
  join surfaces (`e2e_join_decimal_stringification_matches_native_at_both_surfaces`, task 3.6) — each
  asserting returned row CONTENT against a ground-truth query rather than merely "the query did not
  crash".
- **Alternatives:** Rely on golden-SQL unit tests, which fully determine the emitted SQL. Rejected:
  CLAUDE.md § "Verification discipline" requires a claimed SQL capability fix to be verified against
  a live Exasol instance, and #279 is the precedent for why — the entire pre-#285 decline design was
  built on a capability assumption that code inspection and the capability registry both endorsed and
  a live query disproved.
- **Why the decimal case is the fifth and not unit-only:** decision [1] chooses the FULL pipeline over
  the LIKE guard alone specifically because it closes #223 slice 2, a SILENT wrong answer. A unit
  assertion that the emitted fragment carries a `decimal_to_varchar_exasol` node proves what the
  adapter EMITS, not that Exasol and DataFusion now agree on the join's row set — the exact assumption
  class #279 disproved. The plan's headline justification cannot be the one guard shipping with no live
  evidence. The case was unobservable with the pre-existing fixtures (every numeric column in
  `dim_customer`/`fact_orders` is Iceberg `Long` → `DECIMAL(20,0)`, and the trim is a no-op on a
  scale-0 value), so task 3.5 seeds one scale ≥ 2 DECIMAL column on `fact_orders` to make it
  observable. The extension is additive: no existing test asserts that table's column count or does
  `SELECT *` over it.
- **Rationale:** A rendered string proves what we emitted, not what Exasol does with it.
- **Promotes to ADR:** no

### [11] The shared-column-name scenario is pinned at the partition level, with no row-equality claim

- **Decision:** The new feature's "Two N-scan sides sharing a column name are each screened against
  their own side's types" scenario DROPS its "returned rows SHALL equal native Exasol evaluation"
  clause and is verified by unit test only. A Background bullet in that feature states why. The two
  `vs-adapter/pushdown-declined-filter-self-apply` join scenarios KEEP their row-equality clauses and
  gain Integration rows in § Scenario Coverage mapping them to the already-planned E2E tests
  (`e2e_broadcast_like_on_decimal_column_falls_back_and_filters`,
  `e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where`), so no row-equality claim in this
  plan is orphaned.
- **Alternatives:** Seed two join tables declaring the SAME column name at DIFFERENT Iceberg types
  (e.g. `KEYCOL` as `string` on one, `long` on the other) plus an E2E test. Rejected: the fixture is
  unnatural — all four seed tables deliberately use disjoint prefixes (`C_`/`O_`/`L_`/`S_`), and a
  manufactured cross-table name collision in a shared star schema is a broader fixture change than the
  claim needs.
- **Rationale:** The claim is about WHICH `col_types` slice the screen consults, which is pure
  planning-time computation a unit test fully determines — nothing about it depends on what Exasol
  returns. That is the opposite of decision [10]'s decimal case, where the claim is about DATA VALUES
  (the trimmed vs. full-scale string) and is therefore only observable live. The live row-equality
  guarantee at the same residual route is already carried by
  `e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where`, which exercises the identical route
  with a non-colliding name; the collision changes only which metadata slice is consulted.
- **Promotes to ADR:** no

### [12] `join_col_types` is the single owner of the broadcast surface's column-type union

- **Decision:** One `pub(super)` helper `join_col_types(request, join) -> Vec<(String, String)>` in
  `joins/rendering.rs` produces the broadcast surface's type universe. Both `render_broadcast_join`
  (new caller, task 1.2) and `extract_join_projection` (existing, rebuilt its own `combined` union at
  `joins/rendering.rs:28-29`) call it.
- **Alternatives:** Build the union inline in `render_broadcast_join` and leave
  `extract_join_projection`'s copy alone. Rejected: one decision — "the broadcast surface's type
  universe is the union of both involved tables' columns, matched by bare name" — would live in two
  places in one function's own control flow with nothing enforcing agreement. The new feature's
  Background makes that decision load-bearing ("WHICH column-type universe a surface may screen
  against").
- **Rationale:** Same shape `vs-adapter/pushdown-col-types-consolidation` exists to prevent for the
  single-table guards. Extracting the existing derivation costs one function and removes a
  divergence risk rather than adding an abstraction.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] `type_screened_leg_filter` screened the wrong tree for renderability

- **Finding** (round 1, COMPLETENESS_GAP BLOCKER): task 2.2 partitioned conjuncts on
  `apply_type_rewrites(c, col_types).is_some()` alone and then handed the leg the REWRITTEN tree. The
  only renderability screen in the N-scan path (`renderable_only`) runs on the RAW tree before
  attribution, so nothing checked the rewritten tree. `build_side_fan_out_sql`'s
  `render_df_filter_safe` returning `None` would leave the leg with no filter while the conjunct was
  also absent from the residual — applied nowhere, extra rows, no error. That is #279's exact defect at
  a new site, and the broadcast site inherits the guard against it via `classify_where_filter`'s
  `(Some(raw), Some(tree)) if !datafusion_renderable(tree)` arm (`support.rs:1092`) while the N-scan
  site did not — an asymmetry no delta justified.
- **Direction change:** The leg-eligibility predicate now requires BOTH conditions on the REWRITTEN
  conjunct: type-accepted AND `datafusion_renderable`. The fail-closed arm fires in both directions —
  the whole side-local set goes residual if the re-formed accepted tree fails the pipeline OR is not
  renderable. Task 2.1 gains a failing unit test
  (`type_screened_leg_filter_declines_type_accepted_but_unrenderable_rewrite`) asserting such a conjunct
  lands in the DECLINED half in RAW form, mapped in § Scenario Coverage. The invariant is restated as
  "type-accepted AND whose REWRITTEN form the dialect can render" in
  `vs-adapter/pushdown-planning-join-fallback` (Background + two scenario clauses),
  `vs-adapter/pushdown-declined-filter-self-apply` scenario 2, and the new feature's N-scan decline
  scenario + Background, each with an explicit clause that such a conjunct SHALL become residual and
  MUST NOT be omitted from both halves. plan.md's Patterns table gains a row naming the rule.
- **ADR rationale:** "Screen the tree you render, not the tree you received" is the general form of
  the #279 defect and applies to every future render surface wired to the pipeline, so it must be
  recorded where a later planner will find it before repeating the asymmetry.
- **Promotes to ADR:** yes

### [plan-review] The Design § Context coverage claim contradicted the plan's own artifacts

- **Finding** (round 1, REQUIREMENT_CONFLICT BLOCKER): § Design § Context claimed the two join sites
  "are the only pushed expression trees with no column-type awareness at all". The plan's own
  `pushdown-planning-string-fn-type-coercion` delta says the grouped-aggregate and aggregate-argument
  render paths stay out of scope, § Non-Goals leaves #223 slices 1 and 3 open, and slice 3 IS a pushed
  expression surface with no column-type awareness. As written the sentence converted tracked work into
  a silent gap — and it is the kind of line that propagates verbatim into a PR body.
- **Direction change:** Narrowed to "the only JOIN WHERE-filter surfaces with no column-type
  awareness", and a following paragraph states this plan does NOT complete type-rewrite coverage,
  naming the three surfaces that stay unwired and keep their exposure (grouped-aggregate render path,
  aggregate-argument render path, #223 slice 3's GROUP-BY-only DECIMAL keys). Same edit split the
  paragraph at the "The two JOIN WHERE-filter sites" boundary per the prose guardrail.
- **Promotes to ADR:** no

### [plan-review] #223 slice 2 had no live verification and was unobservable with the seed fixtures

- **Finding** (round 1, TRACEABILITY_GAP BLOCKER): closing #223 slice 2 is decision [1]'s headline
  justification for wiring the FULL pipeline rather than the LIKE guard, yet the decimal guard was the
  only one in the plan with no E2E test — one unit render assertion, and § [10] listed four mandatory
  E2E tests, none of them the decimal case. It was also unobservable: every numeric column in
  `dim_customer`/`fact_orders` is Iceberg `Long` → `DECIMAL(20,0)`, where the trim is a no-op, so the
  plan's own Impact row (`2912.00` → `2912`) described a divergence no planned test could produce.
- **Direction change:** Took the review's option (a), the fixture extension, NOT the
  "ships-on-unit-evidence" record. Added task 3.5 (seed one scale ≥ 2 DECIMAL column on `fact_orders`)
  and task 3.6 (live E2E `e2e_join_decimal_stringification_matches_native_at_both_surfaces`, asserting
  row equality against native Exasol evaluation at BOTH `VS_NAME` broadcast and `VS_NAME_LOW` N-scan
  surfaces), with the Integration row in § Scenario Coverage, the § Manual Testing row, the Group C
  parallelization entry (3.5 → 3.6), and § [10] revised to five mandatory E2E tests. Chosen over the
  record-and-ship option because the claim is a SILENT wrong answer in DATA VALUES — precisely the
  class unit tests cannot observe and CLAUDE.md § "Verification discipline" requires a live check for —
  and because the fixture change is additive (no existing test asserts that table's column count or
  does `SELECT *` over it).
- **Promotes to ADR:** no

### [plan-review] Row-equality claims mapped only to unit tests

- **Finding** (round 1, TRACEABILITY_GAP BLOCKER): the new feature's shared-column-name scenario and
  both `pushdown-declined-filter-self-apply` join scenarios each ended "the returned rows SHALL equal
  native Exasol evaluation of the same join" while § Scenario Coverage mapped each to a Unit test only —
  violating the plan's own stated rule that every result-claiming scenario carries an E2E test. The
  shared-name case was additionally unverifiable: all four seed tables use disjoint column prefixes, so
  no two sides share a name at any type.
- **Direction change:** Took the review's option (b) for the shared-name scenario and the finding's
  unconditional half for the other two. Deleted the row-equality clause from the shared-name scenario
  and added a Background bullet in the new feature stating it is pinned at the partition level only and
  why; added a paragraph in § Verification saying the same. Added the two Integration rows mapping the
  `pushdown-declined-filter-self-apply` scenarios to the existing planned E2E tests, so their
  row-equality clauses are traceable rather than orphaned. Option (b) chosen over seeding a shared
  column name because the shared-name claim is about WHICH `col_types` slice the screen consults —
  pure planning-time computation a unit test fully determines — whereas a manufactured cross-table
  name collision is a broader change to a shared star schema than the claim needs. Recorded as
  decision [11]. Note the deliberate asymmetry with the previous finding: the decimal claim is about
  data VALUES and got a fixture; this claim is about metadata SELECTION and got a narrowed scope.
- **Promotes to ADR:** no

# Plan Review Findings: fix-join-filter-type-rewrites (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 10 (Blockers: 4, Advisory: 6)
- Intent Fidelity blockers: 0

## Premortem

Three failure stories, six months out:

1. **The leg silently drops a rewritten conjunct.** `type_screened_leg_filter` hands the leg a tree
   that was screened for renderability in its RAW form and for type-acceptance, then swapped for a
   REWRITTEN tree nobody re-screened. `build_side_fan_out_sql`'s `render_df_filter_safe` returns
   `None`, the leg gets no filter, the conjunct is not in the residual either, and the join returns
   extra rows with no error — #279's exact defect, at the new spot. → BLOCKER 1.
2. **#223 slice 2 is marked fixed but was never observed.** The plan's headline justification for the
   full pipeline over the LIKE-only wiring is that it closes a SILENT wrong answer (DECIMAL
   stringification). That is the one guard with zero live verification, and the join E2E fixtures
   cannot observe it at all. → BLOCKER 2.
3. **The coverage claim outlives the plan.** `plan.md`'s "the only pushed expression trees with no
   column-type awareness at all" reaches the PR body; a reviewer reads type-coercion coverage as
   complete; #223 slice 3 and the aggregate/GROUP-BY render surfaces stop being tracked — the silent
   gap CLAUDE.md forbids. → BLOCKER 4.

## Intent Fidelity

[no objection — axis checked]

- **Both sites covered, not just broadcast.** #215's cited lines map to
  `render_broadcast_join`'s filter (`sql_builders.rs:82` today) and `build_side_fan_out_sql`'s
  per-leg filter (`sql_builders.rs:589-591`). Task 1.2 wires the first, task 2.4 the second; both
  carry unit tasks (1.1, 2.3) and live E2E tasks (3.1, 3.3). Verified against the source.
- **Full pipeline, per the interview.** `apply_type_rewrites` (all three guards) at both sites via
  `classify_where_filter` (broadcast) and `type_screened_leg_filter` (N-scan). Not re-litigated.
- **Issue-linking precision holds.** `grep -rn "Closes\|Fixes #"` over the plan directory returns
  only `Closes #215` plus the two MUST-NOT rows forbidding `Closes #223` and `Closes #228`.
  `decision-log.md` § [8] scopes #223 to slice 2 and #228 to "exposure narrowed". No stray close.
- **No scope creep.** Every artifact traces to the wiring: one new feature (6 scenarios), five
  CHANGED deltas correcting statements the wiring falsifies, one new helper.

## Feasibility

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: `plan.md` § Dependencies
- Issue: the section names only "branch `feat/fix-declined-filter-self-apply` (PR #285, #279)". But
  issue #215's own body carries a second declared dependency: "Depends on #228 — wiring the full
  type-rewrite pipeline into the join filter sites makes #228's `INSTR`/`LOCATE` >2-argument arity
  decline newly reachable at the join surfaces… #228's own fix… removes the decline case there
  entirely, rather than just changing how it is reported." The plan discharges #228 only as an
  "exposure narrowed" bookkeeping row, never as a dependency it has considered and dismissed. A
  future reader cannot tell whether the planner saw the declaration or missed it.
- Fix: In `plan.md` § Dependencies, add a sentence discharging #215's declared #228 dependency
  explicitly: state that it is a SOFT dependency (the arity decline is safe post-#285, and #228's own
  fix would later replace the decline with a faithful multi-argument rendering rather than
  invalidating this wiring), so #228 need not land first. Add the same one-line note to
  `decision-log.md` § [8].

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: `plan.md` § Impact, row "`LIKE` over a `DATE` column"; task 3.2
- Issue: the row reads "Correct rows, still pushed down as `CAST(<col> AS VARCHAR) LIKE …`"
  unconditionally, and task 3.2 asserts "the rows equal the ground-truth filtered set". The plan's
  own new-feature delta qualifies this: "the altered-session `NLS_DATE_FORMAT` tracked exception
  (#216) SHALL be identical to the single-table WHERE surface's". Row correctness therefore holds
  only under the session date format the CAST arm assumes. Stating it unconditionally in the
  PR-facing Impact table overstates the guarantee, and task 3.2 gives the implementer no reason to
  pin the session format.
- Fix: In `plan.md` § Impact, qualify the DATE row's After cell with "under the default
  `NLS_DATE_FORMAT`; the #216 tracked exception carries over unchanged from the single-table
  surface". In task 3.2, add that the test MUST assert against the default session `NLS_DATE_FORMAT`
  (or set it explicitly) so the ground-truth comparison is not format-dependent.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: `plan.md` § Impact; `vs-adapter/pushdown-planning-join-filter-type-coercion/spec.md`
  scenario 1, second GIVEN clause
- Issue: the delta makes an unresolvable column name a decline trigger — "or the column's name does
  not resolve in that union at all" — and `apply_type_rewrites`'s two `Option`-returning guards do
  decline on a lookup miss (`support.rs:1074-1078`, and `vs-adapter/pushdown-col-types-consolidation`
  records the miss as each guard's own decline branch). That is a NEW broadcast-decline trigger
  unrelated to type coercion: a broadcast filter carrying a bare-column `LIKE` or governed string
  function over a name absent from `involvedTables` metadata now forfeits the broadcast plan where it
  previously rendered. `plan.md` § Impact's five-row change table does not list it, and its last row
  ("Anything with no type-rewrite trigger | Correct | Byte-identical SQL, unchanged") implies no
  other behavior moves.
- Fix: Add a sixth row to `plan.md` § Impact for the unresolvable-column case — before: rendered
  bare; after: broadcast declined, N-scan self-applies — and state in the Impact prose whether any
  reachable request can hit it (a bare column absent from `involvedTables` metadata), so a reviewer
  can judge the blast radius rather than discover it from a golden-fixture failure.

[Also checked, no objection: **file-count neutrality**. `plan_join` (`joins/mod.rs:121-139`) resolves
every side's file list ONCE, pruned by `side_local_filter` over the RAW filter, BEFORE the
broadcast-vs-N-scan decision — so `plan.md` § Impact's "Iceberg manifest pruning is unaffected in
every case, so no query opens more files" is correct for the broadcast→N-scan transition too.
**Exasol-dialect residual renderability**: verified, not assumed — `render_df_filter_qualified`
(`joins/rendering.rs:108-121`) routes to `render_df_filter_exasol_safe`, and `vs-expression`
registers `("INSTR", ExasolForm::VerbatimCall)` / `("LOCATE", ExasolForm::VerbatimCall)`
(`lib.rs:198-199`) with a test pinning `INSTR('hello', 'l', 3)`, so a type-declined 3-argument INSTR
really does reach Exasol intact. **Visibility**: `classify_where_filter` and `apply_type_rewrites`
are `pub(super)` in `pushdown::support`, i.e. visible in `pushdown` and its descendants including
`pushdown::joins::sql_builders` — no re-export needed. **Fixtures exist** for tasks 3.1/3.2/3.4:
`dim_customer(C_CUSTKEY, C_NAME)` and `fact_orders(O_ORDERKEY, O_CUSTKEY, O_ORDERDATE)`
(`tests/common/seed.rs:993-1032`), with `VS_NAME` / `VS_NAME_LOW` already wired.
**Rewrite-once equivalence**: applying `apply_type_rewrites` to the re-formed `predicate_and` of
individually-accepted conjuncts is equivalent to rewriting each conjunct, because all three passes
are per-node post-order traversals and `predicate_and` is not a governed node — decision [6]'s
"guard runs twice" shortcut is sound and carries a stated upgrade path. **NFRs**: no wire-format,
scan-spec, or ABI change; performance consequences stated per decline shape; no concurrency or
migration surface.]

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER
- Location: `plan.md` task 2.2; `vs-adapter/pushdown-planning-join-fallback/spec.md` § Scenarios,
  clause "the filter each leg receives SHALL already be screened as DataFusion-renderable AND
  type-accepted, and SHALL be the pipeline's REWRITTEN tree, so the leg's own render cannot decline";
  same invariant restated in `vs-adapter/pushdown-declined-filter-self-apply/spec.md` scenario 2
- Issue: the mechanism does not establish the invariant it asserts, and the failure mode is a
  silently dropped predicate — #279's exact defect at a new site. Task 2.2 partitions by
  `apply_type_rewrites(c, col_types).is_some()` and then hands the leg the REWRITTEN tree. The only
  renderability screen in the N-scan path is `renderable_only`, applied to the RAW tree before
  attribution (`sql_builders.rs:381`). Nothing re-checks the REWRITTEN tree. `build_side_fan_out_sql`
  then does `side_filter.map(strip_table_alias).and_then(|f| render_df_filter_safe(&f))`
  (`sql_builders.rs:589-591`) — a `None` there yields a leg with NO filter, and because the conjunct
  went to the LEG half it is absent from the residual too, so it is applied nowhere and the join
  returns extra rows with no error. The single-table owner the plan says it is mirroring does carry
  this arm: `classify_where_filter` re-checks the rewritten tree explicitly —
  `(Some(raw), Some(tree)) if !datafusion_renderable(tree) => (None, Some(raw))`
  (`support.rs:1090`). Task 2.2's fail-closed arm checks only that the re-formed tree "survives the
  pipeline", which is a type check, not a renderability check — so the broadcast site inherits the
  guard and the N-scan site does not, an asymmetry no delta justifies.
- Fix: In `plan.md` task 2.2, change the partition predicate to require BOTH conditions on the
  rewritten conjunct — `apply_type_rewrites(c, col_types)` is `Some` AND `datafusion_renderable` of
  that rewritten conjunct is true — and extend the fail-closed arm to `(None, Some(side_local))` when
  the re-formed accepted tree is not `datafusion_renderable` either. In task 2.1 add a failing unit
  test for a conjunct whose rewrite is type-accepted but not DataFusion-renderable, asserting it
  lands in the DECLINED half in RAW form. In
  `vs-adapter/pushdown-planning-join-fallback/spec.md`, restate the leg-eligibility rule's third
  condition as "accepted by the type-rewrite pipeline run against THAT SIDE's own column metadata
  AND whose REWRITTEN form the DataFusion dialect can render", and add an explicit clause that a
  rewrite which renders in raw form but not in rewritten form SHALL become residual, never be
  omitted. Mirror that clause in
  `vs-adapter/pushdown-declined-filter-self-apply/spec.md` scenario 2 and in the new feature's
  N-scan decline scenario.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `plan.md` § Design § Context, sentence "The two JOIN WHERE-filter sites were left
  unwired and are the only pushed expression trees with no column-type awareness at all"
- Issue: the claim contradicts two of this plan's own artifacts. The plan's own
  `vs-adapter/pushdown-planning-string-fn-type-coercion/spec.md` delta states "The grouped-aggregate
  render path, the aggregate-argument render path, `CHR`/`UNICODECHR`, and a non-bare-column
  string-position argument all remain out of scope, unchanged by this delta" and "any render surface
  still unwired to the guard remains exposed"; `plan.md` § Non-Goals leaves "#223 slices 1 and 3"
  open, and #223 slice 3 IS a pushed expression surface with no column-type awareness (a DECIMAL
  stringification reachable only as a GROUP-BY key). The
  `vs-adapter/pushdown-planning-like-type-coercion` delta likewise counts FOUR wired surfaces, not
  "all of them". As written the sentence converts tracked work into a silent gap — the outcome
  CLAUDE.md § "Iceberg specification compliance" forbids in the general case and § "Verification
  discipline" forbids in spirit — and it is the kind of line that propagates verbatim into a PR body.
- Fix: In `plan.md` § Design § Context, replace "are the only pushed expression trees with no
  column-type awareness at all" with "are the only JOIN WHERE-filter surfaces with no column-type
  awareness", and append one sentence naming the surfaces that stay unwired after this plan — the
  grouped-aggregate render path, the aggregate-argument render path, and #223 slice 3's GROUP-BY-only
  keys — so the remaining exposure stays visible rather than being implied closed.

[Also checked, no objection: the six new scenarios are each testable as written (concrete GIVEN
column types, concrete WHEN, falsifiable THEN); the three-way residual disjointness the fallback
delta asserts is provable from the code — type-declined ⊂ `side_local_filter(leg_eligible, t_i)`,
which is disjoint from `cross_side_residual_filter(leg_eligible)` by
`conjunct_single_side`'s `is_none()` complement (`joins/rendering.rs:241-243`) and from
`declined_only(where_filter)` by `renderable_only`'s complement; the per-side type universe claim is
correct because the N-scan path has no `disjoint_schema_guard` precondition while the broadcast path
does (`sql_builders.rs:70-73`); the "raw tree forwarded to Iceberg pruning is unchanged" claim is
already enforced structurally, since pruning consumes `side_local_filter` over the RAW request
filter in `plan_join`, not the leg tree.]

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: `plan.md` § Verification § Scenario Coverage, row
  "pushdown-planning-decimal-string-format / WHERE-clause stringification of a DECIMAL column renders
  the trimmed form"; `decision-log.md` § [10]
- Issue: the plan's stated justification for wiring the FULL pipeline rather than the LIKE guard is
  that it "closes #223 slice 2 (a silent wrong answer, worse than #215's loud failure)" — yet the
  decimal half is the ONLY guard in this plan with no live verification. Its single mapped test is a
  unit render assertion, `join_decimal_stringification_renders_trimmed_at_both_join_sites`, and
  `decision-log.md` § [10] enumerates exactly four mandatory E2E tests, none of them the decimal
  case. Worse, the case is UNOBSERVABLE with the existing join fixtures: every numeric column in
  `dim_customer`/`fact_orders` is Iceberg `Long` (`tests/common/seed.rs:996`, `1014-1015`) → Exasol
  `DECIMAL(20,0)`, and `vs-adapter/pushdown-col-types-consolidation` records that the trim is "a
  harmless no-op on a scale-0 value". So no scale > 0 DECIMAL exists at any join surface, and the
  plan's own Impact row ("full-scale decimal text, e.g. `2912.00`" → "`2912`") describes a divergence
  no planned test can produce. A unit test proving the emitted fragment carries a
  `decimal_to_varchar_exasol` node proves what the adapter emits, not that Exasol and DataFusion now
  agree on the join's row set — the exact assumption class #279 disproved, and the reason
  CLAUDE.md § "Verification discipline" requires a live check.
- Fix: Add a task to `plan.md` § Implementation Tasks, Group C: extend
  `crates/lakehouse-engine/tests/common/seed.rs`'s star-schema seeding with one scale > 0 DECIMAL
  column on `fact_orders` (Iceberg `decimal(P, S)` with S ≥ 2, values whose trailing zeros are
  significant, e.g. `2912.00`), then add a live-Exasol E2E test in
  `crates/lakehouse-engine/tests/e2e_join_test.rs` asserting that a join WHERE filter stringifying
  that column (`LENGTH(<col>) > n`, per #211's headline repro shape) returns the same rows as native
  Exasol evaluation — run at BOTH join surfaces (`VS_NAME` broadcast and `VS_NAME_LOW` N-scan). Add
  the corresponding Integration row to § Scenario Coverage and § Manual Testing, and add the new test
  to `decision-log.md` § [10]'s mandatory list (making it five, not four). If the fixture extension
  is judged too invasive, `decision-log.md` § [10] MUST instead record explicitly that #223 slice 2
  ships on unit evidence alone and why that is acceptable — it MUST NOT stay unstated.

#### [TRACEABILITY_GAP] BLOCKER
- Location: `vs-adapter/pushdown-planning-join-filter-type-coercion/spec.md` scenario "Two N-scan
  sides sharing a column name are each screened against their own side's types", final clause; and
  `plan.md` § Verification § Scenario Coverage's single Unit row for it
- Issue: the scenario ends "*AND* the returned rows SHALL equal native Exasol evaluation of the same
  join" — a claim about what a live query returns — and its only mapped test is the unit
  `type_screened_leg_filter_uses_owning_side_types_for_shared_column_name`. The plan states its own
  rule two paragraphs later: "Every scenario whose claim is about the RESULT a live query returns
  additionally carries a Docker-Exasol E2E test", then violates it here. This is not an incidental
  clause: the shared-column-name-with-different-types case is the entire reason
  `type_screened_leg_filter` must be per-side rather than folded into `renderable_only`
  (`decision-log.md` § [3]), so it is the plan's most load-bearing correctness claim. It is also
  unverifiable with the current fixtures — all four seed tables use disjoint column prefixes
  (`C_`, `O_`, `L_`, `S_`), so no two sides share a name at any type. The same defect applies to
  both `vs-adapter/pushdown-declined-filter-self-apply/spec.md` scenarios, each of which ends "the
  returned rows SHALL equal native Exasol evaluation of the same join" while § Scenario Coverage maps
  each to a Unit test only.
- Fix: Either (a) add a Group C task seeding two join tables that declare the SAME column name with
  DIFFERENT Iceberg types (e.g. `KEYCOL` as `string` on one, `long` on the other) plus an E2E test
  asserting the `VARCHAR` side's `LIKE` reaches its leg while the `DECIMAL` side's becomes a
  qualified outer-`WHERE` conjunct and the rows match native evaluation; or (b) DELETE the
  "returned rows SHALL equal native Exasol evaluation" clause from that scenario and add a Background
  bullet in the new feature stating that the shared-name case is pinned at the partition level only,
  because no E2E fixture declares a shared column name. Additionally, in § Scenario Coverage, add the
  existing E2E tests (`e2e_broadcast_like_on_decimal_column_falls_back_and_filters`,
  `e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where`) as Integration rows against the
  two `pushdown-declined-filter-self-apply` scenarios, so their row-equality clauses are traceable
  rather than orphaned.

#### [TASK_GRANULARITY] ADVISORY
- Location: `plan.md` § Parallelization, "Group A ∥ Group B — different functions in different files;
  only `sql_builders.rs`'s import block and `tests` module are shared"
- Issue: the justification is factually wrong and the entry contradicts itself. Group B's tasks 2.3
  and 2.4 both target `sql_builders.rs` — 2.3 adds tests to its `tests` module, 2.4 restructures
  `build_n_scan_join_sql`, a production function in it (lines 349-485). So A and B share a 3128-line
  production file, not just its import block and `tests` module. The entry then hedges with "land A
  before B if edit conflicts appear", which is a sequential dependency, not a parallel group — two
  implementer agents editing that file concurrently is precisely the hazard the hedge admits.
- Fix: In `plan.md` § Parallelization, either move 2.3 and 2.4 into Group A's sequence (making the
  order 1.1 → 1.2 → 1.3 → 2.3 → 2.4 in `sql_builders.rs`, with only 2.1 → 2.2 in `rendering.rs` as
  the genuinely parallel group), or state Group A → Group B as an unconditional sequential dependency
  and delete the "different functions in different files" claim and the conditional hedge.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: `plan.md` task 1.2 ("build `col_types` as the union of `left_cols` and `right_cols`")
- Issue: the broadcast join's column-type universe would then be derived twice inside one function's
  own control flow. Task 1.2 adds an inline union of `left_cols`/`right_cols` in
  `render_broadcast_join`, and the very next statement in that function calls
  `extract_join_projection`, which builds the same union internally —
  `let mut combined = involved_table_columns(request, &join.tables[0].table_name);
  combined.extend(involved_table_columns(request, &join.tables[1].table_name));`
  (`joins/rendering.rs:28-29`). One decision — "the broadcast surface's type universe is the union of
  both involved tables' columns, matched by bare name" — would live in two places, in two files, with
  nothing enforcing agreement. The new feature's Background makes that decision load-bearing
  ("WHICH column-type universe a surface may screen against"), so it deserves one owner. This is also
  the shape `vs-adapter/pushdown-col-types-consolidation` exists to prevent for the single-table
  guards.
- Fix: In `plan.md` task 1.2, name one `pub(super)` helper in `joins/rendering.rs` (e.g.
  `join_col_types(request, join) -> Vec<(String, String)>`) as the sole producer of the broadcast
  union, have `render_broadcast_join` call it, and change `extract_join_projection` to call it too
  instead of rebuilding `combined`. Record the single-owner choice as a Design Decision in
  `decision-log.md`.

[Also checked, no objection: `type_screened_leg_filter` is genuinely deep for its interface — one
call yields a total, disjoint, fail-closed, per-side-correct partition a caller cannot get right by
invoking `partition_conjuncts` twice — and it lands in `joins/rendering.rs` beside the existing
`renderable_only`/`declined_only` and `side_local_filter`/`cross_side_residual_filter` pairs, adding
no module, no boundary, and no dependency-direction change. Reusing `classify_where_filter` at the
broadcast site avoids a second owner of the rewrite-then-classify sequence (`_decision/045`). No
configuration parameter, no interface with one implementation, no speculative abstraction. The one
tactical shortcut — decision [6]'s guard-runs-twice — names its ceiling and a local upgrade path,
per the strategic/tactical rule.]

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: `plan.md` § Summary (sentence 1); § Design § Context (paragraph 1); § Impact
  (final paragraph); `plan.md` task 3.3
- Issue: four guardrail violations in governed prose. (1) § Summary's first sentence runs ~62 words
  against the 25-word cap, stacking the wiring, both surfaces, the current screen, two issue numbers,
  and two failure modes into one clause chain. (2) § Design § Context's opening paragraph carries two
  ideas — which guards ship today, and which surfaces are unwired — against "one idea per
  paragraph". (3) § Impact's performance paragraph opens "a query that now declines loses a pushdown
  it never legitimately had", which buries the conclusion the reader needs (declines cost the
  broadcast plan or one conjunct's leg filter) behind a rhetorical framing. (4) Task 3.3 says
  "the forced-above-threshold virtual schema (`VS_NAME_LOW`)", but `VS_NAME_LOW` is configured with
  `with_join_broadcast_max_bytes("1")` (`tests/e2e_join_test.rs:91`) — the THRESHOLD is lowered, not
  the schema raised above one. The phrase reads backwards on one pass.
- Fix: In `plan.md`, split § Summary's first sentence into two, each at or under 25 words, leading
  with "Wire `apply_type_rewrites` into both join WHERE-filter render sites." Split § Design §
  Context's first paragraph at the "The two JOIN WHERE-filter sites" boundary. Rewrite § Impact's
  performance paragraph to lead with what each decline costs. In task 3.3, replace
  "the forced-above-threshold virtual schema (`VS_NAME_LOW`)" with "`VS_NAME_LOW`, whose lowered
  `join_broadcast_max_bytes` forces the N-scan fallback".

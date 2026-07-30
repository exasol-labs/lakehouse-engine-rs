# Plan Review Findings: fix-declined-filter-self-apply (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 10 (Blockers: 2, Advisory: 8)
- Intent Fidelity blockers: 0

## Premortem

Six months out, three ways this plan is remembered as a failure:

1. **"The join fix made every join slower and nobody noticed for a quarter."** Task 4 edits
   `side_local_filter`, which has a second consumer nobody re-read: `joins/mod.rs:122` feeds it to
   `resolve_one_join_side` as the Iceberg manifest-pruning predicate. A declined conjunct silently
   stops pruning files. Correct results, more S3 reads, no test fails. → `[REQUIREMENT_CONFLICT]` B1.
2. **"The correctness fix started hard-failing queries that used to work."** Task 4's new
   `join_render_decline` trigger fires on a residual that renders trivially true, because it reads
   the trivially-true-suppressing renderer's `None` as "unrenderable" — the same three-way `None`
   conflation the plan exists to delete, re-created one dialect over. → `[REQUIREMENT_CONFLICT]` B2.
3. **"Task 5 ate three days and broke six test mirrors."** A two-function signature change with 3
   production and 5 test call sites, plus 7 hand-written test mirrors of the filter pipeline in
   `mod.rs`/`topn.rs` that silently stop mirroring production. No census in the plan. →
   `[EFFORT_MISESTIMATION]` A1.

## Intent Fidelity

[no objection — axis checked: all four interview answers are operationalized. All three surfaces are
in scope (tasks 3-5). Self-apply via outer WHERE reuses `cross_side_residual_filter` /
`render_df_filter_qualified` as directed. Wrapper-only-on-decline is honored — verified that the 3
existing `qualified_single_table_fallback_pushdown` callers (`pushdown/mod.rs:430`, `:500`, `:543`)
keep a `None` declined-predicate argument, so the no-decline path emits byte-identical SQL. #228 is
noted as a structural side effect and explicitly not adjudicated (decision [9]). The
issue's mandatory CLAUDE.md update is task 2, stated as a general fact with no issue number.]

The three claims the brief flagged for adversarial scrutiny all survive verification:

- **Broadcast is not a design fork** — confirmed on all three grounds.
  `specs/vs-adapter/pushdown-planning/../pushdown-planning-join/spec.md:10` does read "a
  condition/filter/projection the `crates/vs-expression` translator can render; any deviation is
  served by the unified unaccelerated fallback". `build_broadcast_join_sql` has no outer `WHERE`
  (`joins/sql_builders.rs:589-631`). And the projection-narrowing argument holds:
  `extract_join_projection` (`joins/rendering.rs:21-29`) delegates to `project_columns` over the
  select list, so a filter-only column is genuinely out of scope and the widened fallback trips the
  `widened` decline at `sql_builders.rs:86`. Dropping `[expert]` from task 3 is warranted — the task
  is one guard plus a doc comment.
- **`topn.rs`/`single_group_agg.rs`/`grouped_agg.rs` are not a fourth exposure** — confirmed.
  `mod.rs`'s `#[cfg(test)] mod tests` opens at line 769, so the only production
  `render_df_filter_safe` call in the single-table path is `mod.rs:193-194`. `topn.rs`'s use is at
  line 505, inside the `#[cfg(test)]` block opening at line 475. `single_group_agg.rs` and
  `grouped_agg.rs` do not reference the filter renderer at all.
- **No new public API is needed** — confirmed. `render_expression_safe`
  (`crates/vs-expression/src/lib.rs:1499`) is `render_expression_inner(.., DataFusion).ok()?` with no
  trivially-true suppression, whereas `render_df_filter_safe` (`:1512`) adds the `== "TRUE" || ==
  "NULL"` suppression. `render_expression_inner` returns `Ok(None)` only for `Json::Null`
  (`:544-546`), which every call site already filters out. So `render_expression_safe(..).is_none()`
  is exactly the declined case.
- **No native partial-pushdown mechanism** — accepted as adequately checked. Not independently
  verifiable from this repo (no vendored adapter API), but the negative is corroborated in-repo: the
  adapter's own response envelope is `{"type": "pushdown", "sql": ...}` at
  `sql_builders.rs:879`, with no second field anywhere.

## Feasibility

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: plan.md § Implementation Tasks task 4; `vs-adapter/pushdown-file-pruning/spec.md`
  § Background; `vs-adapter/pushdown-declined-filter-self-apply/spec.md` § Background
- Issue: Task 4 says "In `joins/rendering.rs`, add the `datafusion_renderable` condition to BOTH
  `side_local_filter` … and `cross_side_residual_filter`". `side_local_filter` has a second
  production consumer the plan never names: `crates/lakehouse-engine/src/adapter/pushdown/joins/mod.rs:122`
  — `let side_filter = filter.and_then(|f| side_local_filter(f, &leaf.table_name));` — which is
  passed straight into `resolve_one_join_side(.., side_filter.as_ref())` as that side's **Iceberg
  manifest-pruning predicate** (`plan_join`'s own comment: "each pruned by its own side-local WHERE
  conjuncts for Iceberg manifest pruning"). Adding the DataFusion-renderability condition inside
  `side_local_filter` therefore also strips the declined conjunct from Iceberg pruning on the join
  path — more files opened, silently, with no failing test. That contradicts three recorded
  statements in this plan: plan.md § Non-Goals ("No change to file pruning … each is a separate and
  already-correct mechanism"); `pushdown-file-pruning/spec.md` § Background ("Pruning stays sound
  under a render decline: **the raw filter tree forwarded to pruning is unchanged**"); and the new
  feature spec's Background ("Iceberg-level file pruning is unaffected … the raw filter tree
  forwarded to pruning is unchanged"). It also falsifies the new spec's Iceberg-spec-check bullet
  ("It reads no manifest field, evaluates no column bound"), which CLAUDE.md requires to be accurate.
- Fix: Rewrite plan.md task 4 to leave `side_local_filter` and `cross_side_residual_filter`
  unchanged as pruning-facing predicates, and apply the renderability screen at the two **render**
  call sites inside `build_n_scan_join_sql` instead: the `cross_side_residual_filter` call at
  `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs:396` and the per-leg
  `side_local_filter` call at `:419`. Name the mechanism explicitly (for example a
  `renderable_only`/`declined_only` conjunct split in `rendering.rs` used only by those two sites,
  keeping the two screened sets exact complements). Add a sentence to
  `vs-adapter/pushdown-planning-join-fallback/spec.md` § Background stating that each side's Iceberg
  manifest-pruning predicate keeps every side-local conjunct, renderable or not, because pruning only
  ever removes files that provably cannot match. Add a clause to that delta's CHANGED scenario
  asserting the per-side pruning predicate is unchanged by a render decline, and add a unit test to
  plan.md task 6 named `join_side_pruning_input_unchanged_when_df_render_declines`, plus its row in
  § Verification § Scenario Coverage.

#### [EFFORT_MISESTIMATION] ADVISORY
- Location: plan.md § Implementation Tasks task 5
- Issue: Task 5 says "Give `build_qualified_single_table_fallback_sql` and
  `qualified_single_table_fallback_pushdown` … a declined-predicate parameter" with no call-site
  census. The real fan-out: `qualified_single_table_fallback_pushdown` has 3 production callers
  (`pushdown/mod.rs:430`, `:500`, `:543`); `build_qualified_single_table_fallback_sql` is
  `#[cfg(test)]`-only re-exported (`joins/mod.rs:23-26`) with 5 test callers
  (`grouped_agg.rs:3129`, `support.rs:3017`, `joins/sql_builders.rs:1997`, `:2269`, `:2310`).
  Separately, `mod.rs`'s test module hand-mirrors the production `apply_type_rewrites` →
  `render_df_filter_safe` pipeline at lines 871, 907, 941, 972, 1002, and 1043, and `topn.rs:495-505`
  (`plan_scan_sql`) mirrors the dispatch decision path — 7 mirrors that silently stop mirroring
  production once the decline route exists, hiding exactly the regression class this plan fixes. The
  project's own recorded lesson is that a signature change without an exact call-site census
  (including test crates) is what blows up an implementer.
- Fix: Add to plan.md task 5 an explicit checklist of the 3 production and 5 test call sites listed
  above, each with the argument to pass (`None` for the 3 existing decline routes). Add a task-5
  clause requiring every mirror at `mod.rs:871`, `:907`, `:941`, `:972`, `:1002`, `:1043` and
  `topn.rs:495-505` to be updated to reproduce the new decline branch, or to assert in its own
  doc comment that it deliberately mirrors only the no-decline path.

#### [NFR_IGNORED] ADVISORY
- Location: plan.md § Impact
- Issue: § Impact describes the single-table decline path as "gaining one `SELECT … FROM (…) AS
  "LHS_T0" WHERE …` boundary" and stops there. It does not state the resource consequence: on the
  decline path every unfiltered row now crosses the UDF boundary and is filtered by Exasol, and per
  the project's recorded finding that the transparent-VS path always buffers emit-UDF scan output,
  that lands in temp-DB RAM. The mission names bounded, self-throttling execution as a first-class
  constraint, and the recorded scalar-over-set materialization spike measured 2.5 GB vs 22 GB+ for
  exactly this class of shape change. The plan neither bounds nor measures it.
- Fix: Add one bullet to plan.md § Impact stating that the decline path ships the shard's unfiltered
  output to Exasol for filtering, so its temp-DB RAM scales with scanned rows rather than result
  rows, and that this matches the three existing wrapper routes rather than introducing a new class.
  Add one row to § Verification § Manual Testing measuring `TEMP_DB_RAM_PEAK` for a declined-filter
  row scan against the same query with a rendering filter.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: plan.md § Implementation Tasks task 4;
  `vs-adapter/pushdown-planning-join-fallback/spec.md` § Background bullet 3 and its CHANGED scenario
- Issue: Task 4 says "return `join_render_decline` when the residual set is non-empty but
  `render_df_filter_qualified` returns `None`", and the join-fallback delta hardens that into spec
  prose: "A residual set that is non-empty but renders to nothing is an unrenderable predicate, not
  an empty clause, and returns the wrapper's existing client-facing error." That is false.
  `render_df_filter_qualified` (`joins/rendering.rs:113-118`) delegates to
  `render_df_filter_exasol_safe`, which returns `None` for a trivially-true result as well as an
  unrenderable one (`crates/vs-expression/src/lib.rs:1551-1558`: `if result == "TRUE" || result ==
  "NULL" { None }`). A column-free conjunct is residual by construction — `conjunct_single_side`
  returns `None` when `!any_column` (`joins/rendering.rs:152-158`) — so a two-table join carrying a
  single trivially-true top-level conjunct has a non-empty residual set that renders to nothing, and
  the plan converts today's correct "no outer WHERE" into a hard client-facing error. This
  contradicts the recorded scenario
  `sql-comprehension/vs-expression-translator-scalar-ops/Trivially-true filter suppressed in safe
  variant`, whose clause reads "SHALL suppress the same two trivially-true results, so the outer
  WHERE residual of the N-scan join wrapper omits a no-op conjunct on the Exasol path too" — a spec
  the plan does not list as CHANGED. It is also the plan's own root-cause defect re-created one
  dialect over: the plan correctly uses the non-suppressing `render_expression_safe` on the
  DataFusion side and then reads the suppressing renderer's `None` as "declined" on the Exasol side.
- Fix: Rewrite plan.md task 4's error condition to trigger only on a genuine Exasol render failure —
  gate it on the non-suppressing `render_expression_qualified` (`joins/rendering.rs:100-105`)
  returning `None` for the combined residual tree, so a residual that renders trivially true still
  emits no outer `WHERE`. Rewrite the third § Background bullet of
  `vs-adapter/pushdown-planning-join-fallback/spec.md` to distinguish the three outcomes explicitly
  (residual absent, residual trivially true, residual unrenderable) and to state that only the third
  errors. Change that delta's clause "a non-empty residual set the qualified Exasol render cannot
  express SHALL return the wrapper's existing client-facing error" to name the non-suppressing render
  as the decision, and add a clause: "a non-empty residual set that renders trivially true SHALL emit
  no outer `WHERE` and SHALL NOT error." Add a unit test
  `trivially_true_residual_emits_no_outer_where_and_does_not_error` to plan.md task 6 with its
  § Scenario Coverage row.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks task 11
- Issue: Task 11's census claims to cover "every code doc comment asserting the disproven backstop",
  and plan.md § Goals promises "every spec and doc comment asserting the disproven backstop is
  corrected, so the library cannot re-seed the defect". Two live sites are missing:
  `crates/lakehouse-engine/src/adapter/pushdown/support.rs:537-540` — the rewrite-primitive doc's "A
  decline propagates to the root" section: "That mirrors the all-or-nothing untranslatable-predicate
  backstop — the whole filter is dropped so Exasol evaluates it natively"; and
  `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs:30-32` — the doc comment on
  `RenderedJoinPushdown::filter`, the exact field task 3 changes the semantics of: "`None` when the
  request carries none (or it is trivially true and Exasol keeps it as a backstop)".
- Fix: Add both sites to plan.md task 11's census by symbol: the rewrite-primitive doc comment in
  `pushdown/support.rs` whose "A decline propagates to the root" section names the backstop, and the
  `RenderedJoinPushdown::filter` field doc in `joins/sql_builders.rs`. For the latter, require the
  replacement text to state the field's post-change meaning — absent or trivially true only, never
  declined, because a declined filter forfeits the broadcast plan.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `vs-adapter/pushdown-planning/spec.md` § Scenarios
- Issue: The delta supersedes the recorded Background bullet at
  `specs/vs-adapter/pushdown-planning/spec.md:38` and adds one NEW scenario, but leaves the recorded
  scenario a reader consults first for filter pushdown — "Filter predicate is pushed into the scan
  spec" — unmarked. Its clause still reads "SHALL translate the predicate into the shard-invariant
  common spec passed to the UDF, **omitting (never mistranslating) any node it cannot render**", with
  no self-apply terminal. Read literally it sanctions per-node omission while keeping the rest of the
  tree, which is the defect; read loosely it describes the whole-filter decline but stops before the
  wrapper. Either way the primary recorded scenario for this behavior no longer describes what ships.
- Fix: Add a `DELTA:CHANGED` copy of the recorded scenario "Filter predicate is pushed into the scan
  spec" to `vs-adapter/pushdown-planning/spec.md`, replacing the "omitting (never mistranslating) any
  node it cannot render" clause with two clauses: the decline is all-or-nothing over the whole
  top-level filter, and the declined filter is self-applied in the qualified wrapper's `WHERE` per
  `vs-adapter/pushdown-declined-filter-self-apply`. Add its § Scenario Coverage row.

## Task Breakdown

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Implementation Tasks tasks 3-6; § Parallelization Groups B and C
- Issue: Tasks 3, 4, and 5 are pure implementation with no test named in them; every unit test for
  all three lands in task 6, a separate task in a separate parallel group ("Group B → Group C").
  That inverts the failing-test-first cycle the implementer agents run under: task 3 ships a
  behavior change with no test in the same unit of work, and task 6 retro-fits eight tests across
  five files. It also makes each of tasks 3-5 unverifiable as a unit — the plan's own criterion for
  task sizing.
- Fix: Dissolve plan.md task 6 by moving each of its tests into the task that creates the behavior:
  `broadcast_declines_on_unrenderable_filter_stays_eligible_when_absent` into task 3;
  `declined_side_local_conjunct_partitions_to_residual` and its rendering-conjunct complement into
  task 4; `single_table_wrapper_renders_declined_predicate_in_exasol_dialect`,
  `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper`,
  `nested_like_decline_routes_to_wrapper_where`, `trivially_true_filter_omitted_without_wrapper`, and
  `iceberg_pruning_input_unchanged_when_df_render_declines` into task 5. Keep only the two
  `dispatch_golden` no-change fixtures as their own task, since they assert across all three sites.
  Update § Parallelization to drop Group C and re-point Group B → Group D.

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Verification § Scenario Coverage;
  `vs-adapter/pushdown-planning-like-type-coercion/spec.md`
- Issue: The like-type-coercion delta carries four CHANGED scenarios (DECIMAL column, integer column,
  unresolvable column type, nested non-string LIKE). The coverage table collapses three of them into
  one row — "LIKE on a DECIMAL / integer / unresolvable column declines the whole filter (CHANGED)" —
  mapped to a single test named for the DECIMAL case
  (`e2e_declined_filter_like_on_decimal_returns_filtered_rows`). The integer-column and
  unresolvable-type scenarios therefore have no test that distinguishes them, and the
  unresolvable-type scenario adds a substantive new clause (self-apply on a fail-safe decline whose
  subject type never resolved) that no listed test exercises.
- Fix: Split that row of plan.md § Verification § Scenario Coverage into three, one per CHANGED
  scenario, and add two unit tests to the task that implements task 5:
  `declined_like_on_integer_column_routes_to_wrapper_where` and
  `declined_like_on_unresolvable_column_routes_to_wrapper_where`, both in
  `crates/lakehouse-engine/src/adapter/pushdown/support.rs`.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Implementation Tasks task 5
- Issue: Task 5 instructs "In `handle_pushdown` **and** `build_dispatch_sql` (`pushdown/mod.rs`),
  compute the decline — filter present and non-null AND (`apply_type_rewrites` returned `None` OR
  `!datafusion_renderable(rewritten)`)". That names two owners for one decision, in two functions,
  each free to drift — the back-door duplication the design-philosophy guardrail flags first, and the
  same shape as the defect being fixed (one classification, several independent sites). The task also
  never says which of the two actually routes, so an implementer cannot tell whether to compute once
  and thread the result or to recompute. Decision [6] deliberately gave the trivially-true rule a
  single owner; the decline classification gets none.
- Fix: Rewrite plan.md task 5 to compute the decline exactly once, in `handle_pushdown`, alongside
  the existing `filter` computation at `pushdown/mod.rs:190-194` (both read the same
  `filter_json_raw` and `col_types`), and to thread the result into `build_dispatch_sql` as one new
  parameter carrying the ORIGINAL filter tree on decline and nothing otherwise. State in the task
  that `build_dispatch_sql` MUST NOT recompute renderability. Add a matching clause to
  `vs-adapter/pushdown-declined-filter-self-apply/spec.md` § Background naming the single owner of
  the decline classification, mirroring the bullet that already names `crates/vs-expression` as owner
  of the trivially-true rule.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: `vs-adapter/pushdown-declined-filter-self-apply/spec.md` lines 3-9 (the Feature
  description under `# Feature:`)
- Issue: The governed Feature-description line runs three sentences and ~90 words, with a 34-word
  lead sentence — over the 25-word cap. Sentences two and three restate § Background bullets 2 and 3
  verbatim in substance ("Exasol decides what to delegate purely from the capabilities response,
  before the pushdown request is built"; "Every WHERE-filter render site therefore distinguishes an
  ABSENT filter … from a DECLINED one"), so the description is not front-loading a conclusion, it is
  previewing the Background.
- Fix: Cut the Feature description in
  `vs-adapter/pushdown-declined-filter-self-apply/spec.md` to one sentence under 25 words stating
  the guarantee alone — for example: "Guarantees every WHERE predicate the adapter accepts is
  evaluated, by self-applying in the adapter's own returned SQL any predicate it cannot push to
  DataFusion." Delete the second and third sentences; both facts already appear in § Background.

[otherwise no objection — axis checked: plan.md § Summary holds the two-sentence cap and leads with
the decision; § Impact and § Consequences lead with outcomes; the decision-log Rationale fields are
terse and cite artifacts rather than hedge. Spot-checked for banned filler (`basically`, `simply`,
`obviously`, `just`, `actually`) and escape clauses (`as appropriate`, `where possible`, `etc.`,
`and/or`) across plan.md, decision-log.md, and all 11 deltas — none found.]

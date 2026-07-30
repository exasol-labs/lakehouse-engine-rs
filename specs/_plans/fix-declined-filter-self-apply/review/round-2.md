# Plan Review Findings: fix-declined-filter-self-apply (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 4 (Blockers: 1, Advisory: 3)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

- **Resolved: [REQUIREMENT_CONFLICT] Task 4 screened the Iceberg pruning input.** Verified against the
  real code, not the report. `side_local_filter`
  (`crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs:200-213`) and
  `cross_side_residual_filter` (`:215-226`) are untouched and still purely structural (`keep` predicate
  = `conjunct_single_side` attribution only). plan.md task 4 now opens "Leave `side_local_filter` and
  `cross_side_residual_filter` UNCHANGED" and names `joins/mod.rs:122` as the pruning consumer the
  screen must not reach — confirmed as
  `let side_filter = filter.and_then(|f| side_local_filter(f, &leaf.table_name));`. The two render call
  sites the plan cites are correct: `joins/sql_builders.rs:396` is `.and_then(cross_side_residual_filter)`
  and `:419` is the per-leg `side_local_filter` call, both inside `build_n_scan_join_sql`.
  The partition is an exact complement: legs receive `{c : renderable(c) ∧ single_side(c)=Some(t)}`;
  the outer residual receives `cross_side_residual_filter(leg_eligible)` = `{c : renderable(c) ∧
  single_side(c)=None}` unioned with `declined_only(where_filter)` = `{c : ¬renderable(c)}`. Union = all
  conjuncts, pairwise disjoint — nothing doubled, nothing dropped. Re-forming and re-flattening is
  safe: `partition_conjuncts` rebuilds a `predicate_and` and `flatten_conjuncts` (`:159-173`) recurses
  through nested `predicate_and`, so the top-level conjunct set is preserved through the two-stage
  screen. Projection is also safe, which the plan did not need to claim but which had to hold:
  `referenced_side_columns` (`:285-325`) narrows each leg from the FULL filter via
  `referenced_clause_values` (`:246-283`), so a conjunct moved from a leg to the outer `WHERE` still has
  its column projected by that leg. Spec side: `pushdown-planning-join-fallback/spec.md` § Background
  bullet 2 and CHANGED clause line 41 both state the pruning predicate keeps every side-local conjunct;
  the self-apply delta's Background bullet at line 50-53 names both pruning inputs.
- **Resolved: [REQUIREMENT_CONFLICT] Task 4's error condition re-created the root-cause conflation.**
  The gate is now the non-suppressing renderer, and the two renderers behave as the plan asserts.
  `render_df_filter_qualified` (`rendering.rs:106-117`) delegates to `render_df_filter_exasol_safe`,
  which suppresses (`crates/vs-expression/src/lib.rs`: `if result == "TRUE" || result == "NULL" { None }`);
  `render_expression_qualified` (`rendering.rs:76-104`) delegates to `render_expression_exasol_safe`,
  which does not. So `df_filter == None ∧ expression == Some` is exactly `{render ∈ {TRUE, NULL}}` —
  the classification is precise, not approximate. plan.md task 4 lines 240-245 and task 5 lines 269-271
  both state the three outcomes and gate the error on `render_expression_qualified`; § Decision
  lines 120-123 and the § Consequences row record the same. `pushdown-planning-join-fallback/spec.md`
  § Background bullet 4 distinguishes absent / trivially-true / unrenderable, and its CHANGED scenario
  carries both clauses (line 42 errors only on the non-suppressing render, line 43 forbids erroring on a
  trivially-true residual).
- **Both new tests are specified, named as prescribed, and carry an asserted invariant.**
  `join_side_pruning_input_unchanged_when_df_render_declines` — plan.md task 4 line 251, with the
  two-sided assertion spelled out ("`side_local_filter` still returns the declined conjunct for
  `plan_join`'s pruning input while the screened leg filter omits it"), § Verification row at line 393,
  location `joins/rendering.rs` in both. `trivially_true_residual_emits_no_outer_where_and_does_not_error`
  — task 4 line 254, § Verification row at line 394, location `joins/sql_builders.rs` in both. Both are
  writable where placed: `side_local_filter`, `renderable_only`, and `declined_only` all live in
  `rendering.rs`, and the residual render site is in `sql_builders.rs`.
- **Both deliberate deviations are acceptable, and § Parallelization is consistent.** Keeping task 6 as
  the golden-fixture task is what the round-1 advisory's own `Fix:` line instructed ("Keep only the two
  `dispatch_golden` no-change fixtures as their own task"); the "drop Group C" half was conditional on
  dissolving task 6 entirely, which the same Fix line contradicts. Task 6 exists (lines 307-309), Group
  C = {6} resolves to it, and both its fixtures appear in § Scenario Coverage (lines 387, 397). No group
  or dependency edge names a task that no longer exists. Every test named in tasks 1, 3, 4, and 5 has a
  matching § Scenario Coverage row with the same file location. `speq plan validate
  fix-declined-filter-self-apply` passes (AND-step-count warnings only).

## Intent Fidelity

[no objection — axis checked: the revision changed no user-facing decision. Both direction changes were
prescribed by round-1 `Fix:` lines and are recorded as `[plan-review]` entries in decision-log.md
(lines 231-251, 252-271) with decision [5] revised in place rather than duplicated. The four interview
answers stay operationalized: three surfaces in scope (tasks 3-5), self-apply via the qualified
wrapper's outer `WHERE`, wrapper-only-on-decline (the 3 pre-existing
`qualified_single_table_fallback_pushdown` callers at `pushdown/mod.rs:430`, `:500`, `:543` all pass
`None`, verified), CLAUDE.md fact as task 2 with no issue number. #228 still explicitly not adjudicated.]

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks task 5; `vs-adapter/pushdown-planning/spec.md` § Scenarios,
  NEW scenario "A declined WHERE filter routes the single-table request to the qualified wrapper";
  `vs-adapter/pushdown-declined-filter-self-apply/spec.md` § Scenarios, scenario 1
- Issue: Task 5 routes a declined filter to `qualified_single_table_fallback_pushdown` "ahead of the
  routing classifier … so one route serves the row-scan, top-N, single-group-aggregate,
  grouped-aggregate, and `COUNT(DISTINCT)` shapes alike", and the NEW spec scenario hardens it: "of any
  dispatch shape — row scan, top-N, single-group aggregate, grouped aggregate, or `COUNT(DISTINCT)` …
  the dispatcher SHALL route the request to the qualified single-table wrapper BEFORE the routing
  classifier runs". That assumes the wrapper is column-shape-correct for every shape it now receives. It
  is not, for a request with an absent or empty `selectList` — a genuine `SELECT *`, which only the new
  route can reach. `qualified_single_table_fallback_pushdown`
  (`crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs:846-879`) derives the fan-out
  projection from `referenced_column_projection` (`:707-744`), which walks
  `referenced_clause_values` and has NO absent/empty-`selectList` short-circuit — deliberately, per that
  function's doc and the recorded `vs-adapter/pushdown-joins-module-structure` scenario "One clause walk
  feeds both wrapper column-narrowing routines". So for `SELECT * FROM t WHERE SECOND(C_TS, 3) > 1` the
  collected name set is the filter's columns alone (`{C_TS}`), the fan-out projects only `C_TS`, and
  `build_qualified_single_table_fallback_sql` (`:746-831`) builds `cols_per_side` from that same narrowed
  `fan_out_spec.common.projection`. The outer select list then takes
  `n_scan_join_select_items`' absent-`selectList` branch (`:233-257`) into
  `n_full_row_qualified_items` (`:181-196`), which enumerates `cols_per_side` — one column, not the base
  row. Exasol validates the pushdown result positionally, which `build_dispatch_sql`'s own comment at
  `pushdown/mod.rs:517-540` documents as `04000` "Expected number of columns is N but pushdown query has
  M", or "Data type mismatch in column number K" when the counts coincide. The shape is reachable and
  the repo treats it as real: `project_columns` (`support.rs:1098-1298`) has a dedicated arm — "A
  `None`/empty/non-array select list is NOT a widening — the full base row is the correct answer there,
  and `false` keeps a genuine `SELECT *` on the scan path" — so today such a request takes the bare
  row-scan path (`projection_widened == false`) and never reaches this wrapper, and the N-scan wrapper
  handles the same shape correctly only because `referenced_side_columns` carries the guard
  `referenced_column_projection` lacks. The empty-array variant diverges too: `project_columns` projects
  the first column only (arity 1), while the wrapper would return one column per referenced column. Net
  effect: the plan turns `SELECT * … WHERE <declined>` from "wrong rows" into a hard client-facing
  error or a silently truncated column set — not the deliberate both-dialects-unrenderable trade-off
  § Impact documents, since the predicate renders fine in the Exasol dialect. No task step, spec clause,
  test, or § Impact bullet covers it.
- Fix: Add a step to plan.md task 5: on the decline route, when `pushdown_req["selectList"]` is absent,
  JSON null, or an empty array, pass the FULL base-row projection (every `col_types` entry, in order,
  with its Exasol type) instead of `referenced_column_projection`'s narrowed set, so
  `n_full_row_qualified_items` enumerates exactly the columns Exasol validates positionally. State in
  the task that the guard MUST be added at the new decline route — as a parameter or a pre-computed
  projection passed into `qualified_single_table_fallback_pushdown` — and MUST NOT be folded into
  `referenced_column_projection`, because `referenced_clause_values`' doc comment and
  `vs-adapter/pushdown-joins-module-structure`'s "One clause walk feeds both wrapper column-narrowing
  routines" scenario forbid that. Name both shapes explicitly: absent/JSON-null `selectList`
  (`project_columns` → full base row, `projection_widened == false`) and empty-array `selectList`
  (`project_columns` → first column only). Add a clause to the NEW scenario "A declined WHERE filter
  routes the single-table request to the qualified wrapper" in `vs-adapter/pushdown-planning/spec.md`:
  the wrapper's returned column count, order, and declared types SHALL equal what the request's
  `selectList` declares, and an absent or empty `selectList` SHALL return the full base row, so the
  route never trips Exasol's positional `04000` validation. Add the unit test
  `declined_filter_with_absent_select_list_projects_full_row` to task 5
  (`crates/lakehouse-engine/src/adapter/pushdown/mod.rs`) with its § Verification § Scenario Coverage
  row, add a `SELECT * FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE SECOND(C_TS, 3) > 1` case to task
  7's e2e list, and add a § Manual Testing row for it — CLAUDE.md requires the positional-validation
  behavior to be confirmed against the live Docker Exasol container, not assumed.

## Requirement Quality

[no objection — axis checked: the two revised deltas are testable as written and no longer conflict with
a recorded spec. `pushdown-planning-join-fallback/spec.md`'s CHANGED scenario states the leg/residual
partition as an iff (line 37-38), asserts totality and disjointness (line 39), pins the pruning
predicate (line 41), and splits the render into the three outcomes with only the third erroring (lines
42-43) — each writable as a pass/fail test. The trivially-true clause now agrees with the recorded
`sql-comprehension/vs-expression-translator-scalar-ops` suppression scenario instead of contradicting
it, so that spec correctly stays off the CHANGED list. The self-apply delta's Background bullets 6-7
(lines 30-39) and § Scenarios 7 carry the same three-outcome rule, and its Iceberg-spec-check bullet
(lines 54-57) is now accurate given the pruning inputs are untouched. The absent-`selectList` gap is
raised once, under Feasibility, with its spec-clause fix included there.]

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Implementation Tasks task 5, the "Exact call-site census" table
- Issue: The census claims to be exact — "every site below takes the new argument" — and covers 11 sites,
  all verified correct at the cited lines (`pushdown/mod.rs:239`, `:430`, `:500`, `:543`, `:1554`;
  `dispatch_golden.rs:193`; `joins/sql_builders.rs:1997`, `:2269`, `:2310`; `grouped_agg.rs:3129`;
  `support.rs:3017`). One caller is missing: `joins/sql_builders.rs:2478` calls
  `qualified_single_table_fallback_pushdown` directly from that file's test module (the wrapper
  error-path test documented at `:2454`). Task 5 adds a parameter to that symbol, so the omitted site
  will not compile. The project's recorded lesson is that a signature-change census must be exact
  including test crates; a census advertised as exact and silently short is the failure mode that
  lesson names.
- Fix: Add a twelfth row to plan.md task 5's census table: `joins/sql_builders.rs:2478`
  (`qualified_single_table_fallback_pushdown`, test) with argument `None`. Change the table's
  introductory sentence to state that it covers both symbols' production AND test callers, so the
  implementer knows the census spans `qualified_single_table_fallback_pushdown` as well as
  `build_qualified_single_table_fallback_sql`.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Implementation Tasks task 4 (the doc-comment clause) and task 11 (the census)
- Issue: The render-site screen makes two doc comments in `joins/rendering.rs` false, and neither task
  corrects them. `cross_side_residual_filter`'s doc (`:215-226`) reads "This is the exact set-complement
  of the per-side [`side_local_filter`] slices: every conjunct is either side-local to exactly one table
  (pushed into that side's fan-out leg) or cross-side residual (kept here, in the outer wrapper's
  WHERE)" — after the change a side-local declined conjunct is side-local yet NOT pushed into its leg,
  so the stated complement no longer describes the render path. `side_local_filter`'s doc (`:200-213`)
  reads "This is what is threaded into (a) that side's `resolve_file_list` for Iceberg manifest pruning
  and (b) that side's fan-out `ScanSpec.filter`", which stops being the whole truth once (b) is fed a
  pre-screened tree while (a) is not — precisely the distinction the B1 fix exists to make. Task 4
  rewrites only `build_side_fan_out_sql`'s stale claim, and task 11's census lists neither function. The
  result is the partition invariant documented in one place and enforced in another, with the two
  disagreeing — the same one-decision-two-owners shape the plan avoids everywhere else.
- Fix: Add a clause to plan.md task 4 requiring both doc comments to be updated in the same unit of
  work: `side_local_filter`'s doc MUST state that consumer (a) receives the raw filter and consumer (b)
  receives a pre-screened one, and that the function itself makes no renderability decision;
  `cross_side_residual_filter`'s doc MUST state that the complement it forms is over whatever tree it is
  given, and that on the render path the outer `WHERE` additionally carries the declined conjuncts, so
  the total partition is `renderable_only`/`declined_only` composed with these two. Add both symbols to
  task 11's census so the docs-last pass verifies them.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md § Impact, lines 174-187
- Issue: The lead-in reads "Three behavior changes an operator will observe:" and is followed by four
  bullets — the temp-DB RAM bullet added in this revision was not counted. A reader scanning the
  governed § Impact section gets a wrong count on the first line, and a PR reviewer reading the same
  section into a comment carries the error forward. The added bullet's first sentence also runs 30
  words, over the 25-word cap for governed prose.
- Fix: In plan.md § Impact change "Three behavior changes an operator will observe:" to "Four behavior
  changes an operator will observe:", and split the new bullet's first sentence at the comma so neither
  half exceeds 25 words.

[otherwise no objection — axis checked: the revised Feature description in
`vs-adapter/pushdown-declined-filter-self-apply/spec.md:3-4` is now one sentence of 24 words stating the
guarantee alone, with the Background-restating sentences deleted. The prose added this round to
`pushdown-planning-join-fallback/spec.md` § Background and to plan.md § Decision, § Consequences, and
the task bodies is un-governed (Background bullets, tables, task lines) and in any case free of banned
filler — spot-checked `basically`, `simply`, `obviously`, `just`, `actually`, `as appropriate`, `where
possible`, `etc.`, `and/or` across plan.md, decision-log.md, and the three revised deltas: none found.]

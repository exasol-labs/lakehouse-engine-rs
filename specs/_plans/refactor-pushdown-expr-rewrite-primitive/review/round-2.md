# Plan Review Findings: refactor-pushdown-expr-rewrite-primitive (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 4 (Blockers: 1, Advisory: 3)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

- **Not resolved: [UNSTATED_ASSUMPTION] the unresolvable-subject sub-case** — the CHANGED scenario and
  Background bullet 21 were fixed correctly, but the absolute survives verbatim in `plan.md:101` and is
  restated normatively in the sibling NEW scenario at
  `vs-adapter/pushdown-planning-like-type-coercion/spec.md:45`. See Feasibility below.
- **Resolved: [REQUIREMENT_CONFLICT] byte-identity clause** —
  `vs-adapter/pushdown-module-structure/spec.md:24` now scopes byte-identity to "every request whose
  per-node decisions the extraction itself leaves unchanged" and names the like-coercion widened-reach
  scenarios as "the ONE deliberate exception … byte-identity here scopes to the extraction, NOT to this
  plan's end state". The universal claim is gone; the two deltas no longer contradict. The overlapping
  evidence-label correction inside the clause introduced no new inconsistency of substance — see the
  Prose axis for the one residual divergence with the (deliberately unaddressed) `plan.md:139` row.
- **Resolved: [TASK_GRANULARITY] leaf-equivalence test ordering** — the test is now task 1 in Group A,
  ordered `Group A → Group B` before the primitive extraction, and its text mandates that it "MUST be
  added and PASS against today's `rewrite_decimal_stringifications` with its `!node.is_object()` early
  return still in place" and "re-run unchanged after task 4", with the halt condition stated. Verified
  the test can pass pre-migration: `support.rs:670-673` returns `node.clone()` for a non-object.
  Verified no residual dependency on the old grouping — § Parallelization assigns all eleven tasks
  across Groups A–I with a complete chain (`A→B→C→D→E→F→G→H→I`), and no task text cites a stale
  number (task 1 → "task 4"; § Requirements → "task 10"; task 10 → "task 9 covers the first").
- **Resolved: [TRACEABILITY_GAP] stale-documentation sweep** — `grep -rn "junction" crates/` re-run
  this session returns exactly the four live sites the plan enumerates: `support.rs:506`,
  `support.rs:875`, `support.rs:5297`, `mod.rs:899`. Every other hit is an unrelated HAVING/AND-junction
  reference in `grouped_agg.rs`, `request_shape.rs`, `e2e_scan_test.rs`, or `vs-expression`, as
  § Requirements states. Task 9's range `support.rs:500-507` is exactly the traversal paragraph
  (`500` = "Walks the filter tree through the only node types…", `507` = "returned unchanged — this
  guard only inspects `LIKE` subjects."). Task 10's three sites resolve to real doc comments preceding
  real tests (`string_fn_guard_reaches_function_under_comparison_predicate` at `support.rs:5300`,
  `where_filter_string_fn_under_comparison_predicate_coerced` at `mod.rs:906`), the `mod.rs:188-209`
  entry is now unconditional and quotes the right parenthetical, and `mod.rs:963-968` /
  `joins/rendering.rs:529` are correctly recorded as clean. One site outside the grep's reach is still
  missing — see Task Breakdown, ADVISORY.

## Intent Fidelity

No objection — axis checked. Both #257 commits remain planned and sequenced; the primitive-plus-two-consts
shape is unchanged; every "Do NOT" still honored (no visitor trait, no typed AST, no pass-ordering
pipeline, #177's blind `walk_json` separate per decision-log [6]); filter-only wiring preserved per
interview A3 with #215/#219 named in both plan.md and the delta; tracked exceptions #211/#216/#223/#228
untouched. Verified `project_columns` (`support.rs:1104-1125`) still calls only
`string_function_arg_type_guard` and `rewrite_decimal_stringifications`, never the LIKE guard, so no
surface is silently added. The round-1 `pub(super)`/#177 SCOPE_CREEP advisory is knowingly unaddressed
per the orchestrator and is not re-raised.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER

- Location: `plan.md:101` (§ Consequences, row 3 Rationale); `vs-adapter/pushdown-planning-like-type-coercion/spec.md:45` (NEW scenario, THEN clause); same file `:20` (Background bullet 20)
- Issue: round-1 B1's absolute was corrected in two of four places and left standing in two.
  `plan.md:101` still reads "the pre-change behavior at every newly-reached position was a hard scan
  failure, never a working pushdown" — the exact sentence the fix was to delete. That row now
  contradicts itself: its Decision cell says the widened reach "may turn a former pushdown into a
  decline" while its Rationale cell says no former pushdown existed. It also contradicts the corrected
  § Impact at `plan.md:126-130` and the corrected decision-log [7] rationale.
  Worse, the claim is restated **normatively** in a permanent-spec clause the round-1 fix did not name:
  `spec.md:45` enumerates the decline triggers as "a DECIMAL, integer, DOUBLE, BOOLEAN, TIMESTAMP, or
  **unresolvable** subject" and then contrasts all of them with "rather than hard-failing the DataFusion
  scan as the junction-only traversal did". For the unresolvable subject that contrast is false, and the
  sibling CHANGED clause at `:36` says so explicitly ("MAY instead lose a pushdown that previously
  rendered as-is and succeeded … SHALL NOT be recorded as a fixed hard failure"). Verified again this
  session: `extract_all_column_types` (`support.rs:435-451`) `filter_map`s `involvedTables[0].columns`
  and drops any entry missing `name` or `dataType`, first involved table only — a genuinely VARCHAR
  column can miss the lookup and rendered `Utf8 LIKE Utf8` successfully at a newly-reached position.
  Recording that as a fixed hard failure is the wrong requirement, in the same delta file that forbids
  it one clause earlier. Background bullet 20 carries the same over-generalization without qualification
  ("was never type-checked, rendered as-is, and hard-failed the DataFusion scan"), which is false for a
  resolvable VARCHAR subject as well as an unresolvable one, and is only walked back by bullet 21.
- Fix: In `plan.md` § Consequences row 3, replace the Rationale cell with: "A decline is always correct
  (Exasol evaluates natively). Where the subject type resolves to a non-string type the pre-change
  render hard-failed the scan, so the decline fixes a crash; where the name does not resolve it may cost
  a working pushdown — slower, never wrong (§ Impact, decision-log [7])." In
  `vs-adapter/pushdown-planning-like-type-coercion/spec.md:45`, remove `unresolvable` from the
  enumeration that the "rather than hard-failing the DataFusion scan" contrast governs — keep the
  contrast for "a DECIMAL, integer, DOUBLE, BOOLEAN, or TIMESTAMP subject" — and append a clause
  stating that an unresolvable subject at that position SHALL decline under the same fail-safe rule,
  with the pushdown-loss trade as recorded in the "A nested non-string LIKE declines the entire
  enclosing filter" scenario. In the same file `:20`, scope the blind-spot sentence to a non-string
  subject: "…was never type-checked and rendered as-is, so a non-string subject hard-failed the
  DataFusion scan."

Certified otherwise: no new `[HIDDEN_DEPENDENCY]` — the change stays inside `pushdown/support.rs` and
`pushdown/mod.rs`, and `cargo test --features exasol-e2e --no-run` is on both gates. No `[NFR_IGNORED]`
— traversal depth and shape are unchanged for the two wide walkers (verified `support.rs:676-691` and
`:901-917` are byte-for-byte the same child conditions the primitive must reproduce), and the
`filter_json_raw` invariant holds at `mod.rs:210-214`, which composes on a borrowed tree while
`resolve_file_list` at `mod.rs:229-230` receives the original. No `[EFFORT_MISESTIMATION]`: task 8's two
stated equivalences both check out — the old `predicate_not` arm at `support.rs:557-559` recursed a
non-object child and got `Some(clone)` back via the `_ =>` arm at `:565`, matching the primitive's skip;
and a bare `column` subject survives the primitive's pre-dispatch child visit because the closure acts
only on the two LIKE node types.

## Requirement Quality

No objection — axis checked. Round-1 B2 is resolved (see Recheck). Verified every test name the
§ Verification table cites exists: `like_guard_nested_decimal_declines_whole_filter`
(`support.rs:4031`), `like_guard_not_wrapped_decimal_declines` (`:4098`),
`rewrite_reaches_decimal_inside_case_then_branch` (`:4381`),
`rewrite_nested_concat_wraps_only_inner_decimal` (`:4196`),
`string_fn_guard_passes_through_non_object_node` (`:5149`),
`string_fn_guard_reaches_function_under_comparison_predicate` (`:5300`),
`string_fn_guard_nested_decline_propagates_to_root` (`:5319`), and all five `mod.rs` chain tests
(`:827`, `:873`, `:906`, `:938`, `:970`). Verified no existing assertion flips under commit 2: the only
`mod.rs` chain test carrying a LIKE over a non-string subject is
`where_filter_upper_decimal_inside_like_subject_coerced`, whose subject is a `function_scalar`, not a
bare `column`, so the widened traversal visits its `arguments` inertly and the closure leaves it alone.
Verified the manual-test expectation at `plan.md:289` is real: for `CASE WHEN L_QUANTITY LIKE '1%' …`
neither the string guard (`support.rs:921` requires `type == "function_scalar"`) nor the decimal
rewriter (`:696-745`, whose `_ => out` arm covers `predicate_like`) touches a bare DECIMAL LIKE subject
today, so the pre-change render does reach DataFusion and hard-fail. The two round-1
`[AMBIGUOUS_REQUIREMENT]` advisories (`MAY apply` at module-structure `:23`, the rationale-only clause
at string-fn `:22`) are knowingly unaddressed and the revision did not turn either into a blocker.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY

- Location: `plan.md:144` (§ Requirements, "Stale-documentation sweep") and task 10 (`:204-222`)
- Issue: the sweep now claims completeness — the four grep sites "are the complete grep-verified set as
  of planning" plus exactly one site "the grep does NOT catch". There are two. The catch-all arm's
  inline comment at `support.rs:563-564` reads "Any other node (predicate_equal, column, literals, …)
  is not a LIKE and **cannot nest one in this grammar** — returned unchanged". Commit 2 falsifies it
  precisely: a `predicate_equal` is exactly the node the widened traversal now descends through to
  reach a LIKE under `left`, which is the plan's own headline repro. The comment survives task 8's
  rewrite because it annotates the `_` arm that becomes the closure's catch-all, and § Dead Code
  Removal names only `~support.rs:544-561`, stopping two lines short of it. Lower severity than
  round-1 B4 because the comment sits inside the function task 8 rewrites, so an attentive implementer
  is likely to hit it — but the plan asserts a complete enumeration and this one is outside it.
- Fix: In `plan.md` task 10, add a second non-grep site alongside `mod.rs:188-209`: the inline comment
  on `like_subject_type_guard`'s `_` match arm (`support.rs:563-564`), whose "cannot nest one in this
  grammar" claim MUST be deleted or restated as "is not itself a LIKE" when the arm becomes the
  closure's catch-all. Change § Requirements' "the chain comment at `mod.rs:188-209` asserts the same
  contrast without using the word" to name both non-grep sites.

#### [TRACEABILITY_GAP] ADVISORY

- Location: `vs-adapter/pushdown-module-structure/spec.md:23`; `plan.md` § Verification, row 1
- Issue: the clause justifies the leaf simplification by citing evidence that exists for two of the
  three guards: "which SHALL be behavior-preserving because every guard's per-node decision returns a
  node carrying no `type` it governs unchanged, the property **each guard's own leaf pass-through
  test** pins". Only two such tests will exist —
  `string_fn_guard_passes_through_non_object_node` (`support.rs:5149`) and the new
  `decimal_rewrite_passes_through_non_object_node`. `like_subject_type_guard` has no leaf pass-through
  test among its nine unit tests (`support.rs:3897-4114`), and the plan adds none. The behavior is
  fine — the LIKE guard never had an `!node.is_object()` early return, so its `_ => Some(filter.clone())`
  arm at `support.rs:565` already applies its dispatch to leaves and commit 2 changes nothing there —
  so the defect is the spec clause overclaiming its evidence, not a missing guard behavior. A future
  reader auditing "each guard's own leaf pass-through test" finds two of three.
- Fix: In `vs-adapter/pushdown-module-structure/spec.md:23`, scope the evidence phrase to the guards it
  covers: replace "the property each guard's own leaf pass-through test pins" with "the property the
  leaf pass-through test of each guard that previously early-returned on a non-object pins, the LIKE
  guard having always applied its dispatch to leaves".

Certified otherwise: no `[TASK_GRANULARITY]` — round-1 B3 is resolved (see Recheck), every delta has an
implementing task (module-structure → 1-5; like-coercion → 7-10; string-fn → 5), and the two-task Group
C and Group H pairs touch disjoint functions with the serialization caveat already stated at
`plan.md:249-250`.

## Design Depth

No objection — axis checked. The revision changed no design decision. The primitive stays deep: one
sentence describes it, and it removes the real information leakage the round-1 review confirmed (the
curated field list asserted independently at `support.rs:676/684` and `:901/910`, synchronized only by
the comment at `:869-875`). Verified the two duplicated child loops are byte-identical, so one const
pair genuinely owns the decision. Signature
`fn rewrite_expr_tree(node: &Json, f: &impl Fn(&Json) -> Option<Json>) -> Option<Json>` recurses
without a second monomorphization and needs no `col_types` parameter — the closure captures it. No
`[BOUNDARY_VIOLATION]`, no new configuration parameter. The round-1 `.expect` `[TACTICAL_SHORTCUT]`
advisory is knowingly unaddressed.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY

- Location: `plan.md:144` (§ Requirements sweep row), tasks 9-10 (`:195-222`), `decision-log.md:131`
- Issue: the revision specifies the doc sweep in four places and repeats the four-site list in three of
  them. The § Requirements cell alone runs seven lines and states the grep mandate three ways — "MUST
  run `grep -rn "junction" crates/`", "the grep MUST be re-run because the other hits … are unrelated",
  "The grep alone is NOT sufficient". Task 10 then re-enumerates the same sites with the same rationale.
  One idea per paragraph and the terseness rule are both broken, and a duplicated site list is a
  maintenance hazard: correcting one copy leaves three stale. The round-1 § Impact and Iceberg-row
  `[PROSE_BLOAT]` advisories are knowingly unaddressed; this instance is new, introduced by the B4 fix.
- Fix: In `plan.md:144`, cut the § Requirements sweep row to two sentences — the obligation ("Commit 2
  invalidates every code-documentation claim about `like_subject_type_guard`'s reach; each MUST be
  corrected in the same commit") and the gate ("before closing commit 2 the implementer MUST run
  `grep -rn "junction" crates/`, which is necessary but not sufficient — two sites assert the contrast
  without the word") — and cite tasks 9-10 for the site list instead of repeating it.

Residual note, not a new finding: `plan.md:139` still labels the `support.rs` guard tests a
"rendered-SQL corpus" while the corrected clause at `vs-adapter/pushdown-module-structure/spec.md:24`
now calls the same tests a "JSON-shape corpus". Verified the spec is the accurate one — those tests
assert JSON-tree equality (`support.rs:4157`, `:5306-5313`). This is the round-1
`[PROSE_UNCLEAR]` advisory the orchestrator deliberately left unactioned; the partial fix means the two
artifacts of this plan now label the same evidence two ways. Fixing `plan.md:139` closes it.

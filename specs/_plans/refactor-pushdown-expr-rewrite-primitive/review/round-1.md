# Plan Review Findings: refactor-pushdown-expr-rewrite-primitive (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 11 (Blockers: 4, Advisory: 7)
- Intent Fidelity blockers: 0

## Premortem

Three ways this fails six months out:

1. **A silent pushdown regression nobody predicted.** The plan's load-bearing claim — every
   newly-reached LIKE position "was a hard scan failure, never a working pushdown" — is false for one
   enumerated decline trigger (unresolvable subject). A single customer query loses its filter
   pushdown, gets slower, and the spec says that cannot happen. → Feasibility B1.
2. **The recorded spec library contradicts itself.** `pushdown-module-structure` records "byte-identical
   scan-driving SQL for every pushdown request" while `pushdown-planning-like-type-coercion` records the
   opposite for LIKE-in-CASE. A future planner cites whichever suits it. → Requirement Quality B2.
3. **The equivalence proof was never a proof.** The leaf-equivalence test is scheduled after the
   migration it exists to validate, so it pins the new behavior instead of the old. Six months later a
   subtle leaf divergence surfaces and the test that was supposed to catch it never could. → Task
   Breakdown B3.

The orchestrator's three flagged planner corrections all check out: production has exactly ONE chain
site (`mod.rs:210-214`; `mod tests` starts at `mod.rs:738-739`, the other five `like_subject_type_guard`
calls are at `:843/:882/:919/:951/:983`); the #207 blind spot is a LIKE nested in a CASE, reaching the
searched CASE's `arguments` (`crates/vs-expression/src/lib.rs:537-546` confirms WHEN predicates live in
`arguments`); and no existing test assertion flips under commit 2 (checked all nine LIKE unit tests at
`support.rs:3896-4114` and all five `mod.rs` chain tests). The doc-sweep enumeration does NOT check out
— see B4.

## Intent Fidelity

#### [SCOPE_CREEP] ADVISORY
- Location: plan.md § Patterns / decision-log.md § [4]; `vs-adapter/pushdown-module-structure/spec.md` Background bullet 11
- Issue: the plan declares `rewrite_expr_tree` `pub(super)` for a consumer that does not exist in this plan, and records that justification into a permanent spec Background: "The primitive is `pub(super)` in `support.rs` so the sibling `pushdown/joins` submodule can reuse it for its two rebuild-shape join walks (issue #177) without a later visibility change." Issue #257's own suggested snippet shows `fn rewrite_expr_tree(...)` — private. The recorded `pushdown-module-structure` Background already states the governing rule: "A cross-submodule private helper widens to the narrowest visibility that compiles (`pub(super)`), never to a broader public than it had before." `rewrite_expr_tree` is not cross-submodule in this plan, so the narrowest visibility that compiles is private. The `strip_table_alias` precedent cited in decision-log [4] is not analogous — that helper has a real cross-submodule caller today.
- Fix: In plan.md § Patterns, change the `pub(super) visibility` row to a private `fn` and note that #177 widens it when it adds the first cross-submodule caller. Update decision-log.md [4] to record private-until-needed as the decision. Delete the `pub(super)`/#177 sentence from `vs-adapter/pushdown-module-structure/spec.md` Background bullet 11, keeping only the sentence about #177's blind collect walker staying a separate primitive.

Certified otherwise: no `[INTENT_DRIFT]` — both commits of #257 are planned as sequenced, the primitive-plus-two-consts shape matches #257's snippet, and every "Do NOT" is honored (no visitor trait, no typed AST, no pass-ordering pipeline, #177's blind `walk_json` kept separate). No `[SCOPE_REDUCTION]` — filter-only wiring is preserved per interview A3, and tracked exceptions #211/#215/#216/#219/#223/#228 all survive: #223 and #228 live in `support.rs` step-2 code that tasks 3 and 10 leave in place (`support.rs:935`, `:961-963`), #216 in the like-coercion spec, #215/#219 named in both the plan and the delta.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Impact (lines 118-124); `vs-adapter/pushdown-planning-like-type-coercion/spec.md` Background bullet 21 and CHANGED scenario clause (line 34)
- Issue: the plan's universal claim is false for one of the decline triggers the same scenario enumerates. plan.md states a LIKE "over a non-string **or unresolvable** column nested where the junction-only traversal did not reach … previously pushed down and hard-failed the DataFusion scan"; the delta asserts "the pre-change behavior at those positions was never a working pushdown" and normatively that the decline "SHALL replace the pre-change behavior of pushing the predicate down and hard-failing the DataFusion scan, so a filter shape that previously returned no result at all now returns the correct result". For a **non-string** subject that is true — DataFusion's LIKE accepts only Utf8-family types, and VARCHAR/CHAR subjects pass through unchanged. For an **unresolvable** subject it is not: `extract_all_column_types` (`support.rs:435-451`) builds the map with `filter_map` over `involvedTables[0].columns` and silently drops any entry missing `name` or `dataType`, so a genuinely VARCHAR column can miss the lookup. Pre-change at a newly-reached position that shape rendered as-is and **succeeded** (`Utf8 LIKE Utf8`); post-change it declines the whole filter. Correct, but a lost pushdown, not a fixed crash. The recorded spec already treats the lookup miss as a reachable state — it has its own fail-safe scenario. The trade itself is settled by interview A2; the defect is that the plan and delta record an absolute that a test cannot satisfy.
- Fix: In `vs-adapter/pushdown-planning-like-type-coercion/spec.md`, split the CHANGED scenario's line-34 clause in two: keep the "replaces a hard scan failure" claim scoped to a subject whose Exasol type RESOLVES to a non-string type, and add a separate clause stating that for an UNRESOLVABLE subject at a newly-reached position the decline replaces a pushdown that previously rendered as-is, so a working pushdown MAY be traded for correct native Exasol evaluation. Rewrite Background bullet 21 to match — drop "was never a working pushdown" and name the unresolvable-subject case explicitly. In plan.md § Impact, separate the two sub-cases the same way.

Certified otherwise: no `[HIDDEN_DEPENDENCY]` — the change is confined to `pushdown/support.rs` and `pushdown/mod.rs`, no new crate, and `cargo test --features exasol-e2e --no-run` is on the gate per the project census rule. No `[NFR_IGNORED]` — recursion depth and shape are unchanged (both wide walkers already recurse the full curated set), the extra `f` call per leaf is one clone, no security/migration/concurrency surface, and the `filter_json_raw` invariant is real: `mod.rs:210-214` operates on a reference and every guard clones, so `resolve_file_list` at `mod.rs:229-230` still sees the untouched tree. No `[EFFORT_MISESTIMATION]` beyond B4.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `vs-adapter/pushdown-module-structure/spec.md` NEW scenario, final clause (line 24)
- Issue: the clause asserts "the scan-driving SQL generated for **every pushdown request** SHALL be byte-identical to its pre-refactor output". Commit 2 of this same plan deliberately breaks that for one request class, and the sibling delta says so: `vs-adapter/pushdown-planning-like-type-coercion/spec.md` line 43-44 requires a LIKE-in-CASE over a DECIMAL subject to decline (previously rendered) and over a DATE subject to be rewritten to `CAST(<col> AS VARCHAR)` (previously rendered bare). Both deltas record together at `/speq:record`, so the permanent library would carry a scenario asserting universal byte-identity next to a scenario asserting the opposite for a named shape. The clause needs to be scoped to the primitive extraction, not to the plan's end state.
- Fix: In `vs-adapter/pushdown-module-structure/spec.md`, rewrite the final clause of the NEW scenario to scope byte-identity to the traversal extraction: the SQL SHALL be byte-identical for every request whose per-node decisions are unchanged by the extraction, and the clause SHALL name `vs-adapter/pushdown-planning-like-type-coercion`'s widened-reach scenarios as the one deliberate exception, introduced by a separate commit and covered by that feature's own scenarios.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Features (lines 108-112) — no delta for `vs-adapter/pushdown-planning-decimal-string-format`
- Issue: commit 1 deletes `rewrite_decimal_stringifications`' traversal (plan.md § Dead Code Removal names `~support.rs:665-691`), but that rewriter's owning feature gets no delta. Its recorded spec attributes the recursion to the function: `specs/vs-adapter/pushdown-planning-decimal-string-format/spec.md:12` — "`rewrite_decimal_stringifications` is a recursive tree walk … at every other node it recurses into child expressions without wrapping … Nesting is handled by the recursion itself" — and `:5` — "A single shared recursive rewriter (`rewrite_decimal_stringifications`) walks each tree". After the migration the function owns no traversal, which is exactly what the module-structure delta asserts ("no traversal code of its own"). The plan judged the parallel claim in `pushdown-planning-string-fn-type-coercion` (`:24`) worth a delta; the third rewriter's identical claim was left unreconciled. Inconsistent treatment of the same defect.
- Fix: Add `specs/_plans/refactor-pushdown-expr-rewrite-primitive/vs-adapter/pushdown-planning-decimal-string-format/spec.md` with a Background-only delta reconciling `:5` and `:12` — state that the rewriter contributes a per-node stringifier decision and delegates recursion to the shared post-order primitive (`vs-adapter/pushdown-module-structure`), and that post-order nesting behavior is unchanged. Add the feature to plan.md § Features as CHANGED.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `vs-adapter/pushdown-module-structure/spec.md` NEW scenario (line 23)
- Issue: the clause reads "and **MAY** apply a guard's per-node decision to a non-object leaf node". The behavior is not optional — decision-log [2] decides it, task 1 mandates reproducing today's child conditions, and task 3 mandates deleting the `!node.is_object()` early return. RFC-2119 `MAY` permits the rejected alternative (an `is_object` early return inside the primitive) to also conform, and nothing testable follows from a `MAY`.
- Fix: Change `MAY apply` to `SHALL apply` in that clause, keeping the trailing behavior-preservation rationale unchanged.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `vs-adapter/pushdown-planning-string-fn-type-coercion/spec.md` CHANGED scenario, final clause (line 22)
- Issue: the clause's operative content is a rationale, not a behavior: `like_subject_type_guard` "SHALL leave that node alone because its subject is not a bare `column` node, NOT because its traversal cannot reach it". No test can distinguish those two reasons — the observable output is identical before and after commit 2, which the plan's own Verification table concedes ("the reworded clause changes rationale, not behavior"). A scenario clause that no test can fail is not a requirement.
- Fix: In that clause, keep only the verifiable half — the guard SHALL leave a non-bare-column LIKE subject unchanged while `string_function_arg_type_guard` coerces the DECIMAL argument inside it — and move the "same reach, different per-node decision" statement into the delta's Background, where non-testable rationale belongs.

## Task Breakdown

#### [TASK_GRANULARITY] BLOCKER
- Location: plan.md § Implementation Tasks task 2 (lines 154-157) vs § Parallelization (lines 198-214)
- Issue: task 2 reads "Add the leaf-equivalence tests **FIRST, then migrate**", and its stated purpose is "proving the primitive's 'leaves are passed to `f` too' simplification is behavior-preserving". The Parallelization table puts task 2 in Group C, and the sequencing note orders `Group A → Group B → Group C` where Group B is tasks 3 and 4 — the two migrations. The plan even rationalizes the inversion: "Group B → Group C (the leaf test and the doc rewrites describe the migrated shape)". A characterization test written after the migration confirms the new code's behavior and proves nothing about equivalence with the old, and it inverts the project's failing-test-first discipline. The equivalence itself is sound — verified: for a non-object node the decimal walker's step 2 reaches `_ => out` via `get("type") == None` (`support.rs:695-696`, `:746`) and the string guard returns `Some(out)` at `:922-923` — but the plan's own designated proof is scheduled where it cannot serve as one.
- Fix: In plan.md § Parallelization, move task 2 out of Group C into its own group ordered BEFORE Group A, and change the sequencing list to `Task 2 → Group A → Group B → (Group C = task 5) → Group D`. Amend task 2 to state that the test MUST be added and pass against the UNMIGRATED `rewrite_decimal_stringifications` first, then re-run unchanged after task 4.

#### [TRACEABILITY_GAP] BLOCKER
- Location: plan.md § Requirements "Stale-documentation sweep" (line 137) vs task 10 (lines 188-192)
- Issue: the plan states a hard requirement — "Commit 2 invalidates **every** 'junction-only recursion' claim in code documentation; each MUST be corrected in the same commit, not left to rot" — and then enumerates a sweep list that misses a site. `support.rs:5296-5298` is the doc comment on `string_fn_guard_reaches_function_under_comparison_predicate`: "The guard reaches a string function nested under a COMPARISON predicate (under `left`) — the reach `like_subject_type_guard`'s junction-only recursion does not have". It is absent from task 10's list, and the plan's Verification table lists that same test as part of "the unedited existing corpus", so nothing else in the plan will bring an implementer to it. Two further weaknesses in the same task: the `mod.rs:188-209` entry is conditional ("**if** it asserts narrow reach") when the comment does assert it — "over the whole tree (not just LIKE subjects — it reaches a string function nested under any comparison predicate too)" reads only as a contrast with the LIKE guard's narrower reach; and task 9's instruction for `like_subject_type_guard`'s own doc names only the "out of scope … pre-existing risk" caveat, leaving the traversal enumeration at `support.rs:500-506` ("Walks the filter tree through the only node types that can nest a `predicate_like` … `predicate_and` / `predicate_or` … and `predicate_not`. … Any node that is neither a junction nor a `LIKE` is returned unchanged") unnamed. A grep-verified enumeration of every `junction` occurrence in `crates/` gives exactly these live sites: `support.rs:506`, `support.rs:875`, `support.rs:5297`, `mod.rs:899`.
- Fix: In plan.md task 10, add `support.rs:5296-5298` to the sweep list with its test name. Change the `mod.rs:188-209` entry from conditional to unconditional, naming the "not just LIKE subjects" parenthetical as the claim to correct. In task 9, replace "DROP the … caveat" with an explicit instruction to rewrite the whole traversal paragraph at `support.rs:500-507`, including the junction enumeration and the "neither a junction nor a `LIKE`" sentence. Add to § Requirements that the implementer MUST run `grep -rn "junction" crates/` and confirm no live claim about `like_subject_type_guard`'s reach remains.

## Design Depth

#### [TACTICAL_SHORTCUT] ADVISORY
- Location: plan.md § Consequences row 2 and task 4 (lines 159-160); decision-log.md § [3]
- Issue: keeping `rewrite_decimal_stringifications`' `-> Json` signature "via `.expect` with a message naming the invariant" introduces a panic site into the adapter planning path where the pre-refactor function had none. The justification for the signature is sound (both call sites compose with `.map`, not `.and_then`), but `.expect` is not the only way to honor it: the closure is statically always-`Some`, so `.unwrap_or_else(|| node.clone())` preserves the exact contract with no reachable panic and no extra branch a reader must reason about. Choosing the panicking form for a provably-unreachable case adds a failure mode to a query-planning path for no benefit.
- Fix: In plan.md task 4 and § Consequences row 2, and in decision-log.md [3], replace `.expect` with a non-panicking composition — `.unwrap_or_else(|| node.clone())` — and keep the invariant statement as a doc comment rather than a panic message.

Certified otherwise: the primitive is deep, not shallow — one sentence describes it, and calling it is materially cheaper than re-deriving post-order plus curated-field selection plus decline propagation. It removes a real `[INFORMATION_LEAKAGE]`: the curated field list is today asserted independently at `support.rs:675/684` and `support.rs:901/910`, synchronized only by the comment at `:869-870`, and the consts give that decision one owner. No `[BOUNDARY_VIOLATION]` — `support.rs` names no delivery mechanism or storage engine, and the guards stay pure `&Json → Json`/`Option<Json>`. No new configuration parameter; `f` is the only knob. The rejected alternatives (visitor trait, typed AST, universal walker merged with #177's blind `walk_json`) are each rejected for a stated complexity reason, and the blind-walker boundary is correct — `walk_json` recurses `map.values()`, which would silently widen all three guards' rewrite surface into `dataType` and `name`.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md § Requirements, "Byte-identical commit 1" row (line 132)
- Issue: "The existing rendered-SQL corpus in `support.rs`" misnames the evidence. `support.rs`'s guard tests assert JSON-tree equality, not SQL (`support.rs:4157`, `:5306-5313`, `:5334-5338`); the only rendered-SQL assertions backing byte-identity are the five chain-replicating tests in `mod.rs` (`:850`, `:888`, `:925`, `:956`, `:989`). An implementer reading this row looks for a rendered-SQL corpus in the wrong file and may conclude the byte-identity gate is stronger than it is. The mislabel is inherited from issue #257 and should be corrected, not carried forward.
- Fix: In plan.md § Requirements, rewrite that row to name the two evidence classes separately: the JSON-shape equality corpus in `support.rs` for the two migrated walkers, and the five rendered-SQL chain tests in `mod.rs`'s `mod tests` (`:843`, `:882`, `:919`, `:951`, `:983`). Apply the same correction to the § Verification "byte-identical rendered SQL clause" row.

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Requirements "Iceberg-spec compliance" row (line 135) and § Impact (lines 116-126)
- Issue: the Iceberg row packs six lines of prose into one table cell, restating the same determination three ways ("NOT implicated" / "touches no Iceberg file scanning …" / "already quoted in … Background and is unchanged by this plan"), and duplicates decision-log.md [11] almost verbatim. § Impact runs nine lines for two facts (commit 1 changes nothing; commit 2 changes one filter shape in the correctness-improving direction). Both violate the terseness and one-idea-per-paragraph rules.
- Fix: Cut the Iceberg row to two sentences — the determination and the reason (`filter_json_raw` unmodified, no Iceberg-boundary type mapping touched) — and cite decision-log.md [11] for the rest. Cut § Impact to at most five lines: one sentence for commit 1, three for commit 2's single behavior change and the no-breaking-change statement.

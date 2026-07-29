# Decision Log: refactor-pushdown-join-rendering-dedup

## Interview

**Q:** A 6th "join pushdown declined …" error site was found beyond issue #181's literal 5 (same template) — include it in the shared decline helper?
**A:** Include all 6 (recommended).

**Q:** This codebase already has golden characterization tests (`golden_broadcast_join_sql_unchanged`, `golden_n_scan_join_sql_unchanged`, `golden_grouped_qualified_fallback_sql_unchanged`, `golden_ineligible_decline_message_unchanged`) covering exactly the SQL paths this refactor touches — rely on these as the primary safety net, or author a fresh suite?
**A:** Reuse existing goldens (recommended) — run them unchanged after each dedup step; only add a new golden if a decline message's wording is intentionally changed (it must not be — wording stays byte-identical).

**Q:** Finding #6 (one-line pass-through wrappers + convoluted attach-point match — zero duplication reduction, pure readability) — keep in scope or split out?
**A:** Keep in this plan (recommended).

## Design Decisions

### [1] The two clause-walk routines share only their clause set, not a unified narrowing function

- **Decision:** Extract `referenced_clause_values(pushdown_req, visit)` in `joins/rendering.rs`, which owns only *which clauses can name a source column*. Each caller supplies its own per-node collector and keeps its own filter, case folding, short-circuit, and empty-result fallback.
- **Alternatives:** One unified function returning the narrowed column list for both `referenced_side_columns` and `referenced_column_projection`, as issue #181's "one `collect_referenced_column_names(...)` used by both #1 sites" reads at first glance. Rejected — the two routines differ in five ways, not one: the extra join-condition argument, per-table vs all-table attribution, the absent-`selectList` short-circuit (`full_cols` immediately, never inspecting another clause) vs always narrowing, the empty-result fallback (`full_cols` vs `all_cols.first()`), and the return type (`Vec<(String, String)>` vs `(Vec<ProjectionItem>, Vec<String>)`).
- **Rationale:** The decisive difference is case folding. `collect_all_column_names` folds with Unicode `to_uppercase`, `collect_side_column_names` with ASCII-only `to_ascii_uppercase`. Two sources — and only two — state that these MUST NOT be reconciled: the `walk_column_nodes` doc comment in `crates/lakehouse-engine/src/adapter/pushdown/support.rs` ("Case folding is deliberately NOT owned here … Those two MUST NOT be unified. They differ for non-ASCII identifiers — `ß` folds to `SS` under Unicode but stays `ß` under ASCII"), and `specs/vs-adapter/pushdown-module-structure/spec.md` — its §Background bullet "The two case-folding calls this codebase uses are NOT interchangeable" plus the case-folding *AND* of its "One blind traversal primitive backs every column-collecting walk" scenario ("each closure MUST keep its predecessor's case-folding call verbatim … so unifying them SHALL NOT happen under this scenario"). `specs/_decision/037-refactor-pushdown-collect-walk-dedup.md` is **not** a source for this constraint: its three ADRs cover the narrowed traversal, the wrapper-deletion precedent, and the separation from issue #257, and none mentions case folding. No test in the crate uses a non-ASCII identifier, so a merge would change behaviour while the entire suite still passed. Passing the collector in is what makes that impossible by construction.
- **Promotes to ADR:** yes

### [2] Divergence coverage is added before the clause walk is extracted

- **Decision:** Task 6 opens by pinning the three divergent narrowing policies and adding a non-ASCII test asserting the two collectors still fold differently, then extracts the shared walk.
- **Alternatives:** Extract first and rely on the existing suite. Rejected — the existing suite covers `referenced_side_columns`' short-circuit and `referenced_column_projection`'s narrowing, but pins neither the first-column fallback, nor the *absence* of a short-circuit on the projection path, nor the case-folding divergence.
- **Rationale:** These are precisely the invariants a plausible merge breaks, and precisely the ones nothing currently asserts. A refactor whose riskiest step is unverified is not a safe refactor, however small the diff.
- **Promotes to ADR:** no

### [3] `join_render_decline` covers six sites and excludes `ineligible_join_decline`

- **Decision:** One private `join_render_decline(clause: &str) -> UdfError` in `joins/sql_builders.rs` for the six qualified N-scan render declines. `ineligible_join_decline` in `joins/mod.rs` stays separate, with reciprocal doc-comment cross-references.
- **Alternatives:** Issue #181's literal five sites — rejected, the sixth (`build_n_scan_join_sql`'s no-column-metadata `return Err`) uses the identical template and was confirmed by inspection; the user approved including it. One constructor for all seven — rejected, the seventh inserts `the adapter cannot render this join shape, ` before the shared tail, making it a different sentence. Hoisting to `joins/mod.rs` — rejected, all six callers are in `sql_builders.rs`, so `pub(super)` would widen visibility for no caller.
- **Rationale:** The template is the duplicated decision; the clause noun is the parameter. A `&str` parameter rather than an enum: six single-use variants would be ceremony, and task 1.3's full-string test already catches a mistyped fragment.
- **Promotes to ADR:** no

### [4] A new test is added for the six decline messages despite the "reuse existing goldens" instruction

- **Decision:** Add `golden_n_scan_render_decline_messages_unchanged`, asserting all six messages by full-string equality, and require it to pass on unmodified HEAD before task 2 edits anything.
- **Alternatives:** Rely on the four existing goldens as instructed. Rejected on evidence: those four pin the generated SQL and `ineligible_join_decline`'s message. Verification found **no assertion anywhere on any of the six** — the only nearby check, a substring assertion in `sql_builders.rs`, targets the seventh template. Task 2 rewrites all six with zero coverage.
- **Rationale:** This is exactly the interview's stated exception — a new test where the existing goldens do not cover one of the six error sites. The byte-identity risk is concrete: each message is a `\`-continued literal whose newline-plus-indentation collapses to a single space, the classic place a re-write drifts by one character.
- **Promotes to ADR:** no

### [5] The two pass-through wrappers are deleted, but their design intent is relocated, not discarded

- **Decision:** Delete `render_join_condition` and `render_selectlist_item_qualified`; migrate all production and test call sites to `render_expression_safe` / `render_expression_qualified`; move each wrapper's doc-comment rationale onto the surviving delegate or the call site.
- **Alternatives:** Keep them as wrappers because their doc comments carry real design intent (`render_selectlist_item_qualified`'s records byte-compatibility with the removed `render_aggregate_qualified`). Rejected — the knowledge is worth keeping, the indirection is not, and the two are separable.
- **Rationale:** `specs/_decision/037` "Fold by deleting the wrapper, not by leaving a pass-through" is the established precedent, and a body that is one call with the same arguments is the canonical pass-through red flag. Verification confirmed neither name is in the `joins` façade baseline (nine `pub(crate)` + five `pub(super)` items), so deleting them cannot narrow the façade — the open question the planning brief flagged is settled by the spec text itself.
- **Promotes to ADR:** no

### [6] The attach-point rewrite uses a let-chain rather than the issue's `unwrap()` form

- **Decision:** `if resolvable && last_join_point >= 1 && let Some(m) = tables.iter().map(|t| leg_index[t]).max() { … } else { residual.push(…) }`, with the existing `clamp(1, 0)` guard comment carried over verbatim.
- **Alternatives:** Issue #181's suggested `if resolvable && last_join_point >= 1 { …max().unwrap() }`. Rejected — behaviourally identical only because `resolvable` implies `tables` is non-empty, an invariant nothing states; a let-chain gets the same short-circuit without resting readability on it.
- **Rationale:** The point of finding 6 is readability. Replacing an opaque `.then().flatten()` chain with an `unwrap()` that needs a paragraph to justify trades one puzzle for another.
- **Promotes to ADR:** no

### [7] Findings 4 and 5 get Background bullets, not scenarios

- **Decision:** `column_tables` and `shard_side` are recorded as spec Background and covered by the existing byte-identical golden-SQL scenario. Only finding 1, finding 3, and finding 6 get scenarios.
- **Alternatives:** A structural scenario per finding. Rejected — "no copy-paste remains" is not externally verifiable, and a scenario whose only proof is reading the code is not a scenario.
- **Rationale:** Findings 4 and 5 have no observable surface beyond the SQL the goldens already pin byte-for-byte. Findings 1, 3, and 6 each carry something the existing suite does not assert: hidden divergences, six uncovered messages, and a deletion that must be distinguishable from a façade narrowing.
- **Promotes to ADR:** no

### [8] The fan-out builders' shared `build_scan_driving_sql` argument tail is left duplicated

- **Decision:** `shard_side` covers only the sharding prefix. The identical trailing `None, None, &[], &[]` arguments in both `build_scan_driving_sql` calls stay as they are.
- **Alternatives:** A second helper wrapping the call. Rejected — six of the ten arguments genuinely differ between the two call sites.
- **Rationale:** A six-parameter wrapper that exists to elide four literal empties is a shallow layer. The duplication is visible and harmless; the abstraction would not be.
- **Promotes to ADR:** no

### [9] No wider `walk_json` primitive is introduced

- **Decision:** Build on `walk_column_nodes` as it stands.
- **Alternatives:** Issue #181's stated dependency on "`walk_json` from the generic-helpers issue". Already resolved against: issue #177 shipped the narrower `walk_column_nodes` in PR #261, and `specs/_decision/037` records the narrowing as deliberate specifically so a later reader does not restore the wider signature as a cleanup.
- **Rationale:** The prerequisite issue #181 names is satisfied in substance. Reading its literal wording as still-open work would reverse an accepted ADR.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] `vs-adapter/pushdown-module-structure` pins the signature task 3 changes (BLOCKER 1)

- **Finding:** `plan-reviewer` round 1 flagged a requirement conflict. `specs/vs-adapter/pushdown-module-structure/spec.md:138` pins `collect_column_tables`' `pub(super)` visibility **and its three accumulator out-parameters**, and asserts that `conjunct_single_side`, `referenced_side_columns`, and the N-scan side-attribution caller "compile unedited". Task 3 removes exactly those out-parameters and tasks 3.2, 4.1 and 6.5 edit all three of those callers. The plan cited that spec four times as a constraint source but never listed it in § Features, so `/speq:record` would have merged this plan leaving the library asserting a MUST about a deleted signature.
- **Direction change:** `vs-adapter/pushdown-module-structure | CHANGED` added to plan.md § Features with a note stating why, and `vs-adapter/pushdown-module-structure/spec.md` authored as a `DELTA:CHANGED` restating the "One blind traversal primitive backs every column-collecting walk" scenario. Two clauses change: the out-parameter clause becomes the `column_tables(expr: &Json) -> (HashSet<String>, bool, bool)` return form, and the "compile unedited" guarantee is re-scoped to `collect_side_column_names` alone, naming the three callers this plan deliberately edits. The delta's Background states explicitly that the case-folding clause and the §Background bullet on the two non-interchangeable folds are UNCHANGED and still binding, and that issue #181 preserves that divergence rather than relaxing it. Verification gains a Scenario Coverage row and a `dispatch_golden` manual-testing row for the amended scenario. No design change: the tuple return form advisory 8 disputes is retained as planned.
- **Promotes to ADR:** no

### [plan-review] `vs-adapter/pushdown-planning-selectlist-expressions` names the wrapper task 4.3 deletes (BLOCKER 2)

- **Finding:** `specs/vs-adapter/pushdown-planning-selectlist-expressions/spec.md:104` names `render_selectlist_item_qualified` inside the normative dialect-chain *AND* of its widened-projection scenario ("`render_expression_exasol_safe`, reached by `render_selectlist_item_qualified` → `render_expression_qualified`"). Task 4.3 deletes that function, after which the recorded chain describes something that does not exist. The feature was never assessed; § Impact asserted a blanket "None".
- **Direction change:** `vs-adapter/pushdown-planning-selectlist-expressions | CHANGED` added to plan.md § Features, and a `DELTA:CHANGED` authored restating that scenario with the parenthetical rewritten to `(render_expression_exasol_safe, reached by render_expression_qualified)`. Its Background records that the removed hop was a one-line pass-through and that no dialect behaviour changes — same two entry points, same widening, same hard-error set — and points at the joins delta for the relocated doc-comment intent. Task 4.4's grep gate widened from `crates/` to `crates/ specs/` so a stale spec reference fails the gate too, mirrored into the § Manual Testing row. The gate's pass condition is stated as zero hits under `crates/` and every `specs/<domain>/` recorded spec, with hits permitted only in the immutable archived `specs/_decision/001-migrate-legacy-decision-log.md` and in this plan's own `specs/_plans/` files, which name both wrappers precisely to record their deletion — an unqualified "returns nothing" would have been unsatisfiable against those two, which is the same defect class as BLOCKER 4.
- **Promotes to ADR:** no

### [plan-review] Task 4.4's grep gate was unsatisfiable at the moment it runs (round 2 BLOCKER)

- **Finding:** `plan-reviewer` round 2 flagged that the BLOCKER-2 fix left one arm of the gate unsatisfiable — the same defect class it had just repaired twice. Task 4.4 declared "A hit in `specs/vs-adapter/pushdown-planning-selectlist-expressions/spec.md` means that feature's delta … was not authored or not merged, and fails the gate", but that file is a recorded feature spec that provably holds one `render_selectlist_item_qualified` hit at HEAD, and the delta removing it is merged by `/speq:record` — which runs *after* `/speq:implement` verifies the work the gate belongs to. At gate time the hit exists by construction and the implementer has no passing move: merging is `recorder-agent`'s operation, and hand-editing the recorded spec both exceeds implementation scope and duplicates the delta the recorder replaces by scenario name.
- **Direction change:** task 4.4's pass condition split by tree. Zero hits stay required under `crates/`. Under `specs/`, the select-list recorded spec is named as an expected exception until `/speq:record` merges the delta, and the gate substitutes a check the implementer *can* satisfy — that `specs/_plans/refactor-pushdown-join-rendering-dedup/vs-adapter/pushdown-planning-selectlist-expressions/spec.md` exists and contains `` reached by `render_expression_qualified` `` (verified present at delta line 31). The two already-sound exceptions (`specs/_decision/001-migrate-legacy-decision-log.md` and this plan's own directory, `review/` included) are retained. The condition is mirrored verbatim into § Manual Testing row 2's Expected Output, and § Verification → Scenario Coverage row 8 now claims "the authored delta removes the hop from the recorded dialect chain when `/speq:record` merges it" rather than the pre-merge-false "no recorded spec names the deleted hop". No task, scenario, delta, or design decision changed — only what the gate asserts.
- **Promotes to ADR:** no

### [plan-review] The case-folding prohibition was falsely attributed to `specs/_decision/037` (BLOCKER 3)

- **Finding:** plan.md § Context, § Consequences, task 1.1, and decision-log [1] all attributed the case-folding prohibition to `specs/_decision/037-refactor-pushdown-collect-walk-dedup.md`. That file contains three ADRs — the narrowed `walk_column_nodes` traversal, the wrapper-deletion precedent, and the separation from issue #257's rewrite primitive — and does not mention case folding at all. Decision [1] is flagged for ADR promotion, so `/speq:record` would have written the false provenance permanently into the library, and a later planner checking the cited authority would find no constraint and reconcile the two folds.
- **Direction change:** every case-folding attribution to `specs/_decision/037` replaced with the two real sources — the `walk_column_nodes` doc comment in `crates/lakehouse-engine/src/adapter/pushdown/support.rs` ("Case folding is deliberately NOT owned here … Those two MUST NOT be unified", with the `ß` → `SS` example), and `specs/vs-adapter/pushdown-module-structure/spec.md`'s §Background bullet "The two case-folding calls this codebase uses are NOT interchangeable" plus the case-folding *AND* of its "One blind traversal primitive" scenario. Both quotations verified against the files at HEAD. Each corrected site now states positively that `037` is silent on case folding, so the citation cannot be restored as a cleanup. Task 1.1 splits the two constraints by source: `037` for the wrapper-deletion precedent only, `support.rs`' doc comment and the `pushdown-module-structure` scenario for case folding. `037`'s remaining citations in § Non-Goals, § Patterns row 4, § Dependencies and decisions [5] and [9] are genuine wrapper-deletion and `walk_json` attributions and were left untouched.
- **Promotes to ADR:** yes

### [plan-review] The "exactly one owner" guarantee was unsatisfiable for `selectList` (BLOCKER 4)

- **Finding:** the clause-walk scenario promised "the clause set MUST have exactly one owner, so adding or removing a clause SHALL require editing one function rather than two", while task 6.5 in the same plan requires `referenced_side_columns` to keep its absent/empty-`selectList` early return *before* the walk. `selectList` therefore stays named in two functions after the refactor, so for one of the five clauses the requirement is false and cannot be verified pass/fail.
- **Direction change:** the first *THEN* narrowed to `filter`, `groupBy`, `orderBy`, and `having`, and a new *AND* records the retained exception: `referenced_side_columns` MUST keep naming `selectList` a second time in its short-circuit guard, because that guard is a fallback policy the walk deliberately does not own. The exception is stated as retained by design rather than as an incomplete reduction, so a later reader does not "finish" the reduction by folding the guard into the walk — which would silently give the projection path the short-circuit the same scenario forbids it. Already covered by the existing `referenced_side_columns_keeps_all_when_select_list_absent` test in the Verification table.
- **Promotes to ADR:** no

### [plan-review] The `collect_column_tables` → `column_tables` rename left three stale references (round 2 ADVISORY 1)

- **Finding:** `plan-reviewer` round 2 flagged a completeness gap the round-1 revision did not sweep. Task 3 renames `collect_column_tables` to `column_tables`, but three sites keep the old name: `walk_column_nodes`' doc comment in `crates/lakehouse-engine/src/adapter/pushdown/support.rs:1304` ("`collect_column_tables` and `collect_side_column_names` in `joins/rendering.rs` fold with `to_ascii_uppercase`. Those two MUST NOT be unified") — which BLOCKER 3's fix had just promoted to load-bearing authority for the case-folding prohibition, and which no task covered because task 4.4's grep gate matches only the two deleted wrapper names; the `pushdown-module-structure` delta's *GIVEN*, which kept the old name while its changed *AND* pins the new one; and the joins delta's second Background bullet, which described the change using only the old name.
- **Direction change:** task 3.1 gained the doc-comment sweep — update `walk_column_nodes`' doc comment to name `column_tables`, leaving the rest of the case-folding paragraph verbatim, so the authority BLOCKER 3 relies on keeps naming a function that exists. The `pushdown-module-structure` delta's *GIVEN* now reads "`column_tables` (`collect_column_tables` before this delta)", which names the post-merge item while keeping the pre-delta name greppable. The joins delta's Background bullet now reads "`collect_column_tables` returning its three outputs as `column_tables` instead of writing three `&mut` out-params". The advisory's optional follow-on — extending task 4.4's grep to `collect_column_tables` — was NOT adopted: it would need the same record-time exception the round-2 BLOCKER fix established for the select-list spec, for a rename the compiler already catches.
- **Promotes to ADR:** no

### [plan-review] Task 6.3's doc-comment instruction still claimed unqualified sole ownership (round 2 ADVISORY 2)

- **Finding:** `plan-reviewer` round 2 flagged that BLOCKER 4's fix landed in the spec but not in the code artifact a future refactorer reads. Task 6.3 instructed only "Doc-comment it as the single owner of 'which clauses of a pushdown request can name a source column'" — the unqualified claim the joins delta's new *AND* had just retracted for `selectList`. The delta states the exception is retained by design "so a later reader does not 'finish' the reduction", but that reader is reading `referenced_clause_values`' doc comment, which as instructed asserted sole ownership and never mentioned the guard. Folding the guard in then reads as the natural cleanup, and it hands the projection path the short-circuit the same scenario forbids — the exact failure the BLOCKER-4 fix was written to prevent.
- **Direction change:** task 6.3's doc-comment instruction extended to require stating that `referenced_side_columns` deliberately keeps its own absent/empty-`selectList` short-circuit before this walk, so `selectList` is named twice by design, and that the guard MUST NOT be folded in because doing so would give `referenced_column_projection` a short-circuit `vs-adapter/pushdown-joins-module-structure`'s "One clause walk feeds both wrapper column-narrowing routines" scenario forbids it. Spec text unchanged — the exception was already recorded there; only the code artifact's instruction now carries it.
- **Promotes to ADR:** no

### [plan-review] The `pushdown-module-structure` delta narrowed a restated clause by paraphrase (round 2 ADVISORY 3)

- **Finding:** `plan-reviewer` round 2 flagged an ambiguous requirement. The delta's third Background bullet claimed "so no `use` path widens and this feature's 'narrowest visibility that compiles' rule holds unedited", while the recorded clause it restates verbatim (`specs/vs-adapter/pushdown-module-structure/spec.md:135`, delta line 23) reads "so NO item's visibility widens and no join-module `use` path **changes**". Tasks 3.2, 4.4, and 6.4 each change `sql_builders.rs`' `use super::rendering::{…}` list. The fair reading scopes the recorded consequence to declaring the primitive in `support`, so this is not a conflict — but the delta resolved it by paraphrase instead of saying so, leaving a later auditor who greps the recorded wording with an apparent violation and no recorded answer.
- **Direction change:** that Background bullet now states the scoping explicitly: "so no item's visibility widens. The restated clause's 'no join-module `use` path changes' consequence scopes to declaring the primitive in `support`; issue #181 does edit the `use super::rendering::{…}` list inside `joins` (tasks 3.2, 4.4, 6.4), which widens no path and adds no cross-module reach." The recorded clause is still restated verbatim in the *AND* — the narrowing is now on the record rather than inferable from a paraphrase. No task, scenario step, or design decision changed.
- **Promotes to ADR:** no

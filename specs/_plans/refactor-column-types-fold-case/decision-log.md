# Decision Log: refactor-column-types-fold-case

## Interview

**Q:** After deleting the CONSTRUCTED-literal test that pins the two-fold divergence (`each_builder_keeps_its_own_case_fold_on_a_constructed_non_ascii_literal` in `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs`), should a new test assert the unified single-fold behavior, or is existing pushdown/join test coverage enough?
**A:** No new test. Existing pushdown/join tests already exercise `column_types` end-to-end with real (already-uppercased) column names; a dedicated unified-fold test would just restate `str::to_uppercase`'s stdlib behavior with no observable divergence left to guard.

## Design Decisions

### [1] Unicode `to_uppercase` is the surviving fold

- **Decision:** `column_types` folds with `str::to_uppercase` in its own body; `str::to_ascii_uppercase` disappears from both wrappers.
- **Alternatives:** Keep the ASCII fold, which is cheaper and is what the join side has always applied. Rejected.
- **Rationale:** `to_uppercase` is the fold `resolve_table_schema` already applies to every Iceberg field name, and the fold the consuming `column_exa_type` lookup applies. Choosing it makes the join side converge on the adapter's single existing normalization; choosing ASCII would make the adapter's normalization the odd one out and leave the guards' lookup fold unpaired.
- **Promotes to ADR:** no

### [2] Delete the divergence test, add no replacement

- **Decision:** `each_builder_keeps_its_own_case_fold_on_a_constructed_non_ascii_literal` is deleted with its section banner and its `#[cfg(test)]` import. No unified-fold test replaces it.
- **Alternatives:** Rewrite it as an agreement assertion over the two wrappers.
- **Rationale:** The test asserts the two wrappers disagree. After the change that statement is false, not weakened, so correcting it is not an option — only deleting or inverting it. An inverted agreement assertion over two wrappers of one builder restates `str::to_uppercase` and would pass under any fold, which is the same reasoning the superseded scenario used to reject an agreement test over already-uppercased names.
- **Promotes to ADR:** no

### [3] Five stale comments are in scope, not just the parameter

- **Decision:** Reword or delete every comment asserting the removed divergence. FIVE carriers, not three: (1) `column_types`' two-parameters-by-design paragraph plus its `(#270)` note; (2) `involved_table_columns`' "ASCII-only `to_ascii_uppercase`" doc claim; (3) `involved_table_columns`' closing partial-application paragraph, which names "the ASCII-only fold this side has always applied" (`joins/planning.rs:359-360`); (4) `column_exa_type`'s doc sentence claiming "`involved_table_columns`' ASCII-folded keys agree for every column name the adapter can declare" (`support.rs:671-673`); and (5) the sentence in `column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list`'s doc comment naming `involved_table_columns` as the ASCII-folded list's producer.
- **Alternatives:** Delete only the parameter and its immediate justification, leaving the other comments for a later pass.
- **Rationale:** Every one of the five would assert, after the change, that a builder produces an ASCII-folded list when none does — exactly the stale-citation defect the parent feature's delta exists to delete. Carriers (3) and (4) are the ones an incomplete enumeration loses: (3) sits two lines below carrier (2) in the same doc comment, so an instruction to "keep the find-by-name selection rationale intact" reads as preserving it; (4) is the NEWEST carrier, added by the parent plan's own review fix, and lives on a function this change does not otherwise edit.
- **Promotes to ADR:** no

### [4] The altered fold pairing with `collect_side_column_names` is named, not silently changed

- **Decision:** Record in the spec that `referenced_side_columns` filters `involved_table_columns`' names against the ASCII-folded set `collect_side_column_names` builds, so a pair that folded identically now folds differently, and state both why they still agree and what the failure mode would be.
- **Alternatives:** Treat the change as purely local to `column_types` and say nothing, since the divergence is unreachable either way.
- **Rationale:** This is the one place the change creates a NEW cross-fold pairing rather than removing one, so an unnamed divergence here would be indistinguishable from a regression later. The pairing's agreement rests SOLELY on the upstream normalization the rest of the scenario rests on — there is no second safety net. `referenced_side_columns` (`joins/rendering.rs:316-325`) falls back to `full_cols` only when narrowing yields NOTHING, so the failure mode if that premise broke is a DROPPED column, not a wider projection: a MIXED side whose `full_cols` is `[STRASSE, ID]` measured against an ASCII-folded reference set `{STRAßE, ID}` narrows to the non-empty `[ID]`, the fallback does not fire, and the fan-out leg loses `STRASSE` while the outer wrapper still references it. A wider projection is the ALL-MISS case only, and the spec records that bound rather than the "never a dropped column" one.
- **Promotes to ADR:** no

### [5] The selection closure parameter stays as-is

- **Decision:** `column_types` keeps `select_table: impl FnOnce(&[Json]) -> Option<&Json>`. Both wrappers survive with unchanged signatures and declaration sites.
- **Alternatives:** Reshape the selection into `Option<&str>` (None = first table) now that it is the only parameter left; or inline the builder and delete a wrapper.
- **Rationale:** Issue #270 scopes this change to `fold_case`. Each wrapper still supplies a selection the builder does not choose, so neither is a pass-through, and reshaping the surviving parameter would edit both call sites for no observable gain.
- **Promotes to ADR:** no

### [6] The recorded information-leakage refusal to unify is overridden, not left standing

Supersedes the recorded ADR with **ID `col-types-fold-divergence-unreachable-design-preserved`**, titled **"The fold divergence is unreachable, and preserved for a design reason rather than a behavioral one"** (`specs/_decision/042-refactor-col-types-guard-dedup.md:131-168`).

- **Decision:** This plan unifies the folds, which the recorded feature explicitly refused on information-leakage grounds. The refusal is quoted, answered, and superseded in the spec delta rather than left as the library's current ruling.
- **The objection, verbatim** (recorded Background bullet, `specs/vs-adapter/pushdown-col-types-consolidation/spec.md:16`; the same rejection is recorded in `specs/_decision/042-refactor-col-types-guard-dedup.md:158-159`): unifying is refused because "making either builder's fold depend on `resolve_table_schema`'s uppercasing would put one module's decision inside another module's body, which is the information leakage this plan exists to remove", and the options table rejects "Unify the folds now that no reachable input distinguishes them" as "still a behavior change outside a pure refactor's scope, and encodes a dependency on another module's decision".
- **Alternatives:** Unify silently and leave the recorded refusal standing; or honor the refusal and close #270 as won't-do.
- **Rationale:** Removing `fold_case` is settled by the user's request. The live question is not WHETHER to unify, but whether the opposite ruling may stay in the library. It may not, and it no longer decides, for two reasons.

  FIRST, the surviving fold is selected to match the list's CONSUMER, `support::column_exa_type`. That consumer lives in the same module as `column_types`, so the builder encodes its OWN module's decision. The leakage the objection names would arise only on the ASCII alternative, which leaves the in-module consumer's fold unpaired.

  SECOND, `resolve_table_schema`'s uppercasing is a BEHAVIOR-PRESERVATION premise, not a rule that selects the fold. `tests/e2e_non_ascii_identifier_test.rs::non_ascii_table_and_column_stay_queryable` guards that premise. Drop it and the fold is still `to_uppercase`; only the byte-identity claim weakens.

  The pure-refactor half of the objection is discharged separately. The two folds are provably equal over every reachable input, so the diff is byte-identical — what the recorded ruling doubted rather than what it forbade. The unreachable-input-domain STANDARD that bullet invoked, that an unnamed divergence is indistinguishable from a regression, is kept. It applies to the one new cross-fold pairing this change creates (see [4]).
- **Promotes to ADR:** yes

### [7] `pushdown-planning-like-type-coercion`'s stale fold bullet is deferred, not silently left

- **Decision:** No delta for `vs-adapter/pushdown-planning-like-type-coercion`. Its Background bullet 44 — "The Unicode-versus-ASCII fold divergence between the two `col_types` builders … `vs-adapter/pushdown-module-structure` records the divergence, the live capture showing it unreachable, and the issue tracking removal of the `fold_case` parameter that preserves it" — is left for a separate cleanup, recorded here rather than left unremarked.
- **Alternatives:** Add a one-bullet `DELTA:CHANGED` delta restating it as "both `col_types` builders now fold with the Unicode `to_uppercase`".
- **Rationale:** Two of that bullet's three claims were ALREADY stale before this change — issue #265 moved the divergence, the capture, and the tracking issue out of `vs-adapter/pushdown-module-structure` into `vs-adapter/pushdown-col-types-consolidation`, so the cross-reference points at a feature that no longer holds the content. Fixing it properly means retargeting the reference AND restating the divergence, which is a `vs-adapter/pushdown-planning-*` cross-reference audit rather than a consequence of removing `fold_case`. A delta here would also have to carry that feature's unresolvable-column scenario verbatim to satisfy `speq plan validate`, when no clause of it changes — restating a scenario this change does not touch. The behavior this feature specifies is byte-identical either way: it reads `involvedTables[0].columns` through `extract_all_column_types`, the surviving Unicode path, exactly as before. One criterion separates this deferral from the `datafusion-scan/type-mapping-module-structure` cross-reference retarget, which this plan DOES fix: that feature already has a delta file here, opened for the pass-through example, so correcting its stale owner reference costs one more bullet in a file already being edited. `vs-adapter/pushdown-planning-like-type-coercion` has no delta file, and would need a brand-new one for a reference this change does not otherwise touch.
- **Promotes to ADR:** no

### [8] The builder's two-parameter shape is superseded by the one-parameter shape

Supersedes the recorded ADR with **ID `column-types-builder-separate-selection-and-fold-params`**, titled **"The merged builder takes table selection and case fold as two separate parameters"** (`specs/_decision/042-refactor-col-types-guard-dedup.md:33-56`).

- **Decision:** `column_types` takes `(request, select_table)`. The `fold_case` parameter is gone, and the builder applies `str::to_uppercase` in its own body.
- **Alternatives:** Keep `fold_case` and pass `str::to_uppercase` from both wrappers, unifying the fold value without removing the parameter. Rejected — a parameter with one reachable argument is the dead flexibility #270 exists to delete.
- **Rationale:** The recorded ADR's options table rejected "Unify the fold for both callers", and that rejection is discharged. The exhaustive fixed-point sweep over all 1,112,064 Unicode scalar values found zero input where the two folds differ on `to_uppercase` output. `resolve_table_schema` Unicode-uppercases every declared name upstream, so no other input reaches either wrapper. Unification is therefore byte-identical, not the behavior change the options table assumed. Which fold survives is decided in § [1]; why the recorded refusal no longer rules is decided in § [6].
- **Promotes to ADR:** yes

## Review Findings

### [plan-review] Failure-mode bound of the new cross-fold pairing was false

- **Finding:** Feasibility `[UNSTATED_ASSUMPTION]` BLOCKER F1. All three artifacts stated that a `involved_table_columns` × `collect_side_column_names` fold disagreement yields "a wider projection — never a dropped column or a wrong result", citing `referenced_side_columns`' empty-narrowing fallback. That fallback fires only when narrowing yields NOTHING (`joins/rendering.rs:316-325`), so a MIXED side drops the diverging column instead.
- **Direction change:** The bound is restated correctly in the delta's DELTA:NEW Background bullet 1, in § Scenario clause 33 (now with an explicit MUST NOT against recording the old bound), and in `decision-log.md` § [4]: the mixed case DROPS the diverging column, the all-miss case widens the projection, and the upstream `resolve_table_schema` normalization is named as the SOLE reason the pairing is safe — there is no second safety net.
- **Promotes to ADR:** no

### [plan-review] The DELTA:CHANGED block superseded no recorded bullet

- **Finding:** Requirement Quality `[COMPLETENESS_GAP]` BLOCKER R1. Two CHANGED bullets were supplied against a recorded Background of 20, naming none of the bullets they replaced, leaving recorded bullets 13, 16, and 20 asserting two folds, a preserved-by-design divergence, and a per-wrapper case fold — and giving `recorder-agent` no way to match either CHANGED bullet to its target.
- **Direction change:** All five CHANGED bullets now open with this feature's own `This bullet SUPERSEDES the preceding Background bullet "<quoted text>"` convention. The two existing bullets name recorded bullets 10 and 17; three new bullets supersede 13 (restating agreement without "the ASCII-folding join-side builder"), 16 (dropping the preserved-for-a-design-reason and follow-up-issue sentences, carrying [6]'s reasoning instead), and 20 (resting the partial-application exception on the table selection alone).
- **Promotes to ADR:** no

### [plan-review] A second feature's recorded Background contradicted the change with no delta

- **Finding:** Requirement Quality `[REQUIREMENT_CONFLICT]` BLOCKER R2. `specs/datafusion-scan/type-mapping-module-structure/spec.md:14` — the recorded carrier of the pass-through-deletion EXCEPTION under which both wrappers survive — states that both wrappers "each supply their own table selection and case fold to `support::column_types`". After this change neither supplies a case fold, and plan.md listed one CHANGED feature.
- **Direction change:** Added `specs/_plans/refactor-column-types-fold-case/datafusion-scan/type-mapping-module-structure/spec.md` with a DELTA:CHANGED Background bullet superseding that closing sentence — the exception's RULE unchanged, only its example, now "each supply their own table selection" — and added the feature as a second CHANGED row in plan.md § Features.
- **Promotes to ADR:** no

### [plan-review] The comment-carrier enumeration claimed exhaustiveness and was wrong

- **Finding:** Requirement Quality `[COMPLETENESS_GAP]` BLOCKER R4. § Scenario clause 32 asserted "THREE carriers exist and all three are in scope"; five sentences in the code assert the removed divergence. The two unlisted ones were `column_exa_type`'s doc sentence (`support.rs:671-673`, the newest carrier, on a function no task touched) and `involved_table_columns`' closing partial-application paragraph (`joins/planning.rs:359-360`), which the clause's own "keep its find-by-name selection rationale intact" instruction would have preserved verbatim.
- **Direction change:** Clause 32 now enumerates FIVE numbered carriers with the quoted text and the required reword for each. `column_exa_type`'s reword and the closing-paragraph edit are both in plan.md § Implementation Tasks and § Dead Code Removal, and `decision-log.md` § [3] is retitled and rewritten to the corrected count.
- **Promotes to ADR:** no

### [plan-review] Tasks 1-3 were not a parallel group and task 4 was not independent

- **Finding:** Task Breakdown `[TASK_GRANULARITY]` BLOCKER. § Parallelization listed "Group A | Tasks 1, 2, 3" while the prose below said the crate does not compile between them, so they could be neither parallelized nor independently sequenced; and Group B (task 4) was declared independent while editing the same ~6,100-line `support.rs` that task 1 edits.
- **Direction change:** Tasks 1-3 are merged into ONE task carrying each former task as a bullet, and the remainder is renumbered to 2 (both `support.rs` doc-comment rewords) and 3 (verification gate). § Parallelization is now a single sequential chain `Task 1 → Task 2 → Task 3` with no parallel group, and the independence claim is replaced by the rule that every task before the gate edits `support.rs` and so must run one at a time.
- **Promotes to ADR:** no

### [plan-review] The second feature's delta named both the old and the new merge owner

- **Finding:** Requirement Quality `[REQUIREMENT_CONFLICT]` BLOCKER B1 (round 2). The R2 fix retargeted the relocation scenario's wrapper clause at `vs-adapter/pushdown-col-types-consolidation` but left recorded Background bullet 21 of the same feature (`specs/datafusion-scan/type-mapping-module-structure/spec.md:21`) closing with "The merge itself is specified by `vs-adapter/pushdown-module-structure`, which owns the two functions' file set". The merged feature would assert both owners at once — worse for the next reader than the consistently stale state before the delta.
- **Direction change:** Added a THIRD `DELTA:CHANGED` Background bullet to the delta, superseding recorded bullet 21: the scope-fence reasoning and the fence-lifted statement stay verbatim, and only the closing sentence changes to name `vs-adapter/pushdown-col-types-consolidation` as the merge's specifier and the two functions' contract owner. It also states explicitly that recorded bullet 19's guard-rewiring attribution to `vs-adapter/pushdown-module-structure` is NOT affected. Delta Background bullet 2's count is corrected from "exactly ONE scenario clause besides the bullet above" to name both superseded Background bullets. `decision-log.md` § [7] gains the criterion separating this retarget from the `vs-adapter/pushdown-planning-like-type-coercion` deferral: this feature's delta file already exists for the pass-through example, that feature's does not.
- **Promotes to ADR:** no

### [plan-review] Two Accepted ADRs were falsified with nothing marked to supersede them

- **Finding:** Design Depth `[REQUIREMENT_CONFLICT]` BLOCKER B2 (round 2), the unresolved half of round-1 D1. § [6]'s ADR-promotion field read `no`, so `/speq:spec-merge` would emit no `**Supersedes:**` pointer and both falsified ADRs in `specs/_decision/042-refactor-col-types-guard-dedup.md` would stay `Status: Accepted` — `col-types-fold-divergence-unreachable-design-preserved` (line 133), whose options table rejects "Unify the folds now that no reachable input distinguishes them", and `column-types-builder-separate-selection-and-fold-params` (line 35), whose Decision is the three-parameter signature this plan deletes. § [6] also named its target by Decision text rather than by ID slug, which the recorder cannot resolve.
- **Direction change:** § [6]'s ADR-promotion field is now `yes`, and it names ID slug `col-types-fold-divergence-unreachable-design-preserved` with its title and line range. New § [8] covers the second ADR, promoted likewise, naming ID slug `column-types-builder-separate-selection-and-fold-params`; its Decision states the `(request, select_table)` shape and its Rationale discharges the "Unify the fold for both callers" rejection via the Unicode fixed-point sweep plus `resolve_table_schema`'s upstream normalization, cross-referencing § [1] and § [6] instead of restating them.
- **Promotes to ADR:** no

### [plan-review] The recorded refusal to unify was operationalized but never answered

- **Finding:** Design Depth `[INFORMATION_LEAKAGE]` BLOCKER D1. The recorded feature rejected exactly this unification on information-leakage grounds (recorded bullet 16; `specs/_decision/042-refactor-col-types-guard-dedup.md:158`), and the plan performed it while leaving that ruling standing, waiving the design diagnostic outright at `plan.md:38` and arguing only `to_uppercase` versus `to_ascii_uppercase` in § [1].
- **Direction change:** Added `decision-log.md` § [6], which quotes the objection verbatim, names the superseded recorded decision, and states why it no longer decides — the surviving fold matches the in-module consumer `column_exa_type` rather than a foreign module's convention, and the `resolve_table_schema` dependency is a behavior-preservation premise guarded by the E2E test rather than a fold-selection rule. Recorded bullet 16 is superseded in the delta with a bullet carrying that reasoning, and `plan.md`'s diagnostic-waiver sentence is replaced by the one applicable `/speq:design-philosophy` diagnostic row and its answer.
- **Promotes to ADR:** no

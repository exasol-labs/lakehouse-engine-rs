# Decision Log: refactor-pushdown-expr-rewrite-primitive

## Interview

**Q:** Issue #257 sequences this as two commits: (1) pure refactor extracting the primitive,
(2) migrating `like_subject_type_guard` onto it, which is a behavior change closing the #207
`function_scalar_case` blind spot. Should this plan cover both?
**A:** Both commits. Plan the primitive extraction AND the LIKE-guard migration with its regression
test, as #257 sequences. The behavior change is a strict improvement — a LIKE over a non-string
column nested in a CASE currently hard-fails the DataFusion scan; after the migration it declines
and Exasol evaluates natively.

**Q:** Widening the LIKE guard's traversal means it now reaches LIKE nodes under comparison
operands, `arguments`, and CASE `basis`/`results`. In those newly-reached positions a non-string or
unresolvable subject column will DECLINE the whole filter (Exasol evaluates natively) where it
previously pushed down and hard-failed at scan. Confirm that trade?
**A:** Accept — correctness first. A declined filter is always correct (Exasol native eval); the
pre-change behavior in those positions was a hard scan failure, never a working pushdown. Matches
the all-or-nothing untranslatable-predicate backstop already documented in the like-type-coercion
spec.

**Q:** The LIKE guard is wired ONLY into the single-table filter chain (`mod.rs:211`) — not the
select-list/projection path (tracked as #219) and not the broadcast-join per-leg filter path (#215).
Does this refactor extend it to those surfaces?
**A:** No — keep filter-only. Wiring new surfaces is a separate behavior change with its own
fallback semantics (a select-list decline must set the widen-projection flag, not drop a filter).
#215/#219 stay tracked exceptions; this plan touches traversal shape only.

## Design Decisions

### [1] One free function plus a per-node closure, not a visitor trait or typed AST

- **Decision:** `fn rewrite_expr_tree(node: &Json, f: &impl Fn(&Json) -> Option<Json>) -> Option<Json>` (private — see [4]), plus two module-level consts holding the curated child-field lists. Each guard supplies its step-2 logic as the closure and owns no traversal code.
- **Alternatives:** A `Visitor` trait with a method per node type (rejected — adds a type surface per node kind and buys nothing over an untyped IR); a typed expression AST parsed from the JSON (rejected — contradicts `vs-expression`'s stated no-SQL-parser property and would need a second grammar owner); a pass-ordering pipeline abstraction over the three guards (rejected — out of scope per #257; the one production chain site keeps its explicit composition and its load-bearing order comment).
- **Rationale:** The pushdown IR is deliberately untyped `serde_json`. A free function plus closure is the honest size for the duplication actually observed (one traversal, three per-node decisions).
- **Promotes to ADR:** yes

### [2] The primitive applies the per-node function to leaves too

- **Decision:** Drop each guard's `!node.is_object()` early return; the primitive recurses (no-op on a non-object, because `Value::get(&str)` yields `None` for every non-object) and then applies `f` to the node, leaves included.
- **Alternatives:** Keep an `is_object` early return inside the primitive that skips `f` for non-objects (rejected — an extra branch preserving a distinction that provably has no observable effect).
- **Rationale:** Proven equivalence, not an assumption: each guard's step 2 is already a no-op on a non-object node. `get("type")` yields `None`, so the decimal walker falls to its `_ => out` arm and the string walker's `!= Some("function_scalar")` check returns `Some(out)` unchanged. `string_fn_guard_passes_through_non_object_node` already pins this for the string guard; the plan adds the mirror test for the decimal walker.
- **Promotes to ADR:** no

### [3] `rewrite_decimal_stringifications` keeps its infallible `-> Json` signature, without a panic site

- **Decision:** Compose the primitive call with `.unwrap_or_else(|| node.clone())`, keeping the `-> Json` signature. The always-`Some` invariant lives in the doc comment.
- **Alternatives:** Change the signature to `-> Option<Json>` for uniformity with the two fallible guards (rejected — both call sites compose with `.map`, not `.and_then`; the change would churn callers and invite a decline path that cannot occur). Keep `-> Json` via `.expect` with the invariant as the panic message (rejected — see Review Findings [7]).
- **Rationale:** The infallible walker composes as the never-declining case of one primitive, and its contract at the call sites is unchanged. The closure is statically always-`Some`, so `.unwrap_or_else` is unreachable rather than merely improbable — it honors the signature without adding a panic site to the query-planning path, where the pre-refactor function had none.
- **Promotes to ADR:** no

### [4] The primitive is private until a cross-submodule caller exists

- **Decision:** Declare `rewrite_expr_tree` as a private `fn` in `support.rs`. Issue #177 widens it to `pub(super)` when it adds the first cross-submodule caller (`joins/rendering.rs`).
- **Alternatives:** `pub(super)` from the start, so #177 needs no visibility change (rejected — see Review Findings [6]).
- **Rationale:** Private is the narrowest visibility that compiles, which is the rule `vs-adapter/pushdown-module-structure` already records: "A cross-submodule private helper widens to the narrowest visibility that compiles (`pub(super)`), never to a broader public than it had before." `rewrite_expr_tree` is not cross-submodule in this plan, so it does not qualify. #257's own suggested snippet shows it private too, and the `strip_table_alias` precedent does not transfer — that helper has a real cross-submodule caller today. Widening later is a one-word change, and #257's "this issue OWNS the primitive, #177 reuses it" division is unaffected.
- **Promotes to ADR:** no

### [5] Named consts for the curated field lists rather than inline literals

- **Decision:** Keep `EXPR_ARRAY_FIELDS` and `EXPR_SINGLE_FIELDS` as named module-level consts, private to `support.rs`.
- **Alternatives:** Inline the two array literals in the primitive's body (rejected — the primitive would then be the only place the curation rationale could live, and the lists could not be named from a spec clause or a doc comment elsewhere).
- **Rationale:** The curated list is itself a design decision — "never descend into `dataType`/`name`" — and needs a documentable, greppable home. #257 names the consts explicitly.
- **Promotes to ADR:** no

### [6] Issue #177's blind collect walker stays a separate primitive

- **Decision:** Do not merge `rewrite_expr_tree` with the blind, collect-style `walk_json` that issue #177 dedups.
- **Alternatives:** One universal JSON walker serving both (rejected).
- **Rationale:** The blind walker recurses over every `map.values()` entry; the curated walker must NOT touch `dataType` or `name`. Merging them would silently widen the rewrite surface of all three type guards. #257 states the same boundary: it owns the curated rewrite primitive, #177 reuses it for its two rebuild-shape join walks and keeps its blind collect walk separate.
- **Promotes to ADR:** yes

### [7] The LIKE guard's widened reach may convert a former pushdown into a decline

- **Decision:** Accept the trade and record it as an explicit spec clause in `vs-adapter/pushdown-planning-like-type-coercion`, not as an implication of the widened traversal — split into its two sub-cases, because they are not the same trade (see Review Findings [1]).
- **Alternatives:** Keep the junction-only traversal and leave #207's documented blind spot open (rejected — the blind spot's only remaining home was a code comment, since #207 is closed); restrict the widened traversal to positions where no former pushdown existed (rejected — unimplementable without re-deriving which shapes DataFusion happens to reject).
- **Rationale:** A decline is unconditionally correct: Exasol evaluates the predicate natively. Where the subject type resolves to a non-string type the pre-change render hard-failed, so the decline fixes a crash. Where the subject name does not resolve, the pre-change render may have succeeded, so the decline costs a pushdown — accepted per interview A2, and slower rather than wrong.
- **Promotes to ADR:** yes

### [8] Commit 2 keeps the LIKE guard filter-only

- **Decision:** Widen traversal, add no call site. `project_columns` (#219) and the broadcast-join per-leg filter path (#215) stay untreated and separately tracked.
- **Alternatives:** Close #215/#219 in the same plan (rejected).
- **Rationale:** Each new surface has different fallback semantics — a select-list decline must set the existing widen-projection/full-base-row flag, whereas a filter decline drops the filter. Bundling them would hide two behavior changes behind a refactor. The uniform primitive makes closing them cheaper later; it does not close them.
- **Promotes to ADR:** no

### [9] Commit-1 dedup scenario goes to `pushdown-module-structure`, not a new feature

- **Decision:** Record the shared-primitive scenario as a NEW scenario in `vs-adapter/pushdown-module-structure`; record the traversal-widening as CHANGED/NEW scenarios in `vs-adapter/pushdown-planning-like-type-coercion`; correct the one stale reach claim in `vs-adapter/pushdown-planning-string-fn-type-coercion`.
- **Alternatives:** A new `pushdown-expression-rewrite-primitive` feature (rejected — `pushdown-module-structure` already carries exactly this scenario shape, e.g. "The dispatcher builds each fan-out spec from one shared shard-invariant base" and "Both qualified single-table fallback guards call one shared helper", and its Background already states the byte-identical-organization-only contract).
- **Rationale:** Fewer features, each with a coherent one-sentence responsibility. A new feature for one dedup scenario would fragment the refactor-scenario home.
- **Promotes to ADR:** no

### [10] Issue #257's call-site count is corrected in this plan

- **Decision:** Record the verified census: ONE production chain site (`mod.rs:210-214`, the single-table chokepoint) plus five chain replications inside `mod tests` (which starts at `mod.rs:789`), plus the select-list chain in `project_columns` (string + decimal guards only). #257's "six `mod.rs` sites" counts the production site plus the five test replications.
- **Alternatives:** Carry #257's count forward unverified (rejected).
- **Rationale:** The byte-identity proof for commit 1 rests on knowing exactly which tests replicate the production chain. A miscounted census would leave the implementer looking for five production sites that do not exist.
- **Promotes to ADR:** no

### [11] Iceberg-spec compliance determination recorded rather than omitted

- **Decision:** State explicitly in plan.md that the Apache Iceberg table spec is not implicated by this change, with the reason.
- **Alternatives:** Omit the check because the change is a refactor (rejected — CLAUDE.md requires the determination for anything touching scanning, pushdown, or schema/type handling, and a silent omission is indistinguishable from a skipped check).
- **Rationale:** The change alters traversal shape over the Exasol pushdown JSON IR only. It touches no file scanning, no delete application, and no Iceberg-boundary type mapping, and `filter_json_raw` — the tree fed to `resolve_file_list` pruning — stays unmodified. The Iceberg normative basis for the decline-not-cast rule (primitives such as `boolean`, `double`, and `timestamp` carry no spec-defined text form) is already quoted in the string-fn feature's Background and is unchanged here.
- **Promotes to ADR:** no

## Review Findings

### [1] [plan-review] Unresolvable LIKE subject was not "never a working pushdown"

- **Finding:** The plan and the like-coercion delta recorded an absolute — every newly-reached LIKE position "previously pushed down and hard-failed the DataFusion scan" / "was never a working pushdown" — that is false for one of the decline triggers the same scenario enumerates. Verified: `extract_all_column_types` (`support.rs:435-451`) `filter_map`s over `involvedTables[0].columns` and silently drops any entry missing `name` or `dataType`, and reads the FIRST involved table only, so a genuinely VARCHAR column can miss the lookup. At a newly-reached position that shape rendered `Utf8 LIKE Utf8` and SUCCEEDED; post-change it declines. No test could satisfy the absolute the spec recorded.
- **Direction change:** The trade itself is unchanged (settled by interview A2). The CHANGED scenario's reach clause split in two — the "replaces a hard scan failure" claim now scoped to a subject whose Exasol type RESOLVES to a non-string type, plus a separate clause stating that an UNRESOLVABLE subject MAY lose a working pushdown, which SHALL NOT be recorded as a fixed hard failure. Background rewritten to name the `extract_all_column_types` mechanism and drop "was never a working pushdown". plan.md § Impact split into the same two sub-cases. Decision-log [7] rationale corrected.
- **Promotes to ADR:** yes

### [2] [plan-review] Byte-identity clause contradicted the same plan's commit 2

- **Finding:** The `pushdown-module-structure` NEW scenario asserted byte-identical scan-driving SQL for "every pushdown request", while the sibling like-coercion delta requires a LIKE-in-CASE over a DECIMAL subject to decline (previously rendered) and over a DATE subject to be rewritten to `CAST(<col> AS VARCHAR)` (previously rendered bare). Both deltas record together, so the permanent library would carry a universal byte-identity claim next to a scenario asserting the opposite for a named shape.
- **Direction change:** The clause now scopes byte-identity to the traversal extraction — every request whose per-node decisions the extraction itself leaves unchanged — and names the like-coercion widened-reach scenarios as the one deliberate exception, arriving in a separate commit and covered by that feature's own scenarios. The clause also now names its evidence correctly (JSON-shape corpus for the two migrated walkers, plus the wired-chain rendered-SQL tests) instead of attributing rendered-SQL assertions to `support.rs`.
- **Promotes to ADR:** no

### [3] [plan-review] The leaf-equivalence test was scheduled after the migration it validates

- **Finding:** Task 2 read "add the leaf-equivalence tests FIRST, then migrate", but § Parallelization placed it in Group C, AFTER Group B's two migrations, and rationalized the inversion. A characterization test written after the migration pins the new code's behavior and proves nothing about equivalence with the old — the plan's own designated proof was scheduled where it could not serve as one.
- **Direction change:** The leaf-equivalence test became task 1 and its own first parallel group, ordered before the primitive extraction. Its text now mandates that it be added and PASS against today's `rewrite_decimal_stringifications` with the `!node.is_object()` early return still in place, then re-run unchanged after the migration, and states the stop condition: if it can only pass after the migration, the simplification is not behavior-preserving. Groups relabelled A–I with the new order.
- **Promotes to ADR:** yes

### [4] [plan-review] The stale-documentation sweep missed a live reach claim

- **Finding:** The plan required every stale reach claim to be corrected in commit 2, then enumerated a list missing `support.rs:5361-5363` (the doc comment on `string_fn_guard_reaches_function_under_comparison_predicate`), which the Verification table simultaneously listed as "unedited existing corpus" — so nothing would have brought an implementer to it. Two weaker spots in the same task: the `mod.rs:188-209` entry was conditional when the comment does assert the contrast, and task 9 named only the caveat sentence, leaving the junction enumeration at `support.rs:500-506` unnamed.
- **Direction change:** Re-verified with `grep -rn "junction" crates/`: exactly four live claims about the guard's reach — `support.rs:506`, `support.rs:875`, `support.rs:5362`, `mod.rs:949`. Task 9 now mandates rewriting the whole traversal paragraph (all three false claims quoted). Task 10 enumerates the other three sites with their test names, makes `mod.rs:188-209` unconditional (quoting the "not just LIKE subjects" parenthetical), and records that `mod.rs:1013-1018` and `joins/rendering.rs:529` were checked and assert no reach claim. § Requirements now mandates the grep before closing commit 2, and states that the grep alone is insufficient because the chain comment asserts the contrast without the word.
- **Promotes to ADR:** no

### [5] [plan-review] The unresolvable-subject absolute survived at two more sites (round 2)

- **Finding:** The round-1 fix for the false absolute landed in only two of four places. It survived verbatim at `plan.md:101` (§ Consequences row 3 Rationale), where the Rationale cell then contradicted its own Decision cell, the corrected § Impact, and the corrected decision-log [7]. Worse, it was restated NORMATIVELY at `vs-adapter/pushdown-planning-like-type-coercion/spec.md:45`, whose THEN clause enumerated "a DECIMAL, integer, DOUBLE, BOOLEAN, TIMESTAMP, or **unresolvable** subject" and governed all of them with "rather than hard-failing the DataFusion scan as the junction-only traversal did" — a self-contradicting requirement, because the sibling clause at `:36` explicitly forbids recording the unresolvable case that way. Background bullet 20 carried the same over-generalization unqualified.
- **Direction change:** All three sites corrected with the reviewer's supplied text: the § Consequences Rationale now splits the two sub-cases and cites § Impact and decision-log [7]; `unresolvable` is removed from the enumeration the hard-failure contrast governs, with a new clause stating that an unresolvable subject declines at that position under the same fail-safe rule and carries the pushdown-loss trade recorded in the nested-LIKE scenario; bullet 20's blind-spot sentence is scoped to "so a non-string subject hard-failed the DataFusion scan". No design decision, scope, or task changed. Appending the new clause pushed that scenario to four AND steps, over the CLI's recommended three, so the scenario's trailing filter-only wiring clause was dropped rather than the reviewer's clause trimmed — it duplicated the same statement in the permanent feature intro (`:14`) and Background bullet (`:18`), so interview A3's filter-only scope and the #215/#219 tracking stay recorded twice in this delta. A directory-wide grep confirms no fourth restatement remains: every other hit is either already scoped to a non-string subject (both feature intros, § Impact, task 9), or a deliberate historical record — the verbatim interview Q&A at `decision-log.md:10-18` and the Review Findings entries that quote the defect, neither of which may be rewritten without falsifying the record.
- **Promotes to ADR:** no

### [6] [plan-review] The primitive's `pub(super)` visibility was speculative

- **Finding:** `rewrite_expr_tree` was declared `pub(super)` for a consumer that does not exist in this plan, and that justification was written into a permanent spec Background. `pushdown-module-structure` already records the governing rule — a cross-submodule private helper widens to the narrowest visibility that compiles — and `rewrite_expr_tree` is not cross-submodule here, so the narrowest visibility is private. #257's own snippet shows it private. The cited `strip_table_alias` precedent does not transfer: that helper has a real cross-submodule caller today.
- **Direction change:** The primitive is now a private `fn` in the key-interface block, § Patterns row, task 2, and decision-log [1]/[4]. § Patterns and [4] record that #177 widens it to `pub(super)` when it adds the first cross-submodule caller. The `pub(super)`/#177 sentence is deleted from the module-structure delta's Background, keeping only the sentence that #177's blind collect walker stays a separate primitive. #257's "this issue owns the primitive, #177 reuses it" division is unaffected.
- **Promotes to ADR:** no

### [7] [plan-review] `.expect` would add a panic site to the query-planning path

- **Finding:** Keeping `rewrite_decimal_stringifications`' `-> Json` signature via `.expect` introduced a panic site where the pre-refactor function had none. The signature justification was sound, but `.expect` was not the only way to honor it: the closure is statically always-`Some`.
- **Direction change:** `.unwrap_or_else(|| node.clone())` replaces `.expect` in task 4, § Consequences row 2, and decision-log [3]. The always-`Some` invariant moves to the doc comment instead of a panic message. No behavior change — the fallback is unreachable either way.
- **Promotes to ADR:** no

### [8] [plan-review] The decimal rewriter's owning feature had no delta

- **Finding:** Commit 1 deletes `rewrite_decimal_stringifications`' traversal, but its owning feature got no delta, while the identical claim in `pushdown-planning-string-fn-type-coercion` was judged worth one — inconsistent treatment of the same defect. The permanent spec attributes the recursion to the function at `:5` ("A single shared recursive rewriter … walks each tree") and `:12` ("Nesting is handled by the recursion itself").
- **Direction change:** Added a fourth delta, `vs-adapter/pushdown-planning-decimal-string-format/spec.md`, reconciling both claims: the rewriter contributes a per-node stringifier decision and delegates recursion to the shared primitive, with post-order nesting behavior and infallibility unchanged. Added to plan.md § Features as CHANGED, to § Verification with its existing tests, and to task 5 as its implementing task. The advisory asked for a Background-only delta; `speq plan validate` rejects a delta with no scenario ("Missing Scenarios section"), so the reconciliation is anchored on the one scenario that actually asserts the disputed attribution — "Implicit CONCAT over a DECIMAL column …", whose THEN read "the recursive rewriter SHALL descend". It now reads that the shared post-order traversal descends and the rewriter's per-node decision replaces each argument, with a byte-identical clause added. No rendered output changes.
- **Promotes to ADR:** no

### [9] [plan-review] Requirement-quality and prose cleanups

- **Finding:** Four separate advisories: a `MAY apply` in the module-structure leaf clause permitted the rejected `is_object` early return to also conform; that same clause claimed "each guard's own leaf pass-through test" as evidence when only two of three guards will have one; the string-fn composition clause's operative content was an untestable rationale ("NOT because its traversal cannot reach it"); and plan.md mislabelled the `support.rs` guard tests a "rendered-SQL corpus" when they assert JSON-tree equality, diverging from the corrected module-structure clause. Plus two bloat findings: the Iceberg row and § Impact, and the sweep specification repeated in four places.
- **Direction change:** `MAY apply` → `SHALL apply`, with the evidence phrase scoped to "each guard that previously early-returned on a non-object … the LIKE guard having always applied its dispatch to leaves" (both landed on one clause, so applied as one rewrite). The string-fn clause keeps only the verifiable half — the LIKE guard leaves a non-bare-`column` subject unchanged while the string guard coerces the DECIMAL argument inside it — with the reach rationale moved to that delta's Background. plan.md's byte-identity requirement and Verification row now name the two evidence classes separately, closing the labelling divergence. The Iceberg row is two sentences citing [11]; § Impact is four lines; the § Requirements sweep row is two sentences citing tasks 9-10 for the site list.
- **Promotes to ADR:** no

### [10] [plan-review] A second non-grep sweep site

- **Finding:** The sweep claimed completeness with one site outside the grep's reach; there are two. The inline comment on `like_subject_type_guard`'s `_` match arm (`support.rs:563-564`) reads "Any other node (predicate_equal, column, literals, …) is not a LIKE and cannot nest one in this grammar". Commit 2 falsifies that precisely — `predicate_equal` is the node the widened traversal descends through to reach a LIKE under `left`, this plan's headline repro — and it survives task 8's rewrite because it annotates the `_` arm that becomes the closure's catch-all. § Dead Code Removal stopped two lines short of it.
- **Direction change:** Task 10 now lists both non-grep sites, with the instruction to delete the "cannot nest one in this grammar" claim or restate it as "is not itself a LIKE". § Dead Code Removal's range extends from `~544-561` to `~544-564` with a note on why. § Requirements names both non-grep sites rather than one.
- **Promotes to ADR:** no

### [11] [plan-review] Anchor resync to `main` at `f4f6fd3`

- **Finding:** `main` fast-forwarded from `e5a7e15` to `f4f6fd3` (#256, LIMIT_WITH_OFFSET), landing +67 lines in `support.rs` and +183 in `mod.rs` — the `fix/191-order-by-offset` branch #257 warned about.
- **Direction change:** Verified by symbol name against the new HEAD, not by arithmetic. #256 added no fourth traversal copy — its new `support.rs` functions (`extract_offset`, `render_limit_offset`) are flat helpers — so the three-guard premise and every design decision stand. Every production anchor the plan cites still resolves unchanged (`like_subject_type_guard` 536, `rewrite_decimal_stringifications` 661, `string_function_arg_type_guard` 887, traversal doc 500-507, `_`-arm comment 563-564, chain 210-214, select-list 1113/1125). Only test-region anchors drifted and were refreshed: `mod tests` → `support.rs:1415` / `mod.rs:789`; the five chain sites → `893/932/969/1001/1033`; junction sites → `support.rs:5362` and `mod.rs:949` (doc `5361-5363` / `949`); the clean-checked doc → `mod.rs:1013-1018`. Two coordinator-supplied numbers were corrected against the file: `filter_decimal_comparison_not_rewritten` is at `mod.rs:923` (not 922), and the decimal walker's non-object early return is at `support.rs:667-669` (not 670-673). `grep -rn "junction" crates/` re-run: the four live sites are `support.rs:506`, `support.rs:875`, `support.rs:5362`, `mod.rs:949`.
- **Promotes to ADR:** no

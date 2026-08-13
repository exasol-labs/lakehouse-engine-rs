# Plan Review Findings: fix-vended-storage-shared-policy (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 6 (Blockers: 1, Advisory: 5)
- Intent Fidelity blockers: 0

This is the final round of the bounded review. The one BLOCKER is the unresolved half of round-1
BLOCKER 1: the refuted prior-behaviour claim was deleted from `plan.md` and left standing, verbatim
in substance, in a spec-delta Background bullet that the recorder merges into the permanent library.
Round-1's eight ADVISORY findings were deliberately untouched by the fixup pass and remain open.

## Round-1 Blocker Recheck

- **Not resolved (half): [UNSTATED_ASSUMPTION] Impact's plaintext-endpoint bullet asserted a prior behaviour the code does not have.**
  The `plan.md` half IS resolved: § Impact's fourth bullet now states the real prior behaviour
  ("the CONNECTION's `endpoint` is never read on the vended path … it is discarded"), names both new
  failure modes, and the clause "so no working configuration regresses" is gone (grep-verified
  absent from every artifact). § Migration gained corrected `endpoint` and `region` rows, and the
  FIRST § Impact bullet was corrected consistently ("previously had that CONNECTION value
  DISCARDED") — the fixer's unprompted second correction is right and the two bullets now agree.
  What survives is a THIRD instance in `vs-adapter/pushdown-planning-cloud-credentials/spec.md:15`:
  "the same CONNECTION previously either hit the store-address error or fell through to the AWS
  default". That is the same refuted assertion, and it carries an inverted conclusion ("strictly
  narrower than before, not wider"). See the Feasibility finding below. Prior behaviour re-verified
  first-hand: `s3_backend_from_vended` (`crates/lakehouse-catalog/src/vended.rs:114-168`) computes
  `let endpoint = vended_config_value(vended, "s3.endpoint");` and reads no CONNECTION field.

- **Resolved: [REQUIREMENT_CONFLICT] A ninth normative copy of the reversed rule was left standing in `vs-adapter/connection-credentials`.**
  `specs/_plans/fix-vended-storage-shared-policy/vs-adapter/connection-credentials/spec.md` exists,
  is listed in plan.md § Features (8 rows), carries a § Verification row, and validates. Its
  `DELTA:CHANGED` scenario reproduces the recorded scenario clause-for-clause and accounts for all
  NINE fields the recorded `MUST NOT read` clause named: six credential fields stay forbidden,
  `endpoint`/`region` move to an addressing clause, `path_style` gets its own MUST-NOT clause. No
  recorded clause is silently dropped (compared against
  `specs/vs-adapter/connection-credentials/spec.md:135-143`). The citation is unambiguous — it names
  `vs-adapter/pushdown-planning-cloud-credentials` § "Vended-credentials request advertises access
  delegation and resolves the store address with the CONNECTION winning when set", which is the
  exact title of the `DELTA:NEW` scenario in that delta, so it resolves once both land. The
  addressing clause is independently testable on its own terms (is `endpoint`/`region` present in
  the effective storage when the CONNECTION states one), not merely non-contradictory. The
  `rest-catalog-oauth-auth` clause-92 judgment call is CORRECT: the clause's storage block always
  carried an `endpoint` and a `region` even before this plan, so "only the S3 storage credentials"
  was never a statement about where addressing comes from, and the same sentence defers explicitly
  to `pushdown-planning-cloud-credentials`. No ninth delta is needed.

- **Resolved: [COMPLETENESS_GAP] Half the replacement for the superseded credential guarantee had no mechanism.**
  Non-`pub` fields plus `endpoint()`/`region()` accessors are specified in four places that agree:
  plan.md § Key interfaces line 74, task 2.3 (now `[expert]`), § Patterns line 94, decision-log [5],
  and normatively in `vs-adapter/storage-backend-enum/spec.md:50` ("both addressing fields SHALL be
  declared NON-`pub` … SHALL NOT COMPILE"). Task 2.3 implements it, not just names it. Task 5.6's
  probe is real, not vacuous: `CATALOG_SOURCES` in
  `crates/lakehouse-catalog/tests/catalog_public_surface.rs:36-52` already embeds `src/storage.rs`
  via `include_str!`, and `source("storage.rs")` is the existing accessor, so a source-text
  assertion over `struct StaticStoreAddress`'s declaration fails on a future `pub endpoint: String`.
  The mechanism claim also holds in Rust terms: private fields are module-scoped, so `vended.rs`
  and `unity/vended.rs` cannot build the value field-by-field, and `Default` yields only the empty
  address. One ADVISORY on the accessors' visibility is below.

- **Resolved: [TRACEABILITY_GAP] The single production construction of the new type had no test at its own layer.**
  Task 6.4 exists, is `[expert]`, and targets
  `crates/lakehouse-engine/src/adapter/pushdown/file_resolution_tests.rs` — the correct sibling under
  CLAUDE.md's test-layout rule; that file exists and is declared `#[path = "file_resolution_tests.rs"]
  mod tests;` at `file_resolution.rs:781-782`. The cited justification is VERIFIED first-hand, not
  taken on faith: `iceberg-0.10.0/src/scan/mod.rs:202-216` is exactly the `None =>` arm whose
  `let Some(current_snapshot_id) = … else { return Ok(TableScan { … plan_context: None … }); }`
  returns a scan with no plan context, and `TableScan::plan_files` at the same file's line 364
  returns `Ok(Box::pin(futures::stream::empty()))` when `plan_context` is `None`. The other
  I/O-issuing step on that path also short-circuits: `ensure_supported_delete_mechanisms`
  (`file_resolution.rs:420-499`) returns `Ok(())` immediately when `metadata.current_snapshot()` is
  `None`. So `resolve_file_list` returns the effective `StorageBackend` with zero files and no
  object-store access, exactly as the task claims. The fixture is buildable from the existing
  helper: `load_table_body_with_empty_location` (`file_resolution_tests.rs:925-935`) already emits
  metadata with no `snapshots` and no `current-snapshot-id`, and needs only a non-empty `location`
  plus a `config` map. Specifying an HTTPS CONNECTION `endpoint` is the right call — a plaintext one
  would trip the new gate the task is not testing.

- **Resolved: [TASK_GRANULARITY] Group B's expert tagging inverted its own dependency order.**
  Task 2.3 is retagged `[expert]`, and § Parallelization gained an intra-group order block for all
  four groups. The specific round-1 inversion (expert `s3_backend` routed ahead of standard
  `StaticStoreAddress`) is closed: 2.1, 2.2, and 2.3 are now all `[expert]`, so they land in one
  expert set. Two new inaccuracies in that same block are ADVISORY below.

- **Resolved: [TRACEABILITY_GAP] One probe had no task and one probe had two names.**
  Task 5.5 creates `shared_vended_policy_steps_are_not_public` over `CATALOG_SOURCES` for all five
  shared functions plus `struct VendedS3` and the `lib.rs` re-export check, and folds in the
  correction of `demoted_and_deleted_functions_are_not_declared_public`. Verified against the real
  test: `catalog_public_surface.rs:92-110` does pin a five-name list containing
  `"pub fn s3_backend_from_vended"`, and the recategorisation is sound — the list's existing
  `"pub fn build_s3_file_io"` entry is already a deleted-predecessor guard, so
  `s3_backend_from_vended` plus its deleted sibling `adls_backend_from_vended` join that class
  rather than duplicating the new test's assertions. Neither name dangles: both appear in plan.md
  § Dead Code Removal as deletions of real symbols. The credential-absence probe is reconciled to
  ONE name, `static_store_address_is_reachable_and_declares_no_credential_field`, in task 5.3 and in
  both § Verification rows (lines 252, 260). Task 5.6's `static_store_address_fields_are_not_public`
  is created once and named in the same two rows.

`speq plan validate fix-vended-storage-shared-policy` re-run independently: **passes**, 8 deltas,
warnings only (AND-step counts). Global consistency swept end to end: plan.md § Features, § Verification,
and decision-log [10]'s title and list all say EIGHT deltas; no stale seven-delta or seven-feature
count survives anywhere.

## Intent Fidelity

[no objection — axis checked. The interview's two decisions still govern: CONNECTION-wins-per-field
is unchanged in decision-log [1] and now restated identically in the new `connection-credentials`
delta, and no "vended wins" phrasing exists in any artifact (grep-verified). The eighth delta is a
legitimate incremental fix, not late scope creep: it supersedes an existing recorded clause the plan
already reverses, adds no behaviour, no task, and no test beyond one § Verification row reusing
tests tasks 6.4 and 3.3 already create, and therefore stays inside the interview's "one plan
covering both" answer. Calibration on round 1: catching this delta on its own pass was right, but
round 1 scoped its own BLOCKER-1 Fix to `plan.md` § Impact when the same misstatement was already
sitting in a spec delta destined for the permanent library — the more consequential of the two
locations. Round-1's ADVISORY [SCOPE_REDUCTION] on `path_style` remains open by design.]

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: `vs-adapter/pushdown-planning-cloud-credentials/spec.md`, Background bullet at line 15 ("**The plaintext consent gate now guards the RESOLVED endpoint, whichever source supplied it.**")
- Issue: The bullet closes with the exact claim round 1 refuted, and draws the opposite conclusion from the corrected `plan.md`: "That is strictly narrower than before, not wider: the same CONNECTION previously either hit the store-address error or fell through to the AWS default." Re-verified against the code: `s3_backend_from_vended` (`crates/lakehouse-catalog/src/vended.rs:114-168`) computes `let endpoint = vended_config_value(vended, "s3.endpoint");` and reads no `ConnectionCreds` field at all, so a CONNECTION `endpoint` is neither a competitor nor a fallback on the vended path — it is discarded. The bullet is therefore false in the majority case and inverted in its conclusion: whenever the response vends an HTTPS `s3.endpoint` OR a `client.region`, the old code neither errored nor fell back, so a CONNECTION carrying `use_vended_credentials: true`, a stale plaintext `endpoint` (`http://minio:9000`, the shape of every MinIO CONNECTION in this repo's fixtures), and `ALLOW_HTTP` false works today and becomes a plan-time `UdfError::User`. The gate is WIDER, not narrower. This is worse than the round-1 location, not equivalent: `plan.md` is archived on record, whereas a spec Background bullet is merged into `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md` and becomes the library's own recorded justification — so the library would carry a false factual premise for the one clause a future reader consults to decide whether the gate may be relaxed, and it would contradict `plan.md` § Impact's fourth bullet and both new § Migration rows in the same commit.
- Fix: In `specs/_plans/fix-vended-storage-shared-policy/vs-adapter/pushdown-planning-cloud-credentials/spec.md`, delete the sentence "That is strictly narrower than before, not wider: the same CONNECTION previously either hit the store-address error or fell through to the AWS default." from the line-15 Background bullet and replace it with the verified prior behaviour and the true direction: a CONNECTION `endpoint` was never read on the vended path (`s3_backend_from_vended` read `vended_config_value(vended, "s3.endpoint")` alone), so this gate WIDENS the refusal set — a CONNECTION carrying a plaintext `endpoint` beside a response that vends an HTTPS `s3.endpoint` or a `client.region` works today and becomes a plan-time `UdfError::User`, and under `ALLOW_HTTP = 'true'` the same CONNECTION moves the store address off the vended endpoint onto the stale CONNECTION one. State that this matches `plan.md` § Impact's fourth bullet and the two § Migration rows. Then add a `[plan-review]` entry to `decision-log.md` recording that the round-1 correction was applied to `plan.md` only and that the spec-delta copy is the durable one.

## Requirement Quality

[no objection — axis checked. The new `connection-credentials` `DELTA:CHANGED` scenario is complete
against the recorded original (all nine named fields re-homed, four unrelated clauses reproduced
verbatim, one extended with a non-contradicting reason) and testable clause by clause. `speq plan
validate` passes on all eight deltas. The two enforcement clauses added to
`storage-backend-enum/spec.md:50` and `catalog-crate-structure/spec.md:21` are consistent with each
other and with decision-log [5]; neither conflicts with a recorded clause in the live library that
this plan does not already supersede. The round-1 ADVISORY findings on `storage-backend-enum` clause
35's doc-comment stripping, on the missing non-empty-address selector tests, and on task 1.1's
`account name` token remain open by design.]

#### [SCOPE_CREEP] ADVISORY
- Location: plan.md § Key interfaces line 74 and task 2.3; `vs-adapter/catalog-crate-structure/spec.md` clauses 19 and 21; decision-log.md [5]
- Issue: The chosen mechanism adds two `pub fn` accessors to the crate's public surface with no external consumer, inside the one feature whose stated purpose is "a concept-level public surface and every mechanism step crate-private". `s3_backend` lives in `storage.rs`, the same module as the struct, so it can read the private fields directly — an accessor is not needed for it, and `pub` is not needed for an accessor it does use. No cross-module read exists: `vended.rs` and `unity/vended.rs` only forward `&StaticStoreAddress` to `s3_backend`, and task 5.3's probe asserts over the declaration's TEXT rather than reading a value. decision-log [5]'s defence — "the accessors are wired as the production read path (`s3_backend` reads through them) so they are not dead public surface" — is circular: reading through an accessor from inside the defining module is a style choice, not a visibility requirement. `pub(crate)` (or no accessor at all) preserves the entire compile-time guarantee, since field privacy is what closes the one-construction hole. Separately, the two clauses disagree on the item count: clause 19 supersedes the recorded `pub` enumeration to admit "exactly ONE type … plus its `Default` and exactly ONE conversion" and does not name the accessors, while clause 21 relies on them ("exposes its two fields through accessors only") — so an auditor reading clause 19 against the code finds two `pub fn` the enumeration never admitted.
- Fix: In plan.md § Key interfaces line 74 and task 2.3, narrow the accessors to `pub(crate) fn endpoint(&self) -> &str` / `pub(crate) fn region(&self) -> &str`, stating that field privacy alone carries the one-construction guarantee and that no caller outside `lakehouse-catalog` reads either value. Amend decision-log [5] § Rationale to replace the "production read path" justification with that reason. In `vs-adapter/catalog-crate-structure/spec.md`, reword clause 21 to say the fields are non-`pub` and read through crate-private accessors, so clause 19's enumeration of ONE type plus its `Default` plus ONE conversion stays exhaustive as written. If the accessors are instead kept `pub`, add them to clause 19's admitted list and to task 5.4's `use`-list edit.

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: `vs-adapter/unity-catalog-vended-credentials/spec.md`, Background bullet 10 and scenario clause 23; decision-log.md [plan-review] entry on the `connection-credentials` delta
- Issue: The round-2 fixup justified CITING the per-field precedence rule in the new `connection-credentials` delta rather than restating it, on the ground that "restating the per-field precedence here would have put one rule in two homes, which is the failure this plan exists to remove". That principle is not applied to `unity-catalog-vended-credentials`, which restates the same rule twice in full: Background bullet 10 ("For `endpoint` and `region` independently, a non-empty CONNECTION value takes precedence over whatever the catalog vends; vended addressing fills in only when the CONNECTION is silent") and normative clause 23 ("taking each independently from the CONNECTION when the CONNECTION's value is non-empty and from the vended response otherwise"). Clause 23 names the shared rule AND reproduces its content, so a later change to the precedence rule in `pushdown-planning-cloud-credentials` leaves two normative copies free to drift — the exact defect class this plan exists to remove, now recreated in the spec library rather than in the code. This is pre-existing from round 1, not introduced by the fixup, but the round-2 citation decision makes the inconsistency visible: two deltas in one plan apply opposite rules to the same duplication.
- Fix: In `specs/_plans/fix-vended-storage-shared-policy/vs-adapter/unity-catalog-vended-credentials/spec.md`, replace the restated precedence in scenario clause 23 with a citation matching the `connection-credentials` delta's form — the selector SHALL resolve `endpoint` and `region` through the ONE shared store-address rule specified in `vs-adapter/pushdown-planning-cloud-credentials` § "Vended-credentials request advertises access delegation and resolves the store address with the CONNECTION winning when set" — keeping the empty-address success clause 24, which is this feature's own behaviour. Reduce Background bullet 10 to the interview attribution plus that same pointer. Record the applied principle once in decision-log [10]: the precedence rule has one normative home and every other delta cites it.

## Task Breakdown

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Parallelization, the new intra-group order block (Group B bullet and Group D bullet); plan.md § Implementation Tasks 2.1, 2.2, 6.1, 6.4
- Issue: Two inaccuracies in the block the fixup added. (1) Group B's bullet reads "**Within Group B: 2.3 → 2.1 · 2.2 → 2.4.** … 2.1 and 2.2 are then independent of each other", but they are not: task 2.2 adds `s3_backend`, whose § Key interfaces signature is `s3_backend(VendedS3, location: &str, allow_http: bool, address: &StaticStoreAddress)`, and task 2.1 is what adds `pub(crate) struct VendedS3`. `VendedS3` is a signature dependency of 2.2 exactly as `StaticStoreAddress` is — the reason 2.3 was moved first — so the same argument places 2.1 before 2.2. The bullet also contradicts itself: `·` is this plan's concurrency marker (used that way for Groups C and D), yet the same sentence ends "2.1, 2.2, and 2.3 all edit `storage.rs`, so they are never file-disjoint and this order is the one the expert set must execute in". (2) Group D's bullet states "inside task 6, 6.1 precedes 6.4", but only 6.4 carries `[expert]` while 6.1 does not, so the same expert/standard partition that produced round-1 BLOCKER 5 can invert the stated order — and because 6.1 edits `file_resolution.rs` while 6.4 edits `file_resolution_tests.rs`, the two look file-disjoint and may simply run concurrently. Not raised as a BLOCKER: unlike round 1, an explicit order IS now stated, both Group B tasks are in one expert set on one file, and the worst outcome is a duplicated one-line edit to `file_resolution.rs:262` that the compiler and 6.4's own assertion surface immediately — no wrong behaviour reaches the tree. The remedy is two words.
- Fix: In plan.md § Parallelization, rewrite the Group B order as "**Within Group B: 2.3 → 2.1 → 2.2 → 2.4**" and replace "2.1 and 2.2 are then independent of each other" with "2.2's signature names both `VendedS3` (task 2.1) and `StaticStoreAddress` (task 2.3), so all three are strictly sequential on one file". In plan.md § Implementation Tasks, tag task 6.1 `[expert]` so it joins 6.4's expert set, and state in the Group D bullet that 6.1 and 6.4 are one sequential pair inside that set rather than two file-disjoint tasks.

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Test Disposition; plan.md § Verification > Scenario Coverage line 250; plan.md § Implementation Tasks 5.5, 2.4
- Issue: Two residues of the class round-1 BLOCKER 6 closed. (1) Task 5.5 edits an existing test — `demoted_and_deleted_functions_are_not_declared_public` (`crates/lakehouse-catalog/tests/catalog_public_surface.rs:92-110`): it adds a name to the pinned list and rewrites the doc comment — but § Test Disposition has no row for it, while every other touched test in that file has one. The section is the implementer's checklist of what may change and how, so an edited test missing from it is an unpinned edit. (2) `shared_home_builds_both_backends_from_neutral_vended_values` is named in § Verification as the sole test for the `storage-backend-enum` "Vended policy and construction move into the enum's own module" scenario, but no task names it: task 1.3 adds derivation tests and task 2.4 lists five matrices by description, none of which is "builds both backends from neutral vended values". The implementer must infer which task owns it.
- Fix: In plan.md § Test Disposition, add a row for `demoted_and_deleted_functions_are_not_declared_public` (`tests/catalog_public_surface.rs`) with disposition "AMENDED by task 5.5 — `s3_backend_from_vended` recategorised as a deleted predecessor, `adls_backend_from_vended` added beside it, doc comment rewritten; the existing four entries and the `CATALOG_SOURCES` loop are UNCHANGED". In plan.md § Implementation Tasks 2.4, name `shared_home_builds_both_backends_from_neutral_vended_values` explicitly among the tests that task creates.

## Design Depth

[no objection — axis checked against the Quick Diagnostic for the parts the fixup changed. The
private-fields mechanism moves the one-construction decision from prose to the compiler and keeps a
single owner: `From<&ConnectionCreds>` in `storage.rs` remains the only place deciding which
CONNECTION fields cross into vended resolution, and Rust's module-scoped field privacy actually
delivers that — `vended.rs` and `unity/vended.rs` cannot construct the value field-by-field, and
`Default` yields only the empty address, so the guarantee is enforced at every call site in both
crates rather than at review. Dependency direction is unaffected: the type still names no delivery
mechanism and carries two plain strings. The new `connection-credentials` delta adds no module and
no interface, so it introduces nothing for this axis to weigh. Two ADVISORY findings above touch
this axis's concerns — the accessors' visibility and the duplicated precedence rule. Round-1's
ADVISORY findings on `VendedBackendKind`'s mirror probe and on the `abfs`-gate error-precedence
inversion remain open by design.]

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Impact, lead sentence and bullets 1 and 4
- Issue: The correction made two bullets describe one change. Bullet 1 ("**Breaking — a CONNECTION-configured store address now overrides a vended one.**") and bullet 4 ("**Breaking — a CONNECTION `endpoint` the vended path previously IGNORED now places the store, and a plaintext one is refused at plan time.**") both establish the same premise and the same consequence: the CONNECTION value was discarded, now it wins. Bullet 4's only unique payload is the plaintext-gate consequence and the code citation. The lead sentence then miscounts on that duplication — "Four operator-visible changes on the vended path, TWO of them breaking" counts the precedence change twice, so an approver reading for the breaking set sees two where there is one plus its plaintext side effect. § Impact is the section an architect reads to approve, and it now costs two paragraphs to deliver one fact.
- Fix: In plan.md § Impact, merge bullets 1 and 4 into one Breaking bullet stating once that the CONNECTION's `endpoint` and `region` were never read on the vended path and now win when non-empty (keeping the `crates/lakehouse-catalog/src/vended.rs:114-168` citation and the note that both in-repo vended fixtures carry neither), followed by the two new failure modes the plaintext gate creates. Change the lead sentence to "Three operator-visible changes on the vended path, ONE of them breaking." and keep the two Fixed bullets unchanged.

[Otherwise no objection — axis checked on the changed prose: the corrected § Impact bullet and both
new § Migration rows are statement-first and internally consistent; the new `connection-credentials`
delta's description line and Background bullets are terse, name the actor, and reserve ALL-CAPS
RFC-2119 keywords for its scenario clauses; the new intra-group order block and the six
`[plan-review]` decision-log entries carry no filler, hedging, or escape clauses. Round-1's ADVISORY
[PROSE_UNCLEAR] on the four inaccurate statements — "byte-identical", "33 tests", the unnamed
rewritten test, and the `lakekeeper_vended_*` row — remains open by design; note that its unnamed
rewritten test still leaves § Test Disposition line 212 instructing a RENAME with no new name.]

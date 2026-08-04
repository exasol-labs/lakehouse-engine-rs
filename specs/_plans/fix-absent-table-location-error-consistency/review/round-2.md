# Plan Review Findings: fix-absent-table-location-error-consistency (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 5 (Blockers: 1, Advisory: 4)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

- **Resolved: `[UNSTATED_ASSUMPTION]` — the plan conflated an absent `location` key with an empty
  `location` value.** Option (b) was executed and the reviser's pushback against option (a) holds.
  Every source citation checks out at the pinned version: `TableMetadataV2V3Shared.location: String`
  at `iceberg-0.10.0/src/spec/table_metadata.rs:810`, `TableMetadataV1.location: String` at `:855`,
  `#[serde(untagged)] enum TableMetadataEnum { V3, V2, V1 }` at `:783-788`, and
  `#[serde(try_from = "TableMetadataEnum")]` at `:64`. The omitted-key error text is confirmed:
  `authed_get_json` returns `UdfError::User(format!("failed to parse catalog response: {}",
  redact(&e.to_string())))` (`crates/lakehouse-catalog/src/iceberg_io.rs:89-94`). Read against the
  fetched issue body, #296's own offending snippet branches on `table_s3_location.is_empty()` and its
  acceptance criteria target the `warehouse` substitution, the `UdfError::User` variant, and the
  no-panic rule — all three of which the omitted-key path already satisfies. A1's accepted snippet
  tests `.is_empty()` verbatim, so scoping the guard to that shape operationalizes the interview
  rather than narrowing it. Round 1's "acceptance criterion unmet" framing was overstated; the real
  defect was terminology conflation, and the delta now reads "EMPTY" in the GIVEN, THEN, and
  substitution clauses with a Background bullet assigning each wire shape its owner. Decision [8]
  records the trade honestly, including that the two shapes "differ ONLY in message specificity".
- **Resolved: `[REQUIREMENT_CONFLICT]` — the empty-root property was attributed to the wrong spec
  owner.** Both references now point at `datafusion-scan/scan-execution-file-metadata`
  (`vs-adapter/pushdown-planning/spec.md:89-92` and the scenario clause at `:110`), and the delta
  additionally states that `vs-adapter/pushdown-planning-file-encoding` carries no empty-root rule.
  The new delta exists and its two reproduced scenarios diff byte-identical against
  `specs/datafusion-scan/scan-execution-file-metadata/spec.md:37-43` and `:61-66`. § Dead Code
  Removal now cites `reconstruct_abs_uri` (`crates/lakehouse-engine/src/scan/object_store.rs:250`,
  verified) and correctly names `pushdown/mod.rs:250` as `relativize_shards_to_root` (verified). The
  feature is added to § Features, § Scenario Coverage, and § Manual Testing. The fix is complete —
  but the new delta introduces a fresh defect of the same family; see the BLOCKER under Requirement
  Quality.
- **Resolved: `[AMBIGUOUS_REQUIREMENT]` — both verification sweeps could never pass.** Ran both
  commands verbatim from the repo root. Sweep 1 returns exactly the four hits task 5 enumerates —
  `specs/vs-adapter/pushdown-planning-cloud-credentials/spec.md:66` and `:153`,
  `crates/lakehouse-catalog/src/session.rs:279`, `docs/catalogs.md:156` — and nothing else. Sweep 2
  returns exactly `crates/lakehouse-engine/tests/cloud_e2e_test.rs:10` and `:794`, both of which
  task 3 rewrites, so "MUST return nothing after task 3" is now achievable. The `.git/` exclusion the
  original omitted is present in both.
- **Resolved: `[TRACEABILITY_GAP]` — no task removed the in-code comment asserting the vended-only
  framing.** Task 2 now names `file_resolution.rs:243-247` and quotes the offending clause, which is
  verified verbatim at `:247` ("— so an absent location is its own error on the vended branch
  below."), requires the guard-runs-above-the-split replacement, and enumerates the surviving
  substance that MUST be kept. § Dead Code Removal carries a matching row for `:247`.
- **Applied and correct: `[HIDDEN_DEPENDENCY]` (round-1 advisory).** Task 1's fifth harness note
  requires `"net"` and `"io-util"` on `[dev-dependencies] tokio`. Both cited feature lists are
  accurate: `crates/lakehouse-engine/Cargo.toml:71` is `["rt-multi-thread", "macros", "time"]` and
  workspace `Cargo.toml:65` is `["rt", "macros"]`.

## Premortem

Three ways this plan fails after it ships:

1. **The permanent spec library gains six duplicated normative bullets.** `/speq:record` appends a
   delta's Background bullets to the recorded feature's Background — it deletes a recorded bullet only
   when a delta bullet explicitly says `SUPERSEDES` and quotes it. The new
   `datafusion-scan/scan-execution-file-metadata` delta reproduces all six recorded bullets alongside
   its one new one, so the merged feature carries the no-HEAD rule, the path-resolution rule, and the
   empty-root rule twice each. The next plan editing one copy leaves the other stale — precisely the
   drift decision [5] invokes to justify one-owner-per-rule.
2. **An iceberg-rust upgrade silently falsifies a `SHALL`.** The delta's final scenario clause
   asserts that an omitted `location` key is rejected at deserialization. That holds only because
   `iceberg-0.10.0` declares `location: String` non-`Option`. A v4-capable release makes the field
   optional; the clause becomes false, no test fails, and an omitted `location` flows past every
   guard into an empty `table_root` again. Decision [8]'s entire case rests on that derive and
   nothing guards it.
3. **A later reader cannot find the third clause the plan orders them to preserve.** Four artifact
   locations instruct the reader to retain "three empty-table-root `SHALL` clauses". Only two carry
   an RFC-2119 keyword. The reader concludes one was already lost and either invents a clause or
   deletes the retention bullet as stale.

## Intent Fidelity

No objection — axis checked. The guard placement, the `.is_empty()` condition, and the
`relativize_path_to_root` exclusion match A1's accepted snippet verbatim (plan.md:108,
decision-log.md:9-21, § Dead Code Removal's explicit MUST-NOT-REMOVE row). The test remains the host
`cargo test` unit test A2 accepted, driving both `use_vended_credentials` arms. Issue #296's five
acceptance criteria map onto the plan: the guard (criterion 1), § Audit Findings plus the join and
`createVirtualSchema` bullets (criterion 2), task 1 (criterion 3), task 5's sweeps (criterion 4), and
§ Dead Code Removal's retention row (criterion 5). The issue's `cloud_e2e_test.rs` "mirrored
fallback" deliverable is already satisfied in the tree — `:731` reads
`let anchor = result.metadata.location();` with a non-empty assertion and no `warehouse` branch — so
task 3's two doc strings are the remaining work there. Round 1's `[INTENT_DRIFT]` advisory is
carried by the orchestrator and not re-raised.

## Feasibility

No objection — axis checked. Task 1's harness is buildable as specified. Verified: `use_sigv4 = true`
short-circuits both requests the harness must avoid — `resolve_catalog_auth` returns
`Ok(CatalogAuth::Sigv4)` before any HTTP (`crates/lakehouse-catalog/src/auth.rs:221-222`) and
`resolve_load_table_prefix` returns `glue_catalog_prefix(warehouse)` without the `/v1/config`
round-trip (`crates/lakehouse-catalog/src/session.rs:148-151`), leaving exactly one GET per
`resolve_file_list` call. `CatalogSession::resolve` builds `reqwest::Client::new()` per call
(`session.rs:204`), so the two-listener design is required and sufficient. The minimal v2 metadata
fixture at `crates/lakehouse-catalog/src/vended.rs:303-317` carries no `snapshots` and no
`current-snapshot-id`, so the pre-fix non-vended arm reaches `plan_files()` without object-store I/O
and returns something other than the required `UdfError::User` — the failing-test-first gate holds
without a network hang. The three loopback-fake precedents cited in § Patterns are real
`#[tokio::test]`s at `session.rs:407`, `:464`, and `:521`.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `specs/_plans/fix-absent-table-location-error-consistency/datafusion-scan/scan-execution-file-metadata/spec.md`
  § Background bullets 2-7; plan.md:64
- Issue: The new delta reproduces all six of the recorded feature's Background bullets verbatim
  alongside its one new bullet, and this repo's merge convention appends delta Background bullets
  without deduplication — so `/speq:record` will write each of those six rules into
  `specs/datafusion-scan/scan-execution-file-metadata/spec.md` twice. Evidence for the convention:
  `specs/vs-adapter/pushdown-planning-like-type-coercion/spec.md` has accumulated 31 Background
  bullets across successive plans, while that feature's deltas in
  `specs/_recorded/2026-07-30-fix-declined-filter-self-apply/` and
  `specs/_recorded/2026-07-31-fix-join-filter-type-rewrites/` carry only 5 and 2 NEW bullets
  respectively; a recorded bullet disappears only when a delta bullet names it (`grep -c 'A
  non-string LIKE subject anywhere'` on the merged spec returns 1 — the `SUPERSEDES` quote alone).
  The plan is internally inconsistent about this too: its other two deltas correctly carry new-only
  bullets (8 for `vs-adapter/pushdown-planning`, 3 for `pushdown-planning-cloud-credentials`, none
  duplicating recorded text), and plan.md:64 states the delta "adds one Background bullet" when the
  file adds seven. Six duplicated normative bullets is the exact drift failure decision [5] cites as
  its reason for one-owner-per-rule, self-inflicted by the plan that argues it.
- Fix: In
  `specs/_plans/fix-absent-table-location-error-consistency/datafusion-scan/scan-execution-file-metadata/spec.md`,
  delete Background bullets 2 through 7 (every bullet reproduced from
  `specs/datafusion-scan/scan-execution-file-metadata/spec.md:10-25`), keeping ONLY the new
  "**The empty-table-root clauses are retained as a wire-format totality property…**" bullet. Where
  that bullet needs to point at a retained rule, cite it by its opening phrase (for example, "the
  Background bullet beginning 'When the common spec carries an empty table root'") instead of
  reproducing the text. Leave the feature-description paragraph and the two `DELTA:CHANGED`
  scenarios as they are — the scenario markers merge by name and are correct. Then reconcile plan.md
  § Features' sentence with the file: it must state that the delta adds exactly one Background bullet
  and reproduces two scenarios byte-identically.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: plan.md:64 and plan.md:149; `vs-adapter/pushdown-planning/spec.md:90` and `:110`;
  `datafusion-scan/scan-execution-file-metadata/spec.md:13-15`
- Issue: All four artifacts instruct the reader to retain "three empty-table-root `SHALL` clauses",
  but only two of the three named items carry an RFC-2119 keyword. In
  `specs/datafusion-scan/scan-execution-file-metadata/spec.md` the two scenario clauses at `:43` and
  `:66` are `SHALL` clauses; the third item — the Background bullet at `:19-20` — reads "When the
  common spec carries an empty table root, every entry is treated as absolute and none are joined",
  with no normative keyword. A reader counting `SHALL` clauses finds two and cannot tell whether the
  third was already lost. The delta's own gloss ("the Background bullet on empty-root handling and
  the final clause of both scenarios") identifies the right three items, so only the label is wrong.
- Fix: Replace "three empty-table-root `SHALL` clauses" with "three empty-table-root clauses — two
  normative `SHALL` clauses and one descriptive Background bullet" at plan.md:64 and plan.md:149,
  `vs-adapter/pushdown-planning/spec.md:90` and `:110`, and
  `datafusion-scan/scan-execution-file-metadata/spec.md:13-15`.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: `vs-adapter/pushdown-planning/spec.md:111` (final scenario clause); plan.md § Scenario
  Coverage; plan.md task 1, harness note 4; decision-log.md decision [8]
- Issue: The revision promoted the omitted-key wire shape from a Background statement (what round 1's
  option (b) asked for) into a normative scenario clause: "a `loadTable` body that OMITS the
  `location` key SHALL also be rejected as a `UdfError::User`, at deserialization rather than by this
  guard, and MUST NOT be reported by any message that substitutes the `warehouse`". No task
  implements it, no test covers it, and § Scenario Coverage maps the scenario to one test that task 1
  requires to use the key-present-but-empty body. The clause is true today only because
  `iceberg-0.10.0` declares `location: String` non-`Option`; a v4-capable iceberg-rust release makes
  the field optional and the `SHALL` becomes false with nothing failing — the same dependency-version
  sensitivity round 1's unactioned `[COMPLETENESS_GAP]` flagged from the other direction. Task 1's
  harness note 4 argues against exercising the shape ("a test that omits it would assert the wrong
  error and would pass before task 2, voiding the failing-test-first gate"), which is true of the
  task-2 TDD test but not of a separate characterization test asserting the deserialization error;
  the note conflates the two. Decision [8] also accepts the diagnostic gap with no scheduled
  follow-up, where this repo's convention for a deliberately-accepted gap is a GitHub issue cited
  inline in the spec (the `(#27)` pattern in
  `specs/datafusion-scan/scan-execution-field-id-projection/spec.md`).
- Fix: Add a second host unit test to plan.md task 1 —
  `omitted_table_location_key_fails_catalog_response_parsing`, serving the same fixture with the
  `"location"` key deleted and asserting a `UdfError::User` whose message names the unparseable
  catalog response and contains no `warehouse` value — and add its § Scenario Coverage row mapped to
  the clause at `vs-adapter/pushdown-planning/spec.md:111`. State in task 1 that this test is a
  characterization test outside the task-2 TDD gate, and correct harness note 4 to scope its
  objection to the task-2 failing test alone. If the test is declined instead, amend decision [8] to
  name the iceberg-rust-upgrade trigger and cite a tracked GitHub issue inline in the clause.

## Design Depth

No objection — axis checked. The change still introduces no module, interface, or boundary, so the
Quick Diagnostic table is legitimately skipped. Guard placement is unchanged from round 1's verified
finding: `resolve_file_list` remains the single seam every location-dependent path crosses once, and
decision [2]'s blast-radius argument against `load_table_any_auth` is confirmed by that function's
signature — it returns `iceberg_catalog_rest::LoadTableResult` to both `resolve_file_list` and
`resolve_table_schema` (`crates/lakehouse-catalog/src/session.rs:234-251`), so a presence check there
would fail a `createVirtualSchema` over a field that path never reads. The new delta adds a spec
owner, not a code boundary. The one tactical shortcut — the accepted diagnostic gap on the omitted-key
shape — is covered by the Task Breakdown advisory above rather than repeated here.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md task 1, harness note 1; plan.md task 1, first sentence
- Issue: Two citations land off their target, in a task tagged `[expert]` whose harness notes are the
  implementer's only guidance. Note 1 reads "`resolve_load_table_prefix` short-circuits the
  `/v1/config` lookup on that path (`session.rs:395-457`)", but that range is the test
  `sigv4_resolve_prefix_derives_catalogs_segment` (doc comment `:395-406`, `#[tokio::test]` at
  `:407`); the short-circuit itself is `if let CatalogAuth::Sigv4 = auth { return
  glue_catalog_prefix(warehouse); }` at `crates/lakehouse-catalog/src/session.rs:148-151`. The same
  range is also cited in § Patterns for a different purpose (the loopback-fake precedent at `:407`),
  so one range now stands for two claims. Separately, task 1's first sentence cites the existing
  `#[cfg(test)] mod tests` at "line 822"; `mod tests` is at
  `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs:823`. Both underlying claims are
  true — only the addresses are wrong.
- Fix: In plan.md task 1, change harness note 1's citation from `session.rs:395-457` to
  `session.rs:148-151` (the SigV4 short-circuit branch), adding "proved by the paired test at
  `session.rs:407`" if the test reference is wanted. Change the task's opening `mod tests` citation
  from line 822 to line 823.

#### [PROSE_BLOAT] ADVISORY
- Location: decision-log.md decision [8] § Alternatives and § Rationale; decision-log.md § Review
  Findings, first entry (Finding and Direction change)
- Issue: Prose written this round breaks the 25-word sentence cap in governed decision-log Rationale
  and Finding text. Decision [8]'s third alternative runs 41 words in one sentence ("Third —
  decisive — issue #296 does not ask for it: … not the omitted-key wire shape it never raises"); its
  Rationale's closing sentence runs 28. The first Review Findings entry's second Finding sentence
  runs 47 words, stacking four source citations into one clause chain, and its Direction change opens
  with a 45-word sentence. This does not re-raise round 1's `[PROSE_BLOAT]` advisory, which the
  orchestrator carries separately — these are newly authored sentences.
- Fix: In decision-log.md decision [8], split the "Third — decisive" sentence into two: one stating
  that #296 does not ask for the omitted-key shape, one stating that the issue defines "absent" as
  its own `table_s3_location.is_empty()` condition. Split the Rationale's closing sentence at the
  semicolon. In § Review Findings' first entry, break the 47-word Finding sentence into one sentence
  per wire shape and move the four source citations into a trailing sentence, and split the Direction
  change's opening sentence at "and a `vs-adapter/pushdown-planning` delta Background bullet".

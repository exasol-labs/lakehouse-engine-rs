# Plan Review Findings: add-direct-storage-catalog-kind (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 7 (Blockers: 3, Advisory: 4)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Round-1 Blocker Recheck

All 7 round-1 BLOCKERs are resolved. Each was verified against the current artifact text rather
than against the planner's report.

- Resolved: [SCOPE_REDUCTION] GROUP BY and two-table join coverage dropped from the parity smoke
  test. `e2e-harness/direct-storage-e2e-properties/spec.md` § Projection, filter, and LIMIT reach
  the direct-storage scan now carries a GROUP BY clause (line 69) and a two-table INNER equi-join
  clause (line 70), its GIVEN names a second fixture directory sharing a join key (line 64), and its
  Background states why both shapes belong in the smoke test (lines 23 to 26). `plan.md`
  § Scenario Coverage gains the two rows at lines 370 and 371, and task 6.4 names
  `group_by_aggregate_matches_the_unpushed_answer` and
  `two_table_join_resolves_both_legs_through_one_store`. The reinstated join clause carries a
  separate precision defect, raised below as a NEW finding rather than as an unresolved one.
- Resolved: [UNSTATED_ASSUMPTION] The scan-spec tag vocabulary is silently lossy for the new
  producer. `datafusion-scan/type-mapping/spec.md` gains the DELTA:NEW scenario "The scan-spec tag
  vocabulary covers every Arrow type the compatible-type classifier admits" (lines 59 to 69), which
  names `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `LargeUtf8`, and `Timestamp` at
  every `TimeUnit`, requires a round trip for each, and forbids the string fallback for an admitted
  type. `datafusion-scan/scan-execution-field-id-projection/spec.md` supersedes the recorded
  twelve-tag enumeration in its Background (lines 25 to 32) and in the DELTA:CHANGED round-trip
  scenario (lines 53 and 57). `vs-adapter/direct-storage-table-discovery/spec.md` line 63 now
  requires an inexpressible folded type to fail the enumeration naming the column and the type.
  Task 3.5 and the two § Scenario Coverage rows at lines 351 and 352 exist. The named eight cases
  match `compatible_exasol_type` (`crates/lakehouse-engine/src/types/mapping.rs` lines 46 to 79)
  exactly. One recorded spec outside the plan still contradicts the widening, raised below as a NEW
  finding.
- Resolved: [COMPLETENESS_GAP] Three tracked-exception citations named the wrong issues. `plan.md`
  § Design Non-Goals now reads "Unity Catalog PARQUET-table routing (#409), non-Iceberg Glue tables
  (#410)", and decision [16] matches. Both titles were re-verified against the tracker: #409 is
  "feat(vs-adapter): route Unity Catalog PARQUET tables to the Parquet reader" and #410 is
  "feat(vs-adapter): read non-Iceberg Glue tables". A search over all 200 most recent issues finds
  none tracking a file-count or file-size limit, which matches the plan's untracked-gap statement.
  `plan.md` § Impact states both untracked gaps, and
  `vs-adapter/direct-storage-table-planning/spec.md` line 61 records the absence of any bound as an
  explicit exception.
- Resolved: [COMPLETENESS_GAP] Two deltas contradicted each other about `load_table`.
  `vs-adapter/direct-storage-table-discovery/spec.md` line 37 now scopes the parity claim to
  `list_tables`, line 38 states that `load_table` is unreachable for this kind, and line 39
  requires a clear error naming the kind with no panic and no synthesized table. Task 4.2 names
  that behavior and § Scenario Coverage line 334 names
  `load_table_returns_a_clear_error_naming_the_direct_storage_kind`.
- Resolved: [AMBIGUOUS_REQUIREMENT] The seam's return shape was unspecified under sample-one-file
  mode. `vs-adapter/parquet-directory-seam/spec.md` line 80 requires the metadata to be paired with
  the files whose footers the mode read and absent for every other listed file, line 81 forbids
  positional indexing, and line 48 scopes the no-re-read promise to the fold-every-file mode. Task
  2.5 restates the paired-and-absent shape.
- Resolved: [REQUIREMENT_CONFLICT] The overflow-asymmetry claim contradicts the recorded
  type-relaxation spec. `datafusion-scan/type-relaxation/spec.md` Background bullet 1 now says the
  read behaviour gains exactly one addition, bullet 2 supersedes the recorded sentence at
  `specs/datafusion-scan/type-relaxation/spec.md` lines 69 to 72, and the amended scenario carries
  the supersession normatively at lines 60 and 61, including the statement that the emit boundary's
  `safe: true` policy stays unreachable.
- Resolved: [AMBIGUOUS_REQUIREMENT] Deriving the widening pair list from its owner is impossible.
  `datafusion-scan/type-relaxation/spec.md` line 70 now requires `supported_relaxation_pairs` to
  stay the concrete pin, line 71 forbids generating either from the other, and line 72 fails the
  suite for a table row with no owner rule. The 17-entry count was verified against
  `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` lines 22 to 71. Task 2.1 and decision
  [5] match.

The 7 round-1 ADVISORY findings were also checked and all 7 are addressed. The `s3a` rationale sits
in decision [12], the mixed timestamp-unit limitation sits in task 6.7,
`stale_declaration_decides_the_emitted_width` sits in task 6.3, the tag round-trip requirement sits
in `vs-adapter/catalog-crate-public-surface-extensions/spec.md` lines 21 and 22, the admission-cap
revisit sits in `plan.md` § Impact, the three leaked Background lines are gone or made load-bearing
by `vs-adapter/direct-storage-properties/spec.md` line 67, and no em dash, semicolon, contraction,
or "behaviour" spelling remains in governed prose outside tables.

## Premortem

Two failure stories for the revised plan, each routed into the taxonomy below.

1. **The E2E binary cannot create its own virtual schema.** `incompatible/` sits under
   `s3://warehouse/direct/`, and its scenario requires every enumeration of that root to fail. Four
   other scenarios create a virtual schema over that same root and require it to succeed. The suite
   is unbuildable as specified, and the author discovers this only after writing the fixtures.
   Routed to `[REQUIREMENT_CONFLICT]` below.
2. **A Delta table's `byte` column silently changes tag.** The vocabulary widening adds `int8` and
   `int16` entries. A recorded Delta spec states the vocabulary has no such entry and pins that
   `byte` and `short` reuse `int32`. An implementer reading the merged library finds two answers and
   picks one. Routed to `[REQUIREMENT_CONFLICT]` below.

## Intent Fidelity

[no objection — axis checked: every item of issue #407 traces to a delta. The user-facing property
table, the CONNECTION rejection list, the first-level-directory discovery rules, the one shared
footer seam with its two consumers, the crate placement, the one shared `LimitStore(16)`, the
`TableFormat::Parquet` and `ScanSource::DirectParquet` planning shape, the concurrent join-leg
resolution, the declaration-wins-on-width rule, the documented Iceberg or Delta directory caveat,
all eight named fixtures, the four property and CONNECTION cases, the five-operation parity smoke
test, and the single Azure `abfss` table are each present. The interview answer that full
`MERGE_SCHEMA` widening ships in this plan is honored, so the issue's fallback seam is correctly
unused.]

## Feasibility

#### [AMBIGUOUS_REQUIREMENT] BLOCKER
- Location: `specs/_plans/add-direct-storage-catalog-kind/e2e-harness/direct-storage-e2e-properties/spec.md` § Scenario: Projection, filter, and LIMIT reach the direct-storage scan, GIVEN (line 64) and the join clause (line 70), with `plan.md` task 6.4
- Issue: the reinstated join clause cannot be implemented or asserted as written, for two separate reasons. First, the GIVEN names "a second fixture directory whose rows share a join key with it" and stops there. Every other fixture in this plan carries a concrete path, a concrete file count, and concrete columns: `s3://warehouse/direct/events/`, `.../nested/A/p1.parquet`, `.../widened/`, `.../missing_col/`, `.../incompatible/`, `.../complex/`, and `s3://warehouse/direct_discovery/orders/`. This one carries no path, no name, no column list, and no join-key column, and no task in § Implementation Tasks creates it, so two implementers build two different fixtures and neither can be reviewed against the spec. Second, the clause requires the test to prove "BOTH legs resolved through the ONE shared admission-limited object store that request opens". That property is not observable from Exasol SQL, which is the only vantage an E2E test in `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` has. The test named `two_table_join_resolves_both_legs_through_one_store` would therefore pass while asserting only row equality, which is the vacuous-test shape. The store-sharing property already has two unit owners: `vs-adapter/pushdown-format-neutral-resolution` § One catalog session per request serves every table the request resolves (`one_session_or_store_per_request_serves_every_leg`) and `vs-adapter/direct-storage-table-discovery` § One admission-limited object store serves every table of one adapter call (`one_admission_limited_store_serves_the_whole_call`).
- Fix: In `e2e-harness/direct-storage-e2e-properties/spec.md` § Projection, filter, and LIMIT reach the direct-storage scan, replace "a second fixture directory whose rows share a join key with it" in the GIVEN with a named fixture directory carrying a concrete object path, a stated file count, and a stated join-key column that matches a named column of the `events/` fixture. Rewrite the join clause so its live assertion is over what Exasol shows: the returned rows equal the equivalent unpushed join, and the `EXPLAIN VIRTUAL` output carries ONE pushdown request holding both legs. State in the same clause that the one-shared-store property is owned by the two unit scenarios named above rather than asserted here, and rename the test in task 6.4 and in § Scenario Coverage so its name states what it checks. Add the new fixture directory to task 6.4's enumerated work.
- Escalation: MECHANICAL. The plan's own deltas name the two unit owners of the store property, and naming a fixture is a spec edit.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `specs/_plans/add-direct-storage-catalog-kind/e2e-harness/direct-storage-e2e/spec.md` § Scenario: A column no widening rule folds fails the refresh naming the column and the files, against § Scenario: A directory of mixed-type Parquet files is declared and queried end to end, § Scenario: A column typed narrowly in one file and widely in another returns every row, § Scenario: MERGE_SCHEMA FALSE declares the sampled footer's type on the refresh and the scan path, and § Scenario: A Delta table directory read as raw Parquet returns its tombstoned rows
- Issue: the fixture layout makes four scenarios unsatisfiable. The incompatible-pair scenario places its fixture at `s3://warehouse/direct/incompatible/` and requires that "`CREATE VIRTUAL SCHEMA` or `REFRESH VIRTUAL SCHEMA`" over it "SHALL FAIL", and that the adapter "MUST NOT ... return a partial table". Enumeration under this kind walks every first-level directory of the base path, per `vs-adapter/direct-storage-table-discovery` § A first-level directory under the base path is a table. The mixed-type scenario creates "a virtual schema created over `s3://warehouse/direct/`" and requires it to declare `EVENTS`, `NESTED`, and the three `complex/` columns. The widening scenario, the `MERGE_SCHEMA = 'FALSE'` scenario, and the Delta-caveat scenario each create a virtual schema over that same root. Every one of those creations enumerates `incompatible/` and must therefore fail. The suite as specified cannot create a single working virtual schema. The properties feature already avoids this by rooting its own fixtures at `s3://warehouse/direct_discovery/`, so the isolation rule exists in the plan but is not applied here.
- Fix: In `e2e-harness/direct-storage-e2e/spec.md`, move the incompatible-pair fixture out of the shared root: give it its own base path, `s3://warehouse/direct_incompatible/incompatible/`, and state in that scenario's GIVEN that the virtual schema is created over `s3://warehouse/direct_incompatible/` so the failing enumeration cannot block the other scenarios' virtual schema. Add a Background bullet stating the rule that a fixture whose scenario requires a failed enumeration MUST sit under a base path no passing scenario shares. Name the two base paths explicitly in the GIVEN of the mixed-type, widening, `MERGE_SCHEMA = 'FALSE'`, and Delta-caveat scenarios instead of writing "that base path", and name the same shared root in the parity scenario of `e2e-harness/direct-storage-e2e-properties/spec.md`. Reflect the second base path in task 6.3.
- Escalation: MECHANICAL. The plan's own deltas settle both the enumeration rule and the fixture paths.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `specs/_plans/add-direct-storage-catalog-kind/datafusion-scan/type-mapping/spec.md` § Scenario: The scan-spec tag vocabulary covers every Arrow type the compatible-type classifier admits, against the recorded `specs/vs-adapter/delta-type-mapping/spec.md` (Background line 186 and the normative clause at line 258)
- Issue: the vocabulary widening contradicts a recorded spec that no delta in this plan supersedes. The recorded Delta spec states as fact: "**`byte` and `short` reuse the existing `int32` tag rather than adding `int8`/`int16` tags.** The compact tag vocabulary shared by `arrow_type_to_tag`/`arrow_type_from_tag` in `crates/lakehouse-engine/src/types/mapping.rs` has no `int8` or `int16` entry ... while a new tag would touch the shared classifier every format reads." Its scenario carries the normative form: "*AND* `byte` and `short` SHALL both map to the EXISTING `int32` tag, and this feature MUST NOT add an `int8` or an `int16` tag to the shared tag vocabulary, because Exasol gives Int8, Int16, and Int32 the same `DECIMAL(precision, 0)` shape". This plan's task 3.5 adds exactly those two entries. The recorded factual premise becomes false on merge, and the recorded rationale is already wrong against the code: `compatible_exasol_type` returns `DECIMAL(3,0)` for `Int8`, `DECIMAL(5,0)` for `Int16`, and `DECIMAL(10,0)` for `Int32` (`crates/lakehouse-engine/src/types/mapping.rs` lines 50 to 53), so the three do not share one Exasol declaration. `plan.md` § Features lists no `vs-adapter/delta-type-mapping` delta. This is the same defect shape round 1 raised for `datafusion-scan/type-relaxation` and the planner accepted: a merged library holding two contradictory statements about one shared mechanism.
- Fix: Add `specs/_plans/add-direct-storage-catalog-kind/vs-adapter/delta-type-mapping/spec.md` as a CHANGED delta. Give it a Background bullet superseding the recorded "has no `int8` or `int16` entry" premise, stating that `datafusion-scan/type-mapping` widens the vocabulary for the direct-storage producer. Add a DELTA:CHANGED block on the scenario carrying the `byte` and `short` clause that keeps Delta's `byte` and `short` mapping to the `int32` tag unchanged, restates the reason as the Delta reader's own deliberate widening rather than as the absence of a tag, and states that no Delta declared Exasol type and no Delta emitted value changes. Add the feature row to `plan.md` § Features, name the delta in group C's Knowledge column, and name it in task 3.5.
- Escalation: MECHANICAL. The recorded spec library and `types/mapping.rs` settle both the contradiction and the corrected rationale.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `specs/_plans/add-direct-storage-catalog-kind/e2e-harness/direct-storage-e2e/spec.md` § Scenario: A directory of mixed-type Parquet files is declared and queried end to end, and `plan.md` § Scenario Coverage row `every_classifier_admitted_arrow_type_round_trips_through_the_tag_vocabulary`
- Issue: the vocabulary widening is pinned by a unit round trip alone, and no fixture reads any newly covered type. The `events/` fixture carries "a 64-bit integer, a string, a date, a timestamp, a decimal, a boolean, and a double", which is the pre-existing twelve-tag set. None of `Int8`, `Int16`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `LargeUtf8`, or a millisecond-unit `Timestamp` appears in any fixture. The failure the round-1 blocker described is a Spark-written `INT64 TIMESTAMP(MILLIS)` column declared `VARCHAR(2000000)` and read back as a string, and that exact column is still never read end to end. A tag round trip proves the renderer and the parser agree. It does not prove the declared Exasol type, the registered DataFusion type, and the emitted value agree for the eight newly covered types.
- Fix: Extend the `events/` fixture in `e2e-harness/direct-storage-e2e/spec.md` § A directory of mixed-type Parquet files is declared and queried end to end with a millisecond-unit timestamp column and an `INT(8, true)` column, and add a clause requiring both to declare their mapped Exasol types (`TIMESTAMP` and `DECIMAL(3,0)`) and to return their written values rather than a JSON string. Name the two added columns in task 6.3.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: `specs/_plans/add-direct-storage-catalog-kind/plan.md` § Verification Scenario Coverage lines 346 and 372, against § Implementation Tasks and § Parallelization
- Issue: two named tests are built by no task. `capabilities_are_identical_under_three_kinds` in `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` covers the `pushdown-format-neutral-resolution` capability scenario, and that file appears in no task and in no group's Knowledge column, so group E has no mandate to touch it. `direct_storage_binary_provisions_from_the_shared_harness` covers the `e2e-harness` scenario, and neither task 6.2, 6.3, nor 6.4 names it. This is the same defect round 1 raised for `stale_declaration_decides_the_emitted_width`, which the planner fixed by naming it in a task.
- Fix: Add `capabilities_are_identical_under_three_kinds` to task 5.6's enumerated test list and add `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` to group E's Knowledge column in `plan.md` § Parallelization. Add `direct_storage_binary_provisions_from_the_shared_harness` to task 6.3's enumerated test list.

## Design Depth

#### [TACTICAL_SHORTCUT] ADVISORY
- Location: `specs/_plans/add-direct-storage-catalog-kind/plan.md` § Impact (lines 132 to 140) and § Checklist, with `vs-adapter/direct-storage-table-planning/spec.md` line 61
- Issue: the one obligation the plan calls mandatory has no owner and no gate. § Impact reads "Two gaps ship UNTRACKED, and one new issue covering both MUST be opened before this plan merges." No task opens it, no § Checklist row checks it, and the spec clause that will carry the citation reads "Its tracking issue SHALL be opened before this plan merges and cited inline in this clause". Once that delta merges into `specs/vs-adapter/direct-storage-table-planning/spec.md`, the clause states a plan-local merge condition that the recorded library cannot express and that no reader can act on, and the exception it records still cites no issue. The plan's largest stated operational risk therefore rests on the same memory the round-1 finding removed it from.
- Fix: Add a `plan.md` § Checklist row "Follow-up issue" whose command is `gh issue list --search "direct-storage file-count limit"` and whose expected result is one open issue, so the obligation is checked where every other pre-merge gate is checked. Reword `vs-adapter/direct-storage-table-planning/spec.md` line 61 so the recorded clause states the exception and its issue citation as a permanent fact, and move the "opened before this plan merges" wording out of the spec and into `plan.md` § Impact alone.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: `specs/_plans/add-direct-storage-catalog-kind/plan.md` § Impact line 135 and § Implementation Tasks bullets 2.1, 2.5, 3.5, and 6.7
- Issue: five passages break two `/speq:writing-guardrails` caps. The § Impact sentence "Nothing refuses a table directory holding hundreds of thousands of files, so the plan-time footer read named in the caution above is unbounded at `CREATE VIRTUAL SCHEMA` and at every plan" runs 29 words and joins two ideas with "so", against the 25-word descriptive cap and the one-idea rule. Task bullets 2.1, 2.5, 3.5, and 6.7 each run past 60 words and each carry three or more instructions in one sentence, against the 20-word procedural cap and the one-instruction rule. Task 3.5 alone states the widening, the eight named types, the doc-comment edit, the byte-identical requirement, and two test assertions in a single sentence.
- Fix: In `plan.md` § Impact, split line 135's first sentence into two: state that nothing refuses a directory holding hundreds of thousands of files, then state that the plan-time footer read is therefore unbounded at `CREATE VIRTUAL SCHEMA` and at every plan. In tasks 2.1, 2.5, 3.5, and 6.7, split each bullet into one imperative sentence per instruction, keeping every named symbol, type, and file path unchanged.

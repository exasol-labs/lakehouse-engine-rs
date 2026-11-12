# Plan Review Findings: add-export-iceberg-append (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 10 (Blockers: 2, Advisory: 8)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Round-1 Blocker Recheck

- Resolved: [INTENT_DRIFT] an optional table column that the export lacks is filled with NULL. The spec's Binding bullet says "Every table column, required or optional, must be bound", and the third mismatch kind is "a table column that no source column binds". The mismatch scenario's source "lacks both columns", the required one and the optional one. Task 2.2 expects `note` in the error. Interview Q5 and the `[plan-review] Every table column must be bound` entry record the requester's choice.
- Resolved: [SCOPE_REDUCTION] a source with no rows skips every mismatch check. The spec Background now states the gap. The GIVEN of the empty-source scenario is "a query that returns no rows and whose columns do not match the table", and task 3.1 `empty_source_appends_nothing` matches it. Interview Q6 and the review entry record the acceptance.
- Resolved: [SCOPE_REDUCTION] format-version 1 and 3 tables are refused without agreement. plan.md § Out of Scope names both versions and cites the requester (Interview Q7).
- Resolved: [UNSTATED_ASSUMPTION] task 1.7 allows a fake-server test to close a coverage gap. Task 1.7 now allows only unit tests of pure functions, names a decision to extract, and says "Add no fake server and no host test that spans modules".
- Resolved: [COMPLETENESS_GAP] three Iceberg spec quotes are cut short or rest on an unverified claim. I checked each quote against `format/spec.md` on apache/iceberg main. The Sorting sentence is now complete. "Omitting a known field when writing a data file is never allowed." and "The write default for a field must be written if a field is not supplied to a write." appear verbatim. The Parquet field-ID sentence matches except for its link. The row-lineage reason is gone, and line 38 states versions 1 and 3 as a scope limit.
- Resolved: [REQUIREMENT_CONFLICT] the recorded "one winner" scenario no longer holds once appends exist. plan.md line 62 adds a CHANGED scenario whose GIVEN says both statements pass the pre-read check while the table does not exist. Line 64 adds the append sentence, and task 4.2 changes docs line 87.
- Resolved: [TRACEABILITY_GAP] the plan cites (#TBD) although issue #567 is open. The spec description and plan.md § Summary cite (#567). The open question is deleted, and no "TBD" remains in the plan files.
- Resolved: [ARCHITECTURE_DRIFT] the plan changes the architecture, but no architecture delta records it. `architecture.md` exists with BASE `d66175d2e63a6bd647976470a7a65e31dac52b9f`. I diffed its four CHANGED blocks against `specs/architecture.md` at cc728feb, the stack top when the plan was written. Each block is the whole section. The only edited lines are the export component, three Data Flow bullets, the exported-table Interfaces bullet (now three bullets), and the `SINGLE_COMMIT` constraint. `speq plan validate add-export-iceberg-append` passes. The stack top has moved since then (see the [HIDDEN_DEPENDENCY] advisory below).

## Premortem

Six months from now this plan failed. These are the likely reasons:

1. The implementer runs the coverage gate against `origin/main` on #610's branch. The diff holds 9,037 inserted Rust lines from #607 to #610, so the 90% figure measures the stack. The implementer adds unit tests to stacked files to pass the gate, and the next stack fix conflicts with them. Category: [UNSTATED_ASSUMPTION], coverage finding.
2. A parallel append into a Spark table reads a NULL in `id` in one of four writers. Exasol aborts the statement while two other writers are still writing. The operator trusts the spec ("the writers that read no such NULL commit their rows"), expects three writers' rows, and finds one writer's rows and orphan files. Category: [AMBIGUOUS_REQUIREMENT], NULL-bullet finding.
3. A DBA reads `docs/security.md`, which says a direct call skips "the check for an existing table". The DBA concludes that the export role cannot change existing tables and grants it widely. A role holder then appends into a production table that Spark owns. Category: [NFR_IGNORED], security-docs finding.
4. Spark gives `spark_target` a default spec with id 0, or the fixture script runs after the INT96 script and writes INT96 timestamps. The E2E test passes without exercising the spec-id fix of task 1.5. The first append into an evolved Spark table fails with "Data file partition spec id does not match table default partition spec id". Category: [TRACEABILITY_GAP], fixture-guard finding.

## Intent Fidelity

No objection. Axis checked: the plan applies the three interview answers as given. Every table column must be bound (spec Binding bullet and the third mismatch kind). The zero-row gap is accepted and stated (spec Background, empty-source scenario). Format version 2 only (plan.md § Out of Scope). The #567 asks map to scenarios: name binding, widening, one error for every mismatch, `SINGLE_COMMIT` with one commit or none, and E2E tests for tables created by an export and by another engine. Decision [5] records the record-time deltas that depart from the literal Interview Q1 answer.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER: the coverage gate measures the stacked PRs' lines, not this plan's
- Location: plan.md § Verification › Checklist, row "Coverage" (line 170); plan.md task 1.7
- Issue: The Checklist runs "the changed-line check against `origin/main`", and task 1.7 acts on its result. Group A runs on #610's branch before the stack merges (§ Dependencies). The changed-line script diffs from `git merge-base <base> HEAD`. Against `origin/main` it therefore counts every line of #607 to #610: `git diff --shortstat origin/main...bc43ebbc -- '*.rs'` reports 66 files and 9,037 inserted lines before this plan adds any. The 90% figure then mostly measures stacked code. A low figure pushes the implementer to add tests to stacked files, which goes against the requester's constraint that fixes to the stacked PRs stay unaffected. A high figure can hide an uncovered append path.
- Fix: In plan.md § Verification › Checklist, change the Coverage row to pass `origin/docs/export-iceberg-lakekeeper-e2e-and-specs` as the base ref of the changed-line check, and state that the check uses `origin/main` only after task 5.1 rebases the branch onto main. Name the same base ref in task 1.7.
- Escalation: MECHANICAL. The script's base-ref argument and the branch layout in § Dependencies settle it.

#### [NFR_IGNORED] ADVISORY: docs/security.md still describes an export role that cannot change existing tables
- Location: plan.md § Impact, § Dependencies table, task 4.2; stack-top `docs/security.md` line 131 (§ Who can bypass the export script); stack-top `docs/index.md` line 33
- Issue: After this plan, a holder of the export role can use a plain `EXPORT` to append to every table that the CONNECTION's catalog identity may commit to, including tables that other engines own. `docs/security.md` line 131 says that a direct `SELECT` call skips "the check for an existing table", and its third bullet presents an append to a table that an export created as a bypass. Both statements become wrong. The pre-read hook no longer refuses an existing table. The writer re-runs `appendable` and the binding, so a forged `append` flag skips no check. § Impact lists the breaking change for jobs, but not the wider write scope of the role. `docs/index.md` line 33 still says the export writes "into a new Iceberg table". Task 4.2 and § Dependencies list neither file. The repo's local test-strategy preference requires `docs/security.md` to state the known bypasses correctly.
- Fix: Add these changes to task 4.2. In `docs/security.md` § Who can bypass the export script, remove the existing-table check from the list of skipped checks. Replace the third bullet with what a direct call can still do. Add one sentence that the export role appends to any table that the CONNECTION's identity may commit to. In `docs/index.md` line 33, write "into a new or an existing Iceberg table". Add both files to § Dependencies. Add the wider scope of the role as a bullet in plan.md § Impact.

#### [HIDDEN_DEPENDENCY] ADVISORY: the stack top moved after the plan was written, so the BASE no longer matches #610's branch
- Location: plan.md § Sequencing With the Stacked PRs, line 59; `architecture.md` BASE line, Components block (lakehouse-catalog-write bullet), and Constraints block
- Issue: `origin/docs/export-iceberg-lakekeeper-e2e-and-specs` advanced to bc43ebbc, which merges main #596 and fix 909d0107 into the stack (commit date 2026-11-11 08:30 +0100). Its `specs/architecture.md` now hashes to `91b9e53cc72d9f7aad895972e8d40648cd0f6a8b`, not `d66175d2`. That commit rewrote the lakehouse-catalog-write bullet in Components and added a pushdown-planner bullet to Constraints. The delta's CHANGED blocks still hold the old text, so recording them unchanged would revert both. `IcebergRestTarget::session()` in `iceberg_rest.rs`, a seam of task 1.5, also changed (`ResolvedIdentity`, `WriteStorage::of(.., role)`). Task 5.1 already copies the blocks again from main, and the rebase rule in § Dependencies covers the source, so the design holds. The sentence on line 59 ("the hash of `specs/architecture.md` on #610's branch") is no longer true.
- Fix: Either copy the four blocks again from bc43ebbc now and set the BASE to `91b9e53cc72d9f7aad895972e8d40648cd0f6a8b`, or reword plan.md line 59 to name commit cc728feb as the source of the BASE. Leave task 5.1 unchanged.

#### [NFR_IGNORED] ADVISORY: parallel commits into a table with the default retry count stay unproven (carried from round 1, declined)
- Location: plan.md tasks 2.2, 3.1, and 4.1
- Issue: The planner declined the 16-writer append into `spark_target`. Task 4.1 now documents the default of 4 retries, and that `SINGLE_COMMIT` or fewer writers avoid the contention, which lowers the risk. No test runs parallel commits into a table that uses Iceberg's default of 4 retries. A writer that runs out of retries still fails the statement after other writers have committed, and a rerun duplicates their rows. The decline is acceptable as an advisory.
- Fix: No change is required. To get the proof, add a 16-writer append into `spark_target` to task 2.2 that asserts every row appears exactly once.

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] [IMPLEMENTATION_LEAKAGE] BLOCKER: the NULL bullet promises that the other writers commit, which no scenario tests and the export cannot guarantee
- Location: `export/export-iceberg-append/spec.md` § Background, NULL bullet (line 33), second sentence
- Issue: The sentence reads "Without `SINGLE_COMMIT`, the writers that read no such NULL commit their rows, so the failed export leaves them in the table". It states a fixed outcome. When one writer fails, Exasol fails the statement, and a writer that has not committed by then may never reach its commit. The cited ADR `export-has-no-final-commit-per-writer-commits` says only that a failed parallel export "can leave some writers' commits in the table". No scenario step depends on the sentence, so it also breaks the Background rule. The fact that round 1 wanted on record is already recorded elsewhere. `architecture.md` § Constraints says "a failed parallel export can leave some writers' commits in the table", and task 4.1 gives accurate docs wording: "a failed append keeps the rows of the writers that committed".
- Fix: Delete the second sentence of the NULL bullet in spec.md § Background. Keep the wording of task 4.1 for the docs.
- Escalation: MECHANICAL. The ADR, the architecture delta, and the spec's own scenarios settle it.

#### [COMPLETENESS_GAP] ADVISORY: the failed SINGLE_COMMIT scenario does not name the trigger that its data-file clause needs
- Location: spec.md § Scenario "A failed SINGLE_COMMIT append leaves the table unchanged", WHEN (line 100); plan.md task 2.2 `failed_single_commit_append_leaves_table_unchanged`, third case
- Issue: The WHEN names a source error at row 500 and a required NULL. In both cases the writer fails before it writes a file, because 1,000 rows stay below `BATCH_ROWS` (8,192). The clause "the data files that the writer wrote SHALL NOT remain" then holds vacuously. The third case of task 2.2, a manifest write that gets HTTP 403 after the data file, proves the clause, but no scenario describes it. specs/testing.md § Coverage rule says "An E2E suite holds no requirement of its own". The recorded create scenario names this trigger: "or the writer fails after it has written its data files and before it commits".
- Fix: Add "or the writer fails after it has written its data files and before it commits" to the WHEN of this scenario, as export/export-iceberg-table does.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY: the Spark append fixtures have no shape guard, and task 2.1 skips four specs/testing.md fixture rules
- Location: plan.md tasks 2.1 and 2.2; plan.md § Dependencies table
- Issue: These gaps remain in the fixture tasks:
  - The spec-id fix of task 1.5 is proven only if the default spec of `spark_target` is unpartitioned and its id is not 0. No test checks this. No test checks that `id` is required, that the schema has a field-ID gap, that `spark_v1` is format version 1, or that `spark_partitioned` has a partition field. specs/testing.md § Correctness oracles uses a fixture guard so that a row test "cannot pass vacuously", and says "When it is uncertain whether a fixture reproduces a defect, the suite establishes it by observation and fails saying so."
  - `run_fixtures.sh` must list the new script before `create_int96_timestamp_fixture.sql`, because the INT96 script's SETs switch the session's Parquet writer (testing.md § Fixtures). Task 2.1 does not say where the script goes.
  - testing.md § Fixtures lists every Spark fixture. Task 2.1 adds four tables and no entry to that list.
  - Tests append to `spark_target` on every run. The plan replaces the rule "A fixture that a test mutates also has its own base path, which the test empties before it writes the initial state" (testing.md § Fixtures) with assertions relative to the state before each export. The reason holds, because a test cannot reset a table that Spark authored, but no artifact records the deviation.
  - `crates/lakehouse-e2e/tests/common/mod.rs` needs a `mod append_fixtures` line, and § Dependencies does not list the file.
- Fix: In task 2.2, add a fixture guard. It reads each table's metadata through the REST catalog and asserts the format version, the default spec id and its fields, the required `id`, and the field-ID gap. On a mismatch it fails with a message that names the fixture. In task 2.1, put the new script before the INT96 script. Add a task that adds the append fixtures to specs/testing.md § Fixtures, with a note that their tests assert relative to the state before each export. Add `tests/common/mod.rs` to § Dependencies.

#### [TRACEABILITY_GAP] ADVISORY: the racing-exports test now depends on timing
- Location: plan.md line 62; stack-top `crates/lakehouse-e2e/tests/e2e_lakekeeper/export_write.rs` `export_racing_exports_leave_one_winner`
- Issue: Before this plan, the test passed no matter which statement ran its pre-read check later, because the hook refused the later one. After this plan, a statement whose pre-read check runs after the other statement's writer created the table appends to it and succeeds, so the test sees two winners. The test relies on its cross join taking seconds ("so both statements pass the pre-read check"). No task touches the test, and its check on the loser's error does not show that the loser ran in create mode.
- Fix: In task 3.2, make `export_racing_exports_leave_one_winner` assert that the loser's error carries the new `existing_table_error` text from task 1.1 ("was created by another statement during this export"). The test then proves that the loser lost the create race.

Task granularity and parallelization: no objection. One implementation group shares one spec delta and one module tree. Group R waits for the stack to merge.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY: binding.rs must render Iceberg type names from Arrow types
- Location: plan.md task 1.2 ("The error renders the table column's type by its Iceberg name, such as `long`, `decimal(12,2)`, `float`, or `struct`"); `architecture.md` § Components, export bullet
- Issue: `binding::bind` takes the table's Arrow schema from `sink.schema()`, which lies on the format-neutral side of the `ExportSink` seam. To name Iceberg types, it must map Arrow types back to Iceberg names, so Iceberg knowledge enters the module that is meant to be target-neutral. The reverse map also loses information: the pinned iceberg-rust maps both `uuid` and `fixed[16]` to `FixedSizeBinary(16)` (`arrow/schema.rs` lines 770-773). The Iceberg sink already holds the Iceberg schema (`iceberg_sink.rs` line 76).
- Fix: Have the sink supply a display type name for each table field next to its Arrow schema, for example through a method on `ExportSink`. Compute the names in `iceberg_sink.rs` from the Iceberg schema, and pass them to `bind`. Otherwise, render the Arrow type, and drop the Iceberg-name requirement from task 1.2 and from the second Manual Testing row.

ADR checks: no objection. All five Design Decisions entries say `Promotes to ADR: no`. I found no `[ADR_CONFLICT]`. The plan keeps per-writer commits, the commit loop's tag re-check, and retries owned by each operation (ADRs `export-has-no-final-commit-per-writer-commits`, `export-commit-loop-rechecks-writer-tag`, and `state-changing-catalog-requests-retried-by-their-operation`). ADR `an-iceberg-column-binds-only-by-field-id-or-the-name-mapping` governs the scan, and the export still writes field IDs, so the export's name binding does not contradict it. I found no `[ARCHITECTURE_DRIFT]`. The delta covers every component, data flow, interface, and constraint change that plan.md states, and the new modules `binding.rs` and `iceberg_append.rs` sit inside the export component that the delta changes.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY: the partition spec has two names, and one sentence is hard to read
- Location: spec.md § Background line 9 and line 37, § Scenario "A table the export cannot append to is refused before any data is read" (line 86); `architecture.md` § Interfaces; plan.md § Context bullet 5 and task 1.5
- Issue: The spec and the architecture delta say "current partition spec". § Context and task 1.5 say "default partition spec" and "default spec id". Iceberg calls the field `default-spec-id`. A reader cannot tell whether the two names mean different things. Line 37 ends with "This is this feature's scoped exception."
- Fix: Use "default partition spec" in spec.md lines 9 and 86 and in `architecture.md` § Interfaces. Rewrite the last sentence of spec.md line 37 as "Not sorting is this feature's scoped exception."

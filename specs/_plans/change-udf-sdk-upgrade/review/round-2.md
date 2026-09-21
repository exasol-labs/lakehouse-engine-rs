# Plan Review Findings: change-udf-sdk-upgrade (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 12 (Blockers: 2, Advisory: 10)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

- **Resolved: `[UNSTATED_ASSUMPTION]` task 5.2 could not tell a stripped echo from an SLC rejection.**
  The prescribed mechanism exists and is checkable, not reworded. `plan.md` task 5.2 carries step
  `(p)` before every value assertion, and the two pieces it depends on are real.
  `isolated_pushdown_statement` is at `crates/lakehouse-engine/tests/common/e2e_harness.rs:348` and
  returns the `PUSHDOWN_SQL` cell of `EXPLAIN VIRTUAL`. That cell carries the `EMITS` clause, which
  `crates/lakehouse-engine/tests/e2e_scan_test.rs:4060` and
  `crates/lakehouse-engine/tests/e2e_emit_declaration_test.rs:101` already assert against.
  `exasol_type_from_json` (`crates/lakehouse-engine/src/types/mapping.rs:684-688`) renders
  `format!("TIMESTAMP({p})")` for every present `p`, including `3`, so step `(p)`'s
  `TIMESTAMP(3)` literal for sub-step (b) fails only on a stripped echo and not on a formatting
  special case. `spec.md:79-83` now scopes the captures and names 2025.1.16 an assumption.
  The STOP condition is correctly gated on `(p)` passing first. The residual defect is that step
  `(p)` confirms `[C2]` only, which is raised below as a new BLOCKER.
- **Resolved: `[REQUIREMENT_CONFLICT]` a delta clause required keeping a test the plan turns red.**
  `datafusion-scan/type-mapping-timestamp-precision/spec.md:199` no longer says "recorded test
  coverage unchanged". It requires the assertion "REPLACED by a per-precision one, asserting
  `Millisecond` for `TIMESTAMP(0)`, `Microsecond` for `TIMESTAMP(6)` and `Nanosecond` for
  `TIMESTAMP(9)`". `plan.md` task 2.5 names
  `exasol_type_to_arrow_parses_timestamp_precision` (`types/mapping_tests.rs:469-474`) with the same
  three assertions, and § Dead Code Removal carries a row for the old one. The recorded test body is
  confirmed as the fixed `let expected = Some(DataType::Timestamp(TimeUnit::Microsecond, None));`
  asserted for `TIMESTAMP(0)`, `(6)` and `(9)`, so the replacement targets the right lines. The
  three assertions agree with clause `:191`'s `0..=3 / 4..=6 / 7..` binning and with task 2.1's
  `from_declared_digits`. No conflict remains between the delta, the task, and the binning rule.
- **Resolved: `[CLUSTER_INCOHERENCE]` group B claimed a file its crate could not compile.**
  The § Parallelization table now lists group B as `4.1-4.2` with `crates/vs-expression` Knowledge
  only, and group C as `4.3, 5.1-5.6` depending on A and B, with
  `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` and the
  `sql-comprehension/vs-expression-translator-cast` delta added to its Knowledge cell. The
  concurrency paragraph states the compile window explicitly. The independence claim for group B
  checks out: `crates/vs-expression/src/lib.rs:11` is the crate's only `exasol_udf_sdk` import and it
  names `error::UdfError` alone. Task 4.3's dependency on group B's decline is satisfied by C
  depending on B. The relocation did not move the problem: `pushdown_tests.rs` appears in no other
  group's Knowledge cell, and the target file already carries wrapper-shape assertions (11 `LHS_T0`
  occurrences) for task 4.3 to match.

## Premortem

Three fresh ways this plan fails after the round-1 revision.

1. **A correct design is still sent back, one step later.** Step `(p)` passes on 2025.1.16 because
   the echo is present. The engine then accepts `EMITS (... TIMESTAMP(9))` syntactically and clamps
   it semantically, exactly as `[C1]` and `[C3]` measured on 8.29.13. The query raises no error and
   returns `COUNT(DISTINCT) == 2`. Task 5.2 tells the implementer this is an SLC rejection and the
   design needs revision. The design is fine and the engine build is old.
2. **The spec's nanosecond claims are narrowed by a wrong-stack run.** Task 5.3 tears down the
   2025.1.16 stack and brings up 8.29.13. Task 5.4 runs next with no reset and asserts a
   `TIMESTAMP(9)` declaration. The 8.x clamp declares bare `TIMESTAMP`, the assertion fails, and task
   5.4's failure branch reverts the fixture and narrows the delta's nanosecond claims for a reason
   that was never the fixture.
3. **A permanent spec ships wrong navigation.** The four code locators round 1 measured as wrong are
   unchanged in `sql-comprehension/vs-expression-translator-cast/spec.md` and in `plan.md` § Design.
   A later reader follows `support.rs:1221` to a blank line and `sql_builders.rs:990` to the wrong
   function while trying to re-derive the decline routing.

## Intent Fidelity

No new objection. The six interview answers are all operationalized: the live E2E task exists
(tasks 5.1-5.6), the emit follows the source precision (decision `[1]`), the four-variant collapse is
in scope (task 2.2 and the new `TIMESTAMP(9)` scenario), the CAST is declined rather than verified
(decision `[5]`, tasks 4.1-4.3), and `plan.md` carries no `#411` reference and no `tracked as`,
`defer` or `follow-up` deferral. The round-1 fixes introduced no reinterpretation of the ask.

#### [SCOPE_CREEP] ADVISORY (carried from round 1, unfixed)
- Location: `specs/_plans/change-udf-sdk-upgrade/notes/planning.md:65`
- Issue: the user constrained `#411` to historical decision-log entries. The hand-off note still
  reintroduces it outside that scope at `:65`.
- Fix: Delete the `#411` reference from `notes/planning.md:65` and point at `decision-log.md`
  § Interview, which already records the decision.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: `plan.md` § Implementation Tasks task 5.2;
  `datafusion-scan/type-mapping-timestamp-precision/spec.md:79-83`
- Issue: step `(p)` reads the ADAPTER's generated SQL. That SQL is built from Exasol's pushdown echo,
  so it confirms `[C2]` and nothing else. It cannot confirm `[C1]`, which is the engine ACCEPTING
  AND HONORING a declared precision. The delta bullet at `:79-83` claims otherwise: "That `[C1]` and
  `[C2]` hold there is an ASSUMPTION, not a stated fact. Group C confirms it before it asserts any
  emitted value, by reading the generated SQL". The unconfirmed half is not hypothetical. `[C1]` in
  `specs/_recorded/2026-08-19-add-timestamp-precision-versioning/decision-log.md` records 8.29.13 as
  "accepted and SILENTLY DOWNGRADED. Identical DDL returned `ok`, both `TS6` and `TS9` reported
  `TIMESTAMP(3)`. 8.x neither rejects the field nor honors it", and `[C3]` records that the same
  build takes `TIMESTAMP(6)` as an EMITS type "accepted syntactically and clamped to milliseconds
  semantically". An accept-and-clamp on the unmeasured 2025.1.16 build passes step `(p)`, raises no
  error, and returns `COUNT(DISTINCT) == 2`. Task 5.2 then prescribes the wrong conclusion: "Only a
  failure that occurs AFTER step (p)'s `EMITS` assertion has passed for that width counts as the
  STOP condition. Such a failure is an SLC rejection, and the design, not the implementation, then
  needs revision." A second cause also escapes the prescribed wording. Task 5.2 requires step `(p)`
  to fail "with a message naming a stripped `fractionalSecondsPrecision` echo on `2025.1.16` as the
  cause", but `[C3]`'s `0A000 Feature not supported` rejection of a `TIMESTAMP(9)` CAST target would
  fail the `EXPLAIN VIRTUAL` behind step `(p)` for a different reason.
- Fix: In `plan.md` task 5.2, replace "Such a failure is an SLC rejection, and the design, not the
  implementation, then needs revision" with a three-way discriminator. State that a UDF or SQL ERROR
  raised after step `(p)` passes is the SLC rejection and is the only STOP. State that a query which
  SUCCEEDS but returns fewer distinct values than the declared width admits is the engine accepting
  the declaration and clamping it, the behaviour `[C1]` and `[C3]` already measured on 8.29.13, and
  MUST be reported as a 2025.1.16 engine limit rather than a design STOP. Widen step `(p)`'s
  prescribed failure message to name both a stripped `fractionalSecondsPrecision` echo and a
  `0A000 Feature not supported` rejection of the `TIMESTAMP(p)` CAST target as candidate causes. In
  `datafusion-scan/type-mapping-timestamp-precision/spec.md:79-83`, scope the confirmation sentence
  to `[C2]` alone, and add that `[C1]`'s accept-and-honor behaviour on 2025.1.16 is confirmed by the
  VALUE assertion instead, whose failure is an engine limit rather than an SLC rejection.

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: `plan.md` § Implementation Tasks tasks 5.3, 5.4 and 5.6; § Checklist row "Test (E2E, 8.x)"
- Issue: task 5.3 runs `docker compose down -v` and brings the stack up on
  `exasol/docker-db:8.29.13`. Task 5.4 follows with no reset and asserts "the column is declared
  `TIMESTAMP(9)` on 2025.1.16 and that both values survive distinct". On the 8.29.13 stack the engine
  clamp declares that column bare `TIMESTAMP` and the scan emits `Timestamp(Millisecond, None)`, so
  two values differing only below the microsecond collapse and the assertion fails. Task 5.4's
  failure branch then misreads the cause: "On failure, record the exact error from iceberg-rust or
  the `apache/iceberg-rest-fixture:1.10.1` catalog, revert the fixture change, and narrow the delta's
  nanosecond claims". A wrong-stack failure therefore narrows the permanent spec for a reason that
  was never the fixture. Task 5.4 also carries no `live_engine_version` guard, unlike task 5.2(a)
  and (c), and `crates/lakehouse-engine/tests/common/timestamp_precision.rs:52` shows that guard is
  available. Without it, task 5.6's requirement that "every assertion tasks 5.2 and 5.4 added is
  green on both engine legs" and § Checklist's "Test (E2E, 8.x) ... 0 failures" are both
  unsatisfiable, because a `TIMESTAMP(9)` declaration cannot hold on the clamped arm.
- Fix: In `plan.md` § Implementation Tasks, move task 5.4 so it runs between tasks 5.2 and 5.3, while
  task 5.1's 2025.1.16 stack is still up, and state that order in the task line itself the way task
  4.3 states its group. Add to task 5.4 the `live_engine_version` guard task 5.2(a) uses, so the
  nanosecond declaration and distinctness assertions run on the `>= 2025` arm only and the 8.29.13
  leg asserts the clamped bare `TIMESTAMP` instead. Restrict task 5.4's failure branch to a SEEDING
  failure by naming the iceberg-rust or catalog error as its only trigger, and state that an
  assertion failure on a non-2025 engine is not that branch. Reword task 5.6 to require task 5.4's
  guarded assertions green on their own arm rather than "on both engine legs".

#### [HIDDEN_DEPENDENCY] ADVISORY (carried from round 1, unfixed)
- Location: `plan.md` § Parallelization group C; § Dependencies
- Issue: the assumption the whole design rests on, that the SLC's strict Arrow-IPC feed accepts a
  `Timestamp(Millisecond, None)` and a `Timestamp(Nanosecond, None)` column at all, is still checked
  only after all thirteen tasks of group A land. No cheaper source-level check was added, although
  the SLC's block reader is public source in `language-container-rs` v0.28.1, and `plan.md` contains
  no `language-container-rs` reference.
- Fix: Add a task to group A, before task 3.1, reading `language-container-rs` v0.28.1's Arrow-IPC
  block reader and recording in `decision-log.md` which Arrow `TimeUnit`s it accepts for an Exasol
  `TIMESTAMP` output column. Note in § Parallelization that this is source evidence only and does not
  replace task 5.2's live measurement.

## Requirement Quality

The round-1 `[REQUIREMENT_CONFLICT]` is gone and the four deltas validate: `speq plan validate
change-udf-sdk-upgrade` reports "validation passed", 4 deltas, warnings on AND-step counts only.

#### [COMPLETENESS_GAP] ADVISORY (carried from round 1, unfixed)
- Location: `datafusion-scan/type-mapping-timestamp-precision/spec.md` DELTA:NEW Background and
  `:199`; `plan.md` tasks 2.3 and 2.5
- Issue: task 2.3 requires `exasol_type_to_arrow` to treat "a bare `TIMESTAMP` as `3`" and task 2.5
  now adds a unit assertion for it ("plus a bare `TIMESTAMP` case asserting `Millisecond`), but no
  delta clause governs that case. Clause `:199` enumerates `TIMESTAMP(0)`, `(6)` and `(9)` only, and
  the deleted Background bullet at `:16` was the one place stating that Exasol's bare `TIMESTAMP` is
  `TIMESTAMP(3)`. The bare declaration is the 8.x arm, so the ungoverned case is the default path on
  one of the two measured engines.
- Fix: Add a Background bullet to the DELTA:NEW block of
  `datafusion-scan/type-mapping-timestamp-precision/spec.md` restating that Exasol's bare
  `TIMESTAMP` is `TIMESTAMP(3)`, and extend clause `:199` with a sentence requiring a bare
  `TIMESTAMP` type string to resolve to the same unit as `TIMESTAMP(3)`.

#### [COMPLETENESS_GAP] ADVISORY (carried from round 1, unfixed)
- Location: `datafusion-scan/type-mapping-timestamp-precision/spec.md:169`; `plan.md` task 3.6 and
  § Verification > Scenario Coverage
- Issue: the clause "a `timestamptz_ns` column SHALL keep its nanosecond digits through that cast on
  an engine declaring `TIMESTAMP(9)`" still has no implementing test. Task 3.6 adds a `coerce_column`
  case for a `Timestamp(Nanosecond, None)` column only. The ZONED case the clause names is untested,
  and § Verification maps the `timestamptz` scenario to `iceberg_types_map_to_exasol_type`, a
  declaration test that never reaches the emit boundary.
- Fix: Extend `plan.md` task 3.6 with a second `coerce_column` case covering a
  `Timestamp(Nanosecond, Some("UTC"))` column declared `TIMESTAMP(9)`, asserting the result is
  `Timestamp(Nanosecond, None)` with all nine digits and the same instant. Add the matching row to
  § Verification > Scenario Coverage.

#### [UNSTATED_ASSUMPTION] ADVISORY (carried from round 1, unfixed)
- Location: `datafusion-scan/type-mapping-timestamp-precision/spec.md:198`; `plan.md` task 2.3 and
  § Dead Code Removal
- Issue: clause `:198` still justifies the change "because it is documented as the single source of
  truth for the Arrow type the strict `emit_batch` feed accepts". `exasol_type_to_arrow` has no
  production call site. Every workspace reference is a doc comment (`types/mapping.rs:67`, `:373`) or
  a test in `types/mapping_tests.rs`. The real emit-path owner is `target_arrow_type`
  (`scan/emit.rs:230-251`). Recording the false role permanently mis-states where the emit decision
  lives.
- Fix: In `datafusion-scan/type-mapping-timestamp-precision/spec.md:198`, replace "the single source
  of truth for the Arrow type the strict `emit_batch` feed accepts" with a statement that
  `exasol_type_to_arrow` is a documented compliance surface with no production call site, kept in
  agreement with `target_arrow_type` so the two cannot drift. Make the same correction in `plan.md`
  task 2.3 and § Dead Code Removal's closing paragraph.

## Task Breakdown

Group C's structure holds after the relocation. Its tasks share the
`datafusion-scan/type-mapping-timestamp-precision` delta and the live-engine files, and task 4.3
shares the `sql-comprehension/vs-expression-translator-cast` delta with task 5.2(c). The
Knowledge-cell overlap between groups B and C is not a parallelism defect, because C declares a
dependency on B. The traceability of the round-1 fixes checks out: delta clause `:199` maps to task
2.5, and the new `:79-83` assumption bullet maps to task 5.2 step `(p)`.

#### [TRACEABILITY_GAP] ADVISORY (carried from round 1, unfixed)
- Location: `plan.md` task 3.3; § Dead Code Removal
- Issue: `micros_to_naive_datetime` (`crates/lakehouse-engine/src/scan/convert.rs:192`) has exactly
  one caller, `convert.rs:134`, the line task 3.3 replaces. It appears in neither task 3.3 nor
  § Dead Code Removal, so it survives as an unused private function and a clippy warning against the
  § Checklist row requiring "0 errors and 0 warnings".
- Fix: Extend `plan.md` task 3.3 with "delete `micros_to_naive_datetime` (`convert.rs:192`), whose
  only caller this task replaces", and add a matching row to § Dead Code Removal.

#### [TRACEABILITY_GAP] ADVISORY (carried from round 1, unfixed)
- Location: `plan.md` § Parallelization group C Knowledge; tasks 5.2 and 5.4
- Issue: `crates/lakehouse-engine/tests/common/timestamp_precision.rs` is the deliberate independent
  oracle and still carries exactly two arms, `ExpectedTimestampPrecision::MICROSECOND` and
  `::MILLISECOND` (`:35-45`). Tasks 5.2 and 5.4 add nanosecond assertions with no arm to compute
  their expectation from. Group C lists the file as Knowledge but no task changes it, so the
  implementer will either hard-code the nanosecond expectation in the test body, defeating the
  oracle, or call the production rule under test.
- Fix: Add a task to group C, before task 5.2, extending
  `crates/lakehouse-engine/tests/common/timestamp_precision.rs` with a NANOSECOND arm and a
  source-width parameter, keeping its version rule an independent re-implementation rather than a
  call into `EngineTimestampSupport`.

## Design Depth

No new objection. The two-type split is unchanged and still passes the diagnostic: each type answers
one question, `engine.clamp(source).declaration()` is easier to call than the branch it replaces, and
`EngineTimestampSupport` takes a `&str` so the type-mapping module performs no I/O. Moving task 4.3
from group B to group C changed sequencing only and introduced no module, interface, or boundary.

#### [INFORMATION_LEAKAGE] ADVISORY (carried from round 1, unfixed)
- Location: `plan.md` § Design > Decision; tasks 2.1, 2.3 and 3.5
- Issue: the single-owner claim holds for the precision-to-`TimeUnit` table and not for the
  `TIMESTAMP(p)` STRING SYNTAX, which stays replicated across four sites.
  `TimestampPrecision::declaration()` produces it, `exasol_type_to_arrow` parses it,
  `exasol_type_from_json` (`types/mapping.rs:684-688`) produces it independently from Exasol's echo,
  and `declared_type_name` in `tests/scan_fixture/mod.rs` produces it a fourth time. The plan states
  neither the residual replication nor a plan to remove it.
- Fix: Add a bullet to `plan.md` § Design > Decision naming the `TIMESTAMP(p)` string syntax as the
  one decision the split does NOT consolidate, listing the four sites. Either schedule a
  render-and-parse pair beside `TimestampPrecision` that all four read, or record the replication as
  a deliberate, named trade-off in the delta's Background.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY (carried from round 1, unfixed)
- Location: `sql-comprehension/vs-expression-translator-cast/spec.md:26-30` and `:44-47`; `plan.md`
  § Design > Decision routing table
- Issue: four code locators destined for the permanent library are still wrong, and one description
  still mischaracterises the code. `support.rs:1221` is a blank line, the real
  `render_expression_safe` `None` branch is at `support.rs:1396-1399`,
  `build_qualified_single_table_fallback_sql` is at `sql_builders.rs:1032` rather than `:990`, and
  `referenced_column_projection` is at `sql_builders.rs:920` rather than `:1098`. There is no
  "`function_scalar_cast` arm": the pattern at 1361-1382 covers sixteen node types and five other
  conditions also set `needs_full_fallback`. The routing conclusion is correct. Only the navigation
  is not.
- Fix: In `sql-comprehension/vs-expression-translator-cast/spec.md:26-30` and `:44-47`, and in
  `plan.md` § Design > Decision's routing table, correct the four references to
  `support.rs:1396-1399`, `sql_builders.rs:1032`, `sql_builders.rs:920` and
  `sql_builders.rs:1138-1139`, and replace "`project_columns`'s `function_scalar_cast` arm" with
  "`project_columns`'s shared scalar-and-predicate arm, one of six conditions that set
  `needs_full_fallback`".

#### [PROSE_BLOAT] ADVISORY (carried from round 1, unfixed)
- Location: `plan.md` prose lines; `decision-log.md` § Review Findings
- Issue: two guardrail deviations remain. `/speq:writing-guardrails` bans semicolons in governed
  prose, and `plan.md` still carries about eighteen outside tables, including the new task 5.2
  sub-step list at `:409-412`. The user's standing instruction is that the decision log stays concise
  and is not a changelog, and § Review Findings still carries five entries from the superseded
  planning round, each fully retracted by a `Superseded by` line. The three new `[plan-review]`
  entries added this round are terse and are not part of this finding. No em dash appears in any new
  text.
- Fix: Split every semicolon-joined sentence in `plan.md` prose into two sentences. In
  `decision-log.md` § Review Findings, compress each of the five superseded entries to its Finding
  line plus its `Superseded by` line, deleting the `Direction change` paragraphs.

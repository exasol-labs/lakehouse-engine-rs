# Plan Review Findings: change-udf-sdk-upgrade (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 5 (Blockers: 2, Advisory: 3)
- Intent Fidelity blockers: 0

### Premortem

Two failure stories drove this round.

**Story 1.** The plan ships. Task 3.2 runs the suite on 8.29.13, the emit is accepted, the values
round-trip millisecond-truncated, and the spec records a measured SHALL. A year later a reader acts
on that SHALL. The truth is that the SLC reported `precision` 6 for the bare `TIMESTAMP` column all
along, so the declaration was never below the emitted resolution, the precision check was never
stressed, and Exasol's own storage layer did the truncation the spec attributes to the emit
boundary. The round-1 defect survived the fix in a different costume. Routed to
`[UNSTATED_ASSUMPTION]`.

**Story 2.** The implementer runs task 3.1 on 2025.1.16, then exports
`EXASOL_IMAGE=exasol/docker-db:8.29.13` and repeats `docker compose up -d` exactly as task 3.2
states. The `exa-data` named volume still holds the 2025.1.16 data directory, so the 8.29.13
container fails to start or starts against an incompatible `/exa`. The measurement never runs, and
the plan's own § Checklist and § Manual Testing rows repeat the same incomplete command. Routed to
`[HIDDEN_DEPENDENCY]`.

## Round-1 Blocker Recheck

- **Resolved:** `[INTENT_DRIFT]` The CAST probe could not observe the SLC precision check —
  task 3.2 no longer casts. It runs `EXASOL_IMAGE=exasol/docker-db:8.29.13`, where
  `TimestampPrecision::Millisecond.declaration()` returns the bare string `"TIMESTAMP"`
  (`crates/lakehouse-engine/src/types/mapping.rs:322-328`), so no CAST stands between the scan and
  the emit boundary and `snap_timestamp_precision` is not on the path. `exasol_type_from_json`
  renders bare `TIMESTAMP` when `fractionalSecondsPrecision` is absent
  (`crates/lakehouse-engine/src/types/mapping.rs:684-690`), which matches the 8.x pushdown echo the
  same module documents. Route (a) appears nowhere in the plan and is tracked as issue #411, whose
  body scopes it accurately to the above-microsecond CAST arm, names the `snap_timestamp_precision`
  vacuity, and marks itself "Not investigated". The `p` set {0, 3, 6, 9} is gone from task 3.2, and
  the round-1 `retained_at` underflow risk went with it: the oracle calls `retained_at` only at 3
  and 6. Task 3.3's stop condition now fires on rejection OR on any non-millisecond round-trip.
  The `isolated_pushdown_statement` helper the new assertion uses exists as cited
  (`crates/lakehouse-engine/tests/common/e2e_harness.rs:348`).
- **Resolved:** `[INTENT_DRIFT]` The spec delta pre-committed to an arm no task could reach —
  `plan.md:234` no longer carries "edits no spec delta". Group B's Knowledge cell now lists
  `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md`
  and states group B "owns its live-engine clause, which task 3.4 corrects to the measurement".
  Task 3.4 exists, is tagged `[expert]`, and forbids widening the SHALL past a reached arm. The
  delta's clause at `type-mapping-timestamp-precision/spec.md:43` now names exactly the two arms the
  plan runs (8.29.13 bare `TIMESTAMP`, 2025.1.16 `TIMESTAMP(6)`), and `:44` moves the
  above-microsecond arm out of the SHALL into a limitation citing #411 inline, matching this repo's
  `(#27)` tracked-exception convention. The two groups are sequenced, not parallel, so the shared
  delta file is a handoff rather than a write conflict.
- **Resolved:** `[UNSTATED_ASSUMPTION]` "Exasol already owns the truncation" was unscoped — all
  three places are split. `plan.md:63-68` states the Arrow-unit ground, then scopes the truncation
  ground to "the catalog-column bare-`TIMESTAMP` path this plan measures" and states the CAST path's
  truncation "happens inside DataFusion via `snap_timestamp_precision`" and is neither verified nor
  relied on. `decision-log.md:36-43` carries the same split in entry `[1]` § Alternatives. The
  Background bullet at `type-mapping-timestamp-precision/spec.md:25-31` records it in the delta, and
  incidentally answers round 1's `[INFORMATION_LEAKAGE]` advisory as well.

## Intent Fidelity

No objection — axis checked. Both user decisions from this round's interview are executed as given.
Route (a) is absent from every artifact and lives only in issue #411, whose scope matches what the
plan cites it for. Group B is now permitted to edit the one delta clause it measures, and only that
clause. Scope is unchanged otherwise: still one pin move, one enum's call sites, three deltas, one
live measurement. No new work appears without a traceable need, and nothing from issue #405 was
dropped this round.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER

- Location: `plan.md` § Design > Decision (lines 51-54), `plan.md` task 3.2 (line 195),
  `decision-log.md` `[1]` § Decision (lines 31-33),
  `datafusion-scan/type-mapping-timestamp-precision/spec.md:43`
- Issue: "which the SLC resolves to `precision: 3` at the handshake" is the single premise that
  makes task 3.2 non-vacuous, and no task observes it. The plan states it four times as fact.
  `plan.md:53` draws the conclusion directly: "The emitted value is genuinely finer than the
  declaration, so the SLC's new precision check is stressed for real." The spec delta writes the
  premise into a normative SHALL: "on Exasol 8.29.13, where the version gate declares a CATALOG
  timestamp column as a bare `TIMESTAMP` that the SLC resolves to `precision` 3 ... the running
  engine SHALL accept that microsecond Arrow column into the BELOW-microsecond declaration".
  Nothing establishes the value 3. The SDK documents `ColumnInfo` as "Declared metadata of one input
  or output column, as the database reported it in the handshake"
  (`exasol-udf-sdk-0.28.1/src/value.rs:174-176`), and neither `ExaType` nor `ColumnInfo` documents a
  default for a declaration carrying no `(p)`. This repo's only nearby evidence is the E2E oracle's
  note that `SYS.EXA_ALL_COLUMNS` renders a bare `TIMESTAMP` as `TIMESTAMP(3)`
  (`crates/lakehouse-engine/tests/common/timestamp_precision.rs:18-23`), which is virtual-schema
  catalog metadata, not the UDF output-column handshake. The probe cannot close the gap on its own:
  `target_arrow_type` ignores the reported precision, so a reported 6 and a reported 3 produce the
  same emitted block, and Exasol's storage layer truncates to millisecond either way. Every
  assertion task 3.2 lists (`declared_type`, the rendered values, `COUNT(DISTINCT) == 2`, the new
  `EMITS` check) passes identically under both. Task 3.3's stop condition therefore cannot fire on
  the failure mode that matters, and the plan would record as measured the same class of claim round
  1 blocked. The repo already has a precedent for how to settle this kind of upstream contract:
  `crates/lakehouse-engine/tests/e2e_emit_declaration_test.rs:7-13` records that the EMITS-to-
  `output_column` agreement "was proven once, at implementation time, against this same local Exasol
  Docker container (task 1.6)" rather than asserted in a recurring test.
- Fix: In `plan.md` task 3.2, add one sentence: "Observe the `precision` the SLC reports for the
  bare-`TIMESTAMP` output column once, by hand, on the 8.29.13 leg, using
  `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS` with `%udf_debug_level debug` (CLAUDE.md § Live
  debugging), and record the observed value in `decision-log.md` entry `[1]`. If the reported
  precision is 6 or higher, the declaration is not below the emitted resolution, the probe proves
  nothing about the SLC precision check, and the plan STOPS for revision." Add the same stop
  condition to task 3.3's STOP sentence. In
  `datafusion-scan/type-mapping-timestamp-precision/spec.md:43`, keep "SHALL accept ... and SHALL
  truncate each value to millisecond" but delete the embedded clause "that the SLC resolves to
  `precision` 3" and replace it with "whose SLC-reported `precision` task 3.2 records", so the SHALL
  states only what the run observes. In `plan.md` § Design > Decision and `decision-log.md` `[1]`
  § Decision, replace each "resolves to `precision: 3`" with "reports a `precision` this plan
  records rather than assumes".

#### [HIDDEN_DEPENDENCY] BLOCKER

- Location: `plan.md` task 3.2 (lines 190-191), task 3.1 (lines 187-188), § Manual Testing rows 1
  and 2, § Checklist rows "Test (E2E, 2025.x)" and "Test (E2E, 8.x)"
- Issue: the plan switches `EXASOL_IMAGE` between two Exasol major versions on one machine without
  ever resetting the persisted data volume, so the second bring-up starts a different engine over
  the first engine's data directory. `docker-compose.yml:128-129` mounts the named volume
  `exa-data` at `/exa`, which holds the whole Exasol instance including `EXAConf` and the database.
  `make test-e2e` touches Docker not at all (`Makefile:81-82` runs `cargo test` only), so nothing in
  the stated commands recreates that volume. Task 3.2 says only "Repeat the same stack bring-up and
  `make test-e2e` run with `EXASOL_IMAGE=exasol/docker-db:8.29.13` exported to BOTH the
  `docker compose up -d` step and the `make` invocation. No harness change is needed". Task 3.1
  then adds a third lifecycle in the other direction: "Re-run it once more after task 3.2 extends the
  scenario". CI does not hit this, because each matrix leg runs on a fresh runner and ends with
  `docker compose down -v` (`.github/workflows/ci.yml:624`); the plan's local procedure has no
  equivalent step. The same omission is repeated verbatim in § Manual Testing and § Checklist, so
  the defect reaches the operator-facing rows too.
- Fix: In `plan.md` task 3.2, insert before the bring-up sentence: "CAUTION: run
  `docker compose down -v` first. The `exa-data` named volume (`docker-compose.yml:128-129`) holds
  the 2025.1.16 data directory, and an 8.29.13 container started over it does not come up. The
  reset also wipes `minio-data`, so the Spark fixture job and the in-process seed re-run on the next
  `up`." Add the identical `docker compose down -v` requirement to the "Re-run it once more"
  sentence of task 3.1. In § Manual Testing, prefix both
  `datafusion-scan/type-mapping-timestamp-precision` commands with `docker compose down -v &&`. In
  § Checklist, change the "Test (E2E, 2025.x)" and "Test (E2E, 8.x)" command cells to start with
  `docker compose down -v`.

#### [EFFORT_MISESTIMATION] ADVISORY

- Location: `plan.md` task 3.2 (lines 197-204)
- Issue: the task directs one assertion "at the arm the oracle expects", but the oracle carries no
  field holding an `EMITS` declaration string, and its only declaration field is documented as the
  opposite of what the assertion needs. `ExpectedTimestampPrecision`
  (`crates/lakehouse-engine/tests/common/timestamp_precision.rs:17-45`) exposes
  `declared_column_type`, `distinct_count` and `retained_fractional_digits` only, and
  `declared_column_type`'s doc comment reads "The exact `SYS.EXA_ALL_COLUMNS.COLUMN_TYPE` string
  this arm declares — `TIMESTAMP(3)` for the millisecond arm, never bare `TIMESTAMP`". An
  implementer who reuses that field for the new `EMITS` assertion asserts `TIMESTAMP(3)` against a
  pushdown statement that reads bare `TIMESTAMP`, and the 8.x leg fails looking like a real
  regression. The oracle file is in group B's Knowledge cell, so the edit is in scope but unnamed.
- Fix: In `plan.md` task 3.2, add one sentence: "Add a fourth field to
  `ExpectedTimestampPrecision` (`crates/lakehouse-engine/tests/common/timestamp_precision.rs`) that
  carries the `EMITS` declaration string for the arm, bare `TIMESTAMP` on `MILLISECOND` and
  `TIMESTAMP(6)` on `MICROSECOND`, and assert against that field. Do NOT reuse
  `declared_column_type`, which is the `SYS.EXA_ALL_COLUMNS` rendering and is never bare."

Checked and no objection on the rest of the feasibility surface. The 8.x image row added to
§ Dependencies verifies exactly as written: `Makefile:3` defines `EXASOL_IMAGE ?=
exasol/docker-db:2025.1.16` and exports it at `:8`, `docker-compose.yml:115` reads it, and
`.github/workflows/ci.yml:523` is precisely the `- image: exasol/docker-db:8.29.13` matrix entry.
`isolated_pushdown_statement` already runs against the 8.29.13 CI leg through
`e2e_credential_exposure_test`, which `Makefile:82` includes, so its 4-column `EXPLAIN VIRTUAL`
layout requirement is not a new risk. `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision`
is a real test (`crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs:121`), replacing
round 1's fabricated name in § Verification row 2.

## Requirement Quality

No objection — axis checked. `speq plan validate change-udf-sdk-upgrade` passes with three AND-step
count warnings only (6/8/8), the same style the recorded library already carries. The
`DELTA:CHANGED` scenario title in `type-mapping-timestamp-precision/spec.md:37` still matches the
recorded title byte for byte, so the replacement covers the recorded clause it must supersede. The
new clause at `:44` is a scoping limitation, not a behaviour, and it cites #411 inline per this
project's tracked-exception convention with a body that matches its citation. No conflict arose
between the delta clauses group B now owns (`:43`, `:44`) and the clauses group A authored (`:41`,
`:42`, `:45`-`:47`), nor with `scan-execution-value-conversion/spec.md:45`, which states the same
"`Timestamp { precision }` to `Timestamp(Microsecond, None)` at EVERY precision" rule and cites this
feature for it. The two non-timestamp deltas are unchanged this round.

## Task Breakdown

#### [TASK_GRANULARITY] ADVISORY

- Location: `plan.md` task 3.1 (lines 187-188) and task 3.2
- Issue: task 3.1 contains a step that must run after task 3.2, so the numbering understates the
  work. "Re-run it once more after task 3.2 extends the scenario, so the extended test is green on
  both engine legs" makes the real order 3.1 on 2025.1.16, then 3.2 on 8.29.13, then 3.1 again on
  2025.1.16. That is three stack lifecycles across two engine versions, not the two the task list
  implies, and it is the same straddle that hides the volume-reset dependency raised above. A task
  that cannot be verified as one unit should be split.
- Fix: In `plan.md` § Implementation Tasks group 3, move task 3.1's final sentence into a new task
  numbered 3.5, tagged `[expert]`: "Re-run the 2025.1.16 leg after task 3.2 extends the scenario, so
  the extended test is green on both engine legs. Reset the stack first per task 3.2's CAUTION."
  Delete that sentence from task 3.1. Update group B's Tasks cell in § Parallelization from
  `3.1-3.4` to `3.1-3.5`.

Checked and no objection on the rest of the breakdown. Every delta still has an implementing task,
and task 3.4 is the implementing task for the delta clause group B owns, with no overlap against
3.1-3.3: 3.1 and 3.2 run the two engine legs, 3.3 writes `decision-log.md`, 3.4 writes the delta.
The group split survives the round-1 fixes: the two groups run in sequence, group A owns every
source and unit-test file, and the one artifact they now share is named explicitly along with the
single clause that is shared. Group B's Knowledge cell lists every file tasks 3.1-3.4 touch.

## Design Depth

No objection — axis checked. The change still introduces no new module, interface or boundary, so
the Quick Diagnostic table does not apply. The round-1 `[INFORMATION_LEAKAGE]` observation is now
recorded in the library rather than only in review: the Background bullet at
`type-mapping-timestamp-precision/spec.md:25-31` names which component owns the truncation on each
declaration path and states that the live claim rests on the catalog path alone. Task 3.4's
authority is scoped to one clause of one scenario, so group B gains no ownership of a decision group
A holds. No tactical shortcut is scheduled without a follow-up: the one deferral, the
above-microsecond CAST arm, carries issue #411.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY

- Location: `plan.md` task 3.2, lines 201 and 202
- Issue: the round-1 revision introduced two new em dashes into task 3.2, which
  `/speq:writing-guardrails` bans outright: "declares the probed timestamp column at the arm the
  oracle expects — bare `TIMESTAMP` with no `(p)` on the millisecond arm, `TIMESTAMP(6)` on the
  microsecond arm — so the scenario fails rather than passes vacuously". The sentence also runs past
  the 25-word descriptive cap.
- Fix: In `plan.md` task 3.2, replace that clause with two sentences: "Assert that the generated
  statement's `EMITS` list declares the probed timestamp column at the arm the oracle expects. The
  millisecond arm declares bare `TIMESTAMP` with no `(p)`, and the microsecond arm declares
  `TIMESTAMP(6)`."

Checked and no objection on the rest of the new prose. The rewritten § Design > Decision paragraph,
decision-log entry `[1]` § Alternatives, task 3.4, and the three `[plan-review]` entries read
cleanly on one pass, use active voice, and name their actors. Round 1's two `[PROSE_UNCLEAR]`
advisories (task 1.2's census sentence, § Parallelization's "prices" clause) and its
"behaviour"/"behavior" and § Goals em-dash advisories remain open and unchanged, as the workflow
allows.

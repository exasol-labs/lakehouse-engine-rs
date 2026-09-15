# Plan Review Findings: remove-emit-exa-types (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 6 (Blockers: 1, Advisory: 5)
- Intent Fidelity blockers: 0

### Premortem

Two failure stories drove this round's fresh pass.

1. **The `Numeric`-to-`Utf8` escape hatch added in round 2 fails the other emit path.** The
   value-conversion delta now routes a `Numeric` column with an absent or out-of-range payload to
   `Utf8`. On the Arrow path that is admitted. On the `Value` partial-aggregate path the same rule
   produces `Value::String` into a `Numeric` column, which `column_accepts` rejects, giving exactly
   the `output column … is … but the value is …` error the partial-aggregate scenario promises
   never happens. Routes to `[REQUIREMENT_CONFLICT]`.
2. **An operator reads § Impact and underestimates the untested surface.** Round 2 restated
   mismatch 4 on the `NESTED_AGGREGATE_PLAN_TYPE` evidence, which added a third unit-only
   mismatch. § Impact still says "Two of the four" and never names mismatch 4, while § Verification
   says three. Routes to `[COMPLETENESS_GAP]`.

### Claims verified this round, not disputed

Checked independently against the repo and against `language-container-rs` at tag `v0.26.0`
(commit `1fe3d27`, the released SDK/SLC the plan pins). Round 2 does not re-open these.

- All three `UdfContext` impls task 1.5 names exist at the cited lines:
  `crates/lakehouse-engine/tests/scan_fixture/mod.rs:81` `BatchCapturingCtx`,
  `crates/lakehouse-engine/src/scan/emit_tests.rs:66` `CapturingCtx`,
  `crates/lakehouse-engine/src/scan/test_support_tests.rs:82` `SinkCtx`. No fourth impl exists.
- `TestContext::with_output_columns` exists (`crates/exasol-udf-sdk/src/test_support.rs:134`), so
  task 2.1's mechanism is real.
- `output_column_count()` defaults to `0` and `output_column`/`input_column` default to
  `UdfError::Unimplemented` (`crates/exasol-udf-sdk/src/context.rs:11-26`), so task 1.5's premise
  holds and § Load-bearing assumptions row 1's stop condition is checkable.
- The `ExaType` enum (`crates/exasol-udf-sdk/src/value.rs:158`) carries exactly the 15 variants the
  value-conversion scenario partitions, `Unsupported` included, and `Numeric`'s `precision`/`scale`
  are `Option<u32>`.
- `NESTED_AGGREGATE_PLAN_TYPE` is the literal `"DOUBLE PRECISION"` at
  `crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg.rs:28`, applied at `:57` through
  `plan_types.push(declared.unwrap_or_else(...))`. Mismatch 4's restatement is decided by repo code
  alone, as the revision claims.
- `column_accepts` at `v0.26.0` (`crates/exa-udf-runtime/src/rowset.rs:1325-1347`) reads exactly as
  plan.md § Context states.
- The `emit_exa_types` census is complete: 5 production construction sites (`pushdown/mod.rs` x2,
  `pushdown/support.rs`, `joins/sql_builders.rs` x2), 2 production reads
  (`joins/sql_builders.rs`), and every test file holding the literal appears in task 3.1, 3.4, or
  4.2.
- `tests/micro_bench.rs` calls `coerce_batch_to_exa_types` at three sites and carries no feature
  gate, as task 2.9 states.
- `speq plan validate remove-emit-exa-types` passes.

## Round-1 Blocker Recheck

- Resolved: [SCOPE_REDUCTION] The E2E drift scenario. Resolved per the user's ruling, not the
  round-1 Fix text. `e2e-harness/e2e-harness-scan-correctness/spec.md` no longer carries the
  "proves agreement, which cannot be asserted by comparing two lists" bullet or the
  `emit_batch`-rejection detector clause; the scenario is renamed "The scan returns correct values
  across the type mix with no spec-carried emit types" and keeps its value-correctness and
  `emit_exa_types`-absence clauses. Task 1.6 is unchanged in substance (Docker container,
  temporary `udf_log!`, deleted after use, `WARNING` halting the plan) and gains a paragraph stating
  that no permanent test replaces it. plan.md § Verification maps acceptance criterion 4 to task 1.6
  alone and states the reason. Task 4.1 forbids building a permanent comparison.
- Resolved: [SCOPE_CREEP] Partial-aggregate bundling. Resolved per the user's ruling, not the
  round-1 Fix text. plan.md § Summary lines 12-18 record that #399's "Not in scope" note predates
  the 0.26.0 release that bundled PR #105's row validation, that the note no longer holds, and that
  bundling under #399 was an explicit user decision. `Closes #399` is named as the only trailer.
  The partial-agg delta's Background carries the same statement.
- Resolved: [HIDDEN_DEPENDENCY] Task 1.5's test-double census. All three impls are named with
  paths verified against the repo, each with its shape (`BatchCapturingCtx` forwards to an inner
  `TestContext`; `SinkCtx` and `CapturingCtx` are hand-rolled and need their own bodies;
  `CapturingCtx` must gain a field carrying the declared columns). § Parallelization group A's
  Knowledge column now lists `test_support_tests.rs` and `emit_tests.rs` and no longer
  `raw_scan_tests.rs`.
- Resolved: [UNSTATED_ASSUMPTION] PR #105's fixture coverage. The claim is corrected in all three
  places: `datafusion-scan/scan-execution-value-conversion/spec.md` lines 27-32, plan.md § Design
  § Context lines 33-36, and § Load-bearing assumptions row 1, whose Evidence cell now opens
  "Partial." Each states that the fixture covers a re-registered STATIC `EMITS` list and that the
  dynamic call-site form is proven separately against the local Docker container.
- Resolved: [UNSTATED_ASSUMPTION] Mismatch 4's evidence. The partial-agg delta's Background and the
  scenario's GIVEN and AND steps now carry the `NESTED_AGGREGATE_PLAN_TYPE` form, verified above.
  decision-log.md [2] names the evidence per mismatch and records why the `TRUNC`/`SIGN` form was
  dropped. Task 2.8 carries the restated case. No `TRUNC`/`SIGN` claim survives anywhere in the
  plan.
- Resolved: [COMPLETENESS_GAP] `ExaType::Numeric` with an absent payload. The clause exists at
  `datafusion-scan/scan-execution-value-conversion/spec.md:64` and task 2.7 sweeps both the absent
  and the out-of-range case. The rule it states is correct for the Arrow path, and its cross-path
  consequence is raised fresh below under Requirement Quality.
- Resolved: [TRACEABILITY_GAP] Task 4.3's claim. Task 4.3 now claims only the
  `AVG`/`STDDEV`-over-integer-and-decimal case, states that the wide-decimal `SUM` and the
  `MIN`/`MAX` over `decimal(p,0)` are not reachable from these fixtures, and keeps "No new seed
  fixture" as a non-goal. plan.md § Impact and § Verification both carry a coverage-split paragraph,
  and § Scenario Coverage's integration row names the one mismatch it reaches. The split's
  arithmetic is off by one, raised fresh below.

## Intent Fidelity

[no objection — axis checked: issue #399's four acceptance criteria each map to a task (1.2; 2.3
and 2.4; 3.1 and 3.2; 1.6 per the user's ruling). Both round-1 intent blockers were settled by
explicit user rulings and the revision applies them as stated. No new task adds work outside
#399's scope, and nothing in #399's Scope section is dropped.]

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks task 2.4
- Issue: task 2.4 says "resolve the declared columns from `ctx` once, before the batch loop".
  `output_column` returns a borrow, `fn output_column(&self, idx: usize) -> Result<&ColumnInfo,
  UdfError>` (`crates/exasol-udf-sdk/src/context.rs:24`), while the emit calls take `&mut self`
  (`context.rs:32` `fn emit(&mut self, ...)`), and `emit_stream` holds `ctx: &mut dyn UdfContext`
  (`crates/lakehouse-engine/src/scan/emit.rs:42-48`). A list of `&ColumnInfo` resolved before the
  loop cannot survive the first `emit_batch` call, so the task as written does not compile. The
  cost is one `clone` per column per call, which the task should state rather than leave the
  implementer to rediscover.
- Fix: In plan.md task 2.4, state that the resolved list is owned: clone each `ColumnInfo.typ` into
  a `Vec<ExaType>` (both types derive `Clone`) before the batch loop, because `output_column`
  returns a borrow of `ctx` and every emit call takes `&mut ctx`.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks task 4.1; plan.md § Verification § Checklist row "Test"
- Issue: task 4.1 adds `crates/lakehouse-engine/tests/e2e_emit_declaration_test.rs` without naming
  the feature gate every sibling end-to-end file carries. `tests/e2e_scan_test.rs:19` opens with
  `#![cfg(feature = "exasol-e2e")]`, and `exasol-e2e = []` is declared at
  `crates/lakehouse-engine/Cargo.toml:12`. Without the gate the new DB-backed test compiles into
  the plain `cargo test` run that § Checklist requires to report 0 failures, and it FAILS rather
  than skips without a live stack, per the project's own fail-not-skip rule.
- Fix: In plan.md task 4.1, require the new file to open with `#![cfg(feature = "exasol-e2e")]`,
  matching `tests/e2e_scan_test.rs:19`, and state that it runs under `make test-e2e`, not under
  plain `cargo test`.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `datafusion-scan/scan-execution-value-conversion/spec.md` § Scenarios, the clause
  "a `Numeric` column whose reported `precision` or `scale` is absent, or whose reported
  `precision` or `scale` falls outside the range `Decimal128` accepts, SHALL map to `Utf8` rather
  than failing the call, because the SDK's own row check admits a string-family value for a
  `Numeric` column"; `datafusion-scan/scan-execution-partial-agg/spec.md` § Scenarios, the clause
  "the emitted `Value` variant SHALL be one the SDK's `column_accepts` rule admits for that
  declared column"; plan.md § Design § Patterns row "One coercion rule, two emit paths"
- Issue: the two deltas give contradictory rules for one input, and the stated evidence names the
  wrong function. Two separate SDK checks govern the two emit paths. On the Arrow path the guard is
  `is_string_family_exatype` (`language-container-rs` v0.26.0
  `crates/exa-udf-runtime/src/rowset.rs:663`), which does admit `ExaType::Numeric`, so
  `(DataType::Utf8, Numeric)` builds a `ColAccessor::Utf8` at `rowset.rs:605`. On the `Value` path
  the check is `column_accepts` (`rowset.rs:1325-1347`), whose only `Value::String` arm covers
  `String`, `Char`, `Geometry`, `HashType` and the two `Interval` variants. A `Numeric` column
  rejects `Value::String`. Task 2.6 routes the partial-aggregate columns through the same declared
  `ExaType` coercion and then `arrow_value_at`, whose catch-all arm
  (`crates/lakehouse-engine/src/scan/convert.rs:145-152`) turns a `Utf8` column into
  `Value::String`. So for a payload-less or out-of-range `Numeric` partial-aggregate column the
  shared rule produces exactly the `output column … is … but the value is …` failure the
  partial-agg scenario forbids, and decision [4] has removed the fallback that would have absorbed
  it. The clause's justification, "the SDK's own row check admits a string-family value for a
  `Numeric` column", is false of the row check and true only of the Arrow accessor guard, and this
  wording is headed for the permanent spec library.
- Fix: In `datafusion-scan/scan-execution-value-conversion/spec.md`, scope that clause to the
  `emit_batch` Arrow path and replace its justification with the accurate one: the Arrow feed's
  string-family guard admits a `Utf8` column for a `Numeric` declaration
  (`language-container-rs` v0.26.0 `crates/exa-udf-runtime/src/rowset.rs:663`). Add one clause to
  `datafusion-scan/scan-execution-partial-agg/spec.md` § Scenarios stating that on the `Value`
  path a partial-aggregate column whose declared `Numeric` carries an absent or out-of-range
  `precision` or `scale` fails the call with decision [4]'s named error and MUST NOT route to
  `Utf8`, because `column_accepts` (`rowset.rs:1325-1347`) admits no `Value::String` in a `Numeric`
  column. Add that case to task 2.8's unit tests, and amend plan.md § Design § Patterns row "One
  coercion rule, two emit paths" to record that the two paths share `target_arrow_type` but differ
  on this one declaration, naming the two SDK checks.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Impact, the paragraphs at lines 143-152; plan.md § Verification, lines
  339-343
- Issue: the two coverage statements disagree, and § Impact's inventory omits mismatch 4 entirely.
  § Verification states that three cases carry unit coverage only: "The wide-decimal `SUM`, the
  `MIN`/`MAX` over a `decimal(p,0)`, and the nested-aggregate `DOUBLE PRECISION` declaration".
  § Impact states "Two of the four mismatches carry unit coverage only" and names only the first
  two. § Impact's preceding paragraph lists the behavior preserved by the fix (`AVG`, `STDDEV`,
  `STDDEV_POP`, `VARIANCE`, `VAR_POP`, wide-decimal `SUM`, `MIN`/`MAX` over `decimal(p,0)`) and
  likewise never names the nested-aggregate case. The round-2 restatement of mismatch 4 added a
  third unit-only case and § Impact was not updated with it. § Impact is the section an operator
  reads for shipped risk.
- Fix: In plan.md § Impact, change "Two of the four mismatches carry unit coverage only" to three,
  add the nested-aggregate `DOUBLE PRECISION` declaration (`scalar_over_agg.rs:28`) to the list of
  cases proven by `partial_agg_tests.rs` alone, and add it to the preceding paragraph's list of
  behavior the fix preserves. Keep § Verification's wording as the one both sections state.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Implementation Tasks task 4.1;
  `e2e-harness/e2e-harness-scan-correctness/spec.md` § Scenarios, first THEN clause
- Issue: the surviving scenario keeps the clause "*THEN* `EXPLAIN VIRTUAL` SHALL show the generated
  `EMITS (...)` clause declaring one Exasol type per select-list item", but task 4.1 enumerates
  what the test asserts and omits it: "Assert the seeded values and the absence of the
  `emit_exa_types` key." The clause is a residue of the drift comparison the revision deleted, and
  it is the one clause of that scenario no task instructs anyone to implement. An implementer
  following task 4.1 literally leaves it uncovered.
- Fix: Add the `EXPLAIN VIRTUAL` assertion to plan.md task 4.1 ("assert that the captured
  `EMITS (...)` clause declares one Exasol type per select-list item"), or delete the clause from
  `e2e-harness/e2e-harness-scan-correctness/spec.md` § Scenarios. Do one or the other, not both.

## Design Depth

[no objection — axis checked: the revision changed no module boundary. Decision [1] keeps one owner
for the declared output type and `target_arrow_type` stays the single `ExaType`-to-Arrow mapping.
Round 1's open `[INFORMATION_LEAKAGE]` advisory on task 2.6, which asks the task to name
`coerce_batch_to_exa_types` as the shared entry point, is the same seam the Requirement Quality
BLOCKER above lands on; fixing that advisory alongside it would make the path split explicit in one
place instead of two. Group A and group B both list `emit_tests.rs` in their Knowledge columns,
which plan.md lines 314-316 already resolve by running the four groups in sequence.]

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md:226-229 (task 1.6's closing paragraph), plan.md:333-337 (§ Verification),
  plan.md:148-152 (§ Impact); `datafusion-scan/scan-execution-partial-agg/spec.md`:10-14
- Issue: the prose added this round breaks three `/speq:writing-guardrails` rules. Three newly
  authored sentences exceed the 25-word descriptive cap and join two ideas with "since" or "so":
  task 1.6's "No permanent test replaces it, by deliberate choice: a recurring assertion ... rather
  than testing this repo's logic" (48 words), § Verification's "No permanent E2E scenario asserts
  that comparison, deliberately: ... rather than this repo's logic" (44 words), and § Impact's "No
  seeded table and no bench dataset holds ... reach the engine untested at the integration and
  end-to-end level" (49 words). plan.md:336-337 uses a weak modal in governed prose: "A future
  reader looking for that missing scenario should read this paragraph". In the partial-agg delta,
  inserting the #399 sentence between the bold lead and "It adds ONE scenario and changes none"
  leaves "It" pointing at the note rather than at the delta.
- Fix: Split each of the three over-cap sentences in plan.md at its "since"/"so"/"and" joint, one
  idea per sentence. Rewrite plan.md:336-337 as a fact: "This paragraph records the reason the
  scenario is absent." In `datafusion-scan/scan-execution-partial-agg/spec.md`, move "It adds ONE
  scenario and changes none" ahead of the "Issue #399's Scope section states ..." sentence, so its
  subject stays the delta.

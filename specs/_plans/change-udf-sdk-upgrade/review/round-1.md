# Plan Review Findings: change-udf-sdk-upgrade (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 13 (Blockers: 3, Advisory: 10)
- Intent Fidelity blockers: 2

### Premortem

Three failure stories drove this review.

**Story 1.** The plan ships. Task 3 passes, decision-log `[1]` records "measured: the engine
truncates on its own side". A year later an 8.x cluster declares a catalog `timestamp` column as
bare `TIMESTAMP` and the scan emits a genuine microsecond Arrow column into it. The 0.28.x SLC
rejects the block, or rounds instead of truncating, and every timestamp query on 8.x breaks. The
"measurement" had observed DataFusion's own truncation, never the SLC's check. Routed to
`[INTENT_DRIFT]` and `[UNSTATED_ASSUMPTION]`.

**Story 2.** The recorded spec carries a normative SHALL about the engine accepting a microsecond
column into an above-microsecond declaration, attributed to a live measurement. That arm was never
reachable on the pinned image, and no task was permitted to amend the delta. A later change trusts
the SHALL and ships a data bug. Routed to `[INTENT_DRIFT]`.

**Story 3.** The site census in task 2 is treated as authoritative over the compiler. Two
construction sites in an already-listed file are missed, so task 1.2's "any file the compiler names
that task 2 does not list" gate never fires. The census degrades into trial and error. Routed to
`[TRACEABILITY_GAP]`.

## Intent Fidelity

Checked and no objection on two points the brief raised. **The third spec delta is warranted, not
scope creep**: `specs/datafusion-scan/scan-execution-partial-agg/spec.md:93` really does carry the
now-false clause "reports an absent `precision` or `scale`", and
`crates/lakehouse-engine/src/scan/partial_agg_tests.rs:895-898` really constructs
`ExaType::Numeric { precision: None, scale: None }`. **Decision-log conciseness holds**: three
entries, only `[1]` marked `Promotes to ADR: yes`; the `Option<u32>`-to-`u32` mechanics (`[2]`) and
the third-delta justification (`[3]`) are terse and unpromoted, as the user asked.

#### [INTENT_DRIFT] BLOCKER

- Location: `plan.md` § Implementation Tasks task 3.2, and § Design > Decision (lines 47-51)
- Issue: the live proof cannot observe the behaviour the plan says it proves, so issue #405's "must
  be resolved here, not deferred" is restated rather than answered. Task 3.2 probes
  `CAST(<ts column> AS TIMESTAMP(p))` for `p` in {0, 3, 6, 9}. A projected CAST select-list item is
  rendered into the scan's **DataFusion** SQL by `vs_expression::render_expression`, which applies
  `snap_timestamp_precision` (`crates/vs-expression/src/lib.rs:431-438`). That function maps
  0→0, 3→3, 6→6, 9→9: for exactly the four precisions the task picks, the snap is the identity.
  DataFusion therefore evaluates `CAST(x AS TIMESTAMP(p))` itself and truncates the value to `p`
  **before** the emit boundary. `target_arrow_type` then widens the already-truncated column back to
  `Timestamp(Microsecond, None)`. The value handed to `emit_batch` carries no fractional digit below
  the declared precision, so the SLC's new precision check is never stressed. The assertion passes
  whether or not Exasol truncates, and whether or not the SLC accepts a finer value. `plan.md:49`
  claims task 3 "proves on the running engine that the SLC accepts that column into a
  lower-precision declaration and that the engine truncates the value on its own side"; the probe
  can establish neither half.
- Fix: In `plan.md` § Implementation Tasks task 3.2, replace the `p` set {0, 3, 6, 9} with a probe
  whose declared EMITS precision is strictly finer than the Arrow value's resolution at the emit
  boundary. Take at least one of two routes and name which in the task text. Route (a): use `p` in
  {1, 2, 4, 5, 7, 8}, where `exasol_type_from_json` declares `TIMESTAMP(p)` verbatim
  (`crates/lakehouse-engine/src/types/mapping.rs:684-690`) while `snap_timestamp_precision` renders
  `TIMESTAMP(snap(p))` into DataFusion, so the emitted value is genuinely finer than the
  declaration; add a sub-task that first measures which of those six Exasol 2025.1.16 accepts as a
  CAST target, because `crates/lakehouse-engine/tests/common/timestamp_precision.rs:20-23` records
  Exasol 8.29.13 rejecting every precision outside {3, 6} with `0A000 Feature not supported`.
  Route (b): drive the probe through a catalog timestamp column on the pre-2025 bare-`TIMESTAMP`
  arm, where no CAST exists and a microsecond Arrow value goes straight into a `TIMESTAMP(3)`
  declaration, by running the suite against an 8.x `EXASOL_IMAGE` in addition to the pinned
  2025.1.16 (`Makefile:3`). If neither route is reachable, state that in `plan.md` § Design >
  Decision and in the spec delta, and record the fixed microsecond target as an unverified inherited
  limit with a tracked issue rather than as a measured decision.

#### [INTENT_DRIFT] BLOCKER

- Location: `datafusion-scan/type-mapping-timestamp-precision/spec.md:36`, `plan.md` § Parallelization
  group B, `plan.md` task 3.3
- Issue: the spec delta pre-commits to an outcome no task is authorized to correct, against the
  user's instruction that "the spec delta's precision-handling text must be written to match
  whatever that task confirms". The delta asserts, as a normative SHALL attributed to a measurement,
  that the engine "SHALL accept that microsecond column into an output column declared BELOW
  microsecond resolution and SHALL truncate each value to the declared `p` on its own side rather
  than failing the emit, and SHALL accept it into one declared ABOVE microsecond resolution without
  altering the value, measured against the local Exasol Docker container". Task 3.3 already concedes
  the reachable evidence is narrower: "the probe reaches `TIMESTAMP(3)` through a CAST on 2025.1.16,
  and an 8.x engine is not exercised". `plan.md:194` then forbids the group that runs the
  measurement from touching any delta: "group B verifies the `TIMESTAMP(p)` scenario group A
  authored and edits no spec delta". Task 3.3's stop condition fires only on an outright
  contradiction, never on a clause the measurement simply cannot reach, so the expected outcome is a
  false SHALL recorded into the permanent library with no mechanism to correct it.
- Fix: In `plan.md` § Parallelization, delete "and edits no spec delta" from group B's Knowledge
  cell and add `specs/_plans/change-udf-sdk-upgrade/datafusion-scan/type-mapping-timestamp-precision/spec.md`
  to that cell. Add task 3.4 to `plan.md` § Implementation Tasks, tagged `[expert]`: "Rewrite the
  live-engine clause of `type-mapping-timestamp-precision`'s `A TIMESTAMP(p) EMITS string maps back
  to the microsecond Arrow timestamp` scenario so it states exactly what task 3.2 measured, naming
  the engine version and the `p` values exercised. Move every declaration arm the measurement did
  not reach out of the SHALL and into a named, tracked limitation with a GitHub issue cited inline."
  Then rewrite `type-mapping-timestamp-precision/spec.md:36` now to scope its claim to the
  below-microsecond arm alone, and delete the above-microsecond half of the sentence until task 3.2
  produces evidence for it.

#### [SCOPE_REDUCTION] ADVISORY

- Location: `plan.md` § Impact
- Issue: issue #405 names "SLC reinstall + `.so` rebuild op follow-up" in its impact list. The plan
  covers it as narrative only, with no task and no tracked issue, so nothing carries it forward
  after the PR merges. Separately, `specs/mission.md` § Tech Stack records `exasol-udf-sdk 0.23.0`
  while the live pin is already `0.26.1` (`Cargo.toml:74`); this plan moves the pin again and
  amends neither the mission row nor `CLAUDE.md` § Build, which names SDK 0.19.0/0.19.1 behaviour.
- Fix: Add a task to `plan.md` § Implementation Tasks group A: "Update the `UDF runtime` row of
  `specs/mission.md` § Tech Stack to `exasol-udf-sdk 0.28.1`." Add a row to `plan.md` § Impact
  naming the operator follow-up and either the GitHub issue that tracks the SLC reinstall or the
  runbook file the plan amends instead.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER

- Location: `plan.md` § Design > Decision (line 55), and `decision-log.md` `[1]` § Alternatives
- Issue: the sentence "Exasol already owns the truncation, so a second truncation inside the scan
  would add a rule without adding a guarantee" is load-bearing. It is the stated reason for
  rejecting precision-following coercion, and `decision-log.md` `[1]` repeats it as one of the two
  grounds. It is false on the only path this plan tests. For a projected
  `CAST(x AS TIMESTAMP(p))`, DataFusion owns the truncation, because `snap_timestamp_precision`
  renders the cast into the scan's own DataFusion SQL. Exasol owns the truncation only for a
  catalog timestamp column on the pre-2025 bare-`TIMESTAMP` arm, which the plan explicitly does not
  exercise (task 3.3: "an 8.x engine is not exercised"). The plan never states which of the two
  declaration paths it means, so the reader cannot tell which component truncates in which case.
- Fix: In `plan.md` § Design > Decision, replace the single "Exasol already owns the truncation"
  sentence with two sentences that name the paths separately: one stating that on a projected CAST
  the truncation happens inside DataFusion via `snap_timestamp_precision`, and one stating that on a
  catalog timestamp column declared bare `TIMESTAMP` the truncation happens in Exasol. Make the same
  split in `decision-log.md` `[1]` § Alternatives. Add the corresponding Background bullet to
  `datafusion-scan/type-mapping-timestamp-precision/spec.md`.

#### [EFFORT_MISESTIMATION] ADVISORY

- Location: `plan.md` task 3.2
- Issue: the task directs reuse of "the helper `retained_at` the file already carries" for `p` in
  {0, 3, 6, 9}, but that helper cannot take `p` above 6.
  `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs:87-90` computes
  `let step = 10i64.pow(6 - precision);` on a `u32` precision, so `retained_at(_, 9)` underflows and
  panics. The same applies to any `p` in {7, 8}, which the fix for the first Intent Fidelity blocker
  may introduce.
- Fix: In `plan.md` task 3.2, add one sentence: "Extend `retained_at` with a branch that returns
  `micros` unchanged for `precision` at or above 6, because `10i64.pow(6 - precision)` underflows
  above that."

#### [HIDDEN_DEPENDENCY] ADVISORY

- Location: `plan.md` task 3.2
- Issue: the task asserts "through `assert_pushed_to_scan_udf` that each projection reaches the scan
  UDF", but that helper proves nothing about the declaration under test.
  `crates/lakehouse-engine/tests/e2e_timestamp_precision_test.rs:102-108` only checks
  `pushed.contains(SCAN_SCRIPT_NAME)`, which is true for every virtual-schema query on that table,
  CAST or not. If Exasol declines to delegate the CAST and evaluates it after the scan, the
  assertion still passes and the generated `EMITS` clause still reads `TIMESTAMP(6)`. The test would
  then measure Exasol's post-scan cast, not the emit boundary. The harness already carries the right
  tool: `isolated_pushdown_statement` (`crates/lakehouse-engine/tests/common/e2e_harness.rs:348`)
  returns the `PUSHDOWN_SQL` cell, and the adapter renders `EMITS ({emits})` into that statement
  (`crates/lakehouse-engine/src/adapter/pushdown/support.rs:451` and `:474`).
- Fix: In `plan.md` task 3.2, replace the `assert_pushed_to_scan_udf` sentence with: "Assert through
  `isolated_pushdown_statement` that the generated statement's `EMITS` list literally declares
  `TIMESTAMP(p)` for the probed column at each `p`, so the test fails rather than passes vacuously
  when Exasol evaluates the CAST itself."

Checked and no objection on three further feasibility claims. The dependency table verifies:
`exasol-udf-sdk` 0.28.1 is published on crates.io, and release `v0.28.1` of
`exasol-labs/language-container-rs` carries both `lc-rust-0.28.1.tar.gz` and
`lc-rust-0.28.1-aarch64.tar.gz`. The § Design > Context diff claim holds: `value.rs` is the only
changed file under `exasol-udf-sdk/src` apart from its own `value_tests.rs` sibling, and the only
changed item in it is the `ExaType` enum, exactly as the plan's code block states. Task 3.1's claim
that the harness registers SLC 0.28.1 by itself holds:
`crates/lakehouse-engine/tests/common/e2e_harness.rs:57` defines
`pub const SLC_VERSION: &str = sdk_version_from_fingerprint();`. The § Impact claim that the pin
lives in one place holds: `Cargo.toml:74-75` is the only non-lockfile occurrence, `Makefile:129`
derives it by `sed`, and `bench/run.sh:341` reads `make -s print-slc-version`.

## Requirement Quality

#### [COMPLETENESS_GAP] ADVISORY

- Location: `datafusion-scan/scan-execution-value-conversion/spec.md:29-32`
- Issue: the Background bullet records a five-variant removal as safe on upstream's evidence alone:
  "Upstream removed `TimestampTz`, `Geometry`, `HashType`, `IntervalYearToMonth` and
  `IntervalDayToSecond` as unreachable, confirmed by its own live canaries on 8.29.x, 2025.1.x and
  2026.1.x. No emitted value changes, because Exasol never declared a UDF column at any of those
  types." This repo's own rule is stricter: CLAUDE.md § Verification discipline requires a claimed
  SQL capability limitation to be verified against a live Exasol system rather than assumed from
  documentation. Task 3 exercises no canary for the five removed types, and the bullet cites no
  upstream artifact a reader can check. Issue #405 names the source as ADR 035; the delta does not.
- Fix: In `datafusion-scan/scan-execution-value-conversion/spec.md`, append to that Background
  bullet the inline citation of upstream ADR 035 and its release, and rephrase the second sentence
  as an upstream-sourced claim this repo adopts rather than as a fact this repo verified.

Checked and no objection on the remaining requirement-quality surface. The `DELTA:REMOVED` blocks
match the recorded text verbatim
(`specs/datafusion-scan/type-mapping-timestamp-precision/spec.md:51-53` and
`specs/datafusion-scan/scan-execution-value-conversion/spec.md:34-36`). Both `DELTA:CHANGED` scenario
titles match their recorded titles exactly, so both now-false recorded clauses naming
`ExaType::TimestampTz`
(`specs/datafusion-scan/type-mapping-timestamp-precision/spec.md:107` and
`specs/datafusion-scan/scan-execution-value-conversion/spec.md:64`) fall inside a replaced scenario.
The value-conversion delta's variant enumeration covers all ten surviving variants with none
double-counted. `plan.md` § Recorded text this plan does not amend is accurate:
`crates/lakehouse-engine/src/adapter/pushdown/support.rs` and
`crates/lakehouse-engine/src/types/mapping.rs` reference only this repo's own `ExaTypeClass` and
`classify_exa_type`, never the SDK enum. The "match SHALL stay EXHAUSTIVE with no wildcard" clause
has no Verification row, correctly: it is a compile-time property, and a runtime source probe would
violate this project's standing rule against grep surface probes.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY

- Location: `plan.md` § Verification > Scenario Coverage, row 3
- Issue: the row names the unit test `coerce_batch_to_exa_types_casts_every_declared_variant`. No
  test by that name exists anywhere under `crates/`. The two real tests that cover the scenario are
  `coerce_maps_every_exa_type_variant_to_its_arrow_target`
  (`crates/lakehouse-engine/src/scan/emit_tests.rs:327`) and
  `coerce_batch_casts_every_column_to_declared_exatype` (same file, `:403`). The Verification table
  is what the recorder merges into the permanent library, so a fabricated test name becomes recorded
  drift.
- Fix: In `plan.md` § Verification > Scenario Coverage row 3, replace
  `coerce_batch_to_exa_types_casts_every_declared_variant` with
  `coerce_maps_every_exa_type_variant_to_its_arrow_target, coerce_batch_casts_every_column_to_declared_exatype, emit_stream_fails_on_numeric_with_out_of_range_payload`.

#### [TRACEABILITY_GAP] ADVISORY

- Location: `plan.md` tasks 1.2, 2.4 and 2.5
- Issue: task 1.2 claims "Task 2 lists the files a full text census of every `ExaType` reference
  found", and its safety gate fires only when the compiler names a **file** task 2 omits. Three
  sites inside already-listed files are missing, so the gate cannot catch them and the census claim
  is false. (a) `crates/lakehouse-engine/src/scan/emit_tests.rs` constructs bare
  `ExaType::Timestamp` at `:568` and `:746`, both compile errors under
  `Timestamp { precision: u32 }`; task 2.5 names only the sweep at `:330`, the
  `exa_type_timestamp_maps_to_microsecond_target` test, the two numeric drift tests, and
  `Char { size }` at `:347` and `:484`. (b) `crates/lakehouse-engine/tests/scan_fixture/mod.rs`
  carries `declared_size`, `declared_precision` and `declared_scale` at `:99-118`, whose pattern
  arms destructure `Numeric { precision, .. }`, `Numeric { scale, .. }` and
  `String { size } | Char { size }` and return `Option<u32>`; task 2.4 names only `varchar()`,
  `decimal()` and `declared_type_name`. (c) `crates/lakehouse-engine/tests/micro_bench.rs`'s
  `numeric()` helper is at `:68`, not `:69`.
- Fix: In `plan.md` task 2.5, add "and the two bare `ExaType::Timestamp` construction sites at
  `:568` and `:746`". In task 2.4, add "plus `declared_size`, `declared_precision` and
  `declared_scale` at `:99-118`, whose returns gain a `Some(...)` wrap for the same reason as task
  2.3". In task 2.7, change the `numeric()` line reference from `:69` to `:68`. In task 1.2, change
  "Any file the compiler names that task 2 does not list" to "Any file OR SITE the compiler names
  that task 2 does not list".

Checked and no objection on cluster shape and expert tagging. The two groups are correctly
sequenced rather than claimed parallel, group B's dependency on group A's `.so` and matching SLC is
stated, and the groups share no file. The three `[expert]` tags sit on exactly the live-verification
tasks, which carry the coerce-versus-keep judgement and the reading of live engine behaviour; the
nine mechanical call-site tasks are correctly untagged. Task 2.5's claim of three absent-payload
cases in `emit_stream_fails_on_numeric_with_absent_or_out_of_range_payload` and one in
`a_drifted_numeric_never_resolves_to_the_string_target` is exact, as is task 2.6's claim of one in
`partial_agg_fails_on_numeric_with_absent_or_out_of_range_payload`. Task 2.4's count of "the twelve
other files under `tests/`" is exact, and every one of those twelve uses only surviving variants or
the two fixture helpers.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY

- Location: `datafusion-scan/type-mapping-timestamp-precision/spec.md` § Background, and
  `plan.md` § Design
- Issue: one decision, what fractional-second precision a timestamp column carries, is reflected in
  three modules with nothing enforcing agreement, and this plan touches the third without naming the
  split. `crates/lakehouse-engine/src/types/mapping.rs:684-690` declares `TIMESTAMP(p)` verbatim in
  the `EMITS` clause. `crates/vs-expression/src/lib.rs:431-438` renders `TIMESTAMP(snap(p))` into
  the DataFusion scan SQL, where `snap` collapses {1,2,4,5,7,8} onto {0,3,6,9}.
  `crates/lakehouse-engine/src/scan/emit.rs:241` discards `p` and forces microsecond. For `p` in
  {1, 2, 4, 5, 7, 8} the first two disagree by construction, and no module owns the reconciliation.
  The plan's response to newly readable precision is to document ignoring it, which is defensible,
  but it leaves the divergence unrecorded anywhere in the spec library.
- Fix: Add a Background bullet to
  `datafusion-scan/type-mapping-timestamp-precision/spec.md`: "The declared Exasol precision and the
  precision DataFusion computes at can differ. `exasol_type_from_json` declares `TIMESTAMP(p)`
  verbatim, `vs-expression`'s `snap_timestamp_precision` renders `TIMESTAMP(snap(p))` into the scan
  SQL because DataFusion parses only `p` in {0, 3, 6, 9}, and `target_arrow_type` reads neither.
  For `p` in {1, 2, 4, 5, 7, 8} the emitted value is therefore finer than the declaration, and
  Exasol performs the remaining truncation."

Checked and no objection on the other three design-depth tags. The change introduces no new module,
interface or boundary, so the Quick Diagnostic table is not applicable. `decimal_target` keeps its
`&ColumnInfo` parameter only to name the column in the error, which is the shallowest defensible
signature. The plan schedules no tactical shortcut. No planned business logic gains a dependency on
a delivery mechanism, storage engine or framework.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY

- Location: `plan.md` task 1.2 (line 126)
- Issue: "Task 2 lists the files a full text census of every `ExaType` reference found." is
  ungrammatical and not understandable on one read. It is also the sentence that states the census
  guarantee the Task Breakdown finding above contradicts.
- Fix: In `plan.md` task 1.2, replace that sentence with two: "A full text census of every
  `ExaType` reference produced task 2's file list. Add any file or site the compiler names that task
  2 omits before the group proceeds."

#### [PROSE_UNCLEAR] ADVISORY

- Location: `plan.md` § Parallelization, closing paragraph (line 198)
- Issue: "so keeping it separate prices the mechanical bump at the standard model" uses "prices" as
  unexplained jargon. The reader cannot tell what is being priced or against what.
- Fix: In `plan.md` § Parallelization, replace that clause with "so the mechanical bump runs on the
  standard model and only the live verification runs on the expert model."

#### [PROSE_BLOAT] ADVISORY

- Location: `plan.md` task 3.2, `decision-log.md` lines 12, 21 and 38, `plan.md` lines 38 and 40
- Issue: two vocabulary-consistency breaks. The artifacts spell one concept two ways: "behaviour"
  in `plan.md` task 3.2 and `decision-log.md`, against "behavior" in
  `datafusion-scan/scan-execution-value-conversion/spec.md:45` and throughout the recorded library.
  Separately, `plan.md:38` and `:40` open the Goals and Non-Goals bullets with an em dash, which
  `/speq:writing-guardrails` bans outright.
- Fix: In `plan.md` and `decision-log.md`, change every "behaviour" to "behavior". In `plan.md`
  lines 38 and 40, replace the em dash after `**Goals**` and `**Non-Goals**` with a colon.

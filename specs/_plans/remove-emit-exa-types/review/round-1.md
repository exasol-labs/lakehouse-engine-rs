# Plan Review Findings: remove-emit-exa-types (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 18 (Blockers: 7, Advisory: 11)
- Intent Fidelity blockers: 2

### Premortem

Three failure stories drove this review.

1. **The removed fallback turns a metadata gap into a total outage.** Decision [4] deletes every
   fallback path. Task 1.6 proves `output_column` on one local Docker container. A cluster that
   reports the metadata differently fails every scan query instead of degrading, and the plan
   records no rollback. Routes to `[NFR_IGNORED]`, `[UNSTATED_ASSUMPTION]`.
2. **Two of the four partial-aggregate fixes ship untested against the engine.** No seed fixture
   and no bench dataset carries a `decimal(p,0)` column or a decimal with precision at least 27.
   A customer runs `MIN(order_id)` over a Spark-authored `decimal(18,0)` and the query fails.
   Routes to `[TRACEABILITY_GAP]`, `[COMPLETENESS_GAP]`.
3. **`ExaType::Numeric` arrives with no precision.** The variant carries `Option<u32>` fields. The
   scenario states no rule for the absent case, so the implementer invents one at the emit
   boundary. Routes to `[COMPLETENESS_GAP]`.

### Claims verified, not disputed

The following plan claims were checked independently and hold. They are recorded here so round 2
does not re-open them.

- All four dependency-table entries are true: `language-container-rs#89` closed 2026-09-14 by PR
  #105 (merge commit `1fe3d27`, now `origin/main` HEAD); `exasol-udf-sdk`, `exasol-udf-macros` and
  `cargo-exasol-udf` 0.26.0 are on crates.io; SLC release `v0.26.0` is published with both
  `lc-rust-0.26.0.tar.gz` and `lc-rust-0.26.0-aarch64.tar.gz`.
- The `column_accepts` matrix reads exactly as plan.md § Context states
  (`language-container-rs` `crates/exa-udf-runtime/src/rowset.rs:1212-1234`): a `Double` column
  accepts only `Value::Double`, `Int32`/`Int64` reject `Value::Numeric`, `Numeric` rejects
  `Value::Double`, and no numeric column accepts `Value::String`.
- **All four partial-aggregate mismatches are real.** `grouped_agg.rs:690-692` hardcodes
  `AvgSum`/`StatSum`/`StatSumSq` as `DOUBLE PRECISION` while `partial_agg.rs:367-371` emits a bare
  `SUM(<arg>)`. `sum_emit_type` (`grouped_agg.rs:752-763`) caps the declared type at
  `DECIMAL(36,s)` while DataFusion widens `SUM(Decimal128(p,s))` to `min(38, p+10)`, so precision
  at least 27 produces `Value::String`. `col_type_for` declares MIN/MAX with the source column's
  type, and `arrow_value_at` (`convert.rs:136-143`) produces `Value::Numeric` for every in-range
  `Decimal128`, so a `decimal(p,0)` with `p` at most 18 lands `Value::Numeric` in an `Int64` bin.
  No existing cast guards any of them: `partial_row_from_batch` and `run_grouped_partial_aggregate`
  feed `arrow_value_at` output straight into `ctx.emit`.
- Materiality holds for mismatch 1. `validate_agg_col_types` (`grouped_agg.rs:777-808`) type-checks
  only SUM and the STDDEV/VARIANCE family, so `AVG` and `MIN`/`MAX` are ungated, and
  `FN_AGG_SUM`/`MIN`/`MAX`/`AVG`/`STDDEV*`/`VAR*` are all advertised in `capabilities.rs:169-183`.
- The single-group path shares `partial_emits_items` with the grouped path (`support.rs:272` and
  `grouped_agg.rs:554`), so task 2.6's two-path scope is correct.
- Group keys are declared `VARCHAR(2000000)` (`grouped_agg.rs:551-553`) and `value_to_gk_string`
  produces `Value::String`, so decision [5]'s skip is sound.
- Task 3.2's five construction sites and task 3.3's two read-backs exist exactly as cited. The 11
  golden dispatch fixtures carrying the literal are 11 of 24.
- `Makefile:129` and `deploy/scripts/install.sh`'s `resolve_engine_pinned_slc_version` both derive
  the SLC version from the workspace `exasol-udf-sdk` pin, so plan.md § Impact is accurate.
- `bench/run.sh:339` and `deploy/scripts/secrets.sh:49` do carry a hardcoded `0.21.0`. The
  `0.21.0` values in `deploy/scripts/tests/install.test.sh` are synthetic fixture data, as task 1.3
  states.
- `speq plan validate remove-emit-exa-types` passes. All five `DELTA:CHANGED` target scenario names
  match a recorded scenario exactly. Every delta scenario has a row in § Scenario Coverage.
- Issue #399 is linked in plan.md:5 and `Closes #399` guidance is present at plan.md:12.

## Intent Fidelity

#### [SCOPE_REDUCTION] BLOCKER
- Location: `e2e-harness/e2e-harness-scan-correctness/spec.md` § Scenarios, and plan.md § Implementation Tasks task 1.6
- Issue: the clarifying interview answered acceptance criterion 4 with "add or extend an E2E test
  that runs a real scan query and asserts that `ctx.output_column`'s reported types match the
  adapter's `EMITS` clause". The delta substitutes an indirect proof and declares the direct one
  impossible: "The scenario proves agreement, which cannot be asserted by comparing two lists ...
  Agreement is proven by execution instead". The only direct comparison in the plan is task 1.6's
  temporary `udf_log!` capture, which the same task then deletes ("then remove the temporary
  logging"), so no permanent artifact asserts what the user asked for. The delta's stated detector
  is also wrong: the scenario clause "the query MUST NOT fail with an `emit_batch` Arrow-to-ExaType
  rejection ... which is what a disagreement between the declared `EMITS` clause and
  `UdfContext::output_column` would produce" is tautological for per-column types, because
  `target_arrow_type` derives the Arrow target from `output_column` and `rowset.rs`'s accessor
  matrix then validates that Arrow type against the same `output_column` value. Both sides read one
  source, so a per-column type disagreement cannot surface there. Only arity drift is caught.
- Fix: In `e2e-harness/e2e-harness-scan-correctness/spec.md`, delete the clause naming the
  `emit_batch` rejection as the drift detector and delete the Background sentence "The scenario
  proves agreement, which cannot be asserted by comparing two lists." Add a clause requiring the
  scan UDF to report its runtime `UdfContext::output_column(i).type_name` list for the call, and
  requiring the test to assert that list equals the `EMITS (...)` clause captured from
  `EXPLAIN VIRTUAL`, item for item. In plan.md task 4.1, add the mechanism that carries that list
  to the test (a dedicated probe query, or an env-gated `udf_log!` captured through the
  `SCRIPT_OUTPUT_ADDRESS` listener the harness already uses), and state that it is permanent, not
  temporary. Keep the existing value-correctness and `emit_exa_types`-absence clauses as they are.

#### [SCOPE_CREEP] BLOCKER
- Location: plan.md § Summary, § Implementation Tasks section 2, § Parallelization group B; decision-log.md § Design Decisions [2]
- Issue: the four partial-aggregate fixes are real and the bump does force them (verified above), so
  the work belongs in this change. The defect is how the plan carries it. Issue #399's own Scope
  section states "**Not in scope:** the partial-aggregate path (`scan/partial_agg.rs`) doesn't use
  `emit_exa_types` at all ... #89 doesn't change anything there", and plan.md never records that it
  is overriding that statement. No GitHub issue tracks the partial-aggregate work: a sweep of all
  225 issues in `exasol-labs/lakehouse-engine-rs` (searches for partial aggregate, type mismatch,
  AVG, `column_accepts`, `output_column`, `partial_agg`) returns none, which contradicts CLAUDE.md
  § Feature tracking ("New features are tracked as GitHub issues (`gh issue create`) before/at the
  start of work"). Tasks 2.6 and 2.8 sit inside "### 2. The declared output column becomes the emit
  authority" interleaved with the #399 tasks, so the implementing commit carries `Closes #399`
  while silently closing untracked work.
- Fix: Do not remove the partial-aggregate work. Open a GitHub issue for it (title it for the
  defect, for example "partial-aggregate cells violate their declared EMITS column type"), and cite
  that issue number in plan.md § Summary, in the new task section, and in the
  `datafusion-scan/scan-execution-partial-agg` delta's Background in place of "This delta is issue
  #399's blocking prerequisite". Move tasks 2.6 and 2.8 into a new numbered section
  "### N. Partial-aggregate conformance to the declared column" and give it its own row in
  § Parallelization. In plan.md § Summary, add one sentence stating that issue #399's "Not in
  scope" note for `scan/partial_agg.rs` predates the 0.26.0 row validation and no longer holds. In
  plan.md:12, state both trailers the implementing commit carries.

#### [SCOPE_REDUCTION] ADVISORY
- Location: `e2e-harness/e2e-harness-scan-correctness/spec.md` § Background, bullet "**`Binary` needs no separate fixture.**"
- Issue: the interview named the JSON-fallback set explicitly as "List/Struct/Map/Binary/etc.". The
  delta drops Binary with the reasoning that "The coercion dispatches on the declared `ExaType`,
  never on the source Arrow type". That reasoning is correct, and a Binary column reaches the emit
  boundary as `Utf8` anyway via `CAST(col AS VARCHAR)`. The residue is that the user named a type
  the scenario does not exercise, and the plan does not say so in a place a human reviewing the
  plan will see.
- Fix: Add one sentence to plan.md § Features or § Impact recording that the E2E type mix covers
  list, struct and map but not binary, and naming the shared declared-`ExaType` dispatch as the
  reason.

## Feasibility

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Implementation Tasks task 1.5; plan.md § Load-bearing assumptions, row 3
- Issue: the census of delegating `UdfContext` impls is wrong in one entry and missing another.
  Task 1.5 names "`crates/lakehouse-engine/tests/scan_fixture/mod.rs` `BatchCapturingCtx` and the
  `SinkCtx` double in `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`". `SinkCtx` is declared
  at `crates/lakehouse-engine/src/scan/test_support_tests.rs:80-82`; `raw_scan_tests.rs` only
  imports it at :1195. A third impl exists and is not named at all: `CapturingCtx` at
  `crates/lakehouse-engine/src/scan/emit_tests.rs:66`. `CapturingCtx` is the double that drives
  `emit_stream` at `emit_tests.rs:150` and `emit_tests.rs:606`, so it is the one impl that cannot
  work without the accessors. It is hand-rolled rather than a `TestContext` delegate, so it cannot
  forward and needs its own `output_column_count` and `output_column` bodies. Task 1.5's own
  warning names the consequence: "They are trait-defaulted, so a missing forward compiles and then
  returns `Unimplemented` at run time." With decision [4] removing every fallback, that is a
  run-time failure of every test routed through `CapturingCtx`.
- Fix: Rewrite task 1.5 to name all three impls with correct paths:
  `crates/lakehouse-engine/tests/scan_fixture/mod.rs:81` `BatchCapturingCtx`,
  `crates/lakehouse-engine/src/scan/test_support_tests.rs:82` `SinkCtx`, and
  `crates/lakehouse-engine/src/scan/emit_tests.rs:66` `CapturingCtx`. State that `CapturingCtx` is
  not a `TestContext` delegate and needs its own accessor bodies returning the declared columns the
  test sets. Correct the `raw_scan_tests.rs` path in plan.md § Parallelization group A's Knowledge
  column to `test_support_tests.rs` and add `emit_tests.rs`.

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: `datafusion-scan/scan-execution-value-conversion/spec.md` § Background, bullet "**`exasol-udf-sdk` 0.26.0 supplies the authoritative reading.**"; plan.md § Design § Context; plan.md § Load-bearing assumptions, row 1
- Issue: the delta states that PR #105 "wires these from the call-site `EMITS` list and covers the
  dynamic-EMITS shape with a live `column-meta` integration fixture". The fixture does not cover
  the dynamic-EMITS shape. `column_metadata_reaches_the_udf`
  (`language-container-rs` `crates/it/tests/db_roundtrip.rs:2254-2308`) issues
  `CREATE OR REPLACE RUST SCALAR SCRIPT describe_output(dummy BOOLEAN) EMITS (col_a VARCHAR(200),
  col_b DECIMAL(18,0))`, then re-registers the same entry point against
  `EMITS (col_z VARCHAR(200))`. Both forms are **static** column lists in the script DDL. It proves
  the accessors follow a re-registered static list. `LAKEHOUSE_SCAN` uses the dynamic form, where
  the script DDL carries `EMITS (...)` with no list and the adapter supplies the columns at the
  call site. That is precisely the case task 1.6 exists to prove, so the plan must not also claim
  upstream already proved it. This wording is headed into the permanent spec library, and CLAUDE.md
  § Verification discipline forbids asserting behavior from an upstream fixture alone.
- Fix: In `datafusion-scan/scan-execution-value-conversion/spec.md`, replace "covers the
  dynamic-EMITS shape with a live `column-meta` integration fixture" with a statement of what the
  fixture actually covers: a live integration fixture in which the same entry point follows a
  re-registered static `EMITS` list. Add one sentence recording that the dynamic call-site form is
  proven separately against the local Exasol Docker container. Apply the same correction to
  plan.md § Design § Context and to the Evidence cell of plan.md § Load-bearing assumptions row 1.

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: decision-log.md § Design Decisions [2] Rationale; `datafusion-scan/scan-execution-partial-agg/spec.md` § Background, fourth bullet, last sentence
- Issue: decision [2] states "All four were verified against the upstream matrix and the two repo
  call sites, not inferred." That is true for mismatches 1, 2 and 3, and false for mismatch 4. The
  delta states mismatch 4 as fact: "An expression-argument aggregate whose Exasol-declared result is
  `DECIMAL` but whose DataFusion expression is `Float64` produces `Value::Double` into a `Numeric`
  column." The DataFusion half is decidable in-repo (`TRUNC` and `SIGN` are advertised at
  `capabilities.rs:91` and `:85`, rendered to DataFusion `trunc`/`signum`, both of which return
  `Float64` for a non-`Float32` argument). The Exasol half is not: whether Exasol reports `DECIMAL`
  in `selectListDataTypes` for `SUM(TRUNC(dec,2))` or `MIN(SIGN(x))` is a live-engine fact.
  CLAUDE.md § Verification discipline states that a claimed SQL capability or limitation "MUST be
  verified against a live Exasol system (`EXPLAIN VIRTUAL`, an actual pushed query, or an E2E
  test), not assumed from documentation, memory, or a capability registry (`capabilities.rs`)
  alone". A stricter variant of mismatch 4 needs no Exasol assumption at all and is decided
  entirely by repo code: `NESTED_AGGREGATE_PLAN_TYPE` is the hardcoded literal `"DOUBLE PRECISION"`
  (`crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg.rs:28`), applied at `:57` to any
  aggregate reached only nested inside a scalar, so `SELECT ROUND(SUM(dec_a * dec_b), 2)` declares
  `DOUBLE PRECISION` while DataFusion yields `Decimal128`, giving `Value::Numeric` into an
  `ExaType::Double` column.
- Fix: Restate mismatch 4 in
  `datafusion-scan/scan-execution-partial-agg/spec.md` § Background using the
  `NESTED_AGGREGATE_PLAN_TYPE` case, which both halves of the repo decide: an aggregate reached only
  nested inside a scalar is declared `DOUBLE PRECISION` by
  `scalar_over_agg.rs:28`, while its DataFusion expression yields `Decimal128`, so `Value::Numeric`
  meets an `ExaType::Double` column. In decision-log.md [2], replace "All four were verified against
  the upstream matrix and the two repo call sites, not inferred" with a sentence naming the evidence
  per mismatch. If the `TRUNC`/`SIGN` form is kept anywhere in the plan, add it to task 1.6's scope
  as a captured pushdown request whose `selectListDataTypes` is read from the running container, and
  mark it unverified until that capture exists.

#### [NFR_IGNORED] ADVISORY
- Location: plan.md § Impact
- Issue: decision [4] deletes every fallback, so a context that does not report output columns fails
  the call instead of degrading. § Impact covers the SLC fingerprint and the wire-format change but
  records no rollback for the case where task 1.6 passes on the local Docker container and a real
  cluster then behaves differently. The blast radius is every scan query, not one feature.
- Fix: Add a short paragraph to plan.md § Impact naming the rollback: revert the engine release and
  reinstall SLC v0.24.0, because the `.so`, the SLC and the adapter deploy together and the scan
  script DDL is recreated per deployment. State that no partial rollback exists, since the removed
  field and the SDK pin move in one commit.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER
- Location: `datafusion-scan/scan-execution-value-conversion/spec.md` § Scenarios, the clause mapping `Numeric { precision, scale }` to `Decimal128(precision, scale)`
- Issue: the scenario reads the mapping as if the payload were always present. The SDK declares
  `ExaType::Numeric { precision: Option<u32>, scale: Option<u32> }`
  (`language-container-rs` `crates/exasol-udf-sdk/src/value.rs:158-177`), populated from optional
  protobuf fields (`crates/exa-zmq-protocol/src/meta.rs:69-72`), and `precision: None` /
  `scale: None` is a representable state used across the upstream tree. `DataType::Decimal128`
  takes `(u8, i8)`, so the implementer must also decide the `u32` conversion. The scenario states
  neither rule, so no pass/fail test can be written for an absent precision, and task 2.2's
  instruction to "Cover every `ExaType` variant" cannot be satisfied as written. Decision [4]
  removed every fallback, which makes the unspecified case a run-time failure rather than a benign
  default. Reachability against a real Exasol output column is not established either way; the type
  forces the decision regardless.
- Fix: Add one clause to the scenario in
  `datafusion-scan/scan-execution-value-conversion/spec.md` stating what
  `ExaType::Numeric` with an absent `precision` or `scale` maps to, and what an out-of-range `u32`
  maps to. Route both to `Utf8`, which `is_string_family_exatype`
  (`language-container-rs` `crates/exa-udf-runtime/src/rowset.rs:659-673`) already admits for a
  `Numeric` column, so the emit succeeds rather than failing the call. Add the case to task 2.7's
  `ExaType` sweep.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `datafusion-scan/scan-execution-partial-agg/spec.md` § Scenarios
- Issue: the partial-aggregate scenario states the coercion and the group-key skip but states no
  failure rule, while the sibling value-conversion scenario requires the call to fail when
  `output_column_count()` disagrees with the batch column count or `output_column(i)` errors. The
  partial-aggregate path emits through `ctx.emit` rather than `emit_stream`, so it does not inherit
  that rule. Arity for that path is group keys plus partial columns on both sides, so the check is
  cheap.
- Fix: Add one clause to the scenario in
  `datafusion-scan/scan-execution-partial-agg/spec.md` requiring the same
  named failure when `output_column_count()` does not equal the partial batch's column count, or
  when `output_column(i)` errors for a partial-aggregate column.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `datafusion-scan/scan-execution-value-conversion/spec.md` § Scenarios, the clause ending "preserves the behavior the removed type-string path gave an unrecognized declaration"
- Issue: one clause carries five separate mappings plus two justifications, and `speq plan validate`
  flags the scenario at 6 AND steps. The trailing justification refers to behavior of code the same
  delta deletes, so a reader cannot check the claim from the spec alone.
- Fix: Split the clause in `datafusion-scan/scan-execution-value-conversion/spec.md` into one
  clause for the non-decimal variant mappings and one for the `Utf8` catch-all, and delete the
  trailing "preserves the behavior the removed type-string path gave an unrecognized declaration".

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: plan.md § Implementation Tasks task 4.3; plan.md § Design § Non-Goals ("No new seed fixture"); `datafusion-scan/scan-execution-partial-agg/spec.md` § Scenarios
- Issue: task 4.3 states its purpose as "so `AVG`, `STDDEV` and `MIN`/`MAX` are exercised on a
  non-`double` column for the first time", and the partial-aggregate scenario names "a
  `Decimal128(37,s)` sum over a wide decimal, and a `Decimal128(p,0)` minimum for a column declared
  `DECIMAL(p,0)` with `p` at most 18" among the cases. Neither is reachable. No seeded table and no
  bench dataset carries a scale-0 decimal column, and the widest decimal in the fixtures is
  `decimal(21,1)`, below the precision-27 threshold at which `sum_emit_type`'s `DECIMAL(36,s)` cap
  is exceeded. `typed_distinct_probe` carries `decimal(9,2)` and `decimal(20,4)`
  (`crates/lakehouse-engine/tests/common/seed.rs`), and MIN over either is `Value::Numeric` into an
  `ExaType::Numeric` column, which `column_accepts` admits, so task 4.3 exercises no mismatch for
  MIN/MAX. § Non-Goals forbids the fixture change that would make it reachable. The result is that
  mismatches 2 and 3 reach the engine with unit coverage only, while plan.md § Impact asserts
  "Without the partial-aggregate fix in this plan the SDK bump alone would fail each of them",
  implying all four are demonstrated.
- Fix: Choose one and make it explicit. Either lift "No new seed fixture" from plan.md § Design
  § Non-Goals and extend `crates/lakehouse-engine/tests/common/seed.rs`'s `typed_distinct_probe`
  with a `decimal(18,0)` column and a `decimal(30,2)` column, then restate task 4.3 to name the
  queries that hit mismatches 2 and 3; or keep the non-goal, restate task 4.3 to claim only the
  `AVG`/`STDDEV`-over-integer case it can reach, and add one sentence to plan.md § Impact recording
  that mismatches 2 and 3 carry unit coverage only because no fixture reaches them. In either case
  make the coverage split visible in plan.md § Verification § Scenario Coverage.

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Implementation Tasks task 1.6; plan.md § Parallelization
- Issue: task 1.6 is the plan's hard gate and it is numbered into "### 1. SDK and SLC version
  lockstep" while § Parallelization assigns it to group B. The task compensates with "Runs FIRST
  inside group B, not in group A", but an implementer working section by section reads section 1 as
  one unit. The gate content itself is sound: it is ordered first in its group, its `WARNING` states
  that failure stops the plan, and it is executable after group A alone because the field still
  exists at that point.
- Fix: Renumber task 1.6 to 2.0 in plan.md § Implementation Tasks, move it under
  "### 2. The declared output column becomes the emit authority" as its first item, and update the
  § Parallelization group B Tasks cell to `2.0, 2.1-2.10`. Delete the now-redundant sentence "Runs
  FIRST inside group B, not in group A."

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Implementation Tasks task 3.2
- Issue: task 3.2 deletes the field from five construction sites but leaves six doc comments naming
  the removed field: `adapter/pushdown/mod.rs:357`, `:456`, `:743`, `:763`, `:789`, and
  `adapter/pushdown/support.rs:312`. `mod.rs:455-458` and `support.rs:312` are function-level doc
  comments describing the spec shape, so they become wrong, not merely stale. `mod.rs:456` is the
  sharpest case: it records that the aggregate path "emits via the freely-coercing Value path, not
  the strict emit_batch IPC path", which is the exact assumption SDK 0.26.0 removes. One further
  reference sits outside the crate, in a comment at `bench/import_ceiling.sh:42`.
- Fix: Extend task 3.2 in plan.md to require updating the doc comments at
  `adapter/pushdown/mod.rs:357`, `:456`, `:743`, `:763`, `:789` and
  `adapter/pushdown/support.rs:312`, keeping them to one line each per CLAUDE.md, and correcting
  `mod.rs:456`'s "freely-coercing Value path" claim. Add `bench/import_ceiling.sh:42` to the same
  task.

#### [TRACEABILITY_GAP] ADVISORY
- Location: decision-log.md § Design Decisions [8]; plan.md § Implementation Tasks task 1.4
- Issue: decision [8] rejects moving the hardcoded default because "it leaves the same drift class
  that let the value fall two releases behind the workspace pin", then task 1.4 does exactly that
  for the second file: it derives `bench/run.sh`'s default from the workspace pin but sets
  `deploy/scripts/secrets.sh`'s `BENCH_SLC_VERSION` to a hardcoded `0.26.0`. `secrets.sh:46-49`
  writes that value into the generated `bench/.env`, and `bench/run.sh:339` reads the `.env` before
  applying its default (`SLC_VERSION="${BENCH_SLC_VERSION:-...}"`), so the generated value wins for
  every remote run. The derived default therefore never applies on the path where the drift
  originally occurred.
- Fix: Change task 1.4 in plan.md to omit the `BENCH_SLC_VERSION` line from the heredoc
  `deploy/scripts/secrets.sh` writes, so `bench/run.sh`'s derived default governs both targets, and
  record that change in decision-log.md [8] in place of "moves to `0.26.0`". If the remote path
  needs an explicit pin, derive it in `secrets.sh` from the same `Cargo.toml` expression
  `Makefile:129` uses.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Implementation Tasks task 2.6; plan.md § Design § Patterns, row "One coercion rule, two emit paths"
- Issue: § Patterns commits to one coercion rule across both emit paths, but task 2.6 does not say
  which function the partial-aggregate path calls. Task 2.3 makes `coerce_batch_to_exa_types` take
  the declared column metadata and task 2.4 makes it arity-strict, while task 2.6 needs to skip the
  leading group-key columns. An implementer reading task 2.6 alone has a standing invitation to
  write a second `ExaType`-to-Arrow mapping inside `partial_agg.rs`, which reinstates the exact
  replication this plan removes. The arity is in fact identical on both sides of the partial-agg
  path (group keys plus partial columns), so a shared function with a skip offset works.
- Fix: Rewrite task 2.6 in plan.md to name the shared entry point: the partial-aggregate path calls
  the same `coerce_batch_to_exa_types` with a group-key offset, and `target_arrow_type` stays the
  only place an `ExaType` maps to an Arrow type. Add "and MUST NOT re-derive the Arrow target
  outside `target_arrow_type`" to the scenario in
  `datafusion-scan/scan-execution-partial-agg/spec.md`.

#### [TACTICAL_SHORTCUT] ADVISORY
- Location: decision-log.md § Design Decisions [2] and [3]; plan.md § Design § Consequences
- Issue: two ADR promotions are marked, and the split does not follow the stated rule that only a
  genuinely architectural decision is promoted. Decision [1] is architectural and correctly
  promoted: it names one owner for the declared output type. Decision [2] is a scope-and-sequencing
  decision whose technical content is decision [1]'s mechanism restated, so promoting it records the
  same architecture twice. Decision [3], which deletes the replicated DECIMAL precision binning, and
  decision [4], which removes the fallback and makes drift fail the call, are both durable contract
  changes and are not promoted. The routine `exasol-udf-sdk` 0.26.0 bump is correctly left in
  § Migration with no ADR.
- Fix: In decision-log.md, set decision [2] `Promotes to ADR` to `no` and fold its durable content
  (the SDK now validates every emitted row against its declared column, so both emit paths must
  conform) into decision [1]'s Rationale. Set decision [4] `Promotes to ADR` to `yes`, since the
  no-fallback contract is the decision a future reader must find.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Iceberg and Delta specification compliance
- Issue: CLAUDE.md requires quoting the relevant normative section, and the section quotes Iceberg
  `#### Schemas and Data Types` and Delta `Schema Serialization Format`. Both are generic
  schema-container text and neither bears on the two surfaces this plan actually touches: the
  DECIMAL-to-`ExaType` binning, whose source moves from a repo heuristic to the engine's reported
  bin, and the `TIMESTAMP(p)` collapse that the `type-mapping-timestamp-precision` delta restates.
  The conclusion is right, since `crates/lakehouse-engine/src/types/mapping.rs` is unchanged, but
  the quoted evidence does not support it.
- Fix: In plan.md § Iceberg and Delta specification compliance, replace the two generic quotes with
  the normative text that governs the touched surfaces: Iceberg `#### Primitive Types` for
  `decimal(P,S)` and for `timestamp` microsecond resolution, and the Delta protocol's primitive-type
  list for `decimal` and `timestamp`. State in one sentence that the plan changes where the scan
  reads an already-recorded Exasol declaration, not the Iceberg-to-Exasol or Delta-to-Exasol rule,
  so neither specification's conformance changes.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md:22, plan.md:38, plan.md:40; `datafusion-scan/type-mapping-timestamp-precision/spec.md`:31; decision-log.md:132
- Issue: `/speq:writing-guardrails` bans hyperbole and em dashes in governed prose. plan.md:22 ends
  "Two transmissions of one decision, with nothing enforcing agreement: textbook back-door
  information leakage" — "textbook" is an intensifier carrying no content. Newly authored em dashes
  appear at plan.md:38 ("**Goals** — one owner"), plan.md:40 ("**Non-Goals** — no change"), and in
  the rewritten GIVEN clause at `type-mapping-timestamp-precision/spec.md`:31. decision-log.md:132
  carries the filler "just" in "just to reach its Background". The remaining em dashes sit in
  recorded feature titles and in the verbatim `scan-execution-spec-reconstitution` scenario, and
  stay. The artifacts are otherwise clean: no semicolons, no contractions, no `e.g.`, `i.e.` or
  `etc.`, and the weak modals found are all inside reproduced interview questions.
- Fix: Delete "textbook" from plan.md:22. Replace the em dash at plan.md:38 and plan.md:40 with a
  colon. Replace the em dash at `datafusion-scan/type-mapping-timestamp-precision/spec.md`:31 with
  a comma. Delete "just" from decision-log.md:132.

# Plan Review Findings: change-udf-sdk-upgrade (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 13 (Blockers: 3, Advisory: 10)
- Intent Fidelity blockers: 0

## Premortem

Three ways this plan fails after it ships.

1. **A false STOP sends a correct design back.** Task 5.2(a) declares
   `CAST(ts AS TIMESTAMP(9))` on the `2025.1.16` leg. That leg's pushdown echo omits
   `fractionalSecondsPrecision` (a behaviour `[C2]` measured on `2025.2.1`, not on `2025.1.16`), so
   `exasol_type_from_json` declares a bare `TIMESTAMP`, the scan emits `Millisecond`, and
   `COUNT(DISTINCT)` returns 2 instead of 4. The implementer reads "STOP and report if either is
   rejected: the design, not the implementation, then needs revision" and returns the whole design
   for revision over an engine-build difference the assertion cannot separate from an SLC rejection.
2. **The defect is re-introduced by the plan's own delta.** Group A finishes, `cargo check` is green,
   then `cargo test` fails on `exasol_type_to_arrow_parses_timestamp_precision`. The implementer
   reads the delta clause "it SHALL keep its `TIMESTAMP WITH LOCAL TIME ZONE` arm and its recorded
   test coverage unchanged", concludes task 2.3 was wrong, and restores the fixed microsecond answer,
   restoring the two disagreeing tables the plan exists to remove.
3. **The advertised parallelism deadlocks.** Groups A and B launch together, as the Parallelization
   table authorises. Group B writes task 4.3's failing test in
   `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` and cannot compile the crate,
   because group A's task 1.1 has already moved the SDK pin and every `ExaType::Timestamp` site in
   the same crate is mid-rewrite.

## Intent Fidelity

Verified against the brief, not taken on faith:

- The three findings beyond the original brief hold. `timestamp_to_micros` is this repo's own private
  function at `crates/lakehouse-engine/src/scan/convert.rs:162`, its `TimeUnit::Nanosecond` branch
  divides by 1,000, and `crates/lakehouse-engine/src/scan/` carries no `use iceberg::` in any
  production file. iceberg-rust 0.10.0 maps `TimestampNs`/`TimestamptzNs` to
  `Timestamp(Nanosecond, _)` and its INT96 visitor returns `None` for a nanosecond target, both
  confirmed in the vendored registry source. Deleting the attribution rather than rewording it is
  correct.
- Claim 4 is not a shortcut past this repo's verification discipline. `[C1]`, `[C2]` and `[C3]` exist
  verbatim in `specs/_recorded/2026-08-19-add-timestamp-precision-versioning/decision-log.md:251-310`
  and are LIVE captures, not documentation or memory. They bound the DECLARATION side only, and the
  plan still measures every EMIT-side claim live in group C. The version extrapolation inside them is
  a separate defect, raised under Feasibility.
- The decline cost is not understated. The inner fan-out spec really is built with `limit: None` and
  `order_by: Vec::new()` (`adapter/pushdown/joins/sql_builders.rs:1138-1139`), the outer wrapper
  really does own select list, GROUP BY, HAVING, ORDER BY and LIMIT
  (`outer_wrapper_clauses`, `sql_builders.rs:316-360`), and the WHERE predicate really does travel
  inside the scan spec on this route (`mod.rs:656` passes `filter.clone()` and `declined_filter:
  None`, mutually exclusive by `classify_where_filter`, `support.rs:1178-1189`). The cost is stated
  in plan.md § Design, plan.md § Impact item 2, decision-log `[5]`, the delta Background and a delta
  SHALL clause.
- Task 5.2's STOP is real. Decision-log `[1]` repeats it ("a rejected emit at any width stops the
  plan"), § Verification names the Arrow-IPC acceptance as one of three claims only a running engine
  answers, and the delta clause at `:188` requires local measurement "rather than carried over from
  the SDK's upstream fixture, because ... the upstream fixture exercises the `Value` emit path rather
  than this Arrow one". The gap the planner flagged is handled honestly.
- Issue #411 appears in `plan.md` nowhere. `decision-log.md` carries it at `:50` (the interview
  answer that closes it), `:191`/`:192` and `:204` (historical review findings, each followed by an
  explicit `Superseded by` line). No `tracked as`, `defer`, `follow-up` or `separate issue` string
  occurs in `plan.md` or any of the four deltas.

#### [SCOPE_CREEP] ADVISORY
- Location: `specs/_plans/change-udf-sdk-upgrade/notes/planning.md:65`
- Issue: the user constrained #411 to historical decision-log entries alone. The hand-off note
  reintroduces it outside that scope: "`gh issue create` -> issue #411 (later CLOSED at the user's
  instruction; the arm it tracked is now ...". The superseding parenthesis is present, so the reader
  is not misled, but the location is outside the one the user allowed.
- Fix: Delete the `#411` reference from `notes/planning.md:65` and replace it with a pointer to
  `decision-log.md` § Interview, which already records the decision.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: `plan.md` § Implementation Tasks task 5.2; `datafusion-scan/type-mapping-timestamp-precision/spec.md:74-77`
- Issue: task 5.2 is the plan's only design-gating measurement, and it cannot distinguish its two
  failure modes. Task 5.2(a) says "`[C2]` confirms 2025.x echoes `fractionalSecondsPrecision`, so
  the generated `EMITS` declares `TIMESTAMP(9)`", then asserts only values and
  `COUNT(DISTINCT) == 4`. `[C2]` was captured on `2025.2.1`
  (`specs/_recorded/2026-08-19-add-timestamp-precision-versioning/decision-log.md:276-296`), and the
  plan runs the unclamped leg on `2025.1.16` (`docker-compose.yml:115`, `Makefile:3`, pinned by
  commit 0f5291d). The delta generalises the single capture to "2025.x" at `:74-77`. If `2025.1.16`
  strips the key, the EMITS declares a bare `TIMESTAMP`, the scan emits `Millisecond`, and
  `COUNT(DISTINCT)` returns 2. That failure is indistinguishable from the SLC rejecting a nanosecond
  Arrow-IPC block, which the same task defines as a STOP that sends the design back for revision.
  The identical defect class is already recorded against this plan: "The CAST probe could not observe
  the SLC precision check" (`decision-log.md` § Review Findings).
- Fix: In `plan.md` task 5.2, add a precondition step before (a), (b) and (c): read the generated SQL
  through `isolated_pushdown_statement`
  (`crates/lakehouse-engine/tests/common/e2e_harness.rs:348`) and assert the `EMITS` clause declares
  the expected `TIMESTAMP(p)` literal for each width, failing with a message that names a stripped
  `fractionalSecondsPrecision` echo as the cause. State that only a failure AFTER that assertion
  passes counts as the STOP condition. In
  `datafusion-scan/type-mapping-timestamp-precision/spec.md:74-77`, scope `[C2]` and `[C1]` to the
  engine builds they were captured on (`2025.2.1`, `8.29.13`) and mark the generalisation to
  `2025.1.16` as an assumption group C confirms rather than a stated fact.

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: `plan.md` § Parallelization group C; § Dependencies
- Issue: the assumption the whole design rests on, that the SLC's strict Arrow-IPC feed accepts a
  `Timestamp(Millisecond, None)` and a `Timestamp(Nanosecond, None)` column at all, is checked only
  after all thirteen tasks of group A are implemented, because group C depends on A. The plan states
  that only the microsecond unit has ever been fed to the SLC from this repo. A rejection invalidates
  `TimestampPrecision::arrow_unit`, `target_arrow_type`, `exasol_type_to_arrow` and the partial-agg
  conversion together. § Dependencies lists no cheaper earlier check, although the SLC's block reader
  is public source in `language-container-rs` v0.28.1.
- Fix: Add a task to group A, before task 3.1: read `language-container-rs` v0.28.1's Arrow-IPC block
  reader and record in `decision-log.md` which Arrow `TimeUnit`s it accepts for an Exasol `TIMESTAMP`
  output column. Note in `plan.md` § Parallelization that this is source evidence only and does not
  replace task 5.2's live measurement.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `datafusion-scan/type-mapping-timestamp-precision/spec.md:192`; `plan.md` task 2.3 and task 2.5
- Issue: the delta clause ends "it SHALL keep its `TIMESTAMP WITH LOCAL TIME ZONE` arm and its
  recorded test coverage unchanged". The recorded test coverage is
  `exasol_type_to_arrow_parses_timestamp_precision`
  (`crates/lakehouse-engine/src/types/mapping_tests.rs:469-474`), whose whole body is
  `let expected = Some(DataType::Timestamp(TimeUnit::Microsecond, None));` asserted for
  `TIMESTAMP(0)`, `TIMESTAMP(6)` and `TIMESTAMP(9)`. Task 2.3 changes exactly that answer, so the
  clause requires the plan to keep a test the plan's own task makes red. Task 2.5's list of tests to
  update does not name it, task 2.3 does not mention it, and § Dead Code Removal does not carry it.
  Task 1.2's census gate does not catch it either: this is a test FAILURE, not a compile error, and
  that gate reads the compiler's output only.
- Fix: In `datafusion-scan/type-mapping-timestamp-precision/spec.md:192`, replace "and its recorded
  test coverage unchanged" with a clause requiring the recorded fixed-microsecond assertion to be
  REPLACED by a per-precision one. In `plan.md` task 2.5, add
  `exasol_type_to_arrow_parses_timestamp_precision` (`types/mapping_tests.rs:469-474`) to the list of
  tests to rewrite, asserting `Millisecond` for `TIMESTAMP(0)`, `Microsecond` for `TIMESTAMP(6)` and
  `Nanosecond` for `TIMESTAMP(9)`, plus the bare `TIMESTAMP` case. Add a row for its old assertion to
  `plan.md` § Dead Code Removal.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `datafusion-scan/type-mapping-timestamp-precision/spec.md:12-29` (DELTA:REMOVED) and `:192`; `plan.md` task 2.3
- Issue: task 2.3 requires `exasol_type_to_arrow` to treat "a bare `TIMESTAMP` as `3`". No delta
  clause governs that case. Clause `:192` covers the `TIMESTAMP(p)` type STRING only, and clause
  `:185` covers a reported numeric `precision`, not an absent one. The fact the rule rests on,
  "Exasol's bare `TIMESTAMP` IS `TIMESTAMP(3)`", is deleted with the old Background block at `:16`
  and is restated nowhere in the new one. The bare declaration is the 8.x catalog arm, so this is
  the default path on one of the two engines the plan measures.
- Fix: Add a Background bullet to `datafusion-scan/type-mapping-timestamp-precision/spec.md`'s
  DELTA:NEW block restating that Exasol's bare `TIMESTAMP` is `TIMESTAMP(3)`, and extend clause
  `:192` with a sentence requiring a bare `TIMESTAMP` type string to resolve to the same unit as
  `TIMESTAMP(3)`.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `datafusion-scan/type-mapping-timestamp-precision/spec.md:163`; `plan.md` § Verification > Scenario Coverage
- Issue: the new clause "a `timestamptz_ns` column SHALL keep its nanosecond digits through that cast
  on an engine declaring `TIMESTAMP(9)`, because the zone flattening changes the timezone field only
  and MUST NOT change the unit" has no implementing test. Task 3.6 adds a `coerce_column` test for a
  `Timestamp(Nanosecond, None)` column only. The ZONED case,
  `Timestamp(Nanosecond, Some("UTC"))` cast to `Timestamp(Nanosecond, None)`, is the one the clause
  names, and § Verification maps the whole `timestamptz` scenario to
  `iceberg_types_map_to_exasol_type`, a declaration test that never reaches the emit boundary.
- Fix: Extend `plan.md` task 3.6 to add a second `coerce_column` case covering a
  `Timestamp(Nanosecond, Some("UTC"))` column declared `TIMESTAMP(9)`, asserting the result is
  `Timestamp(Nanosecond, None)` with all nine digits and the same instant. Add the corresponding row
  to § Verification > Scenario Coverage.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: `datafusion-scan/type-mapping-timestamp-precision/spec.md:192`; `plan.md` task 2.3 and § Dead Code Removal
- Issue: both places justify changing `exasol_type_to_arrow` on the ground that it is "the single
  source of truth for the Arrow type the strict `emit_batch` feed accepts". It has no production call
  site: every reference in the workspace is a doc comment (`types/mapping.rs:67`, `:373`) or a test in
  `types/mapping_tests.rs`. The recorded `datafusion-scan/type-mapping-module-structure` spec already
  records this for its paired inverse, "`arrow_to_exasol_type` SHALL be retained as `pub` even though
  it has NO call site anywhere in the crate". The real emit-path owner is `target_arrow_type`
  (`scan/emit.rs:230-251`). Recording the false role permanently mis-states where the emit decision
  lives.
- Fix: In `datafusion-scan/type-mapping-timestamp-precision/spec.md:192`, replace "the single source
  of truth for the Arrow type the strict `emit_batch` feed accepts" with a statement that
  `exasol_type_to_arrow` is a documented compliance surface with no production call site, kept in
  agreement with `target_arrow_type` so the two cannot drift. Make the same correction in `plan.md`
  task 2.3 and § Dead Code Removal's closing paragraph.

## Task Breakdown

#### [CLUSTER_INCOHERENCE] BLOCKER
- Location: `plan.md` § Parallelization, groups A and B; task 4.3
- Issue: "Groups A and B share no file and no spec delta, so they may run concurrently" is false for
  task 4.3. That task adds a test to
  `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`, inside the very crate group A
  rewrites. Group A's task 1.1 moves the SDK pin, after which every `ExaType::Timestamp`,
  `ExaType::Numeric`, `ExaType::String` and `ExaType::Char` site in `lakehouse-engine` fails to
  compile until group A's tasks 3.1 through 3.8 land. Group B cannot run a failing test first, or any
  test at all, in that window. Tasks 4.1 and 4.2 are genuinely independent, because
  `crates/vs-expression` imports only `exasol_udf_sdk::error::UdfError`, which the bump leaves
  byte-identical.
- Fix: In `plan.md` § Parallelization, move task 4.3 out of group B. Either place it in a new group
  that depends on A and B, or add it to group C ahead of task 5.1 and extend group C's Knowledge cell
  with `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`. Rewrite the paragraph
  claiming both groups are knowledge-complete so it states that group B owns `crates/vs-expression`
  alone.

#### [TRACEABILITY_GAP] ADVISORY
- Location: `plan.md` task 3.3; § Dead Code Removal
- Issue: `micros_to_naive_datetime` (`crates/lakehouse-engine/src/scan/convert.rs:192`) has exactly
  one caller, `convert.rs:134`, the line task 3.3 replaces. The helper becomes dead once task 3.3
  lands. It appears in neither task 3.3 nor § Dead Code Removal, so it survives as an unused private
  function and a clippy warning against the § Checklist row requiring "0 errors and 0 warnings".
- Fix: Extend `plan.md` task 3.3 with "delete `micros_to_naive_datetime` (`convert.rs:192`), whose
  only caller this task replaces", and add a matching row to § Dead Code Removal.

#### [TRACEABILITY_GAP] ADVISORY
- Location: `plan.md` § Parallelization group C Knowledge; task 5.2, task 5.4
- Issue: `crates/lakehouse-engine/tests/common/timestamp_precision.rs` is the deliberate independent
  oracle for this suite, and it carries exactly two arms, `ExpectedTimestampPrecision::MICROSECOND`
  and `::MILLISECOND` (`:35-43`), with its own `year < 2025` rule at `:64-68`. Tasks 5.2 and 5.4 add
  nanosecond assertions with no arm to compute their expectation from. Group C lists the file as
  Knowledge but no task changes it, so the implementer will either hard-code the nanosecond
  expectation in the test body, defeating the oracle's purpose, or call the production rule under
  test.
- Fix: Add a task to group C, before task 5.2, extending
  `crates/lakehouse-engine/tests/common/timestamp_precision.rs` with a NANOSECOND arm and a
  source-width parameter, keeping its version rule an independent re-implementation rather than a
  call into `EngineTimestampSupport`.

## Design Depth

The two-type split is sound against the diagnostic. Each type answers one question in one sentence,
`engine.clamp(source).declaration()` is easier to call than the branch it replaces, and the emit
boundary reading `from_declared_digits(p).arrow_unit()` removes the second precision-to-unit table
that is the recorded defect. `EngineTimestampSupport` takes a `&str`, so the type-mapping module
still performs no I/O and reads no ambient state.

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: `plan.md` § Design > Decision; tasks 2.1, 2.3, 3.5
- Issue: the plan's central claim is that one owner makes the two sides' agreement structural. That
  holds for the precision-to-`TimeUnit` table and not for the `TIMESTAMP(p)` STRING SYNTAX, which
  stays replicated across four sites. `TimestampPrecision::declaration()` produces it (task 2.1),
  `exasol_type_to_arrow` parses it (task 2.3, "parsing `p` from the type string"),
  `exasol_type_from_json` (`types/mapping.rs:660`) produces it independently from Exasol's echo, and
  `declared_type_name` in `tests/scan_fixture/mod.rs` produces it a fourth time (task 3.5). A change
  to the rendered form has to be made in every one. The plan neither states this residual replication
  nor removes it.
- Fix: Add a bullet to `plan.md` § Design > Decision naming the `TIMESTAMP(p)` string syntax as the
  one decision the split does NOT consolidate, listing the four sites. Either schedule a
  render-and-parse pair beside `TimestampPrecision` that all four read, or record the replication as
  a deliberate, named trade-off in the delta's Background so a later reader does not read the
  single-owner claim as covering it.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: `sql-comprehension/vs-expression-translator-cast/spec.md:26-30` and `:44-47`; `plan.md` § Design > Decision routing table
- Issue: three code locators in a Background bullet destined for the permanent library are wrong, and
  one description mischaracterises the code. `support.rs:1221` is a blank line between
  `is_valid_emits_output_type` (1218-1220) and a doc block; `project_columns` is at 1242-1418 and the
  `render_expression_safe` `None` branch is at 1396-1399. There is no "`function_scalar_cast` arm":
  the pattern at 1361-1382 covers sixteen node types at once, and five other conditions also set
  `needs_full_fallback`. `build_qualified_single_table_fallback_sql` is at 1032, not 990.
  `referenced_column_projection` is at 920, not 1098, which is the doc start of a different function.
  The routing conclusion is correct; only the navigation is not.
- Fix: In `sql-comprehension/vs-expression-translator-cast/spec.md:26-30` and `:44-47`, and in
  `plan.md` § Design > Decision's routing table, correct the four line references to
  `support.rs:1396-1399`, `sql_builders.rs:1032`, `sql_builders.rs:920` and
  `sql_builders.rs:1138-1139`, and replace "`project_columns`'s `function_scalar_cast` arm" with
  "`project_columns`'s shared scalar-and-predicate arm, one of six conditions that set
  `needs_full_fallback`".

#### [PROSE_BLOAT] ADVISORY
- Location: `plan.md` (18 prose lines); `decision-log.md` § Review Findings
- Issue: two guardrail deviations. First, `/speq:writing-guardrails` bans semicolons in governed
  prose, and `plan.md` carries about eighteen outside tables, for example `:91`, `:120`, `:177`,
  `:208`, `:233`, `:370`, `:455`, `:479`. Second, the user's standing instruction is that the
  decision log stays concise and is not a changelog. § Review Findings carries five entries from the
  superseded planning round, each fully retracted by a `Superseded by` line, totalling about
  sixty-five lines that describe decisions no longer in force. No em dash appears in any new text.
- Fix: Split every semicolon-joined sentence in `plan.md` prose into two sentences. In
  `decision-log.md` § Review Findings, compress each of the five superseded entries to its Finding
  line plus its `Superseded by` line, deleting the `Direction change` paragraphs, which describe a
  plan shape no longer present.

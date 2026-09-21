# Decision Log: change-udf-sdk-upgrade

## Interview

**Q:** Issue #405 says the timestamp-precision question must be resolved by this plan, not deferred.
Does Exasol truncate an emitted microsecond value to a declared `TIMESTAMP(p)`, and does the 8.x
bare-`TIMESTAMP` trade-off still hold? No Exasol runs locally right now. How should the plan handle
it?

**A:** Plan a live E2E task. Add an implementation task that brings up Exasol Docker, declares
`TIMESTAMP(p)` EMITS columns, emits microsecond values, and records the observed
truncation or rounding behaviour. Write the spec delta's precision text to match what that task
confirms. Log the decision here, under this plan and this issue, rather than deferring it to a new
issue.

**Q:** The live check's outcome is unknown. Should the plan commit now to coercing emitted values to
the declared precision unless the E2E task shows it is unnecessary, or keep today's fixed microsecond
emit unless the E2E task shows it is unsafe?

**A:** Keep the fixed microsecond emit by default. Today's behaviour is the baseline. Add coercion
only if the E2E task proves Exasol produces wrong or rejected results without it. Minimise the
changed surface for what is fundamentally a dependency bump.

**Q:** Upstream `language-container-rs` E2E tests establish that an Exasol UDF can emit at precision
0, 3, 6 and 9 on Exasol 2025+, and at 3 only on Exasol 8.29. Both Iceberg and Delta default to
microsecond but Iceberg v3 also has `timestamp_ns`/`timestamptz_ns`, and Parquet encodes
MILLIS/MICROS/NANOS. Does the fixed microsecond emit still stand?

**A:** No. Emit at whatever precision is needed, 3, 6 or 9, except on Exasol 8.29, which always emits
3. Do not special-case only the precisions Iceberg and Delta happen to use today. This supersedes
the second answer above.

**Q:** `iceberg_primitive_to_exasol` collapses all four Iceberg timestamp variants onto one
declaration while `iceberg_primitive_to_arrow` already distinguishes them, so a `timestamp_ns`
column's nanosecond digits are destroyed at the emit boundary. Is that in scope?

**A:** Yes, in this plan, under this issue. It is a silent gap, unlike the 8.x millisecond
truncation, which is a recorded trade-off.

**Q:** A separate recorded spec, `sql-comprehension/vs-expression-translator-cast`, asserts an
unverified SHALL: that Exasol truncates an up-snapped `CAST(x AS TIMESTAMP(p))` back to the
requested `p`. `snap_timestamp_precision` approximates, the adapter declares the literal `p`, and
nothing ever measured the claim. Fix or verify?

**A:** Fix. Decline the node in the DataFusion dialect for `p` outside `{0,3,6,9}` so it routes
through the adapter's own Exasol-dialect wrapper, where the literal `p` renders verbatim. State the
decline mechanism precisely and cite the code; do not hand-wave whether the rest of the query stays
accelerated.

**Q:** Issue #411 tracked the above-microsecond CAST arm as a deferred limitation. Keep it?

**A:** No. No new ticket; fold it into this plan. Every open item from this redesign lands as a task
or a stated, evidence-based spec claim here.

## Design Decisions

### [1] A timestamp is emitted at its source precision, clamped to what the engine can emit

- **Decision:** The catalog declaration and the emit-boundary Arrow unit both follow the COLUMN's
  source width (millisecond, microsecond or nanosecond), clamped by what the running engine can
  emit: 3 only on Exasol 8.x, 3/6/9 on 2025.x and later. On the catalog path DECLARED therefore
  equals EMITTED on both engine arms, so no component truncates after the scan.
- **Alternatives:** Keep the fixed `Timestamp(Microsecond, None)` target at every declared
  precision. Rejected: it destroys every nanosecond digit of an Iceberg `timestamp_ns` column
  through the strict cast in `coerce_column`, and it makes the 8.x arm an unmeasured reliance on
  Exasol narrowing a finer value rather than a declared narrowing.
- **Rationale:** Under 0.26.1 the fixed target was a limit inherited from a type carrying no
  precision. Under 0.28.1 the precision is readable, and the two recorded specs stated a reason that
  was never verifiable either way. Group C measures all three widths against 2025.1.16 and 8.29.13;
  a rejected emit at any width stops the plan.
- **Supersedes:** "The scan keeps a fixed microsecond Arrow emit target at every declared TIMESTAMP
  precision".
- **Promotes to ADR:** yes

### [2] Two owners for the two precision axes, not one enum with a third variant

- **Decision:** `TimestampPrecision` owns the per-COLUMN source width, its declaration string, its
  Arrow `TimeUnit`, and the inverse mapping from a declared Exasol precision. `EngineTimestampSupport`
  owns the per-REQUEST version rule and the clamp. A producer writes
  `engine.clamp(source).declaration()`.
- **Alternatives:** Add a `Nanosecond` variant to the existing enum. Rejected: the defect IS a
  request-scoped value standing in for a column-scoped one, and a third variant on the same type
  preserves that confusion.
- **Rationale:** The emit boundary reads the same `TimestampPrecision` table in the other direction,
  so the declaring side and the emitting side cannot disagree about what `TIMESTAMP(9)` means. One
  owner per decision is what makes the agreement structural rather than a convention.
- **Promotes to ADR:** yes

### [3] The source width is three values, not the ten Exasol precisions

- **Decision:** `Millisecond`, `Microsecond`, `Nanosecond`, named format-neutrally.
- **Alternatives:** Carry the Exasol `p` directly, or carry an Iceberg/Delta-shaped descriptor.
- **Rationale:** Three is the real domain on both sides: Parquet encodes MILLIS/MICROS/NANOS and
  Arrow has the matching three sub-second units. A future direct-Parquet reader populates the same
  type unchanged, and no arm exists for a precision no source produces.
- **Promotes to ADR:** no

### [4] The emit unit is floored at millisecond for a declared precision below 3

- **Decision:** `from_declared_digits` maps `0..=3` to `Millisecond`, `4..=6` to `Microsecond` and
  `7..` to `Nanosecond`, so a mapping gap can only ever emit a value Exasol truncates.
- **Alternatives:** Map `p = 0` to Arrow `Second`, which matches the declaration exactly.
- **Rationale:** No emit path in this repo has fed the SLC a second-unit Arrow column, and the only
  declaration reaching `p = 0` is the exotic projected `CAST(x AS TIMESTAMP(0))`. One Exasol-side
  truncation is cheaper than an unexercised IPC shape.
- **Promotes to ADR:** no

### [5] An inexpressible CAST precision is declined, not approximated

- **Decision:** `render_expression`'s DataFusion-dialect TIMESTAMP arm renders `p` in `{0,3,6,9}`
  verbatim and declines every other value; `snap_timestamp_precision` is deleted. The node then
  routes into the adapter's own Exasol-dialect wrapper, which renders the literal `p`.
- **Alternatives:** Keep the snap and verify the up-snap truncation live. Rejected: it verifies a
  workaround rather than removing the approximation, and the snap contradicts that feature's own
  recorded rule that the rendered target set equals the set whose DataFusion result matches Exasol's.
- **Rationale:** Declining costs the query its per-shard LIMIT and top-N pushdown and makes Exasol
  compute every select-list item, while keeping the sharded fan-out, the referenced-column
  narrowing and the scan-side WHERE predicate. For six exotic precisions, exactness wins. The routing
  was read out of the code, not assumed: `support.rs:1221` for the select list, `support.rs:564` for
  WHERE, `grouped_agg.rs:189` for GROUP BY, and `topn.rs:130` for ORDER BY, which already renders in
  the Exasol dialect and is therefore unreachable for this decline.
- **Supersedes:** the unverified "Exasol SHALL truncate back to the requested `p`" clause.
- **Promotes to ADR:** yes

### [6] The partial-aggregate path's nanosecond truncation is fixed here, not left behind

- **Decision:** `timestamp_to_micros` (`scan/convert.rs:162`) is replaced by a conversion that keeps
  every digit the coerced array carries, and must not represent the instant as an `i64` count of
  nanoseconds, whose range covers only 1677-2262.
- **Alternatives:** Leave the `Value` path at microsecond.
- **Rationale:** Same defect class, reachable: `validate_agg_col_types` requires a numeric column for
  `SUM` and the statistical family only, so `MIN`/`MAX` over a timestamp column is pushed into this
  path. A `TIMESTAMP(9)` declaration honest on one emit path and false on the other is worse than
  either.
- **Promotes to ADR:** no

### [7] The iceberg-rust truncation rationale is deleted as false, not reworded

- **Decision:** The recorded reason for never targeting `TIMESTAMP(9)` is removed from the spec.
- **Rationale:** `timestamp_to_micros` is this repo's own function (`scan/convert.rs:162`) and does
  not exist in iceberg-rust. iceberg 0.10.0 preserves nanoseconds end to end
  (`src/arrow/schema.rs:671-677`, `src/arrow/int96.rs:93-106`), and the scan does not use that
  reader at all: `crates/lakehouse-engine/src/scan/` carries no `iceberg::` import and reads through
  DataFusion's own `ParquetSource`. Keeping a reworded version would preserve a false attribution.
- **Promotes to ADR:** no

### [8] Four spec deltas, not the two issue #405 names

- **Decision:** `datafusion-scan/scan-execution-partial-agg` and
  `sql-comprehension/vs-expression-translator-cast` get deltas alongside the two the issue names.
- **Alternatives:** Follow the issue's list. Rejected, because the issue states its own impact list
  is unconfirmed.
- **Rationale:** The partial-agg spec carries the same absent-payload clause and the second timestamp
  truncation site. The CAST spec carries an unverified SHALL that decision [5] removes. Leaving
  either would keep a false normative clause in the library.
- **Promotes to ADR:** no

### [9] The nanosecond source fixture is attempted, and its outcome is recorded either way

- **Decision:** Task 5.4 tries to seed an Iceberg v3 `timestamp_ns` column through the existing
  in-process seeder, using the `format-version` table PROPERTY. On failure it records the exact
  blocker and narrows the delta's nanosecond claims to the declaration, the Arrow unit, the SLC's
  acceptance of it, and value fidelity through it.
- **Alternatives:** Assume the fixture works, or skip it and claim the nanosecond path is verified.
- **Rationale:** `TableCreation::format_version` is a no-op against a REST catalog in iceberg-rust,
  so the fixture is plausible but not certain. Either outcome yields a stated, accurate claim; an
  unstated limit would be the silent gap this plan exists to close.
- **Promotes to ADR:** no

### [10] The absent-payload NUMERIC drift guard is deleted, not re-derived

- **Decision:** Both emit paths drop the branch that failed a `Numeric` column reporting an absent
  `precision` or `scale`. The out-of-range branch stays.
- **Alternatives:** Re-derive the pair from `ColumnInfo::type_name` to keep detecting an SLC-defaulted
  payload. Rejected, because it restores the replicated type-string parse that issue #399 deleted.
- **Rationale:** `ExaType::Numeric` no longer holds an absent value, so the branch is unreachable
  rather than merely unused. A valid Exasol NUMERIC declaration always carries both values, so the
  guard never fired in practice.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The CAST probe could not observe the SLC precision check

- **Finding:** `[INTENT_DRIFT]` on task 3.2 and § Design > Decision. The probe cast to
  `TIMESTAMP(p)` for `p` in {0, 3, 6, 9}, where `vs_expression::snap_timestamp_precision` is the
  identity, so DataFusion truncated the value before the emit boundary and the assertion passed
  regardless of engine behavior.
- **Direction change:** Task 3.2 becomes a catalog-column probe on an `exasol/docker-db:8.29.13`
  leg, where the version gate declares a bare `TIMESTAMP` and no CAST intervenes. The CAST-mismatch
  route the reviewer also offered was not taken; it is out of scope and tracked as issue #411.
- **Superseded by [1] and [5]:** the CAST route is now in scope, issue #411 is closed at the user's
  instruction, and the probe is a three-width emit measurement rather than a truncation observation.
- **Promotes to ADR:** no

### [plan-review] The spec delta pre-committed to an arm no task could reach

- **Finding:** `[INTENT_DRIFT]` on `type-mapping-timestamp-precision/spec.md` and § Parallelization.
  The live-engine SHALL covered both a below- and an above-microsecond declaration, while group B was
  forbidden from editing any delta, so an unreached claim would have recorded as fact.
- **Direction change:** Group B's Knowledge cell gains the delta and drops "edits no spec delta", and
  new task 3.4 (`[expert]`) corrects the live clause to the measurement. The SHALL now names the two
  arms actually run, `8.29.13` bare `TIMESTAMP` and `2025.1.16` `TIMESTAMP(6)`; the above-microsecond
  arm moved out into a tracked limitation citing #411.
- **Superseded by [1]:** the above-microsecond arm is now reached by task 5.2(a) and the tracking
  issue is gone. The rule the finding established stands unchanged: group C owns the live clauses
  and corrects them to the measurement (task 5.5).
- **Promotes to ADR:** no

### [plan-review] "Exasol already owns the truncation" was unscoped

- **Finding:** `[UNSTATED_ASSUMPTION]` on § Design > Decision and entry `[1]` § Alternatives. The
  sentence is true only on the catalog-column bare-`TIMESTAMP` path; on a projected CAST, DataFusion
  owns the truncation.
- **Direction change:** Both places now scope the claim to the catalog-column path this plan
  measures and state that the CAST path's DataFusion-side truncation is neither verified nor relied
  on. The same split is recorded as a Background bullet in the delta.
- **Superseded by [1]:** no component truncates on the catalog path any more, because declared now
  equals emitted. The Background bullet naming which component acts on which path is kept, corrected
  for the decline.
- **Promotes to ADR:** no

### [plan-review] The SLC-reported precision for a bare TIMESTAMP was assumed, never observed

- **Finding:** `[UNSTATED_ASSUMPTION]` on task 3.2, § Design > Decision, entry `[1]` § Decision and
  `type-mapping-timestamp-precision/spec.md:43`. All four stated `precision` 3 as fact. Every
  assertion task 3.2 lists passes identically whether the SLC reports 3 or 6, so the probe could not
  establish the one premise that makes it non-vacuous.
- **Direction change:** Task 3.2 observes the reported `precision` once by hand, through
  `ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS` with `%udf_debug_level debug`, and records it in entry
  `[1]`. A reported value of 6 or higher stops the plan, in task 3.2 and again in task 3.3's STOP
  list. The four `precision` 3 statements now name a value the run records rather than assumes.
- **Carried into task 5.3:** the hand observation stays, but it now selects which
  `from_declared_digits` arm the clamped engine takes rather than deciding whether the probe is
  vacuous.
- **Promotes to ADR:** no

### [plan-review] Switching EXASOL_IMAGE reused the previous engine's data volume

- **Finding:** `[HIDDEN_DEPENDENCY]` on tasks 3.1 and 3.2, § Manual Testing and § Checklist. The
  `exa-data` named volume (`docker-compose.yml:128-129`) holds the whole Exasol instance, and
  `make test-e2e` runs `cargo test` only (`Makefile:81-82`). An 8.29.13 container started over the
  2025.1.16 data directory does not come up, so the measurement never runs.
- **Direction change:** Every bring-up now runs `docker compose down -v` first: task 3.1, task 3.2
  as a labelled CAUTION naming the wiped `minio-data` re-seed, new task 3.5, both § Manual Testing
  commands and both § Checklist E2E rows.
- **Carried into tasks 5.1, 5.3 and 5.6** under the renumbering.
- **Promotes to ADR:** no

### [plan-review] Task 5.2 could not tell a stripped echo from an SLC rejection

- **Finding:** `[UNSTATED_ASSUMPTION]`. `[C2]` was captured on 2025.2.1, and the plan runs the
  unclamped leg on 2025.1.16. A stripped `fractionalSecondsPrecision` echo there yields the same
  `COUNT(DISTINCT) == 2` as an SLC rejection, which task 5.2 treats as a design STOP.
- **Fix:** Task 5.2 gains precondition step (p), an `EMITS`-literal assertion read through
  `isolated_pushdown_statement` before any value assertion. Only a failure after (p) passes is a
  STOP. The delta scopes `[C1]`-`[C3]` to their capture builds and names 2025.1.16 an assumption.
- **Promotes to ADR:** no

### [plan-review] A delta clause required keeping a test the plan turns red

- **Finding:** `[REQUIREMENT_CONFLICT]`. The clause held
  `exasol_type_to_arrow`'s "recorded test coverage unchanged", but
  `exasol_type_to_arrow_parses_timestamp_precision` asserts one fixed microsecond answer for
  `TIMESTAMP(0)`, `(6)` and `(9)`, which task 2.3 changes.
- **Fix:** The clause now requires that assertion REPLACED by a per-precision one. Task 2.5 names
  the test, and § Dead Code Removal carries the old assertion.
- **Promotes to ADR:** no

### [plan-review] Group B claimed a file its crate could not compile

- **Finding:** `[CLUSTER_INCOHERENCE]`. Task 4.3 writes in
  `adapter/pushdown/pushdown_tests.rs`, inside the crate group A rewrites, so it cannot run
  concurrently with A despite the table authorising it.
- **Fix:** Task 4.3 moves to group C, ahead of task 5.1. Group B is tasks 4.1-4.2 and
  `crates/vs-expression` alone.
- **Promotes to ADR:** no

### [plan-review] Step (p) confirms the echo, never the engine honoring it

- **Finding:** `[UNSTATED_ASSUMPTION]`. Step (p) reads the adapter's generated SQL, so it confirms
  `[C2]` alone. An accept-and-clamp on the unmeasured 2025.1.16 build, the behaviour `[C1]` and
  `[C3]` record on 8.29.13, passes step (p), raises no error, and returns too few distinct values.
  Task 5.2 called every post-(p) failure an SLC rejection and a design STOP.
- **Direction change:** Task 5.2 classifies each outcome into three named cases: an error after (p)
  is the only STOP, a successful query with too few distinct values is a 2025.1.16 engine limit, and
  a step (p) failure is a build difference. Step (p)'s failure message now names both a stripped
  `fractionalSecondsPrecision` echo and a `0A000 Feature not supported` CAST-target rejection. The
  delta scopes step (p) to `[C2]` and assigns `[C1]` to the VALUE assertion.
- **Promotes to ADR:** no

### [plan-review] The nanosecond fixture ran on the engine that cannot hold its assertion

- **Finding:** `[HIDDEN_DEPENDENCY]`. Task 5.4 followed task 5.3's 8.29.13 bring-up with no reset and
  asserted a `TIMESTAMP(9)` declaration, which the engine clamp removes. Its failure branch would
  have narrowed the permanent spec for a wrong-stack reason, and task 5.6 plus § Checklist demanded a
  green run the clamped arm cannot give.
- **Direction change:** Task 5.4 runs between tasks 5.2 and 5.3, keeping its label per task 4.3's
  convention, and guards on `live_engine_version`: `TIMESTAMP(9)` and surviving-distinct on the
  `>= 2025` arm, clamped `TIMESTAMP(3)` and collapsed-distinct on 8.29.13. Its failure branch covers
  a seeding error only. Task 5.6 requires each guarded assertion green on its own arm.
- **Promotes to ADR:** no

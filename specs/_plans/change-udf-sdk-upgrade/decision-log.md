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

## Design Decisions

### [1] The scan keeps a fixed microsecond Arrow emit target at every declared TIMESTAMP precision

- **Decision:** `target_arrow_type` reads `ExaType::Timestamp { precision }` and returns
  `DataType::Timestamp(TimeUnit::Microsecond, None)` for every `precision`. Task 3 measures on the
  running engine that the SLC accepts that column into a lower-precision declaration and that the
  engine truncates the value on its own side, on the CATALOG-COLUMN path: an Exasol 8.29.13 leg,
  where the version gate declares a bare `TIMESTAMP` for which the SLC reports a `precision` this
  plan records rather than assumes, and no CAST sits between the scan and the emit boundary. Task
  3.3 records both engine versions, the declaration arm each exercised, the observed value for each,
  and the SLC-reported `precision` task 3.2 observes by hand. A contradicting measurement stops the
  plan. A reported `precision` of 6 or higher stops it too, because the declaration is then no
  coarser than the emitted resolution.
- **Alternatives:** Coerce the Arrow column to the `TimeUnit` matching the declared `p`. Rejected on
  two grounds. Arrow's `TimeUnit` offers Second, Millisecond, Microsecond and Nanosecond only, so
  `p` in {1, 2, 4, 5, 7, 8} has no expressible target. And on the catalog-column bare-`TIMESTAMP`
  path this plan measures, Exasol itself owns the truncation, so a second truncation inside the scan
  adds a rule without adding a guarantee. That second ground is scoped to that path deliberately: on
  a projected `CAST(x AS TIMESTAMP(p))` the truncation happens inside DataFusion, because
  `vs_expression::snap_timestamp_precision` renders the cast into the scan's own DataFusion SQL. The
  plan neither verifies that path nor rests this decision on it (issue #411).
- **Rationale:** Under 0.26.1 the fixed target was a limit inherited from a type that carried no
  precision. Under 0.28.1 the precision is readable, so the same behaviour needs evidence behind it.
  The two recorded specs stated a reason that was never verified, because the old type made it
  unverifiable either way. This entry replaces that reason with a measurement, and every later reader
  of `target_arrow_type` depends on it.
- **Promotes to ADR:** yes

### [2] The absent-payload NUMERIC drift guard is deleted, not re-derived

- **Decision:** Both emit paths drop the branch that failed a `Numeric` column reporting an absent
  `precision` or `scale`. The out-of-range branch stays.
- **Alternatives:** Re-derive the pair from `ColumnInfo::type_name` to keep detecting an SLC-defaulted
  payload. Rejected, because it restores the replicated type-string parse that issue #399 deleted.
- **Rationale:** `ExaType::Numeric` no longer holds an absent value, so the branch is unreachable
  rather than merely unused. A valid Exasol NUMERIC declaration always carries both values, so the
  guard never fired in practice.
- **Promotes to ADR:** no

### [3] Three spec deltas, not the two issue #405 names

- **Decision:** `datafusion-scan/scan-execution-partial-agg` gets a delta alongside the two specs the
  issue names.
- **Alternatives:** Follow the issue's list. Rejected, because the issue states its own impact list
  is unconfirmed.
- **Rationale:** That spec's scenario "Every emitted partial-aggregate cell matches its declared
  output column" carries the same absent-payload clause, and `partial_agg_tests.rs` carries the
  matching test case. Leaving it would keep a false normative clause in the library.
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
- **Promotes to ADR:** no

### [plan-review] The spec delta pre-committed to an arm no task could reach

- **Finding:** `[INTENT_DRIFT]` on `type-mapping-timestamp-precision/spec.md` and § Parallelization.
  The live-engine SHALL covered both a below- and an above-microsecond declaration, while group B was
  forbidden from editing any delta, so an unreached claim would have recorded as fact.
- **Direction change:** Group B's Knowledge cell gains the delta and drops "edits no spec delta", and
  new task 3.4 (`[expert]`) corrects the live clause to the measurement. The SHALL now names the two
  arms actually run, `8.29.13` bare `TIMESTAMP` and `2025.1.16` `TIMESTAMP(6)`; the above-microsecond
  arm moved out into a tracked limitation citing #411.
- **Promotes to ADR:** no

### [plan-review] "Exasol already owns the truncation" was unscoped

- **Finding:** `[UNSTATED_ASSUMPTION]` on § Design > Decision and entry `[1]` § Alternatives. The
  sentence is true only on the catalog-column bare-`TIMESTAMP` path; on a projected CAST, DataFusion
  owns the truncation.
- **Direction change:** Both places now scope the claim to the catalog-column path this plan
  measures and state that the CAST path's DataFusion-side truncation is neither verified nor relied
  on. The same split is recorded as a Background bullet in the delta.
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
- **Promotes to ADR:** no

### [plan-review] Switching EXASOL_IMAGE reused the previous engine's data volume

- **Finding:** `[HIDDEN_DEPENDENCY]` on tasks 3.1 and 3.2, § Manual Testing and § Checklist. The
  `exa-data` named volume (`docker-compose.yml:128-129`) holds the whole Exasol instance, and
  `make test-e2e` runs `cargo test` only (`Makefile:81-82`). An 8.29.13 container started over the
  2025.1.16 data directory does not come up, so the measurement never runs.
- **Direction change:** Every bring-up now runs `docker compose down -v` first: task 3.1, task 3.2
  as a labelled CAUTION naming the wiped `minio-data` re-seed, new task 3.5, both § Manual Testing
  commands and both § Checklist E2E rows.
- **Promotes to ADR:** no

# Decision Log: fix-float-div-predicate-divzero

## Interview

Headless mode. No live interview took place. Issue
[#370](https://github.com/exasol-labs/lakehouse-engine-rs/issues/370) is the complete clarifying
record: it carries the exact pushed SQL fragments, the observed row counts against the native
oracle, the broadcast-join reproduction, and the distinction from issue #246. The orchestrator
passed the full issue body and the recorded permanent spec
`sql-comprehension/vs-expression-translator-float-div` as the baseline.

**Q:** Does this predicate-position gap have a real, safe, architecturally viable fix, or must it be
recorded as another accepted tracked exception, as the analogous `0/0` emit-boundary gap was?
**A (decided from the code, headless):** It has a fix. The reasoning is decision [1] below. The
argument that ruled out the emit-boundary fix does not transfer to the rendering layer.

**Q:** Where are filter predicates actually evaluated in the DataFusion scan physical plan?
**A (read from the code):** They are not built as `Expr` trees at all. `crates/vs-expression`
renders the predicate to a SQL string, the adapter puts it in `ScanSpec.common.filter`, and each
scan path splices that string into the SQL it hands to `SessionContext::sql`:
`raw_scan.rs::build_scan_sql` for the raw-row path, `join_scan.rs` for the broadcast join, and
`partial_agg.rs::build_partial_agg_sql_filtered` / `build_grouped_partial_agg_sql` for both
aggregate paths. DataFusion then plans and evaluates it. The rendering layer, not the physical
plan, is therefore where the outcome is decided, and it is one layer for every position and every
scan path.

**Q:** Does the fix need a live reproduction first?
**A:** Yes, per CLAUDE.md's verification discipline. Tasks 1.1 and 1.2 are parallelization group A,
the first group, and they reproduce #370 and measure the guarded shape against the local Docker
stack before any code change lands. Planning did not re-measure: issue #370 already carries the
live evidence for the unguarded shapes, each pushed filter confirmed from `EXPLAIN VIRTUAL`. The
guarded shape is not in #370 and is measured for the first time by task 1.2.

## Design Decisions

### [1] Fix the divide-by-zero at the rendering layer with a checked-division function

- **Decision:** The DataFusion dialect renders `FLOAT_DIV` as `vs_checked_float_div(<left>,
  <right>)`, a scalar function the scan session registers, instead of the `/` operator. The
  function raises when its own result is not finite. This supersedes the earlier ADR
  **"Record the divide-by-zero behaviour from measurement; do not emulate it"**
  (`specs/_decision/079-fix-float-div-truncation.md`) for the predicate case and for the `0/0`
  case. That ADR's measurements stand unchanged. Its conclusion that no fix exists does not.
- **Alternatives:**
  - Widen `arrow_value_at`'s `is_nan()` check to `!is_finite()`. Rejected, and already rejected in
    ADR `079`. The emit boundary cannot tell a computed non-finite value from one stored in the
    source table, and it never sees a predicate at all.
  - Render `NULLIF(<right>, 0)`. Rejected, and already rejected in ADR `079`. NULL is the wrong
    answer already observed, and it conflates a zero divisor with a NULL divisor.
  - Stop pushing any predicate that contains a division and apply it in the Exasol wrapper SQL
    instead. This gives exact `22012` parity for free. Rejected: it costs filter pushdown on every
    division predicate, forces the scan to project the operand columns, and trades measured
    performance on the common case for error-message fidelity in a pathological one.
  - Accept the gap and file a tracked exception, the pattern `#27` and `#246` follow. Rejected: a
    tracked exception is the right answer when no safe fix exists. Here one does.
- **Rationale:** The argument that ruled out the emit-boundary fix is that the boundary cannot
  distinguish a computed non-finite value from a stored one. A checked division has no such
  ambiguity. It sees exactly two operands of a division the pushdown itself synthesised, and it
  raises on the result of that division. A plain `SELECT <double_col>` over a table storing `NaN`
  reaches no checked division and is untouched. The check also sits at the one place every position
  shares, so projection and predicate stop drifting apart.
- **Supersedes:** float-div-zero-behaviour-from-measurement-not-emulation
- **Promotes to ADR:** yes

### [2] Raise on ANY non-finite result, not only on a zero divisor

- **Decision:** The function raises when the computed `Float64` result is not finite. A zero divisor
  raises with a message naming a division by zero. Any other non-finite cause raises with a message
  naming a numeric value out of range.
- **Alternatives:** Check only `<right> == 0.0` and let every other non-finite result through.
  Rejected.
- **Rationale:** A finite numerator over a tiny divisor overflows to `±Inf` and reproduces issue
  #370's exact defect class: a non-finite value consumed by a comparison, never reaching an emit
  check. A divisor-only check would leave that route open. Exasol admits no non-finite `DOUBLE` at
  all, so a non-finite result is never a representable answer, whatever produced it. Two distinct
  messages keep the two causes separable in a support case.
- **Promotes to ADR:** yes

### [3] The function name is owned by `crates/vs-expression`; the implementation is owned by `crates/lakehouse-engine`

- **Decision:** `crates/vs-expression` exports the function name as one public constant, documented
  with the full contract the implementation must satisfy. The rendering reads that constant.
  `crates/lakehouse-engine` implements the `ScalarUDF` and registers it under the same constant.
- **Alternatives:**
  - Implement the `ScalarUDF` inside `crates/vs-expression`. Rejected: that crate depends only on
    `exasol-udf-sdk` and `serde_json` by design, and is documented as having no SQL-parser
    dependency. Adding DataFusion to a crate meant for sibling reuse is a large change for a small
    fix.
  - Duplicate the name as a string literal on both sides. Rejected: two owners of one name is the
    drift this constant exists to prevent.
- **Rationale:** The DataFusion dialect now has a runtime prerequisite: a consumer of that dialect
  must register this function. That prerequisite is stated once, at the constant, rather than
  implied. A consumer that forgets it fails loudly at first use with an unresolved-function error,
  never silently. The sibling project that shares this crate is affected in the same explicit way.
- **Promotes to ADR:** yes

### [4] The DataFusion dialect renders no cast: drop the `CAST(<left> AS DOUBLE)` wrapper

- **Decision:** The DataFusion dialect renders NO cast for a `FLOAT_DIV` node. It no longer wraps
  the left operand in `CAST(... AS DOUBLE)`, and it does not wrap the right operand either. The
  called function coerces both operands to `Float64` itself. This reverses the recorded ADR
  **"Render the DOUBLE cast in the DataFusion dialect only, not in both dialects"**
  (slug `float-div-cast-datafusion-dialect-only`, `specs/_decision/079-fix-float-div-truncation.md`),
  whose Decision states that `FLOAT_DIV` renders `(CAST(<left> AS DOUBLE) / <right>)` through the
  DataFusion-dialect entry points. That ADR's own conclusion, that the Exasol dialect needs no help
  and must stay byte-identical, is retained unchanged and is restated here.
- **Alternatives:** Keep the cast and coerce only the right operand inside the function. Rejected.
  Leave the superseded ADR unreferenced and let both stand. Rejected: `/speq:spec-merge` handles
  supersession only through a new ADR's `**Supersedes:**` field and forbids the recorder from
  editing any other file in `specs/_decision/`, so an unreferenced ADR would remain Accepted while
  contradicting the recorded spec. This repo's own precedent rejects the loose form as well
  (`specs/_decision/045-fix-declined-filter-self-apply.md:67` records "Supersede without naming the
  error | Rejected").
- **Rationale:** The right operand always needed coercion inside the function, so keeping a
  SQL-level cast for the left one would state the always-`DOUBLE` decision in two modules. One
  owner is the point. The E2E and unit expectations change either way, so this costs nothing extra.
  This entry promotes an ADR solely so the cast-rendering ADR it reverses acquires a pointer. Two
  Accepted ADRs disagreeing about what the DataFusion dialect renders is the outcome the pointer
  exists to prevent.
- **Supersedes:** float-div-cast-datafusion-dialect-only
- **Promotes to ADR:** yes

### [5] Register in `build_session_context`, do not consolidate the two registrations

- **Decision:** The new function is registered by a call in `build_session_context`, alongside the
  existing `register_nested_json_render_udf` call.
- **Alternatives:** Introduce one `register_scan_udfs` owning both registrations, and move
  `NestedJsonRenderUdf` out of `raw_scan.rs` into a shared module.
- **Rationale:** `build_session_context` already is the single registration site, and all three run
  paths take their session from it. Adding a second call there follows the existing pattern rather
  than inventing one. Moving `NestedJsonRenderUdf` would enlarge the diff, touch
  `NESTED_JSON_RENDER_UDF_NAME`'s consumers in `raw_scan.rs` and `join_scan.rs`, and change no
  behaviour. This is not a deferred cleanup with no follow-up: there is nothing to clean up while
  the site stays single.
- **Promotes to ADR:** no

### [6] `classify_scan_error` recognises the failure by type, never by message text

- **Decision:** The checked division raises a dedicated error carried on the DataFusion error
  chain. `classify_scan_error` matches on that type and surfaces the message without the
  `scan failed: assigned data could not be read` prefix.
- **Alternatives:**
  - Leave the prefix in place. Rejected: it misnames a user arithmetic error as a storage failure,
    and the plan introduces the error, so it owns the framing.
  - Match a known substring in the message. Rejected: a string match is a silent coupling that
    breaks on any wording change and is invisible to the compiler.
- **Rationale:** The redaction guarantee still applies. The new arm redacts secrets exactly as the
  existing arms do, and a unit test pins it.
- **Promotes to ADR:** no

### [7] The residual evaluation-set divergence is recorded as a tracked exception, in BOTH directions

- **Decision:** The error is a per-row side effect of an expression DataFusion evaluates over a row
  set of its own choosing, so it diverges from native Exasol in both directions and ONE tracked
  exception covers both. Direction one, SUPPRESSION: DataFusion may skip the division for a row
  another conjunct, file pruning, row-group pruning, or a LIMIT already removed, so a query native
  Exasol fails may succeed. Direction two, OVER-RAISE: DataFusion may evaluate the division for a
  row an adjacent guard conjunct already excluded, so a query that succeeds today may fail after
  this change. Native Exasol's behaviour is not measured for either direction. Both are recorded in
  the spec and tracked as ONE NEW GitHub issue that **the orchestrator MUST file before this plan
  ships**, cited inline in the spec the way `(#27)` is cited in
  `specs/datafusion-scan/scan-execution-field-id-projection/spec.md`. The spec deltas carry the
  greppable placeholder `(#TODO-suppression)` at each citation site until then.
  - Proposed title: `FLOAT_DIV divide-by-zero error follows DataFusion's evaluation set, not the query's logical row set`
  - Proposed scope: after #370's fix, a zero divisor raises exactly where the scan evaluates the
    division, which is neither a subset nor a superset of the rows the query logically selects.
    Under-raise: rows removed by another conjunct, by Iceberg or Delta file pruning, by Parquet
    row-group pruning, or by an applied LIMIT may never reach the division. Over-raise:
    `datafusion-physical-expr` 54.1 defines `PRE_SELECTION_THRESHOLD: f32 = 0.2` in
    `src/expressions/binary.rs`, and `check_short_circuit` pre-selects the surviving rows for an
    `AND` only when the left conjunct's true ratio over the batch is at or below that threshold.
    Above it, `BinaryExpr::evaluate` evaluates the right conjunct over the full batch, and a null in
    the left conjunct disables the strategy entirely. A division in the LEFT conjunct is never
    protected at all. So `WHERE <d> <> 0 AND <n> / <d> > 0` can raise despite its guard, and the
    outcome depends on per-batch selectivity and on conjunct order. Native Exasol's answer for the
    guarded shape is measured by plan task 1.2 and recorded in the spec. The divergence is scoped to
    error-raising alone in both directions: a query that does not raise returns exactly the rows
    Exasol returns, because a row reaches the result only when its division was evaluated and
    finite.
- **Alternatives:**
  - Claim full parity and say nothing. Rejected, per CLAUDE.md: a known deviation is either fixed in
    the plan or recorded as an accurately scoped tracked exception, never a silent gap. Fixing the
    suppression direction would mean forcing evaluation of a predicate over rows that cannot
    contribute to the result, which is a performance cost paid on every query to change only an
    error message.
  - File two issues, one per direction. Rejected: they are the same underlying fact stated twice.
    One issue with one accurate scope is the form ADR `079` chose when it rejected a blanket
    divide-by-zero issue in favour of accurately scoped ones.
  - Suppress the over-raise direction by rendering the guard into the function. Rejected: the
    translator has no way to know which conjunct guards which division, and inventing one would
    reintroduce the operand-type reasoning `crates/vs-expression` deliberately does not do.
- **Rationale:** The harmful half of #370 is the wrong row count, and the fix removes it entirely in
  both directions. What remains changes only whether an error is raised, never which rows a
  successful query returns. Recording only the suppression direction would have understated the
  exception, because the over-raise direction is the one that can turn a working query into an
  intermittent failure.
- **Promotes to ADR:** yes

### [8] The NaN comparison ordering issue #370 reported is scoped out and tracked separately

- **Decision:** Issue #370's second observation, that `NaN < -1E300` matched all 20 rows and
  `NaN > 1E300` matched none, is out of scope for this fix. After the change a pushed `FLOAT_DIV`
  cannot produce a `NaN`, so #370's own reproducer no longer reaches it. Comparison semantics for a
  `NaN` READ FROM a column stay unmeasured and unspecified, and are tracked as a NEW GitHub issue
  that **the orchestrator MUST file before this plan ships**. The spec deltas carry the greppable
  placeholder `(#TODO-stored-nan)` at each citation site until then.
  - Proposed title: `Pushed comparison against a stored IEEE-754 NaN does not follow IEEE semantics`
  - Proposed scope: verify live, against a Delta or Iceberg table storing a `NaN` in a `double`
    column, what a pushed comparison returns for that row, and specify the result. Issue #370
    observed non-IEEE ordering for a computed `NaN` and states the mechanism was not investigated.
    Iceberg anticipates a stored `NaN` normatively (its statistics rules state "NaNs are not
    permitted as lower or upper bounds", and its manifest `field_summary` carries
    `nan_value_counts`), so this is a reachable table shape and not a hypothetical one.
- **Alternatives:**
  - Fold it into this plan. Rejected: it is a different mechanism on a different value source, and
    this plan is a fix, not a redesign. Verifying it needs a fixture this repo does not have.
  - Say nothing, since the fix removes #370's own path to it. Rejected: that would make it a silent
    gap the moment #370 closes.
- **Rationale:** One issue, one mechanism, one accurate scope. This is the same reasoning ADR `079`
  used to reject a single blanket divide-by-zero issue.
- **Promotes to ADR:** yes

### [9] Issue #246 stays open; the widening this feature recorded against it is withdrawn

- **Decision:** The spec withdraws the recorded claim that this feature widens `#246`'s
  reachability, because a pushed `FLOAT_DIV` can no longer produce a `NaN` at all. `#246` stays
  open and MUST NOT be treated as closed: it still covers an out-of-domain math kernel and a `NaN`
  stored in the source table, and it still records that `arrow_value_at` errors on the
  partial-aggregate path where `emit_batch` does not.
- **Alternatives:** Claim the plan closes `#246`. Rejected: it closes one route into it, not the
  gap.
- **Rationale:** Withdrawing an accurate-at-the-time claim keeps the recorded spec honest without
  overstating what the fix achieves.
- **Promotes to ADR:** no

### [10] Iceberg and Delta compliance: no deviation, one named target-type trade-off

- **Decision:** Re-checked per CLAUDE.md against the Apache Iceberg table spec
  (https://iceberg.apache.org/spec/) and the Delta Lake protocol
  (https://github.com/delta-io/delta/blob/master/PROTOCOL.md). Iceberg `#### Primitive Types` gives
  `float` "32-bit IEEE 754 floating point" and `double` "64-bit IEEE 754 floating point". Delta
  `## Primitive Types` gives `float` "Single precision (32-bit) IEEE 754 floating point number" and
  `double` "Double precision (64-bit) IEEE 754 floating point number". Neither document defines
  expression result types. Nothing in this plan changes how a stored `float` or `double` is decoded,
  pruned, or projected, so no reader requirement of either specification is touched. There is no
  deviation to fix or track.
- **Alternatives:** None. This is a required check, not a choice.
- **Rationale:** The one consequence worth naming is that a stored `±Inf` or `NaN` operand fed into a
  pushed division now raises. Iceberg anticipates a stored `NaN` normatively ("NaNs are not
  permitted as lower or upper bounds", plus `nan_value_counts` in the manifest `field_summary`), so
  the shape is reachable. Per CLAUDE.md, a deviation driven by an Exasol target-type limitation is
  not a gap for either specification, and Exasol admits no non-finite `DOUBLE`. It is named in the
  spec as a deliberate trade-off rather than left unstated, and it is consistent with what already
  happens: projecting the same value already fails at `22002`.
- **Promotes to ADR:** no

### [11] Three sequential knowledge clusters, one `[expert]` task

- **Decision:** Group A is the live pre-fix reproduction, tasks 1.1 and 1.2, and it MUST run against
  an unmodified tree with an empty `Depends on`. Group B is the `crates/vs-expression` rendering
  with its unit tests, and it depends on A. Group C is the `crates/lakehouse-engine` evaluation, the
  registration, the error framing, and every E2E test, and it depends on B. Only task 3.2 carries
  `[expert]`.
- **Alternatives:**
  - Put the reproduction in the implementation group, as the first draft did. Rejected: group B
    changes the `FLOAT_DIV` DataFusion arm to emit `vs_checked_float_div` before group C registers
    the function, so a reproduction query run at that point returns an unresolved-function error
    rather than #370's measured row counts. The pre-fix defect stops being observable the moment
    the rendering changes, and CLAUDE.md's verification discipline requires the reproduction to
    precede the fix.
  - One group. Rejected: it prices the whole plan at the expert model for one task.
  - Split the E2E tests out of the scan-side implementation. Rejected: E2E and the scan-side
    implementation share the same fixtures and the same `EXPLAIN VIRTUAL` reading, so splitting
    them makes one agent re-derive the other's mental model.
- **Rationale:** The reproduction is its own knowledge cluster because its orientation is the live
  Docker stack and issue #370's measurements, not either crate's source. Groups B and C follow the
  two crates and the two spec deltas. All three run in sequence rather than as a parallel fan-out,
  because group C imports the constant group B exports, both edit `tests/e2e_scan_test.rs`, and
  group A must finish before either touches the tree. Task 3.2 is the one place a null-mask or
  negative-zero mistake would silently produce wrong results, which is the exact defect class this
  plan exists to remove.
- **Promotes to ADR:** no

### [12] Non-finite values from pushed scalar functions other than `FLOAT_DIV` are a third tracked exception

- **Decision:** This plan fixes one producer of a non-finite value in predicate position. Every
  other advertised scalar function that can produce one keeps the exact gap issue #370 reports, and
  that residual is recorded as a third accurately scoped tracked exception rather than left
  unstated. It is tracked as a NEW GitHub issue that **the orchestrator MUST file before this plan
  ships**, cited inline in both spec deltas. The deltas carry the greppable placeholder
  `(#TODO-scalar-fns)` at each citation site until then.
  - Proposed title: `Non-finite value from a pushed scalar function other than FLOAT_DIV changes the row count in predicate position`
  - Proposed scope: `crates/lakehouse-engine/src/adapter/capabilities.rs` advertises `FN_SQRT`,
    `FN_LN`, `FN_LOG`, `FN_ACOS`, `FN_ASIN`, `FN_EXP`, `FN_POWER`, and `FN_MOD`. Each is translated
    into a pushed predicate, and each can yield `NaN` or `±Inf` from in-domain column data:
    `SQRT` and `LN` and `LOG` on a negative argument, `ACOS` and `ASIN` outside `[-1, 1]`, `EXP` and
    `POWER` on overflow, `MOD` on a zero divisor. `WHERE SQRT(<negative_col>) > 0` and
    `WHERE EXP(<large_col>) > 0` reproduce #370's mechanism with no division involved: the
    comparison consumes the non-finite value inside the scan, and no emit-boundary check ever sees
    it. Measure each function's behaviour live against a native Exasol oracle, then either extend
    the checked-function treatment this plan establishes for `FLOAT_DIV` or decline the affected
    capabilities. Per CLAUDE.md's pushdown-delegation rule, Exasol never re-checks a predicate whose
    capability the adapter advertises, so the adapter owns the outcome for every one of these.
- **Alternatives:**
  - Fold the other functions into this plan. Rejected: each has its own domain and its own native
    Exasol behaviour, none of which is measured, and this plan is a fix rather than a redesign.
    Extending it would multiply the live-measurement surface by eight before #370 is closed.
  - Fold them into entry [8]'s stored-`NaN` issue. Rejected: entry [8] is scoped to a `NaN` READ
    FROM a column, and its title excludes a computed one. Widening it would make it inaccurate.
  - Fold them into entry [7]'s issue. Rejected: entry [7] is scoped to WHICH ROWS the division is
    evaluated over, not to which functions can produce a non-finite value.
  - Say nothing, as the first draft's § Non-Goals did for `MOD` by zero. Rejected: "not measured"
    in a plan's Non-Goals is neither a fix nor a tracked exception, and it never reaches the
    recorded spec, which is exactly the silent gap CLAUDE.md forbids.
- **Rationale:** One issue, one mechanism, one accurate scope, the same reasoning ADR `079` used to
  reject a single blanket divide-by-zero issue. The mechanism here really is one mechanism: a
  non-finite value consumed by a comparison inside the scan. What differs from `FLOAT_DIV` is only
  which function produced it.
- **Promotes to ADR:** yes

## Review Findings

<!-- Populated by speq-plan-pr after plan-reviewer resolves a blocker, and by speq-implement after code review. -->

### [1] [plan-review] The over-raise direction of the divergence was unmodelled

- **Finding:** `[UNSTATED_ASSUMPTION]` BLOCKER, round 1 § Feasibility. The plan modelled only one
  direction of divergence, that the division-by-zero error may be SUPPRESSED. The opposite
  direction was unmodelled and is a regression on queries that work today.
  `datafusion-physical-expr` 54.1 defines `PRE_SELECTION_THRESHOLD: f32 = 0.2` in
  `src/expressions/binary.rs`, and `check_short_circuit` pre-selects for an `AND` only when the left
  conjunct's true ratio is at or below it. Above that ratio `BinaryExpr::evaluate` evaluates the
  right conjunct over the full batch including the excluded rows, a null in the left conjunct
  disables the strategy entirely, and a division in the LEFT conjunct is never protected at all. So
  `WHERE <d> <> 0 AND <n> / <d> > 0` succeeds today and can raise after this change, depending on
  per-batch selectivity and conjunct order. plan.md claimed the change "converts silent wrong
  answers into loud failures" and that "no pushdown is refused that was accepted before", naming
  neither converse, and native Exasol's answer for the guarded shape was never measured.
- **Direction change:** Added plan.md task 1.2 measuring the guarded shape live against the native
  oracle in both conjunct orders, before any code change. Added four `DELTA:NEW` clauses to the
  scenario "A division by zero inside a filter predicate fails the query rather than changing the
  row count", requiring the measured outcome to be recorded and naming the batch-selectivity
  mechanism, the left-conjunct hole, and the null-disabling case. Added a matching Background bullet
  to the same delta. Added E2E test
  `e2e_float_div_guarded_by_a_non_zero_conjunct_matches_the_measured_outcome` as plan.md task 4.5
  and as a § Verification § Scenario Coverage row. Added the guarded regression to plan.md § Impact
  and qualified the "loud failures" sentence. Rewrote decision-log entry [7] to cover both
  directions under one tracked exception, retitling the proposed issue to
  `FLOAT_DIV divide-by-zero error follows DataFusion's evaluation set, not the query's logical row set`.
- **Promotes to ADR:** yes

### [2] [plan-review] The reproduction was scheduled after the fix it reproduces

- **Finding:** `[HIDDEN_DEPENDENCY]` BLOCKER, round 1 § Feasibility. Task 1.1 sat in group B, which
  depended on group A. Group A changes the `FLOAT_DIV` DataFusion arm to emit `vs_checked_float_div`
  before group B task 3.3 registers the function, so the reproduction query would have returned an
  unresolved-function error rather than issue #370's measured 20-of-20 and 0-of-20 row counts. The
  pre-fix defect was unobservable at the point the plan scheduled its reproduction, contradicting
  both the § Interview statement and CLAUDE.md's verification discipline.
- **Direction change:** Split the reproduction into its own first parallelization group A, tasks 1.1
  and 1.2, with an empty `Depends on` and a `Knowledge` cell stating the group MUST run against an
  unmodified tree. Relabelled the rendering group to B, depending on A, and the evaluation group to
  C, depending on B. Added the same MUST to the head of plan.md § Implementation Tasks section 1.
  Rewrote decision-log entry [11] as three sequential clusters and recorded the rejected first-draft
  arrangement as an alternative. Corrected the § Interview answer to name group A and both tasks.
- **Promotes to ADR:** no

### [3] [plan-review] Two `DELTA:CHANGED` scenarios carried headings the recorded spec does not have

- **Finding:** `[REQUIREMENT_CONFLICT]` BLOCKER, round 1 § Requirement Quality. `/speq:spec-merge`
  maps `DELTA:CHANGED` to "replace scenario with same name". The delta headings
  "FLOAT_DIV renders a checked float division in the DataFusion dialect" and
  "A pushed-down division by zero fails the query in projection position" match no recorded heading,
  so the merge would have appended rather than replaced. The permanent library would then have
  carried both `SHALL return (CAST(<left> AS DOUBLE) / <right>)` and
  `MUST NOT return (CAST(<left> AS DOUBLE) / <right>)` for the same node, at 8 scenarios rather than
  the 6 the plan implies.
- **Direction change:** Restored both headings to the recorded names verbatim,
  "FLOAT_DIV renders true float division in the DataFusion dialect" and
  "A pushed-down division by zero fails the query rather than returning a wrong value". The
  projection-position narrowing the rejected heading carried is already stated by that scenario's
  own GIVEN and WHEN steps, so no requirement was lost. Updated the four matching rows in plan.md
  § Verification § Scenario Coverage. Verified the remaining two `DELTA:CHANGED` headings and the
  one `DELTA:REMOVED` heading against `specs/sql-comprehension/vs-expression-translator-float-div/spec.md`:
  all three already match verbatim.
- **Promotes to ADR:** no

### [4] [plan-review] The supersession of the cast-rendering ADR had no pointer

- **Finding:** `[REQUIREMENT_CONFLICT]` BLOCKER, round 1 § Requirement Quality. Two defects. The
  Accepted ADR `float-div-cast-datafusion-dialect-only` states that `FLOAT_DIV` renders
  `(CAST(<left> AS DOUBLE) / <right>)` through the DataFusion-dialect entry points. Decision [4]
  deleted that rendering but was recorded as NOT promoting an ADR, and `/speq:spec-merge` forbids the
  recorder from editing any other file in `specs/_decision/`, so nothing would have pointed at it
  and the permanent decision log would have kept an Accepted ADR contradicting the recorded spec.
  Separately, decision [1] named its superseded ADR by title and path only, where `/speq:spec-merge`
  requires the existing ADR slug and this repo's own precedent rejects the loose form
  (`specs/_decision/045-fix-declined-filter-self-apply.md:67`).
- **Direction change:** Flipped decision-log entry [4] to promote an ADR, retitled it
  "The DataFusion dialect renders no cast: drop the `CAST(<left> AS DOUBLE)` wrapper", added a
  Decision bullet stating the dialect renders no cast on either operand and naming the reversed ADR,
  and added `- **Supersedes:** float-div-cast-datafusion-dialect-only`. Added
  `- **Supersedes:** float-div-zero-behaviour-from-measurement-not-emulation` to entry [1] as the
  literal slug. Each new ADR now carries exactly one supersede pointer.
- **Promotes to ADR:** yes

### [5] [plan-review] The tracked exceptions carried no citation token the recorded spec would keep

- **Finding:** `[COMPLETENESS_GAP]` BLOCKER, round 1 § Requirement Quality. The spec deltas said
  only "tracked as a separate issue rather than left unstated" and "recorded as a tracked exception
  rather than a silent gap", with no issue number and no placeholder. `/speq:record` merges those
  strings verbatim, and nothing in the speq pipeline files a GitHub issue. The obligation lived only
  in plan.md prose the recorded spec never sees. CLAUDE.md requires a GitHub issue cited inline in
  the spec, the form every existing tracked exception in this library follows (`(#246)`, `(#219)`,
  `(#216)`, `(#309)`, `(#27)`).
- **Direction change:** Inserted greppable placeholder tokens at every citation site:
  `(#TODO-suppression)`, `(#TODO-stored-nan)`, and `(#TODO-scalar-fns)`, in both spec deltas and in
  plan.md § Impact. Added plan.md task 4.10, which files the three issues and replaces every token
  with the filed number in the `(#NNN)` form, failing if
  `grep -rn '(#TODO-' specs/_plans/fix-float-div-predicate-divzero/` returns any line. Added the
  same grep as a § Verification § Checklist step. Named the placeholder in decision-log entries [7],
  [8], and [12].
- **Promotes to ADR:** no

### [6] [plan-review] Seven other advertised functions produce the same defect and were untracked

- **Finding:** `[COMPLETENESS_GAP]` BLOCKER, round 1 § Requirement Quality. The delta states the
  general rule that a non-finite value produced inside the scan can never be a correct answer, then
  covered exactly one producer of it. `crates/lakehouse-engine/src/adapter/capabilities.rs`
  advertises `FN_SQRT`, `FN_LN`, `FN_LOG`, `FN_ACOS`, `FN_ASIN`, `FN_EXP`, `FN_POWER`, and `FN_MOD`,
  all translated into pushed predicates and all able to yield `NaN` or `±Inf`.
  `WHERE SQRT(<negative_col>) > 0` reproduces #370's mechanism exactly. Neither proposed follow-up
  issue covered them: entry [7] is scoped to evaluation order and pruning, entry [8] to a `NaN` read
  from a column. plan.md § Non-Goals dismissed `MOD` by zero as "not measured", which is neither a
  fix nor a tracked exception and never reaches the recorded spec.
- **Direction change:** Added decision-log entry [12], a third tracked exception scoped to a
  non-finite value produced in predicate position by a pushed scalar function other than
  `FLOAT_DIV`, naming all eight capabilities, with a proposed title and scope in the same form as
  entries [7] and [8]. Added it as item 3 of plan.md § Impact "Tracked exceptions this plan does not
  fix". Added a `DELTA:NEW` Background bullet to
  `datafusion-scan/scan-execution-expression-pushdown/spec.md` and a matching one to
  `sql-comprehension/vs-expression-translator-float-div/spec.md`, both stating that the checked
  division covers `FLOAT_DIV` alone and citing `(#TODO-scalar-fns)` inline. Replaced the `MOD`
  clause in plan.md § Non-Goals with a pointer to the new exception.
- **Promotes to ADR:** yes
</content>
</invoke>

# Decision Log: remove-emit-exa-types

## Interview

**Q:** Upstream `language-container-rs#89` landed as PR #105 (merged), shipping `exasol-udf-sdk`
0.26.0 / SLC v0.26.0 on 2026-09-14 with `EXA_UDF_ABI_VERSION` 8 to 9,
`UdfContext::input_column`/`output_column`/`output_column_count`, and an owned `ColumnInfo` (not
the `ColumnInfo<'a>` originally proposed in #89). The repo is pinned at `exasol-udf-sdk` 0.24.0.
Should this plan bump the SDK and SLC pin and do the `emit_exa_types` removal now, or record that
the blocker cleared and scope a follow-up?
**A:** Do it now. Bump `exasol-udf-sdk` to 0.26.0, rebuild and repin the SLC, remove
`emit_exa_types`, wire `raw_scan.rs` and `join_scan.rs` to `ctx.output_column`. One plan, fully
closes #399.

**Q:** `raw_scan.rs` and `join_scan.rs` both call `emit_stream`/`emit_one_batch` with an
`exa_types: &[String]` parameter today. Once removed, output-type information comes only from
`ctx.output_column(idx)`. Should the emit-coercion code call `ctx` directly, or should the adapter
still pass something down, for example just a column count for a cheap sanity check?
**A:** `ctx` only. `emit_stream`/`emit_one_batch` take `&dyn UdfContext` and call
`output_column(idx)` per column. `ScanSpec` carries no emit-type field at all. Single source of
truth, matching #89's intent. No sanity-check field kept on `ScanSpec`.

**Q:** Acceptance criterion 4 in the issue asks to confirm no drift between what the adapter
declares in `EMITS (...)` and what `ctx.output_column` reports at runtime, via E2E. How should that
be covered?
**A:** A new E2E scenario. Add or extend an E2E test that runs a real scan query and asserts that
`ctx.output_column`'s reported types match the adapter's `EMITS` clause for a representative type
mix, including the JSON-fallback types (list, struct, map, binary) declared as
`VARCHAR(2000000)`.

## Design Decisions

### [1] The declared output column is the single authority for the emitted type, on every emit path

- **Decision:** The scan reads the declared `ExaType` for output column `i` from
  `UdfContext::output_column(i)` and coerces the value to it, on both the Arrow `emit_batch` path
  and the `Value` partial-aggregate path. No module carries a second copy of that declaration and
  no module re-derives it.
- **Alternatives:** Keep `CommonScanSpec::emit_exa_types` as a sanity-check copy. Rejected: two
  copies with nothing enforcing agreement is the defect being removed, and the interview ruled it
  out explicitly. Keep the copy only for the join path, which round-tripped `proj_types` through
  the spec. Rejected: the value is available at every call site as a local.
- **Rationale:** `LAKEHOUSE_SCAN` is declared `EMITS (...)`, so the call-site clause Exasol parses
  is already the only declaration. The duplicate was back-door leakage: the adapter and the scan
  independently assumed one format decision. The same leak causes the four partial-aggregate
  mismatches, and one authority fixes both paths at once.
- **Promotes to ADR:** yes

### [2] The SDK bump's row validation forces a partial-aggregate fix into this plan

- **Decision:** Fix the four pre-existing partial-aggregate type mismatches in this plan, by
  coercing each partial-aggregate column to its declared `ExaType` before the per-cell conversion.
- **Alternatives:** Bump the SDK and defer the fix to a follow-up. Rejected: `AVG` over an integer
  column works today through the old bridge's silent coercion and would start failing, which is a
  regression this plan would introduce. Cast per aggregate kind in `partial_select_items`.
  Rejected: it cannot fix `MIN`/`MAX` over a `decimal(p,0)` or a wide-decimal `SUM`, because the
  scan does not know those declared types without reading the context.
- **Rationale:** `column_accepts` (`exa-udf-runtime`'s `rowset.rs`) rejects `Value::Int64` and
  `Value::Numeric` in a `Double` column, `Value::Numeric` in an `Int32`/`Int64` column,
  `Value::Double` in a `Numeric` column, and `Value::String` in any numeric column. Four paths in
  this repo violate those rules, each decided by repo code read against the upstream matrix, none
  inferred and none resting on an unverified live-engine claim:
  - `AvgSum`/`StatSum`/`StatSumSq` are declared `DOUBLE PRECISION` at `grouped_agg.rs:690-692`
    while `partial_agg.rs:367-371` emits a bare `SUM(<arg>)`, so DataFusion keeps the argument type.
  - `sum_emit_type` (`grouped_agg.rs:752-763`) caps the declared type at `DECIMAL(36,s)` while
    DataFusion widens `SUM(Decimal128(p,s))` to `min(38, p+10)`, so precision at least 27 emits
    `Value::String`.
  - `col_type_for` declares `MIN`/`MAX` with the source column's type and `arrow_value_at`
    (`convert.rs:136-143`) produces `Value::Numeric` for every in-range `Decimal128`, so a
    `decimal(p,0)` with `p` at most 18 lands `Value::Numeric` in an `Int64` bin.
  - `NESTED_AGGREGATE_PLAN_TYPE` (`scalar_over_agg.rs:28`) is the hardcoded literal
    `DOUBLE PRECISION`, applied at `:57` to any aggregate reached only nested inside a scalar,
    while the DataFusion expression over decimal arguments yields `Decimal128`, so
    `Value::Numeric` meets a `Double` column.
  An earlier draft stated the fourth mismatch as `SUM(TRUNC(dec,2))` or `MIN(SIGN(x))` declared
  `DECIMAL` by Exasol. That form was dropped: whether Exasol reports `DECIMAL` in
  `selectListDataTypes` for those expressions is a live-engine fact this plan has not captured, and
  CLAUDE.md § Verification discipline forbids asserting it from `capabilities.rs` alone. The
  `NESTED_AGGREGATE_PLAN_TYPE` form needs no Exasol assumption.
- **Promotes to ADR:** yes

### [3] Map `ExaType` to Arrow directly, deleting the replicated DECIMAL precision binning

- **Decision:** `target_arrow_type` matches on the `ExaType` variant the SDK reports.
  `exasol_type_to_arrow`'s string parse and its scale-0 precision binning (at most 9 to `Int32`, at
  most 18 to `Int64`, otherwise `Numeric`) leave the emit path.
- **Alternatives:** Call `exasol_type_to_arrow(&ColumnInfo.type_name)`, which the SDK also reports.
  Rejected: it keeps a local re-derivation of a decision the engine has already made and reported,
  so the two can disagree, which is the class of defect two recorded live bench failures already
  produced.
- **Rationale:** `ExaType` is the wire block the engine chose for the column. Reading it makes the
  binning structural rather than replicated, and it makes the `TIMESTAMP(p)` precision-collapse
  rule hold by construction, because `ExaType::Timestamp` models no precision.
- **Promotes to ADR:** no

### [4] An absent or wrong-arity declared column list fails the call, with no fallback

- **Decision:** `emit_stream` fails with a named error when `output_column_count()` disagrees with
  the batch column count, and when `output_column(i)` errors for a column the batch carries. The
  pre-#399 view-type-normalization fallback for an empty or short list is deleted.
- **Alternatives:** Keep the fallback so a context without column metadata still emits. Rejected:
  it would hide exactly the drift acceptance criterion 4 asks to prove, and it would degrade into a
  confusing engine-side IPC type error instead of a clear message.
- **Rationale:** The fallback existed for scan specs written before `emit_exa_types`. With the
  field gone, every call has a call-site `EMITS` clause, so an absent declaration is drift. The
  cost is that each of the 22 test contexts driving the scan must declare its output columns, which
  is the honest cost of making the test state the shape it simulates.
- **Promotes to ADR:** no

### [5] Group-key columns keep their existing stringification and are not coerced

- **Decision:** The partial-aggregate coercion skips the leading group-key columns.
  `value_to_gk_string` is unchanged.
- **Alternatives:** Coerce the whole partial batch uniformly. Rejected: an Arrow
  `cast(Date32 → Utf8)` formats differently from `NaiveDate::to_string()`, so a group's key text,
  and therefore its identity across the Exasol merge, could change.
- **Rationale:** Group-key columns are declared `VARCHAR(2000000)` and already emit
  `Value::String`, which `column_accepts` admits. They carry no mismatch to fix, so touching them
  would be risk without benefit.
- **Promotes to ADR:** no

### [6] `exasol_type_to_arrow` is retained as `pub` after losing its production call site

- **Decision:** Keep the function, its `pub` visibility, and its test module. Record the status in
  the `datafusion-scan/scan-execution-value-conversion` delta's Background.
- **Alternatives:** Delete it as dead code. Rejected: that feature already records the identical
  status for its inverse `arrow_to_exasol_type`, on the grounds that it is the only public name for
  its direction that CLAUDE.md § Data types documents as a compliance surface, and that removing a
  public API item is a scope ADD.
- **Rationale:** Applying the recorded rule consistently avoids both a spec conflict and scope
  creep beyond issue #399.
- **Promotes to ADR:** no

### [7] Four features that mention `emit_exa_types` get no delta, and the plan names them

- **Decision:** Only a feature whose scenario this change breaks gets a delta. The four others are
  listed in plan.md's "Recorded text this plan does not amend" table, with the handling for each.
  Two carry a dated live `EXPLAIN VIRTUAL` capture that stays as recorded
  (`sql-comprehension/vs-expression-translator-float-div`,
  `vs-adapter/pushdown-planning-capability-extensions`). Two carry a Background bullet that becomes
  stale (`vs-adapter/create-virtual-schema-declaration-details`,
  `vs-adapter/pushdown-planning-char-type-declaration`).
- **Alternatives:** Write Background-only deltas that supersede the two stale bullets. Rejected:
  `speq plan validate` requires every delta file to carry a scenario, and none of the 59 recorded
  delta files is Background-only, so the format does not support it. Reproduce a whole long
  scenario in a `DELTA:CHANGED` block just to reach its Background. Rejected: `DELTA:CHANGED`
  replaces a scenario by name, which would duplicate ten long steps for no gain.
- **Rationale:** A dated measurement stays true as a measurement, and
  `vs-adapter/pushdown-catalog-session` already records that historically accurate Background
  bullets are not superseded. The two genuinely stale bullets are implementation-mechanism notes
  whose replacement is stated authoritatively in the
  `datafusion-scan/scan-execution-value-conversion` delta. Naming all four in plan.md keeps the
  residue recorded rather than silent.
- **Promotes to ADR:** no

### [8] The bench SLC pin derives from the workspace pin instead of a hardcoded default

- **Decision:** `bench/run.sh` defaults `SLC_VERSION` to the workspace `exasol-udf-sdk` pin, as
  `Makefile:129` already does, keeping `BENCH_SLC_VERSION` as the override.
  `deploy/scripts/secrets.sh` moves to `0.26.0`.
- **Alternatives:** Move the hardcoded default from `0.21.0` to `0.26.0`. Rejected: it leaves the
  same drift class that let the value fall two releases behind the workspace pin.
- **Rationale:** The SDK fingerprint is checked at UDF load, so a bench run that builds and uploads
  this repo's `.so` against a stale SLC fails. The Makefile and `deploy/scripts/install.sh` already
  derive the version, and `bench/run.sh` was the one copy that did not.
- **Promotes to ADR:** no

### [9] A payload-less or out-of-range `Numeric` declaration fails the call on both emit paths

- **Decision:** The emit boundary fails the call, naming the offending output column, when a
  declared `ExaType::Numeric` reports an absent `precision` or `scale`, or reports one outside what
  `Decimal128` represents. The failure mechanism is decision [4]'s no-fallback error. The rule is
  identical on the Arrow `emit_batch` path and the `Value` partial-aggregate path. Neither path
  substitutes `Utf8`.
- **Alternatives:** Route the case to `Utf8` on both paths. Rejected: `column_accepts`
  (`exa-udf-runtime`'s `rowset.rs`) admits no `Value::String` in a `Numeric` column, so the `Value`
  path would fail with the SDK's own `output column … is … but the value is …` error, which is the
  failure the partial-aggregate scenario exists to remove. Route to `Utf8` on the Arrow path only,
  where `is_string_family_exatype` admits it, and fail on the `Value` path. Rejected by the user:
  two rules for one declaration split an authority this plan exists to unify, to cover a state
  neither path is expected to reach.
- **Rationale:** `ExaType::Numeric`'s `Option<u32>` fields are a structural artifact of the wire
  format, not a documented possibility for a genuine NUMERIC declaration. The protobuf
  `column_definition` (`exa-proto`'s `zmqcontainer.proto:33-36`) carries one `optional precision`
  and `optional scale` pair shared by every column type, unset outside DECIMAL.
  `column_from_pb` (`exa-zmq-protocol`'s `meta.rs:69-72`) forwards both into `ExaType::Numeric` with
  no check that a NUMERIC declaration carries them. A precision and a scale are required for a
  DECIMAL declaration to be valid SQL, and Exasol caps precision at 36, inside `Decimal128`'s
  maximum of 38. Both branches guard a state the type system makes representable and a real column
  is not expected to produce, so one hard-fail rule is preferred to a per-path special case.
  Forward note: a future `language-container-rs` change may drop the `Option` wrapper from
  `ExaType::Numeric`'s `precision` and `scale`, which would make the absent case unrepresentable at
  the type level. That change is out of scope here and no issue is filed for it. It is recorded as
  context for why this treatment is a bounded, deliberate simplification.
- **Promotes to ADR:** no

## Review Findings

Round 1 raised 7 BLOCKER findings. All 7 are resolved below. Round 2 raised 1 BLOCKER, resolved in
the final entry. The ADVISORY findings of both rounds were left untouched, as advisory findings do
not gate a revision.

### [plan-review] [SCOPE_REDUCTION] The permanent E2E drift scenario is dropped, task 1.6 stands alone

- **Finding:** `plan-reviewer` held that acceptance criterion 4's interview answer asked for a
  permanent E2E test comparing the runtime `ctx.output_column` list to the `EMITS` clause item for
  item, that the delta substituted an indirect proof, and that the delta's stated `emit_batch`
  rejection detector is tautological because both sides read `output_column`. Its Fix was to add a
  permanent direct comparison to the E2E delta and to task 4.1.
- **Direction change:** This resolution follows the USER's explicit ruling, NOT the reviewer's
  literal Fix. The user distinguished a one-time implementation-time proof that the no-fallback bet
  is safe from a permanent recurring scenario that would mostly re-test
  `language-container-rs`'s own SDK and SLC contract. Task 1.6 stays exactly a one-time gate
  (Docker container, temporary `udf_log!` capture, deleted after use, plan halts on failure) and the
  permanent E2E drift scenario is dropped entirely. In
  `e2e-harness/e2e-harness-scan-correctness/spec.md` the "proves agreement, which cannot be asserted
  by comparing two lists" Background bullet and the `emit_batch`-rejection detector clause are
  deleted, and the scenario is renamed to what it actually asserts, value correctness across the
  type mix with no spec-carried emit types. Its value-correctness and `emit_exa_types`-absence
  clauses are kept, so the file stays. Task 1.6 and plan.md § Verification now state plainly that
  task 1.6 alone satisfies acceptance criterion 4 and that no permanent test was added, with the
  reason, so a later reader does not read the absence as an omission.
- **Promotes to ADR:** no

### [plan-review] [SCOPE_CREEP] The partial-aggregate fix is bundled under #399 with no separate issue

- **Finding:** `plan-reviewer` confirmed the four partial-aggregate mismatches are real and that the
  bump forces them, but held that no GitHub issue tracks the work, that issue #399's own Scope
  section says the partial-aggregate path is not in scope, and that CLAUDE.md § Feature tracking
  requires an issue. Its Fix was to run `gh issue create` and cite the new number alongside #399.
- **Direction change:** This resolution follows the USER's explicit ruling, NOT the reviewer's
  literal Fix. No issue was opened. The user made an informed override of both #399's "Not in scope"
  text and CLAUDE.md's issue-tracking convention, accepting the partial-aggregate fixes under #399,
  closed by the same commit. plan.md § Summary now records that #399's note predates the 0.26.0
  release's bundled row-validation change (PR #105) and no longer holds, and that bundling was a
  deliberate user decision. The `datafusion-scan/scan-execution-partial-agg` Background now states
  that the delta is #399's blocking prerequisite AND fixes pre-existing mismatches #399 did not
  originally cover. plan.md keeps `Closes #399` as the only trailer, and tasks 2.6 and 2.8 keep
  their numbering.
- **Promotes to ADR:** no

### [plan-review] [HIDDEN_DEPENDENCY] Task 1.5's UdfContext test-double census was wrong and short

- **Finding:** Task 1.5 named `BatchCapturingCtx` and placed `SinkCtx` in `raw_scan_tests.rs`, and
  it missed `CapturingCtx` entirely. `SinkCtx` is declared at `test_support_tests.rs:80`;
  `raw_scan_tests.rs:1195` only imports it. `CapturingCtx` (`emit_tests.rs:34`, impl at `:66`) is
  the double that drives `emit_stream`, so with decision [4]'s fallback removed, missing accessors
  fail those tests at run time.
- **Direction change:** Task 1.5 now names all three impls with verified paths and states each
  one's shape: `BatchCapturingCtx` forwards to an inner `TestContext`, while `SinkCtx` and
  `CapturingCtx` are hand-rolled and need their own accessor bodies. `CapturingCtx` must gain a
  field carrying the declared columns the test sets. § Parallelization group A's Knowledge column
  now reads `test_support_tests.rs` and `emit_tests.rs` in place of `raw_scan_tests.rs`.
- **Promotes to ADR:** no

### [plan-review] [UNSTATED_ASSUMPTION] PR #105's column-meta fixture covers a static EMITS list, not the dynamic form

- **Finding:** The plan claimed PR #105 "covers the dynamic-EMITS shape with a live `column-meta`
  integration fixture". `column_metadata_reaches_the_udf`
  (`language-container-rs` `crates/it/tests/db_roundtrip.rs:2254-2308`) registers
  `describe_output(dummy BOOLEAN) EMITS (col_a VARCHAR(200), col_b DECIMAL(18,0))` and then
  re-registers it against a second STATIC list. `LAKEHOUSE_SCAN` uses the dynamic call-site form,
  which is exactly what task 1.6 exists to prove.
- **Direction change:** The claim is corrected in three places: the
  `datafusion-scan/scan-execution-value-conversion` Background now states what the fixture covers, a
  re-registered static list, and records that the dynamic call-site form is proven separately
  against the local Exasol Docker container; plan.md § Design § Context carries the same correction;
  and § Load-bearing assumptions row 1's Evidence cell is marked partial with the gap named.
- **Promotes to ADR:** no

### [plan-review] [UNSTATED_ASSUMPTION] Partial-aggregate mismatch 4 is restated on repo-decidable evidence

- **Finding:** Decision [2] claimed all four mismatches were verified, but mismatch 4's Exasol half,
  whether `selectListDataTypes` reports `DECIMAL` for `SUM(TRUNC(dec,2))` or `MIN(SIGN(x))`, is a
  live-engine fact the plan had not captured. CLAUDE.md § Verification discipline forbids asserting
  it from `capabilities.rs` alone.
- **Direction change:** Mismatch 4 is restated in
  `datafusion-scan/scan-execution-partial-agg/spec.md` § Background using
  `NESTED_AGGREGATE_PLAN_TYPE` (`adapter/pushdown/scalar_over_agg.rs:28`), the hardcoded
  `DOUBLE PRECISION` applied at `:57` to an aggregate reached only nested inside a scalar, whose
  DataFusion expression over decimal arguments yields `Decimal128`. Both halves are then decided by
  repo code. The `TRUNC`/`SIGN` form is dropped from the plan entirely, so task 1.6 needs no added
  live-capture scope. Decision [2]'s Rationale now names the evidence per mismatch and records why
  the earlier form was dropped. The partial-agg scenario and task 2.8 carry the restated case.
- **Promotes to ADR:** no

### [plan-review] [COMPLETENESS_GAP] An absent or out-of-range Numeric precision routes to Utf8

- **Finding:** The value-conversion scenario mapped `ExaType::Numeric { precision, scale }` to
  `Decimal128(precision, scale)` as if the payload were always present. The SDK declares both as
  `Option<u32>` (`language-container-rs` `crates/exasol-udf-sdk/src/value.rs:163-166`) and
  `DataType::Decimal128` takes `(u8, i8)`, so the absent and out-of-range cases were unspecified,
  and decision [4] removed the fallback that would have absorbed them.
- **Direction change:** A clause was added to the value-conversion scenario routing a `Numeric`
  column with an absent or out-of-range `precision` or `scale` to `Utf8` rather than failing the
  call, which `is_string_family_exatype`
  (`language-container-rs` `crates/exa-udf-runtime/src/rowset.rs:659-673`) already admits for a
  `Numeric` column. Task 2.7's `ExaType` sweep now covers both cases. Round 2 superseded the `Utf8`
  routing, per the entry below. The case is now a hard failure on both paths.
- **Promotes to ADR:** no

### [plan-review] [TRACEABILITY_GAP] Task 4.3 claims only the mismatch its fixtures reach

- **Finding:** Task 4.3 claimed to exercise `AVG`, `STDDEV` and `MIN`/`MAX` on a non-`double` column,
  but no seed fixture or bench dataset carries a scale-0 decimal or a decimal with precision at
  least 27, so mismatches 2 and 3 are unreachable end to end, and § Non-Goals forbids adding a
  fixture.
- **Direction change:** The lighter of the two offered fixes was taken. "No new seed fixture" stays
  in § Non-Goals. Task 4.3 is restated to claim only the `AVG`/`STDDEV`-over-integer-and-decimal
  case it can reach, and says so. plan.md § Impact records that the wide-decimal `SUM` and the
  `MIN`/`MAX` over `decimal(p,0)` carry unit coverage only, and § Verification states the coverage
  split above § Scenario Coverage, whose partial-aggregate integration row now names the one
  mismatch it reaches.
- **Promotes to ADR:** no

### [plan-review] [REQUIREMENT_CONFLICT] The Numeric Utf8 escape hatch becomes a hard failure on both paths

Supersedes `[plan-review] [COMPLETENESS_GAP] An absent or out-of-range Numeric precision routes to
Utf8`.

- **Finding:** `plan-reviewer` held that round 1's `Utf8` routing is safe on the Arrow path, where
  `is_string_family_exatype` (`rowset.rs:663`) admits a `Utf8` column for a `Numeric` declaration,
  but wrong on the `Value` partial-aggregate path, where `column_accepts` (`rowset.rs:1325-1347`)
  rejects `Value::String` in a `Numeric` column. The shared rule would therefore produce exactly the
  `output column … is … but the value is …` error the partial-aggregate scenario forbids. The
  reviewer also held that the clause's stated justification names the wrong SDK function. Its Fix
  was to scope the `Utf8` clause to the Arrow path and add a separate hard-fail clause for the
  `Value` path.
- **Direction change:** This resolution follows the USER's explicit ruling, NOT the reviewer's
  literal per-path-split Fix. The user traced the conflict to its root: `ExaType::Numeric`'s
  `Option<u32>` fields are a wire-level artifact of one `precision`/`scale` pair shared across every
  column type, not a documented state for a genuine NUMERIC declaration. The `Utf8` fallback is
  removed from both paths. An absent or out-of-range `precision` or `scale` now fails the call
  naming the offending column, through decision [4]'s existing no-fallback mechanism, identically on
  both paths. This removes the asymmetry rather than writing one correct rule per path. New decision
  [9] records the reasoning and the forward note about a possible future `language-container-rs`
  change dropping the `Option` wrapper. The value-conversion delta's scenario carries the hard-fail
  clause and a clause stating that the `Value` path behaves identically, and its Background records
  the wire-artifact fact. The partial-agg delta's scenario carries the matching clause. plan.md
  § Design § Patterns row "One coercion rule, two emit paths" now states that both paths agree on
  this declaration. Tasks 2.7 and 2.8 each add a test asserting the failure and the absence of a
  `Utf8` substitution. § Load-bearing assumptions gains the row stating the assumption the ruling
  rests on.
- **Promotes to ADR:** no

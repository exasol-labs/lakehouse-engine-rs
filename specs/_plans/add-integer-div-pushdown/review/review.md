# Plan Review Findings: add-integer-div-pushdown

## Summary
- Axes checked: 6/6
- Total findings: 12 (Blockers: 2, Findings: 10)
- Intent Fidelity blockers: 0

Ways this plan may fail, routed into the taxonomy:

1. The new scenario "Integer division over integer operands runs in the scan" says the scan SHALL compute every integer-operand `DIV`. An accepted ADR routes an unprojected group key and a nested aggregate to the wrapper, so verification fails on a well-formed request. Routed to `[ADR_CONFLICT]`.
2. The plan ships new deviations from native Exasol and states that `docs/semantic-differences.md` does not list them. `specs/mission.md` accepts a deviation only when that file documents it. Routed to `[REQUIREMENT_CONFLICT]`, with `[UNSTATED_ASSUMPTION]` for an unproven regression path in Out of scope.
3. The "shared module" becomes two parallel UDF types with copied NULL, record-and-raise, and identity code, while two recorded specs keep saying there is a single checked-division function. Routed to `[INFORMATION_COUPLING]` and `[REQUIREMENT_CONFLICT]`.

## Intent Fidelity
no objection, axis checked: every clause of the request maps to a task. Integer-only pushdown is tasks 1.1 and 1.2. Decline otherwise is task 1.2 plus the E2E `Lacks` rows. Sign handling, zero divisor, and result type against live Exasol are task 3.3, whose `native_copy_mismatch` compares column `dataType` (`e2e_harness.rs` `comparable_outcome`). The checked integer function is task 2.1. One shared `checked_div.rs` with unchanged `FLOAT_DIV` is task 2.1. The Background rewrites in the capability-extensions and string-conversion deltas are what `.speq/plan-hook.md` requires of a changed section, not scope creep.

## Feasibility

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` § Scenario: Integer division over a decimal or DOUBLE operand runs in Exasol; § Scenario: Integer division over integer operands runs in the scan
- Issue: the scenarios record native Exasol values that plan.md § Context does not list as live-checked. "`DIV(7.5, 2)` is `3` of type `DECIMAL(p,0)`, and a DOUBLE numerator returns a DOUBLE" and "a numerator of `-9223372036854775808` over a divisor of `-1` returns `9223372036854775808`" are stated as normative. Context lists only truncation, `22012`, NULL, and the integer-operand result type. `AGENTS.md` requires every SQL claim to be verified against a live Exasol. Recorded plans 027 and 028 exist to remove live-refuted claims of exactly this kind.
- Fix: Run `SELECT DIV(7.5, 2)`, `DIV(CAST(7 AS DOUBLE), 2)`, and `DIV(CAST(-9223372036854775808 AS DECIMAL(19,0)), -1)` against the Docker Exasol with a CTAS to read the result types, and add the results to plan.md § Context. If any value differs, or the check cannot run, delete that example from the scenario and keep only "the result SHALL equal native Exasol's in value and type".

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Out of scope, bullet 1
- Issue: the bullet says "An operand the typed pass classes as integer but DataFusion types as `Float64`, such as a `CASE`, `GREATEST`, or `LEAST` over integers wider than 36 digits, fails scan planning instead of falling back." No repro backs it, and the code contradicts it. `exasol_numeric.rs` `common_exact` caps each operand at 36 digits under `Unify::Combine` before widening, so an all-integer `CASE`, `GREATEST`, `LEAST`, or `COALESCE` never exceeds 36 and `unify` never casts it to `Float64`. Every `ResultRule::Integer` function also types as an integer in DataFusion 55 (`date_part` returns `Int32` for every field but epoch and nanosecond). If the bullet were true, it would be a regression: a query Exasol answers today would fail once `FN_DIV` is advertised, with no fallback.
- Fix: Add a unit test in `checked_div_tests.rs` that plans `vs_checked_int_div(greatest(<widest integer expression>, 1), 2)` through `scan_session()`. If it plans, delete the bullet. If it fails, make the typed pass decline that operand shape in task 1.2 so the query falls back, and replace the bullet with that task.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Context; task 2.1
- Issue: `AGENTS.md` § Workflow requires a plan that touches pushdown or type handling to quote the governing Iceberg table spec or Delta Lake protocol section. The accepted operand set of `vs_checked_int_div` (signed and unsigned Arrow integers, `Decimal128(p,0)`, Null) rests on which integer primitives the formats define, and the plan quotes neither.
- Fix: Add one plan.md § Context bullet that quotes the Iceberg spec "Primitive Types" rows for `int`, `long`, and `decimal(P,S)` and the Delta protocol "Primitive Types" rows for `byte`, `short`, `integer`, `long`, and `decimal`. State that unsigned Arrow types reach the scan only from Hive Parquet files.

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § Verification § Manual Testing
- Issue: every command queries `MY_LAKEHOUSE.TYPED_DISTINCT_PROBE` "after `make cross-udf-build` and `make e2e-up`". `make e2e-up` runs `scripts/e2e-stack.sh e2e up`, which only starts containers. The `MY_LAKEHOUSE` virtual schema (`fixtures.rs` `LAKEHOUSE_VS`) and the seeded `typed_distinct_probe` table are created by the E2E suite's `stack()` provisioning on its first test. On a fresh own stack the manual commands fail with an unknown schema.
- Fix: In plan.md § Manual Testing, add "and one run of `make test-e2e` with the own-stack overrides, which creates `MY_LAKEHOUSE` and seeds `TYPED_DISTINCT_PROBE`" to the precondition sentence.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: plan.md § Out of scope, bullet 2; plan.md § Impact, bullet 2; plan.md § Implementation Tasks, task 3.2
- Issue: the plan ships two deviations from native Exasol and documents neither in `docs/semantic-differences.md`. `specs/mission.md` Core Capability 12 states: "A deviation is accepted only when it is documented in `docs/semantic-differences.md`." First, a pushed `DIV` by zero fails with SQL state `22002` where native Exasol reports `22012` (Impact bullet 2 and the zero-divisor scenario). Second, a guarded `DIV` by zero returns rows where native Exasol raises; Out of scope bullet 2 says this "is not listed in `docs/semantic-differences.md`". Task 3.2 edits only `capabilities.md` and `capability-matrix.md`. `docs/capabilities.md` § Scalar functions also tells readers that `semantic-differences.md` lists every function whose result differs. `FLOAT_DIV` carries the same undocumented deviations, but that is no license for a new capability to add more.
- Fix: Extend task 3.2: add a "Division by zero" section to `docs/semantic-differences.md`. It states that a pushed `DIV` by zero fails with SQL state `22002` instead of `22012`, and that a cheap guard conjunct such as `WHERE b <> 0 AND DIV(a, b) > 0` can return rows where native Exasol raises in either conjunct order. Then delete Out of scope bullet 2, because the doc section covers it.

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: `specs/sql-comprehension/vs-expression-translator-float-div/spec.md` line 22; `specs/datafusion-scan/scan-execution-expression-pushdown/spec.md` lines 41 and 43; `pushdown-capabilities/pushdown-planning-capability-extensions/spec.md` § Scenario: Arithmetic operator scalar-function capabilities are advertised (included in this plan's delta file without a DELTA marker); plan.md § Out of scope, bullet 3
- Issue: after this plan merges, recorded text contradicts the code. The float-div scenario says "`vs_checked_float_div(<left>, <right>)`, a call to the single checked-division function the crate declares", but the crate will declare two. The scan scenario says the session registers a function "under the exact name `crates/vs-expression` exports as its checked-division constant" and that "the registration adds one entry to the session's function registry", but task 2.1 registers two. The capability-extensions scenario says "each advertised arithmetic operator ... is never one the translator would decline", while the scalar-ops delta lists `FN_DIV` among "the arithmetic capabilities" and the typed pass declines it for decimal and DOUBLE operands. Out of scope bullet 3 names only the first of these and leaves it unfixed.
- Fix: Add a minimal `DELTA:CHANGED` block for each named scenario. Change "the single checked-division function the crate declares" to "the checked float-division function". Change "its checked-division constant" to "its checked float-division constant". Change "the registration adds one entry" to "the registrations add entries". In the capability-extensions arithmetic scenario, change "so an advertised operator is never one the translator would decline" to "so an operand shape the scan declines takes the path's fallback instead of failing". Each changed scenario must also drop its own implementation detail, as `.speq/plan-hook.md` requires. If that rewrite is too large for this plan, keep bullet 3, extend it to name all four locations, and leave this finding for the user to acknowledge.

#### [COMPLETENESS_GAP] FINDINGS
- Location: `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` § Scenario: A pushed-down integer division by zero fails the query
- Issue: the GIVEN reads "a `DIV` over integer operands that the scan computes, in the select list or in a WHERE filter". The scan also evaluates the checked function in an `ORDER BY` key of a pushed TopN, a projected `GROUP BY` key, a split-aggregate argument such as `SUM(DIV(a, b))`, and a broadcast-join fact-leg filter. Plan.md § Context says Exasol pushes `DIV` in group keys and aggregate arguments. The scenario leaves the zero-divisor outcome in those positions unstated. The `FLOAT_DIV` scan spec states it for the aggregate paths explicitly.
- Fix: Change the GIVEN to "a `DIV` over integer operands that the scan computes, in any position: select list, WHERE filter, ORDER BY key, GROUP BY key, aggregate argument, or a broadcast-join fact-leg filter". The existing E2E test stays the scenario's proof. The interview fixed the E2E positions, so this needs no new E2E row.

#### [IMPLEMENTATION_DETAIL] FINDINGS
- Location: `pushdown-capabilities/pushdown-planning-capability-extensions/spec.md` § Background, bullet 1
- Issue: "Exasol delegates an advertised capability fully and never re-checks it." restates the `AGENTS.md` § Architecture rules bullet "Once the adapter advertises a capability, Exasol delegates it fully and never re-checks it." `.speq/plan-hook.md` lists "Restatements of `AGENTS.md` architecture rules" as implementation detail that never goes in a spec. The delta authors this Background fresh under `DELTA:CHANGED`, so the rule applies. The scenario that needs the fact ("An advertised arithmetic expression ... remains correctness-safe") already states it in its own AND step.
- Fix: Delete the first sentence of Background bullet 1. Keep "Exasol evaluates an unadvertised capability itself over the rows the scan returns." only if a scenario step depends on it. Otherwise delete the whole bullet.

## Task Breakdown

#### [TRACEABILITY_GAP] FINDINGS
- Location: plan.md § Verification § Scenario Coverage, row 3; plan.md § Parallelization, group B `Knowledge`
- Issue: row 3 maps "Integer division over integer operands runs in the scan (`i64::MIN / -1`, result type per input)" to the test name "the integer cases of task 2.2", which is not a test name. `.speq/plan-hook.md` requires the mapping to name one test and its layer per scenario. Group B implements and tests that scenario's quotient and result type, but its `Knowledge` column lists only the zero-divisor scenario.
- Fix: In row 3, name the concrete test function from task 2.2, for example `integer_division_truncates_and_types_like_exasol`, and name that function in task 2.2. Add the scenario "Integer division over integer operands runs in the scan" to group B's `Knowledge` cell.

## Design Depth

#### [ADR_CONFLICT] BLOCKER
- Location: `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` § Scenario: Integer division over integer operands runs in the scan
- Issue: the scenario reads "*GIVEN* a pushed `DIV(<left>, <right>)` whose operands are each an integer (`DECIMAL(p,0)`) or NULL ... *THEN* the scan SHALL compute it, and its pushdown SQL SHALL call `vs_checked_int_div(<left>, <right>)`". Accepted ADR `split-aggregate-columns-take-exact-types-or-decline` records: "A nested `MIN`, `MAX`, or `SUM` over an expression, and an unprojected expression key, run through the wrapper". So `SELECT COUNT(*) FROM t GROUP BY DIV(ID, 10)` and `SELECT SUM(DIV(ID, 2)) + 1 FROM t` are pushed integer `DIV`s that Exasol computes, not the scan. The same holds when another part of the enclosing item declines, such as a select item that also contains an unadvertised function. The THEN is false on these well-formed requests, so the verifier cannot pass the scenario as written. The decision log has no `Supersedes:` entry.
- Fix: Conform to the ADR. Change the THEN to "*THEN* the `DIV` node SHALL NOT be the reason the scan declines the enclosing select item, filter, or key, and wherever the scan computes that enclosing expression, its pushdown SQL SHALL call `vs_checked_int_div(<left>, <right>)`". Keep the remaining AND steps unchanged.

#### [INFORMATION_COUPLING] FINDINGS
- Location: plan.md § Implementation Tasks, task 2.1
- Issue: task 2.1 adds `CheckedIntDivUdf` beside `CheckedFloatDivUdf` with "`Signature::any(2, Volatility::Immutable)`, the same `RaisedFailure` recording". It does not say which code the two share. Read literally, it copies the `{ signature, raised }` struct, `new`, the `PartialEq`, `Eq`, and `Hash` impls that exclude the failure slot, the two-argument check, the null-mask union, the skip-NULL-rows-before-the-check loop, and the record-then-`External` error mapping. Each of these is one design decision, which would then live in two places. The user asked for "one shared checked-division module for both divisions", and `.speq/plan-hook.md` forbids a plan that adds code duplicating existing code.
- Fix: Rewrite task 2.1 to make the sharing explicit. Use one `CheckedDivUdf` struct parameterized by a `CheckedDivKind { Float, Integer }` that owns the signature, the failure slot, the identity impls, the argument check, the null-mask union, and the record-and-raise mapping. The kind supplies `return_type`, the name constant, and the per-row checked quotient. `register_checked_div_udfs` registers one instance per kind. Update the Dead Code Removal table to also list `CheckedFloatDivUdf`.

no objection on the remaining Design Depth checks, axis checked: decision-log has no `Promotes to ADR: yes` entry, so `[ADR_OVERPROMOTION]` cannot apply. `Architecture: no change` holds, because `specs/architecture.md` names neither checked division nor registered scan functions (grep "checked", "division": none). ADR `advertised-capability-null-contract-is-adapters-to-reproduce` rejects a custom UDF only when a DataFusion function shares the Exasol name and arity, and DataFusion has no `div`. ADRs `capability-invariant-advertise-only-what-the-engine-can-back-correctly` and `no-exasol-side-fallback-for-an-advertised-capability` are met: `render_scan_expression` is the only DataFusion-dialect entry point (`render_expression` is `pub(crate)`), so the task 1.2 decline reaches every scan path.

## Prose Quality

#### [PROSE_UNCLEAR] FINDINGS
- Location: decision-log.md § Design Decisions [1], Rationale
- Issue: "Over `Decimal128(p,0)`, the type an Iceberg `long` and most integer expressions reach the scan as, it returns a scaled decimal". An Iceberg `long` column reaches the scan as `Int64`. Task 2.2 tests that type as the column case ("`Int64` `i64::MIN / -1`"). A computed integer such as `ID - 7` reaches it as `Decimal128(20,0)` through the exact-arithmetic rewrite. The sentence names the wrong actor for the decimal case and holds two claims.
- Fix: Replace the sentence with: "A computed integer such as `ID - 7` reaches the scan as `Decimal128(p,0)`. Over that type, `/` returns a scaled decimal (`-3.5`) that a cast rounds to `-4`."

# Plan: fix-float-div-truncation

## Summary

Render Exasol's `FLOAT_DIV` (`/`) as `(CAST(<left> AS DOUBLE) / <right>)` in the DataFusion dialect
so pushed-down division reproduces Exasol's always-`DOUBLE` `FN_FLOAT_DIV` instead of DataFusion's
operand-typed `/`, which truncated integer/integer division to an integer and both decimal/integer
and decimal/decimal division to scale 6 (issue #186). The Exasol dialect keeps its bare `/`
unchanged, and the residual divide-by-zero and 1-ULP behaviours are recorded from live measurement
rather than assumed.

## Design

### Context

Exasol's `FN_FLOAT_DIV` is *always* true float division and *always* results in `DOUBLE`, for every
operand-type combination — verified live against the Docker container, including a CTAS whose result
column types as `DOUBLE` in `EXA_ALL_COLUMNS` for every operand pairing. DataFusion 54.1's `/` is
operand-typed: `Int64/Int64` → truncating `Int64`, `Decimal128(18,2)/Decimal128(18,2)` →
`Decimal128(24,6)`, `Decimal128(18,2)/Int64` → `Decimal128(22,6)`. The translator mapped `FLOAT_DIV`
onto a bare `/`, sharing one arm with `ADD`/`SUB`/`MULT`, so every pushed-down division on integer or
decimal columns returned a silently wrong value — on **every row of a valid query**, in projected
values and in filter predicates alike, so wrong row counts as well as wrong numbers.

**All three operand classes were broken, not the two the issue claims.** Reproduced live through the
local virtual schema against a native-table oracle: int/int `7/2` → `3.0` vs `3.5`; decimal/int
`40.99/7` → `5.855714` vs `5.855714285714286`; **decimal/decimal**
`DECIMAL(9,2)/DECIMAL(20,4)` → `0.000102` vs `0.000102474999897525`. The issue's decimal/decimal
"control" is only accidentally correct when both operands carry few enough significant digits that
scale 6 loses none.

The translator cannot select a rendering by operand type. `crates/vs-expression` is specified as "a
pure, stateless, sibling-shared JSON-to-SQL translator with no column-type context", and decision
`016-add-fn-div-pushdown` records that the arithmetic arm "renders operands via recursive calls into
opaque SQL strings without inspecting their types". So the rendering must be correct for every
operand type at once, or the operator must be declined.

- **Goals** — make pushed-down division match native Exasol for every operand-type combination the
  Iceberg and Delta type domains admit; leave `ADD`/`SUB`/`MULT`/`NEG` byte-identical; leave every
  Exasol-dialect consumer's SQL byte-identical; state the residual divide-by-zero and 1-ULP
  behaviours from measurement rather than leaving them silent or overclaiming parity.
- **Non-Goals** — lifting the `DIV` decline; closing the `0/0`-to-silent-NULL gap (that is issue
  `#246`'s territory, and unsafe to close at the emit boundary); widening `arrow_value_at`'s `NaN`
  check to cover infinities; any change to advertised capabilities — `FN_FLOAT_DIV` stays advertised
  and stays translated; chasing bit-exactness against Exasol's internal decimal division.

### Decision

Cast the **left operand only**, **unconditionally**, in the **DataFusion dialect only**.

#### Architecture

```
                       function_scalar "FLOAT_DIV"
                                  │
              ┌───────────────────┴───────────────────┐
              ▼                                       ▼
    DataFusion dialect                        Exasol dialect
    (scan UDF's SQL frontend)                 (Exasol's own core engine)
              │                                       │
   (CAST(<l> AS DOUBLE) / <r>)                  (<l> / <r>)   ← UNCHANGED
              │                                       │
   DataFusion coerces <r> to Float64            Exasol's / IS FN_FLOAT_DIV
              │                                       │
        Float64 result  ──── matches ────►  DOUBLE PRECISION EMITS column
                                            (Exasol's own selectListDataTypes)
```

Three measured claims carry the design:

1. **One cast is enough.** On DataFusion 54.1 / arrow 58.3, `Int64÷Int64`, `Decimal128÷Int64`,
   `Int64÷Decimal128`, `Decimal128÷Decimal128` and `Float64÷Float64` all plan, execute, and yield
   `Float64` with only the left operand cast. `Float64 ÷ Decimal128` — the pairing most likely to
   trip coercion — was checked explicitly and coerces cleanly. Casting both would add a redundant
   no-op cast to every rendering.
2. **Unconditional is the only option, and that is what distinguishes `FLOAT_DIV` from `DIV`.**
   Type-blindness makes an unconditional cast the *only* faithful rendering of `FLOAT_DIV` — and the
   *reason no* faithful rendering of `DIV` exists, since `DIV` needs truncation, correct for integer
   operands and wrong for every other kind. The same constraint unblocks one operator and blocks the
   other; the `DIV` scenario's rationale is amended so the spec says this rather than resting on the
   divide-by-zero argument this plan measures and reframes.
3. **The Exasol dialect needs no cast.** Live-confirmed premise: Exasol's `/` is float division for
   every operand pairing, and a CTAS types every such result column `DOUBLE`. Adding the cast there
   would rewrite Exasol-facing SQL across the qualified single-table wrapper, the N-scan join
   wrapper, the grouped merge, the single-group scalar-over-aggregate merge and the self-applied
   WHERE path — including two byte-exact `dispatch_golden` fixtures and two native-oracle E2E string
   comparisons of the scalar-over-aggregate feature that landed on this branch — for zero
   correctness gain.

#### Patterns

| Pattern | Where | Why |
|---|---|---|
| Deliberate, test-pinned dialect divergence | the `FLOAT_DIV` arm | Mirrors `cast_char_target_diverges_between_dialects`: the divergence guard is *retargeted*, not deleted, so the new asymmetry stays pinned |
| Single owner for the target-type spelling | reuse `render_cast_target`'s `DOUBLE`/`DOUBLE PRECISION` → `DOUBLE` mapping | Avoids a second, independently-maintained spelling of the same target type in the same crate |
| Colocated pure string helper | a `cast_to_double(expr_sql: &str) -> String`-shaped wrapper, following `format_decimal_exasol_style` | The crate's established precedent for wrapping an already-rendered fragment; no JSON, no type context |
| Characterization test over a measured divergence | the two divide-by-zero scenarios | Pins behaviour that is known-divergent so a future change to it is noticed rather than silent |

### Consequences

| Decision | Alternatives Considered | Rationale |
|---|---|---|
| Cast in the DataFusion dialect only | Cast in both dialects (a single unconditional rendering, no `dialect` branch) | Both are semantically correct — Exasol's `/` is already float division, live-confirmed. DataFusion-only halves the change surface (6 sites instead of 11), leaves every Exasol-dialect golden and native-oracle E2E byte-identical, and confines a DataFusion type-coercion workaround to the dialect that needs it instead of leaking it into SQL a different engine parses. The cost — `FLOAT_DIV` becomes the first arithmetic operator whose rendering diverges by dialect — is paid by retargeting one existing guard test |
| Cast the left operand only | Cast both operands; cast the division's result (`CAST((<l> / <r>) AS DOUBLE)`) | Casting both is redundant under DataFusion's coercion. Casting the *result* is actively wrong: the truncation happens *during* the division, so a later cast merely widens an already-truncated value |
| Accept ~1 ULP parity for a non-zero-scale decimal numerator | Route the division through a high-scale decimal intermediate to chase bit-exactness | Measured: 1027 of 3335 rows differ by exactly one ULP, max relative difference `3.17e-16`; the int/int sweep is bit-exact at 0 of 3335. The residual is Exasol-internal — DataFusion returns byte-identical values to Exasol's *own* `CAST(A AS DOUBLE)/B` form — and relative error drops from ~1e-7 to ~2e-16. A decimal intermediate would cap out at precision 38, reintroduce decimal-division-by-zero errors, and chase a difference that is below the resolution of the `DOUBLE` column the value lands in |
| Record the divide-by-zero behaviour rather than emulate it | Render `NULLIF(<right>, 0)`; widen `arrow_value_at`'s `NaN` check to `!is_finite()`; decline `FLOAT_DIV` entirely | Measurement dissolved most of the concern: for `x/0` the query fails either way (`22002` post-fix from the engine rejecting `inf`, `22002` pre-fix from `Arrow error: Divide by zero error`, against Exasol's native `22012`) and no wrong value is returned. The one silent case is `0/0` → `NaN` → NULL on the raw-scan path, which is already reachable today for `DOUBLE`-typed numerators with no cast involved, so it is `#246`'s tracked gap widened rather than a new defect. `NULLIF` would substitute exactly the wrong answer already observed and would additionally silence the `x/0` case that currently fails loudly. Widening the emit check cannot distinguish a *computed* non-finite value from one legitimately *stored* — Iceberg and Delta both type these columns IEEE-754 `double`, Parquet `DOUBLE` admits `±Inf`/`NaN` — so it would break valid reads. Declining `FLOAT_DIV` would withhold the fix from the most common arithmetic operator and regress the shipped scalar-over-aggregate decomposition, which depends on `SUM(x) / COUNT(*)` rendering |
| Leave the `DIV` decline in place, amend only its rationale | Lift the decline with a `TRUNC(<l>/<r>)` emulation | Out of scope, and it faces the harder problem: `DIV` must truncate, so a wrong rendering is wrong on every row for non-integer operands. Only the now-inconsistent divide-by-zero clause of its recorded rationale is corrected |

### Table-format specification check (CLAUDE.md obligation)

This change touches pushdown, so it was checked against both specifications rather than assumed.
**Neither imposes any requirement on how an engine computes or types a division.** "division",
"divisor" and "arithmetic" appear nowhere normatively in either document; the single "divide" hit in
the Iceberg spec concerns Roaring-bitmap key splitting in `#### Deletion Vectors`. Both fix only the
*stored* operand domain and the widening relation over it:

- Iceberg `## Specification` → `### Schemas and Data Types` → `#### Primitive Types`: `int` —
  "32-bit signed integers"; `long` — "64-bit signed integers"; `double` — "64-bit IEEE 754 floating
  point"; `decimal(P,S)` — "Fixed-point decimal; precision P, scale S", "Scale is fixed, precision
  must be 38 or less". Its `#### Schema Evolution` permits only `int`→`long`, `float`→`double`, and
  `decimal(P, S)`→`decimal(P', S)` "if `P' > P`", "Widen precision only".
- Delta `## Schema Serialization Format` → `### Primitive Types`: `integer` — "4-byte signed
  integer"; `long` — "8-byte signed integer"; `double` — "8-byte double-precision floating-point
  numbers"; `decimal` — "signed decimal number with fixed precision ... The precision and scale can
  be up to 38." Its `# Type Widening` additionally permits "`Byte`, `Short` or `Int` -> `Double`"
  and "`Byte`, `Short` or `Int` -> `Decimal(10 + k1, k2)` where `k1 >= k2 >= 0`", with
  `## Reader Requirements for Type Widening` requiring readers to "convert such values to the
  current, wider type".

Consequence for this design: the operand Arrow type reaching the operator is legitimately any of
`Int32`/`Int64`/`Decimal128`/`Float32`/`Float64`, and on Delta it can **change between table
versions**. That is a further argument for the unconditional, type-blind cast over any
type-conditional rendering. **There is no format-spec deviation to fix or to track here** — the
divide-by-zero behaviour is an Exasol-vs-DataFusion SQL semantics matter, outside both
specifications.

## Features

| Feature | Status | Spec |
|---|---|---|
| `sql-comprehension/vs-expression-translator-scalar-ops` | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |
| `sql-comprehension/vs-expression-translator-scalar-fns` | CHANGED | `sql-comprehension/vs-expression-translator-scalar-fns/spec.md` |

## Impact

**Breaking change to returned values — this is the point of the fix.** Any query whose pushed-down
select list or WHERE predicate divides integer or decimal columns changes its result: from a
truncated quotient to Exasol's true `DOUBLE` quotient, and from a possibly wrong row count to the
correct one. Relative error against native Exasol drops from ~1e-7 to ~2e-16; the result is
bit-exact for a scale-0 numerator and within ~1 ULP for a non-zero-scale decimal numerator. Only
divisions whose left operand is already `DOUBLE` are numerically unchanged.

No capability change: `FN_FLOAT_DIV` stays advertised and stays translated, so no query moves
between the pushdown and the Exasol-evaluated path. No Exasol-facing SQL changes, so no wrapper,
merge, or golden fixture shape moves.

**Residual behaviours, measured not assumed.** A projected `x/0` fails the query at SQL state
`22002` (the engine rejecting `inf`) where native Exasol fails at `22012` — a message-and-state
difference, not a wrong value, and one that pre-dates this change. A projected `0/0` returns a
silent NULL where native Exasol errors: that is issue `#246`'s tracked NaN-at-emit gap, already
reachable today for `DOUBLE`-typed numerators, widened here to integer and decimal numerators. A
divide-by-zero in a PREDICATE position is not yet measured and task 1.2 owns it.

## Dependencies

None. No new crate, no dependency bump, no SLC change — the change is one rendering arm in
`crates/vs-expression`. A `.so` rebuild is needed only to run the E2E gate.

## Implementation Tasks

> **Planning already reproduced the bug locally**, per CLAUDE.md § Verification discipline: against
> the running local stack (`exasol/docker-db:2025.2.1`, `MY_LAKEHOUSE` VS, pre-fix `.so`), all four
> facets were confirmed against native-table oracles, and the fix shape was proven end-to-end using
> a user-side `CAST(... AS DOUBLE)` that `EXPLAIN VIRTUAL` shows pushes down as the exact fragment
> the fix will emit. Phase 1 turns those captures into failing tests rather than re-discovering them,
> and closes the evidence gaps planning could not.

### 1. Failing E2E repro and the remaining live measurements (Group A)

- [ ] 1.1 Bring up the local stack — `make test-e2e` does **not** start it
      (`docker compose up -d --wait minio minio-init iceberg-rest exasol`); check for a stray
      `bench/.env` first (CLAUDE.md § Bench harness gotchas). Add E2E tests that FAIL against the
      current `.so`, each against a native-table oracle: **int/int** and the **filter row count**
      over `FACT_LINEITEM` (`L_ORDERKEY`, `L_LINENUMBER`, both `DECIMAL(20,0)`, seeded
      `L_ORDERKEY=7, L_LINENUMBER ∈ {1,2}`) in `tests/e2e_scan_test.rs`; **decimal/int** and
      **decimal/decimal** over `TYPED_DISTINCT_PROBE` (`C_DECIMAL_A` `DECIMAL(9,2)`, `C_DECIMAL_B`
      `DECIMAL(20,4)`) in `tests/e2e_capability_test.rs`, which is where that fixture is wired, or
      over `FACT_ORDERS.O_TOTALPRICE` (`DECIMAL(10,2)`) if `e2e_scan_test.rs` is preferred. Assert
      a **relative tolerance** (~1e-15), not string equality, for every decimal-numerator shape —
      Exasol's own decimal division differs from the cast form by up to 1 ULP; a scale-0 numerator
      may be asserted bit-exact.
- [ ] 1.2 Measure the one case planning could not: a divide-by-zero in a **PREDICATE** position
      (`WHERE <a> / <b> > <k>` with a zero divisor). An infinity compared against a bound never
      reaches the emit boundary, so unlike the projection path this could silently admit or reject
      rows where Exasol refuses the query at `22012`. Use a non-foldable divisor (`(col - col)`;
      no Iceberg fixture has a zero-valued column) and verify with `EXPLAIN VIRTUAL` that the
      predicate really is pushed. Also measure the same shape through the **join/broadcast** pushdown
      path, which planning did not exercise. Record the results in `decision-log.md`; if either
      diverges, task 6.1 files the issue and the spec delta's placeholder clause is replaced with its
      number — if neither does, record it as verified-safe in the spec rather than leaving the clause
      hypothetical. [expert]

### 2. The rendering change (Group B)

- [ ] 2.1 In `crates/vs-expression/src/lib.rs`, split `FLOAT_DIV` out of the shared
      `format!("({left} {op} {right})")` in the `"ADD" | "SUB" | "MULT" | "FLOAT_DIV"` arm so the
      DataFusion dialect emits `(CAST(<left> AS DOUBLE) / <right>)` and the Exasol dialect keeps the
      bare form. Keep ONE arm — the arity check, operand rendering and error messages are shared, so
      only the final assembly branches. Take the `DOUBLE` spelling from the existing
      `render_cast_target` mapping rather than hardcoding a second copy, and follow
      `format_decimal_exasol_style`'s colocated-pure-helper shape for the wrapper. Refresh the
      operator-list comments in the module doc and above the arm.

### 3. Repin the pinned renderings (Group C)

- [ ] 3.1 `crates/vs-expression/src/lib_tests.rs`: `arithmetic_operator_set_matches_advertised_capabilities`
      — the shared `format!` template no longer holds for all four rows, so carry an expected string
      per row instead of just an operator; `renders_arithmetic_div` — expect
      `(CAST("A" AS DOUBLE) / 2)`.
- [ ] 3.2 `crates/vs-expression/src/lib_tests.rs`: retarget
      `arithmetic_operators_render_identically_in_both_dialects` — keep it asserting identity for
      `ADD`/`SUB`/`MULT` and add the `FLOAT_DIV` **divergence** assertion (DataFusion casts, Exasol
      does not), following `cast_char_target_diverges_between_dialects`. Amend its doc comment; do
      not delete the guard.
- [ ] 3.3 Add DataFusion-dialect unit tests for the operand matrix and NULL propagation: left
      operand a column / a literal / a nested expression / an aggregate, each right-operand shape,
      and NULL on either side plus NULL-over-zero. Assert the rendered string only — these are pure,
      I/O-free renderings.

### 4. Prove the Exasol dialect did not move (Group D)

- [ ] 4.1 Confirm — do not update — the four Exasol-dialect sites the census identified as
      byte-identical under this design: `lib_tests.rs`'s
      `exasol_dialect_renders_declared_verbatim_surface` `FLOAT_DIV` fixture (`("A" / "B")`),
      `scalar_over_agg_tests.rs`'s `render_substitutes_the_callers_merged_expressions_by_plan_slot`,
      `single_group_agg_tests.rs`'s
      `merge_select_interleaves_items_in_selectlist_order_with_per_item_casts`, and both
      `testdata/dispatch_golden/single_group_scalar_over_aggregate_{dedup,interleaved}.sql`. A diff
      in any of these means the change leaked into the Exasol dialect and is a regression, not an
      expected update. Also confirm the AVG and König–Huygens merge `/` fragments in
      `scalar_over_agg.rs` and `support_tests.rs`'s `sql.contains(" / ")` are untouched — those are
      adapter-authored, not translator output.

### 5. Close the evidence gaps and pin the divergences (Group E)

- [ ] 5.1 Confirm the fix from the **translator's own output**, not a proxy: planning proved the
      fix shape with a user-side `CAST(... AS DOUBLE)`, so add an `EXPLAIN VIRTUAL` assertion that
      the pushed projection for a bare `<a> / <b>` select item is
      `(CAST("A" AS DOUBLE) / "B")` with `"emit_exa_types":["DOUBLE PRECISION"]`, and re-run task
      1.1's tests against a freshly built `.so` so they pass.
- [ ] 5.2 Add the two divide-by-zero **characterization** tests, asserting the measured behaviour so
      a future change to it is noticed rather than silent, each with a doc comment saying it pins a
      known divergence: `x/0` projected — the query FAILS at `22002` with the engine's
      `numeric value out of range: value inf` (native Exasol fails at `22012`); `0/0` projected — the
      query SUCCEEDS with a silent NULL, citing `#246` as the owning gap and noting the
      partial-aggregate path errors on the same input. Use `(col - col)` as the divisor.
- [ ] 5.3 Re-run the two existing native-oracle scalar-over-aggregate E2E tests
      (`e2e_scan_test.rs`'s `..._shared_count_matches_native_oracle` and `..._interleaved_...`) and
      `e2e_join_test.rs`'s two `RETURN_PCT` string-equality tests. Under this design their SQL is
      unchanged, so they must pass **unedited** — the census flagged them as the sites that would
      have been at risk had the cast been rendered in the Exasol dialect.

### 6. Track what remains (Group F)

- [ ] 6.1 Act on task 1.2's measurement. If the predicate-position or join-path divide-by-zero
      diverges silently, file a GitHub issue scoped to exactly that — a division by zero inside a
      pushed predicate never reaches the emit boundary, so neither the `inf` rejection nor `#246`'s
      NaN check can catch it — and replace the placeholder clause in the spec delta with its number.
      If it does not diverge, replace that clause with the verified-safe statement instead. Do **not**
      file a second issue for the `0/0` silent NULL: `#246` already owns the raw-scan NaN-at-emit gap
      and this feature only widens its reachability, which the spec delta records inline.

### 7. Verification (Group G)

- [ ] 7.1 `cargo test` — 0 failures.
- [ ] 7.2 `cargo clippy --all-targets && cargo fmt` — 0 warnings, no reformatting.
- [ ] 7.3 `make cross-musl-udf-build` then `make test-e2e` against the live local stack — 0 failures.

## Call-Site Census

Handed over as a checklist because host `cargo test` does not compile the feature-gated e2e crates,
so an omission surfaces only at the e2e gate. Every line number was located during planning and may
drift — re-locate each with Serena before editing.

**Production — 1 site.** `crates/vs-expression/src/lib.rs`, the
`"ADD" | "SUB" | "MULT" | "FLOAT_DIV"` arm of `render_expression_inner` (arm ~996-1020, final
`format!` ~1019). Reuse facts: `render_cast_target`'s `"DOUBLE" | "DOUBLE PRECISION" => "DOUBLE"` arm
(~493) is **dialect-invariant**, so the spelling needs no branch; the only `CAST(... AS ...)`
`format!` in the crate is at the end of `render_cast` (~591) and is JSON-driven, so it cannot be
called with a pre-rendered fragment; `format_decimal_exasol_style` (~547) is the crate's precedent
for a pure helper that wraps an already-rendered fragment. `enum Dialect { DataFusion, Exasol }`
(~78). `TRANSLATED_SCALAR_FNS` carries `("FLOAT_DIV", ExasolForm::Shaped)` (~122) and stays
`Shaped` — no change. Comments to refresh: module doc (~41), the capability-lockstep comment above
the arm (~993).

**MUST UPDATE — `crates/vs-expression/src/lib_tests.rs` (3 sites).**
`arithmetic_operator_set_matches_advertised_capabilities` (~391-420): `FLOAT_DIV` table row (~397)
and the shared `format!` template (~414) — expected `("L_EXTENDEDPRICE" / "L_DISCOUNT")` becomes
`(CAST("L_EXTENDEDPRICE" AS DOUBLE) / "L_DISCOUNT")`, and the uniform template must give way to a
per-row expected string. `renders_arithmetic_div` (~432): `("A" / 2)` becomes
`(CAST("A" AS DOUBLE) / 2)`. `arithmetic_operators_render_identically_in_both_dialects`
(~3407-3438): table row ~3417 and `expected` template ~3428 — becomes a *divergence* assertion for
`FLOAT_DIV` and an identity assertion for the other three; amend the doc comment ~3408-3410.

**MUST NOT CHANGE — Exasol-dialect sites (verify byte-identical, 4 sites).**
`lib_tests.rs`'s `exasol_dialect_renders_declared_verbatim_surface` `FLOAT_DIV` `shaped(...)`
fixture (~3166-3170, expects `("A" / "B")`; compared with exact `assert_eq!`). Its banned-token
sweep (~3369-3388) does not list `CAST`, so it stays green either way.
`scalar_over_agg_tests.rs`'s `render_substitutes_the_callers_merged_expressions_by_plan_slot`
(assertion ~261, `"ROUND((<MERGED_SUM> / <MERGED_COUNT>), 2)"`) — `render_scalar_over_merge`
(`scalar_over_agg.rs` ~182) calls `render_expression_exasol`, so this stays put.
`single_group_agg_tests.rs`'s `merge_select_interleaves_items_in_selectlist_order_with_per_item_casts`
(golden ~1060). `testdata/dispatch_golden/single_group_scalar_over_aggregate_dedup.sql` and
`..._interleaved.sql` (both line 1). Under a both-dialects design all four WOULD have changed; that
they do not is this design's payoff, so treat any diff as a regression.

**UNAFFECTED — verified, no action.** `capabilities.rs` (~53-57) and `capabilities_tests.rs`
(~240, ~480) — capability-name membership only. `joins/sql_builders_tests.rs`'s
`render_expression_qualified_renders_scalar_over_aggregate` (~1650-1693) — renders the node but
asserts only `contains` on the nested-aggregate substrings, never the `/`.
`lib_tests.rs::render_expression_renders_scalar_wrapping_aggregates` (~2350-2400) — same.
`grouped_agg_tests.rs`'s `round_pct_over_aggregates()` fixture (~1770-1785) and its three consumers,
and `grouped_undecomposable_falls_back_to_qualified_wrapper` (~2005-2094) — substring and structural
assertions only; note `!soa.contains("CASE")` survives because the new token is `CAST`, and
`outer_select_items` (~1500-1529) is a depth-aware paren-tracking splitter, so extra nesting is safe.
`single_group_agg_tests.rs`'s other eight `float_div()` call sites (~112, 188, 248, 283, 895, 921,
945) — plan/type/structure assertions. `support_tests.rs` (~1409-1416) `sql.contains(" / ")` — the
AVG merge, adapter-authored. `scalar_over_agg.rs` (~357, 427-443) — AVG and König–Huygens merge
formulas, adapter-authored, must not be touched. `lib.rs` (~1472-1473) `HOURS_BETWEEN`/
`MINUTES_BETWEEN` `/ 3600` and `/ 60` — a different arm. `dispatch_golden/`
`single_group_all_agg_kinds.sql`, `grouped_all_agg_kinds.sql`,
`single_group_scalar_over_variance.sql` — their `/` is adapter-authored merge SQL, not a
`FLOAT_DIV` node. `e2e_scan_test.rs`'s `test_group_by_expression_key` and
`test_group_by_avg_correctness` (~1850-1952) — `CAST(score / 25.0 AS DECIMAL(4,0))` over a `Float64`
`score`, so the new cast is a semantic no-op and their value assertions hold. `docs/capabilities.md`
(~77) — capability table. `specs/vs-adapter/pushdown-planning-capability-extensions/spec.md`
(~100-106) — advertises the capability, pins no shape.

**E2E fixtures for the new repro tests.** `e2e_scan_test.rs` reaches `events` (`vs_table()`: `id`
`Int64`, `score` `Float64`, **no decimal column**), `fact_lineitem` (`vs_lineitem_table()`:
`L_ORDERKEY`/`L_LINENUMBER`/`L_SUPPKEY`/`L_QUANTITY` all `Int64` → `DECIMAL(20,0)`,
`L_EXTENDEDPRICE` `Float64`) and `fact_orders` (`vs_fact_table()`: `O_TOTALPRICE`
`Decimal128(10,2)`, `O_ORDERKEY`/`O_CUSTKEY` `Int64`). The richest decimal fixture,
`typed_distinct_probe` (`c_decimal_a` `DECIMAL(9,2)`, `c_decimal_b` `DECIMAL(20,4)`, `c_qty`
`DECIMAL(20,0)`, `c_double`/`c_price` `DOUBLE`), is wired only into `e2e_capability_test.rs` and
`e2e_count_distinct_test.rs` — planning's decimal/decimal repro used it, so the decimal shapes
belong there unless `fact_orders` suffices. `e2e_capability_test.rs`'s
`e2e_selectlist_expression_pushdown` (~399-441, `SELECT id, score * 2.0, UPPER(name)`) is the
closest existing template for a DataFusion-dialect projection-expression test. Note the TPC-H
`LINEITEM`/`CUSTOMER` tables the issue's repro used exist only via `tests/tpch_loader.rs`, which is
bench-only and not part of `make test-e2e` — hence the fixture substitution.

## Parallelization

| Parallel Group | Tasks |
|---|---|
| Group A | 1.1, 1.2 |
| Group B | 2.1 |
| Group C | 3.1, 3.2, 3.3 |
| Group D | 4.1 |
| Group E | 5.1, 5.2, 5.3 |
| Group F | 6.1 |
| Group G | 7.1, 7.2, 7.3 |

Sequential dependencies:
- Group A → Group B. The repro tests must fail against the pre-fix `.so` before the arm changes, and
  task 1.2's measurement is what task 5.2's characterization tests and task 6.1's decision rest on.
- Group B → Group C → Group D. The renderings can only be repinned after the arm changes, and the
  byte-identity proof is only meaningful once both are done.
- Group D → Group E → Group F → Group G. Task 6.1 replaces a placeholder clause in the spec delta,
  so it runs after the behaviour it describes is pinned by tasks 5.1-5.2.
- 1.1 and 1.2 share one stack bring-up and may run together; 3.1, 3.2 and 3.3 touch one file and may
  run together.

## Dead Code Removal

| Type | Location | Reason |
|---|---|---|
| — | — | None. The change replaces one `format!` expression with a branch; no function, test, or module becomes unreachable. The `op` lookup's `"FLOAT_DIV" => "/"` entry stays live if the branch reuses the shared template, and becomes removable only if the implementer chooses a fully separate assembly — in which case remove it rather than leaving a dead arm |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|---|---|---|---|
| Arithmetic operators translate to binary SQL expressions (CHANGED) | Unit | `crates/vs-expression/src/lib_tests.rs` | `arithmetic_operator_set_matches_advertised_capabilities` |
| FLOAT_DIV renders true float division in the DataFusion dialect (NEW) | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_arithmetic_div` + the operand-matrix and NULL-propagation tests from task 3.3 |
| FLOAT_DIV renders true float division in the DataFusion dialect (NEW) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs`, `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_float_div_int_over_int_matches_native_oracle`, `e2e_float_div_filter_row_count_matches_native_oracle`, `e2e_float_div_decimal_over_int_matches_native_oracle`, `e2e_float_div_decimal_over_decimal_matches_native_oracle`, `e2e_float_div_pushes_double_cast_projection` (the `EXPLAIN VIRTUAL` assertion from task 5.1) |
| The Exasol dialect keeps rendering FLOAT_DIV as a bare division operator (NEW) | Unit | `crates/vs-expression/src/lib_tests.rs` | `arithmetic_operators_render_identically_in_both_dialects` (retargeted), `exasol_dialect_renders_declared_verbatim_surface` (unedited) |
| The Exasol dialect keeps rendering FLOAT_DIV as a bare division operator (NEW) | Integration | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `single_group_scalar_over_aggregate_dedup_matches_golden`, `single_group_scalar_over_aggregate_interleaved_matches_golden` (both unedited, byte-identical goldens) |
| A pushed-down division by zero fails the query rather than returning a wrong value (NEW) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_by_zero_fails_the_query` (characterization) |
| Zero divided by zero reaches the tracked NaN-at-emit gap (NEW) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_zero_over_zero_returns_null_tracked_by_246` (characterization), plus the task 1.2 predicate-position and join-path measurements |
| Integer division DIV is deliberately not translated (CHANGED) | Unit | `crates/vs-expression/src/lib_tests.rs` | `div_falls_through_as_unsupported` (unedited — only the scenario's recorded rationale changes) |
| FLOAT_DIV stays outside the verbatim rule in both dialects (NEW, scalar-fns) | Unit | `crates/vs-expression/src/lib_tests.rs` | `exasol_dialect_renders_declared_verbatim_surface` (its `FLOAT_DIV` `shaped(...)` fixture, unedited — `TRANSLATED_SCALAR_FNS` keeps `("FLOAT_DIV", ExasolForm::Shaped)`) |

### Manual Testing

| Feature | Command | Expected Output |
|---|---|---|
| Scalar Operations — int/int division | `exapump sql -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0" -q "SELECT L_ORDERKEY, L_LINENUMBER, L_ORDERKEY/L_LINENUMBER FROM MY_LAKEHOUSE.FACT_LINEITEM WHERE L_ORDERKEY=7 ORDER BY L_LINENUMBER"` | `7.0` and `3.5`, matching the native-table oracle bit-exactly — not `3` and not `3.0` |
| Scalar Operations — decimal/int division | `exapump sql -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0" -q "SELECT ID, C_DECIMAL_A, C_DECIMAL_A/7 FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE ID=6"` | `5.855714285714286` (within ~1 ULP of the native oracle) — not `5.855714` |
| Scalar Operations — decimal/decimal division | `exapump sql ... -q "SELECT ID, C_DECIMAL_A/C_DECIMAL_B FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE ID IN (1,6)"` | `0.00010499999989500001` / `0.000102474999897525` within ~1 ULP — not `0.000104` / `0.000102` |
| Scalar Operations — filter facet | `exapump sql ... -q "SELECT COUNT(*) FROM MY_LAKEHOUSE.FACT_LINEITEM WHERE L_ORDERKEY/L_LINENUMBER > 3 AND L_ORDERKEY = 7"` | `2`, equal to the native-table count — not `1` |
| Scalar Operations — the translator's own output | `exapump sql ... -q "EXPLAIN VIRTUAL SELECT L_ORDERKEY/L_LINENUMBER FROM MY_LAKEHOUSE.FACT_LINEITEM"` | Pushed projection reads `(CAST(\"L_ORDERKEY\" AS DOUBLE) / \"L_LINENUMBER\")` with `"emit_exa_types":["DOUBLE PRECISION"]` |
| Scalar Operations — Exasol dialect unmoved | `exapump sql ... -q "EXPLAIN VIRTUAL SELECT COUNT(*), ROUND(SUM(L_QUANTITY)/COUNT(*), 2) FROM MY_LAKEHOUSE.FACT_LINEITEM"` | The merge item still reads `ROUND((SUM(\"PARTIAL_sum_0\") / SUM(\"PARTIAL_count_1\")), 2)` with **no** `CAST(... AS DOUBLE)` inside it |
| Scalar Operations — `x/0` | `exapump sql ... -q "SELECT L_ORDERKEY/(L_LINENUMBER-L_LINENUMBER) FROM MY_LAKEHOUSE.FACT_LINEITEM"` | Query FAILS with `numeric value out of range: value inf ...` (SQL state `22002`); the native table fails with `division by zero` (`22012`) |
| Scalar Operations — `0/0` | `exapump sql ... -q "SELECT CAST(L_LINENUMBER-L_LINENUMBER AS DOUBLE)/(L_LINENUMBER-L_LINENUMBER) FROM MY_LAKEHOUSE.FACT_LINEITEM"` | Query SUCCEEDS returning NULL — the recorded `#246` gap; the native table fails with `22012` |

### Checklist

| Step | Command | Expected |
|---|---|---|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` (stack brought up manually first) | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |

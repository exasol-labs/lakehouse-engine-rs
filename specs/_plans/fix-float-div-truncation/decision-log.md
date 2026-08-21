# Decision Log: fix-float-div-truncation

## Interview

**Q:** Should this plan also address Exasol vs. DataFusion divide-by-zero semantics for `FLOAT_DIV`
(Exasol's `FN_FLOAT_DIV` may raise/return differently than DataFusion float division on `x/0`), or
stay strictly scoped to the truncation fix?

**A:** Investigate and fix divide-by-zero too. Live-verify against the local Docker Exasol E2E
harness what native Exasol `FN_FLOAT_DIV` does on division by zero — raises an error (and if so,
which SQL state), returns NULL, or returns infinity — and compare against what
`CAST(<left> AS DOUBLE) / <right>` produces in DataFusion on the same shapes. If they diverge,
design and include a fix for that divergence in this same plan (an emulation in the rendered SQL, or
a documented decline of the expression back to Exasol-side evaluation, following the existing
pattern used for `DIV` and the bitwise operators — see the "Integer division DIV is deliberately not
translated" and "Bitwise operator functions are deliberately not translated" scenarios in
`specs/sql-comprehension/vs-expression-translator-scalar-ops/spec.md` for the established convention
of declining to translate when DataFusion cannot faithfully reproduce Exasol semantics). If they
match, state that explicitly in the spec delta as a verified-safe case, not a silent assumption. Do
NOT rely on memory or documentation — it must be a live check against the running Exasol container
per CLAUDE.md § Verification discipline.

**Q:** Should the plan include a task to reproduce the truncation bug and re-verify the fix against
the local Docker Exasol container, given the issue was only verified on staging?

**A:** Yes, required. Per CLAUDE.md § Verification discipline: "A reported bug MUST be reproduced
locally against the Docker Exasol container before it is fixed. Do not trust an issue's claimed
repro... run the query." Include an explicit task, before the code-change tasks are considered done,
to (1) bring up the local Exasol Docker E2E stack, (2) reproduce the three failing repro queries
from the issue against it — or equivalent local fixtures, since the issue's repro used a staging
`DBXA`/`DBXA_COPY` TPC-H setup that may not exist locally, so adapt to whatever local E2E fixture
already exercises `FLOAT_DIV` — (3) confirm the fix resolves them locally, and (4) run the
divide-by-zero live check from the first answer against the same local container.

## Design Decisions

### [1] Render the DOUBLE cast in the DataFusion dialect only, not in both dialects

- **Decision:** `FLOAT_DIV` renders `(CAST(<left> AS DOUBLE) / <right>)` through the DataFusion-dialect
  entry points and keeps the bare `(<left> / <right>)` through the Exasol-dialect ones. `FLOAT_DIV`
  becomes the first arithmetic operator whose rendering diverges by dialect.
- **Alternatives:** Render the cast unconditionally in both dialects — the simpler code (no
  `dialect` branch) and the shape issue #186's "decided approach" literally describes. Rejected.
- **Rationale:** Exasol's own `/` **is** `FN_FLOAT_DIV`, so the Exasol dialect is already correct and
  the cast there is a semantic no-op. The premise was treated as load-bearing and verified live
  rather than assumed: `7/2 = 3.5` on integer literals, `CAST(711.56 AS DECIMAL(18,2))/CAST(7 AS
  DECIMAL(18,0)) = 101.65142857142857` at full double precision, the same over columns so nothing is
  constant-folded, and a CTAS whose result column types as `DOUBLE` in `EXA_ALL_COLUMNS` for every
  operand pairing including `DECIMAL/DECIMAL` and `DECIMAL/DOUBLE`. Had any of those returned a
  truncated value the design would have inverted. Rendering the cast anyway would rewrite
  Exasol-facing SQL across the
  qualified single-table wrapper, the N-scan join wrapper, the grouped merge, the single-group
  scalar-over-aggregate merge and the self-applied WHERE path — 5 extra must-update sites including
  two byte-exact `dispatch_golden` fixtures and two native-oracle E2E string comparisons of the
  scalar-over-aggregate feature that landed on this very branch — for zero correctness gain. It would
  also leak a DataFusion type-coercion workaround into SQL parsed by a different engine, whose
  parser this repo has repeatedly been bitten by (CHAR CAST, `%`, `SIGNUM`, TSTZ EMITS). Confining
  the workaround to the dialect that needs it states the real rule: *the translator's job is to make
  each target engine reproduce Exasol's `FN_FLOAT_DIV`, and Exasol needs no help.* Note this narrows
  rather than contradicts the issue's decided approach — the issue's three staging repros were all
  DataFusion-dialect (two projections and a filter); the Exasol dialect was never exercised by that
  verification. The cost is one existing guard test retargeted from an identity assertion to a
  divergence assertion, which is exactly how the CHAR CAST divergence is already handled in
  `sql-comprehension/vs-expression-translator-cast`.
- **Promotes to ADR:** yes

### [2] Cast the left operand only, unconditionally, with no type inspection

- **Decision:** Cast exactly one operand — the left — and do so for every `FLOAT_DIV` node
  regardless of what the operands look like.
- **Alternatives:** Cast both operands; cast the division's result
  (`CAST((<l> / <r>) AS DOUBLE)`); cast conditionally on operand type.
- **Rationale:** DataFusion's binary type coercion promotes the other side of a `Float64` operand to
  `Float64`, so one cast forces float division for every operand-type combination; a second cast
  would be a no-op on every rendering. Casting the *result* is actively wrong — the truncation
  happens *during* the division, so a later cast just widens an already-truncated value. Conditional
  casting is impossible by design: `crates/vs-expression` is specified as a pure, stateless
  translator "with no column-type context", and decision `016-add-fn-div-pushdown` records that the
  arithmetic arm "renders operands via recursive calls into opaque SQL strings without inspecting
  their types". The Iceberg/Delta check reinforces this: the operand type can legitimately be
  `Int32`/`Int64`/`Decimal128`/`Float32`/`Float64`, and Delta's `§ Type Widening` lets it *change
  between table versions*, so any type-conditional rendering would be betting on metadata the
  translator cannot see.
- **Promotes to ADR:** no

### [3] Record the divide-by-zero behaviour from measurement; do not emulate it

- **Decision:** Measure each divide-by-zero case live and record it, splitting what the interview
  posed as one question into three cases with different owners. `x/0` (non-zero numerator): the scan
  produces `±Inf`, the Exasol engine **rejects it at the emit boundary** at SQL state `22002`
  (`numeric value out of range: value inf ...`), so the query fails and no wrong value is returned —
  an accepted message-and-state difference from Exasol's native `22012`, which the pre-fix rendering
  already had (`Arrow error: Divide by zero error`, also `22002`). `0/0`: yields `NaN`, and the
  raw-scan `emit_batch` path delivers a silent NULL — that is issue **`#246`**'s already-tracked
  NaN-at-emit gap, so cite it inline rather than filing a duplicate. Predicate-position
  divide-by-zero: **not yet measured**, owned by task 1.2, and tracked separately only if it
  diverges.
- **Alternatives:** (a) render `NULLIF(<right>, 0)` so a zero divisor yields NULL; (b) widen the
  existing `NaN` domain-error check in `arrow_value_at` (`scan/convert.rs`) from `is_nan()` to
  `!is_finite()`; (c) decline `FLOAT_DIV` entirely and let Exasol evaluate it, the `DIV` precedent;
  (d) file one blanket tracked-exception issue for "divide-by-zero divergence".
- **Rationale:** Live measurement dissolved most of the concern the interview raised, and the
  planning draft that assumed a silent wrong value for `x/0` was **wrong** — the check mattered.
  Native Exasol raises `22012` for every operand pairing including `DOUBLE/DOUBLE`, column-driven,
  and never returns NULL or infinity; and Exasol admits no non-finite `DOUBLE` at all
  (`CAST('inf' AS DOUBLE)` → `22018`, `1E400` → `22003`), which is exactly why an infinity cannot
  reach a result set through the projection path. So `x/0` needs no fix: the query fails before and
  after. Each alternative then fails on its own terms. (a) `NULLIF` would substitute NULL — precisely
  the wrong answer already observed in the `0/0` case — would conflate a zero divisor with a NULL
  divisor, and would additionally *silence* the `x/0` case that currently fails loudly. (b) is the
  decisive one: **the emit boundary cannot distinguish a computed non-finite value from one
  legitimately stored in the source table.** Iceberg types these columns "64-bit IEEE 754 floating
  point" and Delta "8-byte double-precision floating-point numbers", and Parquet `DOUBLE` admits
  `±Inf` and `NaN`, so such a check would break reading a table that legitimately contains them —
  and it would miss the predicate case entirely, which never reaches that boundary. (c) would
  withhold the truncation fix from the most common arithmetic operator and regress the shipped
  scalar-over-aggregate decomposition, which depends on `SUM(x) / COUNT(*)` rendering. (d) would be
  inaccurate scoping: `0/0` is verifiably reachable **today**, with no cast involved, whenever the
  numerator column is already `DOUBLE`-typed — measured live on
  `(L_EXTENDEDPRICE - L_EXTENDEDPRICE) / (L_LINENUMBER - L_LINENUMBER)` — so this feature widens
  `#246`'s reachability to integer and decimal numerators rather than creating a new class of defect,
  and a second issue would fragment one gap across two trackers. CLAUDE.md's rule is that a known
  deviation must be fixed or recorded as an *accurately-scoped* tracked exception; three cases with
  three different answers is the accurate scoping.
- **Promotes to ADR:** yes

### [7] Accept ~1 ULP parity for a non-zero-scale decimal numerator, and say so

- **Decision:** State the fix's parity as bit-exact for a scale-0 numerator and equal within ~1 ULP
  for a non-zero-scale decimal numerator, and require oracle comparisons for decimal-numerator
  shapes to assert a relative tolerance rather than string equality.
- **Alternatives:** Claim exact parity (the issue implies it); route the division through a
  high-scale decimal intermediate to chase bit-exactness.
- **Rationale:** A sweep over a real `DECIMAL(18,2)` column — 3335 rows × divisors `{2,3,7,11,13}` —
  found 1027 results (31%) differing from native Exasol by exactly one ULP, max absolute difference
  `4.44e-16`, max relative difference `3.17e-16`; the same sweep over `DECIMAL(p,0)/DECIMAL(p,0)` was
  bit-exact at 0 of 3335. The residual is **Exasol-internal, not a DataFusion mismatch**: DataFusion
  54.1 returns byte-identical values to Exasol's *own* `CAST(A AS DOUBLE)/B` form, so Exasol's plain
  `/` on a decimal numerator evidently rounds later than the cast form does. Chasing it with a
  decimal intermediate would cap out at precision 38, reintroduce decimal-division-by-zero errors,
  and pursue a difference below the resolution of the `DOUBLE` column the value lands in. Set against
  the bug being fixed, relative error drops from ~1e-7 to ~2e-16 — a ~10⁹ improvement. Naming it
  matters for a practical reason: an E2E test that asserted string equality against a native oracle
  for a decimal-numerator division would fail intermittently on 31% of rows, so the tolerance is a
  requirement, not a caveat. Related trap recorded so nobody re-discovers it as a bug: native Exasol
  constant-folds the LITERAL `9007199254740993/2` to `4503599627370496.5` in exact arithmetic, but
  over a `DECIMAL(18,0)` column — the only path pushdown reaches — it returns
  `4503599627370496.0`, identical to the cast form.
- **Promotes to ADR:** no

### [4] Amend the DIV decline's rationale so the spec stays internally consistent

- **Decision:** Rewrite the `DIV` scenario's final clause so its decline rests on the per-row
  truncation problem rather than on the divide-by-zero divergence, and say explicitly that
  `FLOAT_DIV` now knowingly accepts that divergence.
- **Alternatives:** Leave the `DIV` scenario untouched and keep the plan strictly scoped to
  `FLOAT_DIV`.
- **Rationale:** The `DIV` scenario currently cites "a `TRUNC(m/n)` emulation diverges from Exasol
  for DOUBLE operands on division by zero — Exasol raises SQL state 22012, DataFusion float division
  yields infinity" as part of why `DIV` is declined. Accepting exactly that divergence for
  `FLOAT_DIV` while citing it as disqualifying for `DIV` would leave the spec self-contradictory, and
  a contradiction in a spec is indistinguishable from a regression to the next reader. `DIV`'s
  decline does not actually depend on that clause: its real disqualifier is that it needs
  *truncation*, so a wrong rendering is wrong on every row for non-integer operands — a strictly
  stronger objection, and the same type-blindness that *unblocks* `FLOAT_DIV`. Amending the
  rationale keeps the decline intact while making the asymmetry legible.
- **Promotes to ADR:** no

### [5] Keep one match arm rather than splitting FLOAT_DIV into its own

- **Decision:** Branch only the final assembly inside the existing
  `"ADD" | "SUB" | "MULT" | "FLOAT_DIV"` arm, and take the `DOUBLE` spelling from the existing
  `render_cast_target` mapping rather than writing a second literal.
- **Alternatives:** Give `FLOAT_DIV` its own match arm; hardcode `"DOUBLE"` inline.
- **Rationale:** The arity check, operand rendering, and error messages are identical across all four
  operators — a separate arm would duplicate all of them so one `format!` could differ, and would
  create two places to keep the capability lockstep in sync. A second `"DOUBLE"` literal in the same
  crate is back-door leakage: two sites would independently decide the same target-type spelling with
  nothing enforcing agreement. `format_decimal_exasol_style` is the crate's established precedent for
  a colocated pure helper that wraps an already-rendered SQL fragment, so the wrapper follows it.
- **Promotes to ADR:** no

### [6] Reproduce locally before fixing — and treat decimal/decimal as broken too

- **Decision:** Reproduce every facet against the local Docker container during planning, then have
  task group A turn those captures into failing tests rather than re-discover them. Add
  decimal/decimal as a **fourth broken shape** rather than the passing control the issue calls it.
  Task 4.1 is a negative verification that the Exasol-dialect sites did not move.
- **Alternatives:** Trust the issue's staging verification and the census's static analysis; keep the
  issue's three-facet framing.
- **Rationale:** CLAUDE.md requires local reproduction before a fix and forbids asserting a SQL
  capability or limitation from documentation, memory, or a capability registry — and doing it
  changed the plan twice. First, the issue's repro ran against a staging `DBXA`/`DBXA_COPY` TPC-H
  setup that does not exist locally (its `LINEITEM`/`CUSTOMER` come from `tests/tpch_loader.rs`,
  which is bench-only and not part of `make test-e2e`), so the shapes were re-expressed over local
  fixtures: `FACT_LINEITEM` (`L_ORDERKEY`/`L_LINENUMBER`, both `DECIMAL(20,0)`, seeded
  `L_ORDERKEY=7, L_LINENUMBER ∈ {1,2}` — a direct structural analogue of the issue's shape) for
  int/int and the filter facet, and `TYPED_DISTINCT_PROBE` (`C_DECIMAL_A` `DECIMAL(9,2)`,
  `C_DECIMAL_B` `DECIMAL(20,4)`) for the decimal shapes. Second, and more importantly, the run
  falsified the issue's own claim that decimal/decimal is an already-correct control:
  `DECIMAL(9,2)/DECIMAL(20,4)` returned `0.000102` against Exasol's `0.000102474999897525`.
  DataFusion's `Decimal128(24,6)` quotient is correct only when both operands carry few enough
  significant digits that scale 6 loses none, so the control shape is broken whenever it is
  interesting. The filter facet keeps its own repro because a truncated quotient changes *row
  counts*, a different failure mode from a wrong projected value, and it is the facet that never
  reaches the emit boundary. Task 4.1 is stated as a task rather than left implicit because under
  this design four Exasol-dialect goldens must stay byte-identical, and "unchanged" is only evidence
  if someone checked.
- **Promotes to ADR:** no

### [8] Close the two evidence gaps planning could not, rather than shipping on a proxy

- **Decision:** Keep two follow-through tasks that planning's live run could not satisfy: confirm the
  fix from the **translator's own output** via `EXPLAIN VIRTUAL` against a freshly built `.so`
  (task 5.1), and measure the **predicate-position** and **join/broadcast-path** divide-by-zero cases
  (task 1.2).
- **Alternatives:** Treat planning's live evidence as sufficient and let the E2E gate catch anything
  else.
- **Rationale:** Planning proved the fix shape end-to-end without rebuilding the `.so`, by issuing a
  user-side `CAST(... AS DOUBLE)` that `EXPLAIN VIRTUAL` shows Exasol pushes down as the byte-identical
  fragment the fix will emit — strong evidence that the *rendering* is right, but not evidence that
  the *translator* emits it, which is the thing actually being changed. Conflating the two is exactly
  the kind of inference CLAUDE.md's verification discipline forbids. The predicate case is a distinct
  code path, not a variant of the projection case: an infinity compared against a bound is consumed
  inside DataFusion and never reaches the emit boundary, so neither the engine's `inf` rejection nor
  `#246`'s NaN check can catch it, and it is the one place a silent wrong row count could survive the
  fix. Leaving both to the E2E gate would mean discovering them as failures without a recorded
  expectation to compare against.
- **Promotes to ADR:** no

### [9] Predicate-position divide-by-zero diverges SILENTLY — task 6.1 must file an issue

- **Decision:** Task 1.2's measurement is in: a divide-by-zero inside a **pushed scan filter**
  silently returns a wrong row count instead of raising, in both the single-table predicate position
  and the broadcast-join leg. This resolves the "not yet measured" clause of decision `[3]` and the
  task-1.2 half of decision `[8]`. Task 6.1 therefore takes its **file-an-issue** branch, not its
  verified-safe branch. **One** issue covers both positions — they are the same conjunct-rendering
  path with the same root cause and byte-identical observed behaviour, so splitting them would
  fragment one gap across two trackers. That issue is **distinct from `#246`** and must not be folded
  into it: `#246` is a *projected value* silently becoming NULL at the raw-scan `emit_batch`
  boundary, whereas a predicate is consumed inside DataFusion, never reaches emit, and its symptom is
  a wrong **row count** rather than a wrong value.
- **Alternatives:** Record verified-safe (falsified — see below); widen `#246` to cover it (wrong code
  path and wrong failure mode); file two issues, one per position (inaccurate scoping).
- **Rationale:** Measured live against `exasol/docker-db:2025.2.1` on the local Docker stack with the
  pre-fix `.so`, VS `MY_LAKEHOUSE` / `MY_LAKEHOUSE_JOIN`, using `(col - col)` as the divisor. Every
  row below was confirmed pushed by reading the `filter` string out of the `EXPLAIN VIRTUAL`
  `PUSHDOWN_SQL` ScanSpec, so no result is a Exasol-side-evaluation artefact.

  **Single-table predicate**, `MY_LAKEHOUSE.FACT_LINEITEM` (20 rows):

  | Pushed `filter` fragment | Observed |
  |---|---|
  | `(0 < (CAST("L_ORDERKEY" AS DOUBLE) / ("L_LINENUMBER" - "L_LINENUMBER")))` | succeeds, **20 of 20** |
  | same, `(... < 0)` | succeeds, **0** |
  | same, `> 1E300` | succeeds, **20** |
  | `CAST(-L_ORDERKEY AS DOUBLE)` numerator, `< 0` | succeeds, **20** |
  | `(0 < ("L_ORDERKEY" / ("L_LINENUMBER" - "L_LINENUMBER")))` — pre-fix bare `/`, decimal | **FAILS** `22002`, `Error evaluating filter predicate: ArrowError(DivideByZero, Some(""))` |
  | `(0 < ("L_EXTENDEDPRICE" / ("L_LINENUMBER" - "L_LINENUMBER")))` — pre-fix bare `/`, **DOUBLE** numerator | succeeds, **20** |
  | same, `< 0` | succeeds, **0** |
  | `(L_EXTENDEDPRICE - L_EXTENDEDPRICE) / (L_LINENUMBER - L_LINENUMBER)` (`0/0` → NaN), `> 0` / `> 1E300` | succeeds, **0** |
  | same, `< 0` / `< -1E300` | succeeds, **20** |

  **Native oracle** (`LHVS.GT_LINEITEM_SCAN`, Exasol-side): both the `x/0` and the `0/0` shapes fail
  with `data exception - division by zero`, SQL state **`22012`** — confirming Exasol raises in
  predicate position exactly as decision `[3]` found it does in projection position.

  **Broadcast join**, `FACT_ORDERS o ⋈ DIM_CUSTOMER c ON O_CUSTKEY = C_CUSTKEY WHERE O_ORDERDATE >=
  DATE '2024-01-05'` (baseline **6** rows). `EXPLAIN VIRTUAL` confirms the path: the `"join":{`
  common-blob block is present and the `LHS_T0` two-scan wrapper is absent, so broadcast is retained,
  and the conjunct lands in the fact-leg filter as `((DATE ''2024-01-05'' <= "O_ORDERDATE") AND (0 <
  (CAST("O_ORDERKEY" AS DOUBLE) / ("O_CUSTKEY" - "O_CUSTKEY"))))`. Results: `inf > 0` succeeds with
  **6 of 6**; `inf < 0` succeeds with **0**; the pre-fix bare `/` form **FAILS** at `22002` with the
  identical `ArrowError(DivideByZero)` message; a native inline-`VALUES` oracle of the same join shape
  fails at `22012`. The join path therefore adds **no divergence of its own** — it reproduces the
  single-table one exactly, which is why one issue suffices.

  Three findings drive the scoping. First, the divergence is real and silent, so `[8]`'s premortem was
  right to keep this task: the fix **converts a loud failure into a silent wrong answer** in predicate
  position (`22002` → 20 of 20 rows), the one direction of change no other guard can catch, because
  `±inf` is consumed by the comparison and never reaches the boundary where the engine's `inf`
  rejection or `#246`'s NaN check would fire. Second — and this is what keeps the issue accurately
  scoped rather than blaming it on this feature — the gap is **already reachable today with no cast
  involved**, whenever the numerator column is already `DOUBLE`-typed: bare `"L_EXTENDEDPRICE" /
  ("L_LINENUMBER" - "L_LINENUMBER")` pushes without a cast and already returns 20 rows where native
  raises. So this feature **widens** a pre-existing predicate-position gap from `DOUBLE` numerators to
  integer and decimal numerators, exactly the widening framing decision `[3]` applied to `#246`, and
  the issue must say so rather than presenting the defect as newly created. Third, the `0/0` NaN case
  is wrong in a second, independent way worth recording: NaN sorts **below every bound** in the pushed
  comparison (`NaN < -1E300` matched all 20 rows, `NaN > 1E300` matched none), so both comparison
  directions are wrong — and this is not even IEEE 754 semantics, under which both would be false.
  `±inf`, by contrast, compares as a faithful IEEE infinity throughout; only its *existence* is the
  divergence from Exasol. The mechanism behind the NaN ordering was not investigated (DataFusion's
  comparison kernel was not inspected), so the issue should report it as observed behaviour, not as a
  diagnosed cause.

  This does **not** change the fix: reverting to the bare `/` to keep the loud `22002` would restore
  the truncation bug on every row, and decision `[3]` already rejected `NULLIF` emulation and
  declining `FLOAT_DIV`. The divergence is accepted and tracked, per CLAUDE.md's rule that a known
  deviation be either fixed or recorded as an accurately-scoped tracked exception.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->

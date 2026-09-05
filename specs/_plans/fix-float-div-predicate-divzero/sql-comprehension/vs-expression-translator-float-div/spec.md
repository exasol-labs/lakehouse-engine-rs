# Feature: VS Expression Translator — Floating-Point Division

Splits floating-point division (`FLOAT_DIV`, Exasol's `/`) out of the shared arithmetic-operator
shape in `sql-comprehension/vs-expression-translator-scalar-ops`, because Exasol's `FN_FLOAT_DIV` is
always true `DOUBLE` division while DataFusion's `/` is operand-typed and truncated integer and
decimal operands (issue #186). The verbatim-exclusion table in
`sql-comprehension/vs-expression-translator-scalar-fns` also references this feature: `FLOAT_DIV` is
an operator wire name, not an Exasol function name, so it never joins that table's verbatim rule.

<!-- DELTA:CHANGED -->
The DataFusion-dialect rendering now also owns the divide-by-zero outcome. Issue #370 measured a
pushed `FLOAT_DIV` by zero inside a `WHERE` predicate returning a silently wrong row count, because
the infinity `x/0` produces is consumed inside DataFusion's comparison and never reaches the emit
boundary that rejects it in projection position. The dialect therefore renders a checked division
call instead of the `/` operator, so the zero divisor raises at the point of division in every
position rather than only where the value happens to reach an emit-time check.
<!-- /DELTA:CHANGED -->

## Background

A conversion or operator node is translated only when its DataFusion 54 result matches Exasol's to
the precision of the target type, and only when a mismatch cannot produce a silently wrong value.
`FLOAT_DIV` qualifies under both halves of that rule and is the reason the rule needs both halves
stated: its cast rendering is bit-exact against Exasol for a scale-0 numerator and within ~1 ULP
(max relative difference `3.17e-16`) for a non-zero-scale decimal numerator, which is at the
resolution limit of the `DOUBLE` column the value lands in; and its one behavioural divergence,
division by zero, fails the query rather than returning a wrong value. This is the opposite outcome
from Exasol integer division (`DIV`, specified in `sql-comprehension/vs-expression-translator-scalar-ops`):
`DIV` needs truncation, which DataFusion's `/` delivers only for integer operands, so no single
type-blind rendering reproduces it — the per-row problem, not the zero-divisor one, is what
disqualifies `DIV` and does not disqualify `FLOAT_DIV`.

* **`FLOAT_DIV` shared the bare-operator arm with `ADD`/`SUB`/`MULT` and inherited DataFusion's
  operand-typed `/`, which is not Exasol's `/` (issue #186).** Exasol's `FN_FLOAT_DIV` is *always*
  true float division and *always* results in `DOUBLE`, whatever the operand types — verified live
  against the Docker Exasol container (`exasol/docker-db:2025.2.1`): `7/2` is `3.5`, and a CTAS over
  every operand pairing (`DECIMAL(18,0)/DECIMAL(18,0)`, `DECIMAL(18,2)/DECIMAL(18,0)`,
  `DECIMAL(18,2)/DOUBLE`, `DECIMAL(18,0)/DOUBLE`) types the result column `DOUBLE` in
  `EXA_ALL_COLUMNS`. DataFusion 54.1's `/` is operand-typed: `Int64/Int64` gives truncating integer
  division typed `Int64`, `Decimal128(18,2)/Decimal128(18,2)` gives `Decimal128(24,6)` and
  `Decimal128(18,2)/Int64` gives `Decimal128(22,6)` — the scale-6 cap that is issue #186's second
  facet. Rendering the node as a bare `/` therefore returned silently wrong values on the DataFusion
  path, on EVERY row of a valid query, in projected values and in filter predicates alike.
* **All three operand classes were broken, not two — the issue's "control" does not generalize.**
  Reproduced live through the local virtual schema against a native-table oracle:
  `L_ORDERKEY / L_LINENUMBER` at `(7, 2)` returned `3.0` against Exasol's `3.5`;
  `C_DECIMAL_A / 7` at `40.99` (`DECIMAL(9,2)`) returned `5.855714` against `5.855714285714286`;
  `COUNT(*) WHERE L_ORDERKEY / L_LINENUMBER > 3 AND L_ORDERKEY = 7` returned `1` against `2`; and
  `C_DECIMAL_A / C_DECIMAL_B` (`DECIMAL(9,2)/DECIMAL(20,4)`) returned `0.000102` against
  `0.000102474999897525`. Decimal/decimal is only accidentally correct when both operands carry so
  few significant digits that `Decimal128(_,6)` loses none.
* **The translator cannot fix this by inspecting operand types — it has none.**
  `crates/vs-expression` stays "a pure, stateless, sibling-shared JSON-to-SQL translator with no
  column-type context" (see `sql-comprehension/vs-expression-translator-scalar-ops`), and decision
  `016-add-fn-div-pushdown` records that "the arithmetic operator arm renders operands via recursive
  calls into opaque SQL strings without inspecting their types". An UNCONDITIONAL cast of the left
  operand to `DOUBLE` is therefore the only type-blind rendering that reproduces Exasol's
  always-`DOUBLE` `FN_FLOAT_DIV`, and it is exactly what the type-blindness makes IMPOSSIBLE for
  `DIV` — `DIV` needs truncation, which is correct for integer operands and wrong for every other
  kind, so no single `DIV` rendering exists. The same limitation unblocks one operator and blocks
  the other.
* **Casting only the LEFT operand is sufficient, and it is enough for every operand pairing.**
  Measured on DataFusion 54.1 / arrow 58.3: `CAST(<int64> AS DOUBLE) / <int64>`,
  `CAST(<dec(18,2)> AS DOUBLE) / <int64>`, `CAST(<int64> AS DOUBLE) / <dec(18,2)>`,
  `CAST(<dec(18,2)> AS DOUBLE) / <dec(18,2)>` and `CAST(<float64> AS DOUBLE) / <float64>` all plan
  and execute, and all yield `Float64`. `Float64 ÷ Decimal128` in particular coerces cleanly rather
  than erroring, so the right operand never needs its own cast. `CAST(x AS DOUBLE)` — the bare
  `DOUBLE` spelling, not `DOUBLE PRECISION` — is accepted by Exasol's parser too.
* **The fix is DataFusion-dialect only — the Exasol dialect is already correct.** Exasol's own `/`
  is `FN_FLOAT_DIV`, so every Exasol-dialect consumer (the qualified single-table wrapper, the
  N-scan join wrapper, the grouped merge, the single-group scalar-over-aggregate merge, and
  `render_self_applied_where`) already divides as `DOUBLE` with no cast. Rendering the cast there
  too would change Exasol-facing SQL — including two byte-exact `dispatch_golden` fixtures and two
  native-oracle E2E comparisons of the just-shipped scalar-over-aggregate feature — for no
  correctness gain. This makes `FLOAT_DIV` the first arithmetic operator whose rendering diverges
  by dialect, so the "byte-identically in BOTH dialects" claim in
  `sql-comprehension/vs-expression-translator-scalar-ops` narrows to the four operators it still
  holds for, and the divergence guard that pinned the old claim is RETARGETED rather than removed
  (the same treatment the CHAR CAST divergence received in
  `sql-comprehension/vs-expression-translator-cast`).
* **`DOUBLE` has one owner.** `render_cast_target` already maps both `DOUBLE` and
  `DOUBLE PRECISION` to the string `DOUBLE`, dialect-invariantly, and that spelling is the one
  proven against both parsers. The new rendering reuses it rather than introducing a second,
  independently-maintained spelling of the same target type.
* **The `Float64` result matches the EMITS column the adapter already declares.** For a select-list
  expression the adapter takes the EMITS type from Exasol's own `selectListDataTypes` entry
  (`support.rs`'s `declared_select_type`), and Exasol types `<a> / <b>` as `DOUBLE` — confirmed live,
  `EXPLAIN VIRTUAL` for the projected division shows `"selectListDataTypes":[{"type":"DOUBLE"}]` and
  `"emit_exa_types":["DOUBLE PRECISION"]`. Before the fix the scan produced an `Int64`/`Decimal128`
  column that `coerce_batch_to_exa_types` cast UP into that `DOUBLE PRECISION` column — which is why
  the truncated integer quotient surfaced as `3.0` rather than as `3`. After the fix the column is
  already `Float64`, so it takes that function's zero-copy fast path instead.
* **Parity is ~1 ULP, not bit-exact, for a non-zero-scale decimal numerator — and that is a
  ~10⁹ improvement, not a new defect.** Exasol's own `<decimal> / <x>` is not bit-identical to
  `CAST(<decimal> AS DOUBLE) / <x>`: over a `DECIMAL(18,2)` column, 3335 rows × divisors
  `{2,3,7,11,13}`, 1027 of 3335 results (31%) differ by exactly one ULP — max absolute difference
  `4.44e-16`, max relative difference `3.17e-16`. The same sweep over `DECIMAL(p,0)/DECIMAL(p,0)` was
  **bit-exact, 0 of 3335 differing**. The residual is Exasol-internal, not a DataFusion mismatch:
  DataFusion 54.1 returns byte-identical values to Exasol's *own* `CAST(A AS DOUBLE)/B` form. Set
  against the pre-fix scale-6 truncation, relative error drops from ~1e-7 to ~2e-16. It is named
  rather than claimed away, and it is why an equality assertion against a native oracle must use a
  relative tolerance for decimal-numerator shapes rather than string equality.
* **The 2^53+1 literal is a false divergence — do not report it as one.** Native Exasol
  `SELECT 9007199254740993/2` returns `4503599627370496.5`, because Exasol constant-folds the literal
  in exact arithmetic. Over a `DECIMAL(18,0)` COLUMN — the only path pushdown can reach — Exasol
  returns `4503599627370496.0`, identical to the cast form. There is no reachable divergence here.

<!-- DELTA:CHANGED -->
* **Division by zero is a rendering-layer problem, not an emit-layer one (issue #370).** The
  measured behaviour before this change had three different outcomes for the same user error.
  A projected `x/0` produced `±Inf` and failed at the emit boundary with `numeric value out of
  range: value inf ... is not in [ -1.7976e+308 .. 1.7976e+308 ]` (SQL state `22002`). A projected
  `0/0` produced `NaN` and the raw-scan `emit_batch` path returned a silent `NULL`. A `x/0` inside a
  `WHERE` predicate never reached any emit-time check at all, so the comparison consumed the
  infinity and the query succeeded with a wrong row count. Issue #370 measured every row of that
  table live on `exasol/docker-db:2025.2.1`, with each pushed filter confirmed from the `filter`
  field of the `EXPLAIN VIRTUAL` `PUSHDOWN_SQL` ScanSpec: over the 20-row `FACT_LINEITEM` fixture,
  `(0 < (CAST("L_ORDERKEY" AS DOUBLE) / ("L_LINENUMBER" - "L_LINENUMBER")))` returned 20 of 20 rows,
  the same shape with `< 0` returned 0 of 20, and the `0/0` shape returned 0 of 20 for `> 0` and 20
  of 20 for `< 0`. Native Exasol raises `data exception - division by zero` (SQL state `22012`) for
  every one of these shapes, in predicate position exactly as in projection position. The single
  common cause is that the operator `/` decides what to do about a zero divisor, and the operator
  has no way to fail.
* **The fix moves the decision into a function the crate names, so one rendering owns all three
  outcomes.** The DataFusion dialect renders `vs_checked_float_div(<left>, <right>)`. The function
  coerces both operands to `Float64`, divides, propagates NULL, and RAISES when its own result is
  not finite. Every position gets the same outcome from the same check, because the check sits where
  the division happens rather than where a value happens to be consumed. This also removes the
  `CAST(<left> AS DOUBLE)` wrapper: the function owns the always-`DOUBLE` coercion for BOTH operands,
  so keeping a SQL-level cast for one of them would state the same decision in two places.
* **Rejecting a computed non-finite result carries none of the risk that ruled out the emit-boundary
  fix.** `arrow_value_at`'s check cannot distinguish a COMPUTED non-finite value from one
  legitimately STORED in the source table, which is why widening it from `is_nan()` to
  `!is_finite()` was rejected and remains rejected. `vs_checked_float_div` has no such ambiguity: it
  sees only the two operands of a division the pushdown itself synthesised, and it raises on the
  result of that division, never on a value read straight out of a column. A plain
  `SELECT <double_col>` over a table that stores `NaN` or `±Inf` reaches no checked division and is
  untouched by this feature.
* **The one named trade-off is a stored non-finite operand.** When a source column legitimately
  stores `±Inf` or `NaN` and the user divides it, the result is not finite and the checked division
  raises. This is deliberate and it is consistent with what already happens: the same value in a
  projection already fails at the emit boundary with `22002`, because Exasol admits no non-finite
  `DOUBLE` at all (`CAST('inf' AS DOUBLE)` and `CAST('nan' AS DOUBLE)` are rejected at `22018`,
  `1E400` at `22003`). A value Exasol cannot represent must not become a silent comparison result
  either. See the format-specification bullet below for why this is an Exasol target-type limit
  rather than an Iceberg or Delta deviation.
* **The error is raised only for a row whose division the scan evaluates.** DataFusion may not
  evaluate the division for a row that another conjunct, file pruning, row-group pruning, or a
  LIMIT already removed. Whether native Exasol raises for such a row is NOT measured. What follows
  from the design, and is the point of the fix, is that the returned rows never disagree with
  Exasol: a row reaches the result only when its division was evaluated and finite. The residual is
  therefore scoped to error-raising alone, never to row content, and is tracked as a separate
  issue rather than left unstated `(#TODO-suppression)`. The divergence runs in both directions.
  DataFusion 54.1 may also evaluate the division for a row an adjacent conjunct already excluded,
  so a query that succeeds today can raise after this change. That direction is part of the same
  tracked exception, and the next bullet states its mechanism.
* **A guarded division is not protected by its guard, and the protection that does exist is
  batch-selectivity dependent.** `datafusion-physical-expr` 54.1 defines
  `PRE_SELECTION_THRESHOLD: f32 = 0.2` in `src/expressions/binary.rs` and applies it in
  `check_short_circuit`. For an `AND`, that function returns `ReturnLeft` when the left conjunct is
  all-false over the batch, `ReturnRight` when it is all-true, and a pre-selection filter only when
  the left conjunct's true ratio is at or below 0.2. Above that ratio it returns no strategy and
  `BinaryExpr::evaluate` evaluates the right conjunct over the FULL batch, including rows the left
  conjunct excluded. A null in the left conjunct disables the strategy entirely. Nothing protects a
  division that sits in the LEFT conjunct at all. So `WHERE <d> <> 0 AND <n> / <d> > 0` can raise
  after this change even though every surviving row has a non-zero divisor, and whether it raises
  depends on per-batch selectivity and on conjunct order. Task 1.2 measures native Exasol's own
  behaviour for the guarded shape, in both conjunct orders, because CLAUDE.md forbids assuming it.
  This over-raise direction is part of the same tracked exception `(#TODO-suppression)`.
* **The `0/0` NaN route into `#246` closes; `#246` itself stays open.** A `0/0` now raises at the
  checked division and never reaches `emit_batch`, so the widening this feature previously recorded
  against `#246` is withdrawn. `#246` continues to cover every other way a `NaN` reaches the
  raw-scan emit boundary, including an out-of-domain math kernel and a stored `NaN`, and this
  feature MUST NOT be read as closing it.
* **The NaN ordering issue #370 reported is out of scope here, and is tracked separately.** Issue
  #370 also observed that `NaN < -1E300` matched all 20 rows while `NaN > 1E300` matched none, and
  states that the mechanism was not investigated. After this change a pushed `FLOAT_DIV` can no
  longer produce a `NaN`, so #370's own reproducer no longer reaches that behaviour. Comparison
  semantics for a `NaN` READ FROM a column remain unmeasured and unspecified, and are recorded as a
  tracked exception rather than a silent gap `(#TODO-stored-nan)`.
* **The checked division covers `FLOAT_DIV` alone, and every other pushed function that can produce
  a non-finite value keeps the gap this fix closes.** `crates/lakehouse-engine/src/adapter/capabilities.rs`
  advertises `FN_SQRT`, `FN_LN`, `FN_LOG`, `FN_ACOS`, `FN_ASIN`, `FN_EXP`, `FN_POWER`, and `FN_MOD`,
  each of which the translator renders into a pushed predicate and each of which can yield `NaN` or
  `±Inf`. `WHERE SQRT(<negative_col>) > 0` reproduces issue #370's mechanism exactly, because the
  comparison consumes the non-finite value inside the scan and no emit-boundary check ever sees it.
  This plan fixes the `FLOAT_DIV` producer only. The remaining producers are recorded as a tracked
  exception rather than a silent gap `(#TODO-scalar-fns)`.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
* **Neither table-format specification constrains this, and the trade-off above is a target-type
  limit rather than a deviation.** Re-checked per CLAUDE.md against the Apache Iceberg table spec
  (https://iceberg.apache.org/spec/) and the Delta Lake protocol
  (https://github.com/delta-io/delta/blob/master/PROTOCOL.md). Both fix only the STORED operand
  domain. Iceberg `#### Primitive Types` gives `float` "32-bit IEEE 754 floating point" and `double`
  "64-bit IEEE 754 floating point"; Delta `## Primitive Types` gives `float` "Single precision
  (32-bit) IEEE 754 floating point number" and `double` "Double precision (64-bit) IEEE 754
  floating point number". Neither document defines expression result types, and the words
  "division", "divisor" and "arithmetic" appear nowhere normatively in either. Iceberg does
  anticipate a stored `NaN`: its field-level statistics rules state "NaNs are not permitted as lower
  or upper bounds" and its manifest `field_summary` carries a `nan_value_counts` entry. That is the
  reason the trade-off above is named. Exasol has no non-finite `DOUBLE`, so a stored `±Inf` or `NaN`
  is unreadable through this engine whether or not it passes through a division. Per CLAUDE.md, a
  deviation driven by an Exasol target-type limitation is not a gap for either specification, and it
  is named here as a deliberate trade-off. Nothing in this feature changes how a stored `float` or
  `double` value is decoded, pruned, or projected, so no reader requirement of either specification
  is touched. The earlier widening relations remain accurate and unaffected: Iceberg
  `#### Schema Evolution` permits `int`→`long`, `float`→`double` and `decimal(P, S)`→`decimal(P', S)`
  "if `P' > P`", and Delta `§ Type Widening` additionally permits "`Byte`, `Short` or `Int` ->
  `Double`" and "`Byte`, `Short` or `Int` -> `Decimal(10 + k1, k2)` where `k1 >= k2 >= 0`", so the
  operand Arrow type reaching this operator is legitimately any of
  `Int32`/`Int64`/`Decimal128`/`Float32`/`Float64` and on Delta it can CHANGE between table versions.
  That is a further argument for one type-blind checked-division function over any type-conditional
  rendering.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: FLOAT_DIV renders true float division in the DataFusion dialect

* *GIVEN* a VS expression node of type `function_scalar` named `FLOAT_DIV` — the wire name for Exasol's `/`, advertised as `FN_FLOAT_DIV`
* *AND* Exasol's `FN_FLOAT_DIV` always performs true float division and always results in `DOUBLE`, for every operand-type combination — verified live against the Docker Exasol container: `7/2 = 3.5` (NOT `3`), `CAST(711.56 AS DECIMAL(18,2))/CAST(7 AS DECIMAL(18,0)) = 101.65142857142857` at full double precision (NOT scale-capped), and a CTAS over each operand pairing types the result column `DOUBLE` in `EXA_ALL_COLUMNS`
* *AND* the translator carries no operand-type context of its own, so it cannot render one shape for integer operands and another for decimal or double operands
* *WHEN* the node is rendered through the DataFusion-dialect entry points (`render_expression`, `render_expression_safe`, `render_df_filter_safe`) — the ones whose output DataFusion's SQL frontend parses inside the scan UDF
* *THEN* the translator SHALL return `vs_checked_float_div(<left>, <right>)`, a call to the single checked-division function the crate declares, with both operands rendered recursively and neither wrapped in a CAST
* *AND* the function name SHALL be exported from `crates/vs-expression` as one public constant that the rendering itself reads, so the name has ONE owner and the scan crate that registers the function cannot drift from the crate that emits it
* *AND* that constant's documentation SHALL state the full contract the registered implementation MUST satisfy: two arguments, both coerced to `Float64`, a `Float64` result, NULL propagated, and an error raised when the result is not finite
* *AND* it MUST NOT return the bare `(<left> / <right>)`, which DataFusion evaluates by operand type and which returned silently wrong values on every row for all three operand classes, each reproduced live through the local virtual schema against a native-table oracle: `L_ORDERKEY / L_LINENUMBER` at `(7, 2)` gave `3.0` against `3.5`; `C_DECIMAL_A / 7` at `40.99` gave `5.855714` against `5.855714285714286`; `C_DECIMAL_A / C_DECIMAL_B` (`DECIMAL(9,2)/DECIMAL(20,4)`) gave `0.000102` against `0.000102474999897525`; and `COUNT(*) WHERE L_ORDERKEY / L_LINENUMBER > 3 AND L_ORDERKEY = 7` gave `1` against `2` — a truncated quotient changes row counts as well as values (issue #186)
* *AND* it MUST NOT return `(CAST(<left> AS DOUBLE) / <right>)` either, the shape this feature previously specified: it fixes the truncation but leaves the `/` operator deciding the zero-divisor outcome, which is issue #370
* *AND* decimal/decimal division SHALL NOT be treated as an already-correct control case: DataFusion's `Decimal128(24,6)` quotient is correct only when both operands carry few enough significant digits that scale 6 loses none
* *AND* the rendering SHALL stay type-blind and unconditional, correct for every operand-type combination the two lakehouse formats can present — `Int64`, `Decimal128`, `Float32` and `Float64` on either side, in any pairing — because the called function, not the SQL text, performs the coercion
* *AND* NULL SHALL propagate unchanged — a NULL left operand, a NULL right operand, and a NULL left operand over a zero right operand each yield NULL, not an error and not `NaN`
* *AND* the resulting `Float64` column SHALL match the `DOUBLE PRECISION` EMITS type the adapter declares for the item from Exasol's own `selectListDataTypes` — live-confirmed as `{"type":"DOUBLE"}` with `"emit_exa_types":["DOUBLE PRECISION"]` — so the emit-boundary type coercion keeps the column on its zero-copy fast path
* *AND* parity against native Exasol SHALL be asserted as bit-exact for an integer (scale-0) numerator and as equal within ~1 ULP for a non-zero-scale decimal numerator, because Exasol's own decimal division is not bit-identical to converting the numerator to `DOUBLE` first — over a `DECIMAL(18,2)` column, 3335 rows × divisors `{2,3,7,11,13}`, 1027 results (31%) differed by exactly one ULP, max relative difference `3.17e-16`, while the same sweep over `DECIMAL(p,0)/DECIMAL(p,0)` was bit-exact at 0 of 3335 — so an oracle comparison for a decimal-numerator shape MUST use a relative tolerance, never string equality
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The Exasol dialect keeps rendering FLOAT_DIV as a bare division operator

* *GIVEN* the same `function_scalar` node named `FLOAT_DIV`
* *WHEN* the node is rendered through the Exasol-dialect entry points (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`) — the ones whose output Exasol's own core engine parses
* *THEN* the translator SHALL return the bare `(<left> / <right>)`, UNCHANGED from every earlier revision of this feature, because Exasol's `/` IS `FN_FLOAT_DIV`: it already divides as `DOUBLE` for every operand type AND it already raises `22012` on a zero divisor, so it needs neither the cast nor the checked call
* *AND* the two dialects SHALL therefore DIVERGE on the same `FLOAT_DIV` node — `vs_checked_float_div(<l>, <r>)` in the DataFusion dialect, `(<l> / <r>)` in the Exasol dialect — and the existing divergence guard SHALL be RETARGETED to assert this pair rather than deleted, so the divergence stays pinned by a test (the same treatment the CHAR CAST divergence received in `sql-comprehension/vs-expression-translator-cast`)
* *AND* every Exasol-dialect consumer's SQL SHALL stay byte-identical — the qualified single-table wrapper, the N-scan join wrapper, the grouped merge, the single-group scalar-over-aggregate merge (`render_scalar_over_merge`, which calls `render_expression_exasol`), and the self-applied WHERE path — so both `dispatch_golden` fixtures carrying a translated `FLOAT_DIV` (`single_group_scalar_over_aggregate_dedup.sql`, `single_group_scalar_over_aggregate_interleaved.sql`) MUST remain unchanged, and a diff in either is a regression rather than an expected update
* *AND* the `/` characters in the AVG and statistical merge fragments (`scalar_over_agg.rs`'s `SUM(<partial>) / NULLIF(SUM(<partial>), 0)` and the König–Huygens numerator) SHALL be unaffected in both dialects, because they are adapter-authored merge SQL that never passes through the translator's `FLOAT_DIV` arm
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A pushed-down division by zero fails the query rather than returning a wrong value

* *GIVEN* a pushed-down `FLOAT_DIV` whose right operand evaluates to zero for at least one scanned row, the numerator being non-zero
* *AND* native Exasol raises `data exception - division by zero` (SQL state `22012`) for every operand pairing including `DOUBLE/DOUBLE`, verified live and column-driven so nothing is constant-folded, and never returns NULL and never returns infinity
* *WHEN* the DataFusion-dialect rendering `vs_checked_float_div(<left>, <right>)` is evaluated in the scan and the result is projected
* *THEN* the checked division SHALL raise, the scan SHALL fail, and the surfaced message SHALL name the cause as a division by zero
* *AND* the query SHALL NOT return a wrong value and SHALL NOT return NULL for the affected row
* *AND* this SHALL be recorded as a NARROWED, still-accepted divergence rather than parity: the query failed before this change too, at `22002` with `numeric value out of range: value inf ...`, so the outcome (a failed query) is unchanged and only the message and the raising layer move — the raising layer moves from the Exasol engine's emit-boundary range check to the scan's own division, and the message moves from an infinity complaint to Exasol's own vocabulary
* *AND* the SQL state the Exasol engine attaches to the scan UDF's error SHALL be RECORDED from the live run rather than assumed, because the raising layer changed
* *AND* Exasol SHALL be understood to admit no non-finite `DOUBLE` at all — `CAST('inf' AS DOUBLE)`, `CAST('Infinity' AS DOUBLE)` and `CAST('nan' AS DOUBLE)` are each rejected at `22018`, and `1E400` at `22003` — which is why raising at the division is the only outcome consistent with what Exasol can represent
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: Zero divided by zero reaches the tracked NaN-at-emit gap

* *GIVEN* a pushed-down `FLOAT_DIV` whose numerator AND denominator both evaluate to zero for a scanned row
* *WHEN* the DataFusion-dialect rendering is evaluated and the result is projected through the raw-scan path
* *THEN* this scenario SHALL be REPLACED by "Zero divided by zero fails the query instead of reaching the NaN-at-emit gap" and by "A division by zero inside a filter predicate fails the query rather than changing the row count", both below
* *AND* the replacement SHALL be recorded as a superseded measurement, not a retracted one: every observation this scenario carried was measured live and remains accurate as a description of the pre-fix rendering, which no longer exists
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A division by zero inside a filter predicate fails the query rather than changing the row count

* *GIVEN* a pushed-down `FLOAT_DIV` by zero inside a `WHERE` filter predicate, the shape issue #370 measured live over the 20-row `FACT_LINEITEM` fixture with each pushed filter read out of the `EXPLAIN VIRTUAL` `PUSHDOWN_SQL` ScanSpec
* *AND* native Exasol raises `data exception - division by zero` (SQL state `22012`) for this shape in predicate position exactly as in projection position, verified live against the `LHVS.GT_LINEITEM_SCAN` native oracle
* *WHEN* the pushed filter is evaluated in the scan
* *THEN* the checked division SHALL raise and the query SHALL FAIL, in both comparison directions and for both the `x/0` and the `0/0` shape
* *AND* it MUST NOT succeed with a row count that disagrees with native Exasol, the pre-fix behaviour: `(0 < (CAST("L_ORDERKEY" AS DOUBLE) / ("L_LINENUMBER" - "L_LINENUMBER")))` returned 20 of 20 rows, the same shape with `< 0` returned 0 of 20, the `0/0` shape returned 0 of 20 for `> 0` and 20 of 20 for `< 0`, and a `DOUBLE`-typed numerator reached the same wrong counts with no cast involved at all
* *AND* the same outcome SHALL hold for a broadcast-join fact-leg filter, the second position issue #370 measured: with the conjunct landing in the fact leg as `((DATE '2024-01-05' <= "O_ORDERDATE") AND (0 < (CAST("O_ORDERKEY" AS DOUBLE) / ("O_CUSTKEY" - "O_CUSTKEY"))))` and `EXPLAIN VIRTUAL` confirming broadcast is retained (a `"join":{` common blob, no `LHS_T0` two-scan wrapper), the join path SHALL add no divergence of its own, exactly as it added none before the fix
* *AND* the raising SHALL come from the same single checked-division function the projection path uses, so the two positions cannot drift apart again
* *AND* a NULL divisor SHALL still yield NULL and SHALL NOT raise, so `WHERE <a> / NULL > 0` returns no rows rather than failing
* *AND* the error SHALL be raised only for a row whose division the scan actually evaluates: DataFusion MAY skip the division for a row that another conjunct, file pruning, row-group pruning, or a LIMIT already removed, and whether native Exasol raises for such a row is NOT measured, so the residual is scoped to error-raising alone and recorded as a tracked exception `(#TODO-suppression)`
* *AND* the rows a successful query returns SHALL NOT be affected by that residual: a row reaches the result only when its division was evaluated and finite, so a query that does not raise returns exactly the rows native Exasol returns
* *AND* a GUARDED division, the shape `WHERE <d> <> 0 AND <n> / <d> > 0` and its reversed conjunct order, SHALL have its outcome MEASURED live against the native Exasol oracle by task 1.2 and RECORDED here rather than assumed, because a guard does NOT prevent the checked division from raising and this is the one shape where a query that succeeds today can start failing
* *AND* the mechanism SHALL be recorded as batch-selectivity dependence rather than as a stable property: `datafusion-physical-expr` 54.1 defines `PRE_SELECTION_THRESHOLD: f32 = 0.2` in `src/expressions/binary.rs`, and `check_short_circuit` returns a pre-selection filter for an `AND` only when the left conjunct's true ratio over the batch is at or below that threshold, returns `ReturnLeft` when the left conjunct is all-false, returns `ReturnRight` when it is all-true, and otherwise lets `BinaryExpr::evaluate` evaluate the right conjunct over the FULL batch including the rows the left conjunct excluded
* *AND* a division sitting in the LEFT conjunct SHALL be understood to have NO protection at all from this mechanism, and a null in the left conjunct SHALL be understood to disable it entirely, so conjunct order and per-batch data both change the outcome
* *AND* this over-raise direction SHALL be covered by the SAME tracked exception as the suppression direction `(#TODO-suppression)`, because both are the same underlying fact: the error is a per-row side effect of an expression DataFusion is free to evaluate over a row set of its own choosing
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Zero divided by zero fails the query instead of reaching the NaN-at-emit gap

* *GIVEN* a pushed-down `FLOAT_DIV` whose numerator AND denominator both evaluate to zero for a scanned row
* *WHEN* the DataFusion-dialect rendering is evaluated, in projection position or in predicate position
* *THEN* the checked division SHALL raise, because `0.0 / 0.0` is `NaN` and `NaN` is not finite, and the query SHALL fail with the same division-by-zero message as any other zero divisor — matching native Exasol, which raises `22012` for this shape too
* *AND* the projected `0/0` SHALL NO LONGER return a silent NULL: the pre-fix raw-scan path emitted Arrow IPC bytes through `ctx.emit_batch` with no per-value check, and Exasol received NULL for every such row (verified live), which this feature now prevents by raising before the value is ever emitted
* *AND* the WIDENING of `#246` that this feature previously recorded SHALL be WITHDRAWN: a pushed `FLOAT_DIV` can no longer produce a `NaN` at all, so it is no longer a route into that gap for any numerator type
* *AND* `#246` SHALL remain OPEN and MUST NOT be treated as closed by this feature: it continues to cover every other `NaN` that reaches the raw-scan emit boundary, including an out-of-domain math kernel and a `NaN` stored in the source table, and it continues to record that `arrow_value_at` errors on the partial-aggregate path where `emit_batch` does not
* *AND* this feature MUST NOT close the remaining `#246` surface by widening `arrow_value_at`'s check from `is_nan()` to `!is_finite()`, because that boundary still cannot distinguish a COMPUTED non-finite value from one legitimately STORED in the source table — Iceberg types the column "64-bit IEEE 754 floating point", Delta "Double precision (64-bit) IEEE 754 floating point number", and Parquet `DOUBLE` admits `±Inf` and `NaN` — so such a check would break reading a table that legitimately contains them
* *AND* this feature MUST NOT close the gap by rendering `NULLIF(<right>, 0)` either: NULL is exactly the wrong answer already observed, it makes a genuine zero divisor indistinguishable from a NULL divisor, and it would suppress the loud failure the `x/0` case already produces
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: FLOAT_DIV stays outside the verbatim rule in both dialects

* *GIVEN* the crate's single per-function declaration of each translated `function_scalar` name, which both gates the dispatch and drives the enforcing Exasol-dialect sweep test
* *AND* `FLOAT_DIV` declared with the dialect-shaped form rather than the verbatim form, alongside `ADD`, `SUB`, `MULT`, and `NEG`
* *WHEN* the sweep test renders every declared name through `render_expression_exasol` and compares it against that name's declared expectation
* *THEN* `FLOAT_DIV` SHALL keep its shaped declaration and MUST NOT be moved to the verbatim form, because Exasol has no function called `FLOAT_DIV` — a verbatim rendering would emit `FLOAT_DIV(<l>, <r>)`, which Exasol rejects the same way it rejects `SIGNUM` and `STRPOS` (`function or script <NAME> not found`, SQL code 42000)
* *AND* the sweep's Exasol-dialect expectation for `FLOAT_DIV` SHALL remain the bare `(<l> / <r>)` — unchanged by issue #186's fix and unchanged by issue #370's, both of which act on the DataFusion side only
* *AND* the sweep's banned-token list SHALL GAIN the checked-division function name, because Exasol has no such function and a leak of that name into an Exasol-parsed fragment would fail at 42000 exactly as `SIGNUM` and `STRPOS` do — this is the first `FLOAT_DIV`-related token that belongs on the list
* *AND* `CAST` MUST still NOT be added to that list, since `CAST` is valid Exasol SQL that the CAST scenarios legitimately emit in the Exasol dialect
<!-- /DELTA:CHANGED -->
</content>
</invoke>

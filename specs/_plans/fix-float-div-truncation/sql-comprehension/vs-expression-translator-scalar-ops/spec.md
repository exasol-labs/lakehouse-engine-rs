# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic
operators and the safe/fallback entry points. CAST target-type rendering is covered in
`sql-comprehension/vs-expression-translator-cast`. Named math/string/conditional scalar functions
are covered in `sql-comprehension/vs-expression-translator-scalar-fns`; date/time functions in
`sql-comprehension/vs-expression-translator-date-fns`. This delta splits floating-point division out
of the shared arithmetic-operator shape, because Exasol's `FN_FLOAT_DIV` is always true `DOUBLE`
division while DataFusion's `/` is operand-typed and truncated integer and decimal operands
(issue #186).

## Background

<!-- DELTA:CHANGED -->
The arithmetic operator nodes and every decline in this feature behave identically in both dialects
with ONE exception: `+`, `-`, `*` and unary `-` are the same syntax in both parsers, and a declined
function is declined in both because the adapter advertises one capability set for both dialects —
but `/` (`FLOAT_DIV`) DIVERGES, rendering `(CAST(<l> AS DOUBLE) / <r>)` for DataFusion and a bare
`(<l> / <r>)` for Exasol (issue #186).

A conversion or operator node is translated only when its DataFusion 54 result matches Exasol's to
the precision of the target type, and only when a mismatch cannot produce a silently wrong value.
`FLOAT_DIV` qualifies under both halves of that rule and is the reason the rule needs both halves
stated: its cast rendering is bit-exact against Exasol for a scale-0 numerator and within ~1 ULP
(max relative difference `3.17e-16`) for a non-zero-scale decimal numerator, which is at the
resolution limit of the `DOUBLE` column the value lands in; and its one behavioural divergence,
division by zero, fails the query rather than returning a wrong value. Exasol `DIV` returns the
integer quotient by truncating toward zero — verified live: `DIV(-7,2) = -3` and `DIV(15.7,6.2) = 2`
— and raises a division-by-zero error (SQL state 22012). DataFusion 54 has no `div` builtin; its `/`
truncates only integer operands and divides non-integer operands fractionally. No single rendering
reproduces `DIV` across every operand type, so `DIV` stays unsupported — the per-row problem, not
the zero-divisor one, is what disqualifies it.
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
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
* **The translator cannot fix this by inspecting operand types — it has none.** This feature's
  Background already records that `crates/vs-expression` "stays a pure, stateless, sibling-shared
  JSON-to-SQL translator with no column-type context", and decision `016-add-fn-div-pushdown`
  records that "the arithmetic operator arm renders operands via recursive calls into opaque SQL
  strings without inspecting their types". An UNCONDITIONAL cast of the left operand to `DOUBLE` is
  therefore the only type-blind rendering that reproduces Exasol's always-`DOUBLE` `FN_FLOAT_DIV`,
  and it is exactly what the type-blindness makes IMPOSSIBLE for `DIV` — `DIV` needs truncation,
  which is correct for integer operands and wrong for every other kind, so no single `DIV`
  rendering exists. The same limitation unblocks one operator and blocks the other.
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
  by dialect, so the "byte-identically in BOTH dialects" claim narrows to the four operators it
  still holds for, and the divergence guard that pinned the old claim is RETARGETED rather than
  removed (the same treatment the CHAR CAST divergence received in
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
* **Division by zero: `x/0` still fails the query, `0/0` reaches the pre-existing NaN-at-emit gap.**
  Measured live end-to-end. Native Exasol raises `data exception - division by zero` (SQL state
  `22012`) for every operand pairing including `DOUBLE/DOUBLE`, and never returns NULL or infinity.
  Post-fix the scan produces `±Inf`, which the Exasol ENGINE rejects at the emit boundary
  (`numeric value out of range: value inf ... is not in [ -1.7976e+308 .. 1.7976e+308 ]`, SQL state
  `22002`) — so the query fails either way and no wrong value is returned; pre-fix it failed too,
  with `Arrow error: Divide by zero error` (also `22002`). `0/0` is different: it yields `NaN`, and
  the raw-scan `emit_batch` path carries no per-value check, so Exasol receives a silent NULL where
  it would natively have errored. That is the already-tracked NaN-at-emit gap (`#246`), reachable
  today without any cast for a `DOUBLE`-typed numerator (verified live) — the fix widens its
  reachability to integer and decimal numerators rather than creating a new class of defect. The
  partial-aggregate path errors correctly on the same `NaN` through `arrow_value_at`'s check, which
  is exactly the inconsistency `#246` records.
* **The emit boundary is the wrong place to close the remaining gap.** `arrow_value_at` tests only
  `is_nan()`, and widening it to `!is_finite()` would not help: the boundary cannot distinguish a
  COMPUTED non-finite value from one legitimately STORED in the source table — Iceberg types these
  columns "64-bit IEEE 754 floating point", Delta "8-byte double-precision floating-point numbers",
  and Parquet `DOUBLE` admits `±Inf` and `NaN` — so such a check would break reading a table that
  legitimately contains them. A predicate-position division by zero never reaches that boundary at
  all.
* **Neither table-format specification constrains this.** Checked per CLAUDE.md against the Apache
  Iceberg table spec and the Delta Lake protocol: the words "division", "divisor" and "arithmetic"
  appear nowhere normatively in either document, and neither defines expression result types. They
  fix only the STORED operand domain and the widening relation over it — Iceberg
  `#### Primitive Types` gives `int` "32-bit signed integers", `long` "64-bit signed integers",
  `double` "64-bit IEEE 754 floating point", `decimal(P,S)` "Fixed-point decimal; precision P,
  scale S" / "Scale is fixed, precision must be 38 or less", and its `#### Schema Evolution` permits
  only `int`→`long`, `float`→`double` and `decimal(P, S)`→`decimal(P', S)` "if `P' > P`" / "Widen
  precision only"; Delta's `§ Schema Serialization Format` gives `integer` "4-byte signed integer",
  `long` "8-byte signed integer", `double` "8-byte double-precision floating-point numbers",
  `decimal` "signed decimal number with fixed precision ... The precision and scale can be up to
  38.", and its `§ Type Widening` additionally permits "`Byte`, `Short` or `Int` -> `Double`" and
  "`Byte`, `Short` or `Int` -> `Decimal(10 + k1, k2)` where `k1 >= k2 >= 0`". So the operand Arrow
  type reaching this operator is legitimately any of `Int32`/`Int64`/`Decimal128`/`Float32`/
  `Float64`, and on Delta it can CHANGE between table versions — a further argument for an
  unconditional, type-blind cast over any type-conditional rendering. There is no format-spec
  deviation to fix or track here; how a query engine computes and types a division is outside both
  specifications.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Arithmetic operators translate to binary SQL expressions

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is the Exasol scalar-function name for addition, subtraction, or multiplication, or for unary negation
* *AND* the exact `name` strings Exasol emits for these operators have been verified against live `EXPLAIN VIRTUAL` output for an arithmetic pushdown (so the translator matches what Exasol actually sends, e.g. `MULT` for `*`, not an assumed `MUL`)
* *WHEN* `render_expression` or `render_expression_exasol` processes the node
* *THEN* the `ADD`, `SUB`, and `MULT` nodes SHALL return `(<left> <op> <right>)` where the operators are `+`, `-`, `*` respectively, for operands that are themselves any renderable expression (including two bare column references, e.g. `(L_EXTENDEDPRICE * L_DISCOUNT)`), byte-identically in BOTH dialects — the operator syntax is shared by both parsers, and these wire names are NOT Exasol function names (Exasol has no function called `ADD`), so the Exasol dialect's verbatim rule for named functions MUST NOT be applied to them
* *AND* unary negation SHALL return `(-<operand>)` and SHALL compose inside an aggregate argument (e.g. `SUM(-<operand>)`) so it flows through the arithmetic-aggregate decomposition path
* *AND* floating-point division (`FLOAT_DIV`) SHALL NOT be rendered by this shape — it is the one arithmetic operator whose rendering diverges by dialect, specified in the two FLOAT_DIV scenarios below (issue #186); this scenario's "byte-identically in BOTH dialects" claim covers `ADD`, `SUB`, `MULT`, and `NEG` only
* *AND* the set of arithmetic `name` strings the translator matches SHALL correspond exactly to the arithmetic operator capabilities the adapter advertises (`vs-adapter/pushdown-planning-capability-extensions`) — `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`, and `FN_NEG` — so no advertised operator is left unrenderable and no rendered operator is left unadvertised
* *AND* Exasol integer division (`DIV`) SHALL NOT be matched here and `FN_DIV` SHALL NOT be advertised
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: FLOAT_DIV renders true float division in the DataFusion dialect

* *GIVEN* a VS expression node of type `function_scalar` named `FLOAT_DIV` — the wire name for Exasol's `/`, advertised as `FN_FLOAT_DIV`
* *AND* Exasol's `FN_FLOAT_DIV` always performs true float division and always results in `DOUBLE`, for every operand-type combination — verified live against the Docker Exasol container: `7/2 = 3.5` (NOT `3`), `CAST(711.56 AS DECIMAL(18,2))/CAST(7 AS DECIMAL(18,0)) = 101.65142857142857` at full double precision (NOT scale-capped), and a CTAS over each operand pairing types the result column `DOUBLE` in `EXA_ALL_COLUMNS`
* *AND* the translator carries no operand-type context of its own, so it cannot render one shape for integer operands and another for decimal or double operands
* *WHEN* the node is rendered through the DataFusion-dialect entry points (`render_expression`, `render_expression_safe`, `render_df_filter_safe`) — the ones whose output DataFusion's SQL frontend parses inside the scan UDF
* *THEN* the translator SHALL return `(CAST(<left> AS DOUBLE) / <right>)`, casting the LEFT operand unconditionally and leaving the right operand as rendered
* *AND* it MUST NOT return the bare `(<left> / <right>)`, which DataFusion evaluates by operand type and which returned silently wrong values on every row for all three operand classes, each reproduced live through the local virtual schema against a native-table oracle: `L_ORDERKEY / L_LINENUMBER` at `(7, 2)` gave `3.0` against `3.5`; `C_DECIMAL_A / 7` at `40.99` gave `5.855714` against `5.855714285714286`; `C_DECIMAL_A / C_DECIMAL_B` (`DECIMAL(9,2)/DECIMAL(20,4)`) gave `0.000102` against `0.000102474999897525`; and `COUNT(*) WHERE L_ORDERKEY / L_LINENUMBER > 3 AND L_ORDERKEY = 7` gave `1` against `2` — a truncated quotient changes row counts as well as values (issue #186)
* *AND* decimal/decimal division SHALL NOT be treated as an already-correct control case: DataFusion's `Decimal128(24,6)` quotient is correct only when both operands carry few enough significant digits that scale 6 loses none
* *AND* casting only the LEFT operand SHALL suffice for every operand-type combination the two lakehouse formats can present, verified on DataFusion 54.1 / arrow 58.3 for `Int64÷Int64`, `Decimal128÷Int64`, `Int64÷Decimal128`, `Decimal128÷Decimal128` and `Float64÷Float64` — each plans, executes, and yields `Float64`, `Float64 ÷ Decimal128` included, and the cast is a semantic no-op when the left operand is already `DOUBLE`, so no operand type needs excluding; the translator MUST NOT cast both operands, which would add a redundant no-op cast to every rendering
* *AND* the target type SHALL be spelled `DOUBLE`, reusing the single mapping `render_cast_target` already applies to a `DOUBLE`/`DOUBLE PRECISION` CAST node, so the spelling has ONE owner in the crate rather than a second independently-maintained copy; Exasol's own parser accepts that bare spelling too
* *AND* NULL SHALL propagate unchanged — a NULL left operand, a NULL right operand, and a NULL left operand over a zero right operand each yield NULL, not an error and not `NaN`
* *AND* the resulting `Float64` column SHALL match the `DOUBLE PRECISION` EMITS type the adapter declares for the item from Exasol's own `selectListDataTypes` — live-confirmed as `{"type":"DOUBLE"}` with `"emit_exa_types":["DOUBLE PRECISION"]` — so the emit-boundary type coercion keeps the column on its zero-copy fast path instead of casting a truncated `Int64`/`Decimal128` column up into a `DOUBLE` column, the mechanism that made the pre-fix integer quotient surface as `3.0` rather than as `3`
* *AND* parity against native Exasol SHALL be asserted as bit-exact for an integer (scale-0) numerator and as equal within ~1 ULP for a non-zero-scale decimal numerator, because Exasol's own decimal division is not bit-identical to converting the numerator to `DOUBLE` first — over a `DECIMAL(18,2)` column, 3335 rows × divisors `{2,3,7,11,13}`, 1027 results (31%) differed by exactly one ULP, max relative difference `3.17e-16`, while the same sweep over `DECIMAL(p,0)/DECIMAL(p,0)` was bit-exact at 0 of 3335 — so an oracle comparison for a decimal-numerator shape MUST use a relative tolerance, never string equality

### Scenario: The Exasol dialect keeps rendering FLOAT_DIV as a bare division operator

* *GIVEN* the same `function_scalar` node named `FLOAT_DIV`
* *WHEN* the node is rendered through the Exasol-dialect entry points (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`) — the ones whose output Exasol's own core engine parses
* *THEN* the translator SHALL return the bare `(<left> / <right>)`, UNCHANGED from the pre-fix rendering, because Exasol's `/` IS `FN_FLOAT_DIV` and already divides as `DOUBLE` for every operand type, so the cast would add Exasol-facing SQL that changes nothing
* *AND* the two dialects SHALL therefore DIVERGE on the same `FLOAT_DIV` node — `(CAST(<l> AS DOUBLE) / <r>)` in the DataFusion dialect, `(<l> / <r>)` in the Exasol dialect — and the existing both-dialects identity guard for arithmetic operators SHALL be RETARGETED to assert this divergence rather than deleted, so the divergence stays pinned by a test (the same treatment the CHAR CAST divergence received in `sql-comprehension/vs-expression-translator-cast`)
* *AND* every Exasol-dialect consumer's SQL SHALL stay byte-identical — the qualified single-table wrapper, the N-scan join wrapper, the grouped merge, the single-group scalar-over-aggregate merge (`render_scalar_over_merge`, which calls `render_expression_exasol`), and the self-applied WHERE path — so both `dispatch_golden` fixtures carrying a translated `FLOAT_DIV` (`single_group_scalar_over_aggregate_dedup.sql`, `single_group_scalar_over_aggregate_interleaved.sql`) MUST remain unchanged, and a diff in either is a regression rather than an expected update
* *AND* the `/` characters in the AVG and statistical merge fragments (`scalar_over_agg.rs`'s `SUM(<partial>) / NULLIF(SUM(<partial>), 0)` and the König–Huygens numerator) SHALL be unaffected in both dialects, because they are adapter-authored merge SQL that never passes through the translator's `FLOAT_DIV` arm

### Scenario: A pushed-down division by zero fails the query rather than returning a wrong value

* *GIVEN* a pushed-down `FLOAT_DIV` whose right operand evaluates to zero for at least one scanned row, the numerator being non-zero
* *AND* native Exasol raises `data exception - division by zero` (SQL state `22012`) for every operand pairing including `DOUBLE/DOUBLE`, verified live and column-driven so nothing is constant-folded, and never returns NULL and never returns infinity
* *WHEN* the DataFusion-dialect rendering `(CAST(<left> AS DOUBLE) / <right>)` is evaluated in the scan and the result is projected
* *THEN* the scan SHALL produce `±Infinity`, and the Exasol ENGINE SHALL reject it at the emit boundary with `numeric value out of range: value inf ... is not in [ -1.7976e+308 .. 1.7976e+308 ]` (SQL state `22002`), so the query FAILS and MUST NOT return a wrong value
* *AND* this SHALL be recorded as an accepted message-and-SQL-state divergence, NOT a correctness regression: the pre-fix rendering also failed the same query at `22002`, with `Arrow error: Divide by zero error`, so the query-fails-either-way outcome is unchanged by this feature; only the text and the raising layer differ from Exasol's `22012`
* *AND* Exasol SHALL be understood to admit no non-finite `DOUBLE` at all — `CAST('inf' AS DOUBLE)`, `CAST('Infinity' AS DOUBLE)` and `CAST('nan' AS DOUBLE)` are each rejected at `22018`, and `1E400` at `22003` — which is why an infinity cannot silently reach a result set through the projection path

### Scenario: Zero divided by zero reaches the tracked NaN-at-emit gap

* *GIVEN* a pushed-down `FLOAT_DIV` whose numerator AND denominator both evaluate to zero for a scanned row
* *WHEN* the DataFusion-dialect rendering is evaluated and the result is projected through the raw-scan path
* *THEN* DataFusion SHALL produce `NaN`, and because the raw-scan path emits Arrow IPC bytes through `ctx.emit_batch` with no per-value check, Exasol SHALL receive a silent `NULL` where it would natively have raised `22012` — verified live, the query succeeded and returned NULL for every such row
* *AND* this SHALL be recorded as a WIDENING of the already-tracked NaN-at-emit gap `(#246)`, not as a new class of defect: the same silent NULL is reachable TODAY with no cast involved whenever the numerator column is already `DOUBLE`-typed (verified live for `(L_EXTENDEDPRICE - L_EXTENDEDPRICE) / (L_LINENUMBER - L_LINENUMBER)`), and this feature only extends that reachability to integer and decimal numerators
* *AND* the partial-aggregate path SHALL keep erroring correctly on the same `NaN` through `arrow_value_at`'s `is_nan()` check (`numeric value out of range: NaN result from an out-of-domain math operation`), so the two emit paths disagree on identical input — which is precisely the inconsistency `#246` records and MUST NOT be re-tracked as a second issue by this feature
* *AND* this feature MUST NOT close the gap by widening `arrow_value_at`'s check from `is_nan()` to `!is_finite()`, because that boundary cannot distinguish a COMPUTED non-finite value from one legitimately STORED in the source table — Iceberg and Delta both type these columns IEEE-754 `double` and Parquet `DOUBLE` admits `±Inf` and `NaN` — so such a check would break reading a table that legitimately contains them
* *AND* this feature MUST NOT close the gap by rendering `NULLIF(<right>, 0)` either: NULL is exactly the wrong answer already being observed, it makes a genuine zero divisor indistinguishable from a NULL divisor, and it would additionally suppress the `x/0` case that currently fails loudly
* *AND* a division by zero in a PREDICATE position SHALL be treated as a distinct, VERIFIED-DIVERGENT case, measured live in both the single-table predicate position and the broadcast-join leg: an infinity compared against a bound never reaches the emit boundary, so `WHERE <a> / <b> > <k>` SILENTLY admits or rejects rows (observed 20 of 20, or 0 of 20) where native Exasol raises `22012` — the fix converts a loud `22002` failure into a silent wrong row count, and the join path adds no divergence of its own
* *AND* this SHALL be recorded as a WIDENING of an already-reachable predicate-position gap, NOT a newly introduced defect: the same silent divergence is reachable TODAY with no cast involved whenever the numerator column is already `DOUBLE`-typed. It is tracked as issue `#370`, DISTINCT from `#246` — `#246` covers a projected-value NaN-to-NULL divergence at the emit boundary, whereas `#370` covers a predicate row-count divergence that never reaches emit
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Integer division DIV is deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `DIV` — Exasol integer-quotient division, which truncates toward zero (`DIV(-7,2) = -3`, verified live) and raises a division-by-zero error
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming `DIV` as unsupported
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate `DIV`, because DataFusion 54 has no `div` builtin and, unlike `FLOAT_DIV`, no single type-blind rendering reproduces it: `DIV` needs TRUNCATION, which DataFusion's `/` delivers for integer operands and not for any other kind, and unlike CAST's explicit `dataType` field, DIV's operand types are not carried in the expression node, so the translator cannot identify and selectively render only the safe integer-operand case
* *AND* this decline SHALL NOT be read as resting on the division-by-zero divergence, which the FLOAT_DIV scenarios above now measure and record: for `x/0` the query fails either way, and for `0/0` the divergence belongs to the tracked NaN-at-emit gap `(#246)` rather than to the rendering. `DIV`'s disqualifying defect is that a wrong rendering would be wrong on EVERY row for non-integer operands, not only when a divisor is zero — that is the difference between the two operators, and a future `TRUNC(m/n)` emulation would have to answer the per-row problem, not the zero-divisor one
<!-- /DELTA:CHANGED -->

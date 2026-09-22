# Decisions: fix-float-div-predicate-divzero

## ADR: Fix the divide-by-zero at the rendering layer with a checked-division function

**ID:** checked-float-division-rendering-layer-fix
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted
**Supersedes:** float-div-zero-behaviour-from-measurement-not-emulation

### Context

Issue #370 measured a pushed `FLOAT_DIV` by zero inside a `WHERE` predicate returning a silently
wrong row count: the infinity `x/0` produces is consumed inside DataFusion's comparison and never
reaches the emit boundary that rejects it in projection position. Filter predicates are not built
as `Expr` trees. `crates/vs-expression` renders the predicate to a SQL string that each scan path
splices into the SQL it hands to `SessionContext::sql`, so the rendering layer, not the physical
plan, decides the outcome, and it is one layer for every position and every scan path. The earlier
ADR `float-div-zero-behaviour-from-measurement-not-emulation` recorded the divide-by-zero behaviour
from measurement rather than fixing it, and concluded no fix existed for the predicate case. That
conclusion does not hold once the rendering layer, rather than the emit boundary, is considered.

### Decision

The DataFusion dialect renders `FLOAT_DIV` as `vs_checked_float_div(<left>, <right>)`, a scalar
function the scan session registers, instead of the `/` operator. The function raises when its own
result is not finite. This supersedes the earlier ADR for the predicate case and for the `0/0`
case; that ADR's measurements stand unchanged, but its conclusion that no fix exists does not.

### Options Considered

| Option | Verdict |
|--------|---------|
| Checked-division function at the rendering layer | ✓ Chosen — sees only the two operands of a division the pushdown itself synthesised, so it never inspects a value read straight out of a column, and it sits at the one place every position shares |
| Widen `arrow_value_at`'s `is_nan()` check to `!is_finite()` | ✗ Rejected — already rejected in the superseded ADR; the emit boundary cannot tell a computed non-finite value from one stored in the source table, and it never sees a predicate at all |
| Render `NULLIF(<right>, 0)` | ✗ Rejected — already rejected in the superseded ADR; NULL is the wrong answer already observed, and it conflates a zero divisor with a NULL divisor |
| Stop pushing any predicate containing a division; apply it in the Exasol wrapper instead | ✗ Rejected — gives exact `22012` parity but costs filter pushdown on every division predicate and forces the scan to project the operand columns |
| Accept the gap and file a tracked exception | ✗ Rejected — a tracked exception is the right answer when no safe fix exists; here one does |

### Consequences

Projection and predicate stop drifting apart, because one check now decides the outcome for both.
A plain `SELECT <double_col>` over a table storing `NaN` reaches no checked division and is
untouched by this fix.

---

## ADR: Raise on any non-finite result, not only on a zero divisor

**ID:** checked-float-division-raises-on-any-non-finite-result
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted

### Context

A finite numerator over a tiny divisor can overflow to `±Inf`, reproducing issue #370's exact
defect class even when the divisor itself is non-zero: a non-finite value consumed by a comparison
that never reaches an emit-time check. Exasol admits no non-finite `DOUBLE` at all, so a non-finite
result is never a representable answer, whatever produced it.

### Decision

The checked-division function raises when the computed `Float64` result is not finite, not only
when the divisor is zero. A zero divisor raises with a message naming a division by zero. Any
other non-finite cause raises with a message naming a numeric value out of range.

### Options Considered

| Option | Verdict |
|--------|---------|
| Raise on any non-finite result | ✓ Chosen — closes the overflow route a divisor-only check would leave open, and keeps the two causes separable by message for a support case |
| Check only `<right> == 0.0` | ✗ Rejected — leaves the overflow-to-infinity route open, reproducing issue #370's defect class through a non-zero divisor |

### Consequences

A finite-over-tiny-divisor overflow now fails the query with a distinct message from a division by
zero, rather than silently producing an infinity a later check might or might not catch.

---

## ADR: The function name is owned by `crates/vs-expression`; the implementation is owned by `crates/lakehouse-engine`

**ID:** checked-float-division-name-owned-by-vs-expression
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted

### Context

The DataFusion dialect now has a runtime prerequisite: a consumer of that dialect must register the
checked-division function under the exact name the rendering emits. `crates/vs-expression` is a
pure, stateless, sibling-shared JSON-to-SQL translator with no SQL-parser dependency, so it cannot
own a DataFusion `ScalarUDF` implementation without a large, unwanted dependency change.

### Decision

`crates/vs-expression` exports the function name as one public constant, documented with the full
contract the implementation must satisfy: two arguments, both coerced to `Float64`, a `Float64`
result, NULL propagated, and an error raised when the result is not finite. The rendering reads
that constant. `crates/lakehouse-engine` implements the `ScalarUDF` and registers it under the same
constant.

### Options Considered

| Option | Verdict |
|--------|---------|
| Name owned by `vs-expression`, implementation owned by `lakehouse-engine`, both reading one constant | ✓ Chosen — states the runtime prerequisite once, at the constant, and a consumer that forgets to register fails loudly at first use rather than silently |
| Implement the `ScalarUDF` inside `crates/vs-expression` | ✗ Rejected — that crate depends only on `exasol-udf-sdk` and `serde_json` by design; adding DataFusion is a large change for a small fix |
| Duplicate the name as a string literal on both sides | ✗ Rejected — two owners of one name is exactly the drift the constant exists to prevent |

### Consequences

The sibling project that shares `crates/vs-expression` inherits the same explicit prerequisite: it
must register a matching function under the exported name to use the DataFusion dialect.

---

## ADR: The DataFusion dialect renders no cast: drop the `CAST(<left> AS DOUBLE)` wrapper

**ID:** checked-float-division-drops-datafusion-cast
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted
**Supersedes:** float-div-cast-datafusion-dialect-only

### Context

The superseded ADR `float-div-cast-datafusion-dialect-only` established that `FLOAT_DIV` renders
`(CAST(<left> AS DOUBLE) / <right>)` through the DataFusion-dialect entry points, and that the
Exasol dialect needs no help and must stay byte-identical. Once the DataFusion dialect renders a
checked-division function call instead of a bare operator, that function must coerce both operands
to `Float64` itself to raise correctly, so a SQL-level cast for one operand would state the
always-`DOUBLE` decision in two places.

### Decision

The DataFusion dialect renders no cast for a `FLOAT_DIV` node. It no longer wraps the left operand
in `CAST(... AS DOUBLE)`, and it does not wrap the right operand either. The called function
coerces both operands to `Float64` itself. The superseded ADR's conclusion that the Exasol dialect
needs no help and must stay byte-identical is retained unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Drop the cast; let the function coerce both operands | ✓ Chosen — one owner of the always-`DOUBLE` coercion decision, no SQL-level restatement |
| Keep the cast and coerce only the right operand inside the function | ✗ Rejected — states the coercion decision in two modules for no benefit |

### Consequences

The E2E and unit expectations for the rendered SQL change from the cast form to the function-call
form. The Exasol dialect is unaffected. This ADR exists so the cast-rendering ADR it reverses
acquires a pointer, keeping two Accepted ADRs from disagreeing about what the DataFusion dialect
renders.

---

## ADR: The residual evaluation-set divergence is a tracked exception, in both directions

**ID:** float-div-evaluation-set-divergence-tracked-exception
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted

### Context

The checked division's error is a per-row side effect of an expression DataFusion evaluates over a
row set of its own choosing, so it can diverge from native Exasol in two directions. Suppression:
DataFusion may skip the division for a row another conjunct, file pruning, row-group pruning, or a
LIMIT already removed, so a query native Exasol fails may succeed. Over-raise: `datafusion-physical-expr`
54.1's `PRE_SELECTION_THRESHOLD: f32 = 0.2` in `src/expressions/binary.rs` means `check_short_circuit`
pre-selects surviving rows for an `AND` only when the left conjunct's true ratio is at or below that
threshold; above it, the right conjunct is evaluated over the full batch including rows the left
conjunct excluded, a null in the left conjunct disables the strategy entirely, and a division in the
left conjunct is never protected at all. So `WHERE <d> <> 0 AND <n> / <d> > 0` can raise despite its
guard, depending on per-batch selectivity and conjunct order. Native Exasol's own answer for the
guarded shape was measured live (task 1.2): it returns 10 of 20 rows without raising, in both
conjunct orders.

### Decision

Both directions are recorded as ONE tracked exception, GitHub issue `#392`, cited inline in both
spec deltas. The divergence is scoped to error-raising alone in both directions: a query that does
not raise returns exactly the rows Exasol returns, because a row reaches the result only when its
division was evaluated and finite.

### Options Considered

| Option | Verdict |
|--------|---------|
| One tracked exception covering both directions | ✓ Chosen — both are the same underlying fact: the error is a per-row side effect of an expression DataFusion is free to evaluate over a row set of its own choosing |
| Claim full parity and say nothing | ✗ Rejected — a known deviation must be fixed or recorded as an accurately scoped tracked exception, never a silent gap |
| File two issues, one per direction | ✗ Rejected — they are the same underlying fact stated twice |
| Suppress the over-raise direction by rendering the guard into the function | ✗ Rejected — the translator has no way to know which conjunct guards which division, and inventing one would reintroduce operand-type reasoning the crate deliberately avoids |

### Consequences

The harmful half of issue #370, the wrong row count, is removed entirely in both directions. What
remains changes only whether an error is raised, never which rows a successful query returns. The
over-raise direction is the one that can turn a working query into an intermittent failure, so it
is recorded rather than left as an unstated risk.

---

## ADR: The NaN comparison ordering issue #370 reported is scoped out and tracked separately

**ID:** float-div-nan-comparison-ordering-scoped-out
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted

### Context

Issue #370's second observation was that `NaN < -1E300` matched all 20 rows while `NaN > 1E300`
matched none, with the mechanism left uninvestigated. After the checked-division fix, a pushed
`FLOAT_DIV` can no longer produce a `NaN`, so issue #370's own reproducer no longer reaches that
behaviour. Comparison semantics for a `NaN` read from a column, rather than computed, stay
unmeasured. Iceberg anticipates a stored `NaN` normatively: its statistics rules state "NaNs are
not permitted as lower or upper bounds," and its manifest `field_summary` carries a
`nan_value_counts` entry, so the shape is reachable rather than hypothetical.

### Decision

The NaN comparison ordering issue #370 reported is out of scope for this fix. It is tracked as
GitHub issue `#393`, cited inline in both spec deltas, scoped to verifying live what a pushed
comparison returns for a stored `NaN` in a `double` column.

### Options Considered

| Option | Verdict |
|--------|---------|
| Track separately as its own issue | ✓ Chosen — a different mechanism on a different value source than the fix this plan makes; verifying it needs a fixture this repo does not have |
| Fold it into this plan | ✗ Rejected — this plan is a fix, not a redesign, and the mechanism is unmeasured |
| Say nothing, since the fix removes issue #370's own path to it | ✗ Rejected — would make it a silent gap the moment issue #370 closes |

### Consequences

Comparison semantics for a stored `NaN` remain an open, explicitly tracked question, distinct from
the divide-by-zero defect this plan fixes.

---

## ADR: Non-finite values from pushed scalar functions other than `FLOAT_DIV` are a third tracked exception

**ID:** non-finite-scalar-functions-other-than-float-div-tracked-exception
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted

### Context

This plan fixes one producer of a non-finite value in predicate position. `crates/lakehouse-engine/src/adapter/capabilities.rs`
also advertises `FN_SQRT`, `FN_LN`, `FN_LOG`, `FN_ACOS`, `FN_ASIN`, `FN_EXP`, `FN_POWER`, and
`FN_MOD`. Each is translated into a pushed predicate and each can yield `NaN` or `±Inf` from
in-domain column data: `SQRT`, `LN`, and `LOG` on a negative argument, `ACOS` and `ASIN` outside
`[-1, 1]`, `EXP` and `POWER` on overflow, `MOD` on a zero divisor. `WHERE SQRT(<negative_col>) > 0`
reproduces issue #370's mechanism with no division involved: the comparison consumes the
non-finite value inside the scan, and no emit-boundary check ever sees it.

### Decision

The residual is recorded as a third, accurately scoped tracked exception rather than left
unstated, GitHub issue `#394`, cited inline in both spec deltas, scoped to measuring each
function's live behaviour against a native Exasol oracle and then either extending the
checked-function treatment this plan establishes or declining the affected capabilities.

### Options Considered

| Option | Verdict |
|--------|---------|
| One tracked exception naming all eight capabilities | ✓ Chosen — one issue, one mechanism, one accurate scope, the same reasoning the earlier truncation ADR used to reject a blanket divide-by-zero issue |
| Fold the other functions into this plan | ✗ Rejected — each has its own domain and its own unmeasured native Exasol behaviour; extending would multiply the live-measurement surface by eight before issue #370 is closed |
| Fold them into the NaN-comparison-ordering issue | ✗ Rejected — that issue is scoped to a `NaN` read from a column, and its title excludes a computed one |
| Fold them into the evaluation-set-divergence issue | ✗ Rejected — that issue is scoped to which rows the division is evaluated over, not to which functions can produce a non-finite value |
| Say nothing | ✗ Rejected — "not measured" is neither a fix nor a tracked exception, and it never reaches the recorded spec |

### Consequences

Eight advertised capabilities keep the exact gap issue #370 reports, explicitly tracked rather than
silently present, until a follow-up plan measures and addresses each one.

---

## ADR: The checked division records its first failure on the session because the Parquet row filter destroys the error type

**ID:** checked-float-division-session-scoped-failure-recording
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted

### Context

`datafusion-datasource-parquet` 54.1 flattens any predicate error at `row_filter.rs:150-170` into
`ArrowError::ComputeError(format!("Error evaluating filter predicate: {e:?}"))`. This destroys the
error's type before `classify_scan_error` sees it, so a checked division raised inside a predicate
pushed into the Parquet row filter, issue #370's own route, reaches the classifier indistinguishable
from a storage failure. Live pre-amendment evidence: `scan failed: assigned data could not be read:
Parquet error: External: Compute error: Error evaluating filter predicate: External(ZeroDivisor {
numerator: 1.0, divisor: 0.0 })`. Relying on the DataFusion error chain alone, as first specified,
does not survive this route.

### Decision

The checked division records its first failure as a typed `CheckedFloatDivError` in a `OnceLock` on
the registered UDF instance. `session_checked_float_div_failure` reads it back through the session's
own function registry, by downcasting the resolved `ScalarUDFImpl`. `run_scan_dispatch`, the one
dispatcher all three run paths funnel through, calls `emit::reframe_checked_division` once on a
failed scan. That reframing REPLACES the surfaced failure with the division, except when the
surfaced failure is a memory exhaustion, where it COMPOSES: division first, then the memory
exhaustion, both redacted. `ResourcesExhausted` is the one case `classify_scan_error` recognises on
the typed error root before any text exists, so it is the one case separable from the flattened
division without matching DataFusion's wording.

### Options Considered

| Option | Verdict |
|--------|---------|
| Session-scoped `OnceLock` on the registered UDF instance, read back through the session's function registry | ✓ Chosen — scoped to exactly one scan invocation, so no failure survives into another, and it is the narrowest carrier that still crosses the row-filter's type-destroying boundary |
| Rely on the DataFusion error chain alone | ✗ Rejected — does not survive a predicate pushed into the Parquet row filter, issue #370's own route |
| Recover the value from the flattened `Error evaluating filter predicate` text | ✗ Rejected — the message-text coupling this plan rules out; breaks on any DataFusion wording change |
| Hold the value in a process-global `static` or `LazyLock` | ✗ Rejected — violates the stateless-UDF rule; one query's division would leak into the next scan on a pooled UDF VM |
| Thread an `Arc<OnceLock<CheckedFloatDivError>>` from `run_scan_one` through `build_session_context` | ✗ Rejected — `build_session` is an injected test seam, and every test that substitutes it would need a changed signature |
| Compose with EVERY incoming classification, not only memory exhaustion | ✗ Rejected, reverted on live evidence — `make test-e2e` failed two live E2E tests because the flattened row-filter route's surfaced failure IS the same division under the storage-read framing, so unconditional composition republished that framing on the plan's own primary route |

### Consequences

**Accepted limitation:** an unrelated storage failure raised in another partition of a scan that
also divided by zero is masked, because DataFusion surfaces exactly one partition's error. This
collision is real but rarer than issue #370's own route and has never been observed live, whereas
the storage framing on a user's own division is measured. Recovering it would need a structural
channel that survives `UdfError`, which carries only a `String`; revisit if a live case appears.

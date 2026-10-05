# Decisions: fix-float-div-predicate-divzero

## ADR: Fix the divide-by-zero at the rendering layer with a checked-division function

**ID:** checked-float-division-rendering-layer-fix
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted
**Supersedes:** float-div-zero-behaviour-from-measurement-not-emulation

### Context

Issue #370 measured a pushed `FLOAT_DIV` by zero inside a `WHERE` predicate returning a wrong row count. The infinity from `x/0` is consumed inside DataFusion's comparison and never reaches the emit boundary that rejects it in projection position. `crates/vs-expression` renders the predicate to a SQL string that every scan path hands to `SessionContext::sql`, so the rendering layer decides the outcome for every position and scan path.

### Decision

The DataFusion dialect renders `FLOAT_DIV` as a call to `vs_checked_float_div`, a scalar function that the scan session registers, instead of the `/` operator. The function raises when its result is not finite. This supersedes the earlier ADR for the predicate case and the `0/0` case. Its measurements stand, but its conclusion that no fix exists does not.

### Options Considered

| Option | Verdict |
|--------|---------|
| Widen the emit-time `is_nan()` check to `!is_finite()` | Rejected: the emit boundary cannot tell a computed non-finite value from a stored one, and it never sees a predicate |
| Render `NULLIF(<right>, 0)` | Rejected: NULL is the wrong answer, and it conflates zero and NULL divisors |
| Stop pushing predicates that contain a division | Rejected: gives exact `22012` parity but loses filter pushdown and forces the scan to project the operand columns |
| Accept the gap as a tracked exception | Rejected: an exception fits only when no safe fix exists |

### Consequences

Projection and predicate share one check. A plain `SELECT <double_col>` over a stored `NaN` reaches no checked division and is unchanged.

The check raises on any non-finite `Float64` result, with distinct messages for division by zero and out-of-range overflow.

## ADR: The function name is owned by `crates/vs-expression`; the implementation is owned by `crates/lakehouse-engine`

**ID:** checked-float-division-name-owned-by-vs-expression
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted

### Context

A consumer of the DataFusion dialect must register the checked-division function under the exact name the rendering emits. `crates/vs-expression` is a pure, stateless translator shared with a sibling project, and it cannot own a DataFusion `ScalarUDF` without a large dependency change.

### Decision

`crates/vs-expression` exports the function name as one public constant, documented with the contract: two arguments coerced to `Float64`, a `Float64` result, NULL propagated, and an error when the result is not finite. The rendering reads the constant. `crates/lakehouse-engine` implements the function and registers it under the same constant.

### Options Considered

| Option | Verdict |
|--------|---------|
| Implement the `ScalarUDF` in `vs-expression` | Rejected: the crate depends only on `exasol-udf-sdk` and `serde_json` by design |
| Duplicate the name as a literal on both sides | Rejected: two owners of one name drift |

### Consequences

A consumer that forgets to register the function fails loudly at first use. The sibling project must register a matching function to use the DataFusion dialect.

## ADR: The DataFusion dialect renders no cast: drop the `CAST(<left> AS DOUBLE)` wrapper

**ID:** checked-float-division-drops-datafusion-cast
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted
**Supersedes:** float-div-cast-datafusion-dialect-only

### Context

The checked-division function must coerce both operands to `Float64` itself, so a SQL-level cast would state the always-`DOUBLE` decision in two places.

### Decision

The DataFusion dialect renders no cast for `FLOAT_DIV` and wraps neither operand, because the function coerces both. The Exasol dialect stays byte-identical, as the superseded ADR decided.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the cast and coerce only the right operand in the function | Rejected: states the coercion in two modules |

### Consequences

E2E and unit expectations change from the cast form to the function-call form. This ADR keeps two Accepted ADRs from disagreeing about what the DataFusion dialect renders.

## ADR: The residual evaluation-set divergence is a tracked exception, in both directions

**ID:** float-div-evaluation-set-divergence-tracked-exception
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted

### Context

The checked division's error is a per-row side effect of an expression that DataFusion evaluates over a row set of its own choosing, so it can diverge from Exasol in two directions. Suppression: DataFusion may skip the division for a row that another conjunct, pruning, or a LIMIT already removed, so a query Exasol fails may succeed. Over-raise: `PRE_SELECTION_THRESHOLD` (0.2) in `datafusion-physical-expr` means the right side of an `AND` is evaluated over the full batch when the left conjunct's true ratio is above it. A null in the left conjunct disables the strategy, and a division in the left conjunct is never protected. So `WHERE <d> <> 0 AND <n> / <d> > 0` can raise despite its guard, where Exasol returns 10 of 20 rows in both conjunct orders.

### Decision

Both directions are one tracked exception, issue #392, cited inline in both spec deltas. The divergence affects error-raising only: a query that does not raise returns exactly Exasol's rows, because a row reaches the result only when its division was evaluated and finite.

### Options Considered

| Option | Verdict |
|--------|---------|
| Claim full parity | Rejected: a known deviation must be recorded as an accurately scoped exception |
| Two issues, one per direction | Rejected: both are the same underlying fact |
| Render the guard into the function | Rejected: the translator cannot know which conjunct guards which division |

### Consequences

The wrong row count of issue #370 is removed in both directions. The over-raise direction can turn a working query into an intermittent failure, so it is recorded.

## ADR: The checked division records its first failure on the session because the Parquet row filter destroys the error type

**ID:** checked-float-division-session-scoped-failure-recording
**Plan:** fix-float-div-predicate-divzero
**Status:** Accepted

### Context

`datafusion-datasource-parquet` flattens any predicate error into a text-only `ArrowError::ComputeError`, so a checked division raised in a predicate pushed into the Parquet row filter, issue #370's route, reaches `classify_scan_error` looking like a storage failure.

### Decision

The checked division records its first failure as a typed `CheckedFloatDivError` in a `OnceLock` on the registered UDF instance. `session_checked_float_div_failure` reads it through the session's function registry. `run_scan_dispatch`, the dispatcher for all three run paths, calls `emit::reframe_checked_division` once on a failed scan. The reframing replaces the surfaced failure with the division, except for a memory exhaustion, where it reports the division first and then the memory exhaustion, both redacted. `ResourcesExhausted` is the one case recognised from the typed error root before any text exists.

### Options Considered

| Option | Verdict |
|--------|---------|
| Rely on the DataFusion error chain | Rejected: does not survive the Parquet row filter |
| Recover the value from the flattened error text | Rejected: couples to DataFusion's wording |
| Process-global `static` or `LazyLock` | Rejected: violates the stateless-UDF rule, and one query's division would leak into the next scan |
| Thread an `Arc<OnceLock<...>>` through `build_session_context` | Rejected: `build_session` is an injected test seam, and every substituting test would change signature |
| Compose with every incoming classification | Rejected: live E2E showed the row-filter route's surfaced failure is the same division under storage framing, so composing republished that framing |

### Consequences

An unrelated storage failure in another partition of a scan that also divided by zero is masked, because DataFusion surfaces one partition's error. This is rarer than issue #370's route and never observed live. Recovering it needs a channel that survives `UdfError`, which carries only a `String`, so revisit it if a live case appears.

# Decisions: fix-float-div-truncation

## ADR: Render the DOUBLE cast in the DataFusion dialect only, not in both dialects

**ID:** float-div-cast-datafusion-dialect-only
**Plan:** fix-float-div-truncation
**Status:** Accepted

### Context

`FLOAT_DIV` shared the bare-operator arm with `ADD`/`SUB`/`MULT` and inherited DataFusion's
operand-typed `/`, which truncated integer and decimal operands and returned silently wrong
values on every row of a valid query (issue #186). Exasol's own `/` **is** `FN_FLOAT_DIV`, so
the Exasol dialect is already correct — verified live: `7/2 = 3.5` on integer literals,
`CAST(711.56 AS DECIMAL(18,2))/CAST(7 AS DECIMAL(18,0)) = 101.65142857142857` at full double
precision, the same over columns so nothing is constant-folded, and a CTAS whose result column
types as `DOUBLE` in `EXA_ALL_COLUMNS` for every operand pairing including `DECIMAL/DECIMAL`
and `DECIMAL/DOUBLE`. Rendering the cast in both dialects would rewrite Exasol-facing SQL across
five consumer sites — the qualified single-table wrapper, the N-scan join wrapper, the grouped
merge, the single-group scalar-over-aggregate merge, and the self-applied WHERE path — including
two byte-exact `dispatch_golden` fixtures and two native-oracle E2E string comparisons of the
scalar-over-aggregate feature, for zero correctness gain. It would also leak a DataFusion
type-coercion workaround into SQL parsed by a different engine, whose parser this repo has
repeatedly been bitten by (CHAR CAST, `%`, `SIGNUM`, TSTZ EMITS).

### Decision

`FLOAT_DIV` renders `(CAST(<left> AS DOUBLE) / <right>)` through the DataFusion-dialect entry
points and keeps the bare `(<left> / <right>)` through the Exasol-dialect ones. `FLOAT_DIV`
becomes the first arithmetic operator whose rendering diverges by dialect; the existing
both-dialects identity guard is retargeted to assert the divergence rather than deleted, the same
treatment the CHAR CAST divergence already received.

### Options Considered

| Option | Verdict |
|--------|---------|
| Cast only in the DataFusion dialect | ✓ Chosen — the translator's job is to make each target engine reproduce Exasol's `FN_FLOAT_DIV`, and Exasol needs no help |
| Render the cast unconditionally in both dialects | ✗ Rejected — simpler code, but rewrites five Exasol-facing consumer sites and two golden fixtures for no correctness gain, and leaks a DataFusion-specific workaround into Exasol's own parser |

### Consequences

The issue's decided approach narrows rather than is contradicted: its three staging repros were
all DataFusion-dialect, so the Exasol dialect was never exercised by that verification. The cost
is one existing guard test retargeted from an identity assertion to a divergence assertion.

---

## ADR: Record the divide-by-zero behaviour from measurement; do not emulate it

**ID:** float-div-zero-behaviour-from-measurement-not-emulation
**Plan:** fix-float-div-truncation
**Status:** Accepted

### Context

The interview asked whether Exasol vs. DataFusion divide-by-zero semantics for `FLOAT_DIV`
needed a fix alongside the truncation bug. Live measurement dissolved most of the concern the
interview raised, and the planning draft that assumed a silent wrong value for `x/0` was wrong —
the check mattered. Native Exasol raises `22012` for every operand pairing including
`DOUBLE/DOUBLE`, column-driven, and never returns NULL or infinity; Exasol admits no non-finite
`DOUBLE` at all (`CAST('inf' AS DOUBLE)` → `22018`, `1E400` → `22003`), which is why an infinity
cannot reach a result set through the projection path — so `x/0` needs no fix, the query fails
before and after the change. `0/0` yields `NaN`, and the raw-scan `emit_batch` path delivers a
silent NULL: that is issue `#246`'s already-tracked NaN-at-emit gap, widened rather than newly
created since it is reachable today whenever the numerator column is already `DOUBLE`-typed.
Predicate-position divide-by-zero was measured separately (task 1.2) and found to silently admit
or reject rows where native Exasol raises `22012`, tracked as issue `#370`, distinct from `#246`
because it is a row-count divergence that never reaches the emit boundary rather than a
projected-value divergence that does.

### Decision

Measure each divide-by-zero case live and record it as three cases with three different owners
rather than emulating any of them: `x/0` needs no fix (fails both before and after, only the
message and raising layer differ from Exasol's `22012`); `0/0` widens the already-tracked `#246`
gap; predicate-position divide-by-zero widens a newly-filed, distinct issue `#370`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Record measured behaviour, cite existing/new tracked issues | ✓ Chosen — matches CLAUDE.md's rule that a known deviation be fixed or recorded as an accurately-scoped tracked exception |
| Render `NULLIF(<right>, 0)` so a zero divisor yields NULL | ✗ Rejected — NULL is exactly the wrong answer already observed in the `0/0` case, conflates a zero divisor with a NULL divisor, and would additionally silence the `x/0` case that currently fails loudly |
| Widen `arrow_value_at`'s `is_nan()` check to `!is_finite()` | ✗ Rejected — the emit boundary cannot distinguish a computed non-finite value from one legitimately stored in the source table; Iceberg and Delta both type these columns IEEE-754 `double` and Parquet `DOUBLE` admits `±Inf`/`NaN`; it also misses the predicate case entirely |
| Decline `FLOAT_DIV` entirely, the `DIV` precedent | ✗ Rejected — would withhold the truncation fix from the most common arithmetic operator and regress the shipped scalar-over-aggregate decomposition, which depends on `SUM(x) / COUNT(*)` rendering |
| File one blanket tracked-exception issue for "divide-by-zero divergence" | ✗ Rejected — inaccurate scoping; `#246` (projected-value) and `#370` (predicate row-count) are different failure modes on different code paths and warrant separate tracking |

### Consequences

Three cases now have three explicit owners instead of one under-scoped concern. `x/0` requires no
code change. `#246`'s reachability is documented as widened from `DOUBLE`-typed numerators to
integer and decimal numerators. `#370` is filed as a new, distinct issue covering both the
single-table predicate position and the broadcast-join leg, which were measured to add no
divergence of its own beyond reproducing the single-table case.

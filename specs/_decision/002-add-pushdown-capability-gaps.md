# Decisions: add-pushdown-capability-gaps

## ADR: Exclude FN_DIV — No Faithful DataFusion Floor Division

**ID:** exclude-fn-div-no-faithful-datafusion-floor-division
**Plan:** `add-pushdown-capability-gaps`
**Status:** Superseded by exclude-fn-div-no-faithful-datafusion-truncated-division

Live Exasol verification (2026-07-22) found this ADR's premise false: Exasol `DIV` truncates toward
zero, not floor division, and matches DataFusion integer `/`. The decline outcome stands; only this
ADR's stated reason was wrong. See `specs/_decision/016-add-fn-div-pushdown.md` (plan
`add-fn-div-pushdown`) for the corrected ADR.

### Context

Issue #105 asked whether `FN_NEG` and `FN_DIV` have exact DataFusion reproductions. Exasol `DIV` is
floor division; DataFusion 54 `/` truncates integer division toward zero, diverging from Exasol for
negative operands, and DataFusion 54 exposes no `div` function at all.

### Decision

Do not advertise `FN_DIV`. The `crates/vs-expression` translator declines a `DIV` node in both
raising and safe modes; the adapter omits the expression and Exasol evaluates `DIV` itself as a
correctness backstop.

### Options Considered

| Option | Verdict |
|--------|---------|
| Leave `FN_DIV` unadvertised; Exasol post-processes `DIV` | ✓ Chosen — the safe fallback is already correct; no faithful DataFusion reproduction exists |
| Emulate via `CAST(FLOOR(a / CAST(b AS DOUBLE)) AS …)` | ✗ Rejected — div-by-zero, negative-operand, and decimal-rounding parity against Exasol are unverified |

### Consequences

`DIV` expressions never push down; Exasol always evaluates them. No silent divergence between
Exasol floor division and DataFusion truncating division is possible.

---

## ADR: Exclude FN_TO_CHAR and FN_TO_NUMBER — Format-Model Incompatibility

**ID:** exclude-fn-to-char-and-fn-to-number-format-model-incompatibility
**Plan:** `add-pushdown-capability-gaps`
**Status:** Accepted

### Context

Issue #104's cast-target audit surfaced `TO_CHAR`/`TO_NUMBER` as candidates. DataFusion 54's
`to_char` uses strftime masks, not Exasol's Oracle-style format models, and rejects numeric
formatting; DataFusion 54 has no `to_number` at all. Capability advertisement is per function, not
per argument shape, so partial support (e.g. only the no-format case) cannot be expressed safely.

### Decision

Do not advertise `FN_TO_CHAR` or `FN_TO_NUMBER`. The no-format string-to-number conversion remains
reachable through the already-advertised `FN_CAST`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Leave both unadvertised | ✓ Chosen — capability advertisement is per function; a format-argument variant the translator cannot render would otherwise be pushed |
| Advertise the no-format case only | ✗ Rejected — Exasol would still push format-argument variants the translator cannot render faithfully |

### Consequences

`TO_CHAR` and `TO_NUMBER` always evaluate in Exasol. Simple string-to-number conversion without a
format model stays reachable via `CAST`.

---

## ADR: Exclude the Regexp Scalar Functions — Rust Regex Dialect Divergence

**ID:** exclude-regexp-scalar-functions-rust-regex-dialect-divergence
**Plan:** `add-pushdown-capability-gaps`
**Status:** Accepted

### Context

Issue #106 asked whether to push only literal regex patterns or gate on a dialect check.
DataFusion 54 runs the Rust `regex` crate, which rejects the pattern backreferences and lookaround
Exasol's PCRE dialect accepts, lacks a `regexp_substr` equivalent, and its argument shapes diverge
from Exasol's position/occurrence/return-option arguments. The translator cannot compile a pattern
to detect Rust-regex incompatibility without embedding a regex engine.

### Decision

Do not advertise `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`.
The pre-existing `FN_PRED_REGEXP_LIKE` predicate advertisement is unaffected and stays out of scope.

### Options Considered

| Option | Verdict |
|--------|---------|
| Exclude all four regexp scalar functions | ✓ Chosen — neither a literal-only nor a dialect-gated push can be verified safe without embedding a regex engine |
| Advertise `REGEXP_REPLACE`/`INSTR`/`COUNT` and pre-validate patterns | ✗ Rejected — no way to detect Rust-regex incompatibility without a regex engine; an incompatible pushed pattern would fail the node-local scan instead of falling back |

### Consequences

All four regexp scalar functions always evaluate in Exasol. `FN_PRED_REGEXP_LIKE` pushdown is
unchanged.

---

## ADR: Advertise Only FN_WEEK from Issue #107 — Calendar-Semantic Divergence

**ID:** advertise-only-fn-week-calendar-semantic-divergence
**Plan:** `add-pushdown-capability-gaps`
**Status:** Accepted

### Context

Issue #107 asked whether to advertise the date-function group as a block or split by parity.
DataFusion 54 has no `add_days`/`add_months`/`last_day`/`convert_tz` builtins, its variable×INTERVAL
scaling is unverified, date-diff needs divergent multi-step emulation, month/year arithmetic needs
Oracle end-of-month clamping DataFusion lacks, and `date_part('dow')` numbers Sunday as 0. Exasol
`WEEK` and DataFusion `date_part('week')` are both documented ISO-8601.

### Decision

Advertise only `FN_WEEK` (ISO-8601, `date_part('week', …)`), gated on a year-boundary parity test.
Exclude `FN_ADD_*`, `FN_*_BETWEEN`, `FN_ADD_MONTHS`, `FN_ADD_YEARS`, `FN_MONTHS_BETWEEN`,
`FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_LAST_DAY`, and `FN_CONVERT_TZ`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Split by parity — advertise only `FN_WEEK` | ✓ Chosen — only `WEEK` holds verified ISO-8601 parity with DataFusion |
| Advertise the whole date-function group as a block | ✗ Rejected — most of the group has unverified or divergent DataFusion semantics |

### Consequences

Only `WEEK` expressions push down among the date-function group; the rest always evaluate in
Exasol. `FN_WEEK` is withdrawn if a future year-boundary case is found to diverge.

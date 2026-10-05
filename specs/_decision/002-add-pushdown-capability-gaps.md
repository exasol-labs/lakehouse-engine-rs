# Decisions: add-pushdown-capability-gaps

## ADR: Exclude FN_DIV — No Faithful DataFusion Floor Division

**ID:** exclude-fn-div-no-faithful-datafusion-floor-division
**Plan:** `add-pushdown-capability-gaps`
**Status:** Superseded by exclude-fn-div-no-faithful-datafusion-truncated-division

The stated reason was wrong: Exasol `DIV` truncates toward zero and matches DataFusion integer `/`. See `specs/_decision/016-add-fn-div-pushdown.md` for the corrected ADR.

### Context

Exasol `DIV` is floor division. DataFusion `/` truncates integer division toward zero and DataFusion has no `div` function.

### Decision

The engine does not advertise `FN_DIV`. The translator declines a `DIV` node, and Exasol evaluates `DIV` itself.

### Options Considered

| Option | Verdict |
|--------|---------|
| Emulate with `CAST(FLOOR(a / CAST(b AS DOUBLE)) AS …)` | Rejected: division by zero, negative operands, and decimal rounding are unverified against Exasol |

### Consequences

`DIV` expressions never push down.

---

## ADR: Exclude FN_TO_CHAR and FN_TO_NUMBER — Format-Model Incompatibility

**ID:** exclude-fn-to-char-and-fn-to-number-format-model-incompatibility
**Plan:** `add-pushdown-capability-gaps`
**Status:** Accepted

### Context

DataFusion `to_char` uses strftime masks, not Exasol's Oracle-style format models, and rejects numeric formatting. DataFusion has no `to_number`. Capability advertisement is per function, so partial support cannot be expressed.

### Decision

The engine does not advertise `FN_TO_CHAR` or `FN_TO_NUMBER`. String-to-number conversion without a format model stays reachable through `FN_CAST`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise the no-format case only | Rejected: Exasol would still push format-argument variants the translator cannot render |

### Consequences

`TO_CHAR` and `TO_NUMBER` always evaluate in Exasol.

---

## ADR: Exclude the Regexp Scalar Functions — Rust Regex Dialect Divergence

**ID:** exclude-regexp-scalar-functions-rust-regex-dialect-divergence
**Plan:** `add-pushdown-capability-gaps`
**Status:** Accepted

### Context

DataFusion uses the Rust `regex` crate, which rejects the backreferences and lookaround that Exasol's PCRE dialect accepts. It has no `regexp_substr` equivalent, and its argument shapes differ from Exasol's. The translator cannot detect an incompatible pattern without embedding a regex engine.

### Decision

The engine does not advertise `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`. The existing `FN_PRED_REGEXP_LIKE` advertisement is unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise `REGEXP_REPLACE`/`INSTR`/`COUNT` and pre-validate patterns | Rejected: an incompatible pushed pattern would fail the node-local scan instead of falling back |

### Consequences

All four regexp scalar functions always evaluate in Exasol.

---

## ADR: Advertise Only FN_WEEK from Issue #107 — Calendar-Semantic Divergence

**ID:** advertise-only-fn-week-calendar-semantic-divergence
**Plan:** `add-pushdown-capability-gaps`
**Status:** Accepted

### Context

DataFusion lacks `add_days`, `add_months`, `last_day`, and `convert_tz`. Its date-diff needs divergent emulation, it lacks Oracle end-of-month clamping, and `date_part('dow')` numbers Sunday as 0. Exasol `WEEK` and DataFusion `date_part('week')` are both ISO-8601.

### Decision

The engine advertises only `FN_WEEK`, gated on a year-boundary parity test. It excludes `FN_ADD_*`, `FN_*_BETWEEN`, `FN_ADD_MONTHS`, `FN_ADD_YEARS`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_LAST_DAY`, and `FN_CONVERT_TZ`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise the whole date-function group | Rejected: most of the group has unverified or divergent DataFusion semantics |

### Consequences

`FN_WEEK` is withdrawn if a year-boundary case diverges.

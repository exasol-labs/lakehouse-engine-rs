# Decision Log: add-bitwise-function-pushdown

## Interview

Headless plan (`speq-plan-pr`); no live interview occurred. The questions resolved below are the
"Open questions" from issue #108, answered by verifying against the pinned dependency versions and
the Exasol and Iceberg specifications.

**Q:** Exasol bit functions operate on a 64-bit unsigned integer domain; do the DataFusion
expressions match that width and unsigned semantics, and how do out-of-range/overflow and
shift/rotate counts ≥ 64 behave?
**A:** No, they do not match. Exasol bit functions operate on unsigned 64-bit integers (range
`0`–`18446744073709551615`, result `DECIMAL(20,0)`; confirmed against docs.exasol.com). DataFusion
54.0.0's bitwise operators (`&`/`|`/`#`/`<<`/`>>`) act on the operand's signed Arrow integer type,
and Iceberg sources carry only signed integers (`int` = 32-bit signed, `long` = 64-bit signed; the
Iceberg spec Primitive Types table defines no unsigned primitive). A bit-63-set result is a large
positive value in Exasol but a negative value under signed `Int64`, which the `Int64` →
`DECIMAL(20,0)` mapping then carries. `BIT_RSHIFT` diverges unconditionally on any bit-63-set
operand because DataFusion's signed `>>` is arithmetic (sign-extending) versus Exasol's logical
(zero-fill). Exasol constrains shift/rotate counts to `0`–`63`; Rust/Arrow shift by ≥ bit width is
masked or undefined, so the counts do not correspond.

**Q:** This is the lowest-priority item in the pushdown-gap set — does that change the bar?
**A:** No. Low priority calibrates scope, not rigor. The backing-path and semantic-parity bar
applies unchanged; a capability is advertised only when its DataFusion result matches Exasol.

**Q:** Does any subset of the eleven functions have a faithful DataFusion translation?
**A:** No. The five operator-backed functions (`BIT_AND`/`BIT_OR`/`BIT_XOR`/`BIT_LSHIFT`/
`BIT_RSHIFT`) fail on the unsigned-domain divergence above, which the type/value-blind translator
cannot fence off. The other six have no DataFusion builtin at all: the SQL planner
(`parse_sql_unary_op`) rejects unary `~` (blocking `BIT_NOT`), and `datafusion-functions` 54.0.0
registers no rotate / bit-test / bit-set / bits-to-number scalar function (blocking `BIT_LROTATE`,
`BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, `BIT_TO_NUM`).

## Design Decisions

### [1] Decline all eleven `FN_BIT_*` bitwise operator functions

- **Decision:** Keep all eleven unadvertised at pinned DataFusion 54.0.0; the adapter falls back to
  row scanning and Exasol evaluates each bitwise function. Record the decline as a cited tracked
  exception (issue #108) in both governing specs.
- **Alternatives:** Advertise the operator-backed subset (`BIT_AND`/`BIT_OR`/`BIT_XOR`/shifts) and
  decline the rest. Rejected — the subset diverges on the unsigned-64-bit domain (sign-bit result
  interpretation; `BIT_RSHIFT` arithmetic-vs-logical shift) and the translator cannot restrict to
  the safe non-negative, bit-63-clear operand subset because operand types and values are not
  carried in the expression node, the same limitation the recorded `DIV` decline cites.
- **Rationale:** The backing-path bar requires the DataFusion result to match Exasol's semantics.
  No faithful, unconditional rendering exists for any of the eleven at the pinned version.
- **Supersedes:** none.
- **Promotes to ADR:** yes

### [2] Author an exclusion plan rather than escalate the all-declined outcome

- **Decision:** Treat the all-declined outcome as a valued, precedented deliverable and author the
  exclusion plan, rather than escalating via `OPEN QUESTIONS:`.
- **Alternatives:** Escalate the all-declined finding to a human as a candidate no-op plan
  (the headless escalation clause permits this when no faithful translation exists for the whole
  set). Rejected — the repo has a direct, merged precedent (`039-add-fn-regexp-pushdown`, issue
  #106, PR #167) of authoring exactly this "document as permanently out of scope" plan for a whole
  function family, which the planning brief handed over as the model. The decline is grounded in
  concrete DataFusion 54.0.0 source evidence, and CLAUDE.md requires a known gap to be a cited,
  tracked exception locked by regression tests — not a silent omission — so the plan is not a no-op.
- **Rationale:** Escalation adds no information a human could resolve differently; the evidence is
  definitive and the deliverable pattern is established.
- **Promotes to ADR:** no

### [3] Place the translator decline scenario in `vs-expression-translator-scalar-ops`

- **Decision:** Record the translator decline in `sql-comprehension/vs-expression-translator-scalar-ops`,
  alongside the arithmetic-operator arms and the `DIV`/`TO_CHAR`/`TO_NUMBER` declines.
- **Alternatives:** Place it in `sql-comprehension/vs-expression-translator-scalar-fns` (where the
  regexp decline lives). Rejected — bitwise AND/OR/XOR/NOT/shift are operators, the same family as
  arithmetic operators, whose declines already live in scalar-ops; grouping the bitwise decline
  there keeps the operator-family declines co-located.
- **Rationale:** Family coherence with the closest analogous recorded decline (`DIV`).
- **Promotes to ADR:** no

### [4] No production decline arm; the eleven names fall through

- **Decision:** Add no dedicated match arm for the `BIT_*` names; they fall through to the existing
  unsupported-`function_scalar` path. Lock the behavior with tests only.
- **Alternatives:** Add an explicit decline arm naming each function. Rejected — it duplicates the
  generic unsupported-node path with no behavioral gain, matching how the regexp decline is handled.
- **Rationale:** Least code; the tests pin the decline and the specs record the rationale.
- **Promotes to ADR:** no

### [5] The Iceberg-compliance gate is satisfied by naming the source-type trade-off

- **Decision:** Record the Iceberg signed-integer domain (`int`/`long` are signed; no unsigned
  primitive) as the named source-type driver of the decline, quoting the spec Primitive Types table.
- **Alternatives:** Omit the Iceberg citation. Rejected — CLAUDE.md requires any scanning/pushdown/
  type-handling plan to be checked against the Iceberg spec with the normative section quoted, and a
  source-type-driven trade-off named rather than left unstated.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated in Revision Mode after plan-reviewer blockers, and by speq-implement after code review. -->

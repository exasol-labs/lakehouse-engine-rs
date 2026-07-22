# Decisions: add-bitwise-function-pushdown

## ADR: Decline All Eleven FN_BIT_* Bitwise Operator Functions

**ID:** decline-bitwise-operator-functions-unsigned-domain-divergence
**Plan:** `add-bitwise-function-pushdown`
**Status:** Accepted

### Context

Issue #108 asked whether any of the eleven Exasol `FN_BIT_*` bitwise operator functions
(`BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `BIT_LSHIFT`, `BIT_RSHIFT`, `BIT_LROTATE`,
`BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, `BIT_TO_NUM`) has a faithful DataFusion translation at the
pinned DataFusion 54.0.0. Exasol defines these functions over unsigned 64-bit integers (range
`0`-`18446744073709551615`, result `DECIMAL(20,0)`). DataFusion's `&`/`|`/`#`/`<<`/`>>` operators
act on the operand's signed Arrow integer type, and Iceberg sources carry only signed `int`
(32-bit) and `long` (64-bit) — the Iceberg spec Primitive Types table defines no unsigned integer
primitive. A bit-63-set result is therefore a large positive value in Exasol but negative under
signed `Int64`, and the `Int64` → `DECIMAL(20,0)` mapping carries that negative value. `BIT_RSHIFT`
diverges unconditionally on any bit-63-set operand because DataFusion's signed `>>` is arithmetic
(sign-extending) versus Exasol's logical (zero-fill). The remaining six functions have no
DataFusion builtin at all: the SQL planner rejects unary `~` (blocking `BIT_NOT`), and
`datafusion-functions` 54.0.0 registers no rotate, bit-test, bit-set, or bits-to-number scalar
function (blocking `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, `BIT_TO_NUM`).

### Decision

Keep all eleven `FN_BIT_*` capabilities unadvertised at pinned DataFusion 54.0.0. The adapter falls
back to row scanning and Exasol evaluates each bitwise function. The decline is recorded as a
cited, tracked exception (issue #108) in both `vs-adapter/pushdown-planning-capability-extensions`
and `sql-comprehension/vs-expression-translator-scalar-ops`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline all eleven; document and cite #108 | ✓ Chosen — no faithful, unconditional rendering exists for any of the eleven at the pinned version |
| Advertise the operator-backed subset (`BIT_AND`/`BIT_OR`/`BIT_XOR`/shifts) and decline the rest | ✗ Rejected — the subset diverges on the unsigned-64-bit domain (sign-bit result interpretation; `BIT_RSHIFT` arithmetic-vs-logical shift), and the translator cannot restrict to the safe non-negative, bit-63-clear operand subset because operand types and values are not carried in the expression node — the same limitation the recorded `DIV` decline cites |

### Consequences

All eleven bitwise operator functions continue to evaluate in Exasol. The decline carries a
concrete DataFusion 54.0.0 source-level evidence trail and an inline citation to issue #108 in both
governing specs, so the gap reads as investigated-and-declined rather than a silent omission.
`FN_BIT_LENGTH` (a string function, not a bitwise operator) and the existing join capability set
are unaffected.

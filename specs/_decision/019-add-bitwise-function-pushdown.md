# Decisions: add-bitwise-function-pushdown

## ADR: Decline All Eleven FN_BIT_* Bitwise Operator Functions

**ID:** decline-bitwise-operator-functions-unsigned-domain-divergence
**Plan:** `add-bitwise-function-pushdown`
**Status:** Accepted

### Context

Exasol defines the `FN_BIT_*` functions over unsigned 64-bit integers, while DataFusion operators act on signed Arrow integers and Iceberg has no unsigned integer type. A bit-63-set result is positive in Exasol and negative in DataFusion, and `BIT_RSHIFT` sign-extends in DataFusion where Exasol zero-fills. DataFusion has no builtin for `BIT_NOT`, the rotates, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`.

### Decision

All eleven `FN_BIT_*` capabilities stay unadvertised, and Exasol evaluates them. The decline is recorded as a cited exception (issue #108) in both governing specs.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise the operator-backed subset (AND, OR, XOR, shifts) | Rejected: the subset diverges on the unsigned domain, and the translator cannot restrict to safe operands because the expression node carries no operand types or values |

### Consequences

`FN_BIT_LENGTH` and the join capability set are unaffected.

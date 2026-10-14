# Decisions: fix-exasol-numeric-typing

## ADR: Scan sessions apply Exasol's numeric typing before DataFusion's type coercion

**ID:** scan-applies-exasol-numeric-typing
**Plan:** fix-exasol-numeric-typing
**Status:** Accepted

### Context

DataFusion combines DOUBLE with DECIMAL in DECIMAL, and its integer arithmetic and `SUM` wrap around on overflow. Exasol combines them in DOUBLE and raises an error on overflow, and a pushed expression must return native Exasol's result.

### Decision

One constructor builds every scan session, and any planning that derives the scan's result types uses it too. The session runs a rewrite rule before DataFusion's type coercion. The rule converts DECIMAL to DOUBLE where the two meet, and it routes exact `+`, `-` and `*` through registered 128-bit functions that check for overflow only when the operand precisions allow one.

### Options Considered

| Option | Verdict |
|--------|---------|
| Cast exact operands to DECIMAL, in the rule or in the rendered SQL | Rejected: Arrow's integer-to-decimal cast validates every value, which makes a Parquet scan 1.7 to 3 times slower |
| Keep DataFusion's typing and document the differences | Rejected: the scan returns wrong results without an error |

### Consequences

- A DataFusion upgrade that changes the analyzer or its type coercion can break the rule. The native-parity E2E test detects it.
- An exact expression costs up to 1.5 times its previous scan time on Parquet. Most shapes cost 1.0 to 1.25 times.

# Decisions: fix-decimal-precision-scale-guard

## ADR: The Iceberg spec does NOT constrain p and s the way issue #329 claims

**ID:** iceberg-spec-does-not-constrain-decimal-precision-scale
**Plan:** fix-decimal-precision-scale-guard
**Status:** Accepted

### Context

A guard rejects catalog-declared decimals with precision 0 or scale above precision. The Iceberg spec requires only precision of 38 or less, and the Iceberg library checks neither bound, so `decimal(0, 0)` and `decimal(5, 10)` deserialize cleanly.

### Decision

The guard rests solely on the Exasol target-type limitation. The type-mapping spec records that the Iceberg spec permits both pairs.

### Options Considered

| Option | Verdict |
|--------|---------|
| Argue the inputs are out of spec, as in issue #329 | Rejected: the spec does not forbid them |

### Consequences

- A spec-compliant catalog can serve either pair, so the guard is reachable.

# Decisions: fix-decimal-precision-scale-guard

## ADR: The Iceberg spec does NOT constrain p and s the way issue #329 claims

**ID:** iceberg-spec-does-not-constrain-decimal-precision-scale
**Plan:** fix-decimal-precision-scale-guard
**Status:** Accepted

### Context

Issue #329 proposed guarding catalog-declared decimals against precision 0 and scale above precision, on the premise that the Iceberg spec forbids both. The spec only requires precision of 38 or less. The Iceberg library does not check either bound, so `decimal(0, 0)` and `decimal(5, 10)` deserialize cleanly.

### Decision

The guard is justified solely by the Exasol target-type limitation. The type-mapping spec records that the Iceberg spec permits both bad pairs.

### Options Considered

| Option | Verdict |
|--------|---------|
| Argue the inputs are out of spec, as in issue #329 | Rejected: the spec text does not say that |

### Consequences

A spec-compliant catalog can legally serve either pair, so "only a misbehaving catalog produces it" is not a valid reachability argument. This strengthens the case for the guard.

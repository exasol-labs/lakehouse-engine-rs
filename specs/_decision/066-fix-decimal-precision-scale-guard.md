# Decisions: fix-decimal-precision-scale-guard

## ADR: The Iceberg spec does NOT constrain p and s the way issue #329 claims

**ID:** iceberg-spec-does-not-constrain-decimal-precision-scale
**Plan:** fix-decimal-precision-scale-guard
**Status:** Accepted

### Context

Issue #329 proposed guarding a catalog-declared decimal against `p = 0` and `s > p` on the premise that "the Iceberg spec constrains p and s the same way" — treating both bad pairs as evidence of a misbehaving catalog. The Apache Iceberg table spec's Primitive Types table gives `decimal(P,S)` exactly one constraint: "Fixed-point decimal; precision P, scale S" · "Scale is fixed, precision must be 38 or less." It states no lower bound on `P` and no relation between `S` and `P`. Verified in the vendored source, `iceberg 0.10.0`'s `deserialize_decimal` bypasses `Type::decimal`'s own bound assertions and builds `PrimitiveType::Decimal { precision, scale }` from two unchecked `u32` parses, so `decimal(0, 0)` and `decimal(5, 10)` deserialize cleanly from table metadata.

### Decision

Justify the guard solely by the Exasol target-type limitation, and record in the `datafusion-scan/type-mapping` delta that the Iceberg spec permits both bad pairs.

### Options Considered

| Option | Verdict |
|--------|---------|
| Justify the guard by the Exasol target-type limitation alone | ✓ Chosen — the spec's own normative text does not forbid either pair, so the guard's only sound basis is what Exasol itself can represent |
| Carry issue #329's premise forward — argue the inputs are out-of-spec | ✗ Rejected — the spec text does not say that; asserting it would misstate the Primitive Types table's actual constraint |

### Consequences

"Only a misbehaving catalog produces it" is not a sound reachability argument for either bad pair — a spec-compliant catalog may legally serve `decimal(0,0)` or `decimal(5,10)`. This strengthens the case for the fix rather than weakening it, and it stops a future reader from re-deriving the wrong reason for the guard.

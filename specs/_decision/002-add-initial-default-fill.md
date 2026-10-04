# Decisions: add-initial-default-fill

## ADR: Implement Iceberg Column-Projection Rule 3 for All Absent Fields

**ID:** implement-iceberg-column-projection-rule-3-for-all-absent-fields
**Plan:** `add-initial-default-fill`
**Status:** Accepted

### Context

The scan UDF NULL-filled every absent column and ignored the Iceberg `initial-default`. The Iceberg table spec returns the `initial-default` for any absent field, required or nullable.

### Decision

The scan returns the defined `initial-default` for an absent field, whether the field is required or nullable. A nullable field with no default is NULL-filled, and a required field with no default fails with a clear error. `write-default` is never used on reads, because it governs writer-side backfill.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fix only the required-column case named in #27 | Rejected: leaves a known deviation for nullable columns with a default |

### Consequences

The scenario "nullable absent column is NULL-filled" applies only when the column defines no default.

---

## ADR: Encode Only Primitive initial-default Values

**ID:** encode-only-primitive-initial-default-values
**Plan:** `add-initial-default-fill`
**Status:** Accepted

### Context

The logical schema's Arrow-type tag vocabulary is primitive-only. Exasol has no struct, list, or map types, so those columns surface only as JSON-fallback VARCHAR.

### Decision

The engine encodes and applies only a primitive-typed `initial-default`. A struct, list, or map `initial-default` is not represented, and that column is NULL-filled (nullable) or fails with the required-absent error.

### Options Considered

| Option | Verdict |
|--------|---------|
| Encode complex-typed defaults | Rejected: the logical-schema carrier has no complex-type representation |

### Consequences

The feature spec names this gap. It is an Exasol target-type limitation, so no tracked exception is needed.

---

## ADR: Rule 3 Resolves Before the Unimplemented Rule 1

**ID:** rule-3-resolves-before-the-unimplemented-rule-1
**Plan:** `add-initial-default-fill`
**Status:** Accepted

### Context

The Iceberg spec resolves an absent field in order: identity-transform partition value (rule 1), name-mapping, `initial-default` (rule 3), null. The engine does not implement rule 1.

### Decision

When both an identity-transform partition value and an `initial-default` could resolve the same absent field, the engine returns the `initial-default`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Implement rule 1 first to match the spec ordering | Rejected: out of scope, rule 1 is unimplemented engine-wide |

### Consequences

The ordering deviation is recorded in the feature spec. No tracked exception is needed.

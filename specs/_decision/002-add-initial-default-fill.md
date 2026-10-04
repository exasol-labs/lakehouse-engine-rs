# Decisions: add-initial-default-fill

## ADR: Implement Iceberg Column-Projection Rule 3 for All Absent Fields

**ID:** implement-iceberg-column-projection-rule-3-for-all-absent-fields
**Plan:** `add-initial-default-fill`
**Status:** Accepted

### Context

The scan UDF NULL-filled every absent column and returned a clean error only for a
required absent column, ignoring the Iceberg `initial-default`. The Iceberg table spec
applies column-projection rule (3) — return the defined `initial-default` — to ANY absent
field, required or nullable, not only required ones.

### Decision

An absent field-id with a defined primitive `initial-default` returns that default for
pre-existing rows, whether the field is required or nullable. A nullable field with no
default still NULL-fills; a required field with no default still errors cleanly.
`write-default` is never consulted — it governs writer-side backfill, not reads.

### Options Considered

| Option | Verdict |
|--------|---------|
| Full rule-3 compliance for any absent field | ✓ Chosen — closes both the required-column gap (#27) and the latent nullable-with-default deviation in one pass |
| Narrow the fix to only the required-column case named in #27 | ✗ Rejected — would leave a known silent deviation for nullable-with-default columns |

### Consequences

One implementation pass closes two deviations instead of one. The shipped "nullable
absent column is NULL-filled" scenario is refined to apply only when the column defines
no default.

---

## ADR: Encode Only Primitive initial-default Values

**ID:** encode-only-primitive-initial-default-values
**Plan:** `add-initial-default-fill`
**Status:** Accepted

### Context

The logical schema's Arrow-type tag vocabulary is primitive-only (bool, int32, int64,
float32, float64, utf8, date32, timestamp/timestamptz, decimal128). Iceberg permits
`initial-default` on Struct, List, and Map fields too, but Exasol has no struct, list, or
map types — those columns already surface only as JSON-fallback VARCHAR.

### Decision

Only a primitive-typed `initial-default` is encoded and applied. A Struct / List / Map
`initial-default` is not represented; that column falls through to NULL (nullable) or the
required-absent error.

### Options Considered

| Option | Verdict |
|--------|---------|
| Encode only primitive-typed defaults | ✓ Chosen — matches the existing primitive-only Arrow-tag vocabulary; the gap is driven by an Exasol target-type limitation, not a silent omission |
| Encode complex-typed (struct/list/map) defaults too | ✗ Rejected — the logical-schema carrier has no complex-type representation; the Iceberg spec itself requires `unknown`/`variant`/`geometry`/`geography` to default to null |

### Consequences

The trade-off is named explicitly in the feature spec rather than left as a silent gap.
No new tracked exception is required — it is an Exasol target-type limitation, not a
deviation to fix.

---

## ADR: Rule 3 Resolves Before the Unimplemented Rule 1

**ID:** rule-3-resolves-before-the-unimplemented-rule-1
**Plan:** `add-initial-default-fill`
**Status:** Accepted

### Context

The Iceberg spec orders column-projection resolution for an absent field: (1)
Identity-Transform partition value, then (2) name-mapping, then (3) `initial-default`,
then (4) null. Rule (1) is unimplemented anywhere in this engine and remains out of
scope.

### Decision

When both an Identity-Transform partition value (rule 1) and an `initial-default` (rule
3) could resolve the same absent field-id, this engine returns the `initial-default`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Return the `initial-default` (rule 3) given rule 1 is unimplemented | ✓ Chosen — for an added column read from older files, `initial-default` is the correct and only-available value |
| Implement rule (1) first to match the spec's stated ordering | ✗ Rejected — out of scope for this plan; rule (1) is unimplemented engine-wide |

### Consequences

The ordering deviation is recorded as a deliberate, accurately-scoped trade-off in the
feature spec rather than a silent gap. No new tracked exception is required.

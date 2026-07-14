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

## ADR: Read initial-default Once Per Query in the VS Layer

**ID:** read-initial-default-once-per-query-in-the-vs-layer
**Plan:** `add-initial-default-fill`
**Status:** Accepted

### Context

The engine resolves Iceberg metadata once per query in the VS planning layer, never per
UDF invocation, so the UDF never re-reads Iceberg table state. `initial-default` values
must reach the scan UDF without breaking that boundary or existing scan-spec
compatibility.

### Decision

`build_logical_schema` — the one VS-layer site that reads the Iceberg current schema —
reads each field's `NestedField.initial_default` and encodes a primitive default onto a
new optional `LogicalField.initial_default` field, threaded unchanged through the scan
spec to the scan UDF.

### Options Considered

| Option | Verdict |
|--------|---------|
| Encode the default onto an optional `LogicalField` field in the VS layer | ✓ Chosen — preserves the resolve-once-per-query rule; an optional field keeps default-less specs deserializing unchanged, mirroring how name-mapping was added |
| Read Iceberg metadata inside the UDF | ✗ Rejected — violates the resolve-metadata-once-per-query architecture rule |
| Add a separate side-channel argument to carry the default | ✗ Rejected — unnecessary; the existing `LogicalField` carrier already threads per-field metadata |

### Consequences

Scan specs written before this feature deserialize unchanged. The scan UDF gains
`initial-default` fill with no new Iceberg-metadata read path and no per-invocation
catalog access.

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

## ADR: Intercept the Absent-Field Case Before Delegating to DefaultPhysicalExprAdapter

**ID:** intercept-absent-field-before-delegating-to-defaultphysicalexpradapter
**Plan:** `add-initial-default-fill`
**Status:** Accepted

### Context

`FieldIdExprAdapter` delegates null-fill and required-missing handling to
`DefaultPhysicalExprAdapter`. That delegate errors immediately on a required-absent
field, so any post-processing step meant to substitute a default would never run.

### Decision

`FieldIdExprAdapter` computes the per-file set of absent logical field-ids and
substitutes `Literal(default)` for an absent-with-default field BEFORE delegating. Other
cases (nullable-no-default, required-no-default) delegate unchanged. The decision is made
per file, since the adapter is created per file and the same field can be absent in one
file and present in another within one shard.

### Options Considered

| Option | Verdict |
|--------|---------|
| Substitute the default before delegation | ✓ Chosen — the only point at which a required-absent field has not yet errored |
| Post-process the delegated expression to swap NULL literals for defaults | ✗ Rejected — `DefaultPhysicalExprAdapter` errors on a required-absent field before any post-processing could run |

### Consequences

The fill logic lives entirely in `FieldIdExprAdapter`, ahead of the existing delegate,
with no change to `DefaultPhysicalExprAdapter` itself.

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

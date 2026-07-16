# Decisions: fix-timestamptz-mapping

## ADR: Collapse only the Exasol-facing type string; keep the internal timezone-aware Arrow type

**ID:** collapse-exasol-facing-timestamp-string-keep-internal-tz-aware-arrow-type
**Plan:** `fix-timestamptz-mapping`
**Status:** Accepted

### Context

Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` output type
(`sqlCode 22002: Column type not supported`), so any query touching an Iceberg
`timestamptz` column failed to compile the scan script. The internal Arrow representation
registers a timestamptz column as the timezone-aware `Timestamp(_, Some("UTC"))`, and that
timezone label threads through the logical-schema tag round-trip
(`arrow_type_to_tag` / `arrow_type_from_tag`) and `reconstruct_initial_default` (which
builds `ScalarValue::TimestampMicrosecond(_, tz)` from the tag).

### Decision

Change the timestamptz mapping to plain `TIMESTAMP` only in the Exasol-facing type-string
resolvers (`arrow_to_exasol_type`, `iceberg_primitive_to_exasol`). Keep the internal Arrow
representation `Timestamp(_, Some("UTC"))` unchanged in `iceberg_primitive_to_arrow`, the
tag round-trip, and `reconstruct_initial_default`. The emit-boundary coercion already casts
a column declared `EMITS "TIMESTAMP"` to `Timestamp(_, None)`, preserving the UTC-instant
value.

### Options Considered

| Option | Verdict |
|--------|---------|
| Collapse only the Exasol-facing type string; keep the internal tz-aware Arrow type | ✓ Chosen — confines the change to two resolver arms and preserves DataFusion's timezone-correct timestamp semantics |
| Drop the internal timezone label to `Timestamp(_, None)` so registered column, tags, and emit target all agree with no cast | ✗ Rejected — the timezone label threads through the tag round-trip and `reconstruct_initial_default`; dropping it forces matching changes at every site and risks a mismatch inside DataFusion between a timezone-aware predicate literal and a now-timezone-naive column |

### Consequences

The declared Exasol column type no longer distinguishes `timestamptz` from `timestamp` at
the SQL surface, but no emitted value changes — the physical UTC-instant is identical.
DataFusion's internal timestamp comparisons, date-function evaluation, and predicate
binding stay timezone-correct because the internal Arrow representation is untouched.

## ADR: Retire the mapping at its source; keep the generic WLTZ codec branches

**ID:** retire-timestamptz-mapping-at-source-keep-generic-wltz-codec-branches
**Plan:** `fix-timestamptz-mapping`
**Status:** Accepted

### Context

Beyond the Iceberg-column mapping, the codebase carries generic `TIMESTAMP WITH LOCAL TIME
ZONE` (WLTZ) codec/translator branches in `exasol_type_to_json`, `exasol_type_from_json`,
the `exasol_type_to_arrow` WLTZ arm, and the `vs-expression` CAST-target rejection.
`exasol_type_from_json` is called on expression and select-list dataTypes
(`adapter/pushdown.rs` lines 3556, 4279, 5658), not only on VS-declared columns.

### Decision

Fix the mapping at its source (`iceberg_primitive_to_exasol`, `arrow_to_exasol_type`).
Keep the WLTZ branches in `exasol_type_to_json`, `exasol_type_from_json`, the
`exasol_type_to_arrow` WLTZ arm, and the `vs-expression` CAST-target rejection unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Retire the mapping at its source; keep the generic WLTZ codec branches | ✓ Chosen — the bug was the Iceberg-column mapping, not the codec; the codec must stay correct for a WLTZ type the user introduces in a CAST/expression |
| Delete every WLTZ branch as part of "full retirement" | ✗ Rejected — `exasol_type_from_json` is reached by a user `CAST(... AS TIMESTAMP WITH LOCAL TIME ZONE)`; deleting the branch would mis-read a genuine WLTZ type as plain `TIMESTAMP` |

### Consequences

A user-introduced WLTZ CAST or expression continues to translate correctly. The
Iceberg-timestamptz-derived path no longer feeds any WLTZ string into these branches, so
`exasol_type_to_json`'s WLTZ arm becomes defensive-only (no live caller from the Iceberg
column path) but is retained deliberately for codec symmetry.

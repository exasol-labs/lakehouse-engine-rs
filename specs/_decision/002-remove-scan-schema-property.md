# Decisions: remove-scan-schema-property

## ADR: Derive the Qualifying Schema from `ctx.script_schema()`, Not a VS Property

**ID:** derive-qualifying-schema-from-script-schema
**Plan:** remove-scan-schema-property
**Status:** Accepted

### Context

The adapter qualifies the scan, distribute-files, and distinct-merge UDF names with a schema. The `SCAN_SCHEMA` property supplied it, but the UDF handshake already reports that schema, and the property can drift from it.

### Decision

The adapter reads the qualifying schema from the UDF handshake and the `SCAN_SCHEMA` property is deleted.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep `SCAN_SCHEMA` | Rejected: redundant with the handshake and can drift |
| Add a new property with different semantics | Rejected: same redundancy |

### Consequences

A leftover `SCAN_SCHEMA` in an existing `CREATE VIRTUAL SCHEMA` is ignored like any unknown property. No back-compat shim is added.

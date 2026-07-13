# Decisions: remove-scan-schema-property

## ADR: Derive the Qualifying Schema from `ctx.script_schema()`, Not a VS Property

**ID:** derive-qualifying-schema-from-script-schema
**Plan:** remove-scan-schema-property
**Status:** Accepted

### Context

The adapter qualifies the `LAKEHOUSE_SCAN`, `LAKEHOUSE_DISTRIBUTE_FILES`, and distinct-merge UDF names so the pushdown SQL resolves them outside the adapter script's schema context. The `SCAN_SCHEMA` VS property supplied this schema, but it duplicates information `ctx.script_schema()` already reports from the UDF handshake, since the scan, distributor, and distinct-merge scripts are co-deployed in the adapter script's schema. The property could drift from the handshake's real value.

### Decision

Read the qualifying schema from `ctx.script_schema()`, captured synchronously in the `dispatch` pushdown arm and threaded into `handle_pushdown_request`. Delete the `SCAN_SCHEMA` property and its read.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive the schema from `ctx.script_schema()` | ✓ Chosen — single authoritative source; removes a redundant, drift-prone configuration knob |
| Keep the `SCAN_SCHEMA` property | ✗ Rejected — redundant with the handshake and can drift from the script's real schema |
| Introduce a new property with different semantics | ✗ Rejected — same redundancy as keeping `SCAN_SCHEMA` |

### Consequences

The `SCAN_SCHEMA` property and its constant are removed from the adapter. A leftover `SCAN_SCHEMA` in an existing `CREATE VIRTUAL SCHEMA` DDL becomes a silently-ignored unknown property, consistent with how every other unknown property is handled. No back-compat shim is added.

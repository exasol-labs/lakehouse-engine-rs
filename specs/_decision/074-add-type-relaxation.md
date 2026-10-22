# Decisions: add-type-relaxation

## ADR: Iceberg `date` → `timestamp` / `timestamp_ns` is refused at plan time from the schema history

**ID:** refuse-iceberg-date-promotion-from-schema-history
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

The Iceberg library reads `timestamp` and `timestamp_ns` manifest bounds as 8 bytes, so a pre-promotion 4-byte `date` bound fails manifest decode, even for an unfiltered `SELECT *`. A second bounds decode can panic, which makes the engine kill every sibling VM.

### Decision

The planner refuses the two Iceberg `date` promotions to `timestamp` and `timestamp_ns` before any manifest is loaded, using the table's schema history. The error names the table, column, and both types. Delta's `date` to `timestampNtz` stays supported, because Delta stats are typed JSON while Iceberg bounds are untyped byte buffers.

### Options Considered

| Option | Verdict |
|--------|---------|
| Vendor the missing bounds-width inference | Rejected: forks a dependency for two promotion pairs |
| Catch and re-word the manifest decode error | Rejected: runs downstream of the panic path and depends on an error string |
| Leave the opaque failure | Rejected: it names neither column nor promotion |

### Consequences

- The gate refuses on the recorded promotion alone, because checking whether a pre-promotion file survives needs the manifest read that fails.


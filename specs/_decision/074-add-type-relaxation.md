# Decisions: add-type-relaxation

## ADR: Iceberg `date` → `timestamp` / `timestamp_ns` is refused at plan time from the schema history

**ID:** refuse-iceberg-date-promotion-from-schema-history
**Plan:** `add-type-relaxation`
**Status:** Accepted

### Context

The Iceberg library reads `timestamp` and `timestamp_ns` manifest bounds as 8 bytes, so a pre-promotion 4-byte `date` bound fails during manifest decode. This happens even for an unfiltered `SELECT *`, and a second bounds decode in the same crate can panic, which makes the engine SIGKILL every sibling VM of the statement part.

### Decision

The planner refuses the two Iceberg `date` promotions before any manifest is loaded, using the table's schema history, with an error naming the table, column, both types, and a tracked issue. Delta's `date` to `timestampNtz` stays supported, because the difference lies in the metadata format: typed JSON stats for Delta, untyped Avro byte buffers for Iceberg.

### Options Considered

| Option | Verdict |
|--------|---------|
| Vendor the missing bounds-width inference | Rejected: a dependency fork for two promotion pairs |
| Catch and re-word the manifest decode error | Rejected: sits downstream of the panic path and depends on an error string |
| Leave the opaque failure | Rejected: it names neither column nor promotion |

### Consequences

The gate refuses on the recorded promotion alone, without checking whether a pre-promotion file survives, because that check needs the manifest read that fails.


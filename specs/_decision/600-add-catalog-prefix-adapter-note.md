# Decisions: add-catalog-prefix-adapter-note

## ADR: The Iceberg REST catalog prefix is recorded in AdapterNotes

**ID:** iceberg-rest-prefix-in-adapter-notes
**Plan:** add-catalog-prefix-adapter-note
**Status:** Accepted

### Context

Every pushdown statement on an Iceberg REST virtual schema spent one sequential catalog round trip on `/v1/config`, only to read its `prefix`, which changes only when the CONNECTION names another catalog or warehouse.

### Decision

createVirtualSchema, refresh, and setProperties record the prefix their catalog session used as the schema-level note `CATALOG_PREFIX`. A pushdown resolves its session with that prefix and sends no `/v1/config` request.

### Options Considered

| Option | Verdict |
|--------|---------|
| Look the prefix up once per statement | Rejected: one catalog round trip on every query |
| Cache the prefix in the adapter across statements | Rejected: the engine keeps no state across statements |

### Consequences

- After an `ALTER CONNECTION` to another catalog or warehouse, the user runs `ALTER VIRTUAL SCHEMA ... REFRESH`.

# Decisions: add-vs-refresh

## ADR: Refresh and setProperties reuse the createVirtualSchema enumeration

**ID:** vs-refresh-reuses-create-virtual-schema-enumeration
**Plan:** `add-vs-refresh`
**Status:** Accepted

### Context

Re-reading the catalog required `DROP ... CASCADE` and `CREATE`, which destroys dependent views and grants. The adapter is stateless, with no caching or metadata persistence.

### Decision

`refresh` and `setProperties` run the existing create-virtual-schema enumeration. It re-enumerates the full namespace, rebuilds `TABLE_MAP` from scratch, and preserves unrelated `adapterNotes` entries.

### Options Considered

| Option | Verdict |
|--------|---------|
| Separate refresh path that diffs the prior `TABLE_MAP` against the catalog | Rejected: needs cross-request state, which the stateless architecture forbids |

### Consequences

Refresh pays the full-namespace enumeration cost even for a single-table `REFRESH TABLES <t>`.

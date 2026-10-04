# Decisions: add-vs-refresh

## ADR: Refresh and setProperties reuse the createVirtualSchema enumeration

**ID:** vs-refresh-reuses-create-virtual-schema-enumeration
**Plan:** `add-vs-refresh`
**Status:** Accepted

### Context

The only supported way to re-read the catalog was `DROP ... CASCADE` + `CREATE`, which
destroys dependent views and grants. The adapter is stateless (mission.md; CLAUDE.md
"Architecture boundaries" — no caching, no metadata persistence).

### Decision

Route `refresh` and `setProperties` through the existing `handle_create_virtual_schema`
enumeration: full namespace re-enumeration, `TABLE_MAP` rebuilt from scratch, unrelated
`adapterNotes` entries preserved.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reuse `handle_create_virtual_schema`'s full enumeration | ✓ Chosen — refresh is "re-run create", matching the DROP+CREATE workaround minus the destruction; no reinvented listing/mapping code |
| A separate refresh path that diffs prior `TABLE_MAP` against the catalog | ✗ Rejected — diffing introduces cross-request state the stateless architecture forbids |

### Consequences

Refresh and setProperties get correctness for free from the already-verified
`createVirtualSchema` path, at the cost of always paying full-namespace enumeration cost even
for a single-table `REFRESH TABLES <t>`.


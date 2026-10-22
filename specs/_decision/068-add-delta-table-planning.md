# Decisions: add-delta-table-planning

## ADR: `FormatReader` lives in the engine and each implementation owns its whole resolution

**ID:** format-reader-engine-owns-whole-resolution
**Plan:** `add-delta-table-planning`
**Status:** Accepted

### Context

Delta is a second table format that must resolve to the engine's scan spec. The catalog crate must not depend on format or query-engine libraries, and the two formats need different metadata to reach their file lists.

### Decision

A per-format reader in the engine crate resolves a table's whole scan in one call: catalog request, storage credential, and file discovery. The catalog client gains no method.

### Options Considered

| Option | Verdict |
|--------|---------|
| File-planning method on `CatalogClient` | Rejected: the catalog crate may not depend on those libraries |
| Shared caller pre-fetches metadata and readers only plan files | Rejected: the pre-fetch would itself fork per format |

### Consequences

- A caller learns nothing about catalog protocol, credential vending, or the file source (Iceberg manifests or Delta commit log).

## ADR: `CatalogTable` gains a neutral format tag and an opaque vending key

**ID:** catalog-table-neutral-format-tag-opaque-vending-key
**Plan:** `add-delta-table-planning`
**Status:** Accepted

### Context

The Delta reader needs the table format and the vending key from a catalog table, without exposing a Unity Catalog concept across the crate boundary.

### Decision

The catalog table carries a closed format tag (Iceberg or Delta) and an optional opaque vending key. The raw Unity wire fields stay crate-private, and Unity's table load fails on an absent or unrecognized format.

### Options Considered

| Option | Verdict |
|--------|---------|
| `CatalogClient` method that keeps the table ID inside the crate | Rejected: forces re-plumbing the Iceberg path for no benefit |
| Separate Unity session method returning table plus storage | Rejected: duplicates the table load on a second public entry point |
| Re-fetch the table inside the vending step | Rejected: breaks resolving metadata once per query |
| Newtype around the vending key | Rejected: puts a Unity concept on the public surface for no added invariant |

### Consequences

- Listing admission stays with the client, and the table format is data the engine reads.
- A format tag appears in a neutral type, because withholding it would force the engine to assume Unity implies Delta.

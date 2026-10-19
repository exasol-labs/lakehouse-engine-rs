# Decisions: add-delta-table-planning

## ADR: `FormatReader` lives in the engine and each implementation owns its whole resolution

**ID:** format-reader-engine-owns-whole-resolution
**Plan:** `add-delta-table-planning`
**Status:** Accepted

### Context

Delta is a second table format that must resolve to the engine's scan spec. The catalog crate must not name `iceberg`, `datafusion`, `arrow`, `parquet`, `object_store`, or `delta_kernel`, and the two formats need different metadata to reach their file lists.

### Decision

A `FormatReader` trait in the engine crate, as the per-format counterpart of `CatalogClient`, resolves a table's whole scan in one call: catalog request, storage credential, and file discovery. `CatalogClient` gains no method.

### Options Considered

| Option | Verdict |
|--------|---------|
| File-planning method on `CatalogClient` | Rejected: the catalog crate may not name those libraries |
| Shared caller pre-fetches metadata and readers only plan files | Rejected: the pre-fetch would itself fork per format |

### Consequences

A caller learns nothing about catalog protocol, credential vending, or whether files come from Iceberg manifests or a Delta commit log. The `CatalogClient` clause that adding an operation stays additive holds unedited.

## ADR: `CatalogTable` gains a neutral format tag and an opaque vending key

**ID:** catalog-table-neutral-format-tag-opaque-vending-key
**Plan:** `add-delta-table-planning`
**Status:** Accepted

### Context

`CatalogTable` did not carry the table format or the vending key. The Delta reader needs both, without exposing a Unity Catalog concept across the crate boundary.

### Decision

`CatalogTable` carries a closed `TableFormat` enum (Iceberg, Delta) and an optional opaque vending key. The raw Unity wire fields stay crate-private, and Unity's table load fails on an absent or unrecognized format.

### Options Considered

| Option | Verdict |
|--------|---------|
| `CatalogClient` method that keeps the table ID inside the crate | Rejected: forces re-plumbing the shipped Iceberg path for no benefit |
| Separate Unity session method returning table plus storage | Rejected: duplicates the table load on a second public entry point |
| Re-fetch the table inside the vending step | Rejected: breaks "resolve metadata once per query" |
| Newtype around the vending key | Rejected: puts a Unity concept on the public surface for no added invariant |

### Consequences

Listing admission (which entries are Delta base tables) stays owned by the client, while the table format is data the engine reads. This supersedes the clause that the data source format must not appear in any neutral type, because withholding it would force the engine to assume Unity implies Delta.

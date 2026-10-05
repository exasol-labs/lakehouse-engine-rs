# Decisions: add-direct-storage-catalog-kind

## ADR: A `CatalogClient` implementor may be declared in `lakehouse-engine`

**ID:** catalog-client-implementor-may-live-in-lakehouse-engine
**Plan:** add-direct-storage-catalog-kind
**Status:** Accepted

### Context

`DirectStorageCatalogClient` needs the engine-side object-store builder, and `lakehouse-catalog` may not depend on `object_store`. No recorded rule requires a `CatalogClient` implementor to live in the catalog crate. The rule is only that the engine reaches enumeration and table load through the trait, and the single construction site is already engine-side.

### Decision

`DirectStorageCatalogClient` is declared in `crates/lakehouse-engine` and implements the trait from `crates/lakehouse-catalog`. The catalog crate gains no source file, manifest dependency, or public item for this kind beyond three neutral enum variants.

### Options Considered

| Option | Verdict |
|--------|---------|
| Declare the client in `lakehouse-catalog` | Rejected: needs the engine-side object-store builder, and the catalog crate may not declare `object_store`, so the dependency edge would point backwards |

### Consequences

A future catalog kind that needs an engine-side dependency follows the same route. The compiler enforces the one-way dependency edge.

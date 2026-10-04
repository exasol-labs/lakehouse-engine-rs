# Decisions: add-direct-storage-catalog-kind

## ADR: A `CatalogClient` implementor may be declared in `lakehouse-engine`

**ID:** catalog-client-implementor-may-live-in-lakehouse-engine
**Plan:** add-direct-storage-catalog-kind
**Status:** Accepted

### Context

`DirectStorageCatalogClient` needs the engine-side object-store builder.
`vs-adapter/catalog-crate-structure` forbids `lakehouse-catalog` from declaring `object_store` as a
direct dependency. Declaring the client in `lakehouse-catalog` beside the two shipped implementors
would point that dependency edge backwards. Rust's orphan rule permits the impl because the type is
local even though the trait is not. No recorded rule requires an implementor of `CatalogClient`
to live in the catalog crate. The recorded requirement is only that the engine reach every
enumeration and table-load operation through the trait. The single `Box<dyn CatalogClient>`
construction site is already engine-side, so this placement adds no new seam.

### Decision

`DirectStorageCatalogClient` is declared in `crates/lakehouse-engine`. It implements the
`CatalogClient` trait declared in `crates/lakehouse-catalog`. The catalog crate gains no source
file, no manifest dependency, and no public item for this kind beyond three neutral enum variants.

### Options Considered

| Option | Verdict |
|--------|---------|
| Declare the client in `lakehouse-catalog` beside the two shipped implementors | ✗ Rejected — the client needs the engine-side object-store builder, and `vs-adapter/catalog-crate-structure` forbids the catalog crate from declaring `object_store` |

### Consequences

A future catalog kind that needs an engine-side dependency follows the same route. The dependency
edge stays one-way. The compiler enforces that direction.


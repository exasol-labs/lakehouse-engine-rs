# Decisions: add-direct-storage-catalog-kind

## ADR: A `CatalogClient` implementor may be declared in `lakehouse-engine`

**ID:** catalog-client-implementor-may-live-in-lakehouse-engine
**Plan:** add-direct-storage-catalog-kind
**Status:** Accepted

### Context

`DirectStorageCatalogClient` needs the engine-side object-store builder, and
`vs-adapter/catalog-crate-structure` forbids `lakehouse-catalog` from declaring `object_store` as a
direct dependency. Declaring the client in `lakehouse-catalog` beside the two shipped implementors
would point that dependency edge backwards. Rust's orphan rule permits the impl because the type is
local even though the trait is not, and no recorded rule requires an implementor of `CatalogClient`
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

## ADR: A structural invariant is not enforced by a test that matches production source text

**ID:** structural-invariant-not-enforced-by-source-text-matching
**Plan:** add-direct-storage-catalog-kind
**Status:** Accepted

### Context

`vs-adapter/catalog-kind-selection` has required, since issue #318, a source-level probe asserting
that `CatalogKind`'s variant names appear in no production module outside an enumerated set. That
probe was never built. What exists today is a compile-time signature probe
(`catalog_client_tests.rs`) whose own doc comment states it "does not (and cannot) prove the kind is
matched nowhere else". The only probe that could assert the recorded clause would read production
source text and match variant names against an allowlist.

### Decision

This plan does not build the source-level probe. It records that exhaustive matching plus review
holds the permitted-site set instead, a guarantee weaker than the clause promised. The edit to
`crates/lakehouse-catalog/tests/catalog_public_surface.rs` adds only external-vantage construction
of new variants, never a source-text assertion over a variant list. This rule binds every future
plan in this repository: no test in this project may enforce a structural invariant by matching
production source text.

### Options Considered

| Option | Verdict |
|--------|---------|
| Build the probe the recorded clause names, reading every production source file and matching `CatalogKind` variant names against an allowlist | ✗ Rejected on project convention |

### Consequences

The three matching sites stay exhaustive, so a fourth catalog kind is a build failure at each. A
fifth matching site added later is visible in review as a new `CatalogKind` import rather than as a
test failure.

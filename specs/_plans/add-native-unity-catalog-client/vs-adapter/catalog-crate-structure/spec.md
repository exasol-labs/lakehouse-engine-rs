# Feature: Catalog Crate Structure

The catalog access layer lives in a standalone `lakehouse-catalog` crate the engine depends on one way, exposing a concept-level API and hiding every mechanism step behind an external-vantage reachability probe.

## Background

This delta adds a shared `CatalogClient` trait and its catalog-neutral metadata types to the crate surface, so the engine holds ONE operation surface for every catalog kind. It supersedes the recorded enumeration of `pub` items in two directions: it ADMITS the trait, the neutral types, and the two client types the engine constructs, and it DEMOTES `list_namespace_tables` to crate-private now that the Iceberg client is its only caller. The Unity Catalog wire types stay crate-private, because the engine consumes only the neutral shape. The reachability probe is edited to name every added item and to assert both demotions, and the one-way dependency stays intact.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: One shared catalog-client trait and its neutral types become the crate's operation surface

* *GIVEN* the recorded clause enumerating "exactly these items SHALL be `pub`" on `lakehouse-catalog`, and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the shared catalog-client abstraction lands, so the engine adapter runs one listing pipeline for every catalog kind
* *THEN* the crate SHALL declare a trait-object-usable `CatalogClient` trait as the ONLY operation surface the engine's createVirtualSchema path uses to enumerate a namespace or load one table's metadata, and the recorded `pub` enumeration SHALL be SUPERSEDED to admit that trait, the two client types the engine constructs, and the catalog-neutral metadata types the trait returns — a table identifier carrying its namespace as SEGMENTS (never a pre-joined dotted string, because the engine's flattening and `TABLE_MAP` construction consume segments and re-splitting would introduce a separator ambiguity neither catalog guarantees against) plus its name; a table-metadata type carrying that identifier, its table type, its optional storage location, and its ordered columns; a column type carrying its name and a type descriptor; and a listing type carrying the resolved tables plus the identifiers the catalog reported as not loadable
* *AND* the trait SHALL carry NO file-planning, scan, or data-file method in this plan, and its two listing operations SHALL be shaped so that adding one later is an ADDITIVE change that reshapes neither of them
* *AND* the neutral column's type descriptor SHALL be SOURCE-TAGGED — carrying the Iceberg type for an Iceberg REST table and the Unity Catalog type name for a Unity Catalog table — and MUST NOT be pre-mapped to an Exasol type inside this crate, because `lakehouse-catalog` MUST NOT name the Exasol delivery mechanism or the engine's type-mapping home, and because discarding the source type here would destroy the fidelity the deferred Delta type work (#322) reads
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name the trait and every neutral type, and to assert that each client type is usable as a boxed `CatalogClient`, so narrowing one below `pub` or breaking trait-object compatibility is a build failure rather than a silent gap

### Scenario: Namespace enumeration is demoted once the Iceberg client is its only caller

* *GIVEN* the recorded clause naming `list_namespace_tables` among the items that SHALL be `pub` on `lakehouse-catalog`, and its recorded rationale that it reached crate-private-adjacent visibility only to serve one engine-side caller
* *WHEN* the Iceberg REST catalog client implements `CatalogClient` and calls `list_namespace_tables` from inside the crate, leaving the engine with no direct call site
* *THEN* `list_namespace_tables` SHALL be DEMOTED to crate-private and removed from the crate's `pub` re-exports, and the recorded `pub` enumeration SHALL be SUPERSEDED accordingly, because a `pub` function whose only caller is in-crate widens the surface this feature exists to narrow
* *AND* the reachability probe SHALL drop it from the enumerated `pub` set and SHALL assert the crate declares no `pub fn list_namespace_tables`, joining the existing demotion assertions for the vended-mechanism functions, which SHALL remain intact and unweakened
* *AND* the demotion SHALL change no behavior: the same namespaces are enumerated, the same identifiers are returned in the same order, and the same errors are produced under both the signed and unsigned paths

### Scenario: The native Unity Catalog client extends the crate's public surface through an explicit reviewed edit

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *WHEN* the native Unity Catalog client and its vended-credentials selector land in the crate
* *THEN* the crate SHALL add to its public surface exactly the concept-level Unity Catalog items the engine adapter consumes — the `UnityCatalogSession` type and its constructor, the temporary-credentials type `resolve_uc_vended_storage` consumes, and the `resolve_uc_vended_storage` function — each re-exported at the crate root, and it SHALL likewise expose the Iceberg REST catalog client type and its constructor, which are the two types the engine's single construction site builds; both client types SHALL implement `CatalogClient` and the engine adapter SHALL reach every enumeration and table-load operation THROUGH that trait, so neither type needs an inherent enumeration or table-load method on the public surface
* *AND* the Unity Catalog WIRE types (the deserialized `GET /tables` table and column shapes), the authentication strategy, the session's catalog and schema enumeration, and every request-construction step SHALL stay crate-private — so making the session reachable exposes neither auth internals nor a Unity-specific shape, mirroring how `CatalogSession`'s fields stay private; publishing the wire shape would let a consumer branch on which catalog it is talking to, which is the branch this trait exists to remove
* *AND* the reachability probe SHALL be edited — an explicit reviewed change to the probe file — to name every added Unity Catalog public item, and its existing demotion assertions that the crate declares no `pub fn` for the demoted vended-mechanism functions SHALL remain intact and unweakened
* *AND* the one-way dependency SHALL hold: no `lakehouse-catalog` source file SHALL name `lakehouse-engine`, and neither the `CatalogClient` trait, its neutral types, nor the Unity Catalog client SHALL name the Exasol CONNECTION or virtual-schema-property delivery mechanism, which stays in the engine adapter
<!-- /DELTA:NEW -->

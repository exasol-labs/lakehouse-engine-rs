<!-- DELTA:CHANGED -->
# Feature: Catalog Crate Public Surface Extensions

Tracks each explicit, reviewed extension of `lakehouse-catalog`'s enumerated public surface after
the crate boundary itself was drawn — the shared `CatalogClient` trait, a demotion once a caller
moved in-crate, and the narrow additions that the engine-side format, credential, and catalog-signing
work required.

This is the sibling of `vs-adapter/catalog-crate-structure`, split out once the base feature's
scenario count crossed this library's per-spec organization threshold. `vs-adapter/catalog-crate-structure`
owns the crate's existence, its behavior-preservation guarantee, and its concept-level API shape;
this feature owns the running history of what gets ADDED to that `pub` set and why, each entry an
explicit reviewed edit to the crate's reachability probe at
`crates/lakehouse-catalog/tests/catalog_public_surface.rs`.
<!-- /DELTA:CHANGED -->

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/catalog-crate-public-surface-extensions/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The SigV4 signing-region resolver is a public method of the shared credential type

* *GIVEN* the enumerated public surface of `lakehouse-catalog` and its external-vantage reachability probe at `crates/lakehouse-catalog/tests/catalog_public_surface.rs`, which fails to compile if any enumerated item is narrowed below `pub`
* *AND* three readers that need the region a SigV4-signed catalog request is signed for: the adapter's SigV4 credential guard in `lakehouse-engine`, the crate's signed namespace enumeration, and the crate's `CatalogSession` catalog requests
* *WHEN* the crate's public surface is declared
* *THEN* `ConnectionCreds` SHALL declare exactly ONE additional `pub` method, which returns that signing region from the credential set and a catalog URI, and returns nothing when neither the stated `region` nor the URI supplies one
* *AND* the crate SHALL add no other item to its public surface for the signing region
* *AND* the standard AWS Glue endpoint host rule and the signing-region precedence of `vs-adapter/connection-credentials` SHALL each have exactly ONE declaration, beside the crate's SigV4 request signing, so the adapter guard and the two signing paths cannot disagree about whether a signing region exists
* *AND* every step behind that method, including the host-shape parser, SHALL stay crate-private
* *AND* each of the two signing paths SHALL resolve its region through that method once per session or enumeration, and SHALL refuse to sign, returning an error that names `region` and contains no credential value, when the method returns nothing
* *AND* the method SHALL name no Exasol CONNECTION or virtual-schema-property delivery mechanism, and no `lakehouse-catalog` source file SHALL name `lakehouse_engine`
* *AND* the probe SHALL call the method, and its existing demotion assertions SHALL remain intact and unweakened
<!-- /DELTA:NEW -->
